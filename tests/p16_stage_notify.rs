//! P16 H2: the stage notify, a same-server collective message that Days models delay-only
//! (ruling H2-1). On the rail fabric a collective stage whose ranks share a server lowers to a
//! constant timer of its chunk (the sender's lead) whose notify then crosses its host pair's lane
//! to the target, where the whole chunk arrives at once.
//!
//! The contract: the lowered records and lanes; every stage finishes; Scalar equals the CPU
//! executor at one to four workers; and checkpoints taken while notifies are timed, in flight and
//! delivered validate and resume to the uninterrupted state.

use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days::topos::config::SpectrumXConfig;
use days::topos::rail::{RailTopology, ServerLocality};
use days_executor::{
    Backend, CollectiveStage, CpuConfig, EventKind, FlowGeneratorKind, GeneratorStatus, NodeKind,
    ObservationMode, PacketKind, RunResult, SimulationImage, StageRole, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn mini_rail() -> SimulationImage {
    compile_config(repo_path("configs/p16/rail_mini_roce.toml"))
        .unwrap_or_else(|error| panic!("configs/p16/rail_mini_roce.toml must lower: {error}"))
}

/// The fixture's fabric: 8 GPUs, 2 per server, NVLink 2,400 Gb/s and 25 ns.
fn locality() -> ServerLocality {
    let rail = RailTopology::new(&SpectrumXConfig {
        gpus: 8,
        gpus_per_server: 2,
        nics_per_asw: 2,
        psws: 2,
        gpu_type: "H100".to_owned(),
        nic_rate_bps: 100_000_000_000,
        uplink_rate_bps: 400_000_000_000,
        nvlink_rate_bps: 2_400_000_000_000,
        link_delay_ns: 500,
        nvlink_delay_ns: 25,
    })
    .expect("fixture fabric");
    ServerLocality::new(rail.profile())
}

/// `(flow, stage, constant generator)` of every stage notify, in host and generator order.
fn notifies(
    image: &SimulationImage,
) -> Vec<(
    days_executor::FlowId,
    CollectiveStage,
    days_executor::ConstantGenerator,
)> {
    image
        .host_states
        .iter()
        .flat_map(|state| {
            state
                .generators
                .iter()
                .enumerate()
                .filter_map(
                    |(position, generator)| match (generator.kind, state.stage(position)) {
                        (FlowGeneratorKind::Constant(constant), Some(stage))
                            if matches!(stage.role, StageRole::Collective(_)) =>
                        {
                            Some((generator.flow, stage, constant))
                        }
                        _ => None,
                    },
                )
        })
        .collect()
}

/// The host topology identity of a host node: the rail builder numbers GPUs 0..G and lowering
/// lists hosts first, in that order.
fn gpu(image: &SimulationImage, node: days_executor::NodeId) -> u64 {
    assert_eq!(image.nodes[node.0 as usize].kind, NodeKind::Host);
    node.0
}

#[test]
fn same_server_stages_lower_to_notifies_on_their_lanes() {
    let image = mini_rail();
    let locality = locality();
    let notifies = notifies(&image);
    // 8 ranks x 14 stages; the ring hops 0->1, 2->3, 4->5, 6->7 are same-server: 4 x 14.
    assert_eq!(notifies.len(), 56);
    for (flow, stage, constant) in &notifies {
        let descriptor = &image.flows[flow.0 as usize];
        let (source, target) = (
            gpu(&image, descriptor.source),
            gpu(&image, descriptor.target),
        );
        assert!(locality.same_server(source, target), "flow {flow:?}");
        assert!(descriptor.route.is_empty() && descriptor.reverse_route.is_empty());
        let StageRole::Collective(identity) = stage.role else {
            unreachable!()
        };
        assert_eq!(constant.packet_size_bytes, identity.chunk_bytes);
        let delay = locality
            .nvlink_message_delay_ns(identity.chunk_bytes, 1, 9_000)
            .expect("delay");
        assert_eq!(constant.interval_ns + constant.first_departure_ns, delay);
        assert!(constant.interval_ns >= 1);
    }
    // Every other stage is a RoCE queue pair with a fabric route.
    let roce_stages = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
        .count();
    assert_eq!(roce_stages, 56);
    // One lane per same-server ordered pair in use, at `min delay - 1`.
    let lanes = image
        .channels
        .iter()
        .filter(|channel| {
            image.nodes[channel.source.0 as usize].kind == NodeKind::Host
                && image.nodes[channel.target.0 as usize].kind == NodeKind::Host
        })
        .collect::<Vec<_>>();
    assert_eq!(lanes.len(), 4);
    for lane in lanes {
        let minimum = notifies
            .iter()
            .filter(|(flow, _, _)| {
                let descriptor = &image.flows[flow.0 as usize];
                descriptor.source == lane.source && descriptor.target == lane.target
            })
            .map(|(_, _, constant)| constant.interval_ns + constant.first_departure_ns)
            .min()
            .expect("a lane serves a notify");
        assert_eq!(lane.min_delay_ns, minimum - 1);
        assert_eq!(lane.event_kind, EventKind::RemoteArrival);
        let link = image.links[lane.link.0 as usize];
        assert_eq!((link.source, link.target), (lane.source, lane.target));
        assert_eq!(
            (link.rate_bps, link.propagation_ns),
            (2_400_000_000_000, 50)
        );
    }
    // Ring roots (step 1 of the reduce-scatter) start at time zero, notifies included.
    assert!(image.initial_events.iter().any(|event| {
        event.kind == EventKind::PacingTimer
            && image
                .initial_packets
                .binary_search_by_key(&event.payload, |packet| packet.id)
                .is_ok_and(|index| image.initial_packets[index].kind == PacketKind::StageNotify)
    }));
}

fn finished(result: &RunResult) -> bool {
    result.host_states.iter().all(|state| {
        state
            .generators
            .iter()
            .all(|generator| generator.next_emission.status == GeneratorStatus::Finished)
    })
}

#[test]
fn every_stage_finishes_and_no_notify_stays_resident() {
    for (_, image) in notify_fixtures() {
        every_stage_finishes(&image);
    }
}

fn every_stage_finishes(image: &SimulationImage) {
    let image = image.clone();
    let result =
        run_scalar_with_observations(&image, None, ObservationMode::Full).expect("scalar oracle");
    assert!(finished(&result), "every stage finishes");
    assert!(
        result
            .resident_packets
            .iter()
            .all(|packet| packet.kind != PacketKind::StageNotify)
    );
    assert!(result.pending_events.is_empty());
    for (flow, _, constant) in notifies(&image) {
        let generator = result
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .find(|generator| generator.flow == flow)
            .expect("generator");
        assert_eq!(
            (generator.packets_emitted, generator.bytes_emitted),
            (1, constant.packet_size_bytes)
        );
    }
}

fn without_diagnostics(mut result: RunResult) -> RunResult {
    result.diagnostics = None;
    result
}

#[test]
fn scalar_and_cpu_agree_at_one_to_four_workers() {
    for (_, image) in notify_fixtures() {
        scalar_and_cpu_agree(&image);
    }
}

fn scalar_and_cpu_agree(image: &SimulationImage) {
    for mode in [ObservationMode::Full, ObservationMode::Summary] {
        let scalar = run_scalar_with_observations(image, None, mode).expect("scalar");
        for workers in 1..=4 {
            let cpu = run_cpu_with_observations(
                image,
                None,
                CpuConfig {
                    workers,
                    ..CpuConfig::default()
                },
                mode,
            )
            .expect("cpu");
            assert_eq!(
                without_diagnostics(cpu.result),
                without_diagnostics(scalar.clone()),
                "{mode:?}, {workers} workers"
            );
        }
    }
}

fn checkpoint_image(original: &SimulationImage, checkpoint: &RunResult) -> SimulationImage {
    let mut image = original.clone();
    image.host_states.clone_from(&checkpoint.host_states);
    image.switch_states.clone_from(&checkpoint.switch_states);
    image
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    image.initial_events.clone_from(&checkpoint.pending_events);
    image
}

/// The notify fixtures: a RoCE ring all-reduce and a TCP AllGather on the miniature rail.
pub fn notify_fixtures() -> Vec<(String, SimulationImage)> {
    ["rail_mini_roce", "rail_mini_tcp_allgather"]
        .into_iter()
        .map(|name| {
            let image = compile_config(repo_path(&format!("configs/p16/{name}.toml")))
                .unwrap_or_else(|error| panic!("{name} must lower: {error}"));
            (name.to_owned(), image)
        })
        .collect()
}

/// The images of the notify suites: each fixture, then its checkpoints at horizons 1 and 100 and at
/// `count` horizons spread over its run, which hold notifies unreleased, timed, in flight and
/// delivered.
pub fn notify_images(count: u64) -> Vec<(String, SimulationImage)> {
    let mut images = Vec::new();
    for (name, image) in notify_fixtures() {
        let end = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("scalar")
            .departures
            .iter()
            .map(|departure| departure.time_ns)
            .max()
            .expect("the fixture sends");
        images.push((name.clone(), image.clone()));
        // Horizon 1 holds the root notifies' timers (leads of one or two nanoseconds), horizon
        // 100 the root notifies in flight (lanes of hundreds of nanoseconds); the spread holds
        // notifies released, delivered and later in flight.
        let horizons = [1, 100]
            .into_iter()
            .chain((1..=count).map(|step| end * step / (count + 1) + 1));
        for horizon in horizons {
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix");
            images.push((
                format!("{name}@{horizon}"),
                checkpoint_image(&image, &prefix),
            ));
        }
    }
    images
}

#[test]
fn checkpoints_with_timed_and_in_flight_notifies_resume_exactly() {
    let uninterrupted = notify_fixtures()
        .into_iter()
        .map(|(name, image)| {
            let result = run_scalar_with_observations(&image, None, ObservationMode::Summary)
                .expect("scalar");
            (name, result)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut timed = 0;
    let mut in_flight = 0;
    for (label, image) in notify_images(23)
        .into_iter()
        .filter(|(label, _)| label.contains('@'))
    {
        let uninterrupted = &uninterrupted[label.split('@').next().expect("name")];
        validate(&image, Backend::Scalar).unwrap_or_else(|error| panic!("{label}: {error}"));
        let kind_of = |payload| {
            image
                .initial_packets
                .binary_search_by_key(&payload, |packet: &days_executor::PacketDescriptor| {
                    packet.id
                })
                .ok()
                .map(|index| image.initial_packets[index].kind)
        };
        for event in &image.initial_events {
            if kind_of(event.payload) == Some(PacketKind::StageNotify) {
                match event.kind {
                    EventKind::PacingTimer => timed += 1,
                    EventKind::RemoteArrival => in_flight += 1,
                    other => panic!("{label}: a notify rides {other:?}"),
                }
            }
        }
        let resumed =
            run_scalar_with_observations(&image, None, ObservationMode::Summary).expect("resume");
        assert_eq!(resumed.host_states, uninterrupted.host_states, "{label}");
        assert_eq!(
            resumed.switch_states, uninterrupted.switch_states,
            "{label}"
        );
        assert_eq!(
            resumed.resident_packets, uninterrupted.resident_packets,
            "{label}"
        );
        assert_eq!(
            resumed.pending_events, uninterrupted.pending_events,
            "{label}"
        );
    }
    assert!(timed > 0, "some checkpoint holds a timed notify");
    assert!(in_flight > 0, "some checkpoint holds a notify in flight");
}

#[test]
fn a_notify_off_its_lane_or_with_a_wrong_lead_is_refused() {
    let image = mini_rail();
    let (flow, _, _) = notifies(&image)[0];
    let mut wrong_lane = image.clone();
    for state in &mut wrong_lane.host_states {
        for generator in &mut state.generators {
            if generator.flow == flow {
                let FlowGeneratorKind::Constant(constant) = &mut generator.kind else {
                    unreachable!()
                };
                constant.first_departure_ns += 1;
            }
        }
    }
    let error = validate(&wrong_lane, Backend::Scalar).expect_err("lane mismatch");
    assert!(error.to_string().contains("stage notify"), "{error}");

    let mut routed = image.clone();
    routed.flows[flow.0 as usize].route = image.flows[0].route.clone();
    routed.flows[flow.0 as usize]
        .route
        .push(image.host_states[0].egress_link);
    assert!(validate(&routed, Backend::Scalar).is_err());
}

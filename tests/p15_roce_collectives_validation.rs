//! P15 lane R3: the validator's rules for RoCE collective stages (design note §6.2, §6.3).
//!
//! Lowered images and Scalar checkpoints that hold gated, released and finished RoCE stages
//! validate; each one broken in one rule is refused with that rule's message.

use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    Backend, Event, EventKey, EventKind, FlowGeneratorKind, NodeKind, ObservationMode,
    SimulationImage, StageRole, run_scalar_with_observations, validate,
};

fn lower(name: &str) -> SimulationImage {
    compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
}

/// The state after a Scalar run up to `horizon_ns`, as a continuation image.
fn checkpoint(image: &SimulationImage, horizon_ns: u64) -> SimulationImage {
    let result = run_scalar_with_observations(image, Some(horizon_ns), ObservationMode::Full)
        .expect("the Scalar run to the horizon succeeds");
    SimulationImage {
        stop_time_ns: image.stop_time_ns,
        nodes: image.nodes.clone(),
        host_states: result.host_states,
        switch_states: result.switch_states,
        flows: image.flows.clone(),
        initial_packets: result.resident_packets,
        links: image.links.clone(),
        channels: image.channels.clone(),
        initial_events: result.pending_events,
        seed: image.seed,
        stage_joins: image.stage_joins.clone(),
    }
}

fn refused(image: &SimulationImage, needle: &str) {
    let error = validate(image, Backend::Scalar)
        .expect_err("the broken image must be refused")
        .to_string();
    assert!(error.contains(needle), "expected `{needle}` in: {error}");
}

/// (host slot, generator position) of every RoCE stage, with its release flag.
fn roce_stage_slots(image: &SimulationImage) -> Vec<(usize, usize, bool)> {
    image
        .host_states
        .iter()
        .enumerate()
        .flat_map(|(slot, state)| {
            state.generators_with_stages().enumerate().filter_map(
                move |(position, (generator, stage))| {
                    let stage = stage?;
                    (matches!(generator.kind, FlowGeneratorKind::Roce(_))
                        && matches!(stage.role, StageRole::Collective(_)))
                    .then_some((slot, position, stage.activated))
                },
            )
        })
        .collect()
}

fn with_roce(
    image: &SimulationImage,
    slot: usize,
    position: usize,
    edit: impl FnOnce(&mut days_executor::FlowGeneratorState, &mut days_executor::RoceGenerator),
) -> SimulationImage {
    let mut broken = image.clone();
    let generator = &mut broken.host_states[slot].generators[position];
    let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
        unreachable!()
    };
    edit(generator, &mut roce);
    generator.kind = FlowGeneratorKind::Roce(roce);
    broken
}

/// `image` with one more pending `PacingTimer` at `time_ns` on the host in `slot`, carrying
/// `payload`, keyed by the host's next origin sequence.
fn with_tick(
    image: &SimulationImage,
    slot: usize,
    payload: days_executor::PayloadId,
    time_ns: u64,
) -> SimulationImage {
    let mut broken = image.clone();
    let owner = broken
        .nodes
        .iter()
        .find(|node| node.kind == NodeKind::Host && node.state_slot as usize == slot)
        .expect("the host's node")
        .id;
    let origin_seq = broken.host_states[slot].next_origin_seq;
    broken.initial_events.push(Event {
        key: EventKey {
            time_ns,
            phase: 1,
            origin_node: owner,
            origin_seq,
        },
        target: owner,
        kind: EventKind::PacingTimer,
        payload,
    });
    broken.host_states[slot].next_origin_seq += 1;
    broken.initial_events.sort_by_key(|event| event.key);
    broken
}

/// Fix round 1 (review M1): a pacer that owns no tick (a gated stage, or a pair parked by a pause)
/// owns none at any time (in P15 a gated stage also owned no control tick; P16 removed the control
/// tick). The validator counted ticks only at the departure and the control deadline, so a stray tick at another time passed
/// validation and failed the Scalar run.
#[test]
fn a_gated_stage_owns_no_tick_at_any_time() {
    let image = lower("roce_compute_dag.toml");
    let (slot, position, activated) = roce_stage_slots(&image)[0];
    assert!(!activated);
    let FlowGeneratorKind::Roce(roce) = image.host_states[slot].generators[position].kind else {
        unreachable!()
    };
    for time_ns in [1_000, 4_000, 777_777] {
        refused(
            &with_tick(&image, slot, roce.pacing_timer_payload, time_ns),
            "parked pacer owns a pending tick",
        );
    }
}

/// Review M1, the parked branch shared with host-link PFC: a pair a pause parked (no tick pending,
/// packets left) given a stray tick is refused.
#[test]
fn a_pause_parked_pair_owns_no_tick_at_any_time() {
    let image = lower("hostpfc_incast_lossless.toml");
    let mut parked = 0;
    for horizon_ns in [350_000, 375_000] {
        let state = checkpoint(&image, horizon_ns);
        validate(&state, Backend::Scalar).expect("the mid-pause checkpoint validates");
        for (slot, host) in state.host_states.iter().enumerate() {
            for generator in &host.generators {
                let FlowGeneratorKind::Roce(roce) = generator.kind else {
                    continue;
                };
                if roce.pacer_armed || roce.next_psn >= roce.pacer.total_bytes {
                    continue;
                }
                parked += 1;
                refused(
                    &with_tick(
                        &state,
                        slot,
                        roce.pacing_timer_payload,
                        horizon_ns + 3_000_001,
                    ),
                    "parked pacer owns a pending tick",
                );
            }
        }
    }
    assert!(parked > 0, "no checkpoint held a pause-parked pair");
}

/// A gated stage holds the pristine state its release starts from (§6.1).
#[test]
fn a_gated_roce_stage_is_pristine_and_idle() {
    let image = lower("roce_compute_dag.toml");
    validate(&image, Backend::Scalar).expect("the lowered DAG validates");
    let (slot, position, activated) = roce_stage_slots(&image)[0];
    assert!(!activated, "every stage of the DAG is gated at lowering");

    let anchored = with_roce(&image, slot, position, |_, roce| {
        roce.pacer.first_pacing_time_ns = 1_000;
    });
    refused(
        &anchored,
        "dependency-blocked after its sending state changed",
    );
    let control = with_roce(&image, slot, position, |_, roce| {
        roce.controller.current_rate_bps -= 1;
    });
    refused(
        &control,
        "has a DCQCN controller that moved before its first feedback",
    );
    let credited = with_roce(&image, slot, position, |_, roce| {
        roce.pacer.credit_quanta = 1;
    });
    refused(
        &credited,
        "dependency-blocked after its sending state changed",
    );

    // A pending tick for a gated stage's token, at its scheduled departure.
    let mut ticking = image.clone();
    let host = &ticking.host_states[slot];
    let FlowGeneratorKind::Roce(roce) = host.generators[position].kind else {
        unreachable!()
    };
    let owner = ticking
        .nodes
        .iter()
        .find(|node| node.kind == NodeKind::Host && node.state_slot as usize == slot)
        .expect("the host's node")
        .id;
    ticking.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: 1,
            origin_node: owner,
            origin_seq: host.next_origin_seq,
        },
        target: owner,
        kind: EventKind::PacingTimer,
        payload: roce.pacing_timer_payload,
    });
    ticking.host_states[slot].next_origin_seq += 1;
    ticking.initial_events.sort_by_key(|event| event.key);
    refused(&ticking, "parked pacer owns a pending tick");
}

/// Gated RoCE stages reserve the payload identities of their future sends, as TCP stages do.
#[test]
fn gated_roce_stages_reserve_their_payload_identities() {
    let image = lower("roce_compute_dag.toml");
    let node_count = image.nodes.len() as u64;
    let mut exhausted = image;
    for node in exhausted
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let maximum_sequence = (u64::MAX - node.id.0) / node_count;
        exhausted.host_states[node.state_slot as usize].next_payload_seq = maximum_sequence + 1;
    }
    refused(&exhausted, "payload identity sequence");
}

/// A RoCE stage's inbound bytes are its receiver's Go-back-N frontier (§6.2 item 3).
#[test]
fn a_roce_stage_counts_its_receivers_frontier() {
    let image = lower("roce_ring_lossy.toml");
    let state = checkpoint(&image, 3_000_000);
    validate(&state, Backend::Scalar).expect("the checkpoint validates");
    // A gated stage whose inbound predecessor has delivered some bytes but not all.
    let (slot, position) = state
        .host_states
        .iter()
        .enumerate()
        .flat_map(|(slot, host)| {
            host.stages
                .iter()
                .enumerate()
                .filter_map(move |(position, stage)| {
                    let dependencies = stage.as_ref()?.dependencies;
                    (dependencies.inbound_bytes_received > 0 && !dependencies.inbound_complete())
                        .then_some((slot, position))
                })
        })
        .next()
        .expect("a stage part-way through its inbound chunk at 3 ms");
    let mut broken = state.clone();
    let stage = broken.host_states[slot].stages[position]
        .as_mut()
        .expect("a stage");
    stage.dependencies.inbound_bytes_received -= 1;
    refused(&broken, "disagree with the in-order frontiers");
}

/// Host-link PFC (§5.3): a gated stage on a paused host is not on its parked list, and listing it
/// is refused; a checkpoint with one validates.
#[test]
fn gated_stages_on_a_paused_host_stay_off_its_parked_list() {
    let image = lower("roce_ring_allreduce_lossless.toml");
    let mut found = 0;
    for horizon_ns in (1..=60).map(|step| step * 50_000) {
        let state = checkpoint(&image, horizon_ns);
        validate(&state, Backend::Scalar)
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        for (slot, position, activated) in roce_stage_slots(&state) {
            let host = &state.host_states[slot];
            let Some(pfc) = host.pfc.as_deref() else {
                continue;
            };
            if activated || !pfc.is_paused(3) {
                continue;
            }
            found += 1;
            let mut listed = state.clone();
            listed.host_states[slot]
                .pfc
                .as_deref_mut()
                .expect("pause state")
                .pause_parked[3]
                .insert(position);
            refused(&listed, "parked list");
        }
    }
    assert!(
        found > 0,
        "no checkpoint held a gated stage on a paused host"
    );
}

/// Schema Amendment 5: a compute stage's progress rows name its inbound transport from its local
/// predecessor (the same rank's final stage of the same collective), so the validator requires
/// the two predecessors' MTU and pacing interval to agree.
#[test]
fn a_compute_stages_predecessors_share_their_roce_transport() {
    let image = lower("roce_compute_dag.toml");
    validate(&image, Backend::Scalar).expect("the lowered DAG validates");
    let inbound = image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .find_map(|(_, stage)| match stage?.role {
            StageRole::Compute(_) => stage?.dependencies.inbound.one(),
            _ => None,
        })
        .expect("a compute stage after the collective");
    let (slot, position) = image
        .host_states
        .iter()
        .enumerate()
        .find_map(|(slot, state)| {
            state
                .generators
                .iter()
                .position(|generator| generator.flow == inbound)
                .map(|position| (slot, position))
        })
        .expect("the inbound predecessor's generator");
    let mtu = with_roce(&image, slot, position, |_, roce| roce.pacer.mtu_bytes -= 1);
    refused(&mtu, "disagree on the RoCE MTU or pacing interval");
    let interval = with_roce(&image, slot, position, |_, roce| {
        roce.pacer.pacing_interval_ns += 1
    });
    refused(&interval, "disagree on the RoCE MTU or pacing interval");
}

//! P14 T3: delay-only compute stages.
//!
//! A `[[compute]]` entry lowers to one timer-only stage per host. It sends no bytes and completes
//! when its timer fires, `duration_ns` after its prerequisites release it. Dependencies use the
//! same two slots as data stages: `after = "<compute>"` makes the same-host compute stage the local
//! predecessor; `after = "<collective>"` waits for the rank's own final stage (acknowledged) and
//! the previous rank's final stage (delivered); a collective with `after = "<compute>"` gates each
//! rank's root stages on that rank's compute stage.

#[path = "collective_tcp.rs"]
#[allow(dead_code)]
mod tcp;

use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    Backend, CollectiveActivationCause, CollectiveProgressRecord, CollectiveStageKind, CpuConfig,
    FlowGeneratorKind, FlowId, GeneratorStatus, HostState, MechanismTransitionRecord,
    ObservationMode, PacketKind, RunResult, SimulationImage, StageRole, TcpTransitionInput,
    collective_transitions_csv, run_cpu_with_observations, run_scalar_with_observations, validate,
};

const FORWARD_NS: u64 = 5_000;
const BACKWARD_NS: u64 = 7_000;

fn chain_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 3, 9_001, 100).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + &format!(
        r#"
[[compute]]
name = "forward"
hosts = [0, 1, 2]
duration_ns = {FORWARD_NS}

[[compute]]
name = "backward"
hosts = [0, 1, 2]
duration_ns = {BACKWARD_NS}
after = "grad"
"#
    )
}

fn run_everywhere(image: &SimulationImage, label: &str) -> RunResult {
    validate(image, Backend::Scalar).unwrap();
    let scalar = run_scalar_with_observations(image, None, ObservationMode::Full).unwrap();
    for workers in [1, 2, 4] {
        validate(image, Backend::Cpu { workers }).unwrap();
        let cpu = run_cpu_with_observations(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, scalar, "{label}, workers={workers}");
    }
    scalar
}

fn progress(result: &RunResult) -> Vec<CollectiveProgressRecord> {
    result
        .diagnostics
        .as_ref()
        .unwrap()
        .mechanism_transitions
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Collective(record) => Some(*record),
            _ => None,
        })
        .collect()
}

/// (duration, rank) -> (flow, compute id) for every compute stage in the image.
fn compute_stages(image: &SimulationImage) -> BTreeMap<(u64, u32), (FlowId, u64)> {
    image
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
        .filter_map(|(generator, stage)| match stage?.role {
            StageRole::Compute(compute) => Some((
                (compute.duration_ns, compute.rank),
                (generator.flow, compute.compute_id),
            )),
            StageRole::Collective(_) => None,
        })
        .collect()
}

fn finished_at(result: &RunResult, flow: FlowId) -> u64 {
    let generator = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .find(|generator| generator.flow == flow)
        .unwrap();
    assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
    generator.next_emission.departure_time_ns
}

#[test]
fn compute_stages_lower_to_timer_only_generators() {
    let image = tcp::compile_text("compute-lowering", &chain_config());
    let stages = compute_stages(&image);
    assert_eq!(stages.len(), 6);
    for (generator, stage) in image
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
    {
        let Some(stage) = stage else {
            panic!("every stage in the chain carries a record")
        };
        match stage.role {
            StageRole::Compute(compute) => {
                let FlowGeneratorKind::Constant(constant) = generator.kind else {
                    panic!("a compute stage is a zero-byte constant generator")
                };
                assert_eq!(constant.packet_size_bytes, 0);
                assert_eq!(constant.interval_ns, compute.duration_ns);
                let flow = &image.flows[generator.flow.0 as usize];
                assert_eq!(flow.source, flow.target);
                assert!(flow.route.is_empty() && flow.reverse_route.is_empty());
                let forward = compute.duration_ns == FORWARD_NS;
                assert_eq!(stage.activated, forward);
                assert_eq!(
                    generator.next_emission.status,
                    if forward {
                        GeneratorStatus::Scheduled
                    } else {
                        GeneratorStatus::Blocked
                    }
                );
                if forward {
                    assert_eq!(generator.next_emission.departure_time_ns, FORWARD_NS);
                    let token = image
                        .initial_packets
                        .iter()
                        .find(|packet| packet.id == generator.next_emission.payload)
                        .unwrap();
                    assert_eq!((token.size_bytes, token.kind), (0, PacketKind::Data));
                }
            }
            StageRole::Collective(identity) => {
                let root = identity.step == 1
                    && identity.phase == days_executor::CollectivePhase::ReduceScatter;
                assert!(stage.dependencies.local.one().is_some());
                if root {
                    assert!(stage.dependencies.inbound.one().is_none());
                    assert!(!stage.activated);
                    let predecessor = stage.dependencies.local.one().unwrap();
                    assert_eq!(
                        stages[&(FORWARD_NS, identity.rank)].0,
                        predecessor,
                        "forward gates rank"
                    );
                }
            }
        }
    }
    assert_eq!(image.initial_events.len(), 3, "only forward timers start");
    assert!(
        image
            .initial_events
            .iter()
            .all(|event| event.kind == days_executor::EventKind::PacingTimer
                && event.key.time_ns == FORWARD_NS)
    );
}

#[test]
fn compute_ring_compute_chain_has_exact_completion_times() {
    let image = tcp::compile_text("compute-chain", &chain_config());
    let stages = compute_stages(&image);
    let result = run_everywhere(&image, "chain");
    assert!(result.pending_events.is_empty());
    let rows = progress(&result);

    // Forward compute completes at exactly its duration and releases each rank's root stage then.
    for rank in 0..3 {
        let (forward, _) = stages[&(FORWARD_NS, rank)];
        assert_eq!(finished_at(&result, forward), FORWARD_NS);
        let release = rows
            .iter()
            .find(|row| {
                row.cause == CollectiveActivationCause::LocalCompletion && row.cause_flow == forward
            })
            .expect("forward completion releases the ring root");
        assert_eq!(release.key.time_ns, FORWARD_NS);
        assert!(release.activated);
        assert_eq!(release.stage_kind, CollectiveStageKind::Tcp);
    }

    // Backward compute starts when the rank's final ring stage is acknowledged and the previous
    // rank's final stage is delivered in order, and finishes exactly `BACKWARD_NS` later.
    let totals = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => Some((generator.flow, tcp.total_bytes)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut acknowledged = BTreeMap::new();
    for record in &result.diagnostics.as_ref().unwrap().tcp_transitions {
        if let TcpTransitionInput::NewAck { acknowledgment, .. } = record.input {
            if acknowledgment >= totals[&record.flow] {
                acknowledged.insert(record.flow, record.key.time_ns);
            }
        }
    }
    for rank in 0..3 {
        let (backward, _) = stages[&(BACKWARD_NS, rank)];
        let activation = rows
            .iter()
            .find(|row| row.flow == backward && row.activated)
            .expect("backward compute activates");
        assert_eq!(activation.stage_kind, CollectiveStageKind::Compute);
        assert_eq!(activation.duration_ns, BACKWARD_NS);
        let local = acknowledged[&tcp::stage_predecessors(&image)[&activation.flow].0.unwrap()];
        let inbound_rows = rows
            .iter()
            .filter(|row| {
                row.flow == backward
                    && row.cause == CollectiveActivationCause::InboundArrival
                    && row.after_inbound_complete
            })
            .collect::<Vec<_>>();
        assert_eq!(inbound_rows.len(), 1);
        let inbound = inbound_rows[0].key.time_ns;
        assert_eq!(activation.key.time_ns, local.max(inbound));
        assert_eq!(
            finished_at(&result, backward),
            activation.key.time_ns + BACKWARD_NS
        );
        assert_eq!(activation.after_status, GeneratorStatus::Scheduled);
        assert_eq!(
            activation.after_next_time_ns,
            activation.key.time_ns + BACKWARD_NS
        );
    }

    let csv = collective_transitions_csv(
        &result.diagnostics.as_ref().unwrap().mechanism_transitions,
        &image,
    )
    .unwrap();
    assert!(
        csv.lines()
            .next()
            .unwrap()
            .contains(",stage_kind,duration_ns,")
    );
    assert!(csv.lines().any(|row| row.contains(",compute,7000,")));
    // A compute timer completes its successor exactly at arm time + duration.
    for row in &rows {
        if row.cause == CollectiveActivationCause::LocalCompletion
            && stages.values().any(|(flow, _)| *flow == row.cause_flow)
        {
            assert_eq!(row.cause_origin_ns + row.cause_delay_ns, row.key.time_ns);
            assert_eq!(row.ack_number, 0);
        }
    }
}

#[test]
fn compute_after_compute_finishes_at_the_summed_durations() {
    let config = tcp::tcp_collective_config("RingAllReduce", 2, 1_000, 100)
        + r#"
[[compute]]
name = "a"
hosts = [0, 1]
duration_ns = 5000

[[compute]]
name = "b"
hosts = [0, 1]
duration_ns = 7000
after = "a"
"#;
    let image = tcp::compile_text("compute-compute", &config);
    let stages = compute_stages(&image);
    let result = run_everywhere(&image, "compute-compute");
    for rank in 0..2 {
        assert_eq!(finished_at(&result, stages[&(5_000, rank)].0), 5_000);
        assert_eq!(finished_at(&result, stages[&(7_000, rank)].0), 12_000);
    }
}

fn lowering_error(config: &str) -> String {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p14-compute-reject-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, config).unwrap();
    let error = compile_config(&path).expect_err("must reject");
    fs::remove_file(path).unwrap();
    error.to_string()
}

#[test]
fn compute_dependency_errors_are_precise() {
    let base = tcp::tcp_collective_config("RingAllReduce", 3, 9_001, 100);
    let compute = |body: &str| format!("{base}\n[[compute]]\n{body}\n");
    assert_eq!(
        lowering_error(&compute("name = \"a\"\nhosts = [0, 1, 2]\nduration_ns = 0")),
        "invalid scenario: compute `a` duration_ns must be positive"
    );
    assert_eq!(
        lowering_error(&compute(
            "name = \"a\"\nhosts = [0, 1, 2]\nduration_ns = 1\nafter = \"missing\""
        )),
        "invalid scenario: compute `a` depends on unknown stage group `missing`"
    );
    assert_eq!(
        lowering_error(&compute(
            "name = \"a\"\nhosts = [0, 1, 2]\nduration_ns = 1\nafter = \"a\""
        )),
        "invalid scenario: stage group dependencies form a cycle through `a`"
    );
    let named = base.replace("[[collective]]\n", "[[collective]]\nname = \"ring\"\n");
    assert_eq!(
        lowering_error(&format!(
            "{named}\n[[compute]]\nname = \"a\"\nhosts = [0, 1]\nduration_ns = 1\nafter = \"ring\"\n"
        )),
        "invalid scenario: compute `a` hosts must equal the ranks of `ring` in order"
    );
    assert_eq!(
        lowering_error(&format!(
            "{named}\n[[compute]]\nname = \"ring\"\nhosts = [0, 1, 2]\nduration_ns = 1\n"
        )),
        "invalid scenario: stage group name `ring` is used more than once"
    );
}

#[test]
fn compute_validator_rejects_inconsistent_stage_state() {
    let image = tcp::compile_text("compute-validator", &chain_config());
    let stages = compute_stages(&image);
    let locate = |image: &SimulationImage, flow: FlowId| {
        image
            .host_states
            .iter()
            .enumerate()
            .find_map(|(slot, state)| {
                state
                    .generators
                    .iter()
                    .position(|generator| generator.flow == flow)
                    .map(|index| (slot, index))
            })
            .unwrap()
    };
    let (backward, _) = stages[&(BACKWARD_NS, 1)];
    let (forward, _) = stages[&(FORWARD_NS, 1)];
    let reject = |mutate: &dyn Fn(&mut SimulationImage), expected: String| {
        let mut mutated = image.clone();
        mutate(&mut mutated);
        assert_eq!(
            validate(&mutated, Backend::Scalar).unwrap_err().to_string(),
            expected
        );
    };

    let (slot, index) = locate(&image, backward);
    reject(
        &|image| {
            image.host_states[slot].stages[index]
                .as_mut()
                .unwrap()
                .activated = true;
        },
        format!("flow {backward:?} stage release flag disagrees with its prerequisites"),
    );
    reject(
        &|image| {
            let host = &mut image.host_states[slot];
            let mut dependencies = host.stage_dependencies(index).unwrap();
            dependencies.local = days_executor::StagePredecessors::One(forward);
            host.set_stage_dependencies(index, dependencies);
        },
        format!(
            "flow {backward:?} compute predecessors are neither a same-rank compute stage nor a collective's final stages"
        ),
    );
    reject(
        &|image| {
            let host = &mut image.host_states[slot];
            let mut dependencies = host.stage_dependencies(index).unwrap();
            dependencies.inbound_bytes_received = 1;
            host.set_stage_dependencies(index, dependencies);
        },
        format!(
            "flow {backward:?} stage inbound bytes 1 disagree with the in-order frontiers 0 of its inbound predecessors"
        ),
    );
    let (slot, index) = locate(&image, forward);
    reject(
        &|image| {
            image.host_states[slot].generators[index]
                .next_emission
                .departure_time_ns += 1;
        },
        format!("flow {forward:?} compute timer state Scheduled is inconsistent with its release"),
    );
    reject(
        &|image| {
            let generator = &mut image.host_states[slot].generators[index];
            let FlowGeneratorKind::Constant(mut constant) = generator.kind else {
                unreachable!()
            };
            constant.packet_size_bytes = 1;
            generator.kind = FlowGeneratorKind::Constant(constant);
        },
        format!("flow {forward:?} compute stage requires a zero-byte constant timer generator"),
    );
}

#[test]
fn compute_only_scenarios_run_on_scalar_and_cpu_and_validate_on_devices() {
    let config = r#"
seed = 26
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "a"
hosts = [0, 1]
duration_ns = 4000

[[compute]]
name = "late"
hosts = [0, 1]
duration_ns = 7000
after = "a"
"#;
    let image = tcp::compile_text("compute-only", config);
    // P16 G1: the device backends accept compute stages (identity in
    // `tests/p16_device_collectives.rs`).
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend).expect("devices accept compute stages");
    }
    let result = run_everywhere(&image, "compute-only");
    assert!(result.pending_events.is_empty());
    assert_eq!(result.summary.sourced_packets, 0);
    let stages = compute_stages(&image);
    for rank in 0..2 {
        assert_eq!(finished_at(&result, stages[&(4_000, rank)].0), 4_000);
        // 4 us + 7 us passes the 10 us stop: the late stage stops without a timer event.
        let late = result
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .find(|generator| generator.flow == stages[&(7_000, rank)].0)
            .unwrap();
        assert_eq!(late.next_emission.status, GeneratorStatus::Stopped);
        assert_eq!(late.next_emission.departure_time_ns, 11_000);
    }
}

#[test]
fn compute_hosts_must_be_configured_host_attachments() {
    // Review F1: a root compute group naming a non-host must fail lowering, not panic.
    let config = |hosts: &str| {
        format!(
            r#"
seed = 26
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "a"
hosts = {hosts}
duration_ns = 4000
"#
        )
    };
    for (hosts, bad) in [("[0, 7]", 7), ("[0, 2]", 2)] {
        assert_eq!(
            lowering_error(&config(hosts)),
            format!(
                "invalid scenario: compute `a` host {bad} must be a configured host attachment"
            )
        );
    }
    // A compute-after-compute chain whose root names a non-host is caught at the root.
    let chained = config("[0, 7]")
        + "\n[[compute]]\nname = \"b\"\nhosts = [0, 7]\nduration_ns = 1\nafter = \"a\"\n";
    assert_eq!(
        lowering_error(&chained),
        "invalid scenario: compute `a` host 7 must be a configured host attachment"
    );
}

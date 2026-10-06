//! P14 T2: collective stages over TCP.
//!
//! A wrapped TCP stage's local predecessor completes when its last byte is acknowledged at the
//! sender; its inbound predecessor completes when this host's in-order TCP frontier reaches the
//! predecessor's byte count. A stage activates at the later of the two completions.

use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    ArrivalDisposition, Backend, CollectiveActivationCause, CollectiveProgressRecord, CpuConfig,
    FlowGeneratorKind, FlowId, GeneratorStatus, HostState, MechanismTransitionRecord,
    ObservationMode, PacketKind, RunResult, SimulationImage, StageRole, TcpTransitionInput,
    collective_transitions_csv, run_cpu_with_observations, run_scalar_with_observations, validate,
};

pub fn tcp_collective_config(algorithm: &str, ranks: u64, size: u64, capacity: u64) -> String {
    let switch = ranks;
    let edges = (0..ranks)
        .map(|host| format!("[{host}, {switch}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let hosts = (0..ranks)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sinks = (0..ranks)
        .map(|host| ((host + 1) % ranks).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
seed = 26
edges = [{edges}]
hosts = [{hosts}]
duration = 0.05

[switch]
port_rate = 8000000000
capacity = {capacity}
discipline = "FIFO"
drop = "TailDrop"

[[collective]]
collective_type = "{algorithm}"
flow_type = "TCP"
flow_count = {ranks}
sources = [{hosts}]
sinks = [{sinks}]

[collective.traffic]
initial_delay = 0.0
size = {size}
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 500, high = 500 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"
"#
    )
}

pub fn compile_text(label: &str, config: &str) -> SimulationImage {
    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p14-{label}-{}-{}.toml",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, config).expect("write fixture");
    let image = compile_config(&path);
    fs::remove_file(&path).expect("remove fixture");
    image.unwrap_or_else(|error| panic!("{label} must lower: {error}"))
}

pub fn progress(result: &RunResult) -> Vec<CollectiveProgressRecord> {
    result
        .diagnostics
        .as_ref()
        .expect("full observation")
        .mechanism_transitions
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Collective(record) => Some(*record),
            _ => None,
        })
        .collect()
}

pub fn tcp_total_bytes(image: &SimulationImage) -> BTreeMap<FlowId, u64> {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .map(|generator| {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                panic!("every TCP collective stage is an ordinary TCP generator")
            };
            (generator.flow, tcp.total_bytes)
        })
        .collect()
}

/// Sender-side completion: the event time of the new ACK that reaches the flow's total.
pub fn acknowledged_at(
    result: &RunResult,
    totals: &BTreeMap<FlowId, u64>,
) -> BTreeMap<FlowId, u64> {
    let mut completed = BTreeMap::new();
    for record in &result.diagnostics.as_ref().unwrap().tcp_transitions {
        if let TcpTransitionInput::NewAck { acknowledgment, .. } = record.input {
            if acknowledgment >= totals[&record.flow] {
                assert!(completed.insert(record.flow, record.key.time_ns).is_none());
            }
        }
    }
    completed
}

/// Receiver-side completion: replays delivered TCP data and returns when the in-order frontier
/// first reaches the flow's total.
pub fn delivered_at(result: &RunResult, totals: &BTreeMap<FlowId, u64>) -> BTreeMap<FlowId, u64> {
    let packets = result
        .observed_packets
        .iter()
        .map(|packet| (packet.id, *packet))
        .collect::<BTreeMap<_, _>>();
    let mut received = BTreeMap::<FlowId, Vec<(u64, u64)>>::new();
    let mut completed = BTreeMap::new();
    for arrival in &result.arrivals {
        if arrival.disposition != ArrivalDisposition::Delivered {
            continue;
        }
        let packet = packets[&arrival.payload];
        let PacketKind::TcpData(header) = packet.kind else {
            continue;
        };
        let ranges = received.entry(packet.flow).or_default();
        ranges.push((header.sequence, header.sequence + packet.size_bytes));
        ranges.sort_unstable();
        let frontier = ranges.iter().fold(0, |frontier, &(start, end)| {
            if start <= frontier {
                frontier.max(end)
            } else {
                frontier
            }
        });
        if frontier >= totals[&packet.flow] && !completed.contains_key(&packet.flow) {
            completed.insert(packet.flow, arrival.time_ns);
        }
    }
    completed
}

pub fn run_everywhere(image: &SimulationImage, label: &str) -> RunResult {
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

#[test]
fn tcp_collectives_lower_to_wrapped_tcp_generators() {
    for (algorithm, ranks) in [("RingAllReduce", 3), ("AllGather", 4)] {
        let image = compile_text(
            "tcp-lowering",
            &tcp_collective_config(algorithm, ranks, 10_001, 100),
        );
        let stages_per_rank = match algorithm {
            "RingAllReduce" => 2 * (ranks - 1),
            _ => ranks - 1,
        };
        assert_eq!(image.flows.len() as u64, ranks * stages_per_rank);
        let generators = image
            .host_states
            .iter()
            .flat_map(HostState::generators_with_stages)
            .collect::<Vec<_>>();
        for (generator, stage) in &generators {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                panic!("{algorithm}: TCP stages keep the ordinary TCP generator")
            };
            let stage = stage.expect("a TCP collective stage carries its record");
            let StageRole::Collective(identity) = stage.role else {
                panic!("a transport stage has a collective role")
            };
            assert_eq!(tcp.total_bytes, identity.chunk_bytes);
            let root = stage.dependencies.local.one().is_none()
                && stage.dependencies.inbound.one().is_none();
            assert_eq!(identity.step == 1 && root, root, "roots are step one");
            assert_eq!(stage.activated, root);
            assert_eq!(
                generator.next_emission.status,
                if root {
                    GeneratorStatus::Scheduled
                } else {
                    GeneratorStatus::Blocked
                }
            );
        }
        assert_eq!(
            image.initial_events.len() as u64,
            ranks,
            "only roots are scheduled"
        );
        assert_eq!(
            image
                .host_states
                .iter()
                .map(|state| state.tcp_receivers.len())
                .sum::<usize>(),
            generators.len()
        );
    }
}

#[test]
fn tcp_collectives_are_scalar_cpu_byte_identical_and_complete() {
    for algorithm in ["RingAllReduce", "AllGather"] {
        for ranks in [2, 3, 4] {
            let label = format!("{algorithm} n={ranks}");
            let image = compile_text(
                "tcp-identity",
                &tcp_collective_config(algorithm, ranks, 10_001, 100),
            );
            let result = run_everywhere(&image, &label);
            assert!(result.pending_events.is_empty(), "{label}");
            assert_eq!(result.summary.dropped_packets, 0, "{label}");
            let delivered_data = result
                .arrivals
                .iter()
                .filter(|arrival| arrival.disposition == ArrivalDisposition::Delivered)
                .count();
            assert!(delivered_data > 0);
            for (generator, stage) in result
                .host_states
                .iter()
                .flat_map(HostState::generators_with_stages)
            {
                assert_eq!(
                    generator.next_emission.status,
                    GeneratorStatus::Finished,
                    "{label}"
                );
                let stage = stage.unwrap();
                assert!(stage.activated, "{label}");
                assert!(stage.dependencies.prerequisites_complete(), "{label}");
                let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                    unreachable!()
                };
                assert_eq!(tcp.highest_ack, tcp.total_bytes, "{label}");
            }
            let csv = collective_transitions_csv(
                &result.diagnostics.as_ref().unwrap().mechanism_transitions,
            )
            .unwrap();
            let header = csv.lines().next().unwrap();
            let columns = header.split(',').collect::<Vec<_>>();
            let stage_kind = columns
                .iter()
                .position(|name| *name == "stage_kind")
                .unwrap();
            // Two-rank AllGather has only root stages, so no prerequisite ever progresses.
            assert_eq!(
                csv.lines().count() == 1,
                algorithm == "AllGather" && ranks == 2,
                "{label}"
            );
            assert!(
                csv.lines()
                    .skip(1)
                    .all(|row| row.split(',').nth(stage_kind) == Some("tcp"))
            );
        }
    }
}

#[test]
fn tcp_stage_completion_follows_ack_and_in_order_delivery() {
    for algorithm in ["RingAllReduce", "AllGather"] {
        for ranks in [2, 3, 4] {
            let label = format!("{algorithm} n={ranks}");
            let image = compile_text(
                "tcp-order",
                &tcp_collective_config(algorithm, ranks, 10_001, 100),
            );
            let totals = tcp_total_bytes(&image);
            let result = run_everywhere(&image, &label);
            let acknowledged = acknowledged_at(&result, &totals);
            let delivered = delivered_at(&result, &totals);
            assert_eq!(acknowledged.len(), totals.len(), "{label}");
            assert_eq!(delivered.len(), totals.len(), "{label}");

            let rows = progress(&result);
            assert_eq!(
                rows.is_empty(),
                algorithm == "AllGather" && ranks == 2,
                "{label}"
            );
            let mut activations = 0;
            for row in &rows {
                match row.cause {
                    CollectiveActivationCause::LocalCompletion => {
                        assert_eq!(row.key.time_ns, acknowledged[&row.cause_flow], "{label}");
                        // The certificate names the completing ACK and a lower bound on its time.
                        assert_eq!(row.ack_number, totals[&row.cause_flow], "{label}");
                        assert!(row.cause_delay_ns > 0);
                        assert!(row.cause_origin_ns + row.cause_delay_ns <= row.key.time_ns);
                        assert_eq!((row.segment_sequence, row.segment_bytes), (0, 0));
                    }
                    CollectiveActivationCause::InboundArrival => {
                        // Lossless: every certified segment advances the frontier.
                        assert!(row.arrival_bytes > 0);
                        assert!(row.segment_bytes > 0);
                        assert_eq!(
                            (row.ack_number, row.cause_origin_ns, row.cause_delay_ns),
                            (0, 0, 0)
                        );
                        assert_eq!(
                            row.after_inbound_bytes,
                            row.before_inbound_bytes + row.arrival_bytes
                        );
                        if row.after_inbound_complete {
                            assert_eq!(row.key.time_ns, delivered[&row.cause_flow], "{label}");
                            assert_eq!(row.after_inbound_bytes, totals[&row.cause_flow]);
                        } else {
                            assert!(row.key.time_ns < delivered[&row.cause_flow]);
                        }
                    }
                }
                if row.activated {
                    activations += 1;
                    let local = row.local_predecessor.map_or(0, |flow| acknowledged[&flow]);
                    let inbound = row.inbound_predecessor.map_or(0, |flow| delivered[&flow]);
                    assert_eq!(
                        row.key.time_ns,
                        local.max(inbound),
                        "{label}: a stage activates exactly when its later predecessor completes"
                    );
                    assert!(row.after_packets_emitted > 0);
                }
            }
            let non_roots = image
                .host_states
                .iter()
                .flat_map(HostState::generators_with_stages)
                .filter(|(_, stage)| !stage.unwrap().activated)
                .count();
            assert_eq!(
                activations, non_roots,
                "{label}: every blocked stage activates once"
            );
        }
    }
}

#[test]
fn stage_never_activates_before_both_predecessors_complete() {
    let image = compile_text(
        "tcp-gate",
        &tcp_collective_config("RingAllReduce", 4, 20_003, 100),
    );
    let totals = tcp_total_bytes(&image);
    let result = run_everywhere(&image, "gate");
    let acknowledged = acknowledged_at(&result, &totals);
    let delivered = delivered_at(&result, &totals);
    let mut rows_by_flow = BTreeMap::<FlowId, Vec<CollectiveProgressRecord>>::new();
    for row in progress(&result) {
        rows_by_flow.entry(row.flow).or_default().push(row);
    }
    for (flow, rows) in rows_by_flow {
        let activation = rows
            .iter()
            .find(|row| row.activated)
            .expect("every progressing stage activates");
        for row in &rows {
            if row.key < activation.key {
                assert!(
                    !(row.after_local_complete && row.after_inbound_complete),
                    "{flow:?} was complete before its activation row"
                );
            }
        }
        let first_send = result
            .departures
            .iter()
            .filter(|departure| {
                result
                    .observed_packets
                    .iter()
                    .any(|packet| packet.id == departure.payload && packet.flow == flow)
            })
            .map(|departure| departure.time_ns)
            .min()
            .unwrap();
        let local = activation
            .local_predecessor
            .map_or(0, |predecessor| acknowledged[&predecessor]);
        let inbound = activation
            .inbound_predecessor
            .map_or(0, |predecessor| delivered[&predecessor]);
        assert!(first_send >= local.max(inbound), "{flow:?} sent early");
    }
}

#[test]
fn dcqcn_collectives_stay_rejected_at_lowering() {
    let config = tcp_collective_config("RingAllReduce", 3, 10_001, 100)
        .replace("flow_type = \"TCP\"", "flow_type = \"DCQCN\"");
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p14-dcqcn-reject-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, config).unwrap();
    let error =
        compile_config(&path).expect_err("DCQCN collectives stay refused: DCQCN is unreliable");
    fs::remove_file(path).unwrap();
    assert_eq!(
        error.to_string(),
        "unsupported collective flow type `DCQCN`; collectives require a reliable transport, flow_type = \"TCP\" or \"RoCE\" (a RoCE queue pair is DCQCN with Go-back-N)"
    );
}

/// P16 G1: the device backends accept wrapped TCP stages (identity in
/// `tests/p16_device_collectives.rs`).
#[test]
fn wrapped_stages_validate_on_devices() {
    let image = compile_text(
        "tcp-device",
        &tcp_collective_config("RingAllReduce", 2, 1_000, 100),
    );
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend).expect("devices accept collective stages");
    }
}

#[test]
fn validator_rejects_an_unreleased_tcp_stage_with_sending_state() {
    let image = compile_text(
        "tcp-unreleased",
        &tcp_collective_config("RingAllReduce", 3, 10_001, 100),
    );
    validate(&image, Backend::Scalar).unwrap();
    let (slot, index) = image
        .host_states
        .iter()
        .enumerate()
        .find_map(|(slot, state)| {
            state
                .generators_with_stages()
                .position(|(_, stage)| !stage.unwrap().activated)
                .map(|index| (slot, index))
        })
        .unwrap();
    let flow = image.host_states[slot].generators[index].flow;

    let mut sent = image.clone();
    sent.host_states[slot].generators[index].packets_emitted = 1;
    assert_eq!(
        validate(&sent, Backend::Scalar).unwrap_err().to_string(),
        format!(
            "flow {flow:?} TCP collective stage is dependency-blocked after sending state changed"
        )
    );

    // A release flag set before both prerequisites complete is itself inconsistent.
    let mut released = image.clone();
    released.host_states[slot].stages[index]
        .as_mut()
        .unwrap()
        .activated = true;
    assert_eq!(
        validate(&released, Backend::Scalar)
            .unwrap_err()
            .to_string(),
        format!("flow {flow:?} stage release flag disagrees with its prerequisites")
    );
}

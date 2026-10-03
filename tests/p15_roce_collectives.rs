//! P15 lane R3: collective stages over RoCE queue pairs run on Scalar and CPU
//! (`days-gpu/evidence/P15/collectives-design.md` §3 to §6 and §9; rulings C1 to C6 and
//! Amendment 4 of `plans/briefs/p15/qp-schema.md`).
//!
//! Each fixture is pinned by its mechanism contract and by Scalar = CPU identity at 1 to 4
//! workers. The stage rules are checked on the records: a release starts the pair's pacing grid at
//! the release instant, an inbound stage counts the receiver's Go-back-N frontier, and a local
//! stage completes on the ACK that brings the pair's cumulative acknowledgment to its total.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    Backend, CollectiveActivationCause, CollectivePhase, CollectiveProgressRecord, CpuConfig,
    DcqcnTransitionKind, FlowGeneratorKind, FlowId, GeneratorStatus, MechanismTransitionRecord,
    ObservationMode, PacketKind, PfcControlAction, RoceSenderKind, RoceSenderRecord,
    RoceTransitionRecord, RunResult, SimulationImage, StageRole, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};

fn fixture_path(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}"))
}

fn lower(name: &str) -> SimulationImage {
    compile_config(fixture_path(name))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
}

fn lower_text(label: &str, text: &str) -> SimulationImage {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p15-r3-run-{label}-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, text).expect("write the scenario");
    let image = compile_config(&path);
    let _ = std::fs::remove_file(&path);
    image.unwrap_or_else(|error| panic!("{label} must lower: {error}"))
}

fn fixture_text(name: &str) -> String {
    std::fs::read_to_string(fixture_path(name)).expect("read the fixture")
}

fn scalar(image: &SimulationImage, label: &str) -> RunResult {
    run_scalar_with_observations(image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{label}: Scalar run failed: {error}"))
}

/// The Scalar full-observation result, after checking that every CPU worker count matches it.
fn run_identical(image: &SimulationImage, label: &str) -> RunResult {
    let scalar = scalar(image, label);
    for workers in 1..=4 {
        let cpu = run_cpu_with_observations(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("{label}: CPU run with {workers} workers failed: {error}"));
        assert!(
            cpu.result == scalar,
            "{label}: CPU with {workers} workers differs from Scalar"
        );
    }
    scalar
}

fn records(result: &RunResult) -> &[MechanismTransitionRecord] {
    &result
        .diagnostics
        .as_ref()
        .expect("full observation carries diagnostics")
        .mechanism_transitions
}

fn progress(result: &RunResult) -> Vec<CollectiveProgressRecord> {
    records(result)
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Collective(record) => Some(*record),
            _ => None,
        })
        .collect()
}

/// Each queue pair's sender rows in event order.
fn sender_rows(result: &RunResult) -> BTreeMap<FlowId, Vec<RoceSenderRecord>> {
    let mut rows = BTreeMap::<FlowId, Vec<RoceSenderRecord>>::new();
    for record in records(result) {
        if let MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender)) = record {
            rows.entry(sender.flow).or_default().push(*sender);
        }
    }
    for flow_rows in rows.values_mut() {
        flow_rows.sort_by_key(|row| row.key);
    }
    rows
}

/// The RoCE collective stages of an image: flow -> (chunk bytes, stage position).
fn roce_stages(image: &SimulationImage) -> BTreeMap<FlowId, (u64, (CollectivePhase, u32, u32))> {
    image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter_map(|(generator, stage)| match (generator.kind, stage?.role) {
            (FlowGeneratorKind::Roce(roce), StageRole::Collective(identity)) => Some((
                generator.flow,
                (
                    roce.pacer.total_bytes,
                    (identity.phase, identity.rank, identity.step),
                ),
            )),
            _ => None,
        })
        .collect()
}

fn is_roce_row(row: &CollectiveProgressRecord) -> bool {
    format!("{:?}", row.stage_kind) == "Roce"
}

#[derive(Debug, Default)]
struct Contract {
    stages: usize,
    finished_stages: usize,
    retransmissions: usize,
    nacks: usize,
    timeouts: usize,
    armed_timeouts: usize,
    pfc_pauses: usize,
    host_pauses: usize,
    dropped: u128,
}

fn contract(image: &SimulationImage, result: &RunResult) -> Contract {
    let stages = roce_stages(image);
    let mut contract = Contract {
        stages: stages.len(),
        dropped: result.summary.dropped_packets,
        ..Contract::default()
    };
    for generator in result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| stages.contains_key(&generator.flow))
    {
        if generator.next_emission.status == GeneratorStatus::Finished {
            contract.finished_stages += 1;
        }
    }
    for packet in &result.observed_packets {
        match packet.kind {
            PacketKind::RoceData(header) if header.retransmission => contract.retransmissions += 1,
            PacketKind::RoceNack(_) => contract.nacks += 1,
            _ => {}
        }
    }
    for record in records(result) {
        match record {
            MechanismTransitionRecord::PfcControl(control)
                if control.action == PfcControlAction::Pause =>
            {
                contract.pfc_pauses += 1;
                if result
                    .host_states
                    .iter()
                    .any(|state| state.egress_link == control.controlled_link)
                {
                    contract.host_pauses += 1;
                }
            }
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender)) => {
                if sender.kind == RoceSenderKind::Timeout {
                    contract.timeouts += 1;
                }
                if sender.after.rto_deadline_ns.is_some() {
                    contract.armed_timeouts += 1;
                }
            }
            _ => {}
        }
    }
    contract
}

#[test]
fn ring_allreduce_completes_without_loss_under_pfc() {
    let image = lower("roce_ring_allreduce_lossless.toml");
    let result = run_identical(&image, "ring lossless");
    let contract = contract(&image, &result);
    assert_eq!(contract.stages, 24, "{contract:?}");
    assert_eq!(contract.finished_stages, 24, "{contract:?}");
    assert_eq!(
        (
            contract.dropped,
            contract.nacks,
            contract.retransmissions,
            contract.timeouts
        ),
        (0, 0, 0, 0),
        "{contract:?}"
    );
    assert!(contract.pfc_pauses > 0, "PFC must act: {contract:?}");
}

/// Ruling C3: the stages of a lossless collective run with the timeout off, so no timer is ever
/// armed and nothing is pending at the stop.
#[test]
fn allgather_runs_lossless_with_the_timeout_off() {
    let image = lower("roce_allgather_lossless.toml");
    let result = run_identical(&image, "allgather lossless");
    let contract = contract(&image, &result);
    assert_eq!(contract.stages, 12, "{contract:?}");
    assert_eq!(contract.finished_stages, 12, "{contract:?}");
    assert_eq!(
        (
            contract.dropped,
            contract.nacks,
            contract.retransmissions,
            contract.timeouts,
            contract.armed_timeouts
        ),
        (0, 0, 0, 0, 0),
        "{contract:?}"
    );
    assert!(contract.pfc_pauses > 0, "PFC must act: {contract:?}");
    assert!(result.pending_events.is_empty());
}

#[test]
fn lossy_ring_recovers_every_loss_inside_its_stages() {
    let image = lower("roce_ring_lossy.toml");
    let result = run_identical(&image, "ring lossy");
    let contract = contract(&image, &result);
    assert_eq!(contract.finished_stages, 24, "{contract:?}");
    assert!(contract.dropped > 0, "{contract:?}");
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
    // Some inbound rows answer an out-of-order or duplicate packet and advance nothing.
    assert!(
        progress(&result).iter().any(|row| is_roce_row(row)
            && row.cause == CollectiveActivationCause::InboundArrival
            && row.arrival_bytes == 0),
        "the lossy ring must certify packets that do not advance the frontier"
    );
}

#[test]
fn compute_dag_releases_each_root_at_the_compute_deadline() {
    let image = lower("roce_compute_dag.toml");
    let result = run_identical(&image, "compute dag");
    let contract = contract(&image, &result);
    assert_eq!(contract.finished_stages, 24, "{contract:?}");
    assert_eq!(contract.dropped, 0, "{contract:?}");
    let rows = progress(&result);
    let roots = rows
        .iter()
        .filter(|row| is_roce_row(row) && row.activated && row.local_predecessor.is_some())
        .filter(|row| row.inbound_predecessor.is_none())
        .collect::<Vec<_>>();
    assert_eq!(roots.len(), 4, "every rank's root is gated by `forward`");
    for root in roots {
        assert_eq!(root.key.time_ns, 5_000, "{root:?}");
        assert_eq!(root.after_next_time_ns, 5_000, "{root:?}");
    }
    // `backward` finishes exactly 7,000 ns after its release.
    let backward_releases = rows
        .iter()
        .filter(|row| row.activated && row.duration_ns == 7_000)
        .map(|row| (row.flow, row.key.time_ns))
        .collect::<Vec<_>>();
    assert_eq!(backward_releases.len(), 4);
    for (flow, released) in backward_releases {
        let generator = result
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .find(|generator| generator.flow == flow)
            .expect("the compute stage");
        assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
        assert_eq!(generator.next_emission.departure_time_ns, released + 7_000);
    }
}

/// Every RoCE collective of the design, including the mixed image, and the rules that hold on
/// each run (Amendment 4; design note §3 to §5).
#[test]
fn stage_rules_hold_on_every_fixture() {
    for name in [
        "roce_ring_allreduce_lossless.toml",
        "roce_allgather_lossless.toml",
        "roce_ring_lossy.toml",
        "roce_compute_dag.toml",
        "roce_tcp_mixed_collectives.toml",
        "roce_ring_release_paused.toml",
    ] {
        let image = lower(name);
        let result = scalar(&image, name);
        check_stage_rules(name, &image, &result);
    }
}

fn check_stage_rules(name: &str, image: &SimulationImage, result: &RunResult) {
    let stages = roce_stages(image);
    let senders = sender_rows(result);
    let control_ticks = records(result)
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Dcqcn(row) if row.kind == DcqcnTransitionKind::Control => {
                Some((row.flow, row.key))
            }
            _ => None,
        })
        .fold(BTreeMap::<FlowId, Vec<_>>::new(), |mut map, (flow, key)| {
            map.entry(flow).or_default().push(key);
            map
        });
    let rows = progress(result);
    assert!(!rows.is_empty(), "{name}: progress rows");
    let mut activations = BTreeMap::new();
    let mut frontiers = BTreeMap::<(FlowId, FlowId), u64>::new();
    for row in &rows {
        if !stages.contains_key(&row.flow) {
            continue;
        }
        assert!(
            is_roce_row(row),
            "{name}: a RoCE stage writes roce rows: {row:?}"
        );
        assert_eq!(
            (row.packet_size_bytes, row.interval_ns),
            (1000, 1000),
            "{name}"
        );
        if row.activated {
            // The first tick is at the release instant, on a grid anchored there (C2).
            assert_eq!(row.after_next_time_ns, row.key.time_ns, "{name}: {row:?}");
            assert_eq!((row.after_packets_emitted, row.after_bytes_emitted), (0, 0));
            assert!(matches!(
                row.after_status,
                GeneratorStatus::Scheduled | GeneratorStatus::Blocked
            ));
            activations.insert(row.flow, row.key.time_ns);
        }
        match row.cause {
            CollectiveActivationCause::InboundArrival => {
                // The Go-back-N frontier: an advance only for the PSN at the frontier.
                let frontier = frontiers.entry((row.flow, row.cause_flow)).or_default();
                assert_eq!(row.before_inbound_bytes, *frontier, "{name}: {row:?}");
                let advance = if row.segment_sequence == *frontier {
                    row.segment_bytes
                } else {
                    0
                };
                assert_eq!(row.arrival_bytes, advance, "{name}: {row:?}");
                *frontier += advance;
                assert_eq!(row.after_inbound_bytes, *frontier, "{name}: {row:?}");
                assert_eq!(
                    (row.ack_number, row.cause_origin_ns, row.cause_delay_ns),
                    (0, 0, 0)
                );
            }
            CollectiveActivationCause::LocalCompletion if stages.contains_key(&row.cause_flow) => {
                // The ACK that brings the predecessor's acknowledgment to its total.
                let (total, _) = stages[&row.cause_flow];
                assert_eq!(row.ack_number, total, "{name}: {row:?}");
                let completing = senders[&row.cause_flow]
                    .iter()
                    .find(|sender| sender.after.snd_una == total)
                    .expect("the predecessor completes");
                assert_eq!(completing.kind, RoceSenderKind::Ack, "{name}");
                assert_eq!(completing.key, row.key, "{name}: completion event");
                assert!(row.cause_delay_ns > 0);
                assert!(row.key.time_ns >= row.cause_origin_ns + row.cause_delay_ns);
            }
            CollectiveActivationCause::LocalCompletion => {}
        }
    }
    for (flow, (_, _)) in &stages {
        let first = senders
            .get(flow)
            .and_then(|rows| rows.first())
            .unwrap_or_else(|| panic!("{name}: stage {flow:?} has sender rows"));
        assert_eq!(first.kind, RoceSenderKind::Tick, "{name}");
        assert_eq!(first.before.next_tick_ns, Some(first.key.time_ns));
        assert_eq!(
            first.before.rto_deadline_ns, None,
            "{name}: no timer before a send"
        );
        assert_eq!(first.first_pacing_time_ns, first.key.time_ns, "{name}");
        // A released stage's first tick is its release; a root's is the initial delay.
        if let Some(&released) = activations.get(flow) {
            assert_eq!(first.key.time_ns, released, "{name}");
        }
        let control = control_ticks.get(flow).and_then(|ticks| ticks.first());
        let expected = first.key.time_ns + 50_000;
        if expected <= image.stop_time_ns {
            let control = control.unwrap_or_else(|| panic!("{name}: a first control tick"));
            assert_eq!(control.time_ns, expected, "{name}: first control tick");
            // A release emits the pacing tick, then the control tick, as lowering orders a
            // plain pair's (design note S-R6).
            if activations.contains_key(flow) {
                assert_eq!(control.origin_seq, first.key.origin_seq + 1, "{name}");
            }
        }
    }
}

/// Ruling C3: with the timeout off, a stage whose tail is lost stalls with no error, and so do its
/// successors. The documented signs (`collectives.mdx`, RoCE stages): the stage's generator is not
/// `Finished` and nothing is pending for it, its successors' records read `activated: false`, and
/// no local-completion row names it.
#[test]
fn with_the_timeout_off_a_lost_tail_stalls_its_stage_and_successors_visibly() {
    let text = fixture_text("roce_ring_lossy.toml").replace(
        "retransmit_timeout_ns = 1000000",
        "retransmit_timeout_ns = 0",
    );
    let image = lower_text("lossy-no-timeout", &text);
    let result = run_identical(&image, "lossy, timeout off");
    let contract = contract(&image, &result);
    assert_eq!((contract.timeouts, contract.armed_timeouts), (0, 0));
    assert!(result.pending_events.is_empty(), "no timer is ever armed");
    let completed_causes = progress(&result)
        .into_iter()
        .filter(|row| row.cause == CollectiveActivationCause::LocalCompletion)
        .map(|row| row.cause_flow)
        .collect::<std::collections::BTreeSet<_>>();
    let mut stalled = 0;
    for state in &result.host_states {
        for (generator, stage) in state.generators_with_stages() {
            let (FlowGeneratorKind::Roce(roce), Some(stage)) = (generator.kind, stage) else {
                continue;
            };
            if !stage.activated || generator.next_emission.status == GeneratorStatus::Finished {
                continue;
            }
            stalled += 1;
            assert!(roce.snd_una < roce.pacer.total_bytes);
            assert!(!roce.pacer_armed, "a stalled pair is parked");
            assert!(!completed_causes.contains(&generator.flow));
            for (successor, successor_stage) in state.generators_with_stages() {
                let Some(successor_stage) = successor_stage else {
                    continue;
                };
                if successor_stage.dependencies.local_predecessor == Some(generator.flow) {
                    assert!(!successor_stage.activated, "{:?} waits", successor.flow);
                }
            }
        }
    }
    assert!(stalled > 0, "a lost tail must stall a stage: {contract:?}");
    assert!(contract.finished_stages < 24, "{contract:?}");
}

/// Design note §5.3 and ruling C6: a stage released while its data class is paused at its host
/// arms its first tick at the release like any other; that tick parks the pair (no credit, no
/// packet, `class_paused`), the pair joins the host's parked list, and a RESUME restarts it on
/// the grid anchored at the release.
#[test]
fn a_stage_released_on_a_paused_class_parks_at_its_first_tick_and_resumes() {
    let image = lower("roce_ring_release_paused.toml");
    let result = run_identical(&image, "release paused");
    let contract = contract(&image, &result);
    assert_eq!(contract.finished_stages, 24, "{contract:?}");
    assert_eq!(contract.dropped, 0, "{contract:?}");
    check_stage_rules("roce_ring_release_paused.toml", &image, &result);
    let stages = roce_stages(&image);
    let releases = progress(&result)
        .into_iter()
        .filter(|row| row.activated)
        .map(|row| (row.flow, row.key.time_ns))
        .collect::<BTreeMap<_, _>>();
    let senders = sender_rows(&result);
    let mut parked_at_release = 0;
    for (flow, rows) in senders.iter().filter(|(flow, _)| stages.contains_key(flow)) {
        let first = rows[0];
        if !first.class_paused {
            continue;
        }
        parked_at_release += 1;
        assert_eq!(
            Some(&first.key.time_ns),
            releases.get(flow),
            "a released stage"
        );
        assert_eq!(first.rate_bps, None, "a paused tick credits nothing");
        assert!(first.emitted.is_none());
        assert_eq!(first.after.credit_quanta, 0);
        let resume = rows
            .iter()
            .find(|row| row.kind == RoceSenderKind::Resume)
            .expect("a RESUME restarts the pair");
        let interval = resume.pacing_interval_ns;
        let restart = resume.after.next_tick_ns.expect("armed by the RESUME");
        assert_eq!(
            (restart - first.key.time_ns) % interval,
            0,
            "on the release's grid"
        );
        assert!(restart > resume.key.time_ns);

        // Between the paused tick and the RESUME, the pair is on its host's parked list.
        let state = checkpoint(&image, first.key.time_ns + 1);
        validate(&state, Backend::Scalar).expect("the mid-pause checkpoint validates");
        let (slot, position) = state
            .host_states
            .iter()
            .enumerate()
            .find_map(|(slot, host)| {
                host.generators
                    .iter()
                    .position(|generator| generator.flow == *flow)
                    .map(|position| (slot, position))
            })
            .expect("the stage's host");
        let pfc = state.host_states[slot]
            .pfc
            .as_deref()
            .expect("host pause state");
        assert!(
            pfc.pause_parked[3].contains(&position),
            "listed while paused"
        );
    }
    assert!(
        parked_at_release > 0,
        "a stage must release on a paused class"
    );
}

/// The TCP collective of the mixed image shares no queue with the RoCE one, so its progress is the
/// TCP collective's progress alone, up to flow identity.
#[test]
fn a_tcp_collective_beside_a_roce_collective_progresses_as_alone() {
    let mixed_image = lower("roce_tcp_mixed_collectives.toml");
    let mixed = run_identical(&mixed_image, "mixed");
    let text = fixture_text("roce_tcp_mixed_collectives.toml");
    let alone_image = lower_text(
        "tcp-alone",
        &text[..text.rfind("[[collective]]").expect("two collectives")],
    );
    let alone = scalar(&alone_image, "tcp alone");
    let tcp_rows = |image: &SimulationImage, result: &RunResult| {
        let positions = image
            .host_states
            .iter()
            .flat_map(|state| state.generators_with_stages())
            .filter_map(|(generator, stage)| match (generator.kind, stage?.role) {
                (FlowGeneratorKind::Tcp(_), StageRole::Collective(identity)) => Some((
                    generator.flow,
                    (identity.phase, identity.rank, identity.step),
                )),
                _ => None,
            })
            .collect::<BTreeMap<_, _>>();
        progress(result)
            .into_iter()
            .filter(|row| positions.contains_key(&row.flow))
            .map(|row| {
                (
                    (row.key.time_ns, row.key.phase),
                    positions[&row.flow],
                    (row.cause, positions.get(&row.cause_flow).copied()),
                    row.arrival_bytes,
                    row.activated,
                    (row.before_inbound_bytes, row.after_inbound_bytes),
                    (row.after_packets_emitted, row.after_bytes_emitted),
                    row.after_status,
                    row.after_next_time_ns,
                    (row.segment_sequence, row.segment_bytes),
                    (row.ack_number, row.cause_origin_ns, row.cause_delay_ns),
                )
            })
            .collect::<Vec<_>>()
    };
    let expected = tcp_rows(&alone_image, &alone);
    assert!(!expected.is_empty());
    assert_eq!(tcp_rows(&mixed_image, &mixed), expected);
    let contract = contract(&mixed_image, &mixed);
    assert_eq!(contract.finished_stages, 12, "{contract:?}");
}

/// Design note §5.2: a stage released at `T` sends exactly as a plain queue pair whose initial
/// delay is `T`. Compared on the semantic columns; origin sequences and payloads differ by
/// construction (runtime against lowered events, another allocation order).
#[test]
fn a_released_stage_sends_as_a_queue_pair_starting_at_its_release() {
    let fabric = r#"seed = 42
edges = [[0, 1]]
hosts = [0, 1]
duration = 0.01

[switch]
port_rate = 1000000000
capacity = 300
discipline = "FIFO"
drop = "ECN_THRESHOLD"
ecn_threshold = 1.0
"#;
    let dcqcn = |table: &str| {
        format!(
            r#"
[{table}.traffic.dcqcn]
rate_gbps = 1.0
min_rate_gbps = 0.01
max_rate_gbps = 1.0
g = 0.00390625
ai_rate_gbps = 0.005
hai_rate_gbps = 0.05
mi_factor = 0.5
rtt_ns = 50000
cnp_interval_ns = 10000
pacing_interval_ns = 1000
increase_byte_threshold = 100000

[{table}.traffic.roce]
retransmit_timeout_ns = 1000000
"#
        )
    };
    let staged = format!(
        r#"{fabric}
[[compute]]
name = "wait"
hosts = [0, 1]
duration_ns = 7000

[[collective]]
name = "gather"
after = "wait"
collective_type = "AllGather"
flow_type = "RoCE"
flow_count = 2
sources = [0, 1]
sinks = [1, 0]

[collective.traffic]
initial_delay = 0.0
size = 40000
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}
{}"#,
        dcqcn("collective")
    );
    let plain_flow = |source: u64, target: u64| {
        format!(
            r#"
[[flow]]
flow_type = "RoCE"
graph = [[{source}, {target}]]

[flow.traffic]
initial_delay = 0.000007
size = 20000
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}
{}"#,
            dcqcn("flow")
        )
    };
    let plain = format!("{fabric}{}{}", plain_flow(0, 1), plain_flow(1, 0));
    let semantic = |image: &SimulationImage, result: &RunResult| {
        let source_of = |flow: FlowId| image.flows[flow.0 as usize].source;
        sender_rows(result)
            .into_values()
            .map(|rows| {
                let source = source_of(rows[0].flow);
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        (
                            row.key.time_ns,
                            row.key.phase,
                            row.kind,
                            row.class_paused,
                            row.first_pacing_time_ns,
                            row.rate_bps,
                            row.input_acknowledgment,
                            row.emitted.map(|emitted| {
                                (emitted.psn, emitted.bytes, emitted.retransmission)
                            }),
                            row.before,
                            row.after,
                        )
                    })
                    .collect::<Vec<_>>();
                (source, rows)
            })
            .collect::<BTreeMap<_, _>>()
    };
    let staged_image = lower_text("released", &staged);
    let plain_image = lower_text("plain", &plain);
    let staged_rows = semantic(&staged_image, &scalar(&staged_image, "released"));
    let plain_rows = semantic(&plain_image, &scalar(&plain_image, "plain"));
    assert_eq!(staged_rows.len(), 2);
    assert!(staged_rows.values().all(|rows| rows[0].0 == 7_000));
    assert_eq!(staged_rows, plain_rows);
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
    }
}

/// Complete state at the stop: host and switch states, resident packets and pending events.
fn complete_state(result: &RunResult) -> String {
    format!(
        "{:#?}{:#?}{:#?}{:#?}",
        result.host_states, result.switch_states, result.resident_packets, result.pending_events
    )
}

/// Design note §6.3: checkpoints holding gated, active and finished RoCE stages revalidate, and a
/// continuation from each reaches the uninterrupted run's complete state on Scalar and CPU.
#[test]
fn checkpoints_of_gated_active_and_finished_stages_revalidate_and_resume() {
    let image = lower("roce_compute_dag.toml");
    let full = run_scalar_with_observations(&image, None, ObservationMode::Summary)
        .expect("the uninterrupted run");
    let expected = complete_state(&full);
    let (mut gated, mut active, mut finished) = (0, 0, 0);
    for horizon_ns in (0..=8).map(|step| step * 250_000 + 4_000) {
        let state = checkpoint(&image, horizon_ns);
        validate(&state, Backend::Scalar)
            .unwrap_or_else(|error| panic!("checkpoint at {horizon_ns} ns: {error}"));
        for (generator, stage) in state
            .host_states
            .iter()
            .flat_map(|host| host.generators_with_stages())
            .filter(|(generator, _)| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
        {
            match (
                stage.expect("every pair is a stage").activated,
                generator.next_emission.status,
            ) {
                (false, _) => gated += 1,
                (true, GeneratorStatus::Finished) => finished += 1,
                (true, _) => active += 1,
            }
        }
        let resumed = run_scalar_with_observations(&state, None, ObservationMode::Summary)
            .unwrap_or_else(|error| panic!("resume at {horizon_ns} ns: {error}"));
        assert!(
            complete_state(&resumed) == expected,
            "Scalar continuation from {horizon_ns} ns differs"
        );
        let cpu = run_cpu_with_observations(
            &state,
            None,
            CpuConfig {
                workers: 2,
                ..CpuConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("CPU resume at {horizon_ns} ns: {error}"));
        assert!(
            complete_state(&cpu.result) == expected,
            "CPU continuation from {horizon_ns} ns differs"
        );
    }
    assert!(
        gated > 0 && active > 0 && finished > 0,
        "{gated} {active} {finished}"
    );
}

/// FNV-1a64 over the pretty `Debug` rendering, the `result_fnv1a64` the `days` CLI prints.
fn fingerprint(value: &impl std::fmt::Debug) -> (u64, u64) {
    let text = format!("{value:#?}");
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (text.len() as u64, hash)
}

/// Frozen at authoring (`5752c51`, 2026-10-01, Mac): the Scalar summary-mode results, which the
/// `days` CLI reproduced on Scalar and CPU at 2 workers
/// (`days-gpu/evidence/P15/collectives-impl/raw/anchors-mac-5752c51.txt`).
const ANCHORS: [(&str, u64, u64); 6] = [
    (
        "roce_ring_allreduce_lossless.toml",
        199_110,
        0x8b3d_8615_76b7_d646,
    ),
    (
        "roce_allgather_lossless.toml",
        126_202,
        0x84c0_e6d2_a574_9c80,
    ),
    ("roce_ring_lossy.toml", 190_244, 0xa065_c89e_82b4_e3b9),
    ("roce_compute_dag.toml", 218_236, 0xb3e9_c56f_a727_6c1c),
    (
        "roce_tcp_mixed_collectives.toml",
        141_856,
        0xee09_f67f_aca4_91d5,
    ),
    (
        "roce_ring_release_paused.toml",
        203_330,
        0x273c_f356_1189_2368,
    ),
];

#[test]
fn roce_collective_fixtures_match_their_frozen_anchors() {
    for (name, bytes, fnv1a64) in ANCHORS {
        let image = lower(name);
        let result = run_scalar_with_observations(&image, None, ObservationMode::Summary)
            .unwrap_or_else(|error| panic!("{name}: Scalar run failed: {error}"));
        let actual = fingerprint(&result);
        assert_eq!(
            actual,
            (bytes, fnv1a64),
            "{name}: frozen anchor moved (got bytes={} fnv1a64={:016x})",
            actual.0,
            actual.1
        );
    }
}

/// The lowered RoCE queue pair of `flow`: (MTU, pacing interval), or `None` for any other flow.
fn roce_transport(image: &SimulationImage, flow: FlowId) -> Option<(u64, u64)> {
    image
        .host_states
        .iter()
        .flat_map(|state| state.generators.iter())
        .find(|generator| generator.flow == flow)
        .and_then(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => {
                Some((roce.pacer.mtu_bytes, roce.pacer.pacing_interval_ns))
            }
            _ => None,
        })
}

/// Schema Amendment 5 (LeanGuard part 3, review M1): a compute stage whose inbound predecessor is
/// a RoCE stage writes that queue pair's MTU and pacing interval on every progress row, so the
/// certificate can be replayed with the Go-back-N frontier even when the predecessor is an
/// unlogged root (`roce_allgather_compute_lossy`, an ungated two-rank AllGather); every other
/// compute row writes zero. Every compute inbound row of a RoCE predecessor is a packet of that
/// pair at its PSN and advances the frontier exactly when the PSN is the frontier.
#[test]
fn compute_rows_name_their_roce_inbound_transport() {
    for name in ["roce_compute_dag.toml", "roce_allgather_compute_lossy.toml"] {
        let image = lower(name);
        let result = run_identical(&image, name);
        let rows: Vec<_> = progress(&result)
            .into_iter()
            .filter(|row| row.stage_kind == days_executor::CollectiveStageKind::Compute)
            .collect();
        let (mut transported, mut out_of_order) = (0, 0);
        for row in &rows {
            let expected = row
                .inbound_predecessor
                .and_then(|flow| roce_transport(&image, flow))
                .unwrap_or((0, 0));
            assert_eq!(
                (row.packet_size_bytes, row.interval_ns),
                expected,
                "{name}: compute row of flow {:?} at {:?}",
                row.flow,
                row.key
            );
            if expected.0 == 0 {
                continue;
            }
            transported += 1;
            if row.cause == CollectiveActivationCause::InboundArrival {
                let (mtu, total) = (expected.0, row.inbound_predecessor_bytes);
                let psn = row.segment_sequence;
                assert!(
                    psn % mtu == 0 && psn < total && row.segment_bytes == mtu.min(total - psn),
                    "{name}: compute inbound row is not its pair's packet at PSN {psn}"
                );
                let advance = if psn == row.before_inbound_bytes {
                    row.segment_bytes
                } else {
                    out_of_order += 1;
                    0
                };
                assert_eq!(row.arrival_bytes, advance, "{name}: Go-back-N advance");
            }
        }
        assert!(
            transported > 0,
            "{name}: no compute row follows a RoCE stage"
        );
        if name == "roce_allgather_compute_lossy.toml" {
            assert!(
                out_of_order > 0,
                "{name}: the lossy contract needs compute inbound rows off the frontier"
            );
        }
    }
}

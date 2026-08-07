//! T20j verdict-equality gate for the flow-indexed load-time validator.
//!
//! `days_executor::validate` used to answer "which initial packets, or preloaded ACK arrivals,
//! belong to this flow?" — and "how many initial timeout events carry this timer's executable
//! identity?" — with a full linear scan of `initial_packets`/`initial_events`, once per TCP
//! generator, at ten call sites. Those scans are now index lookups. This gate proves
//! the replacement changes scan mechanics only: on every executor-lowerable fixture family in
//! `configs/`, and on running checkpoints of them, the indexed walk visits exactly the elements
//! the filter visited, in exactly the same order, and every fold over them produces the same
//! value. The matching rejection corpus lives in `executor/tests/validate.rs` and
//! `executor/tests/tcp_semantics.rs`, which assert the exact diagnostic text of the first
//! failure, including which entity a multi-violation image names first.
//!
//! Like the T20e planner gate, this is an equality gate rather than a specification oracle:
//! primitives shared by both paths can drift without failing it.
#![cfg(feature = "test")]

use std::collections::BTreeSet;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, Event, EventKey, EventKind, FlowGeneratorKind, FlowId, GeneratorStatus,
    ObservationMode, PacketDescriptor, PacketKind, PayloadId, SchedulerKind, SimulationImage,
    TcpAckHeader, assert_validate_flow_index_equivalent_for_testing, event_phase,
    run_scalar_with_observations, validate,
};

/// Every fixture family under `configs/` that the executor scenario lowering accepts, biased to
/// the smaller members so the gate stays inside a unit-test budget. The families it omits are the
/// ones `compile_config` rejects outright (legacy source routing, explicit flow identifiers,
/// non-constant size distributions, `time_quantum_ns`, VirtualClock, timer-driven PFC), plus the
/// multi-hundred-thousand-flow frontier and real-image-gate members of families already listed.
const FIXTURES: &[&str] = &[
    "configs/fattree.toml",
    // Constant open-loop sources, single- and multi-threaded lowering.
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
    "configs/benchmarks/baseline/fattree_k4_f8_mt.toml",
    "configs/benchmarks/baseline/fattree_k8_f64_st.toml",
    "configs/benchmarks/baseline/fattree_k16_f512_st.toml",
    // Closed-loop TCP, open-loop rate, and the mixed RQ9 cohort.
    "configs/benchmarks/p11/rq9_closed_k16.toml",
    "configs/benchmarks/p11/rq9_open_k16.toml",
    "configs/benchmarks/p11/rq9_open_k16_matched.toml",
    "configs/benchmarks/p11/rq9_mixed_k16.toml",
    // Exact-rate pacing at a one-microsecond lookahead.
    "configs/benchmarks/small_lookahead/open_loop_100g_1us_st.toml",
    "configs/benchmarks/small_lookahead/open_loop_100g_1us_mt.toml",
    // Reno and CUBIC congestion control.
    "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
    "configs/benchmarks/tcp/fattree_k16_tcp_reno_f1024.toml",
    "configs/benchmarks/tcp/fattree_k16_tcp_cubic_f1024.toml",
    // Width-via-load cohorts and the sustained load sweep.
    "configs/benchmarks/width_via_load/fattree_k32_target_w01000.toml",
    "configs/benchmarks/width_via_load/fattree_k32_target_w03000.toml",
    "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml",
    "configs/benchmarks/width_via_load_full/fattree_k32_load_30_sustained.toml",
    // A wide, deep topology whose LP count dwarfs its flow count.
    "configs/benchmarks/real_image_gate/fattree_k48_h16_f1024_st.toml",
];

/// Fixtures whose checkpoints carry resident packets, preloaded arrivals and installed TCP
/// timers — the state that makes the flow-keyed groups and the ACK counts non-trivial.
const CHECKPOINT_FIXTURES: &[&str] = &[
    "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
    "configs/benchmarks/p11/rq9_closed_k16.toml",
    "configs/benchmarks/p11/rq9_mixed_k16.toml",
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
];

fn compile_fixture(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("failed to lower {relative}: {error}"))
}

fn checkpoint_after(source: &SimulationImage, fixture: &str, events: u64) -> SimulationImage {
    let prefix = run_scalar_with_observations(source, Some(events), ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{fixture}: prefix run failed: {error}"));
    let mut checkpoint = source.clone();
    checkpoint.host_states = prefix.host_states;
    checkpoint.switch_states = prefix.switch_states;
    checkpoint.initial_packets = prefix.resident_packets;
    checkpoint.initial_events = prefix.pending_events;
    checkpoint
}

/// The times of the events the validator counts as preloaded TCP ACK arrivals.
fn preloaded_tcp_ack_arrival_times(image: &SimulationImage) -> Vec<u64> {
    let mut times = image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::RemoteArrival)
        .filter(|event| {
            image
                .initial_packets
                .iter()
                .find(|packet| packet.id == event.payload)
                .is_some_and(|packet| {
                    matches!(packet.kind, PacketKind::TcpAck(_))
                        && image
                            .flows
                            .get(packet.flow.0 as usize)
                            .is_some_and(|flow| flow.source == event.target)
                })
        })
        .map(|event| event.key.time_ns)
        .collect::<Vec<_>>();
    times.sort_unstable();
    times
}

#[test]
fn flow_indexed_validate_matches_the_pre_index_scans_on_every_fixture_family() {
    for fixture in FIXTURES {
        let image = compile_fixture(fixture);
        assert_validate_flow_index_equivalent_for_testing(&image)
            .unwrap_or_else(|mismatch| panic!("{fixture}: {mismatch}"));
    }
}

#[test]
fn every_fixture_family_still_validates_on_the_scalar_and_cpu_backends() {
    for fixture in FIXTURES {
        let image = compile_fixture(fixture);
        for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
            validate(&image, backend)
                .unwrap_or_else(|error| panic!("{fixture} on {backend:?}: {error}"));
        }
    }
}

/// The number of TCP generators whose active retransmission timer the validator will look up in
/// the initial event table — the query domain of `validate_blocked_tcp_timer`.
fn blocked_tcp_timer_queries(image: &SimulationImage) -> usize {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Blocked)
        .filter(|generator| match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => tcp.active_timer.is_some(),
            _ => false,
        })
        .count()
}

#[test]
fn flow_indexed_validate_matches_the_pre_index_scans_on_running_checkpoints() {
    let mut with_preloaded_acks = 0_usize;
    let mut blocked_timer_queries = 0_usize;
    for fixture in CHECKPOINT_FIXTURES {
        let source = compile_fixture(fixture);
        // 4,096 events is the deepest prefix the current validator survives on every fixture:
        // `rq9_mixed_k16` at 100,000 events reaches a retired TCP generator whose flow still owns
        // an acknowledged initial segment, and `tcp_future_data_max_frame` evaluates its
        // `then_some` argument eagerly there and subtracts past zero. That is a pre-existing
        // debug-build panic at 3df9388, unrelated to the flow index, and is reported rather than
        // fixed here because it is a formula-evaluation change, not a scan-mechanics change.
        for events in [1_u64, 8, 64, 512, 4096] {
            let checkpoint = checkpoint_after(&source, fixture, events);
            if !preloaded_tcp_ack_arrival_times(&checkpoint).is_empty() {
                with_preloaded_acks += 1;
            }
            blocked_timer_queries += blocked_tcp_timer_queries(&checkpoint);
            assert_validate_flow_index_equivalent_for_testing(&checkpoint)
                .unwrap_or_else(|mismatch| panic!("{fixture} after {events} events: {mismatch}"));
            validate(&checkpoint, Backend::Scalar)
                .unwrap_or_else(|error| panic!("{fixture} after {events} events: {error}"));
        }
    }
    // Residents and in-flight ACKs are exactly what a lowered fixture lacks; if no checkpoint
    // carried a preloaded arrival the corpus would only be testing freshly lowered images.
    assert!(
        with_preloaded_acks > 0,
        "the checkpoint corpus must reach the preloaded TCP ACK arrival sites at all"
    );
    // A freshly lowered image has every generator Scheduled, so the timeout-event count is only
    // ever queried on checkpoints. Without a Blocked TCP generator holding an active timer the
    // gate's timer comparison would run zero times.
    assert!(
        blocked_timer_queries > 0,
        "the checkpoint corpus must reach the Blocked TCP timer event-count site at all"
    );
}

/// Injects preloaded TCP ACK arrivals at `times_ns`, alternating between the first two flows.
///
/// A freshly lowered fixture has none and a scalar prefix run rarely leaves more than one in
/// flight, so without this the two preloaded-ACK counts the validator keeps — every arrival for
/// the flow, and the subset inside the run horizon — are never told apart. The result is
/// deliberately not counter-consistent: it feeds the index-versus-scan comparison, which reads
/// the image and never rejects, not `validate`.
fn with_preloaded_ack_arrivals(source: &SimulationImage, times_ns: &[u64]) -> SimulationImage {
    let mut image = source.clone();
    let node_count = image.nodes.len() as u64;
    for (offset, time_ns) in times_ns.iter().copied().enumerate() {
        let flow = image.flows[offset % 2].clone();
        let target_slot = image.nodes[flow.target.0 as usize].state_slot as usize;
        let sequence = image.host_states[target_slot].next_payload_seq;
        let origin_seq = image.host_states[target_slot].next_origin_seq;
        image.host_states[target_slot].next_payload_seq = sequence + 1;
        image.host_states[target_slot].next_origin_seq = origin_seq + 1;
        let payload = PayloadId(flow.target.0 + node_count * sequence);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: flow.id,
            size_bytes: 40,
            ecn_marked: false,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment: 0,
                acknowledged_bytes: 0,
                echoed_sent_time_ns: 0,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: flow.target,
                origin_seq,
            },
            target: flow.source,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.sort_by_key(|event| event.key);
    image
}

/// The timer-installation bound counts only the preloaded ACK arrivals inside the run horizon;
/// the attempt bound counts every one of them. A stop time that admits some arrivals of a flow
/// and not others is the only image shape that tells the two counts apart.
#[test]
fn truncated_stop_times_separate_the_two_preloaded_ack_counts() {
    let source = compile_fixture("configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml");
    let times = [10_u64, 20, 30, 40, 50, 60];
    let image = with_preloaded_ack_arrivals(&source, &times);
    assert_eq!(
        preloaded_tcp_ack_arrival_times(&image),
        times,
        "the injected arrivals must be exactly the ones the validator counts"
    );

    for horizon in [0_u64, 9, 10, 25, 35, 59, 60, u64::MAX] {
        let mut truncated = image.clone();
        truncated.stop_time_ns = horizon;
        assert_validate_flow_index_equivalent_for_testing(&truncated)
            .unwrap_or_else(|mismatch| panic!("horizon {horizon}: {mismatch}"));
    }
}

/// Rewrites every switch queue's scheduler as single-class deficit round robin with `quantum`.
///
/// Every fixture in `FIXTURES` declares FIFO, and every `configs/**` fixture that declares DRR is
/// rejected by `compile_config` for an unrelated reason (`configs/benchmarks/scheduling/*` selects
/// `routing`, `configs/ci/leanguard_drr.toml` uses explicit flow identifiers and an explicit
/// graph). So the DRR image the ninth site needs is built here, from a lowered TCP fixture.
fn with_single_class_drr_scheduling(source: &SimulationImage, quantum: u64) -> SimulationImage {
    let mut image = source.clone();
    for state in &mut image.switch_states {
        for queue in &mut state.queues {
            queue.scheduler = SchedulerKind::deficit_round_robin(vec![quantum]);
        }
    }
    image
}

/// The ninth indexed site, `maximum_drr_frame_bytes`, calls the same `tcp_future_data_max_frame`
/// helper as the eighth and is reached only from the `SchedulerKind::DeficitRoundRobin` arm of
/// `validate_scheduler_state`. No fixture in `FIXTURES` reaches it, so this test builds a DRR
/// image and pins the helper's value **through the validator's own verdict**: the DRR arm rejects
/// exactly when `quantum + (maximum_frame - 1)` overflows `u64`, so the largest accepted quantum
/// is `u64::MAX - (maximum_frame - 1)` and the verdict is a step function of the value the ninth
/// site computes. A frame bound that changed by one in either direction would move that step.
#[test]
fn deficit_round_robin_scheduling_pins_the_ninth_sites_frame_bound() {
    let source = compile_fixture("configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml");

    // The reachability precondition of the site: DRR queues that have an egress link, and TCP
    // generators to evaluate the frame bound for.
    let drr_egress_queues = source
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .filter(|queue| queue.egress_link.is_some())
        .count();
    let tcp_generators = source
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Tcp(_)))
        .count();
    assert!(
        drr_egress_queues > 0 && tcp_generators > 0,
        "the DRR image must reach maximum_drr_frame_bytes at all"
    );

    // 1460 = the CUBIC MSS of this fixture's fresh segments, which dominates the 40-byte ACKs and
    // the (empty) initial packet table. Frozen: it is the value the ninth site returns.
    const MAXIMUM_FRAME_BYTES: u64 = 1460;
    let largest_accepted_quantum = u64::MAX - (MAXIMUM_FRAME_BYTES - 1);

    let accepted = with_single_class_drr_scheduling(&source, largest_accepted_quantum);
    assert_validate_flow_index_equivalent_for_testing(&accepted)
        .expect("the DRR image's indexed walks must match the scanned walks");
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        validate(&accepted, backend).unwrap_or_else(|error| {
            panic!("DRR quantum {largest_accepted_quantum} on {backend:?}: {error}")
        });
    }

    let rejected = with_single_class_drr_scheduling(&source, largest_accepted_quantum + 1);
    let error = validate(&rejected, Backend::Scalar)
        .expect_err("one byte more of quantum must overflow the DRR deficit bound");
    assert!(
        error
            .to_string()
            .contains(&format!("cannot accumulate enough deficit for a {MAXIMUM_FRAME_BYTES}-byte packet without overflowing")),
        "the rejection must name the frame bound the ninth site computed: {error}"
    );
}

/// A packet whose flow identifier falls outside the dense flow table cannot be grouped, so the
/// index keeps it in one shared unindexed group that a query filters by equality. Nothing the
/// validator accepts reaches that path, but the equality it relies on must hold there too.
///
/// Two *different* outside identifiers share that group deliberately: with only one, the group is
/// already the answer and the fallback's equality filter is inert, so the branch would be
/// exercised without being tested. With two, a query for either must reject the other's packets.
#[test]
fn flow_indexed_walks_match_the_scans_for_flows_outside_the_dense_table() {
    let mut image = compile_fixture("configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml");
    let first_outside = FlowId(image.flows.len() as u64 + 5);
    let second_outside = FlowId(image.flows.len() as u64 + 9);
    image.initial_packets[0].flow = first_outside;
    image.initial_packets[3].flow = first_outside;
    image.initial_packets[5].flow = second_outside;

    // The gate queries exactly the identifiers that label a packet without resolving to a dense
    // slot. Recomputing that set here is what keeps the unindexed branch from being gated
    // vacuously: if it were empty, the gate below would issue no unindexed query at all.
    let outside_queries = image
        .initial_packets
        .iter()
        .map(|packet| packet.flow)
        .filter(|id| {
            image
                .flows
                .get(id.0 as usize)
                .is_none_or(|flow| flow.id != *id)
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        outside_queries,
        BTreeSet::from([first_outside, second_outside]),
        "the gate must issue an unindexed query for each of the two outside identifiers"
    );

    assert_validate_flow_index_equivalent_for_testing(&image)
        .expect("unindexed flow groups must match the scanned walks");
    validate(&image, Backend::Scalar).expect_err("an unknown packet flow must still reject");
}

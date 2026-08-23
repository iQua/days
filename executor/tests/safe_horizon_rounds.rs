use std::collections::{BTreeMap, VecDeque};

use days_executor::{
    Backend, ChunkGranularity, ConstantGenerator, CpuConfig, CpuFaultInjection, CpuFaultKind,
    Event, EventKey, EventKind, ExecutionError, FlowDescriptor, FlowGeneratorKind,
    FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination,
    HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode,
    PacketDescriptor, PacketKind, PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind,
    SimulationImage, StaticPartitionPolicy, SwitchQueueState, SwitchState, WorkClass, event_phase,
    run_cpu_with_observations, run_scalar_rounds_with_observations, run_scalar_with_observations,
    validate,
};
// The only unqualified uses of this type are in `assert_device_full_result_eq`, which is
// device-gated; the two remaining uses spell out `days_executor::RunResult`.
#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
use days_executor::RunResult;
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, run_cuda_with_observations};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{
    MetalConfig, RoundMetricsWindow, run_cpu_with_metrics_window, run_metal_with_observations,
    run_scalar_rounds_with_windowed_replay_trace,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const LINK: LinkId = LinkId(0);
const FLOW: FlowId = FlowId(0);
const PACKET: PayloadId = PayloadId(0);

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn assert_device_full_result_eq(actual: &RunResult, scalar: &RunResult, context: &str) {
    assert!(
        scalar.diagnostics.is_some(),
        "{context}: scalar Full diagnostics must be present"
    );
    assert!(
        actual.diagnostics.is_none(),
        "{context}: device Full diagnostics must be absent"
    );
    let mut expected = scalar.clone();
    expected.diagnostics = None;
    assert_eq!(actual, &expected, "{context}");
}

fn image(stop_time_ns: u64, event_time_ns: u64, channel_delay_ns: u64) -> SimulationImage {
    SimulationImage {
        stop_time_ns,
        nodes: vec![
            NodeDescriptor {
                id: SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: SINK,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![LINK],
            reverse_route: vec![],
        }],
        initial_packets: vec![PacketDescriptor {
            id: PACKET,
            flow: FLOW,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        }],
        links: vec![LinkDescriptor {
            id: LINK,
            source: SOURCE,
            target: SINK,
            rate_bps: 8_000_000_000,
            propagation_ns: channel_delay_ns - 1,
        }],
        channels: vec![RemoteChannel {
            source: SOURCE,
            target: SINK,
            link: LINK,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: channel_delay_ns,
        }],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: event_time_ns,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: PACKET,
        }],
        seed: 1,
    }
}

fn minimum_channel_delay_ns(image: &SimulationImage) -> u64 {
    image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min()
        .expect("a simulation image has at least one channel")
}

fn add_sink_owned_egress(image: &mut SimulationImage) {
    let sink_egress = LinkDescriptor {
        id: LinkId(1),
        source: SINK,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    image.host_states[1].egress_link = sink_egress.id;
    image.links.push(sink_egress);
}

fn causally_incompatible_positions_image() -> SimulationImage {
    let mut image = image(0, 0, 1);
    add_sink_owned_egress(&mut image);
    image.host_states[0].next_origin_seq = 2;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload: PACKET,
        },
        Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: PACKET,
        },
    ];
    image
}

fn in_flight_image(propagation_ns: u64) -> SimulationImage {
    let transmission_complete_ns = 1;
    let remote_arrival_ns = transmission_complete_ns + propagation_ns;
    let mut image = image(remote_arrival_ns, 0, propagation_ns + 1);
    add_sink_owned_egress(&mut image);
    image.host_states[0].in_service = Some(PACKET);
    image.host_states[0].next_origin_seq = 2;
    image.host_states[0].sourced_packets = 1;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: transmission_complete_ns,
                phase: event_phase(EventKind::TxComplete),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::TxComplete,
            payload: PACKET,
        },
        Event {
            key: EventKey {
                time_ns: remote_arrival_ns,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload: PACKET,
        },
    ];
    image.initial_events.sort_unstable_by_key(|event| event.key);
    image
}

fn ready_token_and_remote_arrival_image() -> SimulationImage {
    let queued = PayloadId::from_node_sequence(SOURCE, 2, 1).unwrap();
    let mut image = image(5, 0, 4);
    add_sink_owned_egress(&mut image);
    image.initial_packets.push(PacketDescriptor {
        id: queued,
        flow: FLOW,
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::Data,
    });
    image.host_states[0].queue = VecDeque::from([queued]);
    image.host_states[0].tx_ready_pending = true;
    image.host_states[0].next_origin_seq = 3;
    image.host_states[0].sourced_packets = 2;
    image.host_states[0].departed_packets = 1;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 1,
                phase: event_phase(EventKind::TxReady),
                origin_node: SOURCE,
                origin_seq: 2,
            },
            target: SOURCE,
            kind: EventKind::TxReady,
            payload: PACKET,
        },
        Event {
            key: EventKey {
                time_ns: 4,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload: PACKET,
        },
    ];
    image
}

fn assert_equivalent(image: &SimulationImage, exclusive_horizon_ns: Option<u64>) {
    let global =
        run_scalar_with_observations(image, exclusive_horizon_ns, ObservationMode::Full).unwrap();
    let rounds =
        run_scalar_rounds_with_observations(image, exclusive_horizon_ns, ObservationMode::Full)
            .unwrap();

    assert_eq!(rounds.result, global);
    for round in &rounds.rounds {
        assert_eq!(
            round.events_processed,
            round
                .lp_work
                .iter()
                .map(|work| work.events_processed)
                .sum::<u64>()
        );
        assert_eq!(round.active_lp_count, round.lp_work.len());
        assert!(round.lp_work.iter().all(|work| work.events_processed > 0));
    }
}

#[test]
fn causally_incompatible_initial_payload_positions_are_rejected() {
    let image = causally_incompatible_positions_image();
    let expected = "payload PayloadId(0) has causally incompatible initial positions: PacketArrival event 1 at node NodeId(0) and RemoteArrival event 0 on link LinkId(0) from NodeId(0) to NodeId(1)";

    for backend in [Backend::Scalar, Backend::Cpu { workers: 1 }] {
        let diagnostic = validate(&image, backend)
            .expect_err("causally incompatible positions must reject")
            .to_string();
        println!("{backend} validation: Err({diagnostic})");
        assert_eq!(diagnostic, expected);
    }
}

#[test]
fn in_flight_pair_must_be_siblings_from_one_transmission() {
    let mut image = in_flight_image(0);
    image.initial_events[0].key.origin_seq = 2;
    image.host_states[0].next_origin_seq = 3;

    assert_eq!(
        validate(&image, Backend::Scalar)
            .expect_err("independently emitted positions must reject")
            .to_string(),
        "payload PayloadId(0) has causally incompatible initial positions: TxComplete event 1 at node NodeId(0) and RemoteArrival event 0 on link LinkId(0) from NodeId(0) to NodeId(1)"
    );
}

#[test]
fn in_flight_completion_and_remote_arrival_remain_valid_and_equivalent() {
    for propagation_ns in [0, 3] {
        let image = in_flight_image(propagation_ns);
        validate(&image, Backend::Scalar).unwrap();
        validate(&image, Backend::Cpu { workers: 1 }).unwrap();
        assert_equivalent(&image, None);

        let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 4,
                granularity: ChunkGranularity::Fixed(1),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, result);
        assert_eq!(result.summary.departed_packets, 1);
        assert_eq!(result.summary.received_packets, 1);
        assert!(result.resident_packets.is_empty());
        assert!(result.pending_events.is_empty());
    }
}

#[test]
fn ready_control_token_can_coexist_with_an_in_flight_payload() {
    let image = ready_token_and_remote_arrival_image();
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 1 }).unwrap();
    assert_equivalent(&image, None);

    let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();
    assert_eq!(cpu.result, result);
    assert_eq!(result.summary.departed_packets, 1);
    assert_eq!(result.summary.received_packets, 2);
    assert!(result.resident_packets.is_empty());
    assert!(result.pending_events.is_empty());
}

#[test]
fn backends_preserve_accepted_orphan_packet_snapshots() {
    let mut lost_remote = in_flight_image(4);
    lost_remote
        .initial_events
        .retain(|event| event.kind == EventKind::TxComplete);
    validate(&lost_remote, Backend::Scalar).unwrap();
    validate(&lost_remote, Backend::Cpu { workers: 2 }).unwrap();

    let mut orphan_ready = ready_token_and_remote_arrival_image();
    orphan_ready
        .initial_events
        .retain(|event| event.kind == EventKind::TxReady);
    validate(&orphan_ready, Backend::Scalar).unwrap();
    validate(&orphan_ready, Backend::Cpu { workers: 2 }).unwrap();

    for image in [lost_remote, orphan_ready] {
        let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        let actual = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity: ChunkGranularity::Fixed(1),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(actual.result, expected);

        #[cfg(feature = "cuda")]
        {
            let actual = run_cuda_with_observations(
                &image,
                None,
                CudaConfig::default(),
                ObservationMode::Full,
            )
            .unwrap();
            assert_device_full_result_eq(&actual.result, &expected, "CUDA orphan snapshot");
        }

        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        {
            let actual = run_metal_with_observations(
                &image,
                None,
                MetalConfig::default(),
                ObservationMode::Full,
            )
            .unwrap();
            assert_device_full_result_eq(&actual.result, &expected, "Metal orphan snapshot");
        }
    }
}

#[test]
fn scalar_rounds_match_the_global_queue_and_record_sparse_round_work() {
    let image = image(100, 0, 10);
    assert_equivalent(&image, None);

    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();
    assert_eq!(run.rounds[0].events_processed, 3);
    assert_eq!(run.rounds[0].messages_exchanged, 1);
    assert_eq!(run.rounds[0].lp_work[0].node, SOURCE);
    assert_eq!(run.rounds[1].events_processed, 1);
    assert_eq!(run.rounds[1].lp_work[0].node, SINK);
    assert_eq!(run.rounds[0].parallel_efficiency, 1.0);
    assert_eq!(run.rounds[1].parallel_efficiency, 1.0);
}

/// The horizon is exactly `frontier + minimum channel delay`, clamped by the configured stop.
///
/// This is the definition the equivalent-slot denominator is derived from: a pipeline that must
/// quantise at the shortest delay anywhere in the fabric needs one tick per `min_delay_ns`.
#[test]
fn every_round_horizon_is_the_frontier_plus_the_minimum_channel_delay() {
    let image = image(100, 0, 10);
    let quantum = u128::from(minimum_channel_delay_ns(&image));
    let run_end = u128::from(image.stop_time_ns) + 1;
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    assert!(!run.rounds.is_empty(), "fixture must execute rounds");
    for (index, round) in run.rounds.iter().enumerate() {
        assert_eq!(
            round.exclusive_horizon_ns,
            run_end.min(u128::from(round.frontier_ns) + quantum),
            "round {index} horizon is not the frontier plus the lookahead quantum",
        );
    }
}

/// Round boundaries never move backwards, and no round starts before the previous one ended.
///
/// Without this the per-round trace could not be read as a partition of simulated time, and the
/// horizon-width-over-simulated-time artifact would be meaningless.
#[test]
fn round_boundaries_partition_simulated_time_forwards() {
    let image = image(100, 0, 10);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    for pair in run.rounds.windows(2) {
        assert!(
            u128::from(pair[1].frontier_ns) >= pair[0].exclusive_horizon_ns,
            "round frontier {} precedes the previous horizon {}",
            pair[1].frontier_ns,
            pair[0].exclusive_horizon_ns,
        );
        assert!(
            pair[1].exclusive_horizon_ns > pair[0].exclusive_horizon_ns,
            "round horizon did not advance past {}",
            pair[0].exclusive_horizon_ns,
        );
    }
}

/// The hardware-independent claim: our round count is bounded by the equivalent slot count.
///
/// A slot pipeline quantised at the fabric's shortest delay needs `ceil(span / quantum)` ticks to
/// cover the same simulated span. Because the horizon advances by at least one quantum per round,
/// our round count can only meet or beat that bound. The rounds-vs-slots ratio published for a
/// fixture is therefore a structural statement, never a hardware one.
#[test]
fn rounds_never_exceed_the_equivalent_slot_count_at_the_minimum_delay_quantum() {
    let image = image(100, 0, 10);
    let quantum = u128::from(minimum_channel_delay_ns(&image));
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    let span_ns = u128::from(image.stop_time_ns);
    let equivalent_slots = span_ns.div_ceil(quantum);
    assert!(
        run.rounds.len() as u128 <= equivalent_slots,
        "{} rounds exceed the {equivalent_slots} equivalent slots of a {quantum} ns quantum",
        run.rounds.len(),
    );
}

#[test]
fn cpu_handles_no_active_lps_and_more_workers_than_nodes() {
    let mut image = image(100, 0, 10);
    image.initial_events.clear();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 8,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert!(actual.rounds.is_empty());
    assert_eq!(actual.result, expected);
}

#[test]
fn inclusive_stop_and_half_open_external_horizon_remain_distinct() {
    let image = image(10, 10, 1);
    assert_equivalent(&image, None);
    assert_equivalent(&image, Some(10));

    let through_stop = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("the inclusive stop event should execute");
    let before_horizon =
        run_scalar_rounds_with_observations(&image, Some(10), ObservationMode::Full)
            .expect("the half-open horizon should return without executing its boundary");
    assert_eq!(through_stop.result.summary.sourced_packets, 1);
    assert_eq!(before_horizon.result.summary.sourced_packets, 0);
}

fn direct_packets(channel_delay_ns: u64, packet_count: u64) -> SimulationImage {
    let mut image = image(1_000, 0, channel_delay_ns);
    image.flows.clear();
    image.initial_packets.clear();
    image.initial_events.clear();
    image.host_states[0].next_origin_seq = packet_count;
    for sequence in 0..packet_count {
        let flow = FlowId(sequence);
        let payload = PayloadId::from_node_sequence(SOURCE, 2, sequence).unwrap();
        image.flows.push(FlowDescriptor {
            id: flow,
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![LINK],
            reverse_route: vec![],
        });
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: sequence,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: sequence,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload,
        });
    }
    image
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn windowed_round_metrics_match_full_runs_without_retaining_the_tail() {
    let image = direct_packets(1, 8);
    let scalar_full =
        run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary).unwrap();
    let cpu_config = CpuConfig {
        workers: 4,
        ..CpuConfig::default()
    };
    let cpu_full =
        run_cpu_with_observations(&image, None, cpu_config, ObservationMode::Summary).unwrap();
    assert!(
        scalar_full.rounds.len() > 4,
        "the fixture needs a tail after the retained window"
    );
    assert_eq!(cpu_full.rounds.len(), scalar_full.rounds.len());

    let window = RoundMetricsWindow {
        start_round: 1,
        rounds: 2,
    };
    let (scalar, trace) =
        run_scalar_rounds_with_windowed_replay_trace(&image, None, window).unwrap();
    let cpu = run_cpu_with_metrics_window(&image, None, cpu_config, window).unwrap();
    let packet_arrivals = trace
        .event_counts_by_round(days_executor::EventKind::PacketArrival)
        .unwrap();

    assert_eq!(scalar.result, scalar_full.result);
    assert_eq!(cpu.result, scalar_full.result);
    assert_eq!(scalar.rounds, scalar_full.rounds[1..3]);
    assert_eq!(cpu.rounds.len(), 2);
    for (retained, full) in cpu.rounds.iter().zip(&cpu_full.rounds[1..3]) {
        assert_eq!(retained.semantic, full.semantic);
        assert_eq!(retained.owner_batch_messages, full.owner_batch_messages);
        assert_eq!(retained.owner_batches_merged, full.owner_batches_merged);
        assert_eq!(
            retained.early_owner_batches_merged,
            full.early_owner_batches_merged
        );
    }

    let total_events = scalar_full
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    assert_eq!(scalar.totals.whole_run.rounds, scalar_full.rounds.len());
    assert_eq!(scalar.totals.whole_run.events_processed, total_events);
    assert_eq!(scalar.totals.before_window.rounds, 1);
    assert_eq!(scalar.totals.retained_window.rounds, 2);
    assert_eq!(
        scalar.totals.retained_window.active_lp_rounds,
        scalar_full.rounds[1..3]
            .iter()
            .map(|round| round.active_lp_count as u128)
            .sum::<u128>()
    );
    assert_eq!(
        scalar.totals.retained_window.maximum_active_lps,
        scalar_full.rounds[1..3]
            .iter()
            .map(|round| round.active_lp_count)
            .max()
            .unwrap()
    );
    assert_eq!(
        scalar.totals.after_window.rounds,
        scalar_full.rounds.len() - 3
    );
    assert_eq!(cpu.totals, scalar.totals);
    assert_eq!(trace.source_round_count, scalar_full.rounds.len());
    assert_eq!(
        trace
            .rounds
            .iter()
            .map(|round| round.source_round)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(packet_arrivals.len(), trace.rounds.len());
    assert!(packet_arrivals.iter().any(|&count| count > 0));
}

#[test]
fn wide_and_narrow_lookahead_pin_events_per_round_separately_from_efficiency() {
    let wide = direct_packets(100, 8);
    let narrow = direct_packets(1, 2);
    assert_equivalent(&wide, None);
    assert_equivalent(&narrow, None);

    let wide = run_scalar_rounds_with_observations(&wide, None, ObservationMode::Full).unwrap();
    let narrow = run_scalar_rounds_with_observations(&narrow, None, ObservationMode::Full).unwrap();
    assert_eq!(wide.rounds[0].events_per_round(), 24);
    assert_eq!(wide.rounds[0].messages_exchanged, 8);
    assert_eq!(narrow.rounds[0].events_per_round(), 2);
    assert_eq!(narrow.rounds[0].messages_exchanged, 1);
    assert_eq!(wide.rounds[0].parallel_efficiency, 1.0);
    assert_eq!(narrow.rounds[0].parallel_efficiency, 1.0);
}

#[test]
fn same_time_tx_ready_continuations_skip_round_queue_churn_without_changing_state() {
    let image = direct_packets(100, 8);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Static,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(scalar.result, expected);
    assert_eq!(cpu.result, expected);
    assert_eq!(
        scalar
            .rounds
            .iter()
            .flat_map(|round| &round.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>(),
        7
    );
    assert_eq!(
        cpu.rounds
            .iter()
            .flat_map(|round| &round.semantic.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>(),
        7
    );
}

#[test]
fn cross_transport_round_semantics_match_for_remote_and_finish_merges() {
    let image = image(100, 0, 10);
    for exclusive_horizon_ns in [None, Some(10)] {
        let expected = run_scalar_rounds_with_observations(
            &image,
            exclusive_horizon_ns,
            ObservationMode::Full,
        )
        .unwrap();
        let semantics = |round: &days_executor::RoundMetrics| {
            (
                round.frontier_ns,
                round.exclusive_horizon_ns,
                round.horizon_advance_ns,
                round.events_processed,
                round.active_lp_count,
                round.lp_work.clone(),
                round.parallel_efficiency,
                round.messages_exchanged,
                round.frontier_updates,
                round.frontier_heap_pops,
            )
        };
        let expected_semantics = expected.rounds.iter().map(&semantics).collect::<Vec<_>>();
        assert_eq!(
            expected
                .rounds
                .iter()
                .map(|round| round.frontier_updates)
                .collect::<Vec<_>>(),
            if exclusive_horizon_ns.is_some() {
                vec![2]
            } else {
                vec![2, 1]
            }
        );

        for workers in [1, 2, 4] {
            for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
                let actual = run_cpu_with_observations(
                    &image,
                    exclusive_horizon_ns,
                    CpuConfig {
                        workers,
                        granularity,
                        ..CpuConfig::default()
                    },
                    ObservationMode::Full,
                )
                .unwrap();
                let actual_semantics = actual
                    .rounds
                    .iter()
                    .map(|round| semantics(&round.semantic))
                    .collect::<Vec<_>>();

                assert_eq!(
                    actual.result, expected.result,
                    "workers={workers}, granularity={granularity:?}, \
                     exclusive_horizon_ns={exclusive_horizon_ns:?}"
                );
                assert_eq!(
                    actual_semantics, expected_semantics,
                    "workers={workers}, granularity={granularity:?}, \
                     exclusive_horizon_ns={exclusive_horizon_ns:?}"
                );
            }
        }
    }
}

#[test]
fn cpu_batches_remote_events_once_per_source_and_target_owner_per_round() {
    let mut image = incast_image(8);
    image.initial_events.clear();
    for sender in 0..8 {
        let sender = NodeId(sender);
        let state = &mut image.host_states[sender.0 as usize];
        state.in_service = None;
        state.next_origin_seq = 1;
        state.sourced_packets = 0;
        let payload = image.initial_packets[sender.0 as usize].id;
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: sender,
                origin_seq: 0,
            },
            target: sender,
            kind: EventKind::PacketArrival,
            payload,
        });
    }
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 1,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    assert_eq!(actual.rounds[0].semantic.active_lp_count, 8);
    assert_eq!(actual.rounds[0].semantic.messages_exchanged, 8);
    assert_eq!(actual.rounds[0].partition.bulk_chunks.len(), 8);
    assert_eq!(actual.rounds[0].owner_batch_messages, 1);
    assert!(
        actual
            .rounds
            .iter()
            .all(|round| round.owner_batch_messages <= 1)
    );
}

#[test]
fn static_cpu_pool_publishes_one_horizon_and_delivers_owner_batches_directly() {
    let image = direct_packets(100, 8);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let workers = 4;
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers,
            granularity: ChunkGranularity::Static,
            straggler_threshold_events: None,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    for round in &actual.rounds {
        assert_eq!(round.worker_wake_messages, workers as u64);
        assert_eq!(round.worker_completion_messages, workers as u64);
        assert_eq!(round.chunk_request_messages, 0);
        assert_eq!(
            round.owner_delivery_messages, round.owner_batch_messages,
            "static owner batches must bypass the coordinator"
        );
        assert_eq!(
            round.owner_batches_merged, round.owner_delivery_messages,
            "the next publication must seal every direct owner batch"
        );
        assert!(round.early_owner_batches_merged <= round.owner_batches_merged);
        assert!(round.early_owner_merge_ns <= round.owner_merge_ns);
        assert_eq!(
            round.pool_messages(),
            2 * workers as u64 + round.owner_delivery_messages
        );
    }
}

#[test]
fn summary_mode_samples_static_lp_wall_clock_buckets() {
    let image = direct_packets(1, 100);
    let config = CpuConfig {
        workers: 4,
        granularity: ChunkGranularity::Static,
        ..CpuConfig::default()
    };
    let summary =
        run_cpu_with_observations(&image, None, config, ObservationMode::Summary).unwrap();
    let full = run_cpu_with_observations(&image, None, config, ObservationMode::Full).unwrap();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Summary).unwrap();

    assert_eq!(summary.result, expected);
    assert_eq!(summary.result.summary, full.result.summary);
    assert!(summary.rounds.len() > 64);
    assert!(
        summary
            .rounds
            .iter()
            .enumerate()
            .all(|(round, metrics)| { metrics.lp_timings.is_empty() == (round % 64 != 0) })
    );
    assert!(
        full.rounds
            .iter()
            .all(|round| { round.lp_timings.len() == round.semantic.active_lp_count })
    );
}

#[test]
fn bounded_spin_configuration_preserves_static_and_dynamic_results() {
    let image = direct_packets(100, 8);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for spin_before_park in [0, 4_096] {
        for granularity in [ChunkGranularity::Static, ChunkGranularity::Fixed(1)] {
            let actual = run_cpu_with_observations(
                &image,
                None,
                CpuConfig {
                    workers: 4,
                    granularity,
                    spin_before_park,
                    ..CpuConfig::default()
                },
                ObservationMode::Full,
            )
            .unwrap();
            assert_eq!(
                actual.result, expected,
                "spin_before_park={spin_before_park}, granularity={granularity:?}"
            );
        }
    }
}

fn blocked_feedback_image() -> SimulationImage {
    let forward = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(1),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let reverse = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let feedback = PayloadId(1);
    SimulationImage {
        stop_time_ns: 10,
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LinkId(0),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![FlowGeneratorState {
                    flow: FlowId(0),
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Blocked,
                        departure_time_ns: 0,
                        payload: PayloadId(0),
                    },
                    rng_state: 1,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: 0,
                        interval_ns: 1,
                        packet_size_bytes: 1,
                        termination: GeneratorTermination::Bytes(1),
                    }),
                }],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(1),
                queue: VecDeque::from([feedback]),
                in_service: None,
                tx_ready_pending: true,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: FlowId(0),
            source: NodeId(0),
            target: NodeId(1),
            priority: 0,
            route: vec![LinkId(0)],
            reverse_route: vec![LinkId(1)],
        }],
        initial_packets: vec![PacketDescriptor {
            id: feedback,
            flow: FlowId(0),
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Feedback,
        }],
        links: vec![forward, reverse],
        channels: vec![
            RemoteChannel::for_packet_link(forward, 1).unwrap(),
            RemoteChannel::for_packet_link(reverse, 1).unwrap(),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 5,
                phase: event_phase(EventKind::TxReady),
                origin_node: NodeId(1),
                origin_seq: 0,
            },
            target: NodeId(1),
            kind: EventKind::TxReady,
            payload: feedback,
        }],
        seed: 9,
    }
}

#[test]
fn blocked_lp_is_absent_from_the_frontier_until_feedback_arrives() {
    let image = blocked_feedback_image();
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 1 }).unwrap();
    assert_equivalent(&image, None);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();

    assert_eq!(run.rounds[0].frontier_ns, 5);
    assert_eq!(run.rounds[0].exclusive_horizon_ns, 6);
    assert_eq!(
        run.rounds[0]
            .lp_work
            .iter()
            .map(|work| work.node)
            .collect::<Vec<_>>(),
        vec![NodeId(1)]
    );
    assert!(
        run.rounds[1]
            .lp_work
            .iter()
            .any(|work| work.node == NodeId(0))
    );
    assert_eq!(
        run.result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked
    );
    assert_eq!(run.result.host_states[0].generators[0].feedback.arrivals, 1);
}

fn canonical_exchange_image() -> SimulationImage {
    let source_zero_link = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(2),
        rate_bps: 8_000_000_000,
        propagation_ns: 9,
    };
    let source_one_link = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(2),
        rate_bps: 8_000_000_000,
        propagation_ns: 10,
    };
    let switch_link = LinkDescriptor {
        id: LinkId(2),
        source: NodeId(2),
        target: NodeId(3),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let sink_egress = LinkDescriptor {
        id: LinkId(3),
        source: NodeId(3),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let packet_zero = PayloadId(0);
    let packet_one = PayloadId(1);
    SimulationImage {
        stop_time_ns: 100,
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(2),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Host,
                state_slot: 2,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LinkId(0),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(1),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(3),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![SwitchState {
            physical_switch: 0,
            queues: vec![SwitchQueueState {
                egress_link: Some(LinkId(2)),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 1,
                drop_mark: Default::default(),
                pfc: None,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        }],
        flows: vec![
            FlowDescriptor {
                id: FlowId(0),
                source: NodeId(0),
                target: NodeId(3),
                priority: 0,
                route: vec![LinkId(0), LinkId(2)],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(1),
                target: NodeId(3),
                priority: 0,
                route: vec![LinkId(1), LinkId(2)],
                reverse_route: vec![],
            },
        ],
        initial_packets: vec![
            PacketDescriptor {
                id: packet_zero,
                flow: FlowId(0),
                size_bytes: 1,
                ecn_marked: false,
                kind: PacketKind::Data,
            },
            PacketDescriptor {
                id: packet_one,
                flow: FlowId(1),
                size_bytes: 1,
                ecn_marked: false,
                kind: PacketKind::Data,
            },
        ],
        links: vec![source_zero_link, source_one_link, switch_link, sink_egress],
        channels: vec![
            RemoteChannel::for_packet_link(source_zero_link, 1).unwrap(),
            RemoteChannel::for_packet_link(source_one_link, 1).unwrap(),
            RemoteChannel::for_packet_link(switch_link, 1).unwrap(),
        ],
        initial_events: vec![
            Event {
                key: EventKey {
                    time_ns: 0,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(1),
                    origin_seq: 0,
                },
                target: NodeId(1),
                kind: EventKind::PacketArrival,
                payload: packet_one,
            },
            Event {
                key: EventKey {
                    time_ns: 1,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(0),
                    origin_seq: 0,
                },
                target: NodeId(0),
                kind: EventKind::PacketArrival,
                payload: packet_zero,
            },
        ],
        seed: 10,
    }
}

#[test]
fn canonical_exchange_reorders_outboxes_before_finite_queue_admission() {
    let image = canonical_exchange_image();
    validate(&image, Backend::Cpu { workers: 1 }).unwrap();
    assert_equivalent(&image, None);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();
    let at_switch = run
        .result
        .arrivals
        .iter()
        .filter(|arrival| arrival.time_ns == 11)
        .collect::<Vec<_>>();

    assert_eq!(at_switch[0].payload, PayloadId(0));
    assert_eq!(
        at_switch[0].disposition,
        days_executor::ArrivalDisposition::Admitted
    );
    assert_eq!(at_switch[1].payload, PayloadId(1));
    assert_eq!(
        at_switch[1].disposition,
        days_executor::ArrivalDisposition::Dropped
    );
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();
    assert_eq!(cpu.result, run.result);
}

fn add_idle_switches(image: &mut SimulationImage, count: usize) {
    for _ in 0..count {
        let id = NodeId(image.nodes.len() as u64);
        let state_slot = image.switch_states.len() as u32;
        let egress_link = LinkDescriptor {
            id: LinkId(image.links.len() as u64),
            source: id,
            target: SOURCE,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        };
        image.nodes.push(NodeDescriptor {
            id,
            kind: NodeKind::Switch,
            state_slot,
        });
        image.links.push(egress_link);
        image.switch_states.push(SwitchState {
            physical_switch: u64::from(state_slot),
            queues: vec![SwitchQueueState {
                egress_link: Some(egress_link.id),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 1,
                drop_mark: Default::default(),
                pfc: None,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        });
    }
}

#[test]
fn idle_lp_count_does_not_change_round_loop_operations() {
    let mut small = image(100, 0, 10);
    add_sink_owned_egress(&mut small);
    let mut large = small.clone();
    add_idle_switches(&mut small, 100);
    add_idle_switches(&mut large, 10_000);
    for image in [&small, &large] {
        validate(image, Backend::Scalar).unwrap();
        validate(image, Backend::Cpu { workers: 4 }).unwrap();
    }

    let scalar_small =
        run_scalar_rounds_with_observations(&small, None, ObservationMode::Full).unwrap();
    let scalar_large =
        run_scalar_rounds_with_observations(&large, None, ObservationMode::Full).unwrap();
    let operations = |run: &days_executor::ScalarRoundRun| {
        run.rounds
            .iter()
            .map(|round| {
                (
                    round.frontier_ns,
                    round.exclusive_horizon_ns,
                    round.events_processed,
                    round.active_lp_count,
                    round.lp_work.clone(),
                    round.messages_exchanged,
                    round.frontier_updates,
                    round.frontier_heap_pops,
                )
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(operations(&scalar_small), operations(&scalar_large));

    let config = CpuConfig {
        workers: 4,
        granularity: ChunkGranularity::Fixed(1),
        ..CpuConfig::default()
    };
    let cpu_small = run_cpu_with_observations(&small, None, config, ObservationMode::Full).unwrap();
    let cpu_large = run_cpu_with_observations(&large, None, config, ObservationMode::Full).unwrap();
    let cpu_operations = |run: &days_executor::CpuRun| {
        run.rounds
            .iter()
            .map(|round| {
                (
                    round.semantic.frontier_ns,
                    round.semantic.exclusive_horizon_ns,
                    round.semantic.events_processed,
                    round.semantic.active_lp_count,
                    round.semantic.lp_work.clone(),
                    round.semantic.messages_exchanged,
                    round.semantic.frontier_updates,
                    round.semantic.frontier_heap_pops,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(cpu_operations(&cpu_small), cpu_operations(&cpu_large));
    let physical_probes = |run: &days_executor::ScalarRoundRun| {
        run.rounds
            .iter()
            .map(|round| round.physical_lp_probes)
            .collect::<Vec<_>>()
    };
    let cpu_physical_probes = |run: &days_executor::CpuRun| {
        run.rounds
            .iter()
            .map(|round| round.semantic.physical_lp_probes)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        (
            physical_probes(&scalar_small),
            cpu_physical_probes(&cpu_small)
        ),
        (
            physical_probes(&scalar_large),
            cpu_physical_probes(&cpu_large)
        )
    );
    assert_eq!(cpu_small.result, scalar_small.result);
    assert_eq!(cpu_large.result, scalar_large.result);
}

#[derive(Clone, Copy)]
struct StableRng(u64);

impl StableRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn range(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn heterogeneous_image(seed: u64) -> SimulationImage {
    let mut rng = StableRng(seed);
    let idle_switches = rng.range(5) as usize;
    let node_count = 5 + idle_switches;
    let source_link = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(1),
        rate_bps: [2_000_000_000, 4_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(5),
    };
    let switch_link = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(3),
        rate_bps: [1_000_000_000, 2_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(7),
    };
    let alternate_switch_link = LinkDescriptor {
        id: LinkId(2),
        source: NodeId(2),
        target: NodeId(4),
        rate_bps: [1_000_000_000, 4_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(7),
    };
    let return_link = LinkDescriptor {
        id: LinkId(3),
        source: NodeId(3),
        target: NodeId(1),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let alternate_return_link = LinkDescriptor {
        id: LinkId(4),
        source: NodeId(4),
        target: NodeId(2),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let flow_count = 2 + rng.range(10);
    let packet_count = flow_count;
    let mut flows = Vec::new();
    for flow in 0..flow_count {
        let alternate_sink = flow % 2 == 1;
        flows.push(FlowDescriptor {
            id: FlowId(flow),
            source: NodeId(0),
            target: if alternate_sink { NodeId(4) } else { NodeId(3) },
            priority: 0,
            route: vec![LinkId(0), LinkId(if alternate_sink { 2 } else { 1 })],
            reverse_route: vec![],
        });
    }

    let mut initial_packets = Vec::new();
    let mut initial_events = Vec::new();
    let mut minimum_source_size = u64::MAX;
    let mut minimum_switch_size = u64::MAX;
    let mut minimum_alternate_switch_size = u64::MAX;
    for sequence in 0..packet_count {
        let flow = FlowId(sequence);
        let size_bytes = 1 + rng.range(4);
        let payload =
            PayloadId::from_node_sequence(NodeId(0), node_count as u64, sequence).unwrap();
        initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes,
            ecn_marked: false,
            kind: PacketKind::Data,
        });
        initial_events.push(Event {
            key: EventKey {
                time_ns: rng.range(18),
                phase: event_phase(EventKind::PacketArrival),
                origin_node: NodeId(0),
                origin_seq: sequence,
            },
            target: NodeId(0),
            kind: EventKind::PacketArrival,
            payload,
        });
        minimum_source_size = minimum_source_size.min(size_bytes);
        if flows[flow.0 as usize].target == NodeId(3) {
            minimum_switch_size = minimum_switch_size.min(size_bytes);
        } else {
            minimum_alternate_switch_size = minimum_alternate_switch_size.min(size_bytes);
        }
    }
    initial_events.sort_unstable_by_key(|event| event.key);
    if minimum_switch_size == u64::MAX {
        minimum_switch_size = 1;
    }
    if minimum_alternate_switch_size == u64::MAX {
        minimum_alternate_switch_size = 1;
    }

    let mut image = SimulationImage {
        stop_time_ns: 20 + rng.range(120),
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(2),
                kind: NodeKind::Switch,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(4),
                kind: NodeKind::Host,
                state_slot: 2,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LinkId(0),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: packet_count,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(3),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(4),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![
            SwitchState {
                physical_switch: 0,
                queues: vec![SwitchQueueState {
                    egress_link: Some(LinkId(1)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1 + rng.range(5),
                    drop_mark: Default::default(),
                    pfc: None,
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                }],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            },
            SwitchState {
                physical_switch: 0,
                queues: vec![SwitchQueueState {
                    egress_link: Some(LinkId(2)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1 + rng.range(5),
                    drop_mark: Default::default(),
                    pfc: None,
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                }],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            },
        ],
        flows,
        initial_packets,
        links: vec![
            source_link,
            switch_link,
            alternate_switch_link,
            return_link,
            alternate_return_link,
        ],
        channels: vec![
            RemoteChannel::for_packet_link_to(source_link, NodeId(1), minimum_source_size).unwrap(),
            RemoteChannel::for_packet_link_to(source_link, NodeId(2), minimum_source_size).unwrap(),
            RemoteChannel::for_packet_link(switch_link, minimum_switch_size).unwrap(),
            RemoteChannel::for_packet_link(alternate_switch_link, minimum_alternate_switch_size)
                .unwrap(),
        ],
        initial_events,
        seed,
    };
    add_idle_switches(&mut image, idle_switches);
    image
}

fn scheduler_corpus() -> [SchedulerKind; 3] {
    [
        SchedulerKind::Fifo,
        SchedulerKind::static_priority(vec![1, 7, 3]),
        SchedulerKind::weighted_fair_queue(vec![1, 7, 3]),
    ]
}

fn heterogeneous_scheduler_image(seed: u64, scheduler: SchedulerKind) -> SimulationImage {
    let mut image = heterogeneous_image(seed);
    for queue in image
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
    {
        queue.scheduler = scheduler.clone();
    }
    image
}

fn pre_split_heterogeneous_image(seed: u64) -> SimulationImage {
    let mut image = heterogeneous_image(seed);
    let post_node_count = image.nodes.len() as u64;
    let pre_node_count = post_node_count - 1;

    let second_port = image.switch_states.remove(1);
    image.switch_states[0].queues.extend(second_port.queues);
    image.nodes = image
        .nodes
        .into_iter()
        .filter_map(|mut node| {
            if node.id == NodeId(2) {
                return None;
            }
            if node.id.0 > 2 {
                node.id.0 -= 1;
            }
            if node.kind == NodeKind::Switch && node.state_slot > 1 {
                node.state_slot -= 1;
            }
            Some(node)
        })
        .collect();

    for link in &mut image.links {
        match link.id {
            LinkId(0) => {
                link.source = NodeId(0);
                link.target = NodeId(1);
            }
            LinkId(1) => {
                link.source = NodeId(1);
                link.target = NodeId(2);
            }
            LinkId(2) => {
                link.source = NodeId(1);
                link.target = NodeId(3);
            }
            LinkId(3) => {
                link.source = NodeId(2);
                link.target = NodeId(0);
            }
            LinkId(4) => {
                link.source = NodeId(3);
                link.target = NodeId(0);
            }
            LinkId(_) => {
                link.source.0 -= 1;
            }
        }
    }
    for flow in &mut image.flows {
        if flow.target.0 > 2 {
            flow.target.0 -= 1;
        }
    }

    let remap_payload = |payload: PayloadId| {
        let sequence = payload.0 / post_node_count;
        PayloadId(sequence * pre_node_count)
    };
    for packet in &mut image.initial_packets {
        packet.id = remap_payload(packet.id);
    }
    for event in &mut image.initial_events {
        event.payload = remap_payload(event.payload);
    }

    let minimum_by_link =
        image
            .channels
            .iter()
            .fold(BTreeMap::<LinkId, u64>::new(), |mut minimums, channel| {
                minimums
                    .entry(channel.link)
                    .and_modify(|minimum| *minimum = (*minimum).min(channel.min_delay_ns))
                    .or_insert(channel.min_delay_ns);
                minimums
            });
    image.channels = [LinkId(0), LinkId(1), LinkId(2)]
        .into_iter()
        .map(|link_id| {
            let link = image.links[link_id.0 as usize];
            RemoteChannel::for_packet_link(link, minimum_by_link[&link_id]).unwrap()
        })
        .collect();
    image
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticPacket {
    source_sequence: u64,
    flow: FlowId,
    kind: u8,
    size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NormalizedHostState {
    egress_link: LinkId,
    queue: Vec<SemanticPacket>,
    in_service: Option<SemanticPacket>,
    tx_ready_pending: bool,
    generators: Vec<FlowGeneratorState>,
    next_origin_seq: u64,
    next_payload_seq: u64,
    sourced_packets: u64,
    departed_packets: u64,
    received_packets: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NormalizedQueue {
    physical_switch: u64,
    egress_link: Option<LinkId>,
    scheduler: u8,
    queue_capacity_packets: u64,
    queue: Vec<SemanticPacket>,
    in_service: Option<SemanticPacket>,
    tx_ready_pending: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticDeparture {
    packet: SemanticPacket,
    time_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SemanticArrival {
    packet: SemanticPacket,
    time_ns: u64,
    disposition: u8,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct NormalizedPendingEvent {
    packet: SemanticPacket,
    time_ns: u64,
    phase: u16,
    kind: u16,
    physical_target: u64,
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedTerminalResult {
    summary: days_executor::RunSummary,
    host_states: Vec<NormalizedHostState>,
    switch_counters: BTreeMap<u64, (u64, u64, u64, u64)>,
    queues: Vec<NormalizedQueue>,
    resident_packets: Vec<SemanticPacket>,
    observed_packets: Vec<SemanticPacket>,
    pending_events: BTreeMap<u64, Vec<NormalizedPendingEvent>>,
}

#[derive(Debug, Eq, PartialEq)]
struct NormalizedPhysicalResult {
    terminal: NormalizedTerminalResult,
    departures: Vec<SemanticDeparture>,
    arrivals: Vec<SemanticArrival>,
}

fn normalized_physical_result(
    result: &days_executor::RunResult,
    post_split: bool,
    node_count: u64,
) -> NormalizedPhysicalResult {
    let packets = result
        .observed_packets
        .iter()
        .chain(&result.resident_packets)
        .map(|packet| (packet.id, *packet))
        .collect::<BTreeMap<_, _>>();
    let semantic_packet = |payload: PayloadId| {
        let packet = packets[&payload];
        SemanticPacket {
            source_sequence: payload.0 / node_count,
            flow: packet.flow,
            kind: packet.kind.code(),
            size_bytes: packet.size_bytes,
        }
    };
    let physical_target = |target: NodeId| {
        if post_split {
            match target {
                NodeId(0) => 0,
                NodeId(1) | NodeId(2) => 1,
                NodeId(id) => id - 1,
            }
        } else {
            target.0
        }
    };

    let host_states = result
        .host_states
        .iter()
        .map(|state| NormalizedHostState {
            egress_link: state.egress_link,
            queue: state.queue.iter().copied().map(semantic_packet).collect(),
            in_service: state.in_service.map(semantic_packet),
            tx_ready_pending: state.tx_ready_pending,
            generators: state.generators.clone(),
            next_origin_seq: state.next_origin_seq,
            next_payload_seq: state.next_payload_seq,
            sourced_packets: state.sourced_packets,
            departed_packets: state.departed_packets,
            received_packets: state.received_packets,
        })
        .collect();
    let mut queues = result
        .switch_states
        .iter()
        .flat_map(|state| {
            state.queues.iter().map(|queue| NormalizedQueue {
                physical_switch: state.physical_switch,
                egress_link: queue.egress_link,
                scheduler: queue.scheduler.code(),
                queue_capacity_packets: queue.queue_capacity_packets,
                queue: queue.queue.iter().copied().map(semantic_packet).collect(),
                in_service: queue.in_service.map(semantic_packet),
                tx_ready_pending: queue.tx_ready_pending,
            })
        })
        .collect::<Vec<_>>();
    queues.sort_unstable();

    let departures = result
        .departures
        .iter()
        .map(|departure| SemanticDeparture {
            packet: semantic_packet(departure.payload),
            time_ns: departure.time_ns,
        })
        .collect::<Vec<_>>();
    let arrivals = result
        .arrivals
        .iter()
        .map(|arrival| {
            let disposition = match arrival.disposition {
                days_executor::ArrivalDisposition::Admitted => 0,
                days_executor::ArrivalDisposition::Dropped => 1,
                days_executor::ArrivalDisposition::Delivered => 2,
                days_executor::ArrivalDisposition::Feedback => 3,
            };
            SemanticArrival {
                packet: semantic_packet(arrival.payload),
                time_ns: arrival.time_ns,
                disposition,
            }
        })
        .collect::<Vec<_>>();
    let mut pending_events = BTreeMap::<u64, Vec<NormalizedPendingEvent>>::new();
    for event in &result.pending_events {
        pending_events
            .entry(event.key.time_ns)
            .or_default()
            .push(NormalizedPendingEvent {
                packet: semantic_packet(event.payload),
                time_ns: event.key.time_ns,
                phase: event.key.phase,
                kind: event.kind as u16,
                physical_target: physical_target(event.target),
            });
    }
    for events in pending_events.values_mut() {
        events.sort_unstable();
    }
    let mut resident_packets = result
        .resident_packets
        .iter()
        .map(|packet| semantic_packet(packet.id))
        .collect::<Vec<_>>();
    resident_packets.sort_unstable();
    let mut observed_packets = result
        .observed_packets
        .iter()
        .map(|packet| semantic_packet(packet.id))
        .collect::<Vec<_>>();
    observed_packets.sort_unstable();
    let switch_counters =
        result
            .switch_states
            .iter()
            .fold(BTreeMap::new(), |mut counters, state| {
                let entry = counters
                    .entry(state.physical_switch)
                    .or_insert((0_u64, 0_u64, 0_u64, 0_u64));
                entry.0 += state.next_origin_seq;
                entry.1 += state.arrived_packets;
                entry.2 += state.dropped_packets;
                entry.3 += state.departed_packets;
                counters
            });

    NormalizedPhysicalResult {
        terminal: NormalizedTerminalResult {
            summary: result.summary,
            host_states,
            switch_counters,
            queues,
            resident_packets,
            observed_packets,
            pending_events,
        },
        departures,
        arrivals,
    }
}

fn timestamp_multisets<T>(records: &[T], time_ns: impl Fn(&T) -> u64) -> BTreeMap<u64, Vec<T>>
where
    T: Clone + Ord,
{
    let mut by_time = BTreeMap::<u64, Vec<T>>::new();
    for record in records {
        by_time
            .entry(time_ns(record))
            .or_default()
            .push(record.clone());
    }
    for records in by_time.values_mut() {
        records.sort_unstable();
    }
    by_time
}

fn assert_permutation_aware_physical_equality(
    before: &NormalizedPhysicalResult,
    after: &NormalizedPhysicalResult,
) {
    assert_eq!(after.terminal, before.terminal);

    assert_eq!(
        after
            .departures
            .iter()
            .map(|record| record.time_ns)
            .collect::<Vec<_>>(),
        before
            .departures
            .iter()
            .map(|record| record.time_ns)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        timestamp_multisets(&after.departures, |record| record.time_ns),
        timestamp_multisets(&before.departures, |record| record.time_ns)
    );
    assert_eq!(
        after
            .arrivals
            .iter()
            .map(|record| record.time_ns)
            .collect::<Vec<_>>(),
        before
            .arrivals
            .iter()
            .map(|record| record.time_ns)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        timestamp_multisets(&after.arrivals, |record| record.time_ns),
        timestamp_multisets(&before.arrivals, |record| record.time_ns)
    );

    let departures_by_flow = |records: &[SemanticDeparture]| {
        let mut by_flow = BTreeMap::<FlowId, Vec<SemanticDeparture>>::new();
        for record in records {
            by_flow.entry(record.packet.flow).or_default().push(*record);
        }
        by_flow
    };
    let arrivals_by_flow = |records: &[SemanticArrival]| {
        let mut by_flow = BTreeMap::<FlowId, Vec<SemanticArrival>>::new();
        for record in records {
            by_flow.entry(record.packet.flow).or_default().push(*record);
        }
        by_flow
    };
    assert_eq!(
        departures_by_flow(&after.departures),
        departures_by_flow(&before.departures)
    );
    assert_eq!(
        arrivals_by_flow(&after.arrivals),
        arrivals_by_flow(&before.arrivals)
    );

    let departures_by_packet = |records: &[SemanticDeparture]| {
        let mut by_packet = BTreeMap::<SemanticPacket, Vec<u64>>::new();
        for record in records {
            by_packet
                .entry(record.packet)
                .or_default()
                .push(record.time_ns);
        }
        by_packet
    };
    let arrivals_by_packet = |records: &[SemanticArrival]| {
        let mut by_packet = BTreeMap::<SemanticPacket, Vec<(u64, u8)>>::new();
        for record in records {
            by_packet
                .entry(record.packet)
                .or_default()
                .push((record.time_ns, record.disposition));
        }
        by_packet
    };
    assert_eq!(
        departures_by_packet(&after.departures),
        departures_by_packet(&before.departures)
    );
    assert_eq!(
        arrivals_by_packet(&after.arrivals),
        arrivals_by_packet(&before.arrivals)
    );

    let dropped_packets = |records: &[SemanticArrival]| {
        let mut packets = records
            .iter()
            .filter(|record| record.disposition == 1)
            .map(|record| record.packet)
            .collect::<Vec<_>>();
        packets.sort_unstable();
        packets
    };
    assert_eq!(
        dropped_packets(&after.arrivals),
        dropped_packets(&before.arrivals)
    );
}

fn equal_time_star_image() -> SimulationImage {
    let flow_zero_packet = PayloadId(0);
    let flow_one_packet = PayloadId(5);
    let flow_zero_egress = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(2),
        target: NodeId(3),
        rate_bps: 8_000_000_000,
        propagation_ns: 1,
    };
    let flow_one_egress = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(4),
        rate_bps: 8_000_000_000,
        propagation_ns: 1,
    };
    let source_link = LinkDescriptor {
        id: LinkId(2),
        source: NodeId(0),
        target: NodeId(1),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let sink_zero_egress = LinkDescriptor {
        id: LinkId(3),
        source: NodeId(3),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let sink_one_egress = LinkDescriptor {
        id: LinkId(4),
        source: NodeId(4),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };

    SimulationImage {
        stop_time_ns: 4,
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(2),
                kind: NodeKind::Switch,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(4),
                kind: NodeKind::Host,
                state_slot: 2,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: source_link.id,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 2,
                sourced_packets: 2,
                departed_packets: 2,
                received_packets: 0,
            },
            HostState {
                egress_link: sink_zero_egress.id,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: sink_one_egress.id,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![
            SwitchState {
                physical_switch: 0,
                queues: vec![SwitchQueueState {
                    egress_link: Some(flow_one_egress.id),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1,
                    drop_mark: Default::default(),
                    pfc: None,
                    queue: VecDeque::new(),
                    in_service: Some(flow_one_packet),
                    tx_ready_pending: false,
                }],
                next_origin_seq: 2,
                arrived_packets: 1,
                dropped_packets: 0,
                departed_packets: 0,
            },
            SwitchState {
                physical_switch: 0,
                queues: vec![SwitchQueueState {
                    egress_link: Some(flow_zero_egress.id),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1,
                    drop_mark: Default::default(),
                    pfc: None,
                    queue: VecDeque::new(),
                    in_service: Some(flow_zero_packet),
                    tx_ready_pending: false,
                }],
                next_origin_seq: 2,
                arrived_packets: 1,
                dropped_packets: 0,
                departed_packets: 0,
            },
        ],
        flows: vec![
            FlowDescriptor {
                id: FlowId(0),
                source: NodeId(0),
                target: NodeId(3),
                priority: 0,
                route: vec![source_link.id, flow_zero_egress.id],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(0),
                target: NodeId(4),
                priority: 0,
                route: vec![source_link.id, flow_one_egress.id],
                reverse_route: vec![],
            },
        ],
        initial_packets: vec![
            PacketDescriptor {
                id: flow_zero_packet,
                flow: FlowId(0),
                size_bytes: 1,
                ecn_marked: false,
                kind: PacketKind::Data,
            },
            PacketDescriptor {
                id: flow_one_packet,
                flow: FlowId(1),
                size_bytes: 1,
                ecn_marked: false,
                kind: PacketKind::Data,
            },
        ],
        links: vec![
            flow_zero_egress,
            flow_one_egress,
            source_link,
            sink_zero_egress,
            sink_one_egress,
        ],
        channels: vec![
            RemoteChannel::for_packet_link_to(source_link, NodeId(1), 1).unwrap(),
            RemoteChannel::for_packet_link_to(source_link, NodeId(2), 1).unwrap(),
            RemoteChannel::for_packet_link(flow_zero_egress, 1).unwrap(),
            RemoteChannel::for_packet_link(flow_one_egress, 1).unwrap(),
        ],
        initial_events: vec![
            Event {
                key: EventKey {
                    time_ns: 3,
                    phase: event_phase(EventKind::TxComplete),
                    origin_node: NodeId(1),
                    origin_seq: 0,
                },
                target: NodeId(1),
                kind: EventKind::TxComplete,
                payload: flow_one_packet,
            },
            Event {
                key: EventKey {
                    time_ns: 3,
                    phase: event_phase(EventKind::TxComplete),
                    origin_node: NodeId(2),
                    origin_seq: 0,
                },
                target: NodeId(2),
                kind: EventKind::TxComplete,
                payload: flow_zero_packet,
            },
            Event {
                key: EventKey {
                    time_ns: 4,
                    phase: event_phase(EventKind::RemoteArrival),
                    origin_node: NodeId(1),
                    origin_seq: 1,
                },
                target: NodeId(4),
                kind: EventKind::RemoteArrival,
                payload: flow_one_packet,
            },
            Event {
                key: EventKey {
                    time_ns: 4,
                    phase: event_phase(EventKind::RemoteArrival),
                    origin_node: NodeId(2),
                    origin_seq: 1,
                },
                target: NodeId(3),
                kind: EventKind::RemoteArrival,
                payload: flow_zero_packet,
            },
        ],
        seed: 1,
    }
}

fn pre_split_equal_time_star_image() -> SimulationImage {
    let mut image = equal_time_star_image();
    let post_node_count = image.nodes.len() as u64;
    let pre_node_count = post_node_count - 1;
    let flow_zero_port = image.switch_states.remove(1);
    image.switch_states[0].arrived_packets += flow_zero_port.arrived_packets;
    image.switch_states[0].dropped_packets += flow_zero_port.dropped_packets;
    image.switch_states[0].departed_packets += flow_zero_port.departed_packets;
    image.switch_states[0].queues.extend(flow_zero_port.queues);
    image.switch_states[0].next_origin_seq = 4;
    image.nodes.remove(2);
    for node in &mut image.nodes {
        if node.id.0 > 2 {
            node.id.0 -= 1;
        }
    }
    for link in &mut image.links {
        if link.source == NodeId(2) {
            link.source = NodeId(1);
        } else if link.source.0 > 2 {
            link.source.0 -= 1;
        }
        if link.target.0 > 2 {
            link.target.0 -= 1;
        }
    }
    for flow in &mut image.flows {
        flow.target.0 -= 1;
    }
    let remap_payload =
        |payload: PayloadId| PayloadId((payload.0 / post_node_count) * pre_node_count);
    for packet in &mut image.initial_packets {
        packet.id = remap_payload(packet.id);
    }
    for state in &mut image.switch_states {
        for queue in &mut state.queues {
            queue.queue = queue.queue.iter().copied().map(remap_payload).collect();
            queue.in_service = queue.in_service.map(remap_payload);
        }
    }
    for event in &mut image.initial_events {
        let flow = image.initial_packets[usize::from(event.payload != PayloadId(0))].flow;
        event.payload = remap_payload(event.payload);
        event.key.origin_node = NodeId(1);
        event.key.origin_seq = match (flow, event.kind) {
            (FlowId(0), EventKind::TxComplete) => 0,
            (FlowId(0), EventKind::RemoteArrival) => 1,
            (FlowId(1), EventKind::TxComplete) => 2,
            (FlowId(1), EventKind::RemoteArrival) => 3,
            _ => unreachable!("the star fixture has two in-flight data packets"),
        };
        if event.target == NodeId(2) {
            event.target = NodeId(1);
        } else if event.target.0 > 2 {
            event.target.0 -= 1;
        }
    }
    image.initial_events.sort_unstable_by_key(|event| event.key);
    image.channels = [LinkId(0), LinkId(1), LinkId(2)]
        .map(|link_id| {
            let link = image.links[link_id.0 as usize];
            RemoteChannel::for_packet_link(link, 1).unwrap()
        })
        .to_vec();
    image
}

#[test]
fn port_split_documents_equal_time_cross_origin_permutation() {
    let before_image = pre_split_equal_time_star_image();
    let after_image = equal_time_star_image();
    validate(&after_image, Backend::Scalar).unwrap();
    validate(&after_image, Backend::Cpu { workers: 4 }).unwrap();

    let before = run_scalar_with_observations(&before_image, None, ObservationMode::Full).unwrap();
    let after = run_scalar_with_observations(&after_image, None, ObservationMode::Full).unwrap();
    let round_after =
        run_scalar_rounds_with_observations(&after_image, None, ObservationMode::Full).unwrap();
    assert_eq!(round_after.result, after);
    for config in [
        CpuConfig {
            workers: 1,
            granularity: ChunkGranularity::Static,
            ..CpuConfig::default()
        },
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Static,
            ..CpuConfig::default()
        },
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            straggler_threshold_events: Some(0),
            dedicated_straggler_workers: 1,
            ..CpuConfig::default()
        },
    ] {
        let cpu =
            run_cpu_with_observations(&after_image, None, config, ObservationMode::Full).unwrap();
        assert_eq!(cpu.result, after);
    }

    let before = normalized_physical_result(
        &before,
        false,
        u64::try_from(before_image.nodes.len()).unwrap(),
    );
    let after = normalized_physical_result(
        &after,
        true,
        u64::try_from(after_image.nodes.len()).unwrap(),
    );
    let flow_zero = SemanticPacket {
        source_sequence: 0,
        flow: FlowId(0),
        kind: PacketKind::Data.code(),
        size_bytes: 1,
    };
    let flow_one = SemanticPacket {
        source_sequence: 1,
        flow: FlowId(1),
        kind: PacketKind::Data.code(),
        size_bytes: 1,
    };
    assert_eq!(
        before.departures,
        vec![
            SemanticDeparture {
                packet: flow_zero,
                time_ns: 3,
            },
            SemanticDeparture {
                packet: flow_one,
                time_ns: 3,
            },
        ]
    );
    assert_eq!(
        after.departures,
        vec![
            SemanticDeparture {
                packet: flow_one,
                time_ns: 3,
            },
            SemanticDeparture {
                packet: flow_zero,
                time_ns: 3,
            },
        ]
    );
    assert_eq!(
        before.arrivals,
        vec![
            SemanticArrival {
                packet: flow_zero,
                time_ns: 4,
                disposition: 2,
            },
            SemanticArrival {
                packet: flow_one,
                time_ns: 4,
                disposition: 2,
            },
        ]
    );
    assert_eq!(
        after.arrivals,
        vec![
            SemanticArrival {
                packet: flow_one,
                time_ns: 4,
                disposition: 2,
            },
            SemanticArrival {
                packet: flow_zero,
                time_ns: 4,
                disposition: 2,
            },
        ]
    );
    assert_ne!(after.departures, before.departures);
    assert_ne!(after.arrivals, before.arrivals);
    assert_permutation_aware_physical_equality(&before, &after);
}

#[test]
fn port_lps_take_independent_same_time_tx_ready_continuations() {
    let mut image = heterogeneous_image(1);
    image.flows.truncate(4);
    image.initial_packets.truncate(4);
    image.initial_events.clear();

    let queue_a_in_service = image.initial_packets[0].id;
    let queue_b_in_service = image.initial_packets[1].id;
    let queue_a_waiting = image.initial_packets[2].id;
    let queue_b_waiting = image.initial_packets[3].id;
    image.switch_states[0].queues[0].queue = VecDeque::from([queue_a_waiting]);
    image.switch_states[0].queues[0].in_service = Some(queue_a_in_service);
    image.switch_states[0].queues[0].tx_ready_pending = false;
    image.switch_states[0].queues[0].queue_capacity_packets = 1;
    image.switch_states[1].queues[0].queue = VecDeque::from([queue_b_waiting]);
    image.switch_states[1].queues[0].in_service = Some(queue_b_in_service);
    image.switch_states[1].queues[0].tx_ready_pending = false;
    image.switch_states[1].queues[0].queue_capacity_packets = 1;
    image.switch_states[0].next_origin_seq = 1;
    image.switch_states[1].next_origin_seq = 1;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 10,
                phase: event_phase(EventKind::TxComplete),
                origin_node: NodeId(1),
                origin_seq: 0,
            },
            target: NodeId(1),
            kind: EventKind::TxComplete,
            payload: queue_a_in_service,
        },
        Event {
            key: EventKey {
                time_ns: 10,
                phase: event_phase(EventKind::TxComplete),
                origin_node: NodeId(2),
                origin_seq: 0,
            },
            target: NodeId(2),
            kind: EventKind::TxComplete,
            payload: queue_b_in_service,
        },
    ];

    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 4 }).unwrap();
    let expected = run_scalar_with_observations(&image, Some(11), ObservationMode::Full).unwrap();
    let scalar =
        run_scalar_rounds_with_observations(&image, Some(11), ObservationMode::Full).unwrap();
    let cpu = [ChunkGranularity::Static, ChunkGranularity::Fixed(1)].map(|granularity| {
        run_cpu_with_observations(
            &image,
            Some(11),
            CpuConfig {
                workers: 4,
                granularity,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap()
    });

    assert_eq!(scalar.result, expected);
    assert!(cpu.iter().all(|run| run.result == expected));
    assert_eq!(
        scalar
            .rounds
            .iter()
            .flat_map(|round| &round.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>(),
        2
    );
    assert!(cpu.iter().all(|run| {
        run.rounds
            .iter()
            .flat_map(|round| &round.semantic.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>()
            == 2
    }));

    let origin_sequence = |payload, kind| {
        expected
            .pending_events
            .iter()
            .find(|event| event.payload == payload && event.kind == kind)
            .map(|event| event.key.origin_seq)
            .expect("each waiting packet must have one pending transmission event")
    };
    assert_eq!(
        (
            origin_sequence(queue_a_waiting, EventKind::TxComplete),
            origin_sequence(queue_a_waiting, EventKind::RemoteArrival),
            origin_sequence(queue_b_waiting, EventKind::TxComplete),
            origin_sequence(queue_b_waiting, EventKind::RemoteArrival),
        ),
        (2, 3, 2, 3)
    );
}

#[test]
fn port_decomposition_preserves_pre_split_physical_outcomes() {
    for seed in 0..128 {
        let post_split = heterogeneous_image(seed);
        let pre_split = pre_split_heterogeneous_image(seed);
        for horizon in [None, Some(1 + (seed * 17) % post_split.stop_time_ns)] {
            let before =
                run_scalar_with_observations(&pre_split, horizon, ObservationMode::Full).unwrap();
            let after =
                run_scalar_with_observations(&post_split, horizon, ObservationMode::Full).unwrap();
            let before = normalized_physical_result(
                &before,
                false,
                u64::try_from(pre_split.nodes.len()).unwrap(),
            );
            let after = normalized_physical_result(
                &after,
                true,
                u64::try_from(post_split.nodes.len()).unwrap(),
            );
            assert_permutation_aware_physical_equality(&before, &after);
        }
    }
}

#[test]
fn randomized_small_heterogeneous_images_match_complete_global_state() {
    let mut comparisons = 0;
    for seed in 0..128 {
        for scheduler in scheduler_corpus() {
            let label = scheduler.label();
            let image = heterogeneous_scheduler_image(seed, scheduler);
            validate(&image, Backend::Scalar).unwrap_or_else(|error| {
                panic!("seed {seed}, scheduler {label} scalar validation failed: {error}")
            });
            validate(&image, Backend::Cpu { workers: 1 }).unwrap_or_else(|error| {
                panic!("seed {seed}, scheduler {label} CPU validation failed: {error}")
            });
            assert_equivalent(&image, None);
            comparisons += 1;
            let cut = 1 + (seed * 17) % image.stop_time_ns;
            assert_equivalent(&image, Some(cut));
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, 128 * 3 * 2);
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn production_metal_matches_representative_cartesian_images() {
    // Metal has no CPU worker/partition axes. Sixteen stable seeds at full and partial horizons
    // retain the image/queue/rate/propagation/capacity axes while keeping the feature suite quick.
    for seed in 0..16 {
        for scheduler in scheduler_corpus() {
            let label = scheduler.label();
            let image = heterogeneous_scheduler_image(seed, scheduler);
            validate(&image, Backend::Metal).unwrap_or_else(|error| {
                panic!("seed {seed}, scheduler {label} Metal validation failed: {error}")
            });
            let cut = 1 + (seed * 17) % image.stop_time_ns;
            for horizon in [None, Some(cut)] {
                let expected = run_scalar_with_observations(
                    &image,
                    horizon,
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "seed {seed}, scheduler {label}, horizon {horizon:?} scalar execution failed: {error}"
                    )
                });
                for streams_enabled in [true, false] {
                    let actual = run_metal_with_observations(
                        &image,
                        horizon,
                        MetalConfig {
                            streams_enabled,
                            ..MetalConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "seed {seed}, scheduler {label}, horizon {horizon:?}, streams={streams_enabled} Metal execution failed: {error}"
                        )
                    });
                    assert_device_full_result_eq(
                        &actual.result,
                        &expected,
                        &format!(
                            "seed {seed}, scheduler {label}, horizon {horizon:?}, streams={streams_enabled}"
                        ),
                    );
                }
            }
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
fn production_cuda_matches_representative_scheduler_cartesian_images() {
    for seed in 0..16 {
        for scheduler in scheduler_corpus() {
            let label = scheduler.label();
            let image = heterogeneous_scheduler_image(seed, scheduler);
            validate(&image, Backend::Cuda).unwrap_or_else(|error| {
                panic!("seed {seed}, scheduler {label} CUDA validation failed: {error}")
            });
            let cut = 1 + (seed * 17) % image.stop_time_ns;
            for horizon in [None, Some(cut)] {
                let expected =
                    run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
                for streams_enabled in [true, false] {
                    let actual = run_cuda_with_observations(
                        &image,
                        horizon,
                        CudaConfig {
                            streams_enabled,
                            ..CudaConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "seed {seed}, scheduler {label}, horizon {horizon:?}, streams={streams_enabled} CUDA execution failed: {error}"
                        )
                    });
                    assert_device_full_result_eq(
                        &actual.result,
                        &expected,
                        &format!(
                            "seed {seed}, scheduler {label}, horizon {horizon:?}, streams={streams_enabled}"
                        ),
                    );
                }
            }
        }
    }
}

#[test]
fn cpu_configuration_matrix_preserves_complete_state_at_full_and_partial_horizons() {
    let mut comparisons = 0;
    for seed in 0..128 {
        for scheduler in scheduler_corpus() {
            let label = scheduler.label();
            let image = heterogeneous_scheduler_image(seed, scheduler);
            let full_expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
                .unwrap_or_else(|error| {
                    panic!("seed {seed}, scheduler {label} scalar execution failed: {error}")
                });
            let cut = 1 + (seed * 17) % image.stop_time_ns;
            let partial_expected = run_scalar_with_observations(
                &image,
                Some(cut),
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| {
                panic!(
                    "seed {seed}, scheduler {label}, horizon {cut} scalar execution failed: {error}"
                )
            });
            let horizons = [(None, &full_expected), (Some(cut), &partial_expected)];

            for static_partition in [
                StaticPartitionPolicy::Modulo,
                StaticPartitionPolicy::RouteLoad,
            ] {
                for workers in 1..=4 {
                    for granularity in [
                        ChunkGranularity::Static,
                        ChunkGranularity::Fixed(1),
                        ChunkGranularity::Fixed(3),
                    ] {
                        for straggler_threshold_events in [None, Some(0), Some(3)] {
                            for (horizon, expected) in horizons {
                                let config = CpuConfig {
                                    workers,
                                    granularity,
                                    static_partition,
                                    straggler_threshold_events,
                                    ..CpuConfig::default()
                                };
                                let actual = run_cpu_with_observations(
                                    &image,
                                    horizon,
                                    config,
                                    ObservationMode::Full,
                                )
                                .unwrap_or_else(|error| {
                                    panic!(
                                        "seed {seed}, scheduler {label}, horizon {horizon:?}, \
                                        partition {static_partition:?}, workers {workers}, \
                                        granularity {granularity:?}, threshold \
                                        {straggler_threshold_events:?} failed: {error}"
                                    )
                                });
                                let maximum_owner_batches = u64::try_from(workers * workers)
                                    .expect("worker bound must fit u64");
                                assert!(actual.rounds.iter().all(|round| {
                                    round.owner_batch_messages <= maximum_owner_batches
                                        && round.owner_batch_messages
                                            <= round.semantic.messages_exchanged
                                }));
                                assert_eq!(
                                    &actual.result, expected,
                                    "seed {seed}, scheduler {label}, horizon {horizon:?}, \
                                     partition {static_partition:?}, workers {workers}, \
                                     granularity {granularity:?}, threshold \
                                     {straggler_threshold_events:?}"
                                );
                                comparisons += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 128 * 3 * 2 * 2 * 4 * 3 * 3);
}

fn incast_image(sender_count: usize) -> SimulationImage {
    let hot_port = NodeId(sender_count as u64);
    let sink = NodeId(sender_count as u64 + 1);
    let port_egress = LinkId(sender_count as u64);
    let sink_egress = LinkId(sender_count as u64 + 1);
    let mut nodes = Vec::new();
    let mut host_states = Vec::new();
    let mut flows = Vec::new();
    let mut initial_packets = Vec::new();
    let mut links = Vec::new();
    let mut channels = Vec::new();
    let mut initial_events = Vec::new();

    for sender in 0..sender_count {
        let sender = NodeId(sender as u64);
        let link = LinkDescriptor {
            id: LinkId(sender.0),
            source: sender,
            target: hot_port,
            rate_bps: 8_000_000_000,
            propagation_ns: 9,
        };
        let flow = FlowId(sender.0);
        let payload = PayloadId::from_node_sequence(sender, (sender_count + 2) as u64, 0).unwrap();
        nodes.push(NodeDescriptor {
            id: sender,
            kind: NodeKind::Host,
            state_slot: sender.0 as u32,
        });
        host_states.push(HostState {
            egress_link: link.id,
            queue: VecDeque::new(),
            in_service: Some(payload),
            tx_ready_pending: false,
            generators: vec![],
            tcp_receivers: vec![],
            dcqcn_receivers: vec![],
            next_origin_seq: 2,
            next_payload_seq: 0,
            sourced_packets: 1,
            departed_packets: 0,
            received_packets: 0,
        });
        flows.push(FlowDescriptor {
            id: flow,
            source: sender,
            target: sink,
            priority: 0,
            route: vec![link.id, port_egress],
            reverse_route: vec![],
        });
        initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        });
        links.push(link);
        channels.push(RemoteChannel::for_packet_link(link, 1).unwrap());
        initial_events.push(Event {
            key: EventKey {
                time_ns: 10,
                phase: event_phase(EventKind::TxComplete),
                origin_node: sender,
                origin_seq: 0,
            },
            target: sender,
            kind: EventKind::TxComplete,
            payload,
        });
        initial_events.push(Event {
            key: EventKey {
                time_ns: 19,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: sender,
                origin_seq: 1,
            },
            target: hot_port,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    nodes.push(NodeDescriptor {
        id: hot_port,
        kind: NodeKind::Switch,
        state_slot: 0,
    });
    let port_link = LinkDescriptor {
        id: port_egress,
        source: hot_port,
        target: sink,
        rate_bps: 8_000_000_000,
        propagation_ns: 9,
    };
    links.push(port_link);
    channels.push(RemoteChannel::for_packet_link(port_link, 1).unwrap());
    nodes.push(NodeDescriptor {
        id: sink,
        kind: NodeKind::Host,
        state_slot: sender_count as u32,
    });
    host_states.push(HostState {
        egress_link: sink_egress,
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: vec![],
        tcp_receivers: vec![],
        dcqcn_receivers: vec![],
        next_origin_seq: 0,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    });
    links.push(LinkDescriptor {
        id: sink_egress,
        source: sink,
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    initial_events.sort_unstable_by_key(|event| event.key);

    SimulationImage {
        stop_time_ns: 19,
        nodes,
        host_states,
        switch_states: vec![SwitchState {
            physical_switch: 0,
            queues: vec![SwitchQueueState {
                egress_link: Some(port_egress),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 0,
                drop_mark: Default::default(),
                pfc: None,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        }],
        flows,
        initial_packets,
        links,
        channels,
        initial_events,
        seed: 1,
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn high_lp_only_image(node_count: usize) -> SimulationImage {
    assert!(node_count > 1_024);
    let source = NodeId(node_count as u64 - 2);
    let sink = NodeId(node_count as u64 - 1);
    let forward = LinkId(source.0);
    let sink_egress = LinkId(sink.0);
    let packet =
        PayloadId::from_node_sequence(source, node_count as u64, 0).expect("payload ID must fit");
    let mut nodes = Vec::with_capacity(node_count);
    let mut host_states = Vec::with_capacity(node_count);
    let mut links = Vec::with_capacity(node_count);

    for index in 0..source.0 {
        let node = NodeId(index);
        let egress = LinkId(index);
        nodes.push(NodeDescriptor {
            id: node,
            kind: NodeKind::Host,
            state_slot: index as u32,
        });
        host_states.push(HostState {
            egress_link: egress,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators: vec![],
            tcp_receivers: vec![],
            dcqcn_receivers: vec![],
            next_origin_seq: 0,
            next_payload_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        });
        links.push(LinkDescriptor {
            id: egress,
            source: node,
            target: sink,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        });
    }

    nodes.push(NodeDescriptor {
        id: source,
        kind: NodeKind::Host,
        state_slot: source.0 as u32,
    });
    host_states.push(HostState {
        egress_link: forward,
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: vec![],
        tcp_receivers: vec![],
        dcqcn_receivers: vec![],
        next_origin_seq: 1,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    });
    links.push(LinkDescriptor {
        id: forward,
        source,
        target: sink,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });

    nodes.push(NodeDescriptor {
        id: sink,
        kind: NodeKind::Host,
        state_slot: sink.0 as u32,
    });
    host_states.push(HostState {
        egress_link: sink_egress,
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: vec![],
        tcp_receivers: vec![],
        dcqcn_receivers: vec![],
        next_origin_seq: 0,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    });
    links.push(LinkDescriptor {
        id: sink_egress,
        source: sink,
        target: source,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });

    SimulationImage {
        stop_time_ns: 2,
        nodes,
        host_states,
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: FLOW,
            source,
            target: sink,
            priority: 0,
            route: vec![forward],
            reverse_route: vec![],
        }],
        initial_packets: vec![PacketDescriptor {
            id: packet,
            flow: FLOW,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        }],
        links,
        channels: vec![
            RemoteChannel::for_packet_link(
                LinkDescriptor {
                    id: forward,
                    source,
                    target: sink,
                    rate_bps: 8_000_000_000,
                    propagation_ns: 0,
                },
                1,
            )
            .expect("forward channel delay must fit"),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: source,
                origin_seq: 0,
            },
            target: source,
            kind: EventKind::PacketArrival,
            payload: packet,
        }],
        seed: 29,
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn multiple_high_lp_image(node_count: usize) -> SimulationImage {
    assert!(node_count > 1_026);
    let mut image = high_lp_only_image(node_count);
    let source = NodeId(1_025);
    let sink = NodeId(node_count as u64 - 1);
    let flow = FlowId(1);
    let link = image.links[source.0 as usize];
    let packet =
        PayloadId::from_node_sequence(source, node_count as u64, 0).expect("payload ID must fit");

    image.host_states[source.0 as usize].next_origin_seq = 1;
    image.flows.push(FlowDescriptor {
        id: flow,
        source,
        target: sink,
        priority: 0,
        route: vec![link.id],
        reverse_route: vec![],
    });
    image.initial_packets.push(PacketDescriptor {
        id: packet,
        flow,
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::Data,
    });
    image
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    image
        .channels
        .push(RemoteChannel::for_packet_link(link, 1).expect("forward channel delay must fit"));
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: source,
            origin_seq: 0,
        },
        target: source,
        kind: EventKind::PacketArrival,
        payload: packet,
    });
    image.initial_events.sort_unstable_by_key(|event| event.key);
    image
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn production_metal_horizon_scans_active_lps_beyond_1024_lanes() {
    let image = high_lp_only_image(2_000);
    assert_eq!(image.initial_events.len(), 1);
    assert!(image.initial_events[0].target.0 >= 1_024);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("wide scalar oracle must run");
    assert_eq!(expected.summary.received_packets, 1);
    for streams_enabled in [true, false] {
        let actual = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                streams_enabled,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("wide Metal image must run");
        assert_device_full_result_eq(&actual.result, &expected, "wide Metal image");
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn production_metal_horizon_scans_multiple_active_lps_beyond_1024_lanes() {
    let image = multiple_high_lp_image(2_000);
    assert_eq!(image.initial_events.len(), 2);
    assert!(
        image
            .initial_events
            .iter()
            .all(|event| event.target.0 > 1_024)
    );
    assert_ne!(
        image.initial_events[0].target,
        image.initial_events[1].target
    );
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("wide scalar oracle must run");
    assert_eq!(expected.summary.received_packets, 2);
    for streams_enabled in [true, false] {
        let actual = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                streams_enabled,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("wide Metal image must run");
        assert_device_full_result_eq(&actual.result, &expected, "multiple-wide Metal image");
    }
}

#[test]
fn incast_dominating_lp_is_classified_first_and_routed_to_a_dedicated_worker() {
    let image = incast_image(16);
    let hot_port = NodeId(16);
    let descriptor = image.nodes[hot_port.0 as usize];
    assert_eq!(descriptor.kind, NodeKind::Switch);
    let port_state = &image.switch_states[descriptor.state_slot as usize];
    assert_eq!(port_state.physical_switch, 0);
    assert_eq!(port_state.queues.len(), 1);
    assert_eq!(port_state.queues[0].egress_link, Some(LinkId(16)));
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 4 }).unwrap();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            straggler_threshold_events: Some(4),
            dedicated_straggler_workers: 1,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    assert_eq!(
        actual.rounds[0].partition.stragglers,
        vec![days_executor::LpWorkEstimate {
            node: hot_port,
            estimated_events: 16,
        }]
    );
    assert!(!actual.rounds[0].partition.bulk_chunks.is_empty());
    assert_eq!(
        actual.rounds[0].partition.reserved_straggler_workers,
        vec![0]
    );
    let port = actual.rounds[0]
        .lp_timings
        .iter()
        .find(|timing| timing.node == hot_port)
        .unwrap();
    assert_eq!(port.class, WorkClass::Straggler);
    assert_eq!(port.worker, 0);
    assert_eq!(port.dispatch_order, 0);
    assert!(
        actual.rounds[0]
            .partition
            .reserved_straggler_workers
            .contains(&port.worker)
    );
    assert!(
        actual.rounds[0]
            .lp_timings
            .iter()
            .any(|timing| timing.class == WorkClass::Bulk)
    );
    assert!(
        actual.rounds[0]
            .lp_timings
            .iter()
            .filter(|timing| timing.class == WorkClass::Bulk)
            .all(|timing| !actual.rounds[0]
                .partition
                .reserved_straggler_workers
                .contains(&timing.worker))
    );
    assert!(
        actual.rounds[0]
            .lp_timings
            .iter()
            .filter(|timing| timing.class == WorkClass::Bulk)
            .all(|timing| timing.worker != port.worker
                && timing.started_after_ns >= port.started_after_ns)
    );
}

#[test]
fn static_classification_uses_the_fused_two_message_worker_protocol() {
    let image = incast_image(16);
    let hot_port = NodeId(16);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let workers = 4;
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers,
            granularity: ChunkGranularity::Static,
            straggler_threshold_events: Some(4),
            dedicated_straggler_workers: 1,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    let first = &actual.rounds[0];
    assert_eq!(
        first.partition.stragglers,
        vec![days_executor::LpWorkEstimate {
            node: hot_port,
            estimated_events: 16,
        }]
    );
    assert_eq!(first.partition.reserved_straggler_workers, vec![0]);
    assert!(first.lp_timings.iter().any(|timing| timing.node == hot_port
        && timing.worker == 0
        && timing.class == WorkClass::Straggler));
    assert!(
        first
            .lp_timings
            .iter()
            .filter(|timing| timing.class == WorkClass::Bulk)
            .all(|timing| timing.worker != 0)
    );
    for round in &actual.rounds {
        assert_eq!(round.worker_wake_messages, workers as u64);
        assert_eq!(round.worker_completion_messages, workers as u64);
        assert_eq!(round.chunk_request_messages, 0);
        let peer_messages = (workers * workers) as u64;
        assert_eq!(round.classification_presence_messages, peer_messages);
        assert_eq!(round.classification_work_messages, peer_messages);
        assert_eq!(round.classification_return_messages, peer_messages);
    }
}

#[test]
#[ignore = "manual release-mode classification ablation"]
fn benchmark_fused_static_classification_on_incast() {
    let image = incast_image(16);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for threshold in [None, Some(4)] {
        let config = CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Static,
            straggler_threshold_events: threshold,
            dedicated_straggler_workers: 1,
            ..CpuConfig::default()
        };
        run_cpu_with_observations(&image, None, config, ObservationMode::Full).unwrap();
        let mut samples = Vec::with_capacity(31);
        for _ in 0..31 {
            let started = std::time::Instant::now();
            let actual =
                run_cpu_with_observations(&image, None, config, ObservationMode::Full).unwrap();
            assert_eq!(actual.result, expected);
            samples.push(started.elapsed().as_nanos());
        }
        samples.sort_unstable();
        println!(
            "incast_fan_in=16 workers=4 straggler_threshold={threshold:?} median_ns={}",
            samples[samples.len() / 2]
        );
    }
}

fn port_fault_image() -> SimulationImage {
    let mut image = incast_image(2);
    image
        .initial_events
        .retain(|event| event.kind == EventKind::RemoteArrival);
    for state in &mut image.host_states[..2] {
        state.in_service = None;
        state.departed_packets = 1;
    }
    image
}

fn assert_clean_port_execution(
    image: &SimulationImage,
    expected: &days_executor::RunResult,
    granularity: ChunkGranularity,
    hot_port: NodeId,
) {
    let clean = run_cpu_with_observations(
        image,
        None,
        CpuConfig {
            workers: 2,
            granularity,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("a clean rerun after injected failures must succeed");
    assert_eq!(
        clean.result, *expected,
        "clean rerun granularity={granularity:?}"
    );
    let port_timing = clean.rounds[0]
        .lp_timings
        .iter()
        .find(|timing| timing.node == hot_port)
        .expect("the active port LP must have a timing record");
    assert_eq!(port_timing.worker, 0, "granularity={granularity:?}");
}

#[test]
fn cpu_worker_faults_and_capacity_errors_abort_without_a_partial_result() {
    let image = port_fault_image();
    let hot_port = NodeId(2);
    assert_eq!(image.nodes[hot_port.0 as usize].kind, NodeKind::Switch);
    assert_eq!(image.switch_states[0].queues.len(), 1);
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 2 }).unwrap();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
        assert_clean_port_execution(&image, &expected, granularity, hot_port);
        let error = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                fault_injection: Some(CpuFaultInjection {
                    worker: 0,
                    round: 0,
                    after_events: 1,
                    kind: CpuFaultKind::Failure,
                }),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("an injected worker fault must abort the run");
        assert_eq!(
            error,
            ExecutionError::WorkerFailed {
                worker: 0,
                round: 0,
            },
            "granularity={granularity:?}"
        );

        let capacity_error = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                max_outbox_events_per_lp: Some(0),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("an exhausted LP outbox must abort the run");
        assert_eq!(
            capacity_error,
            ExecutionError::OutboxCapacityExceeded {
                node: hot_port,
                capacity: 0,
            },
            "granularity={granularity:?}"
        );
    }
}

#[test]
fn cpu_panic_root_cause_beats_a_forced_earlier_disconnect() {
    let image = port_fault_image();
    let hot_port = NodeId(2);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
        assert_clean_port_execution(&image, &expected, granularity, hot_port);
        let error = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                fault_injection: Some(CpuFaultInjection {
                    worker: 0,
                    round: 0,
                    after_events: 1,
                    kind: CpuFaultKind::Panic,
                }),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("an injected worker panic must abort the run");

        // The injected-panic wrapper queues a disconnect report first to force the reviewed race.
        assert_eq!(
            error,
            ExecutionError::WorkerPanicked { worker: 0 },
            "granularity={granularity:?}"
        );
    }
}

#[test]
fn cpu_arithmetic_overflow_aborts_with_the_scalar_error() {
    let mut image = port_fault_image();
    let hot_port = NodeId(2);
    image.switch_states[0].next_origin_seq = u64::MAX;
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect_err("the scalar oracle must reject exhausted origin sequences");

    assert_eq!(scalar, ExecutionError::OriginSequenceOverflow(hot_port));
    for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("the CPU executor must reject exhausted origin sequences");
        assert_eq!(cpu, scalar, "granularity={granularity:?}");
    }
}

use std::collections::VecDeque;

use days_executor::{
    Backend, CUBIC_WINDOW_SCALE, ChunkGranularity, CpuConfig, Event, EventFelClass, EventKey,
    EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId,
    GeneratorFeedbackState, GeneratorStatus, HostState, LinkDescriptor, LinkId, NodeDescriptor,
    NodeId, NodeKind, ObservationMode, PacketDescriptor, PacketKind, PayloadId, RemoteChannel,
    ScheduledEmission, SchedulerKind, SimulationImage, StaticPartitionPolicy, SwitchQueueState,
    SwitchState, TcpCongestionControl, TcpDataHeader, TcpGenerator, TcpPhase, TcpReceiverState,
    TcpTransitionInput, event_fel_class, event_phase, run_cpu_with_observations,
    run_scalar_rounds_with_observations, run_scalar_with_observations, validate,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const FORWARD: LinkId = LinkId(0);
const REVERSE: LinkId = LinkId(1);
const FLOW: FlowId = FlowId(0);
const FIRST: PayloadId = PayloadId(0);
const MSS: u64 = 512;
const ACK_BYTES: u64 = 40;

fn tcp_image(control: TcpCongestionControl, total_bytes: u64) -> SimulationImage {
    let first_size = MSS.min(total_bytes);
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let reverse = LinkDescriptor {
        id: REVERSE,
        source: SINK,
        target: SOURCE,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let first = PacketDescriptor {
        id: FIRST,
        flow: FLOW,
        size_bytes: first_size,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    };
    SimulationImage {
        stop_time_ns: 1_000_000_001,
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
                egress_link: FORWARD,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![FlowGeneratorState {
                    flow: FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: 0,
                        payload: FIRST,
                    },
                    rng_state: 7,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Tcp(TcpGenerator::new(
                        total_bytes,
                        MSS,
                        ACK_BYTES,
                        control,
                    )),
                }],
                tcp_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 1,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: REVERSE,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![TcpReceiverState::new(FLOW, ACK_BYTES)],
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
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        }],
        initial_packets: vec![first],
        links: vec![forward, reverse],
        channels: vec![
            RemoteChannel::for_packet_link(forward, first_size).unwrap(),
            RemoteChannel::for_packet_link(reverse, ACK_BYTES).unwrap(),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: FIRST,
        }],
        seed: 1,
    }
}

fn switched_tcp_image(
    control: TcpCongestionControl,
    scheduler: SchedulerKind,
    queue_capacity_packets: u64,
) -> SimulationImage {
    let source = NodeId(0);
    let forward_switch = NodeId(1);
    let sink = NodeId(2);
    let reverse_switch = NodeId(3);
    let links = [
        LinkDescriptor {
            id: LinkId(0),
            source,
            target: forward_switch,
            rate_bps: 100_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(1),
            source: forward_switch,
            target: sink,
            rate_bps: 10_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(2),
            source: sink,
            target: reverse_switch,
            rate_bps: 100_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(3),
            source: reverse_switch,
            target: source,
            rate_bps: 100_000_000_000,
            propagation_ns: 0,
        },
    ];
    let first = PacketDescriptor {
        id: FIRST,
        flow: FLOW,
        size_bytes: MSS,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    };
    SimulationImage {
        stop_time_ns: 5_000_000_000,
        nodes: vec![
            NodeDescriptor {
                id: source,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: forward_switch,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: sink,
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: reverse_switch,
                kind: NodeKind::Switch,
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
                    flow: FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: 0,
                        payload: FIRST,
                    },
                    rng_state: 11,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Tcp(TcpGenerator::new(
                        16 * MSS,
                        MSS,
                        ACK_BYTES,
                        control,
                    )),
                }],
                tcp_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 1,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(2),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![TcpReceiverState::new(FLOW, ACK_BYTES)],
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
                    scheduler,
                    queue_capacity_packets,
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
                physical_switch: 1,
                queues: vec![SwitchQueueState {
                    egress_link: Some(LinkId(3)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 0,
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
        flows: vec![FlowDescriptor {
            id: FLOW,
            source,
            target: sink,
            route: vec![LinkId(0), LinkId(1)],
            reverse_route: vec![LinkId(2), LinkId(3)],
        }],
        initial_packets: vec![first],
        links: links.to_vec(),
        channels: vec![
            RemoteChannel::for_packet_link(links[0], MSS).unwrap(),
            RemoteChannel::for_packet_link(links[1], MSS).unwrap(),
            RemoteChannel::for_packet_link(links[2], ACK_BYTES).unwrap(),
            RemoteChannel::for_packet_link(links[3], ACK_BYTES).unwrap(),
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
            payload: FIRST,
        }],
        seed: 11,
    }
}

#[test]
fn reno_slow_start_doubles_exactly_over_one_window_of_acks() {
    let mut reno = TcpCongestionControl::reno(MSS);
    assert_eq!(reno.cwnd_bytes(MSS), 2 * MSS);

    reno.on_new_ack(MSS, 100, 100, 2 * MSS, 2 * MSS);
    reno.on_new_ack(MSS, 100, 100, MSS, 2 * MSS);

    assert_eq!(reno.phase(), TcpPhase::SlowStart);
    assert_eq!(reno.cwnd_bytes(MSS), 4 * MSS);
}

#[test]
fn reno_single_flow_known_buffer_sawtooth_matches_closed_form() {
    // One saturated flow with a 24-segment BDP and an 8-segment TailDrop buffer has the
    // deterministic loss threshold W = BDP + B = 32 segments. Reno beta=1/2 gives W/2 after
    // recovery, additive increase gives W/2 RTTs per cycle, and the linear time mean is 3W/4.
    const BDP_SEGMENTS: u64 = 24;
    const BUFFER_SEGMENTS: u64 = 8;
    const PEAK_SEGMENTS: u64 = BDP_SEGMENTS + BUFFER_SEGMENTS;
    const LOW_SEGMENTS: u64 = PEAK_SEGMENTS / 2;
    const PERIOD_RTTS: u64 = PEAK_SEGMENTS - LOW_SEGMENTS;
    const MEAN_SEGMENTS: u64 = (PEAK_SEGMENTS + LOW_SEGMENTS) / 2;

    let mut reno = TcpCongestionControl::reno(MSS);
    let TcpCongestionControl::Reno(ref mut state) = reno else {
        unreachable!()
    };
    state.phase = TcpPhase::CongestionAvoidance;
    state.cwnd_bytes = LOW_SEGMENTS * MSS;
    state.ssthresh_bytes = LOW_SEGMENTS * MSS;
    state.ca_credit = 0;

    let mut twice_trapezoid_area = 0_u64;
    let mut acknowledgment = 0_u64;
    for rtt in 0..PERIOD_RTTS {
        let before = reno.cwnd_bytes(MSS);
        for _ in 0..before / MSS {
            acknowledgment += MSS;
            reno.on_new_ack(MSS, rtt, 1, before, acknowledgment);
        }
        let after = reno.cwnd_bytes(MSS);
        assert_eq!(after, before + MSS, "Reno additive increase at RTT {rtt}");
        twice_trapezoid_area += before / MSS + after / MSS;
    }

    assert_eq!(reno.cwnd_bytes(MSS) / MSS, PEAK_SEGMENTS);
    assert_eq!(PERIOD_RTTS, PEAK_SEGMENTS / 2);
    assert_eq!(twice_trapezoid_area / (2 * PERIOD_RTTS), MEAN_SEGMENTS);
    reno.on_fast_retransmit(PEAK_SEGMENTS * MSS, PERIOD_RTTS);
    reno.on_recovery_exit();
    assert_eq!(reno.cwnd_bytes(MSS) / MSS, LOW_SEGMENTS);
    println!(
        "reno_sawtooth bdp={BDP_SEGMENTS} buffer={BUFFER_SEGMENTS} peak={PEAK_SEGMENTS} period_rtts={PERIOD_RTTS} mean={MEAN_SEGMENTS}"
    );
}

#[test]
fn cubic_window_uses_the_documented_integer_lattice() {
    let mut cubic = TcpCongestionControl::cubic(MSS);
    cubic.force_cubic_epoch_for_test(100 * CUBIC_WINDOW_SCALE, 0);

    let first = cubic.cubic_target_for_test(1_000_000_000, 100_000_000);
    let second = cubic.cubic_target_for_test(2_000_000_000, 100_000_000);

    assert!(first > 0);
    assert!(second > first);
    assert!(second < 100 * CUBIC_WINDOW_SCALE);
}

#[test]
fn retransmission_timeout_is_a_phase_one_fallback_heap_event() {
    assert_eq!(event_phase(EventKind::RetransmissionTimeout), 1);
    assert_eq!(
        event_fel_class(EventKind::RetransmissionTimeout),
        EventFelClass::FallbackHeap
    );

    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar rounds should execute the stale timer");
    assert!(
        scalar
            .rounds
            .iter()
            .flat_map(|round| &round.lp_work)
            .any(|work| work.fallback_heap_pushes > 0),
        "the scalar stream FEL must route TCP timers through its fallback heap"
    );

    let cpu = run_cpu_with_observations(
        &image,
        None,
        days_executor::CpuConfig::default(),
        ObservationMode::Full,
    )
    .expect("CPU should execute the stale timer");
    assert!(
        cpu.rounds
            .iter()
            .flat_map(|round| &round.semantic.lp_work)
            .any(|work| work.fallback_heap_pushes > 0),
        "the CPU stream FEL must route TCP timers through its fallback heap"
    );
}

#[test]
fn closed_loop_reno_emits_acks_blocks_and_finishes_with_fresh_payloads() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 4 * MSS);
    validate(&image, Backend::Scalar).expect("TCP scalar image should validate");
    validate(&image, Backend::Cpu { workers: 2 }).expect("TCP CPU image should validate");

    let blocked = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("the source should reach its congestion-window gate");
    assert_eq!(
        blocked.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked
    );

    let run = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("closed-loop Reno should run");
    let generator = run.host_states[0].generators[0];

    assert_eq!(run.summary.received_bytes, 4 * u128::from(MSS));
    assert_eq!(run.summary.feedback_packets, 4);
    assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
    assert_eq!(generator.feedback.outstanding_bytes, 0);
    assert_eq!(generator.feedback.unacknowledged_bytes, 0);
    assert_eq!(generator.feedback.arrivals, 4);
    assert_eq!(
        run.observed_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::TcpData(_)))
            .map(|packet| packet.id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4,
        "every transmission attempt consumes a distinct PayloadId"
    );
}

#[test]
fn scalar_rounds_and_cpu_are_byte_identical_for_reno_and_cubic() {
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        let image = tcp_image(control, 8 * MSS);
        let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
            .expect("scalar rounds should run");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            days_executor::CpuConfig {
                workers: 2,
                ..days_executor::CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CPU should run");
        assert_eq!(cpu.result, scalar.result);
    }
}

#[test]
fn tcp_cartesian_matrix_is_byte_identical_across_queue_and_cpu_axes() {
    let disciplines = [
        ("FIFO", SchedulerKind::Fifo, 0),
        ("TailDrop", SchedulerKind::Fifo, 1),
        ("SP", SchedulerKind::static_priority(vec![1]), 0),
        ("WFQ", SchedulerKind::weighted_fair_queue(vec![1]), 0),
    ];
    let mut comparisons = 0;
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        for (discipline, scheduler, capacity) in &disciplines {
            let image = switched_tcp_image(control, scheduler.clone(), *capacity);
            let partial_horizon = Some(2_000);
            let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
                .unwrap_or_else(|error| panic!("{discipline} scalar full failed: {error}"));
            let partial =
                run_scalar_with_observations(&image, partial_horizon, ObservationMode::Full)
                    .unwrap_or_else(|error| panic!("{discipline} scalar partial failed: {error}"));
            for partition in [
                StaticPartitionPolicy::Modulo,
                StaticPartitionPolicy::RouteLoad,
            ] {
                for workers in [1, 2, 4] {
                    for granularity in [ChunkGranularity::Static, ChunkGranularity::Fixed(1)] {
                        for (horizon, expected) in [(None, &full), (partial_horizon, &partial)] {
                            let actual = run_cpu_with_observations(
                                &image,
                                horizon,
                                CpuConfig {
                                    workers,
                                    granularity,
                                    static_partition: partition,
                                    ..CpuConfig::default()
                                },
                                ObservationMode::Full,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{} {discipline} {partition:?}/{workers}/{granularity:?}/{horizon:?} failed: {error}",
                                    control.label()
                                )
                            });
                            assert_eq!(
                                &actual.result,
                                expected,
                                "{} {discipline} {partition:?}/{workers}/{granularity:?}/{horizon:?}",
                                control.label()
                            );
                            comparisons += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 2 * 4 * 2 * 3 * 2 * 2);
}

#[test]
fn forty_byte_acks_reduce_the_measured_horizon_from_41ns_to_4ns() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    assert_eq!(image.channels[0].min_delay_ns, 41);
    assert_eq!(image.channels[1].min_delay_ns, 4);

    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("round execution should run");
    assert_eq!(run.rounds[0].horizon_advance_ns, 4);
}

#[test]
fn tcp_images_reject_devices_with_the_t24_capability_diagnostic() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    for backend in [Backend::Metal, Backend::Cuda] {
        let error = validate(&image, backend).expect_err("T23 devices must reject TCP");
        assert_eq!(
            error.to_string(),
            format!(
                "TCP Reno generator for flow FlowId(0) requires Scalar or Cpu; {backend} support is T24"
            )
        );
    }
}

#[test]
fn validator_rejects_inconsistent_tcp_transport_state_without_panicking() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.next_sequence = 2 * MSS;
    let PacketKind::TcpData(ref mut header) = image.initial_packets[0].kind else {
        unreachable!()
    };
    header.sequence = 2 * MSS;

    let error = validate(&image, Backend::Scalar).expect_err("invalid TCP state must be rejected");
    assert!(
        error
            .to_string()
            .contains("TCP emitted byte state exceeds total bytes")
    );
}

#[test]
fn validator_rejects_duplicate_tcp_receiver_ownership() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    image.host_states[1]
        .tcp_receivers
        .push(TcpReceiverState::new(FLOW, ACK_BYTES));

    let error = validate(&image, Backend::Scalar).expect_err("duplicate receiver must be rejected");
    assert!(error.to_string().contains("duplicate receiver state"));
}

#[test]
fn scalar_adversarial_trace_covers_tcp_leanguard_transition_classes() {
    let mut records = Vec::new();
    for (index, control) in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ]
    .into_iter()
    .enumerate()
    {
        let image = switched_tcp_image(control, SchedulerKind::Fifo, 1);
        let run = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("adversarial TailDrop trace should run");
        let retransmissions = run
            .observed_packets
            .iter()
            .filter_map(|packet| match packet.kind {
                PacketKind::TcpData(header) if header.retransmission => Some((packet.id, header)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(!retransmissions.is_empty());
        for (attempt, header) in retransmissions {
            assert!(run.observed_packets.iter().any(|packet| {
                packet.id != attempt
                    && matches!(
                        packet.kind,
                        PacketKind::TcpData(original)
                            if !original.retransmission && original.sequence == header.sequence
                    )
            }));
        }
        records.extend(run.tcp_transitions.into_iter().map(|mut record| {
            record.flow = FlowId(index as u64);
            record
        }));
    }

    assert!(
        records
            .iter()
            .any(|record| matches!(record.input, TcpTransitionInput::NewAck { .. }))
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record.input, TcpTransitionInput::DuplicateAck { .. }))
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(record.input, TcpTransitionInput::Timeout { .. }))
    );
    let csv = days_executor::tcp_transitions_csv(&records);
    assert_eq!(csv.lines().count(), records.len() + 1);
    if let Some(path) = std::env::var_os("DAYS_TCP_TRACE_OUT") {
        std::fs::write(path, csv).expect("write requested TCP LeanGuard trace");
    }
}

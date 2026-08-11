#![cfg(feature = "cuda")]

use std::collections::VecDeque;

use days_executor::{
    ArrivalDisposition, Backend, ConstantGenerator, CudaArena, CudaConfig, CudaError, CudaExecutor,
    Event, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId,
    GeneratorFeedbackState, GeneratorStatus, GeneratorTermination, HostState, LinkDescriptor,
    LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor, PacketKind,
    PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind, SimulationImage, SwitchQueueState,
    SwitchState, event_phase, run_cuda_with_observations, run_scalar_with_observations, validate,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const FORWARD: LinkId = LinkId(0);
const REVERSE: LinkId = LinkId(1);
const FLOW: FlowId = FlowId(0);
const FIRST_PACKET: PayloadId = PayloadId(0);

fn generator_image(termination: GeneratorTermination) -> SimulationImage {
    let first_packet = PacketDescriptor {
        id: FIRST_PACKET,
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: PacketKind::Data,
    };
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };

    SimulationImage {
        stop_time_ns: 20,
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
                        payload: FIRST_PACKET,
                    },
                    rng_state: 7,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: 0,
                        interval_ns: 2,
                        packet_size_bytes: 2,
                        termination,
                    }),
                }],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
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
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        }],
        initial_packets: vec![first_packet],
        links: vec![
            forward,
            LinkDescriptor {
                id: REVERSE,
                source: SINK,
                target: SOURCE,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(forward, first_packet.size_bytes)
                .expect("generator link delay must fit"),
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
            payload: FIRST_PACKET,
        }],
        seed: 1,
    }
}

const FIFO_SOURCE: NodeId = NodeId(0);
const FIFO_SWITCH: NodeId = NodeId(1);
const FIFO_SINK: NodeId = NodeId(2);
const FIFO_SOURCE_LINK: LinkId = LinkId(0);
const FIFO_SWITCH_LINK: LinkId = LinkId(1);
const FIFO_SINK_EGRESS: LinkId = LinkId(2);
const FIFO_PAYLOADS: [PayloadId; 5] = [
    PayloadId(0),
    PayloadId(3),
    PayloadId(6),
    PayloadId(9),
    PayloadId(12),
];

fn source_arrival(time_ns: u64, origin_seq: u64, payload: PayloadId) -> Event {
    Event {
        key: EventKey {
            time_ns,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: FIFO_SOURCE,
            origin_seq,
        },
        target: FIFO_SOURCE,
        kind: EventKind::PacketArrival,
        payload,
    }
}

fn fifo_taildrop_image() -> SimulationImage {
    SimulationImage {
        stop_time_ns: 30,
        nodes: vec![
            NodeDescriptor {
                id: FIFO_SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: FIFO_SWITCH,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: FIFO_SINK,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: FIFO_SOURCE_LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 5,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: FIFO_SINK_EGRESS,
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
                egress_link: Some(FIFO_SWITCH_LINK),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 2,
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
        flows: (0..FIFO_PAYLOADS.len())
            .map(|index| FlowDescriptor {
                id: FlowId(index as u64),
                source: FIFO_SOURCE,
                target: FIFO_SINK,
                priority: 0,
                route: vec![FIFO_SOURCE_LINK, FIFO_SWITCH_LINK],
                reverse_route: vec![],
            })
            .collect(),
        initial_packets: FIFO_PAYLOADS
            .into_iter()
            .enumerate()
            .map(|(index, id)| PacketDescriptor {
                id,
                flow: FlowId(index as u64),
                size_bytes: 2,
                ecn_marked: false,
                kind: PacketKind::Data,
            })
            .collect(),
        links: vec![
            LinkDescriptor {
                id: FIFO_SOURCE_LINK,
                source: FIFO_SOURCE,
                target: FIFO_SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
            LinkDescriptor {
                id: FIFO_SWITCH_LINK,
                source: FIFO_SWITCH,
                target: FIFO_SINK,
                rate_bps: 3_000_000_000,
                propagation_ns: 0,
            },
            LinkDescriptor {
                id: FIFO_SINK_EGRESS,
                source: FIFO_SINK,
                target: FIFO_SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel {
                source: FIFO_SOURCE,
                target: FIFO_SWITCH,
                link: FIFO_SOURCE_LINK,
                event_kind: EventKind::RemoteArrival,
                min_delay_ns: 2,
            },
            RemoteChannel {
                source: FIFO_SWITCH,
                target: FIFO_SINK,
                link: FIFO_SWITCH_LINK,
                event_kind: EventKind::RemoteArrival,
                min_delay_ns: 6,
            },
        ],
        initial_events: vec![
            source_arrival(0, 0, FIFO_PAYLOADS[0]),
            source_arrival(0, 1, FIFO_PAYLOADS[1]),
            source_arrival(0, 2, FIFO_PAYLOADS[2]),
            source_arrival(7, 3, FIFO_PAYLOADS[3]),
            source_arrival(8, 4, FIFO_PAYLOADS[4]),
        ],
        seed: 7,
    }
}

fn long_continuation_image() -> SimulationImage {
    const PACKET_COUNT: u64 = 80;
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 99,
    };
    let reverse = LinkDescriptor {
        id: REVERSE,
        source: SINK,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let packets = (0..PACKET_COUNT)
        .map(|sequence| PacketDescriptor {
            id: PayloadId::from_node_sequence(SOURCE, 2, sequence)
                .expect("backlog payload IDs must fit"),
            flow: FLOW,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        })
        .collect::<Vec<_>>();

    SimulationImage {
        stop_time_ns: PACKET_COUNT - 1,
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
                queue: packets.iter().map(|packet| packet.id).collect(),
                in_service: None,
                tx_ready_pending: true,
                generators: vec![FlowGeneratorState {
                    flow: FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Finished,
                        departure_time_ns: 0,
                        payload: packets[0].id,
                    },
                    rng_state: 1,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: 0,
                        interval_ns: 1_000,
                        packet_size_bytes: 1,
                        termination: GeneratorTermination::Bytes(0),
                    }),
                }],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: PACKET_COUNT,
                sourced_packets: PACKET_COUNT,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: REVERSE,
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
            route: vec![FORWARD],
            reverse_route: vec![],
        }],
        initial_packets: packets.clone(),
        links: vec![forward, reverse],
        channels: vec![
            RemoteChannel::for_packet_link(forward, 1).expect("backlog channel delay must fit"),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::TxReady),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::TxReady,
            payload: packets[0].id,
        }],
        seed: 19,
    }
}

fn assert_full_parity(image: &SimulationImage, exclusive_horizon_ns: Option<u64>) {
    validate(image, Backend::Cuda).expect("CUDA fixture must validate");
    let scalar = run_scalar_with_observations(image, exclusive_horizon_ns, ObservationMode::Full)
        .expect("scalar oracle must run");
    assert!(scalar.diagnostics.is_some());
    for streams_enabled in [true, false] {
        let cuda = run_cuda_with_observations(
            image,
            exclusive_horizon_ns,
            CudaConfig {
                streams_enabled,
                ..CudaConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CUDA backend must run");

        assert!(cuda.result.diagnostics.is_none());
        let mut expected = scalar.clone();
        expected.diagnostics = None;
        assert_eq!(cuda.result, expected);
        assert!(cuda.graph_replays > 0);
        assert_eq!(cuda.graph_replays, cuda.wave_boundary_syncs);
    }
}

#[test]
fn cuda_small_semantic_corpus_matches_the_complete_scalar_result() {
    assert_full_parity(&generator_image(GeneratorTermination::Bytes(5)), None);
    assert_full_parity(&generator_image(GeneratorTermination::DurationNs(5)), None);
    let fifo = fifo_taildrop_image();
    let scalar = run_scalar_with_observations(&fifo, Some(27), ObservationMode::Full)
        .expect("scalar FIFO oracle must run");
    assert_eq!(scalar.switch_states[0].dropped_packets, 1);
    assert!(scalar.arrivals.iter().any(|arrival| {
        arrival.payload == FIFO_PAYLOADS[4] && arrival.disposition == ArrivalDisposition::Dropped
    }));
    assert_full_parity(&fifo, Some(27));
}

#[test]
fn cuda_two_runs_are_byte_exact() {
    let image = fifo_taildrop_image();
    let executor = CudaExecutor::new().expect("CUDA executor must initialize");
    let first = executor
        .run_with_observations(
            &image,
            Some(27),
            CudaConfig::default(),
            ObservationMode::Full,
        )
        .expect("first CUDA run must succeed");
    let second = executor
        .run_with_observations(
            &image,
            Some(27),
            CudaConfig::default(),
            ObservationMode::Full,
        )
        .expect("second CUDA run must succeed");

    assert_eq!(first.result, second.result);
    assert_eq!(first.rounds, second.rounds);
    assert_eq!(first.transitions, second.transitions);
}

#[test]
fn cuda_phase_profile_uses_device_timestamps_without_changing_the_result() {
    let image = fifo_taildrop_image();
    let executor = CudaExecutor::new().expect("CUDA executor must initialize");
    let unprofiled = executor
        .run_with_observations(
            &image,
            Some(27),
            CudaConfig::default(),
            ObservationMode::Full,
        )
        .expect("unprofiled CUDA run must succeed");
    let profiled = executor
        .run_profiled_with_observations(
            &image,
            Some(27),
            CudaConfig::default(),
            ObservationMode::Full,
        )
        .expect("profiled CUDA run must succeed");

    assert_eq!(profiled.run.result, unprofiled.result);
    assert_eq!(profiled.run.rounds, unprofiled.rounds);
    assert_eq!(profiled.run.transitions, unprofiled.transitions);
    assert_eq!(
        profiled.profile.recorded_attempts,
        profiled.run.encoded_attempts
    );
    assert!(profiled.profile.round_reset_ns > 0);
    assert!(profiled.profile.round_prepare_ns > 0);
    assert!(profiled.profile.total_kernel_ns() > 0);
    assert!(profiled.profile.total_kernel_ns() <= profiled.run.device_ns);
}

#[test]
fn cuda_continuation_state_crosses_graph_waves_exactly() {
    let image = long_continuation_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar continuation oracle must run");
    let uncapped = run_cuda_with_observations(
        &image,
        None,
        CudaConfig {
            max_transitions_per_lp_per_round: usize::MAX,
            ..CudaConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("uncapped CUDA run must succeed");
    let crossed = run_cuda_with_observations(
        &image,
        None,
        CudaConfig {
            max_transitions_per_lp_per_round: 1,
            attempts_per_graph_wave: 8,
            ..CudaConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("continuation state must survive graph-wave boundaries");

    assert!(scalar.diagnostics.is_some());
    assert!(uncapped.result.diagnostics.is_none());
    assert!(crossed.result.diagnostics.is_none());
    let mut expected = scalar.clone();
    expected.diagnostics = None;
    assert_eq!(uncapped.result, expected);
    assert_eq!(crossed.result, expected);
    assert_eq!(crossed.rounds, uncapped.rounds);
    assert_eq!(crossed.transitions, uncapped.transitions);
    assert!(crossed.continuation_relaunches > 64);
    assert!(crossed.graph_replays > 1);
    assert!(crossed.mid_round_wave_boundary_syncs > 0);
    assert_eq!(crossed.graph_replays, crossed.wave_boundary_syncs);
}

#[test]
fn cuda_device_capacity_fault_is_explicit_and_executor_recovers() {
    let image = generator_image(GeneratorTermination::Bytes(2));
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar recovery oracle must run");
    let executor = CudaExecutor::new().expect("CUDA executor must initialize");

    let error = executor
        .run(
            &image,
            None,
            CudaConfig {
                max_queue_packets_per_lp: Some(0),
                max_capacity_retries: 0,
                ..CudaConfig::default()
            },
        )
        .expect_err("zero queue capacity must fault in the transition kernel");
    assert_eq!(
        error,
        CudaError::CapacityExceeded {
            arena: CudaArena::Queue,
            node: Some(SOURCE),
            flow: None,
            stream: None,
            capacity: 0,
            demand: 1,
        }
    );

    let recovered = executor
        .run_with_observations(
            &image,
            None,
            CudaConfig {
                capacity_caps: days_executor::DeviceCapacityCaps {
                    queue_packets_per_lp: Some(0),
                    ..days_executor::DeviceCapacityCaps::default()
                },
                ..CudaConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("adaptive CUDA execution must recover from a zero queue cap");
    assert!(expected.diagnostics.is_some());
    assert!(recovered.result.diagnostics.is_none());
    let mut expected_without_diagnostics = expected.clone();
    expected_without_diagnostics.diagnostics = None;
    assert_eq!(recovered.result, expected_without_diagnostics);
    assert_eq!(recovered.capacity_retry_trace.len(), 1);
    assert_eq!(recovered.capacity_retry_trace[0].arena, CudaArena::Queue);
    assert_eq!(recovered.capacity_retry_trace[0].node, Some(SOURCE));
    assert_eq!(recovered.capacity_retry_trace[0].flow, None);
    assert_eq!(recovered.capacity_retry_trace[0].stream, None);
    assert_eq!(recovered.capacity_retry_trace[0].capacity, 0);
    assert_eq!(recovered.capacity_retry_trace[0].demand, 1);
    assert_eq!(recovered.capacity_retry_trace[0].grown_capacity, 2);
}

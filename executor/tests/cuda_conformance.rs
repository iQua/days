#![cfg(feature = "cuda")]

use std::collections::VecDeque;
#[cfg(feature = "cuda-test-hooks")]
use std::thread;

use days_executor::{
    ArrivalDisposition, Backend, ConstantGenerator, CudaConfig, Event, EventKey, EventKind,
    FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState,
    GeneratorStatus, GeneratorTermination, HostState, LinkDescriptor, LinkId, NodeDescriptor,
    NodeId, NodeKind, ObservationMode, PacketDescriptor, PacketKind, PayloadId, RemoteChannel,
    ScheduledEmission, SchedulerKind, SimulationImage, SwitchQueueState, SwitchState, event_phase,
    run_cuda, run_cuda_with_observations, run_scalar_with_observations, validate,
};
#[cfg(feature = "cuda-test-hooks")]
use days_executor::{CudaArena, CudaError, CudaExecutor};

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

fn feedback_image() -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.host_states[0].generators[0].next_emission.status = GeneratorStatus::Blocked;
    image.host_states[0].next_origin_seq = 0;
    image.host_states[0].next_payload_seq = 0;
    image.host_states[1].next_origin_seq = 1;
    let feedback = PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: PacketKind::Feedback,
    };
    image.initial_packets = vec![feedback];
    image.initial_events = vec![Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: SINK,
            origin_seq: 0,
        },
        target: SOURCE,
        kind: EventKind::RemoteArrival,
        payload: feedback.id,
    }];
    image.channels.push(
        RemoteChannel::for_packet_link(image.links[1], feedback.size_bytes)
            .expect("feedback delay must fit"),
    );
    image
}

fn blocked_reverse_route_image() -> SimulationImage {
    let mut image = feedback_image();
    let feedback = image.initial_packets[0];
    image.host_states[1].queue.push_back(feedback.id);
    image.host_states[1].tx_ready_pending = true;
    image.initial_events[0] = Event {
        key: EventKey {
            time_ns: 5,
            phase: event_phase(EventKind::TxReady),
            origin_node: SINK,
            origin_seq: 0,
        },
        target: SINK,
        kind: EventKind::TxReady,
        payload: feedback.id,
    };
    image
}

fn rich_mid_state_image() -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(12));
    let FlowGeneratorKind::Constant(mut generator) = image.host_states[0].generators[0].kind else {
        panic!("fixture uses a constant generator")
    };
    generator.interval_ns = 1;
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(generator);

    let feedback = PacketDescriptor {
        id: PayloadId::from_node_sequence(SINK, 2, 0).expect("feedback payload ID must fit"),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: PacketKind::Feedback,
    };
    image.initial_packets.push(feedback);
    image
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    image.host_states[1].queue.push_back(feedback.id);
    image.host_states[1].tx_ready_pending = true;
    image.host_states[1].next_origin_seq = 1;
    image.channels.push(
        RemoteChannel::for_packet_link(image.links[1], feedback.size_bytes)
            .expect("feedback channel delay must fit"),
    );
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 5,
            phase: event_phase(EventKind::TxReady),
            origin_node: SINK,
            origin_seq: 0,
        },
        target: SINK,
        kind: EventKind::TxReady,
        payload: feedback.id,
    });
    image.initial_events.sort_unstable_by_key(|event| event.key);

    let partial = run_scalar_with_observations(&image, Some(5), ObservationMode::Full)
        .expect("partial scalar checkpoint construction must run");
    image.host_states = partial.host_states;
    image.switch_states = partial.switch_states;
    image.initial_packets = partial.resident_packets;
    image
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    image.initial_events = partial.pending_events;
    image
}

fn stopped_generator_checkpoint_image() -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(4));
    image.stop_time_ns = 0;
    let partial = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("stopped scalar checkpoint construction must run");
    image.host_states = partial.host_states;
    image.switch_states = partial.switch_states;
    image.initial_packets = partial.resident_packets;
    image
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    image.initial_events = partial.pending_events;
    image
}

fn multi_producer_target_image(producers: usize) -> SimulationImage {
    assert!(producers > 1);
    let node_count = producers + 1;
    let sink = NodeId(producers as u64);
    let sink_egress = LinkId(producers as u64);
    let mut nodes = Vec::with_capacity(node_count);
    let mut host_states = Vec::with_capacity(node_count);
    let mut flows = Vec::with_capacity(producers);
    let mut packets = Vec::with_capacity(producers);
    let mut links = Vec::with_capacity(node_count);
    let mut channels = Vec::with_capacity(producers);
    let mut initial_events = Vec::with_capacity(producers);

    for index in 0..producers {
        let source = NodeId(index as u64);
        let link = LinkDescriptor {
            id: LinkId(index as u64),
            source,
            target: sink,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        };
        let packet = PacketDescriptor {
            id: PayloadId::from_node_sequence(source, node_count as u64, 0)
                .expect("wide payload ID must fit"),
            flow: FlowId(index as u64),
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        };
        nodes.push(NodeDescriptor {
            id: source,
            kind: NodeKind::Host,
            state_slot: index as u32,
        });
        host_states.push(HostState {
            egress_link: link.id,
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
        flows.push(FlowDescriptor {
            id: packet.flow,
            source,
            target: sink,
            priority: 0,
            route: vec![link.id],
            reverse_route: vec![],
        });
        packets.push(packet);
        links.push(link);
        channels.push(
            RemoteChannel::for_packet_link(link, packet.size_bytes)
                .expect("wide channel delay must fit"),
        );
        initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: source,
                origin_seq: 0,
            },
            target: source,
            kind: EventKind::PacketArrival,
            payload: packet.id,
        });
    }

    nodes.push(NodeDescriptor {
        id: sink,
        kind: NodeKind::Host,
        state_slot: producers as u32,
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

    SimulationImage {
        stop_time_ns: 4,
        nodes,
        host_states,
        switch_states: vec![],
        flows,
        initial_packets: packets,
        links,
        channels,
        initial_events,
        seed: 31,
    }
}

fn fan_in_tail_drop_contention_image() -> SimulationImage {
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
    let packet_zero = PacketDescriptor {
        id: PayloadId(0),
        flow: FlowId(0),
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::Data,
    };
    let packet_one = PacketDescriptor {
        id: PayloadId(1),
        flow: FlowId(1),
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::Data,
    };

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
                egress_link: source_zero_link.id,
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
                egress_link: source_one_link.id,
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
                egress_link: sink_egress.id,
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
                egress_link: Some(switch_link.id),
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
                id: packet_zero.flow,
                source: NodeId(0),
                target: NodeId(3),
                priority: 0,
                route: vec![source_zero_link.id, switch_link.id],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: packet_one.flow,
                source: NodeId(1),
                target: NodeId(3),
                priority: 0,
                route: vec![source_one_link.id, switch_link.id],
                reverse_route: vec![],
            },
        ],
        initial_packets: vec![packet_zero, packet_one],
        links: vec![source_zero_link, source_one_link, switch_link, sink_egress],
        channels: vec![
            RemoteChannel::for_packet_link(source_zero_link, packet_zero.size_bytes)
                .expect("source-zero channel delay must fit"),
            RemoteChannel::for_packet_link(source_one_link, packet_one.size_bytes)
                .expect("source-one channel delay must fit"),
            RemoteChannel::for_packet_link(switch_link, packet_zero.size_bytes)
                .expect("switch channel delay must fit"),
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
                payload: packet_one.id,
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
                payload: packet_zero.id,
            },
        ],
        seed: 37,
    }
}

fn assert_full_parity(image: &SimulationImage, exclusive_horizon_ns: Option<u64>) {
    validate(image, Backend::Cuda).expect("production CUDA fixture must validate");
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
        .unwrap_or_else(|error| {
            panic!("production CUDA backend with streams={streams_enabled} failed: {error}")
        });

        assert!(cuda.result.diagnostics.is_none());
        let mut expected = scalar.clone();
        expected.diagnostics = None;
        assert_eq!(
            cuda.result, expected,
            "CUDA result with streams={streams_enabled} differs from scalar"
        );
    }
}

#[test]
#[ignore = "release-only 257/1,025-producer CUDA geometry matrix"]
fn cuda_block_boundaries_and_geometry_match_full_scalar_result() {
    for producers in [257, 1_025] {
        let image = multi_producer_target_image(producers);
        assert_eq!(image.initial_events.len(), producers);
        assert_eq!(image.channels.len(), producers);
        assert_eq!(image.nodes.len(), producers + 1);
        if producers == 1_025 {
            assert!(image.initial_events.len() > 1_024);
            assert!(image.channels.len() > 1_024);
            assert!(image.nodes.len() > 1_024);
        }
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("wide scalar oracle must run");
        assert_eq!(scalar.summary.received_packets, producers as u128);

        for streams_enabled in [true, false] {
            let mut reference = None;
            for round_threads_per_block in [1, 32, 128, 512, 1_024] {
                let cuda = run_cuda_with_observations(
                    &image,
                    None,
                    CudaConfig {
                        streams_enabled,
                        round_threads_per_block,
                        ..CudaConfig::default()
                    },
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "CUDA producers={producers} streams={streams_enabled} \
                         geometry={round_threads_per_block} failed: {error}"
                    )
                });
                assert_eq!(cuda.result, scalar);
                if let Some(reference) = &reference {
                    assert_eq!(&cuda.result, reference);
                } else {
                    reference = Some(cuda.result);
                }
            }
        }
    }
}

#[test]
fn cuda_fallback_heap_fan_in_tie_order_decides_tail_drop_winner() {
    let image = fan_in_tail_drop_contention_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("fan-in scalar oracle must run");
    assert_eq!(
        scalar
            .arrivals
            .iter()
            .filter(|arrival| arrival.time_ns == 11)
            .map(|arrival| (arrival.payload, arrival.disposition))
            .collect::<Vec<_>>(),
        vec![
            (PayloadId(0), ArrivalDisposition::Admitted),
            (PayloadId(1), ArrivalDisposition::Dropped),
        ]
    );
    assert_eq!(scalar.summary.admitted_packets, 1);
    assert_eq!(scalar.summary.dropped_packets, 1);
    assert_eq!(scalar.summary.received_packets, 1);

    for streams_enabled in [false, true] {
        let cuda = run_cuda_with_observations(
            &image,
            None,
            CudaConfig {
                streams_enabled,
                ..CudaConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| {
            panic!("fan-in CUDA backend with streams={streams_enabled} failed: {error}")
        });

        // Heap mode routes both runtime arrivals through the per-LP fallback heap. Streams mode
        // instead checks that the k-way merge of the two per-channel inboxes chooses the same
        // canonical winner; it is not evidence about the fallback heap.
        assert_eq!(cuda.memory_layout.streams_enabled, streams_enabled);
        if streams_enabled {
            assert!(cuda.memory_layout.channel_stream_event_slots >= 2);
        } else {
            assert_eq!(cuda.memory_layout.channel_stream_event_slots, 0);
            assert!(cuda.memory_layout.fallback_heap_event_slots >= 2);
        }
        assert_eq!(cuda.result, scalar);
    }
}

#[test]
fn cuda_full_path_matches_scalar_from_rich_mid_states() {
    let image = rich_mid_state_image();
    assert!(
        image
            .host_states
            .iter()
            .any(|state| !state.queue.is_empty())
    );
    assert!(
        image
            .host_states
            .iter()
            .any(|state| state.in_service.is_some())
    );
    assert!(image.host_states.iter().any(|state| state.tx_ready_pending));
    assert!(
        image
            .initial_events
            .iter()
            .any(|event| event.kind == EventKind::TxComplete)
    );
    assert!(
        image
            .initial_events
            .iter()
            .any(|event| event.kind == EventKind::RemoteArrival)
    );
    assert!(image.host_states.iter().any(|state| {
        state.generators.iter().any(|generator| {
            generator.packets_emitted > 0
                && generator.next_emission.status == GeneratorStatus::Scheduled
        })
    }));
    let checkpoint = run_cuda(&image, None, CudaConfig::default())
        .expect("streams-enabled checkpoint classification must run");
    assert_eq!(
        checkpoint.memory_layout.checkpoint_fallback_events,
        image.initial_events.len()
    );
    assert_full_parity(&image, None);

    let blocked = blocked_reverse_route_image();
    assert_eq!(
        blocked.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked
    );
    assert_full_parity(&blocked, None);

    let stopped = stopped_generator_checkpoint_image();
    assert_eq!(
        stopped.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Stopped
    );
    assert!(!stopped.initial_events.is_empty());
    assert_full_parity(&stopped, None);
}

#[test]
fn cuda_full_domain_stop_uses_the_one_past_u64_sentinel() {
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.stop_time_ns = u64::MAX;
    image.host_states[0].generators.clear();
    image.host_states[0].next_payload_seq = 0;
    image.initial_events[0] = Event {
        key: EventKey {
            time_ns: u64::MAX - 2,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: SOURCE,
            origin_seq: 0,
        },
        target: SINK,
        kind: EventKind::RemoteArrival,
        payload: FIRST_PACKET,
    };

    assert_full_parity(&image, None);
}

fn terminal_arrival_at_max(stop_time_ns: u64) -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.stop_time_ns = stop_time_ns;
    image.host_states[0].generators.clear();
    image.host_states[0].next_payload_seq = 0;
    image.initial_events[0] = Event {
        key: EventKey {
            time_ns: u64::MAX,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: SOURCE,
            origin_seq: 0,
        },
        target: SINK,
        kind: EventKind::RemoteArrival,
        payload: FIRST_PACKET,
    };
    image
}

#[test]
fn cuda_executes_a_real_event_at_u64_max_through_the_inclusive_stop() {
    let image = terminal_arrival_at_max(u64::MAX);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar must execute the endpoint event");
    assert_eq!(scalar.summary.received_packets, 1);
    assert!(scalar.pending_events.is_empty());
    assert_full_parity(&image, None);
}

#[test]
fn cuda_keeps_a_u64_max_event_pending_above_an_earlier_stop() {
    let image = terminal_arrival_at_max(u64::MAX - 1);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar must stop before the endpoint event");
    assert_eq!(scalar.pending_events, image.initial_events);
    assert_full_parity(&image, None);
}

#[test]
fn cuda_serialization_uses_the_full_u128_numerator() {
    let packet_size = 3_000_000_000;
    let mut image = generator_image(GeneratorTermination::Bytes(packet_size));
    image.stop_time_ns = packet_size + 1;
    image.initial_packets[0].size_bytes = packet_size;
    let FlowGeneratorKind::Constant(mut constant) = image.host_states[0].generators[0].kind else {
        panic!("fixture uses a constant generator")
    };
    constant.packet_size_bytes = packet_size;
    constant.termination = GeneratorTermination::Bytes(packet_size);
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(constant);
    image.channels[0] =
        RemoteChannel::for_packet_link(image.links[0], packet_size).expect("delay must fit");

    assert_full_parity(&image, None);
}

#[test]
fn cuda_final_bytes_emission_does_not_compute_an_unused_overflowing_successor() {
    let departure = u64::MAX - 4;
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.stop_time_ns = u64::MAX;
    image.initial_events[0].key.time_ns = departure;
    let generator = &mut image.host_states[0].generators[0];
    generator.next_emission.departure_time_ns = departure;
    let FlowGeneratorKind::Constant(mut constant) = generator.kind else {
        panic!("fixture uses a constant generator")
    };
    constant.first_departure_ns = departure;
    constant.interval_ns = 10;
    generator.kind = FlowGeneratorKind::Constant(constant);

    assert_full_parity(&image, None);
}

#[cfg(feature = "cuda-test-hooks")]
#[test]
fn cuda_device_capacity_faults_are_explicit_and_do_not_poison_the_executor() {
    let image = generator_image(GeneratorTermination::Bytes(4));
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar recovery oracle must run");
    let executor = CudaExecutor::new().expect("CUDA executor must initialize");
    let cases = [
        (
            CudaConfig {
                streams_enabled: false,
                max_fel_events_per_lp: Some(1),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::Fel,
            Some(SOURCE),
            1,
        ),
        (
            CudaConfig {
                max_channel_events_per_stream: Some(0),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::ChannelInbox,
            Some(SINK),
            0,
        ),
        (
            CudaConfig {
                fault_injection: Some(CudaArena::ServiceStream),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::ServiceStream,
            Some(SOURCE),
            0,
        ),
        (
            CudaConfig {
                fault_injection: Some(CudaArena::GeneratorStream),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::GeneratorStream,
            Some(SOURCE),
            0,
        ),
        (
            CudaConfig {
                max_outbox_events: Some(0),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::Outbox,
            None,
            0,
        ),
        (
            CudaConfig {
                max_observations: Some(0),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::ObservedPackets,
            None,
            0,
        ),
        (
            CudaConfig {
                fault_injection: Some(CudaArena::Departures),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::Departures,
            None,
            0,
        ),
        (
            CudaConfig {
                fault_injection: Some(CudaArena::Arrivals),
                ..CudaConfig::default()
            },
            ObservationMode::Full,
            CudaArena::Arrivals,
            None,
            0,
        ),
    ];

    for (config, observation_mode, arena, node, capacity) in cases {
        let error = executor
            .run_with_observations(&image, None, config, observation_mode)
            .unwrap_err();
        assert_eq!(
            error,
            CudaError::CapacityExceeded {
                arena,
                node,
                capacity,
            }
        );
        let recovered = executor
            .run_with_observations(&image, None, CudaConfig::default(), ObservationMode::Full)
            .expect("the same executor must recover after each device capacity fault");
        assert_eq!(recovered.result, expected);
    }
}

#[cfg(feature = "cuda-test-hooks")]
#[test]
fn process_wide_cuda_guard_recovers_after_a_mid_execution_panic() {
    let image = generator_image(GeneratorTermination::Bytes(2));
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar recovery oracle must run");
    let panic_image = image.clone();

    let panic = thread::spawn(move || {
        days_executor::cuda::panic_after_next_execution_for_testing();
        run_cuda_with_observations(
            &panic_image,
            None,
            CudaConfig::default(),
            ObservationMode::Full,
        )
        .expect("the injected CUDA execution must reach the panic boundary");
    })
    .join()
    .expect_err("the guarded CUDA execution must panic");
    assert_eq!(
        panic.downcast_ref::<&'static str>(),
        Some(&"injected panic after CUDA execution")
    );

    let recovered =
        run_cuda_with_observations(&image, None, CudaConfig::default(), ObservationMode::Full)
            .expect("CUDA execution must recover after the guarded panic");
    assert_eq!(recovered.result, expected);
}

#[test]
fn cuda_feedback_arrival_matches_full_scalar_result() {
    assert_full_parity(&feedback_image(), None);
}

#[test]
fn cuda_reverse_route_execution_matches_full_scalar_result() {
    let image = blocked_reverse_route_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("reverse-route scalar oracle must run");
    assert_eq!(scalar.summary.feedback_packets, 1);
    assert_eq!(scalar.host_states[0].generators[0].feedback.arrivals, 1);
    assert_full_parity(&image, None);
}

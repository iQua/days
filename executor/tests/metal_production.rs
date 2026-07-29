#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use std::collections::VecDeque;

use days_executor::{
    ArrivalDisposition, Backend, ConstantGenerator, Event, EventKey, EventKind, FlowDescriptor,
    FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus,
    GeneratorTermination, HostState, LinkDescriptor, LinkId, MetalArena, MetalConfig, MetalError,
    NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor, PacketKind, PayloadId,
    RemoteChannel, ScheduledEmission, SchedulerKind, SimulationImage, SwitchQueueState,
    SwitchState, event_phase, run_metal, run_metal_with_observations, run_scalar_with_observations,
    validate,
};

const GENERATOR_SOURCE: NodeId = NodeId(0);
const GENERATOR_SINK: NodeId = NodeId(1);
const GENERATOR_FORWARD: LinkId = LinkId(0);
const GENERATOR_REVERSE: LinkId = LinkId(1);
const GENERATOR_FLOW: FlowId = FlowId(0);
const GENERATOR_FIRST_PACKET: PayloadId = PayloadId(0);

fn generator_image(termination: GeneratorTermination) -> SimulationImage {
    let first_packet = PacketDescriptor {
        id: GENERATOR_FIRST_PACKET,
        flow: GENERATOR_FLOW,
        size_bytes: 2,
        kind: PacketKind::Data,
    };
    let forward = LinkDescriptor {
        id: GENERATOR_FORWARD,
        source: GENERATOR_SOURCE,
        target: GENERATOR_SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };

    SimulationImage {
        stop_time_ns: 20,
        nodes: vec![
            NodeDescriptor {
                id: GENERATOR_SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: GENERATOR_SINK,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: GENERATOR_FORWARD,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![FlowGeneratorState {
                    flow: GENERATOR_FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: 0,
                        payload: GENERATOR_FIRST_PACKET,
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
                next_origin_seq: 1,
                next_payload_seq: 1,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: GENERATOR_REVERSE,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: GENERATOR_FLOW,
            source: GENERATOR_SOURCE,
            target: GENERATOR_SINK,
            route: vec![GENERATOR_FORWARD],
            reverse_route: vec![GENERATOR_REVERSE],
        }],
        initial_packets: vec![first_packet],
        links: vec![
            forward,
            LinkDescriptor {
                id: GENERATOR_REVERSE,
                source: GENERATOR_SINK,
                target: GENERATOR_SOURCE,
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
                origin_node: GENERATOR_SOURCE,
                origin_seq: 0,
            },
            target: GENERATOR_SOURCE,
            kind: EventKind::PacketArrival,
            payload: GENERATOR_FIRST_PACKET,
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
        flow: GENERATOR_FLOW,
        size_bytes: 2,
        kind: PacketKind::Feedback,
    };
    image.initial_packets = vec![feedback];
    image.initial_events = vec![Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: GENERATOR_SINK,
            origin_seq: 0,
        },
        target: GENERATOR_SOURCE,
        kind: EventKind::RemoteArrival,
        payload: feedback.id,
    }];
    image.channels.push(
        RemoteChannel::for_packet_link(image.links[1], feedback.size_bytes)
            .expect("feedback delay must fit"),
    );
    image
}

fn converging_generators_image() -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(3));
    let second_flow = FlowId(1);
    let second_packet = PacketDescriptor {
        id: PayloadId(2),
        flow: second_flow,
        size_bytes: 1,
        kind: PacketKind::Data,
    };
    image.flows.push(FlowDescriptor {
        id: second_flow,
        source: GENERATOR_SOURCE,
        target: GENERATOR_SINK,
        route: vec![GENERATOR_FORWARD],
        reverse_route: vec![GENERATOR_REVERSE],
    });
    image.initial_packets[0].size_bytes = 1;
    image.initial_packets.push(second_packet);
    let FlowGeneratorKind::Constant(mut first_constant) = image.host_states[0].generators[0].kind;
    first_constant.packet_size_bytes = 1;
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(first_constant);
    image.host_states[0].generators.push(FlowGeneratorState {
        flow: second_flow,
        packets_emitted: 0,
        bytes_emitted: 0,
        next_emission: ScheduledEmission {
            status: GeneratorStatus::Scheduled,
            departure_time_ns: 0,
            payload: second_packet.id,
        },
        rng_state: 11,
        feedback: GeneratorFeedbackState {
            arrivals: 0,
            outstanding_bytes: 0,
            unacknowledged_bytes: 0,
        },
        kind: FlowGeneratorKind::Constant(ConstantGenerator {
            first_departure_ns: 0,
            interval_ns: 2,
            packet_size_bytes: 1,
            termination: GeneratorTermination::Bytes(3),
        }),
    });
    image.host_states[0].next_origin_seq = 2;
    image.host_states[0].next_payload_seq = 2;
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: GENERATOR_SOURCE,
            origin_seq: 1,
        },
        target: GENERATOR_SOURCE,
        kind: EventKind::PacketArrival,
        payload: second_packet.id,
    });
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 1).expect("delay must fit");
    image
}

fn reverse_switch_feedback_image() -> SimulationImage {
    let source = NodeId(0);
    let forward_switch = NodeId(1);
    let sink = NodeId(2);
    let reverse_switch = NodeId(3);
    let links = [
        LinkDescriptor {
            id: LinkId(0),
            source,
            target: forward_switch,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(1),
            source: forward_switch,
            target: sink,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(2),
            source: sink,
            target: reverse_switch,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(3),
            source: reverse_switch,
            target: source,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
    ];
    let packets = [PayloadId(2), PayloadId(6)].map(|id| PacketDescriptor {
        id,
        flow: FlowId(0),
        size_bytes: 1,
        kind: PacketKind::Feedback,
    });

    SimulationImage {
        stop_time_ns: 20,
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
                    flow: FlowId(0),
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Blocked,
                        departure_time_ns: 0,
                        payload: PayloadId(0),
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
                        packet_size_bytes: 1,
                        termination: GeneratorTermination::Bytes(1),
                    }),
                }],
                next_origin_seq: 0,
                next_payload_seq: 0,
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
                next_origin_seq: 2,
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
            SwitchState {
                physical_switch: 0,
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
            id: FlowId(0),
            source,
            target: sink,
            route: vec![LinkId(0), LinkId(1)],
            reverse_route: vec![LinkId(2), LinkId(3)],
        }],
        initial_packets: packets.to_vec(),
        links: links.to_vec(),
        channels: vec![
            RemoteChannel::for_packet_link_to(links[0], forward_switch, 1).unwrap(),
            RemoteChannel::for_packet_link(links[1], 1).unwrap(),
            RemoteChannel::for_packet_link_to(links[2], reverse_switch, 1).unwrap(),
            RemoteChannel::for_packet_link(links[3], 1).unwrap(),
        ],
        initial_events: packets
            .into_iter()
            .enumerate()
            .map(|(sequence, packet)| Event {
                key: EventKey {
                    time_ns: 1,
                    phase: event_phase(EventKind::RemoteArrival),
                    origin_node: sink,
                    origin_seq: sequence as u64,
                },
                target: reverse_switch,
                kind: EventKind::RemoteArrival,
                payload: packet.id,
            })
            .collect(),
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

fn assert_full_parity(image: &SimulationImage, exclusive_horizon_ns: Option<u64>) {
    validate(image, Backend::Metal).expect("production Metal fixture must validate");
    let scalar = run_scalar_with_observations(image, exclusive_horizon_ns, ObservationMode::Full)
        .expect("scalar oracle must run");
    let metal = run_metal_with_observations(
        image,
        exclusive_horizon_ns,
        MetalConfig::default(),
        ObservationMode::Full,
    )
    .expect("production Metal backend must run");

    assert_eq!(metal.result, scalar);
}

#[test]
fn metal_constant_generator_bytes_matches_full_scalar_result() {
    let image = generator_image(GeneratorTermination::Bytes(5));
    assert_full_parity(&image, None);
}

#[test]
fn metal_constant_generator_duration_matches_full_scalar_result() {
    let image = generator_image(GeneratorTermination::DurationNs(5));
    assert_full_parity(&image, None);
}

#[test]
fn metal_fifo_taildrop_and_single_selection_match_full_scalar_result() {
    let image = fifo_taildrop_image();
    let scalar = run_scalar_with_observations(&image, Some(27), ObservationMode::Full)
        .expect("scalar oracle must run");

    assert_eq!(scalar.switch_states[0].dropped_packets, 1);
    assert_eq!(scalar.switch_states[0].departed_packets, 4);
    assert!(scalar.arrivals.iter().any(|arrival| {
        arrival.payload == FIFO_PAYLOADS[4] && arrival.disposition == ArrivalDisposition::Dropped
    }));
    assert!(
        scalar
            .departures
            .iter()
            .any(|departure| { departure.payload == FIFO_PAYLOADS[2] && departure.time_ns == 20 })
    );
    assert!(
        scalar
            .departures
            .iter()
            .any(|departure| { departure.payload == FIFO_PAYLOADS[3] && departure.time_ns == 26 })
    );

    assert_full_parity(&image, Some(27));
}

#[test]
fn metal_run_to_run_is_exactly_deterministic() {
    let image = fifo_taildrop_image();
    let first = run_metal_with_observations(
        &image,
        Some(27),
        MetalConfig::default(),
        ObservationMode::Full,
    )
    .expect("first production Metal run must succeed");
    let second = run_metal_with_observations(
        &image,
        Some(27),
        MetalConfig::default(),
        ObservationMode::Full,
    )
    .expect("second production Metal run must succeed");

    assert_eq!(first.result, second.result);
}

#[test]
fn metal_reports_explicit_fel_capacity_exhaustion() {
    let image = fifo_taildrop_image();
    let config = MetalConfig {
        max_fel_events_per_lp: Some(0),
        ..MetalConfig::default()
    };
    let error: MetalError = match run_metal(&image, Some(27), config) {
        Ok(_) => panic!("zero FEL capacity must fail explicitly"),
        Err(error) => error,
    };
    let message = error.to_string();

    assert!(
        message.to_ascii_lowercase().contains("fel"),
        "capacity error must identify the FEL, got: {message}"
    );
}

#[test]
fn metal_device_queue_capacity_fault_is_explicit() {
    let image = generator_image(GeneratorTermination::Bytes(2));
    let error = run_metal(
        &image,
        None,
        MetalConfig {
            max_queue_packets_per_lp: Some(0),
            ..MetalConfig::default()
        },
    )
    .expect_err("zero queue arena capacity must fault on device");

    assert_eq!(
        error,
        MetalError::CapacityExceeded {
            arena: MetalArena::Queue,
            node: Some(GENERATOR_SOURCE),
            capacity: 0,
        }
    );
}

#[test]
fn metal_feedback_arrival_matches_full_scalar_result() {
    assert_full_parity(&feedback_image(), None);
}

#[test]
fn metal_preserves_accepted_orphan_packet_snapshots() {
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.host_states[0].generators.clear();
    image.host_states[0].next_origin_seq = 0;
    image.host_states[0].next_payload_seq = 0;
    image.initial_events.clear();

    assert_full_parity(&image, None);
}

#[test]
fn metal_converging_generators_keep_canonical_source_order() {
    assert_full_parity(&converging_generators_image(), None);
}

#[test]
fn metal_source_queue_order_is_independent_of_same_time_event_key_order() {
    let mut image = converging_generators_image();
    image.initial_events[0].key.origin_seq = 1;
    image.initial_events[1].key.origin_seq = 0;
    image.initial_events.sort_unstable_by_key(|event| event.key);

    assert_full_parity(&image, None);
}

#[test]
fn metal_reverse_route_switch_capacity_is_sized_for_feedback() {
    assert_full_parity(&reverse_switch_feedback_image(), None);
}

#[test]
fn metal_full_domain_stop_uses_the_one_past_u64_sentinel() {
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.stop_time_ns = u64::MAX;
    image.host_states[0].generators.clear();
    image.host_states[0].next_payload_seq = 0;
    image.initial_events[0] = Event {
        key: EventKey {
            time_ns: u64::MAX - 2,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: GENERATOR_SOURCE,
            origin_seq: 0,
        },
        target: GENERATOR_SINK,
        kind: EventKind::RemoteArrival,
        payload: GENERATOR_FIRST_PACKET,
    };

    assert_full_parity(&image, None);
}

#[test]
fn metal_serialization_uses_the_full_u128_numerator() {
    let packet_size = 3_000_000_000;
    let mut image = generator_image(GeneratorTermination::Bytes(packet_size));
    image.stop_time_ns = packet_size + 1;
    image.initial_packets[0].size_bytes = packet_size;
    let FlowGeneratorKind::Constant(mut constant) = image.host_states[0].generators[0].kind;
    constant.packet_size_bytes = packet_size;
    constant.termination = GeneratorTermination::Bytes(packet_size);
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(constant);
    image.channels[0] =
        RemoteChannel::for_packet_link(image.links[0], packet_size).expect("delay must fit");

    assert_full_parity(&image, None);
}

#[test]
fn metal_final_bytes_emission_does_not_compute_an_unused_overflowing_successor() {
    let departure = u64::MAX - 4;
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.stop_time_ns = u64::MAX;
    image.initial_events[0].key.time_ns = departure;
    let generator = &mut image.host_states[0].generators[0];
    generator.next_emission.departure_time_ns = departure;
    let FlowGeneratorKind::Constant(mut constant) = generator.kind;
    constant.first_departure_ns = departure;
    constant.interval_ns = 10;
    generator.kind = FlowGeneratorKind::Constant(constant);

    assert_full_parity(&image, None);
}

#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use std::collections::VecDeque;
use std::sync::{Arc, Barrier};
use std::thread;

use days_executor::{
    ArrivalDisposition, Backend, ConstantGenerator, Event, EventKey, EventKind, FlowDescriptor,
    FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus,
    GeneratorTermination, HostState, LinkDescriptor, LinkId, MetalArena, MetalConfig, MetalError,
    MetalExecutor, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor, PacketKind,
    PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind, SimulationImage, SwitchQueueState,
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
        ecn_marked: false,
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
                tcp_receivers: vec![],
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
                tcp_receivers: vec![],
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
            priority: 0,
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
        ecn_marked: false,
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
        ecn_marked: false,
        kind: PacketKind::Data,
    };
    image.flows.push(FlowDescriptor {
        id: second_flow,
        source: GENERATOR_SOURCE,
        target: GENERATOR_SINK,
        priority: 0,
        route: vec![GENERATOR_FORWARD],
        reverse_route: vec![GENERATOR_REVERSE],
    });
    image.initial_packets[0].size_bytes = 1;
    image.initial_packets.push(second_packet);
    let FlowGeneratorKind::Constant(mut first_constant) = image.host_states[0].generators[0].kind
    else {
        panic!("fixture uses a constant generator")
    };
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
        ecn_marked: false,
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
                tcp_receivers: vec![],
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
                tcp_receivers: vec![],
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
                    egress_link: Some(LinkId(3)),
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
            },
        ],
        flows: vec![FlowDescriptor {
            id: FlowId(0),
            source,
            target: sink,
            priority: 0,
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
                tcp_receivers: vec![],
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

fn backlog_drain_image() -> SimulationImage {
    const PACKET_COUNT: u64 = 12;
    let forward = LinkDescriptor {
        id: LinkId(0),
        source: GENERATOR_SOURCE,
        target: GENERATOR_SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 99,
    };
    let reverse = LinkDescriptor {
        id: LinkId(1),
        source: GENERATOR_SINK,
        target: GENERATOR_SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let packets = (0..PACKET_COUNT)
        .map(|sequence| PacketDescriptor {
            id: PayloadId::from_node_sequence(GENERATOR_SOURCE, 2, sequence)
                .expect("backlog payload IDs must fit"),
            flow: GENERATOR_FLOW,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        })
        .collect::<Vec<_>>();

    SimulationImage {
        stop_time_ns: 11,
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
                egress_link: forward.id,
                queue: packets.iter().map(|packet| packet.id).collect(),
                in_service: None,
                tx_ready_pending: true,
                generators: vec![FlowGeneratorState {
                    flow: GENERATOR_FLOW,
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
                next_origin_seq: 1,
                next_payload_seq: PACKET_COUNT,
                sourced_packets: PACKET_COUNT,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: reverse.id,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
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
            priority: 0,
            route: vec![forward.id],
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
                origin_node: GENERATOR_SOURCE,
                origin_seq: 0,
            },
            target: GENERATOR_SOURCE,
            kind: EventKind::TxReady,
            payload: packets[0].id,
        }],
        seed: 19,
    }
}

fn uneven_multi_lp_backlog_image() -> SimulationImage {
    const NODE_COUNT: usize = 6;
    const PACKET_COUNTS: [u64; 3] = [3, 7, 40];
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    let mut host_states = Vec::with_capacity(NODE_COUNT);
    let mut flows = Vec::with_capacity(PACKET_COUNTS.len());
    let mut packets = Vec::new();
    let mut links = Vec::with_capacity(PACKET_COUNTS.len() * 2);
    let mut channels = Vec::with_capacity(PACKET_COUNTS.len());
    let mut initial_events = Vec::with_capacity(PACKET_COUNTS.len());

    for (index, packet_count) in PACKET_COUNTS.into_iter().enumerate() {
        let source = NodeId((index * 2) as u64);
        let sink = NodeId(source.0 + 1);
        let flow = FlowId(index as u64);
        let forward = LinkDescriptor {
            id: LinkId((index * 2) as u64),
            source,
            target: sink,
            rate_bps: 8_000_000_000,
            propagation_ns: 99,
        };
        let reverse = LinkDescriptor {
            id: LinkId(forward.id.0 + 1),
            source: sink,
            target: source,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        };
        let flow_packets = (0..packet_count)
            .map(|sequence| PacketDescriptor {
                id: PayloadId::from_node_sequence(source, NODE_COUNT as u64, sequence)
                    .expect("multi-LP backlog payload IDs must fit"),
                flow,
                size_bytes: 1,
                ecn_marked: false,
                kind: PacketKind::Data,
            })
            .collect::<Vec<_>>();

        nodes.extend([
            NodeDescriptor {
                id: source,
                kind: NodeKind::Host,
                state_slot: (index * 2) as u32,
            },
            NodeDescriptor {
                id: sink,
                kind: NodeKind::Host,
                state_slot: (index * 2 + 1) as u32,
            },
        ]);
        host_states.extend([
            HostState {
                egress_link: forward.id,
                queue: flow_packets.iter().map(|packet| packet.id).collect(),
                in_service: None,
                tx_ready_pending: true,
                generators: vec![FlowGeneratorState {
                    flow,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Finished,
                        departure_time_ns: 0,
                        payload: flow_packets[0].id,
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
                next_origin_seq: 1,
                next_payload_seq: packet_count,
                sourced_packets: packet_count,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: reverse.id,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ]);
        flows.push(FlowDescriptor {
            id: flow,
            source,
            target: sink,
            priority: 0,
            route: vec![forward.id],
            reverse_route: vec![],
        });
        packets.extend_from_slice(&flow_packets);
        links.extend([forward, reverse]);
        channels.push(
            RemoteChannel::for_packet_link(forward, 1)
                .expect("multi-LP backlog channel delay must fit"),
        );
        initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::TxReady),
                origin_node: source,
                origin_seq: 0,
            },
            target: source,
            kind: EventKind::TxReady,
            payload: flow_packets[0].id,
        });
    }
    packets.sort_unstable_by_key(|packet| packet.id);
    initial_events.sort_unstable_by_key(|event| event.key);

    SimulationImage {
        stop_time_ns: 39,
        nodes,
        host_states,
        switch_states: vec![],
        flows,
        initial_packets: packets,
        links,
        channels,
        initial_events,
        seed: 29,
    }
}

fn long_flight_backlog_image() -> SimulationImage {
    const PACKET_COUNT: u64 = 12;
    const SOURCE: NodeId = NodeId(0);
    const SWITCH: NodeId = NodeId(1);
    const SINK: NodeId = NodeId(2);
    const INGRESS: LinkId = LinkId(0);
    const LONG_EGRESS: LinkId = LinkId(1);
    const SINK_EGRESS: LinkId = LinkId(2);
    const FLOW: FlowId = FlowId(0);

    let packets = (0..PACKET_COUNT)
        .map(|sequence| PacketDescriptor {
            id: PayloadId::from_node_sequence(SOURCE, 3, sequence)
                .expect("long-flight payload IDs must fit"),
            flow: FLOW,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        })
        .collect::<Vec<_>>();
    let ingress = LinkDescriptor {
        id: INGRESS,
        source: SOURCE,
        target: SWITCH,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let long_egress = LinkDescriptor {
        id: LONG_EGRESS,
        source: SWITCH,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 100,
    };

    SimulationImage {
        stop_time_ns: 11,
        nodes: vec![
            NodeDescriptor {
                id: SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: SWITCH,
                kind: NodeKind::Switch,
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
                egress_link: INGRESS,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                next_origin_seq: PACKET_COUNT,
                next_payload_seq: PACKET_COUNT,
                sourced_packets: PACKET_COUNT,
                departed_packets: PACKET_COUNT,
                received_packets: 0,
            },
            HostState {
                egress_link: SINK_EGRESS,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
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
                egress_link: Some(LONG_EGRESS),
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
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![INGRESS, LONG_EGRESS],
            reverse_route: vec![],
        }],
        initial_packets: packets.clone(),
        links: vec![
            ingress,
            long_egress,
            LinkDescriptor {
                id: SINK_EGRESS,
                source: SINK,
                target: SOURCE,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(ingress, 1).expect("ingress channel delay must fit"),
            RemoteChannel::for_packet_link(long_egress, 1)
                .expect("long egress channel delay must fit"),
        ],
        initial_events: packets
            .iter()
            .enumerate()
            .map(|(sequence, packet)| Event {
                key: EventKey {
                    time_ns: sequence as u64,
                    phase: event_phase(EventKind::RemoteArrival),
                    origin_node: SOURCE,
                    origin_seq: sequence as u64,
                },
                target: SWITCH,
                kind: EventKind::RemoteArrival,
                payload: packet.id,
            })
            .collect(),
        seed: 23,
    }
}

fn rich_mid_state_image() -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(12));
    let FlowGeneratorKind::Constant(mut generator) = image.host_states[0].generators[0].kind else {
        panic!("fixture uses a constant generator")
    };
    generator.interval_ns = 1;
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(generator);

    let feedback = PacketDescriptor {
        id: PayloadId::from_node_sequence(GENERATOR_SINK, 2, 0)
            .expect("feedback payload ID must fit"),
        flow: GENERATOR_FLOW,
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
            origin_node: GENERATOR_SINK,
            origin_seq: 0,
        },
        target: GENERATOR_SINK,
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

fn assert_full_parity(image: &SimulationImage, exclusive_horizon_ns: Option<u64>) {
    validate(image, Backend::Metal).expect("production Metal fixture must validate");
    let scalar = run_scalar_with_observations(image, exclusive_horizon_ns, ObservationMode::Full)
        .expect("scalar oracle must run");
    for streams_enabled in [true, false] {
        let metal = run_metal_with_observations(
            image,
            exclusive_horizon_ns,
            MetalConfig {
                streams_enabled,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| {
            panic!("production Metal backend with streams={streams_enabled} failed: {error}")
        });

        assert_eq!(
            metal.result, scalar,
            "Metal result with streams={streams_enabled} differs from scalar"
        );
    }
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
fn metal_equal_rate_paced_source_queue_bound_is_tight() {
    // The fixture emits 64 two-byte packets every 2 ns onto a link whose exact serialization is
    // also 2 ns. PacketArrival precedes the tied TxComplete, so every tie reaches the worst-case
    // queued occupancy of exactly one before TxReady drains it.
    let mut image = generator_image(GeneratorTermination::Bytes(128));
    image.stop_time_ns = 126;
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("equal-rate scalar oracle must run");
    let derived =
        run_metal_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full)
            .expect("derived paced-source capacity must cover every equality tie");
    let exact = run_metal_with_observations(
        &image,
        None,
        MetalConfig {
            max_queue_packets_per_lp: Some(1),
            ..MetalConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("one queued packet is the exact equal-rate source bound");

    assert_eq!(scalar.summary.sourced_packets, 64);
    assert_eq!(scalar.summary.departed_packets, 63);
    assert!(scalar.host_states[0].queue.is_empty());
    assert!(scalar.host_states[0].in_service.is_some());
    assert_eq!(derived.result, scalar);
    assert_eq!(exact.result, scalar);

    let error = run_metal(
        &image,
        None,
        MetalConfig {
            max_queue_packets_per_lp: Some(0),
            ..MetalConfig::default()
        },
    )
    .expect_err("the equality case must reach one queued packet");
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
    for streams_enabled in [true, false] {
        let config = MetalConfig {
            streams_enabled,
            ..MetalConfig::default()
        };
        let first = run_metal_with_observations(&image, Some(27), config, ObservationMode::Full)
            .expect("first production Metal run must succeed");
        let second = run_metal_with_observations(&image, Some(27), config, ObservationMode::Full)
            .expect("second production Metal run must succeed");

        assert_eq!(first.result, second.result);
    }
}

#[test]
fn concurrent_public_api_runs_match_the_scalar_result() {
    const EXECUTORS: usize = 8;

    let image = Arc::new(fifo_taildrop_image());
    let expected = run_scalar_with_observations(&image, Some(27), ObservationMode::Full)
        .expect("scalar oracle must run");
    let executors = (0..EXECUTORS)
        .map(|_| MetalExecutor::new().expect("Metal executor must initialize"))
        .collect::<Vec<_>>();
    let start = Arc::new(Barrier::new(EXECUTORS));
    let threads = executors
        .into_iter()
        .map(|executor| {
            let image = Arc::clone(&image);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                executor.run_with_observations(
                    &image,
                    Some(27),
                    MetalConfig::default(),
                    ObservationMode::Full,
                )
            })
        })
        .collect::<Vec<_>>();

    for thread in threads {
        let actual = thread
            .join()
            .expect("concurrent Metal worker must not panic")
            .expect("concurrent Metal run must succeed");
        assert_eq!(actual.result, expected);
    }
}

#[cfg(feature = "metal-test-hooks")]
#[test]
fn process_wide_guard_recovers_after_a_mid_execution_panic() {
    let image = generator_image(GeneratorTermination::Bytes(2));
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar recovery oracle must run");
    let panic_image = image.clone();

    let panic = thread::spawn(move || {
        days_executor::metal::panic_after_next_execution_for_testing();
        run_metal_with_observations(
            &panic_image,
            None,
            MetalConfig::default(),
            ObservationMode::Full,
        )
        .expect("the injected Metal execution must reach the panic boundary");
    })
    .join()
    .expect_err("the guarded Metal execution must panic");
    assert_eq!(
        panic.downcast_ref::<&'static str>(),
        Some(&"injected panic after Metal execution"),
        "the worker must panic at the guarded post-execution boundary"
    );

    let recovered =
        run_metal_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full)
            .expect("Metal execution must recover after the guarded panic");
    assert_eq!(recovered.result, expected);
}

#[test]
fn metal_phase_profiling_is_opt_in_and_preserves_the_full_result() {
    let image = fifo_taildrop_image();
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    for streams_enabled in [true, false] {
        let config = MetalConfig {
            streams_enabled,
            ..MetalConfig::default()
        };
        let ordinary = executor
            .run_with_observations(&image, Some(27), config, ObservationMode::Full)
            .expect("ordinary production Metal run must succeed");
        let profiled = executor
            .run_with_observations_profiled(&image, Some(27), config, ObservationMode::Full)
            .expect("profiled production Metal run must succeed");

        assert_eq!(profiled.result, ordinary.result);
        assert_eq!(profiled.rounds, ordinary.rounds);
        assert_eq!(profiled.transitions, ordinary.transitions);
        assert_eq!(profiled.encoded_attempts, ordinary.encoded_attempts);
        let required_attempts = ordinary
            .rounds
            .saturating_add(ordinary.continuation_relaunches)
            .saturating_add(1);
        assert!(ordinary.encoded_attempts >= required_attempts);
        assert!(ordinary.encoded_attempts - required_attempts <= 63);
        assert!(ordinary.phase_profile.is_none());
        let profile = profiled
            .phase_profile
            .expect("profiled run must return phase timestamps");
        assert!(profile.estimate_complete);
        assert_eq!(
            profile.useful_attempts,
            profiled.rounds + profiled.continuation_relaunches
        );
        assert!(profile.captured_attempts > profile.useful_attempts);
        let estimated_ns = profile.estimated_total.total_ns();
        assert!(estimated_ns > 0);
        assert!(estimated_ns <= profiled.device_ns.saturating_mul(2));
    }
}

#[test]
fn metal_fel_probe_preserves_outcome_and_counts_real_heap_work() {
    let image = fifo_taildrop_image();
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let expected_diagnostic_error =
        MetalError::Validation("legacy heap FEL diagnostics require streams_enabled=false".into());
    assert_eq!(
        executor
            .run_fel_control_profiled(&image, Some(27), MetalConfig::default())
            .expect_err("the heap-only FEL control must reject stream mode"),
        expected_diagnostic_error
    );
    assert_eq!(
        executor
            .run_fel_probe_profiled(&image, Some(27), MetalConfig::default())
            .expect_err("the heap-only FEL probe must reject stream mode"),
        expected_diagnostic_error
    );
    assert_eq!(
        executor
            .run_merge_fan_in_profiled(&image, Some(27), MetalConfig::default())
            .expect_err("the heap-only merge probe must reject stream mode"),
        MetalError::Validation(
            "legacy heap exchange diagnostics require streams_enabled=false".into()
        )
    );
    let heap_config = MetalConfig {
        streams_enabled: false,
        ..MetalConfig::default()
    };
    let baseline = executor
        .run_profiled(&image, Some(27), heap_config)
        .expect("profiled baseline must run");
    let control = executor
        .run_fel_control_profiled(&image, Some(27), heap_config)
        .expect("profiled FEL matched control must run");
    let probe = executor
        .run_fel_probe_profiled(&image, Some(27), heap_config)
        .expect("profiled FEL probe must run");
    let fan_in = executor
        .run_merge_fan_in_profiled(&image, Some(27), heap_config)
        .expect("profiled merge fan-in characterization must run");

    assert_eq!(probe.run.result, baseline.result);
    assert_eq!(probe.run.rounds, baseline.rounds);
    assert_eq!(probe.run.transitions, baseline.transitions);
    assert_eq!(
        probe.run.continuation_relaunches,
        baseline.continuation_relaunches
    );
    assert_eq!(control.run.result, baseline.result);
    assert_eq!(control.run.rounds, baseline.rounds);
    assert_eq!(control.run.transitions, baseline.transitions);
    assert_eq!(
        control.run.continuation_relaunches,
        baseline.continuation_relaunches
    );
    assert_eq!(fan_in.run.result, baseline.result);
    assert_eq!(fan_in.run.rounds, baseline.rounds);
    assert_eq!(fan_in.run.transitions, baseline.transitions);
    assert_eq!(probe.injected_fel_round_trips, probe.run.transitions);
    assert_eq!(probe.local_fel_pushes, 18);
    assert_eq!(probe.local_fel_pushes, control.local_fel_pushes);
    assert_eq!(fan_in.fan_in.eventful_target_rounds, 9);
    assert_eq!(fan_in.fan_in.active_producer_target_rounds, 9);
    assert_eq!(fan_in.fan_in.remote_events, 9);
    assert_eq!(fan_in.fan_in.maximum_active_fan_in, 1);
    assert_eq!(fan_in.fan_in.first_maximum_fan_in_target, Some(FIFO_SWITCH));
    assert_eq!(fan_in.fan_in.maximum_fan_in_target_count, 2);
    assert!(control.diagnostic_pipeline_creation_ns > 0);
    assert_eq!(probe.diagnostic_pipeline_creation_ns, 0);
    assert_eq!(fan_in.diagnostic_pipeline_creation_ns, 0);

    let decomposition = probe
        .decompose_against(&baseline, &control)
        .expect("complete paired profiles must decompose");
    assert_eq!(decomposition.rounds, baseline.rounds);
    assert_eq!(decomposition.transitions, baseline.transitions);
    assert_eq!(
        decomposition.injected_fel_operations,
        baseline
            .transitions
            .checked_mul(2)
            .expect("small test operation count must fit")
    );
    assert_eq!(
        decomposition.production_drain_fel_operations,
        baseline
            .transitions
            .checked_add(probe.local_fel_pushes)
            .expect("small test operation count must fit")
    );
    assert!(decomposition.baseline_drain_execute_ns > 0);
    assert!(decomposition.matched_control_drain_execute_ns > 0);
    assert!(decomposition.probe_drain_execute_ns > 0);
}

#[test]
fn metal_global_outbox_capacity_faults_identically_in_both_stream_modes() {
    let mut image = uneven_multi_lp_backlog_image();
    for state in image.host_states.iter_mut().step_by(2) {
        state.queue.truncate(1);
        state.sourced_packets = 1;
        state.next_payload_seq = 1;
    }
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar multi-producer recovery oracle must run");
    let executor = MetalExecutor::new().expect("Metal executor must initialize");

    for streams_enabled in [false, true] {
        let error = executor
            .run(
                &image,
                None,
                MetalConfig {
                    streams_enabled,
                    max_outbox_events: Some(2),
                    ..MetalConfig::default()
                },
            )
            .expect_err("three producers must exceed the two-record global outbox");
        assert_eq!(
            error,
            MetalError::CapacityExceeded {
                arena: MetalArena::Outbox,
                node: None,
                capacity: 2,
            }
        );
        assert_eq!(
            executor
                .run_with_observations(
                    &image,
                    None,
                    MetalConfig {
                        streams_enabled,
                        ..MetalConfig::default()
                    },
                    ObservationMode::Full,
                )
                .expect("the same executor must recover after the global outbox fault")
                .result,
            expected
        );
    }
}

#[test]
fn metal_device_capacity_faults_are_explicit_and_do_not_poison_the_executor() {
    let image = generator_image(GeneratorTermination::Bytes(4));
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar recovery oracle must run");
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let heap_config = MetalConfig {
        streams_enabled: false,
        ..MetalConfig::default()
    };

    let fel = executor
        .run(
            &image,
            None,
            MetalConfig {
                max_fel_events_per_lp: Some(1),
                ..heap_config
            },
        )
        .expect_err("the second device-generated local child must exhaust the FEL");
    assert_eq!(
        fel,
        MetalError::CapacityExceeded {
            arena: MetalArena::Fel,
            node: Some(GENERATOR_SOURCE),
            capacity: 1,
        }
    );
    assert_eq!(
        executor
            .run_with_observations(&image, None, heap_config, ObservationMode::Full)
            .expect("the same executor must recover after the FEL fault")
            .result,
        expected
    );

    let channel_stream = executor
        .run(
            &image,
            None,
            MetalConfig {
                max_channel_events_per_stream: Some(0),
                ..MetalConfig::default()
            },
        )
        .expect_err("the first remote child must exhaust its channel inbox stream");
    assert_eq!(
        channel_stream,
        MetalError::CapacityExceeded {
            arena: MetalArena::ChannelInbox,
            node: Some(GENERATOR_SINK),
            capacity: 0,
        }
    );
    assert_eq!(
        executor
            .run_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full,)
            .expect("the same executor must recover after the channel-stream fault")
            .result,
        expected
    );

    let outbox = executor
        .run(
            &image,
            None,
            MetalConfig {
                max_outbox_events: Some(0),
                ..MetalConfig::default()
            },
        )
        .expect_err("the first remote device child must exhaust the outbox");
    assert_eq!(
        outbox,
        MetalError::CapacityExceeded {
            arena: MetalArena::Outbox,
            node: None,
            capacity: 0,
        }
    );
    assert_eq!(
        executor
            .run_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full,)
            .expect("the same executor must recover after the outbox fault")
            .result,
        expected
    );

    let observed = executor
        .run_with_observations(
            &image,
            None,
            MetalConfig {
                max_observations: Some(0),
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("the first device observation must exhaust the log");
    assert_eq!(
        observed,
        MetalError::CapacityExceeded {
            arena: MetalArena::ObservedPackets,
            node: None,
            capacity: 0,
        }
    );
    assert_eq!(
        executor
            .run_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full,)
            .expect("the same executor must recover after the observation fault")
            .result,
        expected
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
fn metal_default_capacity_handles_a_service_rate_backlog_drain() {
    let image = backlog_drain_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar backlog oracle must run");
    let metal =
        run_metal_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full)
            .expect("derived Metal capacities must cover the backlog drain");

    assert_eq!(scalar.summary.departed_packets, 11);
    assert!(scalar.host_states[0].in_service.is_some());
    assert_eq!(scalar.pending_events.len(), 13);
    assert_eq!(metal.result, scalar);

    let raised = run_metal_with_observations(
        &image,
        None,
        MetalConfig {
            max_fel_events_per_lp: Some(12),
            max_outbox_events: Some(12),
            ..MetalConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("an explicit capacity must be able to raise the old derived FEL/outbox bounds");
    assert_eq!(raised.result, scalar);
}

#[test]
fn metal_default_fel_capacity_covers_packets_resident_on_a_long_link() {
    let image = long_flight_backlog_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar long-flight backlog oracle must run");
    let metal =
        run_metal_with_observations(&image, None, MetalConfig::default(), ObservationMode::Full)
            .expect("derived Metal FEL capacity must cover accumulated in-flight packets");

    assert_eq!(scalar.summary.departed_packets, 11);
    assert!(scalar.switch_states[0].queues[0].in_service.is_some());
    assert_eq!(scalar.pending_events.len(), 13);
    assert_eq!(metal.result, scalar);
}

#[test]
fn metal_tiny_physical_transition_chunks_relaunch_to_exact_parity() {
    let image = backlog_drain_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar backlog oracle must run");

    for streams_enabled in [true, false] {
        let config = MetalConfig {
            streams_enabled,
            ..MetalConfig::default()
        };
        let default = run_metal_with_observations(&image, None, config, ObservationMode::Full)
            .expect("default Metal chunks must run");
        let uncapped = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                max_transitions_per_lp_per_round: usize::MAX,
                ..config
            },
            ObservationMode::Full,
        )
        .expect("the explicit uncapped Metal run must run");
        let tiny = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                max_transitions_per_lp_per_round: 1,
                ..config
            },
            ObservationMode::Full,
        )
        .expect("one-transition dispatches must continue the same semantic round");

        assert!(default.transitions > 1);
        assert_eq!(default.continuation_relaunches, 0);
        assert_eq!(default.result, scalar);
        assert_eq!(tiny.result, scalar);
        assert!(tiny.continuation_relaunches > 10);
        assert_eq!(tiny.rounds, default.rounds);
        assert_eq!(tiny.transitions, default.transitions);
        assert_eq!(uncapped.result, scalar);
        assert_eq!(uncapped.continuation_relaunches, 0);
        assert_eq!(uncapped.rounds, default.rounds);
        assert_eq!(uncapped.transitions, default.transitions);
    }
}

#[test]
fn metal_continuations_advance_across_uneven_active_lps() {
    let image = uneven_multi_lp_backlog_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar multi-LP backlog oracle must run");
    let uncapped = run_metal_with_observations(
        &image,
        None,
        MetalConfig {
            max_transitions_per_lp_per_round: usize::MAX,
            ..MetalConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("uncapped multi-LP Metal run must run");
    let capped = run_metal_with_observations(
        &image,
        None,
        MetalConfig {
            max_transitions_per_lp_per_round: 4,
            ..MetalConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("small dispatch chunks must advance across active LPs");

    assert_eq!(
        [
            scalar.host_states[0].departed_packets,
            scalar.host_states[2].departed_packets,
            scalar.host_states[4].departed_packets,
        ],
        [3, 7, 39]
    );
    assert_eq!(uncapped.result, scalar);
    assert_eq!(capped.result, scalar);
    assert_eq!(capped.result, uncapped.result);
    assert_eq!(uncapped.continuation_relaunches, 0);
    // The per-LP cap advances all active LPs in the same encoded dispatch, unlike T14's shared
    // serial cap. This fixture still requires many physical continuation launches.
    assert!(capped.continuation_relaunches > 10);
    assert_eq!(capped.rounds, uncapped.rounds);
    assert_eq!(capped.transitions, uncapped.transitions);
    assert_eq!(capped.wave_boundary_syncs, 1);
    assert_eq!(capped.mid_round_wave_boundary_syncs, 0);
}

#[test]
fn metal_continuations_cross_a_bounded_encoding_wave_exactly() {
    let image = uneven_multi_lp_backlog_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar wave-boundary oracle must run");

    for streams_enabled in [true, false] {
        let config = MetalConfig {
            streams_enabled,
            ..MetalConfig::default()
        };
        let uncapped = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                max_transitions_per_lp_per_round: usize::MAX,
                ..config
            },
            ObservationMode::Full,
        )
        .expect("uncapped wave-boundary Metal run must run");
        let crossed = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                max_transitions_per_lp_per_round: 1,
                rounds_per_command_buffer: 1,
                ..config
            },
            ObservationMode::Full,
        )
        .expect("one-pair command buffers must continue across a bounded wave");

        assert_eq!(uncapped.result, scalar);
        assert_eq!(crossed.result, scalar);
        assert_eq!(crossed.result, uncapped.result);
        assert!(crossed.continuation_relaunches > 64);
        assert_eq!(crossed.rounds, uncapped.rounds);
        assert_eq!(crossed.transitions, uncapped.transitions);
        assert_eq!(crossed.wave_boundary_syncs, 2);
        assert_eq!(crossed.mid_round_wave_boundary_syncs, 1);
        assert_eq!(uncapped.wave_boundary_syncs, 1);
        assert_eq!(uncapped.mid_round_wave_boundary_syncs, 0);
    }
}

#[test]
fn metal_full_path_matches_scalar_from_a_rich_mid_state() {
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

    let checkpoint = run_metal(&image, None, MetalConfig::default())
        .expect("streams-enabled checkpoint classification must run");
    assert_eq!(
        checkpoint.memory_layout.checkpoint_fallback_events,
        image.initial_events.len()
    );
    assert_full_parity(&image, None);
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

fn terminal_arrival_at_max(stop_time_ns: u64) -> SimulationImage {
    let mut image = generator_image(GeneratorTermination::Bytes(2));
    image.stop_time_ns = stop_time_ns;
    image.host_states[0].generators.clear();
    image.host_states[0].next_payload_seq = 0;
    image.initial_events[0] = Event {
        key: EventKey {
            time_ns: u64::MAX,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: GENERATOR_SOURCE,
            origin_seq: 0,
        },
        target: GENERATOR_SINK,
        kind: EventKind::RemoteArrival,
        payload: GENERATOR_FIRST_PACKET,
    };
    image
}

#[test]
fn metal_executes_a_real_event_at_u64_max_through_the_inclusive_stop() {
    let image = terminal_arrival_at_max(u64::MAX);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar must execute the endpoint event");

    assert_eq!(scalar.summary.received_packets, 1);
    assert!(scalar.pending_events.is_empty());
    for streams_enabled in [true, false] {
        let metal = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                streams_enabled,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("Metal must execute the endpoint event");
        assert_eq!(metal.result, scalar);
    }
}

#[test]
fn metal_keeps_a_u64_max_event_pending_above_an_earlier_stop() {
    let image = terminal_arrival_at_max(u64::MAX - 1);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar must stop before the endpoint event");

    assert_eq!(scalar.pending_events, image.initial_events);
    for streams_enabled in [true, false] {
        let metal = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                streams_enabled,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("Metal must stop before the endpoint event");
        assert_eq!(metal.result, scalar);
    }
}

#[test]
fn metal_serialization_uses_the_full_u128_numerator() {
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
fn metal_final_bytes_emission_does_not_compute_an_unused_overflowing_successor() {
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

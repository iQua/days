use std::collections::VecDeque;

use days_executor::{
    Backend, ConstantGenerator, Event, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind,
    FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination,
    HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode,
    PacketDescriptor, PacketKind, PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind,
    SimulationImage, SwitchQueueState, SwitchState, event_phase,
    run_scalar_rounds_with_observations, run_scalar_with_observations, validate,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const LINK: LinkId = LinkId(0);
const FLOW: FlowId = FlowId(0);
const PACKET: PayloadId = PayloadId(0);

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
            route: vec![LINK],
            reverse_route: vec![],
        }],
        initial_packets: vec![PacketDescriptor {
            id: PACKET,
            flow: FLOW,
            size_bytes: 1,
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
            round.lp_work.iter().map(|work| work.events_processed).sum()
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
    assert_eq!(result.summary.departed_packets, 1);
    assert_eq!(result.summary.received_packets, 2);
    assert!(result.resident_packets.is_empty());
    assert!(result.pending_events.is_empty());
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
            route: vec![LINK],
            reverse_route: vec![],
        });
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes: 1,
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
            route: vec![LinkId(0)],
            reverse_route: vec![LinkId(1)],
        }],
        initial_packets: vec![PacketDescriptor {
            id: feedback,
            flow: FlowId(0),
            size_bytes: 1,
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
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![SwitchState {
            queues: vec![SwitchQueueState {
                egress_link: Some(LinkId(2)),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 1,
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
                route: vec![LinkId(0), LinkId(2)],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(1),
                target: NodeId(3),
                route: vec![LinkId(1), LinkId(2)],
                reverse_route: vec![],
            },
        ],
        initial_packets: vec![
            PacketDescriptor {
                id: packet_zero,
                flow: FlowId(0),
                size_bytes: 1,
                kind: PacketKind::Data,
            },
            PacketDescriptor {
                id: packet_one,
                flow: FlowId(1),
                size_bytes: 1,
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
}

fn add_idle_switches(image: &mut SimulationImage, count: usize) {
    for _ in 0..count {
        let id = NodeId(image.nodes.len() as u64);
        let state_slot = image.switch_states.len() as u32;
        image.nodes.push(NodeDescriptor {
            id,
            kind: NodeKind::Switch,
            state_slot,
        });
        image.switch_states.push(SwitchState {
            queues: vec![],
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
    let mut large = small.clone();
    add_idle_switches(&mut small, 100);
    add_idle_switches(&mut large, 10_000);

    let small = run_scalar_rounds_with_observations(&small, None, ObservationMode::Full).unwrap();
    let large = run_scalar_rounds_with_observations(&large, None, ObservationMode::Full).unwrap();
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

    assert_eq!(operations(&small), operations(&large));
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
    let node_count = 4 + idle_switches;
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
        target: NodeId(2),
        rate_bps: [1_000_000_000, 2_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(7),
    };
    let alternate_switch_link = LinkDescriptor {
        id: LinkId(2),
        source: NodeId(1),
        target: NodeId(3),
        rate_bps: [1_000_000_000, 4_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(7),
    };
    let return_link = LinkDescriptor {
        id: LinkId(3),
        source: NodeId(2),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let alternate_return_link = LinkDescriptor {
        id: LinkId(4),
        source: NodeId(3),
        target: NodeId(0),
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
            target: if alternate_sink { NodeId(3) } else { NodeId(2) },
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
        if flows[flow.0 as usize].target == NodeId(2) {
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
                kind: NodeKind::Host,
                state_slot: 1,
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
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![SwitchState {
            queues: vec![
                SwitchQueueState {
                    egress_link: Some(LinkId(1)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1 + rng.range(5),
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                },
                SwitchQueueState {
                    egress_link: Some(LinkId(2)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1 + rng.range(5),
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                },
            ],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        }],
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
            RemoteChannel::for_packet_link(source_link, minimum_source_size).unwrap(),
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

#[test]
fn randomized_small_heterogeneous_images_match_complete_global_state() {
    for seed in 0..128 {
        let image = heterogeneous_image(seed);
        validate(&image, Backend::Scalar)
            .unwrap_or_else(|error| panic!("seed {seed} scalar validation failed: {error}"));
        validate(&image, Backend::Cpu { workers: 1 })
            .unwrap_or_else(|error| panic!("seed {seed} CPU validation failed: {error}"));
        assert_equivalent(&image, None);
        let cut = 1 + (seed * 17) % image.stop_time_ns;
        assert_equivalent(&image, Some(cut));
    }
}

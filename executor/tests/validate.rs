use std::collections::VecDeque;

use days_executor::{
    Backend, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState, LinkDescriptor, LinkId,
    NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor, PayloadId, RemoteChannel,
    SchedulerKind, SimulationImage, SwitchQueueState, SwitchState, WfqSchedulerState, event_phase,
    run_scalar_with_observations, validate,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{DropMarkPolicy, EcnThresholdPolicy, QueueDepthUnit};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{MetalConfig, run_metal_with_observations};
use num_bigint::BigUint;
use num_rational::Ratio;

const SOURCE: NodeId = NodeId(0);
const SWITCH: NodeId = NodeId(1);
const SINK: NodeId = NodeId(2);
const SOURCE_LINK: LinkId = LinkId(0);
const SWITCH_LINK: LinkId = LinkId(1);
const SINK_EGRESS: LinkId = LinkId(2);
const FLOW: FlowId = FlowId(0);
const PACKET: PayloadId = PayloadId(0);

fn valid_image() -> SimulationImage {
    SimulationImage {
        stop_time_ns: u64::MAX,
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
                egress_link: SOURCE_LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_payload_seq: 0,
                next_origin_seq: 1,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: SINK_EGRESS,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_payload_seq: 0,
                next_origin_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![SwitchState {
            physical_switch: 0,
            queues: vec![SwitchQueueState {
                egress_link: Some(SWITCH_LINK),
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
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![SOURCE_LINK, SWITCH_LINK],
            reverse_route: vec![],
        }],
        initial_packets: vec![PacketDescriptor {
            id: PACKET,
            flow: FLOW,
            size_bytes: 2,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        }],
        links: vec![
            LinkDescriptor {
                id: SOURCE_LINK,
                source: SOURCE,
                target: SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
            LinkDescriptor {
                id: SWITCH_LINK,
                source: SWITCH,
                target: SINK,
                rate_bps: 3_000_000_000,
                propagation_ns: 0,
            },
            LinkDescriptor {
                id: SINK_EGRESS,
                source: SINK,
                target: SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel {
                source: SOURCE,
                target: SWITCH,
                link: SOURCE_LINK,
                event_kind: EventKind::RemoteArrival,
                min_delay_ns: 2,
            },
            RemoteChannel {
                source: SWITCH,
                target: SINK,
                link: SWITCH_LINK,
                event_kind: EventKind::RemoteArrival,
                min_delay_ns: 6,
            },
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
            payload: PACKET,
        }],
        seed: 7,
    }
}

fn rejection(image: &SimulationImage, backend: Backend) -> String {
    let diagnostic = validate(image, backend)
        .expect_err("mutated image must reject")
        .to_string();
    println!("{diagnostic}");
    diagnostic
}

fn wfq_waiting_image() -> SimulationImage {
    let mut image = valid_image();
    let queue = &mut image.switch_states[0].queues[0];
    queue.queue.push_back(PACKET);
    queue.tx_ready_pending = true;
    let mut state = WfqSchedulerState::new(vec![1]);
    state.finish_times[0] = Ratio::from_integer(BigUint::from(16_u8));
    state.active_packets[0] = 1;
    state
        .packet_finish_times
        .insert(PACKET, Ratio::from_integer(BigUint::from(16_u8)));
    queue.scheduler = SchedulerKind::WeightedFairQueue(state);
    image.switch_states[0].arrived_packets = 1;
    image.switch_states[0].next_origin_seq = 1;
    image.initial_events[0] = Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::TxReady),
            origin_node: SWITCH,
            origin_seq: 0,
        },
        target: SWITCH,
        kind: EventKind::TxReady,
        payload: PACKET,
    };
    image
}

fn wfq_in_service_image() -> SimulationImage {
    let source = wfq_waiting_image();
    let prefix = run_scalar_with_observations(&source, Some(1), ObservationMode::Full)
        .expect("TxReady prefix must run");
    let mut checkpoint = source;
    checkpoint.host_states = prefix.host_states;
    checkpoint.switch_states = prefix.switch_states;
    checkpoint.initial_packets = prefix.resident_packets;
    checkpoint.initial_events = prefix.pending_events;
    checkpoint
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn empty_host_state(egress_link: LinkId) -> HostState {
    HostState {
        egress_link,
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: vec![],
        tcp_receivers: vec![],
        dcqcn_receivers: vec![],
        next_payload_seq: 0,
        next_origin_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn switch_state(
    physical_switch: u64,
    egress_link: LinkId,
    scheduler: SchedulerKind,
    drop_mark: DropMarkPolicy,
) -> SwitchState {
    SwitchState {
        physical_switch,
        queues: vec![SwitchQueueState {
            egress_link: Some(egress_link),
            scheduler,
            queue_capacity_packets: 64,
            drop_mark,
            pfc: None,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
        }],
        next_origin_seq: 0,
        arrived_packets: 0,
        dropped_packets: 0,
        departed_packets: 0,
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn checkpoint_image(source: &SimulationImage, horizon_ns: u64) -> SimulationImage {
    let prefix = run_scalar_with_observations(source, Some(horizon_ns), ObservationMode::Full)
        .expect("resident-waiter prefix must execute");
    let mut checkpoint = source.clone();
    checkpoint.host_states = prefix.host_states;
    checkpoint.switch_states = prefix.switch_states;
    checkpoint.initial_packets = prefix.resident_packets;
    checkpoint.initial_events = prefix.pending_events;
    checkpoint
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn host_resident_waiter_image(
    scheduler: SchedulerKind,
    drop_mark: DropMarkPolicy,
) -> SimulationImage {
    let links = [
        LinkDescriptor {
            id: LinkId(0),
            source: NodeId(0),
            target: NodeId(3),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(1),
            source: NodeId(3),
            target: NodeId(1),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(2),
            source: NodeId(4),
            target: NodeId(2),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(3),
            source: NodeId(1),
            target: NodeId(3),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(4),
            source: NodeId(2),
            target: NodeId(4),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
    ];
    let packets = [
        PacketDescriptor {
            id: PayloadId(0),
            flow: FlowId(0),
            size_bytes: 8,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        },
        PacketDescriptor {
            id: PayloadId(5),
            flow: FlowId(1),
            size_bytes: 1,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        },
    ];
    let mut source = empty_host_state(LinkId(0));
    source.next_payload_seq = 2;
    source.next_origin_seq = 2;
    let image = SimulationImage {
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
                kind: NodeKind::Host,
                state_slot: 2,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(4),
                kind: NodeKind::Switch,
                state_slot: 1,
            },
        ],
        host_states: vec![
            source,
            empty_host_state(LinkId(3)),
            empty_host_state(LinkId(4)),
        ],
        switch_states: vec![
            switch_state(0, LinkId(1), SchedulerKind::Fifo, DropMarkPolicy::TailDrop),
            switch_state(0, LinkId(2), scheduler, drop_mark),
        ],
        flows: vec![
            FlowDescriptor {
                id: FlowId(0),
                source: NodeId(0),
                target: NodeId(1),
                priority: 0,
                route: vec![LinkId(0), LinkId(1)],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(0),
                target: NodeId(2),
                priority: 0,
                route: vec![LinkId(0), LinkId(2)],
                reverse_route: vec![],
            },
        ],
        initial_packets: packets.to_vec(),
        links: links.to_vec(),
        channels: vec![
            RemoteChannel::for_packet_link_to(links[0], NodeId(3), 1).unwrap(),
            RemoteChannel::for_packet_link_to(links[0], NodeId(4), 1).unwrap(),
            RemoteChannel::for_packet_link(links[1], 1).unwrap(),
            RemoteChannel::for_packet_link(links[2], 1).unwrap(),
        ],
        initial_events: packets
            .iter()
            .enumerate()
            .map(|(origin_seq, packet)| Event {
                key: EventKey {
                    time_ns: 0,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(0),
                    origin_seq: origin_seq as u64,
                },
                target: NodeId(0),
                kind: EventKind::PacketArrival,
                payload: packet.id,
            })
            .collect(),
        seed: 26,
    };
    let checkpoint = checkpoint_image(&image, 1);
    assert_eq!(checkpoint.host_states[0].in_service, Some(PayloadId(0)));
    assert_eq!(
        checkpoint.host_states[0].queue,
        VecDeque::from([PayloadId(5)])
    );
    assert!(
        checkpoint
            .initial_events
            .iter()
            .all(|event| { event.key.time_ns == 8 && event.payload == PayloadId(0) })
    );
    checkpoint
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn idle_host_resident_waiter_image(
    scheduler: SchedulerKind,
    drop_mark: DropMarkPolicy,
) -> SimulationImage {
    let mut checkpoint = host_resident_waiter_image(scheduler, drop_mark);
    let state = &mut checkpoint.host_states[0];
    let in_service = state
        .in_service
        .take()
        .expect("host waiter checkpoint has an in-service packet");
    state.queue.push_front(in_service);
    state.tx_ready_pending = true;
    let origin_seq = state.next_origin_seq;
    state.next_origin_seq += 1;
    checkpoint.initial_events = vec![Event {
        key: EventKey {
            time_ns: 8,
            phase: event_phase(EventKind::TxReady),
            origin_node: NodeId(0),
            origin_seq,
        },
        target: NodeId(0),
        kind: EventKind::TxReady,
        payload: in_service,
    }];
    validate(&checkpoint, Backend::Scalar)
        .expect("idle divergent host waiter checkpoint must validate");
    checkpoint
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn switch_resident_waiter_image(
    scheduler: SchedulerKind,
    drop_mark: DropMarkPolicy,
) -> SimulationImage {
    let links = [
        LinkDescriptor {
            id: LinkId(0),
            source: NodeId(0),
            target: NodeId(3),
            rate_bps: 64_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(1),
            source: NodeId(3),
            target: NodeId(4),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(2),
            source: NodeId(4),
            target: NodeId(1),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(3),
            source: NodeId(5),
            target: NodeId(2),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(4),
            source: NodeId(1),
            target: NodeId(4),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(5),
            source: NodeId(2),
            target: NodeId(5),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
    ];
    let packets = [
        PacketDescriptor {
            id: PayloadId(0),
            flow: FlowId(0),
            size_bytes: 8,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        },
        PacketDescriptor {
            id: PayloadId(6),
            flow: FlowId(1),
            size_bytes: 1,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        },
    ];
    let mut source = empty_host_state(LinkId(0));
    source.next_payload_seq = 2;
    source.next_origin_seq = 2;
    let image = SimulationImage {
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
                kind: NodeKind::Host,
                state_slot: 2,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(4),
                kind: NodeKind::Switch,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(5),
                kind: NodeKind::Switch,
                state_slot: 2,
            },
        ],
        host_states: vec![
            source,
            empty_host_state(LinkId(4)),
            empty_host_state(LinkId(5)),
        ],
        switch_states: vec![
            switch_state(0, LinkId(1), SchedulerKind::Fifo, DropMarkPolicy::TailDrop),
            switch_state(1, LinkId(2), SchedulerKind::Fifo, DropMarkPolicy::TailDrop),
            switch_state(1, LinkId(3), scheduler, drop_mark),
        ],
        flows: vec![
            FlowDescriptor {
                id: FlowId(0),
                source: NodeId(0),
                target: NodeId(1),
                priority: 0,
                route: vec![LinkId(0), LinkId(1), LinkId(2)],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(0),
                target: NodeId(2),
                priority: 0,
                route: vec![LinkId(0), LinkId(1), LinkId(3)],
                reverse_route: vec![],
            },
        ],
        initial_packets: packets.to_vec(),
        links: links.to_vec(),
        channels: vec![
            RemoteChannel::for_packet_link(links[0], 1).unwrap(),
            RemoteChannel::for_packet_link_to(links[1], NodeId(4), 1).unwrap(),
            RemoteChannel::for_packet_link_to(links[1], NodeId(5), 1).unwrap(),
            RemoteChannel::for_packet_link(links[2], 1).unwrap(),
            RemoteChannel::for_packet_link(links[3], 1).unwrap(),
        ],
        initial_events: packets
            .iter()
            .enumerate()
            .map(|(origin_seq, packet)| Event {
                key: EventKey {
                    time_ns: 0,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(0),
                    origin_seq: origin_seq as u64,
                },
                target: NodeId(0),
                kind: EventKind::PacketArrival,
                payload: packet.id,
            })
            .collect(),
        seed: 26,
    };
    let checkpoint = checkpoint_image(&image, 3);
    assert_eq!(
        checkpoint.switch_states[0].queues[0].in_service,
        Some(PayloadId(0))
    );
    assert_eq!(
        checkpoint.switch_states[0].queues[0].queue,
        VecDeque::from([PayloadId(6)])
    );
    assert!(checkpoint.initial_events.iter().any(|event| {
        event.key.time_ns == 9
            && event.kind == EventKind::TxComplete
            && event.target == NodeId(3)
            && event.payload == PayloadId(0)
    }));
    checkpoint
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn resident_waiter_cases() -> Vec<(SimulationImage, u64, NodeId, PayloadId, bool)> {
    let ecn = DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
        unit: QueueDepthUnit::Packets,
        capacity: 64,
        threshold: 1,
    });
    let planes = [
        (SchedulerKind::Fifo, ecn, true),
        (
            SchedulerKind::deficit_round_robin(vec![1]),
            DropMarkPolicy::TailDrop,
            false,
        ),
        (
            SchedulerKind::weighted_round_robin(vec![1]),
            DropMarkPolicy::TailDrop,
            false,
        ),
    ];
    let mut cases = Vec::new();
    for (scheduler, drop_mark, aqm) in planes {
        cases.push((
            host_resident_waiter_image(scheduler.clone(), drop_mark),
            8,
            NodeId(4),
            PayloadId(5),
            aqm,
        ));
        cases.push((
            idle_host_resident_waiter_image(scheduler.clone(), drop_mark),
            8,
            NodeId(4),
            PayloadId(5),
            aqm,
        ));
        cases.push((
            switch_resident_waiter_image(scheduler, drop_mark),
            9,
            NodeId(5),
            PayloadId(6),
            aqm,
        ));
    }
    cases
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_summary_observation_matches_reachable_unported_planes() {
    for (image, _, _, _, _) in resident_waiter_cases() {
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Summary)
            .expect("summary scalar suffix must execute");
        let metal = run_metal_with_observations(
            &image,
            None,
            MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("Summary mode must not require unported transition records");

        assert_eq!(metal.result, scalar);
    }
}

#[test]
fn valid_images_and_safe_understated_bounds_are_accepted() {
    let image = valid_image();
    validate(&image, Backend::Scalar).expect("the scalar image should validate");
    validate(&image, Backend::Cpu { workers: 2 }).expect("the parallel image should validate");

    let mut understated = image;
    understated.channels[1].min_delay_ns = 1;
    validate(&understated, Backend::Cpu { workers: 2 })
        .expect("an understated positive bound is conservative");
}

#[test]
fn node_link_and_event_ids_must_be_unique_and_in_range() {
    let mut duplicate_node = valid_image();
    duplicate_node.nodes[1].id = SOURCE;
    assert_eq!(
        rejection(&duplicate_node, Backend::Scalar),
        "duplicate node ID NodeId(0) at descriptor 1"
    );

    let mut node_range = valid_image();
    node_range.nodes[0].id = NodeId(3);
    assert_eq!(
        rejection(&node_range, Backend::Scalar),
        "node ID NodeId(3) at descriptor 0 is outside dense range 0..3"
    );

    let mut duplicate_link = valid_image();
    duplicate_link.links[1].id = SOURCE_LINK;
    assert_eq!(
        rejection(&duplicate_link, Backend::Scalar),
        "duplicate link ID LinkId(0) at descriptor 1"
    );

    let mut link_range = valid_image();
    link_range.links[0].id = LinkId(3);
    assert_eq!(
        rejection(&link_range, Backend::Scalar),
        "link ID LinkId(3) at descriptor 0 is outside dense range 0..3"
    );

    let mut duplicate_event = valid_image();
    duplicate_event
        .initial_events
        .push(duplicate_event.initial_events[0]);
    assert_eq!(
        rejection(&duplicate_event, Backend::Scalar),
        "duplicate event key EventKey { time_ns: 0, phase: 0, origin_node: NodeId(0), origin_seq: 0 } at initial event 1"
    );
}

#[test]
fn descriptor_ids_must_match_their_dense_table_indices() {
    let mut nodes = valid_image();
    nodes.nodes.swap(0, 1);
    assert_eq!(
        rejection(&nodes, Backend::Scalar),
        "node ID NodeId(1) at descriptor 0 does not match dense table index 0"
    );

    let mut links = valid_image();
    links.links.swap(0, 1);
    assert_eq!(
        rejection(&links, Backend::Scalar),
        "link ID LinkId(1) at descriptor 0 does not match dense table index 0"
    );

    let mut flows = valid_image();
    flows.flows.push(FlowDescriptor {
        id: FlowId(1),
        source: SOURCE,
        target: SINK,
        priority: 0,
        route: vec![SOURCE_LINK, SWITCH_LINK],
        reverse_route: vec![],
    });
    flows.flows.swap(0, 1);
    assert_eq!(
        rejection(&flows, Backend::Scalar),
        "flow ID FlowId(1) at descriptor 0 does not match dense table index 0"
    );

    let mut packets = valid_image();
    packets.initial_packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });
    packets.initial_packets.swap(0, 1);
    assert_eq!(
        rejection(&packets, Backend::Scalar),
        "packet ID PayloadId(0) at descriptor 1 does not advance previous packet ID PayloadId(1)"
    );
}

#[test]
fn every_role_state_slot_has_exactly_one_owner() {
    let mut invalid = valid_image();
    invalid.nodes[0].state_slot = 2;
    assert_eq!(
        rejection(&invalid, Backend::Scalar),
        "node NodeId(0) has Host state slot 2, but the arena length is 2"
    );

    let mut duplicate = valid_image();
    duplicate.nodes[2].state_slot = 0;
    assert_eq!(
        rejection(&duplicate, Backend::Scalar),
        "Host state slot 0 is owned by both node NodeId(0) and node NodeId(2)"
    );

    let mut unowned = valid_image();
    unowned.host_states.push(unowned.host_states[1].clone());
    assert_eq!(
        rejection(&unowned, Backend::Scalar),
        "Host state slot 2 has no owner node"
    );
}

#[test]
fn events_must_target_their_owner_and_a_supported_role_handler() {
    let mut unknown = valid_image();
    unknown.initial_events[0].target = NodeId(9);
    assert_eq!(
        rejection(&unknown, Backend::Scalar),
        "initial event 0 targets unknown node NodeId(9)"
    );

    let mut wrong_owner = valid_image();
    wrong_owner.initial_events[0].target = SINK;
    wrong_owner.initial_events[0].key.origin_node = SINK;
    assert_eq!(
        rejection(&wrong_owner, Backend::Scalar),
        "PacketArrival event 0 for payload PayloadId(0) targets node NodeId(2), but flow FlowId(0) is sourced by node NodeId(0)"
    );

    let mut unsupported_pair = valid_image();
    unsupported_pair.initial_events[0].target = SWITCH;
    unsupported_pair.initial_events[0].key.origin_node = SWITCH;
    assert_eq!(
        rejection(&unsupported_pair, Backend::Scalar),
        "initial event 0 targets Switch node NodeId(1), which does not support PacketArrival"
    );
}

#[test]
fn links_require_valid_endpoints_positive_rate_and_checked_delay() {
    let mut source = valid_image();
    source.links[0].source = NodeId(9);
    assert_eq!(
        rejection(&source, Backend::Scalar),
        "link LinkId(0) names unknown source node NodeId(9)"
    );

    let mut target = valid_image();
    target.links[0].target = NodeId(9);
    assert_eq!(
        rejection(&target, Backend::Scalar),
        "link LinkId(0) names unknown target node NodeId(9)"
    );

    let mut zero_rate = valid_image();
    zero_rate.links[0].rate_bps = 0;
    assert_eq!(
        rejection(&zero_rate, Backend::Scalar),
        "link LinkId(0) rate_bps must be positive"
    );

    let mut overflow = valid_image();
    overflow.links[0].propagation_ns = u64::MAX;
    assert_eq!(
        rejection(&overflow, Backend::Scalar),
        "link LinkId(0) delay overflows for packet PayloadId(0): link arrival time exceeds the u64 nanosecond domain"
    );
}

#[test]
fn routes_and_preloaded_service_state_must_be_executable() {
    let mut unknown_forward_link = valid_image();
    unknown_forward_link.flows[0].route[1] = LinkId(9);
    assert_eq!(
        rejection(&unknown_forward_link, Backend::Scalar),
        "flow FlowId(0) route step 1 references unknown link LinkId(9)"
    );

    let mut unknown_reverse_link = valid_image();
    unknown_reverse_link.links.push(LinkDescriptor {
        id: LinkId(3),
        source: SWITCH,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    unknown_reverse_link.flows[0].reverse_route = vec![SINK_EGRESS, LinkId(9)];
    assert_eq!(
        rejection(&unknown_reverse_link, Backend::Scalar),
        "flow FlowId(0) reverse route step 1 references unknown link LinkId(9)"
    );

    let mut interior_host = valid_image();
    interior_host.links.push(LinkDescriptor {
        id: LinkId(3),
        source: SWITCH,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    interior_host.flows[0].route = vec![SOURCE_LINK, SWITCH_LINK, SINK_EGRESS, LinkId(3)];
    assert_eq!(
        rejection(&interior_host, Backend::Scalar),
        "flow FlowId(0) route reaches interior Host node NodeId(2) at step 1; only Switch nodes may forward"
    );

    let mut wrong_queue = valid_image();
    let other_egress = LinkId(3);
    wrong_queue.links.push(LinkDescriptor {
        id: other_egress,
        source: SWITCH,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    wrong_queue.switch_states[0].queues[0] = SwitchQueueState {
        egress_link: Some(other_egress),
        scheduler: SchedulerKind::Fifo,
        queue_capacity_packets: 2,
        drop_mark: Default::default(),
        pfc: None,
        queue: VecDeque::from([PACKET]),
        in_service: None,
        tx_ready_pending: true,
    };
    assert_eq!(
        rejection(&wrong_queue, Backend::Scalar),
        "node NodeId(1) switch queue contains packet PayloadId(0) for egress Some(LinkId(1)), expected LinkId(3)"
    );

    let mut shared_switch_lp = valid_image();
    let duplicate_queue = shared_switch_lp.switch_states[0].queues[0].clone();
    shared_switch_lp.switch_states[0]
        .queues
        .push(duplicate_queue);
    assert_eq!(
        rejection(&shared_switch_lp, Backend::Scalar),
        "switch node NodeId(1) owns 2 egress queues; a switch LP may own at most one"
    );
}

#[test]
fn channels_must_match_emissions_and_certified_bounds() {
    let mut endpoints = valid_image();
    endpoints.channels[0].target = SINK;
    assert_eq!(
        rejection(&endpoints, Backend::Scalar),
        "channel 0 references link LinkId(0) to node NodeId(2), which has no possible route-selected packet emission"
    );

    let mut missing = valid_image();
    missing.channels.remove(0);
    assert_eq!(
        rejection(&missing, Backend::Scalar),
        "link LinkId(0) can emit RemoteArrival from node NodeId(0) to node NodeId(1), but no channel is declared"
    );

    let mut zero = valid_image();
    zero.channels[0].min_delay_ns = 0;
    validate(&zero, Backend::Scalar).expect("scalar execution permits a zero declared bound");
    assert_eq!(
        rejection(&zero, Backend::Cpu { workers: 1 }),
        "parallel backend Cpu channel 0 has zero min_delay_ns"
    );

    let mut overstated = valid_image();
    overstated.channels[0].min_delay_ns = 3;
    assert_eq!(
        rejection(&overstated, Backend::Scalar),
        "channel 0 declares min_delay_ns 3, exceeding derived bound 2 for link LinkId(0)"
    );
}

#[test]
fn channel_bounds_use_the_minimum_delay_across_packets_on_the_link() {
    let mut image = valid_image();
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(3),
        flow: FLOW,
        size_bytes: 100,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });

    validate(&image, Backend::Scalar)
        .expect("the two-nanosecond bound must cover every packet on the link");

    image.channels[0].min_delay_ns = 100;
    assert_eq!(
        rejection(&image, Backend::Scalar),
        "channel 0 declares min_delay_ns 100, exceeding derived bound 2 for link LinkId(0)"
    );
}

#[test]
fn channel_bounds_use_only_packets_admitted_to_the_referenced_link() {
    let mut image = valid_image();
    image.initial_packets[0].size_bytes = 100;
    image.channels[0].min_delay_ns = 100;
    image.channels[1].min_delay_ns = 267;

    let return_link = LinkId(3);
    let return_port = NodeId(3);
    image.nodes.push(NodeDescriptor {
        id: return_port,
        kind: NodeKind::Switch,
        state_slot: 1,
    });
    image.links.push(LinkDescriptor {
        id: return_link,
        source: return_port,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    image.switch_states.push(SwitchState {
        physical_switch: 0,
        queues: vec![SwitchQueueState {
            egress_link: Some(return_link),
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
    });
    image.flows.push(FlowDescriptor {
        id: FlowId(1),
        source: SINK,
        target: SOURCE,
        priority: 0,
        route: vec![SINK_EGRESS, return_link],
        reverse_route: vec![],
    });
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(2),
        flow: FlowId(1),
        size_bytes: 1,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });
    image.channels.extend([
        RemoteChannel {
            source: SINK,
            target: return_port,
            link: SINK_EGRESS,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: 1,
        },
        RemoteChannel {
            source: return_port,
            target: SOURCE,
            link: return_link,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: 1,
        },
    ]);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: SINK,
            origin_seq: 0,
        },
        target: SINK,
        kind: EventKind::PacketArrival,
        payload: PayloadId(2),
    });
    image.host_states[1].next_origin_seq = 1;

    validate(&image, Backend::Scalar)
        .expect("the unrelated one-byte packet must not lower the 100-byte link bound");

    image.channels[0].min_delay_ns = 101;
    assert_eq!(
        rejection(&image, Backend::Scalar),
        "channel 0 declares min_delay_ns 101, exceeding derived bound 100 for link LinkId(0)"
    );
}

#[test]
fn keys_services_capacities_and_arithmetic_must_fit_the_backend() {
    let mut phase = valid_image();
    phase.initial_events[0].key.phase = 1;
    assert_eq!(
        rejection(&phase, Backend::Scalar),
        "initial event 0 has noncanonical phase 1 for PacketArrival; expected 0"
    );

    let mut sequence = valid_image();
    sequence.host_states[0].next_origin_seq = 0;
    assert_eq!(
        rejection(&sequence, Backend::Scalar),
        "node NodeId(0) next origin sequence 0 does not advance existing sequence 0"
    );

    for scheduler in [
        SchedulerKind::static_priority(vec![1, 2]),
        SchedulerKind::weighted_fair_queue(vec![1, 2]),
    ] {
        let mut image = valid_image();
        image.switch_states[0].queues[0].scheduler = scheduler;
        validate(&image, Backend::Scalar).expect("scalar must support SP/WFQ");
        validate(&image, Backend::Cpu { workers: 2 }).expect("CPU must support SP/WFQ");
        validate(&image, Backend::Metal).expect("Metal must support bounded SP/WFQ");
        validate(&image, Backend::Cuda).expect("CUDA must support bounded SP/WFQ");
    }

    let mut wide_checkpoint = valid_image();
    let mut state = WfqSchedulerState::new(vec![1]);
    state.finish_times[0] = Ratio::from_integer(BigUint::from(1_u8) << 320_usize);
    wide_checkpoint.switch_states[0].queues[0].scheduler = SchedulerKind::WeightedFairQueue(state);
    validate(&wide_checkpoint, Backend::Scalar)
        .expect("Scalar keeps unbounded exact WFQ checkpoint arithmetic");
    validate(&wide_checkpoint, Backend::Cpu { workers: 2 })
        .expect("CPU keeps unbounded exact WFQ checkpoint arithmetic");
    for backend in [Backend::Metal, Backend::Cuda] {
        assert_eq!(
            rejection(&wide_checkpoint, backend),
            format!(
                "switch node NodeId(1) queue 0 WFQ finish state for class 0 numerator requires 321 bits; backend {backend} exact-rational limit is 320 bits (Scalar and Cpu are unbounded)"
            )
        );
    }

    let mut noncanonical_checkpoint = valid_image();
    let mut state = WfqSchedulerState::new(vec![1]);
    state.finish_times[0] = Ratio::new_raw(BigUint::from(2_u8), BigUint::from(2_u8));
    noncanonical_checkpoint.switch_states[0].queues[0].scheduler =
        SchedulerKind::WeightedFairQueue(state);
    validate(&noncanonical_checkpoint, Backend::Scalar)
        .expect("Scalar accepts unbounded raw rational checkpoints");
    validate(&noncanonical_checkpoint, Backend::Cpu { workers: 2 })
        .expect("CPU accepts unbounded raw rational checkpoints");
    for backend in [Backend::Metal, Backend::Cuda] {
        assert_eq!(
            rejection(&noncanonical_checkpoint, backend),
            format!(
                "switch node NodeId(1) queue 0 WFQ finish state for class 0 is not a reduced canonical rational; backend {backend} requires canonical checkpoint rationals (Scalar and Cpu are unbounded)"
            )
        );
    }

    let mut wide_weight_sum = valid_image();
    wide_weight_sum.switch_states[0].queues[0].scheduler =
        SchedulerKind::weighted_fair_queue(vec![18_446_744_074]);
    validate(&wide_weight_sum, Backend::Scalar)
        .expect("Scalar keeps unbounded active-weight denominator arithmetic");
    validate(&wide_weight_sum, Backend::Cpu { workers: 2 })
        .expect("CPU keeps unbounded active-weight denominator arithmetic");
    for backend in [Backend::Metal, Backend::Cuda] {
        assert_eq!(
            rejection(&wide_weight_sum, backend),
            format!(
                "switch node NodeId(1) queue 0 WFQ total weight 18446744074 makes the 1_000_000_000 * active-weight denominator exceed u64; backend {backend} requires a total weight at most 18446744073 (Scalar and Cpu are unbounded)"
            )
        );
    }

    let mut empty_priorities = valid_image();
    empty_priorities.switch_states[0].queues[0].scheduler = SchedulerKind::static_priority(vec![]);
    assert_eq!(
        rejection(&empty_priorities, Backend::Scalar),
        "switch node NodeId(1) queue 0 SP priorities must contain at least one class"
    );

    let mut zero_weight = valid_image();
    zero_weight.switch_states[0].queues[0].scheduler =
        SchedulerKind::weighted_fair_queue(vec![1, 0]);
    assert_eq!(
        rejection(&zero_weight, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ weight for class 1 must be positive"
    );

    let mut capacity = valid_image();
    capacity.switch_states[0].queues[0].queue_capacity_packets = u64::from(u32::MAX) + 1;
    assert_eq!(
        rejection(&capacity, Backend::Metal),
        "switch node NodeId(1) queue 0 capacity 4294967296 exceeds backend Metal limit 4294967295"
    );

    let mut zero_packet = valid_image();
    zero_packet.initial_packets[0].size_bytes = 0;
    assert_eq!(
        rejection(&zero_packet, Backend::Scalar),
        "packet PayloadId(0) has zero size, which cannot certify positive serialization"
    );

    let mut serialization = valid_image();
    serialization.initial_packets[0].size_bytes = u64::MAX;
    serialization.links[0].rate_bps = 1;
    assert_eq!(
        rejection(&serialization, Backend::Scalar),
        "link LinkId(0) delay overflows for packet PayloadId(0): serialization time exceeds the u64 nanosecond domain"
    );

    let mut cumulative = valid_image();
    cumulative.initial_packets[0].size_bytes = u64::MAX / 2 + 1;
    cumulative.links[1].rate_bps = 8_000_000_000;
    assert_eq!(
        rejection(&cumulative, Backend::Scalar),
        "packet PayloadId(0) cumulative minimum route delay overflows at link LinkId(1)"
    );

    let mut absolute = valid_image();
    absolute.initial_events[0].key.time_ns = u64::MAX - 1;
    assert_eq!(
        rejection(&absolute, Backend::Scalar),
        "initial event 0 time 18446744073709551614 plus remaining route delay 8 overflows for payload PayloadId(0) at node NodeId(0)"
    );

    let mut queued_overflow = valid_image();
    queued_overflow.initial_events[0].key.time_ns = u64::MAX - 10;
    queued_overflow.initial_packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });
    queued_overflow.initial_events.push(Event {
        key: EventKey {
            time_ns: u64::MAX - 9,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: SOURCE,
            origin_seq: 1,
        },
        target: SOURCE,
        kind: EventKind::PacketArrival,
        payload: PayloadId(1),
    });
    queued_overflow.host_states[0].next_origin_seq = 2;
    assert_eq!(
        rejection(&queued_overflow, Backend::Scalar),
        "maximum initial event time 18446744073709551606 plus conservative service bound 16 overflows"
    );

    let mut pending_event = valid_image();
    pending_event.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::TxReady),
            origin_node: SOURCE,
            origin_seq: 1,
        },
        target: SOURCE,
        kind: EventKind::TxReady,
        payload: PACKET,
    });
    pending_event.host_states[0].next_origin_seq = 2;
    assert_eq!(
        rejection(&pending_event, Backend::Scalar),
        "node NodeId(0) egress None TxReady flag is false, but matching event count is 1"
    );

    let mut counter = valid_image();
    counter.host_states[0].sourced_packets = u64::MAX;
    assert_eq!(
        rejection(&counter, Backend::Scalar),
        "node NodeId(0) counter sourced_packets value 18446744073709551615 overflows with remaining upper bound 1"
    );
}

#[test]
fn wfq_checkpoint_rationals_require_nonzero_denominators() {
    let zero_denominator = || Ratio::new_raw(BigUint::from(0_u8), BigUint::from(0_u8));

    let mut virtual_time = valid_image();
    let mut state = WfqSchedulerState::new(vec![1]);
    state.virtual_time = zero_denominator();
    virtual_time.switch_states[0].queues[0].scheduler = SchedulerKind::WeightedFairQueue(state);
    assert_eq!(
        rejection(&virtual_time, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ virtual time has a zero denominator"
    );

    let mut class_finish = valid_image();
    let mut state = WfqSchedulerState::new(vec![1]);
    state.finish_times[0] = zero_denominator();
    class_finish.switch_states[0].queues[0].scheduler = SchedulerKind::WeightedFairQueue(state);
    assert_eq!(
        rejection(&class_finish, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ finish state for class 0 has a zero denominator"
    );

    let mut packet_finish = wfq_waiting_image();
    let SchedulerKind::WeightedFairQueue(state) =
        &mut packet_finish.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.packet_finish_times.insert(PACKET, zero_denominator());
    assert_eq!(
        rejection(&packet_finish, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ finish tag for packet PayloadId(0) has a zero denominator"
    );
}

#[test]
fn wfq_checkpoint_time_cannot_lead_the_pending_event_frontier() {
    let mut idle = valid_image();
    let mut state = WfqSchedulerState::new(vec![1]);
    state.last_updated_ns = 1;
    idle.switch_states[0].queues[0].scheduler = SchedulerKind::WeightedFairQueue(state);
    assert_eq!(
        rejection(&idle, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ last update time 1 exceeds pending event frontier 0"
    );

    let mut active = wfq_waiting_image();
    let SchedulerKind::WeightedFairQueue(state) = &mut active.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.last_updated_ns = 1;
    assert_eq!(
        rejection(&active, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ last update time 1 exceeds pending event frontier 0"
    );
}

#[test]
fn wfq_checkpoint_waiting_tags_are_positive_and_close_class_history() {
    let mut zero = wfq_waiting_image();
    let SchedulerKind::WeightedFairQueue(state) = &mut zero.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.finish_times[0] = Ratio::from_integer(BigUint::from(0_u8));
    state
        .packet_finish_times
        .insert(PACKET, Ratio::from_integer(BigUint::from(0_u8)));
    assert_eq!(
        rejection(&zero, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ finish tag for waiting packet PayloadId(0) must be positive"
    );

    let mut inflated = wfq_waiting_image();
    let SchedulerKind::WeightedFairQueue(state) =
        &mut inflated.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.finish_times[0] = Ratio::from_integer(BigUint::from(17_u8));
    assert_eq!(
        rejection(&inflated, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ finish state for class 0 does not equal its maximum waiting tag"
    );
}

#[test]
fn wfq_checkpoint_in_service_tag_closes_class_history() {
    validate(&wfq_in_service_image(), Backend::Scalar)
        .expect("the valid in-service checkpoint must validate");

    let mut malformed = wfq_in_service_image();
    let SchedulerKind::WeightedFairQueue(state) =
        &mut malformed.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.finish_times[0] = Ratio::from_integer(BigUint::from(17_u8));

    assert_eq!(
        rejection(&malformed, Backend::Scalar),
        "switch node NodeId(1) queue 0 WFQ finish state for class 0 does not equal its in-service packet PayloadId(0) tag"
    );
}

#[test]
fn initial_event_keys_must_be_strictly_ascending() {
    let mut image = valid_image();
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });
    image.initial_events[0].key.time_ns = 1;
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: SOURCE,
            origin_seq: 1,
        },
        target: SOURCE,
        kind: EventKind::PacketArrival,
        payload: PayloadId(1),
    });
    image.host_states[0].next_origin_seq = 2;

    assert_eq!(
        rejection(&image, Backend::Scalar),
        "initial event 1 key EventKey { time_ns: 0, phase: 0, origin_node: NodeId(0), origin_seq: 1 } does not advance previous key EventKey { time_ns: 1, phase: 0, origin_node: NodeId(0), origin_seq: 0 }"
    );
}

#[test]
fn remote_arrivals_require_a_channel_on_the_payload_route() {
    let mut valid = valid_image();
    valid.initial_events[0].kind = EventKind::RemoteArrival;
    valid.initial_events[0].key.origin_node = SWITCH;
    valid.initial_events[0].target = SINK;
    valid.switch_states[0].next_origin_seq = 1;
    validate(&valid, Backend::Scalar).expect("the route channel should admit the remote arrival");

    let mut missing = valid_image();
    missing.initial_events[0].kind = EventKind::RemoteArrival;
    missing.initial_events[0].target = SINK;
    assert_eq!(
        rejection(&missing, Backend::Scalar),
        "RemoteArrival event 0 from NodeId(0) to NodeId(2) has no declared route channel for payload PayloadId(0)"
    );
}

#[test]
fn completion_diagnostics_preserve_initial_event_payload_order() {
    let mut image = valid_image();
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });
    image.initial_events[0].kind = EventKind::TxComplete;
    image.initial_events[0].key.phase = event_phase(EventKind::TxComplete);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::TxComplete),
            origin_node: SOURCE,
            origin_seq: 1,
        },
        target: SOURCE,
        kind: EventKind::TxComplete,
        payload: PayloadId(1),
    });
    image.host_states[0].in_service = Some(PACKET);
    image.host_states[0].next_origin_seq = 2;

    assert_eq!(
        rejection(&image, Backend::Scalar),
        "node NodeId(0) egress None in-service payload is Some(PayloadId(0)), but matching TxComplete payloads are [PayloadId(0), PayloadId(1)]"
    );
}

#[test]
fn aggregate_packet_counts_retain_counter_and_sequence_diagnostics() {
    let mut counter = valid_image();
    counter.initial_packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    });
    counter.host_states[0].sourced_packets = u64::MAX - 1;
    assert_eq!(
        rejection(&counter, Backend::Scalar),
        "node NodeId(0) counter sourced_packets value 18446744073709551614 overflows with remaining upper bound 2"
    );

    let mut sequence = valid_image();
    sequence.host_states[0].next_origin_seq = u64::MAX - 2;
    assert_eq!(
        rejection(&sequence, Backend::Scalar),
        "node NodeId(0) origin sequence space overflows while reserving 3 generated events"
    );
}

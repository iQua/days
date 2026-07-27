use std::collections::VecDeque;

use days_executor::{
    Backend, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState, LinkDescriptor, LinkId,
    NodeDescriptor, NodeId, NodeKind, PacketDescriptor, PayloadId, RemoteChannel, SchedulerKind,
    SimulationImage, SwitchQueueState, SwitchState, event_phase, validate,
};

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
                next_origin_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![SwitchState {
            queues: vec![SwitchQueueState {
                egress_link: Some(SWITCH_LINK),
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
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            route: vec![SOURCE_LINK, SWITCH_LINK],
        }],
        packets: vec![PacketDescriptor {
            id: PACKET,
            flow: FLOW,
            size_bytes: 2,
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
    wrong_queue.switch_states[0].queues.push(SwitchQueueState {
        egress_link: Some(other_egress),
        scheduler: SchedulerKind::Fifo,
        queue_capacity_packets: 2,
        queue: VecDeque::from([PACKET]),
        in_service: None,
        tx_ready_pending: true,
    });
    assert_eq!(
        rejection(&wrong_queue, Backend::Scalar),
        "node NodeId(1) switch queue contains packet PayloadId(0) for egress Some(LinkId(1)), expected LinkId(3)"
    );
}

#[test]
fn channels_must_match_emissions_and_certified_bounds() {
    let mut endpoints = valid_image();
    endpoints.channels[0].target = SINK;
    assert_eq!(
        rejection(&endpoints, Backend::Scalar),
        "channel 0 endpoints NodeId(0)->NodeId(2) do not match link LinkId(0) endpoints NodeId(0)->NodeId(1)"
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
    image.packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 100,
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
    image.packets[0].size_bytes = 100;
    image.channels[0].min_delay_ns = 100;
    image.channels[1].min_delay_ns = 267;

    let return_link = LinkId(3);
    image.links.push(LinkDescriptor {
        id: return_link,
        source: SWITCH,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    image.switch_states[0].queues.push(SwitchQueueState {
        egress_link: Some(return_link),
        scheduler: SchedulerKind::Fifo,
        queue_capacity_packets: 2,
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
    });
    image.flows.push(FlowDescriptor {
        id: FlowId(1),
        source: SINK,
        target: SOURCE,
        route: vec![SINK_EGRESS, return_link],
    });
    image.packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FlowId(1),
        size_bytes: 1,
    });
    image.channels.extend([
        RemoteChannel {
            source: SINK,
            target: SWITCH,
            link: SINK_EGRESS,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: 1,
        },
        RemoteChannel {
            source: SWITCH,
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
        payload: PayloadId(1),
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

    let mut scheduler = valid_image();
    scheduler.switch_states[0].queues[0].scheduler = SchedulerKind::StaticPriority;
    assert_eq!(
        rejection(&scheduler, Backend::Scalar),
        "switch node NodeId(1) queue 0 uses unsupported StaticPriority service on backend Scalar"
    );

    let mut capacity = valid_image();
    capacity.switch_states[0].queues[0].queue_capacity_packets = u64::from(u32::MAX) + 1;
    assert_eq!(
        rejection(&capacity, Backend::Metal),
        "switch node NodeId(1) queue 0 capacity 4294967296 exceeds backend Metal limit 4294967295"
    );

    let mut zero_packet = valid_image();
    zero_packet.packets[0].size_bytes = 0;
    assert_eq!(
        rejection(&zero_packet, Backend::Scalar),
        "packet PayloadId(0) has zero size, which cannot certify positive serialization"
    );

    let mut serialization = valid_image();
    serialization.packets[0].size_bytes = u64::MAX;
    serialization.links[0].rate_bps = 1;
    assert_eq!(
        rejection(&serialization, Backend::Scalar),
        "link LinkId(0) delay overflows for packet PayloadId(0): serialization time exceeds the u64 nanosecond domain"
    );

    let mut cumulative = valid_image();
    cumulative.packets[0].size_bytes = u64::MAX / 2 + 1;
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
    queued_overflow.packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
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
fn initial_event_keys_must_be_strictly_ascending() {
    let mut image = valid_image();
    image.packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 2,
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

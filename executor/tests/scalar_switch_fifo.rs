use std::collections::VecDeque;

use days_executor::{
    ArrivalDisposition, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, PacketArrivalObservation,
    PacketDeparture, PacketDescriptor, PayloadId, RemoteChannel, SchedulerKind, SimulationImage,
    SwitchQueueState, SwitchState, event_phase, run_scalar,
};

const SOURCE: NodeId = NodeId(0);
const SWITCH: NodeId = NodeId(1);
const SINK: NodeId = NodeId(2);
const SOURCE_LINK: LinkId = LinkId(0);
const SWITCH_LINK: LinkId = LinkId(1);
const SINK_EGRESS: LinkId = LinkId(2);
const FLOW: FlowId = FlowId(0);
const P0: PayloadId = PayloadId(0);
const P1: PayloadId = PayloadId(1);
const P2: PayloadId = PayloadId(2);
const P3: PayloadId = PayloadId(3);
const P4: PayloadId = PayloadId(4);

fn source_arrival(time_ns: u64, origin_seq: u64, payload: PayloadId) -> Event {
    Event {
        key: EventKey {
            time_ns,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: SOURCE,
            origin_seq,
        },
        target: SOURCE,
        kind: EventKind::PacketArrival,
        payload,
    }
}

fn image() -> SimulationImage {
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
                next_origin_seq: 5,
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
        packets: vec![
            PacketDescriptor {
                id: P0,
                flow: FLOW,
                size_bytes: 2,
            },
            PacketDescriptor {
                id: P1,
                flow: FLOW,
                size_bytes: 2,
            },
            PacketDescriptor {
                id: P2,
                flow: FLOW,
                size_bytes: 2,
            },
            PacketDescriptor {
                id: P3,
                flow: FLOW,
                size_bytes: 2,
            },
            PacketDescriptor {
                id: P4,
                flow: FLOW,
                size_bytes: 2,
            },
        ],
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
        initial_events: vec![
            source_arrival(0, 0, P0),
            source_arrival(0, 1, P1),
            source_arrival(0, 2, P2),
            source_arrival(7, 3, P3),
            source_arrival(8, 4, P4),
        ],
        seed: 7,
    }
}

#[test]
fn switch_fifo_selects_one_packet_per_tx_ready_and_reaches_the_sink() {
    /*
    P0, P1, and P2 reach the switch at 2, 4, and 6 ns. The switch's 3 Gb/s
    egress serializes each 2 B packet in 6 ns, so P0 is in service from 2 to
    8 ns while P1 and P2 wait.

    At the 8 ns TxReady, correct service commits only P1. P3 arrives at 9 ns
    and fills the second waiting slot; P4 arrives at 11 ns and TailDrops.
    A greedy TxReady that also reserves P2 at 8 ns frees an extra slot and
    incorrectly admits P4. The 20 ns P2 departure also distinguishes the
    correct independently rounded intervals from cumulative serialization,
    which would depart P2 at 19 ns.
    */
    let result = run_scalar(&image(), 27).expect("the complete path must execute");

    assert_eq!(
        result.departures,
        vec![
            PacketDeparture {
                payload: P0,
                time_ns: 2,
            },
            PacketDeparture {
                payload: P1,
                time_ns: 4,
            },
            PacketDeparture {
                payload: P2,
                time_ns: 6,
            },
            PacketDeparture {
                payload: P0,
                time_ns: 8,
            },
            PacketDeparture {
                payload: P3,
                time_ns: 9,
            },
            PacketDeparture {
                payload: P4,
                time_ns: 11,
            },
            PacketDeparture {
                payload: P1,
                time_ns: 14,
            },
            PacketDeparture {
                payload: P2,
                time_ns: 20,
            },
            PacketDeparture {
                payload: P3,
                time_ns: 26,
            },
        ]
    );
    assert_eq!(
        result.arrivals,
        vec![
            PacketArrivalObservation {
                payload: P0,
                time_ns: 2,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: P1,
                time_ns: 4,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: P2,
                time_ns: 6,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: P0,
                time_ns: 8,
                disposition: ArrivalDisposition::Delivered,
            },
            PacketArrivalObservation {
                payload: P3,
                time_ns: 9,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: P4,
                time_ns: 11,
                disposition: ArrivalDisposition::Dropped,
            },
            PacketArrivalObservation {
                payload: P1,
                time_ns: 14,
                disposition: ArrivalDisposition::Delivered,
            },
            PacketArrivalObservation {
                payload: P2,
                time_ns: 20,
                disposition: ArrivalDisposition::Delivered,
            },
            PacketArrivalObservation {
                payload: P3,
                time_ns: 26,
                disposition: ArrivalDisposition::Delivered,
            },
        ]
    );
    assert!(result.pending_events.is_empty());
    assert_eq!(
        result.host_states,
        vec![
            HostState {
                egress_link: SOURCE_LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                next_origin_seq: 20,
                sourced_packets: 5,
                departed_packets: 5,
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
                received_packets: 4,
            },
        ]
    );
    assert_eq!(
        result.switch_states,
        vec![SwitchState {
            queues: vec![SwitchQueueState {
                egress_link: Some(SWITCH_LINK),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 2,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 12,
            arrived_packets: 5,
            dropped_packets: 1,
            departed_packets: 4,
        }]
    );
}

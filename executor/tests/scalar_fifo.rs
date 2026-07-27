use std::collections::VecDeque;

use days_executor::{
    ArrivalDisposition, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, PacketArrivalObservation,
    PacketDeparture, PacketDescriptor, PayloadId, RemoteChannel, SchedulerKind, SimulationImage,
    SwitchQueueState, SwitchState, event_phase, run_scalar,
};

const HOST: NodeId = NodeId(10);
const SWITCH: NodeId = NodeId(20);
const LINK: LinkId = LinkId(30);
const FLOW: FlowId = FlowId(40);
const P0: PayloadId = PayloadId(0);
const P1: PayloadId = PayloadId(1);
const P2: PayloadId = PayloadId(2);
const P3: PayloadId = PayloadId(3);

fn source_arrival(time_ns: u64, origin_seq: u64, payload: PayloadId) -> Event {
    Event {
        key: EventKey {
            time_ns,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: HOST,
            origin_seq,
        },
        target: HOST,
        kind: EventKind::PacketArrival,
        payload,
    }
}

#[test]
fn scalar_fifo_taildrop_matches_the_hand_checked_golden() {
    let image = SimulationImage {
        stop_time_ns: u64::MAX,
        nodes: vec![
            NodeDescriptor {
                id: HOST,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: SWITCH,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
        ],
        host_states: vec![HostState {
            egress_link: LINK,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            next_origin_seq: 4,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        }],
        switch_states: vec![SwitchState {
            queues: vec![SwitchQueueState {
                egress_link: None,
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
            source: HOST,
            target: SWITCH,
            route: vec![LINK],
        }],
        packets: vec![
            PacketDescriptor {
                id: P0,
                flow: FLOW,
                size_bytes: 3,
            },
            PacketDescriptor {
                id: P1,
                flow: FLOW,
                size_bytes: 6,
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
        ],
        links: vec![LinkDescriptor {
            id: LINK,
            source: HOST,
            target: SWITCH,
            rate_bps: 3_000_000_000,
            propagation_ns: 2,
        }],
        channels: vec![RemoteChannel {
            source: HOST,
            target: SWITCH,
            link: LINK,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: 5,
        }],
        initial_events: vec![
            source_arrival(0, 0, P0),
            source_arrival(2, 1, P1),
            source_arrival(9, 2, P2),
            source_arrival(10, 3, P3),
        ],
        seed: 7,
    };

    /*
    Hand-checked golden, with r = 3 Gb/s and d_prop = 2 ns:

      P0: 3 B at t=0
          serialization = ceil(8*3*10^9 / 3*10^9) = 8 ns
          start 0, departure 8, arrival 8+2 = 10

      P1: 6 B arrives at t=2, between service starts 0 and 8
          serialization = ceil(8*6*10^9 / 3*10^9) = 16 ns
          start 8, departure 24, arrival 24+2 = 26

      P2: 2 B arrives at t=9, after P1 started, so it cannot displace P1
          serialization = ceil(8*2*10^9 / 3*10^9) = 6 ns
          start 24, departure 30, arrival 30+2 = 32

      P3: 2 B arrives at t=10
          serialization = 6 ns
          start 30, departure 36, arrival 36+2 = 38

    Thus service starts are [0, 8, 24, 30]. P1's source arrival at t=2 and
    P1's remote arrival at t=26 both land strictly between service starts.
    The switch queue has capacity two: P0 and P1 are admitted, then P2 is
    TailDropped at t=32. The exclusive execution horizon is t=37, leaving only P3's
    RemoteArrival at t=38.

    A regressed eager selector at t=24 would reserve P2 and P3 together:
      P2 cumulative deadline = 24 + ceil(8*2*10^9 / 3*10^9) = 30
      P3 cumulative deadline = 24 + ceil(8*4*10^9 / 3*10^9) = 35
    Correct one-at-a-time selection instead rounds each 2 B interval to 6 ns,
    so P3 departs at 30+6 = 36. The old 3 B/1 B pair did not discriminate:
    its independent 8+3 ns equalled its cumulative ceil(8*4/3)=11 ns.

    Initial origin sequences are 0..3. Deterministic child emission assigns:
      ready0=4, complete0=5, remote0=6,
      ready1=7, complete1=8, remote1=9,
      ready2=10, complete2=11, remote2=12,
      ready3=13, complete3=14, remote3=15.
    */
    let result = run_scalar(&image, Some(37)).expect("the hand-built image must execute");

    assert_eq!(
        result.host_states,
        vec![HostState {
            egress_link: LINK,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            next_origin_seq: 16,
            sourced_packets: 4,
            departed_packets: 4,
            received_packets: 0,
        }]
    );
    assert_eq!(
        result.switch_states,
        vec![SwitchState {
            queues: vec![SwitchQueueState {
                egress_link: None,
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 2,
                queue: VecDeque::from([P0, P1]),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 0,
            arrived_packets: 3,
            dropped_packets: 1,
            departed_packets: 0,
        }]
    );
    assert_eq!(
        result.departures,
        vec![
            PacketDeparture {
                payload: P0,
                time_ns: 8,
            },
            PacketDeparture {
                payload: P1,
                time_ns: 24,
            },
            PacketDeparture {
                payload: P2,
                time_ns: 30,
            },
            PacketDeparture {
                payload: P3,
                time_ns: 36,
            },
        ]
    );
    assert_eq!(
        result.arrivals,
        vec![
            PacketArrivalObservation {
                payload: P0,
                time_ns: 10,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: P1,
                time_ns: 26,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: P2,
                time_ns: 32,
                disposition: ArrivalDisposition::Dropped,
            },
        ]
    );
    assert_eq!(
        result.pending_events,
        vec![Event {
            key: EventKey {
                time_ns: 38,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: HOST,
                origin_seq: 15,
            },
            target: SWITCH,
            kind: EventKind::RemoteArrival,
            payload: P3,
        }]
    );
}

use std::collections::VecDeque;

use days_executor::{
    Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState, LinkDescriptor, LinkId,
    NodeDescriptor, NodeId, NodeKind, PacketDescriptor, PayloadId, RemoteChannel, SimulationImage,
    event_phase, run_scalar,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const LINK: LinkId = LinkId(0);
const FLOW: FlowId = FlowId(0);
const AT_BOUNDARY: PayloadId = PayloadId(0);
const AFTER_BOUNDARY: PayloadId = PayloadId(1);
const BOUNDARY_NS: u64 = 10;

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

fn image(stop_time_ns: u64) -> SimulationImage {
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
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_payload_seq: 0,
                next_origin_seq: 2,
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
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_payload_seq: 0,
                next_origin_seq: 0,
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
            route: vec![LINK],
            reverse_route: vec![],
        }],
        initial_packets: vec![
            PacketDescriptor {
                id: AT_BOUNDARY,
                flow: FLOW,
                size_bytes: 1,
                ecn_marked: false,
                kind: days_executor::PacketKind::Data,
            },
            PacketDescriptor {
                id: AFTER_BOUNDARY,
                flow: FLOW,
                size_bytes: 1,
                ecn_marked: false,
                kind: days_executor::PacketKind::Data,
            },
        ],
        links: vec![LinkDescriptor {
            id: LINK,
            source: SOURCE,
            target: SINK,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        }],
        channels: vec![RemoteChannel {
            source: SOURCE,
            target: SINK,
            link: LINK,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: 1,
        }],
        initial_events: vec![
            source_arrival(BOUNDARY_NS, 0, AT_BOUNDARY),
            source_arrival(BOUNDARY_NS + 1, 1, AFTER_BOUNDARY),
        ],
        seed: 1,
    }
}

fn assert_only_boundary_source_was_processed(result: &days_executor::RunResult) {
    assert_eq!(result.host_states[0].sourced_packets, 1);
    assert!(
        result.pending_events.iter().any(|event| {
            event.kind == EventKind::PacketArrival && event.payload == AFTER_BOUNDARY
        }),
        "the source event one nanosecond after the boundary must remain pending"
    );
}

#[test]
fn simulation_stop_is_inclusive() {
    let result = run_scalar(&image(BOUNDARY_NS), None).expect("the boundary image must execute");

    assert_only_boundary_source_was_processed(&result);
}

#[test]
fn execution_horizon_is_half_open() {
    let result = run_scalar(&image(BOUNDARY_NS + 1), Some(BOUNDARY_NS))
        .expect("the boundary image must execute");

    assert_eq!(result.host_states[0].sourced_packets, 0);
}

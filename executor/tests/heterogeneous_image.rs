use std::collections::VecDeque;

use days_executor::{
    EventKind, HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, RemoteChannel,
    SchedulerKind, SimulationImage, SwitchState, TransitionHandler, resolve_transition,
};

#[test]
fn one_image_contains_host_and_switch_state_arenas() {
    let host = NodeId(10);
    let switch = NodeId(20);
    let link = LinkId(30);
    let image = SimulationImage {
        nodes: vec![
            NodeDescriptor {
                id: host,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: switch,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
        ],
        host_states: vec![HostState {
            egress_link: link,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            next_origin_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
        }],
        switch_states: vec![SwitchState {
            scheduler: SchedulerKind::Fifo,
            queue_capacity_packets: 64,
            queue: VecDeque::new(),
            arrived_packets: 0,
            dropped_packets: 0,
        }],
        packets: Vec::new(),
        links: vec![LinkDescriptor {
            id: link,
            source: host,
            target: switch,
            rate_bps: 12_000_000_000,
            propagation_ns: 25,
        }],
        channels: vec![RemoteChannel {
            source: host,
            target: switch,
            link,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: 1_025,
        }],
        initial_events: Vec::new(),
        seed: 7,
    };

    assert_eq!(image.nodes[0].kind, NodeKind::Host);
    assert_eq!(image.nodes[1].kind, NodeKind::Switch);
    assert_eq!(image.nodes[0].state_slot, 0);
    assert_eq!(image.nodes[1].state_slot, 0);
    assert_eq!(image.channels[0].link, image.links[0].id);
}

#[test]
fn every_role_event_pair_resolves_or_is_explicitly_rejected() {
    let cases = [
        (
            NodeKind::Host,
            EventKind::PacketArrival,
            Some(TransitionHandler::HostPacketArrival),
        ),
        (
            NodeKind::Host,
            EventKind::TxReady,
            Some(TransitionHandler::HostTxReady),
        ),
        (
            NodeKind::Host,
            EventKind::TxComplete,
            Some(TransitionHandler::HostTxComplete),
        ),
        (
            NodeKind::Host,
            EventKind::RemoteArrival,
            Some(TransitionHandler::HostRemoteArrival),
        ),
        (NodeKind::Switch, EventKind::PacketArrival, None),
        (
            NodeKind::Switch,
            EventKind::TxReady,
            Some(TransitionHandler::SwitchTxReady),
        ),
        (
            NodeKind::Switch,
            EventKind::TxComplete,
            Some(TransitionHandler::SwitchTxComplete),
        ),
        (
            NodeKind::Switch,
            EventKind::RemoteArrival,
            Some(TransitionHandler::SwitchRemoteArrival),
        ),
    ];

    for (node_kind, event_kind, expected) in cases {
        assert_eq!(resolve_transition(node_kind, event_kind), expected);
    }
}

#[test]
fn link_descriptor_supplies_serialization_and_propagation() {
    let base = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(2),
        target: NodeId(3),
        rate_bps: 12_000_000_000,
        propagation_ns: 0,
    };
    let propagated = LinkDescriptor {
        propagation_ns: 25,
        ..base
    };

    assert_eq!(base.arrival_time_ns(50, 1_500), Ok(1_050));
    assert_eq!(propagated.arrival_time_ns(50, 1_500), Ok(1_075));
    assert!(
        base.arrival_time_ns(0, 1).expect("positive serialization") > 0,
        "zero propagation is valid when serialization gives positive delay"
    );
}

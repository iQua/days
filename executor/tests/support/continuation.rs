use std::collections::VecDeque;

use days_executor::{
    Event, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId,
    GeneratorFeedbackState, GeneratorStatus, HostState, LinkDescriptor, LinkId, NodeDescriptor,
    NodeId, NodeKind, PacketDescriptor, PacketKind, PayloadId, RemoteChannel, ScheduledEmission,
    SimulationImage, TcpCongestionControl, TcpDataHeader, TcpGenerator, TcpReceiverState,
    event_phase,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const FORWARD: LinkId = LinkId(0);
const REVERSE: LinkId = LinkId(1);
const FLOW: FlowId = FlowId(0);
const FIRST: PayloadId = PayloadId(0);
const MSS: u64 = 512;
const ACK_BYTES: u64 = 40;

/// Small TCP fixture with enough queued data and ACKs for service completions to emit same-time
/// `TxReady` continuations.
pub fn tcp_service_continuation_image() -> SimulationImage {
    let total_bytes = 4 * MSS;
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let reverse = LinkDescriptor {
        id: REVERSE,
        source: SINK,
        target: SOURCE,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let first = PacketDescriptor {
        id: FIRST,
        flow: FLOW,
        size_bytes: MSS,
        ecn_marked: false,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    };

    SimulationImage {
        stop_time_ns: 10_000,
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
                        payload: FIRST,
                    },
                    rng_state: 7,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Tcp(TcpGenerator::new(
                        total_bytes,
                        MSS,
                        ACK_BYTES,
                        TcpCongestionControl::reno(MSS),
                    )),
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
                tcp_receivers: vec![TcpReceiverState::new(FLOW, ACK_BYTES)],
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
        initial_packets: vec![first],
        links: vec![forward, reverse],
        channels: vec![
            RemoteChannel::for_packet_link(forward, 1).unwrap(),
            RemoteChannel::for_packet_link(reverse, ACK_BYTES).unwrap(),
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
            payload: FIRST,
        }],
        seed: 1,
    }
}

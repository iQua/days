//! Immutable, backend-neutral simulation image records.

use std::collections::VecDeque;

use crate::{
    Event, EventKind, FlowId, LinkId, NodeId, NodeKind, PayloadId, SchedulerKind, TimeError,
    link_arrival_time_ns,
};

/// The plan's default constant propagation delay for a directed link.
pub const fn default_propagation_ns() -> u64 {
    0
}

/// Semantic identity and role-specific state location of one logical process.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub kind: NodeKind,
    /// Index into `SimulationImage::host_states` or `switch_states`, selected by `kind`.
    pub state_slot: u32,
}

/// Host-owned semantic state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostState {
    pub egress_link: LinkId,
    pub queue: VecDeque<PayloadId>,
    pub in_service: Option<PayloadId>,
    pub tx_ready_pending: bool,
    pub next_origin_seq: u64,
    pub sourced_packets: u64,
    pub departed_packets: u64,
    pub received_packets: u64,
}

/// One switch-owned FIFO/TailDrop egress queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwitchQueueState {
    /// `None` is used only by terminal hand-built fixtures whose flow ends at the switch.
    pub egress_link: Option<LinkId>,
    pub scheduler: SchedulerKind,
    /// Maximum queued packets. A value of zero denotes an unbounded queue.
    pub queue_capacity_packets: u64,
    pub queue: VecDeque<PayloadId>,
    /// Packet committed to the non-preemptive transmission currently in progress.
    pub in_service: Option<PayloadId>,
    /// Whether this queue already owns a future `TxReady` decision point.
    pub tx_ready_pending: bool,
}

/// Switch-owned state containing one queue per directed egress.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwitchState {
    pub queues: Vec<SwitchQueueState>,
    /// One deterministic child-emission sequence shared by every queue owned by this node.
    pub next_origin_seq: u64,
    pub arrived_packets: u64,
    pub dropped_packets: u64,
    pub departed_packets: u64,
}

/// Stable endpoints and canonical directed route for one open-loop flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlowDescriptor {
    pub id: FlowId,
    pub source: NodeId,
    pub target: NodeId,
    pub route: Vec<LinkId>,
}

/// Immutable packet data referenced by a persistent event payload.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketDescriptor {
    pub id: PayloadId,
    pub flow: FlowId,
    pub size_bytes: u64,
}

/// Immutable configuration of one constant-rate, directed, non-preemptive link.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkDescriptor {
    pub id: LinkId,
    pub source: NodeId,
    pub target: NodeId,
    pub rate_bps: u64,
    pub propagation_ns: u64,
}

impl LinkDescriptor {
    /// Computes serialization plus propagation without an absolute start time.
    pub fn delay_ns(&self, bytes: u64) -> Result<u64, TimeError> {
        link_arrival_time_ns(0, bytes, self.rate_bps, self.propagation_ns)
    }

    /// Computes packet arrival using this link's constant rate and propagation delay.
    pub fn arrival_time_ns(&self, start_time_ns: u64, bytes: u64) -> Result<u64, TimeError> {
        link_arrival_time_ns(start_time_ns, bytes, self.rate_bps, self.propagation_ns)
    }
}

/// Declared lower-bound channel for one kind of cross-node event.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteChannel {
    pub source: NodeId,
    pub target: NodeId,
    /// Directed physical link supplying serialization rate and propagation delay.
    pub link: LinkId,
    pub event_kind: EventKind,
    pub min_delay_ns: u64,
}

impl RemoteChannel {
    /// Constructs the canonical packet channel for a link and its admitted minimum packet size.
    pub fn for_packet_link(
        link: LinkDescriptor,
        min_packet_size_bytes: u64,
    ) -> Result<Self, TimeError> {
        Ok(Self {
            source: link.source,
            target: link.target,
            link: link.id,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: link.delay_ns(min_packet_size_bytes)?,
        })
    }
}

/// One immutable semantic image containing every host and switch logical process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationImage {
    pub nodes: Vec<NodeDescriptor>,
    pub host_states: Vec<HostState>,
    pub switch_states: Vec<SwitchState>,
    pub flows: Vec<FlowDescriptor>,
    pub packets: Vec<PacketDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub channels: Vec<RemoteChannel>,
    pub initial_events: Vec<Event>,
    pub seed: u64,
}

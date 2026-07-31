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
    /// Source-owned flow generators in canonical `FlowId` order.
    pub generators: Vec<FlowGeneratorState>,
    pub next_origin_seq: u64,
    /// Per-node packet identity cursor. Every generated packet consumes one sequence value.
    pub next_payload_seq: u64,
    pub sourced_packets: u64,
    pub departed_packets: u64,
    pub received_packets: u64,
}

/// One switch-port-owned TailDrop egress queue with discipline-owned service state.
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

/// Switch-port-owned state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwitchState {
    /// Stable physical topology identity shared by every egress LP of the same switch.
    pub physical_switch: u64,
    /// Lowered images contain exactly one queue. Hand-built images with zero queues remain useful
    /// for terminal validation fixtures.
    pub queues: Vec<SwitchQueueState>,
    /// Deterministic child-emission sequence owned only by this LP.
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
    /// Canonical data-packet route from source to target.
    pub route: Vec<LinkId>,
    /// Canonical feedback-packet route from target back to source.
    pub reverse_route: Vec<LinkId>,
}

/// Whether a generator owns a scheduled emission or is waiting without local work.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorStatus {
    Scheduled = 0,
    Blocked = 1,
    Finished = 2,
    /// Emission remains under the traffic termination but lies beyond the scenario stop.
    Stopped = 3,
}

/// The next packet already produced by a generator for a future emission transition.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledEmission {
    pub status: GeneratorStatus,
    /// Meaningful only when `status == GeneratorStatus::Scheduled`.
    pub departure_time_ns: u64,
    /// Meaningful only when `status == GeneratorStatus::Scheduled`.
    pub payload: PayloadId,
}

/// Feedback-owned transport state reserved by every generator kind.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratorFeedbackState {
    pub arrivals: u64,
    pub outstanding_bytes: u64,
    pub unacknowledged_bytes: u64,
}

/// Closed termination modes supported by the constant generator.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorTermination {
    Bytes(u64),
    DurationNs(u64),
}

/// Fixed-width parameters for the v1 constant generator.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConstantGenerator {
    pub first_departure_ns: u64,
    pub interval_ns: u64,
    pub packet_size_bytes: u64,
    pub termination: GeneratorTermination,
}

/// Closed generator transition set. New traffic families require an explicit image variant.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowGeneratorKind {
    Constant(ConstantGenerator),
}

/// Fixed-width result of routing an ordinary feedback packet into a source generator.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorFeedbackAction {
    None,
    Emit { flow: FlowId, size_bytes: u64 },
}

/// Mutable generator state owned exclusively by the source host LP.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowGeneratorState {
    pub flow: FlowId,
    pub packets_emitted: u64,
    pub bytes_emitted: u64,
    pub next_emission: ScheduledEmission,
    /// Per-flow deterministic stream state derived from the image seed and semantic flow key.
    pub rng_state: u64,
    pub feedback: GeneratorFeedbackState,
    pub kind: FlowGeneratorKind,
}

/// Immutable per-packet data referenced by a persistent event payload.
///
/// Lowered images retain only the first scheduled packet for each active flow. Later records are
/// produced by the source generator and live only while the packet is scheduled or in flight.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketDescriptor {
    pub id: PayloadId,
    pub flow: FlowId,
    pub size_bytes: u64,
    pub kind: PacketKind,
}

/// Closed packet direction used to route ordinary data and feedback packets.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketKind {
    Data = 0,
    Feedback = 1,
}

/// Immutable configuration of one constant-rate, directed, non-preemptive link.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkDescriptor {
    pub id: LinkId,
    /// Logical process that owns transmission on this physical directed link.
    pub source: NodeId,
    /// A logical-process representative of the physical receiving node. Packet delivery uses the
    /// route-selected `RemoteChannel::target`, which may be another egress LP at that same switch.
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
    /// Route-selected receiver LP after crossing `link`.
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
        Self::for_packet_link_to(link, link.target, min_packet_size_bytes)
    }

    /// Constructs a packet channel whose physical link feeds a route-selected downstream LP.
    pub fn for_packet_link_to(
        link: LinkDescriptor,
        target: NodeId,
        min_packet_size_bytes: u64,
    ) -> Result<Self, TimeError> {
        Ok(Self {
            source: link.source,
            target,
            link: link.id,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: link.delay_ns(min_packet_size_bytes)?,
        })
    }
}

/// One immutable semantic image containing every host and switch logical process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationImage {
    /// Inclusive configured simulation endpoint in integer nanoseconds.
    pub stop_time_ns: u64,
    pub nodes: Vec<NodeDescriptor>,
    pub host_states: Vec<HostState>,
    pub switch_states: Vec<SwitchState>,
    pub flows: Vec<FlowDescriptor>,
    /// Packet records needed by initial events or preloaded mutable state, never a whole-run table.
    pub initial_packets: Vec<PacketDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub channels: Vec<RemoteChannel>,
    pub initial_events: Vec<Event>,
    pub seed: u64,
}

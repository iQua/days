//! Immutable, backend-neutral simulation image records.

use std::collections::VecDeque;
use std::fmt;

use crate::{
    DropMarkPolicy, Event, EventKind, FlowId, LinkId, NodeId, NodeKind, PayloadId, SchedulerKind,
    TcpCongestionControl, TimeError, link_arrival_time_ns,
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
    /// Target-owned TCP cumulative-ACK state in canonical `FlowId` order.
    pub tcp_receivers: Vec<TcpReceiverState>,
    pub next_origin_seq: u64,
    /// Per-node packet identity cursor. Every generated packet consumes one sequence value.
    pub next_payload_seq: u64,
    pub sourced_packets: u64,
    pub departed_packets: u64,
    pub received_packets: u64,
}

/// One switch-port-owned TailDrop egress queue with discipline-owned service state.
#[derive(Clone, Eq, PartialEq)]
pub struct SwitchQueueState {
    /// `None` is used only by terminal hand-built fixtures whose flow ends at the switch.
    pub egress_link: Option<LinkId>,
    pub scheduler: SchedulerKind,
    /// Maximum queued packets. A value of zero denotes an unbounded queue.
    pub queue_capacity_packets: u64,
    /// Deterministic admission/marking policy. TailDrop preserves the original capacity field.
    pub drop_mark: DropMarkPolicy,
    /// Optional IEEE 802.1Qbb-style per-priority pause state and ingress monitor.
    pub pfc: Option<PfcQueueState>,
    pub queue: VecDeque<PayloadId>,
    /// Packet committed to the non-preemptive transmission currently in progress.
    pub in_service: Option<PayloadId>,
    /// Whether this queue already owns a future `TxReady` decision point.
    pub tx_ready_pending: bool,
}

impl fmt::Debug for SwitchQueueState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("SwitchQueueState");
        debug
            .field("egress_link", &self.egress_link)
            .field("scheduler", &self.scheduler)
            .field("queue_capacity_packets", &self.queue_capacity_packets);
        if self.drop_mark != DropMarkPolicy::TailDrop {
            debug.field("drop_mark", &self.drop_mark);
        }
        if self.pfc.is_some() {
            debug.field("pfc", &self.pfc);
        }
        debug
            .field("queue", &self.queue)
            .field("in_service", &self.in_service)
            .field("tx_ready_pending", &self.tx_ready_pending)
            .finish()
    }
}

/// Per-priority pause state owned by one switch egress queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PfcQueueState {
    /// Priorities whose waiting packets are ineligible at service start.
    pub paused_priorities: [bool; 8],
    /// Downstream buffer monitors, one for each PFC-controlled incoming link feeding this queue.
    pub ingresses: Vec<PfcIngressState>,
}

/// Exact byte-accounting state for one PFC-controlled incoming link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PfcIngressState {
    pub controlled_link: LinkId,
    /// Index of the reverse control lane in `SimulationImage::channels`.
    pub control_channel_index: u32,
    pub buffer_capacity_bytes: [u64; 8],
    pub max_frame_bytes: u64,
    /// A zero XOFF threshold disables PFC for that priority.
    pub xoff_threshold_bytes: [u64; 8],
    pub xon_threshold_bytes: [u64; 8],
    pub occupancy_bytes: [u64; 8],
    pub pause_asserted: [bool; 8],
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
#[derive(Clone, Eq, PartialEq)]
pub struct FlowDescriptor {
    pub id: FlowId,
    pub source: NodeId,
    pub target: NodeId,
    /// IEEE 802.1Q priority code point used by link-level eligibility mechanisms.
    pub priority: u8,
    /// Canonical data-packet route from source to target.
    pub route: Vec<LinkId>,
    /// Canonical feedback-packet route from target back to source.
    pub reverse_route: Vec<LinkId>,
}

impl fmt::Debug for FlowDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("FlowDescriptor");
        debug
            .field("id", &self.id)
            .field("source", &self.source)
            .field("target", &self.target);
        if self.priority != 0 {
            debug.field("priority", &self.priority);
        }
        debug
            .field("route", &self.route)
            .field("reverse_route", &self.reverse_route)
            .finish()
    }
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
// Image state is deliberately pointer-free and Copy across every backend boundary.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowGeneratorKind {
    Constant(ConstantGenerator),
    Tcp(TcpGenerator),
    Rate(RateGenerator),
}

/// Exact rational rate source paced by the M3 fallback-heap timer.
///
/// Credit uses `rate_denominator * 1_000_000_000` quanta per bit. Each pacing tick adds
/// `rate_numerator_bits_per_second * pacing_interval_ns` quanta and one packet is emitted when the
/// accumulated credit covers `packet_size_bytes * 8` bits.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateGenerator {
    pub first_pacing_time_ns: u64,
    pub pacing_interval_ns: u64,
    pub packet_size_bytes: u64,
    pub total_bytes: u64,
    pub rate_numerator_bits_per_second: u64,
    pub rate_denominator: u64,
    pub credit_quanta: u128,
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

/// One active source-owned retransmission timer.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpTimerState {
    pub attempt: PayloadId,
    pub sequence: u64,
    pub deadline_ns: u64,
    pub generation: u64,
    pub rto_ns: u64,
}

/// Fixed-width source-owned TCP transport and congestion state.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpGenerator {
    pub total_bytes: u64,
    pub mss_bytes: u64,
    pub ack_size_bytes: u64,
    pub next_sequence: u64,
    pub highest_ack: u64,
    pub bytes_in_flight: u64,
    pub duplicate_acks: u64,
    pub recovery_high_sequence: u64,
    pub last_attempt: PayloadId,
    pub timer_generation: u64,
    pub active_timer: Option<TcpTimerState>,
    pub srtt_ns: u64,
    pub rtt_var_ns: u64,
    pub rto_ns: u64,
    pub control: TcpCongestionControl,
}

impl TcpGenerator {
    pub const INITIAL_RTO_NS: u64 = 1_000_000_000;

    pub const fn new(
        total_bytes: u64,
        mss_bytes: u64,
        ack_size_bytes: u64,
        control: TcpCongestionControl,
    ) -> Self {
        Self {
            total_bytes,
            mss_bytes,
            ack_size_bytes,
            next_sequence: 0,
            highest_ack: 0,
            bytes_in_flight: 0,
            duplicate_acks: 0,
            recovery_high_sequence: 0,
            last_attempt: PayloadId(0),
            timer_generation: 0,
            active_timer: None,
            srtt_ns: 0,
            rtt_var_ns: 0,
            rto_ns: Self::INITIAL_RTO_NS,
            control,
        }
    }
}

/// One target-side received byte range, represented half-open as `[start, end)`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpReceiveRange {
    pub start: u64,
    pub end: u64,
}

/// Target-owned cumulative ACK state for one TCP flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpReceiverState {
    pub flow: FlowId,
    pub ack_size_bytes: u64,
    pub next_expected_sequence: u64,
    pub out_of_order: Vec<TcpReceiveRange>,
}

impl TcpReceiverState {
    pub const fn new(flow: FlowId, ack_size_bytes: u64) -> Self {
        Self {
            flow,
            ack_size_bytes,
            next_expected_sequence: 0,
            out_of_order: Vec::new(),
        }
    }
}

/// Immutable per-packet data referenced by a persistent event payload.
///
/// Lowered images retain only the first scheduled packet for each active flow. Later records are
/// produced by the source generator and live only while the packet is scheduled or in flight.
#[repr(C)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PacketDescriptor {
    pub id: PayloadId,
    pub flow: FlowId,
    pub size_bytes: u64,
    /// Congestion-experienced bit carried unchanged across every hop.
    pub ecn_marked: bool,
    pub kind: PacketKind,
}

impl fmt::Debug for PacketDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("PacketDescriptor");
        debug
            .field("id", &self.id)
            .field("flow", &self.flow)
            .field("size_bytes", &self.size_bytes);
        if self.ecn_marked {
            debug.field("ecn_marked", &self.ecn_marked);
        }
        debug.field("kind", &self.kind).finish()
    }
}

/// PFC control payload carried by an ordinary reverse-channel `RemoteArrival`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PfcHeader {
    pub controlled_link: LinkId,
    pub priority: u8,
    pub pause: bool,
}

/// TCP data metadata independent of the transmission-attempt `PayloadId`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpDataHeader {
    pub sequence: u64,
    pub sent_time_ns: u64,
    pub retransmission: bool,
}

/// TCP cumulative-ACK metadata carried by a feedback packet.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpAckHeader {
    pub acknowledgment: u64,
    pub acknowledged_bytes: u64,
    pub echoed_sent_time_ns: u64,
}

/// Closed packet direction used to route ordinary data and feedback packets.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketKind {
    Data = 0,
    Feedback = 1,
    TcpData(TcpDataHeader) = 2,
    TcpAck(TcpAckHeader) = 3,
    Pfc(PfcHeader) = 4,
}

impl PacketKind {
    pub const fn is_data(self) -> bool {
        matches!(self, Self::Data | Self::TcpData(_))
    }

    pub const fn is_feedback(self) -> bool {
        matches!(self, Self::Feedback | Self::TcpAck(_) | Self::Pfc(_))
    }

    pub const fn code(self) -> u8 {
        match self {
            Self::Data => 0,
            Self::Feedback => 1,
            Self::TcpData(_) => 2,
            Self::TcpAck(_) => 3,
            Self::Pfc(_) => 4,
        }
    }
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

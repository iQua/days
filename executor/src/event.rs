//! Canonical event records shared by every executor backend.

/// Stable identifier for one logical process.
///
/// Hosts own one logical process. Each switch egress port owns a distinct logical process derived
/// from the semantic `(physical switch, directed egress link)` key. The value must not depend on
/// allocation order, global counters, pointer values, or map iteration order.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

/// Stable identifier for a directed topology link.
///
/// Scenario lowering derives this value from a semantic topology key.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LinkId(pub u64);

/// Stable identifier for one semantic traffic flow.
///
/// Scenario lowering assigns this after sorting canonical flow keys.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FlowId(pub u64);

/// Stable identifier for an event payload.
///
/// The identifier is fixed-width; payload storage remains outside the persistent event record.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PayloadId(pub u64);

impl PayloadId {
    /// Allocates a globally unique packet identity from one node's monotone local sequence.
    ///
    /// The final local sequence value is reserved so the owning state can always represent the
    /// next (possibly exhausted) cursor without a second flag.
    pub fn from_node_sequence(
        source: NodeId,
        node_count: u64,
        local_sequence: u64,
    ) -> Option<Self> {
        local_sequence
            .checked_add(1)
            .and_then(|_| local_sequence.checked_mul(node_count))
            .and_then(|base| base.checked_add(source.0))
            .map(Self)
    }
}

/// Total canonical ordering key for persistent events.
///
/// Ordering is lexicographic in declaration order. Producers make keys unique by allocating
/// `origin_seq` per node in deterministic child-emission order.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventKey {
    pub time_ns: u64,
    pub phase: u16,
    pub origin_node: NodeId,
    pub origin_seq: u64,
}

/// Closed set of event transitions shared by the FIFO, SP, and WFQ models.
#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventKind {
    /// A source generator transition makes a packet available at its host.
    PacketArrival = 0,
    /// An egress link may select at most one queued packet for service.
    TxReady = 1,
    /// A committed non-preemptive transmission finishes at its source.
    TxComplete = 2,
    /// A transmitted packet reaches the route-selected target logical process.
    RemoteArrival = 3,
    /// A source-owned TCP retransmission timer fires.
    ///
    /// This kind is deliberately not assigned to a monotone producer stream. It is the first
    /// runtime client of the exact stream-FEL fallback heap retained by T15f.
    RetransmissionTimeout = 4,
    /// A source-owned exact rate pacing timer fires.
    PacingTimer = 5,
}

/// Physical FEL class used by the stream decomposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventFelClass {
    Channel,
    Service,
    Generator,
    FallbackHeap,
}

/// Classifies persistent events without backend-specific assumptions.
pub const fn event_fel_class(kind: EventKind) -> EventFelClass {
    match kind {
        EventKind::RemoteArrival => EventFelClass::Channel,
        EventKind::TxReady | EventKind::TxComplete => EventFelClass::Service,
        EventKind::PacketArrival => EventFelClass::Generator,
        EventKind::RetransmissionTimeout | EventKind::PacingTimer => EventFelClass::FallbackHeap,
    }
}

/// Returns the canonical equal-time phase for a closed v1 event kind.
///
/// Arrivals are visible before a transmission completes, and completion is visible before the
/// next service selection. Time remains the primary ordering component.
pub const fn event_phase(kind: EventKind) -> u16 {
    match kind {
        EventKind::PacketArrival | EventKind::RemoteArrival => 0,
        EventKind::TxComplete | EventKind::RetransmissionTimeout | EventKind::PacingTimer => 1,
        EventKind::TxReady => 2,
    }
}

/// Fixed-width persistent event record.
///
/// The record contains no callback, trait object, pointer, reference-counted owner, or
/// backend-specific handle.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Event {
    pub key: EventKey,
    pub target: NodeId,
    pub kind: EventKind,
    pub payload: PayloadId,
}

/// Returns whether a locally emitted child can execute directly after its completion parent.
///
/// The strict comparison with the current LP minimum preserves both canonical ordering and the
/// existing duplicate-key diagnostic. Remote events cannot interpose because safe-horizon drains
/// buffer them until the round barrier.
pub(crate) fn is_same_time_tx_ready_continuation(
    parent: Event,
    child: Event,
    local_node: NodeId,
    next_key: Option<EventKey>,
) -> bool {
    parent.kind == EventKind::TxComplete
        && child.target == local_node
        && child.kind == EventKind::TxReady
        && child.key.time_ns == parent.key.time_ns
        && child.key.phase == event_phase(EventKind::TxReady)
        && next_key.is_none_or(|next| child.key < next)
}

//! Canonical event records shared by every executor backend.

/// Stable identifier for a physical topology node.
///
/// Scenario lowering derives this value from a semantic topology key. It must not depend on
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

/// Closed set of event transitions in the v1 FIFO model.
#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum EventKind {
    /// A precomputed input makes a packet available at a node.
    PacketArrival = 0,
    /// An egress link may select at most one queued packet for service.
    TxReady = 1,
    /// A committed non-preemptive transmission finishes at its source.
    TxComplete = 2,
    /// A transmitted packet reaches the target node.
    RemoteArrival = 3,
}

/// Returns the canonical equal-time phase for a closed v1 event kind.
///
/// Arrivals are visible before a transmission completes, and completion is visible before the
/// next service selection. Time remains the primary ordering component.
pub const fn event_phase(kind: EventKind) -> u16 {
    match kind {
        EventKind::PacketArrival | EventKind::RemoteArrival => 0,
        EventKind::TxComplete => 1,
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

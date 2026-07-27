//! Immutable, backend-neutral simulation image records.

use crate::{Event, EventKind, LinkId, NodeId, SchedulerKind};

/// The plan's default constant propagation delay for a directed link.
pub const fn default_propagation_ns() -> u64 {
    0
}

/// Immutable configuration of one node-owned FIFO/TailDrop queue.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Node {
    pub id: NodeId,
    pub scheduler: SchedulerKind,
    /// Maximum queued packets. A value of zero denotes an unbounded queue.
    pub queue_capacity_packets: u64,
}

/// Immutable configuration of one constant-rate, directed, non-preemptive link.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Link {
    pub id: LinkId,
    pub source: NodeId,
    pub target: NodeId,
    pub rate_bps: u64,
    pub propagation_ns: u64,
}

/// Declared lower-bound channel for one kind of cross-node event.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteChannel {
    pub source: NodeId,
    pub target: NodeId,
    pub event_kind: EventKind,
    pub min_delay_ns: u64,
}

/// Immutable input data shared by all executor backends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationImage {
    pub nodes: Vec<Node>,
    pub links: Vec<Link>,
    pub channels: Vec<RemoteChannel>,
    pub initial_events: Vec<Event>,
    pub seed: u64,
}

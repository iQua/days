//! Immutable, backend-neutral simulation image records.

use crate::{
    Event, EventKind, LinkId, NodeId, NodeKind, SchedulerKind, TimeError, link_arrival_time_ns,
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
///
/// T5 establishes the role-specific arena. T6 adds the state required by host FIFO execution.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostState;

/// Switch-owned FIFO/TailDrop configuration.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SwitchState {
    pub scheduler: SchedulerKind,
    /// Maximum queued packets. A value of zero denotes an unbounded queue.
    pub queue_capacity_packets: u64,
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

/// One immutable semantic image containing every host and switch logical process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationImage {
    pub nodes: Vec<NodeDescriptor>,
    pub host_states: Vec<HostState>,
    pub switch_states: Vec<SwitchState>,
    pub links: Vec<LinkDescriptor>,
    pub channels: Vec<RemoteChannel>,
    pub initial_events: Vec<Event>,
    pub seed: u64,
}

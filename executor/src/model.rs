//! Closed model choices supported by the v1 simulation image.

use crate::EventKind;

/// Semantic role of a logical process in the heterogeneous image.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NodeKind {
    Host = 0,
    Switch = 1,
}

/// Scheduling discipline selected for a node-owned queue.
///
/// TailDrop is the only v1 admission policy, so it does not need a second open-ended selector.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerKind {
    Fifo = 0,
    /// Reserved closed-model value for the later SP phase.
    StaticPriority = 1,
    /// Reserved closed-model value for the later WFQ phase.
    WeightedFairQueue = 2,
}

/// Symbolic transition selected by `(NodeKind, EventKind)`.
///
/// This enum defines the closed dispatch shape only. It contains no callback or handler body.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionHandler {
    HostPacketArrival = 0,
    HostTxReady = 1,
    HostTxComplete = 2,
    HostRemoteArrival = 3,
    SwitchTxReady = 4,
    SwitchTxComplete = 5,
    SwitchRemoteArrival = 6,
}

/// Resolves a role/event pair to its supported v1 transition handler.
///
/// A precomputed `PacketArrival` is host injection and is therefore rejected for switches.
/// Validation of image events against this table belongs to T8.
pub const fn resolve_transition(
    node_kind: NodeKind,
    event_kind: EventKind,
) -> Option<TransitionHandler> {
    match (node_kind, event_kind) {
        (NodeKind::Host, EventKind::PacketArrival) => Some(TransitionHandler::HostPacketArrival),
        (NodeKind::Host, EventKind::TxReady) => Some(TransitionHandler::HostTxReady),
        (NodeKind::Host, EventKind::TxComplete) => Some(TransitionHandler::HostTxComplete),
        (NodeKind::Host, EventKind::RemoteArrival) => Some(TransitionHandler::HostRemoteArrival),
        (NodeKind::Switch, EventKind::PacketArrival) => None,
        (NodeKind::Switch, EventKind::TxReady) => Some(TransitionHandler::SwitchTxReady),
        (NodeKind::Switch, EventKind::TxComplete) => Some(TransitionHandler::SwitchTxComplete),
        (NodeKind::Switch, EventKind::RemoteArrival) => {
            Some(TransitionHandler::SwitchRemoteArrival)
        }
    }
}

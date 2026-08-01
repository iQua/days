//! Closed model choices supported by the v1 simulation image.

use std::collections::BTreeMap;

use num_bigint::BigUint;
use num_rational::Ratio;

use crate::{EventKind, PayloadId};

/// Semantic role of a logical process in the heterogeneous image.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NodeKind {
    Host = 0,
    Switch = 1,
}

/// Exact nonnegative rational used by the WFQ semantic state.
///
/// Both components are arbitrary-precision integers. Normal construction keeps the denominator
/// positive and reduces the fraction without floating-point rounding; image validation rejects
/// malformed `Ratio::new_raw` values with a zero denominator.
pub type ExactRational = Ratio<BigUint>;

/// Mutable exact state of one Weighted Fair Queue.
///
/// `virtual_time` and `finish_times` are normalized by the queue's link rate. This removes the
/// common `rate_bps` denominator from packet finish increments while retaining the exact
/// mathematical ordering of the legacy recurrence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WfqSchedulerState {
    pub weights: Vec<u64>,
    pub virtual_time: ExactRational,
    pub last_updated_ns: u64,
    pub finish_times: Vec<ExactRational>,
    /// Queued plus in-service packets by class, matching legacy active-set accounting.
    pub active_packets: Vec<u64>,
    /// Finish tags for waiting plus in-service packets. Selection retains the chosen packet's tag
    /// until completion so checkpoint validation can close its class finish history exactly.
    pub packet_finish_times: BTreeMap<PayloadId, ExactRational>,
}

/// Mutable exact state of one Deficit Round Robin scheduler.
///
/// Quanta and deficits are bytes. `current_class` is the next class inspected at service start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrrSchedulerState {
    pub quanta_bytes: Vec<u64>,
    pub deficits_bytes: Vec<u64>,
    pub current_class: u64,
}

impl DrrSchedulerState {
    pub fn new(quanta_bytes: Vec<u64>) -> Self {
        Self {
            deficits_bytes: vec![0; quanta_bytes.len()],
            quanta_bytes,
            current_class: 0,
        }
    }
}

/// Mutable exact state of one packet-count Weighted Round Robin scheduler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrrSchedulerState {
    pub weights: Vec<u64>,
    pub packets_sent_in_round: Vec<u64>,
    pub current_class: u64,
}

impl WrrSchedulerState {
    pub fn new(weights: Vec<u64>) -> Self {
        Self {
            packets_sent_in_round: vec![0; weights.len()],
            weights,
            current_class: 0,
        }
    }
}

/// Queue-depth unit used by deterministic admission and marking policies.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueDepthUnit {
    Packets = 0,
    Bytes = 1,
}

/// Deterministic ECN threshold configuration.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EcnThresholdPolicy {
    pub unit: QueueDepthUnit,
    pub capacity: u64,
    /// The arriving packet is marked when post-enqueue depth is at least this value.
    pub threshold: u64,
}

/// Exact deterministic RED state.
///
/// Between the thresholds, `counter` replaces the legacy random draw. The discrete signaling
/// rule is documented beside `drop_mark_decision` in the scalar transition.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedPolicyState {
    pub unit: QueueDepthUnit,
    pub capacity: u64,
    pub min_threshold: u64,
    pub max_threshold: u64,
    pub max_probability_numerator: u64,
    pub max_probability_denominator: u64,
    /// Queue-average numerator at the fixed scale `2^32`.
    pub average_scaled: u128,
    pub counter: u64,
    /// Signal by setting the packet mark instead of dropping it.
    pub mark_ecn: bool,
}

/// Admission/marking hook evaluated exactly once on switch enqueue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DropMarkPolicy {
    /// Preserve the v1 packet-capacity TailDrop behavior.
    #[default]
    TailDrop,
    EcnThreshold(EcnThresholdPolicy),
    Red(RedPolicyState),
}

impl WfqSchedulerState {
    pub fn new(weights: Vec<u64>) -> Self {
        let zero = Ratio::from_integer(BigUint::from(0_u8));
        Self {
            finish_times: vec![zero.clone(); weights.len()],
            active_packets: vec![0; weights.len()],
            weights,
            virtual_time: zero,
            last_updated_ns: 0,
            packet_finish_times: BTreeMap::new(),
        }
    }
}

/// Scheduling discipline and discipline-owned state for a node-owned queue.
///
/// Admission/marking is an orthogonal closed selector in `SwitchQueueState::drop_mark`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerKind {
    Fifo,
    StaticPriority { priorities: Vec<u64> },
    WeightedFairQueue(WfqSchedulerState),
    DeficitRoundRobin(DrrSchedulerState),
    WeightedRoundRobin(WrrSchedulerState),
}

impl SchedulerKind {
    pub fn static_priority(priorities: Vec<u64>) -> Self {
        Self::StaticPriority { priorities }
    }

    pub fn weighted_fair_queue(weights: Vec<u64>) -> Self {
        Self::WeightedFairQueue(WfqSchedulerState::new(weights))
    }

    pub fn deficit_round_robin(quanta_bytes: Vec<u64>) -> Self {
        Self::DeficitRoundRobin(DrrSchedulerState::new(quanta_bytes))
    }

    pub fn weighted_round_robin(weights: Vec<u64>) -> Self {
        Self::WeightedRoundRobin(WrrSchedulerState::new(weights))
    }

    /// Stable device-facing tag shared by the Metal and CUDA scheduler planes.
    pub const fn code(&self) -> u8 {
        match self {
            Self::Fifo => 0,
            Self::StaticPriority { .. } => 1,
            Self::WeightedFairQueue(_) => 2,
            Self::DeficitRoundRobin(_) => 3,
            Self::WeightedRoundRobin(_) => 4,
        }
    }

    pub const fn label(&self) -> &'static str {
        match self {
            Self::Fifo => "FIFO",
            Self::StaticPriority { .. } => "SP",
            Self::WeightedFairQueue(_) => "WFQ",
            Self::DeficitRoundRobin(_) => "DRR",
            Self::WeightedRoundRobin(_) => "WRR",
        }
    }
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
    HostRetransmissionTimeout = 7,
    HostPacingTimer = 8,
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
        (NodeKind::Host, EventKind::RetransmissionTimeout) => {
            Some(TransitionHandler::HostRetransmissionTimeout)
        }
        (NodeKind::Host, EventKind::PacingTimer) => Some(TransitionHandler::HostPacingTimer),
        (NodeKind::Switch, EventKind::PacketArrival) => None,
        (NodeKind::Switch, EventKind::TxReady) => Some(TransitionHandler::SwitchTxReady),
        (NodeKind::Switch, EventKind::TxComplete) => Some(TransitionHandler::SwitchTxComplete),
        (NodeKind::Switch, EventKind::RemoteArrival) => {
            Some(TransitionHandler::SwitchRemoteArrival)
        }
        (NodeKind::Switch, EventKind::RetransmissionTimeout) => None,
        (NodeKind::Switch, EventKind::PacingTimer) => None,
    }
}

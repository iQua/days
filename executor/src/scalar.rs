//! Canonical serial priority-queue execution.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use num_bigint::BigUint;
use num_rational::Ratio;

use crate::stage_index::{HostStageIndex, HostStageSlot, ProbedTable, StageScanProbe};
use crate::{
    Event, EventKey, EventKind, FlowGeneratorKind, FlowId, GeneratorFeedbackAction,
    GeneratorStatus, GeneratorTermination, HostState, LinkId, NodeDescriptor, NodeId, NodeKind,
    PacketDescriptor, PacketKind, PayloadId, SchedulerKind, SimulationImage, SwitchState,
    TcpAckHeader, TcpCongestionControl, TcpDataHeader, TcpReceiveRange, TcpTimerState, TimeError,
    TransitionHandler, WfqSchedulerState, event_phase, resolve_transition,
};

/// Outcome of one remote packet arrival at a switch queue or sink host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrivalDisposition {
    Admitted,
    Dropped,
    Delivered,
    Feedback,
}

/// One completed non-preemptive transmission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketDeparture {
    pub payload: PayloadId,
    /// Serialization completion time. Propagation begins after this logical departure.
    pub time_ns: u64,
}

/// One processed remote packet arrival.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PacketArrivalObservation {
    pub payload: PayloadId,
    pub time_ns: u64,
    pub disposition: ArrivalDisposition,
}

/// Congestion-control input retained for exact LeanGuard transition replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcpTransitionInput {
    NewAck {
        acknowledged_bytes: u64,
        rtt_sample_ns: u64,
        flight_size_bytes: u64,
        acknowledgment: u64,
    },
    DuplicateAck {
        flight_size_bytes: u64,
        recovery_high_sequence: u64,
    },
    Timeout {
        flight_size_bytes: u64,
    },
}

/// One scalar congestion-state transition, keyed by the event that caused it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub flow: FlowId,
    pub mss_bytes: u64,
    pub input: TcpTransitionInput,
    pub before: TcpCongestionControl,
    pub after: TcpCongestionControl,
}

/// Closed admission result retained by an exact AQM transition certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AqmTransitionAction {
    Enqueue,
    Mark,
    Drop,
}

/// One ECN-ramp enqueue decision, keyed by the event that caused it. The policy is immutable, so
/// one copy names it; LeanGuard recomputes the decision and its draw from the row and the image
/// seed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AqmTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    /// Stable index of the queue within the node-owned switch state.
    pub queue_id: u64,
    pub payload: PayloadId,
    pub queued_bytes_before: u64,
    pub packet_size_bytes: u64,
    pub ecn_before: bool,
    pub ecn_after: bool,
    pub policy: crate::EcnRampPolicy,
    pub action: AqmTransitionAction,
}

/// Whether the scalar oracle retains complete per-packet observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ObservationMode {
    #[default]
    Summary,
    Full,
}

/// Constant-space counters accumulated regardless of observation mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RunSummary {
    pub sourced_packets: u128,
    pub sourced_bytes: u128,
    pub departed_packets: u128,
    pub departed_bytes: u128,
    pub admitted_packets: u128,
    pub admitted_bytes: u128,
    pub received_packets: u128,
    pub received_bytes: u128,
    pub dropped_packets: u128,
    pub dropped_bytes: u128,
    pub feedback_packets: u128,
    pub feedback_bytes: u128,
}

/// Exact transition records retained by reference-lane full observation.
///
/// Device backends intentionally omit these diagnostic planes while still returning complete
/// state and measurement planes. [`RunResult::diagnostics`] therefore distinguishes an omitted
/// diagnostic plane from a present plane containing no records.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DiagnosticPlanes {
    /// Exact TCP control transitions retained in full observation mode.
    pub tcp_transitions: Vec<TcpTransitionRecord>,
    /// Exact RED/ECN enqueue transitions retained in full observation mode.
    pub aqm_transitions: Vec<AqmTransitionRecord>,
    /// Exact rate/PFC/DRR/WRR/collective transitions retained in full observation mode.
    pub mechanism_transitions: Vec<crate::MechanismTransitionRecord>,
}

/// Complete normalized scalar state after reaching a configured endpoint or execution horizon.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunResult {
    pub host_states: Vec<HostState>,
    pub switch_states: Vec<SwitchState>,
    pub summary: RunSummary,
    /// Packet data required to resume execution.
    ///
    /// In addition to descriptors referenced by live packet positions, this includes canonical
    /// TCP segment-ledger seeds. On reload, constructors seed `(flow, sequence, size_bytes)` and
    /// apply each sender's `highest_ack` with the same partial-segment split used for live ACKs.
    /// Unreferenced TCP data descriptors are ledger-only and are not installed as resident packets.
    pub resident_packets: Vec<PacketDescriptor>,
    /// Complete packet descriptors referenced by full-mode observations.
    pub observed_packets: Vec<PacketDescriptor>,
    pub departures: Vec<PacketDeparture>,
    pub arrivals: Vec<PacketArrivalObservation>,
    /// Reference-lane diagnostics, present only for scalar/CPU full observation.
    pub diagnostics: Option<DiagnosticPlanes>,
    /// Live unprocessed events in canonical `EventKey` order.
    ///
    /// Live, not retained: a retransmission timeout that stops being its flow's armed timer stops
    /// being pending state and is removed from the future-event list inside the transition that
    /// supersedes it, on every backend, so it is absent here even though its historical deadline
    /// has not passed. Imported timeouts that no armed timer owns are the exception — validation
    /// accepts them and they stay schedulable until they fire. Mechanism API errata E5
    /// (`days-gpu/plans/mechanism-api-v1.md`).
    pub pending_events: Vec<Event>,
}

/// Failure while executing an assumed-accepted image.
///
/// Callers validate serialized or externally constructed images before execution. These errors
/// retain checked runtime arithmetic and immediate transition preconditions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionError {
    DuplicateEventKey(EventKey),
    UnknownNode(NodeId),
    InvalidStateSlot {
        node: NodeId,
        kind: NodeKind,
        state_slot: u32,
    },
    UnsupportedTransition {
        node: NodeId,
        kind: NodeKind,
        event_kind: EventKind,
    },
    UnknownLink(LinkId),
    LinkSourceMismatch {
        link: LinkId,
        expected_source: NodeId,
        actual_source: NodeId,
    },
    UnknownPacket(PayloadId),
    DuplicatePayload(PayloadId),
    UnknownFlow(FlowId),
    UnknownGenerator {
        node: NodeId,
        flow: FlowId,
    },
    UnexpectedGeneratorEmission {
        node: NodeId,
        flow: FlowId,
        payload: PayloadId,
    },
    MissingTcpSegment {
        flow: FlowId,
        sequence: u64,
    },
    InconsistentTcpSegment {
        flow: FlowId,
        sequence: u64,
        original_size_bytes: u64,
        replacement_size_bytes: u64,
    },
    PayloadSequenceOverflow(NodeId),
    GeneratorTimeOverflow(FlowId),
    FlowRouteMiss {
        flow: FlowId,
        node: NodeId,
    },
    MissingSwitchQueue {
        node: NodeId,
        egress_link: Option<LinkId>,
    },
    InvalidSchedulerState(NodeId),
    MissingWfqFinishTag {
        node: NodeId,
        payload: PayloadId,
    },
    NonMonotoneWfqTime {
        node: NodeId,
        previous_ns: u64,
        current_ns: u64,
    },
    HostAlreadyTransmitting(NodeId),
    SwitchAlreadyTransmitting {
        node: NodeId,
        link: LinkId,
    },
    UnexpectedTxComplete {
        node: NodeId,
        expected: Option<PayloadId>,
        actual: PayloadId,
    },
    CounterOverflow(NodeId),
    OriginSequenceOverflow(NodeId),
    NonPositiveLookahead,
    RemoteEventBeforeHorizon {
        key: EventKey,
        exclusive_horizon_ns: u128,
    },
    EventBelowHorizonAfterDrain {
        key: EventKey,
        exclusive_horizon_ns: u128,
    },
    InvalidCpuConfig(&'static str),
    WorkerFailed {
        worker: usize,
        round: u64,
    },
    WorkerPanicked {
        worker: usize,
    },
    WorkerChannelDisconnected,
    OutboxCapacityExceeded {
        node: NodeId,
        capacity: usize,
    },
    NonMonotoneChild {
        parent: EventKey,
        child: EventKey,
    },
    /// A superseded retransmission timer had no scheduled event to remove.
    ///
    /// The live-state contract requires every armed timer to own exactly one pending event, so
    /// this reports a corrupted future-event index rather than a modelling condition.
    SupersededTimerMissing {
        node: NodeId,
        payload: PayloadId,
        deadline_ns: u64,
    },
    /// A RoCE packet disagrees with its queue pair: data beyond the flow's total, or an ACK or
    /// NACK beyond the bytes the sender has sent.
    InconsistentRocePacket {
        flow: FlowId,
        payload: PayloadId,
    },
    Time(TimeError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingCollectiveProgress {
    flow: FlowId,
    cause: crate::CollectiveActivationCause,
    cause_flow: FlowId,
    arrival_bytes: u64,
    /// Local predecessors complete before this transition.
    before_local_completed: u32,
    before_inbound_complete: bool,
    before_inbound_bytes: u64,
    /// Inbound rows: the arriving data segment `[segment_sequence, segment_sequence + segment_bytes)`.
    segment_sequence: u64,
    segment_bytes: u64,
    /// Local rows: the signal that completed the local predecessor.
    completion: CompletionSignal,
}

/// The event that completed a local predecessor, as certified in its successor's progress row.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompletionSignal {
    /// TCP: the completing ACK's cumulative acknowledgment. Zero for a compute timer.
    ack_number: u64,
    /// TCP: when the segment this ACK answers was sent (echoed by the ACK). Compute: when the
    /// timer was armed.
    origin_ns: u64,
    /// TCP: that segment's and this ACK's unloaded round trip, a lower bound on the completion
    /// time's distance from `origin_ns`. Compute: the timer duration, met exactly.
    delay_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CollectiveProgressContext {
    key: EventKey,
    ordinal: u64,
    node: NodeId,
    stop_time_ns: u64,
    activated: bool,
}

/// Transport stages write their MTU (and a RoCE stage its pacing interval); a compute stage writes
/// zero there, and the certificate writer names its inbound predecessors' transport (schema
/// Amendment 5) from the image.
fn collective_progress_record(
    context: CollectiveProgressContext,
    cause: PendingCollectiveProgress,
    generator: &crate::FlowGeneratorState,
    stage: Option<crate::CollectiveStage>,
) -> Option<crate::CollectiveProgressRecord> {
    let stage = stage?;
    let dependencies = stage.dependencies;
    let record = crate::CollectiveProgressRecord {
        key: context.key,
        ordinal: context.ordinal,
        node: context.node,
        flow: generator.flow,
        cause: cause.cause,
        cause_flow: cause.cause_flow,
        arrival_bytes: cause.arrival_bytes,
        collective_id: 0,
        algorithm: None,
        group_size: 0,
        declared_total_bytes: 0,
        rank: 0,
        phase: None,
        step: 0,
        chunk_offset_bytes: 0,
        chunk_bytes: 0,
        packet_size_bytes: 0,
        interval_ns: 0,
        stop_time_ns: context.stop_time_ns,
        inbound_predecessor_bytes: dependencies.inbound_predecessor_bytes,
        before_local_complete: cause.before_local_completed == dependencies.local.count(),
        before_local_completed: cause.before_local_completed,
        before_inbound_complete: cause.before_inbound_complete,
        before_inbound_bytes: cause.before_inbound_bytes,
        activated: context.activated,
        after_local_complete: dependencies.local_complete(),
        after_local_completed: dependencies.local_completed,
        after_inbound_complete: dependencies.inbound_complete(),
        after_inbound_bytes: dependencies.inbound_bytes_received,
        after_packets_emitted: generator.packets_emitted,
        after_bytes_emitted: generator.bytes_emitted,
        after_status: generator.next_emission.status,
        after_next_time_ns: generator.next_emission.departure_time_ns,
        stage_kind: crate::CollectiveStageKind::Tcp,
        duration_ns: 0,
        segment_sequence: cause.segment_sequence,
        segment_bytes: cause.segment_bytes,
        ack_number: cause.completion.ack_number,
        cause_origin_ns: cause.completion.origin_ns,
        cause_delay_ns: cause.completion.delay_ns,
    };
    match (generator.kind, stage.role) {
        (FlowGeneratorKind::Tcp(tcp), crate::StageRole::Collective(identity)) => {
            Some(crate::CollectiveProgressRecord {
                collective_id: identity.collective_id,
                algorithm: Some(identity.algorithm),
                group_size: identity.group_size,
                declared_total_bytes: identity.declared_total_bytes,
                rank: identity.rank,
                phase: Some(identity.phase),
                step: identity.step,
                chunk_offset_bytes: identity.chunk_offset_bytes,
                chunk_bytes: identity.chunk_bytes,
                packet_size_bytes: tcp.mss_bytes,
                ..record
            })
        }
        // Amendment 4: a RoCE stage writes its MTU and pacing interval.
        (FlowGeneratorKind::Roce(roce), crate::StageRole::Collective(identity)) => {
            Some(crate::CollectiveProgressRecord {
                collective_id: identity.collective_id,
                algorithm: Some(identity.algorithm),
                group_size: identity.group_size,
                declared_total_bytes: identity.declared_total_bytes,
                rank: identity.rank,
                phase: Some(identity.phase),
                step: identity.step,
                chunk_offset_bytes: identity.chunk_offset_bytes,
                chunk_bytes: identity.chunk_bytes,
                packet_size_bytes: roce.pacer.mtu_bytes,
                interval_ns: roce.pacer.pacing_interval_ns,
                stage_kind: crate::CollectiveStageKind::Roce,
                ..record
            })
        }
        (FlowGeneratorKind::Constant(constant), crate::StageRole::Collective(identity)) => {
            Some(crate::CollectiveProgressRecord {
                collective_id: identity.collective_id,
                algorithm: Some(identity.algorithm),
                group_size: identity.group_size,
                declared_total_bytes: identity.declared_total_bytes,
                rank: identity.rank,
                phase: Some(identity.phase),
                step: identity.step,
                chunk_offset_bytes: identity.chunk_offset_bytes,
                chunk_bytes: identity.chunk_bytes,
                packet_size_bytes: constant.packet_size_bytes,
                interval_ns: constant.interval_ns,
                stage_kind: crate::CollectiveStageKind::Notify,
                duration_ns: constant.interval_ns + constant.first_departure_ns,
                ..record
            })
        }
        (FlowGeneratorKind::Constant(_), crate::StageRole::Compute(compute)) => {
            Some(crate::CollectiveProgressRecord {
                collective_id: compute.compute_id,
                group_size: compute.group_size,
                rank: compute.rank,
                stage_kind: crate::CollectiveStageKind::Compute,
                duration_ns: compute.duration_ns,
                ..record
            })
        }
        _ => None,
    }
}

/// A host's state as the stage path may touch it.
///
/// The stage-path functions (listed in `xtask`'s `STAGE_PATH_FUNCTIONS`) receive this view from
/// `host_parts_mut`, never the raw `HostState`: its three tables are `ProbedTable`s, so any scan of
/// them is counted by the stage-scan probe, and the `xtask` stage-path audit rejects raw access.
/// The other fields are the host's own, borrowed in place.
struct HostView<'a> {
    generators: ProbedTable<'a, crate::FlowGeneratorState>,
    /// The host's stage table, parallel to `generators` or empty; read by generator position.
    stages: ProbedTable<'a, Option<crate::CollectiveStage>>,
    tcp_receivers: ProbedTable<'a, crate::TcpReceiverState>,
    queue: &'a mut std::collections::VecDeque<PayloadId>,
    in_service: &'a mut Option<PayloadId>,
    tx_ready_pending: &'a mut bool,
    next_payload_seq: &'a mut u64,
    sourced_packets: &'a mut u64,
    received_packets: &'a mut u64,
    /// Egress pause state under host-link PFC, or `None`.
    pfc: &'a mut Option<Box<crate::HostPfcState>>,
    /// Target-owned RoCE queue-pair receivers, read by binary search on the flow (never scanned).
    roce_receivers: &'a mut Option<Box<[crate::RoceReceiverState]>>,
}

/// The collective progress one event produced, taken by flow in push order.
///
/// The stage path pushes causes here as it records them, and reads them back only through `take`
/// and `into_remaining`, which count what they examine. `take` replaces
/// `causes.iter().position(|cause| cause.flow == flow)` followed by `causes.remove`: it returns the
/// first cause for `flow` not yet taken, which is the first match the shrinking vector held.
/// `into_remaining` yields the causes never taken in push order, the order the shrinking vector
/// kept.
#[derive(Default)]
struct PendingCauses {
    causes: Vec<Option<PendingCollectiveProgress>>,
    /// `(flow, push position)` of every cause; sorted by flow and then push position before the
    /// first `take` after a push.
    by_flow: Vec<(FlowId, usize)>,
    sorted: bool,
}

impl PendingCauses {
    fn push(&mut self, cause: PendingCollectiveProgress) {
        self.by_flow.push((cause.flow, self.causes.len()));
        self.causes.push(Some(cause));
        self.sorted = false;
    }

    fn is_empty(&self) -> bool {
        self.causes.is_empty()
    }

    fn take(
        &mut self,
        flow: FlowId,
        probe: &mut StageScanProbe,
    ) -> Option<PendingCollectiveProgress> {
        if !self.sorted {
            // Push positions are unique, so the unstable sort is deterministic.
            self.by_flow.sort_unstable();
            self.sorted = true;
        }
        let start = self.by_flow.partition_point(|(key, _)| *key < flow);
        let mut examined = 1_usize;
        for &(key, position) in &self.by_flow[start..] {
            if key != flow {
                break;
            }
            if let Some(cause) = self.causes[position].take() {
                probe.note(examined);
                return Some(cause);
            }
            examined += 1;
        }
        probe.note(examined);
        None
    }

    fn into_remaining(
        self,
        probe: &mut StageScanProbe,
    ) -> impl Iterator<Item = PendingCollectiveProgress> {
        probe.note(self.causes.len());
        self.causes.into_iter().flatten()
    }
}

/// Marks every stage whose local predecessor is `completed` as locally complete.
///
/// Causes are pushed in canonical `FlowId` order, the order of the host's generator table: the
/// index lists exactly the stages naming `completed` as their local predecessor, in table order.
fn complete_local_successors(
    generators: &ProbedTable<'_, crate::FlowGeneratorState>,
    stages: &mut ProbedTable<'_, Option<crate::CollectiveStage>>,
    index: &mut HostStageIndex,
    completed: FlowId,
    completion: CompletionSignal,
    causes: &mut PendingCauses,
) {
    let Some((successors, releasable)) = index.local_successors_of(completed) else {
        return;
    };
    for &position in successors {
        let successor = &generators[position];
        let Some(stage) = stages.stage_mut(position) else {
            continue;
        };
        let mut dependencies = stage.dependencies;
        // The index lists each successor once per predecessor, and a predecessor completes in
        // exactly one event, so each local edge is counted once.
        if dependencies.local_complete() {
            continue;
        }
        causes.push(PendingCollectiveProgress {
            flow: successor.flow,
            cause: crate::CollectiveActivationCause::LocalCompletion,
            cause_flow: completed,
            arrival_bytes: 0,
            before_local_completed: dependencies.local_completed,
            before_inbound_complete: dependencies.inbound_complete(),
            before_inbound_bytes: dependencies.inbound_bytes_received,
            segment_sequence: 0,
            segment_bytes: 0,
            completion,
        });
        dependencies.local_completed += 1;
        stage.dependencies = dependencies;
        releasable.refresh(position, Some(*stage));
    }
}

/// How delivery of an inbound predecessor's bytes was observed at this host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundProgress {
    /// One data segment `[sequence, sequence + bytes)` arrived and the receiver's in-order
    /// frontier advanced by `advance` bytes (zero for a duplicate or out-of-order segment).
    /// `sequence` is the segment's first byte: a TCP sequence number, or a RoCE PSN (a byte
    /// offset). Retransmitted or out-of-order bytes count only once the frontier covers them: a
    /// TCP receiver fills holes, a Go-back-N receiver advances only on the packet at its frontier.
    Segment {
        sequence: u64,
        bytes: u64,
        advance: u64,
    },
}

/// Advances every stage waiting on `inbound` as its inbound predecessor, in table order.
///
/// Force-inlined: every TCP data arrival calls it, and a host without stages returns at the stage
/// index's first test, so an out-of-line call would cost every TCP segment of every image a call.
#[inline(always)]
fn record_inbound_progress(
    generators: &ProbedTable<'_, crate::FlowGeneratorState>,
    stages: &mut ProbedTable<'_, Option<crate::CollectiveStage>>,
    index: &mut HostStageIndex,
    inbound: FlowId,
    progress: InboundProgress,
    node: NodeId,
    causes: &mut PendingCauses,
) -> Result<(), ExecutionError> {
    let Some((successors, releasable)) = index.inbound_successors_of(inbound) else {
        return Ok(());
    };
    for &position in successors {
        let generator = &generators[position];
        let Some(stage) = stages.stage_mut(position) else {
            continue;
        };
        let mut dependencies = stage.dependencies;
        if dependencies.inbound_complete() {
            continue;
        }
        let before_inbound_bytes = dependencies.inbound_bytes_received;
        let InboundProgress::Segment {
            sequence,
            bytes,
            advance: arrival_bytes,
        } = progress;
        causes.push(PendingCollectiveProgress {
            flow: generator.flow,
            cause: crate::CollectiveActivationCause::InboundArrival,
            cause_flow: inbound,
            arrival_bytes,
            before_local_completed: dependencies.local_completed,
            before_inbound_complete: false,
            before_inbound_bytes,
            segment_sequence: sequence,
            segment_bytes: bytes,
            completion: CompletionSignal::default(),
        });
        dependencies.inbound_bytes_received = dependencies
            .inbound_bytes_received
            .checked_add(arrival_bytes)
            .ok_or(ExecutionError::CounterOverflow(node))?;
        if dependencies.inbound_bytes_received > dependencies.inbound_predecessor_bytes {
            return Err(ExecutionError::CounterOverflow(node));
        }
        stage.dependencies = dependencies;
        releasable.refresh(position, Some(*stage));
    }
    Ok(())
}

impl fmt::Display for ExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateEventKey(key) => write!(formatter, "duplicate event key {key:?}"),
            Self::UnknownNode(node) => write!(formatter, "event targets unknown node {node:?}"),
            Self::InvalidStateSlot {
                node,
                kind,
                state_slot,
            } => write!(
                formatter,
                "node {node:?} has invalid {kind:?} state slot {state_slot}"
            ),
            Self::UnsupportedTransition {
                node,
                kind,
                event_kind,
            } => write!(
                formatter,
                "node {node:?} does not support {event_kind:?} as {kind:?}"
            ),
            Self::UnknownLink(link) => write!(formatter, "unknown link {link:?}"),
            Self::LinkSourceMismatch {
                link,
                expected_source,
                actual_source,
            } => write!(
                formatter,
                "link {link:?} source is {actual_source:?}, expected {expected_source:?}"
            ),
            Self::UnknownPacket(payload) => write!(formatter, "unknown packet {payload:?}"),
            Self::DuplicatePayload(payload) => {
                write!(formatter, "duplicate generated packet identity {payload:?}")
            }
            Self::UnknownFlow(flow) => write!(formatter, "unknown flow {flow:?}"),
            Self::UnknownGenerator { node, flow } => {
                write!(
                    formatter,
                    "host node {node:?} does not own generator for flow {flow:?}"
                )
            }
            Self::UnexpectedGeneratorEmission {
                node,
                flow,
                payload,
            } => write!(
                formatter,
                "host node {node:?} generator for flow {flow:?} did not schedule payload {payload:?}"
            ),
            Self::MissingTcpSegment { flow, sequence } => write!(
                formatter,
                "TCP flow {flow:?} has no recorded original segment at sequence {sequence}"
            ),
            Self::InconsistentTcpSegment {
                flow,
                sequence,
                original_size_bytes,
                replacement_size_bytes,
            } => write!(
                formatter,
                "TCP flow {flow:?} sequence {sequence} changed segment size from \
                 {original_size_bytes} to {replacement_size_bytes} bytes"
            ),
            Self::PayloadSequenceOverflow(node) => {
                write!(
                    formatter,
                    "payload identity sequence exhausted at node {node:?}"
                )
            }
            Self::GeneratorTimeOverflow(flow) => {
                write!(formatter, "generator time overflow for flow {flow:?}")
            }
            Self::FlowRouteMiss { flow, node } => {
                write!(
                    formatter,
                    "flow {flow:?} has no route step at node {node:?}"
                )
            }
            Self::MissingSwitchQueue { node, egress_link } => write!(
                formatter,
                "switch {node:?} has no queue for egress link {egress_link:?}"
            ),
            Self::InvalidSchedulerState(node) => {
                write!(formatter, "switch {node:?} has invalid scheduler state")
            }
            Self::MissingWfqFinishTag { node, payload } => write!(
                formatter,
                "switch {node:?} WFQ queue has no finish tag for waiting packet {payload:?}"
            ),
            Self::NonMonotoneWfqTime {
                node,
                previous_ns,
                current_ns,
            } => write!(
                formatter,
                "switch {node:?} WFQ time moved backward from {previous_ns} ns to {current_ns} ns"
            ),
            Self::HostAlreadyTransmitting(node) => {
                write!(
                    formatter,
                    "host {node:?} received TxReady while transmitting"
                )
            }
            Self::SwitchAlreadyTransmitting { node, link } => write!(
                formatter,
                "switch {node:?} received TxReady while link {link:?} is transmitting"
            ),
            Self::UnexpectedTxComplete {
                node,
                expected,
                actual,
            } => write!(
                formatter,
                "node {node:?} completed {actual:?}, expected {expected:?}"
            ),
            Self::CounterOverflow(node) => {
                write!(formatter, "state counter overflow at node {node:?}")
            }
            Self::OriginSequenceOverflow(node) => {
                write!(formatter, "origin sequence overflow at node {node:?}")
            }
            Self::NonPositiveLookahead => {
                write!(
                    formatter,
                    "safe-horizon execution requires every declared channel delay to be positive"
                )
            }
            Self::RemoteEventBeforeHorizon {
                key,
                exclusive_horizon_ns,
            } => write!(
                formatter,
                "remote event {key:?} precedes exclusive safe horizon {exclusive_horizon_ns}"
            ),
            Self::EventBelowHorizonAfterDrain {
                key,
                exclusive_horizon_ns,
            } => write!(
                formatter,
                "LP still has event {key:?} below exclusive safe horizon \
                 {exclusive_horizon_ns} after its drain"
            ),
            Self::InvalidCpuConfig(message) => {
                write!(formatter, "invalid CPU executor configuration: {message}")
            }
            Self::WorkerFailed { worker, round } => {
                write!(formatter, "CPU worker {worker} failed during round {round}")
            }
            Self::WorkerPanicked { worker } => {
                write!(formatter, "CPU worker {worker} panicked")
            }
            Self::WorkerChannelDisconnected => {
                write!(formatter, "CPU worker channel disconnected")
            }
            Self::OutboxCapacityExceeded { node, capacity } => write!(
                formatter,
                "LP {node:?} exceeded its configured outbox capacity of {capacity} events"
            ),
            Self::NonMonotoneChild { parent, child } => {
                write!(
                    formatter,
                    "child key {child:?} does not advance parent {parent:?}"
                )
            }
            Self::SupersededTimerMissing {
                node,
                payload,
                deadline_ns,
            } => write!(
                formatter,
                "node {node:?} has no pending retransmission timeout for attempt {payload:?} at {deadline_ns} ns"
            ),
            Self::InconsistentRocePacket { flow, payload } => write!(
                formatter,
                "RoCE packet {payload:?} is inconsistent with flow {flow:?}'s queue pair"
            ),
            Self::Time(error) => error.fmt(formatter),
        }
    }
}

impl Error for ExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Time(error) => Some(error),
            _ => None,
        }
    }
}

impl From<TimeError> for ExecutionError {
    fn from(error: TimeError) -> Self {
        Self::Time(error)
    }
}

/// Runs the canonical scalar executor through the image's inclusive simulation stop.
///
/// `exclusive_horizon_ns` optionally limits a partial run to events strictly before that horizon.
/// The horizon is a safety boundary, unlike the scenario endpoint, so the two comparisons remain
/// intentionally distinct.
pub fn run_scalar(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
) -> Result<RunResult, ExecutionError> {
    run_scalar_with_observations(image, exclusive_horizon_ns, ObservationMode::Summary)
}

/// Runs the scalar executor with explicit full-record retention for oracle comparisons.
pub fn run_scalar_with_observations(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    observation_mode: ObservationMode,
) -> Result<RunResult, ExecutionError> {
    let (transitions, pending_events) =
        run_scalar_events(image, exclusive_horizon_ns, observation_mode, |_, _| {})?;
    Ok(transitions.finish(pending_events))
}

/// The Scalar event loop, handing `after_dispatch` the transition state after every event.
fn run_scalar_events<'image>(
    image: &'image SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    observation_mode: ObservationMode,
    mut after_dispatch: impl FnMut(&TransitionState<'image>, Event),
) -> Result<(TransitionState<'image>, Vec<Event>), ExecutionError> {
    let mut transitions = TransitionState::new(image, observation_mode)?;
    let mut events = initial_event_queue(image)?;
    let mut children = Vec::new();
    let mut superseded = Vec::new();

    while events.first_key_value().is_some_and(|(key, _)| {
        key.time_ns <= image.stop_time_ns
            && exclusive_horizon_ns.is_none_or(|horizon_ns| key.time_ns < horizon_ns)
    }) {
        let (_, event) = events
            .pop_first()
            .expect("first_key_value established a pending event");
        transitions.dispatch(event, &mut children)?;
        after_dispatch(&transitions, event);
        transitions.take_superseded_timers(&mut superseded);
        for timer in superseded.drain(..) {
            remove_superseded_timer(&mut events, timer)?;
        }
        for child in children.drain(..) {
            if events.insert(child.key, child).is_some() {
                return Err(ExecutionError::DuplicateEventKey(child.key));
            }
        }
    }

    Ok((transitions, events.into_values().collect()))
}

/// Test hook: a Scalar run with the number of events it dispatched and the number of
/// generator-table, TCP-receiver-table, index and pending-cause entries its stage path examined.
///
/// The run itself is `run_scalar_with_observations`; the probe only counts.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn run_scalar_counting_stage_scans_for_testing(
    image: &SimulationImage,
    observation_mode: ObservationMode,
) -> Result<(RunResult, u64, u64), ExecutionError> {
    let (transitions, pending_events) =
        run_scalar_events(image, None, observation_mode, |_, _| {})?;
    let dispatches = transitions.stage_probe.dispatches();
    let visits = transitions
        .hosts
        .slices()
        .1
        .iter()
        .map(HostStageSlot::visits)
        .fold(transitions.stage_probe.visits(), u64::saturating_add);
    Ok((transitions.finish(pending_events), dispatches, visits))
}

/// Test hook: a Scalar run with the service decisions made at PFC switch queues and the queued
/// entries the PFC service paths read (`PfcServiceProbe`).
///
/// The run itself is `run_scalar_with_observations`; the probe only counts.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn run_scalar_counting_pfc_service_for_testing(
    image: &SimulationImage,
    observation_mode: ObservationMode,
) -> Result<(RunResult, PfcServiceCounts), ExecutionError> {
    let (transitions, pending_events) =
        run_scalar_events(image, None, observation_mode, |_, _| {})?;
    let counts = transitions.pfc_service_probe.counts;
    Ok((transitions.finish(pending_events), counts))
}

/// Proves that the keyed stage path answers every query as the retired scans did, on `image`
/// and after every event of a Scalar run over it.
///
/// This is the P14 scan equality gate. On the image, every host's index is compared with the
/// retained scans (`stage_index::legacy_scans`) for every flow of the image; after each event, the
/// index of the host the event targeted — the only host a transition mutates — must equal a fresh
/// derivation from that host's state, and answer every flow its tables name as the scans do. The
/// run stops at `exclusive_horizon_ns` when given. The images need not validate: a run the executor
/// rejects part-way is checked up to its failing event. Returns the first disagreement, or else
/// the run's own outcome, which the caller compares with `run_scalar_with_observations`.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn assert_scalar_stage_index_equivalent_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
) -> Result<Result<RunResult, ExecutionError>, String> {
    use crate::stage_index::check_host_index;

    let image_flows = image.flows.iter().map(|flow| flow.id).collect::<Vec<_>>();
    let initial = match TransitionState::new(image, ObservationMode::Full) {
        Ok(initial) => initial,
        Err(error) => return Ok(Err(error)),
    };
    let (states, indices) = initial.hosts.slices();
    for (slot, (state, index)) in states.iter().zip(indices).enumerate() {
        check_host_index(
            state,
            &image.stage_joins,
            index.index(),
            image_flows.iter().copied(),
        )
        .map_err(|mismatch| format!("host slot {slot} of the image: {mismatch}"))?;
    }
    let mut first_mismatch = None;
    let run = run_scalar_events(
        image,
        exclusive_horizon_ns,
        ObservationMode::Full,
        |transitions, event| {
            if first_mismatch.is_some() {
                return;
            }
            let Ok(node) = transitions.node(event.target) else {
                return;
            };
            if node.kind != NodeKind::Host {
                return;
            }
            let slot = node.state_slot as usize;
            let (states, indices) = transitions.hosts.slices();
            if let Err(mismatch) =
                check_host_index(&states[slot], &image.stage_joins, indices[slot].index(), [])
            {
                first_mismatch = Some(format!(
                    "host {:?} after event {:?}: {mismatch}",
                    node.id, event.key
                ));
            }
        },
    );
    if let Some(mismatch) = first_mismatch {
        return Err(mismatch);
    }
    Ok(run.map(|(transitions, pending_events)| transitions.finish(pending_events)))
}

/// One host's semantic state and the executor-local stage index derived from it, held together
/// by a CPU host LP.
struct HostEntry {
    state: HostState,
    /// Keyed views of `state`'s generator and TCP-receiver tables; derived on construction and
    /// never serialized.
    index: HostStageSlot,
}

/// Every host's semantic state, and the stage index derived from each, held by the Scalar LP as
/// two parallel tables.
struct HostTables {
    /// The image's host states, cloned whole; `finish` hands this table back as the result's.
    states: Vec<HostState>,
    /// Keyed views of each state's generator and TCP-receiver tables, by state slot; derived on
    /// construction and never serialized.
    indices: Vec<HostStageSlot>,
}

/// The hosts a `TransitionState` owns, with their stage indices, shaped for the executor that owns
/// it.
///
/// A CPU LP owns at most one host: a host LP holds state and index in one allocation, and a switch
/// LP holds nothing. The Scalar LP owns every host in two tables, so its state table is the one it
/// cloned from the image and the result takes it back without a copy. All three present the same
/// parallel slices through [`HostStore::slices`], indexed by state slot. The store is two words,
/// which keeps `TransitionState` at `main`'s size.
enum HostStore {
    /// A switch LP of the CPU executor.
    Empty,
    /// A host LP of the CPU executor.
    Local(Box<HostEntry>),
    /// The Scalar executor's LP.
    Table(Box<HostTables>),
}

impl HostStore {
    /// The Scalar LP's store: the image's host states and an index built from each.
    fn tables(states: Vec<HostState>, joins: &[FlowId]) -> Self {
        let indices = states
            .iter()
            .map(|state| HostStageSlot::build(state, joins))
            .collect();
        Self::Table(Box::new(HostTables { states, indices }))
    }

    /// A CPU host LP's store: its one host and the index built from it.
    fn local(state: HostState, joins: &[FlowId]) -> Self {
        let index = HostStageSlot::build(&state, joins);
        Self::Local(Box::new(HostEntry { state, index }))
    }

    /// The host states and their indices, as parallel slices indexed by state slot.
    fn slices(&self) -> (&[HostState], &[HostStageSlot]) {
        match self {
            Self::Empty => (&[], &[]),
            Self::Local(entry) => (
                std::slice::from_ref(&entry.state),
                std::slice::from_ref(&entry.index),
            ),
            Self::Table(tables) => (&tables.states, &tables.indices),
        }
    }

    /// The host states and their indices, writable, as parallel slices indexed by state slot.
    fn slices_mut(&mut self) -> (&mut [HostState], &mut [HostStageSlot]) {
        match self {
            Self::Empty => (&mut [], &mut []),
            Self::Local(entry) => {
                let HostEntry { state, index } = &mut **entry;
                (std::slice::from_mut(state), std::slice::from_mut(index))
            }
            Self::Table(tables) => {
                let HostTables { states, indices } = &mut **tables;
                (states, indices)
            }
        }
    }

    /// A CPU host LP's one host state.
    fn into_local_state(self) -> Option<HostState> {
        match self {
            Self::Local(entry) => Some(entry.state),
            Self::Empty | Self::Table(_) => None,
        }
    }

    /// The host states in slot order; the Scalar LP's table is moved out as it is.
    fn into_states(self) -> Vec<HostState> {
        match self {
            Self::Empty => Vec::new(),
            Self::Local(entry) => vec![entry.state],
            Self::Table(tables) => tables.states,
        }
    }
}

pub(crate) struct TransitionState<'image> {
    image: &'image SimulationImage,
    hosts: HostStore,
    switch_states: Vec<SwitchState>,
    /// Executor-local redundant state, derived on construction and never serialized.
    switch_queue_bytes: Vec<Vec<u64>>,
    /// Executor-local class orders of the FIFO PFC queues that have had a class paused, by state
    /// slot and queue slot (`PfcClassOrder`); `None` until the first such queue, so an image whose
    /// queues are never paused carries one empty pointer and nothing else. Never serialized.
    switch_pfc_orders: Option<Box<PfcClassOrders>>,
    local_node: Option<NodeDescriptor>,
    packets: BTreeMap<PayloadId, ResidentPacket>,
    observation_mode: ObservationMode,
    summary: RunSummary,
    observed_packets: BTreeMap<PayloadId, PacketDescriptor>,
    departures: Vec<(EventKey, PacketDeparture)>,
    arrivals: Vec<(EventKey, PacketArrivalObservation)>,
    tcp_transitions: Vec<TcpTransitionRecord>,
    aqm_transitions: Vec<AqmTransitionRecord>,
    mechanism_transitions: Vec<crate::MechanismTransitionRecord>,
    tcp_sent_segments: crate::tcp_ledger::TcpSegmentLedger,
    /// Retransmission timers superseded by the transition currently in flight.
    ///
    /// The transition owns sender state; the event queue is owned by the driving backend. This
    /// buffer carries the eager-removal obligation across that boundary and is drained by the
    /// queue owner immediately after every dispatch.
    superseded_timers: Vec<SupersededTimer>,
    /// Test-only dispatch and pending-cause counts; empty in production builds.
    stage_probe: StageScanProbe,
    /// Test-only PFC service-decision counts; empty in production builds.
    pfc_service_probe: PfcServiceProbe,
}

/// Identity of a retransmission-timeout event that stopped being a flow's armed timer.
///
/// The identity is exactly the tuple the lazy pop-skip used to compare, so eager removal and the
/// retired recognition classify the same events. `origin_node` equals `target` for every
/// retransmission timeout, which bounds the ordered-map search to one canonical key range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SupersededTimer {
    pub target: NodeId,
    pub payload: PayloadId,
    pub deadline_ns: u64,
}

impl SupersededTimer {
    /// Returns the inclusive `EventKey` bounds that can hold this timer's event.
    fn key_bounds(self) -> (EventKey, EventKey) {
        let lower = EventKey {
            time_ns: self.deadline_ns,
            phase: event_phase(EventKind::RetransmissionTimeout),
            origin_node: self.target,
            origin_seq: 0,
        };
        let upper = EventKey {
            origin_seq: u64::MAX,
            ..lower
        };
        (lower, upper)
    }

    /// Returns whether `event` is a candidate carrier of this timer identity.
    fn matches(self, event: Event) -> bool {
        event.kind == EventKind::RetransmissionTimeout
            && event.target == self.target
            && event.payload == self.payload
    }
}

/// Removes the event carrying a superseded timer identity from one ordered future-event map.
///
/// The canonical owner of a timer identity is the minimum-`EventKey` event carrying it, which is
/// also the event the lazy recognition would have consumed first. Removal is therefore exact even
/// when a legacy image supplied indistinguishable duplicates.
pub(crate) fn remove_superseded_timer(
    events: &mut BTreeMap<EventKey, Event>,
    timer: SupersededTimer,
) -> Result<Event, ExecutionError> {
    let (lower, upper) = timer.key_bounds();
    let key = events
        .range(lower..=upper)
        .find(|(_, event)| timer.matches(**event))
        .map(|(key, _)| *key)
        .ok_or(ExecutionError::SupersededTimerMissing {
            node: timer.target,
            payload: timer.payload,
            deadline_ns: timer.deadline_ns,
        })?;
    Ok(events
        .remove(&key)
        .expect("range search established the key"))
}

#[derive(Clone, Copy)]
struct ResidentPacket {
    descriptor: PacketDescriptor,
    source_time_ns: Option<u64>,
    transmitters: u64,
    terminal: bool,
}

pub(crate) enum LocalNodeState {
    Host(HostState),
    Switch(SwitchState),
}

pub(crate) struct LocalTransitionResult {
    pub node: NodeDescriptor,
    pub state: LocalNodeState,
    pub summary: RunSummary,
    pub resident_packets: Vec<PacketDescriptor>,
    pub tcp_segment_ledger: Vec<PacketDescriptor>,
    pub observed_packets: Vec<PacketDescriptor>,
    pub departures: Vec<(EventKey, PacketDeparture)>,
    pub arrivals: Vec<(EventKey, PacketArrivalObservation)>,
    pub tcp_transitions: Vec<TcpTransitionRecord>,
    pub aqm_transitions: Vec<AqmTransitionRecord>,
    pub mechanism_transitions: Vec<crate::MechanismTransitionRecord>,
}

#[derive(Clone, Copy)]
struct ChildEmission {
    target: NodeId,
    kind: EventKind,
    payload: PayloadId,
    time_ns: u64,
}

#[derive(Clone, Copy)]
struct PfcFramePlan {
    channel_index: u32,
    flow: FlowId,
    header: crate::PfcHeader,
}

/// Which packet a switch egress queue may serve on one `TxReady`.
///
/// `Head` is the FIFO-order plan: every queued packet is eligible and the discipline always
/// selects position zero, so the plan is the queue head and nothing else has to be inspected.
/// `PfcFirst` is the plan of a PFC queue under a head-serving discipline: the discipline selects
/// position zero of the eligible packets, which is the first queued packet whose PFC class is not
/// paused (`pfc_first_eligible`), so only that packet is inspected. `Eligible` is the per-packet
/// scan that only the round-robin disciplines need, with or without PFC: they choose by class over
/// the whole eligible list. `Head` and `PfcFirst` select exactly the packet `Eligible` would on
/// the queues they serve, so the fast plans are a cost reduction, not a behavior change.
enum SwitchServicePlan {
    Head(Option<PacketDescriptor>),
    PfcFirst(Option<PfcSelection>),
    Eligible {
        positions: Vec<usize>,
        packets: Vec<PacketDescriptor>,
        priorities: Vec<usize>,
        incoming_links: Vec<Option<LinkId>>,
    },
}

/// The packet a `PfcFirst` plan serves: its queue position, record, PFC class and incoming link.
#[derive(Clone, Copy)]
struct PfcSelection {
    position: usize,
    packet: PacketDescriptor,
    priority: usize,
    incoming_link: Option<LinkId>,
}

/// The packets of one FIFO PFC queue by PFC class, in queue order, keyed by arrival order.
///
/// A FIFO queue appends every admitted packet (`switch_remote_arrival`) and removes only the
/// packet it serves, so queue order is arrival order. Numbering the packets in arrival order
/// (`next_seq`) therefore gives every queued packet a key that increases along the queue, and each
/// class's keys, kept in queue order, form a sorted sequence. Then:
/// - the first queued packet whose class is not paused is the smallest front key among the
///   classes that are not paused: one comparison per class, with no queued packet read;
/// - its queue position is the number of keys below its own, summed over the classes by binary
///   search, so the queue itself is not scanned either.
///
/// It is built from the queue when a class of the queue is first paused (a PFC PAUSE frame, or a
/// paused class in the state the run starts from), so a queue that is never paused pays nothing;
/// from then on every admission appends to it and every service removes the served packet. The
/// served packet is always the front of its class: it is either the queue head, whose key is the
/// smallest of all, or the packet the order itself selected.
struct PfcClassOrder {
    /// Key of the next admitted packet.
    next_seq: u64,
    /// Keys of each class's queued packets, in queue order.
    classes: [std::collections::VecDeque<u64>; 8],
}

impl PfcClassOrder {
    /// The order of an empty queue; `push` each queued packet's class, in queue order, to derive
    /// the order of a nonempty one.
    fn new() -> Self {
        Self {
            next_seq: 0,
            classes: Default::default(),
        }
    }

    /// Appends one admitted packet of `class`.
    fn push(&mut self, class: usize) {
        self.classes[class].push_back(self.next_seq);
        self.next_seq += 1;
    }

    /// Removes the served packet, the first queued packet of `class`, and returns its key.
    fn pop(&mut self, class: usize) -> Option<u64> {
        self.classes[class].pop_front()
    }

    /// The queue position of the first queued packet whose class `pfc` does not pause.
    fn first_unpaused_position(&self, pfc: &crate::PfcQueueState) -> Option<usize> {
        let first = self
            .classes
            .iter()
            .enumerate()
            .filter(|(class, _)| !pfc.is_paused(*class))
            .filter_map(|(_, keys)| keys.front().copied())
            .min()?;
        Some(
            self.classes
                .iter()
                .map(|keys| keys.partition_point(|key| *key < first))
                .sum(),
        )
    }
}

/// The class orders of a `TransitionState`'s FIFO PFC queues, by (state slot, queue slot); boxed
/// behind one pointer so that `TransitionState` does not grow (`transition_state_keeps_main_size`).
#[derive(Default)]
struct PfcClassOrders(BTreeMap<(usize, usize), PfcClassOrder>);

/// What construction derives from the switch queues it is given.
struct DerivedSwitchQueues {
    /// Each queue's byte counter, by state slot and queue slot.
    bytes: Vec<Vec<u64>>,
    /// The (state slot, queue slot) of each FIFO PFC queue that starts with a class paused, whose
    /// class order construction then builds (`ensure_pfc_order`).
    paused_fifo_queues: Vec<(usize, usize)>,
}

/// Whether any PFC class of a queue is paused.
fn pfc_any_paused(pfc: &crate::PfcQueueState) -> bool {
    pfc.paused_by_controller
        .iter()
        .any(|controllers| !controllers.is_empty())
}

/// Test-only count of the service decisions made at PFC switch queues and of the queued entries
/// they read.
///
/// An entry is *read* when the decision looks up its packet record (and from it the flow's PFC
/// class or the packet's incoming link): that per-entry work is what a whole-queue plan repeats on
/// every decision. Without the test hooks the probe is empty and `note` compiles to nothing, so the
/// count can neither cost a production run anything nor influence it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PfcServiceProbe {
    #[cfg(feature = "planner-test-hooks")]
    counts: PfcServiceCounts,
}

/// What `PfcServiceProbe` counts (test hooks only).
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PfcServiceCounts {
    /// `TxReady` decisions at PFC switch queues.
    pub decisions: u64,
    /// Queued entries the PFC service paths read.
    pub reads: u64,
    /// First-eligible decisions (FIFO, static priority and WFQ queues) that served a packet behind
    /// the queue head, past a paused class.
    pub past_head: u64,
}

impl PfcServiceProbe {
    /// Records one `TxReady` decision at a PFC queue that read `entries` queued entries and
    /// served the packet at queue position `served` (`None` when nothing was eligible).
    #[inline]
    fn note_decision(&mut self, entries: usize, served: Option<usize>) {
        let _ = served;
        #[cfg(feature = "planner-test-hooks")]
        {
            self.counts.decisions = self.counts.decisions.saturating_add(1);
            if served.is_some_and(|position| position > 0) {
                self.counts.past_head = self.counts.past_head.saturating_add(1);
            }
        }
        self.note_reads(entries);
    }

    /// Records `entries` queued entries read outside a `TxReady` decision: the first-eligible
    /// search after a transmission completes and on a PFC control frame.
    #[inline]
    fn note_reads(&mut self, entries: usize) {
        let _ = entries;
        #[cfg(feature = "planner-test-hooks")]
        {
            self.counts.reads = self
                .counts
                .reads
                .saturating_add(u64::try_from(entries).unwrap_or(u64::MAX));
        }
    }
}

/// Projects an eligible-packet list onto the record shape the round-robin certificates carry.
fn scheduler_packets(packets: &[PacketDescriptor]) -> Vec<crate::SchedulerPacket> {
    packets
        .iter()
        .map(|packet| crate::SchedulerPacket {
            payload: packet.id,
            flow: packet.flow,
            size_bytes: packet.size_bytes,
        })
        .collect()
}

const fn scheduler_packet(packet: PacketDescriptor) -> crate::SchedulerPacket {
    crate::SchedulerPacket {
        payload: packet.id,
        flow: packet.flow,
        size_bytes: packet.size_bytes,
    }
}

/// The SP record of one transition of `packet` at an SP queue with class `priorities`.
fn sp_record(
    kind: crate::SpTransitionKind,
    key: EventKey,
    node: NodeId,
    queue_slot: usize,
    priorities: &[u64],
    packet: PacketDescriptor,
    departure_time_ns: Option<u64>,
) -> Result<crate::MechanismTransitionRecord, ExecutionError> {
    let class = scheduler_class(packet.flow, priorities.len())
        .ok_or(ExecutionError::InvalidSchedulerState(node))?;
    Ok(crate::MechanismTransitionRecord::Sp(
        crate::SpTransitionRecord {
            key,
            node,
            queue_id: u64::try_from(queue_slot).unwrap_or(u64::MAX),
            kind,
            class_count: u64::try_from(priorities.len()).unwrap_or(u64::MAX),
            packet: scheduler_packet(packet),
            class_id: u64::try_from(class).unwrap_or(u64::MAX),
            priority: priorities[class],
            departure_time_ns,
        },
    ))
}

/// The WFQ record of one transition at a WFQ queue, from its state `wfq` after the transition.
#[allow(clippy::too_many_arguments)]
fn wfq_record(
    kind: crate::WfqTransitionKind,
    key: EventKey,
    node: NodeId,
    queue_slot: usize,
    rate_bps: u64,
    wfq: &WfqSchedulerState,
    packet: PacketDescriptor,
    virtual_start: Option<crate::ExactRational>,
    finish: Option<crate::ExactRational>,
    queued_packets: Vec<crate::WfqQueuedPacket>,
    paused_priorities: Vec<u8>,
) -> crate::MechanismTransitionRecord {
    crate::MechanismTransitionRecord::Wfq(Box::new(crate::WfqTransitionRecord {
        key,
        node,
        queue_id: u64::try_from(queue_slot).unwrap_or(u64::MAX),
        kind,
        rate_bps,
        weights: wfq.weights.clone(),
        packet: scheduler_packet(packet),
        virtual_start,
        finish,
        after: crate::WfqReplayState::of(wfq),
        queued_packets,
        paused_priorities,
        pfc_priority: 0,
    }))
}

/// Whether one queue's mechanisms always select the queue head with no per-packet inspection.
///
/// A queue without a PFC monitor cannot report a paused priority, so every queued packet is
/// eligible, and a head-serving discipline (`scheduler_serves_head`) serves the head.
fn queue_serves_head(queue: &crate::SwitchQueueState) -> bool {
    queue.pfc.is_none() && scheduler_serves_head(&queue.scheduler)
}

/// Whether a discipline always selects position zero of the eligible packets.
///
/// FIFO, static priority and weighted fair queueing all maintain their service order in the queue
/// itself, which is exactly why `scheduler_select_position` answers position zero for all three;
/// deficit and weighted round robin choose by class and need the eligible-packet list. The match
/// is deliberately exhaustive: a new discipline must be classified here before it can compile,
/// rather than silently inheriting the head-only and first-eligible plans.
fn scheduler_serves_head(scheduler: &SchedulerKind) -> bool {
    match scheduler {
        SchedulerKind::Fifo
        | SchedulerKind::StaticPriority { .. }
        | SchedulerKind::WeightedFairQueue(_) => true,
        SchedulerKind::DeficitRoundRobin(_) | SchedulerKind::WeightedRoundRobin(_) => false,
    }
}

struct TcpSendPlan {
    packets: Vec<PacketDescriptor>,
    timer: Option<TcpTimerState>,
}

impl<'image> TransitionState<'image> {
    pub(crate) fn new(
        image: &'image SimulationImage,
        observation_mode: ObservationMode,
    ) -> Result<Self, ExecutionError> {
        let mut packets = BTreeMap::new();
        let tcp_sent_segments =
            crate::tcp_ledger::seed_image(image).map_err(tcp_segment_conflict_error)?;
        let live_payloads = crate::tcp_ledger::initial_live_payloads(image);
        for descriptor in image.initial_packets.iter().copied() {
            if matches!(descriptor.kind, PacketKind::TcpData(_))
                && !live_payloads.contains(&descriptor.id)
            {
                continue;
            }
            if packets
                .insert(
                    descriptor.id,
                    ResidentPacket {
                        descriptor,
                        source_time_ns: None,
                        transmitters: 0,
                        terminal: false,
                    },
                )
                .is_some()
            {
                return Err(ExecutionError::DuplicatePayload(descriptor.id));
            }
        }
        for payload in image
            .host_states
            .iter()
            .filter_map(|state| state.in_service)
            .chain(
                image
                    .switch_states
                    .iter()
                    .flat_map(|state| &state.queues)
                    .filter_map(|queue| queue.in_service),
            )
        {
            let packet = packets
                .get_mut(&payload)
                .ok_or(ExecutionError::UnknownPacket(payload))?;
            packet.transmitters = packet
                .transmitters
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(NodeId(0)))?;
        }

        let hosts = HostStore::tables(image.host_states.clone(), &image.stage_joins);
        let switch_states = image.switch_states.clone();
        let DerivedSwitchQueues {
            bytes: switch_queue_bytes,
            paused_fifo_queues,
        } = derive_switch_queue_bytes(image, &switch_states, None, &packets)?;

        let mut state = Self {
            image,
            hosts,
            switch_states,
            switch_queue_bytes,
            switch_pfc_orders: None,
            local_node: None,
            packets,
            observation_mode,
            summary: RunSummary::default(),
            observed_packets: BTreeMap::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            tcp_transitions: Vec::new(),
            aqm_transitions: Vec::new(),
            mechanism_transitions: Vec::new(),
            tcp_sent_segments,
            superseded_timers: Vec::new(),
            stage_probe: StageScanProbe::default(),
            pfc_service_probe: PfcServiceProbe::default(),
        };
        for (state_slot, queue_slot) in paused_fifo_queues {
            state.ensure_pfc_order(state_slot, queue_slot)?;
        }
        Ok(state)
    }

    pub(crate) fn new_local(
        image: &'image SimulationImage,
        node: NodeDescriptor,
        packets: impl IntoIterator<Item = PacketDescriptor>,
        tcp_segment_seeds: impl IntoIterator<Item = PacketDescriptor>,
        observation_mode: ObservationMode,
    ) -> Result<Self, ExecutionError> {
        let (hosts, switch_states) = match node.kind {
            NodeKind::Host => {
                let state = image
                    .host_states
                    .get(node.state_slot as usize)
                    .cloned()
                    .ok_or(ExecutionError::InvalidStateSlot {
                        node: node.id,
                        kind: node.kind,
                        state_slot: node.state_slot,
                    })?;
                (HostStore::local(state, &image.stage_joins), Vec::new())
            }
            NodeKind::Switch => {
                let state = image
                    .switch_states
                    .get(node.state_slot as usize)
                    .cloned()
                    .ok_or(ExecutionError::InvalidStateSlot {
                        node: node.id,
                        kind: node.kind,
                        state_slot: node.state_slot,
                    })?;
                (HostStore::Empty, vec![state])
            }
        };

        let mut resident = BTreeMap::new();
        for descriptor in packets {
            if resident
                .insert(
                    descriptor.id,
                    ResidentPacket {
                        descriptor,
                        source_time_ns: None,
                        transmitters: 0,
                        terminal: false,
                    },
                )
                .is_some()
            {
                return Err(ExecutionError::DuplicatePayload(descriptor.id));
            }
        }
        let tcp_sent_segments = crate::tcp_ledger::seed_packets(image, tcp_segment_seeds)
            .map_err(tcp_segment_conflict_error)?;
        let in_service = match node.kind {
            NodeKind::Host => hosts.slices().0[0]
                .in_service
                .into_iter()
                .collect::<Vec<_>>(),
            NodeKind::Switch => switch_states[0]
                .queues
                .iter()
                .filter_map(|queue| queue.in_service)
                .collect(),
        };
        for payload in in_service {
            let packet = resident
                .get_mut(&payload)
                .ok_or(ExecutionError::UnknownPacket(payload))?;
            packet.transmitters = packet
                .transmitters
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
        }

        let DerivedSwitchQueues {
            bytes: switch_queue_bytes,
            paused_fifo_queues,
        } = derive_switch_queue_bytes(image, &switch_states, Some(node), &resident)?;

        let mut state = Self {
            image,
            hosts,
            switch_states,
            switch_queue_bytes,
            switch_pfc_orders: None,
            local_node: Some(node),
            packets: resident,
            observation_mode,
            summary: RunSummary::default(),
            observed_packets: BTreeMap::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            tcp_transitions: Vec::new(),
            aqm_transitions: Vec::new(),
            mechanism_transitions: Vec::new(),
            tcp_sent_segments,
            superseded_timers: Vec::new(),
            stage_probe: StageScanProbe::default(),
            pfc_service_probe: PfcServiceProbe::default(),
        };
        for (state_slot, queue_slot) in paused_fifo_queues {
            state.ensure_pfc_order(state_slot, queue_slot)?;
        }
        Ok(state)
    }

    /// Records that `timer` stopped being the armed timer of a flow owned by `node`.
    ///
    /// Called at every `active_timer` `Some -> None` transition except the one performed by the
    /// timer's own firing event, which the queue already removed by popping it.
    fn cancel_superseded_timer(&mut self, node: NodeId, timer: TcpTimerState) {
        self.superseded_timers.push(SupersededTimer {
            target: node,
            payload: timer.attempt,
            deadline_ns: timer.deadline_ns,
        });
    }

    /// Drains the eager-removal obligations produced by the last dispatch.
    ///
    /// Queue owners apply these before indexing the transition's children so a re-arm at the same
    /// deadline cannot collide with the identity it replaced.
    pub(crate) fn take_superseded_timers(&mut self, sink: &mut Vec<SupersededTimer>) {
        sink.append(&mut self.superseded_timers);
    }

    /// Returns whether `key` names an event supplied by the image rather than armed by this run.
    ///
    /// Only used by debug assertions guarding the retired lazy timer recognition.
    #[cfg(debug_assertions)]
    fn imported_event(&self, key: EventKey) -> bool {
        self.image
            .initial_events
            .iter()
            .any(|initial| initial.key == key)
    }

    pub(crate) fn install_packet(
        &mut self,
        descriptor: PacketDescriptor,
    ) -> Result<(), ExecutionError> {
        if let Some(existing) = self.packets.get(&descriptor.id) {
            return if existing.descriptor == descriptor {
                Ok(())
            } else {
                Err(ExecutionError::DuplicatePayload(descriptor.id))
            };
        }
        self.packets.insert(
            descriptor.id,
            ResidentPacket {
                descriptor,
                source_time_ns: None,
                transmitters: 0,
                terminal: false,
            },
        );
        Ok(())
    }

    pub(crate) fn packet_descriptor(
        &self,
        payload: PayloadId,
    ) -> Result<PacketDescriptor, ExecutionError> {
        self.packet(payload)
    }

    pub(crate) fn finish(mut self, pending_events: Vec<Event>) -> RunResult {
        debug_assert!(self.local_node.is_none());
        let resident_packets = resumable_packets(self.packets, &self.tcp_sent_segments);
        let observed_packets = self.observed_packets.into_values().collect();
        self.departures.sort_unstable_by_key(|(key, _)| *key);
        self.arrivals.sort_unstable_by_key(|(key, _)| *key);
        self.tcp_transitions
            .sort_unstable_by_key(|record| record.key);
        self.aqm_transitions
            .sort_unstable_by_key(|record| record.key);
        self.mechanism_transitions
            .sort_unstable_by_key(crate::MechanismTransitionRecord::canonical_order_key);
        RunResult {
            host_states: self.hosts.into_states(),
            switch_states: self.switch_states,
            summary: self.summary,
            resident_packets,
            observed_packets,
            departures: self
                .departures
                .into_iter()
                .map(|(_, departure)| departure)
                .collect(),
            arrivals: self
                .arrivals
                .into_iter()
                .map(|(_, arrival)| arrival)
                .collect(),
            diagnostics: (self.observation_mode == ObservationMode::Full).then_some(
                DiagnosticPlanes {
                    tcp_transitions: self.tcp_transitions,
                    aqm_transitions: self.aqm_transitions,
                    mechanism_transitions: self.mechanism_transitions,
                },
            ),
            pending_events,
        }
    }

    pub(crate) fn finish_local(mut self) -> LocalTransitionResult {
        let node = self
            .local_node
            .expect("finish_local requires node-local transition state");
        self.departures.sort_unstable_by_key(|(key, _)| *key);
        self.arrivals.sort_unstable_by_key(|(key, _)| *key);
        self.tcp_transitions
            .sort_unstable_by_key(|record| record.key);
        self.aqm_transitions
            .sort_unstable_by_key(|record| record.key);
        self.mechanism_transitions
            .sort_unstable_by_key(crate::MechanismTransitionRecord::canonical_order_key);
        let state = match node.kind {
            NodeKind::Host => LocalNodeState::Host(
                self.hosts
                    .into_local_state()
                    .expect("local host transition state owns one host"),
            ),
            NodeKind::Switch => LocalNodeState::Switch(
                self.switch_states
                    .pop()
                    .expect("local switch transition state owns one switch"),
            ),
        };
        LocalTransitionResult {
            node,
            state,
            summary: self.summary,
            resident_packets: self
                .packets
                .into_values()
                .map(|packet| packet.descriptor)
                .collect(),
            tcp_segment_ledger: self
                .tcp_sent_segments
                .into_values()
                .flat_map(BTreeMap::into_values)
                .collect(),
            observed_packets: self.observed_packets.into_values().collect(),
            departures: self.departures,
            arrivals: self.arrivals,
            tcp_transitions: self.tcp_transitions,
            aqm_transitions: self.aqm_transitions,
            mechanism_transitions: self.mechanism_transitions,
        }
    }

    pub(crate) fn dispatch(
        &mut self,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let node = self.node(event.target)?;
        let handler = resolve_transition(node.kind, event.kind).ok_or(
            ExecutionError::UnsupportedTransition {
                node: node.id,
                kind: node.kind,
                event_kind: event.kind,
            },
        )?;
        self.stage_probe.note_dispatch();

        match handler {
            TransitionHandler::HostPacketArrival => self.host_packet_arrival(node, event, children),
            TransitionHandler::HostTxReady => self.host_tx_ready(node, event, children),
            TransitionHandler::HostTxComplete => self.host_tx_complete(node, event, children),
            TransitionHandler::HostRemoteArrival => self.host_remote_arrival(node, event, children),
            TransitionHandler::SwitchTxReady => self.switch_tx_ready(node, event, children),
            TransitionHandler::SwitchTxComplete => self.switch_tx_complete(node, event, children),
            TransitionHandler::SwitchRemoteArrival => {
                self.switch_remote_arrival(node, event, children)
            }
            TransitionHandler::HostRetransmissionTimeout => {
                self.host_retransmission_timeout(node, event, children)
            }
            TransitionHandler::HostPacingTimer => self.host_pacing_timer(node, event, children),
        }
    }

    fn host_packet_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let packet = self.packet(event.payload)?;
        let owns_generator = {
            let (_, index) = self.host_parts_mut(node)?;
            index.first_generator(packet.flow).is_some()
        };
        if !owns_generator {
            return self.host_preloaded_packet_arrival(node, event, children);
        }
        if let PacketKind::TcpData(header) = packet.kind {
            return self.host_tcp_initial_send(node, event, packet, header, children);
        }
        self.set_source_time(event.payload, event.key.time_ns)?;
        let (next_packet, next_departure_ns, schedule_ready) = {
            let stop_time_ns = self.image.stop_time_ns;
            let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
            let (mut state, index) = self.host_parts_mut(node)?;
            let generator_index =
                index
                    .first_generator(packet.flow)
                    .ok_or(ExecutionError::UnknownGenerator {
                        node: node.id,
                        flow: packet.flow,
                    })?;
            let (constant, candidate_departure_ns, next_departure_ns, next_status) = {
                let generator = &mut state.generators[generator_index];
                if generator.next_emission.status != GeneratorStatus::Scheduled
                    || generator.next_emission.departure_time_ns != event.key.time_ns
                    || generator.next_emission.payload != event.payload
                {
                    return Err(ExecutionError::UnexpectedGeneratorEmission {
                        node: node.id,
                        flow: packet.flow,
                        payload: event.payload,
                    });
                }

                generator.packets_emitted = generator
                    .packets_emitted
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                generator.bytes_emitted = generator
                    .bytes_emitted
                    .checked_add(packet.size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;

                let FlowGeneratorKind::Constant(constant) = generator.kind else {
                    unreachable!("TCP emissions use the closed-loop transition")
                };
                let next_departure_ns = match constant.termination {
                    GeneratorTermination::Bytes(bytes) if generator.bytes_emitted < bytes => Some(
                        event
                            .key
                            .time_ns
                            .checked_add(constant.interval_ns)
                            .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?,
                    ),
                    GeneratorTermination::Bytes(_) => None,
                    GeneratorTermination::DurationNs(duration_ns) => {
                        let end_time_ns = constant
                            .first_departure_ns
                            .checked_add(duration_ns)
                            .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                        let candidate = event
                            .key
                            .time_ns
                            .checked_add(constant.interval_ns)
                            .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                        (candidate < end_time_ns).then_some(candidate)
                    }
                };
                let next_status = match next_departure_ns {
                    Some(next) if next <= stop_time_ns => GeneratorStatus::Scheduled,
                    Some(_) => GeneratorStatus::Stopped,
                    None => GeneratorStatus::Finished,
                };
                (
                    constant,
                    next_departure_ns,
                    next_departure_ns.filter(|next| *next <= stop_time_ns),
                    next_status,
                )
            };
            let next_packet = if let Some(next_departure_ns) = next_departure_ns {
                let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                *state.next_payload_seq = state
                    .next_payload_seq
                    .checked_add(1)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                state.generators[generator_index].next_emission = crate::ScheduledEmission {
                    status: GeneratorStatus::Scheduled,
                    departure_time_ns: next_departure_ns,
                    payload,
                };
                Some(PacketDescriptor {
                    id: payload,
                    flow: packet.flow,
                    size_bytes: constant.packet_size_bytes,
                    ecn_marked: false,
                    kind: PacketKind::Data,
                })
            } else {
                state.generators[generator_index].next_emission.status = next_status;
                if let Some(candidate) = candidate_departure_ns {
                    state.generators[generator_index]
                        .next_emission
                        .departure_time_ns = candidate;
                }
                None
            };

            *state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let schedule_ready = if state.in_service.is_none() && !*state.tx_ready_pending {
                *state.tx_ready_pending = true;
                true
            } else {
                false
            };
            (next_packet, next_departure_ns, schedule_ready)
        };
        self.enqueue_source_packet(node, event.payload)?;

        self.record_sourced(node.id, packet)?;
        if let Some(next_packet) = next_packet {
            self.insert_packet(next_packet, Some(next_departure_ns.expect("produced time")))?;
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacketArrival,
                    payload: next_packet.id,
                    time_ns: next_departure_ns.expect("a produced packet has a departure"),
                },
                children,
            )?;
        }
        if schedule_ready {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload: event.payload,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }

        Ok(())
    }

    fn activate_ready_collectives(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        mut causes: PendingCauses,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let mut ordinal = 0_u64;
        loop {
            let flow = {
                let (mut state, index) = self.host_parts_mut(node)?;
                // The first stage in table order that is ready to activate.
                let Some(position) = index.first_ready(&state.generators, &state.stages) else {
                    break;
                };
                let stage = state
                    .stages
                    .stage_mut(position)
                    .expect("a ready stage carries its record");
                stage.activated = true;
                index.refresh_releasable(position, Some(*stage));
                state.generators[position].flow
            };
            let Some(cause) = causes.take(flow, &mut self.stage_probe) else {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow,
                    payload: parent.payload,
                });
            };
            self.activate_wrapped_stage(node, parent, flow, cause, ordinal, children)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
        }
        // Progress that did not release a stage is recorded after every release of this event,
        // in canonical cause order.
        for cause in causes.into_remaining(&mut self.stage_probe) {
            self.push_stage_progress(node, parent, cause.flow, cause, ordinal, false)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
        }
        Ok(())
    }

    /// Releases a dependency-gated stage carried by an ordinary transport generator.
    ///
    /// A TCP stage starts exactly as a TCP flow whose first window opens at the release time: the
    /// sender fills its initial congestion window from sequence zero and arms its timer. A RoCE
    /// stage starts exactly as a queue pair whose initial delay is the release time
    /// (`start_roce_stage`).
    fn activate_wrapped_stage(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        flow: FlowId,
        cause: PendingCollectiveProgress,
        ordinal: u64,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let (kind, compute_duration) = {
            let (state, index) = self.host_parts_mut(node)?;
            let position = index
                .first_generator(flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                })?;
            (
                state.generators[position].kind,
                state
                    .stages
                    .stage(position)
                    .and_then(|stage| match stage.role {
                        crate::StageRole::Compute(compute) => Some(compute.duration_ns),
                        crate::StageRole::Collective(_) => None,
                    }),
            )
        };
        if let (FlowGeneratorKind::Constant(constant), duration) = (kind, compute_duration) {
            // A compute stage's token is a zero-byte `Data` timer; a stage notify's (a collective
            // stage on a constant generator, P16 H2) is its chunk, which crosses to the target
            // when the lead elapses.
            let (duration_ns, token) = match duration {
                Some(duration_ns) => (duration_ns, (0, PacketKind::Data)),
                None => (
                    constant.interval_ns,
                    (constant.packet_size_bytes, PacketKind::StageNotify),
                ),
            };
            return self.start_compute_stage(
                node,
                parent,
                flow,
                duration_ns,
                token,
                cause,
                ordinal,
                children,
            );
        }
        if let FlowGeneratorKind::Roce(_) = kind {
            return self.start_roce_stage(node, parent, flow, cause, ordinal, children);
        }
        let FlowGeneratorKind::Tcp(_) = kind else {
            return Err(ExecutionError::UnexpectedGeneratorEmission {
                node: node.id,
                flow,
                payload: parent.payload,
            });
        };
        let plan = self.prepare_tcp_attempts(node, flow, parent.key.time_ns, None, true, false)?;
        self.push_stage_progress(node, parent, flow, cause, ordinal, true)?;
        self.install_tcp_attempts(node, parent, plan, children)
    }

    /// Releases a RoCE collective stage at `t`, the parent event's time (design note §5.2,
    /// rulings C2 and C5): the pacing grid, held at zero while the stage was gated, is anchored at
    /// `t`, and the pacer is armed with its first tick at `t`. The pair is then exactly a queue pair
    /// whose initial delay is `t`; its retransmission timeout is armed by its first send, and a tick
    /// that finds its data class paused parks it (H1). Its DCQCN controller stays pristine: the
    /// Mellanox form starts its timers at the first feedback (P16), so nothing is anchored.
    // Out of line: it runs once per stage, and must not grow `activate_wrapped_stage`'s TCP path.
    #[inline(never)]
    fn start_roce_stage(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        flow: FlowId,
        cause: PendingCollectiveProgress,
        ordinal: u64,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let now = parent.key.time_ns;
        let pacing_token = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position = index
                .first_generator(flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                })?;
            let generator = &mut state.generators[position];
            let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow,
                    payload: parent.payload,
                });
            };
            roce.pacer.first_pacing_time_ns = now;
            roce.pacer_armed = true;
            generator.next_emission = crate::ScheduledEmission {
                status: crate::roce::armed_status(&roce),
                departure_time_ns: now,
                payload: roce.pacing_timer_payload,
            };
            generator.kind = FlowGeneratorKind::Roce(roce);
            roce.pacing_timer_payload
        };
        self.push_stage_progress(node, parent, flow, cause, ordinal, true)?;
        self.emit_from_host(
            node,
            parent,
            ChildEmission {
                target: node.id,
                kind: EventKind::PacingTimer,
                payload: pacing_token,
                time_ns: now,
            },
            children,
        )?;
        Ok(())
    }

    /// Starts a released compute interval, or a stage notify's lead: a source-local timer fires
    /// `duration_ns` later.
    ///
    /// The token, `(size_bytes, kind)`, names the timer event: a compute stage's zero-byte `Data`
    /// token is never enqueued or transmitted; a stage notify's carries its chunk across its lane
    /// when the timer fires. A deadline beyond the stop time leaves the stage `Stopped` without an
    /// event.
    #[allow(clippy::too_many_arguments)]
    fn start_compute_stage(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        flow: FlowId,
        duration_ns: u64,
        (token_size_bytes, token_kind): (u64, PacketKind),
        cause: PendingCollectiveProgress,
        ordinal: u64,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let stop_time_ns = self.image.stop_time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let deadline_ns = parent
            .key
            .time_ns
            .checked_add(duration_ns)
            .ok_or(ExecutionError::GeneratorTimeOverflow(flow))?;
        let token = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let payload = if deadline_ns <= stop_time_ns {
                let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                *state.next_payload_seq = state
                    .next_payload_seq
                    .checked_add(1)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                Some(payload)
            } else {
                None
            };
            let generator = &mut state.generators[index.first_generator(flow).ok_or(
                ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                },
            )?];
            generator.next_emission = crate::ScheduledEmission {
                status: if payload.is_some() {
                    GeneratorStatus::Scheduled
                } else {
                    GeneratorStatus::Stopped
                },
                departure_time_ns: deadline_ns,
                payload: payload.unwrap_or(PayloadId(0)),
            };
            payload
        };
        self.push_stage_progress(node, parent, flow, cause, ordinal, true)?;
        if let Some(payload) = token {
            self.insert_packet(
                PacketDescriptor {
                    id: payload,
                    flow,
                    size_bytes: token_size_bytes,
                    ecn_marked: false,
                    kind: token_kind,
                },
                Some(parent.key.time_ns),
            )?;
            self.emit_from_host(
                node,
                parent,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacingTimer,
                    payload,
                    time_ns: deadline_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    /// A compute interval ends: the stage finishes and releases its local successors.
    fn host_compute_timer(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        flow: FlowId,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let mut causes = PendingCauses::default();
        {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position = index
                .first_generator(flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                })?;
            let stage = state.stages.stage(position);
            let generator = &mut state.generators[position];
            if generator.next_emission.status != GeneratorStatus::Scheduled
                || generator.next_emission.payload != event.payload
                || generator.next_emission.departure_time_ns != event.key.time_ns
            {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow,
                    payload: event.payload,
                });
            }
            generator.next_emission.status = GeneratorStatus::Finished;
            let duration_ns = match stage.map(|stage| stage.role) {
                Some(crate::StageRole::Compute(compute)) => compute.duration_ns,
                _ => {
                    return Err(ExecutionError::UnexpectedGeneratorEmission {
                        node: node.id,
                        flow,
                        payload: event.payload,
                    });
                }
            };
            let completion = CompletionSignal {
                ack_number: 0,
                origin_ns: event.key.time_ns - duration_ns,
                delay_ns: duration_ns,
            };
            complete_local_successors(
                &state.generators,
                &mut state.stages,
                index,
                flow,
                completion,
                &mut causes,
            );
        }
        self.mark_terminal(event.payload)?;
        if !causes.is_empty() {
            self.activate_ready_collectives(node, event, causes, children)?;
        }
        Ok(())
    }

    /// A stage notify's lead ends (P16 H2): the sender's stage finishes, with its one message
    /// emitted, and its notify leaves on the lane to the target host, arriving `lane` later (the
    /// generator's `first_departure_ns`). Then the stage's local successors are released, after
    /// the transition's own emission, as every stage pass runs.
    #[inline(never)]
    fn host_notify_timer(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        flow: FlowId,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let mut causes = PendingCauses::default();
        let lane_ns = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position = index
                .first_generator(flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                })?;
            let generator = &mut state.generators[position];
            let FlowGeneratorKind::Constant(constant) = generator.kind else {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow,
                    payload: event.payload,
                });
            };
            if generator.next_emission.status != GeneratorStatus::Scheduled
                || generator.next_emission.payload != event.payload
                || generator.next_emission.departure_time_ns != event.key.time_ns
            {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow,
                    payload: event.payload,
                });
            }
            generator.next_emission.status = GeneratorStatus::Finished;
            generator.packets_emitted = 1;
            generator.bytes_emitted = constant.packet_size_bytes;
            let completion = CompletionSignal {
                ack_number: 0,
                origin_ns: event.key.time_ns - constant.interval_ns,
                delay_ns: constant.interval_ns,
            };
            complete_local_successors(
                &state.generators,
                &mut state.stages,
                index,
                flow,
                completion,
                &mut causes,
            );
            constant.first_departure_ns
        };
        let target = self.flow(flow)?.target;
        let arrival_ns = event
            .key
            .time_ns
            .checked_add(lane_ns)
            .ok_or(ExecutionError::Time(TimeError::ArrivalOverflow))?;
        self.emit_from_host(
            node,
            event,
            ChildEmission {
                target,
                kind: EventKind::RemoteArrival,
                payload: event.payload,
                time_ns: arrival_ns,
            },
            children,
        )?;
        if !causes.is_empty() {
            self.activate_ready_collectives(node, event, causes, children)?;
        }
        Ok(())
    }

    /// A stage notify arrives at its target (P16 H2): the whole chunk is delivered at once, so
    /// the stages waiting on it as their inbound predecessor advance by it, and are released.
    #[inline(never)]
    fn host_notify_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let mut causes = PendingCauses::default();
        {
            let (mut state, index) = self.host_parts_mut(node)?;
            record_inbound_progress(
                &state.generators,
                &mut state.stages,
                index,
                packet.flow,
                InboundProgress::Segment {
                    sequence: 0,
                    bytes: packet.size_bytes,
                    advance: packet.size_bytes,
                },
                node.id,
                &mut causes,
            )?;
        }
        self.mark_terminal(event.payload)?;
        if !causes.is_empty() {
            self.activate_ready_collectives(node, event, causes, children)?;
        }
        Ok(())
    }

    /// Records one prerequisite transition of a wrapped stage under full observation.
    fn push_stage_progress(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        flow: FlowId,
        cause: PendingCollectiveProgress,
        ordinal: u64,
        activated: bool,
    ) -> Result<(), ExecutionError> {
        if self.observation_mode != ObservationMode::Full {
            return Ok(());
        }
        let stop_time_ns = self.image.stop_time_ns;
        let (state, index) = self.host_parts_mut(node)?;
        let position = index
            .first_generator(flow)
            .ok_or(ExecutionError::UnknownGenerator {
                node: node.id,
                flow,
            })?;
        let stage = state.stages.stage(position);
        let record = collective_progress_record(
            CollectiveProgressContext {
                key: parent.key,
                ordinal,
                node: node.id,
                stop_time_ns,
                activated,
            },
            cause,
            &state.generators[position],
            stage,
        )
        .ok_or(ExecutionError::UnexpectedGeneratorEmission {
            node: node.id,
            flow,
            payload: parent.payload,
        })?;
        self.mechanism_transitions
            .push(crate::MechanismTransitionRecord::Collective(record));
        Ok(())
    }

    fn host_tcp_initial_send(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        header: TcpDataHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        self.set_source_time(event.payload, event.key.time_ns)?;
        {
            let (mut state, index) = self.host_parts_mut(node)?;
            let generator = &mut state.generators[index.first_generator(packet.flow).ok_or(
                ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                },
            )?];
            let FlowGeneratorKind::Tcp(mut tcp) = generator.kind else {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow: packet.flow,
                    payload: packet.id,
                });
            };
            if generator.next_emission.status != GeneratorStatus::Scheduled
                || generator.next_emission.payload != packet.id
                || generator.next_emission.departure_time_ns != event.key.time_ns
                || header.sequence != tcp.next_sequence
                || header.sent_time_ns != event.key.time_ns
                || header.retransmission
            {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow: packet.flow,
                    payload: packet.id,
                });
            }
            generator.packets_emitted = generator
                .packets_emitted
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            generator.bytes_emitted = generator
                .bytes_emitted
                .checked_add(packet.size_bytes)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            tcp.next_sequence = tcp
                .next_sequence
                .checked_add(packet.size_bytes)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            tcp.bytes_in_flight = tcp
                .bytes_in_flight
                .checked_add(packet.size_bytes)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            tcp.last_attempt = packet.id;
            generator.feedback.outstanding_bytes = tcp.bytes_in_flight;
            generator.feedback.unacknowledged_bytes = tcp.bytes_in_flight;
            generator.next_emission.status = GeneratorStatus::Blocked;
            generator.kind = FlowGeneratorKind::Tcp(tcp);
            *state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
        }
        seed_tcp_segment(&mut self.tcp_sent_segments, packet)?;
        self.enqueue_source_packet(node, packet.id)?;
        self.record_sourced(node.id, packet)?;
        let plan =
            self.prepare_tcp_attempts(node, packet.flow, event.key.time_ns, None, true, false)?;
        self.install_tcp_attempts(node, event, plan, children)
    }

    fn host_preloaded_packet_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let packet = self.packet(event.payload)?;
        self.set_source_time(event.payload, event.key.time_ns)?;
        let schedule_ready = {
            let state = self.host_state_mut(node)?;
            state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            if state.in_service.is_none() && !state.tx_ready_pending {
                state.tx_ready_pending = true;
                true
            } else {
                false
            }
        };
        self.enqueue_source_packet(node, event.payload)?;
        self.record_sourced(node.id, packet)?;
        if schedule_ready {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload: event.payload,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    fn host_tx_ready(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let (egress_link, payload) = {
            let state = self.host_state_mut(node)?;
            state.tx_ready_pending = false;
            if state.in_service.is_some() {
                return Err(ExecutionError::HostAlreadyTransmitting(node.id));
            }

            if state.pfc.is_none() {
                let Some(payload) = state.queue.pop_front() else {
                    return Ok(());
                };
                state.in_service = Some(payload);
                (state.egress_link, payload)
            } else {
                // Host-link PFC: the first queued packet whose class is not paused.
                let Some(selected) = self.host_pfc_start_eligible(node)? else {
                    return Ok(());
                };
                selected
            }
        };

        let link = self.link(egress_link)?;
        if link.source != node.id {
            return Err(ExecutionError::LinkSourceMismatch {
                link: link.id,
                expected_source: node.id,
                actual_source: link.source,
            });
        }

        self.start_transmission(node.id, payload)?;
        let arrival_time_ns =
            link.arrival_time_ns(event.key.time_ns, self.packet_size(payload)?)?;
        let departure_time_ns = arrival_time_ns
            .checked_sub(link.propagation_ns)
            .ok_or(ExecutionError::Time(TimeError::ArrivalOverflow))?;

        // Emission order is semantic: completion first, then the remote message.
        self.emit_from_host(
            node,
            event,
            ChildEmission {
                target: node.id,
                kind: EventKind::TxComplete,
                payload,
                time_ns: departure_time_ns,
            },
            children,
        )?;
        self.emit_from_host(
            node,
            event,
            ChildEmission {
                target: self.packet_remote_target(payload, link.id)?,
                kind: EventKind::RemoteArrival,
                payload,
                time_ns: arrival_time_ns,
            },
            children,
        )
    }

    fn host_tx_complete(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let schedule_ready = {
            let state = self.host_state_mut(node)?;
            if state.in_service != Some(event.payload) {
                return Err(ExecutionError::UnexpectedTxComplete {
                    node: node.id,
                    expected: state.in_service,
                    actual: event.payload,
                });
            }

            state.in_service = None;
            state.departed_packets = state
                .departed_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;

            if state.pfc.is_some() {
                None
            } else if !state.queue.is_empty() && !state.tx_ready_pending {
                state.tx_ready_pending = true;
                Some(true)
            } else {
                Some(false)
            }
        };
        // Host-link PFC: service resumes only for a packet whose class is not paused.
        let schedule_ready = match schedule_ready {
            Some(schedule_ready) => schedule_ready,
            None => self.host_pfc_claim_ready(node)?,
        };

        let packet = self.packet(event.payload)?;
        self.record_departure(node.id, packet, event.key)?;
        self.finish_transmission(node.id, event.payload)?;

        if schedule_ready {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload: event.payload,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }

        Ok(())
    }

    /// The queue position of the first packet a host may send under host-link PFC: the first
    /// whose class (`packet_priority`) is not paused, as a switch FIFO serves.
    fn host_pfc_first_eligible(
        &self,
        node: NodeDescriptor,
    ) -> Result<Option<usize>, ExecutionError> {
        let state = self.host_state(node)?;
        let Some(pfc) = state.pfc.as_deref() else {
            return Ok((!state.queue.is_empty()).then_some(0));
        };
        for (position, payload) in state.queue.iter().enumerate() {
            let packet = self.packet(*payload)?;
            let priority = usize::from(self.flow(packet.flow)?.packet_priority(packet.kind));
            if !pfc.is_paused(priority) {
                return Ok(Some(position));
            }
        }
        Ok(None)
    }

    /// Starts serving the first eligible packet of a host with host-link PFC; `None` when every
    /// queued packet's class is paused.
    #[inline(never)]
    fn host_pfc_start_eligible(
        &mut self,
        node: NodeDescriptor,
    ) -> Result<Option<(LinkId, PayloadId)>, ExecutionError> {
        let Some(position) = self.host_pfc_first_eligible(node)? else {
            return Ok(None);
        };
        let state = self.host_state_mut(node)?;
        let payload = state
            .queue
            .remove(position)
            .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
        state.in_service = Some(payload);
        Ok(Some((state.egress_link, payload)))
    }

    /// After a host with host-link PFC completes a transmission: claims the next `TxReady` when
    /// an eligible packet waits and none is pending.
    #[inline(never)]
    fn host_pfc_claim_ready(&mut self, node: NodeDescriptor) -> Result<bool, ExecutionError> {
        let eligible = self.host_pfc_first_eligible(node)?.is_some();
        let state = self.host_state_mut(node)?;
        if eligible && !state.tx_ready_pending {
            state.tx_ready_pending = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn switch_remote_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let mut packet = self.packet(event.payload)?;
        let packet_ecn_before = packet.ecn_marked;
        if let PacketKind::Pfc(header) = packet.kind {
            return self.switch_pfc_remote_arrival(node, event, header, children);
        }
        let egress_link = self.packet_egress_at(packet, node.id)?;
        let rate_bps = egress_link
            .map(|link| self.link(link).map(|descriptor| descriptor.rate_bps))
            .transpose()?;
        let sp_position = self.switch_sp_insertion_position(node, egress_link, packet.flow)?;
        let state_slot = self.local_state_slot(node)?;
        let (queue_id, queue_slot, queue_bytes, queue_has_pfc) = {
            let state = self.switch_state(node)?;
            let (queue_slot, queue) = state
                .queues
                .iter()
                .enumerate()
                .find(|(_, queue)| queue.egress_link == egress_link)
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link,
                })?;
            let queue_bytes = self.switch_queue_bytes[state_slot][queue_slot];
            (
                u64::try_from(queue_slot).unwrap_or(u64::MAX),
                queue_slot,
                queue_bytes,
                queue.pfc.is_some(),
            )
        };
        // The packet's PFC class (P15: its flow's feedback class for a CNP, ACK or NACK). Only a
        // PFC queue reads it, so a queue without one skips the flow lookup and the kind match.
        let priority = if queue_has_pfc {
            usize::from(self.flow(packet.flow)?.packet_priority(packet.kind))
        } else {
            0
        };
        // Only a PFC monitor consults the incoming link, and the route walk that derives it is
        // per-arrival work. A queue without a monitor never reads this value.
        let incoming_link = if queue_has_pfc {
            self.packet_incoming_link_at(packet, node.id)?
        } else {
            None
        };

        let ecn_seed = self.image.seed;
        let (disposition, schedule_ready, mark_packet, pfc_plan, pfc_transition, aqm_transition) = {
            let state = self.switch_state_mut(node)?;
            state.arrived_packets = state
                .arrived_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;

            let queue = state
                .queues
                .iter_mut()
                .find(|queue| queue.egress_link == egress_link)
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link,
                })?;
            let queue_len = u64::try_from(queue.queue.len()).unwrap_or(u64::MAX);
            let pfc_overflow = queue
                .pfc
                .as_ref()
                .and_then(|pfc| {
                    pfc.ingresses
                        .iter()
                        .find(|ingress| Some(ingress.controlled_link) == incoming_link)
                })
                .is_some_and(|ingress| {
                    ingress.xoff_threshold_bytes[priority] != 0
                        && ingress.occupancy_bytes[priority]
                            .checked_add(packet.size_bytes)
                            .is_none_or(|depth| depth > ingress.buffer_capacity_bytes[priority])
                });
            let (action, aqm_transition) = if pfc_overflow {
                (QueueAdmissionAction::Drop, None)
            } else {
                match &queue.drop_mark {
                    crate::DropMarkPolicy::TailDrop => (
                        taildrop_action(
                            queue.queue_capacity_packets,
                            queue_len,
                            queue_bytes,
                            packet.size_bytes,
                            node.id,
                        )?,
                        None,
                    ),
                    crate::DropMarkPolicy::EcnRamp(policy) => {
                        let action = ecn_ramp_action(
                            policy,
                            queue_bytes,
                            packet,
                            ecn_seed,
                            node.id,
                            queue_id,
                            event.payload,
                        );
                        (action, Some((*policy, action)))
                    }
                }
            };
            if action == QueueAdmissionAction::Drop {
                state.dropped_packets = state
                    .dropped_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                (
                    ArrivalDisposition::Dropped,
                    false,
                    false,
                    None,
                    None,
                    aqm_transition,
                )
            } else {
                match &mut queue.scheduler {
                    SchedulerKind::Fifo => queue.queue.push_back(event.payload),
                    SchedulerKind::StaticPriority { .. } => {
                        queue.queue.insert(
                            sp_position.ok_or(ExecutionError::InvalidSchedulerState(node.id))?,
                            event.payload,
                        );
                    }
                    SchedulerKind::WeightedFairQueue(wfq) => {
                        let rate_bps =
                            rate_bps.ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
                        wfq_enqueue(
                            wfq,
                            &mut queue.queue,
                            packet,
                            event.key.time_ns,
                            rate_bps,
                            node.id,
                        )?;
                    }
                    SchedulerKind::DeficitRoundRobin(_) | SchedulerKind::WeightedRoundRobin(_) => {
                        queue.queue.push_back(event.payload);
                    }
                }
                let priority_paused = queue
                    .pfc
                    .as_ref()
                    .is_some_and(|pfc| pfc.is_paused(priority));
                let schedule_ready = queue.egress_link.is_some()
                    && queue.in_service.is_none()
                    && !queue.tx_ready_pending
                    && !priority_paused;
                if schedule_ready {
                    queue.tx_ready_pending = true;
                }
                let ingress = queue.pfc.as_mut().and_then(|pfc| {
                    pfc.ingresses
                        .iter_mut()
                        .find(|ingress| Some(ingress.controlled_link) == incoming_link)
                });
                let (pfc_plan, pfc_transition) = if let Some(ingress) = ingress {
                    let xoff = ingress.xoff_threshold_bytes[priority];
                    if xoff == 0 {
                        (None, None)
                    } else {
                        let before_occupancy = ingress.occupancy_bytes[priority];
                        let before_asserted = ingress.pause_asserted[priority];
                        let depth = before_occupancy
                            .checked_add(packet.size_bytes)
                            .ok_or(ExecutionError::CounterOverflow(node.id))?;
                        ingress.occupancy_bytes[priority] = depth;
                        let plan = if depth < xoff || before_asserted {
                            None
                        } else {
                            ingress.pause_asserted[priority] = true;
                            Some(PfcFramePlan {
                                channel_index: ingress.control_channel_index,
                                flow: packet.flow,
                                header: crate::PfcHeader {
                                    controlled_link: ingress.controlled_link,
                                    priority: priority as u8,
                                    pause: true,
                                },
                            })
                        };
                        let transition = crate::MechanismTransitionRecord::PfcThreshold(
                            crate::PfcThresholdTransitionRecord {
                                key: event.key,
                                node: node.id,
                                queue_id,
                                controlled_link: ingress.controlled_link,
                                priority: priority as u8,
                                xon_bytes: ingress.xon_threshold_bytes[priority],
                                xoff_bytes: xoff,
                                buffer_capacity_bytes: ingress.buffer_capacity_bytes[priority],
                                amount_bytes: packet.size_bytes,
                                action: crate::PfcOccupancyAction::Admit,
                                before_occupancy_bytes: before_occupancy,
                                before_asserted,
                                after_occupancy_bytes: depth,
                                after_asserted: ingress.pause_asserted[priority],
                                emitted: plan.map(|_| crate::PfcControlAction::Pause),
                            },
                        );
                        (plan, Some(transition))
                    }
                } else {
                    (None, None)
                };
                (
                    ArrivalDisposition::Admitted,
                    schedule_ready,
                    action == QueueAdmissionAction::Mark,
                    pfc_plan,
                    pfc_transition,
                    aqm_transition,
                )
            }
        };

        if disposition == ArrivalDisposition::Admitted {
            let counter = &mut self.switch_queue_bytes[state_slot][queue_slot];
            *counter = counter
                .checked_add(packet.size_bytes)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            // Only a FIFO PFC queue keeps an order, and it appended the packet.
            if queue_has_pfc {
                if let Some(order) = self.pfc_order_mut(state_slot, queue_slot) {
                    order.push(priority);
                }
            }
            #[cfg(debug_assertions)]
            self.debug_assert_switch_queue_aux(node, state_slot, queue_slot);
        }

        if mark_packet {
            self.set_packet_marked(event.payload)?;
            packet.ecn_marked = true;
        }

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.extend(pfc_transition);
            if let Some((policy, action)) = aqm_transition {
                self.aqm_transitions.push(AqmTransitionRecord {
                    key: event.key,
                    node: node.id,
                    queue_id,
                    payload: event.payload,
                    queued_bytes_before: queue_bytes,
                    packet_size_bytes: packet.size_bytes,
                    ecn_before: packet_ecn_before,
                    ecn_after: packet.ecn_marked,
                    policy,
                    action: match action {
                        QueueAdmissionAction::Enqueue => AqmTransitionAction::Enqueue,
                        QueueAdmissionAction::Mark => AqmTransitionAction::Mark,
                        QueueAdmissionAction::Drop => AqmTransitionAction::Drop,
                    },
                });
            }
        }

        self.record_arrival(node.id, packet, event.key, disposition)?;
        if disposition == ArrivalDisposition::Dropped {
            self.mark_terminal(event.payload)?;
        }

        if let Some(plan) = pfc_plan {
            self.emit_pfc_frame(node, event, plan, children)?;
        }

        if schedule_ready {
            self.emit_from_switch(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload: event.payload,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    /// Kept out of line: inlined into `dispatch`, this PFC-only handler changed the code the
    /// compiler emits for every other event, and Scalar E1, which carries no PFC, ran about 0.2%
    /// more instructions (paired icount at P16 `b2cdd12`, `days-gpu/evidence/P16/pfcperf/`).
    #[inline(never)]
    fn switch_pfc_remote_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        header: crate::PfcHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let priority = usize::from(header.priority);
        let state_slot = self.local_state_slot(node)?;
        let (queue_slot, may_schedule, transition) = {
            let state = self.switch_state_mut(node)?;
            let (queue_id, queue) = state
                .queues
                .iter_mut()
                .enumerate()
                .find(|(_, queue)| queue.egress_link == Some(header.controlled_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(header.controlled_link),
                })?;
            let pfc = queue
                .pfc
                .as_mut()
                .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
            let before_controllers = pfc.paused_by_controller[priority]
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let was_paused = pfc.is_paused(priority);
            let resumed = if header.pause {
                pfc.paused_by_controller[priority].insert(event.key.origin_node);
                false
            } else if !pfc.paused_by_controller[priority].remove(&event.key.origin_node) {
                // Duplicate/early resume is an idempotent no-op.
                false
            } else if pfc.is_paused(priority) {
                // Another controller still owns the aggregate pause.
                false
            } else {
                debug_assert!(was_paused);
                true
            };
            let transition =
                crate::MechanismTransitionRecord::PfcControl(crate::PfcControlTransitionRecord {
                    key: event.key,
                    node: node.id,
                    queue_id: u64::try_from(queue_id).unwrap_or(u64::MAX),
                    controlled_link: header.controlled_link,
                    controller: event.key.origin_node,
                    priority: header.priority,
                    action: if header.pause {
                        crate::PfcControlAction::Pause
                    } else {
                        crate::PfcControlAction::Resume
                    },
                    before_controllers,
                    after_controllers: pfc.paused_by_controller[priority].iter().copied().collect(),
                });
            (
                queue_id,
                resumed && queue.in_service.is_none() && !queue.tx_ready_pending,
                transition,
            )
        };
        // A paused FIFO queue keeps its class order from the first pause on.
        let mut probed_reads = if header.pause {
            self.ensure_pfc_order(state_slot, queue_slot)?
        } else {
            0
        };
        // A resume that unpauses the class lets an idle queue serve its first eligible packet.
        let schedule_payload = if may_schedule {
            let (first, reads) = {
                let queue = &self.switch_state(node)?.queues[queue_slot];
                let pfc = queue
                    .pfc
                    .as_ref()
                    .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
                let order = self.pfc_order(state_slot, queue_slot);
                self.pfc_first_eligible(node, queue, pfc, order)?
            };
            probed_reads += reads;
            if let Some((_, payload)) = first {
                self.switch_state_mut(node)?.queues[queue_slot].tx_ready_pending = true;
                Some(payload)
            } else {
                None
            }
        } else {
            None
        };
        self.pfc_service_probe.note_reads(probed_reads);
        #[cfg(debug_assertions)]
        self.debug_assert_switch_queue_aux(node, state_slot, queue_slot);
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.push(transition);
        }
        self.mark_terminal(event.payload)?;
        if let Some(payload) = schedule_payload {
            self.emit_from_switch(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    /// A PFC PAUSE or RESUME at a host's egress (host-link PFC, `hostpfc-design.md` §3.2).
    ///
    /// The controller set changes as at a switch queue, and the transition is recorded with the
    /// host as the node and queue 0. When the last controller of a class resumes, an idle host
    /// with an eligible packet schedules `TxReady` at this instant, and every queue pair the pause
    /// parked restarts on its next grid point strictly after it (ruling D3), each with one
    /// `resume` sender row (schema Amendment 2), in generator (`FlowId`) order.
    // Out of line: only host-link PFC images reach it.
    #[inline(never)]
    fn host_pfc_remote_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        header: crate::PfcHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let priority = usize::from(header.priority);
        let now = event.key.time_ns;
        let stop_time_ns = self.image.stop_time_ns;
        let image = self.image;
        // The validator pins every control lane into a host to the host's egress link.
        let (unpaused, transition) = {
            let (state, _) = self.host_parts_mut(node)?;
            let pfc = state
                .pfc
                .as_deref_mut()
                .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
            let before_controllers = pfc.paused_by_controller[priority]
                .iter()
                .copied()
                .collect::<Vec<_>>();
            let unpaused = if header.pause {
                pfc.paused_by_controller[priority].insert(event.key.origin_node);
                false
            } else {
                // A duplicate or early resume is an idempotent no-op; another controller may
                // still own the aggregate pause.
                pfc.paused_by_controller[priority].remove(&event.key.origin_node)
                    && !pfc.is_paused(priority)
            };
            let transition =
                crate::MechanismTransitionRecord::PfcControl(crate::PfcControlTransitionRecord {
                    key: event.key,
                    node: node.id,
                    queue_id: 0,
                    controlled_link: header.controlled_link,
                    controller: event.key.origin_node,
                    priority: header.priority,
                    action: if header.pause {
                        crate::PfcControlAction::Pause
                    } else {
                        crate::PfcControlAction::Resume
                    },
                    before_controllers,
                    after_controllers: pfc.paused_by_controller[priority].iter().copied().collect(),
                });
            (unpaused, transition)
        };
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.push(transition);
        }
        self.mark_terminal(event.payload)?;
        if !unpaused {
            return Ok(());
        }
        let ready_payload = match self.host_pfc_first_eligible(node)? {
            Some(position) => {
                let (state, _) = self.host_parts_mut(node)?;
                if state.in_service.is_none() && !*state.tx_ready_pending {
                    *state.tx_ready_pending = true;
                    Some(state.queue[position])
                } else {
                    None
                }
            }
            None => None,
        };
        // Restart the pause-parked queue pairs of this class.
        let mut restarts = Vec::new();
        let mut records = Vec::new();
        {
            let (mut state, _) = self.host_parts_mut(node)?;
            let parked = std::mem::take(
                &mut state
                    .pfc
                    .as_deref_mut()
                    .ok_or(ExecutionError::InvalidSchedulerState(node.id))?
                    .pause_parked[priority],
            );
            for position in parked {
                // The validator pins every listed position to a queue pair of this host.
                let generator = &mut state.generators[position];
                let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
                    return Err(ExecutionError::InvalidSchedulerState(node.id));
                };
                let before = crate::roce::RoceSenderView::of(generator, &roce);
                let flow = generator.flow;
                // A phase-0 transition: the controller instants before `now` apply first.
                let complete = roce.snd_una >= roce.pacer.total_bytes;
                let materialized = dcqcn_materialize(&mut roce.controller, complete, now)
                    .filter(|(_, advance)| advance.applied_rate_change());
                if let Some((controller_before, advance)) = materialized {
                    records.push(crate::MechanismTransitionRecord::Dcqcn(dcqcn_record(
                        event.key,
                        node.id,
                        flow,
                        crate::DcqcnTransitionKind::Advance,
                        now,
                        advance,
                        false,
                        controller_before,
                        roce.controller,
                    )));
                }
                let tick_ns = restart_roce_pacer(generator, &mut roce, now, stop_time_ns)?;
                settle_roce_sender(generator, &roce, generator.next_emission.status);
                generator.kind = FlowGeneratorKind::Roce(roce);
                records.push(roce_sender_record(
                    event.key,
                    node.id,
                    flow,
                    crate::RoceSenderKind::Resume,
                    None,
                    image_flow_priority(image, flow)?,
                    &roce,
                    None,
                    None,
                    None,
                    before,
                    crate::roce::RoceSenderView::of(generator, &roce),
                ));
                restarts.extend(tick_ns.map(|time_ns| (roce.pacing_timer_payload, time_ns)));
            }
        }
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.extend(records);
        }
        if let Some(payload) = ready_payload {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: now,
                },
                children,
            )?;
        }
        for (payload, time_ns) in restarts {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacingTimer,
                    payload,
                    time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    fn host_remote_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let packet = self.packet(event.payload)?;
        let (flow_id, flow_source, flow_target) = {
            let flow = self.flow(packet.flow)?;
            (flow.id, flow.source, flow.target)
        };
        // The closed-loop kinds own their arrival transitions; the arms are disjoint.
        match packet.kind {
            PacketKind::TcpData(header) => {
                return self.host_tcp_data_arrival(node, event, packet, header, children);
            }
            PacketKind::TcpAck(header) => {
                return self.host_tcp_ack_arrival(node, event, packet, header, children);
            }
            PacketKind::DcqcnCnp(header) => {
                return self.host_dcqcn_cnp_arrival(node, event, packet, header);
            }
            PacketKind::RoceData(header) => {
                return self.host_roce_data_arrival(node, event, packet, header, children);
            }
            PacketKind::RoceAck(header) => {
                return self
                    .host_roce_feedback_arrival(node, event, packet, header, false, children);
            }
            PacketKind::RoceNack(header) => {
                return self
                    .host_roce_feedback_arrival(node, event, packet, header, true, children);
            }
            // Host-link PFC. Dispatched here, after the flow lookup (a PFC frame carries the
            // flow of the packet that triggered it), not by an early return before it: the
            // early return made LLVM stop inlining the TCP and DCQCN CNP arrival handlers into
            // `dispatch` (`hostpfc-impl/tooling/inline-probe/`).
            PacketKind::Pfc(header) => {
                return self.host_pfc_remote_arrival(node, event, header, children);
            }
            PacketKind::StageNotify => {
                return self.host_notify_arrival(node, event, packet, children);
            }
            _ => {}
        }
        if packet.kind == PacketKind::Data
            && self
                .host_state(node)?
                .dcqcn_receivers
                .iter()
                .any(|receiver| receiver.flow == packet.flow)
        {
            return self.host_dcqcn_data_arrival(node, event, packet, children);
        }
        let expected_target = if packet.kind.is_data() {
            flow_target
        } else {
            flow_source
        };
        let (disposition, feedback_action) = {
            let state = self.host_state_mut(node)?;
            let feedback_generator = packet
                .kind
                .is_feedback()
                .then(|| {
                    state
                        .generators
                        .iter_mut()
                        .find(|generator| generator.flow == packet.flow)
                })
                .flatten();
            if let Some(generator) = feedback_generator {
                (
                    ArrivalDisposition::Feedback,
                    apply_generator_feedback(generator, node.id)?,
                )
            } else {
                if expected_target != node.id {
                    return Err(ExecutionError::FlowRouteMiss {
                        flow: flow_id,
                        node: node.id,
                    });
                }
                state.received_packets = state
                    .received_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                (ArrivalDisposition::Delivered, GeneratorFeedbackAction::None)
            }
        };
        self.record_arrival(node.id, packet, event.key, disposition)?;
        self.mark_terminal(event.payload)?;
        if let GeneratorFeedbackAction::Emit { flow, size_bytes } = feedback_action {
            self.emit_feedback_driven_packet(node, event, flow, size_bytes, children)?;
        }
        Ok(())
    }

    fn host_dcqcn_data_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let flow = self.flow(packet.flow)?;
        if flow.target != node.id {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: node.id,
            });
        }
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let cnp_plan = {
            let state = self.host_state_mut(node)?;
            let receiver = state
                .dcqcn_receivers
                .iter_mut()
                .find(|receiver| receiver.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
            state.received_packets = state
                .received_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            if !crate::roce::notification_point_sends_cnp(
                receiver,
                event.key.time_ns,
                packet.ecn_codepoint() == crate::EcnCodepoint::Ce,
            ) {
                None
            } else {
                let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                state.next_payload_seq = state
                    .next_payload_seq
                    .checked_add(1)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                state.sourced_packets = state
                    .sourced_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                let schedule_ready = state.in_service.is_none() && !state.tx_ready_pending;
                if schedule_ready {
                    state.tx_ready_pending = true;
                }
                Some((payload, receiver.cnp_size_bytes, schedule_ready))
            }
        };
        self.record_arrival(node.id, packet, event.key, ArrivalDisposition::Delivered)?;
        self.mark_terminal(packet.id)?;
        if let Some((payload, size_bytes, schedule_ready)) = cnp_plan {
            let cnp = PacketDescriptor {
                id: payload,
                flow: packet.flow,
                size_bytes,
                ecn_marked: false,
                kind: PacketKind::DcqcnCnp(crate::DcqcnCnpHeader {
                    trigger_payload: packet.id,
                }),
            };
            self.insert_packet(cnp, Some(event.key.time_ns))?;
            self.enqueue_source_packet(node, payload)?;
            self.record_sourced(node.id, cnp)?;
            if schedule_ready {
                self.emit_from_host(
                    node,
                    event,
                    ChildEmission {
                        target: node.id,
                        kind: EventKind::TxReady,
                        payload,
                        time_ns: event.key.time_ns,
                    },
                    children,
                )?;
            }
        }
        Ok(())
    }

    fn host_dcqcn_cnp_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        _header: crate::DcqcnCnpHeader,
    ) -> Result<(), ExecutionError> {
        let flow = self.flow(packet.flow)?;
        if flow.source != node.id {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: node.id,
            });
        }
        let transition = {
            // Keyed: a host can hold many queue pairs (the first generator of the flow is the
            // one the retired linear find returned).
            let (mut state, index) = self.host_parts_mut(node)?;
            let position =
                index
                    .first_generator(packet.flow)
                    .ok_or(ExecutionError::UnknownGenerator {
                        node: node.id,
                        flow: packet.flow,
                    })?;
            let generator = &mut state.generators[position];
            generator.feedback.arrivals = generator
                .feedback
                .arrivals
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            // A feedback (P16 ruling D4): the controller instants before `now` apply, then the
            // feedback. A complete flow's controller is frozen and ignores it (ruling D11). A
            // queue pair's rate change also moves its pacer's next-tick prediction.
            let now = event.key.time_ns;
            let applied = match generator.kind {
                FlowGeneratorKind::Dcqcn(mut dcqcn) => {
                    if !matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                    ) {
                        None
                    } else {
                        let before = dcqcn.controller;
                        let advance = dcqcn.controller.on_feedback(now);
                        dcqcn.rate.rate_numerator_bits_per_second =
                            dcqcn.controller.current_rate_bps;
                        generator.kind = FlowGeneratorKind::Dcqcn(dcqcn);
                        Some((before, advance, dcqcn.controller))
                    }
                }
                FlowGeneratorKind::Roce(mut roce) => {
                    if roce.snd_una >= roce.pacer.total_bytes {
                        None
                    } else {
                        let before = roce.controller;
                        let advance = roce.controller.on_feedback(now);
                        settle_roce_sender(generator, &roce, generator.next_emission.status);
                        generator.kind = FlowGeneratorKind::Roce(roce);
                        Some((before, advance, roce.controller))
                    }
                }
                _ => {
                    return Err(ExecutionError::UnknownGenerator {
                        node: node.id,
                        flow: packet.flow,
                    });
                }
            };
            applied.map(|(before, advance, after)| {
                dcqcn_record(
                    event.key,
                    node.id,
                    packet.flow,
                    crate::DcqcnTransitionKind::Feedback,
                    now,
                    advance,
                    false,
                    before,
                    after,
                )
            })
        };
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions
                .extend(transition.map(crate::MechanismTransitionRecord::Dcqcn));
        }
        self.record_arrival(node.id, packet, event.key, ArrivalDisposition::Feedback)?;
        self.mark_terminal(packet.id)
    }

    fn host_tcp_data_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        header: TcpDataHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let flow = self.flow(packet.flow)?;
        if flow.target != node.id {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: node.id,
            });
        }
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let (ack_payload, acknowledgment, ack_size_bytes, stage_causes) = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let receiver = &mut state.tcp_receivers[index.first_receiver(packet.flow).ok_or(
                ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                },
            )?];
            let end = header
                .sequence
                .checked_add(packet.size_bytes)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let frontier_before = receiver.next_expected_sequence;
            tcp_receive_range(receiver, header.sequence, end);
            let advanced = receiver.next_expected_sequence - frontier_before;
            // Every segment of a pending inbound predecessor is certified, including duplicate and
            // out-of-order ones that do not advance the frontier, so the certificate can replay
            // the receiver exactly.
            let mut stage_causes = PendingCauses::default();
            record_inbound_progress(
                &state.generators,
                &mut state.stages,
                index,
                packet.flow,
                InboundProgress::Segment {
                    sequence: header.sequence,
                    bytes: packet.size_bytes,
                    advance: advanced,
                },
                node.id,
                &mut stage_causes,
            )?;
            let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            *state.next_payload_seq = state
                .next_payload_seq
                .checked_add(1)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            *state.received_packets = state
                .received_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            *state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            (
                payload,
                receiver.next_expected_sequence,
                receiver.ack_size_bytes,
                stage_causes,
            )
        };
        self.record_arrival(node.id, packet, event.key, ArrivalDisposition::Delivered)?;
        self.mark_terminal(packet.id)?;
        let ack = PacketDescriptor {
            id: ack_payload,
            flow: packet.flow,
            size_bytes: ack_size_bytes,
            ecn_marked: false,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment,
                acknowledged_bytes: packet.size_bytes,
                echoed_sent_time_ns: header.sent_time_ns,
            }),
        };
        self.insert_packet(ack, Some(event.key.time_ns))?;
        self.enqueue_source_packet(node, ack.id)?;
        self.record_sourced(node.id, ack)?;
        let ready = {
            let (state, _) = self.host_parts_mut(node)?;
            if state.in_service.is_none() && !*state.tx_ready_pending {
                *state.tx_ready_pending = true;
                true
            } else {
                false
            }
        };
        if ready {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload: ack.id,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }
        if !stage_causes.is_empty() {
            self.activate_ready_collectives(node, event, stage_causes, children)?;
        }
        Ok(())
    }

    fn host_tcp_ack_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        header: TcpAckHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let flow = self.flow(packet.flow)?;
        if flow.source != node.id {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: node.id,
            });
        }
        self.record_arrival(node.id, packet, event.key, ArrivalDisposition::Feedback)?;
        self.mark_terminal(packet.id)?;

        let (
            retransmit_sequence,
            fill_window,
            acknowledged_through,
            transition,
            scheduled_send_pending,
            superseded_timer,
            stage_completed,
        ) = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position =
                index
                    .first_generator(packet.flow)
                    .ok_or(ExecutionError::UnknownGenerator {
                        node: node.id,
                        flow: packet.flow,
                    })?;
            let is_stage = state.stages.stage(position).is_some();
            let generator = &mut state.generators[position];
            let FlowGeneratorKind::Tcp(mut tcp) = generator.kind else {
                return Err(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                });
            };
            apply_generator_feedback(generator, node.id)?;
            let acknowledgment = header.acknowledgment.min(tcp.next_sequence);
            let before = tcp.control;
            let mut input = None;
            let mut retransmit = None;
            let mut fill = false;
            let mut acknowledged_through = None;
            // Live-state contract (T20g item 2): a timer that stops being the flow's armed timer
            // stops being pending state. Every `Some -> None` transition below reports the
            // superseded identity so the queue owner removes its event in this same transition.
            let mut superseded = None;
            if acknowledgment > tcp.highest_ack {
                let acknowledged_bytes = acknowledgment - tcp.highest_ack;
                let flight_before = tcp.bytes_in_flight;
                let rtt_sample = event
                    .key
                    .time_ns
                    .saturating_sub(header.echoed_sent_time_ns)
                    .max(1);
                tcp.rto_ns =
                    crate::tcp::update_rto_ns(&mut tcp.srtt_ns, &mut tcp.rtt_var_ns, rtt_sample);
                tcp.control.on_new_ack(
                    acknowledged_bytes,
                    event.key.time_ns,
                    rtt_sample,
                    flight_before,
                    acknowledgment,
                );
                input = Some(TcpTransitionInput::NewAck {
                    acknowledged_bytes,
                    rtt_sample_ns: rtt_sample,
                    flight_size_bytes: flight_before,
                    acknowledgment,
                });
                tcp.bytes_in_flight = tcp.bytes_in_flight.saturating_sub(acknowledged_bytes);
                tcp.highest_ack = acknowledgment;
                acknowledged_through = Some(acknowledgment);
                tcp.duplicate_acks = 0;
                superseded = tcp.active_timer.take();
                if tcp.control.phase() == crate::TcpPhase::FastRecovery
                    && acknowledgment < tcp.recovery_high_sequence
                {
                    retransmit = Some(acknowledgment);
                }
                fill = acknowledgment < tcp.total_bytes;
            } else if acknowledgment == tcp.highest_ack
                && tcp.highest_ack < tcp.total_bytes
                && tcp.bytes_in_flight != 0
            {
                input = Some(TcpTransitionInput::DuplicateAck {
                    flight_size_bytes: tcp.bytes_in_flight,
                    recovery_high_sequence: tcp.next_sequence,
                });
                let fast_retransmit = tcp
                    .control
                    .on_duplicate_ack(tcp.bytes_in_flight, event.key.time_ns);
                tcp.duplicate_acks = tcp.control.duplicate_acks();
                if fast_retransmit {
                    tcp.recovery_high_sequence = tcp.next_sequence;
                    tcp.control
                        .set_recovery_high_sequence(tcp.recovery_high_sequence);
                    superseded = tcp.active_timer.take();
                    retransmit = Some(tcp.highest_ack);
                } else if tcp.duplicate_acks > 3 {
                    fill = true;
                }
            }
            generator.feedback.outstanding_bytes = tcp.bytes_in_flight;
            generator.feedback.unacknowledged_bytes = tcp.bytes_in_flight;
            generator.kind = FlowGeneratorKind::Tcp(tcp);
            let scheduled_send_pending =
                generator.next_emission.status == GeneratorStatus::Scheduled;
            let transition = input.map(|input| TcpTransitionRecord {
                key: event.key,
                node: node.id,
                flow: packet.flow,
                mss_bytes: tcp.mss_bytes,
                input,
                before,
                after: tcp.control,
            });
            // A wrapped stage's local successors wait for its last byte to be acknowledged. Only
            // the new ACK that first reaches the total advances `highest_ack` to it.
            let stage_completed = is_stage
                && acknowledged_through.is_some_and(|acknowledged| acknowledged >= tcp.total_bytes);
            (
                retransmit,
                fill,
                acknowledged_through,
                transition,
                scheduled_send_pending,
                superseded,
                stage_completed,
            )
        };
        let mut stage_causes = PendingCauses::default();
        if stage_completed {
            // The ACK echoes when its triggering segment was sent and how large it was, so the
            // sender can certify the unloaded round trip that segment and this ACK needed: every
            // link of both routes serializes and propagates them at least once.
            let (route, reverse_route) = {
                let descriptor = self.flow(packet.flow)?;
                (descriptor.route.clone(), descriptor.reverse_route.clone())
            };
            let mut unloaded_round_trip_ns = 0_u64;
            for (links, bytes) in [
                (&route, header.acknowledged_bytes),
                (&reverse_route, packet.size_bytes),
            ] {
                for link in links {
                    let delay = self
                        .link(*link)?
                        .delay_ns(bytes)
                        .map_err(|_| ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                    unloaded_round_trip_ns = unloaded_round_trip_ns
                        .checked_add(delay)
                        .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                }
            }
            let completion = CompletionSignal {
                ack_number: acknowledged_through.expect("a completing ACK advances the sender"),
                origin_ns: header.echoed_sent_time_ns,
                delay_ns: unloaded_round_trip_ns,
            };
            let (mut state, index) = self.host_parts_mut(node)?;
            complete_local_successors(
                &state.generators,
                &mut state.stages,
                index,
                packet.flow,
                completion,
                &mut stage_causes,
            );
        }
        if let Some(timer) = superseded_timer {
            self.cancel_superseded_timer(node.id, timer);
        }
        if let Some(acknowledgment) = acknowledged_through {
            acknowledge_tcp_segments(&mut self.tcp_sent_segments, packet.flow, acknowledgment)?;
        }
        let sender_transition = transition.is_some();
        if self.observation_mode == ObservationMode::Full {
            self.tcp_transitions.extend(transition);
        }
        // A Scheduled TCP descriptor already reserves next_sequence and owns its pending
        // PacketArrival. ACKs may update the sender and ledger, and loss recovery may retransmit
        // an older sequence, but fresh window fill must wait for the reserved event.
        if sender_transition && !(scheduled_send_pending && retransmit_sequence.is_none()) {
            let plan = self.prepare_tcp_attempts(
                node,
                packet.flow,
                event.key.time_ns,
                retransmit_sequence,
                fill_window && !scheduled_send_pending,
                scheduled_send_pending,
            )?;
            self.install_tcp_attempts(node, event, plan, children)?;
        }
        if !stage_causes.is_empty() {
            self.activate_ready_collectives(node, event, stage_causes, children)?;
        }
        Ok(())
    }

    fn host_retransmission_timeout(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let mut timed_out = None;
        {
            let state = self.host_state_mut(node)?;
            for generator in &mut state.generators {
                let FlowGeneratorKind::Tcp(mut tcp) = generator.kind else {
                    continue;
                };
                let Some(timer) = tcp.active_timer else {
                    continue;
                };
                if timer.attempt != event.payload || timer.deadline_ns != event.key.time_ns {
                    continue;
                }
                tcp.active_timer = None;
                let before = tcp.control;
                let flight_size_bytes = tcp.bytes_in_flight;
                tcp.control
                    .on_timeout(tcp.bytes_in_flight, event.key.time_ns);
                tcp.rto_ns = timer.rto_ns.saturating_mul(2).min(60_000_000_000);
                let sequence = tcp.highest_ack;
                generator.kind = FlowGeneratorKind::Tcp(tcp);
                timed_out = Some((
                    generator.flow,
                    sequence,
                    TcpTransitionRecord {
                        key: event.key,
                        node: node.id,
                        flow: generator.flow,
                        mss_bytes: tcp.mss_bytes,
                        input: TcpTransitionInput::Timeout { flight_size_bytes },
                        before,
                        after: tcp.control,
                    },
                ));
                break;
            }
        }
        if timed_out.is_none()
            && self
                .packets
                .get(&event.payload)
                .is_some_and(|resident| resident.descriptor.kind == PacketKind::RocePacingTimer)
        {
            return self.host_roce_timeout(node, event, children);
        }
        let Some((flow, sequence, transition)) = timed_out else {
            // Live-state contract (T20g item 2): execution removes a superseded timer event at
            // the transition that supersedes it, so this lazy recognition is unreachable for any
            // event this run armed. Only legacy residue imported by the image can reach it.
            #[cfg(debug_assertions)]
            debug_assert!(
                self.imported_event(event.key),
                "superseded retransmission timeout {:?} survived its disarm transition",
                event.key
            );
            return Ok(());
        };
        if self.observation_mode == ObservationMode::Full {
            self.tcp_transitions.push(transition);
        }
        let plan =
            self.prepare_tcp_attempts(node, flow, event.key.time_ns, Some(sequence), false, false)?;
        self.install_tcp_attempts(node, event, plan, children)
    }

    fn host_pacing_timer(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let packet = self.packet(event.payload)?;
        // A queue pair's pacing token names its own transition: no generator scan is needed.
        if packet.kind == PacketKind::RocePacingTimer {
            return self.host_roce_pacing_timer(node, event, packet, children);
        }
        // A stage notify's token names its sender's timer (P16 H2).
        if packet.kind == PacketKind::StageNotify {
            return self.host_notify_timer(node, event, packet.flow, children);
        }
        let compute_stage_owns = {
            let (state, index) = self.host_parts_mut(node)?;
            index
                .generators_of(packet.flow)
                .iter()
                .any(|(_, position)| {
                    state
                        .stages
                        .stage(*position)
                        .is_some_and(|stage| matches!(stage.role, crate::StageRole::Compute(_)))
                })
        };
        if compute_stage_owns {
            return self.host_compute_timer(node, event, packet.flow, children);
        }
        let dcqcn_owns = {
            let (state, index) = self.host_parts_mut(node)?;
            index
                .generators_of(packet.flow)
                .iter()
                .any(|(_, position)| {
                    matches!(
                        state.generators[*position].kind,
                        FlowGeneratorKind::Dcqcn(_)
                    )
                })
        };
        if dcqcn_owns {
            return self.host_dcqcn_pacing_timer(node, event, packet, children);
        }
        let stop_time_ns = self.image.stop_time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let mut emitted = false;
        let mut next_packet = None;
        let mut next_timer = None;
        let mut terminal_unused_token = false;
        let transition;

        {
            let (mut state, index) = self.host_parts_mut(node)?;
            let Some(generator_index) = index.first_generator(packet.flow) else {
                return Ok(());
            };
            let generator = &mut state.generators[generator_index];
            let FlowGeneratorKind::Rate(mut rate) = generator.kind else {
                return Ok(());
            };
            if !matches!(
                generator.next_emission.status,
                GeneratorStatus::Scheduled | GeneratorStatus::Blocked
            ) || generator.next_emission.payload != event.payload
                || generator.next_emission.departure_time_ns != event.key.time_ns
            {
                return Ok(());
            }

            let config = crate::RateReplayConfig {
                pacing_interval_ns: rate.pacing_interval_ns,
                packet_size_bytes: rate.packet_size_bytes,
                total_bytes: rate.total_bytes,
                rate_numerator_bits_per_second: rate.rate_numerator_bits_per_second,
                rate_denominator: rate.rate_denominator,
            };
            let before = crate::RateReplayState {
                packets_emitted: generator.packets_emitted,
                bytes_emitted: generator.bytes_emitted,
                credit_quanta: rate.credit_quanta,
                status: generator.next_emission.status,
                next_time_ns: generator.next_emission.departure_time_ns,
            };

            let scale = pacing_credit_scale(rate.rate_denominator)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let tick_credit =
                pacing_tick_credit(rate.rate_numerator_bits_per_second, rate.pacing_interval_ns)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let packet_cost = paced_packet_cost(packet.size_bytes, scale)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            rate.credit_quanta = rate
                .credit_quanta
                .checked_add(tick_credit)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            if rate.credit_quanta >= packet_cost {
                rate.credit_quanta -= packet_cost;
                generator.packets_emitted = generator
                    .packets_emitted
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                generator.bytes_emitted = generator
                    .bytes_emitted
                    .checked_add(packet.size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                *state.sourced_packets = state
                    .sourced_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                emitted = true;
            }

            let finished = generator.bytes_emitted >= rate.total_bytes;
            let candidate_time = (!finished)
                .then(|| {
                    event
                        .key
                        .time_ns
                        .checked_add(rate.pacing_interval_ns)
                        .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))
                })
                .transpose()?;
            if let Some(next_time) = candidate_time.filter(|time| *time <= stop_time_ns) {
                let (payload, size_bytes) = if emitted {
                    let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    *state.next_payload_seq = state
                        .next_payload_seq
                        .checked_add(1)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    let remaining = rate.total_bytes - generator.bytes_emitted;
                    (payload, rate.packet_size_bytes.min(remaining))
                } else {
                    (packet.id, packet.size_bytes)
                };
                let next_cost = paced_packet_cost(size_bytes, scale)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                generator.next_emission = crate::ScheduledEmission {
                    status: if rate
                        .credit_quanta
                        .checked_add(tick_credit)
                        .is_some_and(|credit| credit >= next_cost)
                    {
                        GeneratorStatus::Scheduled
                    } else {
                        GeneratorStatus::Blocked
                    },
                    departure_time_ns: next_time,
                    payload,
                };
                if emitted {
                    next_packet = Some(PacketDescriptor {
                        id: payload,
                        flow: packet.flow,
                        size_bytes,
                        ecn_marked: false,
                        kind: PacketKind::Data,
                    });
                }
                next_timer = Some((payload, next_time));
            } else {
                generator.next_emission.status = if finished {
                    GeneratorStatus::Finished
                } else {
                    GeneratorStatus::Stopped
                };
                if let Some(candidate_time) = candidate_time {
                    generator.next_emission.departure_time_ns = candidate_time;
                }
                terminal_unused_token = !emitted;
            }
            generator.kind = FlowGeneratorKind::Rate(rate);
            transition = crate::MechanismTransitionRecord::Rate(crate::RateTransitionRecord {
                key: event.key,
                node: node.id,
                flow: packet.flow,
                payload: packet.id,
                stop_time_ns,
                current_packet_size_bytes: packet.size_bytes,
                config,
                before,
                after: crate::RateReplayState {
                    packets_emitted: generator.packets_emitted,
                    bytes_emitted: generator.bytes_emitted,
                    credit_quanta: rate.credit_quanta,
                    status: generator.next_emission.status,
                    next_time_ns: generator.next_emission.departure_time_ns,
                },
            });
        }

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.push(transition);
        }

        if emitted {
            self.set_source_time(packet.id, event.key.time_ns)?;
            self.enqueue_source_packet(node, packet.id)?;
            self.record_sourced(node.id, packet)?;
        } else if terminal_unused_token {
            self.mark_terminal(packet.id)?;
        }
        if let Some(next_packet) = next_packet {
            self.insert_packet(next_packet, None)?;
        }
        if let Some((payload, time_ns)) = next_timer {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacingTimer,
                    payload,
                    time_ns,
                },
                children,
            )?;
        }
        if emitted {
            let ready_payload = {
                let (state, _) = self.host_parts_mut(node)?;
                if state.in_service.is_none() && !*state.tx_ready_pending {
                    *state.tx_ready_pending = true;
                    Some(packet.id)
                } else {
                    None
                }
            };
            if let Some(payload) = ready_payload {
                self.emit_from_host(
                    node,
                    event,
                    ChildEmission {
                        target: node.id,
                        kind: EventKind::TxReady,
                        payload,
                        time_ns: event.key.time_ns,
                    },
                    children,
                )?;
            }
        }
        Ok(())
    }

    fn host_dcqcn_pacing_timer(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let stop_time_ns = self.image.stop_time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let mut emitted = false;
        let mut next_packet = None;
        let mut next_timer = None;
        let mut terminal_unused_token = false;
        let mut tick_transition = None;
        let full = self.observation_mode == ObservationMode::Full;

        {
            let state = self.host_state_mut(node)?;
            let generator = state
                .generators
                .iter_mut()
                .find(|generator| generator.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
            let FlowGeneratorKind::Dcqcn(mut dcqcn) = generator.kind else {
                return Ok(());
            };
            if !matches!(
                generator.next_emission.status,
                GeneratorStatus::Scheduled | GeneratorStatus::Blocked
            ) || generator.next_emission.payload != event.payload
                || generator.next_emission.departure_time_ns != event.key.time_ns
            {
                return Ok(());
            }

            // A phase-1 transition: the controller instants at or before `now` apply before the
            // tick reads the rate (P16 ruling D2).
            let bound = event.key.time_ns.saturating_add(1);
            let controller_before = dcqcn.controller;
            let mut advance = dcqcn_materialize(&mut dcqcn.controller, false, bound)
                .map_or(crate::DcqcnAdvance::default(), |(_, advance)| advance);
            let scale = pacing_credit_scale(dcqcn.rate.rate_denominator)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let tick_credit = pacing_tick_credit(
                dcqcn.controller.current_rate_bps,
                dcqcn.rate.pacing_interval_ns,
            )
            .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let packet_cost = paced_packet_cost(packet.size_bytes, scale)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            dcqcn.rate.credit_quanta = dcqcn
                .rate
                .credit_quanta
                .checked_add(tick_credit)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            if dcqcn.rate.credit_quanta >= packet_cost {
                dcqcn.rate.credit_quanta -= packet_cost;
                generator.packets_emitted = generator
                    .packets_emitted
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                generator.bytes_emitted = generator
                    .bytes_emitted
                    .checked_add(packet.size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                state.sourced_packets = state
                    .sourced_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                emitted = true;
            }
            dcqcn.rate.rate_numerator_bits_per_second = dcqcn.controller.current_rate_bps;

            let finished = generator.bytes_emitted >= dcqcn.rate.total_bytes;
            let candidate_time = (!finished)
                .then(|| {
                    event
                        .key
                        .time_ns
                        .checked_add(dcqcn.rate.pacing_interval_ns)
                        .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))
                })
                .transpose()?;
            if let Some(next_time) = candidate_time.filter(|time| *time <= stop_time_ns) {
                let (payload, size_bytes) = if emitted {
                    let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    state.next_payload_seq = state
                        .next_payload_seq
                        .checked_add(1)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    let remaining = dcqcn.rate.total_bytes - generator.bytes_emitted;
                    (payload, dcqcn.rate.packet_size_bytes.min(remaining))
                } else {
                    (packet.id, packet.size_bytes)
                };
                let next_tick_credit = pacing_tick_credit(
                    dcqcn.controller.current_rate_bps,
                    dcqcn.rate.pacing_interval_ns,
                )
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
                let next_cost = paced_packet_cost(size_bytes, scale)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                generator.next_emission = crate::ScheduledEmission {
                    status: if dcqcn
                        .rate
                        .credit_quanta
                        .checked_add(next_tick_credit)
                        .is_some_and(|credit| credit >= next_cost)
                    {
                        GeneratorStatus::Scheduled
                    } else {
                        GeneratorStatus::Blocked
                    },
                    departure_time_ns: next_time,
                    payload,
                };
                if emitted {
                    next_packet = Some(PacketDescriptor {
                        id: payload,
                        flow: packet.flow,
                        size_bytes,
                        ecn_marked: false,
                        kind: PacketKind::Data,
                    });
                }
                next_timer = Some((payload, next_time));
            } else {
                generator.next_emission.status = if finished {
                    GeneratorStatus::Finished
                } else {
                    GeneratorStatus::Stopped
                };
                // The flow is complete: its controller freezes at this tick (ruling D11).
                let settled = dcqcn.controller.settle(bound);
                advance.alpha_ticks += settled.alpha_ticks;
                advance.increase_fires += settled.increase_fires;
                advance.decrease_cuts += settled.decrease_cuts;
                if let Some(candidate_time) = candidate_time {
                    generator.next_emission.departure_time_ns = candidate_time;
                }
                terminal_unused_token = !emitted;
            }
            dcqcn.rate.rate_numerator_bits_per_second = dcqcn.controller.current_rate_bps;
            // Every tick of an unreliable flow is a row (ruling D17); the rate it read is the
            // row's `after.current_rate_bps`, unchanged by a freeze.
            if full {
                tick_transition = Some(dcqcn_record(
                    event.key,
                    node.id,
                    packet.flow,
                    crate::DcqcnTransitionKind::Tick,
                    bound,
                    advance,
                    !matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                    ),
                    controller_before,
                    dcqcn.controller,
                ));
            }
            generator.kind = FlowGeneratorKind::Dcqcn(dcqcn);
        }

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions
                .extend(tick_transition.map(crate::MechanismTransitionRecord::Dcqcn));
        }
        if emitted {
            self.set_source_time(packet.id, event.key.time_ns)?;
            self.enqueue_source_packet(node, packet.id)?;
            self.record_sourced(node.id, packet)?;
        } else if terminal_unused_token {
            self.mark_terminal(packet.id)?;
        }
        if let Some(packet) = next_packet {
            self.insert_packet(packet, None)?;
        }
        if let Some((payload, time_ns)) = next_timer {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacingTimer,
                    payload,
                    time_ns,
                },
                children,
            )?;
        }
        if emitted {
            let ready_payload = {
                let state = self.host_state_mut(node)?;
                if state.in_service.is_none() && !state.tx_ready_pending {
                    state.tx_ready_pending = true;
                    Some(packet.id)
                } else {
                    None
                }
            };
            if let Some(payload) = ready_payload {
                self.emit_from_host(
                    node,
                    event,
                    ChildEmission {
                        target: node.id,
                        kind: EventKind::TxReady,
                        payload,
                        time_ns: event.key.time_ns,
                    },
                    children,
                )?;
            }
        }
        Ok(())
    }

    /// A RoCE queue pair's pacing tick (design note §5, step 2): one tick of credit at the
    /// controller's current rate; the packet at `next_psn`, a first transmission or a Go-back-N
    /// retransmission, is sent when the credit covers it. The pacer re-arms one interval later
    /// while packets remain, or parks.
    // Out of line: queue-pair transitions must not grow the shared `dispatch` that every
    // event of every image runs through (P15 inlining check, `qp-impl/callsites.txt`).
    #[inline(never)]
    fn host_roce_pacing_timer(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let now = event.key.time_ns;
        let stop_time_ns = self.image.stop_time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let image = self.image;
        // Records are kept only under full observation, so only then are they built: a summary
        // run of a queue pair pays for neither the views nor the data-class lookup.
        let full = self.observation_mode == ObservationMode::Full;
        let unexpected = ExecutionError::UnexpectedGeneratorEmission {
            node: node.id,
            flow: packet.flow,
            payload: event.payload,
        };
        let (emission, next_tick_ns, timer_ns, dcqcn_transition, record) = 'tick: {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position = index.first_generator(packet.flow).ok_or(unexpected)?;
            let generator = &mut state.generators[position];
            let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
                return Err(unexpected);
            };
            if !roce.pacer_armed
                || roce.pacing_timer_payload != event.payload
                || generator.next_emission.departure_time_ns != now
            {
                return Err(unexpected);
            }
            let before = full.then(|| crate::roce::RoceSenderView::of(generator, &roce));
            roce.pacer_armed = false;
            // A phase-1 transition: the controller instants at or before `now` apply before the
            // tick reads the rate or settles its prediction (P16 ruling D2).
            let bound = now.saturating_add(1);
            let complete = roce.snd_una >= roce.pacer.total_bytes;
            let dcqcn_transition = dcqcn_materialize(&mut roce.controller, complete, bound)
                .filter(|(_, advance)| full && advance.applied_rate_change())
                .map(|(controller_before, advance)| {
                    dcqcn_record(
                        event.key,
                        node.id,
                        packet.flow,
                        crate::DcqcnTransitionKind::Advance,
                        bound,
                        advance,
                        false,
                        controller_before,
                        roce.controller,
                    )
                });
            let total = roce.pacer.total_bytes;
            // Host-link PFC, tested first (LeanGuard's writer contract, `leanguard.md` §12): a
            // tick that finds its data class paused at its host sends nothing, adds no credit and
            // parks, and a pacer with packets left joins the class's parked list for the RESUME.
            // A host without host-link PFC pays one `None` test.
            let paused_class = match state.pfc.as_deref_mut() {
                None => None,
                Some(pfc) => {
                    let class = image_flow_priority(image, packet.flow)?;
                    pfc.is_paused(usize::from(class)).then_some((pfc, class))
                }
            };
            if let Some((pfc, class)) = paused_class {
                if roce.next_psn < total && roce.snd_una < total {
                    pfc.pause_parked[usize::from(class)].insert(position);
                }
                let generator = &mut state.generators[position];
                settle_roce_sender(generator, &roce, GeneratorStatus::Blocked);
                generator.kind = FlowGeneratorKind::Roce(roce);
                let record = before.map(|before| {
                    roce_sender_record(
                        event.key,
                        node.id,
                        packet.flow,
                        crate::RoceSenderKind::Tick,
                        Some(crate::roce::TickPark::ClassPaused),
                        class,
                        &roce,
                        None,
                        None,
                        None,
                        before,
                        crate::roce::RoceSenderView::of(generator, &roce),
                    )
                });
                break 'tick (None, None, None, dcqcn_transition, record);
            }
            // P16 ruling D7: a tick that finds the window closed, at the rate as of the tick,
            // sends nothing, adds no credit and parks until feedback moves `snd_una`. Without a
            // window this is one zero test.
            if roce.window_bytes != 0 && roce.next_psn < total && crate::roce::window_bound(&roce) {
                roce.window_parked = true;
                let class = image_flow_priority(image, packet.flow)?;
                let generator = &mut state.generators[position];
                settle_roce_sender(generator, &roce, GeneratorStatus::Blocked);
                generator.kind = FlowGeneratorKind::Roce(roce);
                let record = before.map(|before| {
                    roce_sender_record(
                        event.key,
                        node.id,
                        packet.flow,
                        crate::RoceSenderKind::Tick,
                        Some(crate::roce::TickPark::WindowBlocked),
                        class,
                        &roce,
                        None,
                        None,
                        None,
                        before,
                        crate::roce::RoceSenderView::of(generator, &roce),
                    )
                });
                break 'tick (None, None, None, dcqcn_transition, record);
            }
            let mut rate_bps = None;
            let mut emission = None;
            let mut timer_ns = None;
            let overflow = ExecutionError::CounterOverflow(node.id);
            if roce.next_psn < total {
                rate_bps = Some(roce.controller.current_rate_bps);
                let scale = pacing_credit_scale(1).ok_or(overflow)?;
                let tick = pacing_tick_credit(
                    roce.controller.current_rate_bps,
                    roce.pacer.pacing_interval_ns,
                )
                .ok_or(overflow)?;
                roce.pacer.credit_quanta =
                    roce.pacer.credit_quanta.checked_add(tick).ok_or(overflow)?;
                let psn = roce.next_psn;
                let size = crate::roce::packet_size(&roce, psn);
                let cost = paced_packet_cost(size, scale).ok_or(overflow)?;
                if roce.pacer.credit_quanta >= cost {
                    roce.pacer.credit_quanta -= cost;
                    let retransmission = psn < generator.bytes_emitted;
                    let outstanding_before = roce.snd_una < generator.bytes_emitted;
                    let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    *state.next_payload_seq = state
                        .next_payload_seq
                        .checked_add(1)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    *state.sourced_packets =
                        state.sourced_packets.checked_add(1).ok_or(overflow)?;
                    let generator = &mut state.generators[position];
                    roce.next_psn = psn + size;
                    if !retransmission {
                        // First transmissions only: the high-water mark, as TCP counts them.
                        generator.bytes_emitted = roce.next_psn;
                        generator.packets_emitted =
                            generator.packets_emitted.checked_add(1).ok_or(overflow)?;
                    }
                    if roce.rto_ns != 0 && !outstanding_before {
                        let deadline = now
                            .checked_add(roce.rto_ns)
                            .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                        roce.rto_deadline_ns = deadline;
                        timer_ns = Some(deadline);
                    }
                    emission = Some(crate::RoceEmission {
                        psn,
                        bytes: size,
                        retransmission,
                        payload,
                    });
                }
            }
            let generator = &mut state.generators[position];
            let parked_status = if roce.next_psn < total {
                let next = now
                    .checked_add(roce.pacer.pacing_interval_ns)
                    .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                generator.next_emission.departure_time_ns = next;
                if next <= stop_time_ns {
                    roce.pacer_armed = true;
                    GeneratorStatus::Blocked
                } else {
                    GeneratorStatus::Stopped
                }
            } else {
                GeneratorStatus::Blocked
            };
            settle_roce_sender(generator, &roce, parked_status);
            generator.kind = FlowGeneratorKind::Roce(roce);
            let next_tick_ns = roce
                .pacer_armed
                .then_some(generator.next_emission.departure_time_ns);
            let record = match before {
                Some(before) => Some(roce_sender_record(
                    event.key,
                    node.id,
                    packet.flow,
                    crate::RoceSenderKind::Tick,
                    None,
                    image_flow_priority(image, packet.flow)?,
                    &roce,
                    rate_bps,
                    None,
                    emission,
                    before,
                    crate::roce::RoceSenderView::of(generator, &roce),
                )),
                None => None,
            };
            (emission, next_tick_ns, timer_ns, dcqcn_transition, record)
        };
        if full {
            self.mechanism_transitions
                .extend(dcqcn_transition.map(crate::MechanismTransitionRecord::Dcqcn));
            self.mechanism_transitions.extend(record);
        }
        if let Some(time_ns) = next_tick_ns {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacingTimer,
                    payload: event.payload,
                    time_ns,
                },
                children,
            )?;
        }
        if let Some(time_ns) = timer_ns {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::RetransmissionTimeout,
                    payload: event.payload,
                    time_ns,
                },
                children,
            )?;
        }
        if let Some(emission) = emission {
            let data = PacketDescriptor {
                id: emission.payload,
                flow: packet.flow,
                size_bytes: emission.bytes,
                ecn_marked: false,
                kind: PacketKind::RoceData(crate::RoceDataHeader {
                    psn: emission.psn,
                    sent_time_ns: now,
                    retransmission: emission.retransmission,
                }),
            };
            self.insert_packet(data, Some(now))?;
            self.enqueue_source_packet(node, data.id)?;
            self.record_sourced(node.id, data)?;
            self.schedule_host_ready(node, event, data.id, children)?;
        }
        Ok(())
    }

    /// A RoCE ACK or NACK at its queue pair's sender (design note §5, steps 4 and 5).
    ///
    /// A fresh ACK advances the cumulative acknowledgment; a NACK also rewinds `next_psn` to it
    /// (Go-back-N). An ACK at or below the acknowledgment, or a NACK below it, is stale. Every
    /// advance or rewind restarts the retransmission timeout (removing the superseded event in
    /// this transition) while data stays outstanding, and a parked pacer with packets to send
    /// again restarts on its next grid point.
    #[allow(clippy::too_many_arguments)]
    // Out of line: queue-pair transitions must not grow the shared `dispatch` that every
    // event of every image runs through (P15 inlining check, `qp-impl/callsites.txt`).
    #[inline(never)]
    fn host_roce_feedback_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        header: crate::RoceAckHeader,
        nack: bool,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let flow = self.flow(packet.flow)?;
        if flow.source != node.id {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: node.id,
            });
        }
        let data_class = flow.priority;
        let full = self.observation_mode == ObservationMode::Full;
        self.record_arrival(node.id, packet, event.key, ArrivalDisposition::Feedback)?;
        self.mark_terminal(packet.id)?;
        let now = event.key.time_ns;
        let stop_time_ns = self.image.stop_time_ns;
        let (superseded_ns, timer_ns, tick_ns, token, record, dcqcn_transition, completed_stage) = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position =
                index
                    .first_generator(packet.flow)
                    .ok_or(ExecutionError::UnknownGenerator {
                        node: node.id,
                        flow: packet.flow,
                    })?;
            let generator = &mut state.generators[position];
            let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
                return Err(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                });
            };
            generator.feedback.arrivals = generator
                .feedback
                .arrivals
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let before = full.then(|| crate::roce::RoceSenderView::of(generator, &roce));
            // A phase-0 transition: the controller instants before `now` apply first (P16 ruling
            // D2); the ACK that completes the pair then freezes the controller (ruling D11).
            let total = roce.pacer.total_bytes;
            let controller_before = roce.controller;
            let mut advance = dcqcn_materialize(&mut roce.controller, roce.snd_una >= total, now)
                .map_or(crate::DcqcnAdvance::default(), |(_, advance)| advance);
            let acknowledgment = header.acknowledgment;
            if acknowledgment > generator.bytes_emitted {
                return Err(ExecutionError::InconsistentRocePacket {
                    flow: packet.flow,
                    payload: packet.id,
                });
            }
            let stale = if nack {
                acknowledgment < roce.snd_una
            } else {
                acknowledgment <= roce.snd_una
            };
            let mut superseded_ns = None;
            let mut timer_ns = None;
            let mut tick_ns = None;
            let snd_una_before = roce.snd_una;
            if !stale {
                let outstanding_before = roce.snd_una < generator.bytes_emitted;
                roce.snd_una = roce.snd_una.max(acknowledgment);
                // After a spurious timeout an older copy can advance the acknowledgment past
                // the rewound next PSN.
                roce.next_psn = roce.next_psn.max(roce.snd_una);
                if nack {
                    roce.next_psn = roce.snd_una;
                }
                if roce.rto_ns != 0 {
                    if outstanding_before {
                        superseded_ns = Some(roce.rto_deadline_ns);
                    }
                    if roce.snd_una < generator.bytes_emitted {
                        let deadline = now
                            .checked_add(roce.rto_ns)
                            .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))?;
                        roce.rto_deadline_ns = deadline;
                        timer_ns = Some(deadline);
                    } else {
                        roce.rto_deadline_ns = 0;
                    }
                }
                tick_ns = restart_roce_pacer(generator, &mut roce, now, stop_time_ns)?;
            }
            // A pair without congestion control (P17) has no controller to freeze or feed: its
            // inert controller is never armed, and its sender ignores the ECN echo, so neither
            // transition runs and no DCQCN record is written. The mode is tested last, so a DCQCN
            // pair pays for it only at its completion and on an echoing ACK or NACK.
            let froze = snd_una_before < total
                && roce.snd_una >= total
                && roce.congestion_control == crate::RoceCongestionControl::Dcqcn;
            if froze {
                let settled = roce.controller.settle(now);
                advance.alpha_ticks += settled.alpha_ticks;
                advance.increase_fires += settled.increase_fires;
                advance.decrease_cuts += settled.decrease_cuts;
            }
            // The ECN echo is the pair's congestion feedback (ruling D4), ignored once the pair is
            // complete (ruling D11), as HPCC's `QpComplete` precedes `cnp_received_mlx`.
            let feedback = header.ce_echo
                && roce.snd_una < total
                && roce.congestion_control == crate::RoceCongestionControl::Dcqcn;
            if feedback {
                let fed = roce.controller.on_feedback(now);
                advance.alpha_ticks += fed.alpha_ticks;
                advance.increase_fires += fed.increase_fires;
                advance.decrease_cuts += fed.decrease_cuts;
            }
            let dcqcn_transition = (full && (feedback || froze || advance.applied_rate_change()))
                .then(|| {
                    dcqcn_record(
                        event.key,
                        node.id,
                        packet.flow,
                        if feedback {
                            crate::DcqcnTransitionKind::Feedback
                        } else {
                            crate::DcqcnTransitionKind::Advance
                        },
                        now,
                        advance,
                        froze,
                        controller_before,
                        roce.controller,
                    )
                });
            settle_roce_sender(generator, &roce, generator.next_emission.status);
            generator.kind = FlowGeneratorKind::Roce(roce);
            leave_parked_list(state.pfc, data_class, position, generator, &roce);
            let generator = &state.generators[position];
            let record = before.map(|before| {
                roce_sender_record(
                    event.key,
                    node.id,
                    packet.flow,
                    if nack {
                        crate::RoceSenderKind::Nack
                    } else {
                        crate::RoceSenderKind::Ack
                    },
                    None,
                    data_class,
                    &roce,
                    None,
                    Some(header),
                    None,
                    before,
                    crate::roce::RoceSenderView::of(generator, &roce),
                )
            });
            // A collective stage completes on the ACK that brings the cumulative acknowledgment
            // to its total (design note §4); a NACK carries the receiver's frontier, which stays
            // below the total, and later ACKs at the total are stale, so exactly one ACK does.
            // The stage record is read only then, once per pair.
            let completed_stage = (snd_una_before < total
                && roce.snd_una >= total
                && state.stages.stage(position).is_some())
            .then_some(acknowledgment);
            (
                superseded_ns,
                timer_ns,
                tick_ns,
                roce.pacing_timer_payload,
                record,
                dcqcn_transition,
                completed_stage,
            )
        };
        if let Some(deadline_ns) = superseded_ns {
            self.superseded_timers.push(SupersededTimer {
                target: node.id,
                payload: token,
                deadline_ns,
            });
        }
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions
                .extend(dcqcn_transition.map(crate::MechanismTransitionRecord::Dcqcn));
            self.mechanism_transitions.extend(record);
        }
        self.emit_roce_timers(node, event, token, tick_ns, timer_ns, children)?;
        if let Some(acknowledgment) = completed_stage {
            // The ACK echoes the send time and size of the data packet it answers, so the stage
            // certifies the unloaded round trip they needed, as a TCP stage does.
            let completion = CompletionSignal {
                ack_number: acknowledgment,
                origin_ns: header.echoed_sent_time_ns,
                delay_ns: unloaded_round_trip_ns(
                    self.image,
                    packet.flow,
                    u64::from(header.acknowledged_bytes),
                    packet.size_bytes,
                )?,
            };
            let mut stage_causes = PendingCauses::default();
            {
                let (mut state, index) = self.host_parts_mut(node)?;
                complete_local_successors(
                    &state.generators,
                    &mut state.stages,
                    index,
                    packet.flow,
                    completion,
                    &mut stage_causes,
                );
            }
            if !stage_causes.is_empty() {
                self.activate_ready_collectives(node, event, stage_causes, children)?;
            }
        }
        Ok(())
    }

    /// A RoCE queue pair's retransmission timeout (design note §5, step 6): Go-back-N rewinds to
    /// the cumulative acknowledgment, the fixed timeout re-arms, and a parked pacer restarts.
    // Out of line: queue-pair transitions must not grow the shared `dispatch` that every
    // event of every image runs through (P15 inlining check, `qp-impl/callsites.txt`).
    #[inline(never)]
    fn host_roce_timeout(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let flow = self.packet(event.payload)?.flow;
        let now = event.key.time_ns;
        let stop_time_ns = self.image.stop_time_ns;
        let full = self.observation_mode == ObservationMode::Full;
        let data_class = image_flow_priority(self.image, flow)?;
        let (timer_ns, tick_ns, record, dcqcn_transition) = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let position = index
                .first_generator(flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                })?;
            let generator = &mut state.generators[position];
            let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
                return Err(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                });
            };
            let armed = roce.rto_ns != 0
                && roce.snd_una < generator.bytes_emitted
                && roce.rto_deadline_ns == now
                && roce.pacing_timer_payload == event.payload;
            if !armed {
                // Live-state contract (T20g item 2): a superseded timeout is removed by the
                // transition that supersedes it, so only imported residue reaches this.
                #[cfg(debug_assertions)]
                debug_assert!(
                    self.imported_event(event.key),
                    "superseded RoCE retransmission timeout {:?} survived its disarm transition",
                    event.key
                );
                return Ok(());
            }
            let before = full.then(|| crate::roce::RoceSenderView::of(generator, &roce));
            // A phase-1 transition: the controller instants at or before `now` apply before the
            // restart settles the pacer's prediction (P16 ruling D2). An armed timeout implies an
            // incomplete pair.
            let bound = now.saturating_add(1);
            let dcqcn_transition = dcqcn_materialize(&mut roce.controller, false, bound)
                .filter(|(_, advance)| full && advance.applied_rate_change())
                .map(|(controller_before, advance)| {
                    dcqcn_record(
                        event.key,
                        node.id,
                        flow,
                        crate::DcqcnTransitionKind::Advance,
                        bound,
                        advance,
                        false,
                        controller_before,
                        roce.controller,
                    )
                });
            roce.next_psn = roce.snd_una;
            let deadline = now
                .checked_add(roce.rto_ns)
                .ok_or(ExecutionError::GeneratorTimeOverflow(flow))?;
            roce.rto_deadline_ns = deadline;
            let tick_ns = restart_roce_pacer(generator, &mut roce, now, stop_time_ns)?;
            settle_roce_sender(generator, &roce, generator.next_emission.status);
            generator.kind = FlowGeneratorKind::Roce(roce);
            leave_parked_list(state.pfc, data_class, position, generator, &roce);
            let generator = &state.generators[position];
            let record = before.map(|before| {
                roce_sender_record(
                    event.key,
                    node.id,
                    flow,
                    crate::RoceSenderKind::Timeout,
                    None,
                    data_class,
                    &roce,
                    None,
                    None,
                    None,
                    before,
                    crate::roce::RoceSenderView::of(generator, &roce),
                )
            });
            (Some(deadline), tick_ns, record, dcqcn_transition)
        };
        if full {
            self.mechanism_transitions
                .extend(dcqcn_transition.map(crate::MechanismTransitionRecord::Dcqcn));
            self.mechanism_transitions.extend(record);
        }
        self.emit_roce_timers(node, event, event.payload, tick_ns, timer_ns, children)
    }

    /// Emits a queue pair's restarted pacing tick, then its re-armed timeout.
    fn emit_roce_timers(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        token: PayloadId,
        tick_ns: Option<u64>,
        timer_ns: Option<u64>,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        for (kind, time_ns) in [
            (EventKind::PacingTimer, tick_ns),
            (EventKind::RetransmissionTimeout, timer_ns),
        ] {
            if let Some(time_ns) = time_ns {
                self.emit_from_host(
                    node,
                    parent,
                    ChildEmission {
                        target: node.id,
                        kind,
                        payload: token,
                        time_ns,
                    },
                    children,
                )?;
            }
        }
        Ok(())
    }

    /// A RoCE data packet at its queue pair's receiver (design note §5, step 9).
    ///
    /// The Go-back-N receiver accepts the in-order packet, or drops the packet and answers it. An
    /// ACK or NACK echoes the packet's CE mark; the receiver sends no CNP (P16 rulings D4, D5).
    // Out of line: queue-pair transitions must not grow the shared `dispatch` that every
    // event of every image runs through (P15 inlining check, `qp-impl/callsites.txt`).
    #[inline(never)]
    fn host_roce_data_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        header: crate::RoceDataHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let flow = self.flow(packet.flow)?;
        if flow.target != node.id {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: node.id,
            });
        }
        let now = event.key.time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let congestion_experienced = packet.ecn_codepoint() == crate::EcnCodepoint::Ce;
        let (feedback, schedule_ready, record, stage_causes) = {
            let (mut state, index) = self.host_parts_mut(node)?;
            let unknown = ExecutionError::UnknownGenerator {
                node: node.id,
                flow: packet.flow,
            };
            let receivers = state.roce_receivers.as_deref_mut().ok_or(unknown)?;
            let position = receivers
                .binary_search_by_key(&packet.flow, |receiver| receiver.flow)
                .map_err(|_| unknown)?;
            let receiver = &mut receivers[position];
            if header
                .psn
                .checked_add(packet.size_bytes)
                .is_none_or(|end| end > receiver.total_bytes)
            {
                return Err(ExecutionError::InconsistentRocePacket {
                    flow: packet.flow,
                    payload: packet.id,
                });
            }
            let before = crate::roce::RoceReceiverView::of(receiver);
            let action = crate::roce::receive(receiver, header.psn, packet.size_bytes, now);
            let receiver = *receiver;
            // A collective stage waiting on this queue pair counts the receiver's Go-back-N
            // frontier (design note §3): the packet size for the PSN at the frontier, else 0.
            // Every packet of a pending inbound predecessor is certified. A host without stages
            // returns at the index's one `None` test.
            let mut stage_causes = PendingCauses::default();
            record_inbound_progress(
                &state.generators,
                &mut state.stages,
                index,
                packet.flow,
                InboundProgress::Segment {
                    sequence: header.psn,
                    bytes: packet.size_bytes,
                    advance: receiver.expected_psn - before.expected_psn,
                },
                node.id,
                &mut stage_causes,
            )?;
            *state.received_packets = state
                .received_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let mut allocate = || -> Result<PayloadId, ExecutionError> {
                let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                *state.next_payload_seq = state
                    .next_payload_seq
                    .checked_add(1)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                *state.sourced_packets = state
                    .sourced_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                Ok(payload)
            };
            let feedback = action
                .sends_feedback()
                .then(&mut allocate)
                .transpose()?
                .map(|payload| (payload, receiver.ack_size_bytes, action));
            let schedule_ready =
                feedback.is_some() && state.in_service.is_none() && !*state.tx_ready_pending;
            if schedule_ready {
                *state.tx_ready_pending = true;
            }
            let record = crate::MechanismTransitionRecord::Roce(
                crate::RoceTransitionRecord::Receiver(crate::RoceReceiverRecord {
                    key: event.key,
                    node: node.id,
                    flow: packet.flow,
                    total_bytes: receiver.total_bytes,
                    ack_every_packets: receiver.ack_every_packets,
                    nack_interval_ns: receiver.nack_interval_ns,
                    duplicate_ack: receiver.duplicate_ack,
                    ack_size_bytes: receiver.ack_size_bytes,
                    packet_psn: header.psn,
                    packet_bytes: packet.size_bytes,
                    packet_sent_time_ns: header.sent_time_ns,
                    packet_retransmission: header.retransmission,
                    packet_ce: congestion_experienced,
                    action,
                    feedback_acknowledgment: feedback.map(|_| receiver.expected_psn),
                    feedback_payload: feedback.map(|(payload, ..)| payload),
                    feedback_ce_echo: feedback.map(|_| congestion_experienced),
                    before,
                    after: crate::roce::RoceReceiverView::of(&receiver),
                }),
            );
            (
                feedback.map(|feedback| (feedback, receiver.expected_psn)),
                schedule_ready,
                record,
                stage_causes,
            )
        };
        self.record_arrival(node.id, packet, event.key, ArrivalDisposition::Delivered)?;
        self.mark_terminal(packet.id)?;
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.push(record);
        }
        let mut first = None;
        if let Some(((payload, size_bytes, action), frontier)) = feedback {
            // The ECN echo of the packet that triggered the ACK or NACK (rulings D4 and D5).
            let header = crate::RoceAckHeader {
                acknowledgment: frontier,
                echoed_sent_time_ns: header.sent_time_ns,
                acknowledged_bytes: u32::try_from(packet.size_bytes).map_err(|_| {
                    ExecutionError::InconsistentRocePacket {
                        flow: packet.flow,
                        payload: packet.id,
                    }
                })?,
                ce_echo: congestion_experienced,
            };
            let reply = PacketDescriptor {
                id: payload,
                flow: packet.flow,
                size_bytes,
                ecn_marked: false,
                kind: if action == crate::RoceReceiverAction::Nack {
                    PacketKind::RoceNack(header)
                } else {
                    PacketKind::RoceAck(header)
                },
            };
            self.insert_packet(reply, Some(now))?;
            self.enqueue_source_packet(node, payload)?;
            self.record_sourced(node.id, reply)?;
            first.get_or_insert(payload);
        }
        if let (true, Some(payload)) = (schedule_ready, first) {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: now,
                },
                children,
            )?;
        }
        if !stage_causes.is_empty() {
            self.activate_ready_collectives(node, event, stage_causes, children)?;
        }
        Ok(())
    }

    /// Owns a same-instant `TxReady` for `payload` when the host's egress is idle.
    fn schedule_host_ready(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        payload: PayloadId,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let ready = {
            let state = self.host_state_mut(node)?;
            let ready = state.in_service.is_none() && !state.tx_ready_pending;
            if ready {
                state.tx_ready_pending = true;
            }
            ready
        };
        if ready {
            self.emit_from_host(
                node,
                parent,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: parent.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    fn switch_tx_ready(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let ready_packet = self.packet(event.payload)?;
        let egress_link = self.packet_egress_at(ready_packet, node.id)?;
        let Some(egress_link) = egress_link else {
            return Err(ExecutionError::MissingSwitchQueue {
                node: node.id,
                egress_link: None,
            });
        };

        let state_slot = self.local_state_slot(node)?;
        let mut probed_entries = None;
        let plan = {
            let state = self.switch_state(node)?;
            let (queue_slot, queue) = state
                .queues
                .iter()
                .enumerate()
                .find(|(_, queue)| queue.egress_link == Some(egress_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(egress_link),
                })?;
            if queue_serves_head(queue) {
                SwitchServicePlan::Head(
                    queue
                        .queue
                        .front()
                        .map(|payload| self.packet(*payload))
                        .transpose()?,
                )
            } else if let Some(pfc) = queue
                .pfc
                .as_ref()
                .filter(|_| scheduler_serves_head(&queue.scheduler))
            {
                // The discipline serves position zero of the eligible packets: the first queued
                // packet whose class is not paused. Only that packet is read.
                let order = self.pfc_order(state_slot, queue_slot);
                let (first, reads) = self.pfc_first_eligible(node, queue, pfc, order)?;
                let selection = first
                    .map(|(position, payload)| {
                        let packet = self.packet(payload)?;
                        Ok::<_, ExecutionError>(PfcSelection {
                            position,
                            packet,
                            priority: self.packet_pfc_class(packet)?,
                            incoming_link: self.packet_incoming_link_at(packet, node.id)?,
                        })
                    })
                    .transpose()?;
                probed_entries = Some((
                    reads + usize::from(selection.is_some()),
                    selection.map(|selection| selection.position),
                ));
                SwitchServicePlan::PfcFirst(selection)
            } else {
                if queue.pfc.is_some() {
                    probed_entries = Some((queue.queue.len(), None));
                }
                let mut positions = Vec::new();
                let mut packets = Vec::new();
                let mut priorities = Vec::new();
                let mut incoming_links = Vec::new();
                for (position, payload) in queue.queue.iter().enumerate() {
                    let packet = self.packet(*payload)?;
                    let priority =
                        usize::from(self.flow(packet.flow)?.packet_priority(packet.kind));
                    let paused = queue
                        .pfc
                        .as_ref()
                        .is_some_and(|pfc| pfc.is_paused(priority));
                    if !paused {
                        positions.push(position);
                        packets.push(packet);
                        priorities.push(priority);
                        incoming_links.push(self.packet_incoming_link_at(packet, node.id)?);
                    }
                }
                SwitchServicePlan::Eligible {
                    positions,
                    packets,
                    priorities,
                    incoming_links,
                }
            }
        };
        if let Some((entries, served)) = probed_entries {
            self.pfc_service_probe.note_decision(entries, served);
        }
        let (
            payload,
            selected_packet,
            selected_priority,
            queue_slot,
            pfc_plan,
            pfc_transition,
            scheduler_transition,
        ) = 'service: {
            let state = self.switch_state_mut(node)?;
            let (queue_slot, queue) = state
                .queues
                .iter_mut()
                .enumerate()
                .find(|(_, queue)| queue.egress_link == Some(egress_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(egress_link),
                })?;
            queue.tx_ready_pending = false;
            if queue.in_service.is_some() {
                return Err(ExecutionError::SwitchAlreadyTransmitting {
                    node: node.id,
                    link: egress_link,
                });
            }

            let (
                position,
                packet,
                priority,
                incoming_link,
                scheduler_before,
                scan_steps,
                eligible_packets,
            ) = match &plan {
                SwitchServicePlan::Head(head) => {
                    // Position zero of a queue whose every packet is eligible. The selection,
                    // the removal and the state updates here are exactly what the
                    // eligible-packet path computes; no PFC monitor and no round-robin
                    // discipline is present, so both transition records are `None`.
                    let Some(packet) = *head else {
                        return Ok(());
                    };
                    let payload = queue
                        .queue
                        .pop_front()
                        .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
                    if let SchedulerKind::WeightedFairQueue(wfq) = &mut queue.scheduler {
                        wfq.packet_finish_times.get(&payload).ok_or(
                            ExecutionError::MissingWfqFinishTag {
                                node: node.id,
                                payload,
                            },
                        )?;
                    }
                    queue.in_service = Some(payload);
                    break 'service (payload, packet, None, queue_slot, None, None, None);
                }
                SwitchServicePlan::PfcFirst(selection) => {
                    // What the eligible-packet path computes for a head-serving discipline:
                    // `scheduler_select_position` answers position zero of the eligible packets
                    // without touching the scheduler, and the scheduler record is `None`.
                    let Some(selection) = *selection else {
                        return Ok(());
                    };
                    (
                        selection.position,
                        selection.packet,
                        selection.priority,
                        selection.incoming_link,
                        None,
                        0,
                        &[][..],
                    )
                }
                SwitchServicePlan::Eligible {
                    positions,
                    packets,
                    priorities,
                    incoming_links,
                } => {
                    let scheduler_before = matches!(
                        queue.scheduler,
                        SchedulerKind::DeficitRoundRobin(_) | SchedulerKind::WeightedRoundRobin(_)
                    )
                    .then(|| queue.scheduler.clone());
                    let Some((eligible_position, scan_steps)) =
                        scheduler_select_position(&mut queue.scheduler, packets, node.id)?
                    else {
                        return Ok(());
                    };
                    (
                        positions[eligible_position],
                        packets[eligible_position],
                        priorities[eligible_position],
                        incoming_links[eligible_position],
                        scheduler_before,
                        scan_steps,
                        &packets[..],
                    )
                }
            };
            let payload = queue
                .queue
                .remove(position)
                .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
            if let SchedulerKind::WeightedFairQueue(wfq) = &mut queue.scheduler {
                wfq.packet_finish_times.get(&payload).ok_or(
                    ExecutionError::MissingWfqFinishTag {
                        node: node.id,
                        payload,
                    },
                )?;
            }
            queue.in_service = Some(payload);
            let queue_id = u64::try_from(queue_slot).unwrap_or(u64::MAX);
            let ingress = queue.pfc.as_mut().and_then(|pfc| {
                pfc.ingresses
                    .iter_mut()
                    .find(|ingress| Some(ingress.controlled_link) == incoming_link)
            });
            let (pfc_plan, pfc_transition) = if let Some(ingress) = ingress {
                let xoff = ingress.xoff_threshold_bytes[priority];
                if xoff == 0 {
                    (None, None)
                } else {
                    let before_occupancy = ingress.occupancy_bytes[priority];
                    let before_asserted = ingress.pause_asserted[priority];
                    let depth = before_occupancy
                        .checked_sub(packet.size_bytes)
                        .ok_or(ExecutionError::CounterOverflow(node.id))?;
                    ingress.occupancy_bytes[priority] = depth;
                    let xon = ingress.xon_threshold_bytes[priority];
                    let plan = if !before_asserted || depth > xon {
                        None
                    } else {
                        ingress.pause_asserted[priority] = false;
                        Some(PfcFramePlan {
                            channel_index: ingress.control_channel_index,
                            flow: packet.flow,
                            header: crate::PfcHeader {
                                controlled_link: ingress.controlled_link,
                                priority: priority as u8,
                                pause: false,
                            },
                        })
                    };
                    let transition = crate::MechanismTransitionRecord::PfcThreshold(
                        crate::PfcThresholdTransitionRecord {
                            key: event.key,
                            node: node.id,
                            queue_id,
                            controlled_link: ingress.controlled_link,
                            priority: priority as u8,
                            xon_bytes: xon,
                            xoff_bytes: xoff,
                            buffer_capacity_bytes: ingress.buffer_capacity_bytes[priority],
                            amount_bytes: packet.size_bytes,
                            action: crate::PfcOccupancyAction::Drain,
                            before_occupancy_bytes: before_occupancy,
                            before_asserted,
                            after_occupancy_bytes: depth,
                            after_asserted: ingress.pause_asserted[priority],
                            emitted: plan.map(|_| crate::PfcControlAction::Resume),
                        },
                    );
                    (plan, Some(transition))
                }
            } else {
                (None, None)
            };
            let scheduler_transition = match (&scheduler_before, &queue.scheduler) {
                (
                    Some(SchedulerKind::DeficitRoundRobin(before)),
                    SchedulerKind::DeficitRoundRobin(after),
                ) => Some(crate::MechanismTransitionRecord::Drr(
                    crate::DrrTransitionRecord {
                        key: event.key,
                        node: node.id,
                        queue_id,
                        class_count: u64::try_from(before.quanta_bytes.len()).unwrap_or(u64::MAX),
                        quanta_bytes: before.quanta_bytes.clone(),
                        before_deficits_bytes: before.deficits_bytes.clone(),
                        before_current_class: before.current_class,
                        scan_steps,
                        eligible_packets: scheduler_packets(eligible_packets),
                        selected_payload: payload,
                        after_deficits_bytes: after.deficits_bytes.clone(),
                        after_current_class: after.current_class,
                    },
                )),
                (
                    Some(SchedulerKind::WeightedRoundRobin(before)),
                    SchedulerKind::WeightedRoundRobin(after),
                ) => Some(crate::MechanismTransitionRecord::Wrr(
                    crate::WrrTransitionRecord {
                        key: event.key,
                        node: node.id,
                        queue_id,
                        class_count: u64::try_from(before.weights.len()).unwrap_or(u64::MAX),
                        weights: before.weights.clone(),
                        before_packets_sent: before.packets_sent_in_round.clone(),
                        before_current_class: before.current_class,
                        eligible_packets: scheduler_packets(eligible_packets),
                        selected_payload: payload,
                        after_packets_sent: after.packets_sent_in_round.clone(),
                        after_current_class: after.current_class,
                    },
                )),
                _ => None,
            };
            (
                payload,
                packet,
                Some(priority),
                queue_slot,
                pfc_plan,
                pfc_transition,
                scheduler_transition,
            )
        };

        let counter = &mut self.switch_queue_bytes[state_slot][queue_slot];
        *counter = counter
            .checked_sub(selected_packet.size_bytes)
            .ok_or(ExecutionError::CounterOverflow(node.id))?;
        // A queue keeps an order only under FIFO, where the served packet is the first of its
        // class: the head, or the packet the order selected (`PfcClassOrder`).
        if let Some(priority) = selected_priority {
            if let Some(order) = self.pfc_order_mut(state_slot, queue_slot) {
                let served = order.pop(priority);
                debug_assert!(
                    served.is_some(),
                    "a FIFO PFC queue served a class it holds no packet of"
                );
            }
        }
        #[cfg(debug_assertions)]
        self.debug_assert_switch_queue_aux(node, state_slot, queue_slot);

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.extend(pfc_transition);
            self.mechanism_transitions.extend(scheduler_transition);
            self.observe_service_start(node, queue_slot, event.key)?;
        }

        let link = self.link(egress_link)?;
        if link.source != node.id {
            return Err(ExecutionError::LinkSourceMismatch {
                link: link.id,
                expected_source: node.id,
                actual_source: link.source,
            });
        }
        self.start_transmission(node.id, payload)?;
        let arrival_time_ns =
            link.arrival_time_ns(event.key.time_ns, selected_packet.size_bytes)?;
        let departure_time_ns = arrival_time_ns
            .checked_sub(link.propagation_ns)
            .ok_or(ExecutionError::Time(TimeError::ArrivalOverflow))?;

        if let Some(plan) = pfc_plan {
            self.emit_pfc_frame(node, event, plan, children)?;
        }

        self.emit_from_switch(
            node,
            event,
            ChildEmission {
                target: node.id,
                kind: EventKind::TxComplete,
                payload,
                time_ns: departure_time_ns,
            },
            children,
        )?;
        self.emit_from_switch(
            node,
            event,
            ChildEmission {
                target: self.packet_remote_target_for(selected_packet, link.id)?,
                kind: EventKind::RemoteArrival,
                payload,
                time_ns: arrival_time_ns,
            },
            children,
        )
    }

    fn switch_tx_complete(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let packet = self.packet(event.payload)?;
        let egress_link = self.packet_egress_at(packet, node.id)?;
        let Some(egress_link) = egress_link else {
            return Err(ExecutionError::MissingSwitchQueue {
                node: node.id,
                egress_link: None,
            });
        };
        let rate_bps = self.link(egress_link)?.rate_bps;

        let (eligible_next_payload, probed_reads) = {
            let state = self.switch_state(node)?;
            let (queue_slot, queue) = state
                .queues
                .iter()
                .enumerate()
                .find(|(_, queue)| queue.egress_link == Some(egress_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(egress_link),
                })?;
            // Without a PFC monitor no priority can be paused, so the first eligible packet is
            // the queue head and no per-packet inspection is observable.
            match &queue.pfc {
                None => (queue.queue.front().copied(), 0),
                Some(pfc) => {
                    let order = self.pfc_order(self.local_state_slot(node)?, queue_slot);
                    let (first, reads) = self.pfc_first_eligible(node, queue, pfc, order)?;
                    (first.map(|(_, payload)| payload), reads)
                }
            }
        };
        self.pfc_service_probe.note_reads(probed_reads);
        let next_payload = {
            let state = self.switch_state_mut(node)?;
            let queue = state
                .queues
                .iter_mut()
                .find(|queue| queue.egress_link == Some(egress_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(egress_link),
                })?;
            if queue.in_service != Some(event.payload) {
                return Err(ExecutionError::UnexpectedTxComplete {
                    node: node.id,
                    expected: queue.in_service,
                    actual: event.payload,
                });
            }
            if let SchedulerKind::WeightedFairQueue(wfq) = &mut queue.scheduler {
                wfq_complete(wfq, packet, event.key.time_ns, rate_bps, node.id)?;
            }
            queue.in_service = None;

            let schedule_payload = if eligible_next_payload.is_some() && !queue.tx_ready_pending {
                queue.tx_ready_pending = true;
                eligible_next_payload
            } else {
                None
            };
            state.departed_packets = state
                .departed_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            schedule_payload
        };

        self.record_departure(node.id, packet, event.key)?;
        self.finish_transmission(node.id, event.payload)?;

        if let Some(payload) = next_payload {
            self.emit_from_switch(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: event.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    fn emit_from_host(
        &mut self,
        origin: NodeDescriptor,
        parent: Event,
        emission: ChildEmission,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let origin_seq = {
            let state = self.host_state_mut(origin)?;
            let origin_seq = state.next_origin_seq;
            state.next_origin_seq = origin_seq
                .checked_add(1)
                .ok_or(ExecutionError::OriginSequenceOverflow(origin.id))?;
            origin_seq
        };
        Self::insert_child(
            parent,
            Event {
                key: EventKey {
                    time_ns: emission.time_ns,
                    phase: event_phase(emission.kind),
                    origin_node: origin.id,
                    origin_seq,
                },
                target: emission.target,
                kind: emission.kind,
                payload: emission.payload,
            },
            children,
        )
    }

    fn emit_from_switch(
        &mut self,
        origin: NodeDescriptor,
        parent: Event,
        emission: ChildEmission,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let origin_seq = {
            let state = self.switch_state_mut(origin)?;
            let origin_seq = state.next_origin_seq;
            state.next_origin_seq = origin_seq
                .checked_add(1)
                .ok_or(ExecutionError::OriginSequenceOverflow(origin.id))?;
            origin_seq
        };
        Self::insert_child(
            parent,
            Event {
                key: EventKey {
                    time_ns: emission.time_ns,
                    phase: event_phase(emission.kind),
                    origin_node: origin.id,
                    origin_seq,
                },
                target: emission.target,
                kind: emission.kind,
                payload: emission.payload,
            },
            children,
        )
    }

    fn insert_child(
        parent: Event,
        child: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        if child.key <= parent.key {
            return Err(ExecutionError::NonMonotoneChild {
                parent: parent.key,
                child: child.key,
            });
        }
        children.push(child);
        Ok(())
    }

    fn node(&self, id: NodeId) -> Result<NodeDescriptor, ExecutionError> {
        indexed_lookup(&self.image.nodes, id.0, |node| node.id == id)
            .copied()
            .ok_or(ExecutionError::UnknownNode(id))
    }

    fn host_state_mut(&mut self, node: NodeDescriptor) -> Result<&mut HostState, ExecutionError> {
        let state_slot = self.local_state_slot(node)?;
        self.hosts
            .slices_mut()
            .0
            .get_mut(state_slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })
    }

    /// The stage view of a host, and the host's stage index, borrowed disjointly.
    ///
    /// Fails exactly where `host_state_mut` fails: the indices are built one per host state. Always
    /// inlined, so the view is taken apart at each call site into the field accesses it names.
    #[inline(always)]
    fn host_parts_mut(
        &mut self,
        node: NodeDescriptor,
    ) -> Result<(HostView<'_>, &mut HostStageIndex), ExecutionError> {
        let state_slot = self.local_state_slot(node)?;
        let invalid = || ExecutionError::InvalidStateSlot {
            node: node.id,
            kind: node.kind,
            state_slot: node.state_slot,
        };
        let (states, indices) = self.hosts.slices_mut();
        let state = states.get_mut(state_slot).ok_or_else(invalid)?;
        let (index, generator_reads, stage_reads, receiver_reads) =
            indices.get_mut(state_slot).ok_or_else(invalid)?.parts_mut();
        let HostState {
            queue,
            in_service,
            tx_ready_pending,
            generators,
            stages,
            tcp_receivers,
            next_payload_seq,
            sourced_packets,
            received_packets,
            pfc,
            roce_receivers,
            ..
        } = state;
        let view = HostView {
            generators: ProbedTable::new(generators, generator_reads),
            stages: ProbedTable::new(stages, stage_reads),
            tcp_receivers: ProbedTable::new(tcp_receivers, receiver_reads),
            queue,
            in_service,
            tx_ready_pending,
            next_payload_seq,
            sourced_packets,
            received_packets,
            pfc,
            roce_receivers,
        };
        Ok((view, index))
    }

    fn switch_state_mut(
        &mut self,
        node: NodeDescriptor,
    ) -> Result<&mut SwitchState, ExecutionError> {
        let state_slot = self.local_state_slot(node)?;
        self.switch_states
            .get_mut(state_slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })
    }

    fn host_state(&self, node: NodeDescriptor) -> Result<&HostState, ExecutionError> {
        let state_slot = self.local_state_slot(node)?;
        self.hosts
            .slices()
            .0
            .get(state_slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })
    }

    fn switch_state(&self, node: NodeDescriptor) -> Result<&SwitchState, ExecutionError> {
        let state_slot = self.local_state_slot(node)?;
        self.switch_states
            .get(state_slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })
    }

    /// The switch, queue slot and egress link of the queue `packet` uses at `node`, when `node`
    /// is a switch; `None` at a host.
    fn scheduler_queue_of(
        &self,
        node: NodeId,
        packet: PacketDescriptor,
    ) -> Result<Option<(NodeDescriptor, usize, LinkId)>, ExecutionError> {
        let node = self.node(node)?;
        if node.kind != NodeKind::Switch {
            return Ok(None);
        }
        let Some(egress_link) = self.packet_egress_at(packet, node.id)? else {
            return Ok(None);
        };
        let queue_slot = self
            .switch_state(node)?
            .queues
            .iter()
            .position(|queue| queue.egress_link == Some(egress_link))
            .ok_or(ExecutionError::MissingSwitchQueue {
                node: node.id,
                egress_link: Some(egress_link),
            })?;
        Ok(Some((node, queue_slot, egress_link)))
    }

    /// Full observation only (P16 L2 certificates): records an admitted packet's enqueue at a
    /// WFQ or SP queue, after the enqueue. A WFQ record carries the packet's finish tag and its
    /// virtual start, the tag less its service `size_bytes * 8 / weight`.
    #[cold]
    #[inline(never)]
    fn observe_scheduler_enqueue(
        &mut self,
        node: NodeId,
        key: EventKey,
        packet: PacketDescriptor,
    ) -> Result<(), ExecutionError> {
        let Some((node, queue_slot, _)) = self.scheduler_queue_of(node, packet)? else {
            return Ok(());
        };
        let queue = &self.switch_state(node)?.queues[queue_slot];
        let record = match &queue.scheduler {
            SchedulerKind::StaticPriority { priorities } => {
                let kind = crate::SpTransitionKind::Enqueue;
                sp_record(kind, key, node.id, queue_slot, priorities, packet, None)?
            }
            SchedulerKind::WeightedFairQueue(wfq) => {
                let finish = wfq.packet_finish_times.get(&packet.id).cloned().ok_or(
                    ExecutionError::MissingWfqFinishTag {
                        node: node.id,
                        payload: packet.id,
                    },
                )?;
                let class = scheduler_class(packet.flow, wfq.weights.len())
                    .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
                let service = Ratio::new(
                    BigUint::from(packet.size_bytes) * BigUint::from(8_u8),
                    BigUint::from(wfq.weights[class]),
                );
                let egress_link = queue
                    .egress_link
                    .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
                let pfc_priority = if queue.pfc.is_some() {
                    self.flow(packet.flow)?.packet_priority(packet.kind)
                } else {
                    0
                };
                let mut record = wfq_record(
                    crate::WfqTransitionKind::Enqueue,
                    key,
                    node.id,
                    queue_slot,
                    self.link(egress_link)?.rate_bps,
                    wfq,
                    packet,
                    Some(finish.clone() - service),
                    Some(finish),
                    Vec::new(),
                    Vec::new(),
                );
                if let crate::MechanismTransitionRecord::Wfq(wfq_record) = &mut record {
                    wfq_record.pfc_priority = pfc_priority;
                }
                record
            }
            _ => return Ok(()),
        };
        self.mechanism_transitions.push(record);
        Ok(())
    }

    /// Full observation only: records the WFQ or SP service start the `TxReady` at `key` just
    /// performed at queue `queue_slot` of `node`: the packet it put in service, and the time the
    /// transmission completes (the time `switch_tx_ready` gives the `TxComplete` it emits).
    #[cold]
    #[inline(never)]
    fn observe_service_start(
        &mut self,
        node: NodeDescriptor,
        queue_slot: usize,
        key: EventKey,
    ) -> Result<(), ExecutionError> {
        let queue = &self.switch_state(node)?.queues[queue_slot];
        if !matches!(
            queue.scheduler,
            SchedulerKind::StaticPriority { .. } | SchedulerKind::WeightedFairQueue(_)
        ) {
            return Ok(());
        }
        let (Some(served), Some(egress_link)) = (queue.in_service, queue.egress_link) else {
            return Err(ExecutionError::InvalidSchedulerState(node.id));
        };
        let served = self.packet(served)?;
        let link = self.link(egress_link)?;
        let departure_time_ns = link
            .arrival_time_ns(key.time_ns, served.size_bytes)?
            .checked_sub(link.propagation_ns)
            .ok_or(ExecutionError::Time(TimeError::ArrivalOverflow))?;
        self.observe_scheduler_service_start(
            node,
            queue_slot,
            key,
            served,
            egress_link,
            departure_time_ns,
        )
    }

    /// Full observation only (P16 L2 certificates): records a service start at a WFQ or SP
    /// queue, after the selection. The WFQ record lists the served packet and then every packet
    /// still waiting, in queue order, each with its finish tag and PFC class, and the PFC
    /// priorities paused at the egress. The SP record carries the time the transmission
    /// completes.
    fn observe_scheduler_service_start(
        &mut self,
        node: NodeDescriptor,
        queue_slot: usize,
        key: EventKey,
        selected: PacketDescriptor,
        egress_link: LinkId,
        departure_time_ns: u64,
    ) -> Result<(), ExecutionError> {
        let queue = &self.switch_state(node)?.queues[queue_slot];
        let link = self.link(egress_link)?;
        let record = match &queue.scheduler {
            SchedulerKind::StaticPriority { priorities } => {
                let kind = crate::SpTransitionKind::Schedule;
                sp_record(
                    kind,
                    key,
                    node.id,
                    queue_slot,
                    priorities,
                    selected,
                    Some(departure_time_ns),
                )?
            }
            SchedulerKind::WeightedFairQueue(wfq) => {
                let tag = |payload: PayloadId| {
                    wfq.packet_finish_times.get(&payload).cloned().ok_or(
                        ExecutionError::MissingWfqFinishTag {
                            node: node.id,
                            payload,
                        },
                    )
                };
                let mut queued_packets = Vec::with_capacity(queue.queue.len() + 1);
                for waiting in std::iter::once(Ok(selected))
                    .chain(queue.queue.iter().map(|payload| self.packet(*payload)))
                {
                    let waiting = waiting?;
                    queued_packets.push(crate::WfqQueuedPacket {
                        packet: scheduler_packet(waiting),
                        pfc_priority: if queue.pfc.is_some() {
                            self.flow(waiting.flow)?.packet_priority(waiting.kind)
                        } else {
                            0
                        },
                        finish: tag(waiting.id)?,
                    });
                }
                let paused_priorities = queue.pfc.as_ref().map_or_else(Vec::new, |pfc| {
                    (0..8_u8)
                        .filter(|priority| pfc.is_paused(usize::from(*priority)))
                        .collect()
                });
                wfq_record(
                    crate::WfqTransitionKind::Select,
                    key,
                    node.id,
                    queue_slot,
                    link.rate_bps,
                    wfq,
                    selected,
                    None,
                    Some(tag(selected.id)?),
                    queued_packets,
                    paused_priorities,
                )
            }
            _ => return Ok(()),
        };
        self.mechanism_transitions.push(record);
        Ok(())
    }

    /// Full observation only (P16 L2 certificates): records a service completion at a WFQ or SP
    /// queue, after the completion.
    #[cold]
    #[inline(never)]
    fn observe_scheduler_completion(
        &mut self,
        node: NodeId,
        key: EventKey,
        packet: PacketDescriptor,
    ) -> Result<(), ExecutionError> {
        let Some((node, queue_slot, egress_link)) = self.scheduler_queue_of(node, packet)? else {
            return Ok(());
        };
        let queue = &self.switch_state(node)?.queues[queue_slot];
        let record = match &queue.scheduler {
            SchedulerKind::StaticPriority { priorities } => {
                let kind = crate::SpTransitionKind::Depart;
                sp_record(
                    kind,
                    key,
                    node.id,
                    queue_slot,
                    priorities,
                    packet,
                    Some(key.time_ns),
                )?
            }
            SchedulerKind::WeightedFairQueue(wfq) => wfq_record(
                crate::WfqTransitionKind::Complete,
                key,
                node.id,
                queue_slot,
                self.link(egress_link)?.rate_bps,
                wfq,
                packet,
                None,
                None,
                Vec::new(),
                Vec::new(),
            ),
            _ => return Ok(()),
        };
        self.mechanism_transitions.push(record);
        Ok(())
    }

    fn switch_sp_insertion_position(
        &self,
        node: NodeDescriptor,
        egress_link: Option<LinkId>,
        incoming_flow: FlowId,
    ) -> Result<Option<usize>, ExecutionError> {
        let queue = self
            .switch_state(node)?
            .queues
            .iter()
            .find(|queue| queue.egress_link == egress_link)
            .ok_or(ExecutionError::MissingSwitchQueue {
                node: node.id,
                egress_link,
            })?;
        let SchedulerKind::StaticPriority { priorities } = &queue.scheduler else {
            return Ok(None);
        };
        let incoming_class = scheduler_class(incoming_flow, priorities.len())
            .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
        let incoming_priority = priorities[incoming_class];
        for (position, payload) in queue.queue.iter().enumerate() {
            let queued = self.packet(*payload)?;
            let queued_class = scheduler_class(queued.flow, priorities.len())
                .ok_or(ExecutionError::InvalidSchedulerState(node.id))?;
            if priorities[queued_class] < incoming_priority {
                return Ok(Some(position));
            }
        }
        Ok(Some(queue.queue.len()))
    }

    fn local_state_slot(&self, node: NodeDescriptor) -> Result<usize, ExecutionError> {
        match self.local_node {
            Some(local) if local == node => Ok(0),
            Some(_) => Err(ExecutionError::UnknownNode(node.id)),
            None => Ok(node.state_slot as usize),
        }
    }

    #[cfg(debug_assertions)]
    fn debug_assert_switch_queue_aux(
        &self,
        node: NodeDescriptor,
        state_slot: usize,
        queue_slot: usize,
    ) {
        let queue = &self.switch_states[state_slot].queues[queue_slot];
        let derived = queue.queue.iter().try_fold(0_u64, |total, payload| {
            total.checked_add(self.packets.get(payload)?.descriptor.size_bytes)
        });
        debug_assert_eq!(
            derived,
            Some(self.switch_queue_bytes[state_slot][queue_slot]),
            "switch {:?} queue {queue_slot} byte counter diverged from its contents",
            node.id,
        );
        if let Some(order) = self.pfc_order(state_slot, queue_slot) {
            // The keys, merged, list the queue's classes in queue order, below the next key.
            let mut keyed = order
                .classes
                .iter()
                .enumerate()
                .flat_map(|(class, keys)| keys.iter().map(move |key| (*key, class)))
                .collect::<Vec<_>>();
            keyed.sort_unstable();
            debug_assert!(
                order
                    .classes
                    .iter()
                    .all(|keys| keys.iter().is_sorted_by(|a, b| a < b)),
                "switch {:?} queue {queue_slot} class order is not increasing",
                node.id,
            );
            debug_assert!(keyed.last().is_none_or(|(key, _)| *key < order.next_seq));
            let classes = queue
                .queue
                .iter()
                .map(|payload| {
                    self.packet(*payload)
                        .and_then(|packet| self.packet_pfc_class(packet))
                        .ok()
                })
                .collect::<Option<Vec<_>>>();
            debug_assert_eq!(
                Some(
                    keyed
                        .into_iter()
                        .map(|(_, class)| class)
                        .collect::<Vec<_>>()
                ),
                classes,
                "switch {:?} queue {queue_slot} class order diverged from its contents",
                node.id,
            );
        }
    }

    /// A packet's PFC class: its flow's priority, or the flow's feedback class for a CNP, ACK or
    /// NACK (`FlowDescriptor::packet_priority`).
    fn packet_pfc_class(&self, packet: PacketDescriptor) -> Result<usize, ExecutionError> {
        Ok(usize::from(
            self.flow(packet.flow)?.packet_priority(packet.kind),
        ))
    }

    /// The queue position and payload of the first queued packet whose PFC class `pfc` does not
    /// pause, with the number of queued entries read to find it.
    ///
    /// This is the packet the eligible-packet plan's position zero names. With no class paused it
    /// is the head, and nothing is read. A FIFO queue with a paused class keeps a
    /// `PfcClassOrder`, which answers without reading the queue. Static priority and WFQ insert
    /// by rank, so queue order is not arrival order there; their search reads from the head up to
    /// the packet, as their admission already does. DRR and WRR choose from the whole eligible
    /// list on `TxReady`; after a transmission or a resume they search from the head likewise.
    fn pfc_first_eligible(
        &self,
        node: NodeDescriptor,
        queue: &crate::SwitchQueueState,
        pfc: &crate::PfcQueueState,
        order: Option<&PfcClassOrder>,
    ) -> Result<(Option<(usize, PayloadId)>, usize), ExecutionError> {
        let Some(head) = queue.queue.front().copied() else {
            return Ok((None, 0));
        };
        if !pfc_any_paused(pfc) {
            return Ok((Some((0, head)), 0));
        }
        let Some(order) = order else {
            debug_assert!(
                !matches!(queue.scheduler, SchedulerKind::Fifo),
                "switch {:?}: a FIFO PFC queue with a paused class keeps a class order",
                node.id,
            );
            return self.pfc_first_eligible_by_search(queue, pfc);
        };
        let first = order
            .first_unpaused_position(pfc)
            .map(|position| {
                queue
                    .queue
                    .get(position)
                    .map(|payload| (position, *payload))
                    .ok_or(ExecutionError::InvalidSchedulerState(node.id))
            })
            .transpose()?;
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            Some(first),
            self.pfc_first_eligible_by_search(queue, pfc)
                .ok()
                .map(|(search, _)| search),
            "switch {:?}: the class order and the search disagree on the first eligible packet",
            node.id,
        );
        Ok((first, 0))
    }

    /// `pfc_first_eligible` by reading the queue from the head.
    fn pfc_first_eligible_by_search(
        &self,
        queue: &crate::SwitchQueueState,
        pfc: &crate::PfcQueueState,
    ) -> Result<(Option<(usize, PayloadId)>, usize), ExecutionError> {
        for (position, payload) in queue.queue.iter().enumerate() {
            if !pfc.is_paused(self.packet_pfc_class(self.packet(*payload)?)?) {
                return Ok((Some((position, *payload)), position + 1));
            }
        }
        Ok((None, queue.queue.len()))
    }

    /// The class order of a FIFO PFC queue that has had a class paused.
    fn pfc_order(&self, state_slot: usize, queue_slot: usize) -> Option<&PfcClassOrder> {
        self.switch_pfc_orders
            .as_deref()?
            .0
            .get(&(state_slot, queue_slot))
    }

    fn pfc_order_mut(
        &mut self,
        state_slot: usize,
        queue_slot: usize,
    ) -> Option<&mut PfcClassOrder> {
        self.switch_pfc_orders
            .as_deref_mut()?
            .0
            .get_mut(&(state_slot, queue_slot))
    }

    /// Gives a FIFO PFC queue its class order if it has none; returns the queued entries read.
    fn ensure_pfc_order(
        &mut self,
        state_slot: usize,
        queue_slot: usize,
    ) -> Result<usize, ExecutionError> {
        let Some(queue) = self
            .switch_states
            .get(state_slot)
            .and_then(|state| state.queues.get(queue_slot))
        else {
            return Ok(0);
        };
        if queue.pfc.is_none()
            || !matches!(queue.scheduler, SchedulerKind::Fifo)
            || self.pfc_order(state_slot, queue_slot).is_some()
        {
            return Ok(0);
        }
        let mut order = PfcClassOrder::new();
        for payload in &queue.queue {
            order.push(self.packet_pfc_class(self.packet(*payload)?)?);
        }
        let reads = queue.queue.len();
        self.switch_pfc_orders
            .get_or_insert_with(Box::default)
            .0
            .insert((state_slot, queue_slot), order);
        Ok(reads)
    }

    fn link(&self, id: LinkId) -> Result<crate::LinkDescriptor, ExecutionError> {
        indexed_lookup(&self.image.links, id.0, |link| link.id == id)
            .copied()
            .ok_or(ExecutionError::UnknownLink(id))
    }

    fn packet_size(&self, id: PayloadId) -> Result<u64, ExecutionError> {
        Ok(self.packet(id)?.size_bytes)
    }

    fn packet(&self, id: PayloadId) -> Result<PacketDescriptor, ExecutionError> {
        self.packets
            .get(&id)
            .map(|packet| packet.descriptor)
            .ok_or(ExecutionError::UnknownPacket(id))
    }

    fn flow(&self, id: FlowId) -> Result<&crate::FlowDescriptor, ExecutionError> {
        indexed_lookup(&self.image.flows, id.0, |flow| flow.id == id)
            .ok_or(ExecutionError::UnknownFlow(id))
    }

    fn packet_egress_at(
        &self,
        packet: PacketDescriptor,
        node: NodeId,
    ) -> Result<Option<LinkId>, ExecutionError> {
        let flow = self.flow(packet.flow)?;

        let route = if packet.kind.is_data() {
            &flow.route
        } else {
            &flow.reverse_route
        };
        for link_id in route {
            let link = self.link(*link_id)?;
            if link.source == node {
                return Ok(Some(link.id));
            }
        }
        let terminal = if packet.kind.is_data() {
            flow.target
        } else {
            flow.source
        };
        if terminal == node {
            return Ok(None);
        }
        Err(ExecutionError::FlowRouteMiss {
            flow: flow.id,
            node,
        })
    }

    fn packet_incoming_link_at(
        &self,
        packet: PacketDescriptor,
        node: NodeId,
    ) -> Result<Option<LinkId>, ExecutionError> {
        let flow = self.flow(packet.flow)?;
        let route = if packet.kind.is_data() {
            &flow.route
        } else {
            &flow.reverse_route
        };
        for (index, link_id) in route.iter().enumerate() {
            let remote_target = if let Some(next) = route.get(index + 1) {
                self.link(*next)?.source
            } else if packet.kind.is_data() {
                flow.target
            } else {
                flow.source
            };
            if remote_target == node {
                return Ok(Some(*link_id));
            }
        }
        Ok(None)
    }

    fn packet_remote_target(
        &self,
        payload: PayloadId,
        egress: LinkId,
    ) -> Result<NodeId, ExecutionError> {
        let packet = self.packet(payload)?;
        self.packet_remote_target_for(packet, egress)
    }

    fn packet_remote_target_for(
        &self,
        packet: PacketDescriptor,
        egress: LinkId,
    ) -> Result<NodeId, ExecutionError> {
        let flow = self.flow(packet.flow)?;
        let route = if packet.kind.is_data() {
            &flow.route
        } else {
            &flow.reverse_route
        };
        let Some(index) = route.iter().position(|link| *link == egress) else {
            return Err(ExecutionError::FlowRouteMiss {
                flow: flow.id,
                node: self.link(egress)?.source,
            });
        };
        if let Some(next) = route.get(index + 1) {
            return Ok(self.link(*next)?.source);
        }
        Ok(if packet.kind.is_data() {
            flow.target
        } else {
            flow.source
        })
    }

    fn emit_pfc_frame(
        &mut self,
        origin: NodeDescriptor,
        parent: Event,
        plan: PfcFramePlan,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let channel = self
            .image
            .channels
            .get(plan.channel_index as usize)
            .copied()
            .ok_or(ExecutionError::UnknownLink(plan.header.controlled_link))?;
        let sequence = self.switch_state(origin)?.next_origin_seq;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let payload = PayloadId::from_node_sequence(origin.id, node_count, sequence)
            .ok_or(ExecutionError::PayloadSequenceOverflow(origin.id))?;
        self.insert_packet(
            PacketDescriptor {
                id: payload,
                flow: plan.flow,
                size_bytes: 64,
                ecn_marked: false,
                kind: PacketKind::Pfc(plan.header),
            },
            Some(parent.key.time_ns),
        )?;
        let arrival_time_ns = parent
            .key
            .time_ns
            .checked_add(channel.min_delay_ns)
            .ok_or(ExecutionError::Time(TimeError::ArrivalOverflow))?;
        self.emit_from_switch(
            origin,
            parent,
            ChildEmission {
                target: channel.target,
                kind: EventKind::RemoteArrival,
                payload,
                time_ns: arrival_time_ns,
            },
            children,
        )
    }

    fn insert_packet(
        &mut self,
        packet: PacketDescriptor,
        source_time_ns: Option<u64>,
    ) -> Result<(), ExecutionError> {
        if self.packets.contains_key(&packet.id) {
            return Err(ExecutionError::DuplicatePayload(packet.id));
        }
        self.packets.insert(
            packet.id,
            ResidentPacket {
                descriptor: packet,
                source_time_ns,
                transmitters: 0,
                terminal: false,
            },
        );
        Ok(())
    }

    fn set_source_time(&mut self, payload: PayloadId, time_ns: u64) -> Result<(), ExecutionError> {
        let packet = self
            .packets
            .get_mut(&payload)
            .ok_or(ExecutionError::UnknownPacket(payload))?;
        packet.source_time_ns = Some(time_ns);
        Ok(())
    }

    fn set_packet_marked(&mut self, payload: PayloadId) -> Result<(), ExecutionError> {
        let packet = self
            .packets
            .get_mut(&payload)
            .ok_or(ExecutionError::UnknownPacket(payload))?;
        packet.descriptor.ecn_marked = true;
        Ok(())
    }

    fn enqueue_source_packet(
        &mut self,
        node: NodeDescriptor,
        payload: PayloadId,
    ) -> Result<(), ExecutionError> {
        let packet = self.packet(payload)?;
        let source_time_ns = self
            .packets
            .get(&payload)
            .and_then(|resident| resident.source_time_ns)
            .ok_or(ExecutionError::UnknownPacket(payload))?;
        let slot = self.local_state_slot(node)?;
        let position = self
            .hosts
            .slices()
            .0
            .get(slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })?
            .queue
            .iter()
            .rposition(|queued| {
                self.packets.get(queued).is_some_and(|resident| {
                    (
                        u8::from(resident.source_time_ns.is_some()),
                        resident.source_time_ns.unwrap_or(0),
                        resident.descriptor.flow,
                        resident.descriptor.id,
                    ) <= (1, source_time_ns, packet.flow, packet.id)
                })
            })
            .map_or(0, |position| position + 1);
        let state = self.host_state_mut(node)?;
        if position < state.queue.len() {
            state.queue.insert(position, payload);
        } else {
            state.queue.push_back(payload);
        }
        Ok(())
    }

    fn emit_feedback_driven_packet(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        flow: FlowId,
        size_bytes: u64,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let (payload, schedule_ready) = {
            let state = self.host_state_mut(node)?;
            let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            state.next_payload_seq = state
                .next_payload_seq
                .checked_add(1)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let schedule_ready = state.in_service.is_none() && !state.tx_ready_pending;
            if schedule_ready {
                state.tx_ready_pending = true;
            }
            (payload, schedule_ready)
        };
        let packet = PacketDescriptor {
            id: payload,
            flow,
            size_bytes,
            ecn_marked: false,
            kind: PacketKind::Data,
        };
        self.insert_packet(packet, Some(parent.key.time_ns))?;
        self.enqueue_source_packet(node, payload)?;
        self.record_sourced(node.id, packet)?;
        if schedule_ready {
            self.emit_from_host(
                node,
                parent,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: parent.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    fn prepare_tcp_attempts(
        &mut self,
        node: NodeDescriptor,
        flow: FlowId,
        now_ns: u64,
        retransmit_sequence: Option<u64>,
        fill_window: bool,
        preserve_scheduled_send: bool,
    ) -> Result<TcpSendPlan, ExecutionError> {
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let retransmit_segment = retransmit_sequence
            .map(|sequence| {
                self.tcp_sent_segments
                    .get(&flow)
                    .and_then(|segments| segments.get(&sequence))
                    .map(|packet| (sequence, packet.size_bytes))
                    .ok_or(ExecutionError::MissingTcpSegment { flow, sequence })
            })
            .transpose()?;
        let (mut state, index) = self.host_parts_mut(node)?;
        let generator_index =
            index
                .first_generator(flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow,
                })?;
        let FlowGeneratorKind::Tcp(mut tcp) = state.generators[generator_index].kind else {
            return Err(ExecutionError::UnknownGenerator {
                node: node.id,
                flow,
            });
        };
        let mut packets = Vec::new();

        if let Some((sequence, size_bytes)) =
            retransmit_segment.filter(|(sequence, _)| *sequence < tcp.total_bytes)
        {
            let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            *state.next_payload_seq = state
                .next_payload_seq
                .checked_add(1)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            tcp.last_attempt = payload;
            packets.push(PacketDescriptor {
                id: payload,
                flow,
                size_bytes,
                ecn_marked: false,
                kind: PacketKind::TcpData(TcpDataHeader {
                    sequence,
                    sent_time_ns: now_ns,
                    retransmission: true,
                }),
            });
        }

        if fill_window {
            loop {
                let cwnd = tcp.control.cwnd_bytes(tcp.mss_bytes);
                let allowance = cwnd.saturating_sub(tcp.bytes_in_flight);
                if allowance == 0 || tcp.next_sequence >= tcp.total_bytes {
                    break;
                }
                let size_bytes = tcp
                    .mss_bytes
                    .min(tcp.total_bytes - tcp.next_sequence)
                    .min(allowance);
                if size_bytes == 0 {
                    break;
                }
                let payload = allocate_payload_id(node.id, node_count, *state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                *state.next_payload_seq = state
                    .next_payload_seq
                    .checked_add(1)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                let sequence = tcp.next_sequence;
                tcp.next_sequence = tcp
                    .next_sequence
                    .checked_add(size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                tcp.bytes_in_flight = tcp
                    .bytes_in_flight
                    .checked_add(size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                tcp.last_attempt = payload;
                state.generators[generator_index].packets_emitted = state.generators
                    [generator_index]
                    .packets_emitted
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                state.generators[generator_index].bytes_emitted = state.generators[generator_index]
                    .bytes_emitted
                    .checked_add(size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                packets.push(PacketDescriptor {
                    id: payload,
                    flow,
                    size_bytes,
                    ecn_marked: false,
                    kind: PacketKind::TcpData(TcpDataHeader {
                        sequence,
                        sent_time_ns: now_ns,
                        retransmission: false,
                    }),
                });
            }
        }

        let timer =
            if !preserve_scheduled_send && tcp.active_timer.is_none() && tcp.bytes_in_flight != 0 {
                tcp.timer_generation = tcp
                    .timer_generation
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                let deadline_ns = now_ns
                    .checked_add(tcp.rto_ns)
                    .ok_or(ExecutionError::GeneratorTimeOverflow(flow))?;
                let timer = TcpTimerState {
                    attempt: tcp.last_attempt,
                    sequence: tcp.highest_ack,
                    deadline_ns,
                    generation: tcp.timer_generation,
                    rto_ns: tcp.rto_ns,
                };
                tcp.active_timer = Some(timer);
                Some(timer)
            } else {
                None
            };

        let generator = &mut state.generators[generator_index];
        generator.feedback.outstanding_bytes = tcp.bytes_in_flight;
        generator.feedback.unacknowledged_bytes = tcp.bytes_in_flight;
        if !preserve_scheduled_send {
            generator.next_emission.status = if tcp.highest_ack >= tcp.total_bytes {
                GeneratorStatus::Finished
            } else {
                GeneratorStatus::Blocked
            };
        }
        generator.kind = FlowGeneratorKind::Tcp(tcp);
        *state.sourced_packets = state
            .sourced_packets
            .checked_add(u64::try_from(packets.len()).unwrap_or(u64::MAX))
            .ok_or(ExecutionError::CounterOverflow(node.id))?;
        for packet in packets.iter().copied() {
            seed_tcp_segment(&mut self.tcp_sent_segments, packet)?;
        }
        Ok(TcpSendPlan { packets, timer })
    }

    fn install_tcp_attempts(
        &mut self,
        node: NodeDescriptor,
        parent: Event,
        plan: TcpSendPlan,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        for packet in plan.packets {
            self.insert_packet(packet, Some(parent.key.time_ns))?;
            self.enqueue_source_packet(node, packet.id)?;
            self.record_sourced(node.id, packet)?;
        }
        let ready_payload = {
            let (state, _) = self.host_parts_mut(node)?;
            if state.in_service.is_none() && !*state.tx_ready_pending {
                let ready = state.queue.front().copied();
                if ready.is_some() {
                    *state.tx_ready_pending = true;
                }
                ready
            } else {
                None
            }
        };
        if let Some(timer) = plan.timer {
            self.emit_from_host(
                node,
                parent,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::RetransmissionTimeout,
                    payload: timer.attempt,
                    time_ns: timer.deadline_ns,
                },
                children,
            )?;
        }
        if let Some(payload) = ready_payload {
            self.emit_from_host(
                node,
                parent,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload,
                    time_ns: parent.key.time_ns,
                },
                children,
            )?;
        }
        Ok(())
    }

    fn record_sourced(
        &mut self,
        node: NodeId,
        packet: PacketDescriptor,
    ) -> Result<(), ExecutionError> {
        self.observe_packet(packet);
        add_summary(&mut self.summary.sourced_packets, 1, node)?;
        add_summary(&mut self.summary.sourced_bytes, packet.size_bytes, node)
    }

    fn record_departure(
        &mut self,
        node: NodeId,
        packet: PacketDescriptor,
        event_key: EventKey,
    ) -> Result<(), ExecutionError> {
        self.observe_packet(packet);
        add_summary(&mut self.summary.departed_packets, 1, node)?;
        add_summary(&mut self.summary.departed_bytes, packet.size_bytes, node)?;
        if self.observation_mode == ObservationMode::Full {
            self.observe_departure(node, packet, event_key)?;
        }
        Ok(())
    }

    /// Full observation only: retains a departure and records the WFQ or SP completion it ends.
    #[cold]
    #[inline(never)]
    fn observe_departure(
        &mut self,
        node: NodeId,
        packet: PacketDescriptor,
        event_key: EventKey,
    ) -> Result<(), ExecutionError> {
        self.departures.push((
            event_key,
            PacketDeparture {
                payload: packet.id,
                time_ns: event_key.time_ns,
            },
        ));
        self.observe_scheduler_completion(node, event_key, packet)
    }

    fn record_arrival(
        &mut self,
        node: NodeId,
        packet: PacketDescriptor,
        event_key: EventKey,
        disposition: ArrivalDisposition,
    ) -> Result<(), ExecutionError> {
        self.observe_packet(packet);
        let (packets, bytes) = match disposition {
            ArrivalDisposition::Admitted => (
                &mut self.summary.admitted_packets,
                &mut self.summary.admitted_bytes,
            ),
            ArrivalDisposition::Dropped => (
                &mut self.summary.dropped_packets,
                &mut self.summary.dropped_bytes,
            ),
            ArrivalDisposition::Delivered => (
                &mut self.summary.received_packets,
                &mut self.summary.received_bytes,
            ),
            ArrivalDisposition::Feedback => (
                &mut self.summary.feedback_packets,
                &mut self.summary.feedback_bytes,
            ),
        };
        add_summary(packets, 1, node)?;
        add_summary(bytes, packet.size_bytes, node)?;
        if self.observation_mode == ObservationMode::Full {
            self.observe_arrival(node, packet, event_key, disposition)?;
        }
        Ok(())
    }

    /// Full observation only: retains an arrival and records the WFQ or SP enqueue it performs.
    #[cold]
    #[inline(never)]
    fn observe_arrival(
        &mut self,
        node: NodeId,
        packet: PacketDescriptor,
        event_key: EventKey,
        disposition: ArrivalDisposition,
    ) -> Result<(), ExecutionError> {
        self.arrivals.push((
            event_key,
            PacketArrivalObservation {
                payload: packet.id,
                time_ns: event_key.time_ns,
                disposition,
            },
        ));
        if disposition == ArrivalDisposition::Admitted {
            self.observe_scheduler_enqueue(node, event_key, packet)?;
        }
        Ok(())
    }

    fn observe_packet(&mut self, packet: PacketDescriptor) {
        if self.observation_mode == ObservationMode::Full {
            self.observed_packets
                .entry(packet.id)
                .and_modify(|observed| observed.ecn_marked |= packet.ecn_marked)
                .or_insert(packet);
        }
    }

    fn start_transmission(
        &mut self,
        node: NodeId,
        payload: PayloadId,
    ) -> Result<(), ExecutionError> {
        let packet = self
            .packets
            .get_mut(&payload)
            .ok_or(ExecutionError::UnknownPacket(payload))?;
        packet.transmitters = packet
            .transmitters
            .checked_add(1)
            .ok_or(ExecutionError::CounterOverflow(node))?;
        Ok(())
    }

    fn finish_transmission(
        &mut self,
        node: NodeId,
        payload: PayloadId,
    ) -> Result<(), ExecutionError> {
        let remove =
            {
                let packet = self
                    .packets
                    .get_mut(&payload)
                    .ok_or(ExecutionError::UnknownPacket(payload))?;
                packet.transmitters = packet.transmitters.checked_sub(1).ok_or(
                    ExecutionError::UnexpectedTxComplete {
                        node,
                        expected: None,
                        actual: payload,
                    },
                )?;
                packet.transmitters == 0 && (packet.terminal || self.local_node.is_some())
            };
        if remove {
            self.packets.remove(&payload);
        }
        Ok(())
    }

    fn mark_terminal(&mut self, payload: PayloadId) -> Result<(), ExecutionError> {
        let remove = {
            let packet = self
                .packets
                .get_mut(&payload)
                .ok_or(ExecutionError::UnknownPacket(payload))?;
            packet.terminal = true;
            packet.transmitters == 0
        };
        if remove {
            self.packets.remove(&payload);
        }
        Ok(())
    }
}

fn seed_tcp_segment(
    segments: &mut crate::tcp_ledger::TcpSegmentLedger,
    packet: PacketDescriptor,
) -> Result<(), ExecutionError> {
    crate::tcp_ledger::seed_segment(segments, packet).map_err(tcp_segment_conflict_error)
}

fn acknowledge_tcp_segments(
    segments: &mut crate::tcp_ledger::TcpSegmentLedger,
    flow: FlowId,
    acknowledgment: u64,
) -> Result<(), ExecutionError> {
    crate::tcp_ledger::acknowledge_segments(segments, flow, acknowledgment)
        .map_err(tcp_segment_conflict_error)
}

fn tcp_segment_conflict_error(conflict: crate::tcp_ledger::TcpSegmentConflict) -> ExecutionError {
    ExecutionError::InconsistentTcpSegment {
        flow: conflict.flow,
        sequence: conflict.sequence,
        original_size_bytes: conflict.original_size_bytes,
        replacement_size_bytes: conflict.replacement_size_bytes,
    }
}

fn resumable_packets(
    packets: BTreeMap<PayloadId, ResidentPacket>,
    tcp_sent_segments: &crate::tcp_ledger::TcpSegmentLedger,
) -> Vec<PacketDescriptor> {
    let mut descriptors = packets
        .into_iter()
        .map(|(payload, packet)| (payload, packet.descriptor))
        .collect::<BTreeMap<_, _>>();
    for packet in tcp_sent_segments.values().flat_map(BTreeMap::values) {
        descriptors.entry(packet.id).or_insert(*packet);
    }
    descriptors.into_values().collect()
}

/// Credit quanta per bit of a paced packet: `rate_denominator * 10^9`.
///
/// The rate and DCQCN pacers, and the RoCE queue-pair pacer, share this arithmetic: one tick adds
/// [`pacing_tick_credit`] quanta, and a packet is emitted when the credit covers
/// [`paced_packet_cost`]. Every expression is exact in `u128`; `None` is an overflow.
#[inline(always)]
fn pacing_credit_scale(rate_denominator: u64) -> Option<u128> {
    u128::from(rate_denominator).checked_mul(1_000_000_000)
}

/// Credit quanta one pacing tick adds: `rate_bps * pacing_interval_ns`.
#[inline(always)]
fn pacing_tick_credit(rate_bps: u64, pacing_interval_ns: u64) -> Option<u128> {
    u128::from(rate_bps).checked_mul(u128::from(pacing_interval_ns))
}

/// Credit quanta a packet of `size_bytes` costs at `scale` quanta per bit.
#[inline(always)]
fn paced_packet_cost(size_bytes: u64, scale: u128) -> Option<u128> {
    u128::from(size_bytes)
        .checked_mul(8)
        .and_then(|bits| bits.checked_mul(scale))
}

fn scheduler_class(flow: FlowId, class_count: usize) -> Option<usize> {
    let class_count = u64::try_from(class_count).ok()?;
    if class_count == 0 {
        return None;
    }
    usize::try_from(flow.0 % class_count).ok()
}

fn zero_rational() -> Ratio<BigUint> {
    Ratio::from_integer(BigUint::from(0_u8))
}

fn wfq_active_weight_sum(state: &WfqSchedulerState) -> BigUint {
    state
        .weights
        .iter()
        .zip(&state.active_packets)
        .filter(|(_, active)| **active != 0)
        .fold(BigUint::from(0_u8), |sum, (weight, _)| {
            sum + BigUint::from(*weight)
        })
}

fn wfq_advance_virtual_time(
    state: &mut WfqSchedulerState,
    time_ns: u64,
    rate_bps: u64,
    node: NodeId,
) -> Result<(), ExecutionError> {
    let elapsed_ns =
        time_ns
            .checked_sub(state.last_updated_ns)
            .ok_or(ExecutionError::NonMonotoneWfqTime {
                node,
                previous_ns: state.last_updated_ns,
                current_ns: time_ns,
            })?;
    let weight_sum = wfq_active_weight_sum(state);
    if weight_sum == BigUint::from(0_u8) {
        return Err(ExecutionError::InvalidSchedulerState(node));
    }
    if elapsed_ns != 0 {
        let numerator = BigUint::from(elapsed_ns) * BigUint::from(rate_bps);
        let denominator = BigUint::from(1_000_000_000_u64) * weight_sum;
        state.virtual_time = state.virtual_time.clone() + Ratio::new(numerator, denominator);
    }
    Ok(())
}

fn wfq_enqueue(
    state: &mut WfqSchedulerState,
    queue: &mut std::collections::VecDeque<PayloadId>,
    packet: PacketDescriptor,
    arrival_time_ns: u64,
    rate_bps: u64,
    node: NodeId,
) -> Result<(), ExecutionError> {
    let class = scheduler_class(packet.flow, state.weights.len())
        .ok_or(ExecutionError::InvalidSchedulerState(node))?;
    if state.weights[class] == 0
        || state.finish_times.len() != state.weights.len()
        || state.active_packets.len() != state.weights.len()
    {
        return Err(ExecutionError::InvalidSchedulerState(node));
    }
    if arrival_time_ns < state.last_updated_ns {
        return Err(ExecutionError::NonMonotoneWfqTime {
            node,
            previous_ns: state.last_updated_ns,
            current_ns: arrival_time_ns,
        });
    }

    if state.active_packets.iter().all(|active| *active == 0) {
        state.virtual_time = zero_rational();
        state.finish_times.fill(zero_rational());
    } else {
        wfq_advance_virtual_time(state, arrival_time_ns, rate_bps, node)?;
    }

    let virtual_start = state
        .virtual_time
        .clone()
        .max(state.finish_times[class].clone());
    let service = Ratio::new(
        BigUint::from(packet.size_bytes) * BigUint::from(8_u8),
        BigUint::from(state.weights[class]),
    );
    let finish = virtual_start + service;
    state.finish_times[class] = finish.clone();

    let mut position = queue.len();
    for (index, queued) in queue.iter().enumerate() {
        let queued_finish =
            state
                .packet_finish_times
                .get(queued)
                .ok_or(ExecutionError::MissingWfqFinishTag {
                    node,
                    payload: *queued,
                })?;
        if queued_finish > &finish {
            position = index;
            break;
        }
    }
    if state
        .packet_finish_times
        .insert(packet.id, finish)
        .is_some()
    {
        return Err(ExecutionError::InvalidSchedulerState(node));
    }
    queue.insert(position, packet.id);
    state.active_packets[class] = state.active_packets[class]
        .checked_add(1)
        .ok_or(ExecutionError::CounterOverflow(node))?;
    state.last_updated_ns = arrival_time_ns;
    Ok(())
}

fn wfq_complete(
    state: &mut WfqSchedulerState,
    packet: PacketDescriptor,
    departure_time_ns: u64,
    rate_bps: u64,
    node: NodeId,
) -> Result<(), ExecutionError> {
    let class = scheduler_class(packet.flow, state.weights.len())
        .ok_or(ExecutionError::InvalidSchedulerState(node))?;
    if state.weights[class] == 0
        || state.finish_times.len() != state.weights.len()
        || state.active_packets.len() != state.weights.len()
        || state.active_packets[class] == 0
    {
        return Err(ExecutionError::InvalidSchedulerState(node));
    }
    wfq_advance_virtual_time(state, departure_time_ns, rate_bps, node)?;
    state
        .packet_finish_times
        .remove(&packet.id)
        .ok_or(ExecutionError::MissingWfqFinishTag {
            node,
            payload: packet.id,
        })?;
    state.active_packets[class] -= 1;
    if state.active_packets.iter().all(|active| *active == 0) {
        state.virtual_time = zero_rational();
        state.finish_times[class] = zero_rational();
    }
    state.last_updated_ns = departure_time_ns;
    Ok(())
}

fn scheduler_select_position(
    scheduler: &mut SchedulerKind,
    packets: &[PacketDescriptor],
    node: NodeId,
) -> Result<Option<(usize, u128)>, ExecutionError> {
    if packets.is_empty() {
        return Ok(None);
    }
    match scheduler {
        // The three head-serving disciplines. `scheduler_serves_head` classifies exactly this
        // set, and the two must be changed together.
        SchedulerKind::Fifo
        | SchedulerKind::StaticPriority { .. }
        | SchedulerKind::WeightedFairQueue(_) => Ok(Some((0, 0))),
        SchedulerKind::DeficitRoundRobin(state) => {
            let class_count = state.quanta_bytes.len();
            if class_count == 0
                || state.deficits_bytes.len() != class_count
                || state.quanta_bytes.contains(&0)
            {
                return Err(ExecutionError::InvalidSchedulerState(node));
            }
            let mut current = usize::try_from(state.current_class)
                .ok()
                .filter(|class| *class < class_count)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
            let mut scan_steps = 0_u128;
            loop {
                let position = packets
                    .iter()
                    .position(|packet| scheduler_class(packet.flow, class_count) == Some(current));
                if let Some(position) = position {
                    let size = packets[position].size_bytes;
                    if state.deficits_bytes[current] > 0 && size <= state.deficits_bytes[current] {
                        state.deficits_bytes[current] -= size;
                        state.current_class = current as u64;
                        return Ok(Some((position, scan_steps)));
                    }
                }
                current += 1;
                if current == class_count {
                    for class in 0..class_count {
                        if packets
                            .iter()
                            .any(|packet| scheduler_class(packet.flow, class_count) == Some(class))
                        {
                            state.deficits_bytes[class] = state.deficits_bytes[class]
                                .checked_add(state.quanta_bytes[class])
                                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
                        } else {
                            state.deficits_bytes[class] = 0;
                        }
                    }
                    current = 0;
                }
                state.current_class = current as u64;
                // A selection scans at most `u64::MAX` deficit rounds across at most `u64::MAX`
                // classes, so the exact instrumentation count is strictly below `u128::MAX`.
                scan_steps += 1;
            }
        }
        SchedulerKind::WeightedRoundRobin(state) => {
            let class_count = state.weights.len();
            if class_count == 0
                || state.packets_sent_in_round.len() != class_count
                || state.weights.contains(&0)
            {
                return Err(ExecutionError::InvalidSchedulerState(node));
            }
            let mut current = usize::try_from(state.current_class)
                .ok()
                .filter(|class| *class < class_count)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
            loop {
                if state.packets_sent_in_round[current] < state.weights[current] {
                    if let Some(position) = packets.iter().position(|packet| {
                        scheduler_class(packet.flow, class_count) == Some(current)
                    }) {
                        state.packets_sent_in_round[current] += 1;
                        state.current_class = current as u64;
                        return Ok(Some((position, 0)));
                    }
                }
                state.packets_sent_in_round[current] = 0;
                current = (current + 1) % class_count;
                state.current_class = current as u64;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueAdmissionAction {
    Enqueue,
    Mark,
    Drop,
}

fn derive_switch_queue_bytes(
    image: &SimulationImage,
    switch_states: &[SwitchState],
    local_node: Option<NodeDescriptor>,
    packets: &BTreeMap<PayloadId, ResidentPacket>,
) -> Result<DerivedSwitchQueues, ExecutionError> {
    let mut node_ids = vec![None; switch_states.len()];
    if let Some(node) = local_node {
        if node.kind == NodeKind::Switch {
            node_ids[0] = Some(node.id);
        }
    } else {
        for node in &image.nodes {
            if node.kind == NodeKind::Switch {
                if let Some(slot) = node_ids.get_mut(node.state_slot as usize) {
                    *slot = Some(node.id);
                }
            }
        }
    }
    let mut paused_fifo_queues = Vec::new();
    let bytes = switch_states
        .iter()
        .enumerate()
        .map(|(state_slot, state)| {
            let node = node_ids[state_slot]
                .ok_or(ExecutionError::UnknownNode(NodeId(state_slot as u64)))?;
            state
                .queues
                .iter()
                .enumerate()
                .map(|(queue_slot, queue)| {
                    if matches!(queue.scheduler, SchedulerKind::Fifo)
                        && queue.pfc.as_ref().is_some_and(pfc_any_paused)
                    {
                        paused_fifo_queues.push((state_slot, queue_slot));
                    }
                    queue.queue.iter().try_fold(0_u64, |total, payload| {
                        let packet = packets
                            .get(payload)
                            .ok_or(ExecutionError::UnknownPacket(*payload))?;
                        total
                            .checked_add(packet.descriptor.size_bytes)
                            .ok_or(ExecutionError::CounterOverflow(node))
                    })
                })
                .collect()
        })
        .collect::<Result<_, _>>()?;
    Ok(DerivedSwitchQueues {
        bytes,
        paused_fifo_queues,
    })
}

/// The ECN ramp's decision for one arrival (`crate::ecn_ramp`); the draw, keyed by the image
/// seed, the switch LP, the queue slot and the arrival's payload, is computed only inside the ramp
/// for ECN-capable data.
fn ecn_ramp_action(
    policy: &crate::EcnRampPolicy,
    queued_bytes: u64,
    packet: PacketDescriptor,
    seed: u64,
    node: NodeId,
    queue_id: u64,
    payload: PayloadId,
) -> QueueAdmissionAction {
    use crate::ecn_ramp::{EcnRampAction, ecn_draw, ecn_queue_key, ecn_ramp_decision};
    match ecn_ramp_decision(
        policy,
        queued_bytes,
        packet.size_bytes,
        packet.kind.is_data(),
        || ecn_draw(ecn_queue_key(seed, node.0, queue_id), payload.0),
    ) {
        EcnRampAction::Enqueue => QueueAdmissionAction::Enqueue,
        EcnRampAction::Mark => QueueAdmissionAction::Mark,
        EcnRampAction::Drop => QueueAdmissionAction::Drop,
    }
}

/// Tail drop at the queue's packet capacity (zero is unbounded); an arrival whose byte total is
/// unrepresentable also drops.
fn taildrop_action(
    capacity_packets: u64,
    queued_packets: u64,
    queued_bytes: u64,
    packet_size_bytes: u64,
    node: NodeId,
) -> Result<QueueAdmissionAction, ExecutionError> {
    let post_packets = queued_packets
        .checked_add(1)
        .ok_or(ExecutionError::CounterOverflow(node))?;
    Ok(
        if queued_bytes.checked_add(packet_size_bytes).is_none()
            || capacity_packets != 0 && post_packets > capacity_packets
        {
            QueueAdmissionAction::Drop
        } else {
            QueueAdmissionAction::Enqueue
        },
    )
}

fn tcp_receive_range(receiver: &mut crate::TcpReceiverState, start: u64, end: u64) {
    if end <= start || end <= receiver.next_expected_sequence {
        return;
    }
    receiver.out_of_order.push(TcpReceiveRange { start, end });
    receiver
        .out_of_order
        .sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged = Vec::<TcpReceiveRange>::with_capacity(receiver.out_of_order.len());
    for range in receiver.out_of_order.drain(..) {
        if let Some(last) = merged.last_mut() {
            if range.start <= last.end {
                last.end = last.end.max(range.end);
                continue;
            }
        }
        merged.push(range);
    }
    let mut next = receiver.next_expected_sequence;
    let mut keep = Vec::with_capacity(merged.len());
    for range in merged {
        if range.start <= next {
            next = next.max(range.end);
        } else {
            keep.push(range);
        }
    }
    receiver.next_expected_sequence = next;
    receiver.out_of_order = keep;
}

fn indexed_lookup<T>(table: &[T], id: u64, matches_id: impl Fn(&T) -> bool) -> Option<&T> {
    let indexed = usize::try_from(id)
        .ok()
        .and_then(|index| table.get(index))
        .filter(|descriptor| matches_id(descriptor));

    // Validated images always return above. The fallback preserves `run_scalar` behavior and
    // checked `Unknown*` errors for legacy hand-built callers that intentionally skip validation.
    indexed.or_else(|| table.iter().find(|descriptor| matches_id(descriptor)))
}

fn initial_event_queue(
    image: &SimulationImage,
) -> Result<BTreeMap<EventKey, Event>, ExecutionError> {
    let mut events = BTreeMap::new();
    for event in image.initial_events.iter().copied() {
        if events.insert(event.key, event).is_some() {
            return Err(ExecutionError::DuplicateEventKey(event.key));
        }
    }
    Ok(events)
}

fn allocate_payload_id(source: NodeId, node_count: u64, sequence: u64) -> Option<PayloadId> {
    PayloadId::from_node_sequence(source, node_count, sequence)
}

fn add_summary(total: &mut u128, value: u64, node: NodeId) -> Result<(), ExecutionError> {
    *total = total
        .checked_add(u128::from(value))
        .ok_or(ExecutionError::CounterOverflow(node))?;
    Ok(())
}

/// P16 ruling D2: a transition of a DCQCN flow or queue pair at exclusive bound `bound` first
/// applies its controller's due rate-increase and rate-decrease instants. Returns the controller
/// before and what was applied, or `None` when nothing was due (the common case: one comparison)
/// or the flow is complete and its controller frozen (ruling D11).
#[inline(always)]
fn dcqcn_materialize(
    controller: &mut crate::DcqcnController,
    frozen: bool,
    bound: u64,
) -> Option<(crate::DcqcnController, crate::DcqcnAdvance)> {
    if frozen || controller.due_ns() >= bound {
        return None;
    }
    let before = *controller;
    Some((before, controller.materialize(bound)))
}

/// A DCQCN controller transition record (schema `days-gpu/plans/briefs/p16/dcqcn-schema.md`).
#[allow(clippy::too_many_arguments)]
const fn dcqcn_record(
    key: EventKey,
    node: NodeId,
    flow: FlowId,
    kind: crate::DcqcnTransitionKind,
    bound_ns: u64,
    advance: crate::DcqcnAdvance,
    frozen: bool,
    before: crate::DcqcnController,
    after: crate::DcqcnController,
) -> crate::DcqcnTransitionRecord {
    crate::DcqcnTransitionRecord {
        key,
        node,
        flow,
        kind,
        bound_ns,
        advance,
        frozen,
        before,
        after,
    }
}

/// Routes an arriving feedback packet through the source-generator contract hook.
///
/// Constant generators only record the arrival. TCP ACK processing calls this hook before its
/// structured cumulative-ACK transition, then uses the caller's host-owned emission path to
/// refill the congestion window without introducing a backend-specific event kind.
/// Settles a queue pair's status and feedback mirrors after a transition (design note §5): the
/// mirrors hold the outstanding bytes, as TCP's hold its bytes in flight.
fn settle_roce_sender(
    generator: &mut crate::FlowGeneratorState,
    roce: &crate::RoceGenerator,
    parked_status: GeneratorStatus,
) {
    generator.next_emission.status = crate::roce::settled_status(roce, parked_status);
    let outstanding = generator.bytes_emitted - roce.snd_una;
    generator.feedback.outstanding_bytes = outstanding;
    generator.feedback.unacknowledged_bytes = outstanding;
}

/// Restarts a parked pacer that has packets to send again, on its next grid point after `now_ns`
/// (ruling D3). Returns the tick to emit, or `None` when the pacer stays as it is; a restart
/// beyond the stop time leaves it `Stopped` there.
fn restart_roce_pacer(
    generator: &mut crate::FlowGeneratorState,
    roce: &mut crate::RoceGenerator,
    now_ns: u64,
    stop_time_ns: u64,
) -> Result<Option<u64>, ExecutionError> {
    // Every restart attempt ends a window park (ruling D7): it runs only for an ACK or NACK that
    // moves `snd_una` and for a timeout, whatever it then finds.
    roce.window_parked = false;
    if roce.pacer_armed
        || roce.next_psn >= roce.pacer.total_bytes
        || roce.snd_una >= roce.pacer.total_bytes
    {
        return Ok(None);
    }
    let time_ns = crate::roce::restart_time_ns(roce, now_ns)
        .ok_or(ExecutionError::GeneratorTimeOverflow(generator.flow))?;
    generator.next_emission.departure_time_ns = time_ns;
    if time_ns <= stop_time_ns {
        roce.pacer_armed = true;
        Ok(Some(time_ns))
    } else {
        generator.next_emission.status = GeneratorStatus::Stopped;
        Ok(None)
    }
}

/// Keeps a host's parked list exact after an ACK, NACK or timeout (host-link PFC): a queue pair
/// stays listed only while its pacer is parked, not stopped, with packets left to send. A restart
/// (the restarted tick finds the class paused and parks it again) or completion takes it out.
fn leave_parked_list(
    pfc: &mut Option<Box<crate::HostPfcState>>,
    data_class: u8,
    position: usize,
    generator: &crate::FlowGeneratorState,
    roce: &crate::RoceGenerator,
) {
    if let Some(pfc) = pfc.as_deref_mut() {
        let restartable = !roce.pacer_armed
            && generator.next_emission.status != GeneratorStatus::Stopped
            && roce.next_psn < roce.pacer.total_bytes
            && roce.snd_una < roce.pacer.total_bytes;
        if !restartable {
            pfc.pause_parked[usize::from(data_class)].remove(&position);
        }
    }
}

/// The unloaded round trip of `flow`'s data packet of `data_bytes` and the ACK of `ack_bytes`
/// that answers it: every link of the forward and reverse routes serializes and propagates each
/// once, a lower bound on the time from the data's send to the ACK's arrival. Read from the image
/// in place (no executor borrow, no route copy).
fn unloaded_round_trip_ns(
    image: &SimulationImage,
    flow: FlowId,
    data_bytes: u64,
    ack_bytes: u64,
) -> Result<u64, ExecutionError> {
    let descriptor = indexed_lookup(&image.flows, flow.0, |candidate| candidate.id == flow)
        .ok_or(ExecutionError::UnknownFlow(flow))?;
    let mut total = 0_u64;
    for (links, bytes) in [
        (&descriptor.route, data_bytes),
        (&descriptor.reverse_route, ack_bytes),
    ] {
        for &id in links {
            let delay = indexed_lookup(&image.links, id.0, |link| link.id == id)
                .ok_or(ExecutionError::UnknownLink(id))?
                .delay_ns(bytes)
                .map_err(|_| ExecutionError::GeneratorTimeOverflow(flow))?;
            total = total
                .checked_add(delay)
                .ok_or(ExecutionError::GeneratorTimeOverflow(flow))?;
        }
    }
    Ok(total)
}

/// A flow's data priority, read from the image (no executor borrow).
fn image_flow_priority(image: &SimulationImage, flow: FlowId) -> Result<u8, ExecutionError> {
    indexed_lookup(&image.flows, flow.0, |descriptor| descriptor.id == flow)
        .map(|descriptor| descriptor.priority)
        .ok_or(ExecutionError::UnknownFlow(flow))
}

/// A queue-pair sender record of the pinned schema.
#[allow(clippy::too_many_arguments)]
fn roce_sender_record(
    key: EventKey,
    node: NodeId,
    flow: FlowId,
    kind: crate::RoceSenderKind,
    park: Option<crate::roce::TickPark>,
    data_class: u8,
    roce: &crate::RoceGenerator,
    rate_bps: Option<u64>,
    input: Option<crate::RoceAckHeader>,
    emitted: Option<crate::RoceEmission>,
    before: crate::RoceSenderView,
    after: crate::RoceSenderView,
) -> crate::MechanismTransitionRecord {
    crate::MechanismTransitionRecord::Roce(crate::RoceTransitionRecord::Sender(
        crate::RoceSenderRecord {
            key,
            node,
            flow,
            kind,
            class_paused: park == Some(crate::roce::TickPark::ClassPaused),
            window_blocked: park == Some(crate::roce::TickPark::WindowBlocked),
            data_class,
            mtu_bytes: roce.pacer.mtu_bytes,
            total_bytes: roce.pacer.total_bytes,
            pacing_interval_ns: roce.pacer.pacing_interval_ns,
            first_pacing_time_ns: roce.pacer.first_pacing_time_ns,
            rto_ns: roce.rto_ns,
            window_bytes: roce.window_bytes,
            variable_window: roce.variable_window,
            maximum_rate_bps: roce.controller.config.maximum_rate_bps,
            initial_rate_bps: roce.controller.config.initial_rate_bps,
            rate_bps,
            input_acknowledgment: input.map(|header| header.acknowledgment),
            input_ce_echo: input.map(|header| header.ce_echo),
            emitted,
            before,
            after,
        },
    ))
}

fn apply_generator_feedback(
    generator: &mut crate::FlowGeneratorState,
    node: NodeId,
) -> Result<GeneratorFeedbackAction, ExecutionError> {
    generator.feedback.arrivals = generator
        .feedback
        .arrivals
        .checked_add(1)
        .ok_or(ExecutionError::CounterOverflow(node))?;
    match generator.kind {
        FlowGeneratorKind::Constant(_) => Ok(GeneratorFeedbackAction::None),
        FlowGeneratorKind::Tcp(_) => Ok(GeneratorFeedbackAction::None),
        FlowGeneratorKind::Rate(_) => Ok(GeneratorFeedbackAction::None),
        FlowGeneratorKind::Dcqcn(_) => Ok(GeneratorFeedbackAction::None),
        FlowGeneratorKind::Roce(_) => Ok(GeneratorFeedbackAction::None),
    }
}

#[cfg(test)]
mod tests {
    use super::TransitionState;
    use crate::stage_index::StageScanProbe;

    /// The CPU executor builds one `TransitionState` per LP and stores it inline in every LP, so
    /// each byte here is a per-LP cost of every CPU run: on E1's 9,472 LPs the LP arrays grow by
    /// 9,472 bytes per byte, and the serial worker walks them. At `main` (948a0e9) it is 512 B.
    /// P14 first kept the host stage indices in a vector of their own beside `host_states` (24 B,
    /// rounded to 32 B by the 16-byte alignment `RunSummary`'s `u128` counters force), 544 B.
    /// Removing that growth, with the per-host allocation that came with it, is what the fix
    /// targets; madrid's re-timing measured the CPU `--workers 1` E1 run recover from +2.3% over
    /// `main` to +0.22% (median paired; -1.96% against the unfixed tip, 39 of 40 pairs). The
    /// recovery is measured; that the memory footprint is its mechanism is the fix's premise, not
    /// something measured directly (`days-gpu/evidence/P14/e1-residue.md`, `e1-retime.md`). Any
    /// field added here rounds up to 528 B.
    ///
    /// The test hooks' dispatch counter (`stage_probe`, 16 B with the hooks, empty without) is the
    /// one field `main` did not have, so it is allowed for. Layout is the compiler's choice, so the
    /// bound is an upper bound on 64-bit targets.
    /// P15 lane R3: `record_inbound_progress` runs on every TCP data arrival of every image, where a
    /// host without stages returns at once. With one caller LLVM inlined it into
    /// `host_tcp_data_arrival`; the RoCE data arrival is a second caller, and without the attribute
    /// the function went out of line, so every TCP segment paid a call (sim call-site check at
    /// `ff95eb1`, `days-gpu/evidence/P15/collectives-impl/sim/gate-ff95eb1/callsites.txt`).
    #[test]
    fn record_inbound_progress_is_force_inlined() {
        let source = include_str!("scalar.rs");
        assert!(
            source.contains("#[inline(always)]\nfn record_inbound_progress("),
            "`record_inbound_progress` must be #[inline(always)]"
        );
    }

    /// See `switch_pfc_remote_arrival`: the PFC frame handler stays out of `dispatch`.
    #[test]
    fn switch_pfc_remote_arrival_stays_out_of_line() {
        let source = include_str!("scalar.rs");
        assert!(
            source.contains("    #[inline(never)]\n    fn switch_pfc_remote_arrival("),
            "`switch_pfc_remote_arrival` must be #[inline(never)]"
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn transition_state_keeps_main_size() {
        const MAIN_TRANSITION_STATE_BYTES: usize = 512;
        let bound = MAIN_TRANSITION_STATE_BYTES
            + std::mem::size_of::<StageScanProbe>()
            + std::mem::size_of::<super::PfcServiceProbe>();
        let size = std::mem::size_of::<TransitionState<'static>>();
        assert!(
            size <= bound,
            "TransitionState grew to {size} B, above {bound} B: keep per-host executor state in \
             the host's own entry"
        );
    }
}

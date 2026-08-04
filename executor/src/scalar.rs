//! Canonical serial priority-queue execution.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use num_bigint::BigUint;
use num_rational::Ratio;

#[cfg(feature = "p11-profile")]
use crate::p11_profile::{P11PacketStoreOperation, P11PacketStoreProfile, P11PacketStoreProfiler};

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

/// One non-TailDrop enqueue decision, keyed by the event that caused it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AqmTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    /// Stable index of the queue within the node-owned switch state.
    pub queue_id: u64,
    pub payload: PayloadId,
    pub queued_packets_before: u64,
    pub queued_bytes_before: u64,
    pub packet_size_bytes: u64,
    pub ecn_before: bool,
    pub ecn_after: bool,
    pub before: crate::DropMarkPolicy,
    pub after: crate::DropMarkPolicy,
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
    /// Exact TCP control transitions retained in full observation mode.
    pub tcp_transitions: Vec<TcpTransitionRecord>,
    /// Exact RED/ECN enqueue transitions retained in full observation mode.
    pub aqm_transitions: Vec<AqmTransitionRecord>,
    /// Exact rate/PFC/DRR/WRR/collective transitions retained in full observation mode.
    pub mechanism_transitions: Vec<crate::MechanismTransitionRecord>,
    /// Unprocessed events in canonical `EventKey` order.
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
    Time(TimeError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingCollectiveProgress {
    flow: FlowId,
    cause: crate::CollectiveActivationCause,
    cause_flow: FlowId,
    arrival_bytes: u64,
    before_local_complete: bool,
    before_inbound_complete: bool,
    before_inbound_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CollectiveProgressContext {
    key: EventKey,
    ordinal: u64,
    node: NodeId,
    stop_time_ns: u64,
    activated: bool,
}

fn collective_progress_record(
    context: CollectiveProgressContext,
    cause: PendingCollectiveProgress,
    collective: crate::CollectiveGenerator,
    generator: &crate::FlowGeneratorState,
) -> crate::CollectiveProgressRecord {
    crate::CollectiveProgressRecord {
        key: context.key,
        ordinal: context.ordinal,
        node: context.node,
        flow: generator.flow,
        cause: cause.cause,
        cause_flow: cause.cause_flow,
        arrival_bytes: cause.arrival_bytes,
        collective_id: collective.collective_id,
        algorithm: collective.algorithm,
        group_size: collective.group_size,
        declared_total_bytes: collective.declared_total_bytes,
        rank: collective.rank,
        phase: collective.phase,
        step: collective.step,
        chunk_offset_bytes: collective.chunk_offset_bytes,
        chunk_bytes: collective.chunk_bytes,
        packet_size_bytes: collective.packet_size_bytes,
        interval_ns: collective.interval_ns,
        stop_time_ns: context.stop_time_ns,
        local_predecessor: collective.local_predecessor,
        inbound_predecessor: collective.inbound_predecessor,
        inbound_predecessor_bytes: collective.inbound_predecessor_bytes,
        before_local_complete: cause.before_local_complete,
        before_inbound_complete: cause.before_inbound_complete,
        before_inbound_bytes: cause.before_inbound_bytes,
        activated: context.activated,
        after_local_complete: collective.local_predecessor_complete,
        after_inbound_complete: collective.inbound_predecessor_complete,
        after_inbound_bytes: collective.inbound_bytes_received,
        after_packets_emitted: generator.packets_emitted,
        after_bytes_emitted: generator.bytes_emitted,
        after_status: generator.next_emission.status,
        after_next_time_ns: generator.next_emission.departure_time_ns,
    }
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
    let mut transitions = TransitionState::new(image, observation_mode)?;
    let mut events = initial_event_queue(image)?;
    let mut children = Vec::new();

    while events.first_key_value().is_some_and(|(key, _)| {
        key.time_ns <= image.stop_time_ns
            && exclusive_horizon_ns.is_none_or(|horizon_ns| key.time_ns < horizon_ns)
    }) {
        let (_, event) = events
            .pop_first()
            .expect("first_key_value established a pending event");
        transitions.dispatch(event, &mut children)?;
        for child in children.drain(..) {
            if events.insert(child.key, child).is_some() {
                return Err(ExecutionError::DuplicateEventKey(child.key));
            }
        }
    }

    Ok(transitions.finish(events.into_values().collect()))
}

pub(crate) struct TransitionState<'image> {
    image: &'image SimulationImage,
    host_states: Vec<HostState>,
    switch_states: Vec<SwitchState>,
    local_node: Option<NodeDescriptor>,
    packets: BTreeMap<PayloadId, ResidentPacket>,
    #[cfg(feature = "p11-profile")]
    p11_packet_store: P11PacketStoreProfiler,
    observation_mode: ObservationMode,
    summary: RunSummary,
    observed_packets: BTreeMap<PayloadId, PacketDescriptor>,
    departures: Vec<(EventKey, PacketDeparture)>,
    arrivals: Vec<(EventKey, PacketArrivalObservation)>,
    tcp_transitions: Vec<TcpTransitionRecord>,
    aqm_transitions: Vec<AqmTransitionRecord>,
    mechanism_transitions: Vec<crate::MechanismTransitionRecord>,
    tcp_sent_segments: crate::tcp_ledger::TcpSegmentLedger,
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

        Ok(Self {
            image,
            host_states: image.host_states.clone(),
            switch_states: image.switch_states.clone(),
            local_node: None,
            packets,
            #[cfg(feature = "p11-profile")]
            p11_packet_store: P11PacketStoreProfiler::default(),
            observation_mode,
            summary: RunSummary::default(),
            observed_packets: BTreeMap::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            tcp_transitions: Vec::new(),
            aqm_transitions: Vec::new(),
            mechanism_transitions: Vec::new(),
            tcp_sent_segments,
        })
    }

    pub(crate) fn new_local(
        image: &'image SimulationImage,
        node: NodeDescriptor,
        packets: impl IntoIterator<Item = PacketDescriptor>,
        tcp_segment_seeds: impl IntoIterator<Item = PacketDescriptor>,
        observation_mode: ObservationMode,
    ) -> Result<Self, ExecutionError> {
        let (host_states, switch_states) = match node.kind {
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
                (vec![state], Vec::new())
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
                (Vec::new(), vec![state])
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
            NodeKind::Host => host_states[0].in_service.into_iter().collect::<Vec<_>>(),
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

        Ok(Self {
            image,
            host_states,
            switch_states,
            local_node: Some(node),
            packets: resident,
            #[cfg(feature = "p11-profile")]
            p11_packet_store: P11PacketStoreProfiler::default(),
            observation_mode,
            summary: RunSummary::default(),
            observed_packets: BTreeMap::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            tcp_transitions: Vec::new(),
            aqm_transitions: Vec::new(),
            mechanism_transitions: Vec::new(),
            tcp_sent_segments,
        })
    }

    pub(crate) fn install_packet(
        &mut self,
        descriptor: PacketDescriptor,
    ) -> Result<(), ExecutionError> {
        #[cfg(feature = "p11-profile")]
        let lookup_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Lookup);
        let existing = self.packets.get(&descriptor.id);
        #[cfg(feature = "p11-profile")]
        drop(lookup_guard);
        if let Some(existing) = existing {
            return if existing.descriptor == descriptor {
                Ok(())
            } else {
                Err(ExecutionError::DuplicatePayload(descriptor.id))
            };
        }
        #[cfg(feature = "p11-profile")]
        let insert_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Insert { generator: false });
        self.packets.insert(
            descriptor.id,
            ResidentPacket {
                descriptor,
                source_time_ns: None,
                transmitters: 0,
                terminal: false,
            },
        );
        #[cfg(feature = "p11-profile")]
        drop(insert_guard);
        Ok(())
    }

    #[cfg(feature = "p11-profile")]
    pub(crate) fn p11_packet_store_profile(&self) -> P11PacketStoreProfile {
        self.p11_packet_store.snapshot()
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
            host_states: self.host_states,
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
            tcp_transitions: self.tcp_transitions,
            aqm_transitions: self.aqm_transitions,
            mechanism_transitions: self.mechanism_transitions,
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
                self.host_states
                    .pop()
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

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    pub(crate) fn queue_occupancy(&self, node_id: NodeId) -> Result<usize, ExecutionError> {
        let node = self.node(node_id)?;
        match node.kind {
            NodeKind::Host => Ok(self.host_state(node)?.queue.len()),
            NodeKind::Switch => Ok(self
                .switch_state(node)?
                .queues
                .iter()
                .map(|queue| queue.queue.len())
                .sum()),
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
        let owns_generator = self
            .host_state(node)?
            .generators
            .iter()
            .any(|generator| generator.flow == packet.flow);
        if !owns_generator {
            return self.host_preloaded_packet_arrival(node, event, children);
        }
        if let PacketKind::TcpData(header) = packet.kind {
            return self.host_tcp_initial_send(node, event, packet, header, children);
        }
        if self
            .host_state(node)?
            .generators
            .iter()
            .find(|generator| generator.flow == packet.flow)
            .is_some_and(|generator| matches!(generator.kind, FlowGeneratorKind::Collective(_)))
        {
            return self.host_collective_scheduled_send(node, event, packet, children);
        }
        self.set_source_time(event.payload, event.key.time_ns)?;
        let (next_packet, next_departure_ns, schedule_ready) = {
            let stop_time_ns = self.image.stop_time_ns;
            let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
            let state = self.host_state_mut(node)?;
            let generator_index = state
                .generators
                .iter()
                .position(|generator| generator.flow == packet.flow)
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
                let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                state.next_payload_seq = state
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

            state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let schedule_ready = if state.in_service.is_none() && !state.tx_ready_pending {
                state.tx_ready_pending = true;
                true
            } else {
                false
            };
            (next_packet, next_departure_ns, schedule_ready)
        };
        self.enqueue_source_packet(node, event.payload)?;

        self.record_sourced(node.id, packet)?;
        if let Some(next_packet) = next_packet {
            self.insert_generated_packet(
                next_packet,
                Some(next_departure_ns.expect("produced time")),
            )?;
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

    fn host_collective_scheduled_send(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        self.set_source_time(packet.id, event.key.time_ns)?;
        let stop_time_ns = self.image.stop_time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let mut progress_causes = Vec::new();
        let (next_packet, next_time, schedule_ready) = {
            let state = self.host_state_mut(node)?;
            let generator_index = state
                .generators
                .iter()
                .position(|generator| generator.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
            let generator = &mut state.generators[generator_index];
            let FlowGeneratorKind::Collective(collective) = generator.kind else {
                return Err(ExecutionError::UnexpectedGeneratorEmission {
                    node: node.id,
                    flow: packet.flow,
                    payload: packet.id,
                });
            };
            if generator.next_emission.status != GeneratorStatus::Scheduled
                || generator.next_emission.departure_time_ns != event.key.time_ns
                || generator.next_emission.payload != packet.id
                || !collective.prerequisites_complete()
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
            let remaining = collective
                .chunk_bytes
                .checked_sub(generator.bytes_emitted)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let candidate = (remaining != 0)
                .then(|| {
                    event
                        .key
                        .time_ns
                        .checked_add(collective.interval_ns)
                        .ok_or(ExecutionError::GeneratorTimeOverflow(packet.flow))
                })
                .transpose()?;
            let (next_packet, next_time) =
                if let Some(next_time) = candidate.filter(|time| *time <= stop_time_ns) {
                    let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    state.next_payload_seq = state
                        .next_payload_seq
                        .checked_add(1)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    generator.next_emission = crate::ScheduledEmission {
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: next_time,
                        payload,
                    };
                    (
                        Some(PacketDescriptor {
                            id: payload,
                            flow: packet.flow,
                            size_bytes: collective.packet_size_bytes.min(remaining),
                            ecn_marked: false,
                            kind: PacketKind::Data,
                        }),
                        Some(next_time),
                    )
                } else {
                    generator.next_emission.status = if remaining == 0 {
                        GeneratorStatus::Finished
                    } else {
                        GeneratorStatus::Stopped
                    };
                    if let Some(candidate) = candidate {
                        generator.next_emission.departure_time_ns = candidate;
                    }
                    (None, None)
                };
            if remaining == 0 {
                for successor in &mut state.generators {
                    if let FlowGeneratorKind::Collective(mut stage) = successor.kind {
                        if stage.local_predecessor == Some(packet.flow)
                            && !stage.local_predecessor_complete
                        {
                            progress_causes.push(PendingCollectiveProgress {
                                flow: successor.flow,
                                cause: crate::CollectiveActivationCause::LocalCompletion,
                                cause_flow: packet.flow,
                                arrival_bytes: 0,
                                before_local_complete: stage.local_predecessor_complete,
                                before_inbound_complete: stage.inbound_predecessor_complete,
                                before_inbound_bytes: stage.inbound_bytes_received,
                            });
                            stage.local_predecessor_complete = true;
                            successor.kind = FlowGeneratorKind::Collective(stage);
                        }
                    }
                }
            }
            state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let schedule_ready = state.in_service.is_none() && !state.tx_ready_pending;
            if schedule_ready {
                state.tx_ready_pending = true;
            }
            (next_packet, next_time, schedule_ready)
        };

        self.enqueue_source_packet(node, packet.id)?;
        self.record_sourced(node.id, packet)?;
        if let Some(next_packet) = next_packet {
            self.insert_generated_packet(
                next_packet,
                Some(next_time.expect("next packet has a time")),
            )?;
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacketArrival,
                    payload: next_packet.id,
                    time_ns: next_time.expect("next packet has a time"),
                },
                children,
            )?;
        }
        self.activate_ready_collectives(node, event, progress_causes, children)?;
        if schedule_ready {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::TxReady,
                    payload: packet.id,
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
        mut causes: Vec<PendingCollectiveProgress>,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let stop_time_ns = self.image.stop_time_ns;
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let mut ordinal = 0_u64;
        loop {
            let activation = {
                let state = self.host_state_mut(node)?;
                let Some(generator_index) = state.generators.iter().position(|generator| {
                    generator.next_emission.status == GeneratorStatus::Blocked
                        && matches!(generator.kind, FlowGeneratorKind::Collective(stage)
                            if stage.prerequisites_complete())
                }) else {
                    break;
                };
                let flow = state.generators[generator_index].flow;
                let Some(cause_index) = causes.iter().position(|cause| cause.flow == flow) else {
                    return Err(ExecutionError::UnexpectedGeneratorEmission {
                        node: node.id,
                        flow,
                        payload: parent.payload,
                    });
                };
                let cause = causes.remove(cause_index);
                let FlowGeneratorKind::Collective(collective) =
                    state.generators[generator_index].kind
                else {
                    unreachable!("position selected a collective generator")
                };
                let first_payload =
                    allocate_payload_id(node.id, node_count, state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                state.next_payload_seq = state
                    .next_payload_seq
                    .checked_add(1)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                let first_size = collective.packet_size_bytes.min(collective.chunk_bytes);
                let generator = &mut state.generators[generator_index];
                generator.packets_emitted = generator
                    .packets_emitted
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                generator.bytes_emitted = generator
                    .bytes_emitted
                    .checked_add(first_size)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                let remaining = collective.chunk_bytes - generator.bytes_emitted;
                let candidate = (remaining != 0)
                    .then(|| {
                        parent
                            .key
                            .time_ns
                            .checked_add(collective.interval_ns)
                            .ok_or(ExecutionError::GeneratorTimeOverflow(flow))
                    })
                    .transpose()?;
                let (next_packet, next_time) = if let Some(next_time) =
                    candidate.filter(|time| *time <= stop_time_ns)
                {
                    let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    state.next_payload_seq = state
                        .next_payload_seq
                        .checked_add(1)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    state.generators[generator_index].next_emission = crate::ScheduledEmission {
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: next_time,
                        payload,
                    };
                    (
                        Some(PacketDescriptor {
                            id: payload,
                            flow,
                            size_bytes: collective.packet_size_bytes.min(remaining),
                            ecn_marked: false,
                            kind: PacketKind::Data,
                        }),
                        Some(next_time),
                    )
                } else {
                    state.generators[generator_index].next_emission = crate::ScheduledEmission {
                        status: if remaining == 0 {
                            GeneratorStatus::Finished
                        } else {
                            GeneratorStatus::Stopped
                        },
                        departure_time_ns: candidate.unwrap_or(parent.key.time_ns),
                        payload: first_payload,
                    };
                    (None, None)
                };
                if remaining == 0 {
                    for successor in &mut state.generators {
                        if let FlowGeneratorKind::Collective(mut stage) = successor.kind {
                            if stage.local_predecessor == Some(flow)
                                && !stage.local_predecessor_complete
                            {
                                causes.push(PendingCollectiveProgress {
                                    flow: successor.flow,
                                    cause: crate::CollectiveActivationCause::LocalCompletion,
                                    cause_flow: flow,
                                    arrival_bytes: 0,
                                    before_local_complete: stage.local_predecessor_complete,
                                    before_inbound_complete: stage.inbound_predecessor_complete,
                                    before_inbound_bytes: stage.inbound_bytes_received,
                                });
                                stage.local_predecessor_complete = true;
                                successor.kind = FlowGeneratorKind::Collective(stage);
                            }
                        }
                    }
                }
                state.sourced_packets = state
                    .sourced_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                let schedule_ready = state.in_service.is_none() && !state.tx_ready_pending;
                if schedule_ready {
                    state.tx_ready_pending = true;
                }
                let generator = &state.generators[generator_index];
                let FlowGeneratorKind::Collective(after_collective) = generator.kind else {
                    unreachable!("collective activation retains its generator kind")
                };
                let transition = collective_progress_record(
                    CollectiveProgressContext {
                        key: parent.key,
                        ordinal,
                        node: node.id,
                        stop_time_ns,
                        activated: true,
                    },
                    cause,
                    after_collective,
                    generator,
                );
                ordinal = ordinal
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                (
                    PacketDescriptor {
                        id: first_payload,
                        flow,
                        size_bytes: first_size,
                        ecn_marked: false,
                        kind: PacketKind::Data,
                    },
                    next_packet,
                    next_time,
                    schedule_ready,
                    transition,
                )
            };
            let (first_packet, next_packet, next_time, schedule_ready, transition) = activation;
            if self.observation_mode == ObservationMode::Full {
                self.mechanism_transitions
                    .push(crate::MechanismTransitionRecord::Collective(transition));
            }
            self.insert_generated_packet(first_packet, Some(parent.key.time_ns))?;
            self.enqueue_source_packet(node, first_packet.id)?;
            self.record_sourced(node.id, first_packet)?;
            if let Some(next_packet) = next_packet {
                self.insert_generated_packet(
                    next_packet,
                    Some(next_time.expect("next packet has a time")),
                )?;
                self.emit_from_host(
                    node,
                    parent,
                    ChildEmission {
                        target: node.id,
                        kind: EventKind::PacketArrival,
                        payload: next_packet.id,
                        time_ns: next_time.expect("next packet has a time"),
                    },
                    children,
                )?;
            }
            if schedule_ready {
                self.emit_from_host(
                    node,
                    parent,
                    ChildEmission {
                        target: node.id,
                        kind: EventKind::TxReady,
                        payload: first_packet.id,
                        time_ns: parent.key.time_ns,
                    },
                    children,
                )?;
            }
        }
        if self.observation_mode == ObservationMode::Full {
            let transitions = {
                let state = self.host_state(node)?;
                let mut transitions = Vec::with_capacity(causes.len());
                for cause in causes {
                    let generator = state
                        .generators
                        .iter()
                        .find(|generator| generator.flow == cause.flow)
                        .ok_or(ExecutionError::UnknownGenerator {
                            node: node.id,
                            flow: cause.flow,
                        })?;
                    let FlowGeneratorKind::Collective(collective) = generator.kind else {
                        return Err(ExecutionError::UnexpectedGeneratorEmission {
                            node: node.id,
                            flow: cause.flow,
                            payload: parent.payload,
                        });
                    };
                    transitions.push(collective_progress_record(
                        CollectiveProgressContext {
                            key: parent.key,
                            ordinal,
                            node: node.id,
                            stop_time_ns,
                            activated: false,
                        },
                        cause,
                        collective,
                        generator,
                    ));
                    ordinal = ordinal
                        .checked_add(1)
                        .ok_or(ExecutionError::CounterOverflow(node.id))?;
                }
                transitions
            };
            self.mechanism_transitions.extend(
                transitions
                    .into_iter()
                    .map(crate::MechanismTransitionRecord::Collective),
            );
        }
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
            let state = self.host_state_mut(node)?;
            let generator = state
                .generators
                .iter_mut()
                .find(|generator| generator.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
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
            state.sourced_packets = state
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

            let Some(payload) = state.queue.pop_front() else {
                return Ok(());
            };
            state.in_service = Some(payload);
            (state.egress_link, payload)
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

            if !state.queue.is_empty() && !state.tx_ready_pending {
                state.tx_ready_pending = true;
                true
            } else {
                false
            }
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
        let egress_link = self.packet_egress_at(event.payload, node.id)?;
        let incoming_link = self.packet_incoming_link_at(event.payload, node.id)?;
        let priority = usize::from(self.flow(packet.flow)?.priority);
        let rate_bps = egress_link
            .map(|link| self.link(link).map(|descriptor| descriptor.rate_bps))
            .transpose()?;
        let sp_position = self.switch_sp_insertion_position(node, egress_link, packet.flow)?;
        let (queue_id, queue_bytes) = {
            let state = self.switch_state(node)?;
            let (queue_id, queue) = state
                .queues
                .iter()
                .enumerate()
                .find(|(_, queue)| queue.egress_link == egress_link)
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link,
                })?;
            let queue_bytes = queue.queue.iter().try_fold(0_u64, |total, payload| {
                total
                    .checked_add(self.packet(*payload)?.size_bytes)
                    .ok_or(ExecutionError::CounterOverflow(node.id))
            })?;
            (u64::try_from(queue_id).unwrap_or(u64::MAX), queue_bytes)
        };

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
                let before = queue.drop_mark;
                let action = drop_mark_decision(
                    &mut queue.drop_mark,
                    queue.queue_capacity_packets,
                    queue_len,
                    queue_bytes,
                    packet.size_bytes,
                    node.id,
                )?;
                let action = if action == QueueAdmissionAction::Mark && !packet.kind.is_data() {
                    QueueAdmissionAction::Enqueue
                } else {
                    action
                };
                let transition = (before != crate::DropMarkPolicy::TailDrop).then_some((
                    before,
                    queue.drop_mark,
                    action,
                    queue_len,
                ));
                (action, transition)
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

        if mark_packet {
            self.set_packet_marked(event.payload)?;
            packet.ecn_marked = true;
        }

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.extend(pfc_transition);
            if let Some((before, after, action, queued_packets_before)) = aqm_transition {
                self.aqm_transitions.push(AqmTransitionRecord {
                    key: event.key,
                    node: node.id,
                    queue_id,
                    payload: event.payload,
                    queued_packets_before,
                    queued_bytes_before: queue_bytes,
                    packet_size_bytes: packet.size_bytes,
                    ecn_before: packet_ecn_before,
                    ecn_after: packet.ecn_marked,
                    before,
                    after,
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

    fn switch_pfc_remote_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        header: crate::PfcHeader,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let priority = usize::from(header.priority);
        let queued = {
            let state = self.switch_state(node)?;
            let queue = state
                .queues
                .iter()
                .find(|queue| queue.egress_link == Some(header.controlled_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(header.controlled_link),
                })?;
            queue
                .queue
                .iter()
                .map(|payload| {
                    let packet = self.packet(*payload)?;
                    Ok((*payload, usize::from(self.flow(packet.flow)?.priority)))
                })
                .collect::<Result<Vec<_>, ExecutionError>>()?
        };
        let (schedule_payload, transition) = {
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
            let schedule_payload = if header.pause {
                pfc.paused_by_controller[priority].insert(event.key.origin_node);
                None
            } else if !pfc.paused_by_controller[priority].remove(&event.key.origin_node) {
                // Duplicate/early resume is an idempotent no-op.
                None
            } else if pfc.is_paused(priority) {
                // Another controller still owns the aggregate pause.
                None
            } else {
                debug_assert!(was_paused);
                let payload = queued.iter().find_map(|(payload, packet_priority)| {
                    (!pfc.is_paused(*packet_priority)).then_some(*payload)
                });
                if payload.is_some() && queue.in_service.is_none() && !queue.tx_ready_pending {
                    queue.tx_ready_pending = true;
                    payload
                } else {
                    None
                }
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
            (schedule_payload, transition)
        };
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
        if let PacketKind::TcpData(header) = packet.kind {
            return self.host_tcp_data_arrival(node, event, packet, header, children);
        }
        if let PacketKind::TcpAck(header) = packet.kind {
            return self.host_tcp_ack_arrival(node, event, packet, header, children);
        }
        if let PacketKind::DcqcnCnp(header) = packet.kind {
            return self.host_dcqcn_cnp_arrival(node, event, packet, header);
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
        let (disposition, feedback_action, collective_progress_causes) = {
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
                    Vec::new(),
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
                let mut collective_progress_causes = Vec::new();
                for generator in &mut state.generators {
                    let FlowGeneratorKind::Collective(mut collective) = generator.kind else {
                        continue;
                    };
                    if collective.inbound_predecessor != Some(packet.flow)
                        || collective.inbound_predecessor_complete
                    {
                        continue;
                    }
                    let before_inbound_bytes = collective.inbound_bytes_received;
                    collective_progress_causes.push(PendingCollectiveProgress {
                        flow: generator.flow,
                        cause: crate::CollectiveActivationCause::InboundArrival,
                        cause_flow: packet.flow,
                        arrival_bytes: packet.size_bytes,
                        before_local_complete: collective.local_predecessor_complete,
                        before_inbound_complete: collective.inbound_predecessor_complete,
                        before_inbound_bytes,
                    });
                    collective.inbound_bytes_received = collective
                        .inbound_bytes_received
                        .checked_add(packet.size_bytes)
                        .ok_or(ExecutionError::CounterOverflow(node.id))?;
                    if collective.inbound_bytes_received > collective.inbound_predecessor_bytes {
                        return Err(ExecutionError::CounterOverflow(node.id));
                    }
                    if collective.inbound_bytes_received == collective.inbound_predecessor_bytes {
                        collective.inbound_predecessor_complete = true;
                    }
                    generator.kind = FlowGeneratorKind::Collective(collective);
                }
                (
                    ArrivalDisposition::Delivered,
                    GeneratorFeedbackAction::None,
                    collective_progress_causes,
                )
            }
        };
        self.record_arrival(node.id, packet, event.key, disposition)?;
        self.mark_terminal(event.payload)?;
        if !collective_progress_causes.is_empty() {
            self.activate_ready_collectives(node, event, collective_progress_causes, children)?;
        }
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
            let interval_open = receiver.last_cnp_time_ns.is_none_or(|last| {
                last.checked_add(receiver.cnp_interval_ns)
                    .is_some_and(|earliest| event.key.time_ns >= earliest)
            });
            if packet.ecn_codepoint() != crate::EcnCodepoint::Ce || !interval_open {
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
                receiver.last_cnp_time_ns = Some(event.key.time_ns);
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
            let state = self.host_state_mut(node)?;
            let generator = state
                .generators
                .iter_mut()
                .find(|generator| generator.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
            generator.feedback.arrivals = generator
                .feedback
                .arrivals
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let FlowGeneratorKind::Dcqcn(mut dcqcn) = generator.kind else {
                return Err(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                });
            };
            let before = dcqcn.controller;
            let applied = dcqcn
                .controller
                .on_cnp(event.key.time_ns)
                .map_err(|_| ExecutionError::CounterOverflow(node.id))?;
            dcqcn.rate.rate_numerator_bits_per_second = dcqcn.controller.current_rate_bps;
            let transition = crate::DcqcnTransitionRecord {
                key: event.key,
                node: node.id,
                flow: packet.flow,
                kind: crate::DcqcnTransitionKind::Cnp,
                applied,
                emitted_bytes: 0,
                before,
                after: dcqcn.controller,
            };
            generator.kind = FlowGeneratorKind::Dcqcn(dcqcn);
            transition
        };
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions
                .push(crate::MechanismTransitionRecord::Dcqcn(transition));
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
        let (ack_payload, acknowledgment, ack_size_bytes) = {
            let state = self.host_state_mut(node)?;
            let receiver = state
                .tcp_receivers
                .iter_mut()
                .find(|receiver| receiver.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
            let end = header
                .sequence
                .checked_add(packet.size_bytes)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            tcp_receive_range(receiver, header.sequence, end);
            let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            state.next_payload_seq = state
                .next_payload_seq
                .checked_add(1)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            state.received_packets = state
                .received_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            (
                payload,
                receiver.next_expected_sequence,
                receiver.ack_size_bytes,
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
            let state = self.host_state_mut(node)?;
            if state.in_service.is_none() && !state.tx_ready_pending {
                state.tx_ready_pending = true;
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
        ) = {
            let state = self.host_state_mut(node)?;
            let generator = state
                .generators
                .iter_mut()
                .find(|generator| generator.flow == packet.flow)
                .ok_or(ExecutionError::UnknownGenerator {
                    node: node.id,
                    flow: packet.flow,
                })?;
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
                tcp.active_timer = None;
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
                    tcp.active_timer = None;
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
            (
                retransmit,
                fill,
                acknowledged_through,
                transition,
                scheduled_send_pending,
            )
        };
        if let Some(acknowledgment) = acknowledged_through {
            acknowledge_tcp_segments(&mut self.tcp_sent_segments, packet.flow, acknowledgment)?;
        }
        let sender_transition = transition.is_some();
        if self.observation_mode == ObservationMode::Full {
            self.tcp_transitions.extend(transition);
        }
        if !sender_transition {
            return Ok(());
        }
        // A Scheduled TCP descriptor already reserves next_sequence and owns its pending
        // PacketArrival. ACKs may update the sender and ledger, and loss recovery may retransmit
        // an older sequence, but fresh window fill must wait for the reserved event.
        if scheduled_send_pending && retransmit_sequence.is_none() {
            return Ok(());
        }
        let plan = self.prepare_tcp_attempts(
            node,
            packet.flow,
            event.key.time_ns,
            retransmit_sequence,
            fill_window && !scheduled_send_pending,
            scheduled_send_pending,
        )?;
        self.install_tcp_attempts(node, event, plan, children)
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
        let Some((flow, sequence, transition)) = timed_out else {
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
        if packet.kind == PacketKind::DcqcnControlTimer {
            return self.host_dcqcn_control_timer(node, event, packet, children);
        }
        if self.host_state(node)?.generators.iter().any(|generator| {
            generator.flow == packet.flow && matches!(generator.kind, FlowGeneratorKind::Dcqcn(_))
        }) {
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
            let state = self.host_state_mut(node)?;
            let Some(generator_index) = state
                .generators
                .iter()
                .position(|generator| generator.flow == packet.flow)
            else {
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

            let scale = u128::from(rate.rate_denominator)
                .checked_mul(1_000_000_000)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let tick_credit = u128::from(rate.rate_numerator_bits_per_second)
                .checked_mul(u128::from(rate.pacing_interval_ns))
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let packet_cost = u128::from(packet.size_bytes)
                .checked_mul(8)
                .and_then(|bits| bits.checked_mul(scale))
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
                state.sourced_packets = state
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
                    let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    state.next_payload_seq = state
                        .next_payload_seq
                        .checked_add(1)
                        .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                    let remaining = rate.total_bytes - generator.bytes_emitted;
                    (payload, rate.packet_size_bytes.min(remaining))
                } else {
                    (packet.id, packet.size_bytes)
                };
                let next_cost = u128::from(size_bytes)
                    .checked_mul(8)
                    .and_then(|bits| bits.checked_mul(scale))
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
            self.insert_generated_packet(next_packet, None)?;
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
        let mut byte_transition = None;

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

            let scale = u128::from(dcqcn.rate.rate_denominator)
                .checked_mul(1_000_000_000)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let tick_credit = u128::from(dcqcn.controller.current_rate_bps)
                .checked_mul(u128::from(dcqcn.rate.pacing_interval_ns))
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            let packet_cost = u128::from(packet.size_bytes)
                .checked_mul(8)
                .and_then(|bits| bits.checked_mul(scale))
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
                let before = dcqcn.controller;
                let applied = dcqcn
                    .controller
                    .on_bytes_emitted(packet.size_bytes)
                    .map_err(|_| ExecutionError::CounterOverflow(node.id))?;
                byte_transition = Some(crate::DcqcnTransitionRecord {
                    key: event.key,
                    node: node.id,
                    flow: packet.flow,
                    kind: crate::DcqcnTransitionKind::Bytes,
                    applied,
                    emitted_bytes: packet.size_bytes,
                    before,
                    after: dcqcn.controller,
                });
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
                let next_tick_credit = u128::from(dcqcn.controller.current_rate_bps)
                    .checked_mul(u128::from(dcqcn.rate.pacing_interval_ns))
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                let next_cost = u128::from(size_bytes)
                    .checked_mul(8)
                    .and_then(|bits| bits.checked_mul(scale))
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
                if let Some(candidate_time) = candidate_time {
                    generator.next_emission.departure_time_ns = candidate_time;
                }
                terminal_unused_token = !emitted;
            }
            generator.kind = FlowGeneratorKind::Dcqcn(dcqcn);
        }

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions
                .extend(byte_transition.map(crate::MechanismTransitionRecord::Dcqcn));
        }
        if emitted {
            self.set_source_time(packet.id, event.key.time_ns)?;
            self.enqueue_source_packet(node, packet.id)?;
            self.record_sourced(node.id, packet)?;
        } else if terminal_unused_token {
            self.mark_terminal(packet.id)?;
        }
        if let Some(packet) = next_packet {
            self.insert_generated_packet(packet, None)?;
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

    fn host_dcqcn_control_timer(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        packet: PacketDescriptor,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let stop_time_ns = self.image.stop_time_ns;
        let (transition, next_time_ns) = {
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
            if dcqcn.control_timer_payload != packet.id
                || dcqcn.controller.next_control_time_ns != event.key.time_ns
            {
                return Ok(());
            }
            let before = dcqcn.controller;
            let applied = dcqcn
                .controller
                .on_control_timer(event.key.time_ns)
                .map_err(|_| ExecutionError::CounterOverflow(node.id))?;
            dcqcn.rate.rate_numerator_bits_per_second = dcqcn.controller.current_rate_bps;
            let next_time_ns = (dcqcn.controller.next_control_time_ns <= stop_time_ns)
                .then_some(dcqcn.controller.next_control_time_ns);
            let transition = crate::DcqcnTransitionRecord {
                key: event.key,
                node: node.id,
                flow: packet.flow,
                kind: crate::DcqcnTransitionKind::Control,
                applied,
                emitted_bytes: 0,
                before,
                after: dcqcn.controller,
            };
            generator.kind = FlowGeneratorKind::Dcqcn(dcqcn);
            (transition, next_time_ns)
        };
        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions
                .push(crate::MechanismTransitionRecord::Dcqcn(transition));
        }
        if let Some(time_ns) = next_time_ns {
            self.emit_from_host(
                node,
                event,
                ChildEmission {
                    target: node.id,
                    kind: EventKind::PacingTimer,
                    payload: packet.id,
                    time_ns,
                },
                children,
            )?;
        } else {
            self.mark_terminal(packet.id)?;
        }
        Ok(())
    }

    fn switch_tx_ready(
        &mut self,
        node: NodeDescriptor,
        event: Event,
        children: &mut Vec<Event>,
    ) -> Result<(), ExecutionError> {
        let egress_link = self.packet_egress_at(event.payload, node.id)?;
        let Some(egress_link) = egress_link else {
            return Err(ExecutionError::MissingSwitchQueue {
                node: node.id,
                egress_link: None,
            });
        };

        let (eligible_positions, eligible_packets, eligible_priorities, eligible_incoming_links) = {
            let state = self.switch_state(node)?;
            let queue = state
                .queues
                .iter()
                .find(|queue| queue.egress_link == Some(egress_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(egress_link),
                })?;
            let mut positions = Vec::new();
            let mut packets = Vec::new();
            let mut priorities = Vec::new();
            let mut incoming_links = Vec::new();
            for (position, payload) in queue.queue.iter().enumerate() {
                let packet = self.packet(*payload)?;
                let priority = usize::from(self.flow(packet.flow)?.priority);
                let paused = queue
                    .pfc
                    .as_ref()
                    .is_some_and(|pfc| pfc.is_paused(priority));
                if !paused {
                    positions.push(position);
                    packets.push(packet);
                    priorities.push(priority);
                    incoming_links.push(self.packet_incoming_link_at(*payload, node.id)?);
                }
            }
            (positions, packets, priorities, incoming_links)
        };
        let (payload, pfc_plan, pfc_transition, scheduler_transition) = {
            let state = self.switch_state_mut(node)?;
            let (queue_id, queue) = state
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

            let scheduler_before = queue.scheduler.clone();
            let Some((eligible_position, scan_steps)) =
                scheduler_select_position(&mut queue.scheduler, &eligible_packets, node.id)?
            else {
                return Ok(());
            };
            let position = eligible_positions[eligible_position];
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
            let packet = eligible_packets[eligible_position];
            let priority = eligible_priorities[eligible_position];
            let incoming_link = eligible_incoming_links[eligible_position];
            let queue_id = u64::try_from(queue_id).unwrap_or(u64::MAX);
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
            let scheduler_packets = eligible_packets
                .iter()
                .map(|packet| crate::SchedulerPacket {
                    payload: packet.id,
                    flow: packet.flow,
                    size_bytes: packet.size_bytes,
                })
                .collect::<Vec<_>>();
            let scheduler_transition = match (&scheduler_before, &queue.scheduler) {
                (
                    SchedulerKind::DeficitRoundRobin(before),
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
                        eligible_packets: scheduler_packets,
                        selected_payload: payload,
                        after_deficits_bytes: after.deficits_bytes.clone(),
                        after_current_class: after.current_class,
                    },
                )),
                (
                    SchedulerKind::WeightedRoundRobin(before),
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
                        eligible_packets: scheduler_packets,
                        selected_payload: payload,
                        after_packets_sent: after.packets_sent_in_round.clone(),
                        after_current_class: after.current_class,
                    },
                )),
                _ => None,
            };
            (payload, pfc_plan, pfc_transition, scheduler_transition)
        };

        if self.observation_mode == ObservationMode::Full {
            self.mechanism_transitions.extend(pfc_transition);
            self.mechanism_transitions.extend(scheduler_transition);
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
            link.arrival_time_ns(event.key.time_ns, self.packet_size(payload)?)?;
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
                target: self.packet_remote_target(payload, link.id)?,
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
        let egress_link = self.packet_egress_at(event.payload, node.id)?;
        let Some(egress_link) = egress_link else {
            return Err(ExecutionError::MissingSwitchQueue {
                node: node.id,
                egress_link: None,
            });
        };
        let rate_bps = self.link(egress_link)?.rate_bps;
        let packet = self.packet(event.payload)?;

        let eligible_next_payload = {
            let state = self.switch_state(node)?;
            let queue = state
                .queues
                .iter()
                .find(|queue| queue.egress_link == Some(egress_link))
                .ok_or(ExecutionError::MissingSwitchQueue {
                    node: node.id,
                    egress_link: Some(egress_link),
                })?;
            queue.queue.iter().find_map(|payload| {
                let packet = self.packet(*payload).ok()?;
                let priority = usize::from(self.flow(packet.flow).ok()?.priority);
                let paused = queue
                    .pfc
                    .as_ref()
                    .is_some_and(|pfc| pfc.is_paused(priority));
                (!paused).then_some(*payload)
            })
        };
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
        self.host_states
            .get_mut(state_slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })
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
        self.host_states
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

    fn link(&self, id: LinkId) -> Result<crate::LinkDescriptor, ExecutionError> {
        indexed_lookup(&self.image.links, id.0, |link| link.id == id)
            .copied()
            .ok_or(ExecutionError::UnknownLink(id))
    }

    fn packet_size(&self, id: PayloadId) -> Result<u64, ExecutionError> {
        Ok(self.packet(id)?.size_bytes)
    }

    fn packet(&self, id: PayloadId) -> Result<PacketDescriptor, ExecutionError> {
        #[cfg(feature = "p11-profile")]
        let lookup_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Lookup);
        let packet = self
            .packets
            .get(&id)
            .map(|packet| packet.descriptor)
            .ok_or(ExecutionError::UnknownPacket(id));
        #[cfg(feature = "p11-profile")]
        drop(lookup_guard);
        packet
    }

    fn flow(&self, id: FlowId) -> Result<&crate::FlowDescriptor, ExecutionError> {
        indexed_lookup(&self.image.flows, id.0, |flow| flow.id == id)
            .ok_or(ExecutionError::UnknownFlow(id))
    }

    fn packet_egress_at(
        &self,
        payload: PayloadId,
        node: NodeId,
    ) -> Result<Option<LinkId>, ExecutionError> {
        let packet = self.packet(payload)?;
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
        payload: PayloadId,
        node: NodeId,
    ) -> Result<Option<LinkId>, ExecutionError> {
        let packet = self.packet(payload)?;
        let flow = self.flow(packet.flow)?;
        let route = if packet.kind.is_data() {
            &flow.route
        } else {
            &flow.reverse_route
        };
        for link_id in route {
            if self.packet_remote_target(payload, *link_id)? == node {
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
        self.insert_packet_profiled(packet, source_time_ns, false)
    }

    fn insert_generated_packet(
        &mut self,
        packet: PacketDescriptor,
        source_time_ns: Option<u64>,
    ) -> Result<(), ExecutionError> {
        self.insert_packet_profiled(packet, source_time_ns, true)
    }

    fn insert_packet_profiled(
        &mut self,
        packet: PacketDescriptor,
        source_time_ns: Option<u64>,
        generator: bool,
    ) -> Result<(), ExecutionError> {
        #[cfg(not(feature = "p11-profile"))]
        let _ = generator;
        #[cfg(feature = "p11-profile")]
        let lookup_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Lookup);
        let duplicate = self.packets.contains_key(&packet.id);
        #[cfg(feature = "p11-profile")]
        drop(lookup_guard);
        if duplicate {
            return Err(ExecutionError::DuplicatePayload(packet.id));
        }
        #[cfg(feature = "p11-profile")]
        let insert_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Insert { generator });
        self.packets.insert(
            packet.id,
            ResidentPacket {
                descriptor: packet,
                source_time_ns,
                transmitters: 0,
                terminal: false,
            },
        );
        #[cfg(feature = "p11-profile")]
        drop(insert_guard);
        Ok(())
    }

    fn resident_packet(&self, payload: PayloadId) -> Option<ResidentPacket> {
        #[cfg(feature = "p11-profile")]
        let lookup_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Lookup);
        let packet = self.packets.get(&payload).copied();
        #[cfg(feature = "p11-profile")]
        drop(lookup_guard);
        packet
    }

    fn resident_packet_mut(
        &mut self,
        payload: PayloadId,
    ) -> Result<&mut ResidentPacket, ExecutionError> {
        #[cfg(feature = "p11-profile")]
        let lookup_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Lookup);
        let packet = self
            .packets
            .get_mut(&payload)
            .ok_or(ExecutionError::UnknownPacket(payload));
        #[cfg(feature = "p11-profile")]
        drop(lookup_guard);
        packet
    }

    fn remove_resident_packet(&mut self, payload: PayloadId) {
        #[cfg(feature = "p11-profile")]
        let remove_guard = self
            .p11_packet_store
            .measure(P11PacketStoreOperation::Remove);
        self.packets.remove(&payload);
        #[cfg(feature = "p11-profile")]
        drop(remove_guard);
    }

    fn set_source_time(&mut self, payload: PayloadId, time_ns: u64) -> Result<(), ExecutionError> {
        let packet = self.resident_packet_mut(payload)?;
        packet.source_time_ns = Some(time_ns);
        Ok(())
    }

    fn set_packet_marked(&mut self, payload: PayloadId) -> Result<(), ExecutionError> {
        let packet = self.resident_packet_mut(payload)?;
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
            .resident_packet(payload)
            .and_then(|resident| resident.source_time_ns)
            .ok_or(ExecutionError::UnknownPacket(payload))?;
        let slot = self.local_state_slot(node)?;
        let position = self
            .host_states
            .get(slot)
            .ok_or(ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            })?
            .queue
            .iter()
            .rposition(|queued| {
                self.resident_packet(*queued).is_some_and(|resident| {
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
        self.insert_generated_packet(packet, Some(parent.key.time_ns))?;
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
        let state = self.host_state_mut(node)?;
        let generator_index = state
            .generators
            .iter()
            .position(|generator| generator.flow == flow)
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
            let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
            state.next_payload_seq = state
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
                let payload = allocate_payload_id(node.id, node_count, state.next_payload_seq)
                    .ok_or(ExecutionError::PayloadSequenceOverflow(node.id))?;
                state.next_payload_seq = state
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
        state.sourced_packets = state
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
            self.insert_generated_packet(packet, Some(parent.key.time_ns))?;
            self.enqueue_source_packet(node, packet.id)?;
            self.record_sourced(node.id, packet)?;
        }
        let ready_payload = {
            let state = self.host_state_mut(node)?;
            if state.in_service.is_none() && !state.tx_ready_pending {
                let ready = state.queue.front().copied();
                if ready.is_some() {
                    state.tx_ready_pending = true;
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
            self.departures.push((
                event_key,
                PacketDeparture {
                    payload: packet.id,
                    time_ns: event_key.time_ns,
                },
            ));
        }
        Ok(())
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
            self.arrivals.push((
                event_key,
                PacketArrivalObservation {
                    payload: packet.id,
                    time_ns: event_key.time_ns,
                    disposition,
                },
            ));
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
        let packet = self.resident_packet_mut(payload)?;
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
                let local_node = self.local_node;
                let packet = self.resident_packet_mut(payload)?;
                packet.transmitters = packet.transmitters.checked_sub(1).ok_or(
                    ExecutionError::UnexpectedTxComplete {
                        node,
                        expected: None,
                        actual: payload,
                    },
                )?;
                packet.transmitters == 0 && (packet.terminal || local_node.is_some())
            };
        if remove {
            self.remove_resident_packet(payload);
        }
        Ok(())
    }

    fn mark_terminal(&mut self, payload: PayloadId) -> Result<(), ExecutionError> {
        let remove = {
            let packet = self.resident_packet_mut(payload)?;
            packet.terminal = true;
            packet.transmitters == 0
        };
        if remove {
            self.remove_resident_packet(payload);
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

fn drop_mark_decision(
    policy: &mut crate::DropMarkPolicy,
    taildrop_capacity_packets: u64,
    queued_packets: u64,
    queued_bytes: u64,
    packet_size_bytes: u64,
    node: NodeId,
) -> Result<QueueAdmissionAction, ExecutionError> {
    let post_packets = queued_packets
        .checked_add(1)
        .ok_or(ExecutionError::CounterOverflow(node))?;
    let post_bytes = queued_bytes.checked_add(packet_size_bytes);
    match policy {
        crate::DropMarkPolicy::TailDrop => Ok(
            if post_bytes.is_none()
                || taildrop_capacity_packets != 0 && post_packets > taildrop_capacity_packets
            {
                QueueAdmissionAction::Drop
            } else {
                QueueAdmissionAction::Enqueue
            },
        ),
        crate::DropMarkPolicy::EcnThreshold(config) => {
            let Some(post_bytes) = post_bytes else {
                return Ok(QueueAdmissionAction::Drop);
            };
            let post_depth = match config.unit {
                crate::QueueDepthUnit::Packets => post_packets,
                crate::QueueDepthUnit::Bytes => post_bytes,
            };
            Ok(if config.capacity != 0 && post_depth > config.capacity {
                QueueAdmissionAction::Drop
            } else if post_depth >= config.threshold {
                QueueAdmissionAction::Mark
            } else {
                QueueAdmissionAction::Enqueue
            })
        }
        crate::DropMarkPolicy::Red(state) => {
            // Let S=2^32, A' = floor((511*A + sample*S)/512), and
            // p(A') = p_num*(A'-min*S)/(p_den*(max-min)*S). In the open threshold
            // interval, increment c and signal exactly when c*p(A') >= 1, then reset c.
            // At/below min resets without signaling; at/above max signals and resets.
            const RED_AVERAGE_SCALE: u128 = 1_u128 << 32;
            let sample_depth = match state.unit {
                crate::QueueDepthUnit::Packets => queued_packets,
                crate::QueueDepthUnit::Bytes => queued_bytes,
            };
            let post_depth = match state.unit {
                crate::QueueDepthUnit::Packets => Some(post_packets),
                crate::QueueDepthUnit::Bytes => post_bytes,
            };
            let weighted_previous = state
                .average_scaled
                .checked_mul(511)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
            let weighted_sample = u128::from(sample_depth)
                .checked_mul(RED_AVERAGE_SCALE)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
            state.average_scaled = weighted_previous
                .checked_add(weighted_sample)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?
                / 512;

            if post_bytes.is_none()
                || post_depth.is_none_or(|depth| state.capacity != 0 && depth > state.capacity)
            {
                return Ok(QueueAdmissionAction::Drop);
            }
            let min_scaled = u128::from(state.min_threshold)
                .checked_mul(RED_AVERAGE_SCALE)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
            let max_scaled = u128::from(state.max_threshold)
                .checked_mul(RED_AVERAGE_SCALE)
                .ok_or(ExecutionError::InvalidSchedulerState(node))?;
            let signal = if state.average_scaled <= min_scaled {
                state.counter = 0;
                false
            } else if state.average_scaled >= max_scaled {
                state.counter = 0;
                true
            } else {
                state.counter = state
                    .counter
                    .checked_add(1)
                    .ok_or(ExecutionError::InvalidSchedulerState(node))?;
                let left = BigUint::from(state.counter)
                    * BigUint::from(state.max_probability_numerator)
                    * BigUint::from(state.average_scaled - min_scaled);
                let right = BigUint::from(state.max_probability_denominator)
                    * BigUint::from(state.max_threshold - state.min_threshold)
                    * BigUint::from(RED_AVERAGE_SCALE);
                if left >= right {
                    state.counter = 0;
                    true
                } else {
                    false
                }
            };
            Ok(if signal {
                if state.mark_ecn {
                    QueueAdmissionAction::Mark
                } else {
                    QueueAdmissionAction::Drop
                }
            } else {
                QueueAdmissionAction::Enqueue
            })
        }
    }
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

/// Routes an arriving feedback packet through the source-generator contract hook.
///
/// Constant generators only record the arrival. TCP ACK processing calls this hook before its
/// structured cumulative-ACK transition, then uses the caller's host-owned emission path to
/// refill the congestion window without introducing a backend-specific event kind.
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
        FlowGeneratorKind::Collective(_) => Ok(GeneratorFeedbackAction::None),
        FlowGeneratorKind::Dcqcn(_) => Ok(GeneratorFeedbackAction::None),
    }
}

//! Canonical serial priority-queue execution.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use num_bigint::BigUint;
use num_rational::Ratio;

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
    /// Nonterminal packet data referenced by queues, service slots, or pending events.
    pub resident_packets: Vec<PacketDescriptor>,
    /// Complete packet descriptors referenced by full-mode observations.
    pub observed_packets: Vec<PacketDescriptor>,
    pub departures: Vec<PacketDeparture>,
    pub arrivals: Vec<PacketArrivalObservation>,
    /// Exact TCP control transitions retained in full observation mode.
    pub tcp_transitions: Vec<TcpTransitionRecord>,
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
    observation_mode: ObservationMode,
    summary: RunSummary,
    observed_packets: BTreeMap<PayloadId, PacketDescriptor>,
    departures: Vec<(EventKey, PacketDeparture)>,
    arrivals: Vec<(EventKey, PacketArrivalObservation)>,
    tcp_transitions: Vec<TcpTransitionRecord>,
    tcp_sent_segments: BTreeMap<(FlowId, u64), u64>,
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
    pub observed_packets: Vec<PacketDescriptor>,
    pub departures: Vec<(EventKey, PacketDeparture)>,
    pub arrivals: Vec<(EventKey, PacketArrivalObservation)>,
    pub tcp_transitions: Vec<TcpTransitionRecord>,
}

#[derive(Clone, Copy)]
struct ChildEmission {
    target: NodeId,
    kind: EventKind,
    payload: PayloadId,
    time_ns: u64,
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
        let mut tcp_sent_segments = BTreeMap::new();
        for descriptor in image.initial_packets.iter().copied() {
            seed_tcp_segment(&mut tcp_sent_segments, descriptor)?;
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
            observation_mode,
            summary: RunSummary::default(),
            observed_packets: BTreeMap::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            tcp_transitions: Vec::new(),
            tcp_sent_segments,
        })
    }

    pub(crate) fn new_local(
        image: &'image SimulationImage,
        node: NodeDescriptor,
        packets: impl IntoIterator<Item = PacketDescriptor>,
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
        let mut tcp_sent_segments = BTreeMap::new();
        for descriptor in packets {
            seed_tcp_segment(&mut tcp_sent_segments, descriptor)?;
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
            observation_mode,
            summary: RunSummary::default(),
            observed_packets: BTreeMap::new(),
            departures: Vec::new(),
            arrivals: Vec::new(),
            tcp_transitions: Vec::new(),
            tcp_sent_segments,
        })
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
        let resident_packets = self
            .packets
            .into_values()
            .map(|packet| packet.descriptor)
            .collect();
        let observed_packets = self.observed_packets.into_values().collect();
        self.departures.sort_unstable_by_key(|(key, _)| *key);
        self.arrivals.sort_unstable_by_key(|(key, _)| *key);
        self.tcp_transitions
            .sort_unstable_by_key(|record| record.key);
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
            observed_packets: self.observed_packets.into_values().collect(),
            departures: self.departures,
            arrivals: self.arrivals,
            tcp_transitions: self.tcp_transitions,
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
        let plan = self.prepare_tcp_attempts(node, packet.flow, event.key.time_ns, None, true)?;
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
        let packet = self.packet(event.payload)?;
        let egress_link = self.packet_egress_at(event.payload, node.id)?;
        let rate_bps = egress_link
            .map(|link| self.link(link).map(|descriptor| descriptor.rate_bps))
            .transpose()?;
        let sp_position = self.switch_sp_insertion_position(node, egress_link, packet.flow)?;

        let (disposition, schedule_ready) = {
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
            if queue.queue_capacity_packets != 0 && queue_len >= queue.queue_capacity_packets {
                state.dropped_packets = state
                    .dropped_packets
                    .checked_add(1)
                    .ok_or(ExecutionError::CounterOverflow(node.id))?;
                (ArrivalDisposition::Dropped, false)
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
                }
                let schedule_ready = queue.egress_link.is_some()
                    && queue.in_service.is_none()
                    && !queue.tx_ready_pending;
                if schedule_ready {
                    queue.tx_ready_pending = true;
                }
                (ArrivalDisposition::Admitted, schedule_ready)
            }
        };

        self.record_arrival(node.id, packet, event.key, disposition)?;
        if disposition == ArrivalDisposition::Dropped {
            self.mark_terminal(event.payload)?;
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

        let (retransmit_sequence, fill_window, acknowledged_through, transition) = {
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
            } else if acknowledgment == tcp.highest_ack && tcp.highest_ack < tcp.total_bytes {
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
            let transition = input.map(|input| TcpTransitionRecord {
                key: event.key,
                node: node.id,
                flow: packet.flow,
                mss_bytes: tcp.mss_bytes,
                input,
                before,
                after: tcp.control,
            });
            (retransmit, fill, acknowledged_through, transition)
        };
        if let Some(acknowledgment) = acknowledged_through {
            self.tcp_sent_segments
                .retain(|(segment_flow, sequence), _| {
                    *segment_flow != packet.flow || *sequence >= acknowledgment
                });
        }
        if self.observation_mode == ObservationMode::Full {
            self.tcp_transitions.extend(transition);
        }
        let plan = self.prepare_tcp_attempts(
            node,
            packet.flow,
            event.key.time_ns,
            retransmit_sequence,
            fill_window,
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
            self.prepare_tcp_attempts(node, flow, event.key.time_ns, Some(sequence), false)?;
        self.install_tcp_attempts(node, event, plan, children)
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

        let payload = {
            let state = self.switch_state_mut(node)?;
            let queue = state
                .queues
                .iter_mut()
                .find(|queue| queue.egress_link == Some(egress_link))
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

            let Some(payload) = queue.queue.pop_front() else {
                return Ok(());
            };
            if let SchedulerKind::WeightedFairQueue(wfq) = &mut queue.scheduler {
                wfq.packet_finish_times.get(&payload).ok_or(
                    ExecutionError::MissingWfqFinishTag {
                        node: node.id,
                        payload,
                    },
                )?;
            }
            queue.in_service = Some(payload);
            payload
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

            let next_payload = queue.queue.front().copied();
            let schedule_payload = if next_payload.is_some() && !queue.tx_ready_pending {
                queue.tx_ready_pending = true;
                next_payload
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
    ) -> Result<TcpSendPlan, ExecutionError> {
        let node_count = u64::try_from(self.image.nodes.len()).unwrap_or(u64::MAX);
        let retransmit_segment = retransmit_sequence
            .map(|sequence| {
                self.tcp_sent_segments
                    .get(&(flow, sequence))
                    .copied()
                    .map(|size_bytes| (sequence, size_bytes))
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
                    kind: PacketKind::TcpData(TcpDataHeader {
                        sequence,
                        sent_time_ns: now_ns,
                        retransmission: false,
                    }),
                });
            }
        }

        let timer = if tcp.active_timer.is_none() && tcp.bytes_in_flight != 0 {
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
        generator.next_emission.status = if tcp.highest_ack >= tcp.total_bytes {
            GeneratorStatus::Finished
        } else {
            GeneratorStatus::Blocked
        };
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
            self.insert_packet(packet, Some(parent.key.time_ns))?;
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
            self.observed_packets.entry(packet.id).or_insert(packet);
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
    segments: &mut BTreeMap<(FlowId, u64), u64>,
    packet: PacketDescriptor,
) -> Result<(), ExecutionError> {
    let PacketKind::TcpData(header) = packet.kind else {
        return Ok(());
    };
    if let Some(original_size_bytes) =
        segments.insert((packet.flow, header.sequence), packet.size_bytes)
    {
        if original_size_bytes != packet.size_bytes {
            return Err(ExecutionError::InconsistentTcpSegment {
                flow: packet.flow,
                sequence: header.sequence,
                original_size_bytes,
                replacement_size_bytes: packet.size_bytes,
            });
        }
    }
    Ok(())
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
    }
}

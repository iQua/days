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
    TimeError, TransitionHandler, WfqSchedulerState, event_phase, resolve_transition,
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
}

#[derive(Clone, Copy)]
struct ChildEmission {
    target: NodeId,
    kind: EventKind,
    payload: PayloadId,
    time_ns: u64,
}

impl<'image> TransitionState<'image> {
    pub(crate) fn new(
        image: &'image SimulationImage,
        observation_mode: ObservationMode,
    ) -> Result<Self, ExecutionError> {
        let mut packets = BTreeMap::new();
        for descriptor in image.initial_packets.iter().copied() {
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
            pending_events,
        }
    }

    pub(crate) fn finish_local(mut self) -> LocalTransitionResult {
        let node = self
            .local_node
            .expect("finish_local requires node-local transition state");
        self.departures.sort_unstable_by_key(|(key, _)| *key);
        self.arrivals.sort_unstable_by_key(|(key, _)| *key);
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

                let FlowGeneratorKind::Constant(constant) = generator.kind;
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
        let expected_target = match packet.kind {
            PacketKind::Data => flow_target,
            PacketKind::Feedback => flow_source,
        };
        let (disposition, feedback_action) = {
            let state = self.host_state_mut(node)?;
            let feedback_generator = (packet.kind == PacketKind::Feedback)
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

        let route = match packet.kind {
            PacketKind::Data => &flow.route,
            PacketKind::Feedback => &flow.reverse_route,
        };
        for link_id in route {
            let link = self.link(*link_id)?;
            if link.source == node {
                return Ok(Some(link.id));
            }
        }
        let terminal = match packet.kind {
            PacketKind::Data => flow.target,
            PacketKind::Feedback => flow.source,
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
        let route = match packet.kind {
            PacketKind::Data => &flow.route,
            PacketKind::Feedback => &flow.reverse_route,
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
        Ok(match packet.kind {
            PacketKind::Data => flow.target,
            PacketKind::Feedback => flow.source,
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

/// Routes an ordinary arriving packet into the closed source-generator transition.
///
/// The constant generator records feedback bookkeeping but never emits because of feedback. A
/// future closed-loop variant extends this closed transition and uses the caller's host-owned
/// emission path without adding an event kind.
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
    }
}

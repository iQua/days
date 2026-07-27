//! Canonical serial priority-queue execution.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use crate::{
    Event, EventKey, EventKind, FlowId, HostState, LinkId, NodeDescriptor, NodeId, NodeKind,
    PayloadId, SimulationImage, SwitchState, TimeError, TransitionHandler, event_phase,
    resolve_transition,
};

/// Whether a packet arrival entered the finite switch FIFO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrivalDisposition {
    Admitted,
    Dropped,
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

/// Complete normalized scalar state at the exclusive stop boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunResult {
    pub host_states: Vec<HostState>,
    pub switch_states: Vec<SwitchState>,
    pub departures: Vec<PacketDeparture>,
    pub arrivals: Vec<PacketArrivalObservation>,
    /// Unprocessed events in canonical `EventKey` order.
    pub pending_events: Vec<Event>,
}

/// Failure while executing an assumed-accepted image.
///
/// T8 adds full image validation. These errors keep the scalar oracle from panicking when a
/// hand-built T6 image violates a transition's immediate preconditions.
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
    UnknownFlow(FlowId),
    FlowRouteMiss {
        flow: FlowId,
        node: NodeId,
    },
    MissingSwitchQueue {
        node: NodeId,
        egress_link: Option<LinkId>,
    },
    HostAlreadyTransmitting(NodeId),
    UnexpectedTxComplete {
        node: NodeId,
        expected: Option<PayloadId>,
        actual: PayloadId,
    },
    CounterOverflow(NodeId),
    OriginSequenceOverflow(NodeId),
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
            Self::UnknownFlow(flow) => write!(formatter, "unknown flow {flow:?}"),
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
            Self::HostAlreadyTransmitting(node) => {
                write!(
                    formatter,
                    "host {node:?} received TxReady while transmitting"
                )
            }
            Self::UnexpectedTxComplete {
                node,
                expected,
                actual,
            } => write!(
                formatter,
                "host {node:?} completed {actual:?}, expected {expected:?}"
            ),
            Self::CounterOverflow(node) => {
                write!(formatter, "state counter overflow at node {node:?}")
            }
            Self::OriginSequenceOverflow(node) => {
                write!(formatter, "origin sequence overflow at node {node:?}")
            }
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

/// Runs the canonical scalar executor up to, but not including, `stop_time_ns`.
pub fn run_scalar(image: &SimulationImage, stop_time_ns: u64) -> Result<RunResult, ExecutionError> {
    ScalarExecutor::new(image)?.run(stop_time_ns)
}

struct ScalarExecutor<'image> {
    image: &'image SimulationImage,
    events: BTreeMap<EventKey, Event>,
    host_states: Vec<HostState>,
    switch_states: Vec<SwitchState>,
    departures: Vec<PacketDeparture>,
    arrivals: Vec<PacketArrivalObservation>,
}

impl<'image> ScalarExecutor<'image> {
    fn new(image: &'image SimulationImage) -> Result<Self, ExecutionError> {
        let mut events = BTreeMap::new();
        for event in image.initial_events.iter().copied() {
            if events.insert(event.key, event).is_some() {
                return Err(ExecutionError::DuplicateEventKey(event.key));
            }
        }

        Ok(Self {
            image,
            events,
            host_states: image.host_states.clone(),
            switch_states: image.switch_states.clone(),
            departures: Vec::new(),
            arrivals: Vec::new(),
        })
    }

    fn run(mut self, stop_time_ns: u64) -> Result<RunResult, ExecutionError> {
        while self
            .events
            .first_key_value()
            .is_some_and(|(key, _)| key.time_ns < stop_time_ns)
        {
            let (_, event) = self
                .events
                .pop_first()
                .expect("first_key_value established a pending event");
            self.dispatch(event)?;
        }

        Ok(RunResult {
            host_states: self.host_states,
            switch_states: self.switch_states,
            departures: self.departures,
            arrivals: self.arrivals,
            pending_events: self.events.into_values().collect(),
        })
    }

    fn dispatch(&mut self, event: Event) -> Result<(), ExecutionError> {
        let node = self.node(event.target)?;
        let handler = resolve_transition(node.kind, event.kind).ok_or(
            ExecutionError::UnsupportedTransition {
                node: node.id,
                kind: node.kind,
                event_kind: event.kind,
            },
        )?;

        match handler {
            TransitionHandler::HostPacketArrival => self.host_packet_arrival(node, event),
            TransitionHandler::HostTxReady => self.host_tx_ready(node, event),
            TransitionHandler::HostTxComplete => self.host_tx_complete(node, event),
            TransitionHandler::SwitchRemoteArrival => self.switch_remote_arrival(node, event),
            TransitionHandler::HostRemoteArrival
            | TransitionHandler::SwitchTxReady
            | TransitionHandler::SwitchTxComplete => Err(ExecutionError::UnsupportedTransition {
                node: node.id,
                kind: node.kind,
                event_kind: event.kind,
            }),
        }
    }

    fn host_packet_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
    ) -> Result<(), ExecutionError> {
        self.packet_size(event.payload)?;

        let schedule_ready = {
            let state = self.host_state_mut(node)?;
            state.sourced_packets = state
                .sourced_packets
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(node.id))?;
            state.queue.push_back(event.payload);

            if state.in_service.is_none() && !state.tx_ready_pending {
                state.tx_ready_pending = true;
                true
            } else {
                false
            }
        };

        if schedule_ready {
            self.emit_from_host(
                node,
                event,
                node.id,
                EventKind::TxReady,
                event.payload,
                event.key.time_ns,
            )?;
        }

        Ok(())
    }

    fn host_tx_ready(&mut self, node: NodeDescriptor, event: Event) -> Result<(), ExecutionError> {
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

        let arrival_time_ns =
            link.arrival_time_ns(event.key.time_ns, self.packet_size(payload)?)?;
        let departure_time_ns = arrival_time_ns
            .checked_sub(link.propagation_ns)
            .ok_or(ExecutionError::Time(TimeError::ArrivalOverflow))?;

        // Emission order is semantic: completion first, then the remote message.
        self.emit_from_host(
            node,
            event,
            node.id,
            EventKind::TxComplete,
            payload,
            departure_time_ns,
        )?;
        self.emit_from_host(
            node,
            event,
            link.target,
            EventKind::RemoteArrival,
            payload,
            arrival_time_ns,
        )
    }

    fn host_tx_complete(
        &mut self,
        node: NodeDescriptor,
        event: Event,
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

        self.departures.push(PacketDeparture {
            payload: event.payload,
            time_ns: event.key.time_ns,
        });

        if schedule_ready {
            self.emit_from_host(
                node,
                event,
                node.id,
                EventKind::TxReady,
                event.payload,
                event.key.time_ns,
            )?;
        }

        Ok(())
    }

    fn switch_remote_arrival(
        &mut self,
        node: NodeDescriptor,
        event: Event,
    ) -> Result<(), ExecutionError> {
        self.packet_size(event.payload)?;
        let egress_link = self.packet_egress_at(event.payload, node.id)?;

        let disposition = {
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
                ArrivalDisposition::Dropped
            } else {
                queue.queue.push_back(event.payload);
                ArrivalDisposition::Admitted
            }
        };

        self.arrivals.push(PacketArrivalObservation {
            payload: event.payload,
            time_ns: event.key.time_ns,
            disposition,
        });
        Ok(())
    }

    fn emit_from_host(
        &mut self,
        origin: NodeDescriptor,
        parent: Event,
        target: NodeId,
        kind: EventKind,
        payload: PayloadId,
        time_ns: u64,
    ) -> Result<(), ExecutionError> {
        let origin_seq = {
            let state = self.host_state_mut(origin)?;
            let origin_seq = state.next_origin_seq;
            state.next_origin_seq = origin_seq
                .checked_add(1)
                .ok_or(ExecutionError::OriginSequenceOverflow(origin.id))?;
            origin_seq
        };
        let child = Event {
            key: EventKey {
                time_ns,
                phase: event_phase(kind),
                origin_node: origin.id,
                origin_seq,
            },
            target,
            kind,
            payload,
        };

        if child.key <= parent.key {
            return Err(ExecutionError::NonMonotoneChild {
                parent: parent.key,
                child: child.key,
            });
        }
        if self.events.insert(child.key, child).is_some() {
            return Err(ExecutionError::DuplicateEventKey(child.key));
        }
        Ok(())
    }

    fn node(&self, id: NodeId) -> Result<NodeDescriptor, ExecutionError> {
        self.image
            .nodes
            .iter()
            .copied()
            .find(|node| node.id == id)
            .ok_or(ExecutionError::UnknownNode(id))
    }

    fn host_state_mut(&mut self, node: NodeDescriptor) -> Result<&mut HostState, ExecutionError> {
        self.host_states
            .get_mut(node.state_slot as usize)
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
        self.switch_states.get_mut(node.state_slot as usize).ok_or(
            ExecutionError::InvalidStateSlot {
                node: node.id,
                kind: node.kind,
                state_slot: node.state_slot,
            },
        )
    }

    fn link(&self, id: LinkId) -> Result<crate::LinkDescriptor, ExecutionError> {
        self.image
            .links
            .iter()
            .copied()
            .find(|link| link.id == id)
            .ok_or(ExecutionError::UnknownLink(id))
    }

    fn packet_size(&self, id: PayloadId) -> Result<u64, ExecutionError> {
        self.image
            .packets
            .iter()
            .find(|packet| packet.id == id)
            .map(|packet| packet.size_bytes)
            .ok_or(ExecutionError::UnknownPacket(id))
    }

    fn packet_egress_at(
        &self,
        payload: PayloadId,
        node: NodeId,
    ) -> Result<Option<LinkId>, ExecutionError> {
        let packet = self
            .image
            .packets
            .iter()
            .find(|packet| packet.id == payload)
            .ok_or(ExecutionError::UnknownPacket(payload))?;
        let flow = self
            .image
            .flows
            .iter()
            .find(|flow| flow.id == packet.flow)
            .ok_or(ExecutionError::UnknownFlow(packet.flow))?;

        for link_id in &flow.route {
            let link = self.link(*link_id)?;
            if link.source == node {
                return Ok(Some(link.id));
            }
        }
        if flow.target == node {
            return Ok(None);
        }
        Err(ExecutionError::FlowRouteMiss {
            flow: flow.id,
            node,
        })
    }
}

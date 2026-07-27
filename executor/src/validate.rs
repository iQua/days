//! Load-time validation for backend-neutral simulation images.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::{
    EventKind, FlowDescriptor, FlowGeneratorKind, GeneratorStatus, GeneratorTermination,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, PacketKind, PayloadId, SchedulerKind,
    SimulationImage, event_phase, resolve_transition,
};

/// Execution target whose representational limits are checked before running.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    Scalar,
    Cpu { workers: usize },
    Metal,
    Cuda,
}

impl Backend {
    const fn is_parallel(self) -> bool {
        !matches!(self, Self::Scalar)
    }

    const fn capacity_limit(self) -> Option<u64> {
        match self {
            Self::Scalar | Self::Cpu { .. } => None,
            Self::Metal | Self::Cuda => Some(u32::MAX as u64),
        }
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar => formatter.write_str("Scalar"),
            Self::Cpu { .. } => formatter.write_str("Cpu"),
            Self::Metal => formatter.write_str("Metal"),
            Self::Cuda => formatter.write_str("Cuda"),
        }
    }
}

/// A specific malformed, unsound, or backend-incompatible image property.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    message: String,
}

impl ValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ValidationError {}

/// Validates all scenario-dependent assumptions required by the selected backend.
pub fn validate(image: &SimulationImage, backend: Backend) -> Result<(), ValidationError> {
    if matches!(backend, Backend::Cpu { workers: 0 }) {
        return Err(ValidationError::new(
            "parallel backend Cpu requires at least one worker",
        ));
    }

    validate_node_ids(image)?;
    validate_link_ids(image)?;
    validate_flow_ids(image)?;
    validate_packet_ids(image)?;
    validate_state_ownership(image)?;
    validate_links(image)?;
    validate_flows(image)?;
    validate_generators(image)?;
    let derived_delays = validate_packets_and_derive_delays(image)?;
    validate_owned_service_state(image, backend)?;
    validate_channels(image, backend, &derived_delays)?;
    validate_events(image)?;
    validate_global_time_capacity(image)?;
    validate_service_event_consistency(image)?;
    let future_work = validate_counters(image)?;
    validate_origin_sequences(image, &future_work)?;
    validate_payload_sequences(image, &future_work)?;
    validate_preloaded_arrival_capacity(image)?;
    Ok(())
}

fn validate_node_ids(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    let count = image.nodes.len() as u64;
    for (index, node) in image.nodes.iter().enumerate() {
        if !seen.insert(node.id) {
            return Err(ValidationError::new(format!(
                "duplicate node ID {:?} at descriptor {index}",
                node.id
            )));
        }
        if node.id.0 >= count {
            return Err(ValidationError::new(format!(
                "node ID {:?} at descriptor {index} is outside dense range 0..{count}",
                node.id
            )));
        }
        if node.id.0 != index as u64 {
            return Err(ValidationError::new(format!(
                "node ID {:?} at descriptor {index} does not match dense table index {index}",
                node.id
            )));
        }
    }
    Ok(())
}

fn validate_link_ids(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    let count = image.links.len() as u64;
    for (index, link) in image.links.iter().enumerate() {
        if !seen.insert(link.id) {
            return Err(ValidationError::new(format!(
                "duplicate link ID {:?} at descriptor {index}",
                link.id
            )));
        }
        if link.id.0 >= count {
            return Err(ValidationError::new(format!(
                "link ID {:?} at descriptor {index} is outside dense range 0..{count}",
                link.id
            )));
        }
        if link.id.0 != index as u64 {
            return Err(ValidationError::new(format!(
                "link ID {:?} at descriptor {index} does not match dense table index {index}",
                link.id
            )));
        }
    }
    Ok(())
}

fn validate_flow_ids(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    let count = image.flows.len() as u64;
    for (index, flow) in image.flows.iter().enumerate() {
        if !seen.insert(flow.id) {
            return Err(ValidationError::new(format!(
                "duplicate flow ID {:?} at descriptor {index}",
                flow.id
            )));
        }
        if flow.id.0 >= count {
            return Err(ValidationError::new(format!(
                "flow ID {:?} at descriptor {index} is outside dense range 0..{count}",
                flow.id
            )));
        }
        if flow.id.0 != index as u64 {
            return Err(ValidationError::new(format!(
                "flow ID {:?} at descriptor {index} does not match dense table index {index}",
                flow.id
            )));
        }
    }
    Ok(())
}

fn validate_packet_ids(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for (index, packet) in image.initial_packets.iter().enumerate() {
        if !seen.insert(packet.id) {
            return Err(ValidationError::new(format!(
                "duplicate packet ID {:?} at descriptor {index}",
                packet.id
            )));
        }
        if index > 0 && image.initial_packets[index - 1].id >= packet.id {
            return Err(ValidationError::new(format!(
                "packet ID {:?} at descriptor {index} does not advance previous packet ID {:?}",
                packet.id,
                image.initial_packets[index - 1].id
            )));
        }
    }
    Ok(())
}

fn validate_state_ownership(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut host_owners = vec![None; image.host_states.len()];
    let mut switch_owners = vec![None; image.switch_states.len()];

    for node in &image.nodes {
        let (owners, arena_len) = match node.kind {
            NodeKind::Host => (&mut host_owners, image.host_states.len()),
            NodeKind::Switch => (&mut switch_owners, image.switch_states.len()),
        };
        let Ok(slot) = usize::try_from(node.state_slot) else {
            return Err(invalid_state_slot(node, arena_len));
        };
        let Some(owner) = owners.get_mut(slot) else {
            return Err(invalid_state_slot(node, arena_len));
        };
        if let Some(first) = owner {
            return Err(ValidationError::new(format!(
                "{:?} state slot {} is owned by both node {first:?} and node {:?}",
                node.kind, node.state_slot, node.id
            )));
        }
        *owner = Some(node.id);
    }

    if let Some(slot) = host_owners.iter().position(Option::is_none) {
        return Err(ValidationError::new(format!(
            "Host state slot {slot} has no owner node"
        )));
    }
    if let Some(slot) = switch_owners.iter().position(Option::is_none) {
        return Err(ValidationError::new(format!(
            "Switch state slot {slot} has no owner node"
        )));
    }
    Ok(())
}

fn invalid_state_slot(node: &NodeDescriptor, arena_len: usize) -> ValidationError {
    ValidationError::new(format!(
        "node {:?} has {:?} state slot {}, but the arena length is {arena_len}",
        node.id, node.kind, node.state_slot
    ))
}

fn validate_links(image: &SimulationImage) -> Result<(), ValidationError> {
    for link in &image.links {
        if node(image, link.source).is_none() {
            return Err(ValidationError::new(format!(
                "link {:?} names unknown source node {:?}",
                link.id, link.source
            )));
        }
        if node(image, link.target).is_none() {
            return Err(ValidationError::new(format!(
                "link {:?} names unknown target node {:?}",
                link.id, link.target
            )));
        }
        if link.rate_bps == 0 {
            return Err(ValidationError::new(format!(
                "link {:?} rate_bps must be positive",
                link.id
            )));
        }
    }
    Ok(())
}

fn validate_flows(image: &SimulationImage) -> Result<(), ValidationError> {
    for flow in &image.flows {
        let source = node(image, flow.source).ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} names unknown source node {:?}",
                flow.id, flow.source
            ))
        })?;
        let target = node(image, flow.target).ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} names unknown target node {:?}",
                flow.id, flow.target
            ))
        })?;
        if source.kind != NodeKind::Host || target.kind != NodeKind::Host {
            return Err(ValidationError::new(format!(
                "flow {:?} endpoints must both be Host nodes, got {:?} and {:?}",
                flow.id, source.kind, target.kind
            )));
        }
        if flow.route.is_empty() {
            return Err(ValidationError::new(format!(
                "flow {:?} has an empty route",
                flow.id
            )));
        }

        let mut expected_source = flow.source;
        let mut visited_nodes = BTreeSet::from([flow.source]);
        for (step, link_id) in flow.route.iter().enumerate() {
            let link = link(image, *link_id).ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} route step {step} references unknown link {link_id:?}",
                    flow.id
                ))
            })?;
            if link.source != expected_source {
                return Err(ValidationError::new(format!(
                    "flow {:?} route step {step} link {:?} starts at {:?}, expected {:?}",
                    flow.id, link.id, link.source, expected_source
                )));
            }
            if step > 0 {
                let interior = node(image, link.source)
                    .expect("link validation established every route source");
                if interior.kind != NodeKind::Switch {
                    return Err(ValidationError::new(format!(
                        "flow {:?} route step {step} uses interior {:?} node {:?}; only Switch nodes may forward",
                        flow.id, interior.kind, interior.id
                    )));
                }
            }
            if !visited_nodes.insert(link.target) {
                return Err(ValidationError::new(format!(
                    "flow {:?} route revisits node {:?} at step {step}",
                    flow.id, link.target
                )));
            }
            if step + 1 < flow.route.len() {
                let interior = node(image, link.target)
                    .expect("link validation established every route target");
                if interior.kind != NodeKind::Switch {
                    return Err(ValidationError::new(format!(
                        "flow {:?} route reaches interior {:?} node {:?} at step {step}; only Switch nodes may forward",
                        flow.id, interior.kind, interior.id
                    )));
                }
            }
            expected_source = link.target;
        }
        if expected_source != flow.target {
            return Err(ValidationError::new(format!(
                "flow {:?} route ends at node {:?}, expected {:?}",
                flow.id, expected_source, flow.target
            )));
        }
        if !flow.reverse_route.is_empty() {
            validate_reverse_route(image, flow)?;
        }
    }
    Ok(())
}

fn validate_reverse_route(
    image: &SimulationImage,
    flow: &FlowDescriptor,
) -> Result<(), ValidationError> {
    let mut expected_source = flow.target;
    let mut visited_nodes = BTreeSet::from([flow.target]);
    for (step, link_id) in flow.reverse_route.iter().enumerate() {
        let link = link(image, *link_id).ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} reverse route step {step} references unknown link {link_id:?}",
                flow.id
            ))
        })?;
        if link.source != expected_source {
            return Err(ValidationError::new(format!(
                "flow {:?} reverse route step {step} link {:?} starts at {:?}, expected {:?}",
                flow.id, link.id, link.source, expected_source
            )));
        }
        if step > 0 {
            let interior =
                node(image, link.source).expect("link validation established reverse source");
            if interior.kind != NodeKind::Switch {
                return Err(ValidationError::new(format!(
                    "flow {:?} reverse route step {step} uses interior {:?} node {:?}; only Switch nodes may forward",
                    flow.id, interior.kind, interior.id
                )));
            }
        }
        if !visited_nodes.insert(link.target) {
            return Err(ValidationError::new(format!(
                "flow {:?} reverse route revisits node {:?} at step {step}",
                flow.id, link.target
            )));
        }
        if step + 1 < flow.reverse_route.len() {
            let interior =
                node(image, link.target).expect("link validation established reverse target");
            if interior.kind != NodeKind::Switch {
                return Err(ValidationError::new(format!(
                    "flow {:?} reverse route reaches interior {:?} node {:?} at step {step}; only Switch nodes may forward",
                    flow.id, interior.kind, interior.id
                )));
            }
        }
        expected_source = link.target;
    }
    if expected_source != flow.source {
        return Err(ValidationError::new(format!(
            "flow {:?} reverse route ends at node {:?}, expected {:?}",
            flow.id, expected_source, flow.source
        )));
    }
    Ok(())
}

fn validate_generators(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut arrival_counts = BTreeMap::<(NodeId, PayloadId, u64), usize>::new();
    for event in image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacketArrival)
    {
        *arrival_counts
            .entry((event.target, event.payload, event.key.time_ns))
            .or_default() += 1;
    }
    let mut owners = BTreeMap::<crate::FlowId, (NodeId, GeneratorStatus, PayloadId, u64)>::new();
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let mut previous = None;
        for (index, generator) in state.generators.iter().enumerate() {
            if previous.is_some_and(|flow| flow >= generator.flow) {
                return Err(ValidationError::new(format!(
                    "host node {:?} generator {index} flow {:?} does not advance previous flow {:?}",
                    owner.id,
                    generator.flow,
                    previous.expect("checked Some")
                )));
            }
            previous = Some(generator.flow);
            let flow = flow(image, generator.flow).ok_or_else(|| {
                ValidationError::new(format!(
                    "host node {:?} generator {index} references unknown flow {:?}",
                    owner.id, generator.flow
                ))
            })?;
            if flow.source != owner.id {
                return Err(ValidationError::new(format!(
                    "host node {:?} owns generator for flow {:?}, but the flow source is {:?}",
                    owner.id, flow.id, flow.source
                )));
            }
            if let Some((first, ..)) = owners.insert(
                flow.id,
                (
                    owner.id,
                    generator.next_emission.status,
                    generator.next_emission.payload,
                    generator.next_emission.departure_time_ns,
                ),
            ) {
                return Err(ValidationError::new(format!(
                    "flow {:?} generator is owned by both node {first:?} and node {:?}",
                    flow.id, owner.id
                )));
            }

            let FlowGeneratorKind::Constant(constant) = generator.kind;
            if constant.interval_ns == 0 {
                return Err(ValidationError::new(format!(
                    "flow {:?} constant generator interval must be positive",
                    flow.id
                )));
            }
            if constant.packet_size_bytes == 0 {
                return Err(ValidationError::new(format!(
                    "flow {:?} constant generator packet size must be positive",
                    flow.id
                )));
            }
            if let GeneratorTermination::DurationNs(duration_ns) = constant.termination {
                constant
                    .first_departure_ns
                    .checked_add(duration_ns)
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} generator duration end time exceeds u64",
                            flow.id
                        ))
                    })?;
            }

            let remaining = remaining_generator_packets(generator)?;
            match generator.next_emission.status {
                GeneratorStatus::Scheduled if remaining == 0 => {
                    return Err(ValidationError::new(format!(
                        "flow {:?} has a scheduled emission after its constant generator finished",
                        flow.id
                    )));
                }
                GeneratorStatus::Finished if remaining != 0 => {
                    return Err(ValidationError::new(format!(
                        "flow {:?} generator is Finished with {remaining} packets remaining",
                        flow.id
                    )));
                }
                GeneratorStatus::Blocked if remaining == 0 => {
                    return Err(ValidationError::new(format!(
                        "flow {:?} generator is Blocked after its constant generator finished",
                        flow.id
                    )));
                }
                GeneratorStatus::Stopped if remaining == 0 => {
                    return Err(ValidationError::new(format!(
                        "flow {:?} generator is Stopped after its constant generator finished",
                        flow.id
                    )));
                }
                GeneratorStatus::Scheduled
                | GeneratorStatus::Blocked
                | GeneratorStatus::Finished
                | GeneratorStatus::Stopped => {}
            }

            if generator.next_emission.status == GeneratorStatus::Scheduled {
                let packet = packet(image, generator.next_emission.payload).ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} scheduled emission references unknown packet {:?}",
                        flow.id, generator.next_emission.payload
                    ))
                })?;
                if packet.flow != flow.id {
                    return Err(ValidationError::new(format!(
                        "flow {:?} scheduled emission packet {:?} belongs to flow {:?}",
                        flow.id, packet.id, packet.flow
                    )));
                }
                if packet.kind != PacketKind::Data {
                    return Err(ValidationError::new(format!(
                        "flow {:?} scheduled packet {:?} is {:?}, expected Data",
                        flow.id, packet.id, packet.kind
                    )));
                }
                if packet.size_bytes != constant.packet_size_bytes {
                    return Err(ValidationError::new(format!(
                        "flow {:?} scheduled packet {:?} has size {}, expected {}",
                        flow.id, packet.id, packet.size_bytes, constant.packet_size_bytes
                    )));
                }
                let expected_departure = constant
                    .interval_ns
                    .checked_mul(generator.packets_emitted)
                    .and_then(|offset| constant.first_departure_ns.checked_add(offset))
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} generator next departure time exceeds u64",
                            flow.id
                        ))
                    })?;
                if generator.next_emission.departure_time_ns != expected_departure {
                    return Err(ValidationError::new(format!(
                        "flow {:?} scheduled departure time {} does not match recurrence value {expected_departure}",
                        flow.id, generator.next_emission.departure_time_ns
                    )));
                }
                let matching_events = arrival_counts
                    .get(&(
                        owner.id,
                        packet.id,
                        generator.next_emission.departure_time_ns,
                    ))
                    .copied()
                    .unwrap_or(0);
                if matching_events != 1 {
                    return Err(ValidationError::new(format!(
                        "flow {:?} scheduled emission has {matching_events} matching PacketArrival events; expected 1",
                        flow.id
                    )));
                }
                validate_scheduled_payload_sequence(image, owner, state, packet.id, flow.id)?;
            } else if generator.next_emission.status == GeneratorStatus::Stopped {
                let expected_departure = constant
                    .interval_ns
                    .checked_mul(generator.packets_emitted)
                    .and_then(|offset| constant.first_departure_ns.checked_add(offset))
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} generator next departure time exceeds u64",
                            flow.id
                        ))
                    })?;
                if generator.next_emission.departure_time_ns != expected_departure {
                    return Err(ValidationError::new(format!(
                        "flow {:?} stopped departure time {} does not match recurrence value {expected_departure}",
                        flow.id, generator.next_emission.departure_time_ns
                    )));
                }
                if expected_departure <= image.stop_time_ns {
                    return Err(ValidationError::new(format!(
                        "flow {:?} generator is Stopped at {expected_departure}, which is not beyond stop time {}",
                        flow.id, image.stop_time_ns
                    )));
                }
            }
        }
    }
    for event in image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacketArrival)
    {
        let Some(packet) = packet(image, event.payload) else {
            continue;
        };
        if let Some((owner, status, payload, time_ns)) = owners.get(&packet.flow) {
            let expected = *status == GeneratorStatus::Scheduled
                && *owner == event.target
                && *payload == event.payload
                && *time_ns == event.key.time_ns;
            if !expected {
                return Err(ValidationError::new(format!(
                    "flow {:?} generator has an unexpected PacketArrival for payload {:?} at node {:?} time {}",
                    packet.flow, event.payload, event.target, event.key.time_ns
                )));
            }
        }
    }
    Ok(())
}

fn validate_preloaded_arrival_capacity(image: &SimulationImage) -> Result<(), ValidationError> {
    let generator_flows = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .map(|generator| generator.flow)
        .collect::<BTreeSet<_>>();
    let mut counts = BTreeMap::<crate::FlowId, usize>::new();
    for event in image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacketArrival)
    {
        let packet = packet(image, event.payload).expect("event validation established packet");
        if generator_flows.contains(&packet.flow) {
            continue;
        }
        let count = counts.entry(packet.flow).or_default();
        *count += 1;
        if *count > 1 {
            return Err(ValidationError::new(format!(
                "flow {:?} without a generator has {count} PacketArrival inputs; at most one preloaded input is supported",
                packet.flow
            )));
        }
    }
    Ok(())
}

fn validate_scheduled_payload_sequence(
    image: &SimulationImage,
    owner: &NodeDescriptor,
    state: &crate::HostState,
    payload: PayloadId,
    flow: crate::FlowId,
) -> Result<(), ValidationError> {
    let node_count = u64::try_from(image.nodes.len()).unwrap_or(u64::MAX);
    let offset = payload.0.checked_sub(owner.id.0).ok_or_else(|| {
        ValidationError::new(format!(
            "flow {flow:?} scheduled payload {payload:?} is not allocated by source node {:?}",
            owner.id
        ))
    })?;
    if node_count == 0 || offset % node_count != 0 {
        return Err(ValidationError::new(format!(
            "flow {flow:?} scheduled payload {payload:?} is not allocated by source node {:?}",
            owner.id
        )));
    }
    let sequence = offset / node_count;
    if sequence >= state.next_payload_seq {
        return Err(ValidationError::new(format!(
            "flow {flow:?} scheduled payload {payload:?} sequence {sequence} is not below node {:?} next payload sequence {}",
            owner.id, state.next_payload_seq
        )));
    }
    Ok(())
}

fn validate_packets_and_derive_delays(
    image: &SimulationImage,
) -> Result<BTreeMap<LinkId, u64>, ValidationError> {
    let mut derived = BTreeMap::<LinkId, u64>::new();
    for packet in &image.initial_packets {
        if packet.size_bytes == 0 {
            return Err(ValidationError::new(format!(
                "packet {:?} has zero size, which cannot certify positive serialization",
                packet.id
            )));
        }
        let flow = flow(image, packet.flow).ok_or_else(|| {
            ValidationError::new(format!(
                "packet {:?} references unknown flow {:?}",
                packet.id, packet.flow
            ))
        })?;
        let mut cumulative_delay = 0_u64;
        for link_id in packet_route(flow, packet.kind) {
            let link = link(image, *link_id).expect("validated flow route names an existing link");
            let delay = link.delay_ns(packet.size_bytes).map_err(|error| {
                ValidationError::new(format!(
                    "link {:?} delay overflows for packet {:?}: {error}",
                    link.id, packet.id
                ))
            })?;
            cumulative_delay = cumulative_delay.checked_add(delay).ok_or_else(|| {
                ValidationError::new(format!(
                    "packet {:?} cumulative minimum route delay overflows at link {:?}",
                    packet.id, link.id
                ))
            })?;
            derived
                .entry(link.id)
                .and_modify(|minimum| *minimum = (*minimum).min(delay))
                .or_insert(delay);
        }
    }
    for state in &image.host_states {
        for generator in &state.generators {
            if remaining_generator_packets(generator)? == 0 {
                continue;
            }
            let flow = flow(image, generator.flow).expect("generator validation established flow");
            let FlowGeneratorKind::Constant(constant) = generator.kind;
            for link_id in &flow.route {
                let link =
                    link(image, *link_id).expect("validated flow route names an existing link");
                let delay = link.delay_ns(constant.packet_size_bytes).map_err(|error| {
                    ValidationError::new(format!(
                        "link {:?} delay overflows for flow {:?} generator: {error}",
                        link.id, flow.id
                    ))
                })?;
                derived
                    .entry(link.id)
                    .and_modify(|minimum| *minimum = (*minimum).min(delay))
                    .or_insert(delay);
            }
        }
    }
    Ok(derived)
}

fn validate_owned_service_state(
    image: &SimulationImage,
    backend: Backend,
) -> Result<(), ValidationError> {
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let egress = link(image, state.egress_link).ok_or_else(|| {
            ValidationError::new(format!(
                "host node {:?} references unknown egress link {:?}",
                owner.id, state.egress_link
            ))
        })?;
        if egress.source != owner.id {
            return Err(ValidationError::new(format!(
                "host node {:?} egress link {:?} starts at node {:?}",
                owner.id, egress.id, egress.source
            )));
        }
        validate_payload_egress(
            image,
            owner.id,
            state.egress_link,
            "host queue",
            state.queue.iter().copied(),
        )?;
        if let Some(payload) = state.in_service {
            validate_payload_egress(
                image,
                owner.id,
                state.egress_link,
                "host in-service slot",
                [payload],
            )?;
        }
        if state.in_service.is_some() && state.tx_ready_pending {
            return Err(ValidationError::new(format!(
                "host node {:?} cannot be in service and have TxReady pending",
                owner.id
            )));
        }
        if state.tx_ready_pending && state.queue.is_empty() {
            return Err(ValidationError::new(format!(
                "host node {:?} has TxReady pending with an empty queue",
                owner.id
            )));
        }
        if !state.queue.is_empty() && state.in_service.is_none() && !state.tx_ready_pending {
            return Err(ValidationError::new(format!(
                "host node {:?} has queued packets but neither active service nor TxReady pending",
                owner.id
            )));
        }
    }

    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        let state = &image.switch_states[owner.state_slot as usize];
        let mut egresses = BTreeSet::new();
        for (queue_index, queue) in state.queues.iter().enumerate() {
            match queue.scheduler {
                SchedulerKind::Fifo => {}
                unsupported => {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} uses unsupported {unsupported:?} service on backend {backend}",
                        owner.id
                    )));
                }
            }
            if let Some(limit) = backend.capacity_limit() {
                if queue.queue_capacity_packets > limit {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} capacity {} exceeds backend {backend} limit {limit}",
                        owner.id, queue.queue_capacity_packets
                    )));
                }
            }
            if queue.queue_capacity_packets != 0
                && queue.queue.len() as u64 > queue.queue_capacity_packets
            {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} contains {} packets, exceeding capacity {}",
                    owner.id,
                    queue.queue.len(),
                    queue.queue_capacity_packets
                )));
            }
            let Some(egress_id) = queue.egress_link else {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} has no egress link",
                    owner.id
                )));
            };
            if !egresses.insert(egress_id) {
                return Err(ValidationError::new(format!(
                    "switch node {:?} has duplicate queues for egress link {egress_id:?}",
                    owner.id
                )));
            }
            let egress = link(image, egress_id).ok_or_else(|| {
                ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} references unknown egress link {egress_id:?}",
                    owner.id
                ))
            })?;
            if egress.source != owner.id {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} egress link {egress_id:?} starts at node {:?}",
                    owner.id, egress.source
                )));
            }
            validate_payload_egress(
                image,
                owner.id,
                egress_id,
                "switch queue",
                queue.queue.iter().copied(),
            )?;
            if let Some(payload) = queue.in_service {
                validate_payload_egress(
                    image,
                    owner.id,
                    egress_id,
                    "switch in-service slot",
                    [payload],
                )?;
            }
            if queue.in_service.is_some() && queue.tx_ready_pending {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} cannot be in service and have TxReady pending",
                    owner.id
                )));
            }
            if queue.tx_ready_pending && queue.queue.is_empty() {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} has TxReady pending with an empty queue",
                    owner.id
                )));
            }
            if !queue.queue.is_empty() && queue.in_service.is_none() && !queue.tx_ready_pending {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} has packets but neither active service nor TxReady pending",
                    owner.id
                )));
            }
        }
    }

    for link_id in possible_emission_links(image) {
        let link = link(image, link_id).expect("route validation established the link");
        let source = node(image, link.source).expect("link validation established the source");
        match source.kind {
            NodeKind::Host => {
                let state = &image.host_states[source.state_slot as usize];
                if state.egress_link != link_id {
                    return Err(ValidationError::new(format!(
                        "host node {:?} can emit over route link {link_id:?}, but owns egress link {:?}",
                        source.id, state.egress_link
                    )));
                }
            }
            NodeKind::Switch => {
                let state = &image.switch_states[source.state_slot as usize];
                if !state
                    .queues
                    .iter()
                    .any(|queue| queue.egress_link == Some(link_id))
                {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} can emit over route link {link_id:?}, but owns no matching queue",
                        source.id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_payload_egress(
    image: &SimulationImage,
    owner: NodeId,
    expected_egress: LinkId,
    location: &str,
    payloads: impl IntoIterator<Item = PayloadId>,
) -> Result<(), ValidationError> {
    for payload in payloads {
        let packet = packet(image, payload).ok_or_else(|| {
            ValidationError::new(format!(
                "node {owner:?} {location} references unknown packet {payload:?}"
            ))
        })?;
        let flow = flow(image, packet.flow).expect("packet validation established the flow");
        let actual_egress = flow_egress_at(image, flow, packet.kind, owner);
        if actual_egress != Some(expected_egress) {
            return Err(ValidationError::new(format!(
                "node {owner:?} {location} contains packet {payload:?} for egress {actual_egress:?}, expected {expected_egress:?}"
            )));
        }
    }
    Ok(())
}

fn validate_channels(
    image: &SimulationImage,
    backend: Backend,
    derived_delays: &BTreeMap<LinkId, u64>,
) -> Result<(), ValidationError> {
    let mut channels_by_link = BTreeMap::<LinkId, usize>::new();
    for (index, channel) in image.channels.iter().enumerate() {
        let link = link(image, channel.link).ok_or_else(|| {
            ValidationError::new(format!(
                "channel {index} references unknown link {:?}",
                channel.link
            ))
        })?;
        if channel.source != link.source || channel.target != link.target {
            return Err(ValidationError::new(format!(
                "channel {index} endpoints {:?}->{:?} do not match link {:?} endpoints {:?}->{:?}",
                channel.source, channel.target, link.id, link.source, link.target
            )));
        }
        if channel.event_kind != EventKind::RemoteArrival {
            return Err(ValidationError::new(format!(
                "channel {index} declares unsupported {:?}; packet links emit RemoteArrival",
                channel.event_kind
            )));
        }
        if let Some(first) = channels_by_link.insert(channel.link, index) {
            return Err(ValidationError::new(format!(
                "channel {index} duplicates channel {first} for link {:?}",
                channel.link
            )));
        }
        let Some(derived) = derived_delays.get(&channel.link).copied() else {
            return Err(ValidationError::new(format!(
                "channel {index} references link {:?}, which has no possible packet emission",
                channel.link
            )));
        };
        if backend.is_parallel() && channel.min_delay_ns == 0 {
            return Err(ValidationError::new(format!(
                "parallel backend {backend} channel {index} has zero min_delay_ns"
            )));
        }
        if channel.min_delay_ns > derived {
            return Err(ValidationError::new(format!(
                "channel {index} declares min_delay_ns {}, exceeding derived bound {derived} for link {:?}",
                channel.min_delay_ns, channel.link
            )));
        }
    }

    for link_id in derived_delays.keys() {
        if !channels_by_link.contains_key(link_id) {
            let link = link(image, *link_id).expect("derived delay names a validated link");
            return Err(ValidationError::new(format!(
                "link {:?} can emit RemoteArrival from node {:?} to node {:?}, but no channel is declared",
                link.id, link.source, link.target
            )));
        }
    }
    Ok(())
}

fn validate_events(image: &SimulationImage) -> Result<(), ValidationError> {
    let declared_route_channels = image
        .channels
        .iter()
        .map(|channel| {
            (
                channel.source,
                channel.target,
                channel.event_kind,
                channel.link,
            )
        })
        .collect::<BTreeSet<_>>();
    let mut keys = BTreeSet::new();
    let mut semantic_events = BTreeSet::new();
    let mut previous = None;
    for (index, event) in image.initial_events.iter().enumerate() {
        if !keys.insert(event.key) {
            return Err(ValidationError::new(format!(
                "duplicate event key {:?} at initial event {index}",
                event.key
            )));
        }
        if let Some(previous) = previous {
            if event.key <= previous {
                return Err(ValidationError::new(format!(
                    "initial event {index} key {:?} does not advance previous key {previous:?}",
                    event.key
                )));
            }
        }
        previous = Some(event.key);

        let origin = node(image, event.key.origin_node).ok_or_else(|| {
            ValidationError::new(format!(
                "initial event {index} has unknown origin node {:?}",
                event.key.origin_node
            ))
        })?;
        let target = node(image, event.target).ok_or_else(|| {
            ValidationError::new(format!(
                "initial event {index} targets unknown node {:?}",
                event.target
            ))
        })?;
        let expected_phase = event_phase(event.kind);
        if event.key.phase != expected_phase {
            return Err(ValidationError::new(format!(
                "initial event {index} has noncanonical phase {} for {:?}; expected {expected_phase}",
                event.key.phase, event.kind
            )));
        }
        if resolve_transition(target.kind, event.kind).is_none() {
            return Err(ValidationError::new(format!(
                "initial event {index} targets {:?} node {:?}, which does not support {:?}",
                target.kind, target.id, event.kind
            )));
        }
        let packet = packet(image, event.payload).ok_or_else(|| {
            ValidationError::new(format!(
                "initial event {index} references unknown packet {:?}",
                event.payload
            ))
        })?;
        let flow = flow(image, packet.flow).expect("packet validation established the flow");
        if !semantic_events.insert((event.payload, event.kind, event.target)) {
            return Err(ValidationError::new(format!(
                "initial event {index} duplicates {:?} for payload {:?} at node {:?}",
                event.kind, event.payload, event.target
            )));
        }

        match event.kind {
            EventKind::PacketArrival => {
                if packet.kind != PacketKind::Data || event.target != flow.source {
                    return Err(ValidationError::new(format!(
                        "PacketArrival event {index} for payload {:?} targets node {:?}, but flow {:?} is sourced by node {:?}",
                        event.payload, event.target, flow.id, flow.source
                    )));
                }
                if origin.id != event.target {
                    return Err(ValidationError::new(format!(
                        "PacketArrival event {index} origin {:?} must equal its owner node {:?}",
                        origin.id, event.target
                    )));
                }
                validate_remaining_route_time(
                    image,
                    flow,
                    packet,
                    event.key.time_ns,
                    event.target,
                    index,
                )?;
            }
            EventKind::TxReady | EventKind::TxComplete => {
                if origin.id != event.target {
                    return Err(ValidationError::new(format!(
                        "{:?} event {index} origin {:?} must equal target owner {:?}",
                        event.kind, origin.id, event.target
                    )));
                }
                if !flow_route_contains_source(image, flow, packet.kind, event.target) {
                    return Err(ValidationError::new(format!(
                        "{:?} event {index} targets node {:?}, which does not own service for payload {:?}",
                        event.kind, event.target, event.payload
                    )));
                }
                if event.kind == EventKind::TxReady {
                    validate_remaining_route_time(
                        image,
                        flow,
                        packet,
                        event.key.time_ns,
                        event.target,
                        index,
                    )?;
                }
            }
            EventKind::RemoteArrival => {
                if !packet_route(flow, packet.kind).iter().any(|link_id| {
                    declared_route_channels.contains(&(
                        origin.id,
                        event.target,
                        EventKind::RemoteArrival,
                        *link_id,
                    ))
                }) {
                    return Err(ValidationError::new(format!(
                        "RemoteArrival event {index} from {:?} to {:?} has no declared route channel for payload {:?}",
                        origin.id, event.target, event.payload
                    )));
                }
                validate_remaining_route_time(
                    image,
                    flow,
                    packet,
                    event.key.time_ns,
                    event.target,
                    index,
                )?;
            }
        }
    }
    Ok(())
}

fn validate_remaining_route_time(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    packet: &crate::PacketDescriptor,
    start_time_ns: u64,
    start_node: NodeId,
    event_index: usize,
) -> Result<(), ValidationError> {
    let terminal = match packet.kind {
        PacketKind::Data => flow.target,
        PacketKind::Feedback => flow.source,
    };
    if start_node == terminal {
        return Ok(());
    }
    let mut remaining_delay = 0_u64;
    let mut started = false;
    for link_id in packet_route(flow, packet.kind) {
        let link = link(image, *link_id).expect("flow validation established the route link");
        if link.source == start_node {
            started = true;
        }
        if started {
            let delay = link
                .delay_ns(packet.size_bytes)
                .expect("packet/link delay validation already succeeded");
            remaining_delay = remaining_delay
                .checked_add(delay)
                .expect("cumulative packet route validation already succeeded");
        }
    }
    if !started {
        return Err(ValidationError::new(format!(
            "initial event {event_index} starts payload {:?} at node {start_node:?}, which is not on flow {:?}",
            packet.id, flow.id
        )));
    }
    start_time_ns
        .checked_add(remaining_delay)
        .ok_or_else(|| {
            ValidationError::new(format!(
                "initial event {event_index} time {start_time_ns} plus remaining route delay {remaining_delay} overflows for payload {:?} at node {start_node:?}",
                packet.id
            ))
        })?;
    Ok(())
}

fn validate_global_time_capacity(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut service_bound = 0_u64;
    let scheduled_payloads = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
        .map(|generator| generator.next_emission.payload)
        .collect::<BTreeSet<_>>();
    for packet in &image.initial_packets {
        if scheduled_payloads.contains(&packet.id) {
            continue;
        }
        let flow = flow(image, packet.flow).expect("packet validation established the flow");
        for link_id in packet_route(flow, packet.kind) {
            let link = link(image, *link_id).expect("flow validation established the route link");
            let delay = link
                .delay_ns(packet.size_bytes)
                .expect("packet/link delay validation already succeeded");
            service_bound = service_bound.checked_add(delay).ok_or_else(|| {
                ValidationError::new(format!(
                    "conservative service-time bound overflows while adding packet {:?} on link {:?}",
                    packet.id, link.id
                ))
            })?;
        }
    }
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        let flow = flow(image, generator.flow).expect("generator validation established the flow");
        let FlowGeneratorKind::Constant(constant) = generator.kind;
        let remaining = executable_generator_packets(generator)?;
        for link_id in &flow.route {
            let link = link(image, *link_id).expect("flow validation established the route link");
            let delay = link
                .delay_ns(constant.packet_size_bytes)
                .expect("generator/link delay validation already succeeded");
            let flow_delay = delay.checked_mul(remaining).ok_or_else(|| {
                ValidationError::new(format!(
                    "conservative service-time bound overflows for flow {:?} generator on link {:?}",
                    flow.id, link.id
                ))
            })?;
            service_bound = service_bound.checked_add(flow_delay).ok_or_else(|| {
                ValidationError::new(format!(
                    "conservative service-time bound overflows for flow {:?} generator on link {:?}",
                    flow.id, link.id
                ))
            })?;
        }
    }
    let maximum_initial_time = image
        .initial_events
        .iter()
        .map(|event| event.key.time_ns)
        .max()
        .unwrap_or(0);
    let maximum_generator_time = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
        .map(|generator| {
            let FlowGeneratorKind::Constant(constant) = generator.kind;
            let remaining = executable_generator_packets(generator)?;
            let intervals = remaining.saturating_sub(1);
            constant
                .interval_ns
                .checked_mul(intervals)
                .and_then(|offset| {
                    generator
                        .next_emission
                        .departure_time_ns
                        .checked_add(offset)
                })
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} latest generated departure time exceeds u64",
                        generator.flow
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    let maximum_work_time = maximum_initial_time.max(maximum_generator_time);
    maximum_work_time.checked_add(service_bound).ok_or_else(|| {
        if maximum_generator_time <= maximum_initial_time {
            ValidationError::new(format!(
                "maximum initial event time {maximum_initial_time} plus conservative service bound {service_bound} overflows"
            ))
        } else {
            ValidationError::new(format!(
                "maximum generator departure time {maximum_generator_time} plus conservative service bound {service_bound} overflows"
            ))
        }
    })?;
    Ok(())
}

fn validate_service_event_consistency(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut service_events = BTreeMap::<(NodeId, Option<LinkId>), ServiceEvents>::new();
    for event in &image.initial_events {
        if !matches!(event.kind, EventKind::TxReady | EventKind::TxComplete) {
            continue;
        }
        let owner = node(image, event.target).expect("event validation established the target");
        let egress = match owner.kind {
            NodeKind::Host => None,
            NodeKind::Switch => Some(
                event_egress(image, event)
                    .expect("event validation established switch egress ownership"),
            ),
        };
        let events = service_events.entry((owner.id, egress)).or_default();
        match event.kind {
            EventKind::TxReady => events.ready_count += 1,
            EventKind::TxComplete => events.completions.push(event.payload),
            EventKind::PacketArrival | EventKind::RemoteArrival => unreachable!(),
        }
    }

    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let events = service_events.get(&(owner.id, None));
        let ready_count = events.map_or(0, |events| events.ready_count);
        let completions = events.map_or(&[][..], |events| events.completions.as_slice());
        validate_ready_flag(owner.id, None, state.tx_ready_pending, ready_count)?;
        validate_completion_state(owner.id, None, state.in_service, completions)?;
    }

    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        let state = &image.switch_states[owner.state_slot as usize];
        for queue in &state.queues {
            let egress = queue
                .egress_link
                .expect("owned service-state validation requires an egress");
            let events = service_events.get(&(owner.id, Some(egress)));
            let ready_count = events.map_or(0, |events| events.ready_count);
            let completions = events.map_or(&[][..], |events| events.completions.as_slice());
            validate_ready_flag(owner.id, Some(egress), queue.tx_ready_pending, ready_count)?;
            validate_completion_state(owner.id, Some(egress), queue.in_service, completions)?;
        }
    }
    validate_unique_mutable_payloads(image)
}

#[derive(Default)]
struct ServiceEvents {
    ready_count: usize,
    completions: Vec<PayloadId>,
}

fn validate_ready_flag(
    owner: NodeId,
    egress: Option<LinkId>,
    pending: bool,
    ready_count: usize,
) -> Result<(), ValidationError> {
    if ready_count > 1 {
        return Err(ValidationError::new(format!(
            "node {owner:?} egress {egress:?} has {ready_count} TxReady events; at most one is allowed"
        )));
    }
    if pending != (ready_count == 1) {
        return Err(ValidationError::new(format!(
            "node {owner:?} egress {egress:?} TxReady flag is {pending}, but matching event count is {ready_count}"
        )));
    }
    Ok(())
}

fn validate_completion_state(
    owner: NodeId,
    egress: Option<LinkId>,
    in_service: Option<PayloadId>,
    completions: &[PayloadId],
) -> Result<(), ValidationError> {
    match in_service {
        Some(payload) if completions == [payload] => Ok(()),
        None if completions.is_empty() => Ok(()),
        _ => Err(ValidationError::new(format!(
            "node {owner:?} egress {egress:?} in-service payload is {in_service:?}, but matching TxComplete payloads are {:?}",
            completions
        ))),
    }
}

fn validate_unique_mutable_payloads(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut owners = BTreeMap::<PayloadId, String>::new();
    for node in &image.nodes {
        match node.kind {
            NodeKind::Host => {
                let state = &image.host_states[node.state_slot as usize];
                for payload in state.queue.iter().copied().chain(state.in_service) {
                    record_mutable_payload(&mut owners, payload, format!("host {:?}", node.id))?;
                }
            }
            NodeKind::Switch => {
                let state = &image.switch_states[node.state_slot as usize];
                for (queue_index, queue) in state.queues.iter().enumerate() {
                    for payload in queue.queue.iter().copied().chain(queue.in_service) {
                        record_mutable_payload(
                            &mut owners,
                            payload,
                            format!("switch {:?} queue {queue_index}", node.id),
                        )?;
                    }
                }
            }
        }
    }
    for event in image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacketArrival)
    {
        if let Some(location) = owners.get(&event.payload) {
            return Err(ValidationError::new(format!(
                "payload {:?} is both a PacketArrival input and mutable state at {location}",
                event.payload
            )));
        }
    }
    Ok(())
}

fn record_mutable_payload(
    owners: &mut BTreeMap<PayloadId, String>,
    payload: PayloadId,
    location: String,
) -> Result<(), ValidationError> {
    if let Some(first) = owners.insert(payload, location.clone()) {
        return Err(ValidationError::new(format!(
            "payload {payload:?} has duplicate mutable ownership at {first} and {location}"
        )));
    }
    Ok(())
}

struct FutureWork {
    data_by_flow: Vec<u64>,
    feedback_by_flow: Vec<u64>,
}

fn future_work(image: &SimulationImage) -> Result<FutureWork, ValidationError> {
    let mut data_by_flow = vec![0_u64; image.flows.len()];
    let mut feedback_by_flow = vec![0_u64; image.flows.len()];
    let scheduled_payloads = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
        .map(|generator| generator.next_emission.payload)
        .collect::<BTreeSet<_>>();
    for packet in &image.initial_packets {
        if scheduled_payloads.contains(&packet.id) {
            continue;
        }
        let counts = match packet.kind {
            PacketKind::Data => &mut data_by_flow,
            PacketKind::Feedback => &mut feedback_by_flow,
        };
        let count = &mut counts[packet.flow.0 as usize];
        *count = count
            .checked_add(1)
            .ok_or_else(|| ValidationError::new("packet count exceeds the u64 counter domain"))?;
    }
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        add_packet_count(
            &mut data_by_flow[generator.flow.0 as usize],
            executable_generator_packets(generator)?,
        )?;
    }
    Ok(FutureWork {
        data_by_flow,
        feedback_by_flow,
    })
}

fn validate_counters(image: &SimulationImage) -> Result<FutureWork, ValidationError> {
    let work = future_work(image)?;
    let mut sourced_by_node = vec![0_u64; image.nodes.len()];
    let mut departed_by_node = vec![0_u64; image.nodes.len()];
    let mut received_by_node = vec![0_u64; image.nodes.len()];
    let mut traversing_by_node = vec![0_u64; image.nodes.len()];
    for (flow, (data_count, feedback_count)) in image
        .flows
        .iter()
        .zip(work.data_by_flow.iter().zip(work.feedback_by_flow.iter()))
    {
        add_packet_count(&mut sourced_by_node[flow.source.0 as usize], *data_count)?;
        add_packet_count(&mut received_by_node[flow.target.0 as usize], *data_count)?;
        add_packet_count(
            &mut received_by_node[flow.source.0 as usize],
            *feedback_count,
        )?;
        for link_id in packet_route(flow, PacketKind::Data) {
            let route_link = link(image, *link_id).expect("flow validation established the link");
            add_packet_count(
                &mut departed_by_node[route_link.source.0 as usize],
                *data_count,
            )?;
            add_packet_count(
                &mut traversing_by_node[route_link.source.0 as usize],
                *data_count,
            )?;
        }
        for link_id in packet_route(flow, PacketKind::Feedback) {
            let route_link = link(image, *link_id).expect("flow validation established the link");
            add_packet_count(
                &mut departed_by_node[route_link.source.0 as usize],
                *feedback_count,
            )?;
            add_packet_count(
                &mut traversing_by_node[route_link.source.0 as usize],
                *feedback_count,
            )?;
        }
    }

    for owner in &image.nodes {
        match owner.kind {
            NodeKind::Host => {
                let state = &image.host_states[owner.state_slot as usize];
                let sourced = sourced_by_node[owner.id.0 as usize];
                let received = received_by_node[owner.id.0 as usize];
                check_counter(owner.id, "sourced_packets", state.sourced_packets, sourced)?;
                check_counter(
                    owner.id,
                    "departed_packets",
                    state.departed_packets,
                    departed_by_node[owner.id.0 as usize],
                )?;
                check_counter(
                    owner.id,
                    "received_packets",
                    state.received_packets,
                    received,
                )?;
            }
            NodeKind::Switch => {
                let state = &image.switch_states[owner.state_slot as usize];
                let traversing = traversing_by_node[owner.id.0 as usize];
                check_counter(
                    owner.id,
                    "arrived_packets",
                    state.arrived_packets,
                    traversing,
                )?;
                check_counter(
                    owner.id,
                    "dropped_packets",
                    state.dropped_packets,
                    traversing,
                )?;
                check_counter(
                    owner.id,
                    "departed_packets",
                    state.departed_packets,
                    traversing,
                )?;
            }
        }
    }
    Ok(work)
}

fn remaining_generator_packets(
    generator: &crate::FlowGeneratorState,
) -> Result<u64, ValidationError> {
    let FlowGeneratorKind::Constant(constant) = generator.kind;
    let total = match constant.termination {
        GeneratorTermination::Bytes(bytes) => {
            if bytes == 0 {
                0
            } else {
                1 + (bytes - 1) / constant.packet_size_bytes
            }
        }
        GeneratorTermination::DurationNs(duration_ns) => {
            if duration_ns == 0 {
                0
            } else {
                1 + (duration_ns - 1) / constant.interval_ns
            }
        }
    };
    total
        .checked_mul(constant.packet_size_bytes)
        .ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} generator byte total exceeds u64",
                generator.flow
            ))
        })?;
    let expected_bytes = generator
        .packets_emitted
        .checked_mul(constant.packet_size_bytes)
        .ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} generator byte bookkeeping exceeds u64",
                generator.flow
            ))
        })?;
    if generator.bytes_emitted != expected_bytes {
        return Err(ValidationError::new(format!(
            "flow {:?} generator records {} emitted bytes, expected {expected_bytes}",
            generator.flow, generator.bytes_emitted
        )));
    }
    total.checked_sub(generator.packets_emitted).ok_or_else(|| {
        ValidationError::new(format!(
            "flow {:?} generator emitted {} packets, exceeding constant total {total}",
            generator.flow, generator.packets_emitted
        ))
    })
}

fn executable_generator_packets(
    generator: &crate::FlowGeneratorState,
) -> Result<u64, ValidationError> {
    match generator.next_emission.status {
        GeneratorStatus::Scheduled => remaining_generator_packets(generator),
        GeneratorStatus::Blocked | GeneratorStatus::Finished | GeneratorStatus::Stopped => Ok(0),
    }
}

fn add_packet_count(total: &mut u64, count: u64) -> Result<(), ValidationError> {
    *total = total
        .checked_add(count)
        .ok_or_else(|| ValidationError::new("packet count exceeds the u64 counter domain"))?;
    Ok(())
}

fn check_counter(
    node: NodeId,
    name: &str,
    current: u64,
    remaining_upper_bound: u64,
) -> Result<(), ValidationError> {
    current.checked_add(remaining_upper_bound).ok_or_else(|| {
        ValidationError::new(format!(
            "node {node:?} counter {name} value {current} overflows with remaining upper bound {remaining_upper_bound}"
        ))
    })?;
    Ok(())
}

fn validate_origin_sequences(
    image: &SimulationImage,
    work: &FutureWork,
) -> Result<(), ValidationError> {
    let mut maximum = BTreeMap::<NodeId, u64>::new();
    for event in &image.initial_events {
        maximum
            .entry(event.key.origin_node)
            .and_modify(|value| *value = (*value).max(event.key.origin_seq))
            .or_insert(event.key.origin_seq);
    }
    let mut transmissions_by_node = vec![0_u128; image.nodes.len()];
    let mut emissions_by_node = vec![0_u128; image.nodes.len()];
    for (flow, (data_count, feedback_count)) in image
        .flows
        .iter()
        .zip(work.data_by_flow.iter().zip(work.feedback_by_flow.iter()))
    {
        for link_id in packet_route(flow, PacketKind::Data) {
            let route_link = link(image, *link_id).expect("flow validation established the link");
            transmissions_by_node[route_link.source.0 as usize] += u128::from(*data_count);
        }
        for link_id in packet_route(flow, PacketKind::Feedback) {
            let route_link = link(image, *link_id).expect("flow validation established the link");
            transmissions_by_node[route_link.source.0 as usize] += u128::from(*feedback_count);
        }
    }
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        for generator in &state.generators {
            let remaining = executable_generator_packets(generator)?;
            let scheduled = u64::from(generator.next_emission.status == GeneratorStatus::Scheduled);
            emissions_by_node[owner.id.0 as usize] +=
                u128::from(remaining.saturating_sub(scheduled));
        }
    }
    for node in &image.nodes {
        let next = match node.kind {
            NodeKind::Host => image.host_states[node.state_slot as usize].next_origin_seq,
            NodeKind::Switch => image.switch_states[node.state_slot as usize].next_origin_seq,
        };
        if let Some(existing) = maximum.get(&node.id) {
            if next <= *existing {
                return Err(ValidationError::new(format!(
                    "node {:?} next origin sequence {next} does not advance existing sequence {existing}",
                    node.id
                )));
            }
        }
        let transmission_events =
            possible_generated_events(node.id, transmissions_by_node[node.id.0 as usize])?;
        let emission_events =
            u64::try_from(emissions_by_node[node.id.0 as usize]).map_err(|_| {
                ValidationError::new(format!(
                    "node {:?} generated-event count exceeds u64",
                    node.id
                ))
            })?;
        let generated = transmission_events
            .checked_add(emission_events)
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "node {:?} generated-event count exceeds u64",
                    node.id
                ))
            })?;
        if next.checked_add(generated).is_none() {
            return Err(ValidationError::new(format!(
                "node {:?} origin sequence space overflows while reserving {generated} generated events",
                node.id,
            )));
        }
    }
    Ok(())
}

fn validate_payload_sequences(
    image: &SimulationImage,
    _work: &FutureWork,
) -> Result<(), ValidationError> {
    let node_count = u64::try_from(image.nodes.len()).unwrap_or(u64::MAX);
    let mut payload_sequences_by_owner = vec![Vec::<(u64, PayloadId)>::new(); image.nodes.len()];
    if node_count != 0 {
        for packet in &image.initial_packets {
            let owner = packet.id.0 % node_count;
            let sequence = packet.id.0 / node_count;
            if let Some(payloads) = payload_sequences_by_owner.get_mut(owner as usize) {
                payloads.push((sequence, packet.id));
            }
        }
    }
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let mut allocations = 0_u64;
        let mut consumed_sequences = 0_u64;
        for generator in &state.generators {
            let remaining = executable_generator_packets(generator)?;
            let already_scheduled =
                u64::from(generator.next_emission.status == GeneratorStatus::Scheduled);
            consumed_sequences = consumed_sequences
                .checked_add(generator.packets_emitted)
                .and_then(|total| total.checked_add(already_scheduled))
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "node {:?} consumed payload sequence count exceeds u64",
                        owner.id
                    ))
                })?;
            let required = remaining.checked_sub(already_scheduled).ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} has a scheduled emission after its constant generator finished",
                    generator.flow
                ))
            })?;
            allocations = allocations.checked_add(required).ok_or_else(|| {
                ValidationError::new(format!(
                    "node {:?} generated-packet count exceeds u64",
                    owner.id
                ))
            })?;
        }
        if state.next_payload_seq < consumed_sequences {
            return Err(ValidationError::new(format!(
                "node {:?} next payload sequence {} is below generator allocation lower bound {consumed_sequences}",
                owner.id, state.next_payload_seq
            )));
        }
        if allocations == 0 {
            continue;
        }
        let end_sequence = state
            .next_payload_seq
            .checked_add(allocations)
            .ok_or_else(|| {
                payload_reservation_error(owner.id, state.next_payload_seq, allocations)
            })?;
        let last_sequence = end_sequence - 1;
        if allocate_payload_id(owner.id, node_count, last_sequence).is_none() {
            return Err(payload_reservation_error(
                owner.id,
                state.next_payload_seq,
                allocations,
            ));
        }
        for (sequence, payload) in &payload_sequences_by_owner[owner.id.0 as usize] {
            if (state.next_payload_seq..end_sequence).contains(sequence) {
                return Err(ValidationError::new(format!(
                    "node {:?} future payload sequence {sequence} collides with initial packet {:?}",
                    owner.id, payload
                )));
            }
        }
    }
    Ok(())
}

fn payload_reservation_error(
    node: NodeId,
    next_sequence: u64,
    allocations: u64,
) -> ValidationError {
    ValidationError::new(format!(
        "node {node:?} payload identity sequence {next_sequence} overflows while reserving {allocations} generated packets"
    ))
}

fn allocate_payload_id(source: NodeId, node_count: u64, sequence: u64) -> Option<PayloadId> {
    PayloadId::from_node_sequence(source, node_count, sequence)
}

fn possible_emission_links(image: &SimulationImage) -> BTreeSet<LinkId> {
    let mut links = image
        .initial_packets
        .iter()
        .flat_map(|packet| {
            flow(image, packet.flow)
                .into_iter()
                .flat_map(|flow| packet_route(flow, packet.kind).iter().copied())
        })
        .collect::<BTreeSet<_>>();
    links.extend(
        image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .filter(|generator| remaining_generator_packets(generator).is_ok_and(|count| count > 0))
            .filter_map(|generator| flow(image, generator.flow))
            .flat_map(|flow| flow.route.iter().copied()),
    );
    links
}

fn possible_generated_events(source: NodeId, transmissions: u128) -> Result<u64, ValidationError> {
    let transmissions = u64::try_from(transmissions).map_err(|_| {
        ValidationError::new(format!("node {source:?} generated-event count exceeds u64"))
    })?;
    transmissions.checked_mul(3).ok_or_else(|| {
        ValidationError::new(format!("node {source:?} generated-event count exceeds u64"))
    })
}

fn flow_route_contains_source(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    packet_kind: PacketKind,
    source: NodeId,
) -> bool {
    flow_egress_at(image, flow, packet_kind, source).is_some()
}

fn flow_egress_at(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    packet_kind: PacketKind,
    source: NodeId,
) -> Option<LinkId> {
    packet_route(flow, packet_kind)
        .iter()
        .copied()
        .find(|link_id| link(image, *link_id).is_some_and(|link| link.source == source))
}

fn event_egress(image: &SimulationImage, event: &crate::Event) -> Option<LinkId> {
    let packet = packet(image, event.payload)?;
    let flow = flow(image, packet.flow)?;
    flow_egress_at(image, flow, packet.kind, event.target)
}

fn packet_route(flow: &FlowDescriptor, packet_kind: PacketKind) -> &[LinkId] {
    match packet_kind {
        PacketKind::Data => &flow.route,
        PacketKind::Feedback => &flow.reverse_route,
    }
}

fn node(image: &SimulationImage, id: NodeId) -> Option<&NodeDescriptor> {
    image
        .nodes
        .get(usize::try_from(id.0).ok()?)
        .filter(|node| node.id == id)
}

fn link(image: &SimulationImage, id: LinkId) -> Option<&LinkDescriptor> {
    image
        .links
        .get(usize::try_from(id.0).ok()?)
        .filter(|link| link.id == id)
}

fn flow(image: &SimulationImage, id: crate::FlowId) -> Option<&FlowDescriptor> {
    image
        .flows
        .get(usize::try_from(id.0).ok()?)
        .filter(|flow| flow.id == id)
}

fn packet(image: &SimulationImage, id: PayloadId) -> Option<&crate::PacketDescriptor> {
    image
        .initial_packets
        .binary_search_by_key(&id, |packet| packet.id)
        .ok()
        .map(|index| &image.initial_packets[index])
}

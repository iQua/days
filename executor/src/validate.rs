//! Load-time validation for backend-neutral simulation images.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use num_bigint::BigUint;

use crate::{
    EventKind, FlowDescriptor, FlowGeneratorKind, GeneratorStatus, GeneratorTermination,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, PacketKind, PayloadId, SchedulerKind,
    SimulationImage, event_phase, resolve_transition,
};

const DEVICE_WFQ_BITS: u64 = 320;
const DEVICE_WFQ_MAX_TOTAL_WEIGHT: u64 = u64::MAX / 1_000_000_000;

/// Execution target whose representational limits are checked before running.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    Scalar,
    Cpu { workers: usize },
    Metal,
    Cuda,
}

/// Validator-derived lower bound between timer-clocked source emission opportunities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateSourceLookahead {
    pub source: NodeId,
    pub flow: crate::FlowId,
    pub lower_bound_ns: u64,
}

/// Reports the per-source pacing bounds consumed by validation.
pub fn rate_source_lookahead(image: &SimulationImage) -> Vec<RateSourceLookahead> {
    image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
        .flat_map(|owner| {
            image.host_states[owner.state_slot as usize]
                .generators
                .iter()
                .filter_map(|generator| {
                    let FlowGeneratorKind::Rate(rate) = generator.kind else {
                        return None;
                    };
                    Some(RateSourceLookahead {
                        source: owner.id,
                        flow: generator.flow,
                        lower_bound_ns: rate.pacing_interval_ns,
                    })
                })
        })
        .collect()
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
    validate_backend_capabilities(image, backend)?;
    let derived_delays = validate_packets_and_derive_delays(image)?;
    validate_tcp_segment_ledger(image)?;
    validate_owned_service_state(image, backend)?;
    let pfc_channels = validate_pfc(image)?;
    validate_channels(image, backend, &derived_delays, &pfc_channels)?;
    validate_events(image, &pfc_channels)?;
    validate_global_time_capacity(image, backend)?;
    validate_service_event_consistency(image)?;
    let future_work = validate_counters(image)?;
    validate_origin_sequences(image, &future_work)?;
    validate_payload_sequences(image, &future_work)?;
    validate_preloaded_arrival_capacity(image)?;
    validate_initial_payload_positions(image)?;
    Ok(())
}

fn validate_backend_capabilities(
    image: &SimulationImage,
    backend: Backend,
) -> Result<(), ValidationError> {
    if !matches!(backend, Backend::Metal | Backend::Cuda) {
        return Ok(());
    }
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        if matches!(generator.kind, FlowGeneratorKind::Rate(_)) {
            return Err(ValidationError::new(format!(
                "backend {backend} does not support rate-based sources; use Scalar or Cpu"
            )));
        }
    }
    for packet in &image.initial_packets {
        if packet.ecn_marked {
            return Err(ValidationError::new(format!(
                "backend {backend} does not support the ECN-marked packet plane; use Scalar or Cpu"
            )));
        }
        if matches!(packet.kind, PacketKind::Pfc(_)) {
            return Err(ValidationError::new(format!(
                "backend {backend} does not support PFC control payloads; use Scalar or Cpu"
            )));
        }
    }
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        if queue.pfc.is_some() {
            return Err(ValidationError::new(format!(
                "backend {backend} does not support PFC per-priority link pause; use Scalar or Cpu"
            )));
        }
        match queue.drop_mark {
            crate::DropMarkPolicy::TailDrop => {}
            crate::DropMarkPolicy::EcnThreshold(_) => {
                return Err(ValidationError::new(format!(
                    "backend {backend} does not support ECN threshold marking; use Scalar or Cpu"
                )));
            }
            crate::DropMarkPolicy::Red(_) => {
                return Err(ValidationError::new(format!(
                    "backend {backend} does not support RED admission; use Scalar or Cpu"
                )));
            }
        }
        match queue.scheduler {
            SchedulerKind::DeficitRoundRobin(_) => {
                return Err(ValidationError::new(format!(
                    "backend {backend} does not support DRR scheduling; use Scalar or Cpu"
                )));
            }
            SchedulerKind::WeightedRoundRobin(_) => {
                return Err(ValidationError::new(format!(
                    "backend {backend} does not support WRR scheduling; use Scalar or Cpu"
                )));
            }
            SchedulerKind::Fifo
            | SchedulerKind::StaticPriority { .. }
            | SchedulerKind::WeightedFairQueue(_) => {}
        }
    }
    Ok(())
}

const fn congestion_control_mss_bytes(control: crate::TcpCongestionControl) -> u64 {
    match control {
        crate::TcpCongestionControl::Reno(state) => state.mss_bytes,
        crate::TcpCongestionControl::Cubic(state) => state.mss_bytes,
    }
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
        if flow.priority > 7 {
            return Err(ValidationError::new(format!(
                "flow {:?} priority {} is outside IEEE 802.1Q range 0..=7",
                flow.id, flow.priority
            )));
        }
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
        validate_route(image, flow, &flow.route, flow.source, flow.target, "route")?;
        if !flow.reverse_route.is_empty() {
            validate_route(
                image,
                flow,
                &flow.reverse_route,
                flow.target,
                flow.source,
                "reverse route",
            )?;
        }
    }
    Ok(())
}

fn validate_route(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    route: &[LinkId],
    start: NodeId,
    terminal: NodeId,
    label: &str,
) -> Result<(), ValidationError> {
    for (step, link_id) in route.iter().enumerate() {
        if link(image, *link_id).is_none() {
            return Err(ValidationError::new(format!(
                "flow {:?} {label} step {step} references unknown link {link_id:?}",
                flow.id,
            )));
        }
    }

    let mut expected_source = start;
    let mut visited = BTreeSet::from([physical_location(image, start)]);
    for (step, link_id) in route.iter().enumerate() {
        let link = link(image, *link_id).expect("route links were resolved before validation");
        if link.source != expected_source {
            return Err(ValidationError::new(format!(
                "flow {:?} {label} step {step} link {:?} starts at {:?}, expected {:?}",
                flow.id, link.id, link.source, expected_source,
            )));
        }
        if step > 0 {
            let interior =
                node(image, link.source).expect("link validation established every route source");
            if interior.kind != NodeKind::Switch {
                return Err(ValidationError::new(format!(
                    "flow {:?} {label} step {step} uses interior {:?} node {:?}; only Switch nodes may forward",
                    flow.id, interior.kind, interior.id,
                )));
            }
        }
        let direct_target = route_target(image, route, step, terminal)
            .expect("flow route lookup established the next link");
        let physical_target = physical_location(image, link.target);
        if step + 1 < route.len() && physical_target != physical_location(image, direct_target) {
            return Err(ValidationError::new(format!(
                "flow {:?} {label} step {step} physical link {:?} reaches {:?}, but next egress LP {:?} belongs to a different physical node",
                flow.id, link.id, link.target, direct_target,
            )));
        }
        if step + 1 == route.len() && link.target != terminal {
            return Err(ValidationError::new(format!(
                "flow {:?} {label} ends at node {:?}, expected {:?}",
                flow.id, link.target, terminal,
            )));
        }
        if !visited.insert(physical_target) {
            return Err(ValidationError::new(format!(
                "flow {:?} {label} revisits physical node {:?} at step {step}",
                flow.id, link.target,
            )));
        }
        if step + 1 < route.len() {
            let interior = node(image, direct_target)
                .expect("link validation established the direct route target");
            if interior.kind != NodeKind::Switch {
                return Err(ValidationError::new(format!(
                    "flow {:?} {label} reaches interior {:?} node {:?} at step {step}; only Switch nodes may forward",
                    flow.id, interior.kind, interior.id,
                )));
            }
        }
        expected_source = direct_target;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PhysicalLocation {
    Host(NodeId),
    Switch(u64),
}

fn physical_location(image: &SimulationImage, id: NodeId) -> PhysicalLocation {
    let descriptor = node(image, id).expect("link and flow validation established the node");
    match descriptor.kind {
        NodeKind::Host => PhysicalLocation::Host(id),
        NodeKind::Switch => PhysicalLocation::Switch(
            image.switch_states[descriptor.state_slot as usize].physical_switch,
        ),
    }
}

fn derived_pfc_max_frame_bytes(
    image: &SimulationImage,
    controlled_link: LinkId,
) -> Result<u64, ValidationError> {
    let resident_maximum = image
        .initial_packets
        .iter()
        .filter(|packet| !matches!(packet.kind, PacketKind::Pfc(_)))
        .filter(|packet| {
            let flow = flow(image, packet.flow).expect("packet validation established the flow");
            packet_route(flow, packet.kind).contains(&controlled_link)
        })
        .map(|packet| packet.size_bytes)
        .max()
        .unwrap_or(0);

    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .try_fold(resident_maximum, |maximum, generator| {
            let flow =
                flow(image, generator.flow).expect("generator validation established the flow");
            if !flow.route.contains(&controlled_link)
                || executable_generator_packets(generator)? == 0
            {
                return Ok(maximum);
            }
            let size = match generator.kind {
                FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
                FlowGeneratorKind::Tcp(tcp) => tcp.mss_bytes,
                FlowGeneratorKind::Rate(rate) => rate.packet_size_bytes,
            };
            Ok(maximum.max(size))
        })
}

fn validate_pfc(image: &SimulationImage) -> Result<BTreeSet<usize>, ValidationError> {
    let mut control_lanes = BTreeSet::new();
    let mut controlled_priorities = BTreeSet::<(LinkId, u8)>::new();
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        let state = &image.switch_states[owner.state_slot as usize];
        for (queue_index, queue) in state.queues.iter().enumerate() {
            let Some(pfc) = &queue.pfc else {
                continue;
            };
            for ingress in &pfc.ingresses {
                let controlled = link(image, ingress.controlled_link).ok_or_else(|| {
                ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} PFC ingress references unknown controlled link {:?}",
                    owner.id, ingress.controlled_link
                ))
            })?;
                if physical_location(image, controlled.target) != physical_location(image, owner.id)
                {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} PFC ingress controls link {:?}, which terminates at a different physical switch",
                        owner.id, controlled.id
                    )));
                }
                let channel_index = ingress.control_channel_index as usize;
                let channel = image.channels.get(channel_index).ok_or_else(|| {
                ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} PFC ingress references unknown control channel {channel_index}",
                    owner.id
                ))
            })?;
                if !control_lanes.insert(channel_index) {
                    return Err(ValidationError::new(format!(
                        "PFC control channel {channel_index} is referenced by more than one ingress monitor"
                    )));
                }
                let reverse = link(image, channel.link).ok_or_else(|| {
                    ValidationError::new(format!(
                        "PFC control channel {channel_index} references unknown reverse link {:?}",
                        channel.link
                    ))
                })?;
                if channel.event_kind != EventKind::RemoteArrival
                    || channel.source != owner.id
                    || channel.target != controlled.source
                {
                    return Err(ValidationError::new(format!(
                        "PFC control channel {channel_index} must be RemoteArrival from downstream LP {:?} to controlled-link owner {:?}",
                        owner.id, controlled.source
                    )));
                }
                if physical_location(image, reverse.source) != physical_location(image, owner.id)
                    || physical_location(image, reverse.target)
                        != physical_location(image, controlled.source)
                {
                    return Err(ValidationError::new(format!(
                        "PFC control channel {channel_index} reverse link {:?} does not connect the downstream and upstream physical switches",
                        reverse.id
                    )));
                }
                let reverse_delay = reverse.delay_ns(64).map_err(|error| {
                    ValidationError::new(format!(
                        "PFC control channel {channel_index} 64-byte delay overflows: {error}"
                    ))
                })?;
                if channel.min_delay_ns != reverse_delay {
                    return Err(ValidationError::new(format!(
                        "PFC control channel {channel_index} min_delay_ns {} must equal exact 64-byte reverse-link delay {reverse_delay}",
                        channel.min_delay_ns
                    )));
                }
                if ingress.max_frame_bytes == 0 {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} PFC maximum frame size must be positive",
                        owner.id
                    )));
                }
                let reachable_frame_bytes = derived_pfc_max_frame_bytes(image, controlled.id)?;
                if ingress.max_frame_bytes < reachable_frame_bytes {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} PFC maximum frame bound {} is below reachable frame size {reachable_frame_bytes} on controlled link {:?}",
                        owner.id, ingress.max_frame_bytes, controlled.id
                    )));
                }

                let reaction_ns = u128::from(controlled.propagation_ns)
                    .checked_add(u128::from(reverse_delay))
                    .ok_or_else(|| ValidationError::new("PFC reaction window overflows u128"))?;
                let line_numerator = u128::from(controlled.rate_bps)
                    .checked_mul(reaction_ns)
                    .ok_or_else(|| {
                        ValidationError::new("PFC line-rate headroom product overflows u128")
                    })?;
                let line_bytes = line_numerator
                    .checked_add(8_000_000_000_u128 - 1)
                    .ok_or_else(|| ValidationError::new("PFC headroom rounding overflows u128"))?
                    / 8_000_000_000_u128;
                let required_headroom = u128::from(ingress.max_frame_bytes - 1)
                    .checked_add(line_bytes)
                    .and_then(|value| value.checked_add(u128::from(ingress.max_frame_bytes)))
                    .ok_or_else(|| ValidationError::new("PFC required headroom overflows u128"))?;

                let mut derived_occupancy = [0_u64; 8];
                for payload in &queue.queue {
                    let packet = packet(image, *payload)
                        .expect("owned-state validation established queue payloads");
                    if packet_incoming_link_at(image, packet, owner.id)
                        == Some(ingress.controlled_link)
                    {
                        let priority = usize::from(
                            flow(image, packet.flow)
                                .expect("packet validation established flow")
                                .priority,
                        );
                        derived_occupancy[priority] = derived_occupancy[priority]
                        .checked_add(packet.size_bytes)
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "switch node {:?} queue {queue_index} PFC derived occupancy overflows",
                                owner.id
                            ))
                        })?;
                    }
                }
                if ingress.occupancy_bytes != derived_occupancy {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} PFC occupancy {:?} does not equal derived waiting bytes {:?}",
                        owner.id, ingress.occupancy_bytes, derived_occupancy
                    )));
                }
                for priority in 0..8 {
                    let xoff = ingress.xoff_threshold_bytes[priority];
                    let xon = ingress.xon_threshold_bytes[priority];
                    let occupancy = ingress.occupancy_bytes[priority];
                    let capacity = ingress.buffer_capacity_bytes[priority];
                    if xoff == 0 {
                        if xon != 0
                            || capacity != 0
                            || occupancy != 0
                            || ingress.pause_asserted[priority]
                        {
                            return Err(ValidationError::new(format!(
                                "switch node {:?} queue {queue_index} disabled PFC priority {priority} must have zero XON, capacity, occupancy, and pause state",
                                owner.id
                            )));
                        }
                        continue;
                    }
                    controlled_priorities.insert((controlled.id, priority as u8));
                    if capacity == 0 || xon > xoff || xoff > capacity {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} requires XON <= XOFF <= capacity, got {xon} <= {xoff} <= {}",
                            owner.id, capacity
                        )));
                    }
                    if occupancy > capacity {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} occupancy {occupancy} exceeds capacity {}",
                            owner.id, capacity
                        )));
                    }
                    if ingress.pause_asserted[priority] && occupancy <= xon {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} is asserted at occupancy {occupancy}, which is at or below XON {xon}",
                            owner.id
                        )));
                    }
                    if !ingress.pause_asserted[priority] && occupancy >= xoff {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} is unasserted at occupancy {occupancy}, at or above XOFF {xoff}",
                            owner.id
                        )));
                    }
                    let available = u128::from(capacity - xoff);
                    if available < required_headroom {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} has {available} bytes of headroom, below derived requirement {required_headroom}",
                            owner.id
                        )));
                    }
                }
            }
        }
    }

    validate_pfc_deadlock_scope(image, &controlled_priorities)?;

    let pfc_events = image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::RemoteArrival)
        .fold(BTreeMap::<PayloadId, usize>::new(), |mut counts, event| {
            *counts.entry(event.payload).or_default() += 1;
            counts
        });
    for packet in image
        .initial_packets
        .iter()
        .filter(|packet| matches!(packet.kind, PacketKind::Pfc(_)))
    {
        if pfc_events.get(&packet.id).copied() != Some(1) {
            return Err(ValidationError::new(format!(
                "initial PFC packet {:?} must own exactly one RemoteArrival event",
                packet.id
            )));
        }
    }
    Ok(control_lanes)
}

fn validate_pfc_deadlock_scope(
    image: &SimulationImage,
    controlled: &BTreeSet<(LinkId, u8)>,
) -> Result<(), ValidationError> {
    let mut edges = BTreeMap::<(LinkId, u8), BTreeSet<(LinkId, u8)>>::new();
    let mut indegree = BTreeMap::<(LinkId, u8), usize>::new();
    for vertex in controlled {
        edges.entry(*vertex).or_default();
        indegree.entry(*vertex).or_default();
    }
    for flow in &image.flows {
        for pair in flow.route.windows(2) {
            let from = (pair[0], flow.priority);
            let to = (pair[1], flow.priority);
            if controlled.contains(&from)
                && controlled.contains(&to)
                && edges.entry(from).or_default().insert(to)
            {
                *indegree.entry(to).or_default() += 1;
            }
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(vertex, degree)| (*degree == 0).then_some(*vertex))
        .collect::<BTreeSet<_>>();
    let mut visited = 0_usize;
    while let Some(vertex) = ready.pop_first() {
        visited += 1;
        for target in edges.get(&vertex).into_iter().flatten() {
            let degree = indegree
                .get_mut(target)
                .expect("PFC graph contains every edge target");
            *degree -= 1;
            if *degree == 0 {
                ready.insert(*target);
            }
        }
    }
    if visited != indegree.len() {
        let cycle = indegree
            .iter()
            .filter_map(|(vertex, degree)| (*degree != 0).then_some(*vertex))
            .collect::<Vec<_>>();
        return Err(ValidationError::new(format!(
            "PFC circular pause dependency rejected by static deadlock scope: {cycle:?}"
        )));
    }
    Ok(())
}

fn pfc_channel_controls(image: &SimulationImage, channel_index: usize) -> Option<LinkId> {
    image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .flat_map(|queue| {
            queue
                .pfc
                .as_ref()
                .into_iter()
                .flat_map(|pfc| &pfc.ingresses)
        })
        .find(|ingress| ingress.control_channel_index as usize == channel_index)
        .map(|ingress| ingress.controlled_link)
}

fn route_target(
    image: &SimulationImage,
    route: &[LinkId],
    index: usize,
    terminal: NodeId,
) -> Option<NodeId> {
    route
        .get(index + 1)
        .and_then(|next| link(image, *next))
        .map(|next| next.source)
        .or_else(|| (index + 1 == route.len()).then_some(terminal))
}

fn validate_generators(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut arrival_counts = BTreeMap::<(NodeId, PayloadId, u64), usize>::new();
    let mut pacing_counts = BTreeMap::<(NodeId, PayloadId, u64), usize>::new();
    for event in image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacketArrival)
    {
        *arrival_counts
            .entry((event.target, event.payload, event.key.time_ns))
            .or_default() += 1;
    }
    for event in image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacingTimer)
    {
        *pacing_counts
            .entry((event.target, event.payload, event.key.time_ns))
            .or_default() += 1;
    }
    let mut owners =
        BTreeMap::<crate::FlowId, (NodeId, GeneratorStatus, PayloadId, u64, bool)>::new();
    let mut receiver_owners = BTreeMap::<crate::FlowId, NodeId>::new();
    let mut claimed_tcp_timers = BTreeMap::<(NodeId, PayloadId, u64), crate::FlowId>::new();
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let mut previous_receiver = None;
        for receiver in &state.tcp_receivers {
            if previous_receiver.is_some_and(|flow| flow >= receiver.flow) {
                return Err(ValidationError::new(format!(
                    "host node {:?} has duplicate receiver state or non-increasing TCP receiver flow {:?}",
                    owner.id, receiver.flow
                )));
            }
            previous_receiver = Some(receiver.flow);
            let receiver_flow = flow(image, receiver.flow).ok_or_else(|| {
                ValidationError::new(format!(
                    "host node {:?} TCP receiver references unknown flow {:?}",
                    owner.id, receiver.flow
                ))
            })?;
            if receiver_flow.target != owner.id {
                return Err(ValidationError::new(format!(
                    "host node {:?} owns TCP receiver for flow {:?}, but the flow target is {:?}",
                    owner.id, receiver.flow, receiver_flow.target
                )));
            }
            if receiver.ack_size_bytes == 0 {
                return Err(ValidationError::new(format!(
                    "flow {:?} TCP receiver ACK size must be positive",
                    receiver.flow
                )));
            }
            if receiver_owners.insert(receiver.flow, owner.id).is_some() {
                return Err(ValidationError::new(format!(
                    "flow {:?} has duplicate receiver state",
                    receiver.flow
                )));
            }
        }
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
                    matches!(generator.kind, FlowGeneratorKind::Tcp(_)),
                ),
            ) {
                return Err(ValidationError::new(format!(
                    "flow {:?} generator is owned by both node {first:?} and node {:?}",
                    flow.id, owner.id
                )));
            }

            if let FlowGeneratorKind::Tcp(tcp) = generator.kind {
                if tcp.total_bytes == 0 || tcp.mss_bytes == 0 || tcp.ack_size_bytes == 0 {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP total_bytes, mss_bytes, and ack_size_bytes must be positive",
                        flow.id
                    )));
                }
                if tcp.rto_ns == 0 || tcp.active_timer.is_some_and(|timer| timer.rto_ns == 0) {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP retransmission timeout must be positive",
                        flow.id
                    )));
                }
                let control_mss_bytes = congestion_control_mss_bytes(tcp.control);
                if tcp.mss_bytes != control_mss_bytes {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP generator MSS {} does not match {} controller MSS {}",
                        flow.id,
                        tcp.mss_bytes,
                        tcp.control.label(),
                        control_mss_bytes
                    )));
                }
                let remaining = remaining_generator_packets(generator)?;
                validate_tcp_timer_capacity(image, generator, tcp)?;
                if tcp.highest_ack > tcp.next_sequence
                    || tcp.bytes_in_flight != tcp.next_sequence - tcp.highest_ack
                    || generator.feedback.outstanding_bytes != tcp.bytes_in_flight
                    || generator.feedback.unacknowledged_bytes != tcp.bytes_in_flight
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP acknowledgment, flight, and feedback byte state is inconsistent",
                        flow.id
                    )));
                }
                // Partial-ACK recovery may request a retransmission at the new cumulative ACK.
                // Keeping both recovery bounds inside the sent prefix guarantees that the
                // normalized ledger below contains that sequence whenever it is referenced.
                if tcp.control.phase() == crate::TcpPhase::FastRecovery
                    && (tcp.recovery_high_sequence > tcp.next_sequence
                        || tcp.control.recovery_high_sequence() > tcp.next_sequence)
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP recovery can retransmit beyond next sequence {}",
                        flow.id, tcp.next_sequence
                    )));
                }
                match generator.next_emission.status {
                    GeneratorStatus::Scheduled if remaining == 0 => {
                        return Err(ValidationError::new(format!(
                            "flow {:?} has a scheduled emission after its TCP generator finished",
                            flow.id
                        )));
                    }
                    GeneratorStatus::Scheduled if tcp.active_timer.is_some() => {
                        return Err(ValidationError::new(format!(
                            "flow {:?} TCP generator is Scheduled with an active retransmission timer",
                            flow.id
                        )));
                    }
                    GeneratorStatus::Finished
                        if remaining != 0
                            || tcp.highest_ack != tcp.total_bytes
                            || tcp.bytes_in_flight != 0 =>
                    {
                        return Err(ValidationError::new(format!(
                            "flow {:?} TCP generator is Finished before all bytes are acknowledged",
                            flow.id
                        )));
                    }
                    GeneratorStatus::Finished if tcp.active_timer.is_some() => {
                        return Err(ValidationError::new(format!(
                            "flow {:?} TCP generator is Finished with an active retransmission timer",
                            flow.id
                        )));
                    }
                    GeneratorStatus::Stopped => {
                        return Err(ValidationError::new(format!(
                            "flow {:?} TCP generator cannot use the open-loop Stopped state",
                            flow.id
                        )));
                    }
                    GeneratorStatus::Blocked => {
                        validate_blocked_tcp_timer(image, owner.id, flow.id, tcp)?;
                        let timer = tcp
                            .active_timer
                            .expect("blocked timer validation established an active timer");
                        if let Some(first_flow) = claimed_tcp_timers
                            .insert((owner.id, timer.attempt, timer.deadline_ns), flow.id)
                        {
                            return Err(ValidationError::new(format!(
                                "flows {first_flow:?} and {:?} TCP active retransmission timers share event identity at node {:?}, payload {:?}, deadline {}",
                                flow.id, owner.id, timer.attempt, timer.deadline_ns
                            )));
                        }
                    }
                    GeneratorStatus::Scheduled | GeneratorStatus::Finished => {}
                }
                if flow.reverse_route.is_empty() {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP generator requires a reverse ACK route",
                        flow.id
                    )));
                }
                let target = node(image, flow.target).expect("flow validation established target");
                let receiver = image.host_states[target.state_slot as usize]
                    .tcp_receivers
                    .iter()
                    .find(|receiver| receiver.flow == flow.id)
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} TCP generator has no receiver state at target node {:?}",
                            flow.id, flow.target
                        ))
                    })?;
                if receiver.ack_size_bytes != tcp.ack_size_bytes {
                    return Err(ValidationError::new(format!(
                        "flow {:?} TCP receiver ACK size {} does not match generator ACK size {}",
                        flow.id, receiver.ack_size_bytes, tcp.ack_size_bytes
                    )));
                }
                if generator.next_emission.status == GeneratorStatus::Scheduled {
                    let packet =
                        packet(image, generator.next_emission.payload).ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} scheduled emission references unknown packet {:?}",
                                flow.id, generator.next_emission.payload
                            ))
                        })?;
                    let PacketKind::TcpData(header) = packet.kind else {
                        return Err(ValidationError::new(format!(
                            "flow {:?} scheduled packet {:?} is {:?}, expected TcpData",
                            flow.id, packet.id, packet.kind
                        )));
                    };
                    if header.sequence != tcp.next_sequence
                        || header.sent_time_ns != generator.next_emission.departure_time_ns
                        || header.retransmission
                    {
                        return Err(ValidationError::new(format!(
                            "flow {:?} scheduled TCP packet {:?} metadata does not match generator state",
                            flow.id, packet.id
                        )));
                    }
                    if packet.size_bytes != tcp.mss_bytes.min(tcp.total_bytes - tcp.next_sequence) {
                        return Err(ValidationError::new(format!(
                            "flow {:?} scheduled TCP packet {:?} has invalid segment size {}",
                            flow.id, packet.id, packet.size_bytes
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
                    validate_scheduled_payload_sequence(
                        image,
                        owner,
                        state,
                        packet.id,
                        flow.id,
                        generator.packets_emitted,
                    )?;
                }
                continue;
            }

            if let FlowGeneratorKind::Rate(rate) = generator.kind {
                if rate.first_pacing_time_ns == 0
                    || rate.pacing_interval_ns == 0
                    || rate.packet_size_bytes == 0
                    || rate.total_bytes == 0
                    || rate.rate_numerator_bits_per_second == 0
                    || rate.rate_denominator == 0
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source requires positive first pacing time, interval, packet size, total bytes, and rational rate",
                        flow.id
                    )));
                }
                if gcd_u64(rate.rate_numerator_bits_per_second, rate.rate_denominator) != 1 {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source rational {}/{} is not canonical",
                        flow.id, rate.rate_numerator_bits_per_second, rate.rate_denominator
                    )));
                }
                if generator.bytes_emitted > rate.total_bytes {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source emitted bytes {} exceed total bytes {}",
                        flow.id, generator.bytes_emitted, rate.total_bytes
                    )));
                }
                let scale = u128::from(rate.rate_denominator)
                    .checked_mul(1_000_000_000)
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} rate source credit scale exceeds u128",
                            flow.id
                        ))
                    })?;
                let tick_credit = u128::from(rate.rate_numerator_bits_per_second)
                    .checked_mul(u128::from(rate.pacing_interval_ns))
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} rate source tick credit exceeds u128",
                            flow.id
                        ))
                    })?;
                let full_packet_cost = u128::from(rate.packet_size_bytes)
                    .checked_mul(8)
                    .and_then(|bits| bits.checked_mul(scale))
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} rate source packet credit cost exceeds u128",
                            flow.id
                        ))
                    })?;
                if tick_credit > full_packet_cost {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source adds {tick_credit} credit quanta per tick, exceeding one full-packet cost {full_packet_cost}",
                        flow.id
                    )));
                }
                if full_packet_cost
                    .saturating_sub(1)
                    .checked_add(tick_credit)
                    .is_none()
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source credit plus one tick can exceed u128",
                        flow.id
                    )));
                }
                let remaining_bytes = rate.total_bytes - generator.bytes_emitted;
                let active = matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                );
                if active != (remaining_bytes != 0)
                    && generator.next_emission.status != GeneratorStatus::Stopped
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source status {:?} is inconsistent with {remaining_bytes} remaining bytes",
                        flow.id, generator.next_emission.status
                    )));
                }
                if generator.next_emission.status == GeneratorStatus::Finished
                    && remaining_bytes != 0
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source is Finished with {remaining_bytes} bytes remaining",
                        flow.id
                    )));
                }
                if active {
                    let packet =
                        packet(image, generator.next_emission.payload).ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} rate source pacing timer references unknown packet {:?}",
                                flow.id, generator.next_emission.payload
                            ))
                        })?;
                    let expected_size = rate.packet_size_bytes.min(remaining_bytes);
                    if packet.flow != flow.id
                        || packet.kind != PacketKind::Data
                        || packet.size_bytes != expected_size
                    {
                        return Err(ValidationError::new(format!(
                            "flow {:?} rate source pacing token {:?} does not match its next data packet",
                            flow.id, packet.id
                        )));
                    }
                    let packet_cost = u128::from(expected_size)
                        .checked_mul(8)
                        .and_then(|bits| bits.checked_mul(scale))
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} rate source next-packet credit cost exceeds u128",
                                flow.id
                            ))
                        })?;
                    if rate.credit_quanta >= packet_cost {
                        return Err(ValidationError::new(format!(
                            "flow {:?} rate source credit {} is not below next-packet cost {packet_cost}",
                            flow.id, rate.credit_quanta
                        )));
                    }
                    let next_credit =
                        rate.credit_quanta.checked_add(tick_credit).ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} rate source next pacing credit exceeds u128",
                                flow.id
                            ))
                        })?;
                    let can_emit = next_credit >= packet_cost;
                    let expected_status = if can_emit {
                        GeneratorStatus::Scheduled
                    } else {
                        GeneratorStatus::Blocked
                    };
                    if generator.next_emission.status != expected_status {
                        return Err(ValidationError::new(format!(
                            "flow {:?} rate source timer status {:?} disagrees with next-tick credit (expected {:?})",
                            flow.id, generator.next_emission.status, expected_status
                        )));
                    }
                    let deadline = generator.next_emission.departure_time_ns;
                    if deadline < rate.first_pacing_time_ns
                        || (deadline - rate.first_pacing_time_ns) % rate.pacing_interval_ns != 0
                    {
                        return Err(ValidationError::new(format!(
                            "flow {:?} rate source pacing deadline {deadline} is off its interval grid",
                            flow.id
                        )));
                    }
                    let matches = pacing_counts
                        .get(&(owner.id, packet.id, deadline))
                        .copied()
                        .unwrap_or(0);
                    if matches != 1 {
                        return Err(ValidationError::new(format!(
                            "flow {:?} rate source has {matches} matching PacingTimer events; expected 1",
                            flow.id
                        )));
                    }
                    validate_scheduled_payload_sequence(
                        image,
                        owner,
                        state,
                        packet.id,
                        flow.id,
                        generator.packets_emitted,
                    )?;
                } else if generator.next_emission.status == GeneratorStatus::Stopped
                    && generator.next_emission.departure_time_ns <= image.stop_time_ns
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source is Stopped at {}, which is not beyond stop time {}",
                        flow.id, generator.next_emission.departure_time_ns, image.stop_time_ns
                    )));
                }
                continue;
            }

            let FlowGeneratorKind::Constant(constant) = generator.kind else {
                unreachable!("TCP and rate generators continue above")
            };
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
                validate_scheduled_payload_sequence(
                    image,
                    owner,
                    state,
                    packet.id,
                    flow.id,
                    generator.packets_emitted,
                )?;
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
    for (receiver_flow, receiver_owner) in receiver_owners {
        match owners.get(&receiver_flow) {
            None => {
                return Err(ValidationError::new(format!(
                    "host node {receiver_owner:?} owns TCP receiver state for flow {receiver_flow:?}, but the flow has no generator"
                )));
            }
            Some((.., false)) => {
                return Err(ValidationError::new(format!(
                    "host node {receiver_owner:?} owns TCP receiver state for flow {receiver_flow:?}, but the flow generator is not TCP"
                )));
            }
            Some((.., true)) => {}
        }
    }
    for packet in image
        .initial_packets
        .iter()
        .filter(|packet| matches!(packet.kind, PacketKind::TcpData(_) | PacketKind::TcpAck(_)))
    {
        if !owners.get(&packet.flow).is_some_and(|(.., is_tcp)| *is_tcp) {
            return Err(ValidationError::new(format!(
                "TCP packet {:?} for flow {:?} requires a TCP generator",
                packet.id, packet.flow
            )));
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
        if let Some((owner, status, payload, time_ns, _)) = owners.get(&packet.flow) {
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

fn validate_tcp_segment_ledger(image: &SimulationImage) -> Result<(), ValidationError> {
    let ledger = crate::tcp_ledger::seed_image(image).map_err(|conflict| {
        ValidationError::new(format!(
            "TCP flow {:?} sequence {} changed segment size from {} to {} bytes",
            conflict.flow,
            conflict.sequence,
            conflict.original_size_bytes,
            conflict.replacement_size_bytes
        ))
    })?;
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
            continue;
        };
        for packet in image
            .initial_packets
            .iter()
            .filter(|packet| packet.flow == generator.flow)
        {
            let PacketKind::TcpData(header) = packet.kind else {
                continue;
            };
            if header.sequence < tcp.next_sequence
                || generator.next_emission.status == GeneratorStatus::Scheduled
                    && packet.id == generator.next_emission.payload
                    && header.sequence == tcp.next_sequence
            {
                continue;
            }
            return Err(ValidationError::new(format!(
                "flow {:?} TCP segment ledger has unexpected initial segment at sequence {} at or beyond next sequence {}",
                generator.flow, header.sequence, tcp.next_sequence
            )));
        }
        let mut expected_sequence = tcp.highest_ack;
        if let Some(segments) = ledger.get(&generator.flow) {
            for (&sequence, packet) in segments.range(tcp.highest_ack..tcp.next_sequence) {
                if sequence != expected_sequence {
                    return Err(incomplete_tcp_segment_ledger(
                        generator.flow,
                        tcp.highest_ack,
                        tcp.next_sequence,
                        expected_sequence,
                    ));
                }
                expected_sequence = sequence.checked_add(packet.size_bytes).ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} TCP segment at sequence {sequence} overflows the byte sequence domain",
                        generator.flow
                    ))
                })?;
                if expected_sequence > tcp.next_sequence {
                    return Err(incomplete_tcp_segment_ledger(
                        generator.flow,
                        tcp.highest_ack,
                        tcp.next_sequence,
                        sequence,
                    ));
                }
            }
        }
        if expected_sequence != tcp.next_sequence {
            return Err(incomplete_tcp_segment_ledger(
                generator.flow,
                tcp.highest_ack,
                tcp.next_sequence,
                expected_sequence,
            ));
        }
    }
    Ok(())
}

fn incomplete_tcp_segment_ledger(
    flow: crate::FlowId,
    highest_ack: u64,
    next_sequence: u64,
    expected_sequence: u64,
) -> ValidationError {
    ValidationError::new(format!(
        "flow {flow:?} TCP segment ledger does not cover unacknowledged byte range {highest_ack}..{next_sequence}; expected segment at sequence {expected_sequence}"
    ))
}

fn validate_blocked_tcp_timer(
    image: &SimulationImage,
    owner: NodeId,
    flow: crate::FlowId,
    tcp: crate::TcpGenerator,
) -> Result<(), ValidationError> {
    let timer = tcp.active_timer.ok_or_else(|| {
        ValidationError::new(format!(
            "flow {flow:?} TCP generator is Blocked without an active retransmission timer"
        ))
    })?;
    if tcp.bytes_in_flight == 0 {
        return Err(ValidationError::new(format!(
            "flow {flow:?} TCP generator is Blocked without unacknowledged data"
        )));
    }
    if timer.sequence != tcp.highest_ack || timer.generation != tcp.timer_generation {
        return Err(ValidationError::new(format!(
            "flow {flow:?} TCP active retransmission timer is inconsistent with sender state"
        )));
    }
    // The timer attempt identifies its pending event. It can legitimately precede last_attempt:
    // later duplicate ACKs may fill the window without replacing the oldest active timer.
    let state_slot = node(image, owner)
        .expect("generator validation established owner")
        .state_slot as usize;
    let next_payload_seq = image.host_states[state_slot].next_payload_seq;
    let node_count = u64::try_from(image.nodes.len()).unwrap_or(u64::MAX);
    let attempt_sequence = timer
        .attempt
        .0
        .checked_sub(owner.0)
        .filter(|offset| node_count != 0 && offset % node_count == 0)
        .map(|offset| offset / node_count);
    if attempt_sequence.is_none_or(|sequence| sequence >= next_payload_seq) {
        return Err(ValidationError::new(format!(
            "flow {flow:?} TCP active retransmission timer attempt {:?} was not allocated by source node {owner:?}",
            timer.attempt
        )));
    }
    let matching_events = image
        .initial_events
        .iter()
        .filter(|event| {
            event.kind == EventKind::RetransmissionTimeout
                && event.target == owner
                && event.payload == timer.attempt
                && event.key.time_ns == timer.deadline_ns
        })
        .count();
    // Timeout events do not encode a generation. When an ACK replaces a timer with the same
    // attempt and deadline, the first indistinguishable event consumes the current timer and the
    // remaining events are stale runtime no-ops. The state-generation check above identifies the
    // live generation, so validation only needs one event with its executable identity.
    if matching_events == 0 {
        return Err(ValidationError::new(format!(
            "flow {flow:?} TCP active retransmission timer has {matching_events} matching events; expected 1"
        )));
    }
    Ok(())
}

fn validate_scheduled_payload_sequence(
    image: &SimulationImage,
    owner: &NodeDescriptor,
    state: &crate::HostState,
    payload: PayloadId,
    flow: crate::FlowId,
    packets_emitted: u64,
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
    if sequence < packets_emitted {
        return Err(ValidationError::new(format!(
            "flow {flow:?} scheduled payload {payload:?} sequence {sequence} was already consumed; generator has emitted {packets_emitted} packets"
        )));
    }
    Ok(())
}

struct DerivedChannelDelays {
    possible: BTreeMap<(LinkId, NodeId), u64>,
    required: BTreeSet<(LinkId, NodeId)>,
}

fn insert_derived_delay(
    delays: &mut BTreeMap<(LinkId, NodeId), u64>,
    route: (LinkId, NodeId),
    delay: u64,
) {
    delays
        .entry(route)
        .and_modify(|minimum| *minimum = (*minimum).min(delay))
        .or_insert(delay);
}

fn validate_packets_and_derive_delays(
    image: &SimulationImage,
) -> Result<DerivedChannelDelays, ValidationError> {
    let live_payloads = crate::tcp_ledger::initial_live_payloads(image);
    let live_tcp_data_flows = image
        .initial_packets
        .iter()
        .filter(|packet| {
            live_payloads.contains(&packet.id) && matches!(packet.kind, PacketKind::TcpData(_))
        })
        .map(|packet| packet.flow)
        .collect::<BTreeSet<_>>();
    let mut possible = BTreeMap::<(LinkId, NodeId), u64>::new();
    let mut required = BTreeSet::<(LinkId, NodeId)>::new();
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
        if let PacketKind::Pfc(header) = packet.kind {
            if packet.size_bytes != 64 {
                return Err(ValidationError::new(format!(
                    "PFC packet {:?} has size {}, expected exactly 64 bytes",
                    packet.id, packet.size_bytes
                )));
            }
            if header.priority > 7 {
                return Err(ValidationError::new(format!(
                    "PFC packet {:?} priority {} is outside 0..=7",
                    packet.id, header.priority
                )));
            }
            continue;
        }
        if packet.kind.is_feedback() {
            let source =
                node(image, flow.source).expect("flow validation established the source node");
            let state = &image.host_states[source.state_slot as usize];
            if !state
                .generators
                .iter()
                .any(|generator| generator.flow == flow.id)
            {
                return Err(ValidationError::new(format!(
                    "feedback packet {:?} for flow {:?} has no generator at source node {:?}",
                    packet.id, flow.id, flow.source
                )));
            }
        }
        let mut cumulative_delay = 0_u64;
        let route = packet_route(flow, packet.kind);
        let terminal = packet_terminal(flow, packet.kind);
        for (index, link_id) in route.iter().enumerate() {
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
            let route = (
                link.id,
                route_target(image, route, index, terminal)
                    .expect("validated route has a direct target"),
            );
            insert_derived_delay(&mut possible, route, delay);
            if live_payloads.contains(&packet.id) {
                required.insert(route);
            }
        }
    }
    for state in &image.host_states {
        for generator in &state.generators {
            let flow = flow(image, generator.flow).expect("generator validation established flow");
            let generator_can_emit = matches!(
                generator.next_emission.status,
                GeneratorStatus::Scheduled | GeneratorStatus::Blocked
            );
            let (packet_size_bytes, route, terminal) = match generator.kind {
                FlowGeneratorKind::Constant(constant) => (
                    constant.packet_size_bytes,
                    flow.route.as_slice(),
                    flow.target,
                ),
                FlowGeneratorKind::Tcp(tcp) => {
                    let ack_can_be_emitted =
                        generator_can_emit || live_tcp_data_flows.contains(&flow.id);
                    for (index, link_id) in flow.reverse_route.iter().enumerate() {
                        let link = link(image, *link_id)
                            .expect("validated reverse route names an existing link");
                        let delay = link.delay_ns(tcp.ack_size_bytes).map_err(|error| {
                            ValidationError::new(format!(
                                "link {:?} delay overflows for flow {:?} TCP ACK: {error}",
                                link.id, flow.id
                            ))
                        })?;
                        let route = (
                            link.id,
                            route_target(image, &flow.reverse_route, index, flow.source)
                                .expect("validated reverse route has a direct target"),
                        );
                        insert_derived_delay(&mut possible, route, delay);
                        if ack_can_be_emitted {
                            required.insert(route);
                        }
                    }
                    // TCP may emit a short final or congestion-window-limited segment.  A
                    // single byte is therefore the conservative lower bound for every future
                    // forward transmission on this route.
                    (1, flow.route.as_slice(), flow.target)
                }
                FlowGeneratorKind::Rate(rate) => {
                    (rate.packet_size_bytes, flow.route.as_slice(), flow.target)
                }
            };
            for (index, link_id) in route.iter().enumerate() {
                let link =
                    link(image, *link_id).expect("validated flow route names an existing link");
                let delay = link.delay_ns(packet_size_bytes).map_err(|error| {
                    ValidationError::new(format!(
                        "link {:?} delay overflows for flow {:?} generator: {error}",
                        link.id, flow.id
                    ))
                })?;
                let route = (
                    link.id,
                    route_target(image, route, index, terminal)
                        .expect("validated route has a direct target"),
                );
                insert_derived_delay(&mut possible, route, delay);
                if generator_can_emit {
                    required.insert(route);
                }
            }
        }
    }
    Ok(DerivedChannelDelays { possible, required })
}

fn validate_owned_service_state(
    image: &SimulationImage,
    backend: Backend,
) -> Result<(), ValidationError> {
    let pending_event_frontier = image
        .initial_events
        .iter()
        .map(|event| event.key.time_ns)
        .min();
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

    let mut switch_egress_owners = BTreeMap::new();
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        let state = &image.switch_states[owner.state_slot as usize];
        if state.queues.len() > 1 {
            return Err(ValidationError::new(format!(
                "switch node {:?} owns {} egress queues; a switch LP may own at most one",
                owner.id,
                state.queues.len()
            )));
        }
        let mut egresses = BTreeSet::new();
        for (queue_index, queue) in state.queues.iter().enumerate() {
            if queue.drop_mark == crate::DropMarkPolicy::TailDrop {
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
            if let Some(first_owner) = switch_egress_owners.insert(egress_id, owner.id) {
                return Err(ValidationError::new(format!(
                    "switch nodes {first_owner:?} and {:?} both own egress link {egress_id:?}",
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
            validate_scheduler_state(image, owner.id, queue_index, queue, pending_event_frontier)?;
            validate_device_scheduler_capability(owner.id, queue_index, queue, backend)?;
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
            let has_eligible_packet = queue.queue.iter().any(|payload| {
                let packet = packet(image, *payload)
                    .expect("owned-state validation established queue payloads");
                let priority = usize::from(
                    flow(image, packet.flow)
                        .expect("packet validation established flow")
                        .priority,
                );
                !queue
                    .pfc
                    .as_ref()
                    .is_some_and(|pfc| pfc.paused_priorities[priority])
            });
            if has_eligible_packet && queue.in_service.is_none() && !queue.tx_ready_pending {
                return Err(ValidationError::new(format!(
                    "switch node {:?} queue {queue_index} has eligible packets but neither active service nor TxReady pending",
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

fn validate_device_scheduler_capability(
    owner: NodeId,
    queue_index: usize,
    queue: &crate::SwitchQueueState,
    backend: Backend,
) -> Result<(), ValidationError> {
    if !matches!(backend, Backend::Metal | Backend::Cuda) {
        return Ok(());
    }
    let SchedulerKind::WeightedFairQueue(state) = &queue.scheduler else {
        return Ok(());
    };

    let total_weight = state
        .weights
        .iter()
        .fold(BigUint::from(0_u8), |sum, weight| {
            sum + BigUint::from(*weight)
        });
    if total_weight > BigUint::from(DEVICE_WFQ_MAX_TOTAL_WEIGHT) {
        return Err(ValidationError::new(format!(
            "switch node {owner:?} queue {queue_index} WFQ total weight {total_weight} makes the 1_000_000_000 * active-weight denominator exceed u64; backend {backend} requires a total weight at most {DEVICE_WFQ_MAX_TOTAL_WEIGHT} (Scalar and Cpu are unbounded)"
        )));
    }

    validate_device_rational(
        owner,
        queue_index,
        backend,
        "virtual time",
        &state.virtual_time,
    )?;
    for (class, finish) in state.finish_times.iter().enumerate() {
        validate_device_rational(
            owner,
            queue_index,
            backend,
            &format!("finish state for class {class}"),
            finish,
        )?;
    }
    for (payload, finish) in &state.packet_finish_times {
        validate_device_rational(
            owner,
            queue_index,
            backend,
            &format!("finish tag for packet {payload:?}"),
            finish,
        )?;
    }
    Ok(())
}

fn validate_device_rational(
    owner: NodeId,
    queue_index: usize,
    backend: Backend,
    field: &str,
    value: &crate::ExactRational,
) -> Result<(), ValidationError> {
    for (component, integer) in [("numerator", value.numer()), ("denominator", value.denom())] {
        let bits = integer.bits();
        if bits > DEVICE_WFQ_BITS {
            return Err(ValidationError::new(format!(
                "switch node {owner:?} queue {queue_index} WFQ {field} {component} requires {bits} bits; backend {backend} exact-rational limit is {DEVICE_WFQ_BITS} bits (Scalar and Cpu are unbounded)"
            )));
        }
    }

    let canonical = crate::ExactRational::new(value.numer().clone(), value.denom().clone());
    if canonical.numer() != value.numer() || canonical.denom() != value.denom() {
        return Err(ValidationError::new(format!(
            "switch node {owner:?} queue {queue_index} WFQ {field} is not a reduced canonical rational; backend {backend} requires canonical checkpoint rationals (Scalar and Cpu are unbounded)"
        )));
    }
    Ok(())
}

fn validate_scheduler_state(
    image: &SimulationImage,
    owner: NodeId,
    queue_index: usize,
    queue: &crate::SwitchQueueState,
    pending_event_frontier: Option<u64>,
) -> Result<(), ValidationError> {
    validate_drop_mark_policy(image, owner, queue_index, queue)?;
    match &queue.scheduler {
        SchedulerKind::Fifo => Ok(()),
        SchedulerKind::StaticPriority { priorities } => {
            if priorities.is_empty() {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} SP priorities must contain at least one class"
                )));
            }
            let mut previous = None;
            for payload in &queue.queue {
                let priority = scheduler_class_value(image, *payload, priorities)
                    .expect("packet and flow validation precede scheduler-state validation");
                if previous.is_some_and(|previous| previous < priority) {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} SP waiting queue is not ordered by descending priority at packet {payload:?}"
                    )));
                }
                previous = Some(priority);
            }
            Ok(())
        }
        SchedulerKind::WeightedFairQueue(state) => {
            if state.weights.is_empty() {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ weights must contain at least one class"
                )));
            }
            if let Some(class) = state.weights.iter().position(|weight| *weight == 0) {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ weight for class {class} must be positive"
                )));
            }
            if state.finish_times.len() != state.weights.len()
                || state.active_packets.len() != state.weights.len()
            {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ class-state lengths must equal its {} weights",
                    state.weights.len()
                )));
            }
            if state.virtual_time.denom() == &BigUint::from(0_u8) {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ virtual time has a zero denominator"
                )));
            }
            for (class, finish) in state.finish_times.iter().enumerate() {
                if finish.denom() == &BigUint::from(0_u8) {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ finish state for class {class} has a zero denominator"
                    )));
                }
            }
            for (payload, finish) in &state.packet_finish_times {
                if finish.denom() == &BigUint::from(0_u8) {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ finish tag for packet {payload:?} has a zero denominator"
                    )));
                }
            }
            if let Some(frontier) =
                pending_event_frontier.filter(|frontier| state.last_updated_ns > *frontier)
            {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ last update time {} exceeds pending event frontier {frontier}",
                    state.last_updated_ns
                )));
            }

            let active = queue
                .queue
                .iter()
                .copied()
                .chain(queue.in_service)
                .collect::<BTreeSet<_>>();
            let tagged = state
                .packet_finish_times
                .keys()
                .copied()
                .collect::<BTreeSet<_>>();
            if active != tagged {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ finish tags must name exactly the waiting plus in-service packets"
                )));
            }

            let mut expected_active = vec![0_u64; state.weights.len()];
            for payload in queue.queue.iter().copied().chain(queue.in_service) {
                let class = scheduler_class(image, payload, state.weights.len())
                    .expect("packet and flow validation precede scheduler-state validation");
                expected_active[class] = expected_active[class].checked_add(1).ok_or_else(|| {
                    ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ active count overflows for class {class}"
                    ))
                })?;
            }
            if state.active_packets != expected_active {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WFQ active counts {:?} do not match queued plus in-service counts {expected_active:?}",
                    state.active_packets
                )));
            }
            if state.active_packets.iter().all(|active| *active == 0)
                && state.virtual_time.numer() != &BigUint::from(0_u8)
            {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} idle WFQ virtual time must be zero"
                )));
            }

            let mut previous = None;
            let mut maximum_waiting_finish = vec![None; state.weights.len()];
            for payload in &queue.queue {
                let finish = &state.packet_finish_times[payload];
                let class = scheduler_class(image, *payload, state.weights.len())
                    .expect("packet and flow validation precede scheduler-state validation");
                if finish.numer() == &BigUint::from(0_u8) {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ finish tag for waiting packet {payload:?} must be positive"
                    )));
                }
                if finish > &state.finish_times[class] {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ packet {payload:?} finish tag exceeds class {class} finish state"
                    )));
                }
                if previous.is_some_and(|previous: &crate::ExactRational| previous > finish) {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ waiting queue is not ordered by nondecreasing exact finish tag at packet {payload:?}"
                    )));
                }
                maximum_waiting_finish[class] = Some(finish);
                previous = Some(finish);
            }
            if let Some(payload) = queue.in_service {
                let in_service_finish = &state.packet_finish_times[&payload];
                if in_service_finish.numer() == &BigUint::from(0_u8) {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} WFQ finish tag for in-service packet {payload:?} must be positive"
                    )));
                }
            }
            for (class, maximum) in maximum_waiting_finish.into_iter().enumerate() {
                if let Some(maximum) = maximum {
                    if maximum != &state.finish_times[class] {
                        return Err(ValidationError::new(format!(
                            "switch node {owner:?} queue {queue_index} WFQ finish state for class {class} does not equal its maximum waiting tag"
                        )));
                    }
                    continue;
                }

                if let Some(payload) = queue.in_service {
                    let in_service_class = scheduler_class(image, payload, state.weights.len())
                        .expect("packet and flow validation precede scheduler-state validation");
                    if class == in_service_class {
                        let in_service_finish = &state.packet_finish_times[&payload];
                        if in_service_finish != &state.finish_times[class] {
                            return Err(ValidationError::new(format!(
                                "switch node {owner:?} queue {queue_index} WFQ finish state for class {class} does not equal its in-service packet {payload:?} tag"
                            )));
                        }
                    }
                }
            }
            Ok(())
        }
        SchedulerKind::DeficitRoundRobin(state) => {
            if state.quanta_bytes.is_empty()
                || state.quanta_bytes.contains(&0)
                || state.deficits_bytes.len() != state.quanta_bytes.len()
                || usize::try_from(state.current_class)
                    .ok()
                    .is_none_or(|class| class >= state.quanta_bytes.len())
            {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} DRR requires positive quanta, matching deficits, and an in-range current class"
                )));
            }
            for (class, quantum) in state.quanta_bytes.iter().copied().enumerate() {
                let maximum_frame = queue
                    .queue
                    .iter()
                    .filter(|payload| {
                        scheduler_class(image, **payload, state.quanta_bytes.len()) == Some(class)
                    })
                    .filter_map(|payload| packet(image, *payload))
                    .map(|packet| packet.size_bytes)
                    .max()
                    .unwrap_or(0);
                if maximum_frame == 0 {
                    continue;
                }
                let deficit = state.deficits_bytes[class];
                if deficit < maximum_frame {
                    let needed = maximum_frame - deficit;
                    let rounds = needed / quantum + u64::from(needed % quantum != 0);
                    if rounds
                        .checked_mul(quantum)
                        .and_then(|addition| deficit.checked_add(addition))
                        .is_none()
                    {
                        return Err(ValidationError::new(format!(
                            "switch node {owner:?} queue {queue_index} DRR class {class} cannot accumulate enough deficit for a {maximum_frame}-byte packet without overflowing"
                        )));
                    }
                }
            }
            Ok(())
        }
        SchedulerKind::WeightedRoundRobin(state) => {
            if state.weights.is_empty()
                || state.weights.contains(&0)
                || state.packets_sent_in_round.len() != state.weights.len()
                || state
                    .packets_sent_in_round
                    .iter()
                    .zip(&state.weights)
                    .any(|(sent, weight)| sent > weight)
                || usize::try_from(state.current_class)
                    .ok()
                    .is_none_or(|class| class >= state.weights.len())
            {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} WRR requires positive weights, matching bounded counters, and an in-range current class"
                )));
            }
            Ok(())
        }
    }
}

fn validate_drop_mark_policy(
    image: &SimulationImage,
    owner: NodeId,
    queue_index: usize,
    queue: &crate::SwitchQueueState,
) -> Result<(), ValidationError> {
    let queued_packets = u64::try_from(queue.queue.len()).map_err(|_| {
        ValidationError::new(format!(
            "switch node {owner:?} queue {queue_index} packet depth exceeds u64"
        ))
    })?;
    let queued_bytes = queue.queue.iter().try_fold(0_u64, |total, payload| {
        let size = packet(image, *payload)
            .expect("packet validation precedes drop/mark validation")
            .size_bytes;
        total.checked_add(size).ok_or_else(|| {
            ValidationError::new(format!(
                "switch node {owner:?} queue {queue_index} byte depth exceeds u64"
            ))
        })
    })?;
    match queue.drop_mark {
        crate::DropMarkPolicy::TailDrop => Ok(()),
        crate::DropMarkPolicy::EcnThreshold(config) => {
            if config.capacity == 0 || config.threshold == 0 || config.threshold > config.capacity {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} ECN threshold must satisfy 0 < threshold <= capacity"
                )));
            }
            let depth = match config.unit {
                crate::QueueDepthUnit::Packets => queued_packets,
                crate::QueueDepthUnit::Bytes => queued_bytes,
            };
            if depth > config.capacity {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} ECN depth {depth} exceeds policy capacity {}",
                    config.capacity
                )));
            }
            Ok(())
        }
        crate::DropMarkPolicy::Red(state) => {
            if state.capacity == 0
                || state.min_threshold >= state.max_threshold
                || state.max_threshold > state.capacity
                || state.max_probability_numerator == 0
                || state.max_probability_denominator == 0
                || state.max_probability_numerator > state.max_probability_denominator
            {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} RED requires 0 <= min < max <= capacity and 0 < max probability <= 1"
                )));
            }
            let depth = match state.unit {
                crate::QueueDepthUnit::Packets => queued_packets,
                crate::QueueDepthUnit::Bytes => queued_bytes,
            };
            if depth > state.capacity {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} RED depth {depth} exceeds policy capacity {}",
                    state.capacity
                )));
            }
            let maximum_average = u128::from(state.capacity)
                .checked_mul(1_u128 << 32)
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} RED average bound exceeds u128"
                    ))
                })?;
            if state.average_scaled > maximum_average {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} RED average {} exceeds scaled capacity {maximum_average}",
                    state.average_scaled
                )));
            }
            let probability_numerator = BigUint::from(state.max_probability_numerator);
            let worst_spacing_numerator = BigUint::from(state.max_probability_denominator)
                * BigUint::from(state.max_threshold - state.min_threshold)
                * BigUint::from(1_u128 << 32);
            let worst_spacing =
                (&worst_spacing_numerator + &probability_numerator - 1_u8) / probability_numerator;
            if worst_spacing > BigUint::from(u64::MAX) {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} RED worst-case signal spacing exceeds u64 counter state"
                )));
            }
            if BigUint::from(state.counter) >= worst_spacing {
                return Err(ValidationError::new(format!(
                    "switch node {owner:?} queue {queue_index} RED counter {} is not below its worst-case signal spacing",
                    state.counter
                )));
            }
            Ok(())
        }
    }
}

fn scheduler_class(
    image: &SimulationImage,
    payload: PayloadId,
    class_count: usize,
) -> Option<usize> {
    let packet = packet(image, payload)?;
    let class_count = u64::try_from(class_count).ok()?;
    usize::try_from(packet.flow.0 % class_count).ok()
}

fn scheduler_class_value(
    image: &SimulationImage,
    payload: PayloadId,
    values: &[u64],
) -> Option<u64> {
    scheduler_class(image, payload, values.len()).map(|class| values[class])
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
    derived_delays: &DerivedChannelDelays,
    pfc_channels: &BTreeSet<usize>,
) -> Result<(), ValidationError> {
    let mut channels_by_route = BTreeMap::<(LinkId, NodeId), usize>::new();
    for (index, channel) in image.channels.iter().enumerate() {
        let link = link(image, channel.link).ok_or_else(|| {
            ValidationError::new(format!(
                "channel {index} references unknown link {:?}",
                channel.link
            ))
        })?;
        if !pfc_channels.contains(&index) && channel.source != link.source {
            return Err(ValidationError::new(format!(
                "channel {index} source {:?} does not match physical link {:?} source {:?}",
                channel.source, link.id, link.source,
            )));
        }
        let target = node(image, channel.target).ok_or_else(|| {
            ValidationError::new(format!(
                "channel {index} names unknown target node {:?}",
                channel.target
            ))
        })?;
        if resolve_transition(target.kind, EventKind::RemoteArrival).is_none() {
            return Err(ValidationError::new(format!(
                "channel {index} targets {:?} node {:?}, which cannot receive RemoteArrival",
                target.kind, target.id
            )));
        }
        if channel.event_kind != EventKind::RemoteArrival {
            return Err(ValidationError::new(format!(
                "channel {index} declares unsupported {:?}; packet links emit RemoteArrival",
                channel.event_kind
            )));
        }
        if pfc_channels.contains(&index) {
            continue;
        }
        if let Some(first) = channels_by_route.insert((channel.link, channel.target), index) {
            return Err(ValidationError::new(format!(
                "channel {index} duplicates channel {first} for link {:?} to node {:?}",
                channel.link, channel.target,
            )));
        }
        let Some(derived) = derived_delays
            .possible
            .get(&(channel.link, channel.target))
            .copied()
        else {
            return Err(ValidationError::new(format!(
                "channel {index} references link {:?} to node {:?}, which has no possible route-selected packet emission",
                channel.link, channel.target,
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

    for &(link_id, target) in &derived_delays.required {
        if !channels_by_route.contains_key(&(link_id, target)) {
            let link = link(image, link_id).expect("derived delay names a validated link");
            return Err(ValidationError::new(format!(
                "link {:?} can emit RemoteArrival from node {:?} to node {:?}, but no channel is declared",
                link.id, link.source, target,
            )));
        }
    }
    Ok(())
}

fn validate_events(
    image: &SimulationImage,
    pfc_channels: &BTreeSet<usize>,
) -> Result<(), ValidationError> {
    let declared_route_channels = image
        .channels
        .iter()
        .enumerate()
        .filter(|(index, _)| !pfc_channels.contains(index))
        .map(|(_, channel)| {
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
        if event.kind == EventKind::RetransmissionTimeout {
            if origin.id != target.id || target.kind != NodeKind::Host {
                return Err(ValidationError::new(format!(
                    "RetransmissionTimeout event {index} must be owned by one host node"
                )));
            }
            continue;
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
                if !packet.kind.is_data() || event.target != flow.source {
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
                if let PacketKind::Pfc(header) = packet.kind {
                    let matches_lane = pfc_channels.iter().any(|channel_index| {
                        let channel = &image.channels[*channel_index];
                        channel.source == origin.id
                            && channel.target == event.target
                            && pfc_channel_controls(image, *channel_index)
                                == Some(header.controlled_link)
                    });
                    if !matches_lane {
                        return Err(ValidationError::new(format!(
                            "PFC RemoteArrival event {index} from {:?} to {:?} has no declared control lane for link {:?}",
                            origin.id, event.target, header.controlled_link
                        )));
                    }
                } else {
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
            EventKind::PacingTimer => {
                if origin.id != event.target || event.target != flow.source {
                    return Err(ValidationError::new(format!(
                        "PacingTimer event {index} for payload {:?} must be owned by source host {:?}",
                        event.payload, flow.source
                    )));
                }
                if !packet.kind.is_data() {
                    return Err(ValidationError::new(format!(
                        "PacingTimer event {index} references non-data payload {:?}",
                        event.payload
                    )));
                }
            }
            EventKind::RetransmissionTimeout => unreachable!("handled before packet lookup"),
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
    let terminal = packet_terminal(flow, packet.kind);
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

fn validate_global_time_capacity(
    image: &SimulationImage,
    backend: Backend,
) -> Result<(), ValidationError> {
    let mut service_bound = 0_u64;
    let live_payloads = crate::tcp_ledger::initial_live_payloads(image);
    let service_payloads = if matches!(backend, Backend::Metal | Backend::Cuda) {
        let mut payloads = image
            .host_states
            .iter()
            .flat_map(|state| state.queue.iter().copied().chain(state.in_service))
            .chain(image.switch_states.iter().flat_map(|state| {
                state
                    .queues
                    .iter()
                    .flat_map(|queue| queue.queue.iter().copied().chain(queue.in_service))
            }))
            .collect::<BTreeSet<_>>();
        for event in &image.initial_events {
            let packet =
                packet(image, event.payload).expect("event validation established the packet");
            let flow = flow(image, packet.flow).expect("packet validation established the flow");
            let terminal = packet_terminal(flow, packet.kind);
            if event.kind != EventKind::RemoteArrival || event.target != terminal {
                payloads.insert(event.payload);
            }
        }
        Some(payloads)
    } else {
        None
    };
    let scheduled_payloads = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
        .map(|generator| generator.next_emission.payload)
        .collect::<BTreeSet<_>>();
    for packet in &image.initial_packets {
        if scheduled_payloads.contains(&packet.id)
            || matches!(packet.kind, PacketKind::TcpData(_)) && !live_payloads.contains(&packet.id)
            || matches!(packet.kind, PacketKind::Pfc(_))
            || service_payloads
                .as_ref()
                .is_some_and(|payloads| !payloads.contains(&packet.id))
        {
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
        let packet_size_bytes = match generator.kind {
            FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
            FlowGeneratorKind::Tcp(tcp) => tcp.mss_bytes,
            FlowGeneratorKind::Rate(rate) => rate.packet_size_bytes,
        };
        let remaining = executable_generator_packets(generator)?;
        for link_id in &flow.route {
            let link = link(image, *link_id).expect("flow validation established the route link");
            let delay = link
                .delay_ns(packet_size_bytes)
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
        .filter(|event| event.key.time_ns <= image.stop_time_ns)
        .map(|event| event.key.time_ns)
        .max()
        .unwrap_or(0);
    let maximum_generator_time = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
        .map(|generator| match generator.kind {
            FlowGeneratorKind::Constant(constant) => {
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
            }
            FlowGeneratorKind::Tcp(_) => Ok(image.stop_time_ns),
            FlowGeneratorKind::Rate(rate) => rate
                .pacing_interval_ns
                .checked_mul(remaining_generator_packets(generator)?.saturating_sub(1))
                .and_then(|offset| {
                    generator
                        .next_emission
                        .departure_time_ns
                        .checked_add(offset)
                })
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} latest pacing time exceeds u64",
                        generator.flow
                    ))
                }),
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
            EventKind::PacketArrival
            | EventKind::RemoteArrival
            | EventKind::PacingTimer
            | EventKind::RetransmissionTimeout => unreachable!(),
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

fn validate_initial_payload_positions(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut events_by_payload = BTreeMap::<PayloadId, Vec<(usize, crate::Event)>>::new();
    for (index, event) in image.initial_events.iter().copied().enumerate() {
        events_by_payload
            .entry(event.payload)
            .or_default()
            .push((index, event));
    }

    for (payload, events) in events_by_payload {
        let packet_arrival = events
            .iter()
            .find(|(_, event)| event.kind == EventKind::PacketArrival);
        if let Some(packet_arrival) = packet_arrival {
            if let Some(other) = events
                .iter()
                .find(|(_, event)| event.kind != EventKind::PacketArrival)
            {
                return incompatible_initial_positions(image, payload, *packet_arrival, *other);
            }
        }

        let completions = events
            .iter()
            .filter(|(_, event)| event.kind == EventKind::TxComplete)
            .collect::<Vec<_>>();
        if completions.len() > 1 {
            return incompatible_initial_positions(
                image,
                payload,
                *completions[0],
                *completions[1],
            );
        }

        let remote_arrivals = events
            .iter()
            .filter(|(_, event)| event.kind == EventKind::RemoteArrival)
            .collect::<Vec<_>>();
        if remote_arrivals.len() > 1 {
            return incompatible_initial_positions(
                image,
                payload,
                *remote_arrivals[0],
                *remote_arrivals[1],
            );
        }

        if let (Some(completion), Some(remote_arrival)) =
            (completions.first(), remote_arrivals.first())
        {
            let completion = **completion;
            let remote_arrival = **remote_arrival;
            if !same_transmission_siblings(image, completion.1, remote_arrival.1) {
                return incompatible_initial_positions(image, payload, completion, remote_arrival);
            }
        }
    }
    Ok(())
}

fn same_transmission_siblings(
    image: &SimulationImage,
    completion: crate::Event,
    remote_arrival: crate::Event,
) -> bool {
    let Some(egress) = event_egress(image, &completion).and_then(|link_id| link(image, link_id))
    else {
        return false;
    };
    let Some(remote_target) = packet_remote_target_after_link(image, completion.payload, egress.id)
    else {
        return false;
    };
    completion.target == remote_arrival.key.origin_node
        && remote_arrival.target == remote_target
        && completion.key.time_ns.checked_add(egress.propagation_ns)
            == Some(remote_arrival.key.time_ns)
        && completion.key.origin_seq.checked_add(1) == Some(remote_arrival.key.origin_seq)
}

fn incompatible_initial_positions(
    image: &SimulationImage,
    payload: PayloadId,
    first: (usize, crate::Event),
    second: (usize, crate::Event),
) -> Result<(), ValidationError> {
    Err(ValidationError::new(format!(
        "payload {payload:?} has causally incompatible initial positions: {} and {}",
        describe_initial_position(image, first.0, first.1),
        describe_initial_position(image, second.0, second.1)
    )))
}

fn describe_initial_position(image: &SimulationImage, index: usize, event: crate::Event) -> String {
    match event.kind {
        EventKind::PacketArrival => {
            format!("PacketArrival event {index} at node {:?}", event.target)
        }
        EventKind::TxReady => format!("TxReady event {index} at node {:?}", event.target),
        EventKind::TxComplete => {
            format!("TxComplete event {index} at node {:?}", event.target)
        }
        EventKind::RemoteArrival => {
            let link = remote_arrival_link(image, event)
                .expect("event validation established the route channel");
            format!(
                "RemoteArrival event {index} on link {link:?} from {:?} to {:?}",
                event.key.origin_node, event.target
            )
        }
        EventKind::RetransmissionTimeout => {
            format!(
                "RetransmissionTimeout event {index} at node {:?}",
                event.target
            )
        }
        EventKind::PacingTimer => {
            format!("PacingTimer event {index} at node {:?}", event.target)
        }
    }
}

fn remote_arrival_link(image: &SimulationImage, event: crate::Event) -> Option<LinkId> {
    let packet = packet(image, event.payload)?;
    if let PacketKind::Pfc(header) = packet.kind {
        return image
            .channels
            .iter()
            .enumerate()
            .find_map(|(index, channel)| {
                (channel.source == event.key.origin_node
                    && channel.target == event.target
                    && pfc_channel_controls(image, index) == Some(header.controlled_link))
                .then_some(channel.link)
            });
    }
    let flow = flow(image, packet.flow)?;
    let route = packet_route(flow, packet.kind);
    let terminal = packet_terminal(flow, packet.kind);
    route
        .iter()
        .copied()
        .enumerate()
        .find_map(|(index, link_id)| {
            let route_link = link(image, link_id)?;
            (route_link.source == event.key.origin_node
                && route_target(image, route, index, terminal) == Some(event.target))
            .then_some(link_id)
        })
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
    pfc_by_node: Vec<u64>,
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
    let live_payloads = crate::tcp_ledger::initial_live_payloads(image);
    for packet in &image.initial_packets {
        if scheduled_payloads.contains(&packet.id)
            || matches!(packet.kind, PacketKind::TcpData(_)) && !live_payloads.contains(&packet.id)
            || matches!(packet.kind, PacketKind::Pfc(_))
        {
            continue;
        }
        let counts = if packet.kind.is_data() {
            &mut data_by_flow
        } else {
            &mut feedback_by_flow
        };
        let count = &mut counts[packet.flow.0 as usize];
        *count = count
            .checked_add(1)
            .ok_or_else(|| ValidationError::new("packet count exceeds the u64 counter domain"))?;
    }
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        match generator.kind {
            FlowGeneratorKind::Constant(_) => add_packet_count(
                &mut data_by_flow[generator.flow.0 as usize],
                executable_generator_packets(generator)?,
            )?,
            FlowGeneratorKind::Tcp(_) => {
                let preloaded_data = data_by_flow[generator.flow.0 as usize];
                let attempts = tcp_attempt_upper_bound(image, generator)?;
                add_packet_count(&mut data_by_flow[generator.flow.0 as usize], attempts)?;
                add_packet_count(
                    &mut feedback_by_flow[generator.flow.0 as usize],
                    preloaded_data,
                )?;
                add_packet_count(&mut feedback_by_flow[generator.flow.0 as usize], attempts)?;
            }
            FlowGeneratorKind::Rate(_) => add_packet_count(
                &mut data_by_flow[generator.flow.0 as usize],
                executable_generator_packets(generator)?,
            )?,
        }
    }
    let mut pfc_by_node = vec![0_u64; image.nodes.len()];
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        for ingress in image.switch_states[owner.state_slot as usize]
            .queues
            .iter()
            .flat_map(|queue| {
                queue
                    .pfc
                    .as_ref()
                    .into_iter()
                    .flat_map(|pfc| &pfc.ingresses)
            })
        {
            for flow in &image.flows {
                if flow.route.contains(&ingress.controlled_link) {
                    let frames =
                        data_by_flow[flow.id.0 as usize]
                            .checked_mul(2)
                            .ok_or_else(|| {
                                ValidationError::new("PFC control-frame bound exceeds u64")
                            })?;
                    add_packet_count(&mut pfc_by_node[owner.id.0 as usize], frames)?;
                }
            }
        }
    }
    Ok(FutureWork {
        data_by_flow,
        feedback_by_flow,
        pfc_by_node,
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
        add_packet_count(
            &mut sourced_by_node[flow.target.0 as usize],
            *feedback_count,
        )?;
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
                for generator in &state.generators {
                    let pending = work.feedback_by_flow[generator.flow.0 as usize];
                    if generator.feedback.arrivals.checked_add(pending).is_none() {
                        return Err(ValidationError::new(format!(
                            "node {:?} flow {:?} generator feedback arrivals {} overflows with {pending} pending feedback arrivals",
                            owner.id, generator.flow, generator.feedback.arrivals
                        )));
                    }
                }
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
    if let FlowGeneratorKind::Rate(rate) = generator.kind {
        if generator.bytes_emitted > rate.total_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} rate source emitted byte state exceeds total bytes {}",
                generator.flow, rate.total_bytes
            )));
        }
        let expected_bytes = u128::from(generator.packets_emitted)
            .checked_mul(u128::from(rate.packet_size_bytes))
            .map(|bytes| bytes.min(u128::from(rate.total_bytes)))
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} rate source byte bookkeeping exceeds u128",
                    generator.flow
                ))
            })?;
        if u128::from(generator.bytes_emitted) != expected_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} rate source records {} emitted bytes, expected {expected_bytes}",
                generator.flow, generator.bytes_emitted
            )));
        }
        return Ok((rate.total_bytes - generator.bytes_emitted).div_ceil(rate.packet_size_bytes));
    }
    let FlowGeneratorKind::Constant(constant) = generator.kind else {
        let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
            unreachable!("rate generators return above")
        };
        if generator.bytes_emitted > tcp.total_bytes || tcp.next_sequence > tcp.total_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} TCP emitted byte state exceeds total bytes {}",
                generator.flow, tcp.total_bytes
            )));
        }
        if generator.bytes_emitted != tcp.next_sequence {
            return Err(ValidationError::new(format!(
                "flow {:?} TCP emitted bytes {} do not match next sequence {}",
                generator.flow, generator.bytes_emitted, tcp.next_sequence
            )));
        }
        let remaining = tcp.total_bytes - tcp.next_sequence;
        return Ok(if remaining == 0 {
            0
        } else {
            1 + (remaining - 1) / tcp.mss_bytes
        });
    };
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
        GeneratorStatus::Blocked
            if matches!(
                generator.kind,
                FlowGeneratorKind::Tcp(_) | FlowGeneratorKind::Rate(_)
            ) =>
        {
            remaining_generator_packets(generator)
        }
        GeneratorStatus::Blocked | GeneratorStatus::Finished | GeneratorStatus::Stopped => Ok(0),
    }
}

fn is_preloaded_tcp_ack_arrival(
    image: &SimulationImage,
    event: &crate::Event,
    generator_flow: crate::FlowId,
) -> bool {
    let Some(flow) = flow(image, generator_flow) else {
        return false;
    };
    event.kind == EventKind::RemoteArrival
        && event.target == flow.source
        && packet(image, event.payload).is_some_and(|packet| {
            packet.flow == generator_flow && matches!(packet.kind, PacketKind::TcpAck(_))
        })
}

/// Conservative bound on timer installations reachable during the configured run.
///
/// Runtime TCP feedback is serialized by a positive-delay reverse route. A phase-0 ACK cancels
/// the flow's single phase-1 timer, so at most one runtime send-plan trigger per timestamp can
/// install a timer. Preloaded ACK events bypass that serialization and are reserved separately.
/// A Scheduled emission beyond the stop time cannot install a timer in this run.
fn tcp_timer_install_upper_bound(
    image: &SimulationImage,
    generator: &crate::FlowGeneratorState,
) -> Result<u64, ValidationError> {
    if matches!(
        generator.next_emission.status,
        GeneratorStatus::Finished | GeneratorStatus::Stopped
    ) {
        return Ok(0);
    }
    let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
        return Ok(0);
    };
    if tcp.highest_ack >= tcp.total_bytes
        || generator.next_emission.status == GeneratorStatus::Scheduled
            && generator.next_emission.departure_time_ns > image.stop_time_ns
    {
        return Ok(0);
    }
    let Some(first_event_time) = image
        .initial_events
        .iter()
        .map(|event| event.key.time_ns)
        .filter(|time_ns| *time_ns <= image.stop_time_ns)
        .min()
    else {
        return Ok(0);
    };
    let preloaded_ack_events = u64::try_from(
        image
            .initial_events
            .iter()
            .filter(|event| {
                event.key.time_ns <= image.stop_time_ns
                    && is_preloaded_tcp_ack_arrival(image, event, generator.flow)
            })
            .count(),
    )
    .map_err(|_| {
        ValidationError::new(format!(
            "flow {:?} TCP finite-run timer installation bound exceeds u64",
            generator.flow
        ))
    })?;
    image
        .stop_time_ns
        .checked_sub(first_event_time)
        .and_then(|span| span.checked_add(1))
        .and_then(|timestamps| timestamps.checked_add(preloaded_ack_events))
        .ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} TCP finite-run timer installation bound exceeds u64",
                generator.flow
            ))
        })
}

fn validate_tcp_timer_capacity(
    image: &SimulationImage,
    generator: &crate::FlowGeneratorState,
    tcp: crate::TcpGenerator,
) -> Result<(), ValidationError> {
    let installations = tcp_timer_install_upper_bound(image, generator)?;
    if tcp.timer_generation.checked_add(installations).is_none() {
        return Err(ValidationError::new(format!(
            "flow {:?} TCP timer generation {} overflows with remaining upper bound {installations}",
            generator.flow, tcp.timer_generation
        )));
    }
    if installations == 0 {
        return Ok(());
    }
    let deadline_headroom = u64::MAX - image.stop_time_ns;
    if tcp.rto_ns > deadline_headroom {
        return Err(ValidationError::new(format!(
            "flow {:?} TCP retransmission timeout {} exceeds deadline headroom {deadline_headroom} for stop time {}",
            generator.flow, tcp.rto_ns, image.stop_time_ns
        )));
    }
    Ok(())
}

/// Conservative finite-run bound used only to reserve counters and node-strided PayloadIds.
///
/// Nominal fresh segments are counted once. Every send-plan trigger can add at most one additional
/// congestion-window-limited fragment and one retransmission. Runtime ACKs for a flow are
/// serialized over its positive-delay reverse route, and a phase-0 ACK cancels the flow's single
/// active phase-1 timer, so there is at most one runtime trigger per timestamp. Preloaded ACK
/// events bypass that serialization and are therefore counted individually.
fn tcp_attempt_upper_bound(
    image: &SimulationImage,
    generator: &crate::FlowGeneratorState,
) -> Result<u64, ValidationError> {
    if matches!(
        generator.next_emission.status,
        GeneratorStatus::Finished | GeneratorStatus::Stopped
    ) {
        return Ok(0);
    }
    let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
        return executable_generator_packets(generator);
    };
    if tcp.highest_ack >= tcp.total_bytes {
        return Ok(0);
    }
    let preloaded_ack_events = u64::try_from(
        image
            .initial_events
            .iter()
            .filter(|event| is_preloaded_tcp_ack_arrival(image, event, generator.flow))
            .count(),
    )
    .map_err(|_| {
        ValidationError::new(format!(
            "flow {:?} TCP finite-run attempt bound exceeds u64",
            generator.flow
        ))
    })?;
    let triggers = image
        .stop_time_ns
        .checked_add(1)
        .and_then(|timestamps| timestamps.checked_add(preloaded_ack_events));
    remaining_generator_packets(generator)?
        .checked_add(
            triggers
                .and_then(|count| count.checked_mul(2))
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} TCP finite-run attempt bound exceeds u64",
                        generator.flow
                    ))
                })?,
        )
        .ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} TCP finite-run attempt bound exceeds u64",
                generator.flow
            ))
        })
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
            let emissions = match generator.kind {
                FlowGeneratorKind::Constant(_) => {
                    let remaining = executable_generator_packets(generator)?;
                    let scheduled =
                        u64::from(generator.next_emission.status == GeneratorStatus::Scheduled);
                    remaining.saturating_sub(scheduled)
                }
                FlowGeneratorKind::Tcp(_) => tcp_attempt_upper_bound(image, generator)?,
                FlowGeneratorKind::Rate(_) => {
                    let remaining = executable_generator_packets(generator)?;
                    let reserved = u64::from(matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                    ));
                    remaining.saturating_sub(reserved)
                }
            };
            emissions_by_node[owner.id.0 as usize] += u128::from(emissions);
        }
    }
    for (node_index, pfc_frames) in work.pfc_by_node.iter().copied().enumerate() {
        emissions_by_node[node_index] += u128::from(pfc_frames);
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
        if node.kind == NodeKind::Switch {
            let node_count = image.nodes.len() as u64;
            for packet in &image.initial_packets {
                if node_count != 0 && packet.id.0 % node_count == node.id.0 {
                    let sequence = packet.id.0 / node_count;
                    if sequence >= next {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} PFC payload sequence {sequence} is not below next origin sequence {next}",
                            node.id
                        )));
                    }
                }
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
    work: &FutureWork,
) -> Result<(), ValidationError> {
    let node_count = u64::try_from(image.nodes.len()).unwrap_or(u64::MAX);
    let mut payload_sequences_by_owner = vec![Vec::<(u64, PayloadId)>::new(); image.nodes.len()];
    if node_count != 0 {
        for packet in &image.initial_packets {
            let owner = packet.id.0 % node_count;
            let sequence = packet.id.0 / node_count;
            let flow = flow(image, packet.flow).expect("packet validation established the flow");
            if let PacketKind::Pfc(_) = packet.kind {
                let control_origin = image
                    .initial_events
                    .iter()
                    .find(|event| {
                        event.payload == packet.id && event.kind == EventKind::RemoteArrival
                    })
                    .map(|event| event.key.origin_node);
                if control_origin != Some(NodeId(owner)) {
                    return Err(ValidationError::new(format!(
                        "PFC packet {:?} is not allocated by its control-lane origin {:?}",
                        packet.id, control_origin
                    )));
                }
            } else if packet.kind.is_data() && owner != flow.source.0 {
                return Err(ValidationError::new(format!(
                    "data packet {:?} for flow {:?} is not allocated by source node {:?}",
                    packet.id, flow.id, flow.source
                )));
            } else if packet.kind.is_feedback() && owner != flow.target.0 {
                return Err(ValidationError::new(format!(
                    "feedback packet {:?} for flow {:?} is not allocated by target node {:?}",
                    packet.id, flow.id, flow.target
                )));
            }
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
            let remaining = match generator.kind {
                FlowGeneratorKind::Constant(_) => executable_generator_packets(generator)?,
                FlowGeneratorKind::Tcp(_) => tcp_attempt_upper_bound(image, generator)?,
                FlowGeneratorKind::Rate(_) => executable_generator_packets(generator)?,
            };
            let already_scheduled = u64::from(
                generator.next_emission.status == GeneratorStatus::Scheduled
                    || matches!(generator.kind, FlowGeneratorKind::Rate(_))
                        && generator.next_emission.status == GeneratorStatus::Blocked,
            );
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
        for receiver in &state.tcp_receivers {
            allocations = allocations
                .checked_add(work.data_by_flow[receiver.flow.0 as usize])
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "node {:?} generated TCP ACK count exceeds u64",
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
        .filter(|packet| !matches!(packet.kind, PacketKind::Pfc(_)))
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

fn packet_remote_target_after_link(
    image: &SimulationImage,
    payload: PayloadId,
    egress: LinkId,
) -> Option<NodeId> {
    let packet = packet(image, payload)?;
    let flow = flow(image, packet.flow)?;
    let route = packet_route(flow, packet.kind);
    let index = route.iter().position(|link_id| *link_id == egress)?;
    route_target(image, route, index, packet_terminal(flow, packet.kind))
}

fn packet_incoming_link_at(
    image: &SimulationImage,
    packet: &crate::PacketDescriptor,
    target: NodeId,
) -> Option<LinkId> {
    let flow = flow(image, packet.flow)?;
    let route = packet_route(flow, packet.kind);
    let terminal = packet_terminal(flow, packet.kind);
    route
        .iter()
        .copied()
        .enumerate()
        .find_map(|(index, link_id)| {
            (route_target(image, route, index, terminal) == Some(target)).then_some(link_id)
        })
}

fn packet_route(flow: &FlowDescriptor, packet_kind: PacketKind) -> &[LinkId] {
    match packet_kind {
        PacketKind::Data | PacketKind::TcpData(_) => &flow.route,
        PacketKind::Feedback | PacketKind::TcpAck(_) | PacketKind::Pfc(_) => &flow.reverse_route,
    }
}

fn packet_terminal(flow: &FlowDescriptor, packet_kind: PacketKind) -> NodeId {
    match packet_kind {
        PacketKind::Data | PacketKind::TcpData(_) => flow.target,
        PacketKind::Feedback | PacketKind::TcpAck(_) | PacketKind::Pfc(_) => flow.source,
    }
}

fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
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

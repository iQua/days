//! Load-time validation for backend-neutral simulation images.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use crate::{
    EventKind, FlowDescriptor, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, PayloadId,
    SchedulerKind, SimulationImage, event_phase, resolve_transition,
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
    let derived_delays = validate_packets_and_derive_delays(image)?;
    validate_owned_service_state(image, backend)?;
    validate_channels(image, backend, &derived_delays)?;
    validate_events(image)?;
    validate_global_time_capacity(image)?;
    validate_service_event_consistency(image)?;
    validate_counters(image)?;
    validate_origin_sequences(image)?;
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
    }
    Ok(())
}

fn validate_packet_ids(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    let count = image.packets.len() as u64;
    for (index, packet) in image.packets.iter().enumerate() {
        if !seen.insert(packet.id) {
            return Err(ValidationError::new(format!(
                "duplicate packet ID {:?} at descriptor {index}",
                packet.id
            )));
        }
        if packet.id.0 >= count {
            return Err(ValidationError::new(format!(
                "packet ID {:?} at descriptor {index} is outside dense range 0..{count}",
                packet.id
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
    }
    Ok(())
}

fn validate_packets_and_derive_delays(
    image: &SimulationImage,
) -> Result<BTreeMap<LinkId, u64>, ValidationError> {
    let mut derived = BTreeMap::<LinkId, u64>::new();
    for packet in &image.packets {
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
        for link_id in &flow.route {
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
        let actual_egress = flow_egress_at(image, flow, owner);
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
                if event.target != flow.source {
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
                    packet.size_bytes,
                    event.key.time_ns,
                    event.target,
                    index,
                    event.payload,
                )?;
            }
            EventKind::TxReady | EventKind::TxComplete => {
                if origin.id != event.target {
                    return Err(ValidationError::new(format!(
                        "{:?} event {index} origin {:?} must equal target owner {:?}",
                        event.kind, origin.id, event.target
                    )));
                }
                if !flow_route_contains_source(image, flow, event.target) {
                    return Err(ValidationError::new(format!(
                        "{:?} event {index} targets node {:?}, which does not own service for payload {:?}",
                        event.kind, event.target, event.payload
                    )));
                }
                if event.kind == EventKind::TxReady {
                    validate_remaining_route_time(
                        image,
                        flow,
                        packet.size_bytes,
                        event.key.time_ns,
                        event.target,
                        index,
                        event.payload,
                    )?;
                }
            }
            EventKind::RemoteArrival => {
                if !image.channels.iter().any(|channel| {
                    channel.source == origin.id
                        && channel.target == event.target
                        && channel.event_kind == EventKind::RemoteArrival
                        && flow.route.contains(&channel.link)
                }) {
                    return Err(ValidationError::new(format!(
                        "RemoteArrival event {index} from {:?} to {:?} has no declared route channel for payload {:?}",
                        origin.id, event.target, event.payload
                    )));
                }
                validate_remaining_route_time(
                    image,
                    flow,
                    packet.size_bytes,
                    event.key.time_ns,
                    event.target,
                    index,
                    event.payload,
                )?;
            }
        }
    }
    Ok(())
}

fn validate_remaining_route_time(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    packet_size_bytes: u64,
    start_time_ns: u64,
    start_node: NodeId,
    event_index: usize,
    payload: PayloadId,
) -> Result<(), ValidationError> {
    if start_node == flow.target {
        return Ok(());
    }
    let mut remaining_delay = 0_u64;
    let mut started = false;
    for link_id in &flow.route {
        let link = link(image, *link_id).expect("flow validation established the route link");
        if link.source == start_node {
            started = true;
        }
        if started {
            let delay = link
                .delay_ns(packet_size_bytes)
                .expect("packet/link delay validation already succeeded");
            remaining_delay = remaining_delay
                .checked_add(delay)
                .expect("cumulative packet route validation already succeeded");
        }
    }
    if !started {
        return Err(ValidationError::new(format!(
            "initial event {event_index} starts payload {payload:?} at node {start_node:?}, which is not on flow {:?}",
            flow.id
        )));
    }
    start_time_ns
        .checked_add(remaining_delay)
        .ok_or_else(|| {
            ValidationError::new(format!(
                "initial event {event_index} time {start_time_ns} plus remaining route delay {remaining_delay} overflows for payload {payload:?} at node {start_node:?}"
            ))
        })?;
    Ok(())
}

fn validate_global_time_capacity(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut service_bound = 0_u64;
    for packet in &image.packets {
        let flow = flow(image, packet.flow).expect("packet validation established the flow");
        for link_id in &flow.route {
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
    let maximum_initial_time = image
        .initial_events
        .iter()
        .map(|event| event.key.time_ns)
        .max()
        .unwrap_or(0);
    maximum_initial_time
        .checked_add(service_bound)
        .ok_or_else(|| {
            ValidationError::new(format!(
                "maximum initial event time {maximum_initial_time} plus conservative service bound {service_bound} overflows"
            ))
        })?;
    Ok(())
}

fn validate_service_event_consistency(image: &SimulationImage) -> Result<(), ValidationError> {
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let ready_count = image
            .initial_events
            .iter()
            .filter(|event| event.target == owner.id && event.kind == EventKind::TxReady)
            .count();
        validate_ready_flag(owner.id, None, state.tx_ready_pending, ready_count)?;
        validate_completion_state(image, owner.id, None, state.in_service)?;
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
            let ready_count = image
                .initial_events
                .iter()
                .filter(|event| {
                    event.target == owner.id
                        && event.kind == EventKind::TxReady
                        && event_egress(image, event) == Some(egress)
                })
                .count();
            validate_ready_flag(owner.id, Some(egress), queue.tx_ready_pending, ready_count)?;
            validate_completion_state(image, owner.id, Some(egress), queue.in_service)?;
        }
    }
    validate_unique_mutable_payloads(image)
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
    image: &SimulationImage,
    owner: NodeId,
    egress: Option<LinkId>,
    in_service: Option<PayloadId>,
) -> Result<(), ValidationError> {
    let completions = image
        .initial_events
        .iter()
        .filter(|event| {
            event.target == owner
                && event.kind == EventKind::TxComplete
                && egress.is_none_or(|link| event_egress(image, event) == Some(link))
        })
        .collect::<Vec<_>>();
    match in_service {
        Some(payload) if completions.len() == 1 && completions[0].payload == payload => Ok(()),
        None if completions.is_empty() => Ok(()),
        _ => Err(ValidationError::new(format!(
            "node {owner:?} egress {egress:?} in-service payload is {in_service:?}, but matching TxComplete payloads are {:?}",
            completions
                .iter()
                .map(|event| event.payload)
                .collect::<Vec<_>>()
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

fn validate_counters(image: &SimulationImage) -> Result<(), ValidationError> {
    for owner in &image.nodes {
        match owner.kind {
            NodeKind::Host => {
                let state = &image.host_states[owner.state_slot as usize];
                let sourced = packet_count(image, |flow| flow.source == owner.id)?;
                let received = packet_count(image, |flow| flow.target == owner.id)?;
                check_counter(owner.id, "sourced_packets", state.sourced_packets, sourced)?;
                check_counter(
                    owner.id,
                    "departed_packets",
                    state.departed_packets,
                    sourced,
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
                let traversing = packet_count(image, |flow| {
                    flow_route_contains_source(image, flow, owner.id)
                })?;
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
    Ok(())
}

fn packet_count(
    image: &SimulationImage,
    predicate: impl Fn(&FlowDescriptor) -> bool,
) -> Result<u64, ValidationError> {
    image
        .packets
        .iter()
        .filter(|packet| flow(image, packet.flow).is_some_and(&predicate))
        .try_fold(0_u64, |count, _| {
            count
                .checked_add(1)
                .ok_or_else(|| ValidationError::new("packet count exceeds the u64 counter domain"))
        })
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

fn validate_origin_sequences(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut maximum = BTreeMap::<NodeId, u64>::new();
    for event in &image.initial_events {
        maximum
            .entry(event.key.origin_node)
            .and_modify(|value| *value = (*value).max(event.key.origin_seq))
            .or_insert(event.key.origin_seq);
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
        let generated = possible_generated_events(image, node.id)?;
        if next.checked_add(generated).is_none() {
            return Err(ValidationError::new(format!(
                "node {:?} origin sequence space overflows while reserving {generated} generated events",
                node.id,
            )));
        }
    }
    Ok(())
}

fn possible_emission_links(image: &SimulationImage) -> BTreeSet<LinkId> {
    image
        .packets
        .iter()
        .filter_map(|packet| flow(image, packet.flow))
        .flat_map(|flow| flow.route.iter().copied())
        .collect()
}

fn possible_generated_events(
    image: &SimulationImage,
    source: NodeId,
) -> Result<u64, ValidationError> {
    let transmissions = image
        .packets
        .iter()
        .filter_map(|packet| {
            flow(image, packet.flow).map(|flow| {
                flow.route
                    .iter()
                    .filter(|link_id| {
                        link(image, **link_id).is_some_and(|link| link.source == source)
                    })
                    .count()
            })
        })
        .try_fold(0_u64, |total, count| {
            let count = u64::try_from(count).map_err(|_| {
                ValidationError::new(format!("node {source:?} generated-event count exceeds u64"))
            })?;
            total.checked_add(count).ok_or_else(|| {
                ValidationError::new(format!("node {source:?} generated-event count exceeds u64"))
            })
        })?;
    transmissions.checked_mul(3).ok_or_else(|| {
        ValidationError::new(format!("node {source:?} generated-event count exceeds u64"))
    })
}

fn flow_route_contains_source(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    source: NodeId,
) -> bool {
    flow_egress_at(image, flow, source).is_some()
}

fn flow_egress_at(
    image: &SimulationImage,
    flow: &FlowDescriptor,
    source: NodeId,
) -> Option<LinkId> {
    flow.route
        .iter()
        .copied()
        .find(|link_id| link(image, *link_id).is_some_and(|link| link.source == source))
}

fn event_egress(image: &SimulationImage, event: &crate::Event) -> Option<LinkId> {
    let packet = packet(image, event.payload)?;
    let flow = flow(image, packet.flow)?;
    flow_egress_at(image, flow, event.target)
}

fn node(image: &SimulationImage, id: NodeId) -> Option<&NodeDescriptor> {
    image.nodes.iter().find(|node| node.id == id)
}

fn link(image: &SimulationImage, id: LinkId) -> Option<&LinkDescriptor> {
    image.links.iter().find(|link| link.id == id)
}

fn flow(image: &SimulationImage, id: crate::FlowId) -> Option<&FlowDescriptor> {
    image.flows.iter().find(|flow| flow.id == id)
}

fn packet(image: &SimulationImage, id: PayloadId) -> Option<&crate::PacketDescriptor> {
    image.packets.iter().find(|packet| packet.id == id)
}

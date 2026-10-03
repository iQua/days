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

/// Per-flow counts of preloaded TCP acknowledgment arrivals.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
struct PreloadedTcpAcks {
    /// Every initial event that is a preloaded TCP ACK arrival for the flow.
    total: u64,
    /// The subset of `total` keyed at or below `image.stop_time_ns`.
    within_stop_time: u64,
}

/// Generator-keyed views of the initial packet and event tables, and a flow-keyed view of the
/// generators, built once per `validate` call.
///
/// The validator asks "which initial packets, or preloaded ACK arrivals, belong to this flow?" —
/// and "how many initial timeout events carry this timer's executable identity?" — once per
/// generator at ten call sites. Answering those with a linear filter makes the whole pass
/// quadratic in the flow count, so each table is indexed once instead: a CSR grouping over
/// `initial_packets` keyed by dense flow slot, a per-flow count of preloaded TCP ACK arrivals, and
/// a count of retransmission-timeout events keyed by their `(target, payload, deadline)` identity.
/// All are built by a single forward pass over each table, so every group lists its members in
/// initial-table order and an indexed walk visits exactly the elements the filter visited, in
/// exactly the same order — the diagnostics that name the first offending element are unchanged.
///
/// P14's stages added one more such question, "which generator owns this flow?", asked once per
/// flow, once per initial packet and at every stage predecessor. It is answered by a first-
/// occurrence slot per dense flow, so it too costs one pass instead of one scan per query. Only
/// stages ask it, or ask for the stage at a collective position, so [`StageLookups`] is built only
/// for an image with a stage generator; any other image skips the walk that fills it.
struct FlowIndex {
    /// CSR offsets into `packet_items`; one entry per dense flow slot plus a terminator.
    packet_offsets: Vec<usize>,
    /// `initial_packets` indices grouped by flow slot, ascending inside every group.
    packet_items: Vec<usize>,
    /// `initial_packets` indices whose flow falls outside the dense flow table.
    unindexed_packets: Vec<usize>,
    /// Preloaded TCP ACK arrival counts per dense flow slot.
    preloaded_tcp_acks: Vec<PreloadedTcpAcks>,
    /// Retransmission-timeout event counts per `(target, payload, deadline_ns)` identity.
    ///
    /// A `BTreeMap` rather than a flow-slot vector because the query key is a timer's executable
    /// identity, not a flow: the same key is what `validate_generators` uses to reject two flows
    /// whose active timers would consume the same event.
    retransmission_timeouts: BTreeMap<(NodeId, PayloadId, u64), u64>,
    /// Smallest initial event time at or below the stop time; invariant across generators.
    first_admissible_event_time_ns: Option<u64>,
    /// The generator lookups; `None` exactly when no generator carries a stage.
    stage_lookups: Option<StageLookups>,
}

/// The generator lookups of a [`FlowIndex`] over an image with at least one stage generator.
struct StageLookups {
    /// The first generator of each dense flow slot, as `(host_states index, generators index)`.
    ///
    /// "First" is in `host_states`-then-`generators` order, the order the linear
    /// `flat_map(..).find(|generator| generator.flow == id)` it replaces visits them in, so a
    /// lookup returns the very generator the scan returned even when an invalid image lists two
    /// generators for one flow.
    generator_slots: Vec<Option<(usize, usize)>>,
    /// Generators whose flow falls outside the dense flow table, in the same order.
    unindexed_generators: Vec<(usize, usize)>,
    /// The first collective stage generator at each `(collective, phase, rank, step)` position,
    /// in the same `host_states`-then-`generators` order. Empty on an image without collectives.
    collective_stages: BTreeMap<CollectivePosition, (usize, usize)>,
}

/// A collective stage's position: `(collective_id, phase, rank, step)`.
type CollectivePosition = (u64, crate::CollectivePhase, u32, u32);

/// Returns the dense flow-table slot of `id`, exactly when `flow(image, id)` resolves.
fn dense_flow_slot(image: &SimulationImage, id: crate::FlowId) -> Option<usize> {
    let slot = usize::try_from(id.0).ok()?;
    image.flows.get(slot).filter(|flow| flow.id == id)?;
    Some(slot)
}

impl FlowIndex {
    fn build(image: &SimulationImage) -> Self {
        let flow_count = image.flows.len();
        let mut packet_offsets = vec![0_usize; flow_count + 1];
        let mut unindexed_packets = Vec::new();
        for (index, packet) in image.initial_packets.iter().enumerate() {
            match dense_flow_slot(image, packet.flow) {
                Some(slot) => packet_offsets[slot + 1] += 1,
                None => unindexed_packets.push(index),
            }
        }
        for slot in 0..flow_count {
            packet_offsets[slot + 1] += packet_offsets[slot];
        }
        let mut cursors = packet_offsets.clone();
        let mut packet_items = vec![0_usize; packet_offsets[flow_count]];
        for (index, packet) in image.initial_packets.iter().enumerate() {
            if let Some(slot) = dense_flow_slot(image, packet.flow) {
                packet_items[cursors[slot]] = index;
                cursors[slot] += 1;
            }
        }

        let mut preloaded_tcp_acks = vec![PreloadedTcpAcks::default(); flow_count];
        let mut retransmission_timeouts = BTreeMap::<(NodeId, PayloadId, u64), u64>::new();
        let mut first_admissible_event_time_ns: Option<u64> = None;
        for event in &image.initial_events {
            let admissible = event.key.time_ns <= image.stop_time_ns;
            if admissible {
                first_admissible_event_time_ns = Some(
                    first_admissible_event_time_ns.map_or(event.key.time_ns, |earliest| {
                        earliest.min(event.key.time_ns)
                    }),
                );
            }
            if event.kind == EventKind::RetransmissionTimeout {
                // Bounded by `initial_events.len()`, so the count cannot leave `u64`.
                *retransmission_timeouts
                    .entry((event.target, event.payload, event.key.time_ns))
                    .or_insert(0) += 1;
            }
            if event.kind != EventKind::RemoteArrival {
                continue;
            }
            let Some(packet) = packet(image, event.payload) else {
                continue;
            };
            if !matches!(packet.kind, PacketKind::TcpAck(_)) {
                continue;
            }
            let Some(slot) = dense_flow_slot(image, packet.flow) else {
                continue;
            };
            if event.target != image.flows[slot].source {
                continue;
            }
            // Both counts are bounded by `initial_events.len()`, so neither can leave `u64`.
            let counts = &mut preloaded_tcp_acks[slot];
            counts.total += 1;
            if admissible {
                counts.within_stop_time += 1;
            }
        }

        // `validate_stage_tables` established that every stage table is empty or has a stage, so
        // a non-empty table is exactly a host with a stage generator, and a stageless image reads
        // no generator here.
        let stage_lookups = image
            .host_states
            .iter()
            .any(|state| !state.stages.is_empty())
            .then(|| StageLookups::build(image));

        Self {
            packet_offsets,
            packet_items,
            unindexed_packets,
            preloaded_tcp_acks,
            retransmission_timeouts,
            first_admissible_event_time_ns,
            stage_lookups,
        }
    }

    /// Whether any generator carries a stage.
    const fn has_stage_generators(&self) -> bool {
        self.stage_lookups.is_some()
    }

    /// Returns the first collective stage generator, in `host_states`-then-`generators` order, at
    /// `position`.
    ///
    /// Exactly equivalent to the `flat_map(..).find(..)` over every generator whose collective
    /// identity matches all four components: the build pass keeps the first generator it meets at
    /// each position, and it meets them in that order.
    ///
    /// Without stage lookups no generator has a collective identity, so the scan finds none.
    fn collective_stage<'a>(
        &self,
        image: &'a SimulationImage,
        position: CollectivePosition,
    ) -> Option<StagedGenerator<'a>> {
        self.stage_lookups
            .as_ref()?
            .collective_stages
            .get(&position)
            .map(|&(host, index)| staged_generator(&image.host_states[host], index))
    }

    /// Returns the first generator, in `host_states`-then-`generators` order, whose flow is `id`.
    ///
    /// Exactly equivalent to `image.host_states.iter().flat_map(staged_generators)
    /// .find(|generator| generator.flow == id)`: a dense slot records the first generator that
    /// resolves to it, and an identifier outside the dense table falls back to the same `find`
    /// over the unindexed generators, which keep their relative order.
    ///
    /// Without stage lookups it is that scan itself. Validation queries it only from stage
    /// validators, which run only when a generator has a stage; the equality gates query it on
    /// every image.
    fn generator_for_flow<'a>(
        &self,
        image: &'a SimulationImage,
        id: crate::FlowId,
    ) -> Option<StagedGenerator<'a>> {
        let Some(lookups) = &self.stage_lookups else {
            return image
                .host_states
                .iter()
                .flat_map(staged_generators)
                .find(|generator| generator.flow == id);
        };
        let at = |(host, index): (usize, usize)| staged_generator(&image.host_states[host], index);
        match dense_flow_slot(image, id) {
            Some(slot) => lookups.generator_slots[slot].map(at),
            None => lookups
                .unindexed_generators
                .iter()
                .copied()
                .map(at)
                .find(|generator| generator.flow == id),
        }
    }

    /// Whether a compute (delay-only) stage owns this flow.
    ///
    /// Without stage lookups no generator is a compute stage, so the answer is `false` without a
    /// lookup, exactly as the lookup would answer.
    fn is_compute_flow(&self, image: &SimulationImage, id: crate::FlowId) -> bool {
        self.has_stage_generators()
            && self
                .generator_for_flow(image, id)
                .is_some_and(is_compute_generator)
    }

    /// Yields the initial packets of `id`, in `initial_packets` order.
    ///
    /// Exactly equivalent to `initial_packets.iter().filter(|packet| packet.flow == id)`: a dense
    /// group holds precisely the packets whose flow resolves to that slot, and an identifier
    /// outside the dense table falls back to an equality filter over the unindexed group.
    fn packets_for_flow<'a>(
        &'a self,
        image: &'a SimulationImage,
        id: crate::FlowId,
    ) -> impl Iterator<Item = &'a crate::PacketDescriptor> + 'a {
        let (group, unindexed) = match dense_flow_slot(image, id) {
            Some(slot) => (
                &self.packet_items[self.packet_offsets[slot]..self.packet_offsets[slot + 1]],
                false,
            ),
            None => (self.unindexed_packets.as_slice(), true),
        };
        group
            .iter()
            .map(|index| &image.initial_packets[*index])
            .filter(move |packet| !unindexed || packet.flow == id)
    }

    /// Returns the preloaded TCP ACK arrival counts of `id`.
    fn preloaded_tcp_acks(&self, image: &SimulationImage, id: crate::FlowId) -> PreloadedTcpAcks {
        dense_flow_slot(image, id)
            .and_then(|slot| self.preloaded_tcp_acks.get(slot).copied())
            .unwrap_or_default()
    }

    /// Returns how many initial retransmission-timeout events carry this executable identity.
    ///
    /// Exactly equivalent to counting `initial_events` with `kind == RetransmissionTimeout &&
    /// target == owner && payload == attempt && key.time_ns == deadline_ns`: the build pass keys
    /// each such event by that same quadruple and increments its bucket, and `count` is
    /// order-insensitive.
    fn retransmission_timeout_events(
        &self,
        owner: NodeId,
        attempt: PayloadId,
        deadline_ns: u64,
    ) -> u64 {
        self.retransmission_timeouts
            .get(&(owner, attempt, deadline_ns))
            .copied()
            .unwrap_or(0)
    }

    /// Returns how many initial retransmission-timeout events carry `payload` at `owner`, at any
    /// deadline: one ordered range of the same buckets.
    fn retransmission_timeout_events_with_payload(&self, owner: NodeId, payload: PayloadId) -> u64 {
        self.retransmission_timeouts
            .range((owner, payload, 0)..=(owner, payload, u64::MAX))
            .map(|(_, count)| *count)
            .sum()
    }
}

impl StageLookups {
    /// One pass over the generators in `host_states`-then-`generators` order, keeping the first
    /// generator met at each dense flow slot and at each collective position.
    fn build(image: &SimulationImage) -> Self {
        let mut generator_slots = vec![None; image.flows.len()];
        let mut unindexed_generators = Vec::new();
        let mut collective_stages = BTreeMap::new();
        for (host, state) in image.host_states.iter().enumerate() {
            for (index, generator) in staged_generators(state).enumerate() {
                match dense_flow_slot(image, generator.flow) {
                    Some(slot) => {
                        generator_slots[slot].get_or_insert((host, index));
                    }
                    None => unindexed_generators.push((host, index)),
                }
                if let Some(stage) = collective_identity(generator) {
                    collective_stages
                        .entry((stage.collective_id, stage.phase, stage.rank, stage.step))
                        .or_insert((host, index));
                }
            }
        }
        Self {
            generator_slots,
            unindexed_generators,
            collective_stages,
        }
    }
}

/// CSR grouping of the executable resident packets by dense flow slot, in resident-table order.
///
/// `future_work`'s PFC reservation asks "which executable residents belong to this flow?" twice
/// per `(PFC ingress, flow)` pair — an `O(ingresses x F x P)` per-flow filter with the same shape
/// as the ones `FlowIndex` removed. `FlowIndex` cannot answer it: the resident table is *derived*
/// by `executable_resident_packets`, not stored on the image, so it is grouped separately, by the
/// same construction and with the same guarantees.
struct ResidentFlowGroups {
    /// CSR offsets into `items`; one entry per dense flow slot plus a terminator.
    offsets: Vec<usize>,
    /// Resident-table indices grouped by flow slot, ascending inside every group.
    items: Vec<usize>,
    /// Resident-table indices whose flow falls outside the dense flow table.
    unindexed: Vec<usize>,
}

impl ResidentFlowGroups {
    fn build(image: &SimulationImage, residents: &[&crate::PacketDescriptor]) -> Self {
        let flow_count = image.flows.len();
        let mut offsets = vec![0_usize; flow_count + 1];
        let mut unindexed = Vec::new();
        for (index, packet) in residents.iter().enumerate() {
            match dense_flow_slot(image, packet.flow) {
                Some(slot) => offsets[slot + 1] += 1,
                None => unindexed.push(index),
            }
        }
        for slot in 0..flow_count {
            offsets[slot + 1] += offsets[slot];
        }
        let mut cursors = offsets.clone();
        let mut items = vec![0_usize; offsets[flow_count]];
        for (index, packet) in residents.iter().enumerate() {
            if let Some(slot) = dense_flow_slot(image, packet.flow) {
                items[cursors[slot]] = index;
                cursors[slot] += 1;
            }
        }
        Self {
            offsets,
            items,
            unindexed,
        }
    }

    /// Yields the residents of `id`, in resident-table order.
    ///
    /// Exactly equivalent to `residents.iter().filter(|packet| packet.flow == id)`, by the same
    /// argument as [`FlowIndex::packets_for_flow`]: a dense group holds precisely the residents
    /// whose flow resolves to that slot, and an identifier outside the dense table falls back to
    /// an equality filter over the shared unindexed group.
    fn group<'a>(
        &'a self,
        image: &SimulationImage,
        residents: &'a [&'a crate::PacketDescriptor],
        id: crate::FlowId,
    ) -> impl Iterator<Item = &'a crate::PacketDescriptor> + 'a {
        let (group, unindexed) = match dense_flow_slot(image, id) {
            Some(slot) => (
                &self.items[self.offsets[slot]..self.offsets[slot + 1]],
                false,
            ),
            None => (self.unindexed.as_slice(), true),
        };
        group
            .iter()
            .map(|index| residents[*index])
            .filter(move |packet| !unindexed || packet.flow == id)
    }
}

/// Groups initial payload sequences by their node-strided owner, in `initial_packets` order.
///
/// The owner of `PayloadId(value)` is `value % node_count` and its sequence is
/// `value / node_count`. Recovering that per node with a full packet rescan costs one scan per
/// LP; one bucketing pass answers it for every node at once, and because `initial_packets` is
/// strictly ascending in `PayloadId` each bucket is ascending in sequence — so the first bucket
/// entry meeting a predicate is the same packet the rescan would have reported first.
fn payload_sequences_by_owner(image: &SimulationImage) -> Vec<Vec<u64>> {
    let mut sequences = vec![Vec::<u64>::new(); image.nodes.len()];
    let node_count = image.nodes.len() as u64;
    if node_count == 0 {
        return sequences;
    }
    for packet in &image.initial_packets {
        sequences[(packet.id.0 % node_count) as usize].push(packet.id.0 / node_count);
    }
    sequences
}

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
    validate_stage_tables(image)?;
    // Dense flow identifiers, strictly ascending payload identifiers and canonical stage tables
    // are established above, which is everything the flow index needs; it reads the image and
    // cannot itself reject.
    let flow_index = FlowIndex::build(image);
    validate_state_ownership(image)?;
    validate_links(image)?;
    validate_flows(image, &flow_index)?;
    validate_generators(image, &flow_index)?;
    validate_backend_capabilities(image, &flow_index, backend)?;
    let derived_delays = validate_packets_and_derive_delays(image, &flow_index)?;
    validate_tcp_segment_ledger(image, &flow_index)?;
    validate_owned_service_state(image, &flow_index, backend)?;
    let pfc_channels = validate_pfc(image)?;
    validate_channels(image, backend, &derived_delays, &pfc_channels)?;
    validate_events(image, &pfc_channels)?;
    validate_pfc_causal_consistency(image)?;
    // `future_work` is a pure function of the image and was previously recomputed by
    // `validate_counters`. It is computed at its first consumer so that a rejection raised while
    // deriving it still surfaces from `validate_global_time_capacity`, exactly as before.
    let future_work = validate_global_time_capacity(image, &flow_index, backend)?;
    validate_service_event_consistency(image)?;
    validate_counters(image, &future_work)?;
    validate_dcqcn_arithmetic_capacity(image, &future_work)?;
    validate_origin_sequences(image, &flow_index, &future_work)?;
    validate_payload_sequences(image, &flow_index, &future_work)?;
    validate_preloaded_arrival_capacity(image)?;
    validate_initial_payload_positions(image)?;
    Ok(())
}

fn validate_backend_capabilities(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    backend: Backend,
) -> Result<(), ValidationError> {
    if !matches!(backend, Backend::Metal | Backend::Cuda) {
        return Ok(());
    }
    // P14 Lane B: both device backends run the DCQCN reaction and notification points and PFC
    // per-priority link pause, so neither needs a refusal here. The flow index already knows
    // whether any generator carries a stage, so this refusal does not walk the generators again.
    if flow_index.has_stage_generators() {
        return Err(ValidationError::new(format!(
            "backend {backend} does not support collective generators; use Scalar or Cpu"
        )));
    }
    // P15 lane R4: both device backends run RoCE queue pairs, host-link PFC and a feedback class
    // apart from the data class (`evidence/P15/device-design.md`), so none needs a refusal here.
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        match queue.drop_mark {
            crate::DropMarkPolicy::TailDrop | crate::DropMarkPolicy::EcnThreshold(_) => {}
            crate::DropMarkPolicy::Red(_) => {
                return Err(ValidationError::new(format!(
                    "backend {backend} does not support RED admission; use Scalar or Cpu"
                )));
            }
        }
        match queue.scheduler {
            SchedulerKind::Fifo
            | SchedulerKind::StaticPriority { .. }
            | SchedulerKind::WeightedFairQueue(_)
            | SchedulerKind::DeficitRoundRobin(_)
            | SchedulerKind::WeightedRoundRobin(_) => {}
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

/// Every host's stage table has one of its two canonical shapes: empty, or one entry per generator
/// with at least one stage.
///
/// Under this rule a host's generators, each with its stage record, determine the table, which is
/// why `HostState`'s `Debug` rendering, and with it every image and result fingerprint, prints the
/// records inline and not the table.
fn validate_stage_tables(image: &SimulationImage) -> Result<(), ValidationError> {
    for (slot, state) in image.host_states.iter().enumerate() {
        if state.stages_are_canonical() {
            continue;
        }
        return Err(ValidationError::new(
            if state.stages.len() == state.generators.len() {
                format!(
                    "host state slot {slot} has a stage table without a stage; a host without stages \
                 has an empty table"
                )
            } else {
                format!(
                    "host state slot {slot} has {} stage table entries for {} generators; the table \
                 is empty or has one entry per generator",
                    state.stages.len(),
                    state.generators.len()
                )
            },
        ));
    }
    Ok(())
}

fn validate_node_ids(image: &SimulationImage) -> Result<(), ValidationError> {
    let count = image.nodes.len() as u64;
    for (index, node) in image.nodes.iter().enumerate() {
        // Every earlier descriptor passed all three checks, so its ID equals its position: the IDs
        // seen so far are exactly 0..index, and this one repeats one of them iff it is below
        // `index`.
        if node.id.0 < index as u64 {
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

fn validate_flows(image: &SimulationImage, flow_index: &FlowIndex) -> Result<(), ValidationError> {
    for flow in &image.flows {
        if flow.priority > 7 {
            return Err(ValidationError::new(format!(
                "flow {:?} priority {} is outside IEEE 802.1Q range 0..=7",
                flow.id, flow.priority
            )));
        }
        if flow.feedback_priority > 7 {
            return Err(ValidationError::new(format!(
                "flow {:?} feedback priority {} is outside IEEE 802.1Q range 0..=7",
                flow.id, flow.feedback_priority
            )));
        }
        // Only a DCQCN or RoCE receiver sends feedback that may ride its own class (a CNP, a RoCE
        // ACK or NACK); a TCP ACK rides the data class. The source walk runs only for such flows.
        if flow.feedback_priority != flow.priority
            && !node(image, flow.source)
                .filter(|source| source.kind == NodeKind::Host)
                .and_then(|source| image.host_states.get(source.state_slot as usize))
                .is_some_and(|state| {
                    state.generators.iter().any(|generator| {
                        generator.flow == flow.id
                            && matches!(
                                generator.kind,
                                FlowGeneratorKind::Dcqcn(_) | FlowGeneratorKind::Roce(_)
                            )
                    })
                })
        {
            return Err(ValidationError::new(format!(
                "flow {:?} feedback priority {} differs from its priority {}, which only a DCQCN or RoCE flow may do",
                flow.id, flow.feedback_priority, flow.priority
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
        if flow_index.is_compute_flow(image, flow.id) {
            // A compute stage is a timer on its own host: it sends nothing and has no route.
            if flow.source != flow.target
                || !flow.route.is_empty()
                || !flow.reverse_route.is_empty()
            {
                return Err(ValidationError::new(format!(
                    "compute flow {:?} must stay on its host with empty routes",
                    flow.id
                )));
            }
            continue;
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
) -> Result<[u64; 8], ValidationError> {
    let mut maximum = [0_u64; 8];
    let resident_packets = executable_resident_packets(image);
    let live_dcqcn_cnp_flows = resident_packets
        .iter()
        .filter(|packet| dcqcn_data_can_still_emit_cnp(image, packet))
        .map(|packet| packet.flow)
        .collect::<BTreeSet<_>>();
    for packet in resident_packets {
        let flow = flow(image, packet.flow).expect("packet validation established the flow");
        if packet_route(flow, packet.kind).contains(&controlled_link)
            && packet_can_still_cross_link(image, packet, controlled_link)
        {
            let priority = usize::from(flow.packet_priority(packet.kind));
            maximum[priority] = maximum[priority].max(packet.size_bytes);
        }
    }
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let executable = executable_generator_packets(image, generator)? != 0;
        let flow = flow(image, generator.flow).expect("generator validation established the flow");
        let priority = usize::from(flow.priority);
        let feedback_priority = usize::from(flow.feedback_priority);
        if executable && flow.route.contains(&controlled_link) {
            let size = match generator.kind {
                FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
                FlowGeneratorKind::Tcp(tcp) => tcp.mss_bytes,
                FlowGeneratorKind::Rate(rate) => rate
                    .packet_size_bytes
                    .min(rate.total_bytes - generator.bytes_emitted),
                FlowGeneratorKind::Dcqcn(dcqcn) => dcqcn
                    .rate
                    .packet_size_bytes
                    .min(dcqcn.rate.total_bytes - generator.bytes_emitted),
                FlowGeneratorKind::Roce(roce) => roce_future_data_max_bytes(roce),
            };
            maximum[priority] = maximum[priority].max(size);
        }
        if flow.reverse_route.contains(&controlled_link) {
            match generator.kind {
                FlowGeneratorKind::Tcp(tcp) => {
                    if executable || tcp.bytes_in_flight != 0 {
                        maximum[feedback_priority] =
                            maximum[feedback_priority].max(tcp.ack_size_bytes);
                    }
                }
                FlowGeneratorKind::Dcqcn(dcqcn) => {
                    if executable || live_dcqcn_cnp_flows.contains(&flow.id) {
                        maximum[feedback_priority] =
                            maximum[feedback_priority].max(dcqcn.cnp_size_bytes);
                    }
                }
                FlowGeneratorKind::Roce(roce) => {
                    // Every data arrival at the receiver can answer with an ACK or NACK and a
                    // CNP, so feedback is live while data can still be sent or is in flight.
                    if executable || roce.snd_una < generator.bytes_emitted {
                        maximum[feedback_priority] =
                            maximum[feedback_priority].max(roce_feedback_max_bytes(image, flow));
                    }
                }
                FlowGeneratorKind::Constant(_) | FlowGeneratorKind::Rate(_) => {}
            }
        }
    }
    Ok(maximum)
}

fn validate_pfc(image: &SimulationImage) -> Result<BTreeSet<usize>, ValidationError> {
    let mut control_lanes = BTreeSet::new();
    let mut controller_monitors = BTreeSet::<(LinkId, NodeId)>::new();
    let mut controlled_priorities = BTreeSet::<(LinkId, u8)>::new();
    let mut controllers_by_link_priority = BTreeMap::<LinkId, [BTreeSet<NodeId>; 8]>::new();
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
                let controlled_owner = node(image, controlled.source)
                    .expect("link validation established the controlled-link source");
                if !controller_monitors.insert((controlled.id, owner.id)) {
                    return Err(ValidationError::new(format!(
                        "duplicate PFC controller {:?} for controlled link {:?}; one controller LP may own only one ingress monitor per controlled link",
                        owner.id, controlled.id
                    )));
                }
                match controlled_owner.kind {
                    NodeKind::Switch => {
                        let controlled_queue = image.switch_states
                            [controlled_owner.state_slot as usize]
                            .queues
                            .iter()
                            .find(|queue| queue.egress_link == Some(controlled.id));
                        if controlled_queue.is_none_or(|queue| queue.pfc.is_none()) {
                            return Err(ValidationError::new(format!(
                                "PFC controlled upstream queue at switch {:?} for link {:?} must own controller-scoped PFC state",
                                controlled.source, controlled.id
                            )));
                        }
                    }
                    // Host-link PFC: the switch pauses the host's NIC.
                    NodeKind::Host => {
                        let host = &image.host_states[controlled_owner.state_slot as usize];
                        if host.egress_link != controlled.id || host.pfc.is_none() {
                            return Err(ValidationError::new(format!(
                                "PFC controlled upstream host {:?} for link {:?} must own host egress PFC state for that egress link",
                                controlled.source, controlled.id
                            )));
                        }
                    }
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
                let reachable_frame_bytes = derived_pfc_max_frame_bytes(image, controlled.id)?;

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
                                .packet_priority(packet.kind),
                        );
                        if ingress.xoff_threshold_bytes[priority] == 0 {
                            continue;
                        }
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
                for (priority, &reachable_frame) in reachable_frame_bytes.iter().enumerate() {
                    let xoff = ingress.xoff_threshold_bytes[priority];
                    let xon = ingress.xon_threshold_bytes[priority];
                    let occupancy = ingress.occupancy_bytes[priority];
                    let capacity = ingress.buffer_capacity_bytes[priority];
                    let maximum_frame = ingress.max_frame_bytes[priority];
                    if xoff == 0 {
                        if xon != 0
                            || capacity != 0
                            || occupancy != 0
                            || ingress.pause_asserted[priority]
                            || maximum_frame != 0
                        {
                            return Err(ValidationError::new(format!(
                                "switch node {:?} queue {queue_index} disabled PFC priority {priority} must have zero XON, capacity, occupancy, maximum frame, and pause state",
                                owner.id
                            )));
                        }
                        continue;
                    }
                    controlled_priorities.insert((controlled.id, priority as u8));
                    controllers_by_link_priority
                        .entry(controlled.id)
                        .or_default()[priority]
                        .insert(owner.id);
                    if maximum_frame < reachable_frame {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} maximum frame bound {maximum_frame} is below reachable frame size {reachable_frame} on controlled link {:?}",
                            owner.id, controlled.id
                        )));
                    }
                    if capacity == 0 || xon >= xoff || xoff > capacity {
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {queue_index} PFC priority {priority} requires XON < XOFF <= capacity, got {xon} < {xoff} <= {}",
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
                    let required_headroom = if maximum_frame == 0 {
                        0
                    } else {
                        u128::from(maximum_frame - 1)
                            .checked_add(line_bytes)
                            .and_then(|value| value.checked_add(u128::from(maximum_frame)))
                            .ok_or_else(|| {
                                ValidationError::new("PFC required headroom overflows u128")
                            })?
                    };
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

    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        for (queue_index, queue) in image.switch_states[owner.state_slot as usize]
            .queues
            .iter()
            .enumerate()
        {
            let (Some(egress_link), Some(pfc)) = (queue.egress_link, queue.pfc.as_ref()) else {
                continue;
            };
            let expected = controllers_by_link_priority.get(&egress_link);
            for priority in 0..8 {
                let unexpected = pfc.paused_by_controller[priority]
                    .iter()
                    .find(|controller| {
                        !expected.is_some_and(|sets| sets[priority].contains(controller))
                    });
                if let Some(controller) = unexpected {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} queue {queue_index} PFC priority {priority} has pause state for undeclared controller {:?}",
                        owner.id, controller
                    )));
                }
            }
        }
    }

    // Host-link PFC: one scan of the host table when no host owns pause state.
    let host_pause_state = image.host_states.iter().any(|state| state.pfc.is_some());
    for owner in image
        .nodes
        .iter()
        .filter(|node| host_pause_state && node.kind == NodeKind::Host)
    {
        let state = &image.host_states[owner.state_slot as usize];
        let Some(pfc) = state.pfc.as_deref() else {
            continue;
        };
        if !controller_monitors
            .iter()
            .any(|(link, _)| *link == state.egress_link)
        {
            return Err(ValidationError::new(format!(
                "host node {:?} owns egress PFC state, but its egress link is not PFC-controlled ({:?})",
                owner.id, state.egress_link
            )));
        }
        let expected = controllers_by_link_priority.get(&state.egress_link);
        for priority in 0..8 {
            if let Some(controller) = pfc.paused_by_controller[priority]
                .iter()
                .find(|controller| {
                    !expected.is_some_and(|sets| sets[priority].contains(controller))
                })
            {
                return Err(ValidationError::new(format!(
                    "host node {:?} PFC priority {priority} has pause state for undeclared controller {:?}",
                    owner.id, controller
                )));
            }
        }
        let parked = expected_pause_parked(image, state, pfc);
        if pfc.pause_parked != parked {
            return Err(ValidationError::new(format!(
                "host node {:?} PFC parked list {:?} differs from its paused, parked, restartable queue pairs {:?}",
                owner.id, pfc.pause_parked, parked
            )));
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

/// Whether `priority` is paused at host `owner`'s egress (host-link PFC).
fn host_class_paused(image: &SimulationImage, owner: NodeId, priority: u8) -> bool {
    node(image, owner)
        .filter(|descriptor| descriptor.kind == NodeKind::Host)
        .and_then(|descriptor| {
            image.host_states[descriptor.state_slot as usize]
                .pfc
                .as_deref()
        })
        .is_some_and(|pfc| pfc.is_paused(usize::from(priority)))
}

/// The queue pairs a host's parked list must hold, by class: each queue pair whose data class is
/// paused there and whose pacer a paused tick parked with packets left to send (parked, not
/// stopped, `next_psn < total` and `snd_una < total`; LeanGuard's restartable parked pairs).
pub(crate) fn expected_pause_parked(
    image: &SimulationImage,
    state: &crate::HostState,
    pfc: &crate::HostPfcState,
) -> [BTreeSet<usize>; 8] {
    let mut expected: [BTreeSet<usize>; 8] = Default::default();
    for (position, generator) in state.generators.iter().enumerate() {
        let FlowGeneratorKind::Roce(roce) = generator.kind else {
            continue;
        };
        // A collective stage not yet released has never ticked, so no pause parked it.
        if state.stage(position).is_some_and(|stage| !stage.activated) {
            continue;
        }
        let Some(flow) = flow(image, generator.flow) else {
            continue;
        };
        let class = usize::from(flow.priority);
        if pfc.is_paused(class)
            && !roce.pacer_armed
            && generator.next_emission.status != GeneratorStatus::Stopped
            && roce.next_psn < roce.pacer.total_bytes
            && roce.snd_una < roce.pacer.total_bytes
        {
            expected[class].insert(position);
        }
    }
    expected
}

/// Pending `PacingTimer` events at `owner` that carry `payload`, at any time: a range of the
/// `(node, payload, time)` map, so the count costs a lookup and the matching entries, and
/// validation stays linear in the pending events.
fn pending_ticks_with_payload(
    pacing_counts: &BTreeMap<(NodeId, PayloadId, u64), usize>,
    owner: NodeId,
    payload: PayloadId,
) -> usize {
    pacing_counts
        .range((owner, payload, 0)..=(owner, payload, u64::MAX))
        .map(|(_, count)| *count)
        .sum()
}

/// A Go-back-N packet boundary: a multiple of the MTU, or the total byte count.
const fn roce_boundary(psn: u64, mtu_bytes: u64, total_bytes: u64) -> bool {
    psn == total_bytes || psn.is_multiple_of(mtu_bytes) && psn < total_bytes
}

/// A RoCE collective stage that its prerequisites have not released holds the state its release
/// starts from (`collectives-design.md` §6.1): nothing sent, acknowledged, credited, timed or
/// fed back; the pacer parked with its scheduled payload the pacing token; the pacer and the
/// controller anchored at zero (ruling C5), the controller otherwise as lowering builds it. The
/// release re-anchors both at its instant, so the control deadline must fit after any release up
/// to the stop. The caller has already required that no pending event carries either token, at any
/// time: no pacing tick or retransmission timeout with the pacing token, no control tick.
fn validate_unreleased_roce_stage(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
    roce: crate::RoceGenerator,
) -> Result<(), &'static str> {
    // Lowering builds the controller with its first control deadline one interval after the
    // anchor (`lowered_dcqcn_controller`), here zero.
    let config = roce.controller.config;
    let pristine_controller = crate::DcqcnController::new(config, config.control_interval_ns)
        .map_err(|_| "has a controller whose first control deadline exceeds u64")?;
    if generator.packets_emitted != 0
        || generator.bytes_emitted != 0
        || generator.feedback.arrivals != 0
        || roce.next_psn != 0
        || roce.snd_una != 0
        || roce.rto_deadline_ns != 0
        || roce.pacer.credit_quanta != 0
        || roce.pacer_armed
        || generator.next_emission.status != GeneratorStatus::Blocked
        || generator.next_emission.departure_time_ns != 0
        || roce.pacer.first_pacing_time_ns != 0
        || roce.controller != pristine_controller
    {
        return Err("collective stage is dependency-blocked after its sending state changed");
    }
    if roce.controller.config.control_interval_ns > u64::MAX - image.stop_time_ns {
        return Err("collective stage's first control deadline after a release exceeds u64");
    }
    Ok(())
}

/// The invariants of one RoCE queue pair's sender (design note `qp-design.md` §7).
#[allow(clippy::too_many_arguments)]
fn validate_roce_generator(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    owner: NodeId,
    generator: StagedGenerator<'_>,
    flow: &FlowDescriptor,
    roce: crate::RoceGenerator,
    pacing_counts: &BTreeMap<(NodeId, PayloadId, u64), usize>,
) -> Result<(), ValidationError> {
    let invalid =
        |what: &str| ValidationError::new(format!("flow {:?} RoCE queue pair {what}", flow.id));
    // A collective stage its prerequisites have not released (design note §6.1): pristine, parked
    // with no pending event, its anchors at zero (ruling C5). Its release arms the pacer.
    let unreleased = generator.stage.is_some_and(|stage| !stage.activated);
    let controller = roce.controller;
    controller
        .config
        .validate()
        .map_err(|error| invalid(&format!("has an invalid DCQCN configuration: {error}")))?;
    if controller.alpha_ppb > crate::DCQCN_FRACTION_SCALE
        || controller.current_rate_bps < controller.config.minimum_rate_bps
        || controller.current_rate_bps > controller.config.maximum_rate_bps
        || controller.target_rate_bps < controller.config.minimum_rate_bps
        || controller.target_rate_bps > controller.config.maximum_rate_bps
        || controller.stage_steps >= crate::DCQCN_STAGE_STEPS
        || controller.stage == crate::DcqcnIncreaseStage::Hyper && controller.stage_steps != 0
        || controller.cnp_seen && controller.last_cnp_time_ns.is_none()
    {
        return Err(invalid("has an out-of-range fixed-point controller state"));
    }
    let pacer = roce.pacer;
    if pacer.pacing_interval_ns == 0 || pacer.mtu_bytes == 0 || pacer.total_bytes == 0 {
        return Err(invalid("needs a positive pacing interval, MTU and total"));
    }
    // Go-back-N ordering: acknowledged <= next to send <= high-water mark <= total, each a
    // packet boundary, so every packet is a pure function of its PSN.
    remaining_generator_packets(generator)?;
    let high_water = generator.bytes_emitted;
    let ordered = roce.snd_una <= roce.next_psn
        && roce.next_psn <= high_water
        && high_water <= pacer.total_bytes;
    let on_boundaries = [roce.snd_una, roce.next_psn, high_water]
        .into_iter()
        .all(|psn| roce_boundary(psn, pacer.mtu_bytes, pacer.total_bytes));
    if !ordered || !on_boundaries {
        return Err(invalid(&format!(
            "sequence state is inconsistent: snd_una {}, next_psn {}, bytes_emitted {high_water}, total {}",
            roce.snd_una, roce.next_psn, pacer.total_bytes
        )));
    }
    let outstanding = high_water - roce.snd_una;
    if generator.feedback.outstanding_bytes != outstanding
        || generator.feedback.unacknowledged_bytes != outstanding
    {
        return Err(invalid(
            "feedback mirrors disagree with its outstanding bytes",
        ));
    }
    let complete = roce.snd_una == pacer.total_bytes;
    let status = generator.next_emission.status;
    if (status == GeneratorStatus::Finished) != complete {
        return Err(invalid(&format!(
            "status {status:?} disagrees with completion (snd_una {} of {})",
            roce.snd_una, pacer.total_bytes
        )));
    }
    // Tokens: a zero-byte control token and pacing token of this flow; the pacing token is the
    // generator's scheduled payload.
    for (token, kind) in [
        (roce.control_timer_payload, PacketKind::DcqcnControlTimer),
        (roce.pacing_timer_payload, PacketKind::RocePacingTimer),
    ] {
        let resident =
            packet(image, token).ok_or_else(|| invalid("names a missing timer token"))?;
        if resident.flow != flow.id
            || resident.kind != kind
            || resident.size_bytes != 0
            || resident.ecn_marked
        {
            return Err(invalid("timer token is inconsistent"));
        }
    }
    if roce.control_timer_payload == roce.pacing_timer_payload
        || generator.next_emission.payload != roce.pacing_timer_payload
    {
        return Err(invalid(
            "timer tokens are not distinct or not its scheduled payload",
        ));
    }
    // Pacer: armed iff exactly one pending tick carries the pacing token at the scheduled
    // departure, on the grid; its status predicts that tick.
    let departure = generator.next_emission.departure_time_ns;
    let ticks_at_departure = pacing_counts
        .get(&(owner, roce.pacing_timer_payload, departure))
        .copied()
        .unwrap_or(0);
    let on_grid = departure >= pacer.first_pacing_time_ns
        && (departure - pacer.first_pacing_time_ns).is_multiple_of(pacer.pacing_interval_ns);
    if roce.pacer_armed {
        if ticks_at_departure != 1 || !on_grid {
            return Err(invalid("armed pacer has no pending on-grid tick"));
        }
        if !matches!(
            status,
            GeneratorStatus::Scheduled | GeneratorStatus::Blocked | GeneratorStatus::Finished
        ) {
            return Err(invalid("armed pacer has a stopped status"));
        }
        if roce.next_psn < pacer.total_bytes && !complete {
            let size = pacer.mtu_bytes.min(pacer.total_bytes - roce.next_psn);
            let credit = pacer
                .credit_quanta
                .checked_add(
                    u128::from(controller.current_rate_bps) * u128::from(pacer.pacing_interval_ns),
                )
                .ok_or_else(|| invalid("pacing credit exceeds u128"))?;
            let cost = u128::from(size) * 8 * 1_000_000_000;
            let expected = if credit >= cost {
                GeneratorStatus::Scheduled
            } else {
                GeneratorStatus::Blocked
            };
            if status != expected {
                return Err(invalid("pacer status disagrees with its next-tick credit"));
            }
        }
    } else {
        // A parked pacer (pause-parked, waiting for feedback, or a gated stage) owns no tick at
        // any time: a stray one would run against a pacer that scheduled nothing.
        if pending_ticks_with_payload(pacing_counts, owner, roce.pacing_timer_payload) != 0 {
            return Err(invalid("parked pacer owns a pending tick"));
        }
        if status == GeneratorStatus::Scheduled {
            return Err(invalid("parked pacer is Scheduled"));
        }
        // A pacer parks with packets left to send only while its data class is paused at its
        // host (host-link PFC); `validate_pfc` pins it in the host's parked list. An unreleased
        // stage is parked before its first tick.
        if status == GeneratorStatus::Blocked
            && roce.next_psn < pacer.total_bytes
            && !unreleased
            && !host_class_paused(image, owner, flow.priority)
        {
            return Err(invalid("parked pacer has packets left to send"));
        }
    }
    if status == GeneratorStatus::Stopped && departure <= image.stop_time_ns {
        return Err(invalid("is Stopped at or before the stop time"));
    }
    // Credit capacity: every remaining tick at the maximum rate stays within u128 (checked
    // arithmetic: exact, and no heap allocation per queue pair).
    let ticks = roce_grid_ticks(image, &generator, roce);
    u128::from(controller.config.maximum_rate_bps)
        .checked_mul(u128::from(pacer.pacing_interval_ns))
        .and_then(|tick| tick.checked_mul(u128::from(ticks)))
        .and_then(|credit| credit.checked_add(pacer.credit_quanta))
        .ok_or_else(|| invalid("pacing credit can exceed u128 before stop"))?;
    // Retransmission timeout: armed iff data is outstanding and the timeout is on; exactly one
    // pending timeout event carries the pacing token, at the deadline.
    let timer_events =
        flow_index.retransmission_timeout_events_with_payload(owner, roce.pacing_timer_payload);
    if roce.rto_ns == 0 || outstanding == 0 {
        if timer_events != 0 || roce.rto_deadline_ns != 0 {
            return Err(invalid("owns a retransmission timer while none is armed"));
        }
    } else if timer_events != 1
        || flow_index.retransmission_timeout_events(
            owner,
            roce.pacing_timer_payload,
            roce.rto_deadline_ns,
        ) != 1
    {
        return Err(invalid(
            "armed retransmission timer has no single pending event",
        ));
    }
    if roce.rto_ns > u64::MAX - image.stop_time_ns {
        return Err(invalid(
            "retransmission timeout exceeds the deadline headroom",
        ));
    }
    // Control tick, exactly as DCQCN's; a completed pair keeps at most its last pending tick.
    let control_events = pacing_counts
        .get(&(
            owner,
            roce.control_timer_payload,
            controller.next_control_time_ns,
        ))
        .copied()
        .unwrap_or(0);
    // An unreleased stage arms its control tick at its release, so it owns none at any time.
    if unreleased
        && pending_ticks_with_payload(pacing_counts, owner, roce.control_timer_payload) != 0
    {
        return Err(invalid("owns a pending control tick before its release"));
    }
    let expected_control =
        usize::from(!unreleased && controller.next_control_time_ns <= image.stop_time_ns);
    if control_events > expected_control || !complete && control_events != expected_control {
        return Err(invalid(&format!(
            "has {control_events} matching control events; expected {expected_control}"
        )));
    }
    if unreleased {
        validate_unreleased_roce_stage(image, generator, roce).map_err(invalid)?;
    }
    if flow.reverse_route.is_empty() {
        return Err(invalid("requires a reverse feedback route"));
    }
    let receiver =
        roce_receiver(image, flow).ok_or_else(|| invalid("has no receiver at its target"))?;
    if receiver.total_bytes != pacer.total_bytes
        || receiver.np.cnp_interval_ns != controller.config.cnp_interval_ns
        || receiver.np.cnp_size_bytes == 0
        || receiver.ack_size_bytes == 0
        || receiver.ack_every_packets == 0
        || receiver.packets_since_ack >= receiver.ack_every_packets
        || !receiver.duplicate_ack && roce.rto_ns != 0
        || !roce_boundary(receiver.expected_psn, pacer.mtu_bytes, pacer.total_bytes)
        || receiver.expected_psn < roce.snd_una
        || receiver.expected_psn > high_water
        || receiver
            .last_nack
            .is_some_and(|mark| mark.expected_psn > receiver.expected_psn)
    {
        return Err(invalid("receiver state disagrees with its sender"));
    }
    Ok(())
}

/// Every resident RoCE packet belongs to a queue pair and matches its PSN arithmetic.
fn validate_roce_packets(image: &SimulationImage) -> Result<(), ValidationError> {
    for packet in &image.initial_packets {
        let (psn, ack) = match packet.kind {
            PacketKind::RoceData(header) => (Some(header.psn), None),
            PacketKind::RoceAck(header) | PacketKind::RoceNack(header) => {
                (None, Some(header.acknowledgment))
            }
            PacketKind::RocePacingTimer => (None, None),
            _ => continue,
        };
        let invalid = || {
            ValidationError::new(format!(
                "RoCE packet {:?} of flow {:?} is inconsistent with its queue pair",
                packet.id, packet.flow
            ))
        };
        let flow = flow(image, packet.flow).ok_or_else(invalid)?;
        let source = node(image, flow.source).ok_or_else(invalid)?;
        let (generator, roce) = image
            .host_states
            .get(source.state_slot as usize)
            .and_then(|state| {
                state
                    .generators
                    .iter()
                    .find(|generator| generator.flow == flow.id)
            })
            .and_then(|generator| match generator.kind {
                FlowGeneratorKind::Roce(roce) => Some((generator, roce)),
                _ => None,
            })
            .ok_or_else(invalid)?;
        let pacer = roce.pacer;
        if let Some(psn) = psn {
            if !roce_boundary(psn, pacer.mtu_bytes, pacer.total_bytes)
                || psn >= generator.bytes_emitted
                || packet.size_bytes != pacer.mtu_bytes.min(pacer.total_bytes - psn)
            {
                return Err(invalid());
            }
        }
        if let Some(ack) = ack {
            let frontier = roce_receiver(image, flow).ok_or_else(invalid)?.expected_psn;
            if !roce_boundary(ack, pacer.mtu_bytes, pacer.total_bytes) || ack > frontier {
                return Err(invalid());
            }
        }
    }
    Ok(())
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
        // Data holds the flow's class along its route; receiver feedback holds the feedback class
        // along the reverse route.
        for (route, priority) in [
            (&flow.route, flow.priority),
            (&flow.reverse_route, flow.feedback_priority),
        ] {
            for pair in route.windows(2) {
                let from = (pair[0], priority);
                let to = (pair[1], priority);
                if controlled.contains(&from)
                    && controlled.contains(&to)
                    && edges.entry(from).or_default().insert(to)
                {
                    *indegree.entry(to).or_default() += 1;
                }
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

fn validate_pfc_causal_consistency(image: &SimulationImage) -> Result<(), ValidationError> {
    for controller in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        let controller_state = &image.switch_states[controller.state_slot as usize];
        for (controller_queue_index, queue) in controller_state.queues.iter().enumerate() {
            let Some(pfc) = &queue.pfc else {
                continue;
            };
            for ingress in &pfc.ingresses {
                let controlled = link(image, ingress.controlled_link)
                    .expect("PFC structural validation established the controlled link");
                let upstream = node(image, controlled.source)
                    .expect("link validation established the controlled-link source");
                let (upstream_queue_index, upstream_paused_sets) = match upstream.kind {
                    NodeKind::Switch => image.switch_states[upstream.state_slot as usize]
                        .queues
                        .iter()
                        .enumerate()
                        .find_map(|(queue_index, queue)| {
                            (queue.egress_link == Some(controlled.id))
                                .then_some((queue_index, queue.pfc.as_ref()))
                        })
                        .and_then(|(queue_index, pfc)| {
                            pfc.map(|pfc| (queue_index, &pfc.paused_by_controller))
                        }),
                    // A host's egress is its queue 0.
                    NodeKind::Host => image.host_states[upstream.state_slot as usize]
                        .pfc
                        .as_deref()
                        .map(|pfc| (0, &pfc.paused_by_controller)),
                }
                .expect("PFC structural validation established upstream controller state");

                for priority in 0..8 {
                    if ingress.xoff_threshold_bytes[priority] == 0 {
                        continue;
                    }
                    let upstream_paused = upstream_paused_sets
                        .get(priority)
                        .is_some_and(|controllers| controllers.contains(&controller.id));
                    let asserted = ingress.pause_asserted[priority];
                    let mut actions = image
                        .initial_events
                        .iter()
                        .filter_map(|event| {
                            if event.kind != EventKind::RemoteArrival
                                || event.key.origin_node != controller.id
                                || event.target != upstream.id
                            {
                                return None;
                            }
                            let packet = packet(image, event.payload)
                                .expect("event validation established the PFC payload");
                            let PacketKind::Pfc(header) = packet.kind else {
                                return None;
                            };
                            (header.controlled_link == controlled.id
                                && usize::from(header.priority) == priority)
                                .then_some((event.key, header.pause))
                        })
                        .collect::<Vec<_>>();
                    actions.sort_unstable_by_key(|(key, _)| *key);
                    let reconciled = actions.iter().fold(upstream_paused, |_, (_, pause)| *pause);
                    if reconciled != asserted {
                        let pending = actions
                            .iter()
                            .map(|(_, pause)| if *pause { "pause" } else { "resume" })
                            .collect::<Vec<_>>();
                        return Err(ValidationError::new(format!(
                            "switch node {:?} queue {upstream_queue_index} PFC causal pause-state mismatch for controlled link {:?} priority {priority} and controller {:?} queue {controller_queue_index}: upstream paused={upstream_paused}, controller asserted={asserted}, in-flight actions={pending:?}",
                            upstream.id, controlled.id, controller.id
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

fn pfc_channel_ingress(
    image: &SimulationImage,
    channel_index: usize,
) -> Option<&crate::PfcIngressState> {
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
}

fn pfc_channel_controls(image: &SimulationImage, channel_index: usize) -> Option<LinkId> {
    pfc_channel_ingress(image, channel_index).map(|ingress| ingress.controlled_link)
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

fn validate_generators(
    image: &SimulationImage,
    flow_index: &FlowIndex,
) -> Result<(), ValidationError> {
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
    let mut dcqcn_receiver_owners = BTreeMap::<crate::FlowId, NodeId>::new();
    let mut roce_receiver_owners = BTreeMap::<crate::FlowId, NodeId>::new();
    let mut collective_positions =
        BTreeMap::<(u64, crate::CollectivePhase, u32, u32), crate::FlowId>::new();
    let mut claimed_tcp_timers = BTreeMap::<(NodeId, PayloadId, u64), crate::FlowId>::new();
    let mut compute_positions = BTreeMap::<(u64, u32), crate::FlowId>::new();
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
        let mut previous_dcqcn_receiver = None;
        for receiver in &state.dcqcn_receivers {
            if previous_dcqcn_receiver.is_some_and(|flow| flow >= receiver.flow) {
                return Err(ValidationError::new(format!(
                    "host node {:?} has duplicate or non-increasing DCQCN receiver flow {:?}",
                    owner.id, receiver.flow
                )));
            }
            previous_dcqcn_receiver = Some(receiver.flow);
            let receiver_flow = flow(image, receiver.flow).ok_or_else(|| {
                ValidationError::new(format!(
                    "host node {:?} DCQCN receiver references unknown flow {:?}",
                    owner.id, receiver.flow
                ))
            })?;
            if receiver_flow.target != owner.id
                || receiver.cnp_size_bytes == 0
                || dcqcn_receiver_owners
                    .insert(receiver.flow, owner.id)
                    .is_some()
            {
                return Err(ValidationError::new(format!(
                    "host node {:?} owns inconsistent DCQCN receiver state for flow {:?}",
                    owner.id, receiver.flow
                )));
            }
        }
        if let Some(receivers) = &state.roce_receivers {
            if receivers.is_empty() {
                return Err(ValidationError::new(format!(
                    "host node {:?} holds an empty RoCE receiver table; a host without RoCE receivers holds none",
                    owner.id
                )));
            }
            let mut previous_roce_receiver = None;
            for receiver in receivers.iter() {
                let receiver_flow = receiver.np.flow;
                if previous_roce_receiver.is_some_and(|flow| flow >= receiver_flow) {
                    return Err(ValidationError::new(format!(
                        "host node {:?} has duplicate or non-increasing RoCE receiver flow {receiver_flow:?}",
                        owner.id
                    )));
                }
                previous_roce_receiver = Some(receiver_flow);
                let target = flow(image, receiver_flow).map(|flow| flow.target);
                if target != Some(owner.id)
                    || roce_receiver_owners
                        .insert(receiver_flow, owner.id)
                        .is_some()
                {
                    return Err(ValidationError::new(format!(
                        "host node {:?} owns inconsistent RoCE receiver state for flow {receiver_flow:?}",
                        owner.id
                    )));
                }
            }
        }
        let mut previous = None;
        for (index, generator) in staged_generators(state).enumerate() {
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

            if let Some(stage) = generator.stage {
                validate_collective_stage(
                    image,
                    flow_index,
                    state,
                    flow,
                    generator,
                    *stage,
                    &mut collective_positions,
                    &mut compute_positions,
                )?;
                if is_compute_generator(generator) {
                    // `validate_compute_stage` owns every invariant of a timer-only stage.
                    continue;
                }
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
                validate_tcp_timer_capacity(image, flow_index, generator, tcp)?;
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
                    GeneratorStatus::Blocked
                        if generator.stage.is_some_and(|stage| !stage.activated) =>
                    {
                        validate_unreleased_tcp_stage(flow.id, generator, tcp)?;
                    }
                    GeneratorStatus::Blocked => {
                        validate_blocked_tcp_timer(image, flow_index, owner.id, flow.id, tcp)?;
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
                if rate.pacing_interval_ns == 0
                    || rate.packet_size_bytes == 0
                    || rate.total_bytes == 0
                    || rate.rate_numerator_bits_per_second == 0
                    || rate.rate_denominator == 0
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} rate source requires positive pacing interval, packet size, total bytes, and rational rate",
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

            if let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind {
                dcqcn.controller.config.validate().map_err(|error| {
                    ValidationError::new(format!(
                        "flow {:?} has invalid DCQCN configuration: {error}",
                        flow.id
                    ))
                })?;
                let rate = dcqcn.rate;
                if rate.pacing_interval_ns == 0
                    || rate.packet_size_bytes == 0
                    || rate.total_bytes == 0
                    || rate.rate_denominator != 1
                    || rate.rate_numerator_bits_per_second != dcqcn.controller.current_rate_bps
                    || dcqcn.cnp_size_bytes == 0
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN rate, packet, CNP, or controller state is inconsistent",
                        flow.id
                    )));
                }
                if dcqcn.controller.alpha_ppb > crate::DCQCN_FRACTION_SCALE
                    || dcqcn.controller.current_rate_bps < dcqcn.controller.config.minimum_rate_bps
                    || dcqcn.controller.current_rate_bps > dcqcn.controller.config.maximum_rate_bps
                    || dcqcn.controller.target_rate_bps < dcqcn.controller.config.minimum_rate_bps
                    || dcqcn.controller.target_rate_bps > dcqcn.controller.config.maximum_rate_bps
                    || dcqcn.controller.stage_steps >= crate::DCQCN_STAGE_STEPS
                    || dcqcn.controller.stage == crate::DcqcnIncreaseStage::Hyper
                        && dcqcn.controller.stage_steps != 0
                    || dcqcn.controller.cnp_seen && dcqcn.controller.last_cnp_time_ns.is_none()
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN fixed-point controller state is out of range",
                        flow.id
                    )));
                }
                if generator.bytes_emitted > rate.total_bytes {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN emitted bytes {} exceed total bytes {}",
                        flow.id, generator.bytes_emitted, rate.total_bytes
                    )));
                }
                let scale = u128::from(rate.rate_denominator)
                    .checked_mul(1_000_000_000)
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} DCQCN credit scale exceeds u128",
                            flow.id
                        ))
                    })?;
                let tick_credit = u128::from(dcqcn.controller.current_rate_bps)
                    .checked_mul(u128::from(rate.pacing_interval_ns))
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} DCQCN tick credit exceeds u128",
                            flow.id
                        ))
                    })?;
                let remaining_ticks = if matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                ) && generator.next_emission.departure_time_ns
                    <= image.stop_time_ns
                {
                    BigUint::from(
                        (image.stop_time_ns - generator.next_emission.departure_time_ns)
                            / rate.pacing_interval_ns,
                    ) + 1_u8
                } else {
                    BigUint::from(0_u8)
                };
                let maximum_future_credit = BigUint::from(rate.credit_quanta)
                    + BigUint::from(dcqcn.controller.config.maximum_rate_bps)
                        * BigUint::from(rate.pacing_interval_ns)
                        * remaining_ticks;
                if maximum_future_credit > BigUint::from(u128::MAX) {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN pacing credit can exceed u128 before stop",
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
                        "flow {:?} DCQCN status {:?} is inconsistent with {remaining_bytes} remaining bytes",
                        flow.id, generator.next_emission.status
                    )));
                }
                if generator.next_emission.status == GeneratorStatus::Finished
                    && remaining_bytes != 0
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN is Finished with {remaining_bytes} bytes remaining",
                        flow.id
                    )));
                }
                if active {
                    let data = packet(image, generator.next_emission.payload).ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} DCQCN pacing timer references unknown packet {:?}",
                            flow.id, generator.next_emission.payload
                        ))
                    })?;
                    let expected_size = rate.packet_size_bytes.min(remaining_bytes);
                    if data.flow != flow.id
                        || data.kind != PacketKind::Data
                        || data.size_bytes != expected_size
                    {
                        return Err(ValidationError::new(format!(
                            "flow {:?} DCQCN pacing token {:?} does not match its next data packet",
                            flow.id, data.id
                        )));
                    }
                    let packet_cost = u128::from(expected_size)
                        .checked_mul(8)
                        .and_then(|bits| bits.checked_mul(scale))
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} DCQCN next-packet credit cost exceeds u128",
                                flow.id
                            ))
                        })?;
                    let can_emit = rate
                        .credit_quanta
                        .checked_add(tick_credit)
                        .is_some_and(|credit| credit >= packet_cost);
                    let expected_status = if can_emit {
                        GeneratorStatus::Scheduled
                    } else {
                        GeneratorStatus::Blocked
                    };
                    if generator.next_emission.status != expected_status {
                        return Err(ValidationError::new(format!(
                            "flow {:?} DCQCN timer status disagrees with next-tick credit",
                            flow.id
                        )));
                    }
                    let deadline = generator.next_emission.departure_time_ns;
                    if deadline < rate.first_pacing_time_ns
                        || (deadline - rate.first_pacing_time_ns) % rate.pacing_interval_ns != 0
                        || pacing_counts
                            .get(&(owner.id, data.id, deadline))
                            .copied()
                            .unwrap_or(0)
                            != 1
                    {
                        return Err(ValidationError::new(format!(
                            "flow {:?} DCQCN data pacing event is missing or off-grid",
                            flow.id
                        )));
                    }
                } else if generator.next_emission.status == GeneratorStatus::Stopped
                    && generator.next_emission.departure_time_ns <= image.stop_time_ns
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN is Stopped at or before stop time",
                        flow.id
                    )));
                }
                let control = packet(image, dcqcn.control_timer_payload).ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} DCQCN control timer references unknown token {:?}",
                        flow.id, dcqcn.control_timer_payload
                    ))
                })?;
                if control.flow != flow.id
                    || control.kind != PacketKind::DcqcnControlTimer
                    || control.size_bytes != 0
                    || control.ecn_marked
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN control token is inconsistent",
                        flow.id
                    )));
                }
                let control_matches = pacing_counts
                    .get(&(owner.id, control.id, dcqcn.controller.next_control_time_ns))
                    .copied()
                    .unwrap_or(0);
                let expected_control =
                    usize::from(dcqcn.controller.next_control_time_ns <= image.stop_time_ns);
                if control_matches != expected_control {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN has {control_matches} matching control events; expected {expected_control}",
                        flow.id
                    )));
                }
                if flow.reverse_route.is_empty() {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN requires a reverse CNP route",
                        flow.id
                    )));
                }
                let target = node(image, flow.target).expect("flow validation established target");
                let receiver = image.host_states[target.state_slot as usize]
                    .dcqcn_receivers
                    .iter()
                    .find(|receiver| receiver.flow == flow.id)
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "flow {:?} DCQCN has no receiver state at target {:?}",
                            flow.id, flow.target
                        ))
                    })?;
                if receiver.cnp_interval_ns != dcqcn.controller.config.cnp_interval_ns
                    || receiver.cnp_size_bytes != dcqcn.cnp_size_bytes
                {
                    return Err(ValidationError::new(format!(
                        "flow {:?} DCQCN receiver and controller CNP parameters disagree",
                        flow.id
                    )));
                }
                continue;
            }

            if let FlowGeneratorKind::Roce(roce) = generator.kind {
                validate_roce_generator(
                    image,
                    flow_index,
                    owner.id,
                    generator,
                    flow,
                    roce,
                    &pacing_counts,
                )?;
                continue;
            }

            let FlowGeneratorKind::Constant(constant) = generator.kind else {
                unreachable!("non-constant generators continue above")
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
    validate_collective_partitions(image)?;
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
    for (receiver_flow, receiver_owner) in dcqcn_receiver_owners {
        let is_dcqcn = image
            .host_states
            .iter()
            .flat_map(staged_generators)
            .find(|generator| generator.flow == receiver_flow)
            .is_some_and(|generator| matches!(generator.kind, FlowGeneratorKind::Dcqcn(_)));
        if !is_dcqcn {
            return Err(ValidationError::new(format!(
                "host node {receiver_owner:?} owns DCQCN receiver state for flow {receiver_flow:?}, but the flow generator is not DCQCN"
            )));
        }
    }
    for (receiver_flow, receiver_owner) in roce_receiver_owners {
        let is_roce = image
            .host_states
            .iter()
            .flat_map(staged_generators)
            .find(|generator| generator.flow == receiver_flow)
            .is_some_and(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)));
        if !is_roce {
            return Err(ValidationError::new(format!(
                "host node {receiver_owner:?} owns RoCE receiver state for flow {receiver_flow:?}, but the flow generator is not a RoCE queue pair"
            )));
        }
    }
    validate_roce_packets(image)?;
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

#[derive(Clone)]
struct CollectivePartitionState {
    algorithm: crate::CollectiveAlgorithm,
    topology_level: u32,
    topology_group: u32,
    group_size: u32,
    declared_total_bytes: u64,
    bounds_by_owner: BTreeMap<u32, (u64, u64)>,
    copies_by_owner: BTreeMap<u32, u64>,
}

fn validate_collective_partitions(image: &SimulationImage) -> Result<(), ValidationError> {
    let mut collectives = BTreeMap::<u64, CollectivePartitionState>::new();
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let Some(crate::CollectiveStage {
            role: crate::StageRole::Collective(stage),
            ..
        }) = generator.stage
        else {
            continue;
        };
        let group_size = u64::from(stage.group_size);
        let owner_offset = match (stage.algorithm, stage.phase) {
            (crate::CollectiveAlgorithm::AllGather, crate::CollectivePhase::AllGather)
            | (crate::CollectiveAlgorithm::RingAllReduce, crate::CollectivePhase::ReduceScatter) => {
                1
            }
            (crate::CollectiveAlgorithm::RingAllReduce, crate::CollectivePhase::AllGather) => 2,
            (crate::CollectiveAlgorithm::AllGather, crate::CollectivePhase::ReduceScatter) => {
                return Err(ValidationError::new(format!(
                    "flow {:?} collective partition has an invalid AllGather phase",
                    generator.flow
                )));
            }
        };
        let owner = (u64::from(stage.rank) + group_size - u64::from(stage.step) + owner_offset)
            % group_size;
        let owner = u32::try_from(owner).expect("collective owner is bounded by u32 group size");
        let bounds = (stage.chunk_offset_bytes, stage.chunk_bytes);
        let state =
            collectives
                .entry(stage.collective_id)
                .or_insert_with(|| CollectivePartitionState {
                    algorithm: stage.algorithm,
                    topology_level: stage.topology_level,
                    topology_group: stage.topology_group,
                    group_size: stage.group_size,
                    declared_total_bytes: stage.declared_total_bytes,
                    bounds_by_owner: BTreeMap::new(),
                    copies_by_owner: BTreeMap::new(),
                });
        if state.algorithm != stage.algorithm
            || state.topology_level != stage.topology_level
            || state.topology_group != stage.topology_group
            || state.group_size != stage.group_size
            || state.declared_total_bytes != stage.declared_total_bytes
        {
            return Err(ValidationError::new(format!(
                "collective partition {} metadata is inconsistent across propagation stages",
                stage.collective_id
            )));
        }
        if state
            .bounds_by_owner
            .get(&owner)
            .is_some_and(|expected| *expected != bounds)
        {
            return Err(ValidationError::new(format!(
                "collective partition {} owner {owner} has inconsistent bounds across propagation stages",
                stage.collective_id
            )));
        }
        state.bounds_by_owner.insert(owner, bounds);
        let copies = state.copies_by_owner.entry(owner).or_default();
        *copies = copies.checked_add(1).ok_or_else(|| {
            ValidationError::new(format!(
                "collective partition {} propagation count exceeds u32",
                stage.collective_id
            ))
        })?;
    }

    for (collective_id, state) in collectives {
        let expected_copies = match state.algorithm {
            crate::CollectiveAlgorithm::AllGather => u64::from(state.group_size - 1),
            crate::CollectiveAlgorithm::RingAllReduce => 2 * u64::from(state.group_size - 1),
        };
        if state.bounds_by_owner.len() != state.group_size as usize
            || state.copies_by_owner.len() != state.group_size as usize
            || state
                .copies_by_owner
                .values()
                .any(|copies| *copies != expected_copies)
        {
            return Err(ValidationError::new(format!(
                "collective partition {collective_id} does not propagate every owner exactly {expected_copies} times"
            )));
        }
        let base = state.declared_total_bytes / u64::from(state.group_size);
        for (index, (owner, (offset, bytes))) in state.bounds_by_owner.into_iter().enumerate() {
            let owner_u64 = u64::from(owner);
            let expected_offset = owner_u64.checked_mul(base).ok_or_else(|| {
                ValidationError::new(format!(
                    "collective partition {collective_id} declared-total offset exceeds u64"
                ))
            })?;
            let expected_bytes = if owner + 1 == state.group_size {
                state.declared_total_bytes - expected_offset
            } else {
                base
            };
            if usize::try_from(owner).ok() != Some(index)
                || offset != expected_offset
                || bytes != expected_bytes
            {
                return Err(ValidationError::new(format!(
                    "collective partition {collective_id} does not match declared total {} under EqualRemainderLast at owner {owner}",
                    state.declared_total_bytes
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
        .flat_map(staged_generators)
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

fn validate_tcp_segment_ledger(
    image: &SimulationImage,
    flow_index: &FlowIndex,
) -> Result<(), ValidationError> {
    let ledger = crate::tcp_ledger::seed_image(image).map_err(|conflict| {
        ValidationError::new(format!(
            "TCP flow {:?} sequence {} changed segment size from {} to {} bytes",
            conflict.flow,
            conflict.sequence,
            conflict.original_size_bytes,
            conflict.replacement_size_bytes
        ))
    })?;
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
            continue;
        };
        for packet in flow_index.packets_for_flow(image, generator.flow) {
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

/// A generator together with its stage record, borrowed from the host's stage table by position.
///
/// The validator asks about a generator's stage wherever it asked about the generator's former
/// `stage` field; this view carries both, and dereferences to the generator for everything else.
/// Validators take it by value, so it holds the record by reference: two pointers per copy.
#[derive(Clone, Copy)]
struct StagedGenerator<'a> {
    generator: &'a crate::FlowGeneratorState,
    stage: Option<&'a crate::CollectiveStage>,
}

impl<'a> StagedGenerator<'a> {
    /// `generator` in place of this view's generator, with the same stage record.
    const fn with_generator<'b>(
        self,
        generator: &'b crate::FlowGeneratorState,
    ) -> StagedGenerator<'b>
    where
        'a: 'b,
    {
        StagedGenerator {
            generator,
            stage: self.stage,
        }
    }
}

impl std::ops::Deref for StagedGenerator<'_> {
    type Target = crate::FlowGeneratorState;

    fn deref(&self) -> &crate::FlowGeneratorState {
        self.generator
    }
}

/// The generator at `position` of `state`, with its stage record.
fn staged_generator(state: &crate::HostState, position: usize) -> StagedGenerator<'_> {
    StagedGenerator {
        generator: &state.generators[position],
        stage: state.stages.get(position).and_then(Option::as_ref),
    }
}

/// `state`'s generators in table order, each with its stage record.
///
/// Each generator pairs with the stage table's entry at its position, or with `None` past the
/// table's end, exactly as [`staged_generator`] reads it, whatever the table's length. On a host
/// without stages the table is empty, every generator pairs with `None`, and no entry is read.
fn staged_generators(state: &crate::HostState) -> impl Iterator<Item = StagedGenerator<'_>> {
    let stages = state
        .stages
        .iter()
        .map(Option::as_ref)
        .chain(std::iter::repeat(None));
    state
        .generators
        .iter()
        .zip(stages)
        .map(|(generator, stage)| StagedGenerator { generator, stage })
}

fn is_compute_generator(generator: StagedGenerator<'_>) -> bool {
    generator
        .stage
        .is_some_and(|stage| matches!(stage.role, crate::StageRole::Compute(_)))
}

/// Collective identity of a stage, when the generator carries one.
fn collective_identity(generator: StagedGenerator<'_>) -> Option<crate::CollectiveStageIdentity> {
    match generator.stage?.role {
        crate::StageRole::Collective(identity) => Some(identity),
        crate::StageRole::Compute(_) => None,
    }
}

/// Dependency-structure invariants of one dependency-gated stage.
///
/// A collective stage's predecessors follow the ring recurrence of its declared algorithm. The
/// local predecessor is complete exactly when its generator has finished (TCP: all bytes
/// acknowledged). The inbound byte count equals this host's in-order TCP frontier for the inbound
/// flow, so the inbound flag is complete exactly when that frontier reaches the chunk.
#[allow(clippy::too_many_arguments)]
fn validate_collective_stage(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    state: &crate::HostState,
    flow: &crate::FlowDescriptor,
    generator: StagedGenerator<'_>,
    stage: crate::CollectiveStage,
    positions: &mut BTreeMap<(u64, crate::CollectivePhase, u32, u32), crate::FlowId>,
    compute_positions: &mut BTreeMap<(u64, u32), crate::FlowId>,
) -> Result<(), ValidationError> {
    let dependencies = stage.dependencies;
    let collective = match stage.role {
        crate::StageRole::Collective(collective) => collective,
        crate::StageRole::Compute(compute) => {
            return validate_compute_stage(
                image,
                flow_index,
                state,
                flow,
                generator,
                compute,
                stage,
                compute_positions,
            );
        }
    };
    let transport_bytes = match generator.kind {
        FlowGeneratorKind::Tcp(tcp) => tcp.total_bytes,
        FlowGeneratorKind::Roce(roce) => roce.pacer.total_bytes,
        _ => {
            return Err(ValidationError::new(format!(
                "flow {:?} collective stage record requires a TCP or RoCE generator",
                flow.id
            )));
        }
    };
    let position = (
        collective.collective_id,
        collective.phase,
        collective.rank,
        collective.step,
    );
    if let Some(previous_flow) = positions.insert(position, flow.id) {
        return Err(ValidationError::new(format!(
            "flow {:?} has duplicate collective stage position ({}, {:?}, {}, {}), already owned by flow {:?}",
            flow.id,
            collective.collective_id,
            collective.phase,
            collective.rank,
            collective.step,
            previous_flow
        )));
    }
    if collective.chunk_policy != crate::CollectiveChunkPolicy::EqualRemainderLast
        || collective.channel_policy != crate::CollectiveChannelPolicy::RingNext
        || collective.algorithm == crate::CollectiveAlgorithm::AllGather
            && collective.phase != crate::CollectivePhase::AllGather
    {
        return Err(ValidationError::new(format!(
            "flow {:?} collective policy or phase is inconsistent with its algorithm",
            flow.id
        )));
    }
    if collective.group_size < 2
        || collective.rank >= collective.group_size
        || collective.step == 0
        || collective.step >= collective.group_size
        || collective.declared_total_bytes == 0
        || collective.chunk_bytes == 0
    {
        return Err(ValidationError::new(format!(
            "flow {:?} collective dimensions and chunk must be in range",
            flow.id
        )));
    }
    collective
        .chunk_offset_bytes
        .checked_add(collective.chunk_bytes)
        .ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} collective chunk endpoint exceeds u64",
                flow.id
            ))
        })?;
    if transport_bytes != collective.chunk_bytes {
        return Err(ValidationError::new(format!(
            "flow {:?} collective stage carries {} bytes for a {}-byte chunk",
            flow.id, transport_bytes, collective.chunk_bytes
        )));
    }
    if dependencies.inbound_bytes_received > dependencies.inbound_predecessor_bytes {
        return Err(ValidationError::new(format!(
            "flow {:?} collective inbound bytes {} exceed required bytes {}",
            flow.id, dependencies.inbound_bytes_received, dependencies.inbound_predecessor_bytes
        )));
    }

    let final_step = collective.group_size - 1;
    let root = collective.step == 1
        && (collective.algorithm == crate::CollectiveAlgorithm::AllGather
            || collective.phase == crate::CollectivePhase::ReduceScatter);
    let find_stage = |rank: u32| {
        let (phase, step) = if collective.step > 1 {
            (collective.phase, collective.step - 1)
        } else {
            (crate::CollectivePhase::ReduceScatter, final_step)
        };
        flow_index
            .collective_stage(image, (collective.collective_id, phase, rank, step))
            .map(|candidate| candidate.flow)
    };
    let previous_rank = if collective.rank == 0 {
        collective.group_size - 1
    } else {
        collective.rank - 1
    };
    // A root may follow the same-rank stage of a compute group on its own host.
    let entry = dependencies
        .local_predecessor
        .filter(|_| root)
        .and_then(|id| flow_index.generator_for_flow(image, id))
        .filter(|candidate| {
            matches!(candidate.stage.map(|stage| stage.role),
                Some(crate::StageRole::Compute(compute))
                    if compute.rank == collective.rank && compute.group_size == collective.group_size)
                && flow_source(image, candidate.flow) == Some(flow.source)
        });
    let (expected_local, expected_inbound) = if root {
        (entry.map(|candidate| candidate.flow), None)
    } else {
        (find_stage(collective.rank), find_stage(previous_rank))
    };
    if dependencies.local_predecessor != expected_local
        || dependencies.inbound_predecessor != expected_inbound
    {
        return Err(ValidationError::new(format!(
            "flow {:?} collective predecessor identities do not match the declared algorithm recurrence",
            flow.id
        )));
    }
    if root {
        let local_complete =
            entry.is_none_or(|entry| entry.next_emission.status == GeneratorStatus::Finished);
        if dependencies.local_predecessor_complete != local_complete
            || !dependencies.inbound_predecessor_complete
            || dependencies.inbound_predecessor_bytes != collective.chunk_bytes
            || dependencies.inbound_bytes_received != 0
        {
            return Err(ValidationError::new(format!(
                "flow {:?} collective root prerequisite state is inconsistent",
                flow.id
            )));
        }
    } else {
        let local = expected_local
            .and_then(|id| flow_index.generator_for_flow(image, id))
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} collective local predecessor is missing",
                    flow.id
                ))
            })?;
        if dependencies.local_predecessor_complete
            != (local.next_emission.status == GeneratorStatus::Finished)
        {
            return Err(ValidationError::new(format!(
                "flow {:?} collective local completion flag disagrees with predecessor state",
                flow.id
            )));
        }
        let inbound = expected_inbound
            .and_then(|id| flow_index.generator_for_flow(image, id))
            .and_then(|candidate| collective_identity(candidate).map(|stage| (candidate, stage)))
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} collective inbound predecessor is missing",
                    flow.id
                ))
            })?;
        if inbound.1.chunk_bytes != dependencies.inbound_predecessor_bytes
            || inbound.1.chunk_offset_bytes != collective.chunk_offset_bytes
            || self::flow(image, inbound.0.flow)
                .is_none_or(|predecessor_flow| predecessor_flow.target != flow.source)
        {
            return Err(ValidationError::new(format!(
                "flow {:?} collective inbound predecessor does not deliver the declared chunk to its source",
                flow.id
            )));
        }
        if dependencies.inbound_predecessor_complete
            != (dependencies.inbound_bytes_received == dependencies.inbound_predecessor_bytes)
        {
            return Err(ValidationError::new(format!(
                "flow {:?} collective inbound completion flag disagrees with received bytes",
                flow.id
            )));
        }
        // One collective, one transport: its stages share one traffic key.
        if std::mem::discriminant(&inbound.0.kind) != std::mem::discriminant(&generator.kind)
            || std::mem::discriminant(&local.kind) != std::mem::discriminant(&generator.kind)
        {
            return Err(ValidationError::new(format!(
                "flow {:?} collective predecessors use another transport",
                flow.id
            )));
        }
        let frontier = host_inbound_frontier(state, &inbound.0);
        if frontier != Some(dependencies.inbound_bytes_received) {
            return Err(ValidationError::new(format!(
                "flow {:?} collective inbound bytes {} disagree with the in-order {} frontier {:?} of flow {:?}",
                flow.id,
                dependencies.inbound_bytes_received,
                transport_label(&inbound.0),
                frontier,
                inbound.0.flow
            )));
        }
    }
    let prerequisites_complete = dependencies.prerequisites_complete();
    if stage.activated != prerequisites_complete {
        return Err(ValidationError::new(format!(
            "flow {:?} collective release flag disagrees with its prerequisites",
            flow.id
        )));
    }
    Ok(())
}

/// The source of flow `id`. `validate_flow_ids` has established `flows[i].id == i` with unique
/// identifiers before any stage is validated, so the dense lookup finds exactly the descriptor
/// the linear `find(|flow| flow.id == id)` found.
fn flow_source(image: &SimulationImage, id: crate::FlowId) -> Option<NodeId> {
    flow(image, id).map(|flow| flow.source)
}

/// The in-order frontier this host's receiver holds for the inbound predecessor `predecessor`:
/// TCP's next expected sequence, or a RoCE queue pair's Go-back-N expected PSN.
fn host_inbound_frontier(
    state: &crate::HostState,
    predecessor: &crate::FlowGeneratorState,
) -> Option<u64> {
    match predecessor.kind {
        FlowGeneratorKind::Roce(_) => state
            .roce_receivers
            .as_deref()?
            .binary_search_by_key(&predecessor.flow, |receiver| receiver.np.flow)
            .ok()
            .map(|index| state.roce_receivers.as_deref().expect("searched")[index].expected_psn),
        _ => host_tcp_receiver(state, predecessor.flow)
            .map(|receiver| receiver.next_expected_sequence),
    }
}

/// The transport of a stage generator, as validation errors name it.
/// A RoCE queue pair's MTU and pacing interval, which a compute stage after it writes on its
/// progress rows (schema Amendment 5); `None` for any other generator.
const fn roce_transport(kind: &FlowGeneratorKind) -> Option<(u64, u64)> {
    match kind {
        FlowGeneratorKind::Roce(roce) => {
            Some((roce.pacer.mtu_bytes, roce.pacer.pacing_interval_ns))
        }
        _ => None,
    }
}

const fn transport_label(generator: &crate::FlowGeneratorState) -> &'static str {
    match generator.kind {
        FlowGeneratorKind::Roce(_) => "RoCE",
        _ => "TCP",
    }
}

/// The host's TCP receiver for `id`.
///
/// `validate_generators` checks a host's `tcp_receivers` strictly ascending by flow before it
/// validates any stage on that host, so the binary search finds the one receiver the linear
/// `find(|receiver| receiver.flow == id)` found.
fn host_tcp_receiver(
    state: &crate::HostState,
    id: crate::FlowId,
) -> Option<&crate::TcpReceiverState> {
    state
        .tcp_receivers
        .binary_search_by_key(&id, |receiver| receiver.flow)
        .ok()
        .map(|index| &state.tcp_receivers[index])
}

/// Invariants of one compute (delay-only) stage.
///
/// The generator is a zero-byte constant timer whose interval is the compute duration. Its local
/// predecessor is the same-rank stage of a compute group, or the same-rank final stage of a
/// collective together with the previous rank's final stage as the inbound predecessor. A released
/// stage is timed (`Scheduled` with its token and one `PacingTimer`), beyond the stop (`Stopped`),
/// or complete (`Finished`).
#[allow(clippy::too_many_arguments)]
fn validate_compute_stage(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    state: &crate::HostState,
    flow: &crate::FlowDescriptor,
    generator: StagedGenerator<'_>,
    compute: crate::ComputeStage,
    stage: crate::CollectiveStage,
    positions: &mut BTreeMap<(u64, u32), crate::FlowId>,
) -> Result<(), ValidationError> {
    let dependencies = stage.dependencies;
    let FlowGeneratorKind::Constant(constant) = generator.kind else {
        return Err(ValidationError::new(format!(
            "flow {:?} compute stage requires a zero-byte constant timer generator",
            flow.id
        )));
    };
    if constant.packet_size_bytes != 0
        || constant.termination != GeneratorTermination::Bytes(0)
        || constant.first_departure_ns != 0
        || constant.interval_ns != compute.duration_ns
        || compute.duration_ns == 0
        || generator.packets_emitted != 0
        || generator.bytes_emitted != 0
    {
        return Err(ValidationError::new(format!(
            "flow {:?} compute stage requires a zero-byte constant timer generator",
            flow.id
        )));
    }
    if compute.rank >= compute.group_size {
        return Err(ValidationError::new(format!(
            "flow {:?} compute stage rank {} is outside group size {}",
            flow.id, compute.rank, compute.group_size
        )));
    }
    if let Some(previous) = positions.insert((compute.compute_id, compute.rank), flow.id) {
        return Err(ValidationError::new(format!(
            "flow {:?} has duplicate compute stage position ({}, {}), already owned by flow {previous:?}",
            flow.id, compute.compute_id, compute.rank
        )));
    }

    let local = dependencies
        .local_predecessor
        .map(|id| {
            flow_index
                .generator_for_flow(image, id)
                .filter(|candidate| flow_source(image, candidate.flow) == Some(flow.source))
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} compute local predecessor {id:?} is not a stage on its host",
                        flow.id
                    ))
                })
        })
        .transpose()?;
    let local_role = local
        .and_then(|candidate| candidate.stage)
        .map(|stage| stage.role);
    match (local_role, dependencies.inbound_predecessor) {
        (None, None) => {}
        (Some(crate::StageRole::Compute(previous)), None)
            if previous.rank == compute.rank && previous.group_size == compute.group_size => {}
        (Some(crate::StageRole::Collective(final_stage)), Some(inbound_id))
            if final_stage.rank == compute.rank
                && final_stage.group_size == compute.group_size
                && final_stage.phase == crate::CollectivePhase::AllGather
                && final_stage.step + 1 == final_stage.group_size =>
        {
            let previous_rank = (compute.rank + compute.group_size - 1) % compute.group_size;
            let inbound = flow_index
                .generator_for_flow(image, inbound_id)
                .and_then(|candidate| collective_identity(candidate).map(|stage| (candidate, stage)))
                .filter(|(candidate, stage)| {
                    stage.collective_id == final_stage.collective_id
                        && stage.phase == final_stage.phase
                        && stage.step == final_stage.step
                        && stage.rank == previous_rank
                        && self::flow(image, candidate.flow)
                            .is_some_and(|flow_descriptor| flow_descriptor.target == flow.source)
                })
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} compute inbound predecessor is not the previous rank's final stage",
                        flow.id
                    ))
                })?;
            if inbound.1.chunk_bytes != dependencies.inbound_predecessor_bytes {
                return Err(ValidationError::new(format!(
                    "flow {:?} compute inbound predecessor does not deliver the declared chunk",
                    flow.id
                )));
            }
            // Schema Amendment 5: the stage's progress rows name its inbound transport from its
            // local predecessor, the same rank's final stage of the same collective.
            if local.map(|candidate| roce_transport(&candidate.kind))
                != Some(roce_transport(&inbound.0.kind))
            {
                return Err(ValidationError::new(format!(
                    "flow {:?} compute local and inbound predecessors disagree on the RoCE MTU or pacing interval",
                    flow.id
                )));
            }
            let frontier = host_inbound_frontier(state, &inbound.0);
            if frontier != Some(dependencies.inbound_bytes_received) {
                return Err(ValidationError::new(format!(
                    "flow {:?} compute inbound bytes {} disagree with the in-order {} frontier {:?} of flow {inbound_id:?}",
                    flow.id,
                    dependencies.inbound_bytes_received,
                    transport_label(&inbound.0),
                    frontier
                )));
            }
        }
        _ => {
            return Err(ValidationError::new(format!(
                "flow {:?} compute predecessors are neither a same-rank compute stage nor a collective's final stages",
                flow.id
            )));
        }
    }
    if dependencies.local_predecessor_complete
        != local.is_none_or(|candidate| candidate.next_emission.status == GeneratorStatus::Finished)
    {
        return Err(ValidationError::new(format!(
            "flow {:?} compute local completion flag disagrees with predecessor state",
            flow.id
        )));
    }
    if dependencies.inbound_bytes_received > dependencies.inbound_predecessor_bytes
        || dependencies.inbound_predecessor_complete
            != (dependencies.inbound_predecessor.is_none()
                || dependencies.inbound_bytes_received == dependencies.inbound_predecessor_bytes)
    {
        return Err(ValidationError::new(format!(
            "flow {:?} compute inbound completion flag disagrees with received bytes",
            flow.id
        )));
    }
    if stage.activated != dependencies.prerequisites_complete() {
        return Err(ValidationError::new(format!(
            "flow {:?} compute release flag disagrees with its prerequisites",
            flow.id
        )));
    }
    let emission = generator.next_emission;
    let consistent = match emission.status {
        GeneratorStatus::Blocked => {
            !stage.activated && emission.departure_time_ns == 0 && emission.payload == PayloadId(0)
        }
        GeneratorStatus::Stopped => {
            stage.activated && emission.departure_time_ns > image.stop_time_ns
        }
        GeneratorStatus::Finished => stage.activated,
        GeneratorStatus::Scheduled => {
            stage.activated
                && emission.departure_time_ns <= image.stop_time_ns
                && packet(image, emission.payload).is_some_and(|token| {
                    token.flow == flow.id
                        && token.size_bytes == 0
                        && token.kind == PacketKind::Data
                        && !token.ecn_marked
                })
                && image
                    .initial_events
                    .iter()
                    .filter(|event| {
                        event.kind == crate::EventKind::PacingTimer
                            && event.target == flow.source
                            && event.payload == emission.payload
                            && event.key.time_ns == emission.departure_time_ns
                    })
                    .count()
                    == 1
        }
    };
    if !consistent {
        return Err(ValidationError::new(format!(
            "flow {:?} compute timer state {:?} is inconsistent with its release",
            flow.id, emission.status
        )));
    }
    Ok(())
}

/// A TCP stage that its prerequisites have not released yet holds the pristine sender state that
/// its first activation starts from: nothing sent, acknowledged, timed, or reserved.
fn validate_unreleased_tcp_stage(
    flow: crate::FlowId,
    generator: StagedGenerator<'_>,
    tcp: crate::TcpGenerator,
) -> Result<(), ValidationError> {
    if generator.packets_emitted != 0
        || generator.bytes_emitted != 0
        || tcp.next_sequence != 0
        || tcp.highest_ack != 0
        || tcp.bytes_in_flight != 0
        || tcp.duplicate_acks != 0
        || tcp.recovery_high_sequence != 0
        || tcp.last_attempt != crate::PayloadId(0)
        || tcp.timer_generation != 0
        || tcp.active_timer.is_some()
        || generator.next_emission.departure_time_ns != 0
        || generator.next_emission.payload != crate::PayloadId(0)
    {
        return Err(ValidationError::new(format!(
            "flow {flow:?} TCP collective stage is dependency-blocked after sending state changed"
        )));
    }
    Ok(())
}

fn validate_blocked_tcp_timer(
    image: &SimulationImage,
    flow_index: &FlowIndex,
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
    let matching_events =
        flow_index.retransmission_timeout_events(owner, timer.attempt, timer.deadline_ns);
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
    flow_index: &FlowIndex,
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
    // A CE packet already admitted into the image can still create its CNP after the source has
    // finished producing data.  Its reverse path is therefore live independently of generator
    // status, just as an in-flight TCP data packet keeps its ACK path live.
    let live_dcqcn_cnp_flows = executable_resident_packets(image)
        .into_iter()
        .filter(|packet| dcqcn_data_can_still_emit_cnp(image, packet))
        .map(|packet| packet.flow)
        .collect::<BTreeSet<_>>();
    let mut possible = BTreeMap::<(LinkId, NodeId), u64>::new();
    let mut required = BTreeSet::<(LinkId, NodeId)>::new();
    for packet in &image.initial_packets {
        if flow_index.is_compute_flow(image, packet.flow) {
            // A compute timer token names its timer event; it is never enqueued or transmitted.
            if packet.size_bytes != 0 || packet.kind != PacketKind::Data || packet.ecn_marked {
                return Err(ValidationError::new(format!(
                    "compute timer token {:?} must be a zero-byte NotECT data token",
                    packet.id
                )));
            }
            continue;
        }
        if packet.size_bytes == 0 && !packet.kind.is_timer_token() {
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
        if packet.ecn_marked && !packet.kind.is_data() {
            return Err(ValidationError::new(format!(
                "packet {:?} has an ECN mark on non-data kind {:?}; control and feedback are NotECT",
                packet.id, packet.kind
            )));
        }
        if packet.kind.is_timer_token() {
            let source =
                node(image, flow.source).expect("flow validation established the source node");
            let owns_token = image.host_states[source.state_slot as usize]
                .generators
                .iter()
                .any(|generator| {
                    generator.flow == flow.id
                        && match (packet.kind, generator.kind) {
                            (PacketKind::DcqcnControlTimer, FlowGeneratorKind::Dcqcn(dcqcn)) => {
                                dcqcn.control_timer_payload == packet.id
                            }
                            (PacketKind::DcqcnControlTimer, FlowGeneratorKind::Roce(roce)) => {
                                roce.control_timer_payload == packet.id
                            }
                            (PacketKind::RocePacingTimer, FlowGeneratorKind::Roce(roce)) => {
                                roce.pacing_timer_payload == packet.id
                            }
                            _ => false,
                        }
                });
            if packet.size_bytes != 0 || !owns_token {
                return Err(ValidationError::new(format!(
                    "DCQCN control token {:?} must be zero-byte NotECT state owned by its DCQCN generator",
                    packet.id
                )));
            }
            continue;
        }
        if matches!(packet.kind, PacketKind::DcqcnCnp(_)) {
            let source =
                node(image, flow.source).expect("flow validation established the source node");
            let is_dcqcn = image.host_states[source.state_slot as usize]
                .generators
                .iter()
                .any(|generator| {
                    generator.flow == flow.id
                        && matches!(
                            generator.kind,
                            FlowGeneratorKind::Dcqcn(_) | FlowGeneratorKind::Roce(_)
                        )
                });
            if packet.size_bytes != 64 || !is_dcqcn {
                return Err(ValidationError::new(format!(
                    "DCQCN CNP packet {:?} must be an exact 64-byte NotECT frame bound to a DCQCN generator",
                    packet.id
                )));
            }
        }
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
        for generator in staged_generators(state) {
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
                    let remaining = rate.total_bytes.saturating_sub(generator.bytes_emitted);
                    let tail = remaining % rate.packet_size_bytes;
                    let minimum = if remaining == 0 || tail == 0 {
                        rate.packet_size_bytes
                    } else {
                        tail
                    };
                    (minimum, flow.route.as_slice(), flow.target)
                }
                FlowGeneratorKind::Dcqcn(dcqcn) => {
                    let cnp_can_be_emitted =
                        generator_can_emit || live_dcqcn_cnp_flows.contains(&flow.id);
                    for (index, link_id) in flow.reverse_route.iter().enumerate() {
                        let link = link(image, *link_id)
                            .expect("validated reverse route names an existing link");
                        let delay = link.delay_ns(dcqcn.cnp_size_bytes).map_err(|error| {
                            ValidationError::new(format!(
                                "link {:?} delay overflows for flow {:?} DCQCN CNP: {error}",
                                link.id, flow.id
                            ))
                        })?;
                        let route = (
                            link.id,
                            route_target(image, &flow.reverse_route, index, flow.source)
                                .expect("validated reverse route has a direct target"),
                        );
                        insert_derived_delay(&mut possible, route, delay);
                        if cnp_can_be_emitted {
                            required.insert(route);
                        }
                    }
                    (
                        {
                            let remaining = dcqcn
                                .rate
                                .total_bytes
                                .saturating_sub(generator.bytes_emitted);
                            let tail = remaining % dcqcn.rate.packet_size_bytes;
                            if remaining == 0 || tail == 0 {
                                dcqcn.rate.packet_size_bytes
                            } else {
                                tail
                            }
                        },
                        flow.route.as_slice(),
                        flow.target,
                    )
                }
                FlowGeneratorKind::Roce(roce) => {
                    let feedback_can_be_emitted =
                        generator_can_emit || roce.snd_una < generator.bytes_emitted;
                    let feedback_sizes = roce_receiver(image, flow).map_or([0, 0], |receiver| {
                        [receiver.ack_size_bytes, receiver.np.cnp_size_bytes]
                    });
                    for (index, link_id) in flow.reverse_route.iter().enumerate() {
                        let link = link(image, *link_id)
                            .expect("validated reverse route names an existing link");
                        let route = (
                            link.id,
                            route_target(image, &flow.reverse_route, index, flow.source)
                                .expect("validated reverse route has a direct target"),
                        );
                        for size in feedback_sizes {
                            let delay = link.delay_ns(size).map_err(|error| {
                                ValidationError::new(format!(
                                    "link {:?} delay overflows for flow {:?} RoCE feedback: {error}",
                                    link.id, flow.id
                                ))
                            })?;
                            insert_derived_delay(&mut possible, route, delay);
                        }
                        if feedback_can_be_emitted {
                            required.insert(route);
                        }
                    }
                    (
                        roce_future_data_min_bytes(roce),
                        flow.route.as_slice(),
                        flow.target,
                    )
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
    flow_index: &FlowIndex,
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
        // With host-link PFC, a packet whose class is paused at the host's egress waits for its
        // RESUME, which schedules the service.
        // Without host-link PFC this is today's emptiness test: no per-packet work.
        let has_eligible_packet = match state.pfc.as_deref() {
            None => !state.queue.is_empty(),
            Some(pfc) => state.queue.iter().any(|payload| {
                packet(image, *payload)
                    .and_then(|packet| {
                        flow(image, packet.flow).map(|flow| flow.packet_priority(packet.kind))
                    })
                    .is_none_or(|priority| !pfc.is_paused(usize::from(priority)))
            }),
        };
        if has_eligible_packet && state.in_service.is_none() && !state.tx_ready_pending {
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
            validate_scheduler_state(
                image,
                flow_index,
                owner.id,
                queue_index,
                queue,
                pending_event_frontier,
            )?;
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
                        .packet_priority(packet.kind),
                );
                !queue
                    .pfc
                    .as_ref()
                    .is_some_and(|pfc| pfc.is_paused(priority))
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

fn route_outgoing_index(
    image: &SimulationImage,
    route: &[LinkId],
    source: NodeId,
) -> Option<usize> {
    route.iter().position(|link_id| {
        link(image, *link_id).is_some_and(|route_link| route_link.source == source)
    })
}

fn packet_can_still_reach_egress(
    image: &SimulationImage,
    packet: &crate::PacketDescriptor,
    egress: LinkId,
) -> bool {
    if matches!(packet.kind, PacketKind::Pfc(_)) {
        return false;
    }
    let Some(flow) = flow(image, packet.flow) else {
        return false;
    };
    let route = packet_route(flow, packet.kind);
    let Some(egress_index) = route.iter().position(|link_id| *link_id == egress) else {
        return false;
    };

    for node in &image.nodes {
        let (waiting, in_service) = match node.kind {
            NodeKind::Host => {
                let state = &image.host_states[node.state_slot as usize];
                (
                    state.queue.contains(&packet.id),
                    state.in_service == Some(packet.id),
                )
            }
            NodeKind::Switch => {
                let state = &image.switch_states[node.state_slot as usize];
                (
                    state
                        .queues
                        .iter()
                        .any(|queue| queue.queue.contains(&packet.id)),
                    state
                        .queues
                        .iter()
                        .any(|queue| queue.in_service == Some(packet.id)),
                )
            }
        };
        let Some(outgoing_index) = route_outgoing_index(image, route, node.id) else {
            continue;
        };
        if waiting && egress_index >= outgoing_index || in_service && egress_index > outgoing_index
        {
            return true;
        }
    }

    image.initial_events.iter().any(|event| {
        if event.payload != packet.id {
            return false;
        }
        let Some(outgoing_index) = route_outgoing_index(image, route, event.target) else {
            return false;
        };
        match event.kind {
            EventKind::PacketArrival | EventKind::TxReady | EventKind::RemoteArrival => {
                egress_index >= outgoing_index
            }
            EventKind::TxComplete => egress_index > outgoing_index,
            EventKind::PacingTimer | EventKind::RetransmissionTimeout => false,
        }
    })
}

fn executable_resident_packets(image: &SimulationImage) -> Vec<&crate::PacketDescriptor> {
    let scheduled_payloads = image
        .host_states
        .iter()
        .flat_map(staged_generators)
        .filter(|generator| {
            generator.next_emission.status == GeneratorStatus::Scheduled
                || matches!(
                    generator.kind,
                    FlowGeneratorKind::Rate(_) | FlowGeneratorKind::Dcqcn(_)
                ) && generator.next_emission.status == GeneratorStatus::Blocked
        })
        .map(|generator| generator.next_emission.payload)
        .collect::<BTreeSet<_>>();
    let live_payloads = crate::tcp_ledger::initial_live_payloads(image);
    image
        .initial_packets
        .iter()
        .filter(|packet| !scheduled_payloads.contains(&packet.id))
        .filter(|packet| {
            !matches!(packet.kind, PacketKind::TcpData(_)) || live_payloads.contains(&packet.id)
        })
        .filter(|packet| !matches!(packet.kind, PacketKind::Pfc(_)))
        .filter(|packet| !packet.kind.is_timer_token())
        .collect()
}

fn dcqcn_data_can_still_emit_cnp(
    image: &SimulationImage,
    packet: &crate::PacketDescriptor,
) -> bool {
    if packet.kind != PacketKind::Data {
        return false;
    }
    let Some(flow) = flow(image, packet.flow) else {
        return false;
    };
    let Some(source) = node(image, flow.source) else {
        return false;
    };
    if !image.host_states[source.state_slot as usize]
        .generators
        .iter()
        .any(|generator| {
            generator.flow == flow.id && matches!(generator.kind, FlowGeneratorKind::Dcqcn(_))
        })
    {
        return false;
    }
    if packet.ecn_marked {
        return true;
    }
    flow.route.iter().copied().any(|candidate| {
        let Some(route_link) = link(image, candidate) else {
            return false;
        };
        let Some(owner) = node(image, route_link.source) else {
            return false;
        };
        owner.kind == NodeKind::Switch
            && image.switch_states[owner.state_slot as usize]
                .queues
                .iter()
                .any(|queue| {
                    queue.egress_link == Some(candidate)
                        && queue.drop_mark != crate::DropMarkPolicy::TailDrop
                })
            && packet_can_still_reach_egress(image, packet, candidate)
    })
}

/// Returns whether a resident packet can still cross `candidate` from its checkpoint position.
/// Unknown positions remain reachable so API images retain a conservative bound.
fn packet_can_still_cross_link(
    image: &SimulationImage,
    packet: &crate::PacketDescriptor,
    candidate: LinkId,
) -> bool {
    let Some(flow) = flow(image, packet.flow) else {
        return false;
    };
    let route = packet_route(flow, packet.kind);
    let Some(candidate_index) = route.iter().position(|link_id| *link_id == candidate) else {
        return false;
    };
    let mut has_position = false;
    let mut reachable = false;

    for node in &image.nodes {
        let owns_packet = match node.kind {
            NodeKind::Host => {
                let state = &image.host_states[node.state_slot as usize];
                state.queue.contains(&packet.id) || state.in_service == Some(packet.id)
            }
            NodeKind::Switch => image.switch_states[node.state_slot as usize]
                .queues
                .iter()
                .any(|queue| {
                    queue.queue.contains(&packet.id) || queue.in_service == Some(packet.id)
                }),
        };
        if !owns_packet {
            continue;
        }
        if let Some(outgoing_index) = route_outgoing_index(image, route, node.id) {
            has_position = true;
            reachable |= candidate_index >= outgoing_index;
        }
    }

    for event in image
        .initial_events
        .iter()
        .filter(|event| event.payload == packet.id)
    {
        let position = match event.kind {
            EventKind::PacketArrival | EventKind::TxReady => {
                route_outgoing_index(image, route, event.target)
            }
            EventKind::TxComplete => event_egress(image, event)
                .and_then(|egress| route.iter().position(|link_id| *link_id == egress)),
            EventKind::RemoteArrival => remote_arrival_link(image, *event)
                .and_then(|incoming| route.iter().position(|link_id| *link_id == incoming)),
            EventKind::PacingTimer | EventKind::RetransmissionTimeout => None,
        };
        if let Some(position) = position {
            has_position = true;
            reachable |= candidate_index >= position;
        }
    }

    !has_position || reachable
}

fn tcp_future_data_max_frame(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    generator: StagedGenerator<'_>,
    tcp: crate::TcpGenerator,
) -> u64 {
    if matches!(
        generator.next_emission.status,
        GeneratorStatus::Finished | GeneratorStatus::Stopped
    ) {
        return 0;
    }
    let fresh = tcp.mss_bytes.min(tcp.total_bytes - tcp.next_sequence);
    let retransmission = flow_index
        .packets_for_flow(image, generator.flow)
        .filter_map(|packet| {
            let PacketKind::TcpData(header) = packet.kind else {
                return None;
            };
            let end = header.sequence.checked_add(packet.size_bytes)?;
            (header.sequence < tcp.next_sequence && end > tcp.highest_ack)
                .then_some(end - header.sequence.max(tcp.highest_ack))
        })
        .max()
        .unwrap_or(0);
    fresh.max(retransmission)
}

fn maximum_drr_frame_bytes(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    egress: LinkId,
    class_count: usize,
    class: usize,
) -> Result<u64, ValidationError> {
    let class_count_u64 = u64::try_from(class_count).map_err(|_| {
        ValidationError::new("DRR class count exceeds the u64 scheduler-class domain")
    })?;
    let mut maximum = image
        .initial_packets
        .iter()
        .filter(|packet| scheduler_class(image, packet.id, class_count) == Some(class))
        .filter(|packet| packet_can_still_reach_egress(image, packet, egress))
        .map(|packet| packet.size_bytes)
        .max()
        .unwrap_or(0);

    for generator in image.host_states.iter().flat_map(staged_generators) {
        let Some(generator_class) = usize::try_from(generator.flow.0 % class_count_u64).ok() else {
            continue;
        };
        if generator_class != class {
            continue;
        }
        let flow = flow(image, generator.flow)
            .expect("generator validation precedes scheduler-state validation");
        let forward_reaches = flow.route.contains(&egress);
        let reverse_reaches = flow.reverse_route.contains(&egress);
        match generator.kind {
            FlowGeneratorKind::Constant(constant) => {
                if forward_reaches && executable_generator_packets(image, generator)? != 0 {
                    maximum = maximum.max(constant.packet_size_bytes);
                }
            }
            FlowGeneratorKind::Rate(rate) => {
                if forward_reaches && executable_generator_packets(image, generator)? != 0 {
                    maximum = maximum.max(
                        rate.packet_size_bytes
                            .min(rate.total_bytes - generator.bytes_emitted),
                    );
                }
            }
            FlowGeneratorKind::Tcp(tcp) => {
                let future_data = tcp_future_data_max_frame(image, flow_index, generator, tcp);
                if forward_reaches {
                    maximum = maximum.max(future_data);
                }
                if reverse_reaches && (future_data != 0 || tcp.bytes_in_flight != 0) {
                    maximum = maximum.max(tcp.ack_size_bytes);
                }
            }
            FlowGeneratorKind::Dcqcn(dcqcn) => {
                if forward_reaches && executable_generator_packets(image, generator)? != 0 {
                    maximum = maximum.max(
                        dcqcn
                            .rate
                            .packet_size_bytes
                            .min(dcqcn.rate.total_bytes - generator.bytes_emitted),
                    );
                }
                if reverse_reaches && executable_generator_packets(image, generator)? != 0 {
                    maximum = maximum.max(dcqcn.cnp_size_bytes);
                }
            }
            FlowGeneratorKind::Roce(roce) => {
                let executable = executable_generator_packets(image, generator)? != 0;
                if forward_reaches && executable {
                    maximum = maximum.max(roce_future_data_max_bytes(roce));
                }
                if reverse_reaches && (executable || roce.snd_una < generator.bytes_emitted) {
                    maximum = maximum.max(roce_feedback_max_bytes(image, flow));
                }
            }
        }
    }
    Ok(maximum)
}

fn validate_scheduler_state(
    image: &SimulationImage,
    flow_index: &FlowIndex,
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
                    .egress_link
                    .map(|egress| {
                        maximum_drr_frame_bytes(
                            image,
                            flow_index,
                            egress,
                            state.quanta_bytes.len(),
                            class,
                        )
                    })
                    .transpose()?
                    .unwrap_or(0);
                if maximum_frame == 0 {
                    continue;
                }
                if quantum.checked_add(maximum_frame - 1).is_none() {
                    return Err(ValidationError::new(format!(
                        "switch node {owner:?} queue {queue_index} DRR class {class} cannot accumulate enough deficit for a {maximum_frame}-byte packet without overflowing"
                    )));
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
                    let matching_lane = pfc_channels.iter().copied().find(|channel_index| {
                        let channel = &image.channels[*channel_index];
                        channel.source == origin.id
                            && channel.target == event.target
                            && pfc_channel_controls(image, *channel_index)
                                == Some(header.controlled_link)
                    });
                    let Some(channel_index) = matching_lane else {
                        return Err(ValidationError::new(format!(
                            "PFC RemoteArrival event {index} from {:?} to {:?} has no declared control lane for link {:?}",
                            origin.id, event.target, header.controlled_link
                        )));
                    };
                    let enabled = pfc_channel_ingress(image, channel_index)
                        .and_then(|ingress| {
                            ingress
                                .xoff_threshold_bytes
                                .get(usize::from(header.priority))
                        })
                        .is_some_and(|xoff| *xoff != 0);
                    if !enabled {
                        return Err(ValidationError::new(format!(
                            "PFC RemoteArrival event {index} uses disabled priority {} on control lane {channel_index}",
                            header.priority
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
                if !packet.kind.is_data() && !packet.kind.is_timer_token() {
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

fn divide_rounding_up(numerator: &BigUint, denominator: &BigUint) -> BigUint {
    if numerator == &BigUint::from(0_u8) {
        return BigUint::from(0_u8);
    }
    (numerator + denominator - 1_u8) / denominator
}

fn pacing_rate(generator: StagedGenerator<'_>) -> crate::RateGenerator {
    match generator.kind {
        FlowGeneratorKind::Rate(rate) => rate,
        FlowGeneratorKind::Dcqcn(dcqcn) => dcqcn.rate,
        _ => unreachable!("only rate-paced generators have pacing state"),
    }
}

fn with_rate_numerator(
    generator: StagedGenerator<'_>,
    numerator: u64,
) -> crate::FlowGeneratorState {
    let mut adjusted = *generator;
    match &mut adjusted.kind {
        FlowGeneratorKind::Rate(rate) => rate.rate_numerator_bits_per_second = numerator,
        FlowGeneratorKind::Dcqcn(dcqcn) => {
            dcqcn.rate.rate_numerator_bits_per_second = numerator;
        }
        _ => unreachable!("only rate-paced generators have pacing state"),
    }
    adjusted
}

fn remaining_rate_pacing_ticks(generator: StagedGenerator<'_>) -> Result<BigUint, ValidationError> {
    let rate = pacing_rate(generator);
    let remaining_bytes = BigUint::from(rate.total_bytes - generator.bytes_emitted);
    let packet_size = BigUint::from(rate.packet_size_bytes);
    let full_packets = &remaining_bytes / &packet_size;
    let partial_bytes = &remaining_bytes % &packet_size;
    let scale = BigUint::from(rate.rate_denominator) * BigUint::from(1_000_000_000_u64);
    let tick_credit =
        BigUint::from(rate.rate_numerator_bits_per_second) * BigUint::from(rate.pacing_interval_ns);
    let full_packet_cost = &packet_size * BigUint::from(8_u8) * &scale;
    let mut credit = BigUint::from(rate.credit_quanta);
    let mut ticks = BigUint::from(0_u8);

    if full_packets != BigUint::from(0_u8) {
        let required_credit = &full_packets * &full_packet_cost;
        let credit_ticks = if required_credit > credit {
            divide_rounding_up(&(&required_credit - &credit), &tick_credit)
        } else {
            BigUint::from(0_u8)
        };
        let full_ticks = full_packets.clone().max(credit_ticks);
        credit += &full_ticks * &tick_credit;
        credit -= required_credit;
        ticks += full_ticks;
    }

    if partial_bytes != BigUint::from(0_u8) {
        let partial_cost = partial_bytes * BigUint::from(8_u8) * scale;
        let credit_ticks = if partial_cost > credit {
            divide_rounding_up(&(&partial_cost - &credit), &tick_credit)
        } else {
            BigUint::from(0_u8)
        };
        ticks += credit_ticks.max(BigUint::from(1_u8));
    }
    Ok(ticks)
}

fn executable_rate_pacing_ticks(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<BigUint, ValidationError> {
    if !matches!(
        generator.next_emission.status,
        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
    ) || generator.next_emission.departure_time_ns > image.stop_time_ns
    {
        return Ok(BigUint::from(0_u8));
    }
    remaining_generator_packets(generator)?;
    let rate = pacing_rate(generator);
    let available_ticks = BigUint::from(
        (image.stop_time_ns - generator.next_emission.departure_time_ns) / rate.pacing_interval_ns,
    ) + 1_u8;
    Ok(remaining_rate_pacing_ticks(generator)?.min(available_ticks))
}

fn rate_packets_within_ticks(
    generator: StagedGenerator<'_>,
    ticks: &BigUint,
) -> Result<u64, ValidationError> {
    let remaining_packets = remaining_generator_packets(generator)?;
    if ticks == &BigUint::from(0_u8) || remaining_packets == 0 {
        return Ok(0);
    }
    let rate = pacing_rate(generator);
    let remaining_bytes = rate.total_bytes - generator.bytes_emitted;
    let full_packets = BigUint::from(remaining_bytes / rate.packet_size_bytes);
    let partial_packet = !remaining_bytes.is_multiple_of(rate.packet_size_bytes);
    let scale = BigUint::from(rate.rate_denominator) * BigUint::from(1_000_000_000_u64);
    let tick_credit =
        BigUint::from(rate.rate_numerator_bits_per_second) * BigUint::from(rate.pacing_interval_ns);
    let full_packet_cost = BigUint::from(rate.packet_size_bytes) * BigUint::from(8_u8) * scale;
    let total_credit = BigUint::from(rate.credit_quanta) + ticks * tick_credit;
    let credit_limited = total_credit / full_packet_cost;
    let emitted_full = full_packets.clone().min(ticks.clone()).min(credit_limited);
    let emitted_all_full = emitted_full == full_packets;
    let mut emitted = u64::try_from(emitted_full).map_err(|_| {
        ValidationError::new(format!(
            "flow {:?} executable rate packet count exceeds u64",
            generator.flow
        ))
    })?;
    if partial_packet && emitted_all_full && remaining_rate_pacing_ticks(generator)? <= *ticks {
        emitted = emitted.checked_add(1).ok_or_else(|| {
            ValidationError::new(format!(
                "flow {:?} executable rate packet count exceeds u64",
                generator.flow
            ))
        })?;
    }
    Ok(emitted)
}

fn executable_rate_packets(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    rate_packets_within_ticks(generator, &executable_rate_pacing_ticks(image, generator)?)
}

fn rate_payload_allocations(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    let mut successor_ticks = executable_rate_pacing_ticks(image, generator)?;
    if successor_ticks == BigUint::from(0_u8) {
        return Ok(0);
    }
    successor_ticks -= 1_u8;
    rate_packets_within_ticks(generator, &successor_ticks)
}

fn executable_dcqcn_packets(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!("DCQCN packet capacity requires DCQCN state")
    };
    let fastest = with_rate_numerator(generator, dcqcn.controller.config.maximum_rate_bps);
    executable_rate_packets(image, generator.with_generator(&fastest))
}

fn executable_dcqcn_pacing_ticks(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<BigUint, ValidationError> {
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!("DCQCN pacing capacity requires DCQCN state")
    };
    let slowest = with_rate_numerator(generator, dcqcn.controller.config.minimum_rate_bps);
    executable_rate_pacing_ticks(image, generator.with_generator(&slowest))
}

fn dcqcn_payload_allocations(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!("DCQCN payload capacity requires DCQCN state")
    };
    let fastest = with_rate_numerator(generator, dcqcn.controller.config.maximum_rate_bps);
    rate_payload_allocations(image, generator.with_generator(&fastest))
}

fn executable_dcqcn_control_ticks(
    image: &SimulationImage,
    dcqcn: crate::DcqcnGenerator,
) -> BigUint {
    executable_control_ticks(image, dcqcn.controller)
}

/// Control ticks a DCQCN controller can still run until the stop time, its pending one included.
fn executable_control_ticks(
    image: &SimulationImage,
    controller: crate::DcqcnController,
) -> BigUint {
    if controller.next_control_time_ns > image.stop_time_ns {
        return BigUint::from(0_u8);
    }
    BigUint::from(
        (image.stop_time_ns - controller.next_control_time_ns)
            / controller.config.control_interval_ns,
    ) + 1_u8
}

/// Pacing ticks a RoCE queue pair can still run until the stop time, its pending one included.
///
/// Every tick lies on the grid anchored at `first_pacing_time_ns` and emits at most one data
/// packet, so this also bounds the pair's future data packets and data payload allocations. An
/// armed pacer ticks from its pending departure. A parked pacer of a pair that has not finished
/// can restart on any later grid point, so every grid point up to the stop time bounds it. A
/// finished or stopped pair with a parked pacer ticks no more.
pub(crate) fn roce_grid_ticks(
    image: &SimulationImage,
    generator: &crate::FlowGeneratorState,
    roce: crate::RoceGenerator,
) -> u64 {
    let start = if roce.pacer_armed {
        generator.next_emission.departure_time_ns
    } else if generator.next_emission.status == GeneratorStatus::Blocked {
        roce.pacer.first_pacing_time_ns
    } else {
        return 0;
    };
    if start > image.stop_time_ns || roce.pacer.pacing_interval_ns == 0 {
        return 0;
    }
    (image.stop_time_ns - start) / roce.pacer.pacing_interval_ns + 1
}

/// Retransmission-timeout installations a RoCE queue pair can still make until the stop time.
///
/// A timeout is installed when a send makes data outstanding (at most one per pacing tick), when
/// an ACK or NACK advances or rewinds the pair (at most one per data arrival at its receiver, so
/// at most `data_packets`), and when a timeout fires and re-arms (at most one per `rto_ns` until
/// the stop time). With the timeout off there are none.
fn roce_timer_installations(
    image: &SimulationImage,
    roce: crate::RoceGenerator,
    pacing_ticks: u64,
    data_packets: u64,
) -> Result<u64, ValidationError> {
    if roce.rto_ns == 0 {
        return Ok(0);
    }
    let timeouts = image.stop_time_ns / roce.rto_ns + 1;
    pacing_ticks
        .checked_add(data_packets)
        .and_then(|total| total.checked_add(timeouts))
        .ok_or_else(|| ValidationError::new("RoCE timer installation bound exceeds u64"))
}

/// The RoCE receiver of `flow`, held by the flow's target host in canonical `FlowId` order.
fn roce_receiver<'a>(
    image: &'a SimulationImage,
    flow: &FlowDescriptor,
) -> Option<&'a crate::RoceReceiverState> {
    let target = node(image, flow.target)?;
    if target.kind != NodeKind::Host {
        return None;
    }
    let receivers = image
        .host_states
        .get(target.state_slot as usize)?
        .roce_receivers
        .as_deref()?;
    receivers
        .binary_search_by_key(&flow.id, |receiver| receiver.np.flow)
        .ok()
        .map(|index| &receivers[index])
}

/// The largest feedback packet of a RoCE flow: an ACK or NACK, or a CNP.
fn roce_feedback_max_bytes(image: &SimulationImage, flow: &FlowDescriptor) -> u64 {
    roce_receiver(image, flow).map_or(0, |receiver| {
        receiver.ack_size_bytes.max(receiver.np.cnp_size_bytes)
    })
}

/// The largest data packet a RoCE queue pair can still send: a retransmission may resend any
/// packet from its cumulative acknowledgment on.
fn roce_future_data_max_bytes(roce: crate::RoceGenerator) -> u64 {
    roce.pacer
        .mtu_bytes
        .min(roce.pacer.total_bytes.saturating_sub(roce.snd_una))
}

/// The smallest data packet a RoCE queue pair can still send: a full MTU, or the short last one.
fn roce_future_data_min_bytes(roce: crate::RoceGenerator) -> u64 {
    let tail = roce.pacer.total_bytes % roce.pacer.mtu_bytes.max(1);
    if tail == 0 {
        roce.pacer.mtu_bytes
    } else {
        tail
    }
}

fn latest_dcqcn_control_time(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!("DCQCN control deadline requires DCQCN state")
    };
    let ticks = executable_dcqcn_control_ticks(image, dcqcn);
    if ticks == BigUint::from(0_u8) {
        return Ok(0);
    }
    let computed_successor = BigUint::from(dcqcn.controller.next_control_time_ns)
        + BigUint::from(dcqcn.controller.config.control_interval_ns) * &ticks;
    if computed_successor > BigUint::from(u64::MAX) {
        return Err(ValidationError::new(format!(
            "flow {:?} DCQCN control timer successor exceeds u64",
            generator.flow
        )));
    }
    let latest_event =
        computed_successor - BigUint::from(dcqcn.controller.config.control_interval_ns);
    u64::try_from(latest_event).map_err(|_| {
        ValidationError::new(format!(
            "flow {:?} latest DCQCN control time exceeds u64",
            generator.flow
        ))
    })
}

fn latest_dcqcn_pacing_time(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!("DCQCN pacing deadline requires DCQCN state")
    };
    let slowest = with_rate_numerator(generator, dcqcn.controller.config.minimum_rate_bps);
    latest_rate_pacing_time(image, generator.with_generator(&slowest))
}

fn latest_rate_pacing_time(
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    let rate = pacing_rate(generator);
    let first_time = generator.next_emission.departure_time_ns;
    if first_time > image.stop_time_ns {
        return Ok(0);
    }

    let ticks_to_finish = remaining_rate_pacing_ticks(generator)?;
    let available_ticks =
        BigUint::from((image.stop_time_ns - first_time) / rate.pacing_interval_ns) + 1_u8;
    let executed_ticks = ticks_to_finish.clone().min(available_ticks.clone());
    let computed_additions = if ticks_to_finish > available_ticks {
        available_ticks
    } else {
        ticks_to_finish - 1_u8
    };
    let maximum_computed_time =
        BigUint::from(first_time) + BigUint::from(rate.pacing_interval_ns) * computed_additions;
    if maximum_computed_time > BigUint::from(u64::MAX) {
        return Err(ValidationError::new(format!(
            "flow {:?} latest pacing time exceeds u64",
            generator.flow
        )));
    }

    let latest_event_time = BigUint::from(first_time)
        + BigUint::from(rate.pacing_interval_ns) * (executed_ticks - 1_u8);
    u64::try_from(latest_event_time).map_err(|_| {
        ValidationError::new(format!(
            "flow {:?} latest pacing time exceeds u64",
            generator.flow
        ))
    })
}

/// Bounds the reachable simulation time, and returns the future-work reservation it derives.
///
/// The reservation is a pure function of the image and is consumed again by `validate_counters`,
/// `validate_origin_sequences` and `validate_payload_sequences`; it is derived here, at its first
/// consumer, so that a rejection raised while deriving it keeps this validator's identity.
fn validate_global_time_capacity(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    backend: Backend,
) -> Result<FutureWork, ValidationError> {
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
        for (index, event) in image.initial_events.iter().enumerate() {
            if event.kind == EventKind::RetransmissionTimeout {
                continue;
            }
            let packet = packet(image, event.payload).ok_or_else(|| {
                ValidationError::new(format!(
                    "initial event {index} references unknown packet {:?}",
                    event.payload
                ))
            })?;
            let flow = flow(image, packet.flow).ok_or_else(|| {
                ValidationError::new(format!(
                    "packet {:?} references unknown flow {:?}",
                    packet.id, packet.flow
                ))
            })?;
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
        .flat_map(staged_generators)
        .filter(|generator| {
            generator.next_emission.status == GeneratorStatus::Scheduled
                || generator.next_emission.status == GeneratorStatus::Blocked
                    && matches!(
                        generator.kind,
                        FlowGeneratorKind::Rate(_) | FlowGeneratorKind::Dcqcn(_)
                    )
        })
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
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let flow = flow(image, generator.flow).expect("generator validation established the flow");
        let remaining = executable_generator_packets(image, generator)?;
        if remaining == 0 {
            continue;
        }
        let maximum_packet_size_bytes = match generator.kind {
            FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
            FlowGeneratorKind::Tcp(tcp) => {
                tcp_future_data_max_frame(image, flow_index, generator, tcp)
            }
            FlowGeneratorKind::Rate(rate) => rate
                .packet_size_bytes
                .min(rate.total_bytes - generator.bytes_emitted),
            FlowGeneratorKind::Dcqcn(dcqcn) => dcqcn
                .rate
                .packet_size_bytes
                .min(dcqcn.rate.total_bytes - generator.bytes_emitted),
            FlowGeneratorKind::Roce(roce) => roce_future_data_max_bytes(roce),
        };
        for link_id in &flow.route {
            let link = link(image, *link_id).expect("flow validation established the route link");
            let delay = link
                .delay_ns(maximum_packet_size_bytes)
                .map_err(|error| {
                    ValidationError::new(format!(
                        "link {:?} delay overflows for flow {:?} generator packet size {maximum_packet_size_bytes}: {error}",
                        link.id, flow.id
                    ))
                })?;
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
    let work = future_work(image, flow_index)?;
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let flow = flow(image, generator.flow).expect("generator validation established the flow");
        // DCQCN reserves its CNPs; a RoCE queue pair reserves its ACKs, NACKs and CNPs at the
        // largest of their sizes.
        let (feedback_size_bytes, cnp_count) = match generator.kind {
            FlowGeneratorKind::Dcqcn(dcqcn) => (
                dcqcn.cnp_size_bytes,
                work.dcqcn_cnp_by_flow[flow.id.0 as usize],
            ),
            FlowGeneratorKind::Roce(_) => (
                roce_feedback_max_bytes(image, flow),
                work.feedback_by_flow[flow.id.0 as usize],
            ),
            _ => continue,
        };
        for link_id in &flow.reverse_route {
            let link = link(image, *link_id).expect("flow validation established the route link");
            let delay = link
                .delay_ns(feedback_size_bytes)
                .expect("feedback/link delay validation already succeeded");
            let flow_delay = delay.checked_mul(cnp_count).ok_or_else(|| {
                ValidationError::new(format!(
                    "DCQCN CNP reverse-route time bound overflows for flow {:?} on link {:?}",
                    flow.id, link.id
                ))
            })?;
            service_bound = service_bound.checked_add(flow_delay).ok_or_else(|| {
                ValidationError::new(format!(
                    "DCQCN CNP reverse-route time bound overflows while adding flow {:?} on link {:?}",
                    flow.id, link.id
                ))
            })?;
        }
    }
    let reserves_pfc_time = work.pfc_by_channel.iter().any(|frames| *frames != 0);
    for (channel_index, frames) in work.pfc_by_channel.iter().copied().enumerate() {
        if frames == 0 {
            continue;
        }
        let reverse_delay = image.channels[channel_index].min_delay_ns;
        let pfc_delay = reverse_delay.checked_mul(frames).ok_or_else(|| {
            ValidationError::new(format!(
                "PFC reverse-lane time bound overflows for channel {channel_index} with {frames} frames"
            ))
        })?;
        service_bound = service_bound.checked_add(pfc_delay).ok_or_else(|| {
            ValidationError::new(format!(
                "PFC reverse-lane time bound overflows while adding channel {channel_index}"
            ))
        })?;
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
        .flat_map(staged_generators)
        .filter(|generator| {
            generator.next_emission.status == GeneratorStatus::Scheduled
                || generator.next_emission.status == GeneratorStatus::Blocked
                    && matches!(
                        generator.kind,
                        FlowGeneratorKind::Rate(_) | FlowGeneratorKind::Dcqcn(_)
                    )
                || matches!(
                    generator.kind,
                    FlowGeneratorKind::Dcqcn(dcqcn)
                        if dcqcn.controller.next_control_time_ns <= image.stop_time_ns
                )
                || matches!(generator.kind, FlowGeneratorKind::Roce(_))
        })
        .map(|generator| match generator.kind {
            FlowGeneratorKind::Constant(constant) => {
                let remaining = executable_generator_packets(image, generator)?;
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
            FlowGeneratorKind::Rate(_) => latest_rate_pacing_time(image, generator),
            FlowGeneratorKind::Dcqcn(_) => {
                let pacing = if matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                ) {
                    latest_dcqcn_pacing_time(image, generator)?
                } else {
                    0
                };
                Ok(pacing.max(latest_dcqcn_control_time(image, generator)?))
            }
            // Pacing and control ticks run at most to the stop time; a retransmission timeout
            // armed before it ends within `rto_ns`, which generator validation bounds.
            FlowGeneratorKind::Roce(_) => Ok(image.stop_time_ns),
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    let maximum_work_time = maximum_initial_time.max(maximum_generator_time);
    let bound_name = if reserves_pfc_time {
        "conservative service/PFC time bound"
    } else {
        "conservative service bound"
    };
    maximum_work_time.checked_add(service_bound).ok_or_else(|| {
        if maximum_generator_time <= maximum_initial_time {
            ValidationError::new(format!(
                "maximum initial event time {maximum_initial_time} plus {bound_name} {service_bound} overflows"
            ))
        } else {
            ValidationError::new(format!(
                "maximum generator departure time {maximum_generator_time} plus {bound_name} {service_bound} overflows"
            ))
        }
    })?;
    Ok(work)
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
    dcqcn_cnp_by_flow: Vec<u64>,
    pfc_by_node: Vec<u64>,
    pfc_by_channel: Vec<u64>,
}

fn validate_dcqcn_arithmetic_capacity(
    image: &SimulationImage,
    work: &FutureWork,
) -> Result<(), ValidationError> {
    let resident_cnp_flows = executable_resident_packets(image)
        .into_iter()
        .filter_map(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)).then_some(packet.flow))
        .collect::<BTreeSet<_>>();
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
            continue;
        };
        let index = generator.flow.0 as usize;
        let cnp_can_arrive =
            work.dcqcn_cnp_by_flow[index] != 0 || resident_cnp_flows.contains(&generator.flow);
        if cnp_can_arrive
            && dcqcn.controller.last_cnp_time_ns.is_some_and(|last| {
                last.checked_add(dcqcn.controller.config.cnp_interval_ns)
                    .is_none()
            })
        {
            return Err(ValidationError::new(format!(
                "flow {:?} DCQCN CNP interval deadline can exceed u64 while feedback remains executable",
                generator.flow
            )));
        }

        let executable_packets = executable_dcqcn_packets(image, generator)?;
        let remaining_bytes = dcqcn.rate.total_bytes - generator.bytes_emitted;
        let executable_bytes = u128::from(executable_packets)
            .checked_mul(u128::from(dcqcn.rate.packet_size_bytes))
            .map(|bytes| bytes.min(u128::from(remaining_bytes)))
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} DCQCN executable byte bound exceeds u128",
                    generator.flow
                ))
            })?;
        if u128::from(dcqcn.controller.bytes_since_increase) + executable_bytes
            > u128::from(u64::MAX)
        {
            return Err(ValidationError::new(format!(
                "flow {:?} DCQCN byte counter can exceed u64 across {executable_packets} executable packets",
                generator.flow
            )));
        }
    }
    Ok(())
}

fn route_enters_pfc_controller(
    image: &SimulationImage,
    route: &[LinkId],
    controlled_link: LinkId,
    controller: NodeId,
) -> bool {
    route.windows(2).any(|pair| {
        pair[0] == controlled_link
            && link(image, pair[1]).is_some_and(|next| next.source == controller)
    })
}

fn future_work(
    image: &SimulationImage,
    flow_index: &FlowIndex,
) -> Result<FutureWork, ValidationError> {
    let mut data_by_flow = vec![0_u64; image.flows.len()];
    let mut feedback_by_flow = vec![0_u64; image.flows.len()];
    let mut dcqcn_cnp_by_flow = vec![0_u64; image.flows.len()];
    let resident_packets = executable_resident_packets(image);
    for packet in &resident_packets {
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
    for generator in image.host_states.iter().flat_map(staged_generators) {
        match generator.kind {
            FlowGeneratorKind::Constant(_) => add_packet_count(
                &mut data_by_flow[generator.flow.0 as usize],
                executable_generator_packets(image, generator)?,
            )?,
            FlowGeneratorKind::Tcp(_) => {
                let preloaded_data = data_by_flow[generator.flow.0 as usize];
                let attempts = tcp_attempt_upper_bound(image, flow_index, generator)?;
                add_packet_count(&mut data_by_flow[generator.flow.0 as usize], attempts)?;
                add_packet_count(
                    &mut feedback_by_flow[generator.flow.0 as usize],
                    preloaded_data,
                )?;
                add_packet_count(&mut feedback_by_flow[generator.flow.0 as usize], attempts)?;
            }
            FlowGeneratorKind::Rate(_) => add_packet_count(
                &mut data_by_flow[generator.flow.0 as usize],
                executable_generator_packets(image, generator)?,
            )?,
            FlowGeneratorKind::Dcqcn(_) => {
                let count = executable_generator_packets(image, generator)?;
                let index = generator.flow.0 as usize;
                add_packet_count(&mut data_by_flow[index], count)?;
                let cnp_count = data_by_flow[index];
                dcqcn_cnp_by_flow[index] = cnp_count;
                add_packet_count(&mut feedback_by_flow[index], cnp_count)?;
            }
            FlowGeneratorKind::Roce(_) => {
                // At most one data packet per pacing tick; every data arrival, resident or
                // future, can answer with one ACK or NACK and one CNP.
                let count = executable_generator_packets(image, generator)?;
                let index = generator.flow.0 as usize;
                add_packet_count(&mut data_by_flow[index], count)?;
                let arrivals = data_by_flow[index];
                dcqcn_cnp_by_flow[index] = arrivals;
                add_packet_count(&mut feedback_by_flow[index], arrivals)?;
                add_packet_count(&mut feedback_by_flow[index], arrivals)?;
            }
        }
    }
    // Grouped after the counting loop above so that a resident whose flow is outside the dense
    // table still reaches that loop's direct index first, exactly as before.
    let resident_groups = ResidentFlowGroups::build(image, &resident_packets);
    let mut pfc_by_node = vec![0_u64; image.nodes.len()];
    let mut pfc_by_channel = vec![0_u64; image.channels.len()];
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
            let mut controller_frames = 0_u64;
            for flow in &image.flows {
                // Data is monitored in the flow's class, receiver feedback in its feedback class.
                let data_monitored = ingress.xoff_threshold_bytes[usize::from(flow.priority)] != 0;
                let feedback_monitored =
                    ingress.xoff_threshold_bytes[usize::from(flow.feedback_priority)] != 0;
                if !data_monitored && !feedback_monitored {
                    continue;
                }
                let mut monitored_packets = 0_u64;
                if data_monitored
                    && route_enters_pfc_controller(
                        image,
                        &flow.route,
                        ingress.controlled_link,
                        owner.id,
                    )
                {
                    let past_resident = u64::try_from(
                        resident_groups
                            .group(image, &resident_packets, flow.id)
                            .filter(|packet| packet.kind.is_data())
                            .filter(|packet| {
                                !packet_can_still_cross_link(image, packet, ingress.controlled_link)
                            })
                            .count(),
                    )
                    .map_err(|_| ValidationError::new("PFC resident packet count exceeds u64"))?;
                    let reachable = data_by_flow[flow.id.0 as usize]
                        .checked_sub(past_resident)
                        .expect("resident data count is included in the per-flow total");
                    add_packet_count(&mut monitored_packets, reachable)?;
                }
                if feedback_monitored
                    && route_enters_pfc_controller(
                        image,
                        &flow.reverse_route,
                        ingress.controlled_link,
                        owner.id,
                    )
                {
                    let past_resident = u64::try_from(
                        resident_groups
                            .group(image, &resident_packets, flow.id)
                            .filter(|packet| packet.kind.is_feedback())
                            .filter(|packet| {
                                !packet_can_still_cross_link(image, packet, ingress.controlled_link)
                            })
                            .count(),
                    )
                    .map_err(|_| ValidationError::new("PFC resident packet count exceeds u64"))?;
                    let reachable = feedback_by_flow[flow.id.0 as usize]
                        .checked_sub(past_resident)
                        .expect("resident feedback count is included in the per-flow total");
                    add_packet_count(&mut monitored_packets, reachable)?;
                }
                let frames = monitored_packets
                    .checked_mul(2)
                    .ok_or_else(|| ValidationError::new("PFC control-frame bound exceeds u64"))?;
                add_packet_count(&mut controller_frames, frames)?;
            }
            for priority in 0..8 {
                if ingress.xoff_threshold_bytes[priority] != 0 && ingress.pause_asserted[priority] {
                    add_packet_count(&mut controller_frames, 1)?;
                }
            }
            add_packet_count(&mut pfc_by_node[owner.id.0 as usize], controller_frames)?;
            add_packet_count(
                &mut pfc_by_channel[ingress.control_channel_index as usize],
                controller_frames,
            )?;
        }
    }
    Ok(FutureWork {
        data_by_flow,
        feedback_by_flow,
        dcqcn_cnp_by_flow,
        pfc_by_node,
        pfc_by_channel,
    })
}

fn validate_counters(image: &SimulationImage, work: &FutureWork) -> Result<(), ValidationError> {
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
                for generator in staged_generators(state) {
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
    Ok(())
}

/// Future timer events (and token payloads) of a compute stage: one while it waits for release,
/// none once its timer exists or it has finished. `None` for every other generator.
fn compute_future_timers(generator: StagedGenerator<'_>) -> Option<u64> {
    is_compute_generator(generator)
        .then(|| u64::from(generator.next_emission.status == GeneratorStatus::Blocked))
}

fn remaining_generator_packets(generator: StagedGenerator<'_>) -> Result<u64, ValidationError> {
    if is_compute_generator(generator) {
        // A compute stage sends no data packets.
        return Ok(0);
    }
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
    if let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind {
        let rate = dcqcn.rate;
        if generator.bytes_emitted > rate.total_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} DCQCN emitted byte state exceeds total bytes {}",
                generator.flow, rate.total_bytes
            )));
        }
        let expected_bytes = u128::from(generator.packets_emitted)
            .checked_mul(u128::from(rate.packet_size_bytes))
            .map(|bytes| bytes.min(u128::from(rate.total_bytes)))
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} DCQCN byte bookkeeping exceeds u128",
                    generator.flow
                ))
            })?;
        if u128::from(generator.bytes_emitted) != expected_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} DCQCN records {} emitted bytes, expected {expected_bytes}",
                generator.flow, generator.bytes_emitted
            )));
        }
        return Ok((rate.total_bytes - generator.bytes_emitted).div_ceil(rate.packet_size_bytes));
    }
    if let FlowGeneratorKind::Roce(roce) = generator.kind {
        // First transmissions only: the nominal packets from the high-water mark on.
        let pacer = roce.pacer;
        if generator.bytes_emitted > pacer.total_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} RoCE emitted byte state exceeds total bytes {}",
                generator.flow, pacer.total_bytes
            )));
        }
        let expected_bytes = u128::from(generator.packets_emitted)
            .checked_mul(u128::from(pacer.mtu_bytes))
            .map(|bytes| bytes.min(u128::from(pacer.total_bytes)))
            .ok_or_else(|| {
                ValidationError::new(format!(
                    "flow {:?} RoCE byte bookkeeping exceeds u128",
                    generator.flow
                ))
            })?;
        if u128::from(generator.bytes_emitted) != expected_bytes {
            return Err(ValidationError::new(format!(
                "flow {:?} RoCE records {} emitted bytes, expected {expected_bytes}",
                generator.flow, generator.bytes_emitted
            )));
        }
        return Ok((pacer.total_bytes - generator.bytes_emitted).div_ceil(pacer.mtu_bytes));
    }
    let FlowGeneratorKind::Constant(constant) = generator.kind else {
        let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
            unreachable!("rate, DCQCN, RoCE and collective generators return above")
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
    image: &SimulationImage,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    if matches!(generator.kind, FlowGeneratorKind::Rate(_))
        && matches!(
            generator.next_emission.status,
            GeneratorStatus::Scheduled | GeneratorStatus::Blocked
        )
    {
        return executable_rate_packets(image, generator);
    }
    if matches!(generator.kind, FlowGeneratorKind::Dcqcn(_))
        && matches!(
            generator.next_emission.status,
            GeneratorStatus::Scheduled | GeneratorStatus::Blocked
        )
    {
        return executable_dcqcn_packets(image, generator);
    }
    if let FlowGeneratorKind::Roce(roce) = generator.kind {
        remaining_generator_packets(generator)?;
        return Ok(roce_grid_ticks(image, &generator, roce));
    }
    match generator.next_emission.status {
        GeneratorStatus::Scheduled => remaining_generator_packets(generator),
        GeneratorStatus::Blocked if matches!(generator.kind, FlowGeneratorKind::Tcp(_)) => {
            remaining_generator_packets(generator)
        }
        GeneratorStatus::Blocked | GeneratorStatus::Finished | GeneratorStatus::Stopped => Ok(0),
    }
}

/// Conservative bound on timer installations reachable during the configured run.
///
/// Runtime TCP feedback is serialized by a positive-delay reverse route. A phase-0 ACK cancels
/// the flow's single phase-1 timer, so at most one runtime send-plan trigger per timestamp can
/// install a timer. Preloaded ACK events bypass that serialization and are reserved separately.
/// A Scheduled emission beyond the stop time cannot install a timer in this run.
fn tcp_timer_install_upper_bound(
    image: &SimulationImage,
    flow_index: &FlowIndex,
    generator: StagedGenerator<'_>,
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
    let Some(first_event_time) = flow_index.first_admissible_event_time_ns else {
        return Ok(0);
    };
    let preloaded_ack_events = flow_index
        .preloaded_tcp_acks(image, generator.flow)
        .within_stop_time;
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
    flow_index: &FlowIndex,
    generator: StagedGenerator<'_>,
    tcp: crate::TcpGenerator,
) -> Result<(), ValidationError> {
    let installations = tcp_timer_install_upper_bound(image, flow_index, generator)?;
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
    flow_index: &FlowIndex,
    generator: StagedGenerator<'_>,
) -> Result<u64, ValidationError> {
    if matches!(
        generator.next_emission.status,
        GeneratorStatus::Finished | GeneratorStatus::Stopped
    ) {
        return Ok(0);
    }
    let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
        return executable_generator_packets(image, generator);
    };
    if tcp.highest_ack >= tcp.total_bytes {
        return Ok(0);
    }
    let preloaded_ack_events = flow_index.preloaded_tcp_acks(image, generator.flow).total;
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
    flow_index: &FlowIndex,
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
        for generator in staged_generators(state) {
            let emissions = match generator.kind {
                FlowGeneratorKind::Constant(_) if is_compute_generator(generator) => {
                    compute_future_timers(generator).unwrap_or(0)
                }
                FlowGeneratorKind::Constant(_) => {
                    let remaining = executable_generator_packets(image, generator)?;
                    let scheduled =
                        u64::from(generator.next_emission.status == GeneratorStatus::Scheduled);
                    remaining.saturating_sub(scheduled)
                }
                FlowGeneratorKind::Tcp(_) => tcp_attempt_upper_bound(image, flow_index, generator)?,
                FlowGeneratorKind::Rate(_) => {
                    let ticks = executable_rate_pacing_ticks(image, generator)?;
                    if ticks == BigUint::from(0_u8) {
                        0
                    } else {
                        u64::try_from(ticks - 1_u8).map_err(|_| {
                            ValidationError::new(format!(
                                "flow {:?} successor pacing-timer count exceeds u64",
                                generator.flow
                            ))
                        })?
                    }
                }
                FlowGeneratorKind::Dcqcn(dcqcn) => {
                    let pacing_ticks = executable_dcqcn_pacing_ticks(image, generator)?;
                    let pacing_successors = if pacing_ticks == BigUint::from(0_u8) {
                        0
                    } else {
                        u64::try_from(pacing_ticks - 1_u8).map_err(|_| {
                            ValidationError::new(format!(
                                "flow {:?} DCQCN successor pacing-timer count exceeds u64",
                                generator.flow
                            ))
                        })?
                    };
                    let control_ticks = executable_dcqcn_control_ticks(image, dcqcn);
                    let control_successors = if control_ticks == BigUint::from(0_u8) {
                        0
                    } else {
                        u64::try_from(control_ticks - 1_u8).map_err(|_| {
                            ValidationError::new(format!(
                                "flow {:?} DCQCN successor control-timer count exceeds u64",
                                generator.flow
                            ))
                        })?
                    };
                    pacing_successors
                        .checked_add(control_successors)
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} DCQCN successor timer count exceeds u64",
                                generator.flow
                            ))
                        })?
                }
                FlowGeneratorKind::Roce(roce) => {
                    // Every pacing tick, restarts included, every control tick and every timeout
                    // installation is one emitted event (a conservative count: the pending tick
                    // and control tick already exist).
                    let pacing_ticks = roce_grid_ticks(image, &generator, roce);
                    let data_packets = executable_generator_packets(image, generator)?;
                    // The DCQCN count in u64 arithmetic, with no heap allocation per queue pair.
                    let controller = roce.controller;
                    let control_ticks = if controller.next_control_time_ns > image.stop_time_ns {
                        0
                    } else {
                        (image.stop_time_ns - controller.next_control_time_ns)
                            / controller.config.control_interval_ns
                            + 1
                    };
                    let timers = roce_timer_installations(image, roce, pacing_ticks, data_packets)?;
                    pacing_ticks
                        .checked_add(control_ticks)
                        .and_then(|total| total.checked_add(timers))
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "flow {:?} RoCE timer event count exceeds u64",
                                generator.flow
                            ))
                        })?
                }
            };
            emissions_by_node[owner.id.0 as usize] += u128::from(emissions);
        }
    }
    for (node_index, pfc_frames) in work.pfc_by_node.iter().copied().enumerate() {
        emissions_by_node[node_index] += u128::from(pfc_frames);
    }
    let owned_sequences = payload_sequences_by_owner(image);
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
            for sequence in owned_sequences[node.id.0 as usize].iter().copied() {
                if sequence >= next {
                    return Err(ValidationError::new(format!(
                        "switch node {:?} PFC payload sequence {sequence} is not below next origin sequence {next}",
                        node.id
                    )));
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
        if work.pfc_by_node[node.id.0 as usize] != 0 && generated != 0 {
            let last_sequence = next.checked_add(generated - 1).ok_or_else(|| {
                ValidationError::new(format!(
                    "switch node {:?} PFC payload sequence reservation exceeds u64",
                    node.id
                ))
            })?;
            let node_count = u64::try_from(image.nodes.len()).unwrap_or(u64::MAX);
            if PayloadId::from_node_sequence(node.id, node_count, last_sequence).is_none() {
                return Err(ValidationError::new(format!(
                    "switch node {:?} PFC payload identity space overflows by sequence {last_sequence}",
                    node.id
                )));
            }
        }
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
    flow_index: &FlowIndex,
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
        for generator in staged_generators(state) {
            if let Some(future_timers) = compute_future_timers(generator) {
                consumed_sequences = consumed_sequences
                    .checked_add(u64::from(
                        generator.next_emission.status == GeneratorStatus::Scheduled,
                    ))
                    .ok_or_else(|| {
                        ValidationError::new(format!(
                            "node {:?} consumed payload sequence count exceeds u64",
                            owner.id
                        ))
                    })?;
                allocations = allocations.checked_add(future_timers).ok_or_else(|| {
                    ValidationError::new(format!(
                        "node {:?} generated-packet count exceeds u64",
                        owner.id
                    ))
                })?;
                continue;
            }
            let remaining = match generator.kind {
                FlowGeneratorKind::Constant(_) => executable_generator_packets(image, generator)?,
                FlowGeneratorKind::Tcp(_) => tcp_attempt_upper_bound(image, flow_index, generator)?,
                FlowGeneratorKind::Rate(_) => rate_payload_allocations(image, generator)?,
                FlowGeneratorKind::Dcqcn(_) => dcqcn_payload_allocations(image, generator)?,
                FlowGeneratorKind::Roce(_) => {
                    // Its two timer tokens were allocated at lowering; every data packet,
                    // retransmissions included, takes a fresh payload when it is sent.
                    consumed_sequences = consumed_sequences
                        .checked_add(generator.packets_emitted)
                        .and_then(|total| total.checked_add(2))
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "node {:?} consumed payload sequence count exceeds u64",
                                owner.id
                            ))
                        })?;
                    allocations = allocations
                        .checked_add(executable_generator_packets(image, generator)?)
                        .ok_or_else(|| {
                            ValidationError::new(format!(
                                "node {:?} generated-packet count exceeds u64",
                                owner.id
                            ))
                        })?;
                    continue;
                }
            };
            let already_scheduled = u64::from(
                generator.next_emission.status == GeneratorStatus::Scheduled
                    || matches!(
                        generator.kind,
                        FlowGeneratorKind::Rate(_) | FlowGeneratorKind::Dcqcn(_)
                    ) && generator.next_emission.status == GeneratorStatus::Blocked,
            );
            consumed_sequences = consumed_sequences
                .checked_add(generator.packets_emitted)
                .and_then(|total| total.checked_add(already_scheduled))
                .and_then(|total| {
                    total.checked_add(u64::from(matches!(
                        generator.kind,
                        FlowGeneratorKind::Dcqcn(_)
                    )))
                })
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "node {:?} consumed payload sequence count exceeds u64",
                        owner.id
                    ))
                })?;
            let required = if matches!(
                generator.kind,
                FlowGeneratorKind::Rate(_) | FlowGeneratorKind::Dcqcn(_)
            ) {
                remaining
            } else {
                remaining.checked_sub(already_scheduled).ok_or_else(|| {
                    ValidationError::new(format!(
                        "flow {:?} has a scheduled emission after its constant generator finished",
                        generator.flow
                    ))
                })?
            };
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
        for receiver in &state.dcqcn_receivers {
            allocations = allocations
                .checked_add(work.dcqcn_cnp_by_flow[receiver.flow.0 as usize])
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "node {:?} generated DCQCN CNP count exceeds u64",
                        owner.id
                    ))
                })?;
        }
        for receiver in state.roce_receivers.iter().flatten() {
            // One ACK or NACK and one CNP per data arrival.
            let arrivals = work.data_by_flow[receiver.np.flow.0 as usize];
            allocations = arrivals
                .checked_mul(2)
                .and_then(|feedback| allocations.checked_add(feedback))
                .ok_or_else(|| {
                    ValidationError::new(format!(
                        "node {:?} generated RoCE feedback count exceeds u64",
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
            .flat_map(staged_generators)
            .filter(|generator| {
                remaining_generator_packets(*generator).is_ok_and(|count| count > 0)
            })
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
        PacketKind::Data | PacketKind::TcpData(_) | PacketKind::RoceData(_) => &flow.route,
        PacketKind::Feedback
        | PacketKind::TcpAck(_)
        | PacketKind::Pfc(_)
        | PacketKind::DcqcnCnp(_)
        | PacketKind::RoceAck(_)
        | PacketKind::RoceNack(_) => &flow.reverse_route,
        PacketKind::DcqcnControlTimer | PacketKind::RocePacingTimer => &[],
    }
}

fn packet_terminal(flow: &FlowDescriptor, packet_kind: PacketKind) -> NodeId {
    match packet_kind {
        PacketKind::Data | PacketKind::TcpData(_) | PacketKind::RoceData(_) => flow.target,
        PacketKind::Feedback
        | PacketKind::TcpAck(_)
        | PacketKind::Pfc(_)
        | PacketKind::DcqcnCnp(_)
        | PacketKind::RoceAck(_)
        | PacketKind::RoceNack(_)
        | PacketKind::DcqcnControlTimer
        | PacketKind::RocePacingTimer => flow.source,
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

/// Pre-index reference scans, retained as the oracle of the T20j equality gate.
///
/// Every function below is the exact expression the flow-indexed path replaced. They are
/// compiled only for the test-hook surface and exist so that
/// [`assert_validate_flow_index_equivalent_for_testing`] can prove, on real images, that an
/// indexed walk visits the same elements in the same order and folds to the same values as the
/// linear filter it replaced.
#[cfg(feature = "planner-test-hooks")]
mod legacy_scans {
    use super::{
        EventKind, FlowIndex, GeneratorStatus, PacketKind, PayloadId, SimulationImage,
        StagedGenerator, flow, packet, staged_generators,
    };

    pub(super) fn is_preloaded_tcp_ack_arrival(
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

    /// `validate.rs:5691` before the fix — the generator-invariant admissible-time minimum.
    pub(super) fn first_admissible_event_time_ns(image: &SimulationImage) -> Option<u64> {
        image
            .initial_events
            .iter()
            .map(|event| event.key.time_ns)
            .filter(|time_ns| *time_ns <= image.stop_time_ns)
            .min()
    }

    /// `validate.rs:5701` before the fix — preloaded ACK arrivals inside the run horizon.
    pub(super) fn preloaded_ack_events_within_stop_time(
        image: &SimulationImage,
        generator_flow: crate::FlowId,
    ) -> usize {
        image
            .initial_events
            .iter()
            .filter(|event| {
                event.key.time_ns <= image.stop_time_ns
                    && is_preloaded_tcp_ack_arrival(image, event, generator_flow)
            })
            .count()
    }

    /// `validate.rs:5778` before the fix — every preloaded ACK arrival of the flow.
    pub(super) fn preloaded_ack_events(
        image: &SimulationImage,
        generator_flow: crate::FlowId,
    ) -> usize {
        image
            .initial_events
            .iter()
            .filter(|event| is_preloaded_tcp_ack_arrival(image, event, generator_flow))
            .count()
    }

    /// `validate.rs:2381` and `validate.rs:3341` before the fix — the flow's initial packets.
    pub(super) fn packets_for_flow(
        image: &SimulationImage,
        generator_flow: crate::FlowId,
    ) -> Vec<PayloadId> {
        image
            .initial_packets
            .iter()
            .filter(|packet| packet.flow == generator_flow)
            .map(|packet| packet.id)
            .collect()
    }

    /// `validate.rs:3338` before the fix — the retransmission half of the future frame bound.
    pub(super) fn tcp_future_data_max_frame(
        image: &SimulationImage,
        generator: StagedGenerator<'_>,
        tcp: crate::TcpGenerator,
    ) -> u64 {
        if matches!(
            generator.next_emission.status,
            GeneratorStatus::Finished | GeneratorStatus::Stopped
        ) {
            return 0;
        }
        let fresh = tcp.mss_bytes.min(tcp.total_bytes - tcp.next_sequence);
        let retransmission = image
            .initial_packets
            .iter()
            .filter(|packet| packet.flow == generator.flow)
            .filter_map(|packet| {
                let PacketKind::TcpData(header) = packet.kind else {
                    return None;
                };
                let end = header.sequence.checked_add(packet.size_bytes)?;
                (header.sequence < tcp.next_sequence && end > tcp.highest_ack)
                    .then_some(end - header.sequence.max(tcp.highest_ack))
            })
            .max()
            .unwrap_or(0);
        fresh.max(retransmission)
    }

    /// P14's `stage_generator` before the P14 perf fix — the first generator whose flow is `id`,
    /// in `host_states`-then-`generators` order. It ran once per flow in `validate_flows`, once
    /// per initial packet in `validate_packets_and_derive_delays`, and at every stage predecessor.
    pub(super) fn generator_for_flow(
        image: &SimulationImage,
        id: crate::FlowId,
    ) -> Option<StagedGenerator<'_>> {
        image
            .host_states
            .iter()
            .flat_map(staged_generators)
            .find(|generator| generator.flow == id)
    }

    /// `validate_collective_stage`'s `find_stage` before the P14 perf fix — the first generator,
    /// in `host_states`-then-`generators` order, whose collective identity sits at `position`.
    pub(super) fn collective_stage(
        image: &SimulationImage,
        position: super::CollectivePosition,
    ) -> Option<StagedGenerator<'_>> {
        let (collective_id, phase, rank, step) = position;
        image
            .host_states
            .iter()
            .flat_map(staged_generators)
            .find(|candidate| {
                super::collective_identity(*candidate).is_some_and(|stage| {
                    stage.collective_id == collective_id
                        && stage.phase == phase
                        && stage.rank == rank
                        && stage.step == step
                })
            })
    }

    /// `validate.rs:5316` and `validate.rs:5337` before the fix — the flow's executable residents.
    ///
    /// The two sites differ only in the data/feedback lane filter they apply afterwards, which is
    /// unchanged; what the grouping replaced is the `packet.flow == flow.id` clause.
    pub(super) fn resident_packets_for_flow(
        residents: &[&crate::PacketDescriptor],
        generator_flow: crate::FlowId,
    ) -> Vec<PayloadId> {
        residents
            .iter()
            .filter(|packet| packet.flow == generator_flow)
            .map(|packet| packet.id)
            .collect()
    }

    /// `validate.rs:2653` before the fix — one full event rescan per Blocked TCP generator.
    pub(super) fn matching_retransmission_timeout_events(
        image: &SimulationImage,
        owner: crate::NodeId,
        attempt: PayloadId,
        deadline_ns: u64,
    ) -> usize {
        image
            .initial_events
            .iter()
            .filter(|event| {
                event.kind == EventKind::RetransmissionTimeout
                    && event.target == owner
                    && event.payload == attempt
                    && event.key.time_ns == deadline_ns
            })
            .count()
    }

    /// `validate.rs:5946` before the fix — one full packet rescan per switch LP.
    pub(super) fn owned_payload_sequences(
        image: &SimulationImage,
        node: &crate::NodeDescriptor,
    ) -> Vec<u64> {
        let node_count = image.nodes.len() as u64;
        let mut sequences = Vec::new();
        for packet in &image.initial_packets {
            if node_count != 0 && packet.id.0 % node_count == node.id.0 {
                sequences.push(packet.id.0 / node_count);
            }
        }
        sequences
    }

    /// `validate.rs:4527` and `validate.rs:5190` before the fix — two independent derivations.
    ///
    /// The fix keeps only the first. That is sound exactly when the derivation is a pure function
    /// of the image, which is what this compares: two derivations must agree on the reservation,
    /// or agree on rejecting the image with the same diagnostic.
    pub(super) fn future_work_is_stable(
        image: &SimulationImage,
        flow_index: &FlowIndex,
    ) -> Result<(), String> {
        let first = super::future_work(image, flow_index);
        let second = super::future_work(image, flow_index);
        match (first, second) {
            (Ok(left), Ok(right)) => {
                let matches = left.data_by_flow == right.data_by_flow
                    && left.feedback_by_flow == right.feedback_by_flow
                    && left.dcqcn_cnp_by_flow == right.dcqcn_cnp_by_flow
                    && left.pfc_by_node == right.pfc_by_node
                    && left.pfc_by_channel == right.pfc_by_channel;
                if matches {
                    Ok(())
                } else {
                    Err("future work reservations differ across two derivations".to_string())
                }
            }
            (Err(left), Err(right)) if left.to_string() == right.to_string() => Ok(()),
            (left, right) => Err(format!(
                "future work derivations disagree: {} then {}",
                if left.is_ok() { "accepted" } else { "rejected" },
                if right.is_ok() {
                    "accepted"
                } else {
                    "rejected"
                }
            )),
        }
    }
}

/// Whether [`FlowIndex::build`] built the generator lookups (`generator_for_flow`,
/// `collective_stage`) for `image`.
///
/// Only stage validation asks those questions, so an image without a stage generator should not
/// pay for them.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn validate_flow_index_builds_stage_lookups_for_testing(image: &SimulationImage) -> bool {
    FlowIndex::build(image).has_stage_generators()
}

/// P14 perf equality gate for the flow-keyed generator lookup alone.
///
/// Compares [`FlowIndex::generator_for_flow`] with the scan it replaced. Unlike the full
/// [`assert_validate_flow_index_equivalent_for_testing`], it reads nothing but the generator
/// tables, so it can be pointed at images the validator rejects: duplicate generators for one
/// flow, and generators naming flows outside the dense table.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn assert_validate_generator_index_equivalent_for_testing(
    image: &SimulationImage,
) -> Result<(), String> {
    let flow_index = FlowIndex::build(image);
    // The generator lookup must return the very generator the scan found (pointer identity, not
    // value equality), for every dense flow and for every identifier outside the dense table that
    // a generator or a stage dependency names: those are the only queries that reach the
    // unindexed fallback.
    let mut generator_queries = image
        .flows
        .iter()
        .map(|descriptor| descriptor.id)
        .collect::<Vec<_>>();
    for generator in image.host_states.iter().flat_map(staged_generators) {
        generator_queries.push(generator.flow);
        if let Some(stage) = generator.stage {
            generator_queries.extend(stage.dependencies.local_predecessor);
            generator_queries.extend(stage.dependencies.inbound_predecessor);
        }
    }
    for packet in &image.initial_packets {
        generator_queries.push(packet.flow);
    }
    generator_queries.sort_unstable();
    generator_queries.dedup();
    for id in generator_queries {
        let indexed = flow_index.generator_for_flow(image, id);
        let scanned = legacy_scans::generator_for_flow(image, id);
        let same = match (indexed, scanned) {
            (Some(indexed), Some(scanned)) => std::ptr::eq(indexed.generator, scanned.generator),
            (None, None) => true,
            _ => false,
        };
        if !same {
            return Err(format!(
                "flow {id:?} indexed generator {:?} differs from the scanned generator {:?}",
                indexed.map(|generator| generator.flow),
                scanned.map(|generator| generator.flow)
            ));
        }
        // `is_compute_flow` answers `false` without a lookup when the image has no stage.
        let indexed_compute = flow_index.is_compute_flow(image, id);
        let scanned_compute = scanned.is_some_and(is_compute_generator);
        if indexed_compute != scanned_compute {
            return Err(format!(
                "flow {id:?} indexed compute answer {indexed_compute} differs from the scanned \
                 answer {scanned_compute}"
            ));
        }
    }

    // Every stage's own position, and one perturbed component each: an index keyed more coarsely
    // than the scan's four-way match answers a perturbed key with a stage, while the scan answers
    // `None` or a different stage.
    let mut positions = Vec::new();
    for generator in image.host_states.iter().flat_map(staged_generators) {
        let Some(stage) = collective_identity(generator) else {
            continue;
        };
        let other_phase = match stage.phase {
            crate::CollectivePhase::ReduceScatter => crate::CollectivePhase::AllGather,
            crate::CollectivePhase::AllGather => crate::CollectivePhase::ReduceScatter,
        };
        positions.extend([
            (stage.collective_id, stage.phase, stage.rank, stage.step),
            (
                stage.collective_id.wrapping_add(1),
                stage.phase,
                stage.rank,
                stage.step,
            ),
            (stage.collective_id, other_phase, stage.rank, stage.step),
            (
                stage.collective_id,
                stage.phase,
                stage.rank.wrapping_add(1),
                stage.step,
            ),
            (
                stage.collective_id,
                stage.phase,
                stage.rank,
                stage.step.wrapping_sub(1),
            ),
        ]);
    }
    positions.sort_unstable();
    positions.dedup();
    for position in positions {
        let indexed = flow_index.collective_stage(image, position);
        let scanned = legacy_scans::collective_stage(image, position);
        let same = match (indexed, scanned) {
            (Some(indexed), Some(scanned)) => std::ptr::eq(indexed.generator, scanned.generator),
            (None, None) => true,
            _ => false,
        };
        if !same {
            return Err(format!(
                "collective position {position:?} indexed stage {:?} differs from the scanned stage {:?}",
                indexed.map(|generator| generator.flow),
                scanned.map(|generator| generator.flow)
            ));
        }
    }
    Ok(())
}

/// Proves that the flow-indexed validator answers every per-generator query exactly as the
/// pre-index linear scans did, on `image`.
///
/// This is the T20j verdict-equality gate. It compares, site by site, the value the shipped path
/// derives against the value the retained scan derives: identical element *sequences* where a
/// diagnostic names the first offender, and identical folds everywhere else. It reports the first
/// disagreement instead of panicking so the caller can name the fixture.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn assert_validate_flow_index_equivalent_for_testing(
    image: &SimulationImage,
) -> Result<(), String> {
    let flow_index = FlowIndex::build(image);

    if flow_index.first_admissible_event_time_ns
        != legacy_scans::first_admissible_event_time_ns(image)
    {
        return Err(format!(
            "hoisted first admissible event time {:?} differs from the scanned minimum {:?}",
            flow_index.first_admissible_event_time_ns,
            legacy_scans::first_admissible_event_time_ns(image)
        ));
    }

    // Every flow, not only the generator-owning ones: the grouping must partition the table.
    let mut indexed_total = 0_usize;
    for descriptor in &image.flows {
        let indexed = flow_index
            .packets_for_flow(image, descriptor.id)
            .map(|packet| packet.id)
            .collect::<Vec<_>>();
        let scanned = legacy_scans::packets_for_flow(image, descriptor.id);
        if indexed != scanned {
            return Err(format!(
                "flow {:?} indexed packet walk {indexed:?} differs from the scanned walk {scanned:?}",
                descriptor.id
            ));
        }
        indexed_total += indexed.len();
    }
    let flow_resolved = image
        .initial_packets
        .iter()
        .filter(|packet| dense_flow_slot(image, packet.flow).is_some())
        .count();
    if indexed_total != flow_resolved {
        return Err(format!(
            "flow-keyed packet groups cover {indexed_total} packets, but {flow_resolved} resolve to a flow"
        ));
    }

    // `future_work`'s PFC reservation groups the *derived* resident table, so it is compared the
    // same way: element sequence per flow, plus the partition property.
    let residents = executable_resident_packets(image);
    let resident_groups = ResidentFlowGroups::build(image, &residents);
    let mut grouped_residents = 0_usize;
    for descriptor in &image.flows {
        let indexed = resident_groups
            .group(image, &residents, descriptor.id)
            .map(|packet| packet.id)
            .collect::<Vec<_>>();
        let scanned = legacy_scans::resident_packets_for_flow(&residents, descriptor.id);
        if indexed != scanned {
            return Err(format!(
                "flow {:?} indexed resident walk {indexed:?} differs from the scanned walk {scanned:?}",
                descriptor.id
            ));
        }
        grouped_residents += indexed.len();
    }
    let resident_resolved = residents
        .iter()
        .filter(|packet| dense_flow_slot(image, packet.flow).is_some())
        .count();
    if grouped_residents != resident_resolved {
        return Err(format!(
            "flow-keyed resident groups cover {grouped_residents} packets, but {resident_resolved} resolve to a flow"
        ));
    }

    // Every identifier that labels a packet but resolves to no dense slot. No query derived from
    // `image.flows` can reach one, so without this loop the unindexed fallbacks — the shared group
    // plus its equality filter — would never be evaluated by the gate at all.
    let mut unindexed_queries = image
        .initial_packets
        .iter()
        .map(|packet| packet.flow)
        .filter(|id| dense_flow_slot(image, *id).is_none())
        .collect::<Vec<_>>();
    unindexed_queries.sort_unstable();
    unindexed_queries.dedup();
    for id in unindexed_queries {
        let indexed = flow_index
            .packets_for_flow(image, id)
            .map(|packet| packet.id)
            .collect::<Vec<_>>();
        let scanned = legacy_scans::packets_for_flow(image, id);
        if indexed != scanned {
            return Err(format!(
                "unindexed flow {id:?} indexed packet walk {indexed:?} differs from the scanned walk {scanned:?}"
            ));
        }
        let indexed_residents = resident_groups
            .group(image, &residents, id)
            .map(|packet| packet.id)
            .collect::<Vec<_>>();
        let scanned_residents = legacy_scans::resident_packets_for_flow(&residents, id);
        if indexed_residents != scanned_residents {
            return Err(format!(
                "unindexed flow {id:?} indexed resident walk {indexed_residents:?} differs from the scanned walk {scanned_residents:?}"
            ));
        }
    }

    for generator in image.host_states.iter().flat_map(staged_generators) {
        let acks = flow_index.preloaded_tcp_acks(image, generator.flow);
        let scanned_total = legacy_scans::preloaded_ack_events(image, generator.flow) as u64;
        let scanned_within =
            legacy_scans::preloaded_ack_events_within_stop_time(image, generator.flow) as u64;
        if acks.total != scanned_total || acks.within_stop_time != scanned_within {
            return Err(format!(
                "flow {:?} indexed preloaded ACK counts ({}, {}) differ from the scanned counts ({scanned_total}, {scanned_within})",
                generator.flow, acks.total, acks.within_stop_time
            ));
        }
        // `validate_global_time_capacity` reaches the future-frame bound only for a generator
        // with executable packets left, and the helper's arithmetic assumes that guard. The
        // comparison keeps it so that the gate evaluates the helper exactly where the validator
        // does, on images whose generators have already retired included.
        let executable = executable_generator_packets(image, generator).unwrap_or(0);
        if let (FlowGeneratorKind::Tcp(tcp), true) = (generator.kind, executable != 0) {
            let indexed = tcp_future_data_max_frame(image, &flow_index, generator, tcp);
            let scanned = legacy_scans::tcp_future_data_max_frame(image, generator, tcp);
            if indexed != scanned {
                return Err(format!(
                    "flow {:?} indexed future data frame {indexed} differs from the scanned frame {scanned}",
                    generator.flow
                ));
            }
        }
    }

    assert_validate_generator_index_equivalent_for_testing(image)?;

    // The timeout buckets must partition exactly the timeout events, the same way the flow-keyed
    // packet groups must partition the resolvable packets. Without this an index that admitted
    // events of other kinds would agree at every key the corpus happens to query.
    let indexed_timeout_total = flow_index.retransmission_timeouts.values().sum::<u64>();
    let scanned_timeout_total = image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::RetransmissionTimeout)
        .count() as u64;
    if indexed_timeout_total != scanned_timeout_total {
        return Err(format!(
            "timeout-identity buckets cover {indexed_timeout_total} events, but {scanned_timeout_total} are retransmission timeouts"
        ));
    }

    // The timeout-event count is keyed by a timer's executable identity, so it is compared where
    // the validator asks for it: at every TCP generator that owns an active timer, on the host
    // node that owns the generator. Three neighbouring keys are probed as well, so an index keyed
    // more coarsely than the scan's four-way match cannot agree with it.
    for owner in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
    {
        let Some(state) = image.host_states.get(owner.state_slot as usize) else {
            continue;
        };
        for generator in staged_generators(state) {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                continue;
            };
            let Some(timer) = tcp.active_timer else {
                continue;
            };
            let attempt = timer.attempt;
            let deadline = timer.deadline_ns;
            let probes = [
                (owner.id, attempt, deadline),
                // One perturbed component each: an index keyed more coarsely than the scan's
                // four-way match answers a perturbed key with the unperturbed key's count, while
                // the scan answers 0.
                (NodeId(owner.id.0.wrapping_add(1)), attempt, deadline),
                (owner.id, PayloadId(attempt.0.wrapping_add(1)), deadline),
                (owner.id, attempt, deadline.wrapping_add(1)),
            ];
            for (node_id, payload, deadline_ns) in probes {
                let indexed =
                    flow_index.retransmission_timeout_events(node_id, payload, deadline_ns);
                let scanned = legacy_scans::matching_retransmission_timeout_events(
                    image,
                    node_id,
                    payload,
                    deadline_ns,
                ) as u64;
                if indexed != scanned {
                    return Err(format!(
                        "node {node_id:?} payload {payload:?} deadline {deadline_ns} has {indexed} indexed timeout events but {scanned} scanned ones"
                    ));
                }
            }
        }
    }

    let owned_sequences = payload_sequences_by_owner(image);
    for node in &image.nodes {
        let scanned = legacy_scans::owned_payload_sequences(image, node);
        // `.get` rather than indexing: the gate must report a mismatch, never panic, if it is
        // ever pointed at an image whose node identifiers are not dense.
        let bucketed = owned_sequences.get(node.id.0 as usize);
        if bucketed != Some(&scanned) {
            return Err(format!(
                "node {:?} bucketed payload sequences {bucketed:?} differ from the scanned sequences {scanned:?}",
                node.id
            ));
        }
    }

    legacy_scans::future_work_is_stable(image, &flow_index)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::StagedGenerator;

    /// Every validator that asks about a generator's stage receives this view by value, so its
    /// size is the number of bytes copied per call, once per generator in each of the validators
    /// that walk the generator tables, on every image, stageless ones included. Holding the stage
    /// record by value made the view the generator reference plus a copied
    /// `Option<CollectiveStage>`, 144 B. The bound was motivated by `validate`'s residue on E1's
    /// stageless image, 5.6 M instructions more than `main` (948a0e9); borrowing the record
    /// recovered about 2.3 M of it, and the other 3.3 M is not attributed to the copy
    /// (`days-gpu/evidence/P14/e1-residue.md`). A reference to the record in the host's stage
    /// table keeps the view at two pointers, the reference and the niche-packed optional one.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn staged_generator_view_is_two_pointers() {
        const TWO_POINTERS: usize = 2 * std::mem::size_of::<usize>();
        let size = std::mem::size_of::<StagedGenerator<'static>>();
        assert!(
            size <= TWO_POINTERS,
            "StagedGenerator is {size} B, above {TWO_POINTERS} B: borrow the stage record from \
             the host's stage table instead of copying it"
        );
    }
}

//! Host-only sizing for the default production GPU execution plan.
//!
//! This module mirrors the capacity derivation used by the CUDA and Metal planners, but retains
//! only small host-side capacity vectors. It never materializes event planes or initializes a
//! device backend.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use num_bigint::BigUint;

use crate::device_scheduler::device_scheduler_word_count;
use crate::{
    EventKind, FlowGeneratorKind, GeneratorStatus, LinkId, NodeKind, PacketKind, SimulationImage,
    serialization_time_ns,
};

const EVENT_WORDS: usize = 14;
const NODE_WORDS: usize = 11;
const GENERATOR_WORDS: usize = 43;
const FLOW_WORDS: usize = 6;
const LINK_WORDS: usize = 4;
const ARENA_META_WORDS: usize = 4;
const SUMMARY_COUNTERS: usize = 12;
const LP_STATE_WORDS: usize = 6;
const OBSERVATION_META_WORDS: usize = ARENA_META_WORDS * 3;
const INBOUND_META_WORDS: usize = 2;
const LP_STREAM_META_WORDS: usize = 4;
const OUTBOUND_META_WORDS: usize = 2;
const OUTBOUND_ENTRY_WORDS: usize = 2;
const CHANNEL_BATCH_WORDS: usize = 4;
const ACTIVE_STREAM_ENTRY_WORDS: usize = 5;
const CONTROL_WORDS: usize = 19;
const PARAM_WORDS: usize = 35;
const WORD_BYTES: usize = std::mem::size_of::<u64>();

const PLANE_NAMES: [&str; 28] = [
    "control",
    "params",
    "node_state",
    "generators",
    "flows",
    "routes",
    "links",
    "fel_meta",
    "fel_records",
    "queue_meta",
    "queue_records",
    "in_service",
    "outbox",
    "worklist",
    "summary",
    "observed",
    "departures",
    "arrivals",
    "lp_state",
    "remote_meta",
    "remote_staging",
    "observation_meta",
    "inbound_meta",
    "inbound_producers",
    "merge_cursors",
    "stream_state",
    "stream_records",
    "scheduler_state",
];

/// Exact size of one `u64` device plane in the default production GPU plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DevicePlaneSizing {
    pub index: usize,
    pub name: &'static str,
    pub words: usize,
    pub bytes: usize,
}

/// Exact event-arena sizing fields reported by production GPU runs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeviceEventArenaSizing {
    pub legacy_heap_event_slots: usize,
    pub fallback_heap_event_slots: usize,
    pub channel_stream_event_slots: usize,
    pub service_stream_event_slots: usize,
    pub generator_stream_event_slots: usize,
    pub heap_arena_bytes: usize,
    pub stream_arena_bytes: usize,
    pub legacy_heap_arena_bytes: usize,
}

impl DeviceEventArenaSizing {
    pub fn total_event_arena_bytes(self) -> usize {
        self.heap_arena_bytes
            .saturating_add(self.stream_arena_bytes)
    }
}

/// Complete host-only sizing report for the default production GPU device planes.
///
/// Open-loop images retain the established 28 planes. TCP images add one packed auxiliary plane
/// for receiver ranges, segment ledgers, and full-observation transition state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceSizingReport {
    pub planes: Vec<DevicePlaneSizing>,
    pub total_device_bytes: usize,
    pub event_arenas: DeviceEventArenaSizing,
}

/// Failure to derive an exact device plan size.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceSizingError(String);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RateDeviceWork {
    pub pacing_ticks: usize,
    pub packets: usize,
    pub payload_allocations: usize,
}

pub(crate) fn rate_device_work(
    image: &SimulationImage,
    generator: &crate::FlowGeneratorState,
    rate: crate::RateGenerator,
) -> Result<RateDeviceWork, DeviceSizingError> {
    if !matches!(
        generator.next_emission.status,
        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
    ) || generator.bytes_emitted >= rate.total_bytes
        || generator.next_emission.departure_time_ns > image.stop_time_ns
    {
        return Ok(RateDeviceWork::default());
    }
    let available_ticks = BigUint::from(
        1 + (image.stop_time_ns - generator.next_emission.departure_time_ns)
            / rate.pacing_interval_ns,
    );
    let credit = BigUint::from(rate.credit_quanta);
    let tick =
        BigUint::from(rate.rate_numerator_bits_per_second) * BigUint::from(rate.pacing_interval_ns);
    let scale = BigUint::from(rate.rate_denominator) * BigUint::from(1_000_000_000_u64);
    let remaining = rate.total_bytes - generator.bytes_emitted;
    let full_packets = (remaining - 1) / rate.packet_size_bytes;
    let last_size = remaining - full_packets * rate.packet_size_bytes;
    let full_cost = BigUint::from(rate.packet_size_bytes) * BigUint::from(8_u8) * &scale;
    let last_cost = BigUint::from(last_size) * BigUint::from(8_u8) * &scale;

    fn ceil_deficit(deficit: BigUint, tick: &BigUint) -> BigUint {
        if deficit == BigUint::from(0_u8) {
            BigUint::from(0_u8)
        } else {
            (deficit + tick - BigUint::from(1_u8)) / tick
        }
    }
    fn sub_floor(left: BigUint, right: &BigUint) -> BigUint {
        if left > *right {
            left - right
        } else {
            BigUint::from(0_u8)
        }
    }
    fn full_by_ticks(
        ticks: &BigUint,
        full_packets: u64,
        credit: &BigUint,
        tick: &BigUint,
        cost: &BigUint,
    ) -> BigUint {
        let by_credit = (credit + ticks * tick) / cost;
        by_credit
            .min(ticks.clone())
            .min(BigUint::from(full_packets))
    }
    fn packet_count(
        ticks: &BigUint,
        full_packets: u64,
        credit: &BigUint,
        tick: &BigUint,
        full_cost: &BigUint,
        last_cost: &BigUint,
    ) -> BigUint {
        let full = full_by_ticks(ticks, full_packets, credit, tick, full_cost);
        if full < BigUint::from(full_packets) {
            return full;
        }
        let full_count = BigUint::from(full_packets);
        let full_deficit = sub_floor(&full_count * full_cost, credit);
        let ticks_for_full = full_count.clone().max(ceil_deficit(full_deficit, tick));
        let credit_after_full = credit + &ticks_for_full * tick - &full_count * full_cost;
        let last_deficit = sub_floor(last_cost.clone(), &credit_after_full);
        let extra = BigUint::from(1_u8).max(ceil_deficit(last_deficit, tick));
        full_count + BigUint::from(u8::from(ticks_for_full + extra <= *ticks))
    }

    let packets = packet_count(
        &available_ticks,
        full_packets,
        &credit,
        &tick,
        &full_cost,
        &last_cost,
    );
    let prior_ticks = sub_floor(available_ticks.clone(), &BigUint::from(1_u8));
    let payload_allocations = packet_count(
        &prior_ticks,
        full_packets,
        &credit,
        &tick,
        &full_cost,
        &last_cost,
    )
    .min(BigUint::from(full_packets));
    let all_packets = BigUint::from(full_packets + 1);
    let pacing_ticks = if packets == all_packets {
        let full_count = BigUint::from(full_packets);
        let full_deficit = sub_floor(&full_count * &full_cost, &credit);
        let ticks_for_full = full_count.clone().max(ceil_deficit(full_deficit, &tick));
        let credit_after_full = &credit + &ticks_for_full * &tick - &full_count * &full_cost;
        let last_deficit = sub_floor(last_cost, &credit_after_full);
        ticks_for_full + BigUint::from(1_u8).max(ceil_deficit(last_deficit, &tick))
    } else {
        available_ticks
    };
    Ok(RateDeviceWork {
        pacing_ticks: pacing_ticks.try_into().unwrap_or(usize::MAX),
        packets: packets.try_into().unwrap_or(usize::MAX),
        payload_allocations: payload_allocations.try_into().unwrap_or(usize::MAX),
    })
}

impl fmt::Display for DeviceSizingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for DeviceSizingError {}

/// Sizes the default streams-enabled, summary-observation GPU plan without allocating its planes.
pub fn size_default_device_plan(
    image: &SimulationImage,
) -> Result<DeviceSizingReport, DeviceSizingError> {
    let node_count = image.nodes.len();
    let flow_count = image.flows.len();
    let link_count = image.links.len();
    let flow_packet_counts = flow_packet_counts(image)?;
    let flow_feedback_counts = feedback_packet_counts(image, &flow_packet_counts);
    let minimum_lookahead_ns = image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min();
    let context = CapacityContext::new(
        image,
        &flow_packet_counts,
        &flow_feedback_counts,
        minimum_lookahead_ns,
    )?;

    let mut queue_capacities = vec![1_usize; node_count];
    let mut legacy_fel_capacities = vec![8_usize; node_count];
    for event in &image.initial_events {
        legacy_fel_capacities[event.target.0 as usize] =
            legacy_fel_capacities[event.target.0 as usize].saturating_add(1);
    }
    for (flow_index, flow) in image.flows.iter().enumerate() {
        let packet_count = flow_packet_counts[flow_index];
        let feedback_count = flow_feedback_counts[flow_index];
        let data_count = packet_count.saturating_sub(feedback_count);
        let source_slot = flow.source.0 as usize;
        queue_capacities[source_slot] =
            queue_capacities[source_slot].saturating_add(context.source_queue_bounds[flow_index]);
        legacy_fel_capacities[source_slot] = legacy_fel_capacities[source_slot].saturating_add(4);
        add_flow_route_capacities(
            image,
            &context,
            flow_index,
            data_count,
            PacketKind::Data,
            &mut legacy_fel_capacities,
            &mut queue_capacities,
        );
        add_flow_route_capacities(
            image,
            &context,
            flow_index,
            feedback_count,
            PacketKind::Feedback,
            &mut legacy_fel_capacities,
            &mut queue_capacities,
        );
    }
    for node in &image.nodes {
        let slot = node.id.0 as usize;
        match node.kind {
            NodeKind::Host => {
                let state = &image.host_states[node.state_slot as usize];
                queue_capacities[slot] = queue_capacities[slot].max(state.queue.len());
            }
            NodeKind::Switch => {
                let state = &image.switch_states[node.state_slot as usize];
                let initial = state.queues.first().map_or(0, |queue| queue.queue.len());
                queue_capacities[slot] = queue_capacities[slot].max(initial);
                if let Some(limit) = state
                    .queues
                    .first()
                    .filter(|queue| matches!(queue.drop_mark, crate::DropMarkPolicy::TailDrop))
                    .map(|queue| queue.queue_capacity_packets)
                    .filter(|limit| *limit != 0)
                {
                    queue_capacities[slot] = queue_capacities[slot].min(limit as usize);
                }
            }
        }
    }

    let runtime_tcp_timer_slots = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Tcp(_)))
        .try_fold(0_usize, |total, generator| {
            total
                .checked_add(flow_packet_counts[generator.flow.0 as usize].max(1))
                .ok_or_else(|| sizing_error("TCP fallback FEL slots overflow usize"))
        })?;
    let fallback_fel_event_slots = node_count
        .checked_add(image.initial_events.len())
        .and_then(|slots| slots.checked_add(runtime_tcp_timer_slots))
        .ok_or_else(|| sizing_error("fallback FEL slots overflow usize"))?;
    let legacy_heap_event_slots = checked_sum(&legacy_fel_capacities, "legacy FEL slots")?;
    let queue_slots = checked_sum(&queue_capacities, "queue slots")?;
    let remote_bound = derived_remote_capacity(image, &context);
    let remote_capacities = derived_remote_capacities(image, &context);
    let remote_staging_slots = checked_sum(&remote_capacities, "remote staging slots")?;
    let channel_capacities = derived_channel_stream_capacities(image, &context)?;
    let channel_stream_event_slots = checked_sum(&channel_capacities, "channel stream slots")?;
    let service_stream_event_slots = node_count
        .checked_mul(2)
        .ok_or_else(|| sizing_error("service stream slots overflow usize"))?;
    let generator_stream_event_slots = flow_count
        .checked_mul(2)
        .ok_or_else(|| sizing_error("generator stream slots overflow usize"))?;
    let stream_record_slots = channel_stream_event_slots
        .checked_add(service_stream_event_slots)
        .and_then(|slots| slots.checked_add(generator_stream_event_slots))
        .ok_or_else(|| sizing_error("stream record slots overflow usize"))?;
    let stream_state_words = stream_state_words(image, remote_staging_slots)?.max(1);
    let inbound_producer_words = inbound_producer_words(image);

    let route_words = image
        .flows
        .iter()
        .try_fold(0_usize, |total, flow| {
            total
                .checked_add(flow.route.len())
                .and_then(|words| words.checked_add(flow.reverse_route.len()))
                .ok_or_else(|| sizing_error("route plane overflows usize"))
        })?
        .max(1);
    let words = [
        CONTROL_WORDS,
        PARAM_WORDS,
        checked_product(node_count, NODE_WORDS, "node-state plane")?,
        checked_product(flow_count.max(1), GENERATOR_WORDS, "generator plane")?,
        checked_product(flow_count.max(1), FLOW_WORDS, "flow plane")?,
        route_words,
        checked_product(link_count.max(1), LINK_WORDS, "link plane")?,
        checked_product(node_count, ARENA_META_WORDS, "FEL metadata plane")?,
        checked_product(fallback_fel_event_slots, EVENT_WORDS, "FEL record plane")?.max(1),
        checked_product(node_count, ARENA_META_WORDS, "queue metadata plane")?,
        checked_product(queue_slots, EVENT_WORDS, "queue record plane")?.max(1),
        checked_product(node_count, EVENT_WORDS, "in-service plane")?.max(1),
        checked_product(remote_bound.max(1), EVENT_WORDS, "outbox plane")?,
        node_count.max(1),
        checked_product(node_count.max(1), SUMMARY_COUNTERS * 2, "summary plane")?,
        1,
        1,
        1,
        checked_product(node_count.max(1), LP_STATE_WORDS, "LP-state plane")?,
        checked_product(node_count, ARENA_META_WORDS, "remote metadata plane")?,
        checked_product(remote_staging_slots, EVENT_WORDS, "remote staging plane")?.max(1),
        checked_product(
            node_count,
            OBSERVATION_META_WORDS,
            "observation metadata plane",
        )?,
        checked_product(node_count, INBOUND_META_WORDS, "inbound metadata plane")?,
        inbound_producer_words.max(1),
        inbound_producer_words.max(1),
        stream_state_words,
        checked_product(stream_record_slots, EVENT_WORDS, "stream record plane")?.max(1),
        device_scheduler_word_count(image, &queue_capacities).map_err(DeviceSizingError)?,
    ];

    let mut planes = PLANE_NAMES
        .into_iter()
        .zip(words)
        .enumerate()
        .map(|(index, (name, words))| {
            Ok(DevicePlaneSizing {
                index,
                name,
                words,
                bytes: checked_product(words, WORD_BYTES, "device plane bytes")?,
            })
        })
        .collect::<Result<Vec<_>, DeviceSizingError>>()?;
    if image.host_states.iter().any(|state| {
        !state.tcp_receivers.is_empty()
            || state
                .generators
                .iter()
                .any(|generator| matches!(generator.kind, FlowGeneratorKind::Tcp(_)))
    }) {
        let tcp_words = packed_tcp_state_words(image, &flow_packet_counts)?;
        planes.push(DevicePlaneSizing {
            index: planes.len(),
            name: "tcp_state",
            words: tcp_words,
            bytes: checked_product(tcp_words, WORD_BYTES, "TCP state plane bytes")?,
        });
    }
    let total_device_bytes = planes.iter().try_fold(0_usize, |total, plane| {
        total
            .checked_add(plane.bytes)
            .ok_or_else(|| sizing_error("total device bytes overflow usize"))
    })?;

    let heap_arena_bytes = event_arena_bytes(fallback_fel_event_slots, node_count)?;
    let stream_arena_bytes = checked_product(stream_state_words, WORD_BYTES, "stream state bytes")?
        .checked_add(checked_product(
            checked_product(stream_record_slots, EVENT_WORDS, "stream record words")?,
            WORD_BYTES,
            "stream record bytes",
        )?)
        .ok_or_else(|| sizing_error("stream arena bytes overflow usize"))?;
    let legacy_heap_arena_bytes = event_arena_bytes(legacy_heap_event_slots, node_count)?;

    Ok(DeviceSizingReport {
        planes,
        total_device_bytes,
        event_arenas: DeviceEventArenaSizing {
            legacy_heap_event_slots,
            fallback_heap_event_slots: fallback_fel_event_slots,
            channel_stream_event_slots,
            service_stream_event_slots,
            generator_stream_event_slots,
            heap_arena_bytes,
            stream_arena_bytes,
            legacy_heap_arena_bytes,
        },
    })
}

struct CapacityContext {
    packet_counts: Vec<usize>,
    feedback_counts: Vec<usize>,
    source_queue_bounds: Vec<usize>,
    generator_intervals: Vec<Vec<u64>>,
    minimum_data_sizes: Vec<u64>,
    minimum_feedback_sizes: Vec<u64>,
    lookahead: Option<u64>,
}

impl CapacityContext {
    fn new(
        image: &SimulationImage,
        packet_counts: &[usize],
        feedback_counts: &[usize],
        lookahead: Option<u64>,
    ) -> Result<Self, DeviceSizingError> {
        let flow_count = image.flows.len();
        let mut generator_intervals = vec![Vec::new(); flow_count];
        let mut minimum_data_sizes = vec![u64::MAX; flow_count];
        let mut minimum_feedback_sizes = vec![u64::MAX; flow_count];
        for state in &image.host_states {
            for generator in &state.generators {
                let index = generator.flow.0 as usize;
                match generator.kind {
                    FlowGeneratorKind::Constant(constant) => {
                        minimum_data_sizes[index] =
                            minimum_data_sizes[index].min(constant.packet_size_bytes);
                        if generator.next_emission.status == GeneratorStatus::Scheduled {
                            generator_intervals[index].push(constant.interval_ns);
                        }
                    }
                    FlowGeneratorKind::Tcp(tcp) => {
                        minimum_data_sizes[index] = minimum_data_sizes[index].min(tcp.mss_bytes);
                        minimum_feedback_sizes[index] =
                            minimum_feedback_sizes[index].min(tcp.ack_size_bytes);
                    }
                    FlowGeneratorKind::Rate(rate) => {
                        minimum_data_sizes[index] =
                            minimum_data_sizes[index].min(rate.packet_size_bytes);
                        if matches!(
                            generator.next_emission.status,
                            GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                        ) {
                            generator_intervals[index].push(rate.pacing_interval_ns);
                        }
                    }
                    FlowGeneratorKind::Collective(collective) => {
                        let remaining = collective
                            .chunk_bytes
                            .saturating_sub(generator.bytes_emitted);
                        let tail = remaining % collective.packet_size_bytes;
                        let minimum = if tail == 0 {
                            collective.packet_size_bytes.min(remaining.max(1))
                        } else {
                            tail
                        };
                        minimum_data_sizes[index] = minimum_data_sizes[index].min(minimum);
                        if matches!(
                            generator.next_emission.status,
                            GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                        ) {
                            generator_intervals[index].push(collective.interval_ns);
                        }
                    }
                    FlowGeneratorKind::Dcqcn(dcqcn) => {
                        minimum_data_sizes[index] =
                            minimum_data_sizes[index].min(dcqcn.rate.packet_size_bytes);
                        minimum_feedback_sizes[index] =
                            minimum_feedback_sizes[index].min(dcqcn.cnp_size_bytes);
                        if matches!(
                            generator.next_emission.status,
                            GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                        ) {
                            generator_intervals[index].push(dcqcn.rate.pacing_interval_ns);
                        }
                    }
                }
            }
        }
        for packet in &image.initial_packets {
            let minimum = match packet.kind {
                PacketKind::Data | PacketKind::TcpData(_) => {
                    &mut minimum_data_sizes[packet.flow.0 as usize]
                }
                PacketKind::Feedback
                | PacketKind::TcpAck(_)
                | PacketKind::Pfc(_)
                | PacketKind::DcqcnCnp(_) => &mut minimum_feedback_sizes[packet.flow.0 as usize],
                PacketKind::DcqcnControlTimer => continue,
            };
            *minimum = (*minimum).min(packet.size_bytes);
        }
        for minimum in minimum_data_sizes
            .iter_mut()
            .chain(&mut minimum_feedback_sizes)
        {
            if *minimum == u64::MAX {
                *minimum = 1;
            }
        }
        let source_queue_bounds = source_queue_bounds(image, packet_counts)?;
        Ok(Self {
            packet_counts: packet_counts.to_vec(),
            feedback_counts: feedback_counts.to_vec(),
            source_queue_bounds,
            generator_intervals,
            minimum_data_sizes,
            minimum_feedback_sizes,
            lookahead,
        })
    }
}

fn flow_packet_counts(image: &SimulationImage) -> Result<Vec<usize>, DeviceSizingError> {
    let mut counts = vec![0_usize; image.flows.len()];
    for packet in &image.initial_packets {
        counts[packet.flow.0 as usize] = counts[packet.flow.0 as usize].saturating_add(1);
    }
    for state in &image.host_states {
        for generator in &state.generators {
            let index = generator.flow.0 as usize;
            match generator.kind {
                FlowGeneratorKind::Constant(constant) => {
                    if generator.next_emission.status != GeneratorStatus::Scheduled {
                        continue;
                    }
                    let termination_count = match constant.termination {
                        crate::GeneratorTermination::Bytes(bytes) => {
                            if generator.bytes_emitted >= bytes {
                                0
                            } else {
                                (bytes - generator.bytes_emitted)
                                    .div_ceil(constant.packet_size_bytes)
                            }
                        }
                        crate::GeneratorTermination::DurationNs(duration) => {
                            let end = constant
                                .first_departure_ns
                                .checked_add(duration)
                                .ok_or_else(|| {
                                    sizing_error("generator duration endpoint overflows")
                                })?;
                            if generator.next_emission.departure_time_ns >= end {
                                0
                            } else {
                                1 + (end - 1 - generator.next_emission.departure_time_ns)
                                    / constant.interval_ns
                            }
                        }
                    };
                    let stop_count =
                        if generator.next_emission.departure_time_ns > image.stop_time_ns {
                            0
                        } else {
                            1 + (image.stop_time_ns - generator.next_emission.departure_time_ns)
                                / constant.interval_ns
                        };
                    let future = termination_count.min(stop_count) as usize;
                    counts[index] = counts[index].saturating_add(future.saturating_sub(1));
                }
                FlowGeneratorKind::Tcp(tcp) => {
                    if matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Finished | GeneratorStatus::Stopped
                    ) || tcp.highest_ack >= tcp.total_bytes
                    {
                        continue;
                    }
                    // Closed-loop send plans can fragment one nominal MSS at a congestion-window
                    // boundary and can retransmit. Twice the remaining nominal segment count plus
                    // preloaded ACK triggers is the practical bounded device arena reservation;
                    // a valid trajectory that exceeds it reports an exact capacity error.
                    let bound = tcp_data_attempt_bound(image, generator, tcp);
                    // Every delivered data attempt creates one cumulative ACK attempt.
                    counts[index] = counts[index].saturating_add(bound.saturating_mul(2));
                }
                FlowGeneratorKind::Rate(rate) => {
                    let work = rate_device_work(image, generator, rate)?;
                    let owns_timer_token = image.initial_events.iter().any(|event| {
                        event.kind == EventKind::PacingTimer
                            && event.target == image.flows[index].source
                            && event.payload == generator.next_emission.payload
                            && event.key.time_ns == generator.next_emission.departure_time_ns
                    });
                    if owns_timer_token {
                        counts[index] = counts[index].saturating_sub(1);
                    }
                    counts[index] = counts[index].saturating_add(work.packets);
                }
                FlowGeneratorKind::Collective(collective) => {
                    if matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Finished | GeneratorStatus::Stopped
                    ) {
                        continue;
                    }
                    let remaining = collective
                        .chunk_bytes
                        .saturating_sub(generator.bytes_emitted);
                    let packets = remaining.div_ceil(collective.packet_size_bytes) as usize;
                    let resident =
                        usize::from(generator.next_emission.status == GeneratorStatus::Scheduled);
                    counts[index] = counts[index].saturating_add(packets.saturating_sub(resident));
                }
                FlowGeneratorKind::Dcqcn(dcqcn) => {
                    if !matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                    ) || generator.bytes_emitted >= dcqcn.rate.total_bytes
                    {
                        continue;
                    }
                    let remaining = dcqcn.rate.total_bytes - generator.bytes_emitted;
                    let count = remaining.div_ceil(dcqcn.rate.packet_size_bytes);
                    counts[index] =
                        counts[index].saturating_add(usize::try_from(count).unwrap_or(usize::MAX));
                }
            }
        }
    }
    Ok(counts)
}

fn feedback_packet_counts(image: &SimulationImage, packet_counts: &[usize]) -> Vec<usize> {
    let mut counts = image.initial_packets.iter().fold(
        vec![0_usize; image.flows.len()],
        |mut counts, packet| {
            if packet.kind.is_feedback() {
                counts[packet.flow.0 as usize] = counts[packet.flow.0 as usize].saturating_add(1);
            }
            counts
        },
    );
    for state in &image.host_states {
        for generator in &state.generators {
            if let FlowGeneratorKind::Tcp(tcp) = generator.kind {
                let index = generator.flow.0 as usize;
                let attempts = tcp_data_attempt_bound(image, generator, tcp);
                counts[index] = counts[index].saturating_add(attempts);
                counts[index] = counts[index].min(packet_counts[index]);
            }
        }
    }
    counts
}

fn tcp_data_attempt_bound(
    image: &SimulationImage,
    generator: &crate::FlowGeneratorState,
    tcp: crate::TcpGenerator,
) -> usize {
    if matches!(
        generator.next_emission.status,
        GeneratorStatus::Finished | GeneratorStatus::Stopped
    ) || tcp.highest_ack >= tcp.total_bytes
    {
        return 0;
    }
    let remaining = tcp.total_bytes - tcp.highest_ack;
    let nominal = remaining.div_ceil(tcp.mss_bytes.max(1));
    let preloaded_acks = image
        .initial_packets
        .iter()
        .filter(|packet| {
            packet.flow == generator.flow && matches!(packet.kind, PacketKind::TcpAck(_))
        })
        .count() as u64;
    usize::try_from(
        nominal
            .saturating_mul(2)
            .saturating_add(preloaded_acks)
            .saturating_add(2),
    )
    .unwrap_or(usize::MAX)
}

fn packed_tcp_state_words(
    image: &SimulationImage,
    packet_counts: &[usize],
) -> Result<usize, DeviceSizingError> {
    const RECEIVER_WORDS: usize = 4;
    const RANGE_WORDS: usize = 2;
    let flow_count = image.flows.len().max(1);
    let tcp_record_slots = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Tcp(_)))
        .try_fold(0_usize, |total, generator| {
            total
                .checked_add(packet_counts[generator.flow.0 as usize])
                .ok_or_else(|| sizing_error("TCP auxiliary record slots overflow usize"))
        })?
        .max(1);
    [
        checked_product(flow_count, RECEIVER_WORDS, "TCP receiver rows")?,
        checked_product(flow_count, ARENA_META_WORDS, "TCP range metadata")?,
        checked_product(tcp_record_slots, RANGE_WORDS, "TCP receive ranges")?,
        checked_product(flow_count, ARENA_META_WORDS, "TCP ledger metadata")?,
        checked_product(tcp_record_slots, EVENT_WORDS, "TCP ledger records")?,
        checked_product(
            image.nodes.len().max(1),
            ARENA_META_WORDS,
            "TCP transition metadata",
        )?,
    ]
    .into_iter()
    .try_fold(0_usize, |total, words| {
        total
            .checked_add(words)
            .ok_or_else(|| sizing_error("TCP state plane overflows usize"))
    })
}

fn source_queue_bounds(
    image: &SimulationImage,
    packet_counts: &[usize],
) -> Result<Vec<usize>, DeviceSizingError> {
    let flow_count = image.flows.len();
    let mut flows_per_source = vec![0_usize; image.nodes.len()];
    for flow in &image.flows {
        flows_per_source[flow.source.0 as usize] =
            flows_per_source[flow.source.0 as usize].saturating_add(1);
    }
    let mut initial_data_count = vec![0_usize; flow_count];
    let mut initial_data_payload = vec![None; flow_count];
    let mut initial_data_size = vec![0_u64; flow_count];
    let mut payload_to_flow = BTreeMap::new();
    for packet in &image.initial_packets {
        if packet.kind.is_data() {
            let index = packet.flow.0 as usize;
            initial_data_count[index] = initial_data_count[index].saturating_add(1);
            initial_data_payload[index].get_or_insert(packet.id);
            initial_data_size[index] = packet.size_bytes;
            payload_to_flow.insert(packet.id, index);
        }
    }
    let mut matching_arrivals = vec![0_usize; flow_count];
    for event in &image.initial_events {
        let Some(&flow_index) = payload_to_flow.get(&event.payload) else {
            continue;
        };
        let flow = &image.flows[flow_index];
        let source = &image.nodes[flow.source.0 as usize];
        if source.kind != NodeKind::Host {
            continue;
        }
        let state = &image.host_states[source.state_slot as usize];
        let [generator] = state.generators.as_slice() else {
            continue;
        };
        if event.target == flow.source
            && event.kind == EventKind::PacketArrival
            && event.payload == generator.next_emission.payload
            && event.key.time_ns == generator.next_emission.departure_time_ns
        {
            matching_arrivals[flow_index] = matching_arrivals[flow_index].saturating_add(1);
        }
    }

    image
        .flows
        .iter()
        .enumerate()
        .map(|(flow_index, flow)| {
            let packet_count = packet_counts[flow_index];
            if packet_count == 0 || flows_per_source[flow.source.0 as usize] != 1 {
                return Ok(packet_count);
            }
            let source = &image.nodes[flow.source.0 as usize];
            if source.kind != NodeKind::Host {
                return Ok(packet_count);
            }
            let state = &image.host_states[source.state_slot as usize];
            let [generator] = state.generators.as_slice() else {
                return Ok(packet_count);
            };
            if matches!(generator.kind, FlowGeneratorKind::Tcp(_)) {
                return Ok(packet_count);
            }
            if generator.flow.0 as usize != flow_index
                || generator.next_emission.status != GeneratorStatus::Scheduled
                || generator.packets_emitted != 0
                || generator.bytes_emitted != 0
                || !state.queue.is_empty()
                || state.in_service.is_some()
                || state.tx_ready_pending
                || initial_data_count[flow_index] != 1
                || initial_data_payload[flow_index] != Some(generator.next_emission.payload)
                || matching_arrivals[flow_index] != 1
            {
                return Ok(packet_count);
            }
            let Some(first_link) = flow.route.first().copied() else {
                return Ok(packet_count);
            };
            if first_link != state.egress_link {
                return Ok(packet_count);
            }
            let FlowGeneratorKind::Constant(constant) = generator.kind else {
                return Ok(packet_count);
            };
            if initial_data_size[flow_index] != constant.packet_size_bytes {
                return Ok(packet_count);
            }
            let link = image.links[first_link.0 as usize];
            let serialization = serialization_time_ns(constant.packet_size_bytes, link.rate_bps)
                .map_err(|error| sizing_error(format!("source serialization failed: {error}")))?;
            let paced = if constant.interval_ns >= serialization {
                packet_count.min(1)
            } else {
                packet_count
            };
            Ok(if paced == packet_count {
                packet_count
            } else {
                state.queue.len().saturating_add(paced).min(packet_count)
            })
        })
        .collect()
}

fn add_flow_route_capacities(
    image: &SimulationImage,
    context: &CapacityContext,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    fel_capacities: &mut [usize],
    queue_capacities: &mut [usize],
) {
    if packet_count == 0 {
        return;
    }
    let flow = &image.flows[flow_index];
    let (route, terminal) = match packet_kind {
        PacketKind::Data | PacketKind::TcpData(_) => (flow.route.as_slice(), flow.target),
        PacketKind::Feedback
        | PacketKind::TcpAck(_)
        | PacketKind::Pfc(_)
        | PacketKind::DcqcnCnp(_) => (flow.reverse_route.as_slice(), flow.source),
        PacketKind::DcqcnControlTimer => return,
    };
    for index in 0..route.len() {
        let target = route
            .get(index + 1)
            .map(|next| image.links[next.0 as usize].source)
            .unwrap_or(terminal);
        let target_slot = target.0 as usize;
        let burst = flow_link_fel_bound(
            image,
            context,
            flow_index,
            packet_count,
            packet_kind,
            route[index],
        );
        fel_capacities[target_slot] = fel_capacities[target_slot].saturating_add(burst);
        if image.nodes[target_slot].kind == NodeKind::Switch {
            queue_capacities[target_slot] =
                queue_capacities[target_slot].saturating_add(packet_count);
        }
    }
}

fn flow_link_serialization_ns(
    image: &SimulationImage,
    context: &CapacityContext,
    flow_index: usize,
    packet_kind: PacketKind,
    link_id: LinkId,
) -> u64 {
    let minimum_size = match packet_kind {
        PacketKind::Data | PacketKind::TcpData(_) => context.minimum_data_sizes[flow_index],
        PacketKind::Feedback
        | PacketKind::TcpAck(_)
        | PacketKind::Pfc(_)
        | PacketKind::DcqcnCnp(_) => context.minimum_feedback_sizes[flow_index],
        PacketKind::DcqcnControlTimer => return 0,
    };
    serialization_time_ns(minimum_size, image.links[link_id.0 as usize].rate_bps)
        .expect("lowered GPU image has positive finite serialization intervals")
}

fn flow_link_round_bound(
    image: &SimulationImage,
    context: &CapacityContext,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    link_id: LinkId,
) -> usize {
    if packet_count == 0 {
        return 0;
    }
    let link = image.links[link_id.0 as usize];
    let source = image.nodes[link.source.0 as usize];
    let (current_queue, queue_capacity) = match source.kind {
        NodeKind::Host => {
            let state = &image.host_states[source.state_slot as usize];
            let capacity =
                if packet_kind == PacketKind::Data && source.id == image.flows[flow_index].source {
                    context.source_queue_bounds[flow_index]
                } else {
                    packet_count
                };
            (state.queue.len(), capacity)
        }
        NodeKind::Switch => {
            let queue = image.switch_states[source.state_slot as usize]
                .queues
                .first();
            let current = queue.map_or(0, |queue| queue.queue.len());
            let capacity = queue
                .map(|queue| queue.queue_capacity_packets)
                .filter(|capacity| *capacity != 0)
                .and_then(|capacity| usize::try_from(capacity).ok())
                .unwrap_or(packet_count);
            (current, capacity)
        }
    };
    let queue_bound = current_queue.max(queue_capacity);
    let serialization =
        flow_link_serialization_ns(image, context, flow_index, packet_kind, link_id);
    let service_burst = context.lookahead.map_or(packet_count, |lookahead| {
        usize::try_from(lookahead.div_ceil(serialization)).unwrap_or(usize::MAX)
    });
    let generator_burst =
        if packet_kind == PacketKind::Data && link.source == image.flows[flow_index].source {
            let closed_loop = image.host_states.iter().any(|state| {
                state.generators.iter().any(|generator| {
                    generator.flow.0 as usize == flow_index
                        && matches!(generator.kind, FlowGeneratorKind::Tcp(_))
                })
            });
            if closed_loop {
                packet_count
            } else {
                context.generator_intervals[flow_index]
                    .iter()
                    .map(|interval| {
                        context.lookahead.map_or(packet_count, |lookahead| {
                            packet_count.min(
                                usize::try_from(lookahead / interval)
                                    .unwrap_or(usize::MAX)
                                    .saturating_add(1),
                            )
                        })
                    })
                    .fold(0_usize, usize::saturating_add)
            }
        } else {
            0
        };
    packet_count.min(
        queue_bound
            .saturating_add(1)
            .saturating_add(service_burst)
            .saturating_add(generator_burst),
    )
}

fn flow_link_fel_bound(
    image: &SimulationImage,
    context: &CapacityContext,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    link_id: LinkId,
) -> usize {
    if packet_count == 0 {
        return 0;
    }
    let link = image.links[link_id.0 as usize];
    let serialization =
        flow_link_serialization_ns(image, context, flow_index, packet_kind, link_id);
    let in_flight =
        usize::try_from(link.propagation_ns.div_ceil(serialization)).unwrap_or(usize::MAX);
    packet_count.min(
        flow_link_round_bound(
            image,
            context,
            flow_index,
            packet_count,
            packet_kind,
            link_id,
        )
        .saturating_add(in_flight),
    )
}

fn derived_remote_capacity(image: &SimulationImage, context: &CapacityContext) -> usize {
    image
        .flows
        .iter()
        .enumerate()
        .map(|(index, flow)| {
            let feedback_count = context.feedback_counts[index];
            let data_count = context.packet_counts[index].saturating_sub(feedback_count);
            flow.route
                .iter()
                .map(|link| {
                    flow_link_round_bound(
                        image,
                        context,
                        index,
                        data_count,
                        PacketKind::Data,
                        *link,
                    )
                })
                .chain(flow.reverse_route.iter().map(|link| {
                    flow_link_round_bound(
                        image,
                        context,
                        index,
                        feedback_count,
                        PacketKind::Feedback,
                        *link,
                    )
                }))
                .fold(0_usize, usize::saturating_add)
        })
        .fold(image.nodes.len().saturating_mul(2), usize::saturating_add)
}

fn derived_remote_capacities(image: &SimulationImage, context: &CapacityContext) -> Vec<usize> {
    let mut capacities = vec![2_usize; image.nodes.len()];
    for (index, flow) in image.flows.iter().enumerate() {
        let feedback_count = context.feedback_counts[index];
        let data_count = context.packet_counts[index].saturating_sub(feedback_count);
        for (route, packet_count, packet_kind) in [
            (flow.route.as_slice(), data_count, PacketKind::Data),
            (
                flow.reverse_route.as_slice(),
                feedback_count,
                PacketKind::Feedback,
            ),
        ] {
            for link_id in route {
                let producer = image.links[link_id.0 as usize].source.0 as usize;
                capacities[producer] = capacities[producer].saturating_add(flow_link_round_bound(
                    image,
                    context,
                    index,
                    packet_count,
                    packet_kind,
                    *link_id,
                ));
            }
        }
    }
    capacities
}

fn derived_channel_stream_capacities(
    image: &SimulationImage,
    context: &CapacityContext,
) -> Result<Vec<usize>, DeviceSizingError> {
    let channels = image
        .channels
        .iter()
        .enumerate()
        .map(|(index, channel)| ((channel.link, channel.target), index))
        .collect::<BTreeMap<_, _>>();
    let mut packet_counts = vec![0_usize; image.channels.len()];
    let mut minimum_serialization = vec![None::<u64>; image.channels.len()];
    for (flow_index, flow) in image.flows.iter().enumerate() {
        let feedback_count = context.feedback_counts[flow_index];
        let data_count = context.packet_counts[flow_index].saturating_sub(feedback_count);
        for (route, terminal, packet_count, packet_kind) in [
            (
                flow.route.as_slice(),
                flow.target,
                data_count,
                PacketKind::Data,
            ),
            (
                flow.reverse_route.as_slice(),
                flow.source,
                feedback_count,
                PacketKind::Feedback,
            ),
        ] {
            if packet_count == 0 {
                continue;
            }
            for (step, link_id) in route.iter().enumerate() {
                let target = route
                    .get(step + 1)
                    .map_or(terminal, |next| image.links[next.0 as usize].source);
                let channel = channels.get(&(*link_id, target)).copied().ok_or_else(|| {
                    sizing_error(format!(
                        "no stream classification for link {link_id:?} to node {target:?}"
                    ))
                })?;
                packet_counts[channel] = packet_counts[channel].saturating_add(packet_count);
                let serialization =
                    flow_link_serialization_ns(image, context, flow_index, packet_kind, *link_id);
                minimum_serialization[channel] = Some(
                    minimum_serialization[channel]
                        .map_or(serialization, |current| current.min(serialization)),
                );
            }
        }
    }
    Ok(image
        .channels
        .iter()
        .enumerate()
        .map(|(channel, descriptor)| {
            let packet_count = packet_counts[channel];
            let Some(serialization) = minimum_serialization[channel] else {
                return 2;
            };
            let horizon_emissions = context.lookahead.map_or(packet_count, |horizon| {
                usize::try_from(horizon.div_ceil(serialization))
                    .unwrap_or(usize::MAX)
                    .saturating_add(1)
            });
            let propagation_residency = usize::try_from(
                image.links[descriptor.link.0 as usize]
                    .propagation_ns
                    .div_ceil(serialization),
            )
            .unwrap_or(usize::MAX);
            packet_count
                .min(horizon_emissions.saturating_add(propagation_residency))
                .saturating_add(2)
        })
        .collect())
}

fn stream_state_words(
    image: &SimulationImage,
    remote_staging_slots: usize,
) -> Result<usize, DeviceSizingError> {
    let node_count = image.nodes.len();
    let channel_count = image.channels.len();
    let service_stream_base = channel_count;
    let generator_stream_base = service_stream_base
        .checked_add(node_count)
        .ok_or_else(|| sizing_error("service stream count overflows usize"))?;
    let stream_count = generator_stream_base
        .checked_add(image.flows.len())
        .ok_or_else(|| sizing_error("generator stream count overflows usize"))?;
    let mut lp_streams = vec![BTreeSet::new(); node_count];
    for (channel, descriptor) in image.channels.iter().enumerate() {
        lp_streams[descriptor.target.0 as usize].insert(channel);
    }
    for (node, streams) in lp_streams.iter_mut().enumerate() {
        streams.insert(service_stream_base + node);
    }
    for state in &image.host_states {
        for generator in &state.generators {
            lp_streams[image.flows[generator.flow.0 as usize].source.0 as usize]
                .insert(generator_stream_base + generator.flow.0 as usize);
        }
    }
    let lp_stream_id_words = lp_streams.iter().try_fold(0_usize, |total, streams| {
        total
            .checked_add(streams.len())
            .ok_or_else(|| sizing_error("LP stream-list size overflows usize"))
    })?;
    let lp_active_id_words = lp_streams.iter().try_fold(0_usize, |total, streams| {
        total
            .checked_add(
                streams
                    .len()
                    .saturating_add(1)
                    .saturating_mul(ACTIVE_STREAM_ENTRY_WORDS),
            )
            .ok_or_else(|| sizing_error("active stream-list size overflows usize"))
    })?;

    let mut outbound_targets = vec![BTreeSet::new(); node_count];
    for descriptor in &image.channels {
        if !outbound_targets[descriptor.source.0 as usize].insert(descriptor.target) {
            return Err(sizing_error(
                "stream classification requires one channel per source-target LP pair",
            ));
        }
    }

    [
        checked_product(stream_count, ARENA_META_WORDS, "stream metadata")?,
        checked_product(node_count, LP_STREAM_META_WORDS, "LP stream metadata")?,
        lp_stream_id_words,
        lp_active_id_words,
        checked_product(node_count, OUTBOUND_META_WORDS, "outbound metadata")?,
        checked_product(
            channel_count,
            OUTBOUND_ENTRY_WORDS,
            "outbound channel entries",
        )?,
        checked_product(channel_count, CHANNEL_BATCH_WORDS, "channel batches")?,
        remote_staging_slots,
        channel_count,
    ]
    .into_iter()
    .try_fold(0_usize, |total, words| {
        total
            .checked_add(words)
            .ok_or_else(|| sizing_error("stream state size overflows usize"))
    })
}

fn inbound_producer_words(image: &SimulationImage) -> usize {
    let mut inbound = vec![BTreeSet::new(); image.nodes.len()];
    for flow in &image.flows {
        for (route, terminal) in [
            (flow.route.as_slice(), flow.target),
            (flow.reverse_route.as_slice(), flow.source),
        ] {
            for (step, link_id) in route.iter().enumerate() {
                let producer = image.links[link_id.0 as usize].source.0;
                let target = route
                    .get(step + 1)
                    .map_or(terminal, |next| image.links[next.0 as usize].source)
                    .0 as usize;
                inbound[target].insert(producer);
            }
        }
    }
    inbound.iter().map(BTreeSet::len).sum()
}

fn event_arena_bytes(record_slots: usize, node_count: usize) -> Result<usize, DeviceSizingError> {
    record_slots
        .checked_mul(EVENT_WORDS)
        .and_then(|words| {
            node_count
                .checked_mul(ARENA_META_WORDS)
                .and_then(|meta_words| words.checked_add(meta_words))
        })
        .and_then(|words| words.checked_mul(WORD_BYTES))
        .ok_or_else(|| sizing_error("event arena byte size overflows usize"))
}

fn checked_sum(values: &[usize], label: &str) -> Result<usize, DeviceSizingError> {
    values.iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| sizing_error(format!("{label} overflow usize")))
    })
}

fn checked_product(left: usize, right: usize, label: &str) -> Result<usize, DeviceSizingError> {
    left.checked_mul(right)
        .ok_or_else(|| sizing_error(format!("{label} overflows usize")))
}

fn sizing_error(message: impl Into<String>) -> DeviceSizingError {
    DeviceSizingError(message.into())
}

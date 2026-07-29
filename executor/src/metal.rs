//! Correctness-first production Metal executor.
//!
//! The backend keeps the safe-horizon round loop resident on the device within bounded encoding
//! waves. A deterministic 1,024-lane reduction publishes each horizon and a serial controller
//! kernel performs the correctness path: stable active-LP compaction, per-LP chronological drains,
//! real transitions, local FEL insertion, and boundary-only remote exchange. The host synchronizes
//! only at wave boundaries, and every such synchronization is reported in [`MetalRun`]. Role-split
//! parallel transition kernels are intentionally deferred to the optimization milestone.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::NSString;
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
    MTLComputeCommandEncoder, MTLComputePipelineState, MTLCreateSystemDefaultDevice, MTLDevice,
    MTLDispatchType, MTLLibrary, MTLResourceOptions, MTLSize,
};

use crate::{
    ArrivalDisposition, Backend, Event, EventKey, EventKind, FlowGeneratorKind, GeneratorStatus,
    GeneratorTermination, NodeId, NodeKind, ObservationMode, PacketArrivalObservation,
    PacketDeparture, PacketDescriptor, PacketKind, PayloadId, RunResult, RunSummary,
    SimulationImage, validate,
};

const LANES: usize = 1_024;
const EVENT_WORDS: usize = 11;
const NODE_WORDS: usize = 11;
const GENERATOR_WORDS: usize = 16;
const FLOW_WORDS: usize = 6;
const LINK_WORDS: usize = 4;
const ARENA_META_WORDS: usize = 4;
const SUMMARY_COUNTERS: usize = 12;
const OBSERVED_WORDS: usize = 4;
const DEPARTURE_WORDS: usize = 9;
const ARRIVAL_WORDS: usize = 10;
// The retained k32 profile averages about 1,300 transitions per round. 4,096 keeps ordinary
// rounds single-launch while putting a finite ceiling on pathological serial device work.
const DEFAULT_TRANSITIONS_PER_DISPATCH: usize = 4_096;
// The retained T13 direct-Metal benchmark encodes two-dispatch round pairs at about 0.4 us/pair on
// the development Apple system. Metal practice favors a small number of substantial command
// buffers:
// 16,384 pairs is the proven long-resident setting (~6.7 ms host encoding), while 65,536 pairs
// bounds one pre-commit wave to four default-sized buffers (~27 ms). MAX_COMMAND_BUFFERS remains
// the absolute buffer-count bound when callers deliberately request shorter buffers.
const MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER: usize = 16_384;
const MAX_ENCODED_PAIRS_PER_WAVE: usize = 65_536;
const DEFAULT_ROUNDS_PER_COMMAND_BUFFER: usize = MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER;
const MAX_COMMAND_BUFFERS: usize = 64;
const HORIZON_THREADGROUP_BYTES: usize =
    LANES * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
const NONE: u64 = u64::MAX;

const CONTROL_ERROR: usize = 0;
const CONTROL_ERROR_ARENA: usize = 1;
const CONTROL_ERROR_NODE: usize = 2;
const CONTROL_ERROR_CAPACITY: usize = 3;
const CONTROL_DONE: usize = 4;
const CONTROL_RUN_END_LO: usize = 7;
const CONTROL_RUN_END_HI: usize = 8;
const CONTROL_ROUNDS: usize = 9;
const CONTROL_TRANSITIONS: usize = 10;
const CONTROL_OBSERVED: usize = 12;
const CONTROL_DEPARTURES: usize = 13;
const CONTROL_ARRIVALS: usize = 14;
const CONTROL_CONTINUATION: usize = 17;
const CONTROL_RELAUNCHES: usize = 18;
const CONTROL_WORDS: usize = 19;

type RawMetalBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type MetalPipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

/// Bounded device arena reported by a production Metal capacity fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetalArena {
    Fel,
    Queue,
    Outbox,
    Worklist,
    ObservedPackets,
    Departures,
    Arrivals,
}

impl fmt::Display for MetalArena {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Fel => "FEL",
            Self::Queue => "queue",
            Self::Outbox => "remote outbox",
            Self::Worklist => "active worklist",
            Self::ObservedPackets => "observed-packet log",
            Self::Departures => "departure log",
            Self::Arrivals => "arrival log",
        })
    }
}

/// Production Metal execution failure. No partial [`RunResult`] is returned.
#[derive(Debug, Eq, PartialEq)]
pub enum MetalError {
    Validation(String),
    Unavailable(String),
    CapacityExceeded {
        arena: MetalArena,
        node: Option<NodeId>,
        capacity: usize,
    },
    TransitionLimitExceeded {
        node: NodeId,
        capacity: usize,
    },
    RoundLimitExceeded {
        capacity: usize,
    },
    DeviceExecution {
        code: u64,
        node: Option<NodeId>,
    },
}

impl fmt::Display for MetalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "invalid Metal image: {message}"),
            Self::Unavailable(message) => write!(formatter, "Metal backend unavailable: {message}"),
            Self::CapacityExceeded {
                arena,
                node,
                capacity,
            } => {
                if let Some(node) = node {
                    write!(
                        formatter,
                        "Metal {arena} capacity of {capacity} records exceeded at LP {node:?}"
                    )
                } else {
                    write!(
                        formatter,
                        "Metal {arena} capacity of {capacity} records exceeded"
                    )
                }
            }
            Self::TransitionLimitExceeded { node, capacity } => write!(
                formatter,
                "Metal LP {node:?} exceeded the per-round transition continuation capacity of \
                 {capacity}"
            ),
            Self::RoundLimitExceeded { capacity } => write!(
                formatter,
                "Metal bounded-wave encoding exhausted its capacity of {capacity} rounds before \
                 termination"
            ),
            Self::DeviceExecution { code, node } => {
                write!(
                    formatter,
                    "Metal transition kernel reported semantic error {code}"
                )?;
                if let Some(node) = node {
                    write!(formatter, " at LP {node:?}")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for MetalError {}

/// Physical capacity and bounded-wave encoding policy for one Metal run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetalConfig {
    /// Optional exact per-LP FEL capacity override. Raising the derived default consumes more
    /// device memory; lowering it retains an explicit device capacity fault on overflow.
    pub max_fel_events_per_lp: Option<usize>,
    /// Optional exact per-LP packet-queue capacity override, with the same memory/fault tradeoff.
    pub max_queue_packets_per_lp: Option<usize>,
    /// Optional bound for all remote children produced in one round.
    pub max_outbox_events: Option<usize>,
    /// Optional bound applied independently to full-mode observed, departure, and arrival logs.
    pub max_observations: Option<usize>,
    /// Physical transition budget for one serial device dispatch. The historical field name is
    /// retained for API compatibility; exhausting the budget relaunches the same semantic round
    /// with its horizon, worklist, FELs, and outbox preserved.
    pub max_transitions_per_lp_per_round: usize,
    /// Requested round pairs per serial command buffer. Production execution clamps this to
    /// 16,384 independently of caller configuration.
    pub rounds_per_command_buffer: usize,
    /// Optional hard cap overriding the conservative encoded round bound.
    pub max_rounds: Option<usize>,
}

impl Default for MetalConfig {
    fn default() -> Self {
        Self {
            max_fel_events_per_lp: None,
            max_queue_packets_per_lp: None,
            max_outbox_events: None,
            max_observations: None,
            max_transitions_per_lp_per_round: DEFAULT_TRANSITIONS_PER_DISPATCH,
            rounds_per_command_buffer: DEFAULT_ROUNDS_PER_COMMAND_BUFFER,
            max_rounds: None,
        }
    }
}

/// Complete production Metal result and bounded-wave submission diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalRun {
    pub result: RunResult,
    pub rounds: u64,
    pub transitions: u64,
    /// Device-resident `days_round` continuation dispatches beyond the first launch per round.
    pub continuation_relaunches: u64,
    /// `waitUntilCompleted()` calls, exactly one after each committed bounded wave, including the
    /// final completion wait.
    pub wave_boundary_syncs: u64,
    /// Wave-boundary waits after which the next wave resumed the same semantic round.
    pub mid_round_wave_boundary_syncs: u64,
    /// Host time spent encoding and committing bounded command-buffer waves.
    pub host_encode_submit_ns: u64,
    /// Sum of Metal command-buffer GPU timestamp intervals.
    pub device_ns: u64,
    /// Wall time from the first encode through final device completion. Image planning, pipeline
    /// creation, and final Rust result normalization are intentionally outside this interval.
    pub wall_ns: u64,
}

/// Runs the production Metal executor through the inclusive scenario stop.
pub fn run_metal(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: MetalConfig,
) -> Result<MetalRun, MetalError> {
    run_metal_with_observations(
        image,
        exclusive_horizon_ns,
        config,
        ObservationMode::Summary,
    )
}

/// Runs the production Metal executor with explicit observation retention.
pub fn run_metal_with_observations(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: MetalConfig,
    observation_mode: ObservationMode,
) -> Result<MetalRun, MetalError> {
    MetalExecutor::new()?.run_with_observations(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
    )
}

/// Reusable production Metal pipelines and command queue.
///
/// Each run allocates fresh state buffers, so an explicit device fault cannot contaminate a
/// subsequent run through the same executor.
pub struct MetalExecutor {
    direct: DirectMetal,
}

impl MetalExecutor {
    pub fn new() -> Result<Self, MetalError> {
        Ok(Self {
            direct: DirectMetal::new()?,
        })
    }

    pub fn run(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
    ) -> Result<MetalRun, MetalError> {
        self.run_with_observations(
            image,
            exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        )
    }

    pub fn run_with_observations(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
    ) -> Result<MetalRun, MetalError> {
        validate(image, Backend::Metal)
            .map_err(|error| MetalError::Validation(error.to_string()))?;
        validate_config(config)?;

        let plan = MetalPlan::new(image, exclusive_horizon_ns, config, observation_mode)?;
        let buffers = MetalBuffers::new(&self.direct.device, plan)?;
        let timing = self.direct.run(&buffers, config)?;
        buffers.finish(image, observation_mode, timing)
    }
}

fn validate_config(config: MetalConfig) -> Result<(), MetalError> {
    if config.rounds_per_command_buffer == 0 {
        return Err(MetalError::Validation(
            "rounds_per_command_buffer must be nonzero".into(),
        ));
    }
    if config.max_transitions_per_lp_per_round == 0 {
        return Err(MetalError::Validation(
            "max_transitions_per_lp_per_round must be nonzero".into(),
        ));
    }
    Ok(())
}

fn encoding_limits(rounds_per_command_buffer: usize) -> (usize, usize) {
    let pairs_per_command_buffer =
        rounds_per_command_buffer.min(MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER);
    let pairs_per_wave = pairs_per_command_buffer
        .saturating_mul(MAX_COMMAND_BUFFERS)
        .min(MAX_ENCODED_PAIRS_PER_WAVE);
    (pairs_per_command_buffer, pairs_per_wave)
}

struct MetalPlan {
    control: Vec<u64>,
    params: Vec<u64>,
    node_state: Vec<u64>,
    generators: Vec<u64>,
    flows: Vec<u64>,
    routes: Vec<u64>,
    links: Vec<u64>,
    fel_meta: Vec<u64>,
    fel_records: Vec<u64>,
    queue_meta: Vec<u64>,
    queue_records: Vec<u64>,
    in_service: Vec<u64>,
    outbox: Vec<u64>,
    worklist: Vec<u64>,
    summary: Vec<u64>,
    observed: Vec<u64>,
    departures: Vec<u64>,
    arrivals: Vec<u64>,
    orphan_packets: Vec<PacketDescriptor>,
    round_capacity: usize,
    dispatch_capacity: usize,
}

impl MetalPlan {
    fn new(
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
    ) -> Result<Self, MetalError> {
        let node_count = image.nodes.len();
        let flow_packet_counts = flow_packet_counts(image)?;
        let flow_feedback_counts = image.initial_packets.iter().fold(
            vec![0_usize; image.flows.len()],
            |mut counts, packet| {
                if packet.kind == PacketKind::Feedback {
                    counts[packet.flow.0 as usize] =
                        counts[packet.flow.0 as usize].saturating_add(1);
                }
                counts
            },
        );
        let minimum_lookahead_ns = image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min();
        let initial_by_payload = image
            .initial_packets
            .iter()
            .copied()
            .map(|packet| (packet.id, packet))
            .collect::<BTreeMap<_, _>>();
        let positioned_payloads = image
            .initial_events
            .iter()
            .map(|event| event.payload)
            .chain(
                image
                    .host_states
                    .iter()
                    .flat_map(|state| state.queue.iter().copied().chain(state.in_service)),
            )
            .chain(image.switch_states.iter().flat_map(|state| {
                state
                    .queues
                    .iter()
                    .flat_map(|queue| queue.queue.iter().copied().chain(queue.in_service))
            }))
            .collect::<BTreeSet<_>>();
        let orphan_packets = image
            .initial_packets
            .iter()
            .copied()
            .filter(|packet| !positioned_payloads.contains(&packet.id))
            .collect();

        let mut queue_caps = vec![1_usize; node_count];
        let mut fel_caps = vec![8_usize; node_count];
        for event in &image.initial_events {
            fel_caps[event.target.0 as usize] = fel_caps[event.target.0 as usize].saturating_add(1);
        }
        for (flow_index, flow) in image.flows.iter().enumerate() {
            let packet_count = flow_packet_counts[flow_index];
            let feedback_count = flow_feedback_counts[flow_index];
            let data_count = packet_count.saturating_sub(feedback_count);
            let source_slot = flow.source.0 as usize;
            queue_caps[source_slot] = queue_caps[source_slot].saturating_add(data_count);
            fel_caps[source_slot] = fel_caps[source_slot].saturating_add(4);

            add_flow_route_capacities(
                image,
                flow_index,
                data_count,
                PacketKind::Data,
                minimum_lookahead_ns,
                &mut fel_caps,
                &mut queue_caps,
            );
            add_flow_route_capacities(
                image,
                flow_index,
                feedback_count,
                PacketKind::Feedback,
                minimum_lookahead_ns,
                &mut fel_caps,
                &mut queue_caps,
            );
        }

        for node in &image.nodes {
            let slot = node.id.0 as usize;
            match node.kind {
                NodeKind::Host => {
                    let state = &image.host_states[node.state_slot as usize];
                    queue_caps[slot] = queue_caps[slot].max(state.queue.len());
                }
                NodeKind::Switch => {
                    let state = &image.switch_states[node.state_slot as usize];
                    let initial = state.queues.first().map_or(0, |queue| queue.queue.len());
                    queue_caps[slot] = queue_caps[slot].max(initial);
                    if let Some(limit) = state
                        .queues
                        .first()
                        .map(|queue| queue.queue_capacity_packets)
                        .filter(|limit| *limit != 0)
                    {
                        queue_caps[slot] = queue_caps[slot].min(limit as usize);
                    }
                }
            }
            if let Some(limit) = config.max_fel_events_per_lp {
                fel_caps[slot] = limit;
            }
            if let Some(limit) = config.max_queue_packets_per_lp {
                queue_caps[slot] = limit;
            }
        }

        let mut fel_meta = vec![0_u64; node_count * ARENA_META_WORDS];
        let mut queue_meta = vec![0_u64; node_count * ARENA_META_WORDS];
        let fel_slots = assign_arena_offsets(&mut fel_meta, &fel_caps)?;
        let queue_slots = assign_arena_offsets(&mut queue_meta, &queue_caps)?;
        let mut fel_records = zero_words(fel_slots, EVENT_WORDS)?;
        let mut queue_records = zero_words(queue_slots, EVENT_WORDS)?;
        let mut in_service = zero_words(node_count, EVENT_WORDS)?;
        let mut node_state = vec![0_u64; node_count * NODE_WORDS];
        let mut generators = vec![0_u64; image.flows.len().max(1) * GENERATOR_WORDS];

        for node in &image.nodes {
            let slot = node.id.0 as usize;
            let base = slot * NODE_WORDS;
            node_state[base] = node.kind as u64;
            match node.kind {
                NodeKind::Host => {
                    let state = &image.host_states[node.state_slot as usize];
                    node_state[base + 1] = state.egress_link.0;
                    node_state[base + 2] = 0;
                    node_state[base + 3] = u64::from(state.tx_ready_pending);
                    node_state[base + 4] = u64::from(state.in_service.is_some());
                    node_state[base + 5] = state.next_origin_seq;
                    node_state[base + 6] = state.next_payload_seq;
                    node_state[base + 7] = state.sourced_packets;
                    node_state[base + 8] = state.departed_packets;
                    node_state[base + 9] = state.received_packets;
                    for payload in &state.queue {
                        let packet = packet_for(&initial_by_payload, *payload)?;
                        queue_push_host(
                            slot,
                            packet_record(packet),
                            &mut queue_meta,
                            &mut queue_records,
                        )?;
                    }
                    if let Some(payload) = state.in_service {
                        let packet = packet_for(&initial_by_payload, payload)?;
                        write_record(&mut in_service, slot, packet_record(packet));
                    }
                    for generator in &state.generators {
                        let index = generator.flow.0 as usize;
                        let offset = index * GENERATOR_WORDS;
                        generators[offset] = 1;
                        generators[offset + 1] = node.id.0;
                        generators[offset + 2] = generator.packets_emitted;
                        generators[offset + 3] = generator.bytes_emitted;
                        generators[offset + 4] = generator.next_emission.status as u64;
                        generators[offset + 5] = generator.next_emission.departure_time_ns;
                        generators[offset + 6] = generator.next_emission.payload.0;
                        generators[offset + 7] = generator.rng_state;
                        generators[offset + 8] = generator.feedback.arrivals;
                        generators[offset + 9] = generator.feedback.outstanding_bytes;
                        generators[offset + 10] = generator.feedback.unacknowledged_bytes;
                        let FlowGeneratorKind::Constant(constant) = generator.kind;
                        generators[offset + 11] = constant.first_departure_ns;
                        generators[offset + 12] = constant.interval_ns;
                        generators[offset + 13] = constant.packet_size_bytes;
                        let (kind, value) = match constant.termination {
                            GeneratorTermination::Bytes(bytes) => (0, bytes),
                            GeneratorTermination::DurationNs(duration) => (1, duration),
                        };
                        generators[offset + 14] = kind;
                        generators[offset + 15] = value;
                    }
                }
                NodeKind::Switch => {
                    let state = &image.switch_states[node.state_slot as usize];
                    let queue = state.queues.first();
                    node_state[base + 1] = queue
                        .and_then(|queue| queue.egress_link)
                        .map_or(NONE, |link| link.0);
                    node_state[base + 2] = queue.map_or(0, |queue| queue.queue_capacity_packets);
                    node_state[base + 3] =
                        u64::from(queue.is_some_and(|queue| queue.tx_ready_pending));
                    node_state[base + 4] =
                        u64::from(queue.is_some_and(|queue| queue.in_service.is_some()));
                    node_state[base + 5] = state.next_origin_seq;
                    node_state[base + 7] = state.arrived_packets;
                    node_state[base + 8] = state.dropped_packets;
                    node_state[base + 9] = state.departed_packets;
                    node_state[base + 10] = state.physical_switch;
                    if let Some(queue) = queue {
                        for payload in &queue.queue {
                            let packet = packet_for(&initial_by_payload, *payload)?;
                            queue_push_host(
                                slot,
                                packet_record(packet),
                                &mut queue_meta,
                                &mut queue_records,
                            )?;
                        }
                        if let Some(payload) = queue.in_service {
                            let packet = packet_for(&initial_by_payload, payload)?;
                            write_record(&mut in_service, slot, packet_record(packet));
                        }
                    }
                }
            }
        }

        for event in &image.initial_events {
            let packet = packet_for(&initial_by_payload, event.payload)?;
            let record = event_record(*event, packet);
            heap_push_host(
                event.target.0 as usize,
                record,
                &mut fel_meta,
                &mut fel_records,
            )?;
        }

        let mut routes = Vec::new();
        let mut flows = vec![0_u64; image.flows.len().max(1) * FLOW_WORDS];
        for flow in &image.flows {
            let offset = flow.id.0 as usize * FLOW_WORDS;
            flows[offset] = flow.source.0;
            flows[offset + 1] = flow.target.0;
            flows[offset + 2] = routes.len() as u64;
            flows[offset + 3] = flow.route.len() as u64;
            routes.extend(flow.route.iter().map(|link| link.0));
            flows[offset + 4] = routes.len() as u64;
            flows[offset + 5] = flow.reverse_route.len() as u64;
            routes.extend(flow.reverse_route.iter().map(|link| link.0));
        }
        if routes.is_empty() {
            routes.push(0);
        }
        let mut links = vec![0_u64; image.links.len().max(1) * LINK_WORDS];
        for link in &image.links {
            let offset = link.id.0 as usize * LINK_WORDS;
            links[offset] = link.source.0;
            links[offset + 1] = link.target.0;
            links[offset + 2] = link.rate_bps;
            links[offset + 3] = link.propagation_ns;
        }

        let remote_bound = derived_remote_capacity(
            image,
            &flow_packet_counts,
            &flow_feedback_counts,
            minimum_lookahead_ns,
        );
        let outbox_capacity = config.max_outbox_events.unwrap_or(remote_bound.max(1));
        let event_bound = derived_transition_bound(image, &flow_packet_counts);
        let observation_capacity = if observation_mode == ObservationMode::Full {
            config.max_observations.unwrap_or(event_bound.max(1))
        } else {
            0
        };
        let round_capacity = config
            .max_rounds
            .unwrap_or_else(|| derived_round_bound(image, exclusive_horizon_ns, event_bound))
            .max(1);
        let dispatch_capacity = round_capacity
            .saturating_add(event_bound.div_ceil(config.max_transitions_per_lp_per_round));
        let run_end = exclusive_horizon_ns
            .map(u128::from)
            .unwrap_or(1_u128 << 64)
            .min(u128::from(image.stop_time_ns) + 1);
        let mut control = vec![0_u64; CONTROL_WORDS];
        control[CONTROL_RUN_END_LO] = run_end as u64;
        control[CONTROL_RUN_END_HI] = (run_end >> 64) as u64;
        let params = vec![
            node_count as u64,
            image.flows.len() as u64,
            image.links.len() as u64,
            outbox_capacity as u64,
            node_count as u64,
            observation_capacity as u64,
            observation_capacity as u64,
            observation_capacity as u64,
            u64::from(observation_mode == ObservationMode::Full),
            minimum_lookahead_ns.unwrap_or(0),
            config.max_transitions_per_lp_per_round as u64,
            image.stop_time_ns,
            u64::from(minimum_lookahead_ns.is_some()),
            round_capacity as u64,
        ];

        Ok(Self {
            control,
            params,
            node_state,
            generators,
            flows,
            routes,
            links,
            fel_meta,
            fel_records,
            queue_meta,
            queue_records,
            in_service,
            outbox: zero_words(outbox_capacity, EVENT_WORDS)?,
            worklist: vec![0_u64; node_count.max(1)],
            summary: vec![0_u64; SUMMARY_COUNTERS * 2],
            observed: zero_words(observation_capacity, OBSERVED_WORDS)?,
            departures: zero_words(observation_capacity, DEPARTURE_WORDS)?,
            arrivals: zero_words(observation_capacity, ARRIVAL_WORDS)?,
            orphan_packets,
            round_capacity,
            dispatch_capacity,
        })
    }
}

fn flow_packet_counts(image: &SimulationImage) -> Result<Vec<usize>, MetalError> {
    let mut counts = vec![0_usize; image.flows.len()];
    for packet in &image.initial_packets {
        counts[packet.flow.0 as usize] = counts[packet.flow.0 as usize].saturating_add(1);
    }
    for state in &image.host_states {
        for generator in &state.generators {
            let index = generator.flow.0 as usize;
            if generator.next_emission.status != GeneratorStatus::Scheduled {
                continue;
            }
            let FlowGeneratorKind::Constant(constant) = generator.kind;
            let termination_count = match constant.termination {
                GeneratorTermination::Bytes(bytes) => {
                    if generator.bytes_emitted >= bytes {
                        0
                    } else {
                        let remaining = bytes - generator.bytes_emitted;
                        remaining.div_ceil(constant.packet_size_bytes)
                    }
                }
                GeneratorTermination::DurationNs(duration) => {
                    let end = constant
                        .first_departure_ns
                        .checked_add(duration)
                        .ok_or_else(|| {
                            MetalError::Validation("generator duration endpoint overflows".into())
                        })?;
                    if generator.next_emission.departure_time_ns >= end {
                        0
                    } else {
                        1 + (end - 1 - generator.next_emission.departure_time_ns)
                            / constant.interval_ns
                    }
                }
            };
            let stop_count = if generator.next_emission.departure_time_ns > image.stop_time_ns {
                0
            } else {
                1 + (image.stop_time_ns - generator.next_emission.departure_time_ns)
                    / constant.interval_ns
            };
            let future = termination_count.min(stop_count) as usize;
            counts[index] = counts[index].saturating_add(future.saturating_sub(1));
        }
    }
    Ok(counts)
}

fn generator_round_burst(
    image: &SimulationImage,
    flow_index: usize,
    packet_count: usize,
    lookahead: Option<u64>,
) -> usize {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| {
            generator.flow.0 as usize == flow_index
                && generator.next_emission.status == GeneratorStatus::Scheduled
        })
        .map(|generator| {
            let FlowGeneratorKind::Constant(constant) = generator.kind;
            match lookahead {
                Some(lookahead) => packet_count.min(
                    usize::try_from(lookahead / constant.interval_ns)
                        .unwrap_or(usize::MAX)
                        .saturating_add(1),
                ),
                None => packet_count,
            }
        })
        .fold(0, usize::saturating_add)
}

fn add_flow_route_capacities(
    image: &SimulationImage,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    lookahead: Option<u64>,
    fel_caps: &mut [usize],
    queue_caps: &mut [usize],
) {
    if packet_count == 0 {
        return;
    }
    let flow = &image.flows[flow_index];
    let (route, terminal) = match packet_kind {
        PacketKind::Data => (flow.route.as_slice(), flow.target),
        PacketKind::Feedback => (flow.reverse_route.as_slice(), flow.source),
    };
    for index in 0..route.len() {
        let target = route
            .get(index + 1)
            .map(|next| image.links[next.0 as usize].source)
            .unwrap_or(terminal);
        let target_slot = target.0 as usize;
        let burst = flow_link_fel_bound(
            image,
            flow_index,
            packet_count,
            packet_kind,
            route[index],
            lookahead,
        );
        fel_caps[target_slot] = fel_caps[target_slot].saturating_add(burst);
        if image.nodes[target_slot].kind == NodeKind::Switch {
            queue_caps[target_slot] = queue_caps[target_slot].saturating_add(packet_count);
        }
    }
}

fn flow_link_serialization_ns(
    image: &SimulationImage,
    flow_index: usize,
    packet_kind: PacketKind,
    link: crate::LinkDescriptor,
) -> u64 {
    let minimum_size = image
        .initial_packets
        .iter()
        .filter(|packet| packet.flow.0 as usize == flow_index && packet.kind == packet_kind)
        .map(|packet| packet.size_bytes)
        .chain(
            (packet_kind == PacketKind::Data)
                .then(|| {
                    image
                        .host_states
                        .iter()
                        .flat_map(|state| &state.generators)
                        .filter(move |generator| generator.flow.0 as usize == flow_index)
                        .map(|generator| {
                            let FlowGeneratorKind::Constant(constant) = generator.kind;
                            constant.packet_size_bytes
                        })
                })
                .into_iter()
                .flatten(),
        )
        .min()
        .unwrap_or(1);
    crate::time::serialization_time_ns(minimum_size, link.rate_bps)
        .expect("Metal validation established a positive finite serialization interval")
}

fn flow_link_round_bound(
    image: &SimulationImage,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    link_id: crate::LinkId,
    lookahead: Option<u64>,
) -> usize {
    if packet_count == 0 {
        return 0;
    }
    let link = image.links[link_id.0 as usize];
    let source = image.nodes[link.source.0 as usize];
    let (current_queue, queue_capacity) = match source.kind {
        NodeKind::Host => {
            let state = &image.host_states[source.state_slot as usize];
            (state.queue.len(), packet_count)
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
    let serialization = flow_link_serialization_ns(image, flow_index, packet_kind, link);
    let service_burst = lookahead.map_or(packet_count, |lookahead| {
        usize::try_from(lookahead.div_ceil(serialization)).unwrap_or(usize::MAX)
    });
    let generator_burst =
        if packet_kind == PacketKind::Data && link.source == image.flows[flow_index].source {
            generator_round_burst(image, flow_index, packet_count, lookahead)
        } else {
            0
        };

    // One horizon can expose a checkpoint queue, one packet already in service, link-rate
    // completions, and newly generated packets. The whole-flow count remains the absolute cap.
    packet_count.min(
        queue_bound
            .saturating_add(1)
            .saturating_add(service_burst)
            .saturating_add(generator_burst),
    )
}

fn flow_link_fel_bound(
    image: &SimulationImage,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    link_id: crate::LinkId,
    lookahead: Option<u64>,
) -> usize {
    if packet_count == 0 {
        return 0;
    }
    let link = image.links[link_id.0 as usize];
    let serialization = flow_link_serialization_ns(image, flow_index, packet_kind, link);
    let in_flight =
        usize::try_from(link.propagation_ns.div_ceil(serialization)).unwrap_or(usize::MAX);

    packet_count.min(
        flow_link_round_bound(
            image,
            flow_index,
            packet_count,
            packet_kind,
            link_id,
            lookahead,
        )
        .saturating_add(in_flight),
    )
}

fn derived_remote_capacity(
    image: &SimulationImage,
    counts: &[usize],
    feedback_counts: &[usize],
    lookahead: Option<u64>,
) -> usize {
    image
        .flows
        .iter()
        .enumerate()
        .map(|(index, flow)| {
            let feedback_count = feedback_counts[index];
            let data_count = counts[index].saturating_sub(feedback_count);
            flow.route
                .iter()
                .map(|link| {
                    flow_link_round_bound(
                        image,
                        index,
                        data_count,
                        PacketKind::Data,
                        *link,
                        lookahead,
                    )
                })
                .chain(flow.reverse_route.iter().map(|link| {
                    flow_link_round_bound(
                        image,
                        index,
                        feedback_count,
                        PacketKind::Feedback,
                        *link,
                        lookahead,
                    )
                }))
                .fold(0, usize::saturating_add)
        })
        .fold(image.nodes.len().saturating_mul(2), usize::saturating_add)
}

fn derived_transition_bound(image: &SimulationImage, counts: &[usize]) -> usize {
    image
        .flows
        .iter()
        .enumerate()
        .map(|(index, flow)| {
            counts[index].saturating_mul(1_usize.saturating_add(
                3_usize.saturating_mul(flow.route.len().max(flow.reverse_route.len())),
            ))
        })
        .fold(
            image.initial_events.len().saturating_add(1),
            usize::saturating_add,
        )
}

fn derived_round_bound(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    event_bound: usize,
) -> usize {
    let Some(first) = image
        .initial_events
        .iter()
        .map(|event| event.key.time_ns)
        .min()
    else {
        return 1;
    };
    let Some(lookahead) = image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min()
    else {
        return 2;
    };
    let run_end = exclusive_horizon_ns
        .map(u128::from)
        .unwrap_or(1_u128 << 64)
        .min(u128::from(image.stop_time_ns) + 1);
    let time_bound = run_end
        .saturating_sub(u128::from(first))
        .div_ceil(u128::from(lookahead))
        .saturating_add(2)
        .min(usize::MAX as u128) as usize;
    time_bound.min(event_bound.saturating_add(2)).max(1)
}

fn assign_arena_offsets(meta: &mut [u64], capacities: &[usize]) -> Result<usize, MetalError> {
    let mut offset = 0_usize;
    for (slot, capacity) in capacities.iter().copied().enumerate() {
        let base = slot * ARENA_META_WORDS;
        meta[base] = offset as u64;
        meta[base + 1] = capacity as u64;
        offset = offset
            .checked_add(capacity)
            .ok_or_else(|| MetalError::Validation("device arena size overflows usize".into()))?;
    }
    Ok(offset)
}

fn zero_words(records: usize, words: usize) -> Result<Vec<u64>, MetalError> {
    let length = records
        .checked_mul(words)
        .ok_or_else(|| MetalError::Validation("device buffer size overflows usize".into()))?;
    Ok(vec![0; length.max(1)])
}

fn packet_for(
    packets: &BTreeMap<PayloadId, PacketDescriptor>,
    payload: PayloadId,
) -> Result<PacketDescriptor, MetalError> {
    packets.get(&payload).copied().ok_or_else(|| {
        MetalError::Validation(format!("payload {payload:?} has no initial descriptor"))
    })
}

fn packet_record(packet: PacketDescriptor) -> [u64; EVENT_WORDS] {
    let mut record = [0_u64; EVENT_WORDS];
    record[6] = packet.id.0;
    record[7] = packet.id.0;
    record[8] = packet.flow.0;
    record[9] = packet.size_bytes;
    record[10] = packet.kind as u64;
    record
}

fn event_record(event: Event, packet: PacketDescriptor) -> [u64; EVENT_WORDS] {
    [
        event.key.time_ns,
        u64::from(event.key.phase),
        event.key.origin_node.0,
        event.key.origin_seq,
        event.target.0,
        event.kind as u64,
        event.payload.0,
        packet.id.0,
        packet.flow.0,
        packet.size_bytes,
        packet.kind as u64,
    ]
}

fn write_record(storage: &mut [u64], slot: usize, record: [u64; EVENT_WORDS]) {
    let offset = slot * EVENT_WORDS;
    storage[offset..offset + EVENT_WORDS].copy_from_slice(&record);
}

fn record_less(left: &[u64], right: &[u64]) -> bool {
    (left[0], left[1], left[2], left[3]) < (right[0], right[1], right[2], right[3])
}

fn heap_push_host(
    lp: usize,
    record: [u64; EVENT_WORDS],
    meta: &mut [u64],
    storage: &mut [u64],
) -> Result<(), MetalError> {
    let base = lp * ARENA_META_WORDS;
    let offset = meta[base] as usize;
    let capacity = meta[base + 1] as usize;
    let mut count = meta[base + 3] as usize;
    if count == capacity {
        return Err(MetalError::CapacityExceeded {
            arena: MetalArena::Fel,
            node: Some(NodeId(lp as u64)),
            capacity,
        });
    }
    write_record(storage, offset + count, record);
    count += 1;
    meta[base + 3] = count as u64;
    let mut child = count - 1;
    while child != 0 {
        let parent = (child - 1) / 2;
        let child_start = (offset + child) * EVENT_WORDS;
        let parent_start = (offset + parent) * EVENT_WORDS;
        if !record_less(
            &storage[child_start..child_start + EVENT_WORDS],
            &storage[parent_start..parent_start + EVENT_WORDS],
        ) {
            break;
        }
        for word in 0..EVENT_WORDS {
            storage.swap(child_start + word, parent_start + word);
        }
        child = parent;
    }
    Ok(())
}

fn queue_push_host(
    lp: usize,
    record: [u64; EVENT_WORDS],
    meta: &mut [u64],
    storage: &mut [u64],
) -> Result<(), MetalError> {
    let base = lp * ARENA_META_WORDS;
    let offset = meta[base] as usize;
    let capacity = meta[base + 1] as usize;
    let head = meta[base + 2] as usize;
    let count = meta[base + 3] as usize;
    if count == capacity {
        return Err(MetalError::CapacityExceeded {
            arena: MetalArena::Queue,
            node: Some(NodeId(lp as u64)),
            capacity,
        });
    }
    let physical = (head + count) % capacity.max(1);
    write_record(storage, offset + physical, record);
    meta[base + 3] = (count + 1) as u64;
    Ok(())
}

struct SharedBuffer {
    raw: RawMetalBuffer,
    words: usize,
}

impl SharedBuffer {
    fn new(device: &ProtocolObject<dyn MTLDevice>, words: Vec<u64>) -> Result<Self, MetalError> {
        let words = if words.is_empty() { vec![0] } else { words };
        let bytes = words
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| MetalError::Validation("Metal buffer byte size overflows".into()))?;
        let raw = device
            .newBufferWithLength_options(bytes, MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| {
                MetalError::Unavailable(format!("failed to allocate {bytes}-byte shared buffer"))
            })?;
        unsafe {
            std::ptr::copy_nonoverlapping(
                words.as_ptr(),
                raw.contents().as_ptr().cast::<u64>(),
                words.len(),
            );
        }
        Ok(Self {
            raw,
            words: words.len(),
        })
    }

    fn read(&self) -> Vec<u64> {
        unsafe {
            std::slice::from_raw_parts(self.raw.contents().cast::<u64>().as_ptr(), self.words)
                .to_vec()
        }
    }

    fn word(&self, index: usize) -> u64 {
        assert!(index < self.words);
        unsafe { *self.raw.contents().cast::<u64>().as_ptr().add(index) }
    }
}

struct MetalBuffers {
    planes: Vec<SharedBuffer>,
    orphan_packets: Vec<PacketDescriptor>,
    round_capacity: usize,
    dispatch_capacity: usize,
}

impl MetalBuffers {
    fn new(device: &ProtocolObject<dyn MTLDevice>, plan: MetalPlan) -> Result<Self, MetalError> {
        let round_capacity = plan.round_capacity;
        let dispatch_capacity = plan.dispatch_capacity;
        let orphan_packets = plan.orphan_packets;
        let planes = vec![
            plan.control,
            plan.params,
            plan.node_state,
            plan.generators,
            plan.flows,
            plan.routes,
            plan.links,
            plan.fel_meta,
            plan.fel_records,
            plan.queue_meta,
            plan.queue_records,
            plan.in_service,
            plan.outbox,
            plan.worklist,
            plan.summary,
            plan.observed,
            plan.departures,
            plan.arrivals,
        ]
        .into_iter()
        .map(|words| SharedBuffer::new(device, words))
        .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            planes,
            orphan_packets,
            round_capacity,
            dispatch_capacity,
        })
    }

    fn bind(&self, encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>) {
        for (index, plane) in self.planes.iter().enumerate() {
            unsafe {
                encoder.setBuffer_offset_atIndex(Some(&plane.raw), 0, index);
            }
        }
    }

    fn finish(
        &self,
        image: &SimulationImage,
        observation_mode: ObservationMode,
        timing: MetalTiming,
    ) -> Result<MetalRun, MetalError> {
        let planes = self
            .planes
            .iter()
            .map(SharedBuffer::read)
            .collect::<Vec<_>>();
        let control = &planes[0];
        if control[CONTROL_ERROR] != 0 {
            return Err(decode_device_error(control));
        }
        if control[CONTROL_DONE] == 0 {
            return Err(MetalError::RoundLimitExceeded {
                capacity: self.round_capacity,
            });
        }

        let node_state = &planes[2];
        let generators = &planes[3];
        let fel_meta = &planes[7];
        let fel_records = &planes[8];
        let queue_meta = &planes[9];
        let queue_records = &planes[10];
        let in_service = &planes[11];
        let summary_words = &planes[14];
        let observed_words = &planes[15];
        let departure_words = &planes[16];
        let arrival_words = &planes[17];

        let mut host_states = image.host_states.clone();
        let mut switch_states = image.switch_states.clone();
        let mut resident = self
            .orphan_packets
            .iter()
            .copied()
            .map(|packet| (packet.id, packet))
            .collect::<BTreeMap<_, _>>();
        for node in &image.nodes {
            let lp = node.id.0 as usize;
            let base = lp * NODE_WORDS;
            let queue = read_queue(lp, queue_meta, queue_records);
            for packet in &queue {
                resident.insert(packet.id, *packet);
            }
            let service = (node_state[base + 4] != 0).then(|| read_packet(in_service, lp));
            if let Some(packet) = service {
                resident.insert(packet.id, packet);
            }
            match node.kind {
                NodeKind::Host => {
                    let state = &mut host_states[node.state_slot as usize];
                    state.queue = queue.iter().map(|packet| packet.id).collect();
                    state.in_service = service.map(|packet| packet.id);
                    state.tx_ready_pending = node_state[base + 3] != 0;
                    state.next_origin_seq = node_state[base + 5];
                    state.next_payload_seq = node_state[base + 6];
                    state.sourced_packets = node_state[base + 7];
                    state.departed_packets = node_state[base + 8];
                    state.received_packets = node_state[base + 9];
                    for generator in &mut state.generators {
                        let offset = generator.flow.0 as usize * GENERATOR_WORDS;
                        generator.packets_emitted = generators[offset + 2];
                        generator.bytes_emitted = generators[offset + 3];
                        generator.next_emission.status =
                            decode_generator_status(generators[offset + 4])?;
                        generator.next_emission.departure_time_ns = generators[offset + 5];
                        generator.next_emission.payload = PayloadId(generators[offset + 6]);
                        generator.rng_state = generators[offset + 7];
                        generator.feedback.arrivals = generators[offset + 8];
                        generator.feedback.outstanding_bytes = generators[offset + 9];
                        generator.feedback.unacknowledged_bytes = generators[offset + 10];
                    }
                }
                NodeKind::Switch => {
                    let state = &mut switch_states[node.state_slot as usize];
                    if let Some(switch_queue) = state.queues.first_mut() {
                        switch_queue.queue = queue.iter().map(|packet| packet.id).collect();
                        switch_queue.in_service = service.map(|packet| packet.id);
                        switch_queue.tx_ready_pending = node_state[base + 3] != 0;
                    }
                    state.next_origin_seq = node_state[base + 5];
                    state.arrived_packets = node_state[base + 7];
                    state.dropped_packets = node_state[base + 8];
                    state.departed_packets = node_state[base + 9];
                }
            }
        }

        let mut pending_events = Vec::new();
        for lp in 0..image.nodes.len() {
            let base = lp * ARENA_META_WORDS;
            let offset = fel_meta[base] as usize;
            let count = fel_meta[base + 3] as usize;
            for index in 0..count {
                let record = read_record(fel_records, offset + index);
                let (event, packet) = decode_event(record)?;
                resident.insert(packet.id, packet);
                pending_events.push(event);
            }
        }
        pending_events.sort_unstable_by_key(|event| event.key);

        let summary = decode_summary(summary_words);
        let mut observed_packets = BTreeMap::new();
        let mut departures = Vec::new();
        let mut arrivals = Vec::new();
        if observation_mode == ObservationMode::Full {
            for index in 0..control[CONTROL_OBSERVED] as usize {
                let offset = index * OBSERVED_WORDS;
                let packet = decode_packet_words(&observed_words[offset..offset + OBSERVED_WORDS])?;
                observed_packets.entry(packet.id).or_insert(packet);
            }
            let mut keyed_departures = Vec::new();
            for index in 0..control[CONTROL_DEPARTURES] as usize {
                let offset = index * DEPARTURE_WORDS;
                let words = &departure_words[offset..offset + DEPARTURE_WORDS];
                let key = decode_key(words)?;
                let packet = PacketDescriptor {
                    id: PayloadId(words[4]),
                    flow: crate::FlowId(words[6]),
                    size_bytes: words[7],
                    kind: decode_packet_kind(words[8])?,
                };
                observed_packets.entry(packet.id).or_insert(packet);
                keyed_departures.push((
                    key,
                    PacketDeparture {
                        payload: packet.id,
                        time_ns: words[5],
                    },
                ));
            }
            keyed_departures.sort_unstable_by_key(|(key, _)| *key);
            departures = keyed_departures
                .into_iter()
                .map(|(_, departure)| departure)
                .collect();

            let mut keyed_arrivals = Vec::new();
            for index in 0..control[CONTROL_ARRIVALS] as usize {
                let offset = index * ARRIVAL_WORDS;
                let words = &arrival_words[offset..offset + ARRIVAL_WORDS];
                let key = decode_key(words)?;
                let packet = PacketDescriptor {
                    id: PayloadId(words[4]),
                    flow: crate::FlowId(words[7]),
                    size_bytes: words[8],
                    kind: decode_packet_kind(words[9])?,
                };
                observed_packets.entry(packet.id).or_insert(packet);
                keyed_arrivals.push((
                    key,
                    PacketArrivalObservation {
                        payload: packet.id,
                        time_ns: words[5],
                        disposition: decode_disposition(words[6])?,
                    },
                ));
            }
            keyed_arrivals.sort_unstable_by_key(|(key, _)| *key);
            arrivals = keyed_arrivals
                .into_iter()
                .map(|(_, arrival)| arrival)
                .collect();
        }

        Ok(MetalRun {
            result: RunResult {
                host_states,
                switch_states,
                summary,
                resident_packets: resident.into_values().collect(),
                observed_packets: observed_packets.into_values().collect(),
                departures,
                arrivals,
                pending_events,
            },
            rounds: control[CONTROL_ROUNDS],
            transitions: control[CONTROL_TRANSITIONS],
            continuation_relaunches: control[CONTROL_RELAUNCHES],
            wave_boundary_syncs: timing.wave_boundary_syncs,
            mid_round_wave_boundary_syncs: timing.mid_round_wave_boundary_syncs,
            host_encode_submit_ns: timing.host_encode_submit_ns,
            device_ns: timing.device_ns,
            wall_ns: timing.wall_ns,
        })
    }
}

fn decode_device_error(control: &[u64]) -> MetalError {
    let node = (control[CONTROL_ERROR_NODE] != NONE).then(|| NodeId(control[CONTROL_ERROR_NODE]));
    let capacity = control[CONTROL_ERROR_CAPACITY] as usize;
    match control[CONTROL_ERROR] {
        1 => MetalError::CapacityExceeded {
            arena: decode_arena(control[CONTROL_ERROR_ARENA]),
            node,
            capacity,
        },
        2 => MetalError::TransitionLimitExceeded {
            node: node.unwrap_or(NodeId(0)),
            capacity,
        },
        code => MetalError::DeviceExecution { code, node },
    }
}

fn decode_arena(value: u64) -> MetalArena {
    match value {
        1 => MetalArena::Fel,
        2 => MetalArena::Queue,
        3 => MetalArena::Outbox,
        4 => MetalArena::Worklist,
        5 => MetalArena::ObservedPackets,
        6 => MetalArena::Departures,
        7 => MetalArena::Arrivals,
        _ => MetalArena::Fel,
    }
}

fn read_record(storage: &[u64], slot: usize) -> &[u64] {
    let offset = slot * EVENT_WORDS;
    &storage[offset..offset + EVENT_WORDS]
}

fn read_packet(storage: &[u64], slot: usize) -> PacketDescriptor {
    let record = read_record(storage, slot);
    PacketDescriptor {
        id: PayloadId(record[7]),
        flow: crate::FlowId(record[8]),
        size_bytes: record[9],
        kind: if record[10] == 0 {
            PacketKind::Data
        } else {
            PacketKind::Feedback
        },
    }
}

fn read_queue(lp: usize, meta: &[u64], records: &[u64]) -> Vec<PacketDescriptor> {
    let base = lp * ARENA_META_WORDS;
    let offset = meta[base] as usize;
    let capacity = meta[base + 1] as usize;
    let head = meta[base + 2] as usize;
    let count = meta[base + 3] as usize;
    (0..count)
        .map(|index| read_packet(records, offset + (head + index) % capacity.max(1)))
        .collect()
}

fn decode_event(record: &[u64]) -> Result<(Event, PacketDescriptor), MetalError> {
    let kind = decode_event_kind(record[5])?;
    let packet = PacketDescriptor {
        id: PayloadId(record[7]),
        flow: crate::FlowId(record[8]),
        size_bytes: record[9],
        kind: decode_packet_kind(record[10])?,
    };
    Ok((
        Event {
            key: EventKey {
                time_ns: record[0],
                phase: u16::try_from(record[1]).map_err(|_| MetalError::DeviceExecution {
                    code: 90,
                    node: None,
                })?,
                origin_node: NodeId(record[2]),
                origin_seq: record[3],
            },
            target: NodeId(record[4]),
            kind,
            payload: PayloadId(record[6]),
        },
        packet,
    ))
}

fn decode_key(words: &[u64]) -> Result<EventKey, MetalError> {
    Ok(EventKey {
        time_ns: words[0],
        phase: u16::try_from(words[1]).map_err(|_| MetalError::DeviceExecution {
            code: 91,
            node: None,
        })?,
        origin_node: NodeId(words[2]),
        origin_seq: words[3],
    })
}

fn decode_event_kind(value: u64) -> Result<EventKind, MetalError> {
    match value {
        0 => Ok(EventKind::PacketArrival),
        1 => Ok(EventKind::TxReady),
        2 => Ok(EventKind::TxComplete),
        3 => Ok(EventKind::RemoteArrival),
        _ => Err(MetalError::DeviceExecution {
            code: 92,
            node: None,
        }),
    }
}

fn decode_packet_kind(value: u64) -> Result<PacketKind, MetalError> {
    match value {
        0 => Ok(PacketKind::Data),
        1 => Ok(PacketKind::Feedback),
        _ => Err(MetalError::DeviceExecution {
            code: 93,
            node: None,
        }),
    }
}

fn decode_generator_status(value: u64) -> Result<GeneratorStatus, MetalError> {
    match value {
        0 => Ok(GeneratorStatus::Scheduled),
        1 => Ok(GeneratorStatus::Blocked),
        2 => Ok(GeneratorStatus::Finished),
        3 => Ok(GeneratorStatus::Stopped),
        _ => Err(MetalError::DeviceExecution {
            code: 94,
            node: None,
        }),
    }
}

fn decode_disposition(value: u64) -> Result<ArrivalDisposition, MetalError> {
    match value {
        0 => Ok(ArrivalDisposition::Admitted),
        1 => Ok(ArrivalDisposition::Dropped),
        2 => Ok(ArrivalDisposition::Delivered),
        3 => Ok(ArrivalDisposition::Feedback),
        _ => Err(MetalError::DeviceExecution {
            code: 95,
            node: None,
        }),
    }
}

fn decode_packet_words(words: &[u64]) -> Result<PacketDescriptor, MetalError> {
    Ok(PacketDescriptor {
        id: PayloadId(words[0]),
        flow: crate::FlowId(words[1]),
        size_bytes: words[2],
        kind: decode_packet_kind(words[3])?,
    })
}

fn decode_summary(words: &[u64]) -> RunSummary {
    let counter = |index: usize| -> u128 {
        u128::from(words[index * 2]) | (u128::from(words[index * 2 + 1]) << 64)
    };
    RunSummary {
        sourced_packets: counter(0),
        sourced_bytes: counter(1),
        departed_packets: counter(2),
        departed_bytes: counter(3),
        admitted_packets: counter(4),
        admitted_bytes: counter(5),
        received_packets: counter(6),
        received_bytes: counter(7),
        dropped_packets: counter(8),
        dropped_bytes: counter(9),
        feedback_packets: counter(10),
        feedback_bytes: counter(11),
    }
}

struct DirectMetal {
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    horizon_pipeline: MetalPipeline,
    round_pipeline: MetalPipeline,
}

impl DirectMetal {
    fn new() -> Result<Self, MetalError> {
        let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
            MetalError::Unavailable("system default device is unavailable".into())
        })?;
        let queue = device
            .newCommandQueue()
            .ok_or_else(|| MetalError::Unavailable("command queue creation failed".into()))?;
        let source = include_str!("metal_kernels.metal");
        let horizon_pipeline = create_pipeline(&device, source, "days_horizon")?;
        let round_pipeline = create_pipeline(&device, source, "days_round")?;
        if horizon_pipeline.maxTotalThreadsPerThreadgroup() < LANES {
            return Err(MetalError::Unavailable(format!(
                "horizon pipeline supports only {} threads per threadgroup",
                horizon_pipeline.maxTotalThreadsPerThreadgroup()
            )));
        }
        if device.maxThreadgroupMemoryLength() < HORIZON_THREADGROUP_BYTES {
            return Err(MetalError::Unavailable(format!(
                "device exposes only {} bytes of threadgroup memory",
                device.maxThreadgroupMemoryLength()
            )));
        }
        Ok(Self {
            device,
            queue,
            horizon_pipeline,
            round_pipeline,
        })
    }

    fn run(&self, buffers: &MetalBuffers, config: MetalConfig) -> Result<MetalTiming, MetalError> {
        let wall_started = Instant::now();
        let mut host_encode_submit_ns = 0_u64;
        let mut device_ns = 0_u64;
        let mut wave_boundary_syncs = 0_u64;
        let mut mid_round_wave_boundary_syncs = 0_u64;
        let (pairs_per_command_buffer, pairs_per_wave) =
            encoding_limits(config.rounds_per_command_buffer);
        let mut remaining = buffers.dispatch_capacity;
        while remaining != 0 {
            let wave_started = Instant::now();
            let wave_rounds = remaining.min(pairs_per_wave);
            let mut wave_remaining = wave_rounds;
            let mut command_buffers = Vec::new();
            while wave_remaining != 0 {
                let encoded = wave_remaining.min(pairs_per_command_buffer);
                let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
                    MetalError::Unavailable("command buffer creation failed".into())
                })?;
                let encoder = command_buffer
                    .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
                    .ok_or_else(|| {
                        MetalError::Unavailable("serial compute encoder creation failed".into())
                    })?;
                buffers.bind(&encoder);
                for _ in 0..encoded {
                    encoder.setComputePipelineState(&self.horizon_pipeline);
                    encoder.dispatchThreadgroups_threadsPerThreadgroup(
                        MTLSize {
                            width: 1,
                            height: 1,
                            depth: 1,
                        },
                        MTLSize {
                            width: LANES,
                            height: 1,
                            depth: 1,
                        },
                    );
                    encoder.setComputePipelineState(&self.round_pipeline);
                    encoder.dispatchThreadgroups_threadsPerThreadgroup(
                        MTLSize {
                            width: 1,
                            height: 1,
                            depth: 1,
                        },
                        MTLSize {
                            width: 1,
                            height: 1,
                            depth: 1,
                        },
                    );
                }
                encoder.endEncoding();
                command_buffers.push(command_buffer);
                wave_remaining -= encoded;
            }
            for command_buffer in &command_buffers {
                command_buffer.commit();
            }
            host_encode_submit_ns =
                host_encode_submit_ns.saturating_add(duration_ns(wave_started.elapsed()));
            command_buffers
                .last()
                .expect("nonzero wave produces a command buffer")
                .waitUntilCompleted();
            for command_buffer in &command_buffers {
                if command_buffer.status() != MTLCommandBufferStatus::Completed {
                    let detail = command_buffer
                        .error()
                        .map(|error| error.localizedDescription().to_string())
                        .unwrap_or_else(|| "no NSError detail".into());
                    return Err(MetalError::Unavailable(format!(
                        "command buffer status {:?}: {detail}",
                        command_buffer.status()
                    )));
                }
                let start = command_buffer.GPUStartTime();
                let end = command_buffer.GPUEndTime();
                if !start.is_finite() || !end.is_finite() || end < start {
                    return Err(MetalError::Unavailable(format!(
                        "invalid GPU timestamp range {start}..{end}"
                    )));
                }
                device_ns = device_ns.saturating_add(seconds_ns(end - start));
            }
            wave_boundary_syncs = wave_boundary_syncs.saturating_add(1);
            remaining -= wave_rounds;
            let control = &buffers.planes[0];
            let done = control.word(CONTROL_DONE) != 0;
            let error = control.word(CONTROL_ERROR) != 0;
            if !done && !error && control.word(CONTROL_CONTINUATION) != 0 {
                mid_round_wave_boundary_syncs = mid_round_wave_boundary_syncs.saturating_add(1);
            }
            if done || error {
                break;
            }
        }
        let wall_ns = duration_ns(wall_started.elapsed());
        Ok(MetalTiming {
            host_encode_submit_ns,
            device_ns,
            wall_ns,
            wave_boundary_syncs,
            mid_round_wave_boundary_syncs,
        })
    }
}

fn create_pipeline(
    device: &ProtocolObject<dyn MTLDevice>,
    source: &str,
    entrypoint: &str,
) -> Result<MetalPipeline, MetalError> {
    let source = NSString::from_str(source);
    let library = device
        .newLibraryWithSource_options_error(&source, None)
        .map_err(|error| {
            MetalError::Unavailable(format!(
                "MSL compilation failed for {entrypoint}: {}",
                error.localizedDescription()
            ))
        })?;
    let name = NSString::from_str(entrypoint);
    let function = library.newFunctionWithName(&name).ok_or_else(|| {
        MetalError::Unavailable(format!("MSL entry point `{entrypoint}` was not found"))
    })?;
    device
        .newComputePipelineStateWithFunction_error(&function)
        .map_err(|error| {
            MetalError::Unavailable(format!(
                "pipeline creation failed for {entrypoint}: {}",
                error.localizedDescription()
            ))
        })
}

struct MetalTiming {
    host_encode_submit_ns: u64,
    device_ns: u64,
    wall_ns: u64,
    wave_boundary_syncs: u64,
    mid_round_wave_boundary_syncs: u64,
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn seconds_ns(seconds: f64) -> u64 {
    Duration::from_secs_f64(seconds)
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER, MAX_ENCODED_PAIRS_PER_WAVE, encoding_limits,
    };

    #[test]
    fn encoding_limits_clamp_each_buffer_and_the_total_wave() {
        assert_eq!(
            encoding_limits(usize::MAX),
            (
                MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER,
                MAX_ENCODED_PAIRS_PER_WAVE
            )
        );
        assert_eq!(encoding_limits(1), (1, 64));
    }
}

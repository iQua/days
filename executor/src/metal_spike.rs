//! T13-only CubeCL/Metal feasibility spike.
//!
//! This module is deliberately not a backend. It demonstrates the fixed-capacity primitives and
//! synchronization shape that a later backend would need, without changing executor semantics.

use std::{collections::VecDeque, time::Instant};

use cubecl::metal::{MetalDevice, MetalRuntime};
use cubecl::prelude::*;

use crate::{HostState, LinkId, NodeDescriptor, NodeId, NodeKind, SimulationImage, SwitchState};

/// Exact CubeCL release selected by this spike.
pub const SUBSTRATE_VERSION: &str = "CubeCL 0.11.0-pre.1";

/// Conservative dispatch cap used for one CubeCL command buffer on the measured M5 Max tier.
///
/// CubeCL configures a 50-operation tier threshold and currently flushes only after exceeding it.
/// The spike does not rely on that off-by-one behavior. T14 would need to expose or raise the
/// threshold if measurements showed that more resident rounds were required.
pub const M5_BATCH_OP_THRESHOLD: usize = 50;

const ERROR_NONE: u32 = 0;
const ERROR_TIME_OVERFLOW: u32 = 1;
const ERROR_ZERO_RATE: u32 = 2;
const ERROR_FEL_OVERFLOW: u32 = 3;
const ERROR_OUTBOX_OVERFLOW: u32 = 4;
const ERROR_SEQUENCE_OVERFLOW: u32 = 5;

const FEL_CAPACITY: usize = 4;
const OUTBOX_CAPACITY: usize = 2;
const ROLE_HOST: u32 = 0;
const ROLE_SWITCH: u32 = 1;

type Client = ComputeClient<MetalRuntime>;

/// Topology-neutral, spike-only physical view derived from one semantic image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedSpikeImage {
    pub node_ids: Vec<u64>,
    pub node_kinds: Vec<u32>,
    pub state_slots: Vec<u32>,
    pub host_lp_slots: Vec<u32>,
    pub switch_lp_slots: Vec<u32>,
    pub next_event_time_ns: Vec<u64>,
    pub active: Vec<u32>,
    pub index_slot: Vec<u32>,
}

impl PreparedSpikeImage {
    /// Derives role worklists and SoA hot fields without inspecting topology shape.
    pub fn from_image(image: &SimulationImage) -> Result<Self, MetalSpikeError> {
        let mut host_lp_slots = Vec::new();
        let mut switch_lp_slots = Vec::new();
        let mut node_ids = Vec::with_capacity(image.nodes.len());
        let mut node_kinds = Vec::with_capacity(image.nodes.len());
        let mut state_slots = Vec::with_capacity(image.nodes.len());

        for (lp_slot, node) in image.nodes.iter().enumerate() {
            let lp_slot =
                u32::try_from(lp_slot).map_err(|_| MetalSpikeError::TooManyLogicalProcesses)?;
            node_ids.push(node.id.0);
            state_slots.push(node.state_slot);
            match node.kind {
                NodeKind::Host => {
                    node_kinds.push(ROLE_HOST);
                    host_lp_slots.push(lp_slot);
                }
                NodeKind::Switch => {
                    node_kinds.push(ROLE_SWITCH);
                    switch_lp_slots.push(lp_slot);
                }
            }
        }

        let lp_count = image.nodes.len();
        Ok(Self {
            node_ids,
            node_kinds,
            state_slots,
            host_lp_slots,
            switch_lp_slots,
            next_event_time_ns: vec![u64::MAX; lp_count],
            active: vec![0; lp_count],
            index_slot: vec![u32::MAX; lp_count],
        })
    }
}

/// Explicit host-visible failure from the bounded spike harness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetalSpikeError {
    InvalidBenchmarkConfig(&'static str),
    PrimitiveMismatch(&'static str),
    TooManyLogicalProcesses,
}

impl std::fmt::Display for MetalSpikeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBenchmarkConfig(message) => formatter.write_str(message),
            Self::PrimitiveMismatch(message) => formatter.write_str(message),
            Self::TooManyLogicalProcesses => {
                formatter.write_str("Metal spike requires logical-process slots to fit u32")
            }
        }
    }
}

impl std::error::Error for MetalSpikeError {}

/// Device evidence for every primitive required by T13.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalCorrectnessReport {
    pub fixed_width_u64: bool,
    pub event_key_total_order: bool,
    pub exclusive_lp_ownership: bool,
    pub role_worklists_from_one_image: bool,
    pub bounded_fel: bool,
    pub bounded_outbox: bool,
    pub deterministic_compaction: bool,
    pub device_horizon_between_dispatches: bool,
    pub explicit_inter_dispatch_barrier: bool,
    pub explicit_device_errors: bool,
    pub packet_in_event_fusion: bool,
}

/// Raw completion timing for one dependent dispatch batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchBatchMeasurement {
    pub dispatches: usize,
    pub wall_time_ns: Vec<u64>,
    pub device_time_ns: Vec<u64>,
    pub dependency_chain_verified: bool,
}

impl DispatchBatchMeasurement {
    pub fn median_wall_ns(&self) -> u64 {
        median(&self.wall_time_ns)
    }

    pub fn median_wall_ns_per_dispatch(&self) -> f64 {
        self.median_wall_ns() as f64 / self.dispatches as f64
    }

    pub fn median_device_ns_per_dispatch(&self) -> f64 {
        median(&self.device_time_ns) as f64 / self.dispatches as f64
    }
}

/// Raw completion timing for device-resident safe-horizon-shaped rounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResidentRoundMeasurement {
    pub rounds: usize,
    pub dispatches: usize,
    pub wall_time_ns: Vec<u64>,
    pub device_time_ns: Vec<u64>,
    pub horizon_chain_verified: bool,
}

impl ResidentRoundMeasurement {
    pub fn median_wall_ns(&self) -> u64 {
        median(&self.wall_time_ns)
    }

    pub fn median_wall_ns_per_round(&self) -> f64 {
        self.median_wall_ns() as f64 / self.rounds as f64
    }

    pub fn median_device_ns_per_round(&self) -> f64 {
        median(&self.device_time_ns) as f64 / self.rounds as f64
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalSpikeBenchmarkConfig {
    pub dispatch_counts: Vec<usize>,
    pub samples: usize,
    pub warmup_samples: usize,
}

impl Default for MetalSpikeBenchmarkConfig {
    fn default() -> Self {
        Self {
            dispatch_counts: vec![1, 2, 4, 8, 16, 24, 32, 48],
            samples: 101,
            warmup_samples: 10,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetalSpikeBenchmarkReport {
    pub substrate: &'static str,
    pub batches: Vec<DispatchBatchMeasurement>,
    pub resident_rounds: Vec<ResidentRoundMeasurement>,
}

#[cube]
fn key_less(
    left_time: u64,
    left_phase: u32,
    left_origin: u64,
    left_seq: u64,
    right_time: u64,
    right_phase: u32,
    right_origin: u64,
    right_seq: u64,
) -> bool {
    left_time < right_time
        || (left_time == right_time
            && (left_phase < right_phase
                || (left_phase == right_phase
                    && (left_origin < right_origin
                        || (left_origin == right_origin && left_seq < right_seq)))))
}

#[cube(launch_unchecked)]
fn key_compare_kernel(
    left_time: &[u64],
    left_phase: &[u32],
    left_origin: &[u64],
    left_seq: &[u64],
    right_time: &[u64],
    right_phase: &[u32],
    right_origin: &[u64],
    right_seq: &[u64],
    output: &mut [u32],
) {
    let index = ABSOLUTE_POS;
    if index < output.len() {
        let left_is_less = key_less(
            left_time[index],
            left_phase[index],
            left_origin[index],
            left_seq[index],
            right_time[index],
            right_phase[index],
            right_origin[index],
            right_seq[index],
        );
        let right_is_less = key_less(
            right_time[index],
            right_phase[index],
            right_origin[index],
            right_seq[index],
            left_time[index],
            left_phase[index],
            left_origin[index],
            left_seq[index],
        );
        output[index] = if left_is_less {
            0u32
        } else if right_is_less {
            2u32
        } else {
            1u32
        };
    }
}

#[cube(launch_unchecked)]
fn exact_time_kernel(
    start_time_ns: &[u64],
    size_bytes: &[u64],
    rate_bps: &[u64],
    propagation_ns: &[u64],
    arrival_time_ns: &mut [u64],
    errors: &mut [u32],
) {
    let index = ABSOLUTE_POS;
    if index < arrival_time_ns.len() {
        let rate = rate_bps[index];
        let bytes = size_bytes[index];
        let max = 18446744073709551615u64;
        if rate == 0u64 {
            errors[index] = ERROR_ZERO_RATE;
        } else if bytes > max / 8000000000u64 {
            errors[index] = ERROR_TIME_OVERFLOW;
        } else {
            let numerator = bytes * 8000000000u64;
            let quotient = numerator / rate;
            let remainder = numerator % rate;
            let serialization = quotient + if remainder == 0u64 { 0u64 } else { 1u64 };
            let start = start_time_ns[index];
            let propagation = propagation_ns[index];
            if start > max - propagation || serialization > max - (start + propagation) {
                errors[index] = ERROR_TIME_OVERFLOW;
            } else {
                arrival_time_ns[index] = start + serialization + propagation;
            }
        }
    }
}

#[cube(launch_unchecked)]
fn role_worklist_kernel(
    worklist: &[u32],
    state: &mut [u64],
    owner_role: &mut [u32],
    #[comptime] role: u32,
) {
    let work_index = ABSOLUTE_POS;
    if work_index < worklist.len() {
        let lp_slot = worklist[work_index] as usize;
        state[lp_slot] += 1u64;
        owner_role[lp_slot] = role + 1u32;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn fel_kernel(
    push_time: &[u64],
    push_phase: &[u32],
    push_origin: &[u64],
    push_seq: &[u64],
    push_payload: &[u64],
    push_flow: &[u64],
    push_size: &[u64],
    push_kind: &[u32],
    push_counts: &[u32],
    fel_time: &mut [u64],
    fel_phase: &mut [u32],
    fel_origin: &mut [u64],
    fel_seq: &mut [u64],
    fel_payload: &mut [u64],
    fel_flow: &mut [u64],
    fel_size: &mut [u64],
    fel_kind: &mut [u32],
    fel_counts: &mut [u32],
    popped_time: &mut [u64],
    popped_phase: &mut [u32],
    popped_origin: &mut [u64],
    popped_seq: &mut [u64],
    popped_payload: &mut [u64],
    popped_flow: &mut [u64],
    popped_size: &mut [u64],
    popped_kind: &mut [u32],
    errors: &mut [u32],
    #[comptime] capacity: u32,
) {
    let lp = ABSOLUTE_POS;
    if lp < push_counts.len() {
        let base = lp * capacity as usize;
        let requested = push_counts[lp];
        let mut item = 0u32;
        while item < requested {
            let mut count = fel_counts[lp];
            if count >= capacity {
                if errors[lp] == ERROR_NONE {
                    errors[lp] = ERROR_FEL_OVERFLOW;
                }
            } else {
                let input = base + item as usize;
                let mut position = count;
                let mut scanning = true;
                while position > 0u32 && scanning {
                    let previous = base + position as usize - 1usize;
                    let should_shift = key_less(
                        push_time[input],
                        push_phase[input],
                        push_origin[input],
                        push_seq[input],
                        fel_time[previous],
                        fel_phase[previous],
                        fel_origin[previous],
                        fel_seq[previous],
                    );
                    if should_shift {
                        let destination = base + position as usize;
                        fel_time[destination] = fel_time[previous];
                        fel_phase[destination] = fel_phase[previous];
                        fel_origin[destination] = fel_origin[previous];
                        fel_seq[destination] = fel_seq[previous];
                        fel_payload[destination] = fel_payload[previous];
                        fel_flow[destination] = fel_flow[previous];
                        fel_size[destination] = fel_size[previous];
                        fel_kind[destination] = fel_kind[previous];
                        position -= 1u32;
                    } else {
                        scanning = false;
                    }
                }
                let destination = base + position as usize;
                fel_time[destination] = push_time[input];
                fel_phase[destination] = push_phase[input];
                fel_origin[destination] = push_origin[input];
                fel_seq[destination] = push_seq[input];
                fel_payload[destination] = push_payload[input];
                fel_flow[destination] = push_flow[input];
                fel_size[destination] = push_size[input];
                fel_kind[destination] = push_kind[input];
                count += 1u32;
                fel_counts[lp] = count;
            }
            item += 1u32;
        }

        let count = fel_counts[lp];
        if count > 0u32 {
            popped_time[lp] = fel_time[base];
            popped_phase[lp] = fel_phase[base];
            popped_origin[lp] = fel_origin[base];
            popped_seq[lp] = fel_seq[base];
            popped_payload[lp] = fel_payload[base];
            popped_flow[lp] = fel_flow[base];
            popped_size[lp] = fel_size[base];
            popped_kind[lp] = fel_kind[base];
            let mut offset = 1u32;
            while offset < count {
                let source = base + offset as usize;
                let destination = source - 1usize;
                fel_time[destination] = fel_time[source];
                fel_phase[destination] = fel_phase[source];
                fel_origin[destination] = fel_origin[source];
                fel_seq[destination] = fel_seq[source];
                fel_payload[destination] = fel_payload[source];
                fel_flow[destination] = fel_flow[source];
                fel_size[destination] = fel_size[source];
                fel_kind[destination] = fel_kind[source];
                offset += 1u32;
            }
            fel_counts[lp] = count - 1u32;
        }
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn fused_outbox_kernel(
    requested_writes: &[u32],
    input_target: &[u64],
    input_time: &[u64],
    input_phase: &[u32],
    input_origin: &[u64],
    input_seq: &[u64],
    input_payload: &[u64],
    input_flow: &[u64],
    input_size: &[u64],
    input_kind: &[u32],
    out_target: &mut [u64],
    out_time: &mut [u64],
    out_phase: &mut [u32],
    out_origin: &mut [u64],
    out_seq: &mut [u64],
    out_payload: &mut [u64],
    out_flow: &mut [u64],
    out_size: &mut [u64],
    out_kind: &mut [u32],
    out_counts: &mut [u32],
    errors: &mut [u32],
    #[comptime] capacity: u32,
) {
    let lp = ABSOLUTE_POS;
    if lp < requested_writes.len() {
        let requested = requested_writes[lp];
        let base = lp * capacity as usize;
        let mut ordinal = 0u32;
        while ordinal < requested {
            let count = out_counts[lp];
            if count >= capacity {
                if errors[lp] == ERROR_NONE {
                    errors[lp] = ERROR_OUTBOX_OVERFLOW;
                }
            } else if input_seq[lp] > 18446744073709551615u64 - ordinal as u64 {
                if errors[lp] == ERROR_NONE {
                    errors[lp] = ERROR_SEQUENCE_OVERFLOW;
                }
            } else {
                let destination = base + count as usize;
                out_target[destination] = input_target[lp];
                out_time[destination] = input_time[lp];
                out_phase[destination] = input_phase[lp];
                out_origin[destination] = input_origin[lp];
                out_seq[destination] = input_seq[lp] + ordinal as u64;
                out_payload[destination] = input_payload[lp];
                out_flow[destination] = input_flow[lp];
                out_size[destination] = input_size[lp];
                out_kind[destination] = input_kind[lp];
                out_counts[lp] = count + 1u32;
            }
            ordinal += 1u32;
        }
    }
}

#[cube(launch_unchecked)]
fn prefix_and_horizon_kernel(
    out_counts: &[u32],
    next_event_time_ns: &[u64],
    active: &[u32],
    offsets: &mut [u32],
    total: &mut [u32],
    horizon: &mut [u64],
) {
    if ABSOLUTE_POS == 0usize {
        let mut running = 0u32;
        let mut minimum = 18446744073709551615u64;
        let mut lp = 0usize;
        while lp < out_counts.len() {
            offsets[lp] = running;
            running += out_counts[lp];
            if active[lp] != 0 && next_event_time_ns[lp] < minimum {
                minimum = next_event_time_ns[lp];
            }
            lp += 1usize;
        }
        total[0usize] = running;
        horizon[0usize] = minimum;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn compact_kernel(
    counts: &[u32],
    offsets: &[u32],
    source_target: &[u64],
    source_time: &[u64],
    source_phase: &[u32],
    source_origin: &[u64],
    source_seq: &[u64],
    source_payload: &[u64],
    source_flow: &[u64],
    source_size: &[u64],
    source_kind: &[u32],
    compact_target: &mut [u64],
    compact_time: &mut [u64],
    compact_phase: &mut [u32],
    compact_origin: &mut [u64],
    compact_seq: &mut [u64],
    compact_payload: &mut [u64],
    compact_flow: &mut [u64],
    compact_size: &mut [u64],
    compact_kind: &mut [u32],
    #[comptime] capacity: u32,
) {
    let lp = ABSOLUTE_POS;
    if lp < counts.len() {
        let base = lp * capacity as usize;
        let destination_base = offsets[lp] as usize;
        let mut ordinal = 0u32;
        while ordinal < counts[lp] {
            let source = base + ordinal as usize;
            let destination = destination_base + ordinal as usize;
            compact_target[destination] = source_target[source];
            compact_time[destination] = source_time[source];
            compact_phase[destination] = source_phase[source];
            compact_origin[destination] = source_origin[source];
            compact_seq[destination] = source_seq[source];
            compact_payload[destination] = source_payload[source];
            compact_flow[destination] = source_flow[source];
            compact_size[destination] = source_size[source];
            compact_kind[destination] = source_kind[source];
            ordinal += 1u32;
        }
    }
}

#[cube(launch_unchecked)]
fn consume_horizon_kernel(horizon: &[u64], observed: &mut [u64]) {
    if ABSOLUTE_POS == 0usize {
        observed[0usize] = horizon[0usize];
    }
}

#[cube(launch_unchecked)]
fn dependent_dispatch_kernel(chain: &mut [u64]) {
    if ABSOLUTE_POS == 0usize {
        chain[0usize] += 1u64;
    }
}

#[cube(launch_unchecked)]
fn advance_frontier_kernel(
    next_event_time_ns: &mut [u64],
    horizon: &[u64],
    #[comptime] lookahead_ns: u64,
) {
    let lp = ABSOLUTE_POS;
    if lp < next_event_time_ns.len() && next_event_time_ns[lp] == horizon[0usize] {
        next_event_time_ns[lp] += lookahead_ns;
    }
}

#[cube(launch_unchecked)]
fn reduce_horizon_kernel(
    next_event_time_ns: &[u64],
    active: &[u32],
    horizon: &mut [u64],
    #[comptime] width: u32,
) {
    let lane = UNIT_POS as usize;
    let mut minima = Shared::<[u64]>::new_slice(width as usize);
    minima[lane] = if lane < next_event_time_ns.len() && active[lane] != 0u32 {
        next_event_time_ns[lane]
    } else {
        18446744073709551615u64
    };
    sync_cube();

    let stride = RuntimeCell::<u32>::new(width / 2u32);
    while stride.read() > 0u32 {
        let current_stride = stride.read();
        if UNIT_POS < current_stride {
            let right = lane + current_stride as usize;
            if minima[right] < minima[lane] {
                minima[lane] = minima[right];
            }
        }
        sync_cube();
        stride.store(current_stride / 2u32);
    }
    if UNIT_POS == 0u32 {
        horizon[0usize] = minima[0usize];
    }
}

fn client() -> Client {
    MetalRuntime::client(&MetalDevice::DefaultDevice)
}

fn u64_buffer(client: &Client, values: &[u64]) -> cubecl::server::Handle {
    client.create_from_slice(u64::as_bytes(values))
}

fn u32_buffer(client: &Client, values: &[u32]) -> cubecl::server::Handle {
    client.create_from_slice(u32::as_bytes(values))
}

fn empty_u64(client: &Client, len: usize) -> cubecl::server::Handle {
    client.empty(len * std::mem::size_of::<u64>())
}

fn empty_u32(client: &Client, len: usize) -> cubecl::server::Handle {
    client.empty(len * std::mem::size_of::<u32>())
}

fn read_u64(client: &Client, handle: &cubecl::server::Handle) -> Vec<u64> {
    u64::from_bytes(&client.read_one_unchecked(handle.clone())).to_vec()
}

fn read_u32(client: &Client, handle: &cubecl::server::Handle) -> Vec<u32> {
    u32::from_bytes(&client.read_one_unchecked(handle.clone())).to_vec()
}

unsafe fn arg<R: Runtime, T: CubeElement>(
    handle: cubecl::server::Handle,
    len: usize,
) -> BufferArg<R> {
    debug_assert_eq!(
        u64::try_from(
            len.checked_mul(std::mem::size_of::<T>())
                .expect("spike buffer byte length must fit usize")
        )
        .expect("spike buffer byte length must fit u64"),
        handle.size_in_used()
    );
    unsafe { BufferArg::from_raw_parts(handle, len) }
}

fn launch_key_comparison(client: &Client) -> Result<bool, MetalSpikeError> {
    let left_time = u64_buffer(client, &[0, 5, 5, 5, u64::MAX, 9]);
    let left_phase = u32_buffer(client, &[0, 0, 1, 1, u16::MAX as u32, 2]);
    let left_origin = u64_buffer(client, &[0, 1, 1, 2, u64::MAX, 7]);
    let left_seq = u64_buffer(client, &[0, 1, 2, 1, u64::MAX, 8]);
    let right_time = u64_buffer(client, &[1, 5, 5, 5, u64::MAX, 9]);
    let right_phase = u32_buffer(client, &[0, 1, 1, 1, u16::MAX as u32, 2]);
    let right_origin = u64_buffer(client, &[0, 1, 2, 1, u64::MAX, 7]);
    let right_seq = u64_buffer(client, &[0, 1, 1, 2, u64::MAX, 8]);
    let output = empty_u32(client, 6);
    unsafe {
        key_compare_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u64>(left_time, 6),
            arg::<MetalRuntime, u32>(left_phase, 6),
            arg::<MetalRuntime, u64>(left_origin, 6),
            arg::<MetalRuntime, u64>(left_seq, 6),
            arg::<MetalRuntime, u64>(right_time, 6),
            arg::<MetalRuntime, u32>(right_phase, 6),
            arg::<MetalRuntime, u64>(right_origin, 6),
            arg::<MetalRuntime, u64>(right_seq, 6),
            arg::<MetalRuntime, u32>(output.clone(), 6),
        );
    }
    Ok(read_u32(client, &output) == vec![0, 0, 0, 2, 1, 1])
}

fn launch_exact_time(client: &Client) -> Result<(bool, bool), MetalSpikeError> {
    let start = u64_buffer(client, &[10, 0, u64::MAX - 5, 0]);
    let bytes = u64_buffer(client, &[1_500, u64::MAX, 1, 1]);
    let rate = u64_buffer(client, &[100_000_000_000, 1, 1_000_000_000, 0]);
    let propagation = u64_buffer(client, &[1_000, 0, 0, 0]);
    let arrival = empty_u64(client, 4);
    let errors = u32_buffer(client, &[0, 0, 0, 0]);
    unsafe {
        exact_time_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u64>(start, 4),
            arg::<MetalRuntime, u64>(bytes, 4),
            arg::<MetalRuntime, u64>(rate, 4),
            arg::<MetalRuntime, u64>(propagation, 4),
            arg::<MetalRuntime, u64>(arrival.clone(), 4),
            arg::<MetalRuntime, u32>(errors.clone(), 4),
        );
    }
    let arrivals = read_u64(client, &arrival);
    let error_codes = read_u32(client, &errors);
    Ok((
        arrivals[0] == 1_130,
        error_codes
            == vec![
                ERROR_NONE,
                ERROR_TIME_OVERFLOW,
                ERROR_TIME_OVERFLOW,
                ERROR_ZERO_RATE,
            ],
    ))
}

fn launch_roles(
    client: &Client,
    prepared: &PreparedSpikeImage,
) -> Result<(bool, bool), MetalSpikeError> {
    let lp_count = prepared.node_ids.len();
    let state = u64_buffer(client, &vec![0; lp_count]);
    let owner_role = u32_buffer(client, &vec![0; lp_count]);
    let host_worklist = u32_buffer(client, &prepared.host_lp_slots);
    let switch_worklist = u32_buffer(client, &prepared.switch_lp_slots);
    unsafe {
        role_worklist_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u32>(host_worklist, prepared.host_lp_slots.len()),
            arg::<MetalRuntime, u64>(state.clone(), lp_count),
            arg::<MetalRuntime, u32>(owner_role.clone(), lp_count),
            ROLE_HOST,
        );
        role_worklist_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u32>(switch_worklist, prepared.switch_lp_slots.len()),
            arg::<MetalRuntime, u64>(state.clone(), lp_count),
            arg::<MetalRuntime, u32>(owner_role.clone(), lp_count),
            ROLE_SWITCH,
        );
    }
    Ok((
        read_u64(client, &state) == vec![1; lp_count],
        read_u32(client, &owner_role)
            == prepared
                .node_kinds
                .iter()
                .map(|role| role + 1)
                .collect::<Vec<_>>(),
    ))
}

fn launch_fel(client: &Client) -> Result<(bool, bool, bool), MetalSpikeError> {
    let slots = 2 * FEL_CAPACITY;
    let push_time = u64_buffer(client, &[5, 7, 5, 0, 4, 3, 2, 1]);
    let push_phase = u32_buffer(client, &[2, 0, 1, 0, 0, 0, 0, 0]);
    let push_origin = u64_buffer(client, &[1, 2, 1, 0, 4, 3, 2, 1]);
    let push_seq = u64_buffer(client, &[1, 0, 0, 0, 0, 0, 0, 0]);
    let push_payload = u64_buffer(client, &[50, 70, 51, 0, 40, 30, 20, 10]);
    let push_flow = u64_buffer(client, &[500, 700, 510, 0, 400, 300, 200, 100]);
    let push_size = u64_buffer(client, &[1_500, 9_000, 64, 0, 4, 3, 2, 1]);
    let push_kind = u32_buffer(client, &[0, 1, 1, 0, 0, 0, 0, 0]);
    let push_counts = u32_buffer(client, &[3, 5]);
    let fel_time = u64_buffer(client, &vec![0; slots]);
    let fel_phase = u32_buffer(client, &vec![0; slots]);
    let fel_origin = u64_buffer(client, &vec![0; slots]);
    let fel_seq = u64_buffer(client, &vec![0; slots]);
    let fel_payload = u64_buffer(client, &vec![0; slots]);
    let fel_flow = u64_buffer(client, &vec![0; slots]);
    let fel_size = u64_buffer(client, &vec![0; slots]);
    let fel_kind = u32_buffer(client, &vec![0; slots]);
    let fel_counts = u32_buffer(client, &[0, 0]);
    let popped_time = empty_u64(client, 2);
    let popped_phase = empty_u32(client, 2);
    let popped_origin = empty_u64(client, 2);
    let popped_seq = empty_u64(client, 2);
    let popped_payload = empty_u64(client, 2);
    let popped_flow = empty_u64(client, 2);
    let popped_size = empty_u64(client, 2);
    let popped_kind = empty_u32(client, 2);
    let errors = u32_buffer(client, &[0, 0]);
    unsafe {
        fel_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u64>(push_time, slots),
            arg::<MetalRuntime, u32>(push_phase, slots),
            arg::<MetalRuntime, u64>(push_origin, slots),
            arg::<MetalRuntime, u64>(push_seq, slots),
            arg::<MetalRuntime, u64>(push_payload, slots),
            arg::<MetalRuntime, u64>(push_flow, slots),
            arg::<MetalRuntime, u64>(push_size, slots),
            arg::<MetalRuntime, u32>(push_kind, slots),
            arg::<MetalRuntime, u32>(push_counts, 2),
            arg::<MetalRuntime, u64>(fel_time.clone(), slots),
            arg::<MetalRuntime, u32>(fel_phase.clone(), slots),
            arg::<MetalRuntime, u64>(fel_origin.clone(), slots),
            arg::<MetalRuntime, u64>(fel_seq.clone(), slots),
            arg::<MetalRuntime, u64>(fel_payload.clone(), slots),
            arg::<MetalRuntime, u64>(fel_flow.clone(), slots),
            arg::<MetalRuntime, u64>(fel_size.clone(), slots),
            arg::<MetalRuntime, u32>(fel_kind.clone(), slots),
            arg::<MetalRuntime, u32>(fel_counts.clone(), 2),
            arg::<MetalRuntime, u64>(popped_time.clone(), 2),
            arg::<MetalRuntime, u32>(popped_phase.clone(), 2),
            arg::<MetalRuntime, u64>(popped_origin.clone(), 2),
            arg::<MetalRuntime, u64>(popped_seq.clone(), 2),
            arg::<MetalRuntime, u64>(popped_payload.clone(), 2),
            arg::<MetalRuntime, u64>(popped_flow.clone(), 2),
            arg::<MetalRuntime, u64>(popped_size.clone(), 2),
            arg::<MetalRuntime, u32>(popped_kind.clone(), 2),
            arg::<MetalRuntime, u32>(errors.clone(), 2),
            FEL_CAPACITY as u32,
        );
    }
    let ordered_pop = read_u64(client, &popped_time)[0] == 5
        && read_u32(client, &popped_phase)[0] == 1
        && read_u64(client, &popped_origin)[0] == 1
        && read_u64(client, &popped_seq)[0] == 0
        && read_u32(client, &fel_counts)[0] == 2;
    let explicit_overflow = read_u32(client, &errors) == vec![0, ERROR_FEL_OVERFLOW];
    let fused_packet = read_u64(client, &popped_payload)[0] == 51
        && read_u64(client, &popped_flow)[0] == 510
        && read_u64(client, &popped_size)[0] == 64
        && read_u32(client, &popped_kind)[0] == 1;
    Ok((ordered_pop, explicit_overflow, fused_packet))
}

#[derive(Clone)]
struct OutboxHandles {
    target: cubecl::server::Handle,
    time: cubecl::server::Handle,
    phase: cubecl::server::Handle,
    origin: cubecl::server::Handle,
    seq: cubecl::server::Handle,
    payload: cubecl::server::Handle,
    flow: cubecl::server::Handle,
    size: cubecl::server::Handle,
    kind: cubecl::server::Handle,
    counts: cubecl::server::Handle,
    errors: cubecl::server::Handle,
}

fn launch_outbox(client: &Client) -> Result<(OutboxHandles, bool, bool), MetalSpikeError> {
    let lp_count = 3;
    let slots = lp_count * OUTBOX_CAPACITY;
    let requested = u32_buffer(client, &[2, 1, 3]);
    let input_target = u64_buffer(client, &[2, 0, 1]);
    let input_time = u64_buffer(client, &[10, 20, 30]);
    let input_phase = u32_buffer(client, &[0, 1, 2]);
    let input_origin = u64_buffer(client, &[0, 1, 2]);
    let input_seq = u64_buffer(client, &[5, 6, 7]);
    let input_payload = u64_buffer(client, &[100, 200, 300]);
    let input_flow = u64_buffer(client, &[11, 22, 33]);
    let input_size = u64_buffer(client, &[1_500, 64, 9_000]);
    let input_kind = u32_buffer(client, &[0, 1, 0]);
    let target = empty_u64(client, slots);
    let time = empty_u64(client, slots);
    let phase = empty_u32(client, slots);
    let origin = empty_u64(client, slots);
    let seq = empty_u64(client, slots);
    let payload = empty_u64(client, slots);
    let flow = empty_u64(client, slots);
    let size = empty_u64(client, slots);
    let kind = empty_u32(client, slots);
    let counts = u32_buffer(client, &[0, 0, 0]);
    let errors = u32_buffer(client, &[0, 0, 0]);
    unsafe {
        fused_outbox_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u32>(requested, lp_count),
            arg::<MetalRuntime, u64>(input_target, lp_count),
            arg::<MetalRuntime, u64>(input_time, lp_count),
            arg::<MetalRuntime, u32>(input_phase, lp_count),
            arg::<MetalRuntime, u64>(input_origin, lp_count),
            arg::<MetalRuntime, u64>(input_seq, lp_count),
            arg::<MetalRuntime, u64>(input_payload, lp_count),
            arg::<MetalRuntime, u64>(input_flow, lp_count),
            arg::<MetalRuntime, u64>(input_size, lp_count),
            arg::<MetalRuntime, u32>(input_kind, lp_count),
            arg::<MetalRuntime, u64>(target.clone(), slots),
            arg::<MetalRuntime, u64>(time.clone(), slots),
            arg::<MetalRuntime, u32>(phase.clone(), slots),
            arg::<MetalRuntime, u64>(origin.clone(), slots),
            arg::<MetalRuntime, u64>(seq.clone(), slots),
            arg::<MetalRuntime, u64>(payload.clone(), slots),
            arg::<MetalRuntime, u64>(flow.clone(), slots),
            arg::<MetalRuntime, u64>(size.clone(), slots),
            arg::<MetalRuntime, u32>(kind.clone(), slots),
            arg::<MetalRuntime, u32>(counts.clone(), lp_count),
            arg::<MetalRuntime, u32>(errors.clone(), lp_count),
            OUTBOX_CAPACITY as u32,
        );
    }
    let bounded = read_u32(client, &counts) == vec![2, 1, 2]
        && read_u32(client, &errors) == vec![0, 0, ERROR_OUTBOX_OVERFLOW];
    let fused = read_u64(client, &target)[..2] == [2, 2]
        && read_u64(client, &time)[..2] == [10, 10]
        && read_u32(client, &phase)[..2] == [0, 0]
        && read_u64(client, &origin)[..2] == [0, 0]
        && read_u64(client, &seq)[..2] == [5, 6]
        && read_u64(client, &payload)[..2] == [100, 100]
        && read_u64(client, &flow)[..2] == [11, 11]
        && read_u64(client, &size)[..2] == [1_500, 1_500]
        && read_u32(client, &kind)[..2] == [0, 0];
    Ok((
        OutboxHandles {
            target,
            time,
            phase,
            origin,
            seq,
            payload,
            flow,
            size,
            kind,
            counts,
            errors,
        },
        bounded,
        fused,
    ))
}

fn launch_compaction_and_horizon(
    client: &Client,
    outbox: &OutboxHandles,
) -> Result<(bool, bool, bool), MetalSpikeError> {
    let lp_count = 3;
    let compact_len = 5;
    let next_times = u64_buffer(client, &[90, 40, 70]);
    let active = u32_buffer(client, &[1, 1, 1]);
    let offsets = empty_u32(client, lp_count);
    let total = empty_u32(client, 1);
    let horizon = empty_u64(client, 1);
    let observed = empty_u64(client, 1);
    let compact_target = empty_u64(client, compact_len);
    let compact_time = empty_u64(client, compact_len);
    let compact_phase = empty_u32(client, compact_len);
    let compact_origin = empty_u64(client, compact_len);
    let compact_seq = empty_u64(client, compact_len);
    let compact_payload = empty_u64(client, compact_len);
    let compact_flow = empty_u64(client, compact_len);
    let compact_size = empty_u64(client, compact_len);
    let compact_kind = empty_u32(client, compact_len);
    unsafe {
        prefix_and_horizon_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(1),
            arg::<MetalRuntime, u32>(outbox.counts.clone(), lp_count),
            arg::<MetalRuntime, u64>(next_times, lp_count),
            arg::<MetalRuntime, u32>(active, lp_count),
            arg::<MetalRuntime, u32>(offsets.clone(), lp_count),
            arg::<MetalRuntime, u32>(total.clone(), 1),
            arg::<MetalRuntime, u64>(horizon.clone(), 1),
        );
        compact_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(32),
            arg::<MetalRuntime, u32>(outbox.counts.clone(), lp_count),
            arg::<MetalRuntime, u32>(offsets.clone(), lp_count),
            arg::<MetalRuntime, u64>(outbox.target.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(outbox.time.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u32>(outbox.phase.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(outbox.origin.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(outbox.seq.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(outbox.payload.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(outbox.flow.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(outbox.size.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u32>(outbox.kind.clone(), lp_count * OUTBOX_CAPACITY),
            arg::<MetalRuntime, u64>(compact_target.clone(), compact_len),
            arg::<MetalRuntime, u64>(compact_time.clone(), compact_len),
            arg::<MetalRuntime, u32>(compact_phase.clone(), compact_len),
            arg::<MetalRuntime, u64>(compact_origin.clone(), compact_len),
            arg::<MetalRuntime, u64>(compact_seq.clone(), compact_len),
            arg::<MetalRuntime, u64>(compact_payload.clone(), compact_len),
            arg::<MetalRuntime, u64>(compact_flow.clone(), compact_len),
            arg::<MetalRuntime, u64>(compact_size.clone(), compact_len),
            arg::<MetalRuntime, u32>(compact_kind.clone(), compact_len),
            OUTBOX_CAPACITY as u32,
        );
        consume_horizon_kernel::launch_unchecked::<MetalRuntime>(
            client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(1),
            arg::<MetalRuntime, u64>(horizon, 1),
            arg::<MetalRuntime, u64>(observed.clone(), 1),
        );
    }
    let compacted = read_u32(client, &offsets) == vec![0, 2, 3]
        && read_u32(client, &total) == vec![5]
        && read_u64(client, &compact_target) == vec![2, 2, 0, 1, 1]
        && read_u64(client, &compact_time) == vec![10, 10, 20, 30, 30];
    let fused_packet_preserved = read_u32(client, &compact_phase) == vec![0, 0, 1, 2, 2]
        && read_u64(client, &compact_origin) == vec![0, 0, 1, 2, 2]
        && read_u64(client, &compact_seq) == vec![5, 6, 6, 7, 8]
        && read_u64(client, &compact_payload) == vec![100, 100, 200, 300, 300]
        && read_u64(client, &compact_flow) == vec![11, 11, 22, 33, 33]
        && read_u64(client, &compact_size) == vec![1_500, 1_500, 64, 9_000, 9_000]
        && read_u32(client, &compact_kind) == vec![0, 0, 1, 0, 0];
    Ok((
        compacted,
        read_u64(client, &observed) == vec![40],
        fused_packet_preserved,
    ))
}

/// Runs the bounded device correctness suite and reads results only after dependent dispatches.
pub fn run_metal_correctness_suite() -> Result<MetalCorrectnessReport, MetalSpikeError> {
    let client = client();
    let prepared = PreparedSpikeImage::from_image(&spike_image())?;
    let key_order = launch_key_comparison(&client)?;
    let (exact_u64, time_errors) = launch_exact_time(&client)?;
    let (exclusive_ownership, role_worklists) = launch_roles(&client, &prepared)?;
    let (fel, fel_error, fel_packet_fusion) = launch_fel(&client)?;
    let (outbox, bounded_outbox, fused_packet) = launch_outbox(&client)?;
    let (compaction, horizon, compact_packet_fusion) =
        launch_compaction_and_horizon(&client, &outbox)?;
    let (second_outbox, second_bounded_outbox, second_fused_packet) = launch_outbox(&client)?;
    let (second_compaction, second_horizon, second_compact_packet_fusion) =
        launch_compaction_and_horizon(&client, &second_outbox)?;

    let report = MetalCorrectnessReport {
        fixed_width_u64: exact_u64
            && std::mem::size_of::<u64>() == 8
            && std::mem::size_of::<crate::EventKey>() == 32
            && std::mem::size_of::<crate::Event>() == 56
            && std::mem::size_of::<crate::PacketDescriptor>() == 32
            && std::mem::size_of::<crate::NodeDescriptor>() == 16,
        event_key_total_order: key_order,
        exclusive_lp_ownership: exclusive_ownership,
        role_worklists_from_one_image: role_worklists,
        bounded_fel: fel,
        bounded_outbox: bounded_outbox && second_bounded_outbox,
        deterministic_compaction: compaction && second_compaction,
        device_horizon_between_dispatches: horizon && second_horizon,
        // CubeCL 0.11.0-pre.1 provides serial tracked-resource ordering but no public API for
        // inserting the literal Metal barrier required by T13. The no-go report records this.
        explicit_inter_dispatch_barrier: false,
        explicit_device_errors: time_errors
            && fel_error
            && read_u32(&client, &outbox.errors) == vec![0, 0, ERROR_OUTBOX_OVERFLOW],
        packet_in_event_fusion: fel_packet_fusion
            && fused_packet
            && compact_packet_fusion
            && second_fused_packet
            && second_compact_packet_fusion,
    };

    if report.fixed_width_u64
        && report.event_key_total_order
        && report.exclusive_lp_ownership
        && report.role_worklists_from_one_image
        && report.bounded_fel
        && report.bounded_outbox
        && report.deterministic_compaction
        && report.device_horizon_between_dispatches
        && report.explicit_device_errors
        && report.packet_in_event_fusion
    {
        Ok(report)
    } else {
        Err(MetalSpikeError::PrimitiveMismatch(
            "one or more Metal primitive checks did not match the executor contract",
        ))
    }
}

fn spike_image() -> SimulationImage {
    let host = || HostState {
        egress_link: LinkId(0),
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: Vec::new(),
        next_origin_seq: 0,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    };
    let switch = |physical_switch| SwitchState {
        physical_switch,
        queues: Vec::new(),
        next_origin_seq: 0,
        arrived_packets: 0,
        dropped_packets: 0,
        departed_packets: 0,
    };
    SimulationImage {
        stop_time_ns: 0,
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(2),
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Switch,
                state_slot: 1,
            },
        ],
        host_states: vec![host(), host()],
        switch_states: vec![switch(10), switch(20)],
        flows: Vec::new(),
        initial_packets: Vec::new(),
        links: Vec::new(),
        channels: Vec::new(),
        initial_events: Vec::new(),
        seed: 7,
    }
}

/// Measures launch/completion and device-resident round chains.
///
/// CubeCL's native Metal runtime records consecutive launches on one serial compute encoder. The
/// shared buffers are hazard tracked, so each dispatch consumes the prior dispatch's writes
/// without a host sync or readback. Every requested batch here stays at or below the measured M5
/// Max command-buffer threshold. Each sample uses separate equivalent batches for direct
/// launch-plus-sync wall time and Metal command-buffer device timestamps so profiling setup is not
/// charged to the wall result.
pub fn benchmark_metal(
    config: MetalSpikeBenchmarkConfig,
) -> Result<MetalSpikeBenchmarkReport, MetalSpikeError> {
    if config.samples == 0 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "Metal benchmark needs at least one sample",
        ));
    }
    if config.dispatch_counts.is_empty()
        || config
            .dispatch_counts
            .iter()
            .any(|count| *count == 0 || *count > M5_BATCH_OP_THRESHOLD)
    {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "dispatch counts must be in 1..=50 on the measured M5 Max tier",
        ));
    }

    let client = client();
    let chain = u64_buffer(&client, &[0]);
    let mut expected_chain = 0u64;
    unsafe {
        dependent_dispatch_kernel::launch_unchecked::<MetalRuntime>(
            &client,
            CubeCount::Static(1, 1, 1),
            CubeDim::new_1d(1),
            arg::<MetalRuntime, u64>(chain.clone(), 1),
        );
    }
    expected_chain += 1;
    if read_u64(&client, &chain) != vec![expected_chain] {
        return Err(MetalSpikeError::PrimitiveMismatch(
            "dependent dispatch warmup did not complete",
        ));
    }

    let mut batches = Vec::with_capacity(config.dispatch_counts.len());
    for dispatches in config.dispatch_counts.iter().copied() {
        for _ in 0..config.warmup_samples {
            launch_dependent_batch(&client, &chain, dispatches);
            expected_chain += dispatches as u64;
            if read_u64(&client, &chain) != vec![expected_chain] {
                return Err(MetalSpikeError::PrimitiveMismatch(
                    "warmup dispatch chain lost a device dependency",
                ));
            }
        }
        let mut wall_time_ns = Vec::with_capacity(config.samples);
        let mut device_time_ns = Vec::with_capacity(config.samples);
        let mut verified = true;
        for _ in 0..config.samples {
            let started = Instant::now();
            launch_dependent_batch(&client, &chain, dispatches);
            cubecl::future::block_on(client.sync()).map_err(|_| {
                MetalSpikeError::PrimitiveMismatch("Metal launch/completion sync failed")
            })?;
            wall_time_ns.push(duration_ns(started.elapsed()));
            expected_chain += dispatches as u64;
            verified &= read_u64(&client, &chain) == vec![expected_chain];

            let (_, profile) = client
                .profile(
                    || launch_dependent_batch(&client, &chain, dispatches),
                    "t13 dependent dispatch batch",
                )
                .map_err(|_| MetalSpikeError::PrimitiveMismatch("Metal device profiling failed"))?;
            let ticks = cubecl::future::block_on(profile.resolve());
            device_time_ns.push(duration_ns(ticks.duration()));
            expected_chain += dispatches as u64;
            let actual = read_u64(&client, &chain);
            verified &= actual == vec![expected_chain];
        }
        batches.push(DispatchBatchMeasurement {
            dispatches,
            wall_time_ns,
            device_time_ns,
            dependency_chain_verified: verified,
        });
    }

    let mut round_counts = config
        .dispatch_counts
        .iter()
        .copied()
        .filter(|rounds| rounds.saturating_mul(2) <= M5_BATCH_OP_THRESHOLD)
        .collect::<Vec<_>>();
    round_counts.push(M5_BATCH_OP_THRESHOLD / 2);
    round_counts.sort_unstable();
    round_counts.dedup();
    let lp_count = 256;
    let lookahead_ns = 1_080u64;
    let next_times = u64_buffer(&client, &vec![1_000; lp_count]);
    let active = u32_buffer(&client, &vec![1; lp_count]);
    let horizon = u64_buffer(&client, &[1_000]);
    let mut expected_horizon = 1_000u64;
    let mut resident_rounds = Vec::with_capacity(round_counts.len());

    for rounds in round_counts {
        for _ in 0..config.warmup_samples {
            launch_resident_rounds(
                &client,
                &next_times,
                &active,
                &horizon,
                lp_count,
                rounds,
                lookahead_ns,
            );
            expected_horizon += rounds as u64 * lookahead_ns;
            if read_u64(&client, &horizon) != vec![expected_horizon] {
                return Err(MetalSpikeError::PrimitiveMismatch(
                    "warmup horizon chain lost a device dependency",
                ));
            }
        }
        let mut wall_time_ns = Vec::with_capacity(config.samples);
        let mut device_time_ns = Vec::with_capacity(config.samples);
        let mut verified = true;
        for _ in 0..config.samples {
            let started = Instant::now();
            launch_resident_rounds(
                &client,
                &next_times,
                &active,
                &horizon,
                lp_count,
                rounds,
                lookahead_ns,
            );
            cubecl::future::block_on(client.sync()).map_err(|_| {
                MetalSpikeError::PrimitiveMismatch("Metal launch/completion sync failed")
            })?;
            wall_time_ns.push(duration_ns(started.elapsed()));
            expected_horizon += rounds as u64 * lookahead_ns;
            verified &= read_u64(&client, &horizon) == vec![expected_horizon];

            let (_, profile) = client
                .profile(
                    || {
                        launch_resident_rounds(
                            &client,
                            &next_times,
                            &active,
                            &horizon,
                            lp_count,
                            rounds,
                            lookahead_ns,
                        )
                    },
                    "t13 resident horizon rounds",
                )
                .map_err(|_| MetalSpikeError::PrimitiveMismatch("Metal device profiling failed"))?;
            let ticks = cubecl::future::block_on(profile.resolve());
            device_time_ns.push(duration_ns(ticks.duration()));
            expected_horizon += rounds as u64 * lookahead_ns;
            let actual = read_u64(&client, &horizon);
            verified &= actual == vec![expected_horizon];
        }
        if !verified {
            return Err(MetalSpikeError::PrimitiveMismatch(
                "measured horizon chain lost a device dependency",
            ));
        }
        resident_rounds.push(ResidentRoundMeasurement {
            rounds,
            dispatches: rounds * 2,
            wall_time_ns,
            device_time_ns,
            horizon_chain_verified: verified,
        });
    }

    Ok(MetalSpikeBenchmarkReport {
        substrate: SUBSTRATE_VERSION,
        batches,
        resident_rounds,
    })
}

fn launch_dependent_batch(client: &Client, chain: &cubecl::server::Handle, dispatches: usize) {
    for _ in 0..dispatches {
        unsafe {
            dependent_dispatch_kernel::launch_unchecked::<MetalRuntime>(
                client,
                CubeCount::Static(1, 1, 1),
                CubeDim::new_1d(1),
                arg::<MetalRuntime, u64>(chain.clone(), 1),
            );
        }
    }
}

fn launch_resident_rounds(
    client: &Client,
    next_times: &cubecl::server::Handle,
    active: &cubecl::server::Handle,
    horizon: &cubecl::server::Handle,
    lp_count: usize,
    rounds: usize,
    lookahead_ns: u64,
) {
    for _ in 0..rounds {
        unsafe {
            advance_frontier_kernel::launch_unchecked::<MetalRuntime>(
                client,
                CubeCount::Static((lp_count as u32).div_ceil(64), 1, 1),
                CubeDim::new_1d(64),
                arg::<MetalRuntime, u64>(next_times.clone(), lp_count),
                arg::<MetalRuntime, u64>(horizon.clone(), 1),
                lookahead_ns,
            );
            reduce_horizon_kernel::launch_unchecked::<MetalRuntime>(
                client,
                CubeCount::Static(1, 1, 1),
                CubeDim::new_1d(lp_count as u32),
                arg::<MetalRuntime, u64>(next_times.clone(), lp_count),
                arg::<MetalRuntime, u32>(active.clone(), lp_count),
                arg::<MetalRuntime, u64>(horizon.clone(), 1),
                lp_count as u32,
            );
        }
    }
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn median(values: &[u64]) -> u64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    values[values.len() / 2]
}

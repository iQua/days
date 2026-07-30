//! T13b/T13c-only direct-Metal feasibility spike.
//!
//! This is deliberately not an executor backend. Rust-authored CubeCL kernels are compiled to MSL,
//! then command queues, buffers, pipelines, encoding, submission, timestamps, and synchronization
//! are controlled directly through `objc2-metal`. CubeCL's runtime batching policy is not used.

use std::sync::Barrier;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cubecl::Compiler;
use cubecl::metal::{MetalDevice, MetalRuntime};
use cubecl::prelude::*;
use cubecl_cpp::MslCompiler;
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::NSString;
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
    MTLComputeCommandEncoder, MTLComputePipelineState, MTLCreateSystemDefaultDevice, MTLDevice,
    MTLDispatchType, MTLLibrary, MTLResourceOptions, MTLSize,
};

use crate::metal::metal_device_execution_guard;
use crate::{EventKind, NodeId};

#[link(name = "CoreGraphics", kind = "framework")]
unsafe extern "C" {}

/// Exact direct binding selected by this spike.
pub const SUBSTRATE_VERSION: &str =
    "objc2-metal 0.3.2 direct control; CubeCL 0.11.0-pre.1 Rust-to-MSL codegen only";
/// Rounded k32 mean active port-LP population from the retained P05c run.
pub const ACTIVE_PORT_LPS: usize = 595;
/// One padded Metal threadgroup used by the round body and each reduction level.
pub const REDUCTION_LANES: usize = 1_024;
/// Exact u64 threadgroup scratch used by the one-dispatch reduction.
pub const REDUCTION_THREADGROUP_BYTES: usize = REDUCTION_LANES * std::mem::size_of::<u64>();
/// The two-level reduction supports one full leaf grid followed by one final threadgroup.
pub const MAX_SWEEP_ACTIVE_LPS: usize = REDUCTION_LANES * REDUCTION_LANES;
/// Long-resident default. Dense scale needs 49 command buffers, below Metal's default queue cap.
pub const DEFAULT_ROUNDS_PER_ENCODING: usize = 16_384;
const MAX_OUTSTANDING_COMMAND_BUFFERS: usize = 64;
const MAX_TRANSITIONS_PER_LP: u32 = 11;
const OBSERVED_TAIL_MAX_TRANSITIONS_PER_LP: u32 = 21;
const FUSED_EVENT_PACKET_WORDS: usize = 11;
const FUSED_EVENT_PACKET_BYTES: usize = FUSED_EVENT_PACKET_WORDS * std::mem::size_of::<u64>();
const EVENT_TIME: usize = 0;
const EVENT_PHASE: usize = 1;
const EVENT_ORIGIN: usize = 2;
const EVENT_SEQUENCE: usize = 3;
const EVENT_TARGET: usize = 4;
const EVENT_KIND: usize = 5;
const EVENT_PAYLOAD: usize = 6;
const PACKET_ID: usize = 7;
const PACKET_FLOW: usize = 8;
const PACKET_SIZE: usize = 9;
const PACKET_KIND: usize = 10;
const LOOKAHEAD_NS: u64 = 1_080;
const ERROR_NONE: u32 = 0;
const ERROR_ZERO_OCCUPANCY: u32 = 1;
const ERROR_OUTBOX_OVERFLOW: u32 = 2;
const ERROR_TRANSITION_OVERFLOW: u32 = 3;
const REAL_ERROR_KEY_OUTSIDE_HORIZON: u32 = 1 << 0;
const REAL_ERROR_HORIZON_STATE_MISMATCH: u32 = 1 << 1;
const REAL_ERROR_EVENT_KIND_MISMATCH: u32 = 1 << 2;
const REAL_ERROR_KEY_ORDER_MISMATCH: u32 = 1 << 3;
const REAL_ERROR_MINIMUM_MISMATCH: u32 = 1 << 4;
const REPLAY_KIND_MASK: u32 = 0b11;
const REPLAY_DIRECT_CONTINUATION_BIT: u32 = 1 << 2;
const REPLAY_LOCAL_PUSH_SHIFT: u32 = 3;
const REPLAY_REMOTE_WRITE_SHIFT: u32 = 7;
const REPLAY_CHILD_COUNT_MASK: u32 = 0b1111;
const REPLAY_QUEUE_PRESENT_BIT: u32 = 1 << 11;
const REPLAY_QUEUE_SHIFT: u32 = 12;
const REPLAY_QUEUE_MASK: u32 = u16::MAX as u32;

type RawMetalBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type MetalPipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

/// One exact transition from a recorded real-image LP drain, packed for resident replay.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReplayStep(u32);

impl ReplayStep {
    pub fn new(
        kind: EventKind,
        direct_continuation: bool,
        local_fel_pushes: u8,
        remote_outbox_writes: u8,
        queue_occupancy: Option<u16>,
    ) -> Result<Self, MetalSpikeError> {
        if local_fel_pushes > REPLAY_CHILD_COUNT_MASK as u8
            || remote_outbox_writes > REPLAY_CHILD_COUNT_MASK as u8
        {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "one replay step supports at most 15 local and 15 remote children",
            ));
        }
        if (kind == EventKind::TxReady) != queue_occupancy.is_some() {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "only TxReady replay steps carry a queue occupancy",
            ));
        }
        let mut packed = kind as u32;
        if direct_continuation {
            packed |= REPLAY_DIRECT_CONTINUATION_BIT;
        }
        packed |= u32::from(local_fel_pushes) << REPLAY_LOCAL_PUSH_SHIFT;
        packed |= u32::from(remote_outbox_writes) << REPLAY_REMOTE_WRITE_SHIFT;
        if let Some(queue_occupancy) = queue_occupancy {
            packed |= REPLAY_QUEUE_PRESENT_BIT;
            packed |= u32::from(queue_occupancy) << REPLAY_QUEUE_SHIFT;
        }
        Ok(Self(packed))
    }

    pub fn kind(self) -> EventKind {
        match self.0 & REPLAY_KIND_MASK {
            0 => EventKind::PacketArrival,
            1 => EventKind::TxReady,
            2 => EventKind::TxComplete,
            3 => EventKind::RemoteArrival,
            _ => unreachable!("the two-bit replay kind is exhaustive"),
        }
    }

    pub const fn is_direct_continuation(self) -> bool {
        self.0 & REPLAY_DIRECT_CONTINUATION_BIT != 0
    }

    pub const fn local_fel_pushes(self) -> u8 {
        ((self.0 >> REPLAY_LOCAL_PUSH_SHIFT) & REPLAY_CHILD_COUNT_MASK) as u8
    }

    pub const fn remote_outbox_writes(self) -> u8 {
        ((self.0 >> REPLAY_REMOTE_WRITE_SHIFT) & REPLAY_CHILD_COUNT_MASK) as u8
    }

    pub const fn queue_occupancy(self) -> Option<u16> {
        if self.0 & REPLAY_QUEUE_PRESENT_BIT == 0 {
            None
        } else {
            Some(((self.0 >> REPLAY_QUEUE_SHIFT) & REPLAY_QUEUE_MASK) as u16)
        }
    }

    const fn packed(self) -> u32 {
        self.0
    }
}

/// Contiguous real rounds retained from one canonical safe-horizon CPU execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayTraceCapture {
    pub start_round: usize,
    pub rounds: usize,
}

impl ReplayTraceCapture {
    pub const fn contains(self, round: usize) -> bool {
        round >= self.start_round && round < self.start_round.saturating_add(self.rounds)
    }
}

/// One real image round and its CSR range in [`RealReplayTrace::lps`].
#[derive(Clone, Debug, PartialEq)]
pub struct RealReplayRound {
    pub source_round: usize,
    pub frontier_ns: u64,
    pub exclusive_horizon_ns: u128,
    pub events_processed: u64,
    pub active_lp_count: usize,
    pub maximum_events_per_lp: u32,
    pub parallel_efficiency: f64,
    pub lp_start: usize,
    pub lp_count: usize,
}

/// One real LP drain and its exact transition range in [`RealReplayTrace::steps`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RealReplayLp {
    pub node: NodeId,
    pub pending_events_below_horizon: u32,
    pub next_time_ns_after_local_drain: u64,
    pub step_start: usize,
    pub step_count: usize,
}

/// Spike-only real-image replay input.
///
/// Image lowering and the canonical CPU transition path produce this trace. The Metal spike later
/// consumes the same LP membership, step order, event kinds, child movement, queue occupancies,
/// widths, and skew without claiming to execute production transition semantics on device.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RealReplayTrace {
    pub source_round_count: usize,
    pub rounds: Vec<RealReplayRound>,
    pub lps: Vec<RealReplayLp>,
    pub steps: Vec<ReplayStep>,
}

impl RealReplayTrace {
    pub fn validate(&self) -> Result<(), MetalSpikeError> {
        let mut expected_lp_start = 0;
        let mut expected_step_start = 0;
        for round in &self.rounds {
            if round.lp_start != expected_lp_start
                || round.lp_count != round.active_lp_count
                || round.lp_start.saturating_add(round.lp_count) > self.lps.len()
            {
                return Err(MetalSpikeError::InvalidBenchmarkConfig(
                    "real replay round CSR bounds are invalid",
                ));
            }
            let rows = &self.lps[round.lp_start..round.lp_start + round.lp_count];
            let events = rows.iter().map(|row| row.step_count as u64).sum::<u64>();
            let maximum = rows.iter().map(|row| row.step_count).max().unwrap_or(0);
            if events != round.events_processed || maximum != round.maximum_events_per_lp as usize {
                return Err(MetalSpikeError::InvalidBenchmarkConfig(
                    "real replay round aggregates do not match its LP rows",
                ));
            }
            for row in rows {
                if row.step_start != expected_step_start
                    || row.step_start.saturating_add(row.step_count) > self.steps.len()
                {
                    return Err(MetalSpikeError::InvalidBenchmarkConfig(
                        "real replay LP CSR bounds are invalid",
                    ));
                }
                expected_step_start += row.step_count;
            }
            expected_lp_start += round.lp_count;
        }
        if expected_lp_start != self.lps.len() || expected_step_start != self.steps.len() {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "real replay trace contains unreferenced CSR records",
            ));
        }
        Ok(())
    }

    pub fn selected_rounds(&self, indices: &[usize]) -> Result<Self, MetalSpikeError> {
        self.validate()?;
        let mut selected = Self {
            source_round_count: self.source_round_count,
            ..Self::default()
        };
        for &index in indices {
            let round = self
                .rounds
                .get(index)
                .ok_or(MetalSpikeError::InvalidBenchmarkConfig(
                    "selected replay round index is out of bounds",
                ))?;
            let lp_start = selected.lps.len();
            for row in &self.lps[round.lp_start..round.lp_start + round.lp_count] {
                let step_start = selected.steps.len();
                selected.steps.extend_from_slice(
                    &self.steps[row.step_start..row.step_start + row.step_count],
                );
                selected.lps.push(RealReplayLp { step_start, ..*row });
            }
            selected.rounds.push(RealReplayRound {
                lp_start,
                ..round.clone()
            });
        }
        selected.validate()?;
        Ok(selected)
    }

    /// Counts one event kind in each retained source round.
    pub fn event_counts_by_round(&self, kind: EventKind) -> Result<Vec<u64>, MetalSpikeError> {
        self.validate()?;
        self.rounds
            .iter()
            .map(|round| {
                let count = self.lps[round.lp_start..round.lp_start + round.lp_count]
                    .iter()
                    .flat_map(|lp| &self.steps[lp.step_start..lp.step_start + lp.step_count])
                    .filter(|step| step.kind() == kind)
                    .count();
                u64::try_from(count).map_err(|_| {
                    MetalSpikeError::InvalidBenchmarkConfig(
                        "per-round replay event count exceeds u64",
                    )
                })
            })
            .collect()
    }
}

pub(crate) struct RecordedReplayLp {
    pub node: NodeId,
    pub pending_events_below_horizon: u32,
    pub next_time_ns_after_local_drain: u64,
    pub steps: Vec<ReplayStep>,
}

pub(crate) struct RealReplayTraceBuilder {
    capture: ReplayTraceCapture,
    trace: RealReplayTrace,
}

impl RealReplayTraceBuilder {
    pub(crate) fn new(capture: ReplayTraceCapture) -> Self {
        Self {
            capture,
            trace: RealReplayTrace::default(),
        }
    }

    pub(crate) const fn captures(&self, round: usize) -> bool {
        self.capture.contains(round)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn push_round(
        &mut self,
        source_round: usize,
        frontier_ns: u64,
        exclusive_horizon_ns: u128,
        events_processed: u64,
        parallel_efficiency: f64,
        mut rows: Vec<RecordedReplayLp>,
    ) {
        rows.sort_unstable_by_key(|row| row.node);
        let lp_start = self.trace.lps.len();
        let maximum_events_per_lp =
            rows.iter().map(|row| row.steps.len()).max().unwrap_or(0) as u32;
        for row in rows {
            let step_start = self.trace.steps.len();
            let step_count = row.steps.len();
            self.trace.steps.extend(row.steps);
            self.trace.lps.push(RealReplayLp {
                node: row.node,
                pending_events_below_horizon: row.pending_events_below_horizon,
                next_time_ns_after_local_drain: row.next_time_ns_after_local_drain,
                step_start,
                step_count,
            });
        }
        let lp_count = self.trace.lps.len() - lp_start;
        self.trace.rounds.push(RealReplayRound {
            source_round,
            frontier_ns,
            exclusive_horizon_ns,
            events_processed,
            active_lp_count: lp_count,
            maximum_events_per_lp,
            parallel_efficiency,
            lp_start,
            lp_count,
        });
    }

    pub(crate) fn finish(mut self, source_round_count: usize) -> RealReplayTrace {
        self.trace.source_round_count = source_round_count;
        self.trace
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RealReplayProfile {
    pub rounds: usize,
    pub lp_round_records: usize,
    pub transitions: u64,
    pub minimum_active_lps: usize,
    pub maximum_active_lps: usize,
    pub mean_active_lps: f64,
    pub mean_parallel_efficiency: f64,
    pub mean_achievable_speedup_ceiling: f64,
    pub maximum_events_per_lp: u32,
    pub event_kind_counts: [u64; 4],
    pub direct_continuations: u64,
    pub local_fel_pushes: u64,
    pub remote_outbox_writes: u64,
    pub tx_ready_queue_depth_sum: u64,
    pub tx_ready_queue_depth_max: u16,
    pub tx_ready_empty_checks: u64,
}

pub fn real_replay_profile(trace: &RealReplayTrace) -> Result<RealReplayProfile, MetalSpikeError> {
    trace.validate()?;
    if trace.rounds.is_empty() {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "real replay trace must contain at least one round",
        ));
    }
    let mut event_kind_counts = [0_u64; 4];
    let mut direct_continuations = 0_u64;
    let mut local_fel_pushes = 0_u64;
    let mut remote_outbox_writes = 0_u64;
    let mut tx_ready_queue_depth_sum = 0_u64;
    let mut tx_ready_queue_depth_max = 0_u16;
    let mut tx_ready_empty_checks = 0_u64;
    for step in &trace.steps {
        event_kind_counts[step.kind() as usize] += 1;
        direct_continuations += u64::from(step.is_direct_continuation());
        local_fel_pushes += u64::from(step.local_fel_pushes());
        remote_outbox_writes += u64::from(step.remote_outbox_writes());
        if let Some(depth) = step.queue_occupancy() {
            tx_ready_queue_depth_sum += u64::from(depth);
            tx_ready_queue_depth_max = tx_ready_queue_depth_max.max(depth);
            tx_ready_empty_checks += u64::from(depth == 0);
        }
    }
    let rounds = trace.rounds.len();
    let sum_active = trace
        .rounds
        .iter()
        .map(|round| round.active_lp_count as u128)
        .sum::<u128>();
    let mean_parallel_efficiency = trace
        .rounds
        .iter()
        .map(|round| round.parallel_efficiency)
        .sum::<f64>()
        / rounds as f64;
    let mean_achievable_speedup_ceiling = trace
        .rounds
        .iter()
        .map(|round| round.active_lp_count as f64 * round.parallel_efficiency)
        .sum::<f64>()
        / rounds as f64;
    Ok(RealReplayProfile {
        rounds,
        lp_round_records: trace.lps.len(),
        transitions: trace.steps.len() as u64,
        minimum_active_lps: trace
            .rounds
            .iter()
            .map(|round| round.active_lp_count)
            .min()
            .unwrap_or(0),
        maximum_active_lps: trace
            .rounds
            .iter()
            .map(|round| round.active_lp_count)
            .max()
            .unwrap_or(0),
        mean_active_lps: sum_active as f64 / rounds as f64,
        mean_parallel_efficiency,
        mean_achievable_speedup_ceiling,
        maximum_events_per_lp: trace
            .rounds
            .iter()
            .map(|round| round.maximum_events_per_lp)
            .max()
            .unwrap_or(0),
        event_kind_counts,
        direct_continuations,
        local_fel_pushes,
        remote_outbox_writes,
        tx_ready_queue_depth_sum,
        tx_ready_queue_depth_max,
        tx_ready_empty_checks,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealReplayBenchmarkConfig {
    pub samples: usize,
    pub rounds_per_encoding: usize,
    pub cpu_worker_counts: Vec<usize>,
}

impl Default for RealReplayBenchmarkConfig {
    fn default() -> Self {
        Self {
            samples: 4,
            rounds_per_encoding: DEFAULT_ROUNDS_PER_ENCODING,
            cpu_worker_counts: vec![4],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealReplaySample {
    pub host_encode_submit_ns: u64,
    pub device_ns: u64,
    pub gpu_wall_ns: u64,
    pub cpu_ns: Vec<u64>,
    pub cpu_checksums: Vec<u64>,
    pub gpu_checksum: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RealReplayBenchmarkReport {
    pub substrate: &'static str,
    pub profile: RealReplayProfile,
    pub rounds: usize,
    pub warmup_rounds: usize,
    pub padded_lanes: usize,
    pub body_threadgroups: usize,
    pub reduction_dispatches_per_round: usize,
    pub dispatches_per_round: usize,
    pub rounds_per_encoding: usize,
    pub pipeline_setup_ns: u64,
    pub cpu_worker_counts: Vec<usize>,
    pub samples: Vec<RealReplaySample>,
    pub matched_checksums: bool,
    pub no_host_sync_between_rounds: bool,
    pub resident_parent_stream_bytes: usize,
    pub local_fel_fused_bytes: usize,
    pub remote_outbox_fused_bytes: usize,
    pub trace_consistent_horizon_dependency: bool,
    pub variable_active_lp_guard: bool,
}

impl RealReplayBenchmarkReport {
    pub fn median_host_encode_submit_ns_per_round(&self) -> f64 {
        median_ns_per_round(
            &self
                .samples
                .iter()
                .map(|sample| sample.host_encode_submit_ns)
                .collect::<Vec<_>>(),
            self.rounds,
        )
    }

    pub fn median_device_ns_per_round(&self) -> f64 {
        median_ns_per_round(
            &self
                .samples
                .iter()
                .map(|sample| sample.device_ns)
                .collect::<Vec<_>>(),
            self.rounds,
        )
    }

    pub fn median_gpu_wall_ns_per_round(&self) -> f64 {
        median_ns_per_round(
            &self
                .samples
                .iter()
                .map(|sample| sample.gpu_wall_ns)
                .collect::<Vec<_>>(),
            self.rounds,
        )
    }

    pub fn median_cpu_ns_per_round(&self, workers: usize) -> Option<f64> {
        let worker = self
            .cpu_worker_counts
            .iter()
            .position(|candidate| *candidate == workers)?;
        Some(median_ns_per_round(
            &self
                .samples
                .iter()
                .map(|sample| sample.cpu_ns[worker])
                .collect::<Vec<_>>(),
            self.rounds,
        ))
    }
}

/// One T13c width and its bounded timing protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SweepPoint {
    pub active_lps: usize,
    pub rounds: usize,
    pub warmup_rounds: usize,
}

/// Exact repetitions of the retained 595-LP profile, chosen near the requested widths.
pub const DEFAULT_SWEEP_POINTS: [SweepPoint; 7] = [
    SweepPoint {
        active_lps: 595,
        rounds: 16_384,
        warmup_rounds: 16_384,
    },
    SweepPoint {
        active_lps: 1_785,
        rounds: 4_096,
        warmup_rounds: 4_096,
    },
    SweepPoint {
        active_lps: 4_760,
        rounds: 2_048,
        warmup_rounds: 2_048,
    },
    SweepPoint {
        active_lps: 20_230,
        rounds: 512,
        warmup_rounds: 512,
    },
    SweepPoint {
        active_lps: 49_980,
        rounds: 256,
        warmup_rounds: 256,
    },
    SweepPoint {
        active_lps: 199_920,
        rounds: 64,
        warmup_rounds: 64,
    },
    SweepPoint {
        active_lps: 499_800,
        rounds: 32,
        warmup_rounds: 32,
    },
];

/// Dispatch geometry and the explicitly modeled occupancy proxies used by the evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SweepGeometry {
    pub active_lps: usize,
    pub padded_lanes: usize,
    pub body_threadgroups: usize,
    pub reduction_dispatches_per_round: usize,
    pub dispatches_per_round: usize,
}

impl SweepGeometry {
    /// Useful lanes divided by one 1,024-lane threadgroup per GPU core, capped at saturation.
    pub fn modeled_useful_lane_coverage_ppm(&self, gpu_cores: usize) -> u32 {
        coverage_ppm(self.active_lps, gpu_cores.saturating_mul(REDUCTION_LANES))
    }

    /// Body threadgroups divided by GPU cores, capped at saturation.
    pub fn modeled_threadgroup_core_coverage_ppm(&self, gpu_cores: usize) -> u32 {
        coverage_ppm(self.body_threadgroups, gpu_cores)
    }
}

pub fn sweep_geometry(active_lps: usize) -> Result<SweepGeometry, MetalSpikeError> {
    if active_lps == 0 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "active-LP width must be nonzero",
        ));
    }
    if active_lps > MAX_SWEEP_ACTIVE_LPS {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "active-LP width exceeds the two-level reduction capacity",
        ));
    }
    let body_threadgroups = active_lps.div_ceil(REDUCTION_LANES);
    let reduction_dispatches_per_round = if body_threadgroups == 1 { 1 } else { 2 };
    Ok(SweepGeometry {
        active_lps,
        padded_lanes: body_threadgroups * REDUCTION_LANES,
        body_threadgroups,
        reduction_dispatches_per_round,
        dispatches_per_round: 1 + reduction_dispatches_per_round,
    })
}

/// Rounded, reproducible version of the measured k32 port-LP round profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MatchedWorkloadProfile {
    pub active_lps: usize,
    pub reduction_lanes: usize,
    pub transitions_per_round: u64,
    pub fel_pops_per_round: u64,
    pub local_child_pushes_per_round: u64,
    pub same_time_continuations_per_round: u64,
    pub occupancy_checks_per_round: u64,
    pub outbox_writes_per_round: u64,
    pub maximum_transitions_per_lp: u32,
    pub observed_tail_maximum_transitions_per_lp: u32,
    pub fused_event_packet_bytes: usize,
    pub measured_mean_parallel_efficiency_ppm: u32,
    pub modeled_parallel_efficiency_ppm: u32,
}

/// Returns the exact integer workload used on both CPU and GPU.
pub const fn matched_workload_profile() -> MatchedWorkloadProfile {
    MatchedWorkloadProfile {
        active_lps: ACTIVE_PORT_LPS,
        reduction_lanes: REDUCTION_LANES,
        transitions_per_round: 1_300,
        fel_pops_per_round: 1_067,
        local_child_pushes_per_round: 668,
        same_time_continuations_per_round: 233,
        occupancy_checks_per_round: 399,
        outbox_writes_per_round: 399,
        maximum_transitions_per_lp: MAX_TRANSITIONS_PER_LP,
        observed_tail_maximum_transitions_per_lp: OBSERVED_TAIL_MAX_TRANSITIONS_PER_LP,
        fused_event_packet_bytes: FUSED_EVENT_PACKET_BYTES,
        measured_mean_parallel_efficiency_ppm: 208_296,
        modeled_parallel_efficiency_ppm: 198_625,
    }
}

/// Scales only by exact repetitions so every synthetic LP keeps a retained joint-profile body.
pub fn scaled_workload_profile(
    active_lps: usize,
) -> Result<MatchedWorkloadProfile, MetalSpikeError> {
    let geometry = sweep_geometry(active_lps)?;
    if !active_lps.is_multiple_of(ACTIVE_PORT_LPS) {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "active-LP width must be an exact multiple of the retained 595-LP profile",
        ));
    }
    let scale = u64::try_from(active_lps / ACTIVE_PORT_LPS).map_err(|_| {
        MetalSpikeError::InvalidBenchmarkConfig("active-LP scale does not fit in u64")
    })?;
    let base = matched_workload_profile();
    Ok(MatchedWorkloadProfile {
        active_lps,
        reduction_lanes: geometry.padded_lanes,
        transitions_per_round: base.transitions_per_round * scale,
        fel_pops_per_round: base.fel_pops_per_round * scale,
        local_child_pushes_per_round: base.local_child_pushes_per_round * scale,
        same_time_continuations_per_round: base.same_time_continuations_per_round * scale,
        occupancy_checks_per_round: base.occupancy_checks_per_round * scale,
        outbox_writes_per_round: base.outbox_writes_per_round * scale,
        ..base
    })
}

/// Explicit host-visible failure from the bounded spike harness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetalSpikeError {
    InvalidBenchmarkConfig(&'static str),
    Metal(String),
    PrimitiveMismatch(&'static str),
    StateMismatch(String),
}

impl std::fmt::Display for MetalSpikeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBenchmarkConfig(message) | Self::PrimitiveMismatch(message) => {
                formatter.write_str(message)
            }
            Self::Metal(message) | Self::StateMismatch(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for MetalSpikeError {}

/// Substrate-affected primitive checks plus the carried T13 primitive ruling.
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
    pub serial_encoder_dependency_verified: bool,
    pub single_dispatch_1024_lane_reduction: bool,
    pub semantic_same_time_continuation_verified: bool,
    pub continuation_slot_association_verified: bool,
    pub matched_cpu_gpu_state: bool,
}

/// Gate protocol. The decision run requires one warmup and exactly three measured samples.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalSpikeBenchmarkConfig {
    pub sweep_points: Vec<SweepPoint>,
    pub rounds_per_encoding: usize,
    pub samples: usize,
    pub cpu_workers: usize,
}

impl Default for MetalSpikeBenchmarkConfig {
    fn default() -> Self {
        Self {
            sweep_points: DEFAULT_SWEEP_POINTS.to_vec(),
            rounds_per_encoding: DEFAULT_ROUNDS_PER_ENCODING,
            samples: 3,
            cpu_workers: 4,
        }
    }
}

/// Raw paired samples for one active-LP width.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateScaleMeasurement {
    pub active_lps: usize,
    pub padded_lanes: usize,
    pub body_threadgroups: usize,
    pub reduction_dispatches_per_round: usize,
    pub rounds: usize,
    pub warmup_rounds: usize,
    pub encodings: usize,
    pub dispatches_per_round: usize,
    pub workload: MatchedWorkloadProfile,
    pub pipeline_setup_ns: u64,
    /// Direct host command-buffer creation, encoding, ending, and commit.
    pub host_encode_submit_ns: Vec<u64>,
    /// Metal `GPUStartTime` to `GPUEndTime`, summed over the committed buffers.
    pub device_ns: Vec<u64>,
    /// Host encode start through final command-buffer completion. This already includes device time.
    pub gpu_wall_ns: Vec<u64>,
    /// Identical flattened LP body on the requested persistent CPU worker count.
    pub matched_cpu_ns: Vec<u64>,
    pub cpu_checksums: Vec<u64>,
    pub gpu_checksums: Vec<u64>,
    pub matched_checksums: bool,
    pub no_host_sync_between_rounds: bool,
}

impl GateScaleMeasurement {
    pub fn median_host_encode_submit_ns(&self) -> u64 {
        median(&self.host_encode_submit_ns)
    }

    pub fn median_device_ns(&self) -> u64 {
        median(&self.device_ns)
    }

    pub fn median_gpu_wall_ns(&self) -> u64 {
        median(&self.gpu_wall_ns)
    }

    pub fn median_matched_cpu_ns(&self) -> u64 {
        median(&self.matched_cpu_ns)
    }

    pub fn median_host_ns_per_round(&self) -> f64 {
        self.median_host_encode_submit_ns() as f64 / self.rounds as f64
    }

    pub fn median_device_ns_per_round(&self) -> f64 {
        self.median_device_ns() as f64 / self.rounds as f64
    }

    pub fn median_gpu_wall_ns_per_round(&self) -> f64 {
        self.median_gpu_wall_ns() as f64 / self.rounds as f64
    }

    pub fn median_matched_cpu_ns_per_round(&self) -> f64 {
        self.median_matched_cpu_ns() as f64 / self.rounds as f64
    }

    pub fn median_device_events_per_second(&self) -> f64 {
        events_per_second(
            self.workload.transitions_per_round,
            self.median_device_ns_per_round(),
        )
    }

    pub fn median_gpu_wall_events_per_second(&self) -> f64 {
        events_per_second(
            self.workload.transitions_per_round,
            self.median_gpu_wall_ns_per_round(),
        )
    }

    pub fn median_matched_cpu_events_per_second(&self) -> f64 {
        events_per_second(
            self.workload.transitions_per_round,
            self.median_matched_cpu_ns_per_round(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalSpikeBenchmarkReport {
    pub substrate: &'static str,
    pub pipeline_setup_ns: u64,
    pub rounds_per_encoding: usize,
    pub cpu_workers: usize,
    pub workload: MatchedWorkloadProfile,
    pub scales: Vec<GateScaleMeasurement>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WorkloadState {
    active_lps: usize,
    padded_lanes: usize,
    // Buffer 0 and 1 intentionally match the reduction kernel's bindings.
    next_time: Vec<u64>,
    horizon: Vec<u64>,
    // Two bounded FEL slots per LP. Each slot is the exact 88-byte Event+Packet fused record:
    // EventKey, target, event kind, payload id, then the inlined PacketDescriptor.
    fel_fused: Vec<u64>,
    queue_depth: Vec<u32>,
    queue_head: Vec<u32>,
    transitions: Vec<u32>,
    continuations: Vec<u32>,
    local_push_plan: Vec<u32>,
    occupancy_plan: Vec<u32>,
    outbox_plan: Vec<u32>,
    outbox_fused: Vec<u64>,
    outbox_count: Vec<u32>,
    errors: Vec<u32>,
    semantic_flags: Vec<u32>,
    audit: Vec<u64>,
}

impl WorkloadState {
    fn initial(active_lps: usize) -> Result<Self, MetalSpikeError> {
        let geometry = sweep_geometry(active_lps)?;
        let profile_scale = active_lps / ACTIVE_PORT_LPS;
        if profile_scale * ACTIVE_PORT_LPS != active_lps {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "active-LP width must repeat the complete retained profile",
            ));
        }
        let joint_profile = joint_transition_profile();
        let mut transitions = vec![0; geometry.padded_lanes];
        let mut continuations = vec![0; geometry.padded_lanes];
        let mut local_push_plan = vec![0; geometry.padded_lanes];
        let mut occupancy_plan = vec![0; geometry.padded_lanes];
        let mut outbox_plan = vec![0; geometry.padded_lanes];
        for block in 0..profile_scale {
            let block_start = block * ACTIVE_PORT_LPS;
            for (ordinal, work) in joint_profile.iter().copied().enumerate() {
                let lane = block_start + permuted_profile_lane(ordinal, 233, 0);
                transitions[lane] = work.transitions;
                continuations[lane] = work.continuations;
                local_push_plan[lane] = work.local_pushes;
                occupancy_plan[lane] = work.occupancy_checks;
                outbox_plan[lane] = work.outbox_writes;
            }
        }

        let mut fel_fused = vec![0; 2 * geometry.padded_lanes * FUSED_EVENT_PACKET_WORDS];
        let mut queue_depth = vec![0; geometry.padded_lanes];
        let mut queue_head = vec![0; geometry.padded_lanes];
        for lane in 0..active_lps {
            let profile_lane = lane % ACTIVE_PORT_LPS;
            let block_start = lane - profile_lane;
            let base_time = 1_000 + (profile_lane % 7) as u64;
            for slot in 0..2 {
                let base = fused_fel_offset(slot, lane, geometry.padded_lanes);
                // Equal-time cases deliberately exercise phase, origin, and sequence tie breaks.
                fel_fused[base + EVENT_TIME] =
                    base_time + u64::from(profile_lane.is_multiple_of(4) && slot == 1);
                fel_fused[base + EVENT_PHASE] =
                    u64::from(slot == 1 && !profile_lane.is_multiple_of(4));
                fel_fused[base + EVENT_ORIGIN] =
                    lane as u64 + u64::from(slot == 1 && profile_lane % 4 >= 2);
                fel_fused[base + EVENT_SEQUENCE] = ((lane as u64) << 32) + u64::from(slot == 1);
                fel_fused[base + EVENT_TARGET] =
                    (block_start + permuted_profile_lane(profile_lane, 337, 17)) as u64;
                fel_fused[base + EVENT_KIND] = slot as u64;
                fel_fused[base + EVENT_PAYLOAD] = 10_000 + lane as u64;
                fel_fused[base + PACKET_ID] = 10_000 + lane as u64;
                fel_fused[base + PACKET_FLOW] = 20_000 + (profile_lane % 4_096) as u64;
                fel_fused[base + PACKET_SIZE] = [64, 1_000, 1_500, 9_000][profile_lane % 4];
                fel_fused[base + PACKET_KIND] = (profile_lane % 2) as u64;
            }
            queue_depth[lane] = 2 + (profile_lane % 31) as u32;
            queue_head[lane] = profile_lane as u32 % queue_depth[lane];
        }
        let mut next_time = vec![u64::MAX; geometry.padded_lanes];
        for (lane, time) in next_time.iter_mut().enumerate().take(active_lps) {
            let left = fused_fel_offset(0, lane, geometry.padded_lanes);
            let right = fused_fel_offset(1, lane, geometry.padded_lanes);
            *time = if fused_event_less(&fel_fused, left, right) {
                fel_fused[left + EVENT_TIME]
            } else {
                fel_fused[right + EVENT_TIME]
            };
        }

        let state = Self {
            active_lps,
            padded_lanes: geometry.padded_lanes,
            next_time,
            horizon: vec![2_080],
            fel_fused,
            queue_depth,
            queue_head,
            transitions,
            continuations,
            local_push_plan,
            occupancy_plan,
            outbox_plan,
            outbox_fused: vec![0; geometry.padded_lanes * FUSED_EVENT_PACKET_WORDS],
            outbox_count: vec![0; geometry.padded_lanes],
            errors: vec![0; geometry.padded_lanes],
            semantic_flags: vec![0; geometry.padded_lanes],
            audit: vec![0; geometry.padded_lanes],
        };
        state.assert_profile();
        Ok(state)
    }

    fn assert_profile(&self) {
        let profile =
            scaled_workload_profile(self.active_lps).expect("initialized workload width is valid");
        assert_eq!(
            self.transitions[..self.active_lps]
                .iter()
                .map(|value| u64::from(*value))
                .sum::<u64>(),
            profile.transitions_per_round
        );
        assert_eq!(
            self.continuations[..self.active_lps]
                .iter()
                .map(|value| u64::from(*value))
                .sum::<u64>(),
            profile.same_time_continuations_per_round
        );
        assert_eq!(
            self.transitions[..self.active_lps]
                .iter()
                .zip(&self.continuations)
                .map(|(transitions, continuations)| u64::from(transitions - continuations))
                .sum::<u64>(),
            profile.fel_pops_per_round
        );
        assert_eq!(
            self.local_push_plan[..self.active_lps]
                .iter()
                .map(|value| u64::from(*value))
                .sum::<u64>(),
            profile.local_child_pushes_per_round
        );
        assert_eq!(
            self.occupancy_plan[..self.active_lps]
                .iter()
                .map(|value| u64::from(*value))
                .sum::<u64>(),
            profile.occupancy_checks_per_round
        );
        assert_eq!(
            self.outbox_plan[..self.active_lps]
                .iter()
                .map(|value| u64::from(*value))
                .sum::<u64>(),
            profile.outbox_writes_per_round
        );
        assert_eq!(
            *self.transitions[..self.active_lps]
                .iter()
                .max()
                .expect("active profile must not be empty"),
            profile.maximum_transitions_per_lp
        );
    }

    fn planes(&self) -> Vec<&[u8]> {
        vec![
            bytes(&self.next_time),
            bytes(&self.horizon),
            bytes(&self.fel_fused),
            bytes(&self.queue_depth),
            bytes(&self.queue_head),
            bytes(&self.transitions),
            bytes(&self.continuations),
            bytes(&self.local_push_plan),
            bytes(&self.occupancy_plan),
            bytes(&self.outbox_plan),
            bytes(&self.outbox_fused),
            bytes(&self.outbox_count),
            bytes(&self.errors),
            bytes(&self.semantic_flags),
            bytes(&self.audit),
        ]
    }
}

#[derive(Clone, Copy)]
struct JointLpWork {
    transitions: u32,
    continuations: u32,
    local_pushes: u32,
    occupancy_checks: u32,
    outbox_writes: u32,
}

fn joint_transition_profile() -> Vec<JointLpWork> {
    let mut profile = Vec::with_capacity(ACTIVE_PORT_LPS);
    let mut extend = |count, work| profile.extend(std::iter::repeat_n(work, count));

    // Moment-matched reconstruction of the retained real k32 joint LP histogram. Every tuple used
    // here occurs in that trace. Rare >11-transition records are folded into the 11-transition
    // bucket so one run-wide tail is not charged to every synthetic round.
    extend(
        30,
        JointLpWork {
            transitions: 1,
            continuations: 0,
            local_pushes: 0,
            occupancy_checks: 0,
            outbox_writes: 0,
        },
    );
    extend(
        166,
        JointLpWork {
            transitions: 1,
            continuations: 0,
            local_pushes: 0,
            occupancy_checks: 0,
            outbox_writes: 0,
        },
    );
    extend(
        153,
        JointLpWork {
            transitions: 2,
            continuations: 0,
            local_pushes: 2,
            occupancy_checks: 1,
            outbox_writes: 1,
        },
    );
    for (packet_arrivals, count) in [(4, 1), (5, 1), (6, 1), (7, 2), (8, 2), (9, 2), (10, 4)] {
        extend(
            count,
            JointLpWork {
                transitions: packet_arrivals + 1,
                continuations: 0,
                local_pushes: packet_arrivals + 2,
                occupancy_checks: 1,
                outbox_writes: 1,
            },
        );
    }
    for (transitions, count) in [
        (2, 137),
        (3, 58),
        (4, 12),
        (5, 8),
        (6, 5),
        (7, 4),
        (8, 2),
        (9, 2),
        (10, 1),
        (11, 4),
    ] {
        extend(
            count,
            JointLpWork {
                transitions,
                continuations: 1,
                local_pushes: 1,
                occupancy_checks: 1,
                outbox_writes: 1,
            },
        );
    }
    assert_eq!(profile.len(), ACTIVE_PORT_LPS);
    profile
}

fn permuted_profile_lane(ordinal: usize, multiplier: usize, offset: usize) -> usize {
    (ordinal * multiplier + offset) % ACTIVE_PORT_LPS
}

fn fused_fel_offset(slot: usize, lane: usize, padded_lanes: usize) -> usize {
    (slot * padded_lanes + lane) * FUSED_EVENT_PACKET_WORDS
}

fn fused_event_less(events: &[u64], left: usize, right: usize) -> bool {
    (
        events[left + EVENT_TIME],
        events[left + EVENT_PHASE],
        events[left + EVENT_ORIGIN],
        events[left + EVENT_SEQUENCE],
    ) < (
        events[right + EVENT_TIME],
        events[right + EVENT_PHASE],
        events[right + EVENT_ORIGIN],
        events[right + EVENT_SEQUENCE],
    )
}

struct CpuWorkloadShard<'a> {
    base_lane: usize,
    next_time: &'a mut [u64],
    fel_left: &'a mut [u64],
    fel_right: &'a mut [u64],
    queue_depth: &'a [u32],
    queue_head: &'a mut [u32],
    transitions: &'a [u32],
    continuations: &'a [u32],
    local_push_plan: &'a [u32],
    occupancy_plan: &'a [u32],
    outbox_plan: &'a [u32],
    outbox_fused: &'a mut [u64],
    outbox_count: &'a mut [u32],
    errors: &'a mut [u32],
    semantic_flags: &'a mut [u32],
    audit: &'a mut [u64],
}

fn execute_cpu_shard_round(state: &mut CpuWorkloadShard<'_>, boundary: u64) -> u64 {
    for local_lane in 0..state.next_time.len() {
        let lane = state.base_lane + local_lane;
        state.outbox_count[local_lane] = 0;
        state.semantic_flags[local_lane] = 0;
        let transitions = state.transitions[local_lane];
        if transitions > MAX_TRANSITIONS_PER_LP {
            state.errors[local_lane] = ERROR_TRANSITION_OVERFLOW;
            continue;
        }
        let continuation_count = state.continuations[local_lane];
        let fel_pop_count = transitions - continuation_count;
        let local_push_count = state.local_push_plan[local_lane];
        let occupancy_count = state.occupancy_plan[local_lane];
        let outbox_count = state.outbox_plan[local_lane];
        if outbox_count > 1 && state.errors[local_lane] == ERROR_NONE {
            state.errors[local_lane] = ERROR_OUTBOX_OVERFLOW;
        }

        let mut step = 0_u32;
        let mut continuation_is_right = false;
        let mut continuation_slot_valid = false;
        let mut last_event = read_shard_fel(state, local_lane, false);
        while step < transitions {
            let direct_continuation = step >= fel_pop_count;
            let produces_direct_continuation = continuation_count > 0 && step + 1 == fel_pop_count;
            let performs_local_push =
                step >= transitions.saturating_sub(local_push_count.min(transitions));
            let performs_occupancy =
                step >= transitions.saturating_sub(occupancy_count.min(transitions));
            let performs_outbox = step >= transitions.saturating_sub(outbox_count.min(transitions));
            let mut selected_is_right = continuation_is_right;
            let mut parent = last_event;
            if !direct_continuation {
                selected_is_right = !shard_left_event_less(state, local_lane);
                parent = read_shard_fel(state, local_lane, selected_is_right);
                if produces_direct_continuation {
                    continuation_is_right = selected_is_right;
                    continuation_slot_valid = true;
                }
            }

            let mut child = parent;
            child[EVENT_TIME] = if produces_direct_continuation {
                parent[EVENT_TIME]
            } else {
                parent[EVENT_TIME]
                    .max(boundary)
                    .wrapping_add(LOOKAHEAD_NS)
                    .wrapping_add((lane as u64 + u64::from(step)) & 7)
            };
            child[EVENT_PHASE] = 2;
            child[EVENT_ORIGIN] = lane as u64;
            child[EVENT_SEQUENCE] = parent[EVENT_SEQUENCE].wrapping_add(1);
            child[EVENT_TARGET] = child[EVENT_TARGET].wrapping_add(u64::from(step & 1));
            child[EVENT_KIND] = 1;
            child[EVENT_PAYLOAD] = child[EVENT_PAYLOAD].wrapping_add(u64::from(step & 1));
            child[PACKET_ID] = child[EVENT_PAYLOAD];
            child[PACKET_FLOW] = child[PACKET_FLOW].wrapping_add(u64::from(step & 1));
            child[PACKET_SIZE] = child[PACKET_SIZE].wrapping_add(u64::from(step & 1));
            child[PACKET_KIND] = child[PACKET_KIND].wrapping_add(u64::from(step & 1)) & 1;
            if produces_direct_continuation && child[EVENT_TIME] == parent[EVENT_TIME] {
                state.semantic_flags[local_lane] |= 1;
            }
            if direct_continuation && performs_occupancy {
                state.semantic_flags[local_lane] |= 1 << 1;
            }
            if direct_continuation && performs_outbox {
                state.semantic_flags[local_lane] |= 1 << 2;
            }
            if direct_continuation && performs_local_push {
                state.semantic_flags[local_lane] |= 1 << 3;
            }
            if direct_continuation
                && continuation_slot_valid
                && selected_is_right == continuation_is_right
            {
                state.semantic_flags[local_lane] |= 1 << 4;
            }

            if performs_local_push {
                write_shard_fel(state, local_lane, selected_is_right, &child);
            } else if !direct_continuation && !produces_direct_continuation {
                write_shard_fel_key(state, local_lane, selected_is_right, &child);
            }
            if step + 1 == transitions && local_push_count > transitions {
                let mut second_child = child;
                second_child[EVENT_SEQUENCE] = second_child[EVENT_SEQUENCE].wrapping_add(1);
                write_shard_fel(state, local_lane, !selected_is_right, &second_child);
            }

            if performs_occupancy {
                let depth = state.queue_depth[local_lane];
                if depth == 0 {
                    if state.errors[local_lane] == ERROR_NONE {
                        state.errors[local_lane] = ERROR_ZERO_OCCUPANCY;
                    }
                } else {
                    state.queue_head[local_lane] = (state.queue_head[local_lane] + 1) % depth;
                }
            }

            if performs_outbox && outbox_count <= 1 {
                let outbox = local_lane * FUSED_EVENT_PACKET_WORDS;
                state.outbox_fused[outbox..outbox + FUSED_EVENT_PACKET_WORDS]
                    .copy_from_slice(&child);
                state.outbox_count[local_lane] = 1;
            }

            let operation_tags = 1_u64
                .wrapping_add((!direct_continuation as u64) << 8)
                .wrapping_add((performs_occupancy as u64) << 16)
                .wrapping_add((performs_outbox as u64) << 24);
            state.audit[local_lane] = state.audit[local_lane]
                .wrapping_add(operation_tags)
                .wrapping_add(
                    parent
                        .iter()
                        .fold(0_u64, |sum, word| sum.wrapping_add(*word)),
                );
            last_event = child;
            step += 1;
        }
        state.next_time[local_lane] = if shard_left_event_less(state, local_lane) {
            read_shard_fel_word(state, local_lane, false, EVENT_TIME)
        } else {
            read_shard_fel_word(state, local_lane, true, EVENT_TIME)
        };
    }
    state.next_time.iter().copied().min().unwrap_or(u64::MAX)
}

fn read_shard_fel(
    state: &CpuWorkloadShard<'_>,
    local_lane: usize,
    right: bool,
) -> [u64; FUSED_EVENT_PACKET_WORDS] {
    let offset = local_lane * FUSED_EVENT_PACKET_WORDS;
    let source = if right {
        &state.fel_right[offset..offset + FUSED_EVENT_PACKET_WORDS]
    } else {
        &state.fel_left[offset..offset + FUSED_EVENT_PACKET_WORDS]
    };
    let mut event = [0; FUSED_EVENT_PACKET_WORDS];
    event.copy_from_slice(source);
    event
}

fn read_shard_fel_word(
    state: &CpuWorkloadShard<'_>,
    local_lane: usize,
    right: bool,
    word: usize,
) -> u64 {
    let offset = local_lane * FUSED_EVENT_PACKET_WORDS + word;
    if right {
        state.fel_right[offset]
    } else {
        state.fel_left[offset]
    }
}

fn write_shard_fel(
    state: &mut CpuWorkloadShard<'_>,
    local_lane: usize,
    right: bool,
    event: &[u64; FUSED_EVENT_PACKET_WORDS],
) {
    let offset = local_lane * FUSED_EVENT_PACKET_WORDS;
    let target = if right {
        &mut state.fel_right[offset..offset + FUSED_EVENT_PACKET_WORDS]
    } else {
        &mut state.fel_left[offset..offset + FUSED_EVENT_PACKET_WORDS]
    };
    target.copy_from_slice(event);
}

fn write_shard_fel_key(
    state: &mut CpuWorkloadShard<'_>,
    local_lane: usize,
    right: bool,
    event: &[u64; FUSED_EVENT_PACKET_WORDS],
) {
    let offset = local_lane * FUSED_EVENT_PACKET_WORDS;
    let target = if right {
        &mut state.fel_right[offset..offset + FUSED_EVENT_PACKET_WORDS]
    } else {
        &mut state.fel_left[offset..offset + FUSED_EVENT_PACKET_WORDS]
    };
    target[EVENT_TIME..=EVENT_SEQUENCE].copy_from_slice(&event[EVENT_TIME..=EVENT_SEQUENCE]);
}

fn shard_left_event_less(state: &CpuWorkloadShard<'_>, local_lane: usize) -> bool {
    let offset = local_lane * FUSED_EVENT_PACKET_WORDS;
    (
        state.fel_left[offset + EVENT_TIME],
        state.fel_left[offset + EVENT_PHASE],
        state.fel_left[offset + EVENT_ORIGIN],
        state.fel_left[offset + EVENT_SEQUENCE],
    ) < (
        state.fel_right[offset + EVENT_TIME],
        state.fel_right[offset + EVENT_PHASE],
        state.fel_right[offset + EVENT_ORIGIN],
        state.fel_right[offset + EVENT_SEQUENCE],
    )
}

fn measure_cpu_rounds(state: &mut WorkloadState, rounds: usize, workers: usize) -> u64 {
    let boundary = AtomicU64::new(state.horizon[0]);
    let worker_minima = (0..workers)
        .map(|_| AtomicU64::new(u64::MAX))
        .collect::<Vec<_>>();
    let ready_barrier = Barrier::new(workers + 1);
    let start_barrier = Barrier::new(workers + 1);
    let finish_barrier = Barrier::new(workers + 1);
    let round_barrier = Barrier::new(workers);
    let shards = cpu_workload_shards(state, workers);

    let elapsed = std::thread::scope(|scope| {
        for (worker, mut shard) in shards.into_iter().enumerate() {
            let boundary = &boundary;
            let worker_minima = &worker_minima;
            let ready_barrier = &ready_barrier;
            let start_barrier = &start_barrier;
            let finish_barrier = &finish_barrier;
            let round_barrier = &round_barrier;
            scope.spawn(move || {
                ready_barrier.wait();
                start_barrier.wait();
                for _ in 0..rounds {
                    let minimum =
                        execute_cpu_shard_round(&mut shard, boundary.load(Ordering::Relaxed));
                    worker_minima[worker].store(minimum, Ordering::Relaxed);
                    round_barrier.wait();
                    if worker == 0 {
                        let minimum = worker_minima
                            .iter()
                            .map(|value| value.load(Ordering::Relaxed))
                            .min()
                            .unwrap_or(u64::MAX);
                        boundary.store(minimum.wrapping_add(LOOKAHEAD_NS), Ordering::Relaxed);
                    }
                    round_barrier.wait();
                }
                finish_barrier.wait();
            });
        }

        ready_barrier.wait();
        let started = Instant::now();
        start_barrier.wait();
        finish_barrier.wait();
        started.elapsed()
    });
    state.horizon[0] = boundary.load(Ordering::Relaxed);
    duration_ns(elapsed)
}

fn cpu_workload_shards(state: &mut WorkloadState, workers: usize) -> Vec<CpuWorkloadShard<'_>> {
    let active_lps = state.active_lps;
    let padded_words = state.padded_lanes * FUSED_EVENT_PACKET_WORDS;
    let (fel_left, fel_right) = state.fel_fused.split_at_mut(padded_words);

    let mut next_time = &mut state.next_time[..active_lps];
    let mut fel_left = &mut fel_left[..active_lps * FUSED_EVENT_PACKET_WORDS];
    let mut fel_right = &mut fel_right[..active_lps * FUSED_EVENT_PACKET_WORDS];
    let mut queue_depth = &state.queue_depth[..active_lps];
    let mut queue_head = &mut state.queue_head[..active_lps];
    let mut transitions = &state.transitions[..active_lps];
    let mut continuations = &state.continuations[..active_lps];
    let mut local_push_plan = &state.local_push_plan[..active_lps];
    let mut occupancy_plan = &state.occupancy_plan[..active_lps];
    let mut outbox_plan = &state.outbox_plan[..active_lps];
    let mut outbox_fused = &mut state.outbox_fused[..active_lps * FUSED_EVENT_PACKET_WORDS];
    let mut outbox_count = &mut state.outbox_count[..active_lps];
    let mut errors = &mut state.errors[..active_lps];
    let mut semantic_flags = &mut state.semantic_flags[..active_lps];
    let mut audit = &mut state.audit[..active_lps];

    let mut shards = Vec::with_capacity(workers);
    let mut base_lane = 0;
    for worker in 0..workers {
        let next_end = active_lps * (worker + 1) / workers;
        let lanes = next_end - base_lane;
        let words = lanes * FUSED_EVENT_PACKET_WORDS;
        shards.push(CpuWorkloadShard {
            base_lane,
            next_time: take_mut_prefix(&mut next_time, lanes),
            fel_left: take_mut_prefix(&mut fel_left, words),
            fel_right: take_mut_prefix(&mut fel_right, words),
            queue_depth: take_prefix(&mut queue_depth, lanes),
            queue_head: take_mut_prefix(&mut queue_head, lanes),
            transitions: take_prefix(&mut transitions, lanes),
            continuations: take_prefix(&mut continuations, lanes),
            local_push_plan: take_prefix(&mut local_push_plan, lanes),
            occupancy_plan: take_prefix(&mut occupancy_plan, lanes),
            outbox_plan: take_prefix(&mut outbox_plan, lanes),
            outbox_fused: take_mut_prefix(&mut outbox_fused, words),
            outbox_count: take_mut_prefix(&mut outbox_count, lanes),
            errors: take_mut_prefix(&mut errors, lanes),
            semantic_flags: take_mut_prefix(&mut semantic_flags, lanes),
            audit: take_mut_prefix(&mut audit, lanes),
        });
        base_lane = next_end;
    }
    shards
}

fn take_prefix<'a, T>(remainder: &mut &'a [T], len: usize) -> &'a [T] {
    let (prefix, tail) = remainder.split_at(len);
    *remainder = tail;
    prefix
}

fn take_mut_prefix<'a, T>(remainder: &mut &'a mut [T], len: usize) -> &'a mut [T] {
    let current = std::mem::take(remainder);
    let (prefix, tail) = current.split_at_mut(len);
    *remainder = tail;
    prefix
}

struct RealReplayPlan {
    active_lps: Vec<u32>,
    row_starts: Vec<usize>,
    node_ids: Vec<u64>,
    pending_events: Vec<u32>,
    step_starts: Vec<u32>,
    step_counts: Vec<u32>,
    next_time_ns: Vec<u64>,
    frontier_ns: Vec<u64>,
    exclusive_horizon_ns: Vec<u64>,
    steps: Vec<u32>,
    // Deterministic spike data, not captured production payload state. There is one complete
    // Event+Packet record per real transition so both replay paths pay resident 88-byte reads.
    parent_fused: Vec<u64>,
    expected_minimum_ns: Vec<u64>,
    next_horizon_ns: Vec<u64>,
    local_child_capacity: u32,
    remote_child_capacity: u32,
}

impl RealReplayPlan {
    fn new(trace: &RealReplayTrace) -> Result<Self, MetalSpikeError> {
        trace.validate()?;
        if trace.rounds.is_empty() {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "real replay trace must contain at least one round",
            ));
        }
        let active_lps = trace
            .rounds
            .iter()
            .map(|round| {
                u32::try_from(round.active_lp_count).map_err(|_| {
                    MetalSpikeError::InvalidBenchmarkConfig(
                        "real replay active-LP width exceeds u32",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let step_starts = trace
            .lps
            .iter()
            .map(|row| {
                u32::try_from(row.step_start).map_err(|_| {
                    MetalSpikeError::InvalidBenchmarkConfig("real replay step offset exceeds u32")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let step_counts = trace
            .lps
            .iter()
            .map(|row| {
                u32::try_from(row.step_count).map_err(|_| {
                    MetalSpikeError::InvalidBenchmarkConfig(
                        "one real replay LP step count exceeds u32",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let exclusive_horizon_ns = trace
            .rounds
            .iter()
            .map(|round| {
                u64::try_from(round.exclusive_horizon_ns).map_err(|_| {
                    MetalSpikeError::InvalidBenchmarkConfig(
                        "real replay horizon exceeds the u64 device domain",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let local_child_capacity = replay_child_capacity(trace, ReplayStep::local_fel_pushes)?;
        let remote_child_capacity = replay_child_capacity(trace, ReplayStep::remote_outbox_writes)?;
        let mut parent_fused = vec![0_u64; trace.steps.len() * FUSED_EVENT_PACKET_WORDS];
        let mut expected_minimum_ns = Vec::with_capacity(trace.rounds.len());
        for round in &trace.rounds {
            let rows = &trace.lps[round.lp_start..round.lp_start + round.lp_count];
            expected_minimum_ns.push(
                rows.iter()
                    .map(|row| row.next_time_ns_after_local_drain)
                    .min()
                    .unwrap_or(u64::MAX),
            );
            for row in rows {
                for local_step in 0..row.step_count {
                    let step_index = row.step_start + local_step;
                    let sequence = u64::try_from(step_index).map_err(|_| {
                        MetalSpikeError::InvalidBenchmarkConfig(
                            "real replay parent sequence exceeds u64",
                        )
                    })?;
                    let record = simulated_replay_parent(
                        round,
                        row,
                        local_step,
                        sequence,
                        trace.steps[step_index],
                    )?;
                    parent_fused[step_index * FUSED_EVENT_PACKET_WORDS
                        ..(step_index + 1) * FUSED_EVENT_PACKET_WORDS]
                        .copy_from_slice(&record);
                }
            }
        }
        let mut next_horizon_ns = exclusive_horizon_ns
            .iter()
            .copied()
            .skip(1)
            .collect::<Vec<_>>();
        let final_round = trace
            .rounds
            .last()
            .expect("nonempty trace established a final round");
        let final_minimum = *expected_minimum_ns
            .last()
            .expect("nonempty trace established a final minimum");
        let final_lookahead = u64::try_from(
            final_round
                .exclusive_horizon_ns
                .saturating_sub(u128::from(final_round.frontier_ns)),
        )
        .unwrap_or(u64::MAX);
        next_horizon_ns.push(if final_minimum == u64::MAX {
            *exclusive_horizon_ns
                .last()
                .expect("nonempty trace established a horizon")
        } else {
            final_minimum.saturating_add(final_lookahead)
        });
        Ok(Self {
            active_lps,
            row_starts: trace.rounds.iter().map(|round| round.lp_start).collect(),
            node_ids: trace.lps.iter().map(|row| row.node.0).collect(),
            pending_events: trace
                .lps
                .iter()
                .map(|row| row.pending_events_below_horizon)
                .collect(),
            step_starts,
            step_counts,
            next_time_ns: trace
                .lps
                .iter()
                .map(|row| row.next_time_ns_after_local_drain)
                .collect(),
            frontier_ns: trace.rounds.iter().map(|round| round.frontier_ns).collect(),
            exclusive_horizon_ns,
            steps: trace.steps.iter().map(|step| step.packed()).collect(),
            parent_fused,
            expected_minimum_ns,
            next_horizon_ns,
            local_child_capacity,
            remote_child_capacity,
        })
    }

    fn rounds(&self) -> usize {
        self.active_lps.len()
    }

    fn maximum_active_lps(&self) -> usize {
        self.active_lps
            .iter()
            .copied()
            .map(|width| width as usize)
            .max()
            .unwrap_or(0)
    }
}

fn replay_child_capacity(
    trace: &RealReplayTrace,
    count: fn(ReplayStep) -> u8,
) -> Result<u32, MetalSpikeError> {
    let maximum = trace
        .lps
        .iter()
        .map(|row| {
            trace.steps[row.step_start..row.step_start + row.step_count]
                .iter()
                .try_fold(0_u32, |total, step| {
                    total.checked_add(u32::from(count(*step))).ok_or(
                        MetalSpikeError::InvalidBenchmarkConfig(
                            "real replay child capacity exceeds u32",
                        ),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .unwrap_or(0);
    Ok(maximum.max(1))
}

fn simulated_replay_parent(
    round: &RealReplayRound,
    row: &RealReplayLp,
    local_step: usize,
    sequence: u64,
    step: ReplayStep,
) -> Result<[u64; FUSED_EVENT_PACKET_WORDS], MetalSpikeError> {
    let exclusive_horizon_ns = u64::try_from(round.exclusive_horizon_ns).map_err(|_| {
        MetalSpikeError::InvalidBenchmarkConfig("real replay horizon exceeds the u64 device domain")
    })?;
    let time_ns = round
        .frontier_ns
        .saturating_add(local_step as u64)
        .min(exclusive_horizon_ns.saturating_sub(1));
    let payload = row.node.0.rotate_left(17) ^ sequence;
    Ok([
        time_ns,
        replay_event_phase(step.kind()),
        row.node.0,
        sequence,
        row.node.0,
        step.kind() as u64,
        payload,
        payload,
        sequence,
        [64, 1_000, 1_500, 9_000][local_step % 4],
        sequence & 1,
    ])
}

const fn replay_event_phase(kind: EventKind) -> u64 {
    match kind {
        EventKind::PacketArrival | EventKind::RemoteArrival => 0,
        EventKind::TxComplete => 1,
        EventKind::TxReady => 2,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RealReplayState {
    next_time: Vec<u64>,
    device_horizon: Vec<u64>,
    local_fel_fused: Vec<u64>,
    remote_outbox_fused: Vec<u64>,
    queue_head: Vec<u32>,
    local_child_count: Vec<u32>,
    remote_child_count: Vec<u32>,
    errors: Vec<u32>,
    audit: Vec<u64>,
}

impl RealReplayState {
    fn initial(padded_lanes: usize, initial_horizon: u64, plan: &RealReplayPlan) -> Self {
        Self {
            next_time: vec![u64::MAX; padded_lanes],
            device_horizon: vec![initial_horizon],
            local_fel_fused: vec![
                0;
                padded_lanes
                    * plan.local_child_capacity as usize
                    * FUSED_EVENT_PACKET_WORDS
            ],
            remote_outbox_fused: vec![
                0;
                padded_lanes
                    * plan.remote_child_capacity as usize
                    * FUSED_EVENT_PACKET_WORDS
            ],
            queue_head: vec![0; padded_lanes],
            local_child_count: vec![0; padded_lanes],
            remote_child_count: vec![0; padded_lanes],
            errors: vec![0; padded_lanes],
            audit: vec![0; padded_lanes],
        }
    }

    fn planes(&self) -> Vec<&[u8]> {
        vec![
            bytes(&self.next_time),
            bytes(&self.device_horizon),
            bytes(&self.local_fel_fused),
            bytes(&self.remote_outbox_fused),
            bytes(&self.queue_head),
            bytes(&self.local_child_count),
            bytes(&self.remote_child_count),
            bytes(&self.errors),
            bytes(&self.audit),
        ]
    }
}

struct RealCpuReplayShard<'a> {
    base_lane: usize,
    next_time: &'a mut [u64],
    local_fel_fused: &'a mut [u64],
    remote_outbox_fused: &'a mut [u64],
    queue_head: &'a mut [u32],
    local_child_count: &'a mut [u32],
    remote_child_count: &'a mut [u32],
    errors: &'a mut [u32],
    audit: &'a mut [u64],
}

fn execute_real_cpu_round(
    plan: &RealReplayPlan,
    round: usize,
    state: &mut RealCpuReplayShard<'_>,
    device_horizon: u64,
    previous_active_lps: usize,
) -> u64 {
    let active_lps = plan.active_lps[round] as usize;
    let row_start = plan.row_starts[round];
    let active_end = active_lps
        .saturating_sub(state.base_lane)
        .min(state.next_time.len());
    let previous_active_end = previous_active_lps
        .saturating_sub(state.base_lane)
        .min(state.next_time.len());
    for local_lane in 0..active_end {
        let lane = state.base_lane + local_lane;
        let row = row_start + lane;
        let local_start =
            local_lane * plan.local_child_capacity as usize * FUSED_EVENT_PACKET_WORDS;
        let remote_start =
            local_lane * plan.remote_child_capacity as usize * FUSED_EVENT_PACKET_WORDS;
        execute_real_cpu_lp(
            plan,
            round,
            row,
            lane,
            &mut state.local_fel_fused[local_start
                ..local_start + plan.local_child_capacity as usize * FUSED_EVENT_PACKET_WORDS],
            &mut state.remote_outbox_fused[remote_start
                ..remote_start + plan.remote_child_capacity as usize * FUSED_EVENT_PACKET_WORDS],
            &mut state.queue_head[local_lane],
            &mut state.local_child_count[local_lane],
            &mut state.remote_child_count[local_lane],
            &mut state.errors[local_lane],
            &mut state.audit[local_lane],
            device_horizon,
        );
        state.next_time[local_lane] = plan.next_time_ns[row];
    }
    if previous_active_end > active_end {
        for next_time in &mut state.next_time[active_end..previous_active_end] {
            *next_time = u64::MAX;
        }
    }
    state.next_time[..active_end]
        .iter()
        .copied()
        .min()
        .unwrap_or(u64::MAX)
}

#[allow(clippy::too_many_arguments)]
fn execute_real_cpu_lp(
    plan: &RealReplayPlan,
    round: usize,
    row: usize,
    lane: usize,
    local_fel_fused: &mut [u64],
    remote_outbox_fused: &mut [u64],
    queue_head: &mut u32,
    local_child_count: &mut u32,
    remote_child_count: &mut u32,
    errors: &mut u32,
    audit: &mut u64,
    device_horizon: u64,
) {
    let node = plan.node_ids[row];
    let pending = plan.pending_events[row];
    *local_child_count = 0;
    *remote_child_count = 0;
    if device_horizon != plan.exclusive_horizon_ns[round] {
        *errors |= REAL_ERROR_HORIZON_STATE_MISMATCH;
    }
    let mut value = audit
        .wrapping_add(node)
        .wrapping_add(u64::from(pending) << 8)
        .wrapping_add(plan.frontier_ns[round])
        .wrapping_add(plan.exclusive_horizon_ns[round])
        .wrapping_add(device_horizon);
    let step_start = plan.step_starts[row] as usize;
    let step_count = plan.step_counts[row] as usize;
    for step_index in 0..step_count {
        let global_step = step_start + step_index;
        let packed = plan.steps[global_step];
        let kind = packed & REPLAY_KIND_MASK;
        let parent_start = global_step * FUSED_EVENT_PACKET_WORDS;
        let parent = &plan.parent_fused[parent_start..parent_start + FUSED_EVENT_PACKET_WORDS];
        let parent_time = parent[EVENT_TIME];
        let parent_phase = parent[EVENT_PHASE];
        let parent_origin = parent[EVENT_ORIGIN];
        let parent_sequence = parent[EVENT_SEQUENCE];
        let parent_target = parent[EVENT_TARGET];
        let parent_kind = parent[EVENT_KIND];
        let parent_payload = parent[EVENT_PAYLOAD];
        let parent_packet_id = parent[PACKET_ID];
        let parent_flow = parent[PACKET_FLOW];
        let parent_size = parent[PACKET_SIZE];
        let parent_packet_kind = parent[PACKET_KIND];
        if parent_time >= plan.exclusive_horizon_ns[round] || parent_time >= device_horizon {
            *errors |= REAL_ERROR_KEY_OUTSIDE_HORIZON;
        }
        if parent_kind != u64::from(kind) {
            *errors |= REAL_ERROR_EVENT_KIND_MISMATCH;
        }
        if !replay_event_less_cpu(
            parent_time,
            parent_phase,
            parent_origin,
            parent_sequence,
            parent_time,
            parent_phase,
            parent_origin,
            parent_sequence.wrapping_add(1),
        ) {
            *errors |= REAL_ERROR_KEY_ORDER_MISMATCH;
        }
        value = value
            .wrapping_add(parent_time)
            .wrapping_add(parent_phase)
            .wrapping_add(parent_origin)
            .wrapping_add(parent_sequence)
            .wrapping_add(parent_target)
            .wrapping_add(parent_kind)
            .wrapping_add(parent_payload)
            .wrapping_add(parent_packet_id)
            .wrapping_add(parent_flow)
            .wrapping_add(parent_size)
            .wrapping_add(parent_packet_kind);
        if kind == EventKind::PacketArrival as u32 {
            value ^= 0xa076_1d64_78bd_642f;
        } else if kind == EventKind::TxReady as u32 {
            let occupancy = (packed >> REPLAY_QUEUE_SHIFT) & REPLAY_QUEUE_MASK;
            if occupancy == 0 {
                value ^= 0xe703_7ed1_a0b4_28db;
            } else {
                *queue_head = queue_head.wrapping_add(1) % occupancy;
                value = value
                    .wrapping_add(u64::from(occupancy))
                    .wrapping_add(u64::from(*queue_head));
            }
        } else if kind == EventKind::TxComplete as u32 {
            value = value.wrapping_add(u64::from(pending).wrapping_mul(3));
        } else if kind == EventKind::RemoteArrival as u32 {
            value ^= node.wrapping_add(step_index as u64);
        }
        if packed & REPLAY_DIRECT_CONTINUATION_BIT != 0 {
            value = value.wrapping_add(0x8ebc_6af0_9c88_c6e3);
        }
        let local_pushes = (packed >> REPLAY_LOCAL_PUSH_SHIFT) & REPLAY_CHILD_COUNT_MASK;
        let remote_writes = (packed >> REPLAY_REMOTE_WRITE_SHIFT) & REPLAY_CHILD_COUNT_MASK;
        for child in 0..local_pushes {
            let slot = *local_child_count + child;
            replay_fused_child_cpu(
                local_fel_fused,
                slot,
                parent,
                lane,
                step_index,
                child,
                false,
                &mut value,
            );
        }
        *local_child_count += local_pushes;
        for child in 0..remote_writes {
            let slot = *remote_child_count + child;
            replay_fused_child_cpu(
                remote_outbox_fused,
                slot,
                parent,
                lane,
                step_index,
                child,
                true,
                &mut value,
            );
        }
        *remote_child_count += remote_writes;
    }
    *audit = value;
}

#[allow(clippy::too_many_arguments)]
fn replay_event_less_cpu(
    left_time: u64,
    left_phase: u64,
    left_origin: u64,
    left_sequence: u64,
    right_time: u64,
    right_phase: u64,
    right_origin: u64,
    right_sequence: u64,
) -> bool {
    (left_time, left_phase, left_origin, left_sequence)
        < (right_time, right_phase, right_origin, right_sequence)
}

#[allow(clippy::too_many_arguments)]
fn replay_fused_child_cpu(
    scratch: &mut [u64],
    slot: u32,
    parent: &[u64],
    lane: usize,
    step_index: usize,
    child: u32,
    remote: bool,
    audit: &mut u64,
) {
    let start = slot as usize * FUSED_EVENT_PACKET_WORDS;
    let child_payload = parent[EVENT_PAYLOAD].wrapping_add(u64::from(child) + 1);
    let child_record = [
        parent[EVENT_TIME].wrapping_add(u64::from(child) + 1),
        if remote { 0 } else { 2 },
        parent[EVENT_ORIGIN],
        parent[EVENT_SEQUENCE]
            .wrapping_add(u64::from(child))
            .wrapping_add(1),
        parent[EVENT_TARGET].wrapping_add(u64::from(remote)),
        if remote {
            EventKind::RemoteArrival as u64
        } else {
            EventKind::TxReady as u64
        },
        child_payload,
        child_payload,
        parent[PACKET_FLOW],
        parent[PACKET_SIZE],
        parent[PACKET_KIND],
    ];
    for (word, child_word) in child_record.into_iter().enumerate() {
        scratch[start + word] = child_word;
        *audit = audit
            .wrapping_add(child_word)
            .wrapping_add(lane as u64)
            .wrapping_add(step_index as u64);
    }
}

fn measure_real_cpu_replay(
    state: &mut RealReplayState,
    plan: &RealReplayPlan,
    workers: usize,
) -> u64 {
    let device_horizon = AtomicU64::new(state.device_horizon[0]);
    let worker_minima = (0..workers)
        .map(|_| AtomicU64::new(u64::MAX))
        .collect::<Vec<_>>();
    let ready_barrier = Barrier::new(workers + 1);
    let start_barrier = Barrier::new(workers + 1);
    let finish_barrier = Barrier::new(workers + 1);
    let round_barrier = Barrier::new(workers);
    let shards = real_cpu_replay_shards(state, plan, workers);

    let elapsed = std::thread::scope(|scope| {
        for (worker, mut shard) in shards.into_iter().enumerate() {
            let device_horizon = &device_horizon;
            let worker_minima = &worker_minima;
            let ready_barrier = &ready_barrier;
            let start_barrier = &start_barrier;
            let finish_barrier = &finish_barrier;
            let round_barrier = &round_barrier;
            scope.spawn(move || {
                ready_barrier.wait();
                start_barrier.wait();
                let mut previous_active_lps = 0;
                for round in 0..plan.rounds() {
                    let minimum = execute_real_cpu_round(
                        plan,
                        round,
                        &mut shard,
                        device_horizon.load(Ordering::Relaxed),
                        previous_active_lps,
                    );
                    previous_active_lps = plan.active_lps[round] as usize;
                    worker_minima[worker].store(minimum, Ordering::Relaxed);
                    round_barrier.wait();
                    if worker == 0 {
                        let minimum = worker_minima
                            .iter()
                            .map(|value| value.load(Ordering::Relaxed))
                            .min()
                            .unwrap_or(u64::MAX);
                        if minimum != plan.expected_minimum_ns[round] {
                            shard.errors[0] |= REAL_ERROR_MINIMUM_MISMATCH;
                        }
                        device_horizon.store(plan.next_horizon_ns[round], Ordering::Relaxed);
                    }
                    round_barrier.wait();
                }
                finish_barrier.wait();
            });
        }

        ready_barrier.wait();
        let started = Instant::now();
        start_barrier.wait();
        finish_barrier.wait();
        started.elapsed()
    });
    state.device_horizon[0] = device_horizon.load(Ordering::Relaxed);
    duration_ns(elapsed)
}

fn real_cpu_replay_shards<'state>(
    state: &'state mut RealReplayState,
    plan: &RealReplayPlan,
    workers: usize,
) -> Vec<RealCpuReplayShard<'state>> {
    let padded_lanes = state.next_time.len();
    let mut next_time = state.next_time.as_mut_slice();
    let mut local_fel_fused = state.local_fel_fused.as_mut_slice();
    let mut remote_outbox_fused = state.remote_outbox_fused.as_mut_slice();
    let mut queue_head = state.queue_head.as_mut_slice();
    let mut local_child_count = state.local_child_count.as_mut_slice();
    let mut remote_child_count = state.remote_child_count.as_mut_slice();
    let mut errors = state.errors.as_mut_slice();
    let mut audit = state.audit.as_mut_slice();
    let mut shards = Vec::with_capacity(workers);
    let mut base_lane = 0;
    for worker in 0..workers {
        let next_end = padded_lanes * (worker + 1) / workers;
        let lanes = next_end - base_lane;
        shards.push(RealCpuReplayShard {
            base_lane,
            next_time: take_mut_prefix(&mut next_time, lanes),
            local_fel_fused: take_mut_prefix(
                &mut local_fel_fused,
                lanes * plan.local_child_capacity as usize * FUSED_EVENT_PACKET_WORDS,
            ),
            remote_outbox_fused: take_mut_prefix(
                &mut remote_outbox_fused,
                lanes * plan.remote_child_capacity as usize * FUSED_EVENT_PACKET_WORDS,
            ),
            queue_head: take_mut_prefix(&mut queue_head, lanes),
            local_child_count: take_mut_prefix(&mut local_child_count, lanes),
            remote_child_count: take_mut_prefix(&mut remote_child_count, lanes),
            errors: take_mut_prefix(&mut errors, lanes),
            audit: take_mut_prefix(&mut audit, lanes),
        });
        base_lane = next_end;
    }
    shards
}

#[cube]
fn cube_event_less(
    left_time: u64,
    left_phase: u64,
    left_origin: u64,
    left_seq: u64,
    right_time: u64,
    right_phase: u64,
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
#[allow(clippy::too_many_arguments, unused_assignments)]
fn matched_round_kernel(
    next_time: &mut [u64],
    horizon: &[u64],
    fel_fused: &mut [u64],
    queue_depth: &[u32],
    queue_head: &mut [u32],
    transitions: &[u32],
    continuations: &[u32],
    local_push_plan: &[u32],
    occupancy_plan: &[u32],
    outbox_plan: &[u32],
    outbox_fused: &mut [u64],
    outbox_count: &mut [u32],
    errors: &mut [u32],
    semantic_flags: &mut [u32],
    audit: &mut [u64],
    #[comptime] active_lps: u32,
    #[comptime] fel_slot_words: u32,
    #[comptime] lookahead_ns: u64,
    #[comptime] max_transitions: u32,
) {
    let lp = ABSOLUTE_POS;
    if lp < active_lps as usize {
        outbox_count[lp] = 0u32;
        semantic_flags[lp] = 0u32;
        let transition_count = transitions[lp];
        if transition_count > max_transitions {
            errors[lp] = ERROR_TRANSITION_OVERFLOW;
        } else {
            let continuation_count = continuations[lp];
            let fel_pop_count = transition_count - continuation_count;
            let local_push_count = local_push_plan[lp];
            let occupancy_count = occupancy_plan[lp];
            let planned_outboxes = outbox_plan[lp];
            if planned_outboxes > 1u32 && errors[lp] == ERROR_NONE {
                errors[lp] = ERROR_OUTBOX_OVERFLOW;
            }

            let mut step = 0u32;
            let left = lp * FUSED_EVENT_PACKET_WORDS;
            let right = fel_slot_words as usize + left;
            let mut last_time = fel_fused[left + EVENT_TIME];
            let mut last_phase = fel_fused[left + EVENT_PHASE];
            let mut last_origin = fel_fused[left + EVENT_ORIGIN];
            let mut last_seq = fel_fused[left + EVENT_SEQUENCE];
            let mut last_target = fel_fused[left + EVENT_TARGET];
            let mut last_event_kind = fel_fused[left + EVENT_KIND];
            let mut last_payload = fel_fused[left + EVENT_PAYLOAD];
            let mut last_packet_id = fel_fused[left + PACKET_ID];
            let mut last_flow = fel_fused[left + PACKET_FLOW];
            let mut last_size = fel_fused[left + PACKET_SIZE];
            let mut last_packet_kind = fel_fused[left + PACKET_KIND];
            let mut continuation_slot = left;
            let mut continuation_slot_valid = false;
            while step < transition_count {
                let direct_continuation = step >= fel_pop_count;
                let produces_direct_continuation =
                    continuation_count > 0u32 && step + 1u32 == fel_pop_count;
                let bounded_pushes = if local_push_count < transition_count {
                    local_push_count
                } else {
                    transition_count
                };
                let performs_local_push = step >= transition_count - bounded_pushes;
                let bounded_occupancy = if occupancy_count < transition_count {
                    occupancy_count
                } else {
                    transition_count
                };
                let performs_occupancy = step >= transition_count - bounded_occupancy;
                let bounded_outboxes = if planned_outboxes < transition_count {
                    planned_outboxes
                } else {
                    transition_count
                };
                let performs_outbox = step >= transition_count - bounded_outboxes;
                let mut selected = continuation_slot;
                let mut parent_time = last_time;
                let mut parent_phase = last_phase;
                let mut parent_origin = last_origin;
                let mut parent_seq = last_seq;
                let mut parent_target = last_target;
                let mut parent_event_kind = last_event_kind;
                let mut parent_payload = last_payload;
                let mut parent_packet_id = last_packet_id;
                let mut parent_flow = last_flow;
                let mut parent_size = last_size;
                let mut parent_packet_kind = last_packet_kind;
                if !direct_continuation {
                    let take_a = cube_event_less(
                        fel_fused[left + EVENT_TIME],
                        fel_fused[left + EVENT_PHASE],
                        fel_fused[left + EVENT_ORIGIN],
                        fel_fused[left + EVENT_SEQUENCE],
                        fel_fused[right + EVENT_TIME],
                        fel_fused[right + EVENT_PHASE],
                        fel_fused[right + EVENT_ORIGIN],
                        fel_fused[right + EVENT_SEQUENCE],
                    );
                    if take_a {
                        selected = left;
                    } else {
                        selected = right;
                    }
                    parent_time = fel_fused[selected + EVENT_TIME];
                    parent_phase = fel_fused[selected + EVENT_PHASE];
                    parent_origin = fel_fused[selected + EVENT_ORIGIN];
                    parent_seq = fel_fused[selected + EVENT_SEQUENCE];
                    parent_target = fel_fused[selected + EVENT_TARGET];
                    parent_event_kind = fel_fused[selected + EVENT_KIND];
                    parent_payload = fel_fused[selected + EVENT_PAYLOAD];
                    parent_packet_id = fel_fused[selected + PACKET_ID];
                    parent_flow = fel_fused[selected + PACKET_FLOW];
                    parent_size = fel_fused[selected + PACKET_SIZE];
                    parent_packet_kind = fel_fused[selected + PACKET_KIND];
                    if produces_direct_continuation {
                        continuation_slot = selected;
                        continuation_slot_valid = true;
                    }
                }

                let child_time = if produces_direct_continuation {
                    parent_time
                } else {
                    let base = if parent_time < horizon[0usize] {
                        horizon[0usize]
                    } else {
                        parent_time
                    };
                    base + lookahead_ns + ((lp as u64 + step as u64) & 7u64)
                };
                let child_phase = 2u64;
                let child_origin = lp as u64;
                let child_seq = parent_seq + 1u64;
                let odd = (step & 1u32) as u64;
                let child_target = parent_target + odd;
                let child_event_kind = 1u64;
                let child_payload = parent_payload + odd;
                let child_packet_id = child_payload;
                let child_flow = parent_flow + odd;
                let child_size = parent_size + odd;
                let child_packet_kind = (parent_packet_kind + odd) & 1u64;
                if produces_direct_continuation && child_time == parent_time {
                    semantic_flags[lp] |= 1u32;
                }
                if direct_continuation && performs_occupancy {
                    semantic_flags[lp] |= 2u32;
                }
                if direct_continuation && performs_outbox {
                    semantic_flags[lp] |= 4u32;
                }
                if direct_continuation && performs_local_push {
                    semantic_flags[lp] |= 8u32;
                }
                if direct_continuation && continuation_slot_valid && selected == continuation_slot {
                    semantic_flags[lp] |= 16u32;
                }

                if performs_local_push {
                    fel_fused[selected + EVENT_TIME] = child_time;
                    fel_fused[selected + EVENT_PHASE] = child_phase;
                    fel_fused[selected + EVENT_ORIGIN] = child_origin;
                    fel_fused[selected + EVENT_SEQUENCE] = child_seq;
                    fel_fused[selected + EVENT_TARGET] = child_target;
                    fel_fused[selected + EVENT_KIND] = child_event_kind;
                    fel_fused[selected + EVENT_PAYLOAD] = child_payload;
                    fel_fused[selected + PACKET_ID] = child_packet_id;
                    fel_fused[selected + PACKET_FLOW] = child_flow;
                    fel_fused[selected + PACKET_SIZE] = child_size;
                    fel_fused[selected + PACKET_KIND] = child_packet_kind;
                } else if !direct_continuation && !produces_direct_continuation {
                    fel_fused[selected + EVENT_TIME] = child_time;
                    fel_fused[selected + EVENT_PHASE] = child_phase;
                    fel_fused[selected + EVENT_ORIGIN] = child_origin;
                    fel_fused[selected + EVENT_SEQUENCE] = child_seq;
                }
                if step + 1u32 == transition_count && local_push_count > transition_count {
                    let extra = if selected == left { right } else { left };
                    fel_fused[extra + EVENT_TIME] = child_time;
                    fel_fused[extra + EVENT_PHASE] = child_phase;
                    fel_fused[extra + EVENT_ORIGIN] = child_origin;
                    fel_fused[extra + EVENT_SEQUENCE] = child_seq + 1u64;
                    fel_fused[extra + EVENT_TARGET] = child_target;
                    fel_fused[extra + EVENT_KIND] = child_event_kind;
                    fel_fused[extra + EVENT_PAYLOAD] = child_payload;
                    fel_fused[extra + PACKET_ID] = child_packet_id;
                    fel_fused[extra + PACKET_FLOW] = child_flow;
                    fel_fused[extra + PACKET_SIZE] = child_size;
                    fel_fused[extra + PACKET_KIND] = child_packet_kind;
                }

                if performs_occupancy {
                    let depth = queue_depth[lp];
                    if depth == 0u32 {
                        if errors[lp] == ERROR_NONE {
                            errors[lp] = ERROR_ZERO_OCCUPANCY;
                        }
                    } else {
                        queue_head[lp] = (queue_head[lp] + 1u32) % depth;
                    }
                }

                if performs_outbox && planned_outboxes <= 1u32 {
                    let outbox = lp * FUSED_EVENT_PACKET_WORDS;
                    outbox_fused[outbox + EVENT_TIME] = child_time;
                    outbox_fused[outbox + EVENT_PHASE] = child_phase;
                    outbox_fused[outbox + EVENT_ORIGIN] = child_origin;
                    outbox_fused[outbox + EVENT_SEQUENCE] = child_seq;
                    outbox_fused[outbox + EVENT_TARGET] = child_target;
                    outbox_fused[outbox + EVENT_KIND] = child_event_kind;
                    outbox_fused[outbox + EVENT_PAYLOAD] = child_payload;
                    outbox_fused[outbox + PACKET_ID] = child_packet_id;
                    outbox_fused[outbox + PACKET_FLOW] = child_flow;
                    outbox_fused[outbox + PACKET_SIZE] = child_size;
                    outbox_fused[outbox + PACKET_KIND] = child_packet_kind;
                    outbox_count[lp] = 1u32;
                }

                let operation_tags = 1u64
                    + ((!direct_continuation) as u64) * 256u64
                    + (performs_occupancy as u64) * 65536u64
                    + (performs_outbox as u64) * 16777216u64;
                audit[lp] += operation_tags
                    + parent_time
                    + parent_phase
                    + parent_origin
                    + parent_seq
                    + parent_target
                    + parent_event_kind
                    + parent_payload
                    + parent_packet_id
                    + parent_flow
                    + parent_size
                    + parent_packet_kind;
                last_time = child_time;
                last_phase = child_phase;
                last_origin = child_origin;
                last_seq = child_seq;
                last_target = child_target;
                last_event_kind = child_event_kind;
                last_payload = child_payload;
                last_packet_id = child_packet_id;
                last_flow = child_flow;
                last_size = child_size;
                last_packet_kind = child_packet_kind;
                step += 1u32;
            }
            next_time[lp] = if cube_event_less(
                fel_fused[left + EVENT_TIME],
                fel_fused[left + EVENT_PHASE],
                fel_fused[left + EVENT_ORIGIN],
                fel_fused[left + EVENT_SEQUENCE],
                fel_fused[right + EVENT_TIME],
                fel_fused[right + EVENT_PHASE],
                fel_fused[right + EVENT_ORIGIN],
                fel_fused[right + EVENT_SEQUENCE],
            ) {
                fel_fused[left + EVENT_TIME]
            } else {
                fel_fused[right + EVENT_TIME]
            };
        }
    } else {
        next_time[lp] = 18446744073709551615u64;
    }
}

#[cube(launch_unchecked)]
#[allow(clippy::too_many_arguments)]
fn real_replay_round_kernel(
    node_ids: &[u64],
    pending_events: &[u32],
    step_starts: &[u32],
    step_counts: &[u32],
    planned_next_time: &[u64],
    steps: &[u32],
    parent_fused: &[u64],
    frontier_ns: &[u64],
    exclusive_horizon_ns: &[u64],
    next_time: &mut [u64],
    device_horizon: &[u64],
    local_fel_fused: &mut [u64],
    remote_outbox_fused: &mut [u64],
    queue_head: &mut [u32],
    local_child_count: &mut [u32],
    remote_child_count: &mut [u32],
    errors: &mut [u32],
    audit: &mut [u64],
    active_lps: &[u32],
    local_child_capacity: &[u32],
    remote_child_capacity: &[u32],
) {
    let lp = ABSOLUTE_POS;
    if lp < active_lps[0usize] as usize {
        let node = node_ids[lp];
        let pending = pending_events[lp];
        local_child_count[lp] = 0u32;
        remote_child_count[lp] = 0u32;
        if device_horizon[0usize] != exclusive_horizon_ns[0usize] {
            errors[lp] |= REAL_ERROR_HORIZON_STATE_MISMATCH;
        }
        let mut value = audit[lp]
            + node
            + pending as u64 * 256u64
            + frontier_ns[0usize]
            + exclusive_horizon_ns[0usize]
            + device_horizon[0usize];
        let step_start = step_starts[lp];
        let step_count = step_counts[lp];
        let mut step_index = 0u32;
        while step_index < step_count {
            let global_step = step_start + step_index;
            let packed = steps[global_step as usize];
            let kind = packed & REPLAY_KIND_MASK;
            let parent = global_step as usize * FUSED_EVENT_PACKET_WORDS;
            let parent_time = parent_fused[parent + EVENT_TIME];
            let parent_phase = parent_fused[parent + EVENT_PHASE];
            let parent_origin = parent_fused[parent + EVENT_ORIGIN];
            let parent_sequence = parent_fused[parent + EVENT_SEQUENCE];
            let parent_target = parent_fused[parent + EVENT_TARGET];
            let parent_kind = parent_fused[parent + EVENT_KIND];
            let parent_payload = parent_fused[parent + EVENT_PAYLOAD];
            let parent_packet_id = parent_fused[parent + PACKET_ID];
            let parent_flow = parent_fused[parent + PACKET_FLOW];
            let parent_size = parent_fused[parent + PACKET_SIZE];
            let parent_packet_kind = parent_fused[parent + PACKET_KIND];
            if parent_time >= exclusive_horizon_ns[0usize] || parent_time >= device_horizon[0usize]
            {
                errors[lp] |= REAL_ERROR_KEY_OUTSIDE_HORIZON;
            }
            if parent_kind != kind as u64 {
                errors[lp] |= REAL_ERROR_EVENT_KIND_MISMATCH;
            }
            if !cube_event_less(
                parent_time,
                parent_phase,
                parent_origin,
                parent_sequence,
                parent_time,
                parent_phase,
                parent_origin,
                parent_sequence + 1u64,
            ) {
                errors[lp] |= REAL_ERROR_KEY_ORDER_MISMATCH;
            }
            value = value
                + parent_time
                + parent_phase
                + parent_origin
                + parent_sequence
                + parent_target
                + parent_kind
                + parent_payload
                + parent_packet_id
                + parent_flow
                + parent_size
                + parent_packet_kind;
            if kind == EventKind::PacketArrival as u32 {
                value ^= 0xa076_1d64_78bd_642fu64;
            } else if kind == EventKind::TxReady as u32 {
                let occupancy = (packed >> REPLAY_QUEUE_SHIFT) & REPLAY_QUEUE_MASK;
                if occupancy == 0u32 {
                    value ^= 0xe703_7ed1_a0b4_28dbu64;
                } else {
                    queue_head[lp] = (queue_head[lp] + 1u32) % occupancy;
                    value = value + occupancy as u64 + queue_head[lp] as u64;
                }
            } else if kind == EventKind::TxComplete as u32 {
                value += pending as u64 * 3u64;
            } else if kind == EventKind::RemoteArrival as u32 {
                value ^= node + step_index as u64;
            }
            if packed & REPLAY_DIRECT_CONTINUATION_BIT != 0u32 {
                value += 0x8ebc_6af0_9c88_c6e3u64;
            }

            let local_pushes = (packed >> REPLAY_LOCAL_PUSH_SHIFT) & REPLAY_CHILD_COUNT_MASK;
            let remote_writes = (packed >> REPLAY_REMOTE_WRITE_SHIFT) & REPLAY_CHILD_COUNT_MASK;
            let mut child = 0u32;
            while child < local_pushes {
                let child_slot = local_child_count[lp] + child;
                let slot = (lp * local_child_capacity[0usize] as usize + child_slot as usize)
                    * FUSED_EVENT_PACKET_WORDS;
                let child_payload = parent_payload + child as u64 + 1u64;
                local_fel_fused[slot + EVENT_TIME] = parent_time + child as u64 + 1u64;
                local_fel_fused[slot + EVENT_PHASE] = 2u64;
                local_fel_fused[slot + EVENT_ORIGIN] = parent_origin;
                local_fel_fused[slot + EVENT_SEQUENCE] = parent_sequence + child as u64 + 1u64;
                local_fel_fused[slot + EVENT_TARGET] = parent_target;
                local_fel_fused[slot + EVENT_KIND] = EventKind::TxReady as u64;
                local_fel_fused[slot + EVENT_PAYLOAD] = child_payload;
                local_fel_fused[slot + PACKET_ID] = child_payload;
                local_fel_fused[slot + PACKET_FLOW] = parent_flow;
                local_fel_fused[slot + PACKET_SIZE] = parent_size;
                local_fel_fused[slot + PACKET_KIND] = parent_packet_kind;
                let mut word = 0u32;
                while word < FUSED_EVENT_PACKET_WORDS as u32 {
                    value = value
                        + local_fel_fused[slot + word as usize]
                        + lp as u64
                        + step_index as u64;
                    word += 1u32;
                }
                child += 1u32;
            }
            local_child_count[lp] += local_pushes;
            child = 0u32;
            while child < remote_writes {
                let child_slot = remote_child_count[lp] + child;
                let slot = (lp * remote_child_capacity[0usize] as usize + child_slot as usize)
                    * FUSED_EVENT_PACKET_WORDS;
                let child_payload = parent_payload + child as u64 + 1u64;
                remote_outbox_fused[slot + EVENT_TIME] = parent_time + child as u64 + 1u64;
                remote_outbox_fused[slot + EVENT_PHASE] = 0u64;
                remote_outbox_fused[slot + EVENT_ORIGIN] = parent_origin;
                remote_outbox_fused[slot + EVENT_SEQUENCE] = parent_sequence + child as u64 + 1u64;
                remote_outbox_fused[slot + EVENT_TARGET] = parent_target + 1u64;
                remote_outbox_fused[slot + EVENT_KIND] = EventKind::RemoteArrival as u64;
                remote_outbox_fused[slot + EVENT_PAYLOAD] = child_payload;
                remote_outbox_fused[slot + PACKET_ID] = child_payload;
                remote_outbox_fused[slot + PACKET_FLOW] = parent_flow;
                remote_outbox_fused[slot + PACKET_SIZE] = parent_size;
                remote_outbox_fused[slot + PACKET_KIND] = parent_packet_kind;
                let mut word = 0u32;
                while word < FUSED_EVENT_PACKET_WORDS as u32 {
                    value = value
                        + remote_outbox_fused[slot + word as usize]
                        + lp as u64
                        + step_index as u64;
                    word += 1u32;
                }
                child += 1u32;
            }
            remote_child_count[lp] += remote_writes;
            step_index += 1u32;
        }
        audit[lp] = value;
        next_time[lp] = planned_next_time[lp];
    } else {
        next_time[lp] = u64::MAX;
    }
}

#[cube(launch_unchecked)]
fn block_reduction_kernel(next_time: &[u64], block_minima: &mut [u64], #[comptime] width: u32) {
    let lane = UNIT_POS as usize;
    let mut minima = Shared::<[u64]>::new_slice(width as usize);
    minima[lane] = next_time[ABSOLUTE_POS];
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
        block_minima[CUBE_POS_X as usize] = minima[0usize];
    }
}

#[cube(launch_unchecked)]
fn horizon_reduction_kernel(
    minima_input: &[u64],
    horizon: &mut [u64],
    #[comptime] width: u32,
    #[comptime] lookahead_ns: u64,
) {
    let lane = UNIT_POS as usize;
    let mut minima = Shared::<[u64]>::new_slice(width as usize);
    minima[lane] = minima_input[lane];
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
        horizon[0usize] = minima[0usize] + lookahead_ns;
    }
}

#[cube(launch_unchecked)]
fn real_replay_horizon_reduction_kernel(
    minima_input: &[u64],
    horizon: &mut [u64],
    expected_minimum: &[u64],
    next_horizon: &[u64],
    errors: &mut [u32],
    #[comptime] width: u32,
) {
    let lane = UNIT_POS as usize;
    let mut minima = Shared::<[u64]>::new_slice(width as usize);
    minima[lane] = minima_input[lane];
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
        if minima[0usize] != expected_minimum[0usize] {
            errors[0usize] |= REAL_ERROR_MINIMUM_MISMATCH;
        }
        horizon[0usize] = next_horizon[0usize];
    }
}

struct GeneratedKernel {
    source: String,
    entrypoint: String,
}

fn compile_kernel<K: CubeKernel>(kernel: K) -> Result<GeneratedKernel, MetalSpikeError> {
    let address_type = kernel.address_type();
    let definition = kernel.define();
    let entrypoint = definition.options.kernel_name.clone();
    let representation = Compiler::compile(
        &mut MslCompiler::default(),
        definition,
        &Default::default(),
        ExecutionMode::Unchecked,
        address_type,
    )
    .map_err(|error| MetalSpikeError::Metal(format!("CubeCL MSL codegen failed: {error}")))?;
    Ok(GeneratedKernel {
        source: representation.to_string(),
        entrypoint,
    })
}

fn generated_kernels(
    active_lps: usize,
    padded_lanes: usize,
) -> Result<(GeneratedKernel, GeneratedKernel, GeneratedKernel), MetalSpikeError> {
    let client = MetalRuntime::client(&MetalDevice::DefaultDevice);
    let buffer = BufferCompilationArg { inplace: None };
    let round = matched_round_kernel::MatchedRoundKernel::<MetalRuntime>::new(
        KernelSettings::default()
            .cube_dim(CubeDim::new_1d(REDUCTION_LANES as u32))
            .kernel_name("matched_round_kernel"),
        client.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        active_lps as u32,
        (padded_lanes * FUSED_EVENT_PACKET_WORDS) as u32,
        LOOKAHEAD_NS,
        MAX_TRANSITIONS_PER_LP,
    );
    let block_reduction = block_reduction_kernel::BlockReductionKernel::<MetalRuntime>::new(
        KernelSettings::default()
            .cube_dim(CubeDim::new_1d(REDUCTION_LANES as u32))
            .kernel_name("block_reduction_kernel"),
        client.clone(),
        buffer.clone(),
        buffer.clone(),
        REDUCTION_LANES as u32,
    );
    let reduction = horizon_reduction_kernel::HorizonReductionKernel::<MetalRuntime>::new(
        KernelSettings::default()
            .cube_dim(CubeDim::new_1d(REDUCTION_LANES as u32))
            .kernel_name("horizon_reduction_kernel"),
        client,
        buffer.clone(),
        buffer,
        REDUCTION_LANES as u32,
        LOOKAHEAD_NS,
    );
    Ok((
        compile_kernel(round)?,
        compile_kernel(block_reduction)?,
        compile_kernel(reduction)?,
    ))
}

fn generated_real_replay_kernels()
-> Result<(GeneratedKernel, GeneratedKernel, GeneratedKernel), MetalSpikeError> {
    let client = MetalRuntime::client(&MetalDevice::DefaultDevice);
    let buffer = BufferCompilationArg { inplace: None };
    let round = real_replay_round_kernel::RealReplayRoundKernel::<MetalRuntime>::new(
        KernelSettings::default()
            .cube_dim(CubeDim::new_1d(REDUCTION_LANES as u32))
            .kernel_name("real_replay_round_kernel"),
        client.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
    );
    let block_reduction = block_reduction_kernel::BlockReductionKernel::<MetalRuntime>::new(
        KernelSettings::default()
            .cube_dim(CubeDim::new_1d(REDUCTION_LANES as u32))
            .kernel_name("block_reduction_kernel"),
        client.clone(),
        buffer.clone(),
        buffer.clone(),
        REDUCTION_LANES as u32,
    );
    let reduction = real_replay_horizon_reduction_kernel::RealReplayHorizonReductionKernel::<
        MetalRuntime,
    >::new(
        KernelSettings::default()
            .cube_dim(CubeDim::new_1d(REDUCTION_LANES as u32))
            .kernel_name("real_replay_horizon_reduction_kernel"),
        client,
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer.clone(),
        buffer,
        REDUCTION_LANES as u32,
    );
    Ok((
        compile_kernel(round)?,
        compile_kernel(block_reduction)?,
        compile_kernel(reduction)?,
    ))
}

struct UntypedMetalBuffer {
    raw: RawMetalBuffer,
    bytes: usize,
}

impl UntypedMetalBuffer {
    fn new(
        device: &ProtocolObject<dyn MTLDevice>,
        contents: &[u8],
    ) -> Result<Self, MetalSpikeError> {
        let raw = device
            .newBufferWithLength_options(contents.len(), MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| {
                MetalSpikeError::Metal(format!(
                    "Metal failed to allocate a {}-byte shared buffer",
                    contents.len()
                ))
            })?;
        let buffer = Self {
            raw,
            bytes: contents.len(),
        };
        buffer.write(contents);
        Ok(buffer)
    }

    fn write(&self, contents: &[u8]) {
        assert_eq!(contents.len(), self.bytes);
        unsafe {
            std::ptr::copy_nonoverlapping(
                contents.as_ptr(),
                self.raw.contents().as_ptr().cast::<u8>(),
                contents.len(),
            );
        }
    }
}

struct MetalBuffers {
    planes: Vec<UntypedMetalBuffer>,
    block_minima: UntypedMetalBuffer,
    active_lps: usize,
    padded_lanes: usize,
}

impl MetalBuffers {
    fn new(
        device: &ProtocolObject<dyn MTLDevice>,
        state: &WorkloadState,
    ) -> Result<Self, MetalSpikeError> {
        let planes = state
            .planes()
            .into_iter()
            .map(|plane| UntypedMetalBuffer::new(device, plane))
            .collect::<Result<Vec<_>, _>>()?;
        let block_minima =
            UntypedMetalBuffer::new(device, bytes(&vec![u64::MAX; REDUCTION_LANES]))?;
        Ok(Self {
            planes,
            block_minima,
            active_lps: state.active_lps,
            padded_lanes: state.padded_lanes,
        })
    }

    fn write(&self, state: &WorkloadState) {
        let source = state.planes();
        assert_eq!(self.planes.len(), source.len());
        for (buffer, contents) in self.planes.iter().zip(source) {
            buffer.write(contents);
        }
    }

    fn bind(&self, encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>) {
        for (index, plane) in self.planes.iter().enumerate() {
            unsafe {
                encoder.setBuffer_offset_atIndex(Some(&plane.raw), 0, index);
            }
        }
    }

    fn read_state(&self) -> WorkloadState {
        assert_eq!(self.planes.len(), 15);
        WorkloadState {
            active_lps: self.active_lps,
            padded_lanes: self.padded_lanes,
            next_time: self.read_plane(0, self.padded_lanes),
            horizon: self.read_plane(1, 1),
            fel_fused: self.read_plane(2, 2 * self.padded_lanes * FUSED_EVENT_PACKET_WORDS),
            queue_depth: self.read_plane(3, self.padded_lanes),
            queue_head: self.read_plane(4, self.padded_lanes),
            transitions: self.read_plane(5, self.padded_lanes),
            continuations: self.read_plane(6, self.padded_lanes),
            local_push_plan: self.read_plane(7, self.padded_lanes),
            occupancy_plan: self.read_plane(8, self.padded_lanes),
            outbox_plan: self.read_plane(9, self.padded_lanes),
            outbox_fused: self.read_plane(10, self.padded_lanes * FUSED_EVENT_PACKET_WORDS),
            outbox_count: self.read_plane(11, self.padded_lanes),
            errors: self.read_plane(12, self.padded_lanes),
            semantic_flags: self.read_plane(13, self.padded_lanes),
            audit: self.read_plane(14, self.padded_lanes),
        }
    }

    fn read_plane<T: Copy>(&self, index: usize, len: usize) -> Vec<T> {
        let bytes = len * std::mem::size_of::<T>();
        assert_eq!(self.planes[index].bytes, bytes);
        unsafe {
            std::slice::from_raw_parts(self.planes[index].raw.contents().cast::<T>().as_ptr(), len)
                .to_vec()
        }
    }
}

struct RealReplayMetalBuffers {
    node_ids: UntypedMetalBuffer,
    pending_events: UntypedMetalBuffer,
    step_starts: UntypedMetalBuffer,
    step_counts: UntypedMetalBuffer,
    planned_next_time: UntypedMetalBuffer,
    steps: UntypedMetalBuffer,
    parent_fused: UntypedMetalBuffer,
    frontier_ns: UntypedMetalBuffer,
    exclusive_horizon_ns: UntypedMetalBuffer,
    expected_minimum_ns: UntypedMetalBuffer,
    next_horizon_ns: UntypedMetalBuffer,
    active_lps: UntypedMetalBuffer,
    local_child_capacity: UntypedMetalBuffer,
    remote_child_capacity: UntypedMetalBuffer,
    state: Vec<UntypedMetalBuffer>,
    block_minima: UntypedMetalBuffer,
    padded_lanes: usize,
    local_child_slots: usize,
    remote_child_slots: usize,
}

impl RealReplayMetalBuffers {
    fn new(
        device: &ProtocolObject<dyn MTLDevice>,
        plan: &RealReplayPlan,
        state: &RealReplayState,
    ) -> Result<Self, MetalSpikeError> {
        let padded_lanes = state.next_time.len();
        Ok(Self {
            node_ids: UntypedMetalBuffer::new(device, bytes(&plan.node_ids))?,
            pending_events: UntypedMetalBuffer::new(device, bytes(&plan.pending_events))?,
            step_starts: UntypedMetalBuffer::new(device, bytes(&plan.step_starts))?,
            step_counts: UntypedMetalBuffer::new(device, bytes(&plan.step_counts))?,
            planned_next_time: UntypedMetalBuffer::new(device, bytes(&plan.next_time_ns))?,
            steps: UntypedMetalBuffer::new(device, bytes(&plan.steps))?,
            parent_fused: UntypedMetalBuffer::new(device, bytes(&plan.parent_fused))?,
            frontier_ns: UntypedMetalBuffer::new(device, bytes(&plan.frontier_ns))?,
            exclusive_horizon_ns: UntypedMetalBuffer::new(
                device,
                bytes(&plan.exclusive_horizon_ns),
            )?,
            expected_minimum_ns: UntypedMetalBuffer::new(device, bytes(&plan.expected_minimum_ns))?,
            next_horizon_ns: UntypedMetalBuffer::new(device, bytes(&plan.next_horizon_ns))?,
            active_lps: UntypedMetalBuffer::new(device, bytes(&plan.active_lps))?,
            local_child_capacity: UntypedMetalBuffer::new(
                device,
                bytes(&[plan.local_child_capacity]),
            )?,
            remote_child_capacity: UntypedMetalBuffer::new(
                device,
                bytes(&[plan.remote_child_capacity]),
            )?,
            state: state
                .planes()
                .into_iter()
                .map(|plane| UntypedMetalBuffer::new(device, plane))
                .collect::<Result<Vec<_>, _>>()?,
            block_minima: UntypedMetalBuffer::new(device, bytes(&vec![u64::MAX; REDUCTION_LANES]))?,
            padded_lanes,
            local_child_slots: plan.local_child_capacity as usize,
            remote_child_slots: plan.remote_child_capacity as usize,
        })
    }

    fn write_state(&self, state: &RealReplayState) {
        let planes = state.planes();
        assert_eq!(self.state.len(), planes.len());
        for (buffer, contents) in self.state.iter().zip(planes) {
            buffer.write(contents);
        }
    }

    fn bind_round(
        &self,
        encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
        plan: &RealReplayPlan,
        round: usize,
    ) {
        let row_start = plan.row_starts[round];
        unsafe {
            encoder.setBuffer_offset_atIndex(
                Some(&self.node_ids.raw),
                row_start * std::mem::size_of::<u64>(),
                0,
            );
            encoder.setBuffer_offset_atIndex(
                Some(&self.pending_events.raw),
                row_start * std::mem::size_of::<u32>(),
                1,
            );
            encoder.setBuffer_offset_atIndex(
                Some(&self.step_starts.raw),
                row_start * std::mem::size_of::<u32>(),
                2,
            );
            encoder.setBuffer_offset_atIndex(
                Some(&self.step_counts.raw),
                row_start * std::mem::size_of::<u32>(),
                3,
            );
            encoder.setBuffer_offset_atIndex(
                Some(&self.planned_next_time.raw),
                row_start * std::mem::size_of::<u64>(),
                4,
            );
            encoder.setBuffer_offset_atIndex(Some(&self.steps.raw), 0, 5);
            encoder.setBuffer_offset_atIndex(Some(&self.parent_fused.raw), 0, 6);
            encoder.setBuffer_offset_atIndex(
                Some(&self.frontier_ns.raw),
                round * std::mem::size_of::<u64>(),
                7,
            );
            encoder.setBuffer_offset_atIndex(
                Some(&self.exclusive_horizon_ns.raw),
                round * std::mem::size_of::<u64>(),
                8,
            );
            encoder.setBuffer_offset_atIndex(Some(&self.state[0].raw), 0, 9);
            encoder.setBuffer_offset_atIndex(Some(&self.state[1].raw), 0, 10);
            encoder.setBuffer_offset_atIndex(Some(&self.state[2].raw), 0, 11);
            encoder.setBuffer_offset_atIndex(Some(&self.state[3].raw), 0, 12);
            encoder.setBuffer_offset_atIndex(Some(&self.state[4].raw), 0, 13);
            encoder.setBuffer_offset_atIndex(Some(&self.state[5].raw), 0, 14);
            encoder.setBuffer_offset_atIndex(Some(&self.state[6].raw), 0, 15);
            encoder.setBuffer_offset_atIndex(Some(&self.state[7].raw), 0, 16);
            encoder.setBuffer_offset_atIndex(Some(&self.state[8].raw), 0, 17);
            encoder.setBuffer_offset_atIndex(
                Some(&self.active_lps.raw),
                round * std::mem::size_of::<u32>(),
                18,
            );
            encoder.setBuffer_offset_atIndex(Some(&self.local_child_capacity.raw), 0, 19);
            encoder.setBuffer_offset_atIndex(Some(&self.remote_child_capacity.raw), 0, 20);
        }
    }

    fn bind_reduction_round(
        &self,
        encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
        round: usize,
    ) {
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&self.state[1].raw), 0, 1);
            encoder.setBuffer_offset_atIndex(
                Some(&self.expected_minimum_ns.raw),
                round * std::mem::size_of::<u64>(),
                2,
            );
            encoder.setBuffer_offset_atIndex(
                Some(&self.next_horizon_ns.raw),
                round * std::mem::size_of::<u64>(),
                3,
            );
            encoder.setBuffer_offset_atIndex(Some(&self.state[7].raw), 0, 4);
        }
    }

    fn read_state(&self) -> RealReplayState {
        assert_eq!(self.state.len(), 9);
        RealReplayState {
            next_time: self.read_state_plane(0, self.padded_lanes),
            device_horizon: self.read_state_plane(1, 1),
            local_fel_fused: self.read_state_plane(
                2,
                self.padded_lanes * self.local_child_slots * FUSED_EVENT_PACKET_WORDS,
            ),
            remote_outbox_fused: self.read_state_plane(
                3,
                self.padded_lanes * self.remote_child_slots * FUSED_EVENT_PACKET_WORDS,
            ),
            queue_head: self.read_state_plane(4, self.padded_lanes),
            local_child_count: self.read_state_plane(5, self.padded_lanes),
            remote_child_count: self.read_state_plane(6, self.padded_lanes),
            errors: self.read_state_plane(7, self.padded_lanes),
            audit: self.read_state_plane(8, self.padded_lanes),
        }
    }

    fn read_state_plane<T: Copy>(&self, index: usize, len: usize) -> Vec<T> {
        let buffer = &self.state[index];
        let expected = len * std::mem::size_of::<T>();
        assert_eq!(buffer.bytes, expected);
        unsafe {
            std::slice::from_raw_parts(buffer.raw.contents().cast::<T>().as_ptr(), len).to_vec()
        }
    }
}

struct GpuSample {
    host_encode_submit_ns: u64,
    device_ns: u64,
    wall_ns: u64,
}

struct DirectMetalSpike {
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    round_pipeline: MetalPipeline,
    block_reduction_pipeline: MetalPipeline,
    reduction_pipeline: MetalPipeline,
    geometry: SweepGeometry,
    pipeline_setup_ns: u64,
}

impl DirectMetalSpike {
    fn new(active_lps: usize) -> Result<Self, MetalSpikeError> {
        let setup_started = Instant::now();
        let geometry = sweep_geometry(active_lps)?;
        let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
            MetalSpikeError::Metal("Metal system default device is unavailable".into())
        })?;
        let queue = device
            .newCommandQueue()
            .ok_or_else(|| MetalSpikeError::Metal("Metal command queue creation failed".into()))?;
        let (round_source, block_reduction_source, reduction_source) =
            generated_kernels(active_lps, geometry.padded_lanes)?;
        let round_pipeline = create_pipeline(&device, round_source)?;
        let block_reduction_pipeline = create_pipeline(&device, block_reduction_source)?;
        let reduction_pipeline = create_pipeline(&device, reduction_source)?;
        if round_pipeline.maxTotalThreadsPerThreadgroup() < REDUCTION_LANES {
            return Err(MetalSpikeError::Metal(format!(
                "round pipeline supports only {} threads per threadgroup",
                round_pipeline.maxTotalThreadsPerThreadgroup()
            )));
        }
        if block_reduction_pipeline.maxTotalThreadsPerThreadgroup() < REDUCTION_LANES {
            return Err(MetalSpikeError::Metal(format!(
                "block reduction pipeline supports only {} threads per threadgroup",
                block_reduction_pipeline.maxTotalThreadsPerThreadgroup()
            )));
        }
        if reduction_pipeline.maxTotalThreadsPerThreadgroup() < REDUCTION_LANES {
            return Err(MetalSpikeError::Metal(format!(
                "reduction pipeline supports only {} threads per threadgroup",
                reduction_pipeline.maxTotalThreadsPerThreadgroup()
            )));
        }
        if device.maxThreadgroupMemoryLength() < REDUCTION_THREADGROUP_BYTES {
            return Err(MetalSpikeError::Metal(format!(
                "device exposes only {} bytes of threadgroup memory",
                device.maxThreadgroupMemoryLength()
            )));
        }
        Ok(Self {
            device,
            queue,
            round_pipeline,
            block_reduction_pipeline,
            reduction_pipeline,
            geometry,
            pipeline_setup_ns: duration_ns(setup_started.elapsed()),
        })
    }

    fn run(
        &self,
        buffers: &MetalBuffers,
        rounds: usize,
        rounds_per_encoding: usize,
    ) -> Result<GpuSample, MetalSpikeError> {
        let encoding_count = rounds.div_ceil(rounds_per_encoding);
        if encoding_count > MAX_OUTSTANDING_COMMAND_BUFFERS {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "round scale needs more than 64 outstanding Metal command buffers",
            ));
        }
        let body_group_count = MTLSize {
            width: self.geometry.body_threadgroups,
            height: 1,
            depth: 1,
        };
        let final_group_count = MTLSize {
            width: 1,
            height: 1,
            depth: 1,
        };
        let threadgroup = MTLSize {
            width: REDUCTION_LANES,
            height: 1,
            depth: 1,
        };
        let wall_started = Instant::now();
        let mut command_buffers = Vec::with_capacity(encoding_count);
        let mut remaining = rounds;
        while remaining > 0 {
            let encoded_rounds = remaining.min(rounds_per_encoding);
            let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
                MetalSpikeError::Metal("Metal command buffer creation failed".into())
            })?;
            let encoder = command_buffer
                .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
                .ok_or_else(|| {
                    MetalSpikeError::Metal("serial compute encoder creation failed".into())
                })?;
            buffers.bind(&encoder);
            for round in 0..encoded_rounds {
                if self.geometry.body_threadgroups > 1 && round > 0 {
                    unsafe {
                        encoder.setBuffer_offset_atIndex(Some(&buffers.planes[0].raw), 0, 0);
                        encoder.setBuffer_offset_atIndex(Some(&buffers.planes[1].raw), 0, 1);
                    }
                }
                encoder.setComputePipelineState(&self.round_pipeline);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(body_group_count, threadgroup);
                if self.geometry.body_threadgroups > 1 {
                    encoder.setComputePipelineState(&self.block_reduction_pipeline);
                    unsafe {
                        encoder.setBuffer_offset_atIndex(Some(&buffers.block_minima.raw), 0, 1);
                    }
                    encoder
                        .dispatchThreadgroups_threadsPerThreadgroup(body_group_count, threadgroup);
                    unsafe {
                        encoder.setBuffer_offset_atIndex(Some(&buffers.block_minima.raw), 0, 0);
                        encoder.setBuffer_offset_atIndex(Some(&buffers.planes[1].raw), 0, 1);
                    }
                }
                encoder.setComputePipelineState(&self.reduction_pipeline);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(final_group_count, threadgroup);
            }
            encoder.endEncoding();
            command_buffers.push(command_buffer);
            remaining -= encoded_rounds;
        }
        for command_buffer in &command_buffers {
            command_buffer.commit();
        }
        let host_encode_submit_ns = duration_ns(wall_started.elapsed());
        command_buffers
            .last()
            .expect("nonzero rounds produce a command buffer")
            .waitUntilCompleted();
        let wall_ns = duration_ns(wall_started.elapsed());

        let mut device_ns = 0_u64;
        for command_buffer in &command_buffers {
            if command_buffer.status() != MTLCommandBufferStatus::Completed {
                let detail = command_buffer
                    .error()
                    .map(|error| error.localizedDescription().to_string())
                    .unwrap_or_else(|| "no NSError detail".into());
                return Err(MetalSpikeError::Metal(format!(
                    "Metal command buffer status {:?}: {detail}",
                    command_buffer.status()
                )));
            }
            let start = command_buffer.GPUStartTime();
            let end = command_buffer.GPUEndTime();
            if !start.is_finite() || !end.is_finite() || end < start {
                return Err(MetalSpikeError::Metal(format!(
                    "invalid Metal GPU timestamps {start}..{end}"
                )));
            }
            device_ns = device_ns.saturating_add(seconds_ns(end - start));
        }
        Ok(GpuSample {
            host_encode_submit_ns,
            device_ns,
            wall_ns,
        })
    }
}

struct DirectRealReplay {
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    round_pipeline: MetalPipeline,
    block_reduction_pipeline: MetalPipeline,
    reduction_pipeline: MetalPipeline,
    geometry: SweepGeometry,
    pipeline_setup_ns: u64,
}

impl DirectRealReplay {
    fn new(maximum_active_lps: usize) -> Result<Self, MetalSpikeError> {
        let setup_started = Instant::now();
        let geometry = sweep_geometry(maximum_active_lps)?;
        let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
            MetalSpikeError::Metal("Metal system default device is unavailable".into())
        })?;
        let queue = device
            .newCommandQueue()
            .ok_or_else(|| MetalSpikeError::Metal("Metal command queue creation failed".into()))?;
        let (round_source, block_reduction_source, reduction_source) =
            generated_real_replay_kernels()?;
        let round_pipeline = create_pipeline(&device, round_source)?;
        let block_reduction_pipeline = create_pipeline(&device, block_reduction_source)?;
        let reduction_pipeline = create_pipeline(&device, reduction_source)?;
        for (name, pipeline) in [
            ("real replay round", &round_pipeline),
            ("real replay block reduction", &block_reduction_pipeline),
            ("real replay final reduction", &reduction_pipeline),
        ] {
            if pipeline.maxTotalThreadsPerThreadgroup() < REDUCTION_LANES {
                return Err(MetalSpikeError::Metal(format!(
                    "{name} pipeline supports only {} threads per threadgroup",
                    pipeline.maxTotalThreadsPerThreadgroup()
                )));
            }
        }
        if device.maxThreadgroupMemoryLength() < REDUCTION_THREADGROUP_BYTES {
            return Err(MetalSpikeError::Metal(format!(
                "device exposes only {} bytes of threadgroup memory",
                device.maxThreadgroupMemoryLength()
            )));
        }
        Ok(Self {
            device,
            queue,
            round_pipeline,
            block_reduction_pipeline,
            reduction_pipeline,
            geometry,
            pipeline_setup_ns: duration_ns(setup_started.elapsed()),
        })
    }

    fn run(
        &self,
        plan: &RealReplayPlan,
        buffers: &RealReplayMetalBuffers,
        rounds_per_encoding: usize,
    ) -> Result<GpuSample, MetalSpikeError> {
        let encoding_count = plan.rounds().div_ceil(rounds_per_encoding);
        if encoding_count > MAX_OUTSTANDING_COMMAND_BUFFERS {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "real replay needs more than 64 outstanding Metal command buffers",
            ));
        }
        let body_group_count = MTLSize {
            width: self.geometry.body_threadgroups,
            height: 1,
            depth: 1,
        };
        let final_group_count = MTLSize {
            width: 1,
            height: 1,
            depth: 1,
        };
        let threadgroup = MTLSize {
            width: REDUCTION_LANES,
            height: 1,
            depth: 1,
        };
        let wall_started = Instant::now();
        let mut command_buffers = Vec::with_capacity(encoding_count);
        let mut next_round = 0;
        while next_round < plan.rounds() {
            let encoded_rounds = (plan.rounds() - next_round).min(rounds_per_encoding);
            let command_buffer = self.queue.commandBuffer().ok_or_else(|| {
                MetalSpikeError::Metal("Metal command buffer creation failed".into())
            })?;
            let encoder = command_buffer
                .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
                .ok_or_else(|| {
                    MetalSpikeError::Metal("serial compute encoder creation failed".into())
                })?;
            for round in next_round..next_round + encoded_rounds {
                buffers.bind_round(&encoder, plan, round);
                encoder.setComputePipelineState(&self.round_pipeline);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(body_group_count, threadgroup);
                unsafe {
                    encoder.setBuffer_offset_atIndex(Some(&buffers.state[0].raw), 0, 0);
                }
                if self.geometry.body_threadgroups > 1 {
                    encoder.setComputePipelineState(&self.block_reduction_pipeline);
                    unsafe {
                        encoder.setBuffer_offset_atIndex(Some(&buffers.block_minima.raw), 0, 1);
                    }
                    encoder
                        .dispatchThreadgroups_threadsPerThreadgroup(body_group_count, threadgroup);
                    unsafe {
                        encoder.setBuffer_offset_atIndex(Some(&buffers.block_minima.raw), 0, 0);
                    }
                }
                buffers.bind_reduction_round(&encoder, round);
                encoder.setComputePipelineState(&self.reduction_pipeline);
                encoder.dispatchThreadgroups_threadsPerThreadgroup(final_group_count, threadgroup);
            }
            encoder.endEncoding();
            command_buffers.push(command_buffer);
            next_round += encoded_rounds;
        }
        for command_buffer in &command_buffers {
            command_buffer.commit();
        }
        let host_encode_submit_ns = duration_ns(wall_started.elapsed());
        command_buffers
            .last()
            .expect("nonempty real replay produces a command buffer")
            .waitUntilCompleted();
        let wall_ns = duration_ns(wall_started.elapsed());

        let mut device_ns = 0_u64;
        for command_buffer in &command_buffers {
            if command_buffer.status() != MTLCommandBufferStatus::Completed {
                let detail = command_buffer
                    .error()
                    .map(|error| error.localizedDescription().to_string())
                    .unwrap_or_else(|| "no NSError detail".into());
                return Err(MetalSpikeError::Metal(format!(
                    "Metal command buffer status {:?}: {detail}",
                    command_buffer.status()
                )));
            }
            let start = command_buffer.GPUStartTime();
            let end = command_buffer.GPUEndTime();
            if !start.is_finite() || !end.is_finite() || end < start {
                return Err(MetalSpikeError::Metal(format!(
                    "invalid Metal GPU timestamps {start}..{end}"
                )));
            }
            device_ns = device_ns.saturating_add(seconds_ns(end - start));
        }
        Ok(GpuSample {
            host_encode_submit_ns,
            device_ns,
            wall_ns,
        })
    }
}

fn create_pipeline(
    device: &ProtocolObject<dyn MTLDevice>,
    kernel: GeneratedKernel,
) -> Result<MetalPipeline, MetalSpikeError> {
    let source = NSString::from_str(&kernel.source);
    let library = device
        .newLibraryWithSource_options_error(&source, None)
        .map_err(|error| {
            MetalSpikeError::Metal(format!(
                "MSL compilation failed for {}: {}",
                kernel.entrypoint,
                error.localizedDescription()
            ))
        })?;
    let name = NSString::from_str(&kernel.entrypoint);
    let function = library.newFunctionWithName(&name).ok_or_else(|| {
        MetalSpikeError::Metal(format!("MSL entry point not found: {}", kernel.entrypoint))
    })?;
    device
        .newComputePipelineStateWithFunction_error(&function)
        .map_err(|error| {
            MetalSpikeError::Metal(format!(
                "Metal pipeline creation failed for {}: {}",
                kernel.entrypoint,
                error.localizedDescription()
            ))
        })
}

/// Revalidates direct-buffer ABI, serial write visibility, reduction geometry, fused outboxes, and
/// explicit errors. T13's substrate-independent primitive proofs remain carried evidence.
///
/// Direct Metal execution shares the production process-wide guard; concurrent callers queue
/// until execution and readback complete.
pub fn run_metal_correctness_suite() -> Result<MetalCorrectnessReport, MetalSpikeError> {
    let direct = DirectMetalSpike::new(ACTIVE_PORT_LPS)?;
    let initial = WorkloadState::initial(ACTIVE_PORT_LPS)?;
    let buffers = MetalBuffers::new(&direct.device, &initial)?;
    let _execution_guard = metal_device_execution_guard();

    let continuation_slot_association = initial
        .continuations
        .iter()
        .zip(&initial.transitions)
        .position(|(continuations, transitions)| *continuations == 1 && *transitions == 2)
        .map(|lane| {
            let left = fused_fel_offset(0, lane, initial.padded_lanes);
            let right = fused_fel_offset(1, lane, initial.padded_lanes);
            let mut slot_fixture = initial.clone();
            // Force the predecessor to pop the right slot. This is the case a hardcoded-left
            // direct continuation corrupts, so CPU/GPU agreement alone cannot mask the bug.
            slot_fixture.fel_fused[left + EVENT_TIME] = 1;
            slot_fixture.fel_fused[right + EVENT_TIME] = 0;
            let selected = right;
            let unselected = left;
            let expected_time = slot_fixture.fel_fused[selected + EVENT_TIME]
                .max(slot_fixture.horizon[0])
                .wrapping_add(LOOKAHEAD_NS)
                .wrapping_add((lane as u64 + 1) & 7);
            let expected_sequence =
                slot_fixture.fel_fused[selected + EVENT_SEQUENCE].wrapping_add(2);
            let original_selected =
                &slot_fixture.fel_fused[selected..selected + FUSED_EVENT_PACKET_WORDS];
            let original_unselected =
                &slot_fixture.fel_fused[unselected..unselected + FUSED_EVENT_PACKET_WORDS];

            let mut slot_cpu = slot_fixture.clone();
            measure_cpu_rounds(&mut slot_cpu, 1, 1);
            buffers.write(&slot_fixture);
            direct.run(&buffers, 1, 1)?;
            let slot_gpu = buffers.read_state();

            Ok::<bool, MetalSpikeError>(
                slot_cpu == slot_gpu
                    && &slot_cpu.fel_fused[unselected..unselected + FUSED_EVENT_PACKET_WORDS]
                        == original_unselected
                    && &slot_cpu.fel_fused[selected..selected + FUSED_EVENT_PACKET_WORDS]
                        != original_selected
                    && slot_cpu.fel_fused[selected + EVENT_TIME] == expected_time
                    && slot_cpu.fel_fused[selected + EVENT_SEQUENCE] == expected_sequence,
            )
        })
        .transpose()?
        .unwrap_or(false);

    let mut cpu = initial.clone();
    measure_cpu_rounds(&mut cpu, 32, 1);
    buffers.write(&initial);
    direct.run(&buffers, 32, 32)?;
    let gpu = buffers.read_state();
    if cpu != gpu {
        return Err(MetalSpikeError::PrimitiveMismatch(
            "direct Metal and matched CPU states differ",
        ));
    }
    let semantic_same_time_continuation = cpu.continuations[..cpu.active_lps]
        .iter()
        .zip(&cpu.semantic_flags)
        .filter(|(continuations, _)| **continuations > 0)
        .all(|(_, flags)| *flags == 0b1_1111);

    let mut invalid = initial.clone();
    invalid.queue_depth[0] = 0;
    invalid.occupancy_plan[0] = 1;
    invalid.outbox_plan[1] = 2;
    invalid.transitions[2] = MAX_TRANSITIONS_PER_LP + 1;
    let mut invalid_cpu = invalid.clone();
    measure_cpu_rounds(&mut invalid_cpu, 1, 1);
    buffers.write(&invalid);
    direct.run(&buffers, 1, 1)?;
    let invalid_gpu = buffers.read_state();
    let explicit_errors = invalid_cpu == invalid_gpu
        && invalid_gpu.errors[0] == ERROR_ZERO_OCCUPANCY
        && invalid_gpu.errors[1] == ERROR_OUTBOX_OVERFLOW
        && invalid_gpu.errors[2] == ERROR_TRANSITION_OVERFLOW;

    Ok(MetalCorrectnessReport {
        fixed_width_u64: std::mem::size_of::<u64>() == 8
            && std::mem::size_of::<crate::EventKey>() == 32
            && std::mem::size_of::<crate::Event>() == 56
            && std::mem::size_of::<crate::PacketDescriptor>() == 32
            && std::mem::size_of::<crate::NodeDescriptor>() == 16,
        event_key_total_order: true,
        exclusive_lp_ownership: true,
        role_worklists_from_one_image: true,
        bounded_fel: true,
        bounded_outbox: true,
        deterministic_compaction: true,
        device_horizon_between_dispatches: true,
        // Serial encoder ordering plus tracked shared resources is the audited sufficient rule.
        explicit_inter_dispatch_barrier: false,
        explicit_device_errors: explicit_errors,
        packet_in_event_fusion: true,
        serial_encoder_dependency_verified: cpu == gpu,
        single_dispatch_1024_lane_reduction: REDUCTION_LANES == 1_024
            && REDUCTION_THREADGROUP_BYTES == 8_192,
        semantic_same_time_continuation_verified: semantic_same_time_continuation,
        continuation_slot_association_verified: continuation_slot_association,
        matched_cpu_gpu_state: cpu == gpu,
    })
}

/// Runs the fair T13c sweep: same state and LP body, paired on one machine with a genuine
/// persistent CPU worker configuration, one warmup per width, and three alternating-order samples.
///
/// Direct Metal execution shares the production process-wide guard; concurrent callers queue
/// until execution and readback complete.
pub fn benchmark_metal(
    config: MetalSpikeBenchmarkConfig,
) -> Result<MetalSpikeBenchmarkReport, MetalSpikeError> {
    validate_benchmark_config(&config)?;
    let mut scales = Vec::with_capacity(config.sweep_points.len());
    let mut pipeline_setup_ns = 0_u64;
    for point in config.sweep_points.iter().copied() {
        let geometry = sweep_geometry(point.active_lps)?;
        let workload = scaled_workload_profile(point.active_lps)?;
        let direct = DirectMetalSpike::new(point.active_lps)?;
        pipeline_setup_ns = pipeline_setup_ns.saturating_add(direct.pipeline_setup_ns);
        let initial = WorkloadState::initial(point.active_lps)?;
        let buffers = MetalBuffers::new(&direct.device, &initial)?;
        let _execution_guard = metal_device_execution_guard();

        let mut cpu_warmup = initial.clone();
        measure_cpu_rounds(&mut cpu_warmup, point.warmup_rounds, config.cpu_workers);
        buffers.write(&initial);
        direct.run(&buffers, point.warmup_rounds, config.rounds_per_encoding)?;
        let gpu_warmup = buffers.read_state();
        ensure_states_match(point.active_lps, None, "warmup", &cpu_warmup, &gpu_warmup)?;

        let mut host_encode_submit_ns = Vec::with_capacity(config.samples);
        let mut device_ns = Vec::with_capacity(config.samples);
        let mut gpu_wall_ns = Vec::with_capacity(config.samples);
        let mut matched_cpu_ns = Vec::with_capacity(config.samples);
        let mut cpu_checksums = Vec::with_capacity(config.samples);
        let mut gpu_checksums = Vec::with_capacity(config.samples);

        for sample in 0..config.samples {
            let mut cpu_state = initial.clone();
            let run_cpu = |state: &mut WorkloadState| {
                measure_cpu_rounds(state, point.rounds, config.cpu_workers)
            };
            let run_gpu = || -> Result<(GpuSample, WorkloadState), MetalSpikeError> {
                buffers.write(&initial);
                let measurement = direct.run(&buffers, point.rounds, config.rounds_per_encoding)?;
                Ok((measurement, buffers.read_state()))
            };

            let (cpu_ns, gpu_measurement, gpu_state) = if sample % 2 == 0 {
                let cpu_ns = run_cpu(&mut cpu_state);
                let (gpu, state) = run_gpu()?;
                (cpu_ns, gpu, state)
            } else {
                let (gpu, state) = run_gpu()?;
                let cpu_ns = run_cpu(&mut cpu_state);
                (cpu_ns, gpu, state)
            };
            let cpu_checksum = checksum(&cpu_state);
            let gpu_checksum = checksum(&gpu_state);
            ensure_states_match(
                point.active_lps,
                Some(sample),
                "measured sample",
                &cpu_state,
                &gpu_state,
            )?;
            if cpu_checksum != gpu_checksum {
                return Err(MetalSpikeError::StateMismatch(format!(
                    "checksum divergence at width {} sample {}: CPU {} != GPU {}",
                    point.active_lps, sample, cpu_checksum, gpu_checksum
                )));
            }
            cpu_checksums.push(cpu_checksum);
            gpu_checksums.push(gpu_checksum);
            matched_cpu_ns.push(cpu_ns);
            host_encode_submit_ns.push(gpu_measurement.host_encode_submit_ns);
            device_ns.push(gpu_measurement.device_ns);
            gpu_wall_ns.push(gpu_measurement.wall_ns);
        }
        scales.push(GateScaleMeasurement {
            active_lps: point.active_lps,
            padded_lanes: geometry.padded_lanes,
            body_threadgroups: geometry.body_threadgroups,
            reduction_dispatches_per_round: geometry.reduction_dispatches_per_round,
            rounds: point.rounds,
            warmup_rounds: point.warmup_rounds,
            encodings: point.rounds.div_ceil(config.rounds_per_encoding),
            dispatches_per_round: geometry.dispatches_per_round,
            workload,
            pipeline_setup_ns: direct.pipeline_setup_ns,
            host_encode_submit_ns,
            device_ns,
            gpu_wall_ns,
            matched_cpu_ns,
            cpu_checksums,
            gpu_checksums,
            matched_checksums: true,
            no_host_sync_between_rounds: true,
        });
    }
    Ok(MetalSpikeBenchmarkReport {
        substrate: SUBSTRATE_VERSION,
        pipeline_setup_ns,
        rounds_per_encoding: config.rounds_per_encoding,
        cpu_workers: config.cpu_workers,
        workload: matched_workload_profile(),
        scales,
    })
}

/// Replays one real-image trace window through the same persistent CPU workers and direct Metal
/// drain/reduction shape. The trace supplies real LP membership, work distribution, event-kind
/// order, child counts, queue occupancy, skew, and next-time minima. Event/payload words are
/// deterministic simulated 88-byte records, and all mutated state remains spike-local rather than
/// production simulator state. Metal dispatches the fixed maximum geometry but gates every body on
/// that real round's active width.
///
/// Direct Metal execution shares the production process-wide guard; concurrent callers queue
/// until execution and readback complete.
pub fn benchmark_real_replay(
    warmup_trace: &RealReplayTrace,
    measured_trace: &RealReplayTrace,
    config: RealReplayBenchmarkConfig,
) -> Result<RealReplayBenchmarkReport, MetalSpikeError> {
    validate_real_replay_benchmark(warmup_trace, measured_trace, &config)?;
    let warmup_plan = RealReplayPlan::new(warmup_trace)?;
    let measured_plan = RealReplayPlan::new(measured_trace)?;
    let maximum_active_lps = warmup_plan
        .maximum_active_lps()
        .max(measured_plan.maximum_active_lps());
    let direct = DirectRealReplay::new(maximum_active_lps)?;
    let geometry = direct.geometry;

    let warmup_initial = RealReplayState::initial(
        geometry.padded_lanes,
        warmup_plan.exclusive_horizon_ns[0],
        &warmup_plan,
    );
    let warmup_buffers =
        RealReplayMetalBuffers::new(&direct.device, &warmup_plan, &warmup_initial)?;
    let _execution_guard = metal_device_execution_guard();
    let mut cpu_warmup = warmup_initial.clone();
    measure_real_cpu_replay(&mut cpu_warmup, &warmup_plan, config.cpu_worker_counts[0]);
    direct.run(&warmup_plan, &warmup_buffers, config.rounds_per_encoding)?;
    let gpu_warmup = warmup_buffers.read_state();
    ensure_real_replay_states_match(None, "warmup", &cpu_warmup, &gpu_warmup)?;
    drop(cpu_warmup);
    drop(gpu_warmup);
    drop(warmup_buffers);
    drop(warmup_initial);

    let initial = RealReplayState::initial(
        geometry.padded_lanes,
        measured_plan.exclusive_horizon_ns[0],
        &measured_plan,
    );
    let buffers = RealReplayMetalBuffers::new(&direct.device, &measured_plan, &initial)?;
    // Keep one GPU-produced equality oracle. Timed CPU states are checked against it and released
    // before any GPU timing, so only compact durations and checksums cross an order boundary.
    buffers.write_state(&initial);
    direct.run(&measured_plan, &buffers, config.rounds_per_encoding)?;
    let gpu_reference = buffers.read_state();
    let gpu_reference_checksum = checksum_real_replay(&gpu_reference);

    let mut samples = Vec::with_capacity(config.samples);
    for sample in 0..config.samples {
        let run_cpu_samples = || -> Result<(Vec<u64>, Vec<u64>), MetalSpikeError> {
            let mut cpu_ns = Vec::with_capacity(config.cpu_worker_counts.len());
            let mut cpu_checksums = Vec::with_capacity(config.cpu_worker_counts.len());
            for (worker_index, workers) in config.cpu_worker_counts.iter().copied().enumerate() {
                let mut neutral_state = initial.clone();
                let mut cpu_state = initial.clone();
                // Give every timed CPU replay the same immediate predecessor at this width.
                measure_real_cpu_replay(&mut neutral_state, &measured_plan, workers);
                drop(neutral_state);

                let elapsed = measure_real_cpu_replay(&mut cpu_state, &measured_plan, workers);
                let cpu_checksum = checksum_real_replay(&cpu_state);
                ensure_real_replay_states_match(
                    Some(sample),
                    "measured CPU sample",
                    &cpu_state,
                    &gpu_reference,
                )?;
                if cpu_checksum != gpu_reference_checksum {
                    return Err(MetalSpikeError::StateMismatch(format!(
                        "real replay checksum divergence at sample {sample}, W{}: CPU {} != GPU {}",
                        config.cpu_worker_counts[worker_index],
                        cpu_checksum,
                        gpu_reference_checksum
                    )));
                }
                drop(cpu_state);
                cpu_ns.push(elapsed);
                cpu_checksums.push(cpu_checksum);
            }
            Ok((cpu_ns, cpu_checksums))
        };
        let run_gpu = || -> Result<(GpuSample, u64), MetalSpikeError> {
            // Give every timed GPU replay the same immediate predecessor in either order.
            buffers.write_state(&initial);
            direct.run(&measured_plan, &buffers, config.rounds_per_encoding)?;

            buffers.write_state(&initial);
            let measurement = direct.run(&measured_plan, &buffers, config.rounds_per_encoding)?;
            let gpu_state = buffers.read_state();
            let gpu_checksum = checksum_real_replay(&gpu_state);
            ensure_real_replay_states_match(
                Some(sample),
                "measured GPU sample",
                &gpu_reference,
                &gpu_state,
            )?;
            if gpu_checksum != gpu_reference_checksum {
                return Err(MetalSpikeError::StateMismatch(format!(
                    "real replay GPU checksum divergence at sample {sample}: reference {} != sample {}",
                    gpu_reference_checksum, gpu_checksum
                )));
            }
            drop(gpu_state);
            Ok((measurement, gpu_checksum))
        };
        let ((cpu_ns, cpu_checksums), gpu_measurement, gpu_checksum) = if sample % 2 == 0 {
            let cpu = run_cpu_samples()?;
            let (gpu, checksum) = run_gpu()?;
            (cpu, gpu, checksum)
        } else {
            let (gpu, checksum) = run_gpu()?;
            let cpu = run_cpu_samples()?;
            (cpu, gpu, checksum)
        };
        samples.push(RealReplaySample {
            host_encode_submit_ns: gpu_measurement.host_encode_submit_ns,
            device_ns: gpu_measurement.device_ns,
            gpu_wall_ns: gpu_measurement.wall_ns,
            cpu_ns,
            cpu_checksums,
            gpu_checksum,
        });
    }
    drop(gpu_reference);

    Ok(RealReplayBenchmarkReport {
        substrate: SUBSTRATE_VERSION,
        profile: real_replay_profile(measured_trace)?,
        rounds: measured_plan.rounds(),
        warmup_rounds: warmup_plan.rounds(),
        padded_lanes: geometry.padded_lanes,
        body_threadgroups: geometry.body_threadgroups,
        reduction_dispatches_per_round: geometry.reduction_dispatches_per_round,
        dispatches_per_round: geometry.dispatches_per_round,
        rounds_per_encoding: config.rounds_per_encoding,
        pipeline_setup_ns: direct.pipeline_setup_ns,
        cpu_worker_counts: config.cpu_worker_counts,
        samples,
        matched_checksums: true,
        no_host_sync_between_rounds: true,
        resident_parent_stream_bytes: measured_plan.parent_fused.len() * std::mem::size_of::<u64>(),
        local_fel_fused_bytes: initial.local_fel_fused.len() * std::mem::size_of::<u64>(),
        remote_outbox_fused_bytes: initial.remote_outbox_fused.len() * std::mem::size_of::<u64>(),
        trace_consistent_horizon_dependency: true,
        variable_active_lp_guard: true,
    })
}

fn validate_real_replay_benchmark(
    warmup_trace: &RealReplayTrace,
    measured_trace: &RealReplayTrace,
    config: &RealReplayBenchmarkConfig,
) -> Result<(), MetalSpikeError> {
    warmup_trace.validate()?;
    measured_trace.validate()?;
    if warmup_trace.rounds.is_empty() || measured_trace.rounds.is_empty() {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "real replay requires nonempty warmup and measured traces",
        ));
    }
    if config.samples != 4 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "the real-image gate requires exactly four measured samples",
        ));
    }
    if config.rounds_per_encoding < 1_024 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "real replay long residency requires at least 1,024 rounds per encoding",
        ));
    }
    if config.cpu_worker_counts.is_empty() || config.cpu_worker_counts.contains(&0) || {
        let mut sorted = config.cpu_worker_counts.clone();
        sorted.sort_unstable();
        sorted.dedup();
        sorted.len() != config.cpu_worker_counts.len()
    } {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "real replay CPU worker counts must be nonzero and unique",
        ));
    }
    let maximum_active_lps = warmup_trace
        .rounds
        .iter()
        .chain(&measured_trace.rounds)
        .map(|round| round.active_lp_count)
        .max()
        .unwrap_or(0);
    let geometry = sweep_geometry(maximum_active_lps)?;
    if config
        .cpu_worker_counts
        .iter()
        .any(|workers| *workers > geometry.padded_lanes)
    {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "real replay CPU workers exceed the padded replay width",
        ));
    }
    if [warmup_trace.rounds.len(), measured_trace.rounds.len()]
        .into_iter()
        .any(|rounds| rounds.div_ceil(config.rounds_per_encoding) > MAX_OUTSTANDING_COMMAND_BUFFERS)
    {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "one real replay sample may use at most 64 command buffers",
        ));
    }
    Ok(())
}

fn ensure_real_replay_states_match(
    sample: Option<usize>,
    phase: &str,
    cpu: &RealReplayState,
    gpu: &RealReplayState,
) -> Result<(), MetalSpikeError> {
    if let Some((lane, error)) = cpu
        .errors
        .iter()
        .copied()
        .enumerate()
        .find(|(_, error)| *error != 0)
    {
        return Err(MetalSpikeError::StateMismatch(format!(
            "real replay CPU error at lane {lane} during {phase}: flags=0x{error:x}"
        )));
    }
    if let Some((lane, error)) = gpu
        .errors
        .iter()
        .copied()
        .enumerate()
        .find(|(_, error)| *error != 0)
    {
        return Err(MetalSpikeError::StateMismatch(format!(
            "real replay GPU error at lane {lane} during {phase}: flags=0x{error:x}"
        )));
    }
    if cpu == gpu {
        return Ok(());
    }
    let sample = sample
        .map(|sample| format!(" sample {sample}"))
        .unwrap_or_default();
    Err(MetalSpikeError::StateMismatch(format!(
        "real replay state divergence{sample} during {phase}"
    )))
}

fn checksum_real_replay(state: &RealReplayState) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for plane in state.planes() {
        for byte in plane {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

fn validate_benchmark_config(config: &MetalSpikeBenchmarkConfig) -> Result<(), MetalSpikeError> {
    if config.samples != 3 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "the fair gate requires exactly three measured samples",
        ));
    }
    if config.sweep_points.is_empty() {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "the sweep must contain at least one width",
        ));
    }
    if config.cpu_workers == 0 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "the matched CPU worker count must be nonzero",
        ));
    }
    if config.rounds_per_encoding < 1_024 {
        return Err(MetalSpikeError::InvalidBenchmarkConfig(
            "long residency requires at least 1,024 rounds per encoding",
        ));
    }
    for point in &config.sweep_points {
        scaled_workload_profile(point.active_lps)?;
        if point.rounds == 0 || point.warmup_rounds == 0 {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "every sweep width requires nonzero warmup and measured rounds",
            ));
        }
        if config.cpu_workers > point.active_lps {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "CPU workers cannot exceed the active-LP width",
            ));
        }
        if [point.rounds, point.warmup_rounds]
            .into_iter()
            .any(|rounds| {
                rounds.div_ceil(config.rounds_per_encoding) > MAX_OUTSTANDING_COMMAND_BUFFERS
            })
        {
            return Err(MetalSpikeError::InvalidBenchmarkConfig(
                "one sample may use at most 64 outstanding Metal command buffers",
            ));
        }
    }
    Ok(())
}

fn ensure_states_match(
    active_lps: usize,
    sample: Option<usize>,
    phase: &str,
    cpu: &WorkloadState,
    gpu: &WorkloadState,
) -> Result<(), MetalSpikeError> {
    if cpu == gpu {
        return Ok(());
    }
    let sample = sample
        .map(|sample| format!(" sample {sample}"))
        .unwrap_or_default();
    Err(MetalSpikeError::StateMismatch(format!(
        "full-state divergence at width {active_lps}{sample} during {phase}"
    )))
}

fn checksum(state: &WorkloadState) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for plane in state.planes() {
        for byte in plane {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

fn bytes<T>(values: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(values.as_ptr().cast::<u8>(), std::mem::size_of_val(values))
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn seconds_ns(seconds: f64) -> u64 {
    let nanoseconds = seconds * 1_000_000_000.0;
    if nanoseconds >= u64::MAX as f64 {
        u64::MAX
    } else {
        nanoseconds.round() as u64
    }
}

fn coverage_ppm(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        return 0;
    }
    let ppm = numerator
        .saturating_mul(1_000_000)
        .checked_div(denominator)
        .unwrap_or(0)
        .min(1_000_000);
    ppm as u32
}

fn events_per_second(events_per_round: u64, nanoseconds_per_round: f64) -> f64 {
    events_per_round as f64 * 1_000_000_000.0 / nanoseconds_per_round
}

fn median(values: &[u64]) -> u64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    let upper = values.len() / 2;
    if values.len().is_multiple_of(2) {
        ((u128::from(values[upper - 1]) + u128::from(values[upper])) / 2) as u64
    } else {
        values[upper]
    }
}

fn median_ns_per_round(values: &[u64], rounds: usize) -> f64 {
    assert!(!values.is_empty(), "median requires at least one sample");
    assert!(rounds > 0, "per-round median requires at least one round");
    let mut values = values.to_vec();
    values.sort_unstable();
    let upper = values.len() / 2;
    let twice_median_ns = if values.len().is_multiple_of(2) {
        u128::from(values[upper - 1]) + u128::from(values[upper])
    } else {
        u128::from(values[upper]) * 2
    };
    twice_median_ns as f64 / 2.0 / rounds as f64
}

#[cfg(test)]
mod tests {
    use super::median_ns_per_round;

    #[test]
    fn even_sample_median_preserves_the_half_nanosecond_before_round_division() {
        assert_eq!(median_ns_per_round(&[1, 3, 4, 5], 4), 0.875);
    }
}

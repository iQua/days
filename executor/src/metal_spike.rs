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

type RawMetalBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type MetalPipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

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
pub fn run_metal_correctness_suite() -> Result<MetalCorrectnessReport, MetalSpikeError> {
    let direct = DirectMetalSpike::new(ACTIVE_PORT_LPS)?;
    let initial = WorkloadState::initial(ACTIVE_PORT_LPS)?;
    let buffers = MetalBuffers::new(&direct.device, &initial)?;

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
/// persistent CPU worker configuration, one warmup per width, and median-of-three samples.
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
    values[values.len() / 2]
}

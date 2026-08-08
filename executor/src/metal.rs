//! Correctness-first production Metal executor.
//!
//! The backend keeps the safe-horizon round loop resident on the device within bounded encoding
//! waves. Deterministic 1,024-lane reductions publish each horizon, stably compact active LPs, and
//! resolve round control. One lane per active LP then performs chronological drains and real
//! transitions, followed by a stable parallel prefix and deterministic boundary-only remote
//! exchange. The host synchronizes only at wave boundaries, and every such synchronization is
//! reported in [`MetalRun`]. Role-split transition kernels are intentionally deferred to a later
//! optimization milestone.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::{NSRange, NSString};
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
    MTLCommonCounterSetTimestamp, MTLComputeCommandEncoder, MTLComputePassDescriptor,
    MTLComputePipelineState, MTLCounterErrorValue, MTLCounterResultTimestamp,
    MTLCounterSampleBuffer, MTLCounterSampleBufferDescriptor, MTLCounterSamplingPoint,
    MTLCounterSet, MTLCreateSystemDefaultDevice, MTLDevice, MTLDispatchType, MTLLibrary,
    MTLResourceOptions, MTLSize, MTLStorageMode,
};

use crate::device_compaction::{CompactionEntity, CompactionPlan, CompactionShape};
use crate::device_scheduler::{
    QUEUE_META_WORDS, prepare_device_schedulers, restore_device_scheduler,
};
use crate::planner_capacity::{PlannerCapacityContext, PlannerCapacityMode, TcpMinimumPacketSize};
use crate::tcp::{TcpCubic, TcpReno};
use crate::{
    ArrivalDisposition, Backend, CapacityRetryRecord, CapacityWarmStart, DeviceCapacityCaps,
    DeviceCapacityFloors, Event, EventKey, EventKind, FlowGeneratorKind, FlowId, GeneratorStatus,
    GeneratorTermination, NodeId, NodeKind, ObservationMode, PacketArrivalObservation,
    PacketDeparture, PacketDescriptor, PacketKind, PayloadId, RunResult, RunSummary,
    SimulationImage, TcpAckHeader, TcpCongestionControl, TcpDataHeader, TcpPhase, TcpReceiveRange,
    TcpTimerState, validate,
};

const LANES: usize = 1_024;
const EVENT_WORDS: usize = 14;
const NODE_WORDS: usize = 11;
const GENERATOR_WORDS: usize = 43;
const FLOW_WORDS: usize = 6;
const LINK_WORDS: usize = 4;
const ARENA_META_WORDS: usize = 4;
const SUMMARY_COUNTERS: usize = 12;
const OBSERVED_WORDS: usize = 7;
const DEPARTURE_WORDS: usize = 12;
const ARRIVAL_WORDS: usize = 13;
const LP_STATE_WORDS: usize = 7;
const OBSERVATION_META_WORDS: usize = ARENA_META_WORDS * 3;
const INBOUND_META_WORDS: usize = 2;
const LP_STREAM_META_WORDS: usize = 4;
const OUTBOUND_META_WORDS: usize = 2;
const OUTBOUND_ENTRY_WORDS: usize = 2;
const CHANNEL_BATCH_WORDS: usize = 4;
const ACTIVE_STREAM_ENTRY_WORDS: usize = 5;
const TCP_RECEIVER_WORDS: usize = 7;
const TCP_RANGE_WORDS: usize = 2;
use crate::tcp_ledger_ring::{
    LEDGER_META_HEAD, LEDGER_META_HIGH_WATER, TCP_LEDGER_META_WORDS, TCP_LEDGER_RECORD_WORDS,
    ledger_high_water_vector,
};

/// One failed device attempt, carrying the retry-sizing evidence the failure produced.
///
/// A TCP segment-ledger capacity fault attaches the per-flow occupancy high-water vector read back
/// from the same plane the fault aborted on. Every other failure carries `None`, so the retry loop
/// falls back to the first-offender growth it always used.
struct AttemptFailure {
    error: MetalError,
    ledger_high_water: Option<Vec<u32>>,
}

impl From<MetalError> for AttemptFailure {
    fn from(error: MetalError) -> Self {
        Self {
            error,
            ledger_high_water: None,
        }
    }
}
// The retained k32 profile averages about 1,300 transitions per round. 4,096 keeps ordinary
// LP drains single-launch while putting a finite ceiling on pathological device work per lane.
const DEFAULT_TRANSITIONS_PER_DISPATCH: usize = 4_096;
// Eight SIMD32 groups amortize dispatch overhead without consuming the maximum 1,024-thread
// residency footprint. Work assignment depends only on compacted LP rank, so this is tunable.
const DEFAULT_ROUND_THREADS_PER_THREADGROUP: usize = 256;
// Command buffers retain the proven 16,384-attempt cap. Waves are deliberately much shorter:
// every attempt contains wide dispatches even after C_DONE makes them no-ops, so a 64-attempt
// device-state check bounds speculative tail waste while still amortizing one host synchronization
// across many semantic rounds. MAX_COMMAND_BUFFERS remains the absolute buffer-count bound when
// callers deliberately request shorter buffers.
const MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER: usize = 16_384;
const MAX_ENCODED_PAIRS_PER_WAVE: usize = 64;
// T20l fix 2: the readback gather is one thread per entity, and an entity's work is bounded by its
// own live count. 256 keeps the dispatch a multiple of every current execution width without
// reserving the maximum residency footprint; the gather's output does not depend on it.
const COMPACT_THREADS_PER_THREADGROUP: usize = 256;
const DEFAULT_ROUNDS_PER_COMMAND_BUFFER: usize = MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER;
const MAX_COMMAND_BUFFERS: usize = 64;
const PROFILED_ATTEMPTS: usize = 128;

static METAL_DEVICE_EXECUTION: Mutex<()> = Mutex::new(());
static METAL_DIRECT: Mutex<Option<Arc<DirectMetal>>> = Mutex::new(None);

#[cfg(feature = "metal-test-hooks")]
std::thread_local! {
    static PANIC_AFTER_NEXT_EXECUTION: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    /// Words this thread has copied out of device buffers since the last reset.
    ///
    /// Thread-local like the panic hook above: the retry loop, the readback and the test all run
    /// on the caller's thread, so no shared mutable state is introduced.
    static READBACK_WORDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// Words in every result plane of the last attempt this thread allocated buffers for.
    ///
    /// T20l fix 2's counterfactual: this is exactly what the pre-fix `finish` read back, so
    /// `readback_words / plane_words` is the compaction ratio a test can assert on.
    static PLANE_WORDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Acquires the supported process-wide Metal execution envelope.
///
/// The guard schedules device access rather than protecting Rust state, so a panic must not
/// prevent later callers from executing.
pub(crate) fn metal_device_execution_guard() -> MutexGuard<'static, ()> {
    METAL_DEVICE_EXECUTION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Arms a one-shot panic after this thread's next successful Metal execution.
///
/// This test-only hook fires before readback while the process-wide execution guard remains held.
#[cfg(feature = "metal-test-hooks")]
#[doc(hidden)]
pub fn panic_after_next_execution_for_testing() {
    PANIC_AFTER_NEXT_EXECUTION.with(|armed| armed.set(true));
}

#[cfg(feature = "metal-test-hooks")]
fn panic_after_execution_if_requested() {
    if PANIC_AFTER_NEXT_EXECUTION.with(std::cell::Cell::take) {
        panic!("injected panic after Metal execution");
    }
}

/// Returns and clears this thread's device-to-host readback word count.
///
/// T20l fix 1 is an ordering property — *when* the result arena crosses the bus — and this is the
/// quantity that makes it testable: a screened faulting attempt copies the control plane (and, on
/// a ledger fault, the per-flow occupancy metadata) instead of every plane.
#[cfg(feature = "metal-test-hooks")]
#[doc(hidden)]
pub fn take_readback_words_for_testing() -> u64 {
    READBACK_WORDS.with(std::cell::Cell::take)
}

#[cfg(feature = "metal-test-hooks")]
fn account_readback_words(words: usize) {
    READBACK_WORDS.with(|total| total.set(total.get().saturating_add(words as u64)));
}

/// Returns the total result-plane word count of the last attempt this thread planned.
///
/// T20l fix 2's gate is a ratio, and this is its denominator: the pre-fix `finish` read every one
/// of these words on every successful attempt. It is recorded rather than derived so a test does
/// not have to re-plan the image to know what the uncompacted readback would have cost.
#[cfg(feature = "metal-test-hooks")]
#[doc(hidden)]
pub fn last_plane_words_for_testing() -> u64 {
    PLANE_WORDS.with(std::cell::Cell::get)
}

#[cfg(feature = "metal-test-hooks")]
fn record_plane_words(words: u64) {
    PLANE_WORDS.with(|total| total.set(words));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttemptPhase {
    Horizon,
    Compaction,
    DrainExecute,
    ContinuationControl,
    ExchangePrefix,
    ExchangeScatter,
    TargetMerge,
    FinalControl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DispatchGeometry {
    FixedControl,
    ActiveWorklist,
    Parallel,
}

impl DispatchGeometry {
    const fn threads_per_threadgroup(self, parallel_threads: usize) -> usize {
        match self {
            Self::FixedControl => LANES,
            Self::ActiveWorklist | Self::Parallel => parallel_threads,
        }
    }
}

const ATTEMPT_PHASES: [(AttemptPhase, DispatchGeometry); 8] = [
    (AttemptPhase::Horizon, DispatchGeometry::FixedControl),
    (AttemptPhase::Compaction, DispatchGeometry::FixedControl),
    (AttemptPhase::DrainExecute, DispatchGeometry::ActiveWorklist),
    (
        AttemptPhase::ContinuationControl,
        DispatchGeometry::FixedControl,
    ),
    (AttemptPhase::ExchangePrefix, DispatchGeometry::FixedControl),
    (AttemptPhase::ExchangeScatter, DispatchGeometry::Parallel),
    (AttemptPhase::TargetMerge, DispatchGeometry::Parallel),
    (AttemptPhase::FinalControl, DispatchGeometry::FixedControl),
];
const PROFILE_PHASES: usize = ATTEMPT_PHASES.len();
const PROFILE_SAMPLES_PER_ATTEMPT: usize = PROFILE_PHASES * 2;
const CONTROL_THREADGROUP_BYTES: usize =
    LANES * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
const NONE: u64 = u64::MAX;
const PACKET_ECN_FLAG: u64 = 1_u64 << 63;
const PACKET_KIND_MASK: u64 = !PACKET_ECN_FLAG;
/// Params word holding the absolute word offset of the per-flow receiver state in `tcp_state`.
const PARAM_RECEIVER_OFFSET: usize = 28;
/// Params word holding the absolute word offset of the per-flow ledger metadata in `tcp_state`.
const PARAM_LEDGER_META_OFFSET: usize = 29;
const PARAM_ROUND_THREADS: usize = 30;

const CONTROL_ERROR: usize = 0;
const CONTROL_ERROR_ARENA: usize = 1;
const CONTROL_ERROR_NODE: usize = 2;
const CONTROL_ERROR_CAPACITY: usize = 3;
const CONTROL_ERROR_DEMAND: usize = 19;
const CONTROL_DONE: usize = 4;
const CONTROL_RUN_END_LO: usize = 7;
const CONTROL_RUN_END_HI: usize = 8;
const CONTROL_ROUNDS: usize = 9;
const CONTROL_CONTINUATION: usize = 17;
const CONTROL_RELAUNCHES: usize = 18;
const CONTROL_WORDS: usize = 20;
const CONTROL_INDIRECT_OFFSET_WORDS: usize = CONTROL_WORDS;
const CONTROL_STORAGE_WORDS: usize = CONTROL_WORDS + 2;

type RawMetalBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type RawCommandBuffer = Retained<ProtocolObject<dyn MTLCommandBuffer>>;
type RawCounterSampleBuffer = Retained<ProtocolObject<dyn MTLCounterSampleBuffer>>;
type RawCounterSet = Retained<ProtocolObject<dyn MTLCounterSet>>;
type MetalPipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

/// Bounded device arena reported by a production Metal capacity fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetalArena {
    Fel,
    ChannelInbox,
    ServiceStream,
    GeneratorStream,
    Queue,
    Outbox,
    Worklist,
    ObservedPackets,
    Departures,
    Arrivals,
    TcpReceiverRanges,
    TcpSegmentLedger,
    RemoteStaging,
}

impl fmt::Display for MetalArena {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Fel => "FEL",
            Self::ChannelInbox => "channel inbox stream",
            Self::ServiceStream => "service stream",
            Self::GeneratorStream => "generator stream",
            Self::Queue => "queue",
            Self::Outbox => "remote outbox",
            Self::Worklist => "active worklist",
            Self::ObservedPackets => "observed-packet log",
            Self::Departures => "departure log",
            Self::Arrivals => "arrival log",
            Self::TcpReceiverRanges => "TCP receiver range arena",
            Self::TcpSegmentLedger => "TCP segment ledger",
            Self::RemoteStaging => "per-LP remote staging",
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
        flow: Option<FlowId>,
        stream: Option<usize>,
        capacity: usize,
        demand: usize,
    },
    RetryFailed {
        error: Box<MetalError>,
        capacity_retry_trace: Vec<CapacityRetryRecord<MetalArena>>,
    },
    TransitionLimitExceeded {
        node: NodeId,
        capacity: usize,
    },
    RoundLimitExceeded {
        capacity: usize,
    },
    WfqArithmeticOverflow {
        node: NodeId,
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
                flow,
                stream,
                capacity,
                demand,
            } => {
                if let Some(stream) = stream {
                    write!(
                        formatter,
                        "Metal {arena} capacity of {capacity} records exceeded at stream {stream}; observed demand {demand}"
                    )
                } else if let Some(flow) = flow {
                    write!(
                        formatter,
                        "Metal {arena} capacity of {capacity} records exceeded at flow {flow:?}; observed demand {demand}"
                    )
                } else if let Some(node) = node {
                    write!(
                        formatter,
                        "Metal {arena} capacity of {capacity} records exceeded at LP {node:?}; observed demand {demand}"
                    )
                } else {
                    write!(
                        formatter,
                        "Metal {arena} capacity of {capacity} records exceeded; observed demand {demand}"
                    )
                }
            }
            Self::RetryFailed {
                error,
                capacity_retry_trace,
            } => write!(
                formatter,
                "Metal execution failed after {} capacity retries: {error}",
                capacity_retry_trace.len()
            ),
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
            Self::WfqArithmeticOverflow { node } => write!(
                formatter,
                "Metal WFQ arithmetic at LP {node:?} exceeds the exact 320-bit device limit; use Scalar or Cpu for this image"
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

impl Error for MetalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RetryFailed { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl MetalError {
    fn with_retry_trace(self, capacity_retry_trace: &[CapacityRetryRecord<MetalArena>]) -> Self {
        if capacity_retry_trace.is_empty() {
            self
        } else {
            Self::RetryFailed {
                error: Box::new(self),
                capacity_retry_trace: capacity_retry_trace.to_vec(),
            }
        }
    }
}

/// Physical capacity and bounded-wave encoding policy for one Metal run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetalConfig {
    /// Enables the stream-decomposed FEL. When false, every event uses the retained exact heap and
    /// the original target-owned exchange merge, providing an identical-binary ablation.
    pub streams_enabled: bool,
    /// Optional caps for large capacities derived from the complete image. Exact legacy
    /// `max_*` overrides below retain precedence when both forms are specified.
    pub capacity_caps: DeviceCapacityCaps,
    /// Internal lower bounds raised by deterministic capacity retries. Callers normally leave
    /// these zeroed.
    #[doc(hidden)]
    pub capacity_floors: DeviceCapacityFloors,
    /// Maximum number of all-or-nothing replacement attempts after capacity faults. Zero selects
    /// strict single-shot execution. The deterministic first-writer latch reports one entity per
    /// failed attempt, so this also bounds how many targeted stream growths can be learned.
    pub max_capacity_retries: usize,
    /// Optional exact per-LP FEL capacity override. Raising the derived default consumes more
    /// device memory; lowering it retains an explicit device capacity fault on overflow.
    pub max_fel_events_per_lp: Option<usize>,
    /// Optional exact starting capacity applied independently to every incoming-channel stream.
    /// Lowering it retains an explicit device capacity fault; targeted retries may raise one
    /// stream, and it has no effect when streams are disabled.
    pub max_channel_events_per_stream: Option<usize>,
    /// Optional exact per-LP packet-queue capacity override, with the same memory/fault tradeoff.
    pub max_queue_packets_per_lp: Option<usize>,
    /// Optional bound for all remote children produced in one round.
    pub max_outbox_events: Option<usize>,
    /// Optional bound applied independently to full-mode observed, departure, and arrival logs.
    pub max_observations: Option<usize>,
    /// Physical transition budget for each active LP lane in one `days_round` dispatch. Exhausting
    /// the budget relaunches the same semantic round with its horizon, worklist, FELs, and
    /// producer-local outboxes preserved.
    pub max_transitions_per_lp_per_round: usize,
    /// Threads per threadgroup for parallel round and exchange dispatches. Correctness is
    /// independent of this geometry; the value must be a nonzero multiple of the device execution
    /// width and no larger than the round pipelines support.
    pub round_threads_per_threadgroup: usize,
    /// Requested encoded round attempts per command buffer. Production execution clamps this to
    /// 16,384 independently of caller configuration.
    pub rounds_per_command_buffer: usize,
    /// Optional hard cap overriding the conservative encoded round bound.
    pub max_rounds: Option<usize>,
}

impl Default for MetalConfig {
    fn default() -> Self {
        Self {
            streams_enabled: true,
            capacity_caps: DeviceCapacityCaps::default(),
            capacity_floors: DeviceCapacityFloors::default(),
            max_capacity_retries: 16,
            max_fel_events_per_lp: None,
            max_channel_events_per_stream: None,
            max_queue_packets_per_lp: None,
            max_outbox_events: None,
            max_observations: None,
            max_transitions_per_lp_per_round: DEFAULT_TRANSITIONS_PER_DISPATCH,
            round_threads_per_threadgroup: DEFAULT_ROUND_THREADS_PER_THREADGROUP,
            rounds_per_command_buffer: DEFAULT_ROUNDS_PER_COMMAND_BUFFER,
            max_rounds: None,
        }
    }
}

impl MetalConfig {
    fn raise_capacity(&mut self, arena: MetalArena, capacity: usize, grown: usize) {
        match arena {
            MetalArena::Fel => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_fel_events_per_lp,
                    &mut self.capacity_caps.fallback_fel_events_per_lp,
                    &mut self.capacity_floors.fallback_fel_events_per_lp,
                    capacity,
                    grown,
                );
            }
            MetalArena::ChannelInbox => {
                unreachable!("channel capacity retries use per-stream floors")
            }
            MetalArena::ServiceStream => {
                self.capacity_floors.service_events_per_stream =
                    self.capacity_floors.service_events_per_stream.max(grown);
            }
            MetalArena::GeneratorStream => {
                self.capacity_floors.generator_events_per_stream =
                    self.capacity_floors.generator_events_per_stream.max(grown);
            }
            MetalArena::Queue => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_queue_packets_per_lp,
                    &mut self.capacity_caps.queue_packets_per_lp,
                    &mut self.capacity_floors.queue_packets_per_lp,
                    capacity,
                    grown,
                );
            }
            MetalArena::Outbox => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_outbox_events,
                    &mut self.capacity_caps.outbox_events_total,
                    &mut self.capacity_floors.outbox_events_total,
                    capacity,
                    grown,
                );
            }
            MetalArena::Worklist => {
                self.capacity_floors.worklist_entries_total =
                    self.capacity_floors.worklist_entries_total.max(grown);
            }
            MetalArena::ObservedPackets | MetalArena::Departures | MetalArena::Arrivals => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_observations,
                    &mut self.capacity_caps.observation_events_per_lp,
                    &mut self.capacity_floors.observation_events,
                    capacity,
                    grown,
                );
            }
            MetalArena::TcpReceiverRanges | MetalArena::TcpSegmentLedger => {
                unreachable!("TCP capacity retries use per-flow floors")
            }
            MetalArena::RemoteStaging => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_outbox_events,
                    &mut self.capacity_caps.remote_staging_events_per_lp,
                    &mut self.capacity_floors.remote_staging_events_per_lp,
                    capacity,
                    grown,
                );
            }
        }
    }
}

/// The effective plane-wide capacity the converged attempt planned, expressed as floor lanes.
///
/// [`MetalConfig::raise_capacity`] may satisfy a fault by raising an explicit `max_*` override
/// rather than the matching floor, and one override — `max_outbox_events` — serves **two** arenas,
/// so `capacity_floors` alone does not describe what the successful attempt planned. Every planner
/// site for these arenas reads `override.max(floor)` when an override is present and
/// `bound_derived_capacity(..., floor, ...)` when it is not, so `max(floor, override)` is exactly
/// the capacity that attempt used and never more. The entity-keyed arenas are absent here: their
/// converged capacity lives in the per-stream and per-flow vectors instead.
fn converged_capacity_floors(config: &MetalConfig) -> DeviceCapacityFloors {
    let floors = config.capacity_floors;
    let producer = config.max_outbox_events.unwrap_or(0);
    DeviceCapacityFloors {
        fallback_fel_events_per_lp: floors
            .fallback_fel_events_per_lp
            .max(config.max_fel_events_per_lp.unwrap_or(0)),
        queue_packets_per_lp: floors
            .queue_packets_per_lp
            .max(config.max_queue_packets_per_lp.unwrap_or(0)),
        channel_events_per_stream: floors
            .channel_events_per_stream
            .max(config.max_channel_events_per_stream.unwrap_or(0)),
        remote_staging_events_per_lp: floors.remote_staging_events_per_lp.max(producer),
        outbox_events_total: floors.outbox_events_total.max(producer),
        observation_events: floors
            .observation_events
            .max(config.max_observations.unwrap_or(0)),
        ..floors
    }
}

/// Exact planned Metal event-arena footprint for one run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetalMemoryLayout {
    pub streams_enabled: bool,
    pub legacy_heap_event_slots: usize,
    pub fallback_heap_event_slots: usize,
    /// Every unproven initial/checkpoint pending is loaded into the fallback heap.
    pub checkpoint_fallback_events: usize,
    pub channel_stream_event_slots: usize,
    pub service_stream_event_slots: usize,
    pub generator_stream_event_slots: usize,
    pub heap_arena_bytes: usize,
    pub stream_arena_bytes: usize,
    pub legacy_heap_arena_bytes: usize,
}

impl MetalMemoryLayout {
    pub fn total_event_arena_bytes(self) -> usize {
        self.heap_arena_bytes
            .saturating_add(self.stream_arena_bytes)
    }

    pub fn delta_from_legacy_heap_bytes(self) -> i128 {
        self.total_event_arena_bytes() as i128 - self.legacy_heap_arena_bytes as i128
    }
}

/// Complete production Metal result and bounded-wave submission diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalRun {
    pub result: RunResult,
    /// Capacity faults from discarded attempts, in deterministic retry order.
    pub capacity_retry_trace: Vec<CapacityRetryRecord<MetalArena>>,
    /// The capacity this run converged on, replayable as a later run's starting capacity.
    ///
    /// T20l fix 3: handing this back to
    /// [`MetalExecutor::run_with_observations_warm_started`] makes the same image plan right on
    /// its first attempt. It is a sizing hint for host planning, derived output of this run's own
    /// retry chain, and unrelated to any compiled-image or pipeline cache.
    pub capacity_warm_start: CapacityWarmStart,
    /// Ascending final-plan channel capacity levels after all targeted retries.
    pub channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
    pub rounds: u64,
    pub transitions: u64,
    /// Physical round attempts encoded into submitted command buffers, including termination and
    /// speculative no-op tail attempts.
    pub encoded_attempts: u64,
    /// Encoded `days_round` continuation dispatches beyond the first launch per round.
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
    /// Opt-in diagnostic timings. Normal production runs leave this `None`.
    pub phase_profile: Option<MetalPhaseProfile>,
    /// Exact record and metadata bytes planned for the fallback heap and monotone streams.
    pub memory_layout: MetalMemoryLayout,
}

/// Opt-in T15e FEL round-trip probe result.
///
/// The probe executes the production transition body and geometry unchanged, but reinserts and
/// removes the event that was just popped before each transition. This adds one real heap
/// push/pop pair while restoring the same logical FEL. It is a differential diagnostic, not a
/// production execution mode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalFelProbeRun {
    pub run: MetalRun,
    /// Local FEL pushes performed by the unchanged transition bodies.
    pub local_fel_pushes: u64,
    /// Net-zero heap push/pop pairs counted on-device by the probe.
    pub injected_fel_round_trips: u64,
    /// Lazy creation cost for all diagnostic pipelines paid by this call, or zero when the
    /// executor reused its cache.
    /// This cost is excluded from run and phase timings.
    pub diagnostic_pipeline_creation_ns: u64,
}

/// Matched counting-only control for [`MetalFelProbeRun`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalFelControlRun {
    pub run: MetalRun,
    pub local_fel_pushes: u64,
    /// Lazy creation cost for all diagnostic pipelines paid by this call, or zero when the
    /// executor reused its cache.
    pub diagnostic_pipeline_creation_ns: u64,
}

/// Actual remote-merge fan-in accumulated across eventful target-rounds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetalMergeFanIn {
    pub eventful_target_rounds: u64,
    pub active_producer_target_rounds: u64,
    pub remote_events: u64,
    pub maximum_active_fan_in: u64,
    pub first_maximum_fan_in_target: Option<NodeId>,
    pub maximum_fan_in_target_count: u64,
}

/// Separate fan-in instrumentation run, kept out of FEL differential samples.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetalMergeFanInRun {
    pub run: MetalRun,
    pub fan_in: MetalMergeFanIn,
    /// Lazy creation cost for all diagnostic pipelines paid by this call, or zero when the
    /// executor reused its cache.
    pub diagnostic_pipeline_creation_ns: u64,
}

/// Heuristic drain decomposition derived from a production profile and a matching FEL probe.
///
/// A positive probe-minus-control delta measures an added root-key heap round trip at the exact
/// production transition points. Reinserting the just-popped minimum is a near-worst-case push
/// compared with future-key production children, so the scaled FEL value is an upper-biased stress
/// heuristic, not a mathematical bound or representative production cost. The residual still includes
/// root/horizon checks and fused-loop overhead and is not a pure transition-body measurement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetalDrainDecomposition {
    pub baseline_drain_execute_ns: u64,
    pub matched_control_drain_execute_ns: u64,
    pub probe_drain_execute_ns: u64,
    pub fel_round_trip_delta_ns: i128,
    pub rounds: u64,
    pub transitions: u64,
    pub local_fel_pushes: u64,
    pub production_drain_fel_operations: u64,
    pub injected_fel_operations: u64,
    pub stress_scaled_fel_estimate_ns: Option<u64>,
    pub residual_after_stress_scaled_estimate_ns: Option<u64>,
}

impl MetalFelProbeRun {
    /// Builds the differential split when both profiles captured every useful attempt.
    ///
    /// A non-positive delta is retained as measurement evidence, but the cost estimates are
    /// `None`: timestamp noise cannot establish a nonnegative FEL cost from that sample. The
    /// residual is also `None` when the scaled FEL estimate exceeds the baseline.
    pub fn decompose_against(
        &self,
        baseline: &MetalRun,
        control: &MetalFelControlRun,
    ) -> Result<MetalDrainDecomposition, MetalError> {
        if baseline.result != self.run.result
            || baseline.rounds != self.run.rounds
            || baseline.transitions != self.run.transitions
            || baseline.continuation_relaunches != self.run.continuation_relaunches
            || control.run.result != self.run.result
            || control.run.rounds != self.run.rounds
            || control.run.transitions != self.run.transitions
            || control.run.continuation_relaunches != self.run.continuation_relaunches
        {
            return Err(MetalError::Validation(
                "FEL probe, matched control, and baseline outcomes must match exactly".into(),
            ));
        }
        if self.run.rounds == 0 {
            return Err(MetalError::Validation(
                "FEL decomposition requires at least one completed round".into(),
            ));
        }
        if self.injected_fel_round_trips != self.run.transitions {
            return Err(MetalError::Validation(
                "FEL probe must inject exactly one round trip per transition".into(),
            ));
        }
        let baseline_profile = baseline.phase_profile.as_ref().ok_or_else(|| {
            MetalError::Validation("FEL decomposition baseline must be profiled".into())
        })?;
        let probe_profile = self.run.phase_profile.as_ref().ok_or_else(|| {
            MetalError::Validation("FEL probe run must include phase profiling".into())
        })?;
        let control_profile = control.run.phase_profile.as_ref().ok_or_else(|| {
            MetalError::Validation("FEL matched control must include phase profiling".into())
        })?;
        if baseline_profile.captured_attempts < baseline_profile.useful_attempts
            || probe_profile.captured_attempts < probe_profile.useful_attempts
            || control_profile.captured_attempts < control_profile.useful_attempts
        {
            return Err(MetalError::Validation(
                "FEL decomposition requires complete useful-attempt profile capture".into(),
            ));
        }
        if baseline_profile.useful_attempts != probe_profile.useful_attempts
            || control_profile.useful_attempts != probe_profile.useful_attempts
        {
            return Err(MetalError::Validation(
                "FEL probe, matched control, and baseline useful-attempt counts must match".into(),
            ));
        }
        if control.local_fel_pushes != self.local_fel_pushes {
            return Err(MetalError::Validation(
                "FEL probe and matched control local-push counts must match".into(),
            ));
        }

        let baseline_ns = baseline_profile.useful.drain_execute_ns;
        let control_ns = control_profile.useful.drain_execute_ns;
        let probe_ns = probe_profile.useful.drain_execute_ns;
        let delta_ns = i128::from(probe_ns) - i128::from(control_ns);
        let production_drain_fel_operations = self
            .run
            .transitions
            .checked_add(self.local_fel_pushes)
            .ok_or_else(|| {
                MetalError::Validation("production drain FEL operation count overflows".into())
            })?;
        let injected_fel_operations =
            self.injected_fel_round_trips
                .checked_mul(2)
                .ok_or_else(|| {
                    MetalError::Validation("injected FEL operation count overflows".into())
                })?;
        let stress_scaled_fel_estimate_ns = if delta_ns > 0 && injected_fel_operations != 0 {
            let scaled = (delta_ns as u128)
                .checked_mul(u128::from(production_drain_fel_operations))
                .ok_or_else(|| MetalError::Validation("scaled FEL time overflows".into()))?
                / u128::from(injected_fel_operations);
            Some(u64::try_from(scaled).map_err(|_| {
                MetalError::Validation("scaled FEL time does not fit in u64".into())
            })?)
        } else {
            None
        };
        let residual_after_stress_scaled_estimate_ns =
            stress_scaled_fel_estimate_ns.and_then(|fel_ns| baseline_ns.checked_sub(fel_ns));
        Ok(MetalDrainDecomposition {
            baseline_drain_execute_ns: baseline_ns,
            matched_control_drain_execute_ns: control_ns,
            probe_drain_execute_ns: probe_ns,
            fel_round_trip_delta_ns: delta_ns,
            rounds: self.run.rounds,
            transitions: self.run.transitions,
            local_fel_pushes: self.local_fel_pushes,
            production_drain_fel_operations,
            injected_fel_operations,
            stress_scaled_fel_estimate_ns,
            residual_after_stress_scaled_estimate_ns,
        })
    }
}

/// Dispatch-level GPU timestamp totals for one production round attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetalPhaseTimings {
    pub horizon_ns: u64,
    pub compaction_ns: u64,
    pub drain_execute_ns: u64,
    pub continuation_control_ns: u64,
    pub exchange_prefix_ns: u64,
    pub exchange_scatter_ns: u64,
    /// Target-owned k-way heap merge when streams are disabled; target active-head refresh when
    /// stream decomposition is enabled. Keeping one aligned slot makes same-binary phase ablation
    /// tables directly comparable.
    pub target_merge_ns: u64,
    pub final_control_ns: u64,
}

impl MetalPhaseTimings {
    /// Sum of all sampled dispatch intervals.
    pub fn total_ns(self) -> u64 {
        [
            self.horizon_ns,
            self.compaction_ns,
            self.drain_execute_ns,
            self.continuation_control_ns,
            self.exchange_prefix_ns,
            self.exchange_scatter_ns,
            self.target_merge_ns,
            self.final_control_ns,
        ]
        .into_iter()
        .fold(0, u64::saturating_add)
    }

    fn from_values(values: [u64; PROFILE_PHASES]) -> Self {
        Self {
            horizon_ns: values[0],
            compaction_ns: values[1],
            drain_execute_ns: values[2],
            continuation_control_ns: values[3],
            exchange_prefix_ns: values[4],
            exchange_scatter_ns: values[5],
            target_merge_ns: values[6],
            final_control_ns: values[7],
        }
    }

    fn values(self) -> [u64; PROFILE_PHASES] {
        [
            self.horizon_ns,
            self.compaction_ns,
            self.drain_execute_ns,
            self.continuation_control_ns,
            self.exchange_prefix_ns,
            self.exchange_scatter_ns,
            self.target_merge_ns,
            self.final_control_ns,
        ]
    }

    fn saturating_add(self, other: Self) -> Self {
        Self::from_values(std::array::from_fn(|index| {
            self.values()[index].saturating_add(other.values()[index])
        }))
    }

    fn saturating_mul(self, factor: u64) -> Self {
        Self::from_values(self.values().map(|value| value.saturating_mul(factor)))
    }

    fn divided_by(self, divisor: u64) -> Self {
        if divisor == 0 {
            Self::default()
        } else {
            Self::from_values(self.values().map(|value| value / divisor))
        }
    }
}

/// Opt-in diagnostic phase decomposition from stage-boundary GPU timestamp samples.
///
/// The current Apple device cannot sample counters at dispatch boundaries, so profiling uses one
/// compute pass per dispatch for the first 128 attempts. Remaining attempts use the normal encoder.
/// `estimated_total` combines all captured useful work, the captured termination attempt, and the
/// mean sampled no-op tail multiplied by the number of remaining encoded tail attempts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetalPhaseProfile {
    pub timestamp_frequency_hz: u64,
    pub encoded_attempts: u64,
    pub captured_attempts: u64,
    pub useful_attempts: u64,
    pub idle_sample_attempts: u64,
    /// Gaps between consecutive sampled compute passes within captured command buffers.
    pub captured_pass_gap_ns: u64,
    /// Overlap between consecutive sampled pass intervals. Stage-boundary timestamps can overlap
    /// slightly even when the compute passes use serial dispatch.
    pub captured_pass_overlap_ns: u64,
    pub estimate_complete: bool,
    pub useful: MetalPhaseTimings,
    pub termination: MetalPhaseTimings,
    pub idle_mean: MetalPhaseTimings,
    pub estimated_total: MetalPhaseTimings,
}

/// Runs the production Metal executor through the inclusive scenario stop.
///
/// Metal execution is serialized process-wide: one executor executes at a time, and concurrent
/// callers queue until execution and readback complete. The compiled executor is cached
/// process-wide.
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
///
/// Metal execution is serialized process-wide: one executor executes at a time, and concurrent
/// callers queue until execution and readback complete. Executor construction may proceed
/// concurrently.
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
/// subsequent run through the same executor. Metal execution is serialized process-wide: one
/// executor executes at a time, and concurrent callers queue until execution and readback
/// complete. All wrappers share one process-wide compiled executor.
pub struct MetalExecutor {
    direct: Arc<DirectMetal>,
    initialization_timings: MetalInitializationTimings,
}

/// One-time costs paid while constructing a reusable [`MetalExecutor`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetalInitializationTimings {
    /// Device, queue, capability validation, and other non-pipeline initialization.
    pub device_queue_setup_ns: u64,
    /// Runtime MSL library compilation and compute-pipeline creation for all production phases.
    pub pipeline_creation_ns: u64,
    /// Whether this wrapper reused the process-wide compiled executor.
    pub reused_cached_executor: bool,
}

impl MetalExecutor {
    /// Returns a wrapper around the process-wide compiled executor.
    ///
    /// The first successful call pays device and pipeline initialization. Later calls report a
    /// cache hit with zero initialization durations. Runs remain serialized process-wide.
    pub fn new() -> Result<Self, MetalError> {
        let mut cached = METAL_DIRECT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(direct) = cached.as_ref() {
            return Ok(Self {
                direct: Arc::clone(direct),
                initialization_timings: MetalInitializationTimings {
                    device_queue_setup_ns: 0,
                    pipeline_creation_ns: 0,
                    reused_cached_executor: true,
                },
            });
        }
        let direct = Arc::new(DirectMetal::new()?);
        let initialization_timings = direct.initialization_timings;
        *cached = Some(Arc::clone(&direct));
        Ok(Self {
            direct,
            initialization_timings,
        })
    }

    /// Runs with summary observations under the process-wide Metal execution envelope.
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

    /// Returns the one-time initialization split measured when this reusable executor was built.
    pub const fn initialization_timings(&self) -> MetalInitializationTimings {
        self.initialization_timings
    }

    /// Runs the production backend with diagnostic stage-boundary phase timestamps enabled.
    ///
    /// This mode is intended for tests and benchmark evidence. It preserves simulation semantics
    /// but splits the first 128 round attempts into separate compute passes, so its wall and device
    /// totals are not production performance measurements. Concurrent Metal callers queue behind
    /// the process-wide execution guard.
    pub fn run_profiled(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
    ) -> Result<MetalRun, MetalError> {
        self.run_with_observations_profiled(
            image,
            exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        )
    }

    /// Runs the matched counting-only control for [`Self::run_fel_probe_profiled`].
    ///
    /// The retained diagnostic kernel measures the legacy heap mechanism and therefore requires
    /// `config.streams_enabled == false`. Concurrent Metal callers queue behind the process-wide
    /// execution guard.
    pub fn run_fel_control_profiled(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
    ) -> Result<MetalFelControlRun, MetalError> {
        let (run, local_fel_pushes, injected_fel_round_trips, diagnostic_pipeline_creation_ns) =
            self.run_fel_diagnostic_profiled(image, exclusive_horizon_ns, config, false)?;
        if injected_fel_round_trips != 0 {
            return Err(MetalError::Validation(
                "FEL matched control must not inject heap round trips".into(),
            ));
        }
        Ok(MetalFelControlRun {
            run,
            local_fel_pushes,
            diagnostic_pipeline_creation_ns,
        })
    }

    /// Runs the opt-in T15e FEL round-trip stress probe with phase profiling enabled.
    ///
    /// Compare against both [`Self::run_profiled`] and [`Self::run_fel_control_profiled`] using
    /// [`MetalFelProbeRun::decompose_against`]. Reinserting the just-popped minimum exercises a
    /// near-worst-case heap push, so the scaled result is an upper-biased heuristic, not a
    /// representative average or sufficient evidence by itself to indict the FEL. This legacy
    /// heap diagnostic requires `config.streams_enabled == false`. Concurrent Metal callers queue
    /// behind the process-wide execution guard.
    pub fn run_fel_probe_profiled(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
    ) -> Result<MetalFelProbeRun, MetalError> {
        let (run, local_fel_pushes, injected_fel_round_trips, diagnostic_pipeline_creation_ns) =
            self.run_fel_diagnostic_profiled(image, exclusive_horizon_ns, config, true)?;
        Ok(MetalFelProbeRun {
            run,
            local_fel_pushes,
            injected_fel_round_trips,
            diagnostic_pipeline_creation_ns,
        })
    }

    /// Runs a separate target-merge fan-in counter probe.
    ///
    /// The read-only pre-scan changes target-merge timing, so callers should use a matching
    /// [`Self::run_profiled`] result for production merge time and this result only for exact fan-in
    /// counts and outcome parity. This legacy exchange diagnostic requires
    /// `config.streams_enabled == false`. Concurrent Metal callers queue behind the process-wide
    /// execution guard.
    pub fn run_merge_fan_in_profiled(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
    ) -> Result<MetalMergeFanInRun, MetalError> {
        validate(image, Backend::Metal)
            .map_err(|error| MetalError::Validation(error.to_string()))?;
        validate_config(config)?;
        require_heap_diagnostic(config, "exchange")?;

        let (pipelines, diagnostic_pipeline_creation_ns) = self.direct.fel_probe_pipelines()?;
        let plan = MetalPlan::new(
            image,
            exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        )?;
        let buffers = MetalBuffers::new(&self.direct.device, plan)?;
        let probe = FelProbeResources::new(
            &self.direct.device,
            image.nodes.len(),
            None,
            Some(pipelines.merge_pipeline),
            false,
        )?;
        let _execution_guard = metal_device_execution_guard();
        let timing = self
            .direct
            .run_with_fel_probe(&buffers, config, true, Some(&probe))?;
        let run = buffers
            .finish(&self.direct, image, ObservationMode::Summary, timing)
            .map_err(|failure| failure.error)?;
        let fan_in = probe.merge_fan_in()?;
        Ok(MetalMergeFanInRun {
            run,
            fan_in,
            diagnostic_pipeline_creation_ns,
        })
    }

    fn run_fel_diagnostic_profiled(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        inject_round_trip: bool,
    ) -> Result<(MetalRun, u64, u64, u64), MetalError> {
        validate(image, Backend::Metal)
            .map_err(|error| MetalError::Validation(error.to_string()))?;
        validate_config(config)?;
        require_heap_diagnostic(config, "FEL")?;

        let (pipelines, diagnostic_pipeline_creation_ns) = self.direct.fel_probe_pipelines()?;
        let plan = MetalPlan::new(
            image,
            exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        )?;
        let buffers = MetalBuffers::new(&self.direct.device, plan)?;
        let probe = FelProbeResources::new(
            &self.direct.device,
            image.nodes.len(),
            Some(pipelines.round_pipeline),
            None,
            inject_round_trip,
        )?;
        let _execution_guard = metal_device_execution_guard();
        let timing = self
            .direct
            .run_with_fel_probe(&buffers, config, true, Some(&probe))?;
        let run = buffers
            .finish(&self.direct, image, ObservationMode::Summary, timing)
            .map_err(|failure| failure.error)?;
        let (local_fel_pushes, injected_fel_round_trips) = probe.fel_counts()?;
        Ok((
            run,
            local_fel_pushes,
            injected_fel_round_trips,
            diagnostic_pipeline_creation_ns,
        ))
    }

    /// Runs with explicit observation retention under the process-wide Metal execution envelope.
    pub fn run_with_observations(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
    ) -> Result<MetalRun, MetalError> {
        self.run_with_observations_mode(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            false,
            &CapacityWarmStart::default(),
        )
    }

    /// Runs from an explicit starting capacity instead of from the derived one (T20l fix 3).
    ///
    /// `warm_start` is a [`CapacityWarmStart`] a previous successful run of the same image emitted
    /// on [`MetalRun::capacity_warm_start`]. Supplying it lets the first attempt plan the capacity
    /// the retry chain would have converged on, so a known fixture builds one plan, uploads once
    /// and reads back once instead of discarding three attempts. [`CapacityWarmStart::default()`]
    /// is exactly [`Self::run_with_observations`], which is what every existing caller keeps doing.
    ///
    /// The result cannot depend on the hint: capacity on this path is refuse-or-run, never
    /// semantics (the T20g invariant), so a warm start moves a run between "refuses once, then
    /// runs" and "runs immediately" and never between two answers. See
    /// [`CapacityWarmStart`] for the argument in full.
    pub fn run_with_observations_warm_started(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
        warm_start: &CapacityWarmStart,
    ) -> Result<MetalRun, MetalError> {
        self.run_with_observations_mode(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            false,
            warm_start,
        )
    }

    /// Full-observation counterpart of [`Self::run_profiled`].
    ///
    /// Concurrent Metal callers queue behind the process-wide execution guard.
    pub fn run_with_observations_profiled(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
    ) -> Result<MetalRun, MetalError> {
        self.run_with_observations_mode(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            true,
            &CapacityWarmStart::default(),
        )
    }

    fn run_with_observations_mode(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
        profile: bool,
        warm_start: &CapacityWarmStart,
    ) -> Result<MetalRun, MetalError> {
        validate(image, Backend::Metal)
            .map_err(|error| MetalError::Validation(error.to_string()))?;
        validate_config(config)?;

        let retry_budget = config.max_capacity_retries;
        let mut attempt_config = config;
        attempt_config.capacity_floors = config.capacity_floors.merged_with(warm_start.floors);
        let mut channel_capacity_floors =
            crate::device_capacity::ChannelCapacityFloors::warm_started(
                &warm_start.channel_events_by_stream,
            );
        let mut tcp_capacity_floors = crate::device_capacity::TcpCapacityFloors::warm_started(
            &warm_start.tcp_receiver_ranges_by_base,
            &warm_start.tcp_ledger_segments_by_flow,
            image.flows.len(),
        );
        let mut retry_trace = Vec::new();
        loop {
            let attempt = (|| {
                let plan = MetalPlan::new_with_entity_capacity_floors(
                    image,
                    exclusive_horizon_ns,
                    attempt_config,
                    observation_mode,
                    &mut channel_capacity_floors,
                    &mut tcp_capacity_floors,
                )?;
                let buffers = MetalBuffers::new(&self.direct.device, plan)?;
                let _execution_guard = metal_device_execution_guard();
                let timing = self.direct.run(&buffers, attempt_config, profile)?;
                #[cfg(feature = "metal-test-hooks")]
                panic_after_execution_if_requested();
                buffers.finish(&self.direct, image, observation_mode, timing)
            })();
            match attempt {
                Ok(mut run) => {
                    run.capacity_retry_trace = retry_trace;
                    run.capacity_warm_start = CapacityWarmStart {
                        floors: converged_capacity_floors(&attempt_config),
                        channel_events_by_stream: channel_capacity_floors.converged_capacities(),
                        tcp_receiver_ranges_by_base: tcp_capacity_floors
                            .converged_receiver_ranges(),
                        tcp_ledger_segments_by_flow: tcp_capacity_floors
                            .converged_ledger_segments(),
                    };
                    return Ok(run);
                }
                Err(failure) => {
                    let AttemptFailure {
                        error,
                        ledger_high_water,
                    } = failure;
                    let (arena, node, flow, stream, capacity, demand) = match error {
                        MetalError::CapacityExceeded {
                            arena,
                            node,
                            flow,
                            stream,
                            capacity,
                            demand,
                        } => (arena, node, flow, stream, capacity, demand),
                        error => return Err(error.with_retry_trace(&retry_trace)),
                    };
                    if retry_trace.len() == retry_budget {
                        return Err(MetalError::CapacityExceeded {
                            arena,
                            node,
                            flow,
                            stream,
                            capacity,
                            demand,
                        }
                        .with_retry_trace(&retry_trace));
                    }
                    let grown_capacity = match arena {
                        MetalArena::TcpReceiverRanges => {
                            crate::device_capacity::grown_capacity_with_slack(
                                capacity,
                                demand,
                                crate::device_capacity::TCP_RECEIVER_RETRY_SLACK,
                            )
                        }
                        MetalArena::TcpSegmentLedger => {
                            crate::device_capacity::grown_ledger_capacity(
                                capacity,
                                demand,
                                crate::device_capacity::observed_ledger_high_water(
                                    ledger_high_water.as_deref(),
                                    flow,
                                ),
                            )
                        }
                        _ => crate::device_capacity::grown_capacity(capacity, demand),
                    };
                    if grown_capacity <= capacity {
                        return Err(MetalError::CapacityExceeded {
                            arena,
                            node,
                            flow,
                            stream,
                            capacity,
                            demand,
                        }
                        .with_retry_trace(&retry_trace));
                    }
                    let entity_floor_raised = match (arena, flow, stream) {
                        (MetalArena::ChannelInbox, None, Some(stream)) => {
                            channel_capacity_floors.raise(stream, capacity, grown_capacity)
                        }
                        (MetalArena::ChannelInbox, _, None) => {
                            return Err(MetalError::Validation(
                                "channel capacity fault omitted its stream identity".into(),
                            )
                            .with_retry_trace(&retry_trace));
                        }
                        (MetalArena::TcpReceiverRanges, Some(flow), None) => {
                            tcp_capacity_floors.raise_receiver(flow, capacity, grown_capacity)
                        }
                        (MetalArena::TcpSegmentLedger, Some(flow), None) => {
                            // The faulting flow's own floor first, so its base/floor equality
                            // check still sees the capacity the attempt actually planned; then
                            // the vector, which sizes every other flow in the same replan.
                            let raised =
                                tcp_capacity_floors.raise_ledger(flow, capacity, grown_capacity);
                            if let (true, Some(high_water)) = (raised, ledger_high_water.as_deref())
                            {
                                tcp_capacity_floors.raise_ledger_from_occupancy(high_water);
                            }
                            raised
                        }
                        (MetalArena::TcpReceiverRanges | MetalArena::TcpSegmentLedger, None, _) => {
                            return Err(MetalError::Validation(
                                "TCP capacity fault omitted its flow identity".into(),
                            )
                            .with_retry_trace(&retry_trace));
                        }
                        _ => {
                            attempt_config.raise_capacity(arena, capacity, grown_capacity);
                            true
                        }
                    };
                    if !entity_floor_raised {
                        return Err(MetalError::Validation(
                            "capacity fault did not match its immutable planned entity".into(),
                        )
                        .with_retry_trace(&retry_trace));
                    }
                    retry_trace.push(CapacityRetryRecord {
                        retry: retry_trace.len() + 1,
                        arena,
                        node,
                        flow,
                        stream,
                        capacity,
                        demand,
                        grown_capacity,
                    });
                }
            }
        }
    }
}

/// Asserts that the linear-table planner produces the complete legacy host plan bit-for-bit.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn assert_metal_planner_bit_equal_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: MetalConfig,
    observation_mode: ObservationMode,
) -> Result<(), MetalError> {
    validate(image, Backend::Metal).map_err(|error| MetalError::Validation(error.to_string()))?;
    validate_config(config)?;
    let (packet_counts, feedback_counts) = flow_packet_counts(image)?;
    let data_counts = packet_counts
        .iter()
        .zip(&feedback_counts)
        .map(|(total, feedback)| total.saturating_sub(*feedback))
        .collect::<Vec<_>>();
    let lookahead = image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min();
    if !PlannerCapacityContext::matches_legacy(
        image,
        &data_counts,
        lookahead,
        TcpMinimumPacketSize::One,
    ) {
        return Err(MetalError::Validation(
            "linear lookup tables differ from the legacy helpers".into(),
        ));
    }
    let precomputed = MetalPlan::new_with_capacity_mode(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
        PlannerCapacityMode::Precomputed,
    )?;
    let legacy = MetalPlan::new_with_capacity_mode(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
        PlannerCapacityMode::Legacy,
    )?;
    if precomputed != legacy {
        return Err(MetalError::Validation(
            "linear-table planner differs from the legacy planner".into(),
        ));
    }
    Ok(())
}

/// Measures host plan construction only; device initialization and execution are excluded.
#[cfg(feature = "metal-test-hooks")]
#[doc(hidden)]
pub fn measure_metal_planner_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: MetalConfig,
    observation_mode: ObservationMode,
    legacy: bool,
) -> Result<u64, MetalError> {
    validate(image, Backend::Metal).map_err(|error| MetalError::Validation(error.to_string()))?;
    validate_config(config)?;
    let mode = if legacy {
        PlannerCapacityMode::Legacy
    } else {
        PlannerCapacityMode::Precomputed
    };
    let started = Instant::now();
    let plan = MetalPlan::new_with_capacity_mode(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
        mode,
    )?;
    let planning_ns = duration_ns(started.elapsed());
    std::hint::black_box(&plan.params);
    drop(plan);
    Ok(planning_ns)
}

/// Returns the exact production-plan plane lengths without creating a Metal device.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn size_metal_plan_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: MetalConfig,
    observation_mode: ObservationMode,
) -> Result<crate::DeviceSizingReport, MetalError> {
    validate(image, Backend::Metal).map_err(|error| MetalError::Validation(error.to_string()))?;
    validate_config(config)?;
    let plan = MetalPlan::new(image, exclusive_horizon_ns, config, observation_mode)?;
    let words = [
        plan.control.len(),
        plan.params.len(),
        plan.node_state.len(),
        plan.generators.len(),
        plan.flows.len(),
        plan.routes.len(),
        plan.links.len(),
        plan.fel_meta.len(),
        plan.fel_records.len(),
        plan.queue_meta.len(),
        plan.queue_records.len(),
        plan.in_service.len(),
        plan.outbox.len(),
        plan.worklist.len(),
        plan.summary.len(),
        plan.observed.len(),
        plan.departures.len(),
        plan.arrivals.len(),
        plan.lp_state.len(),
        plan.remote_meta.len(),
        plan.remote_staging.len(),
        plan.observation_meta.len(),
        plan.inbound_meta.len(),
        plan.inbound_producers.len(),
        plan.merge_cursors.len(),
        plan.stream_state.len(),
        plan.stream_records.len(),
        plan.scheduler_state.len(),
    ];
    crate::device_sizing::exact_plan_report(
        words,
        plan.tcp_state.len(),
        crate::DeviceEventArenaSizing {
            legacy_heap_event_slots: plan.memory_layout.legacy_heap_event_slots,
            fallback_heap_event_slots: plan.memory_layout.fallback_heap_event_slots,
            channel_stream_event_slots: plan.memory_layout.channel_stream_event_slots,
            service_stream_event_slots: plan.memory_layout.service_stream_event_slots,
            generator_stream_event_slots: plan.memory_layout.generator_stream_event_slots,
            heap_arena_bytes: plan.memory_layout.heap_arena_bytes,
            stream_arena_bytes: plan.memory_layout.stream_arena_bytes,
            legacy_heap_arena_bytes: plan.memory_layout.legacy_heap_arena_bytes,
        },
        plan.channel_stream_capacity_distribution,
    )
    .map_err(|error| MetalError::Validation(error.to_string()))
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
    if config.round_threads_per_threadgroup == 0 {
        return Err(MetalError::Validation(
            "round_threads_per_threadgroup must be nonzero".into(),
        ));
    }
    Ok(())
}

fn require_heap_diagnostic(config: MetalConfig, mechanism: &str) -> Result<(), MetalError> {
    if config.streams_enabled {
        return Err(MetalError::Validation(format!(
            "legacy heap {mechanism} diagnostics require streams_enabled=false"
        )));
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

#[derive(Eq, PartialEq)]
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
    lp_state: Vec<u64>,
    remote_meta: Vec<u64>,
    remote_staging: Vec<u64>,
    observation_meta: Vec<u64>,
    inbound_meta: Vec<u64>,
    inbound_producers: Vec<u64>,
    merge_cursors: Vec<u64>,
    stream_state: Vec<u64>,
    stream_records: Vec<u64>,
    scheduler_state: Vec<u64>,
    tcp_state: Vec<u64>,
    stream_layout: StreamLayout,
    memory_layout: MetalMemoryLayout,
    channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
    orphan_packets: Vec<PacketDescriptor>,
    round_capacity: usize,
    dispatch_capacity: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct StreamLayout {
    stream_count: usize,
    channel_count: usize,
    service_stream_base: usize,
    generator_stream_base: usize,
    lp_stream_meta_offset: usize,
    lp_stream_ids_offset: usize,
    lp_active_ids_offset: usize,
    outbound_meta_offset: usize,
    outbound_entries_offset: usize,
    channel_batch_offset: usize,
    staging_channel_offset: usize,
    channel_target_offset: usize,
}

struct PreparedStreams {
    state: Vec<u64>,
    records: Vec<u64>,
    layout: StreamLayout,
    memory_layout: MetalMemoryLayout,
    channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
}

#[derive(Clone, Copy, Debug, Default)]
struct TcpStateLayout {
    receiver_offset: usize,
    ledger_meta_offset: usize,
}

struct PreparedTcpState {
    words: Vec<u64>,
    layout: TcpStateLayout,
}

impl MetalPlan {
    fn new(
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
    ) -> Result<Self, MetalError> {
        let mut channel_capacity_floors = crate::device_capacity::ChannelCapacityFloors::default();
        let mut tcp_capacity_floors = crate::device_capacity::TcpCapacityFloors::default();
        Self::new_with_entity_capacity_floors(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            &mut channel_capacity_floors,
            &mut tcp_capacity_floors,
        )
    }

    fn new_with_entity_capacity_floors(
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
        channel_capacity_floors: &mut crate::device_capacity::ChannelCapacityFloors,
        tcp_capacity_floors: &mut crate::device_capacity::TcpCapacityFloors,
    ) -> Result<Self, MetalError> {
        Self::new_with_capacity_mode_and_tcp_floors(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            PlannerCapacityMode::Precomputed,
            channel_capacity_floors,
            tcp_capacity_floors,
        )
    }

    #[cfg(feature = "planner-test-hooks")]
    fn new_with_capacity_mode(
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
        capacity_mode: PlannerCapacityMode,
    ) -> Result<Self, MetalError> {
        let mut channel_capacity_floors = crate::device_capacity::ChannelCapacityFloors::default();
        let mut tcp_capacity_floors = crate::device_capacity::TcpCapacityFloors::default();
        Self::new_with_capacity_mode_and_tcp_floors(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            capacity_mode,
            &mut channel_capacity_floors,
            &mut tcp_capacity_floors,
        )
    }

    fn new_with_capacity_mode_and_tcp_floors(
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: MetalConfig,
        observation_mode: ObservationMode,
        capacity_mode: PlannerCapacityMode,
        channel_capacity_floors: &mut crate::device_capacity::ChannelCapacityFloors,
        tcp_capacity_floors: &mut crate::device_capacity::TcpCapacityFloors,
    ) -> Result<Self, MetalError> {
        let node_count = image.nodes.len();
        let (flow_packet_counts, flow_feedback_counts) = flow_packet_counts(image)?;
        let minimum_lookahead_ns = image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min();
        let flow_data_counts = flow_packet_counts
            .iter()
            .zip(&flow_feedback_counts)
            .map(|(total, feedback)| total.saturating_sub(*feedback))
            .collect::<Vec<_>>();
        let capacity_context = PlannerCapacityContext::new(
            image,
            &flow_data_counts,
            minimum_lookahead_ns,
            TcpMinimumPacketSize::One,
            capacity_mode,
        );
        let initial_by_payload = image
            .initial_packets
            .iter()
            .copied()
            .map(|packet| (packet.id, packet))
            .collect::<BTreeMap<_, _>>();
        let positioned_payloads = crate::tcp_ledger::initial_live_payloads(image);
        let orphan_packets = image
            .initial_packets
            .iter()
            .copied()
            .filter(|packet| !positioned_payloads.contains(&packet.id))
            .filter(|packet| !matches!(packet.kind, PacketKind::TcpData(_)))
            .collect();

        let mut queue_caps = vec![1_usize; node_count];
        let mut aggregate_queue_packets = vec![0_usize; node_count];
        let mut minimum_queue_packet_bytes = vec![u64::MAX; node_count];
        let mut legacy_fel_caps = vec![8_usize; node_count];
        let mut initial_fel_counts = vec![0_usize; node_count];
        for event in &image.initial_events {
            let target = event.target.0 as usize;
            legacy_fel_caps[target] = legacy_fel_caps[target].saturating_add(1);
            initial_fel_counts[target] = initial_fel_counts[target].saturating_add(1);
        }
        for (flow_index, flow) in image.flows.iter().enumerate() {
            let packet_count = flow_packet_counts[flow_index];
            let feedback_count = flow_feedback_counts[flow_index];
            let data_count = packet_count.saturating_sub(feedback_count);
            let source_slot = flow.source.0 as usize;
            queue_caps[source_slot] = queue_caps[source_slot].saturating_add(
                capacity_context.source_queue_packet_bound(image, flow_index, data_count),
            );
            legacy_fel_caps[source_slot] = legacy_fel_caps[source_slot].saturating_add(4);
            if capacity_context.tcp_generator(image, flow_index).is_some() {
                // TCP timers remain fallback-heap events. Under the live-state contract
                // (Mechanism API errata E5) a superseded timeout is removed at the invalidating
                // edge and disarm precedes re-arm inside one transition, so a source-owned flow
                // holds at most one live timer record. Do not restore a per-attempt reservation:
                // imported timeouts that no armed timer owns are covered by the separate
                // initial-event floor in `device_sizing`.
                legacy_fel_caps[source_slot] = legacy_fel_caps[source_slot].saturating_add(
                    capacity_context.tcp_fallback_timer_bound(image, flow_index, data_count),
                );
            }

            let mut route_capacities = FlowRouteCapacities {
                lookahead: minimum_lookahead_ns,
                fel: &mut legacy_fel_caps,
                queue: &mut queue_caps,
                aggregate_queue_packets: &mut aggregate_queue_packets,
                minimum_queue_packet_bytes: &mut minimum_queue_packet_bytes,
            };
            add_flow_route_capacities(
                image,
                &capacity_context,
                flow_index,
                data_count,
                PacketKind::Data,
                &mut route_capacities,
            );
            add_flow_route_capacities(
                image,
                &capacity_context,
                flow_index,
                feedback_count,
                PacketKind::Feedback,
                &mut route_capacities,
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
                    let queue = state.queues.first();
                    let initial = queue.map_or(0, |queue| queue.queue.len());
                    aggregate_queue_packets[slot] = aggregate_queue_packets[slot].max(initial);
                    if let Some(queue) = queue {
                        for payload in &queue.queue {
                            minimum_queue_packet_bytes[slot] = minimum_queue_packet_bytes[slot]
                                .min(packet_for(&initial_by_payload, *payload)?.size_bytes);
                        }
                    }
                    queue_caps[slot] = queue_caps[slot].max(initial);
                    if let Some(queue) = queue {
                        match queue.drop_mark {
                            crate::DropMarkPolicy::TailDrop
                                if queue.queue_capacity_packets != 0 =>
                            {
                                queue_caps[slot] = queue_caps[slot].min(
                                    usize::try_from(queue.queue_capacity_packets)
                                        .unwrap_or(usize::MAX),
                                );
                            }
                            crate::DropMarkPolicy::EcnThreshold(policy) => {
                                queue_caps[slot] = crate::device_sizing::ecn_queue_packet_bound(
                                    aggregate_queue_packets[slot],
                                    policy,
                                    minimum_queue_packet_bytes[slot],
                                )
                                .max(initial)
                                .max(1);
                            }
                            crate::DropMarkPolicy::TailDrop | crate::DropMarkPolicy::Red(_) => {}
                        }
                    }
                }
            }
            if let Some(limit) = config.max_queue_packets_per_lp {
                queue_caps[slot] = limit.max(config.capacity_floors.queue_packets_per_lp);
            } else {
                let resident = match node.kind {
                    NodeKind::Host => image.host_states[node.state_slot as usize].queue.len(),
                    NodeKind::Switch => image.switch_states[node.state_slot as usize]
                        .queues
                        .first()
                        .map_or(0, |queue| queue.queue.len()),
                };
                queue_caps[slot] = crate::device_capacity::bound_derived_capacity(
                    queue_caps[slot],
                    config.capacity_caps.queue_packets_per_lp,
                    config.capacity_floors.queue_packets_per_lp,
                    resident,
                );
            }
        }

        if let Some(limit) = config.max_fel_events_per_lp {
            legacy_fel_caps.fill(limit.max(config.capacity_floors.fallback_fel_events_per_lp));
        } else {
            for (capacity, resident) in legacy_fel_caps
                .iter_mut()
                .zip(initial_fel_counts.iter().copied())
            {
                *capacity = crate::device_capacity::bound_derived_capacity(
                    *capacity,
                    config.capacity_caps.fallback_fel_events_per_lp,
                    config.capacity_floors.fallback_fel_events_per_lp,
                    resident,
                );
            }
        }
        let mut fel_caps = if config.streams_enabled {
            let mut capacities = vec![1_usize; node_count];
            for event in &image.initial_events {
                let target = event.target.0 as usize;
                capacities[target] = capacities[target].saturating_add(1);
            }
            for (flow, descriptor) in image.flows.iter().enumerate() {
                if capacity_context.tcp_generator(image, flow).is_some() {
                    let attempts = capacity_context.tcp_fallback_timer_bound(
                        image,
                        flow,
                        flow_packet_counts[flow].saturating_sub(flow_feedback_counts[flow]),
                    );
                    let source = descriptor.source.0 as usize;
                    capacities[source] = capacities[source].saturating_add(attempts);
                }
            }
            capacities
        } else {
            legacy_fel_caps.clone()
        };
        if let Some(limit) = config.max_fel_events_per_lp {
            fel_caps.fill(limit.max(config.capacity_floors.fallback_fel_events_per_lp));
        } else {
            for (capacity, resident) in fel_caps.iter_mut().zip(initial_fel_counts.iter().copied())
            {
                *capacity = crate::device_capacity::bound_derived_capacity(
                    *capacity,
                    config.capacity_caps.fallback_fel_events_per_lp,
                    config.capacity_floors.fallback_fel_events_per_lp,
                    resident,
                );
            }
        }

        let mut fel_meta = vec![0_u64; node_count * ARENA_META_WORDS];
        let mut queue_meta = vec![0_u64; node_count * QUEUE_META_WORDS];
        let fel_slots = assign_arena_offsets(&mut fel_meta, &fel_caps)?;
        let queue_slots = assign_queue_offsets(&mut queue_meta, &queue_caps)?;
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
                            false,
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
                        match generator.kind {
                            FlowGeneratorKind::Constant(constant) => {
                                generators[offset + 11] = 0;
                                generators[offset + 12] = constant.first_departure_ns;
                                generators[offset + 13] = constant.interval_ns;
                                generators[offset + 14] = constant.packet_size_bytes;
                                let (kind, value) = match constant.termination {
                                    GeneratorTermination::Bytes(bytes) => (0, bytes),
                                    GeneratorTermination::DurationNs(duration) => (1, duration),
                                };
                                generators[offset + 15] = kind;
                                generators[offset + 16] = value;
                            }
                            FlowGeneratorKind::Tcp(tcp) => {
                                generators[offset + 11] = 1;
                                encode_tcp_generator(
                                    tcp,
                                    &mut generators[offset..offset + GENERATOR_WORDS],
                                );
                            }
                            FlowGeneratorKind::Rate(rate) => {
                                generators[offset + 11] = 2;
                                generators[offset + 12] = rate.first_pacing_time_ns;
                                generators[offset + 13] = rate.pacing_interval_ns;
                                generators[offset + 14] = rate.packet_size_bytes;
                                generators[offset + 15] = rate.total_bytes;
                                generators[offset + 16] = rate.rate_numerator_bits_per_second;
                                generators[offset + 17] = rate.rate_denominator;
                                generators[offset + 18] = rate.credit_quanta as u64;
                                generators[offset + 19] = (rate.credit_quanta >> 64) as u64;
                            }
                            FlowGeneratorKind::Collective(_) => {
                                unreachable!(
                                    "Metal capability validation rejects collective generators"
                                )
                            }
                            FlowGeneratorKind::Dcqcn(_) => {
                                unreachable!("Metal capability validation rejects DCQCN generators")
                            }
                        }
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
                                true,
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

        let scheduler_state =
            prepare_device_schedulers(image, &queue_meta).map_err(MetalError::Validation)?;

        for event in &image.initial_events {
            let packet = if event.kind == EventKind::RetransmissionTimeout {
                None
            } else {
                Some(packet_for(&initial_by_payload, event.payload)?)
            };
            let mut record = event_record(*event, packet);
            if event.kind == EventKind::RetransmissionTimeout {
                let node = &image.nodes[event.target.0 as usize];
                let timer_flow = image.host_states[node.state_slot as usize]
                    .generators
                    .iter()
                    .find_map(|generator| match generator.kind {
                        FlowGeneratorKind::Tcp(tcp)
                            if tcp.active_timer.is_some_and(|timer| {
                                timer.attempt == event.payload
                                    && timer.deadline_ns == event.key.time_ns
                            }) =>
                        {
                            Some(generator.flow)
                        }
                        _ => None,
                    });
                record[8] = timer_flow.map_or(NONE, |flow| flow.0);
            }
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
            &capacity_context,
            &flow_packet_counts,
            &flow_feedback_counts,
            minimum_lookahead_ns,
        );
        let outbox_capacity = config.max_outbox_events.map_or_else(
            || {
                crate::device_capacity::bound_derived_capacity(
                    remote_bound.max(1),
                    config.capacity_caps.outbox_events_total,
                    config.capacity_floors.outbox_events_total,
                    0,
                )
            },
            |capacity| capacity.max(config.capacity_floors.outbox_events_total),
        );
        let mut remote_capacities = derived_remote_capacities(
            image,
            &capacity_context,
            &flow_packet_counts,
            &flow_feedback_counts,
            minimum_lookahead_ns,
        );
        if let Some(capacity) = config.max_outbox_events {
            remote_capacities
                .fill(capacity.max(config.capacity_floors.remote_staging_events_per_lp));
        } else {
            for capacity in &mut remote_capacities {
                *capacity = crate::device_capacity::bound_derived_capacity(
                    *capacity,
                    config.capacity_caps.remote_staging_events_per_lp,
                    config.capacity_floors.remote_staging_events_per_lp,
                    0,
                );
            }
        }
        let mut remote_meta = vec![0_u64; node_count * ARENA_META_WORDS];
        let remote_staging_slots = assign_arena_offsets(&mut remote_meta, &remote_capacities)?;
        let streams = prepare_streams(
            image,
            &capacity_context,
            &flow_packet_counts,
            &flow_feedback_counts,
            minimum_lookahead_ns,
            config,
            &legacy_fel_caps,
            &fel_caps,
            &fel_meta,
            &fel_records,
            remote_staging_slots,
            channel_capacity_floors,
        )?;
        let event_bound = derived_transition_bound(image, &flow_packet_counts)?;
        let observation_capacity = if observation_mode == ObservationMode::Full {
            config
                .max_observations
                .unwrap_or(event_bound.max(1))
                .max(config.capacity_floors.observation_events)
        } else {
            0
        };
        let mut observation_capacities = if observation_mode == ObservationMode::Full {
            derived_observation_capacities(image, &flow_packet_counts, &flow_feedback_counts)
        } else {
            vec![0; node_count]
        };
        if let Some(capacity) = config.max_observations {
            observation_capacities.fill(capacity.max(config.capacity_floors.observation_events));
        } else {
            for capacity in &mut observation_capacities {
                *capacity = crate::device_capacity::bound_derived_capacity(
                    *capacity,
                    config.capacity_caps.observation_events_per_lp,
                    config.capacity_floors.observation_events,
                    0,
                );
            }
        }
        let mut observation_meta = vec![0_u64; node_count * OBSERVATION_META_WORDS];
        let observation_slots =
            assign_observation_offsets(&mut observation_meta, &observation_capacities)?;
        let mut tcp_state = prepare_tcp_state(
            image,
            &capacity_context,
            &flow_packet_counts,
            &flow_feedback_counts,
            config.capacity_caps,
            config.capacity_floors,
            tcp_capacity_floors,
        )?;
        publish_live_timer_slots(
            &fel_meta,
            &fel_records,
            node_count,
            tcp_state.layout.ledger_meta_offset,
            &mut tcp_state.words,
        );
        let (inbound_meta, inbound_producers) = remote_inbound_producers(image);
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
        let mut control = vec![0_u64; CONTROL_STORAGE_WORDS];
        control[CONTROL_RUN_END_LO] = run_end as u64;
        control[CONTROL_RUN_END_HI] = (run_end >> 64) as u64;
        let worklist_capacity = node_count
            .max(1)
            .max(config.capacity_floors.worklist_entries_total);
        let params = vec![
            node_count as u64,
            image.flows.len() as u64,
            image.links.len() as u64,
            outbox_capacity as u64,
            worklist_capacity as u64,
            observation_capacity as u64,
            observation_capacity as u64,
            observation_capacity as u64,
            u64::from(observation_mode == ObservationMode::Full),
            minimum_lookahead_ns.unwrap_or(0),
            config.max_transitions_per_lp_per_round as u64,
            image.stop_time_ns,
            u64::from(minimum_lookahead_ns.is_some()),
            round_capacity as u64,
            u64::from(config.streams_enabled),
            streams.layout.stream_count as u64,
            streams.layout.channel_count as u64,
            streams.layout.service_stream_base as u64,
            streams.layout.generator_stream_base as u64,
            streams.layout.lp_stream_meta_offset as u64,
            streams.layout.lp_stream_ids_offset as u64,
            streams.layout.lp_active_ids_offset as u64,
            streams.layout.outbound_meta_offset as u64,
            streams.layout.outbound_entries_offset as u64,
            streams.layout.channel_batch_offset as u64,
            streams.layout.staging_channel_offset as u64,
            streams.layout.channel_target_offset as u64,
            u64::from(cfg!(debug_assertions)),
            tcp_state.layout.receiver_offset as u64,
            tcp_state.layout.ledger_meta_offset as u64,
            config.round_threads_per_threadgroup as u64,
        ];

        if node_count.div_ceil(config.round_threads_per_threadgroup) > u32::MAX as usize {
            return Err(MetalError::Validation(
                "active-worklist threadgroup count exceeds the Metal indirect-dispatch limit"
                    .into(),
            ));
        }
        debug_assert_eq!(
            params[PARAM_ROUND_THREADS],
            config.round_threads_per_threadgroup as u64
        );

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
            worklist: vec![0_u64; worklist_capacity],
            summary: vec![0_u64; node_count.max(1) * SUMMARY_COUNTERS * 2],
            observed: zero_words(observation_slots, OBSERVED_WORDS)?,
            departures: zero_words(observation_slots, DEPARTURE_WORDS)?,
            arrivals: zero_words(observation_slots, ARRIVAL_WORDS)?,
            lp_state: vec![0_u64; node_count.max(1) * LP_STATE_WORDS],
            remote_meta,
            remote_staging: zero_words(remote_staging_slots, EVENT_WORDS)?,
            observation_meta,
            inbound_meta,
            merge_cursors: vec![0_u64; inbound_producers.len().max(1)],
            inbound_producers,
            stream_state: streams.state,
            stream_records: streams.records,
            scheduler_state,
            tcp_state: tcp_state.words,
            stream_layout: streams.layout,
            memory_layout: streams.memory_layout,
            channel_stream_capacity_distribution: streams.channel_stream_capacity_distribution,
            orphan_packets,
            round_capacity,
            dispatch_capacity,
        })
    }
}

fn flow_packet_counts(image: &SimulationImage) -> Result<(Vec<usize>, Vec<usize>), MetalError> {
    let live_payloads = crate::tcp_ledger::initial_live_payloads(image);
    let mut data_counts = vec![0_usize; image.flows.len()];
    let mut feedback_counts = vec![0_usize; image.flows.len()];
    for packet in &image.initial_packets {
        if matches!(packet.kind, PacketKind::TcpData(_)) && !live_payloads.contains(&packet.id) {
            continue;
        }
        let counts = if packet.kind.is_data() {
            &mut data_counts
        } else {
            &mut feedback_counts
        };
        counts[packet.flow.0 as usize] = counts[packet.flow.0 as usize].saturating_add(1);
    }
    let pacing_timer_tokens = image
        .initial_events
        .iter()
        .filter(|event| event.kind == EventKind::PacingTimer)
        .map(|event| (event.target, event.payload, event.key.time_ns))
        .collect::<HashSet<_>>();
    for state in &image.host_states {
        for generator in &state.generators {
            let index = generator.flow.0 as usize;
            if let FlowGeneratorKind::Rate(rate) = generator.kind {
                let work = crate::device_sizing::rate_device_work(image, generator, rate)
                    .map_err(|error| MetalError::Validation(error.to_string()))?;
                let owns_timer_token = pacing_timer_tokens.contains(&(
                    image.flows[index].source,
                    generator.next_emission.payload,
                    generator.next_emission.departure_time_ns,
                ));
                if owns_timer_token {
                    data_counts[index] = data_counts[index].saturating_sub(1);
                }
                data_counts[index] = data_counts[index].saturating_add(work.packets);
                continue;
            }
            let FlowGeneratorKind::Constant(constant) = generator.kind else {
                let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                    unreachable!()
                };
                // Validation rejects the open-loop Stopped state for TCP. A Finished sender has
                // no future attempts, but any genuinely live initial packets were counted above.
                if generator.next_emission.status == GeneratorStatus::Finished {
                    continue;
                }
                let remaining = tcp.total_bytes.saturating_sub(tcp.next_sequence);
                let fresh = remaining.div_ceil(tcp.mss_bytes) as usize;
                let outstanding = tcp.bytes_in_flight.div_ceil(tcp.mss_bytes) as usize;
                // Retransmissions replace an existing ledger entry. Four attempts per outstanding
                // segment plus a small recovery allowance is conservative for the finite T24
                // corpora while retaining an explicit device-capacity fault for pathological loss.
                let attempts = fresh
                    .saturating_add(outstanding.saturating_mul(4))
                    .saturating_add(8);
                let already_scheduled =
                    usize::from(generator.next_emission.status == GeneratorStatus::Scheduled);
                data_counts[index] =
                    data_counts[index].saturating_add(attempts.saturating_sub(already_scheduled));
                continue;
            };
            if generator.next_emission.status != GeneratorStatus::Scheduled {
                continue;
            }
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
            data_counts[index] = data_counts[index].saturating_add(future.saturating_sub(1));
        }
    }
    for state in &image.host_states {
        for generator in &state.generators {
            if matches!(generator.kind, FlowGeneratorKind::Tcp(_)) {
                let flow = generator.flow.0 as usize;
                feedback_counts[flow] = feedback_counts[flow].saturating_add(data_counts[flow]);
            }
        }
    }
    let totals = data_counts
        .into_iter()
        .zip(&feedback_counts)
        .map(|(data, feedback)| data.saturating_add(*feedback))
        .collect();
    Ok((totals, feedback_counts))
}

struct FlowRouteCapacities<'a> {
    lookahead: Option<u64>,
    fel: &'a mut [usize],
    queue: &'a mut [usize],
    aggregate_queue_packets: &'a mut [usize],
    minimum_queue_packet_bytes: &'a mut [u64],
}

fn add_flow_route_capacities(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
    flow_index: usize,
    packet_count: usize,
    packet_kind: PacketKind,
    capacities: &mut FlowRouteCapacities<'_>,
) {
    if packet_count == 0 {
        return;
    }
    let flow = &image.flows[flow_index];
    let (route, terminal) = match packet_kind {
        PacketKind::Data | PacketKind::TcpData(_) => (flow.route.as_slice(), flow.target),
        PacketKind::Feedback | PacketKind::TcpAck(_) => {
            (flow.reverse_route.as_slice(), flow.source)
        }
        PacketKind::Pfc(_) => {
            unreachable!("Metal capability validation rejects PFC payloads")
        }
        PacketKind::DcqcnCnp(_) => (flow.reverse_route.as_slice(), flow.source),
        PacketKind::DcqcnControlTimer => {
            unreachable!("Metal capability validation rejects DCQCN timer payloads")
        }
    };
    for index in 0..route.len() {
        let target = route
            .get(index + 1)
            .map(|next| image.links[next.0 as usize].source)
            .unwrap_or(terminal);
        let target_slot = target.0 as usize;
        let burst = flow_link_fel_bound(
            image,
            capacity_context,
            flow_index,
            packet_count,
            packet_kind,
            route[index],
            capacities.lookahead,
        );
        capacities.fel[target_slot] = capacities.fel[target_slot].saturating_add(burst);
        if image.nodes[target_slot].kind == NodeKind::Switch {
            capacities.aggregate_queue_packets[target_slot] =
                capacities.aggregate_queue_packets[target_slot].saturating_add(packet_count);
            capacities.minimum_queue_packet_bytes[target_slot] = capacities
                .minimum_queue_packet_bytes[target_slot]
                .min(capacity_context.minimum_packet_size(image, flow_index, packet_kind));
            let queue = image.switch_states[image.nodes[target_slot].state_slot as usize]
                .queues
                .first();
            let contribution = if queue.is_some_and(|queue| {
                matches!(queue.drop_mark, crate::DropMarkPolicy::EcnThreshold(_))
            }) {
                capacity_context.horizon_queue_packet_bound(
                    image,
                    flow_index,
                    packet_count,
                    packet_kind,
                    image.links[route[index].0 as usize],
                    capacities.lookahead,
                )
            } else {
                packet_count
            };
            capacities.queue[target_slot] =
                capacities.queue[target_slot].saturating_add(contribution);
        }
    }
}

fn flow_link_serialization_ns(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
    flow_index: usize,
    packet_kind: PacketKind,
    link: crate::LinkDescriptor,
) -> u64 {
    let minimum_size = capacity_context.minimum_packet_size(image, flow_index, packet_kind);
    crate::time::serialization_time_ns(minimum_size, link.rate_bps)
        .expect("Metal validation established a positive finite serialization interval")
}

fn flow_link_round_bound(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
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
            let capacity =
                if packet_kind == PacketKind::Data && source.id == image.flows[flow_index].source {
                    capacity_context.source_queue_packet_bound(image, flow_index, packet_count)
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
        flow_link_serialization_ns(image, capacity_context, flow_index, packet_kind, link);
    let service_burst = lookahead.map_or(packet_count, |lookahead| {
        usize::try_from(lookahead.div_ceil(serialization)).unwrap_or(usize::MAX)
    });
    let generator_burst =
        if packet_kind == PacketKind::Data && link.source == image.flows[flow_index].source {
            capacity_context.generator_round_burst(image, flow_index, packet_count, lookahead)
        } else {
            0
        };

    // For finite lookahead L and serialization S, one horizon can expose the proven checkpoint
    // queue bound Q, one separately stored in-service packet, ceil(L/S) service completions, and
    // floor(L/I)+1 generator emissions. Each component rounds outward, saturating arithmetic
    // cannot shrink the result, and the whole-flow count remains the absolute cap.
    packet_count.min(
        queue_bound
            .saturating_add(1)
            .saturating_add(service_burst)
            .saturating_add(generator_burst),
    )
}

fn flow_link_fel_bound(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
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
    let serialization =
        flow_link_serialization_ns(image, capacity_context, flow_index, packet_kind, link);
    let in_flight =
        usize::try_from(link.propagation_ns.div_ceil(serialization)).unwrap_or(usize::MAX);

    packet_count.min(
        flow_link_round_bound(
            image,
            capacity_context,
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
    capacity_context: &PlannerCapacityContext,
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
                        capacity_context,
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
                        capacity_context,
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

fn derived_remote_capacities(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
    counts: &[usize],
    feedback_counts: &[usize],
    lookahead: Option<u64>,
) -> Vec<usize> {
    let mut capacities = vec![2_usize; image.nodes.len()];
    for (index, flow) in image.flows.iter().enumerate() {
        let feedback_count = feedback_counts[index];
        let data_count = counts[index].saturating_sub(feedback_count);
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
                    capacity_context,
                    index,
                    packet_count,
                    packet_kind,
                    *link_id,
                    lookahead,
                ));
            }
        }
    }
    capacities
}

/// Classifies every v1 runtime event source at prepare time.
///
/// Each declared `RemoteChannel` owns one inbox ring, each LP owns one alternating
/// `TxReady`/`TxComplete` service ring, and each `FlowId` with a generator owns one generator
/// ring. The closed v1 transition set emits no other runtime source; future or otherwise
/// unclassified kinds fall through to the exact heap in `classified_push`. Initial events are
/// deliberately never classified because an image may be a partial-history checkpoint.
#[allow(clippy::too_many_arguments)] // One prepare-time boundary owns all arena sizing inputs.
fn prepare_streams(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
    counts: &[usize],
    feedback_counts: &[usize],
    lookahead: Option<u64>,
    config: MetalConfig,
    legacy_fel_caps: &[usize],
    fallback_fel_caps: &[usize],
    fel_meta: &[u64],
    fel_records: &[u64],
    remote_staging_slots: usize,
    channel_capacity_floors: &mut crate::device_capacity::ChannelCapacityFloors,
) -> Result<PreparedStreams, MetalError> {
    let legacy_heap_event_slots = checked_sum_usize(legacy_fel_caps, "legacy FEL slots")?;
    let fallback_heap_event_slots = checked_sum_usize(fallback_fel_caps, "fallback FEL slots")?;
    let heap_arena_bytes = event_arena_bytes(
        fallback_heap_event_slots,
        image.nodes.len() * ARENA_META_WORDS,
    )?;
    let legacy_heap_arena_bytes = event_arena_bytes(
        legacy_heap_event_slots,
        image.nodes.len() * ARENA_META_WORDS,
    )?;
    if !config.streams_enabled {
        return Ok(PreparedStreams {
            state: vec![0],
            records: vec![0],
            layout: StreamLayout::default(),
            memory_layout: MetalMemoryLayout {
                streams_enabled: false,
                legacy_heap_event_slots,
                fallback_heap_event_slots,
                checkpoint_fallback_events: image.initial_events.len(),
                heap_arena_bytes,
                legacy_heap_arena_bytes,
                ..MetalMemoryLayout::default()
            },
            channel_stream_capacity_distribution: Vec::new(),
        });
    }

    let node_count = image.nodes.len();
    let channel_count = image.channels.len();
    let service_stream_base = channel_count;
    let generator_stream_base = service_stream_base
        .checked_add(node_count)
        .ok_or_else(|| MetalError::Validation("service stream count overflows usize".into()))?;
    let stream_count = generator_stream_base
        .checked_add(image.flows.len())
        .ok_or_else(|| MetalError::Validation("generator stream count overflows usize".into()))?;
    let mut channel_caps = derived_channel_stream_capacities(
        image,
        capacity_context,
        counts,
        feedback_counts,
        lookahead,
    )?;
    if let Some(capacity) = config.max_channel_events_per_stream {
        channel_caps.fill(capacity.max(config.capacity_floors.channel_events_per_stream));
    } else {
        for capacity in &mut channel_caps {
            *capacity = crate::device_capacity::bound_derived_capacity(
                *capacity,
                config.capacity_caps.channel_events_per_stream,
                config.capacity_floors.channel_events_per_stream,
                0,
            );
        }
    }
    for (stream, capacity) in channel_caps.iter_mut().enumerate() {
        *capacity = channel_capacity_floors
            .channel(stream, *capacity)
            .ok_or_else(|| {
                MetalError::Validation(format!(
                    "channel stream {stream} starting capacity changed between retry attempts"
                ))
            })?;
    }
    let channel_stream_capacity_distribution =
        crate::device_capacity::channel_capacity_distribution(&channel_caps);
    let service_caps =
        vec![2_usize.max(config.capacity_floors.service_events_per_stream); node_count];
    let generator_caps =
        vec![2_usize.max(config.capacity_floors.generator_events_per_stream); image.flows.len()];
    let mut stream_caps = Vec::with_capacity(stream_count);
    stream_caps.extend_from_slice(&channel_caps);
    stream_caps.extend_from_slice(&service_caps);
    stream_caps.extend_from_slice(&generator_caps);

    let mut stream_meta = vec![0_u64; stream_count * ARENA_META_WORDS];
    let stream_record_slots = assign_arena_offsets(&mut stream_meta, &stream_caps)?;
    let mut lp_streams = vec![Vec::<u64>::new(); node_count];
    for (channel, descriptor) in image.channels.iter().enumerate() {
        lp_streams[descriptor.target.0 as usize].push(channel as u64);
    }
    for (node, streams) in lp_streams.iter_mut().enumerate() {
        streams.push((service_stream_base + node) as u64);
    }
    for state in &image.host_states {
        for generator in &state.generators {
            let stream = generator_stream_base
                .checked_add(generator.flow.0 as usize)
                .ok_or_else(|| {
                    MetalError::Validation("generator stream identifier overflows usize".into())
                })?;
            lp_streams[image.flows[generator.flow.0 as usize].source.0 as usize]
                .push(stream as u64);
        }
    }
    for streams in &mut lp_streams {
        streams.sort_unstable();
        streams.dedup();
    }

    let mut outbound = vec![Vec::<(u64, u64)>::new(); node_count];
    for (channel, descriptor) in image.channels.iter().enumerate() {
        outbound[descriptor.source.0 as usize].push((descriptor.target.0, channel as u64));
    }
    for entries in &mut outbound {
        entries.sort_unstable();
        if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(MetalError::Validation(
                "stream classification requires one channel per source-target LP pair".into(),
            ));
        }
    }

    let lp_stream_id_words = lp_streams
        .iter()
        .try_fold(0_usize, |total, streams| total.checked_add(streams.len()))
        .ok_or_else(|| MetalError::Validation("LP stream-list size overflows usize".into()))?;
    let lp_active_id_words = lp_streams
        .iter()
        .try_fold(0_usize, |total, streams| {
            total.checked_add(
                streams
                    .len()
                    .saturating_add(1)
                    .saturating_mul(ACTIVE_STREAM_ENTRY_WORDS),
            )
        })
        .ok_or_else(|| MetalError::Validation("active stream-list size overflows usize".into()))?;
    let outbound_entry_count = outbound
        .iter()
        .try_fold(0_usize, |total, entries| total.checked_add(entries.len()))
        .ok_or_else(|| {
            MetalError::Validation("outbound channel-list size overflows usize".into())
        })?;

    let mut next = stream_meta.len();
    let lp_stream_meta_offset = take_words(&mut next, node_count, LP_STREAM_META_WORDS)?;
    let lp_stream_ids_offset = take_words(&mut next, lp_stream_id_words, 1)?;
    let lp_active_ids_offset = take_words(&mut next, lp_active_id_words, 1)?;
    let outbound_meta_offset = take_words(&mut next, node_count, OUTBOUND_META_WORDS)?;
    let outbound_entries_offset =
        take_words(&mut next, outbound_entry_count, OUTBOUND_ENTRY_WORDS)?;
    let channel_batch_offset = take_words(&mut next, channel_count, CHANNEL_BATCH_WORDS)?;
    let staging_channel_offset = take_words(&mut next, remote_staging_slots, 1)?;
    let channel_target_offset = take_words(&mut next, channel_count, 1)?;

    let mut state = stream_meta;
    state.resize(next.max(1), 0);
    let mut declared_cursor = lp_stream_ids_offset;
    let mut active_cursor = lp_active_ids_offset;
    for (node, streams) in lp_streams.iter().enumerate() {
        let meta = lp_stream_meta_offset + node * LP_STREAM_META_WORDS;
        state[meta] = declared_cursor as u64;
        state[meta + 1] = streams.len() as u64;
        state[meta + 2] = active_cursor as u64;
        state[declared_cursor..declared_cursor + streams.len()].copy_from_slice(streams);
        declared_cursor += streams.len();
        let fel_count = fel_meta[node * ARENA_META_WORDS + 3];
        if fel_count != 0 {
            state[active_cursor] = NONE;
            let root = fel_meta[node * ARENA_META_WORDS] as usize * EVENT_WORDS;
            state[active_cursor + 1..active_cursor + ACTIVE_STREAM_ENTRY_WORDS]
                .copy_from_slice(&fel_records[root..root + 4]);
            state[meta + 3] = 1;
        }
        active_cursor += (streams.len() + 1) * ACTIVE_STREAM_ENTRY_WORDS;
    }

    let mut outbound_cursor = outbound_entries_offset;
    for (node, entries) in outbound.iter().enumerate() {
        let meta = outbound_meta_offset + node * OUTBOUND_META_WORDS;
        state[meta] = outbound_cursor as u64;
        state[meta + 1] = entries.len() as u64;
        for &(target, channel) in entries {
            state[outbound_cursor] = target;
            state[outbound_cursor + 1] = channel;
            outbound_cursor += OUTBOUND_ENTRY_WORDS;
        }
    }
    for channel in 0..channel_count {
        let batch = channel_batch_offset + channel * CHANNEL_BATCH_WORDS;
        state[batch + 1] = NONE;
        state[batch + 2] = NONE;
        // The kernel's legacy `P_CHANNEL_TARGET_OFFSET` name is retained for ABI stability. This
        // diagnostic-only word carries the immutable channel-stream index so host retry can grow
        // precisely the ring selected by the deterministic exchange-prefix reduction.
        state[channel_target_offset + channel] = channel as u64;
    }
    state[staging_channel_offset..staging_channel_offset + remote_staging_slots].fill(NONE);

    let channel_stream_event_slots = checked_sum_usize(&channel_caps, "channel stream slots")?;
    let service_stream_event_slots = checked_sum_usize(&service_caps, "service stream slots")?;
    let generator_stream_event_slots =
        checked_sum_usize(&generator_caps, "generator stream slots")?;
    let stream_arena_bytes = stream_record_slots
        .checked_mul(EVENT_WORDS)
        .and_then(|words| words.checked_mul(std::mem::size_of::<u64>()))
        .and_then(|bytes| {
            state
                .len()
                .checked_mul(std::mem::size_of::<u64>())
                .and_then(|meta| bytes.checked_add(meta))
        })
        .ok_or_else(|| MetalError::Validation("stream arena byte size overflows usize".into()))?;

    Ok(PreparedStreams {
        state,
        records: zero_words(stream_record_slots, EVENT_WORDS)?,
        layout: StreamLayout {
            stream_count,
            channel_count,
            service_stream_base,
            generator_stream_base,
            lp_stream_meta_offset,
            lp_stream_ids_offset,
            lp_active_ids_offset,
            outbound_meta_offset,
            outbound_entries_offset,
            channel_batch_offset,
            staging_channel_offset,
            channel_target_offset,
        },
        memory_layout: MetalMemoryLayout {
            streams_enabled: true,
            legacy_heap_event_slots,
            fallback_heap_event_slots,
            checkpoint_fallback_events: image.initial_events.len(),
            channel_stream_event_slots,
            service_stream_event_slots,
            generator_stream_event_slots,
            heap_arena_bytes,
            stream_arena_bytes,
            legacy_heap_arena_bytes,
        },
        channel_stream_capacity_distribution,
    })
}

fn derived_channel_stream_capacities(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
    counts: &[usize],
    feedback_counts: &[usize],
    lookahead: Option<u64>,
) -> Result<Vec<usize>, MetalError> {
    // A serial non-preemptive producer can emit at most ceil(horizon/serialization)+1 events per
    // horizon. Constant propagation can retain at most ceil(propagation/serialization) older
    // events, and two additional records provide outward-rounded slack. The channel's finite
    // whole-run packet count remains an absolute cap on semantic events, before slack.
    let channels = image
        .channels
        .iter()
        .enumerate()
        .map(|(index, channel)| ((channel.link, channel.target), index))
        .collect::<BTreeMap<_, _>>();
    let mut packet_counts = vec![0_usize; image.channels.len()];
    let mut minimum_serialization = vec![None::<u64>; image.channels.len()];
    for (flow_index, flow) in image.flows.iter().enumerate() {
        let feedback_count = feedback_counts[flow_index];
        let data_count = counts[flow_index].saturating_sub(feedback_count);
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
                    MetalError::Validation(format!(
                        "no stream classification for link {link_id:?} to node {target:?}"
                    ))
                })?;
                packet_counts[channel] = packet_counts[channel].saturating_add(packet_count);
                let serialization = flow_link_serialization_ns(
                    image,
                    capacity_context,
                    flow_index,
                    packet_kind,
                    image.links[link_id.0 as usize],
                );
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
            let horizon_emissions = lookahead.map_or(packet_count, |horizon| {
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

fn checked_sum_usize(values: &[usize], label: &str) -> Result<usize, MetalError> {
    values.iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| MetalError::Validation(format!("{label} overflow usize")))
    })
}

fn event_arena_bytes(record_slots: usize, meta_words: usize) -> Result<usize, MetalError> {
    record_slots
        .checked_mul(EVENT_WORDS)
        .and_then(|words| words.checked_add(meta_words))
        .and_then(|words| words.checked_mul(std::mem::size_of::<u64>()))
        .ok_or_else(|| MetalError::Validation("event arena byte size overflows usize".into()))
}

fn take_words(next: &mut usize, records: usize, words: usize) -> Result<usize, MetalError> {
    let start = *next;
    *next =
        (*next)
            .checked_add(records.checked_mul(words).ok_or_else(|| {
                MetalError::Validation("stream state size overflows usize".into())
            })?)
            .ok_or_else(|| MetalError::Validation("stream state size overflows usize".into()))?;
    Ok(start)
}

fn derived_observation_capacities(
    image: &SimulationImage,
    counts: &[usize],
    feedback_counts: &[usize],
) -> Vec<usize> {
    let mut capacities = vec![1_usize; image.nodes.len()];
    for event in &image.initial_events {
        let target = event.target.0 as usize;
        capacities[target] = capacities[target].saturating_add(1);
    }
    for (index, flow) in image.flows.iter().enumerate() {
        let feedback_count = feedback_counts[index];
        let data_count = counts[index].saturating_sub(feedback_count);
        capacities[flow.source.0 as usize] =
            capacities[flow.source.0 as usize].saturating_add(data_count);
        capacities[flow.target.0 as usize] =
            capacities[flow.target.0 as usize].saturating_add(feedback_count);
        add_route_observation_capacities(
            image,
            &flow.route,
            flow.target,
            data_count,
            &mut capacities,
        );
        add_route_observation_capacities(
            image,
            &flow.reverse_route,
            flow.source,
            feedback_count,
            &mut capacities,
        );
    }
    capacities
}

fn add_route_observation_capacities(
    image: &SimulationImage,
    route: &[crate::LinkId],
    terminal: NodeId,
    packet_count: usize,
    capacities: &mut [usize],
) {
    for (step, link_id) in route.iter().enumerate() {
        let producer = image.links[link_id.0 as usize].source.0 as usize;
        capacities[producer] = capacities[producer].saturating_add(packet_count.saturating_mul(2));
        let target = route
            .get(step + 1)
            .map_or(terminal, |next| image.links[next.0 as usize].source)
            .0 as usize;
        capacities[target] = capacities[target].saturating_add(packet_count);
    }
}

fn remote_inbound_producers(image: &SimulationImage) -> (Vec<u64>, Vec<u64>) {
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
    let mut meta = vec![0_u64; image.nodes.len() * INBOUND_META_WORDS];
    let mut producers = Vec::new();
    for (target, target_producers) in inbound.into_iter().enumerate() {
        let base = target * INBOUND_META_WORDS;
        meta[base] = producers.len() as u64;
        meta[base + 1] = target_producers.len() as u64;
        producers.extend(target_producers);
    }
    (meta, producers)
}

fn derived_transition_bound(
    image: &SimulationImage,
    counts: &[usize],
) -> Result<usize, MetalError> {
    let network = image
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
        );
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .try_fold(network, |bound, generator| {
            let FlowGeneratorKind::Rate(rate) = generator.kind else {
                return Ok(bound);
            };
            let work = crate::device_sizing::rate_device_work(image, generator, rate)
                .map_err(|error| MetalError::Validation(error.to_string()))?;
            Ok(bound.saturating_add(work.pacing_ticks))
        })
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

fn assign_queue_offsets(meta: &mut [u64], capacities: &[usize]) -> Result<usize, MetalError> {
    let mut offset = 0_usize;
    for (slot, capacity) in capacities.iter().copied().enumerate() {
        let base = slot * QUEUE_META_WORDS;
        meta[base] = offset as u64;
        meta[base + 1] = capacity as u64;
        offset = offset.checked_add(capacity).ok_or_else(|| {
            MetalError::Validation("device queue arena size overflows usize".into())
        })?;
    }
    Ok(offset)
}

fn assign_observation_offsets(meta: &mut [u64], capacities: &[usize]) -> Result<usize, MetalError> {
    let mut offset = 0_usize;
    for (slot, capacity) in capacities.iter().copied().enumerate() {
        let base = slot * OBSERVATION_META_WORDS;
        for log in 0..3 {
            let log_base = base + log * ARENA_META_WORDS;
            meta[log_base] = offset as u64;
            meta[log_base + 1] = capacity as u64;
        }
        offset = offset
            .checked_add(capacity)
            .ok_or_else(|| MetalError::Validation("observation arena size overflows".into()))?;
    }
    Ok(offset)
}

fn zero_words(records: usize, words: usize) -> Result<Vec<u64>, MetalError> {
    let length = records
        .checked_mul(words)
        .ok_or_else(|| MetalError::Validation("device buffer size overflows usize".into()))?;
    Ok(vec![0; length.max(1)])
}

fn encode_tcp_generator(tcp: crate::TcpGenerator, row: &mut [u64]) {
    row[12] = tcp.total_bytes;
    row[13] = tcp.mss_bytes;
    row[14] = tcp.ack_size_bytes;
    row[15] = tcp.next_sequence;
    row[16] = tcp.highest_ack;
    row[17] = tcp.bytes_in_flight;
    row[18] = tcp.duplicate_acks;
    row[19] = tcp.recovery_high_sequence;
    row[20] = tcp.last_attempt.0;
    row[21] = tcp.timer_generation;
    if let Some(timer) = tcp.active_timer {
        row[22] = 1;
        row[23] = timer.attempt.0;
        row[24] = timer.sequence;
        row[25] = timer.deadline_ns;
        row[26] = timer.generation;
        row[27] = timer.rto_ns;
    }
    row[28] = tcp.srtt_ns;
    row[29] = tcp.rtt_var_ns;
    row[30] = tcp.rto_ns;
    encode_tcp_control(tcp.control, &mut row[31..43]);
}

fn encode_tcp_control(control: TcpCongestionControl, words: &mut [u64]) {
    words.fill(0);
    match control {
        TcpCongestionControl::Reno(state) => {
            words[0] = 0;
            words[1] = state.mss_bytes;
            words[2] = state.cwnd_bytes;
            words[3] = state.ssthresh_bytes;
            words[4] = state.phase as u64;
            words[5] = state.duplicate_acks;
            words[6] = state.recovery_high_sequence;
            words[7] = state.ca_credit;
        }
        TcpCongestionControl::Cubic(state) => {
            words[0] = 1;
            words[1] = state.mss_bytes;
            words[2] = state.cwnd_scaled;
            words[3] = state.ssthresh_scaled;
            words[4] = state.phase as u64;
            words[5] = state.duplicate_acks;
            words[6] = state.recovery_high_sequence;
            words[7] = state.w_max_scaled;
            words[8] = state.w_last_max_scaled;
            words[9] = state.epoch_start_ns;
            words[10] = state.srtt_ns;
            words[11] = state.k_ns;
        }
    }
}

fn prepare_tcp_state(
    image: &SimulationImage,
    capacity_context: &PlannerCapacityContext,
    packet_counts: &[usize],
    feedback_counts: &[usize],
    capacity_caps: DeviceCapacityCaps,
    capacity_floors: DeviceCapacityFloors,
    tcp_capacity_floors: &mut crate::device_capacity::TcpCapacityFloors,
) -> Result<PreparedTcpState, MetalError> {
    let flow_count = image.flows.len();
    let receiver_offset = 0;
    let mut next = flow_count
        .checked_mul(TCP_RECEIVER_WORDS)
        .ok_or_else(|| MetalError::Validation("TCP receiver state size overflows".into()))?;

    let mut receiver_ranges = vec![None; flow_count];
    for (host_slot, state) in image.host_states.iter().enumerate() {
        if state.tcp_receivers.is_empty() {
            continue;
        }
        let owner = capacity_context
            .host_lp(image, host_slot)
            .ok_or_else(|| MetalError::Validation("TCP receiver owner is missing".into()))?;
        for receiver in &state.tcp_receivers {
            let flow = receiver.flow.0 as usize;
            let data_count = packet_counts
                .get(flow)
                .copied()
                .unwrap_or(0)
                .saturating_sub(feedback_counts.get(flow).copied().unwrap_or(0));
            let derived_capacity = capacity_context
                .tcp_receiver_range_bound(image, flow, data_count)
                .max(receiver.out_of_order.len());
            let base_capacity = crate::device_capacity::bound_derived_capacity(
                derived_capacity,
                capacity_caps.tcp_receiver_ranges_per_flow,
                capacity_floors.tcp_receiver_ranges_per_flow,
                receiver.out_of_order.len(),
            );
            let capacity = tcp_capacity_floors
                .receiver(receiver.flow, base_capacity)
                .ok_or_else(|| {
                    MetalError::Validation(
                        "TCP receiver capacity class changed between retry attempts".into(),
                    )
                })?;
            let words = capacity.checked_mul(TCP_RANGE_WORDS).ok_or_else(|| {
                MetalError::Validation("TCP receiver range size overflows".into())
            })?;
            let range_offset = next;
            next = next.checked_add(words).ok_or_else(|| {
                MetalError::Validation("TCP receiver range arena overflows".into())
            })?;
            receiver_ranges[flow] = Some((owner, receiver.clone(), range_offset, capacity));
        }
    }

    let ledger_meta_offset = next;
    next = next
        .checked_add(
            flow_count
                .checked_mul(TCP_LEDGER_META_WORDS)
                .ok_or_else(|| MetalError::Validation("TCP ledger metadata overflows".into()))?,
        )
        .ok_or_else(|| MetalError::Validation("TCP ledger metadata arena overflows".into()))?;
    let ledger = crate::tcp_ledger::seed_image(image).map_err(|conflict| {
        MetalError::Validation(format!(
            "TCP flow {:?} sequence {} changed segment size from {} to {} bytes",
            conflict.flow,
            conflict.sequence,
            conflict.original_size_bytes,
            conflict.replacement_size_bytes
        ))
    })?;
    let mut ledger_layout = vec![(0_usize, 0_usize); flow_count];
    for (flow, layout) in ledger_layout.iter_mut().enumerate() {
        let current = ledger
            .get(&crate::FlowId(flow as u64))
            .map_or(0, BTreeMap::len);
        let data_count = packet_counts
            .get(flow)
            .copied()
            .unwrap_or(0)
            .saturating_sub(feedback_counts.get(flow).copied().unwrap_or(0));
        let base_capacity = crate::device_capacity::bound_derived_capacity(
            capacity_context
                .tcp_ledger_segment_bound(image, flow, data_count)
                .max(current),
            capacity_caps.tcp_ledger_segments_per_flow,
            capacity_floors.tcp_ledger_segments_per_flow,
            current,
        );
        let capacity = tcp_capacity_floors
            .ledger(FlowId(flow as u64), base_capacity)
            .ok_or_else(|| {
                MetalError::Validation(
                    "TCP ledger capacity class changed between retry attempts".into(),
                )
            })?;
        let record_offset = next;
        next = next
            .checked_add(
                capacity
                    .checked_mul(TCP_LEDGER_RECORD_WORDS)
                    .ok_or_else(|| {
                        MetalError::Validation("TCP ledger record size overflows".into())
                    })?,
            )
            .ok_or_else(|| MetalError::Validation("TCP ledger arena overflows".into()))?;
        *layout = (record_offset, capacity);
    }

    let mut words = vec![0_u64; next.max(1)];
    for (flow, entry) in receiver_ranges.into_iter().enumerate() {
        let Some((owner, receiver, range_offset, capacity)) = entry else {
            continue;
        };
        let base = receiver_offset + flow * TCP_RECEIVER_WORDS;
        words[base] = 1;
        words[base + 1] = owner.0;
        words[base + 2] = receiver.ack_size_bytes;
        words[base + 3] = receiver.next_expected_sequence;
        words[base + 4] = range_offset as u64;
        words[base + 5] = capacity as u64;
        words[base + 6] = receiver.out_of_order.len() as u64;
        for (index, range) in receiver.out_of_order.iter().enumerate() {
            let offset = range_offset + index * TCP_RANGE_WORDS;
            words[offset] = range.start;
            words[offset + 1] = range.end;
        }
    }
    for (flow, &(record_offset, capacity)) in ledger_layout.iter().enumerate() {
        let base = ledger_meta_offset + flow * TCP_LEDGER_META_WORDS;
        words[base] = record_offset as u64;
        words[base + 1] = capacity as u64;
        let segments = ledger.get(&crate::FlowId(flow as u64));
        let resident = segments.map_or(0, BTreeMap::len) as u64;
        words[base + 2] = resident;
        // Mutation site H1: a seeded ring starts unrotated, and its high-water starts at the
        // resident count so a flow that never inserts still reports its true peak.
        words[base + LEDGER_META_HEAD] = 0;
        words[base + LEDGER_META_HIGH_WATER] = resident;
        if let Some(segments) = segments {
            for (index, packet) in segments.values().enumerate() {
                let PacketKind::TcpData(header) = packet.kind else {
                    unreachable!("TCP ledgers contain only TCP data")
                };
                let offset = record_offset + index * TCP_LEDGER_RECORD_WORDS;
                words[offset] = packet.id.0;
                words[offset + 1] = packet.size_bytes;
                words[offset + 2] = header.sequence;
                words[offset + 3] = header.sent_time_ns;
                words[offset + 4] = u64::from(header.retransmission);
            }
        }
    }
    Ok(PreparedTcpState {
        words,
        layout: TcpStateLayout {
            receiver_offset,
            ledger_meta_offset,
        },
    })
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
    record[10] = encode_packet_kind_word(packet);
    encode_packet_metadata(packet.kind, &mut record[11..14]);
    record
}

fn event_record(event: Event, packet: Option<PacketDescriptor>) -> [u64; EVENT_WORDS] {
    let mut record = [0_u64; EVENT_WORDS];
    record[0] = event.key.time_ns;
    record[1] = u64::from(event.key.phase);
    record[2] = event.key.origin_node.0;
    record[3] = event.key.origin_seq;
    record[4] = event.target.0;
    record[5] = event.kind as u64;
    record[6] = event.payload.0;
    if let Some(packet) = packet {
        record[7] = packet.id.0;
        record[8] = packet.flow.0;
        record[9] = packet.size_bytes;
        record[10] = encode_packet_kind_word(packet);
        encode_packet_metadata(packet.kind, &mut record[11..14]);
    }
    record
}

fn encode_packet_kind_word(packet: PacketDescriptor) -> u64 {
    u64::from(packet.kind.code())
        | if packet.ecn_marked {
            PACKET_ECN_FLAG
        } else {
            0
        }
}

fn encode_packet_metadata(kind: PacketKind, words: &mut [u64]) {
    words.fill(0);
    match kind {
        PacketKind::Data | PacketKind::Feedback => {}
        PacketKind::TcpData(header) => {
            words[0] = header.sequence;
            words[1] = header.sent_time_ns;
            words[2] = u64::from(header.retransmission);
        }
        PacketKind::TcpAck(header) => {
            words[0] = header.acknowledgment;
            words[1] = header.acknowledged_bytes;
            words[2] = header.echoed_sent_time_ns;
        }
        PacketKind::Pfc(header) => {
            words[0] = header.controlled_link.0;
            words[1] = u64::from(header.priority);
            words[2] = u64::from(header.pause);
        }
        PacketKind::DcqcnCnp(header) => words[0] = header.trigger_payload.0,
        PacketKind::DcqcnControlTimer => {}
    }
}

fn write_record(storage: &mut [u64], slot: usize, record: [u64; EVENT_WORDS]) {
    let offset = slot * EVENT_WORDS;
    storage[offset..offset + EVENT_WORDS].copy_from_slice(&record);
}

fn record_less(left: &[u64], right: &[u64]) -> bool {
    (left[0], left[1], left[2], left[3]) < (right[0], right[1], right[2], right[3])
}

/// Publishes each armed retransmission timer's fallback-heap slot into TCP ledger metadata word +3.
///
/// The kernel removes a superseded timer through this slot, so the import must apply the same
/// ownership rule the runtime does: a flow owns the canonically first heap record carrying its
/// armed identity. Later duplicates and records the heap build could not attribute to an armed
/// timer are legacy residue, which keeps the lazy pop recognition as its only consumer. The stored
/// value is `slot + 1` so that zero means "this flow owns no heap record".
fn publish_live_timer_slots(
    fel_meta: &[u64],
    fel_records: &[u64],
    node_count: usize,
    ledger_meta_offset: usize,
    tcp_words: &mut [u64],
) {
    let mut owners: BTreeMap<u64, ([u64; 4], usize)> = BTreeMap::new();
    for node in 0..node_count {
        let base = node * ARENA_META_WORDS;
        let offset = fel_meta[base] as usize;
        let count = fel_meta[base + 3] as usize;
        for index in 0..count {
            let slot = offset + index;
            let record = slot * EVENT_WORDS;
            if fel_records[record + 5] != EventKind::RetransmissionTimeout as u64 {
                continue;
            }
            let flow = fel_records[record + 8];
            if flow == NONE {
                continue;
            }
            let key = [
                fel_records[record],
                fel_records[record + 1],
                fel_records[record + 2],
                fel_records[record + 3],
            ];
            match owners.get(&flow) {
                Some((owned, _)) if *owned <= key => {}
                _ => {
                    owners.insert(flow, (key, slot));
                }
            }
        }
    }
    for (flow, (_, slot)) in owners {
        let base = ledger_meta_offset + flow as usize * TCP_LEDGER_META_WORDS;
        tcp_words[base + 3] = slot as u64 + 1;
    }
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
            flow: None,
            stream: None,
            capacity,
            demand: count.saturating_add(1),
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
    track_bytes: bool,
    meta: &mut [u64],
    storage: &mut [u64],
) -> Result<(), MetalError> {
    let base = lp * QUEUE_META_WORDS;
    let offset = meta[base] as usize;
    let capacity = meta[base + 1] as usize;
    let head = meta[base + 2] as usize;
    let count = meta[base + 3] as usize;
    if count == capacity {
        return Err(MetalError::CapacityExceeded {
            arena: MetalArena::Queue,
            node: Some(NodeId(lp as u64)),
            flow: None,
            stream: None,
            capacity,
            demand: count.saturating_add(1),
        });
    }
    if track_bytes {
        meta[base + 4] = meta[base + 4].checked_add(record[9]).ok_or_else(|| {
            MetalError::Validation(format!(
                "Metal queue byte total overflows u64 at LP {:?}",
                NodeId(lp as u64)
            ))
        })?;
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

    /// Allocates `words` device words without staging a host-side zero buffer.
    ///
    /// T20l fix 2's gather destination is written in full by the kernel before anything reads it,
    /// so the ordinary [`Self::new`] path — which allocates a host `Vec` and copies it in — would
    /// pay a host-side pass over the compacted arena for no effect.
    fn uninitialized(
        device: &ProtocolObject<dyn MTLDevice>,
        words: usize,
    ) -> Result<Self, MetalError> {
        let words = words.max(1);
        let bytes = words
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| MetalError::Validation("Metal buffer byte size overflows".into()))?;
        let raw = device
            .newBufferWithLength_options(bytes, MTLResourceOptions::StorageModeShared)
            .ok_or_else(|| {
                MetalError::Unavailable(format!("failed to allocate {bytes}-byte shared buffer"))
            })?;
        Ok(Self { raw, words })
    }

    fn read(&self) -> Vec<u64> {
        #[cfg(feature = "metal-test-hooks")]
        account_readback_words(self.words);
        unsafe {
            std::slice::from_raw_parts(self.raw.contents().cast::<u64>().as_ptr(), self.words)
                .to_vec()
        }
    }

    /// Copies `len` words starting at `start`, clamped to the buffer.
    ///
    /// T20l fix 1 uses this for the one sub-plane a failed attempt needs — the per-flow ledger
    /// occupancy metadata — so a capacity fault never materializes the whole `tcp_state` plane.
    /// Out-of-range requests return a short vector rather than panicking, which
    /// [`ledger_high_water_vector`] already reads as zero.
    fn read_range(&self, start: usize, len: usize) -> Vec<u64> {
        let start = start.min(self.words);
        let len = len.min(self.words - start);
        #[cfg(feature = "metal-test-hooks")]
        account_readback_words(len);
        unsafe {
            std::slice::from_raw_parts(self.raw.contents().cast::<u64>().as_ptr().add(start), len)
                .to_vec()
        }
    }

    fn word(&self, index: usize) -> u64 {
        assert!(index < self.words);
        #[cfg(feature = "metal-test-hooks")]
        account_readback_words(1);
        unsafe { *self.raw.contents().cast::<u64>().as_ptr().add(index) }
    }
}

#[derive(Clone)]
struct FelProbePipelines {
    round_pipeline: MetalPipeline,
    merge_pipeline: MetalPipeline,
}

struct FelProbeResources {
    round_pipeline: Option<MetalPipeline>,
    merge_pipeline: Option<MetalPipeline>,
    node_count: usize,
    counts: SharedBuffer,
    merge_fan_in: SharedBuffer,
}

impl FelProbeResources {
    fn new(
        device: &ProtocolObject<dyn MTLDevice>,
        node_count: usize,
        round_pipeline: Option<MetalPipeline>,
        merge_pipeline: Option<MetalPipeline>,
        inject_round_trip: bool,
    ) -> Result<Self, MetalError> {
        let fan_in_words = node_count
            .checked_mul(4)
            .ok_or_else(|| MetalError::Validation("merge fan-in counter size overflows".into()))?;
        let count_words = node_count
            .checked_mul(2)
            .and_then(|words| words.checked_add(1))
            .ok_or_else(|| {
                MetalError::Validation("FEL diagnostic counter size overflows".into())
            })?;
        let mut counts = vec![0; count_words];
        counts[0] = u64::from(inject_round_trip);
        Ok(Self {
            round_pipeline,
            merge_pipeline,
            node_count,
            counts: SharedBuffer::new(device, counts)?,
            merge_fan_in: SharedBuffer::new(device, vec![0; fan_in_words])?,
        })
    }

    fn bind(&self, encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>) {
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&self.counts.raw), 0, 28);
            encoder.setBuffer_offset_atIndex(Some(&self.merge_fan_in.raw), 0, 29);
        }
    }

    fn fel_counts(&self) -> Result<(u64, u64), MetalError> {
        fn checked_sum(values: &[u64], label: &str) -> Result<u64, MetalError> {
            values.iter().try_fold(0_u64, |total, value| {
                total.checked_add(*value).ok_or_else(|| {
                    MetalError::Validation(format!("FEL diagnostic {label} count overflows"))
                })
            })
        }

        let counts = self.counts.read();
        let local_end = self.node_count + 1;
        Ok((
            checked_sum(&counts[1..local_end], "local-push")?,
            checked_sum(
                &counts[local_end..local_end + self.node_count],
                "injected-round-trip",
            )?,
        ))
    }

    fn merge_fan_in(&self) -> Result<MetalMergeFanIn, MetalError> {
        self.merge_fan_in
            .read()
            .chunks_exact(4)
            .enumerate()
            .try_fold(MetalMergeFanIn::default(), |mut total, (target, row)| {
                total.eventful_target_rounds = total
                    .eventful_target_rounds
                    .checked_add(row[0])
                    .ok_or_else(|| {
                        MetalError::Validation("merge eventful-target-round count overflows".into())
                    })?;
                total.active_producer_target_rounds = total
                    .active_producer_target_rounds
                    .checked_add(row[1])
                    .ok_or_else(|| {
                        MetalError::Validation("merge active-producer count overflows".into())
                    })?;
                total.remote_events = total.remote_events.checked_add(row[2]).ok_or_else(|| {
                    MetalError::Validation("merge remote-event count overflows".into())
                })?;
                if row[3] > total.maximum_active_fan_in {
                    total.maximum_active_fan_in = row[3];
                    total.first_maximum_fan_in_target = Some(NodeId(target as u64));
                    total.maximum_fan_in_target_count = 1;
                } else if row[3] != 0 && row[3] == total.maximum_active_fan_in {
                    total.maximum_fan_in_target_count = total
                        .maximum_fan_in_target_count
                        .checked_add(1)
                        .ok_or_else(|| {
                            MetalError::Validation(
                                "merge maximum-fan-in target count overflows".into(),
                            )
                        })?;
                }
                Ok(total)
            })
    }
}

struct MetalBuffers {
    planes: Vec<SharedBuffer>,
    tcp_state: SharedBuffer,
    orphan_packets: Vec<PacketDescriptor>,
    node_count: usize,
    stream_layout: StreamLayout,
    memory_layout: MetalMemoryLayout,
    channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
    round_capacity: usize,
    dispatch_capacity: usize,
}

impl MetalBuffers {
    fn new(device: &ProtocolObject<dyn MTLDevice>, plan: MetalPlan) -> Result<Self, MetalError> {
        let round_capacity = plan.round_capacity;
        let dispatch_capacity = plan.dispatch_capacity;
        let orphan_packets = plan.orphan_packets;
        let stream_layout = plan.stream_layout;
        let memory_layout = plan.memory_layout;
        let channel_stream_capacity_distribution = plan.channel_stream_capacity_distribution;
        let node_count = plan.params[0] as usize;
        let tcp_state = SharedBuffer::new(device, plan.tcp_state)?;
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
            plan.lp_state,
            plan.remote_meta,
            plan.remote_staging,
            plan.observation_meta,
            plan.inbound_meta,
            plan.inbound_producers,
            plan.merge_cursors,
            plan.stream_state,
            plan.stream_records,
            plan.scheduler_state,
        ]
        .into_iter()
        .map(|words| SharedBuffer::new(device, words))
        .collect::<Result<Vec<_>, _>>()?;
        #[cfg(feature = "metal-test-hooks")]
        record_plane_words(
            planes
                .iter()
                .chain(std::iter::once(&tcp_state))
                .fold(0_u64, |total, plane| {
                    total.saturating_add(plane.words as u64)
                }),
        );
        Ok(Self {
            planes,
            tcp_state,
            orphan_packets,
            node_count,
            stream_layout,
            memory_layout,
            channel_stream_capacity_distribution,
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
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(&self.tcp_state.raw), 0, 30);
        }
    }

    fn finish(
        &self,
        direct: &DirectMetal,
        image: &SimulationImage,
        observation_mode: ObservationMode,
        timing: MetalTiming,
    ) -> Result<MetalRun, AttemptFailure> {
        // T20l fix 1: screen the attempt BEFORE the result arena crosses the bus.
        //
        // The control plane is a few dozen words; the result arena is 18.6-19.2 GB at the RQ9
        // frontier. T20l phase 1 §4.3 measured every one of the four attempts paying that copy in
        // full, three of which are discarded unread, at 5.08 s/attempt on an RTX 4090 and
        // 1.14 s/attempt on Apple unified memory. A capacity fault needs the control plane and —
        // for a ledger fault only — the per-flow occupancy metadata carrying the T20i vector, so
        // those are fetched here and everything else is fetched only once the attempt is known to
        // have succeeded.
        //
        // This changes WHEN bytes cross the bus and nothing else. On the success path the same
        // planes are read, in the same order, into the same `planes` vector; on the failure path
        // the same `AttemptFailure` is produced from the same words. Complete state, retry traces
        // and fault payloads are byte-identical either way.
        let control = self.planes[0].read();
        if control[CONTROL_ERROR] != 0 {
            let error = decode_device_error(&control);
            // T20i layer 2: a ledger fault reports the WHOLE per-flow occupancy vector, not just
            // the first offender. Layer 1 measured 377 of 262,144 frontier flows above the derived
            // floor, so first-offender keying would need up to 377 sequential replans; one vector
            // sizes every flow in a single replan. The payload is one pass over 6 words per flow,
            // and under the screen those 6 words per flow are also all that is copied back.
            let ledger_high_water = matches!(
                error,
                MetalError::CapacityExceeded {
                    arena: MetalArena::TcpSegmentLedger,
                    ..
                }
            )
            .then(|| {
                let meta_offset = self.planes[1].word(PARAM_LEDGER_META_OFFSET) as usize;
                let meta = self.tcp_state.read_range(
                    meta_offset,
                    image.flows.len().saturating_mul(TCP_LEDGER_META_WORDS),
                );
                ledger_high_water_vector(&meta, 0, image.flows.len())
            });
            return Err(AttemptFailure {
                error,
                ledger_high_water,
            });
        }
        if control[CONTROL_DONE] == 0 {
            return Err(MetalError::RoundLimitExceeded {
                capacity: self.round_capacity,
            }
            .into());
        }

        // T20l fix 2: on a SUCCESSFUL attempt, copy the LIVE regions rather than the arena.
        //
        // T20l phase 1 §4.2 measured this readback at 2,394,309,899 words (19.15 GB) on the RQ9
        // frontier's successful attempt, delivering 487,958 pending events and 2,931,779 packet
        // descriptors to a decode that costs 0.10-0.9 s against the 4.6-98.6 s the copy costs.
        // §8 recorded why: the decode phases are the only ones that respect occupancy, and the
        // meta planes that bound them are already on the device before the copy starts.
        //
        // Three classes of plane, and each one's justification:
        //
        //   * ELIDED — the decode never reads them, so their live region is empty by inspection
        //     rather than by any device word: `flows`(4), `routes`(5), `links`(6) are planner
        //     inputs; `outbox`(12), `worklist`(13), `remote_meta`(19), `remote_staging`(20),
        //     `inbound_meta`(22), `inbound_producers`(23) and `merge_cursors`(24) are device
        //     scratch. At the frontier that is 691,104,103 words, 28.86% of the plan, of which
        //     `remote_staging` alone is 657,667,584.
        //   * READ WHOLE — fixed words per entity, and the decode consumes all of them: control,
        //     params, node_state, generators, fel_meta, queue_meta, in_service, summary, lp_state,
        //     observation_meta, stream_state, scheduler_state, plus the two contiguous `tcp_state`
        //     metadata regions.
        //   * COMPACTED — striped record arenas whose live region every entity's own device-written
        //     meta words describe. `compact` gathers those regions into a dense buffer on the
        //     device and reads back only that.
        //
        // Byte-identity: the gather reproduces the decode's own slot arithmetic (see
        // `device_compaction`), so the same records reach the same decoders in the same order.
        // Nothing about WHICH words are decoded changes; only which words cross the bus.
        let params = self.planes[1].read();
        let node_state = self.planes[2].read();
        let generators = self.planes[3].read();
        let fel_meta = self.planes[7].read();
        let queue_meta = self.planes[9].read();
        let in_service = self.planes[11].read();
        let summary_words = self.planes[14].read();
        let lp_state = self.planes[18].read();
        let observation_meta = self.planes[21].read();
        // Only the per-stream ring metadata prefix of `stream_state` is decoded; the LP stream
        // lists, outbound metadata, channel batches, staging channels and channel targets that
        // follow it are device scratch. At the frontier that prefix is 1,667,976 of the plane's
        // 52,426,754 words. The extent is the planner's own `stream_count` — the same bound the
        // decode loop below already walks — not an inferred one.
        let stream_state = self.planes[25].read_range(
            0,
            self.stream_layout
                .stream_count
                .saturating_mul(ARENA_META_WORDS),
        );
        let scheduler_state = self.planes[27].read();

        let node_count = image.nodes.len();
        let flow_count = image.flows.len();
        let receiver_base = params[PARAM_RECEIVER_OFFSET] as usize;
        let ledger_meta_offset = params[PARAM_LEDGER_META_OFFSET] as usize;
        let receiver_state = self
            .tcp_state
            .read_range(receiver_base, flow_count.saturating_mul(TCP_RECEIVER_WORDS));
        let ledger_meta = self.tcp_state.read_range(
            ledger_meta_offset,
            flow_count.saturating_mul(TCP_LEDGER_META_WORDS),
        );

        let fel_plan = arena_compaction_plan(
            "FEL record",
            CompactionShape::Linear,
            EVENT_WORDS,
            self.planes[8].words,
            node_count,
            |lp| {
                let base = lp * ARENA_META_WORDS;
                (
                    fel_meta[base],
                    fel_meta[base + 1],
                    fel_meta[base + 2],
                    fel_meta[base + 3],
                )
            },
        )?;
        let queue_plan = arena_compaction_plan(
            "queue record",
            CompactionShape::Ring,
            EVENT_WORDS,
            self.planes[10].words,
            node_count,
            |lp| {
                let base = lp * QUEUE_META_WORDS;
                (
                    queue_meta[base],
                    queue_meta[base + 1],
                    queue_meta[base + 2],
                    queue_meta[base + 3],
                )
            },
        )?;
        let stream_plan = arena_compaction_plan(
            "stream record",
            CompactionShape::Ring,
            EVENT_WORDS,
            self.planes[26].words,
            self.stream_layout.stream_count,
            |stream| {
                let base = stream * ARENA_META_WORDS;
                (
                    stream_state[base],
                    stream_state[base + 1],
                    stream_state[base + 2],
                    stream_state[base + 3],
                )
            },
        )?;
        // The two `tcp_state` record arenas already carry WORD offsets, so they are planned
        // directly rather than through `arena_compaction_plan`'s record-offset conversion.
        let ledger_plan = CompactionPlan::new(
            "TCP segment ledger",
            CompactionShape::Ring,
            TCP_LEDGER_RECORD_WORDS,
            (0..flow_count)
                .map(|flow| {
                    let base = flow * TCP_LEDGER_META_WORDS;
                    CompactionEntity {
                        source_words: ledger_meta[base],
                        capacity: ledger_meta[base + 1],
                        head: ledger_meta[base + LEDGER_META_HEAD],
                        count: ledger_meta[base + 2],
                    }
                })
                .collect(),
            self.tcp_state.words,
        )
        .map_err(compaction_error)?;
        let receiver_range_plan = CompactionPlan::new(
            "TCP receiver range",
            CompactionShape::Linear,
            TCP_RANGE_WORDS,
            (0..flow_count)
                .map(|flow| {
                    let base = flow * TCP_RECEIVER_WORDS;
                    CompactionEntity {
                        source_words: receiver_state[base + 4],
                        capacity: receiver_state[base + 5],
                        head: 0,
                        count: receiver_state[base + 6],
                    }
                })
                .collect(),
            self.tcp_state.words,
        )
        .map_err(compaction_error)?;
        let observation_plans = [
            ("observed packet", 0, OBSERVED_WORDS, 15),
            ("departure", ARENA_META_WORDS, DEPARTURE_WORDS, 16),
            ("arrival", 2 * ARENA_META_WORDS, ARRIVAL_WORDS, 17),
        ]
        .map(|(arena, log_meta_offset, record_words, plane)| {
            arena_compaction_plan(
                arena,
                CompactionShape::Linear,
                record_words,
                self.planes[plane].words,
                if observation_mode == ObservationMode::Full {
                    node_count
                } else {
                    0
                },
                |node| {
                    let base = node * OBSERVATION_META_WORDS + log_meta_offset;
                    (
                        observation_meta[base],
                        observation_meta[base + 1],
                        observation_meta[base + 2],
                        observation_meta[base + 3],
                    )
                },
            )
        });
        let [observed_plan, departure_plan, arrival_plan] = match observation_plans {
            [Ok(observed), Ok(departure), Ok(arrival)] => [observed, departure, arrival],
            [Err(error), _, _] | [_, Err(error), _] | [_, _, Err(error)] => return Err(error),
        };

        let gathered = direct.compact(&[
            (&self.planes[8], &fel_plan),
            (&self.planes[10], &queue_plan),
            (&self.planes[26], &stream_plan),
            (&self.tcp_state, &ledger_plan),
            (&self.tcp_state, &receiver_range_plan),
            (&self.planes[15], &observed_plan),
            (&self.planes[16], &departure_plan),
            (&self.planes[17], &arrival_plan),
        ])?;
        let [
            fel_records,
            queue_records,
            stream_records,
            ledger_records,
            receiver_ranges,
            observed_words,
            departure_words,
            arrival_words,
        ]: [Vec<u64>; 8] = gathered
            .try_into()
            .expect("compaction returns one buffer per request");

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
            let queue = read_queue(&queue_plan, lp, &queue_records);
            #[cfg(debug_assertions)]
            if node.kind == NodeKind::Switch {
                let derived_bytes = queue
                    .iter()
                    .try_fold(0_u64, |total, packet| total.checked_add(packet.size_bytes));
                debug_assert_eq!(
                    derived_bytes,
                    Some(queue_meta[lp * QUEUE_META_WORDS + 4]),
                    "Metal switch {:?} queue byte counter diverged from its contents",
                    node.id,
                );
            }
            for packet in &queue {
                resident.insert(packet.id, *packet);
            }
            let service = (node_state[base + 4] != 0).then(|| read_packet(&in_service, lp));
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
                        match generators[offset + 11] {
                            0 => {}
                            1 => {
                                generator.kind = FlowGeneratorKind::Tcp(decode_tcp_generator(
                                    &generators[offset..offset + GENERATOR_WORDS],
                                )?);
                            }
                            2 => {
                                let FlowGeneratorKind::Rate(mut rate) = generator.kind else {
                                    return Err(MetalError::DeviceExecution {
                                        code: 96,
                                        node: Some(node.id),
                                    }
                                    .into());
                                };
                                rate.first_pacing_time_ns = generators[offset + 12];
                                rate.pacing_interval_ns = generators[offset + 13];
                                rate.packet_size_bytes = generators[offset + 14];
                                rate.total_bytes = generators[offset + 15];
                                rate.rate_numerator_bits_per_second = generators[offset + 16];
                                rate.rate_denominator = generators[offset + 17];
                                rate.credit_quanta = u128::from(generators[offset + 18])
                                    | (u128::from(generators[offset + 19]) << 64);
                                generator.kind = FlowGeneratorKind::Rate(rate);
                            }
                            _ => {
                                return Err(MetalError::DeviceExecution {
                                    code: 96,
                                    node: Some(node.id),
                                }
                                .into());
                            }
                        }
                    }
                    for receiver in &mut state.tcp_receivers {
                        // T20l fix 2: the receiver state region is contiguous and read whole, so
                        // it is rebased to its own start; the out-of-order ranges are gathered.
                        let flow = receiver.flow.0 as usize;
                        let base = flow * TCP_RECEIVER_WORDS;
                        if receiver_state[base] == 0 || receiver_state[base + 1] != node.id.0 {
                            return Err(MetalError::DeviceExecution {
                                code: 96,
                                node: Some(node.id),
                            }
                            .into());
                        }
                        receiver.ack_size_bytes = receiver_state[base + 2];
                        receiver.next_expected_sequence = receiver_state[base + 3];
                        let range_base = receiver_range_plan.destination(flow);
                        receiver.out_of_order = (0..receiver_range_plan.count(flow))
                            .map(|index| {
                                let offset = (range_base + index) * TCP_RANGE_WORDS;
                                TcpReceiveRange {
                                    start: receiver_ranges[offset],
                                    end: receiver_ranges[offset + 1],
                                }
                            })
                            .collect();
                    }
                }
                NodeKind::Switch => {
                    let state = &mut switch_states[node.state_slot as usize];
                    if let Some(switch_queue) = state.queues.first_mut() {
                        switch_queue.queue = queue.iter().map(|packet| packet.id).collect();
                        switch_queue.in_service = service.map(|packet| packet.id);
                        switch_queue.tx_ready_pending = node_state[base + 3] != 0;
                        restore_device_scheduler(lp, &queue_meta, &scheduler_state, switch_queue)
                            .map_err(MetalError::Validation)?;
                    }
                    state.next_origin_seq = node_state[base + 5];
                    state.arrived_packets = node_state[base + 7];
                    state.dropped_packets = node_state[base + 8];
                    state.departed_packets = node_state[base + 9];
                }
            }
        }

        let mut pending_events = Vec::new();
        for lp in 0..node_count {
            let base = fel_plan.destination(lp);
            for index in 0..fel_plan.count(lp) {
                let record = read_record(&fel_records, base + index);
                let (event, packet) = decode_event(record)?;
                if let Some(packet) = packet {
                    resident.insert(packet.id, packet);
                }
                pending_events.push(event);
            }
        }
        for stream in 0..self.stream_layout.stream_count {
            let base = stream_plan.destination(stream);
            for index in 0..stream_plan.count(stream) {
                let record = read_record(&stream_records, base + index);
                let (event, packet) = decode_event(record)?;
                if let Some(packet) = packet {
                    resident.insert(packet.id, packet);
                }
                pending_events.push(event);
            }
        }
        pending_events.sort_unstable_by_key(|event| event.key);

        for flow in 0..flow_count {
            // Canonical decode: logical order, not physical. The ring was already resolved by the
            // gather, which applies `tcp_ledger_ring::ledger_record_slot`'s own arithmetic.
            let base = ledger_plan.destination(flow);
            for index in 0..ledger_plan.count(flow) {
                let record = (base + index) * TCP_LEDGER_RECORD_WORDS;
                let packet = PacketDescriptor {
                    id: PayloadId(ledger_records[record]),
                    flow: crate::FlowId(flow as u64),
                    size_bytes: ledger_records[record + 1],
                    ecn_marked: false,
                    kind: PacketKind::TcpData(TcpDataHeader {
                        sequence: ledger_records[record + 2],
                        sent_time_ns: ledger_records[record + 3],
                        retransmission: ledger_records[record + 4] != 0,
                    }),
                };
                resident.entry(packet.id).or_insert(packet);
            }
        }

        let summary = decode_summary_rows(&summary_words, node_count);
        let mut observed_packets = BTreeMap::new();
        let mut departures = Vec::new();
        let mut arrivals = Vec::new();
        if observation_mode == ObservationMode::Full {
            // The gather already produced what `compact_lp_log` used to produce on the host: the
            // per-LP live prefixes, concatenated in LP order.
            for words in observed_words.chunks_exact(OBSERVED_WORDS) {
                let packet = decode_packet_words(words)?;
                observed_packets
                    .entry(packet.id)
                    .and_modify(|existing: &mut PacketDescriptor| {
                        existing.ecn_marked |= packet.ecn_marked;
                    })
                    .or_insert(packet);
            }
            let mut keyed_departures = Vec::new();
            let mut keyed_arrivals = Vec::new();
            for words in departure_words.chunks_exact(DEPARTURE_WORDS) {
                let key = decode_key(words)?;
                let packet = PacketDescriptor {
                    id: PayloadId(words[4]),
                    flow: crate::FlowId(words[6]),
                    size_bytes: words[7],
                    ecn_marked: words[8] & PACKET_ECN_FLAG != 0,
                    kind: decode_packet_kind(words[8], &words[9..12])?,
                };
                observed_packets
                    .entry(packet.id)
                    .and_modify(|existing| existing.ecn_marked |= packet.ecn_marked)
                    .or_insert(packet);
                keyed_departures.push((
                    key,
                    PacketDeparture {
                        payload: packet.id,
                        time_ns: words[5],
                    },
                ));
            }
            for words in arrival_words.chunks_exact(ARRIVAL_WORDS) {
                let key = decode_key(words)?;
                let packet = PacketDescriptor {
                    id: PayloadId(words[4]),
                    flow: crate::FlowId(words[7]),
                    size_bytes: words[8],
                    ecn_marked: words[9] & PACKET_ECN_FLAG != 0,
                    kind: decode_packet_kind(words[9], &words[10..13])?,
                };
                observed_packets
                    .entry(packet.id)
                    .and_modify(|existing| existing.ecn_marked |= packet.ecn_marked)
                    .or_insert(packet);
                keyed_arrivals.push((
                    key,
                    PacketArrivalObservation {
                        payload: packet.id,
                        time_ns: words[5],
                        disposition: decode_disposition(words[6])?,
                    },
                ));
            }
            keyed_departures.sort_unstable_by_key(|(key, _)| *key);
            departures = keyed_departures
                .into_iter()
                .map(|(_, departure)| departure)
                .collect();
            keyed_arrivals.sort_unstable_by_key(|(key, _)| *key);
            arrivals = keyed_arrivals
                .into_iter()
                .map(|(_, arrival)| arrival)
                .collect();
        }

        let phase_profile = (!timing.profiled_attempts.is_empty()).then(|| {
            build_phase_profile(
                &timing.profiled_attempts,
                timing.encoded_attempts,
                control[CONTROL_ROUNDS],
                control[CONTROL_RELAUNCHES],
                timing.timestamp_frequency_hz,
                timing.profiled_pass_gap_ns,
                timing.profiled_pass_overlap_ns,
            )
        });
        Ok(MetalRun {
            result: RunResult {
                host_states,
                switch_states,
                summary,
                resident_packets: resident.into_values().collect(),
                observed_packets: observed_packets.into_values().collect(),
                departures,
                arrivals,
                diagnostics: None,
                pending_events,
            },
            capacity_retry_trace: Vec::new(),
            capacity_warm_start: CapacityWarmStart::default(),
            channel_stream_capacity_distribution: self.channel_stream_capacity_distribution.clone(),
            rounds: control[CONTROL_ROUNDS],
            transitions: lp_state
                .chunks_exact(LP_STATE_WORDS)
                .take(image.nodes.len())
                .map(|state| state[1])
                .fold(0_u64, u64::saturating_add),
            encoded_attempts: timing.encoded_attempts,
            continuation_relaunches: control[CONTROL_RELAUNCHES],
            wave_boundary_syncs: timing.wave_boundary_syncs,
            mid_round_wave_boundary_syncs: timing.mid_round_wave_boundary_syncs,
            host_encode_submit_ns: timing.host_encode_submit_ns,
            device_ns: timing.device_ns,
            wall_ns: timing.wall_ns,
            phase_profile,
            memory_layout: self.memory_layout,
        })
    }
}

/// Wraps a T20l fix-2 sizing refusal as an attempt failure.
///
/// A compaction plan can only fail when a device-written meta word describes a region outside its
/// own plane, which is a device-state defect. Refusing is the T20g contract: complete state is
/// never truncated to fit a readback.
fn compaction_error(error: crate::device_compaction::CompactionError) -> AttemptFailure {
    MetalError::Validation(format!("device readback compaction refused: {error}")).into()
}

/// Builds one arena's compaction plan from a per-entity `(offset, capacity, head, count)` meta
/// reader whose offset is a **record** index, which is how every plane except `tcp_state` stores it.
fn arena_compaction_plan(
    arena: &'static str,
    shape: CompactionShape,
    record_words: usize,
    source_plane_words: usize,
    entity_count: usize,
    meta: impl Fn(usize) -> (u64, u64, u64, u64),
) -> Result<CompactionPlan, AttemptFailure> {
    CompactionPlan::new(
        arena,
        shape,
        record_words,
        (0..entity_count)
            .map(|entity| {
                let (offset, capacity, head, count) = meta(entity);
                CompactionEntity {
                    // Saturating rather than checked: an offset this large is already past the
                    // source plane, and the plan's own bound check refuses it with the arena named.
                    source_words: offset.saturating_mul(record_words as u64),
                    capacity,
                    head,
                    count,
                }
            })
            .collect(),
        source_plane_words,
    )
    .map_err(compaction_error)
}

fn decode_device_error(control: &[u64]) -> MetalError {
    let identity = (control[CONTROL_ERROR_NODE] != NONE).then_some(control[CONTROL_ERROR_NODE]);
    let capacity = control[CONTROL_ERROR_CAPACITY] as usize;
    let demand = control[CONTROL_ERROR_DEMAND] as usize;
    match control[CONTROL_ERROR] {
        1 => {
            let arena = decode_arena(control[CONTROL_ERROR_ARENA]);
            let tcp = matches!(
                arena,
                MetalArena::TcpReceiverRanges | MetalArena::TcpSegmentLedger
            );
            let channel = arena == MetalArena::ChannelInbox;
            MetalError::CapacityExceeded {
                arena,
                node: if tcp || channel {
                    None
                } else {
                    identity.map(NodeId)
                },
                flow: if tcp { identity.map(FlowId) } else { None },
                stream: if channel {
                    identity.map(|stream| stream as usize)
                } else {
                    None
                },
                capacity,
                demand,
            }
        }
        2 => MetalError::TransitionLimitExceeded {
            node: identity.map(NodeId).unwrap_or(NodeId(0)),
            capacity,
        },
        100 => MetalError::WfqArithmeticOverflow {
            node: identity.map(NodeId).unwrap_or(NodeId(0)),
        },
        // The channel-order diagnostic shares the channel capacity identity slot, which carries
        // an immutable stream index rather than an LP after targeted retry plumbing.
        142 => MetalError::DeviceExecution {
            code: 142,
            node: None,
        },
        code => MetalError::DeviceExecution {
            code,
            node: identity.map(NodeId),
        },
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
        8 => MetalArena::ChannelInbox,
        9 => MetalArena::ServiceStream,
        10 => MetalArena::GeneratorStream,
        11 => MetalArena::TcpReceiverRanges,
        12 => MetalArena::TcpSegmentLedger,
        13 => MetalArena::RemoteStaging,
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
        ecn_marked: record[10] & PACKET_ECN_FLAG != 0,
        kind: decode_packet_kind(record[10], &record[11..14])
            .expect("device error screening precedes packet readback"),
    }
}

/// Decodes one LP's queue out of the gathered queue records.
///
/// T20l fix 2: the ring rotation is resolved by the gather, so logical index `i` is at
/// `destination(lp) + i`. The decoded order is the same logical order the ring walk produced.
fn read_queue(plan: &CompactionPlan, lp: usize, records: &[u64]) -> Vec<PacketDescriptor> {
    let base = plan.destination(lp);
    (0..plan.count(lp))
        .map(|index| read_packet(records, base + index))
        .collect()
}

fn decode_event(record: &[u64]) -> Result<(Event, Option<PacketDescriptor>), MetalError> {
    let kind = decode_event_kind(record[5])?;
    let packet = (kind != EventKind::RetransmissionTimeout)
        .then(|| {
            Ok(PacketDescriptor {
                id: PayloadId(record[7]),
                flow: crate::FlowId(record[8]),
                size_bytes: record[9],
                ecn_marked: record[10] & PACKET_ECN_FLAG != 0,
                kind: decode_packet_kind(record[10], &record[11..14])?,
            })
        })
        .transpose()?;
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
        4 => Ok(EventKind::RetransmissionTimeout),
        5 => Ok(EventKind::PacingTimer),
        _ => Err(MetalError::DeviceExecution {
            code: 92,
            node: None,
        }),
    }
}

fn decode_packet_kind(value: u64, metadata: &[u64]) -> Result<PacketKind, MetalError> {
    match value & PACKET_KIND_MASK {
        0 => Ok(PacketKind::Data),
        1 => Ok(PacketKind::Feedback),
        2 => Ok(PacketKind::TcpData(TcpDataHeader {
            sequence: metadata[0],
            sent_time_ns: metadata[1],
            retransmission: metadata[2] != 0,
        })),
        3 => Ok(PacketKind::TcpAck(TcpAckHeader {
            acknowledgment: metadata[0],
            acknowledged_bytes: metadata[1],
            echoed_sent_time_ns: metadata[2],
        })),
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
        ecn_marked: words[3] & PACKET_ECN_FLAG != 0,
        kind: decode_packet_kind(words[3], &words[4..7])?,
    })
}

fn decode_tcp_phase(value: u64) -> Result<TcpPhase, MetalError> {
    match value {
        0 => Ok(TcpPhase::SlowStart),
        1 => Ok(TcpPhase::CongestionAvoidance),
        2 => Ok(TcpPhase::FastRecovery),
        _ => Err(MetalError::DeviceExecution {
            code: 97,
            node: None,
        }),
    }
}

fn decode_tcp_control(words: &[u64]) -> Result<TcpCongestionControl, MetalError> {
    match words[0] {
        0 => Ok(TcpCongestionControl::Reno(TcpReno {
            mss_bytes: words[1],
            cwnd_bytes: words[2],
            ssthresh_bytes: words[3],
            phase: decode_tcp_phase(words[4])?,
            duplicate_acks: words[5],
            recovery_high_sequence: words[6],
            ca_credit: words[7],
        })),
        1 => Ok(TcpCongestionControl::Cubic(TcpCubic {
            mss_bytes: words[1],
            cwnd_scaled: words[2],
            ssthresh_scaled: words[3],
            phase: decode_tcp_phase(words[4])?,
            duplicate_acks: words[5],
            recovery_high_sequence: words[6],
            w_max_scaled: words[7],
            w_last_max_scaled: words[8],
            epoch_start_ns: words[9],
            srtt_ns: words[10],
            k_ns: words[11],
        })),
        _ => Err(MetalError::DeviceExecution {
            code: 98,
            node: None,
        }),
    }
}

fn decode_tcp_generator(row: &[u64]) -> Result<crate::TcpGenerator, MetalError> {
    let active_timer = (row[22] != 0).then(|| TcpTimerState {
        attempt: PayloadId(row[23]),
        sequence: row[24],
        deadline_ns: row[25],
        generation: row[26],
        rto_ns: row[27],
    });
    Ok(crate::TcpGenerator {
        total_bytes: row[12],
        mss_bytes: row[13],
        ack_size_bytes: row[14],
        next_sequence: row[15],
        highest_ack: row[16],
        bytes_in_flight: row[17],
        duplicate_acks: row[18],
        recovery_high_sequence: row[19],
        last_attempt: PayloadId(row[20]),
        timer_generation: row[21],
        active_timer,
        srtt_ns: row[28],
        rtt_var_ns: row[29],
        rto_ns: row[30],
        control: decode_tcp_control(&row[31..43])?,
    })
}

fn decode_summary_rows(words: &[u64], node_count: usize) -> RunSummary {
    let counter = |index: usize| -> u128 {
        (0..node_count)
            .map(|node| {
                let offset = node * SUMMARY_COUNTERS * 2 + index * 2;
                u128::from(words[offset]) | (u128::from(words[offset + 1]) << 64)
            })
            .sum()
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
    initialization_timings: MetalInitializationTimings,
    horizon_pipeline: MetalPipeline,
    prepare_pipeline: MetalPipeline,
    round_pipeline: MetalPipeline,
    control_pipeline: MetalPipeline,
    exchange_prefix_pipeline: MetalPipeline,
    exchange_scatter_pipeline: MetalPipeline,
    exchange_merge_pipeline: MetalPipeline,
    finalize_pipeline: MetalPipeline,
    /// T20l fix 2: the readback gather. Not part of an attempt; encoded only by
    /// [`DirectMetal::compact`], after the attempt has been screened as successful.
    compact_pipeline: MetalPipeline,
    fel_probe_pipelines: Mutex<Option<FelProbePipelines>>,
}

impl DirectMetal {
    fn new() -> Result<Self, MetalError> {
        let initialization_started = Instant::now();
        let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
            MetalError::Unavailable("system default device is unavailable".into())
        })?;
        let queue = device
            .newCommandQueue()
            .ok_or_else(|| MetalError::Unavailable("command queue creation failed".into()))?;
        let source = include_str!("metal_kernels.metal");
        let pipeline_started = Instant::now();
        let horizon_pipeline = create_pipeline(&device, source, "days_horizon")?;
        let prepare_pipeline = create_pipeline(&device, source, "days_round_prepare")?;
        let round_pipeline = create_pipeline(&device, source, "days_round")?;
        let control_pipeline = create_pipeline(&device, source, "days_round_control")?;
        let exchange_prefix_pipeline = create_pipeline(&device, source, "days_exchange_prefix")?;
        let exchange_scatter_pipeline = create_pipeline(&device, source, "days_exchange_scatter")?;
        let exchange_merge_pipeline = create_pipeline(&device, source, "days_exchange_merge")?;
        let finalize_pipeline = create_pipeline(&device, source, "days_round_finalize")?;
        let compact_pipeline = create_pipeline(&device, source, "days_compact_gather")?;
        let pipeline_creation_ns = duration_ns(pipeline_started.elapsed());
        for (name, pipeline) in [
            ("horizon", &horizon_pipeline),
            ("round-prepare", &prepare_pipeline),
            ("round-control", &control_pipeline),
            ("exchange-prefix", &exchange_prefix_pipeline),
            ("round-finalize", &finalize_pipeline),
        ] {
            if pipeline.maxTotalThreadsPerThreadgroup() < LANES {
                return Err(MetalError::Unavailable(format!(
                    "{name} pipeline supports only {} threads per threadgroup",
                    pipeline.maxTotalThreadsPerThreadgroup()
                )));
            }
        }
        if device.maxThreadgroupMemoryLength() < CONTROL_THREADGROUP_BYTES {
            return Err(MetalError::Unavailable(format!(
                "device exposes only {} bytes of threadgroup memory",
                device.maxThreadgroupMemoryLength()
            )));
        }
        let initialization_ns = duration_ns(initialization_started.elapsed());
        Ok(Self {
            device,
            queue,
            initialization_timings: MetalInitializationTimings {
                device_queue_setup_ns: initialization_ns.saturating_sub(pipeline_creation_ns),
                pipeline_creation_ns,
                reused_cached_executor: false,
            },
            horizon_pipeline,
            prepare_pipeline,
            round_pipeline,
            control_pipeline,
            exchange_prefix_pipeline,
            exchange_scatter_pipeline,
            exchange_merge_pipeline,
            finalize_pipeline,
            compact_pipeline,
            fel_probe_pipelines: Mutex::new(None),
        })
    }

    /// T20l fix 2: gathers each arena's live records into a dense buffer and reads that back.
    ///
    /// One dispatch per arena, all in a single command buffer and a single synchronization. Every
    /// extent comes from [`CompactionPlan`], i.e. from device-written meta words; an arena whose
    /// device-written counts sum to zero is neither dispatched nor read.
    ///
    /// Returns one gathered word vector per request, in request order.
    fn compact(
        &self,
        requests: &[(&SharedBuffer, &CompactionPlan)],
    ) -> Result<Vec<Vec<u64>>, MetalError> {
        let mut resources = Vec::with_capacity(requests.len());
        for (source, plan) in requests {
            if plan.total_records() == 0 || plan.entity_count() == 0 {
                resources.push(None);
                continue;
            }
            debug_assert!(plan.total_words() <= source.words);
            // The gather writes every word of the destination, so it is deliberately not zeroed:
            // a 250 MB `vec![0; n]` staging copy would reintroduce a fraction of the cost this
            // fix removes.
            let destination = SharedBuffer::uninitialized(&self.device, plan.total_words())?;
            let plan_buffer = SharedBuffer::new(&self.device, plan.plan_words())?;
            let argument_buffer = SharedBuffer::new(&self.device, plan.argument_words())?;
            resources.push(Some((destination, plan_buffer, argument_buffer)));
        }
        if resources.iter().all(Option::is_none) {
            return Ok(requests.iter().map(|_| Vec::new()).collect());
        }

        let command_buffer = self
            .queue
            .commandBuffer()
            .ok_or_else(|| MetalError::Unavailable("compaction command buffer failed".into()))?;
        let encoder = command_buffer
            .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
            .ok_or_else(|| MetalError::Unavailable("compaction encoder creation failed".into()))?;
        for ((source, plan), resource) in requests.iter().zip(&resources) {
            let Some((destination, plan_buffer, argument_buffer)) = resource else {
                continue;
            };
            encoder.setComputePipelineState(&self.compact_pipeline);
            unsafe {
                encoder.setBuffer_offset_atIndex(Some(&destination.raw), 0, 0);
                encoder.setBuffer_offset_atIndex(Some(&source.raw), 0, 1);
                encoder.setBuffer_offset_atIndex(Some(&plan_buffer.raw), 0, 2);
                encoder.setBuffer_offset_atIndex(Some(&argument_buffer.raw), 0, 3);
            }
            let groups = plan
                .entity_count()
                .div_ceil(COMPACT_THREADS_PER_THREADGROUP)
                .max(1);
            encoder.dispatchThreadgroups_threadsPerThreadgroup(
                MTLSize {
                    width: groups,
                    height: 1,
                    depth: 1,
                },
                MTLSize {
                    width: COMPACT_THREADS_PER_THREADGROUP,
                    height: 1,
                    depth: 1,
                },
            );
        }
        encoder.endEncoding();
        command_buffer.commit();
        command_buffer.waitUntilCompleted();
        if command_buffer.status() != MTLCommandBufferStatus::Completed {
            let detail = command_buffer
                .error()
                .map(|error| error.localizedDescription().to_string())
                .unwrap_or_else(|| "no NSError detail".into());
            return Err(MetalError::Unavailable(format!(
                "compaction command buffer status {:?}: {detail}",
                command_buffer.status()
            )));
        }
        Ok(requests
            .iter()
            .zip(&resources)
            .map(|((_, plan), resource)| match resource {
                Some((destination, _, _)) => destination.read_range(0, plan.total_words()),
                None => Vec::new(),
            })
            .collect())
    }

    fn fel_probe_pipelines(&self) -> Result<(FelProbePipelines, u64), MetalError> {
        let mut cached = self.fel_probe_pipelines.lock().map_err(|_| {
            MetalError::Unavailable("FEL diagnostic pipeline cache is poisoned".into())
        })?;
        if let Some(pipelines) = cached.as_ref() {
            return Ok((pipelines.clone(), 0));
        }

        let started = Instant::now();
        let source = format!(
            "#define DAYS_T15E_DIAGNOSTICS 1\n{}",
            include_str!("metal_kernels.metal")
        );
        let pipelines = FelProbePipelines {
            round_pipeline: create_pipeline(&self.device, &source, "days_round_fel_probe")?,
            merge_pipeline: create_pipeline(
                &self.device,
                &source,
                "days_exchange_merge_fan_in_probe",
            )?,
        };
        let creation_ns = duration_ns(started.elapsed());
        *cached = Some(pipelines.clone());
        Ok((pipelines, creation_ns))
    }

    fn run(
        &self,
        buffers: &MetalBuffers,
        config: MetalConfig,
        profile: bool,
    ) -> Result<MetalTiming, MetalError> {
        self.run_with_fel_probe(buffers, config, profile, None)
    }

    fn run_with_fel_probe(
        &self,
        buffers: &MetalBuffers,
        config: MetalConfig,
        profile: bool,
        fel_probe: Option<&FelProbeResources>,
    ) -> Result<MetalTiming, MetalError> {
        let round_threads = config.round_threads_per_threadgroup;
        let round_pipeline = fel_probe
            .and_then(|probe| probe.round_pipeline.as_deref())
            .unwrap_or(&self.round_pipeline);
        let merge_pipeline = fel_probe
            .and_then(|probe| probe.merge_pipeline.as_deref())
            .unwrap_or(&self.exchange_merge_pipeline);
        let execution_width = round_pipeline.threadExecutionWidth();
        if !round_threads.is_multiple_of(execution_width) {
            return Err(MetalError::Validation(format!(
                "round_threads_per_threadgroup must be a multiple of the device execution width \
                 {execution_width}"
            )));
        }
        let parallel_pipelines = [
            round_pipeline,
            &self.exchange_scatter_pipeline,
            merge_pipeline,
        ];
        let supported_threads = parallel_pipelines
            .iter()
            .map(|pipeline| pipeline.maxTotalThreadsPerThreadgroup())
            .min()
            .unwrap_or(0);
        if round_threads > supported_threads {
            return Err(MetalError::Validation(format!(
                "round_threads_per_threadgroup {round_threads} exceeds the supported maximum \
                 {supported_threads}"
            )));
        }
        let wall_started = Instant::now();
        let mut host_encode_submit_ns = 0_u64;
        let mut device_ns = 0_u64;
        let mut wave_boundary_syncs = 0_u64;
        let mut mid_round_wave_boundary_syncs = 0_u64;
        let mut encoded_attempts = 0_u64;
        let mut captured_attempts_encoded = 0_usize;
        let mut profiled_attempts = Vec::new();
        let mut profiled_pass_gap_ns = 0_u64;
        let mut profiled_pass_overlap_ns = 0_u64;
        let timestamp_counter_set = profile.then(|| self.timestamp_counter_set()).transpose()?;
        let timestamp_frequency_hz = if profile {
            self.device.queryTimestampFrequency()
        } else {
            0
        };
        if profile && timestamp_frequency_hz == 0 {
            return Err(MetalError::Unavailable(
                "GPU timestamp frequency is zero".into(),
            ));
        }
        let (pairs_per_command_buffer, pairs_per_wave) =
            encoding_limits(config.rounds_per_command_buffer);
        let parallel_groups = buffers.node_count.div_ceil(round_threads).max(1);
        let parallel_grid = MTLSize {
            width: parallel_groups,
            height: 1,
            depth: 1,
        };
        let parallel_group = MTLSize {
            width: round_threads,
            height: 1,
            depth: 1,
        };
        let control_grid = MTLSize {
            width: 1,
            height: 1,
            depth: 1,
        };
        let control_group = MTLSize {
            width: DispatchGeometry::FixedControl.threads_per_threadgroup(round_threads),
            height: 1,
            depth: 1,
        };
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
                let captured = if timestamp_counter_set.is_some() {
                    encoded.min(PROFILED_ATTEMPTS.saturating_sub(captured_attempts_encoded))
                } else {
                    0
                };
                let counter_buffer = if captured == 0 {
                    None
                } else {
                    let sample_count = captured
                        .checked_mul(PROFILE_SAMPLES_PER_ATTEMPT)
                        .ok_or_else(|| {
                            MetalError::Unavailable(
                                "profile counter sample count overflows usize".into(),
                            )
                        })?;
                    Some(
                        self.counter_sample_buffer(
                            timestamp_counter_set
                                .as_deref()
                                .expect("captured attempts require a counter set"),
                            sample_count,
                        )?,
                    )
                };
                if let Some(counter_buffer) = counter_buffer.as_deref() {
                    for attempt in 0..captured {
                        self.encode_profiled_attempt(
                            &command_buffer,
                            counter_buffer,
                            attempt * PROFILE_SAMPLES_PER_ATTEMPT,
                            buffers,
                            control_grid,
                            control_group,
                            parallel_grid,
                            parallel_group,
                            round_pipeline,
                            merge_pipeline,
                            fel_probe,
                        )?;
                    }
                }
                if captured != encoded {
                    let encoder = command_buffer
                        .computeCommandEncoderWithDispatchType(MTLDispatchType::Serial)
                        .ok_or_else(|| {
                            MetalError::Unavailable("serial compute encoder creation failed".into())
                        })?;
                    buffers.bind(&encoder);
                    if let Some(probe) = fel_probe {
                        probe.bind(&encoder);
                    }
                    for _ in captured..encoded {
                        self.encode_attempt(
                            &encoder,
                            buffers,
                            control_grid,
                            control_group,
                            parallel_grid,
                            parallel_group,
                            round_pipeline,
                            merge_pipeline,
                        );
                    }
                    encoder.endEncoding();
                }
                command_buffers.push(ProfiledCommandBuffer {
                    command_buffer,
                    counter_buffer,
                    captured_attempts: captured,
                });
                captured_attempts_encoded = captured_attempts_encoded.saturating_add(captured);
                encoded_attempts = encoded_attempts.saturating_add(encoded as u64);
                wave_remaining -= encoded;
            }
            for encoded in &command_buffers {
                encoded.command_buffer.commit();
            }
            host_encode_submit_ns =
                host_encode_submit_ns.saturating_add(duration_ns(wave_started.elapsed()));
            command_buffers
                .last()
                .expect("nonzero wave produces a command buffer")
                .command_buffer
                .waitUntilCompleted();
            for encoded in &command_buffers {
                let command_buffer = &encoded.command_buffer;
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
                if let Some(counter_buffer) = encoded.counter_buffer.as_deref() {
                    let resolved =
                        resolve_profile_attempts(counter_buffer, encoded.captured_attempts)?;
                    profiled_attempts.extend(resolved.attempts);
                    profiled_pass_gap_ns =
                        profiled_pass_gap_ns.saturating_add(resolved.pass_gap_ns);
                    profiled_pass_overlap_ns =
                        profiled_pass_overlap_ns.saturating_add(resolved.pass_overlap_ns);
                }
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
            encoded_attempts,
            timestamp_frequency_hz,
            profiled_attempts,
            profiled_pass_gap_ns,
            profiled_pass_overlap_ns,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_attempt(
        &self,
        encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
        buffers: &MetalBuffers,
        control_grid: MTLSize,
        control_group: MTLSize,
        parallel_grid: MTLSize,
        parallel_group: MTLSize,
        round_pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
        merge_pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
    ) {
        for (phase, geometry) in ATTEMPT_PHASES {
            encoder.setComputePipelineState(self.pipeline(phase, round_pipeline, merge_pipeline));
            match geometry {
                DispatchGeometry::FixedControl => {
                    encoder.dispatchThreadgroups_threadsPerThreadgroup(control_grid, control_group);
                }
                DispatchGeometry::ActiveWorklist => unsafe {
                    encoder
                        .dispatchThreadgroupsWithIndirectBuffer_indirectBufferOffset_threadsPerThreadgroup(
                            &buffers.planes[0].raw,
                            CONTROL_INDIRECT_OFFSET_WORDS * std::mem::size_of::<u64>(),
                            parallel_group,
                        );
                },
                DispatchGeometry::Parallel => {
                    encoder
                        .dispatchThreadgroups_threadsPerThreadgroup(parallel_grid, parallel_group);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_profiled_attempt(
        &self,
        command_buffer: &ProtocolObject<dyn MTLCommandBuffer>,
        counter_buffer: &ProtocolObject<dyn MTLCounterSampleBuffer>,
        first_sample: usize,
        buffers: &MetalBuffers,
        control_grid: MTLSize,
        control_group: MTLSize,
        parallel_grid: MTLSize,
        parallel_group: MTLSize,
        round_pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
        merge_pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
        fel_probe: Option<&FelProbeResources>,
    ) -> Result<(), MetalError> {
        for (phase_index, (phase, geometry)) in ATTEMPT_PHASES.into_iter().enumerate() {
            let descriptor = MTLComputePassDescriptor::new();
            descriptor.setDispatchType(MTLDispatchType::Serial);
            let attachment = unsafe {
                descriptor
                    .sampleBufferAttachments()
                    .objectAtIndexedSubscript(0)
            };
            attachment.setSampleBuffer(Some(counter_buffer));
            let sample = first_sample + phase_index * 2;
            unsafe {
                attachment.setStartOfEncoderSampleIndex(sample);
                attachment.setEndOfEncoderSampleIndex(sample + 1);
            }
            let encoder = command_buffer
                .computeCommandEncoderWithDescriptor(&descriptor)
                .ok_or_else(|| {
                    MetalError::Unavailable("profile compute encoder creation failed".into())
                })?;
            buffers.bind(&encoder);
            if let Some(probe) = fel_probe {
                probe.bind(&encoder);
            }
            encoder.setComputePipelineState(self.pipeline(phase, round_pipeline, merge_pipeline));
            match geometry {
                DispatchGeometry::FixedControl => {
                    encoder.dispatchThreadgroups_threadsPerThreadgroup(control_grid, control_group);
                }
                DispatchGeometry::ActiveWorklist => unsafe {
                    encoder
                        .dispatchThreadgroupsWithIndirectBuffer_indirectBufferOffset_threadsPerThreadgroup(
                            &buffers.planes[0].raw,
                            CONTROL_INDIRECT_OFFSET_WORDS * std::mem::size_of::<u64>(),
                            parallel_group,
                        );
                },
                DispatchGeometry::Parallel => {
                    encoder
                        .dispatchThreadgroups_threadsPerThreadgroup(parallel_grid, parallel_group);
                }
            }
            encoder.endEncoding();
        }
        Ok(())
    }

    fn pipeline<'a>(
        &'a self,
        phase: AttemptPhase,
        round_pipeline: &'a ProtocolObject<dyn MTLComputePipelineState>,
        merge_pipeline: &'a ProtocolObject<dyn MTLComputePipelineState>,
    ) -> &'a ProtocolObject<dyn MTLComputePipelineState> {
        match phase {
            AttemptPhase::Horizon => &self.horizon_pipeline,
            AttemptPhase::Compaction => &self.prepare_pipeline,
            AttemptPhase::DrainExecute => round_pipeline,
            AttemptPhase::ContinuationControl => &self.control_pipeline,
            AttemptPhase::ExchangePrefix => &self.exchange_prefix_pipeline,
            AttemptPhase::ExchangeScatter => &self.exchange_scatter_pipeline,
            AttemptPhase::TargetMerge => merge_pipeline,
            AttemptPhase::FinalControl => &self.finalize_pipeline,
        }
    }

    fn timestamp_counter_set(&self) -> Result<RawCounterSet, MetalError> {
        if !self
            .device
            .supportsCounterSampling(MTLCounterSamplingPoint::AtStageBoundary)
        {
            return Err(MetalError::Unavailable(
                "device does not support stage-boundary counter sampling".into(),
            ));
        }
        let sets = self
            .device
            .counterSets()
            .ok_or_else(|| MetalError::Unavailable("device exposes no counter sets".into()))?;
        let expected = unsafe { MTLCommonCounterSetTimestamp }.to_string();
        for index in 0..sets.count() {
            let set = sets.objectAtIndex(index);
            if set.name().to_string() == expected {
                return Ok(set);
            }
        }
        Err(MetalError::Unavailable(
            "device exposes no timestamp counter set".into(),
        ))
    }

    fn counter_sample_buffer(
        &self,
        counter_set: &ProtocolObject<dyn MTLCounterSet>,
        sample_count: usize,
    ) -> Result<RawCounterSampleBuffer, MetalError> {
        let descriptor = MTLCounterSampleBufferDescriptor::new();
        descriptor.setCounterSet(Some(counter_set));
        descriptor.setStorageMode(MTLStorageMode::Shared);
        unsafe {
            descriptor.setSampleCount(sample_count);
        }
        self.device
            .newCounterSampleBufferWithDescriptor_error(&descriptor)
            .map_err(|error| {
                MetalError::Unavailable(format!(
                    "counter sample buffer creation failed: {}",
                    error.localizedDescription()
                ))
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

struct ProfiledCommandBuffer {
    command_buffer: RawCommandBuffer,
    counter_buffer: Option<RawCounterSampleBuffer>,
    captured_attempts: usize,
}

struct MetalTiming {
    host_encode_submit_ns: u64,
    device_ns: u64,
    wall_ns: u64,
    wave_boundary_syncs: u64,
    mid_round_wave_boundary_syncs: u64,
    encoded_attempts: u64,
    timestamp_frequency_hz: u64,
    profiled_attempts: Vec<MetalPhaseTimings>,
    profiled_pass_gap_ns: u64,
    profiled_pass_overlap_ns: u64,
}

struct ResolvedProfileAttempts {
    attempts: Vec<MetalPhaseTimings>,
    pass_gap_ns: u64,
    pass_overlap_ns: u64,
}

fn accumulate_profile_interval(
    frontier: &mut Option<u64>,
    start: u64,
    end: u64,
    pass_gap_ns: &mut u64,
    pass_overlap_ns: &mut u64,
) {
    if let Some(previous) = *frontier {
        if start >= previous {
            *pass_gap_ns = pass_gap_ns.saturating_add(start - previous);
        } else {
            *pass_overlap_ns =
                pass_overlap_ns.saturating_add(previous.min(end).saturating_sub(start));
        }
        *frontier = Some(previous.max(end));
    } else {
        *frontier = Some(end);
    }
}

fn resolve_profile_attempts(
    counter_buffer: &ProtocolObject<dyn MTLCounterSampleBuffer>,
    attempts: usize,
) -> Result<ResolvedProfileAttempts, MetalError> {
    let sample_count = attempts
        .checked_mul(PROFILE_SAMPLES_PER_ATTEMPT)
        .ok_or_else(|| MetalError::Unavailable("profile sample count overflows usize".into()))?;
    let data = unsafe { counter_buffer.resolveCounterRange(NSRange::new(0, sample_count)) }
        .ok_or_else(|| MetalError::Unavailable("counter sample resolution failed".into()))?;
    let expected_bytes = sample_count
        .checked_mul(std::mem::size_of::<MTLCounterResultTimestamp>())
        .ok_or_else(|| MetalError::Unavailable("profile byte count overflows usize".into()))?;
    if data.length() != expected_bytes {
        return Err(MetalError::Unavailable(format!(
            "counter sample resolution returned {} bytes, expected {expected_bytes}",
            data.length()
        )));
    }
    let mut samples = vec![MTLCounterResultTimestamp { timestamp: 0 }; sample_count];
    if expected_bytes != 0 {
        let destination = std::ptr::NonNull::new(samples.as_mut_ptr().cast())
            .expect("nonempty sample allocation has a nonnull pointer");
        unsafe {
            data.getBytes_length(destination, expected_bytes);
        }
    }
    let mut frontier = None;
    let mut pass_gap_ns = 0_u64;
    let mut pass_overlap_ns = 0_u64;
    let attempts = samples
        .chunks_exact(PROFILE_SAMPLES_PER_ATTEMPT)
        .map(|attempt| {
            let mut values = [0_u64; PROFILE_PHASES];
            for (phase, value) in values.iter_mut().enumerate() {
                let start = attempt[phase * 2].timestamp;
                let end = attempt[phase * 2 + 1].timestamp;
                if start == 0
                    || end == 0
                    || start == MTLCounterErrorValue
                    || end == MTLCounterErrorValue
                    || end < start
                {
                    return Err(MetalError::Unavailable(format!(
                        "invalid phase timestamp range {start}..{end}"
                    )));
                }
                accumulate_profile_interval(
                    &mut frontier,
                    start,
                    end,
                    &mut pass_gap_ns,
                    &mut pass_overlap_ns,
                );
                // Resolved counter timestamps use `MTLTimestamp`, whose unit is nanoseconds.
                *value = end - start;
            }
            Ok(MetalPhaseTimings::from_values(values))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResolvedProfileAttempts {
        attempts,
        pass_gap_ns,
        pass_overlap_ns,
    })
}

fn build_phase_profile(
    attempts: &[MetalPhaseTimings],
    encoded_attempts: u64,
    rounds: u64,
    continuation_relaunches: u64,
    timestamp_frequency_hz: u64,
    captured_pass_gap_ns: u64,
    captured_pass_overlap_ns: u64,
) -> MetalPhaseProfile {
    let useful_attempts = rounds.saturating_add(continuation_relaunches);
    let captured_useful = usize::try_from(useful_attempts)
        .unwrap_or(usize::MAX)
        .min(attempts.len());
    let useful = attempts[..captured_useful].iter().copied().fold(
        MetalPhaseTimings::default(),
        MetalPhaseTimings::saturating_add,
    );
    let captured_all_useful = captured_useful as u64 == useful_attempts;
    let captured_termination = captured_all_useful && attempts.len() > captured_useful;
    let termination = if captured_termination {
        attempts[captured_useful]
    } else {
        MetalPhaseTimings::default()
    };
    let idle_start = captured_useful.saturating_add(usize::from(captured_termination));
    let idle_samples = attempts.get(idle_start..).unwrap_or_default();
    let idle_sum = idle_samples.iter().copied().fold(
        MetalPhaseTimings::default(),
        MetalPhaseTimings::saturating_add,
    );
    let idle_mean = idle_sum.divided_by(idle_samples.len() as u64);
    let encoded_idle = encoded_attempts
        .saturating_sub(useful_attempts)
        .saturating_sub(u64::from(captured_termination));
    let estimate_complete = captured_termination && (encoded_idle == 0 || !idle_samples.is_empty());
    let estimated_total = if estimate_complete {
        useful
            .saturating_add(termination)
            .saturating_add(idle_mean.saturating_mul(encoded_idle))
    } else {
        attempts.iter().copied().fold(
            MetalPhaseTimings::default(),
            MetalPhaseTimings::saturating_add,
        )
    };
    MetalPhaseProfile {
        timestamp_frequency_hz,
        encoded_attempts,
        captured_attempts: attempts.len() as u64,
        useful_attempts,
        idle_sample_attempts: idle_samples.len() as u64,
        captured_pass_gap_ns,
        captured_pass_overlap_ns,
        estimate_complete,
        useful,
        termination,
        idle_mean,
        estimated_total,
    }
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
        ATTEMPT_PHASES, AttemptPhase, DispatchGeometry, LANES,
        MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER, MAX_ENCODED_PAIRS_PER_WAVE, MetalConfig, MetalError,
        MetalPhaseTimings, PROFILE_SAMPLES_PER_ATTEMPT, PROFILED_ATTEMPTS,
        accumulate_profile_interval, build_phase_profile, decode_device_error, encoding_limits,
    };
    use crate::CapacityRetryRecord;

    #[test]
    fn tcp_capacity_fault_identity_decodes_as_a_flow() {
        let mut control = vec![0_u64; 20];
        control[0] = 1;
        control[1] = 11;
        control[2] = 9;
        control[3] = 64;
        control[19] = 65;

        assert_eq!(
            decode_device_error(&control),
            MetalError::CapacityExceeded {
                arena: super::MetalArena::TcpReceiverRanges,
                node: None,
                flow: Some(crate::FlowId(9)),
                stream: None,
                capacity: 64,
                demand: 65,
            }
        );
    }

    #[test]
    fn channel_capacity_fault_identity_decodes_as_a_stream() {
        let mut control = vec![0_u64; 20];
        control[0] = 1;
        control[1] = 8;
        control[2] = 73;
        control[3] = 8;
        control[19] = 9;

        assert_eq!(
            decode_device_error(&control),
            MetalError::CapacityExceeded {
                arena: super::MetalArena::ChannelInbox,
                node: None,
                flow: None,
                stream: Some(73),
                capacity: 8,
                demand: 9,
            }
        );

        control[0] = 142;
        assert_eq!(
            decode_device_error(&control),
            MetalError::DeviceExecution {
                code: 142,
                node: None,
            },
            "a channel index must not be displayed as an LP for the order diagnostic",
        );
    }

    #[test]
    fn terminal_failure_retains_every_capacity_retry() {
        let retry = CapacityRetryRecord {
            retry: 1,
            arena: super::MetalArena::Queue,
            node: Some(crate::NodeId(3)),
            flow: None,
            stream: None,
            capacity: 0,
            demand: 1,
            grown_capacity: 2,
        };
        let error =
            MetalError::Unavailable("injected terminal failure".into()).with_retry_trace(&[retry]);

        assert_eq!(
            error,
            MetalError::RetryFailed {
                error: Box::new(MetalError::Unavailable("injected terminal failure".into())),
                capacity_retry_trace: vec![retry],
            }
        );
    }

    #[test]
    fn stream_decomposition_is_enabled_by_default() {
        let config = MetalConfig::default();

        assert!(config.streams_enabled);
        assert_eq!(config.max_channel_events_per_stream, None);
        assert_eq!(config.max_capacity_retries, 16);
    }

    #[test]
    fn production_attempt_uses_fixed_parallel_control_geometry() {
        assert_eq!(
            ATTEMPT_PHASES.map(|(phase, _)| phase),
            [
                AttemptPhase::Horizon,
                AttemptPhase::Compaction,
                AttemptPhase::DrainExecute,
                AttemptPhase::ContinuationControl,
                AttemptPhase::ExchangePrefix,
                AttemptPhase::ExchangeScatter,
                AttemptPhase::TargetMerge,
                AttemptPhase::FinalControl,
            ]
        );
        for phase in [
            AttemptPhase::ContinuationControl,
            AttemptPhase::ExchangePrefix,
            AttemptPhase::FinalControl,
        ] {
            let (_, geometry) = ATTEMPT_PHASES
                .into_iter()
                .find(|(candidate, _)| *candidate == phase)
                .expect("every control phase is present");
            assert_eq!(geometry, DispatchGeometry::FixedControl);
            assert_eq!(geometry.threads_per_threadgroup(256), LANES);
        }

        let (_, drain_geometry) = ATTEMPT_PHASES
            .into_iter()
            .find(|(candidate, _)| *candidate == AttemptPhase::DrainExecute)
            .expect("the drain phase is present");
        assert_eq!(drain_geometry, DispatchGeometry::ActiveWorklist);
    }

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

    #[test]
    fn default_encoding_wave_limits_speculative_tail_attempts() {
        assert_eq!(
            encoding_limits(MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER),
            (MAX_ENCODED_PAIRS_PER_COMMAND_BUFFER, 64)
        );
    }

    #[test]
    fn phase_profile_separates_useful_termination_and_idle_tail_attempts() {
        let attempts = [1_u64, 2, 3, 4].map(|value| MetalPhaseTimings::from_values([value; 8]));
        let profile = build_phase_profile(&attempts, 10, 2, 0, 24_000_000, 17, 3);

        assert!(profile.estimate_complete);
        assert_eq!(profile.useful_attempts, 2);
        assert_eq!(profile.idle_sample_attempts, 1);
        assert_eq!(profile.captured_pass_gap_ns, 17);
        assert_eq!(profile.captured_pass_overlap_ns, 3);
        assert_eq!(profile.useful.horizon_ns, 3);
        assert_eq!(profile.termination.horizon_ns, 3);
        assert_eq!(profile.idle_mean.horizon_ns, 4);
        assert_eq!(profile.estimated_total.horizon_ns, 34);
    }

    #[test]
    fn phase_profile_capture_fits_the_device_sample_buffer_limit() {
        const {
            assert!(PROFILED_ATTEMPTS * PROFILE_SAMPLES_PER_ATTEMPT <= 4_096);
        }
    }

    #[test]
    fn phase_profile_does_not_invent_an_uncaptured_termination_attempt() {
        let attempts = vec![MetalPhaseTimings::from_values([1; 8]); PROFILED_ATTEMPTS];
        let profile = build_phase_profile(
            &attempts,
            PROFILED_ATTEMPTS as u64 + 1,
            PROFILED_ATTEMPTS as u64,
            0,
            24_000_000,
            0,
            0,
        );

        assert!(!profile.estimate_complete);
        assert_eq!(profile.termination, MetalPhaseTimings::default());
    }

    #[test]
    fn phase_profile_preserves_the_fused_dispatch_order() {
        let values = [1, 2, 3, 4, 5, 6, 7, 8];
        let timing = MetalPhaseTimings::from_values(values);

        assert_eq!(timing.values(), values);
        assert_eq!(timing.continuation_control_ns, 4);
        assert_eq!(timing.exchange_prefix_ns, 5);
        assert_eq!(timing.final_control_ns, 8);
    }

    #[test]
    fn phase_profile_interval_accounting_handles_gaps_overlaps_and_nesting() {
        let mut frontier = None;
        let mut gaps = 0;
        let mut overlaps = 0;
        for (start, end) in [(0, 10), (12, 20), (18, 25), (19, 22), (30, 35)] {
            accumulate_profile_interval(&mut frontier, start, end, &mut gaps, &mut overlaps);
        }

        assert_eq!(frontier, Some(35));
        assert_eq!(gaps, 7);
        assert_eq!(overlaps, 5);
    }
}

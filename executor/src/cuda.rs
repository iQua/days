//! Correctness-first production CUDA executor.
//!
//! The backend keeps the safe-horizon round loop resident in explicit CUDA device buffers. One
//! fixed eight-kernel attempt DAG is captured as a CUDA Graph and replayed in bounded waves.
//! Deterministic 1,024-lane reductions publish each exact exclusive horizon and compact active
//! LPs; one CUDA lane owns each active LP transition drain; boundary exchange remains
//! producer-local followed by deterministic per-channel scatter. The host reads only the control
//! plane at graph-wave boundaries, then explicitly transfers every result plane for normalization.
//! No unified or host-mapped device memory and no result-affecting atomics are used.

#[cfg(feature = "cuda-test-hooks")]
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use cudarc::driver::{
    CudaContext, CudaEvent, CudaFunction, CudaGraph, CudaSlice, CudaStream, LaunchConfig,
    PushKernelArg, sys,
};
use cudarc::nvrtc::Ptx;

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
    error: CudaError,
    ledger_high_water: Option<Vec<u32>>,
}

impl From<CudaError> for AttemptFailure {
    fn from(error: CudaError) -> Self {
        Self {
            error,
            ledger_high_water: None,
        }
    }
}
const DEFAULT_TRANSITIONS_PER_DISPATCH: usize = 4_096;
const DEFAULT_ROUND_THREADS_PER_BLOCK: usize = 256;
const DEFAULT_ATTEMPTS_PER_GRAPH_WAVE: usize = 64;
const MAX_ATTEMPTS_PER_GRAPH_WAVE: usize = 16_384;
const NONE: u64 = u64::MAX;
/// Params word holding the absolute word offset of the per-flow ledger metadata in the TCP plane.
/// Params word holding the absolute word offset of the per-flow receiver state in `tcp_state`.
const PARAM_RECEIVER_OFFSET: usize = 28;
const PARAM_LEDGER_META_OFFSET: usize = 29;
const PACKET_ECN_FLAG: u64 = 1_u64 << 63;
const PACKET_KIND_MASK: u64 = !PACKET_ECN_FLAG;

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

static CUDA_DEVICE_EXECUTION: Mutex<()> = Mutex::new(());
static CUDA_DIRECT: OnceLock<Result<Arc<DirectCuda>, String>> = OnceLock::new();

#[cfg(feature = "cuda-test-hooks")]
std::thread_local! {
    static PANIC_AFTER_NEXT_EXECUTION: Cell<bool> = const { Cell::new(false) };
    /// Words this thread has copied out of device buffers since the last reset.
    ///
    /// Thread-local like the panic hook above: the retry loop, the readback and the test all run
    /// on the caller's thread, so no shared mutable state is introduced.
    static READBACK_WORDS: Cell<u64> = const { Cell::new(0) };
    /// Words in every result plane of the last attempt this thread allocated buffers for.
    ///
    /// T20l fix 2's counterfactual: this is exactly what the pre-fix `finish` read back, so
    /// `readback_words / plane_words` is the compaction ratio a test can assert on.
    static PLANE_WORDS: Cell<u64> = const { Cell::new(0) };
}

/// Acquires the supported process-wide CUDA execution envelope.
///
/// The guard schedules device access rather than protecting Rust state, so a panic must not
/// prevent later callers from executing.
pub(crate) fn cuda_device_execution_guard() -> MutexGuard<'static, ()> {
    CUDA_DEVICE_EXECUTION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Arms a one-shot panic after this thread's next successful CUDA graph execution.
///
/// This test hook fires while the process-wide execution guard is still held.
#[doc(hidden)]
#[cfg(feature = "cuda-test-hooks")]
pub fn panic_after_next_execution_for_testing() {
    PANIC_AFTER_NEXT_EXECUTION.set(true);
}

#[cfg(feature = "cuda-test-hooks")]
fn panic_after_execution_if_requested() {
    if PANIC_AFTER_NEXT_EXECUTION.replace(false) {
        panic!("injected panic after CUDA execution");
    }
}

/// Returns and clears this thread's device-to-host readback word count.
///
/// T20l fix 1 is an ordering property — *when* the result arena crosses the bus — and this is the
/// quantity that makes it testable: a screened faulting attempt copies the control plane (and, on
/// a ledger fault, the per-flow occupancy metadata) instead of every plane.
#[cfg(feature = "cuda-test-hooks")]
#[doc(hidden)]
pub fn take_readback_words_for_testing() -> u64 {
    READBACK_WORDS.replace(0)
}

#[cfg(feature = "cuda-test-hooks")]
fn account_readback_words(words: usize) {
    READBACK_WORDS.set(READBACK_WORDS.get().saturating_add(words as u64));
}

/// Returns the total result-plane word count of the last attempt this thread planned.
///
/// T20l fix 2's gate is a ratio, and this is its denominator: the pre-fix `finish` read every one
/// of these words on every successful attempt.
#[cfg(feature = "cuda-test-hooks")]
#[doc(hidden)]
pub fn last_plane_words_for_testing() -> u64 {
    PLANE_WORDS.get()
}

#[cfg(feature = "cuda-test-hooks")]
fn record_plane_words(words: u64) {
    PLANE_WORDS.set(words);
}

/// Bounded device arena reported by a production CUDA capacity fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CudaArena {
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

impl fmt::Display for CudaArena {
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
/// Wraps a T20l fix-2 sizing refusal as an attempt failure.
///
/// A compaction plan can only fail when a device-written meta word describes a region outside its
/// own plane, which is a device-state defect. Refusing is the T20g contract: complete state is
/// never truncated to fit a readback.
fn compaction_error(error: crate::device_compaction::CompactionError) -> AttemptFailure {
    CudaError::Validation(format!("device readback compaction refused: {error}")).into()
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

/// Reads one bounded word range out of a result plane, clamped to the plane.
///
/// T20l fix 2 uses this for the two contiguous `tcp_state` metadata regions the decode needs in
/// full; `CudaSlice::slice` panics on an out-of-range range, so the bounds are clamped the way
/// Metal's `SharedBuffer::read_range` clamps them.
fn bounded_plane_words(
    stream: &std::sync::Arc<CudaStream>,
    plane: &CudaSlice<u64>,
    start: usize,
    len: usize,
    context: &str,
) -> Result<Vec<u64>, AttemptFailure> {
    let start = start.min(plane.len());
    let end = start.saturating_add(len).min(plane.len());
    Ok(screen_plane_words(
        stream,
        plane.slice(start..end),
        context,
    )?)
}

fn decode_device_error(control: &[u64]) -> CudaError {
    let identity = (control[CONTROL_ERROR_NODE] != NONE).then_some(control[CONTROL_ERROR_NODE]);
    let capacity = control[CONTROL_ERROR_CAPACITY] as usize;
    let demand = control[CONTROL_ERROR_DEMAND] as usize;
    match control[CONTROL_ERROR] {
        1 => {
            let arena = decode_arena(control[CONTROL_ERROR_ARENA]);
            let tcp = matches!(
                arena,
                CudaArena::TcpReceiverRanges | CudaArena::TcpSegmentLedger
            );
            let channel = arena == CudaArena::ChannelInbox;
            CudaError::CapacityExceeded {
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
        2 => CudaError::TransitionLimitExceeded {
            node: identity.map(NodeId).unwrap_or(NodeId(0)),
            capacity,
        },
        100 => CudaError::WfqArithmeticOverflow {
            node: identity.map(NodeId).unwrap_or(NodeId(0)),
        },
        // The channel-order diagnostic shares the channel capacity identity slot, which carries
        // an immutable stream index rather than an LP after targeted retry plumbing.
        142 => CudaError::DeviceExecution {
            code: 142,
            node: None,
        },
        code => CudaError::DeviceExecution {
            code,
            node: identity.map(NodeId),
        },
    }
}

fn decode_arena(value: u64) -> CudaArena {
    match value {
        1 => CudaArena::Fel,
        2 => CudaArena::Queue,
        3 => CudaArena::Outbox,
        4 => CudaArena::Worklist,
        5 => CudaArena::ObservedPackets,
        6 => CudaArena::Departures,
        7 => CudaArena::Arrivals,
        8 => CudaArena::ChannelInbox,
        9 => CudaArena::ServiceStream,
        10 => CudaArena::GeneratorStream,
        11 => CudaArena::TcpReceiverRanges,
        12 => CudaArena::TcpSegmentLedger,
        13 => CudaArena::RemoteStaging,
        _ => CudaArena::Fel,
    }
}

fn read_record(storage: &[u64], slot: usize) -> &[u64] {
    let offset = slot * EVENT_WORDS;
    &storage[offset..offset + EVENT_WORDS]
}

fn read_packet(storage: &[u64], slot: usize) -> PacketDescriptor {
    let record = read_record(storage, slot);
    decode_packet_fields(&record[7..14]).expect("device packet records have validated kind codes")
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

fn decode_event(record: &[u64]) -> Result<(Event, PacketDescriptor), CudaError> {
    let kind = decode_event_kind(record[5])?;
    let packet = decode_packet_fields(&record[7..14])?;
    Ok((
        Event {
            key: EventKey {
                time_ns: record[0],
                phase: u16::try_from(record[1]).map_err(|_| CudaError::DeviceExecution {
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

fn decode_key(words: &[u64]) -> Result<EventKey, CudaError> {
    Ok(EventKey {
        time_ns: words[0],
        phase: u16::try_from(words[1]).map_err(|_| CudaError::DeviceExecution {
            code: 91,
            node: None,
        })?,
        origin_node: NodeId(words[2]),
        origin_seq: words[3],
    })
}

fn decode_event_kind(value: u64) -> Result<EventKind, CudaError> {
    match value {
        0 => Ok(EventKind::PacketArrival),
        1 => Ok(EventKind::TxReady),
        2 => Ok(EventKind::TxComplete),
        3 => Ok(EventKind::RemoteArrival),
        4 => Ok(EventKind::RetransmissionTimeout),
        5 => Ok(EventKind::PacingTimer),
        _ => Err(CudaError::DeviceExecution {
            code: 92,
            node: None,
        }),
    }
}

fn decode_packet_kind(value: u64, metadata: &[u64]) -> Result<PacketKind, CudaError> {
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
        _ => Err(CudaError::DeviceExecution {
            code: 93,
            node: None,
        }),
    }
}

fn decode_generator_status(value: u64) -> Result<GeneratorStatus, CudaError> {
    match value {
        0 => Ok(GeneratorStatus::Scheduled),
        1 => Ok(GeneratorStatus::Blocked),
        2 => Ok(GeneratorStatus::Finished),
        3 => Ok(GeneratorStatus::Stopped),
        _ => Err(CudaError::DeviceExecution {
            code: 94,
            node: None,
        }),
    }
}

fn decode_tcp_phase(value: u64) -> Result<TcpPhase, CudaError> {
    match value {
        0 => Ok(TcpPhase::SlowStart),
        1 => Ok(TcpPhase::CongestionAvoidance),
        2 => Ok(TcpPhase::FastRecovery),
        _ => Err(CudaError::DeviceExecution {
            code: 96,
            node: None,
        }),
    }
}

fn decode_control(words: &[u64]) -> Result<TcpCongestionControl, CudaError> {
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
        _ => Err(CudaError::DeviceExecution {
            code: 97,
            node: None,
        }),
    }
}

fn decode_disposition(value: u64) -> Result<ArrivalDisposition, CudaError> {
    match value {
        0 => Ok(ArrivalDisposition::Admitted),
        1 => Ok(ArrivalDisposition::Dropped),
        2 => Ok(ArrivalDisposition::Delivered),
        3 => Ok(ArrivalDisposition::Feedback),
        _ => Err(CudaError::DeviceExecution {
            code: 95,
            node: None,
        }),
    }
}

fn decode_packet_words(words: &[u64]) -> Result<PacketDescriptor, CudaError> {
    decode_packet_fields(words)
}

fn decode_packet_fields(words: &[u64]) -> Result<PacketDescriptor, CudaError> {
    Ok(PacketDescriptor {
        id: PayloadId(words[0]),
        flow: crate::FlowId(words[1]),
        size_bytes: words[2],
        ecn_marked: words[3] & PACKET_ECN_FLAG != 0,
        kind: decode_packet_kind(words[3], &words[4..7])?,
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

/// Production CUDA execution failure. No partial [`RunResult`] is returned.
#[derive(Debug, Eq, PartialEq)]
pub enum CudaError {
    Validation(String),
    Unavailable(String),
    CapacityExceeded {
        arena: CudaArena,
        node: Option<NodeId>,
        flow: Option<FlowId>,
        stream: Option<usize>,
        capacity: usize,
        demand: usize,
    },
    RetryFailed {
        error: Box<CudaError>,
        capacity_retry_trace: Vec<CapacityRetryRecord<CudaArena>>,
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

impl fmt::Display for CudaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(message) => write!(formatter, "invalid CUDA image: {message}"),
            Self::Unavailable(message) => write!(formatter, "CUDA backend unavailable: {message}"),
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
                        "CUDA {arena} capacity of {capacity} records exceeded at stream {stream}; observed demand {demand}"
                    )
                } else if let Some(flow) = flow {
                    write!(
                        formatter,
                        "CUDA {arena} capacity of {capacity} records exceeded at flow {flow:?}; observed demand {demand}"
                    )
                } else if let Some(node) = node {
                    write!(
                        formatter,
                        "CUDA {arena} capacity of {capacity} records exceeded at LP {node:?}; observed demand {demand}"
                    )
                } else {
                    write!(
                        formatter,
                        "CUDA {arena} capacity of {capacity} records exceeded; observed demand {demand}"
                    )
                }
            }
            Self::RetryFailed {
                error,
                capacity_retry_trace,
            } => write!(
                formatter,
                "CUDA execution failed after {} capacity retries: {error}",
                capacity_retry_trace.len()
            ),
            Self::TransitionLimitExceeded { node, capacity } => write!(
                formatter,
                "CUDA LP {node:?} exceeded the per-round transition continuation capacity of \
                 {capacity}"
            ),
            Self::RoundLimitExceeded { capacity } => write!(
                formatter,
                "CUDA bounded graph waves exhausted their capacity of {capacity} rounds before \
                 termination"
            ),
            Self::WfqArithmeticOverflow { node } => write!(
                formatter,
                "CUDA WFQ arithmetic at LP {node:?} exceeds the exact 320-bit device limit; use Scalar or Cpu for this image"
            ),
            Self::DeviceExecution { code, node } => {
                write!(
                    formatter,
                    "CUDA transition kernel reported semantic error {code}"
                )?;
                if let Some(node) = node {
                    write!(formatter, " at LP {node:?}")?;
                }
                Ok(())
            }
        }
    }
}

impl Error for CudaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RetryFailed { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl CudaError {
    fn with_retry_trace(self, capacity_retry_trace: &[CapacityRetryRecord<CudaArena>]) -> Self {
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

/// Physical capacity and bounded CUDA Graph wave policy for one run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CudaConfig {
    /// Enables the stream-decomposed FEL. `false` retains the exact fallback heap path.
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
    pub max_fel_events_per_lp: Option<usize>,
    /// Optional exact starting capacity for every incoming-channel stream. Targeted retries may
    /// raise one stream without changing the others.
    pub max_channel_events_per_stream: Option<usize>,
    pub max_queue_packets_per_lp: Option<usize>,
    pub max_outbox_events: Option<usize>,
    pub max_observations: Option<usize>,
    /// Physical transitions performed by one active LP in one round-kernel launch.
    pub max_transitions_per_lp_per_round: usize,
    /// CUDA threads in each parallel transition/exchange block.
    pub round_threads_per_block: usize,
    /// Complete eight-kernel attempts captured in the reusable graph.
    pub attempts_per_graph_wave: usize,
    /// Optional hard cap overriding the conservative semantic round bound.
    pub max_rounds: Option<usize>,
    /// Test-only zero-capacity injection for device arenas without a public sizing override.
    #[doc(hidden)]
    #[cfg(feature = "cuda-test-hooks")]
    pub fault_injection: Option<CudaArena>,
}

impl Default for CudaConfig {
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
            round_threads_per_block: DEFAULT_ROUND_THREADS_PER_BLOCK,
            attempts_per_graph_wave: DEFAULT_ATTEMPTS_PER_GRAPH_WAVE,
            max_rounds: None,
            #[cfg(feature = "cuda-test-hooks")]
            fault_injection: None,
        }
    }
}

impl CudaConfig {
    fn raise_capacity(&mut self, arena: CudaArena, capacity: usize, grown: usize) {
        match arena {
            CudaArena::Fel => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_fel_events_per_lp,
                    &mut self.capacity_caps.fallback_fel_events_per_lp,
                    &mut self.capacity_floors.fallback_fel_events_per_lp,
                    capacity,
                    grown,
                );
            }
            CudaArena::ChannelInbox => {
                unreachable!("channel capacity retries use per-stream floors")
            }
            CudaArena::ServiceStream => {
                self.capacity_floors.service_events_per_stream =
                    self.capacity_floors.service_events_per_stream.max(grown);
            }
            CudaArena::GeneratorStream => {
                self.capacity_floors.generator_events_per_stream =
                    self.capacity_floors.generator_events_per_stream.max(grown);
            }
            CudaArena::Queue => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_queue_packets_per_lp,
                    &mut self.capacity_caps.queue_packets_per_lp,
                    &mut self.capacity_floors.queue_packets_per_lp,
                    capacity,
                    grown,
                );
            }
            CudaArena::Outbox => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_outbox_events,
                    &mut self.capacity_caps.outbox_events_total,
                    &mut self.capacity_floors.outbox_events_total,
                    capacity,
                    grown,
                );
            }
            CudaArena::Worklist => {
                self.capacity_floors.worklist_entries_total =
                    self.capacity_floors.worklist_entries_total.max(grown);
            }
            CudaArena::ObservedPackets | CudaArena::Departures | CudaArena::Arrivals => {
                crate::device_capacity::raise_override_cap_or_floor(
                    &mut self.max_observations,
                    &mut self.capacity_caps.observation_events_per_lp,
                    &mut self.capacity_floors.observation_events,
                    capacity,
                    grown,
                );
            }
            CudaArena::TcpReceiverRanges | CudaArena::TcpSegmentLedger => {
                unreachable!("TCP capacity retries use per-flow floors")
            }
            CudaArena::RemoteStaging => {
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
/// [`CudaConfig::raise_capacity`] may satisfy a fault by raising an explicit `max_*` override
/// rather than the matching floor, and one override — `max_outbox_events` — serves **two** arenas,
/// so `capacity_floors` alone does not describe what the successful attempt planned. Every planner
/// site for these arenas reads `override.max(floor)` when an override is present and
/// `bound_derived_capacity(..., floor, ...)` when it is not, so `max(floor, override)` is exactly
/// the capacity that attempt used and never more. The entity-keyed arenas are absent here: their
/// converged capacity lives in the per-stream and per-flow vectors instead.
fn converged_capacity_floors(config: &CudaConfig) -> DeviceCapacityFloors {
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

/// Exact planned CUDA event-arena footprint for one run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CudaMemoryLayout {
    pub streams_enabled: bool,
    pub legacy_heap_event_slots: usize,
    pub fallback_heap_event_slots: usize,
    pub checkpoint_fallback_events: usize,
    pub channel_stream_event_slots: usize,
    pub service_stream_event_slots: usize,
    pub generator_stream_event_slots: usize,
    pub heap_arena_bytes: usize,
    pub stream_arena_bytes: usize,
    pub legacy_heap_arena_bytes: usize,
}

impl CudaMemoryLayout {
    pub fn total_event_arena_bytes(self) -> usize {
        self.heap_arena_bytes
            .saturating_add(self.stream_arena_bytes)
    }

    pub fn delta_from_legacy_heap_bytes(self) -> i128 {
        self.total_event_arena_bytes() as i128 - self.legacy_heap_arena_bytes as i128
    }
}

/// Complete production CUDA result and graph-wave diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaRun {
    pub result: RunResult,
    /// Capacity faults from discarded attempts, in deterministic retry order.
    pub capacity_retry_trace: Vec<CapacityRetryRecord<CudaArena>>,
    /// The capacity this run converged on, replayable as a later run's starting capacity.
    ///
    /// T20l fix 3: handing this back to
    /// [`CudaExecutor::run_with_observations_warm_started`] makes the same image plan right on its
    /// first attempt. It is a sizing hint for host planning, derived output of this run's own
    /// retry chain, and unrelated to any compiled-image or pipeline cache.
    pub capacity_warm_start: CapacityWarmStart,
    /// Ascending final-plan channel capacity levels after all targeted retries.
    pub channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
    pub rounds: u64,
    pub transitions: u64,
    /// Physical attempts present in launched graph replays, including deterministic no-op tails.
    pub encoded_attempts: u64,
    pub continuation_relaunches: u64,
    /// Replays of the pre-recorded graph DAG.
    pub graph_replays: u64,
    /// Explicit stream synchronizations completing device-to-host control readback,
    /// one per graph replay.
    pub wave_boundary_syncs: u64,
    pub mid_round_wave_boundary_syncs: u64,
    /// One-time graph capture and instantiation for this run.
    pub graph_capture_ns: u64,
    /// Host time spent submitting graph replays.
    pub host_submit_ns: u64,
    /// CUDA-event time covering graph execution.
    pub device_ns: u64,
    /// Wall time from graph capture through final wave completion.
    pub wall_ns: u64,
    pub memory_layout: CudaMemoryLayout,
}

/// Device-timestamp totals for the eight phases in every encoded CUDA graph attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CudaPhaseProfile {
    pub recorded_attempts: u64,
    pub horizon_ns: u64,
    pub prepare_ns: u64,
    pub drain_ns: u64,
    pub control_ns: u64,
    pub exchange_prefix_ns: u64,
    pub exchange_scatter_ns: u64,
    pub exchange_merge_ns: u64,
    pub finalize_ns: u64,
}

impl CudaPhaseProfile {
    pub fn total_kernel_ns(self) -> u64 {
        [
            self.horizon_ns,
            self.prepare_ns,
            self.drain_ns,
            self.control_ns,
            self.exchange_prefix_ns,
            self.exchange_scatter_ns,
            self.exchange_merge_ns,
            self.finalize_ns,
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add)
    }

    fn observe(&mut self, phase: usize, elapsed_ns: u64) {
        let total = match phase {
            0 => &mut self.horizon_ns,
            1 => &mut self.prepare_ns,
            2 => &mut self.drain_ns,
            3 => &mut self.control_ns,
            4 => &mut self.exchange_prefix_ns,
            5 => &mut self.exchange_scatter_ns,
            6 => &mut self.exchange_merge_ns,
            7 => &mut self.finalize_ns,
            _ => unreachable!("CUDA graph has exactly eight profiled phases"),
        };
        *total = total.saturating_add(elapsed_ns);
    }
}

/// Complete CUDA result paired with opt-in phase timestamps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CudaProfiledRun {
    pub run: CudaRun,
    pub profile: CudaPhaseProfile,
}

/// One-time CUDA context, stream, module, and kernel initialization costs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CudaInitializationTimings {
    pub context_stream_setup_ns: u64,
    pub module_function_load_ns: u64,
}

/// Runs the production CUDA executor through the inclusive scenario stop.
///
/// CUDA execution is serialized process-wide: one executor executes at a time, and concurrent
/// callers queue until execution and explicit readback complete. Executor construction may proceed
/// concurrently.
pub fn run_cuda(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CudaConfig,
) -> Result<CudaRun, CudaError> {
    run_cuda_with_observations(
        image,
        exclusive_horizon_ns,
        config,
        ObservationMode::Summary,
    )
}

/// Runs the production CUDA executor with explicit observation retention.
pub fn run_cuda_with_observations(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CudaConfig,
    observation_mode: ObservationMode,
) -> Result<CudaRun, CudaError> {
    CudaExecutor::new()?.run_with_observations(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
    )
}

/// Reusable CUDA context, stream, embedded module, kernels, and graph-capture substrate.
///
/// Each run allocates fresh explicit device buffers, so a device fault cannot contaminate a later
/// run. CUDA execution is serialized process-wide: one executor executes at a time, and concurrent
/// callers queue until execution and readback complete. Executor construction may proceed
/// concurrently.
pub struct CudaExecutor {
    direct: Arc<DirectCuda>,
}

impl CudaExecutor {
    /// Constructs an executor without acquiring the process-wide execution guard.
    pub fn new() -> Result<Self, CudaError> {
        let direct = CUDA_DIRECT.get_or_init(|| {
            DirectCuda::new()
                .map(Arc::new)
                .map_err(|error| error.to_string())
        });
        Ok(Self {
            direct: Arc::clone(
                direct
                    .as_ref()
                    .map_err(|error| CudaError::Unavailable(error.clone()))?,
            ),
        })
    }

    pub fn initialization_timings(&self) -> CudaInitializationTimings {
        self.direct.initialization_timings
    }

    pub fn run(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: CudaConfig,
    ) -> Result<CudaRun, CudaError> {
        self.run_with_observations(
            image,
            exclusive_horizon_ns,
            config,
            ObservationMode::Summary,
        )
    }

    /// Executes and reads back under the process-wide CUDA execution envelope.
    pub fn run_with_observations(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: CudaConfig,
        observation_mode: ObservationMode,
    ) -> Result<CudaRun, CudaError> {
        self.run_with_observations_warm_started(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
            &CapacityWarmStart::default(),
        )
    }

    /// Runs from an explicit starting capacity instead of from the derived one (T20l fix 3).
    ///
    /// `warm_start` is a [`CapacityWarmStart`] a previous successful run of the same image emitted
    /// on [`CudaRun::capacity_warm_start`]. Supplying it lets the first attempt plan the capacity
    /// the retry chain would have converged on, so a known fixture builds one plan, uploads once
    /// and reads back once instead of discarding three attempts. [`CapacityWarmStart::default()`]
    /// is exactly [`Self::run_with_observations`], which is what every existing caller keeps doing.
    ///
    /// The result cannot depend on the hint: capacity on this path is refuse-or-run, never
    /// semantics (the T20g invariant), so a warm start moves a run between "refuses once, then
    /// runs" and "runs immediately" and never between two answers. See [`CapacityWarmStart`] for
    /// the argument in full.
    pub fn run_with_observations_warm_started(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: CudaConfig,
        observation_mode: ObservationMode,
        warm_start: &CapacityWarmStart,
    ) -> Result<CudaRun, CudaError> {
        validate(image, Backend::Cuda).map_err(|error| CudaError::Validation(error.to_string()))?;
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
                let plan = CudaPlan::new_with_entity_capacity_floors(
                    image,
                    exclusive_horizon_ns,
                    attempt_config,
                    observation_mode,
                    &mut channel_capacity_floors,
                    &mut tcp_capacity_floors,
                )?;
                let _execution_guard = cuda_device_execution_guard();
                let buffers = CudaBuffers::new(&self.direct.stream, plan)?;
                let timing = self.direct.run(&buffers, attempt_config)?;
                #[cfg(feature = "cuda-test-hooks")]
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
                        CudaError::CapacityExceeded {
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
                        return Err(CudaError::CapacityExceeded {
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
                        CudaArena::TcpReceiverRanges => {
                            crate::device_capacity::grown_capacity_with_slack(
                                capacity,
                                demand,
                                crate::device_capacity::TCP_RECEIVER_RETRY_SLACK,
                            )
                        }
                        CudaArena::TcpSegmentLedger => {
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
                        return Err(CudaError::CapacityExceeded {
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
                        (CudaArena::ChannelInbox, None, Some(stream)) => {
                            channel_capacity_floors.raise(stream, capacity, grown_capacity)
                        }
                        (CudaArena::ChannelInbox, _, None) => {
                            return Err(CudaError::Validation(
                                "channel capacity fault omitted its stream identity".into(),
                            )
                            .with_retry_trace(&retry_trace));
                        }
                        (CudaArena::TcpReceiverRanges, Some(flow), None) => {
                            tcp_capacity_floors.raise_receiver(flow, capacity, grown_capacity)
                        }
                        (CudaArena::TcpSegmentLedger, Some(flow), None) => {
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
                        (CudaArena::TcpReceiverRanges | CudaArena::TcpSegmentLedger, None, _) => {
                            return Err(CudaError::Validation(
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
                        return Err(CudaError::Validation(
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

    /// Executes with device events bracketing every captured graph phase.
    pub fn run_profiled_with_observations(
        &self,
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: CudaConfig,
        observation_mode: ObservationMode,
    ) -> Result<CudaProfiledRun, CudaError> {
        validate(image, Backend::Cuda).map_err(|error| CudaError::Validation(error.to_string()))?;
        validate_config(config)?;

        let retry_budget = config.max_capacity_retries;
        let mut attempt_config = config;
        let mut channel_capacity_floors = crate::device_capacity::ChannelCapacityFloors::default();
        let mut tcp_capacity_floors = crate::device_capacity::TcpCapacityFloors::default();
        let mut retry_trace = Vec::new();
        loop {
            let attempt = (|| {
                let plan = CudaPlan::new_with_entity_capacity_floors(
                    image,
                    exclusive_horizon_ns,
                    attempt_config,
                    observation_mode,
                    &mut channel_capacity_floors,
                    &mut tcp_capacity_floors,
                )?;
                let _execution_guard = cuda_device_execution_guard();
                let buffers = CudaBuffers::new(&self.direct.stream, plan)?;
                let (timing, profile) = self.direct.run_profiled(&buffers, attempt_config)?;
                #[cfg(feature = "cuda-test-hooks")]
                panic_after_execution_if_requested();
                let run = buffers.finish(&self.direct, image, observation_mode, timing)?;
                Ok(CudaProfiledRun { run, profile })
            })();
            match attempt {
                Ok(mut profiled) => {
                    profiled.run.capacity_retry_trace = retry_trace;
                    profiled.run.capacity_warm_start = CapacityWarmStart {
                        floors: converged_capacity_floors(&attempt_config),
                        channel_events_by_stream: channel_capacity_floors.converged_capacities(),
                        tcp_receiver_ranges_by_base: tcp_capacity_floors
                            .converged_receiver_ranges(),
                        tcp_ledger_segments_by_flow: tcp_capacity_floors
                            .converged_ledger_segments(),
                    };
                    return Ok(profiled);
                }
                Err(failure) => {
                    let AttemptFailure {
                        error,
                        ledger_high_water,
                    } = failure;
                    let (arena, node, flow, stream, capacity, demand) = match error {
                        CudaError::CapacityExceeded {
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
                        return Err(CudaError::CapacityExceeded {
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
                        CudaArena::TcpReceiverRanges => {
                            crate::device_capacity::grown_capacity_with_slack(
                                capacity,
                                demand,
                                crate::device_capacity::TCP_RECEIVER_RETRY_SLACK,
                            )
                        }
                        CudaArena::TcpSegmentLedger => {
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
                        return Err(CudaError::CapacityExceeded {
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
                        (CudaArena::ChannelInbox, None, Some(stream)) => {
                            channel_capacity_floors.raise(stream, capacity, grown_capacity)
                        }
                        (CudaArena::ChannelInbox, _, None) => {
                            return Err(CudaError::Validation(
                                "channel capacity fault omitted its stream identity".into(),
                            )
                            .with_retry_trace(&retry_trace));
                        }
                        (CudaArena::TcpReceiverRanges, Some(flow), None) => {
                            tcp_capacity_floors.raise_receiver(flow, capacity, grown_capacity)
                        }
                        (CudaArena::TcpSegmentLedger, Some(flow), None) => {
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
                        (CudaArena::TcpReceiverRanges | CudaArena::TcpSegmentLedger, None, _) => {
                            return Err(CudaError::Validation(
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
                        return Err(CudaError::Validation(
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
pub fn assert_cuda_planner_bit_equal_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CudaConfig,
    observation_mode: ObservationMode,
) -> Result<(), CudaError> {
    validate(image, Backend::Cuda).map_err(|error| CudaError::Validation(error.to_string()))?;
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
        return Err(CudaError::Validation(
            "linear lookup tables differ from the legacy helpers".into(),
        ));
    }
    let precomputed = CudaPlan::new_with_capacity_mode(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
        PlannerCapacityMode::Precomputed,
    )?;
    let legacy = CudaPlan::new_with_capacity_mode(
        image,
        exclusive_horizon_ns,
        config,
        observation_mode,
        PlannerCapacityMode::Legacy,
    )?;
    if precomputed != legacy {
        return Err(CudaError::Validation(
            "linear-table planner differs from the legacy planner".into(),
        ));
    }
    Ok(())
}

/// Measures host plan construction only; CUDA initialization and execution are excluded.
#[cfg(feature = "cuda-test-hooks")]
#[doc(hidden)]
pub fn measure_cuda_planner_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CudaConfig,
    observation_mode: ObservationMode,
    legacy: bool,
) -> Result<u64, CudaError> {
    validate(image, Backend::Cuda).map_err(|error| CudaError::Validation(error.to_string()))?;
    validate_config(config)?;
    let mode = if legacy {
        PlannerCapacityMode::Legacy
    } else {
        PlannerCapacityMode::Precomputed
    };
    let started = Instant::now();
    let plan = CudaPlan::new_with_capacity_mode(
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

/// Returns the exact production-plan plane lengths without creating a CUDA device.
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub fn size_cuda_plan_for_testing(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CudaConfig,
    observation_mode: ObservationMode,
) -> Result<crate::DeviceSizingReport, CudaError> {
    validate(image, Backend::Cuda).map_err(|error| CudaError::Validation(error.to_string()))?;
    validate_config(config)?;
    let plan = CudaPlan::new(image, exclusive_horizon_ns, config, observation_mode)?;
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
    .map_err(|error| CudaError::Validation(error.to_string()))
}

fn validate_config(config: CudaConfig) -> Result<(), CudaError> {
    if config.attempts_per_graph_wave == 0 {
        return Err(CudaError::Validation(
            "attempts_per_graph_wave must be nonzero".into(),
        ));
    }
    if config.attempts_per_graph_wave > MAX_ATTEMPTS_PER_GRAPH_WAVE {
        return Err(CudaError::Validation(format!(
            "attempts_per_graph_wave {} exceeds the supported maximum {}",
            config.attempts_per_graph_wave, MAX_ATTEMPTS_PER_GRAPH_WAVE
        )));
    }
    if config.max_transitions_per_lp_per_round == 0 {
        return Err(CudaError::Validation(
            "max_transitions_per_lp_per_round must be nonzero".into(),
        ));
    }
    if config.round_threads_per_block == 0 {
        return Err(CudaError::Validation(
            "round_threads_per_block must be nonzero".into(),
        ));
    }
    #[cfg(feature = "cuda-test-hooks")]
    if config.fault_injection.is_some_and(|arena| {
        !matches!(
            arena,
            CudaArena::ServiceStream
                | CudaArena::GeneratorStream
                | CudaArena::Departures
                | CudaArena::Arrivals
        )
    }) {
        return Err(CudaError::Validation(
            "fault_injection supports only service/generator streams and departure/arrival logs"
                .into(),
        ));
    }
    Ok(())
}

fn injected_capacity(config: CudaConfig, arena: CudaArena, default: usize) -> usize {
    #[cfg(feature = "cuda-test-hooks")]
    if config.fault_injection == Some(arena) {
        return 0;
    }
    let _ = (config, arena);
    default
}

#[derive(Eq, PartialEq)]
struct CudaPlan {
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
    memory_layout: CudaMemoryLayout,
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
    memory_layout: CudaMemoryLayout,
    channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
}

#[derive(Clone, Copy, Debug, Default)]
struct TcpLayout {
    receiver_offset: usize,
    ledger_meta_offset: usize,
}

fn encode_control(control: TcpCongestionControl, words: &mut [u64]) {
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
    data_counts: &[usize],
    capacity_caps: DeviceCapacityCaps,
    capacity_floors: DeviceCapacityFloors,
    tcp_capacity_floors: &mut crate::device_capacity::TcpCapacityFloors,
) -> Result<(Vec<u64>, TcpLayout), CudaError> {
    let flow_count = image.flows.len().max(1);
    let receiver_offset = 0;
    let ledger_meta_offset = flow_count
        .checked_mul(TCP_RECEIVER_WORDS)
        .ok_or_else(|| CudaError::Validation("TCP receiver plane size overflows usize".into()))?;
    let mut next = ledger_meta_offset
        .checked_add(flow_count * TCP_LEDGER_META_WORDS)
        .ok_or_else(|| CudaError::Validation("TCP ledger metadata size overflows usize".into()))?;
    let mut state = vec![0_u64; next.max(1)];

    for (owner_slot, host) in image.host_states.iter().enumerate() {
        if host.tcp_receivers.is_empty() {
            continue;
        }
        let owner = capacity_context
            .host_lp(image, owner_slot)
            .map_or(NONE, |node| node.0);
        for receiver in &host.tcp_receivers {
            let flow = receiver.flow.0 as usize;
            let row = receiver_offset + flow * TCP_RECEIVER_WORDS;
            let base_capacity = crate::device_capacity::bound_derived_capacity(
                capacity_context
                    .tcp_receiver_range_bound(image, flow, data_counts[flow])
                    .max(receiver.out_of_order.len()),
                capacity_caps.tcp_receiver_ranges_per_flow,
                capacity_floors.tcp_receiver_ranges_per_flow,
                receiver.out_of_order.len(),
            );
            let capacity = tcp_capacity_floors
                .receiver(receiver.flow, base_capacity)
                .ok_or_else(|| {
                    CudaError::Validation(
                        "TCP receiver capacity class changed between retry attempts".into(),
                    )
                })?;
            let range_offset = next;
            next = next
                .checked_add(capacity.saturating_mul(2))
                .ok_or_else(|| {
                    CudaError::Validation("TCP receive-range arena overflows usize".into())
                })?;
            state.resize(next, 0);
            state[row] = 1;
            state[row + 1] = owner;
            state[row + 2] = receiver.ack_size_bytes;
            state[row + 3] = receiver.next_expected_sequence;
            state[row + 4] = range_offset as u64;
            state[row + 5] = capacity as u64;
            state[row + 6] = receiver.out_of_order.len() as u64;
            for (index, range) in receiver.out_of_order.iter().enumerate() {
                state[range_offset + index * 2] = range.start;
                state[range_offset + index * 2 + 1] = range.end;
            }
        }
    }

    let ledger = crate::tcp_ledger::seed_image(image).map_err(|conflict| {
        CudaError::Validation(format!(
            "TCP flow {:?} sequence {} changed segment size from {} to {} bytes",
            conflict.flow,
            conflict.sequence,
            conflict.original_size_bytes,
            conflict.replacement_size_bytes
        ))
    })?;
    for (flow, data_count) in data_counts.iter().copied().enumerate() {
        let packets = ledger
            .get(&crate::FlowId(flow as u64))
            .map_or_else(Vec::new, |segments| segments.values().copied().collect());
        let base_capacity = crate::device_capacity::bound_derived_capacity(
            capacity_context
                .tcp_ledger_segment_bound(image, flow, data_count)
                .max(packets.len()),
            capacity_caps.tcp_ledger_segments_per_flow,
            capacity_floors.tcp_ledger_segments_per_flow,
            packets.len(),
        );
        let capacity = tcp_capacity_floors
            .ledger(FlowId(flow as u64), base_capacity)
            .ok_or_else(|| {
                CudaError::Validation(
                    "TCP ledger capacity class changed between retry attempts".into(),
                )
            })?;
        let row = ledger_meta_offset + flow * TCP_LEDGER_META_WORDS;
        let record_offset = next;
        next = next
            .checked_add(capacity.saturating_mul(TCP_LEDGER_RECORD_WORDS))
            .ok_or_else(|| {
                CudaError::Validation("TCP segment-ledger arena overflows usize".into())
            })?;
        state.resize(next, 0);
        state[row] = record_offset as u64;
        state[row + 1] = capacity as u64;
        state[row + 2] = packets.len() as u64;
        state[row + 3] = 0;
        // Mutation site H1: a seeded ring starts unrotated, and its high-water starts at the
        // resident count so a flow that never inserts still reports its true peak.
        state[row + LEDGER_META_HEAD] = 0;
        state[row + LEDGER_META_HIGH_WATER] = packets.len() as u64;
        for (index, packet) in packets.into_iter().enumerate() {
            let PacketKind::TcpData(header) = packet.kind else {
                unreachable!("TCP ledgers contain only TCP data")
            };
            let record = record_offset + index * TCP_LEDGER_RECORD_WORDS;
            state[record] = packet.id.0;
            state[record + 1] = packet.size_bytes;
            state[record + 2] = header.sequence;
            state[record + 3] = header.sent_time_ns;
            state[record + 4] = u64::from(header.retransmission);
        }
    }

    Ok((
        state,
        TcpLayout {
            receiver_offset,
            ledger_meta_offset,
        },
    ))
}

impl CudaPlan {
    #[cfg(feature = "planner-test-hooks")]
    fn new(
        image: &SimulationImage,
        exclusive_horizon_ns: Option<u64>,
        config: CudaConfig,
        observation_mode: ObservationMode,
    ) -> Result<Self, CudaError> {
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
        config: CudaConfig,
        observation_mode: ObservationMode,
        channel_capacity_floors: &mut crate::device_capacity::ChannelCapacityFloors,
        tcp_capacity_floors: &mut crate::device_capacity::TcpCapacityFloors,
    ) -> Result<Self, CudaError> {
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
        config: CudaConfig,
        observation_mode: ObservationMode,
        capacity_mode: PlannerCapacityMode,
    ) -> Result<Self, CudaError> {
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
        config: CudaConfig,
        observation_mode: ObservationMode,
        capacity_mode: PlannerCapacityMode,
        channel_capacity_floors: &mut crate::device_capacity::ChannelCapacityFloors,
        tcp_capacity_floors: &mut crate::device_capacity::TcpCapacityFloors,
    ) -> Result<Self, CudaError> {
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
                // Timeout events are intentionally heap-class. Under the live-state contract
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
                    let feedback = flow_feedback_counts[flow];
                    let attempts = capacity_context.tcp_fallback_timer_bound(
                        image,
                        flow,
                        flow_packet_counts[flow].saturating_sub(feedback),
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
                                generators[offset + 12] = tcp.total_bytes;
                                generators[offset + 13] = tcp.mss_bytes;
                                generators[offset + 14] = tcp.ack_size_bytes;
                                generators[offset + 15] = tcp.next_sequence;
                                generators[offset + 16] = tcp.highest_ack;
                                generators[offset + 17] = tcp.bytes_in_flight;
                                generators[offset + 18] = tcp.duplicate_acks;
                                generators[offset + 19] = tcp.recovery_high_sequence;
                                generators[offset + 20] = tcp.last_attempt.0;
                                generators[offset + 21] = tcp.timer_generation;
                                if let Some(timer) = tcp.active_timer {
                                    generators[offset + 22] = 1;
                                    generators[offset + 23] = timer.attempt.0;
                                    generators[offset + 24] = timer.sequence;
                                    generators[offset + 25] = timer.deadline_ns;
                                    generators[offset + 26] = timer.generation;
                                    generators[offset + 27] = timer.rto_ns;
                                }
                                generators[offset + 28] = tcp.srtt_ns;
                                generators[offset + 29] = tcp.rtt_var_ns;
                                generators[offset + 30] = tcp.rto_ns;
                                encode_control(
                                    tcp.control,
                                    &mut generators[offset + 31..offset + 43],
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
                            FlowGeneratorKind::Collective(_) | FlowGeneratorKind::Dcqcn(_) => {
                                unreachable!("CUDA capability validation rejects this generator")
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
            prepare_device_schedulers(image, &queue_meta).map_err(CudaError::Validation)?;

        for event in &image.initial_events {
            let packet = if event.kind == EventKind::RetransmissionTimeout {
                timer_packet_for(image, *event)?
            } else {
                packet_for(&initial_by_payload, event.payload)?
            };
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
        let departure_capacity =
            injected_capacity(config, CudaArena::Departures, observation_capacity);
        let arrival_capacity = injected_capacity(config, CudaArena::Arrivals, observation_capacity);
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
        if departure_capacity != observation_capacity {
            for node in 0..node_count {
                observation_meta[node * OBSERVATION_META_WORDS + ARENA_META_WORDS + 1] =
                    departure_capacity as u64;
            }
        }
        if arrival_capacity != observation_capacity {
            for node in 0..node_count {
                observation_meta[node * OBSERVATION_META_WORDS + 2 * ARENA_META_WORDS + 1] =
                    arrival_capacity as u64;
            }
        }
        let (mut tcp_state, tcp_layout) = prepare_tcp_state(
            image,
            &capacity_context,
            &flow_data_counts,
            config.capacity_caps,
            config.capacity_floors,
            tcp_capacity_floors,
        )?;
        publish_live_timer_slots(
            &fel_meta,
            &fel_records,
            node_count,
            tcp_layout.ledger_meta_offset,
            &mut tcp_state,
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
        let mut control = vec![0_u64; CONTROL_WORDS];
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
            departure_capacity as u64,
            arrival_capacity as u64,
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
            tcp_layout.receiver_offset as u64,
            tcp_layout.ledger_meta_offset as u64,
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
            tcp_state,
            stream_layout: streams.layout,
            memory_layout: streams.memory_layout,
            channel_stream_capacity_distribution: streams.channel_stream_capacity_distribution,
            orphan_packets,
            round_capacity,
            dispatch_capacity,
        })
    }
}

fn flow_packet_counts(image: &SimulationImage) -> Result<(Vec<usize>, Vec<usize>), CudaError> {
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
                    .map_err(|error| CudaError::Validation(error.to_string()))?;
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
                            CudaError::Validation("generator duration endpoint overflows".into())
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
            unreachable!("CUDA capability validation rejects PFC payloads")
        }
        PacketKind::DcqcnCnp(_) => (flow.reverse_route.as_slice(), flow.source),
        PacketKind::DcqcnControlTimer => {
            unreachable!("CUDA capability validation rejects DCQCN timer payloads")
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
        .expect("CUDA validation established a positive finite serialization interval")
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
    config: CudaConfig,
    legacy_fel_caps: &[usize],
    fallback_fel_caps: &[usize],
    fel_meta: &[u64],
    fel_records: &[u64],
    remote_staging_slots: usize,
    channel_capacity_floors: &mut crate::device_capacity::ChannelCapacityFloors,
) -> Result<PreparedStreams, CudaError> {
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
            memory_layout: CudaMemoryLayout {
                streams_enabled: false,
                legacy_heap_event_slots,
                fallback_heap_event_slots,
                checkpoint_fallback_events: image.initial_events.len(),
                heap_arena_bytes,
                legacy_heap_arena_bytes,
                ..CudaMemoryLayout::default()
            },
            channel_stream_capacity_distribution: Vec::new(),
        });
    }

    let node_count = image.nodes.len();
    let channel_count = image.channels.len();
    let service_stream_base = channel_count;
    let generator_stream_base = service_stream_base
        .checked_add(node_count)
        .ok_or_else(|| CudaError::Validation("service stream count overflows usize".into()))?;
    let stream_count = generator_stream_base
        .checked_add(image.flows.len())
        .ok_or_else(|| CudaError::Validation("generator stream count overflows usize".into()))?;
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
                CudaError::Validation(format!(
                    "channel stream {stream} starting capacity changed between retry attempts"
                ))
            })?;
    }
    let channel_stream_capacity_distribution =
        crate::device_capacity::channel_capacity_distribution(&channel_caps);
    let service_capacity = injected_capacity(
        config,
        CudaArena::ServiceStream,
        2_usize.max(config.capacity_floors.service_events_per_stream),
    );
    let generator_capacity = injected_capacity(
        config,
        CudaArena::GeneratorStream,
        2_usize.max(config.capacity_floors.generator_events_per_stream),
    );
    let service_caps = vec![service_capacity; node_count];
    let generator_caps = vec![generator_capacity; image.flows.len()];
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
                    CudaError::Validation("generator stream identifier overflows usize".into())
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
            return Err(CudaError::Validation(
                "stream classification requires one channel per source-target LP pair".into(),
            ));
        }
    }

    let lp_stream_id_words = lp_streams
        .iter()
        .try_fold(0_usize, |total, streams| total.checked_add(streams.len()))
        .ok_or_else(|| CudaError::Validation("LP stream-list size overflows usize".into()))?;
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
        .ok_or_else(|| CudaError::Validation("active stream-list size overflows usize".into()))?;
    let outbound_entry_count = outbound
        .iter()
        .try_fold(0_usize, |total, entries| total.checked_add(entries.len()))
        .ok_or_else(|| {
            CudaError::Validation("outbound channel-list size overflows usize".into())
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
        .ok_or_else(|| CudaError::Validation("stream arena byte size overflows usize".into()))?;

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
        memory_layout: CudaMemoryLayout {
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
) -> Result<Vec<usize>, CudaError> {
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
                    CudaError::Validation(format!(
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

fn checked_sum_usize(values: &[usize], label: &str) -> Result<usize, CudaError> {
    values.iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| CudaError::Validation(format!("{label} overflow usize")))
    })
}

fn event_arena_bytes(record_slots: usize, meta_words: usize) -> Result<usize, CudaError> {
    record_slots
        .checked_mul(EVENT_WORDS)
        .and_then(|words| words.checked_add(meta_words))
        .and_then(|words| words.checked_mul(std::mem::size_of::<u64>()))
        .ok_or_else(|| CudaError::Validation("event arena byte size overflows usize".into()))
}

fn take_words(next: &mut usize, records: usize, words: usize) -> Result<usize, CudaError> {
    let start = *next;
    *next = (*next)
        .checked_add(
            records
                .checked_mul(words)
                .ok_or_else(|| CudaError::Validation("stream state size overflows usize".into()))?,
        )
        .ok_or_else(|| CudaError::Validation("stream state size overflows usize".into()))?;
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

fn derived_transition_bound(image: &SimulationImage, counts: &[usize]) -> Result<usize, CudaError> {
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
                .map_err(|error| CudaError::Validation(error.to_string()))?;
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

fn assign_arena_offsets(meta: &mut [u64], capacities: &[usize]) -> Result<usize, CudaError> {
    let mut offset = 0_usize;
    for (slot, capacity) in capacities.iter().copied().enumerate() {
        let base = slot * ARENA_META_WORDS;
        meta[base] = offset as u64;
        meta[base + 1] = capacity as u64;
        offset = offset
            .checked_add(capacity)
            .ok_or_else(|| CudaError::Validation("device arena size overflows usize".into()))?;
    }
    Ok(offset)
}

fn assign_queue_offsets(meta: &mut [u64], capacities: &[usize]) -> Result<usize, CudaError> {
    let mut offset = 0_usize;
    for (slot, capacity) in capacities.iter().copied().enumerate() {
        let base = slot * QUEUE_META_WORDS;
        meta[base] = offset as u64;
        meta[base + 1] = capacity as u64;
        offset = offset.checked_add(capacity).ok_or_else(|| {
            CudaError::Validation("device queue arena size overflows usize".into())
        })?;
    }
    Ok(offset)
}

fn assign_observation_offsets(meta: &mut [u64], capacities: &[usize]) -> Result<usize, CudaError> {
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
            .ok_or_else(|| CudaError::Validation("observation arena size overflows".into()))?;
    }
    Ok(offset)
}

fn zero_words(records: usize, words: usize) -> Result<Vec<u64>, CudaError> {
    let length = records
        .checked_mul(words)
        .ok_or_else(|| CudaError::Validation("device buffer size overflows usize".into()))?;
    Ok(vec![0; length.max(1)])
}

fn packet_for(
    packets: &BTreeMap<PayloadId, PacketDescriptor>,
    payload: PayloadId,
) -> Result<PacketDescriptor, CudaError> {
    packets.get(&payload).copied().ok_or_else(|| {
        CudaError::Validation(format!("payload {payload:?} has no initial descriptor"))
    })
}

fn timer_packet_for(image: &SimulationImage, event: Event) -> Result<PacketDescriptor, CudaError> {
    let node = image
        .nodes
        .get(event.target.0 as usize)
        .filter(|node| node.id == event.target && node.kind == NodeKind::Host)
        .ok_or_else(|| {
            CudaError::Validation(format!("TCP timeout {:?} targets no host state", event.key))
        })?;
    let mut matches = image.host_states[node.state_slot as usize]
        .generators
        .iter()
        .filter_map(|generator| {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                return None;
            };
            tcp.active_timer
                .filter(|timer| {
                    timer.attempt == event.payload && timer.deadline_ns == event.key.time_ns
                })
                .map(|timer| (generator.flow, timer))
        });
    let owner = matches.next();
    if matches.next().is_some() {
        return Err(CudaError::Validation(format!(
            "TCP timeout {:?} payload {:?} has multiple owning active timers",
            event.key, event.payload
        )));
    }
    let (flow, sequence) = owner
        .map(|(flow, timer)| (flow, timer.sequence))
        .unwrap_or((FlowId(NONE), 0));
    Ok(PacketDescriptor {
        id: event.payload,
        flow,
        size_bytes: 0,
        ecn_marked: false,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence,
            sent_time_ns: event.key.time_ns,
            retransmission: true,
        }),
    })
}

fn packet_record(packet: PacketDescriptor) -> [u64; EVENT_WORDS] {
    let mut record = [0_u64; EVENT_WORDS];
    record[6] = packet.id.0;
    record[7] = packet.id.0;
    record[8] = packet.flow.0;
    record[9] = packet.size_bytes;
    record[10] = encode_packet_kind_word(packet);
    record[11..14].copy_from_slice(&packet_metadata(packet.kind));
    record
}

fn event_record(event: Event, packet: PacketDescriptor) -> [u64; EVENT_WORDS] {
    let metadata = packet_metadata(packet.kind);
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
        encode_packet_kind_word(packet),
        metadata[0],
        metadata[1],
        metadata[2],
    ]
}

fn encode_packet_kind_word(packet: PacketDescriptor) -> u64 {
    u64::from(packet.kind.code())
        | if packet.ecn_marked {
            PACKET_ECN_FLAG
        } else {
            0
        }
}

fn packet_metadata(kind: PacketKind) -> [u64; 3] {
    match kind {
        PacketKind::Data | PacketKind::Feedback => [0; 3],
        PacketKind::TcpData(header) => [
            header.sequence,
            header.sent_time_ns,
            u64::from(header.retransmission),
        ],
        PacketKind::TcpAck(header) => [
            header.acknowledgment,
            header.acknowledged_bytes,
            header.echoed_sent_time_ns,
        ],
        PacketKind::Pfc(header) => [
            header.controlled_link.0,
            u64::from(header.priority),
            u64::from(header.pause),
        ],
        PacketKind::DcqcnCnp(header) => [header.trigger_payload.0, 0, 0],
        PacketKind::DcqcnControlTimer => [0; 3],
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
) -> Result<(), CudaError> {
    let base = lp * ARENA_META_WORDS;
    let offset = meta[base] as usize;
    let capacity = meta[base + 1] as usize;
    let mut count = meta[base + 3] as usize;
    if count == capacity {
        return Err(CudaError::CapacityExceeded {
            arena: CudaArena::Fel,
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
) -> Result<(), CudaError> {
    let base = lp * QUEUE_META_WORDS;
    let offset = meta[base] as usize;
    let capacity = meta[base + 1] as usize;
    let head = meta[base + 2] as usize;
    let count = meta[base + 3] as usize;
    if count == capacity {
        return Err(CudaError::CapacityExceeded {
            arena: CudaArena::Queue,
            node: Some(NodeId(lp as u64)),
            flow: None,
            stream: None,
            capacity,
            demand: count.saturating_add(1),
        });
    }
    if track_bytes {
        meta[base + 4] = meta[base + 4].checked_add(record[9]).ok_or_else(|| {
            CudaError::Validation(format!(
                "CUDA queue byte total overflows u64 at LP {:?}",
                NodeId(lp as u64)
            ))
        })?;
    }
    let physical = (head + count) % capacity.max(1);
    write_record(storage, offset + physical, record);
    meta[base + 3] = (count + 1) as u64;
    Ok(())
}
struct CudaBuffers {
    planes: Vec<CudaSlice<u64>>,
    orphan_packets: Vec<PacketDescriptor>,
    node_count: usize,
    stream_layout: StreamLayout,
    memory_layout: CudaMemoryLayout,
    channel_stream_capacity_distribution: Vec<crate::ChannelStreamCapacityLevel>,
    round_capacity: usize,
    dispatch_capacity: usize,
}

impl CudaBuffers {
    fn new(stream: &std::sync::Arc<CudaStream>, plan: CudaPlan) -> Result<Self, CudaError> {
        let round_capacity = plan.round_capacity;
        let dispatch_capacity = plan.dispatch_capacity;
        let orphan_packets = plan.orphan_packets;
        let stream_layout = plan.stream_layout;
        let memory_layout = plan.memory_layout;
        let channel_stream_capacity_distribution = plan.channel_stream_capacity_distribution;
        let node_count = plan.params[0] as usize;
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
            plan.tcp_state,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, words)| {
            let words = if words.is_empty() { vec![0] } else { words };
            stream
                .clone_htod(&words)
                .map_err(|error| driver_error(format!("result plane {index} upload"), error))
        })
        .collect::<Result<Vec<_>, _>>()?;
        #[cfg(feature = "cuda-test-hooks")]
        record_plane_words(planes.iter().fold(0_u64, |total, plane| {
            total.saturating_add(plane.len() as u64)
        }));
        Ok(Self {
            planes,
            orphan_packets,
            node_count,
            stream_layout,
            memory_layout,
            channel_stream_capacity_distribution,
            round_capacity,
            dispatch_capacity,
        })
    }

    fn finish(
        &self,
        direct: &DirectCuda,
        image: &SimulationImage,
        observation_mode: ObservationMode,
        timing: CudaTiming,
    ) -> Result<CudaRun, AttemptFailure> {
        let stream = &direct.stream;
        // T20l fix 1: screen the attempt BEFORE the result arena crosses the bus.
        //
        // The control plane is a few dozen words; the result arena is 18.6-19.2 GB at the RQ9
        // frontier. T20l phase 1 §4.3 measured every one of the four attempts paying that copy in
        // full, three of which are discarded unread, at 5.08 s/attempt on an RTX 4090. A capacity
        // fault needs the control plane and — for a ledger fault only — the per-flow occupancy
        // metadata carrying the T20i vector, so those are fetched here and everything else is
        // fetched only once the attempt is known to have succeeded.
        //
        // This changes WHEN bytes cross the bus and nothing else. On the success path the same
        // planes are read, in the same order, into the same `planes` vector; on the failure path
        // the same `AttemptFailure` is produced from the same words. Complete state, retry traces
        // and fault payloads are byte-identical either way.
        let control = screen_plane_words(stream, self.planes[0].slice(..), "control plane screen")?;
        if control[CONTROL_ERROR] != 0 {
            let error = decode_device_error(&control);
            // T20i layer 2: a ledger fault reports the WHOLE per-flow occupancy vector, not just
            // the first offender. Layer 1 measured 377 of 262,144 frontier flows above the derived
            // floor, so first-offender keying would need up to 377 sequential replans; one vector
            // sizes every flow in a single replan. The payload is one pass over 6 words per flow,
            // and under the screen those 6 words per flow are also all that is copied back.
            let ledger_high_water = matches!(
                error,
                CudaError::CapacityExceeded {
                    arena: CudaArena::TcpSegmentLedger,
                    ..
                }
            )
            .then(|| {
                let params =
                    screen_plane_words(stream, self.planes[1].slice(..), "params plane screen")?;
                let ledger = &self.planes[28];
                let start = (params[PARAM_LEDGER_META_OFFSET] as usize).min(ledger.len());
                let end = start
                    .saturating_add(image.flows.len().saturating_mul(TCP_LEDGER_META_WORDS))
                    .min(ledger.len());
                let meta = screen_plane_words(
                    stream,
                    ledger.slice(start..end),
                    "ledger occupancy screen",
                )?;
                Ok::<_, CudaError>(ledger_high_water_vector(&meta, 0, image.flows.len()))
            })
            .transpose()?;
            return Err(AttemptFailure {
                error,
                ledger_high_water,
            });
        }
        if control[CONTROL_DONE] == 0 {
            return Err(CudaError::RoundLimitExceeded {
                capacity: self.round_capacity,
            }
            .into());
        }

        // T20l fix 2: on a SUCCESSFUL attempt, copy the LIVE regions rather than the arena.
        //
        // Transliteration of `MetalBuffers::finish`; see that function's comment for the three
        // plane classes (elided, read whole, compacted) and their justification. At the RQ9
        // frontier the elided planes alone are 691,104,103 of 2,394,309,899 words, of which
        // `remote_staging` is 657,667,584.
        let read_plane = |index: usize| -> Result<Vec<u64>, AttemptFailure> {
            let plane = &self.planes[index];
            #[cfg(feature = "cuda-test-hooks")]
            account_readback_words(plane.len());
            let words = stream.clone_dtoh(plane).map_err(|error| {
                AttemptFailure::from(driver_error(
                    format!("result plane {index} readback"),
                    error,
                ))
            })?;
            stream.synchronize().map_err(|error| {
                AttemptFailure::from(driver_error(
                    format!("result plane {index} readback synchronization"),
                    error,
                ))
            })?;
            Ok(words)
        };
        let params = read_plane(1)?;
        let node_state = read_plane(2)?;
        let generators = read_plane(3)?;
        let fel_meta = read_plane(7)?;
        let queue_meta = read_plane(9)?;
        let in_service = read_plane(11)?;
        let summary_words = read_plane(14)?;
        let lp_state = read_plane(18)?;
        let observation_meta = read_plane(21)?;
        let scheduler_state = read_plane(27)?;

        let node_count = image.nodes.len();
        let flow_count = image.flows.len();
        let tcp_plane = &self.planes[28];
        let receiver_base = params[PARAM_RECEIVER_OFFSET] as usize;
        let ledger_meta_offset = params[PARAM_LEDGER_META_OFFSET] as usize;
        let receiver_state = bounded_plane_words(
            stream,
            tcp_plane,
            receiver_base,
            flow_count.saturating_mul(TCP_RECEIVER_WORDS),
            "TCP receiver state readback",
        )?;
        let ledger_meta = bounded_plane_words(
            stream,
            tcp_plane,
            ledger_meta_offset,
            flow_count.saturating_mul(TCP_LEDGER_META_WORDS),
            "TCP ledger metadata readback",
        )?;
        // Only the per-stream ring metadata prefix of `stream_state` is decoded; the LP stream
        // lists, outbound metadata, channel batches, staging channels and channel targets that
        // follow it are device scratch. At the frontier that prefix is 1,667,976 of the plane's
        // 52,426,754 words. The extent is the planner's own `stream_count` — the same bound the
        // decode loop below already walks — not an inferred one.
        let stream_state = bounded_plane_words(
            stream,
            &self.planes[25],
            0,
            self.stream_layout
                .stream_count
                .saturating_mul(ARENA_META_WORDS),
            "stream ring metadata readback",
        )?;

        let fel_plan = arena_compaction_plan(
            "FEL record",
            CompactionShape::Linear,
            EVENT_WORDS,
            self.planes[8].len(),
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
            self.planes[10].len(),
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
            self.planes[26].len(),
            self.stream_layout.stream_count,
            |index| {
                let base = index * ARENA_META_WORDS;
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
            tcp_plane.len(),
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
            tcp_plane.len(),
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
                self.planes[plane].len(),
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
            (tcp_plane, &ledger_plan),
            (tcp_plane, &receiver_range_plan),
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
                    "CUDA switch {:?} queue byte counter diverged from its contents",
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
                        match &mut generator.kind {
                            FlowGeneratorKind::Constant(_) => {}
                            FlowGeneratorKind::Tcp(tcp) => {
                                tcp.total_bytes = generators[offset + 12];
                                tcp.mss_bytes = generators[offset + 13];
                                tcp.ack_size_bytes = generators[offset + 14];
                                tcp.next_sequence = generators[offset + 15];
                                tcp.highest_ack = generators[offset + 16];
                                tcp.bytes_in_flight = generators[offset + 17];
                                tcp.duplicate_acks = generators[offset + 18];
                                tcp.recovery_high_sequence = generators[offset + 19];
                                tcp.last_attempt = PayloadId(generators[offset + 20]);
                                tcp.timer_generation = generators[offset + 21];
                                tcp.active_timer =
                                    (generators[offset + 22] != 0).then(|| TcpTimerState {
                                        attempt: PayloadId(generators[offset + 23]),
                                        sequence: generators[offset + 24],
                                        deadline_ns: generators[offset + 25],
                                        generation: generators[offset + 26],
                                        rto_ns: generators[offset + 27],
                                    });
                                tcp.srtt_ns = generators[offset + 28];
                                tcp.rtt_var_ns = generators[offset + 29];
                                tcp.rto_ns = generators[offset + 30];
                                tcp.control =
                                    decode_control(&generators[offset + 31..offset + 43])?;
                            }
                            FlowGeneratorKind::Rate(rate) => {
                                rate.first_pacing_time_ns = generators[offset + 12];
                                rate.pacing_interval_ns = generators[offset + 13];
                                rate.packet_size_bytes = generators[offset + 14];
                                rate.total_bytes = generators[offset + 15];
                                rate.rate_numerator_bits_per_second = generators[offset + 16];
                                rate.rate_denominator = generators[offset + 17];
                                rate.credit_quanta = u128::from(generators[offset + 18])
                                    | (u128::from(generators[offset + 19]) << 64);
                            }
                            FlowGeneratorKind::Collective(_) | FlowGeneratorKind::Dcqcn(_) => {
                                return Err(CudaError::DeviceExecution {
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
                        let row = flow * TCP_RECEIVER_WORDS;
                        receiver.ack_size_bytes = receiver_state[row + 2];
                        receiver.next_expected_sequence = receiver_state[row + 3];
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
                            .map_err(CudaError::Validation)?;
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
                if event.kind != EventKind::RetransmissionTimeout {
                    resident.insert(packet.id, packet);
                }
                pending_events.push(event);
            }
        }
        for index_stream in 0..self.stream_layout.stream_count {
            let base = stream_plan.destination(index_stream);
            for index in 0..stream_plan.count(index_stream) {
                let record = read_record(&stream_records, base + index);
                let (event, packet) = decode_event(record)?;
                if event.kind != EventKind::RetransmissionTimeout {
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

        Ok(CudaRun {
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
            graph_replays: timing.graph_replays,
            graph_capture_ns: timing.graph_capture_ns,
            host_submit_ns: timing.host_submit_ns,
            device_ns: timing.device_ns,
            wall_ns: timing.wall_ns,
            memory_layout: self.memory_layout,
        })
    }
}

const KERNEL_NAMES: [&str; 8] = [
    "days_horizon",
    "days_round_prepare",
    "days_round",
    "days_round_control",
    "days_exchange_prefix",
    "days_exchange_scatter",
    "days_exchange_merge",
    "days_round_finalize",
];
/// T20l fix 2's readback gather. Deliberately outside [`KERNEL_NAMES`]: it is not part of the
/// captured attempt DAG, does not take the uniform 29-plane ABI, and is launched only after an
/// attempt has been screened as successful.
const COMPACT_KERNEL_NAME: &str = "days_compact_gather";
// T20l fix 2: one thread per entity, and an entity's work is bounded by its own live count. 256
// keeps the launch a whole number of warps without reserving the maximum block footprint; the
// gather's output does not depend on it.
const COMPACT_THREADS_PER_BLOCK: usize = 256;
const CONTROL_KERNELS: [usize; 5] = [0, 1, 3, 4, 7];
const PARALLEL_KERNELS: [usize; 3] = [2, 5, 6];

struct DirectCuda {
    _context: Arc<CudaContext>,
    stream: Arc<CudaStream>,
    functions: Vec<CudaFunction>,
    /// T20l fix 2: the readback gather, loaded beside the eight attempt kernels.
    compact_function: CudaFunction,
    initialization_timings: CudaInitializationTimings,
}

impl DirectCuda {
    fn new() -> Result<Self, CudaError> {
        let setup_started = Instant::now();
        let context =
            CudaContext::new(0).map_err(|error| driver_error("device 0 context", error))?;
        let stream = context
            .new_stream()
            .map_err(|error| driver_error("non-blocking execution stream", error))?;
        // This backend owns one stream, retains every buffer until that stream is synchronized,
        // and serializes all executions process-wide. Automatic cross-stream event tracking would
        // add event nodes to graph capture without providing any safety here.
        unsafe {
            context.disable_event_tracking();
        }
        let context_stream_setup_ns = duration_ns(setup_started.elapsed());

        let module_started = Instant::now();
        let fatbin = include_bytes!(concat!(env!("OUT_DIR"), "/days_cuda_kernels.fatbin"));
        let module = context
            .load_module(Ptx::from_binary(fatbin.to_vec()))
            .map_err(|error| driver_error("embedded sm_121/sm_89 fatbin load", error))?;
        let functions = KERNEL_NAMES
            .iter()
            .map(|name| {
                module
                    .load_function(name)
                    .map_err(|error| driver_error(format!("kernel `{name}` load"), error))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let compact_function = module
            .load_function(COMPACT_KERNEL_NAME)
            .map_err(|error| driver_error(format!("kernel `{COMPACT_KERNEL_NAME}` load"), error))?;
        let module_function_load_ns = duration_ns(module_started.elapsed());

        let (major, minor) = context
            .compute_capability()
            .map_err(|error| driver_error("compute capability query", error))?;
        if (major, minor) != (12, 1) && (major, minor) != (8, 9) {
            return Err(CudaError::Unavailable(format!(
                "embedded kernels target sm_121 and sm_89, but device 0 reports sm_{major}{minor}"
            )));
        }
        for index in CONTROL_KERNELS {
            let supported = functions[index].max_threads_per_block().map_err(|error| {
                driver_error(
                    format!("kernel `{}` thread limit query", KERNEL_NAMES[index]),
                    error,
                )
            })? as usize;
            if supported < LANES {
                return Err(CudaError::Unavailable(format!(
                    "kernel `{}` supports only {supported} threads per block; {LANES} are required \
                     by its deterministic reduction",
                    KERNEL_NAMES[index]
                )));
            }
        }

        Ok(Self {
            _context: context,
            stream,
            functions,
            compact_function,
            initialization_timings: CudaInitializationTimings {
                context_stream_setup_ns,
                module_function_load_ns,
            },
        })
    }

    /// T20l fix 2: gathers each arena's live records into a dense buffer and reads that back.
    ///
    /// One launch per arena on the owning stream, then one synchronization for all of them. Every
    /// extent comes from [`CompactionPlan`], i.e. from device-written meta words; an arena whose
    /// device-written counts sum to zero is neither launched nor read.
    ///
    /// Returns one gathered word vector per request, in request order.
    fn compact(
        &self,
        requests: &[(&CudaSlice<u64>, &CompactionPlan)],
    ) -> Result<Vec<Vec<u64>>, CudaError> {
        let mut destinations = Vec::with_capacity(requests.len());
        // The plan and argument buffers must outlive the launches, so they are retained here
        // rather than dropped at the end of each iteration.
        let mut retained = Vec::with_capacity(requests.len());
        for (source, plan) in requests {
            if plan.total_records() == 0 || plan.entity_count() == 0 {
                destinations.push(None);
                continue;
            }
            debug_assert!(plan.total_words() <= source.len());
            let destination =
                self.stream
                    .alloc_zeros::<u64>(plan.total_words())
                    .map_err(|error| {
                        driver_error(format!("{} compaction destination", plan.arena), error)
                    })?;
            let plan_buffer = self
                .stream
                .clone_htod(&plan.plan_words())
                .map_err(|error| driver_error(format!("{} compaction plan", plan.arena), error))?;
            let argument_buffer =
                self.stream
                    .clone_htod(&plan.argument_words())
                    .map_err(|error| {
                        driver_error(format!("{} compaction arguments", plan.arena), error)
                    })?;
            let config = LaunchConfig {
                grid_dim: (
                    u32::try_from(
                        plan.entity_count()
                            .div_ceil(COMPACT_THREADS_PER_BLOCK)
                            .max(1),
                    )
                    .map_err(|_| {
                        CudaError::Validation(format!(
                            "{} compaction needs more blocks than a CUDA grid allows",
                            plan.arena
                        ))
                    })?,
                    1,
                    1,
                ),
                block_dim: (COMPACT_THREADS_PER_BLOCK as u32, 1, 1),
                shared_mem_bytes: 0,
            };
            let mut arguments = self.stream.launch_builder(&self.compact_function);
            arguments.arg(&destination);
            arguments.arg(*source);
            arguments.arg(&plan_buffer);
            arguments.arg(&argument_buffer);
            unsafe { arguments.launch(config) }.map_err(|error| {
                driver_error(format!("{} compaction launch", plan.arena), error)
            })?;
            destinations.push(Some(destination));
            retained.push((plan_buffer, argument_buffer));
        }
        if destinations.iter().all(Option::is_none) {
            return Ok(requests.iter().map(|_| Vec::new()).collect());
        }

        let mut gathered = Vec::with_capacity(requests.len());
        let mut readback_error = None;
        for destination in &destinations {
            match destination {
                None => gathered.push(Vec::new()),
                Some(destination) => {
                    #[cfg(feature = "cuda-test-hooks")]
                    account_readback_words(destination.len());
                    match self.stream.clone_dtoh(destination) {
                        Ok(words) => gathered.push(words),
                        Err(error) => {
                            readback_error = Some(driver_error("compacted arena readback", error));
                            break;
                        }
                    }
                }
            }
        }
        self.stream
            .synchronize()
            .map_err(|error| driver_error("compaction synchronization", error))?;
        drop(retained);
        if let Some(error) = readback_error {
            return Err(error);
        }
        Ok(gathered)
    }

    fn run(&self, buffers: &CudaBuffers, config: CudaConfig) -> Result<CudaTiming, CudaError> {
        let parallel_threads = config.round_threads_per_block;
        let supported_parallel_threads = PARALLEL_KERNELS
            .iter()
            .map(|index| {
                self.functions[*index]
                    .max_threads_per_block()
                    .map(|value| value as usize)
                    .map_err(|error| {
                        driver_error(
                            format!("kernel `{}` thread limit query", KERNEL_NAMES[*index]),
                            error,
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .unwrap_or(0);
        if parallel_threads > supported_parallel_threads {
            return Err(CudaError::Validation(format!(
                "round_threads_per_block {parallel_threads} exceeds the supported maximum \
                 {supported_parallel_threads}"
            )));
        }

        let parallel_blocks = buffers.node_count.div_ceil(parallel_threads).max(1);
        let parallel_blocks = u32::try_from(parallel_blocks).map_err(|_| {
            CudaError::Validation("parallel CUDA grid does not fit in u32 blocks".into())
        })?;
        let parallel_threads = u32::try_from(parallel_threads).map_err(|_| {
            CudaError::Validation("round_threads_per_block does not fit in u32".into())
        })?;
        let control = LaunchConfig {
            grid_dim: (1, 1, 1),
            block_dim: (LANES as u32, 1, 1),
            shared_mem_bytes: 0,
        };
        let parallel = LaunchConfig {
            grid_dim: (parallel_blocks, 1, 1),
            block_dim: (parallel_threads, 1, 1),
            shared_mem_bytes: 0,
        };
        let configs = [
            control, control, parallel, control, control, parallel, parallel, control,
        ];

        let wall_started = Instant::now();
        let capture_started = Instant::now();
        let graph = self.capture_graph(buffers, &configs, config.attempts_per_graph_wave)?;
        let graph_capture_ns = duration_ns(capture_started.elapsed());

        let maximum_replays = buffers
            .dispatch_capacity
            .div_ceil(config.attempts_per_graph_wave)
            .max(1);
        let mut graph_replays = 0_u64;
        let mut wave_boundary_syncs = 0_u64;
        let mut mid_round_wave_boundary_syncs = 0_u64;
        let mut host_submit_ns = 0_u64;
        let mut device_ns = 0_u64;
        let mut encoded_attempts = 0_u64;

        for _ in 0..maximum_replays {
            let start = self
                .stream
                .record_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))
                .map_err(|error| driver_error("graph-wave start event", error))?;
            let submitted = Instant::now();
            graph
                .launch()
                .map_err(|error| driver_error("CUDA Graph replay", error))?;
            host_submit_ns = host_submit_ns.saturating_add(duration_ns(submitted.elapsed()));
            let end = self
                .stream
                .record_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))
                .map_err(|error| driver_error("graph-wave end event", error))?;
            let control = self
                .stream
                .clone_dtoh(&buffers.planes[0])
                .map_err(|error| driver_error("graph-wave control readback", error))?;
            // This is the explicit boundary synchronization counted below.
            self.stream.synchronize().map_err(|error| {
                driver_error("graph-wave control readback synchronization", error)
            })?;
            let elapsed_ms = start
                .elapsed_ms(&end)
                .map_err(|error| driver_error("graph-wave elapsed time", error))?;
            device_ns = device_ns.saturating_add((f64::from(elapsed_ms) * 1_000_000.0) as u64);
            graph_replays = graph_replays.saturating_add(1);
            wave_boundary_syncs = wave_boundary_syncs.saturating_add(1);
            encoded_attempts =
                encoded_attempts.saturating_add(config.attempts_per_graph_wave as u64);

            let done = control[CONTROL_DONE] != 0;
            let error = control[CONTROL_ERROR] != 0;
            if !done && !error && control[CONTROL_CONTINUATION] != 0 {
                mid_round_wave_boundary_syncs = mid_round_wave_boundary_syncs.saturating_add(1);
            }
            if done || error {
                break;
            }
        }

        Ok(CudaTiming {
            encoded_attempts,
            graph_replays,
            wave_boundary_syncs,
            mid_round_wave_boundary_syncs,
            graph_capture_ns,
            host_submit_ns,
            device_ns,
            wall_ns: duration_ns(wall_started.elapsed()),
        })
    }

    fn run_profiled(
        &self,
        buffers: &CudaBuffers,
        config: CudaConfig,
    ) -> Result<(CudaTiming, CudaPhaseProfile), CudaError> {
        let parallel_threads = config.round_threads_per_block;
        let supported_parallel_threads = PARALLEL_KERNELS
            .iter()
            .map(|index| {
                self.functions[*index]
                    .max_threads_per_block()
                    .map(|value| value as usize)
                    .map_err(|error| {
                        driver_error(
                            format!("kernel `{}` thread limit query", KERNEL_NAMES[*index]),
                            error,
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .min()
            .unwrap_or(0);
        if parallel_threads > supported_parallel_threads {
            return Err(CudaError::Validation(format!(
                "round_threads_per_block {parallel_threads} exceeds the supported maximum \
                 {supported_parallel_threads}"
            )));
        }

        let parallel_blocks = buffers.node_count.div_ceil(parallel_threads).max(1);
        let parallel_blocks = u32::try_from(parallel_blocks).map_err(|_| {
            CudaError::Validation("parallel CUDA grid does not fit in u32 blocks".into())
        })?;
        let parallel_threads = u32::try_from(parallel_threads).map_err(|_| {
            CudaError::Validation("round_threads_per_block does not fit in u32".into())
        })?;
        let control = LaunchConfig {
            grid_dim: (1, 1, 1),
            block_dim: (LANES as u32, 1, 1),
            shared_mem_bytes: 0,
        };
        let parallel = LaunchConfig {
            grid_dim: (parallel_blocks, 1, 1),
            block_dim: (parallel_threads, 1, 1),
            shared_mem_bytes: 0,
        };
        let configs = [
            control, control, parallel, control, control, parallel, parallel, control,
        ];

        let wall_started = Instant::now();
        let maximum_waves = buffers
            .dispatch_capacity
            .div_ceil(config.attempts_per_graph_wave)
            .max(1);
        let mut wave_boundary_syncs = 0_u64;
        let mut mid_round_wave_boundary_syncs = 0_u64;
        let mut host_submit_ns = 0_u64;
        let mut device_ns = 0_u64;
        let mut encoded_attempts = 0_u64;
        let mut profile = CudaPhaseProfile::default();

        for _ in 0..maximum_waves {
            let phase_events = self.create_phase_events(config.attempts_per_graph_wave)?;
            let start = self
                .stream
                .record_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))
                .map_err(|error| driver_error("profile wave start event", error))?;
            let submitted = Instant::now();
            for boundaries in &phase_events {
                boundaries[0]
                    .record(&self.stream)
                    .map_err(|error| driver_error("profile attempt start event", error))?;
                for (phase, ((function, launch_config), name)) in self
                    .functions
                    .iter()
                    .zip(configs)
                    .zip(KERNEL_NAMES)
                    .enumerate()
                {
                    launch_uniform(&self.stream, function, buffers, launch_config)
                        .map_err(|error| driver_error(format!("profile `{name}` launch"), error))?;
                    boundaries[phase + 1]
                        .record(&self.stream)
                        .map_err(|error| {
                            driver_error(format!("profile `{name}` end event"), error)
                        })?;
                }
            }
            host_submit_ns = host_submit_ns.saturating_add(duration_ns(submitted.elapsed()));
            let end = self
                .stream
                .record_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))
                .map_err(|error| driver_error("profile wave end event", error))?;
            let control = self
                .stream
                .clone_dtoh(&buffers.planes[0])
                .map_err(|error| driver_error("profile wave control readback", error))?;
            self.stream
                .synchronize()
                .map_err(|error| driver_error("profile wave synchronization", error))?;
            let elapsed_ms = start
                .elapsed_ms(&end)
                .map_err(|error| driver_error("profile wave elapsed time", error))?;
            device_ns = device_ns.saturating_add((f64::from(elapsed_ms) * 1_000_000.0) as u64);
            wave_boundary_syncs = wave_boundary_syncs.saturating_add(1);
            encoded_attempts =
                encoded_attempts.saturating_add(config.attempts_per_graph_wave as u64);

            for boundaries in &phase_events {
                for phase in 0..KERNEL_NAMES.len() {
                    let elapsed_ms = boundaries[phase]
                        .elapsed_ms(&boundaries[phase + 1])
                        .map_err(|error| {
                            driver_error(
                                format!(
                                    "profile `{}` device-event elapsed time",
                                    KERNEL_NAMES[phase]
                                ),
                                error,
                            )
                        })?;
                    profile.observe(phase, (f64::from(elapsed_ms) * 1_000_000.0) as u64);
                }
            }
            profile.recorded_attempts = profile
                .recorded_attempts
                .saturating_add(phase_events.len() as u64);

            let done = control[CONTROL_DONE] != 0;
            let error = control[CONTROL_ERROR] != 0;
            if !done && !error && control[CONTROL_CONTINUATION] != 0 {
                mid_round_wave_boundary_syncs = mid_round_wave_boundary_syncs.saturating_add(1);
            }
            if done || error {
                break;
            }
        }

        Ok((
            CudaTiming {
                encoded_attempts,
                graph_replays: 0,
                wave_boundary_syncs,
                mid_round_wave_boundary_syncs,
                graph_capture_ns: 0,
                host_submit_ns,
                device_ns,
                wall_ns: duration_ns(wall_started.elapsed()),
            },
            profile,
        ))
    }

    fn create_phase_events(&self, attempts: usize) -> Result<Vec<Vec<CudaEvent>>, CudaError> {
        let mut phase_events = Vec::with_capacity(attempts);
        for _ in 0..attempts {
            let mut boundaries = Vec::with_capacity(KERNEL_NAMES.len() + 1);
            for _ in 0..=KERNEL_NAMES.len() {
                boundaries.push(
                    self.stream
                        .context()
                        .new_event(Some(sys::CUevent_flags::CU_EVENT_DEFAULT))
                        .map_err(|error| driver_error("phase profile event creation", error))?,
                );
            }
            phase_events.push(boundaries);
        }
        Ok(phase_events)
    }

    fn capture_graph(
        &self,
        buffers: &CudaBuffers,
        configs: &[LaunchConfig; 8],
        attempts: usize,
    ) -> Result<CudaGraph, CudaError> {
        self.stream
            .begin_capture(sys::CUstreamCaptureMode::CU_STREAM_CAPTURE_MODE_THREAD_LOCAL)
            .map_err(|error| driver_error("CUDA Graph capture begin", error))?;

        let captured = (|| {
            for _ in 0..attempts {
                for ((function, config), name) in
                    self.functions.iter().zip(configs).zip(KERNEL_NAMES)
                {
                    launch_uniform(&self.stream, function, buffers, *config)
                        .map_err(|error| driver_error(format!("capture `{name}` launch"), error))?;
                }
            }
            Ok(())
        })();
        // cudarc's safe end_capture API currently exposes only the nonzero flag enum. UPLOAD is
        // valid only with cuGraphInstantiateWithParams, so use the semantics-preserving captured
        // stream priority flag here and explicitly upload below.
        let ended = self.stream.end_capture(
            sys::CUgraphInstantiate_flags::CUDA_GRAPH_INSTANTIATE_FLAG_USE_NODE_PRIORITY,
        );
        if let Err(error) = captured {
            let _ = ended;
            return Err(error);
        }
        let graph = ended
            .map_err(|error| driver_error("CUDA Graph capture end/instantiate", error))?
            .ok_or_else(|| CudaError::Unavailable("CUDA Graph capture returned no graph".into()))?;
        graph
            .upload()
            .map_err(|error| driver_error("CUDA Graph upload", error))?;
        Ok(graph)
    }
}

fn launch_uniform(
    stream: &CudaStream,
    function: &CudaFunction,
    buffers: &CudaBuffers,
    config: LaunchConfig,
) -> Result<(), cudarc::driver::DriverError> {
    debug_assert_eq!(buffers.planes.len(), 29);
    let mut arguments = stream.launch_builder(function);
    for plane in &buffers.planes {
        arguments.arg(plane);
    }
    unsafe {
        arguments.launch(config)?;
    }
    Ok(())
}

struct CudaTiming {
    encoded_attempts: u64,
    graph_replays: u64,
    wave_boundary_syncs: u64,
    mid_round_wave_boundary_syncs: u64,
    graph_capture_ns: u64,
    host_submit_ns: u64,
    device_ns: u64,
    wall_ns: u64,
}

/// Copies one bounded device word range to the host and waits for it.
///
/// T20l fix 1's screen needs a handful of words before the result arena is transferred, so each
/// screen copy synchronizes on its own rather than joining the batched result readback.
fn screen_plane_words(
    stream: &std::sync::Arc<CudaStream>,
    view: cudarc::driver::CudaView<'_, u64>,
    context: &str,
) -> Result<Vec<u64>, CudaError> {
    #[cfg(feature = "cuda-test-hooks")]
    account_readback_words(view.len());
    let words = stream
        .clone_dtoh(&view)
        .map_err(|error| driver_error(format!("{context} readback"), error));
    stream
        .synchronize()
        .map_err(|error| driver_error(format!("{context} readback synchronization"), error))?;
    words
}

fn driver_error(context: impl fmt::Display, error: cudarc::driver::DriverError) -> CudaError {
    CudaError::Unavailable(format!("{context} failed: {error:?}"))
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::{CudaArena, CudaError, decode_arena, decode_device_error};
    use crate::CapacityRetryRecord;

    #[test]
    fn cuda_decodes_tcp_capacity_arenas_with_metal_parity() {
        assert_eq!(decode_arena(11), CudaArena::TcpReceiverRanges);
        assert_eq!(decode_arena(12), CudaArena::TcpSegmentLedger);
        assert_eq!(decode_arena(13), CudaArena::RemoteStaging);
    }

    #[test]
    fn cuda_capacity_fault_decodes_observed_demand() {
        let mut control = vec![0_u64; 20];
        control[0] = 1;
        control[1] = 12;
        control[2] = 7;
        control[3] = 8;
        control[19] = 9;

        assert_eq!(
            decode_device_error(&control),
            CudaError::CapacityExceeded {
                arena: CudaArena::TcpSegmentLedger,
                node: None,
                flow: Some(crate::FlowId(7)),
                stream: None,
                capacity: 8,
                demand: 9,
            }
        );
    }

    #[test]
    fn cuda_channel_capacity_fault_decodes_stream_identity() {
        let mut control = vec![0_u64; 20];
        control[0] = 1;
        control[1] = 8;
        control[2] = 73;
        control[3] = 8;
        control[19] = 9;

        assert_eq!(
            decode_device_error(&control),
            CudaError::CapacityExceeded {
                arena: CudaArena::ChannelInbox,
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
            CudaError::DeviceExecution {
                code: 142,
                node: None,
            },
            "a channel index must not be displayed as an LP for the order diagnostic",
        );
    }

    #[test]
    fn cuda_terminal_failure_retains_every_capacity_retry() {
        let retry = CapacityRetryRecord {
            retry: 1,
            arena: CudaArena::Queue,
            node: Some(crate::NodeId(3)),
            flow: None,
            stream: None,
            capacity: 0,
            demand: 1,
            grown_capacity: 2,
        };
        let error =
            CudaError::Unavailable("injected terminal failure".into()).with_retry_trace(&[retry]);

        assert_eq!(
            error,
            CudaError::RetryFailed {
                error: Box::new(CudaError::Unavailable("injected terminal failure".into())),
                capacity_retry_trace: vec![retry],
            }
        );
    }
}

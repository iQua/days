//! Persistent whole-LP CPU worker pool for exact safe-horizon rounds.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::hint::spin_loop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, RecvError, Sender, TryRecvError, bounded, unbounded};

use crate::event::is_same_time_tx_ready_continuation;
use crate::safe_horizon::{LpRoundWork, RoundMetrics};
use crate::scalar::{
    ExecutionError, LocalNodeState, LocalTransitionResult, ObservationMode,
    PacketArrivalObservation, PacketDeparture, RunResult, RunSummary, TransitionState,
};
use crate::{
    Event, EventKey, NodeDescriptor, NodeId, NodeKind, PacketDescriptor, PayloadId, SimulationImage,
};

const TIME_AFTER_U64_MAX: u128 = 1_u128 << 64;
const REMOTE_ORDER_BYTES: usize = 34;

/// LP count per dispatch request.
///
/// `Static` computes `ceil(active_bulk_LPs / bulk_workers)` each round. `Fixed` uses the supplied
/// chunk size and lets a worker request another chunk after completing its current one.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ChunkGranularity {
    #[default]
    Static,
    Fixed(usize),
}

/// Test-only worker-failure kind exposed so integration tests can verify all-or-nothing failure.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CpuFaultKind {
    Failure,
    Panic,
}

/// Test-only deterministic fault injected after an LP has processed a number of events.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CpuFaultInjection {
    pub worker: usize,
    pub round: u64,
    pub after_events: u64,
    pub kind: CpuFaultKind,
}

/// CPU safe-horizon execution policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CpuConfig {
    pub workers: usize,
    pub granularity: ChunkGranularity,
    /// Empty channel polls before a per-round wait parks the thread. Zero parks immediately.
    ///
    /// This bounded spin is pool-lifecycle synchronization: it runs once at round/chunk
    /// boundaries and never from the per-event transition path.
    pub spin_before_park: u32,
    /// LPs with estimated work strictly greater than this value are stragglers.
    pub straggler_threshold_events: Option<u64>,
    /// Maximum workers reserved exclusively for stragglers in a round.
    pub dedicated_straggler_workers: usize,
    /// Optional deterministic capacity bound used by failure and sizing tests.
    pub max_outbox_events_per_lp: Option<usize>,
    #[doc(hidden)]
    pub fault_injection: Option<CpuFaultInjection>,
}

impl Default for CpuConfig {
    fn default() -> Self {
        Self {
            workers: 1,
            granularity: ChunkGranularity::Static,
            spin_before_park: 4_096,
            straggler_threshold_events: None,
            dedicated_straggler_workers: 1,
            max_outbox_events_per_lp: None,
            fault_injection: None,
        }
    }
}

/// Scheduling class selected for an LP in one round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkClass {
    Straggler,
    Bulk,
}

/// Deterministic work estimate used by the bucketing pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LpWorkEstimate {
    pub node: NodeId,
    pub estimated_events: u64,
}

/// Backend-neutral round partition shape reusable by later heterogeneous backends.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WorkPartition {
    pub stragglers: Vec<LpWorkEstimate>,
    pub bulk_chunks: Vec<Vec<LpWorkEstimate>>,
    /// Workers reserved exclusively for straggler chunks while bulk work remains.
    pub reserved_straggler_workers: Vec<usize>,
}

/// One LP's measured placement and drain time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LpExecutionTiming {
    pub node: NodeId,
    pub worker: usize,
    pub class: WorkClass,
    pub dispatch_order: u64,
    pub started_after_ns: u64,
    pub busy_ns: u64,
}

/// Busy and barrier-idle time for one persistent worker in one round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerRoundTiming {
    pub worker: usize,
    pub busy_ns: u64,
    pub idle_ns: u64,
}

/// T9 semantic round metrics plus CPU placement and wall-time instrumentation.
#[derive(Clone, Debug, PartialEq)]
pub struct CpuRoundMetrics {
    pub semantic: RoundMetrics,
    pub partition: WorkPartition,
    /// Nonempty source-worker × target-owner outbox batches produced.
    pub owner_batch_messages: u64,
    /// Per-round worker assignment/wake channel crossings.
    pub worker_wake_messages: u64,
    /// Per-round worker reply channel crossings.
    pub worker_completion_messages: u64,
    /// Additional dynamic-granularity chunk requests.
    pub chunk_request_messages: u64,
    /// Owner-delivery channel crossings not fused into aggregate worker messages.
    pub owner_delivery_messages: u64,
    pub lp_timings: Vec<LpExecutionTiming>,
    pub worker_timings: Vec<WorkerRoundTiming>,
    pub lp_time_parallel_efficiency: f64,
    pub worker_parallel_efficiency: f64,
    pub worker_utilization: f64,
    /// Coordinator assignment construction and worker wake dispatch.
    pub coordinator_partition_ns: u64,
    /// Time from the first worker assignment until every worker completion arrives.
    pub worker_wait_ns: u64,
    /// Coordinator fused-outbox routing and next-minimum reduction.
    pub coordinator_exchange_ns: u64,
    pub round_wall_time_ns: u64,
}

impl CpuRoundMetrics {
    /// Actual steady-state per-round channel crossings.
    pub const fn pool_messages(&self) -> u64 {
        self.worker_wake_messages
            .saturating_add(self.worker_completion_messages)
            .saturating_add(self.chunk_request_messages)
            .saturating_add(self.owner_delivery_messages)
    }
}

/// Complete CPU result. Errors are returned instead of a partially assembled value.
#[derive(Clone, Debug, PartialEq)]
pub struct CpuRun {
    pub result: RunResult,
    pub rounds: Vec<CpuRoundMetrics>,
}

/// Runs exact safe-horizon rounds with persistent CPU workers.
pub fn run_cpu(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CpuConfig,
) -> Result<CpuRun, ExecutionError> {
    run_cpu_with_observations(
        image,
        exclusive_horizon_ns,
        config,
        ObservationMode::Summary,
    )
}

/// Runs exact safe-horizon rounds with explicit full observation retention.
pub fn run_cpu_with_observations(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CpuConfig,
    observation_mode: ObservationMode,
) -> Result<CpuRun, ExecutionError> {
    validate_config(config)?;
    if config.granularity == ChunkGranularity::Static && config.straggler_threshold_events.is_none()
    {
        return run_owned_static_cpu_with_observations(
            image,
            exclusive_horizon_ns,
            config,
            observation_mode,
        );
    }
    let lps = build_lps(image, observation_mode)?;
    let shards = partition_initial_ownership(lps, config.workers)?;
    let minimum_lookahead_ns = image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min();
    if minimum_lookahead_ns == Some(0) {
        return Err(ExecutionError::NonPositiveLookahead);
    }
    let configured_stop = u128::from(image.stop_time_ns) + 1;
    let run_end = exclusive_horizon_ns
        .map(u128::from)
        .unwrap_or(TIME_AFTER_U64_MAX)
        .min(configured_stop);

    crossbeam::scope(|scope| {
        let reply_capacity = config.workers.saturating_mul(4).max(1);
        let (reply_tx, reply_rx) = bounded(reply_capacity);
        let mut routes = Vec::with_capacity(config.workers);
        let mut ingresses = Vec::with_capacity(config.workers);
        for _ in 0..config.workers {
            let (remote_tx, remote_rx) = unbounded();
            routes.push(OwnerRoutes { remote: remote_tx });
            ingresses.push(Some(OwnerIngress { remote: remote_rx }));
        }
        let mut commands = Vec::with_capacity(config.workers);
        for shard in shards {
            let (command_tx, command_rx) = bounded(1);
            commands.push(command_tx);
            let worker_reply = reply_tx.clone();
            let worker_ingress = ingresses[shard.worker]
                .take()
                .expect("each worker owns one ingress receiver");
            scope.spawn(move |_| {
                let worker = shard.worker;
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    worker_loop(
                        shard,
                        command_rx,
                        worker_ingress,
                        &worker_reply,
                        config.spin_before_park,
                    )
                }));
                if outcome.is_err() {
                    if config.fault_injection.is_some_and(|fault| {
                        fault.worker == worker && fault.kind == CpuFaultKind::Panic
                    }) {
                        let _ = worker_reply.send(WorkerReply::Failed {
                            error: ExecutionError::WorkerChannelDisconnected,
                        });
                    }
                    let _ = worker_reply.send(WorkerReply::Failed {
                        error: ExecutionError::WorkerPanicked { worker },
                    });
                }
            });
        }
        drop(reply_tx);

        let result = run_coordinator(
            image,
            config,
            run_end,
            minimum_lookahead_ns,
            &commands,
            &reply_rx,
            &routes,
        );
        drop(commands);
        match result {
            Ok(run) => Ok(run),
            Err(error) => Err(drain_worker_errors(&reply_rx, error)),
        }
    })
    .map_err(|_| ExecutionError::WorkerChannelDisconnected)?
}

fn validate_config(config: CpuConfig) -> Result<(), ExecutionError> {
    if config.workers == 0 {
        return Err(ExecutionError::InvalidCpuConfig(
            "workers must be greater than zero",
        ));
    }
    if matches!(config.granularity, ChunkGranularity::Fixed(0)) {
        return Err(ExecutionError::InvalidCpuConfig(
            "fixed chunk granularity must be greater than zero",
        ));
    }
    if config.straggler_threshold_events.is_some() && config.dedicated_straggler_workers == 0 {
        return Err(ExecutionError::InvalidCpuConfig(
            "straggler routing requires at least one dedicated worker",
        ));
    }
    if let Some(fault) = config.fault_injection {
        if fault.worker >= config.workers {
            return Err(ExecutionError::InvalidCpuConfig(
                "fault injection names a nonexistent worker",
            ));
        }
    }
    Ok(())
}

/// Static, unclassified rounds keep deterministic modulo LP ownership for the pool lifetime.
///
/// Each command carries the horizon and the owner's fused remote inbox; each aggregate completion
/// carries its local minimum, instrumentation, and source-worker × target-owner outbox batches.
/// This is exactly one wake and one completion per worker per round. The command/reply channels and
/// their bounded receive spin are per-round pool-lifecycle synchronization, never per-event work.
fn run_owned_static_cpu_with_observations(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    config: CpuConfig,
    observation_mode: ObservationMode,
) -> Result<CpuRun, ExecutionError> {
    let lps = build_lps(image, observation_mode)?;
    let shards = partition_initial_ownership(lps, config.workers)?;
    let minimum_lookahead_ns = image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min();
    if minimum_lookahead_ns == Some(0) {
        return Err(ExecutionError::NonPositiveLookahead);
    }
    let configured_stop = u128::from(image.stop_time_ns) + 1;
    let run_end = exclusive_horizon_ns
        .map(u128::from)
        .unwrap_or(TIME_AFTER_U64_MAX)
        .min(configured_stop);

    crossbeam::scope(|scope| {
        let (reply_tx, reply_rx) = bounded(config.workers.saturating_mul(2).max(1));
        let mut commands = Vec::with_capacity(config.workers);
        for shard in shards {
            let worker = shard.worker;
            let (command_tx, command_rx) = bounded(1);
            commands.push(command_tx);
            let worker_reply = reply_tx.clone();
            scope.spawn(move |_| {
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    owned_worker_loop(shard, command_rx, &worker_reply, config.spin_before_park)
                }));
                if outcome.is_err() {
                    if config.fault_injection.is_some_and(|fault| {
                        fault.worker == worker && fault.kind == CpuFaultKind::Panic
                    }) {
                        let _ = worker_reply.send(OwnedWorkerReply::Failed {
                            error: ExecutionError::WorkerChannelDisconnected,
                        });
                    }
                    let _ = worker_reply.send(OwnedWorkerReply::Failed {
                        error: ExecutionError::WorkerPanicked { worker },
                    });
                }
            });
        }
        drop(reply_tx);

        let result = run_owned_static_coordinator(
            image,
            config,
            run_end,
            minimum_lookahead_ns,
            &commands,
            &reply_rx,
        );
        drop(commands);
        match result {
            Ok(run) => Ok(run),
            Err(error) => Err(drain_owned_worker_errors(&reply_rx, error)),
        }
    })
    .map_err(|_| ExecutionError::WorkerChannelDisconnected)?
}

struct CpuLp<'image> {
    lp_slot: usize,
    node: NodeDescriptor,
    transitions: TransitionState<'image>,
    futures: BTreeMap<EventKey, Event>,
    pinned_packets: BTreeSet<PayloadId>,
}

impl CpuLp<'_> {
    fn next_key(&self) -> Option<EventKey> {
        self.futures.first_key_value().map(|(key, _)| *key)
    }

    fn estimated_work(&self, exclusive_horizon_ns: u128) -> Result<u64, ExecutionError> {
        u64::try_from(
            self.futures
                .values()
                .take_while(|event| u128::from(event.key.time_ns) < exclusive_horizon_ns)
                .count(),
        )
        .map_err(|_| ExecutionError::CounterOverflow(self.node.id))
    }

    fn drain(
        &mut self,
        exclusive_horizon_ns: u128,
        outbox_capacity: Option<usize>,
        fault: Option<CpuFaultInjection>,
        worker: usize,
        round: u64,
    ) -> Result<(LpRoundWork, Vec<RemoteEnvelope>), ExecutionError> {
        let mut children = Vec::new();
        let mut outbox = Vec::new();
        let mut events_processed = 0_u64;
        let mut same_time_continuations = 0_u64;
        let mut continuation = None;
        while continuation.is_some()
            || self
                .futures
                .first_key_value()
                .is_some_and(|(key, _)| u128::from(key.time_ns) < exclusive_horizon_ns)
        {
            if let Some(fault) = fault {
                if fault.worker == worker
                    && fault.round == round
                    && events_processed == fault.after_events
                {
                    match fault.kind {
                        CpuFaultKind::Failure => {
                            return Err(ExecutionError::WorkerFailed { worker, round });
                        }
                        CpuFaultKind::Panic => panic!("injected CPU worker panic"),
                    }
                }
            }

            let event = if let Some(event) = continuation.take() {
                event
            } else {
                self.futures
                    .pop_first()
                    .expect("first_key_value established a pending event")
                    .1
            };
            let preserved_packet = if self.pinned_packets.contains(&event.payload) {
                Some(self.transitions.packet_descriptor(event.payload)?)
            } else {
                None
            };
            self.transitions.dispatch(event, &mut children)?;
            if let Some(packet) = preserved_packet {
                self.transitions.install_packet(packet)?;
            }
            let direct_child = match children.as_slice() {
                [child]
                    if is_same_time_tx_ready_continuation(
                        event,
                        *child,
                        self.node.id,
                        self.futures.first_key_value().map(|(key, _)| *key),
                    ) =>
                {
                    Some(*child)
                }
                _ => None,
            };
            for child in children.drain(..) {
                if child.target == self.node.id {
                    if direct_child == Some(child) {
                        continuation = Some(child);
                        same_time_continuations = same_time_continuations.saturating_add(1);
                    } else if self.futures.insert(child.key, child).is_some() {
                        return Err(ExecutionError::DuplicateEventKey(child.key));
                    }
                } else {
                    if outbox_capacity.is_some_and(|capacity| outbox.len() >= capacity) {
                        return Err(ExecutionError::OutboxCapacityExceeded {
                            node: self.node.id,
                            capacity: outbox_capacity.expect("capacity was checked"),
                        });
                    }
                    outbox.push(RemoteEnvelope {
                        event: child,
                        packet: self.transitions.packet_descriptor(child.payload)?,
                    });
                }
            }
            events_processed = events_processed
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(self.node.id))?;
        }

        if let Some((key, _)) = self.futures.first_key_value() {
            if u128::from(key.time_ns) < exclusive_horizon_ns {
                return Err(ExecutionError::EventBelowHorizonAfterDrain {
                    key: *key,
                    exclusive_horizon_ns,
                });
            }
        }
        Ok((
            LpRoundWork {
                node: self.node.id,
                events_processed,
                same_time_continuations,
            },
            outbox,
        ))
    }
}

#[derive(Clone, Copy)]
struct RemoteEnvelope {
    event: Event,
    packet: PacketDescriptor,
}

struct ActiveLp<'image> {
    owner_worker: usize,
    owner_slot: usize,
    estimated_events: u64,
    lp: CpuLp<'image>,
}

struct ExecutedLp {
    work: LpRoundWork,
    messages_exchanged: u64,
    timing: LpExecutionTiming,
}

struct ReturnedLp<'image> {
    owner_worker: usize,
    owner_slot: usize,
    lp: CpuLp<'image>,
}

#[derive(Clone)]
struct OwnerRoutes {
    remote: Sender<Vec<RemoteEnvelope>>,
}

struct OwnerIngress {
    remote: Receiver<Vec<RemoteEnvelope>>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FrontierEntry {
    time_ns: u64,
    node: NodeId,
    generation: u64,
    owner_slot: usize,
}

struct OwnerFrontierIndex {
    heap: BinaryHeap<Reverse<FrontierEntry>>,
    generations: Vec<u64>,
}

impl OwnerFrontierIndex {
    fn new(lp_count: usize) -> Self {
        Self {
            heap: BinaryHeap::new(),
            generations: vec![0; lp_count],
        }
    }

    fn update(
        &mut self,
        owner_slot: usize,
        node: NodeId,
        next: Option<EventKey>,
        physical_lp_probes: &mut u64,
    ) -> Result<(), ExecutionError> {
        *physical_lp_probes = physical_lp_probes.saturating_add(1);
        let generation = self.generations[owner_slot]
            .checked_add(1)
            .ok_or(ExecutionError::CounterOverflow(node))?;
        self.generations[owner_slot] = generation;
        if let Some(key) = next {
            self.heap.push(Reverse(FrontierEntry {
                time_ns: key.time_ns,
                node,
                generation,
                owner_slot,
            }));
        }
        Ok(())
    }

    fn peek_min(
        &mut self,
        heap_pops: &mut u64,
        physical_lp_probes: &mut u64,
    ) -> Option<FrontierEntry> {
        loop {
            let entry = self.heap.peek().map(|entry| entry.0)?;
            *physical_lp_probes = physical_lp_probes.saturating_add(1);
            if self.generations[entry.owner_slot] == entry.generation {
                return Some(entry);
            }
            self.heap.pop();
            *heap_pops = heap_pops.saturating_add(1);
        }
    }

    fn pop_before(
        &mut self,
        exclusive_horizon_ns: u128,
        heap_pops: &mut u64,
        physical_lp_probes: &mut u64,
    ) -> Option<FrontierEntry> {
        let entry = self.peek_min(heap_pops, physical_lp_probes)?;
        if u128::from(entry.time_ns) >= exclusive_horizon_ns {
            return None;
        }
        let popped = self
            .heap
            .pop()
            .expect("peek_min established a heap entry")
            .0;
        *heap_pops = heap_pops.saturating_add(1);
        Some(popped)
    }
}

struct WorkerShard<'image> {
    worker: usize,
    lps: Vec<Option<CpuLp<'image>>>,
    frontier: OwnerFrontierIndex,
}

enum WorkerCommand<'image> {
    Extract {
        round: u64,
        exclusive_horizon_ns: u128,
    },
    Execute {
        round: u64,
        round_start: Instant,
        exclusive_horizon_ns: u128,
        class: WorkClass,
        first_dispatch_order: u64,
        items: Vec<ActiveLp<'image>>,
        outbox_capacity: Option<usize>,
        fault: Option<CpuFaultInjection>,
        workers: usize,
    },
    RestoreAndMerge {
        round: u64,
        exclusive_horizon_ns: u128,
        workers: usize,
        returned: Vec<ReturnedLp<'image>>,
    },
    Finish,
}

enum WorkerReply<'image> {
    Ready {
        worker: usize,
        minimum_ns: Option<u64>,
    },
    Active {
        worker: usize,
        round: u64,
        active: Vec<ActiveLp<'image>>,
        heap_pops: u64,
        physical_lp_probes: u64,
        busy_ns: u64,
    },
    ChunkStarted {
        worker: usize,
        round: u64,
        class: WorkClass,
    },
    ChunkComplete {
        worker: usize,
        round: u64,
        class: WorkClass,
        executed: Vec<ExecutedLp>,
        returned: Vec<ReturnedLp<'image>>,
        remote_by_owner: Vec<Vec<RemoteEnvelope>>,
        busy_ns: u64,
    },
    MergeComplete {
        worker: usize,
        round: u64,
        frontier_updates: u64,
        heap_pops: u64,
        physical_lp_probes: u64,
        minimum_ns: Option<u64>,
        busy_ns: u64,
    },
    Finished {
        worker: usize,
        lps: Vec<CpuLp<'image>>,
    },
    Failed {
        error: ExecutionError,
    },
}

enum OwnedWorkerCommand {
    RunRound {
        round: u64,
        round_start: Instant,
        exclusive_horizon_ns: u128,
        remote_floor_ns: u128,
        inbox: Vec<RemoteEnvelope>,
        outbox_capacity: Option<usize>,
        fault: Option<CpuFaultInjection>,
        workers: usize,
    },
    Finish {
        remote_floor_ns: u128,
        inbox: Vec<RemoteEnvelope>,
        workers: usize,
    },
}

enum OwnedWorkerReply<'image> {
    Ready {
        worker: usize,
        minimum_ns: Option<u64>,
    },
    RoundComplete {
        worker: usize,
        round: u64,
        executed: Vec<ExecutedLp>,
        work_estimates: Vec<LpWorkEstimate>,
        remote_by_owner: Vec<Vec<RemoteEnvelope>>,
        inbound_frontier_updates: u64,
        inbound_heap_pops: u64,
        inbound_physical_lp_probes: u64,
        frontier_updates: u64,
        heap_pops: u64,
        physical_lp_probes: u64,
        minimum_ns: Option<u64>,
        busy_ns: u64,
    },
    Finished {
        worker: usize,
        lps: Vec<CpuLp<'image>>,
        inbound_frontier_updates: u64,
        inbound_heap_pops: u64,
        inbound_physical_lp_probes: u64,
    },
    Failed {
        error: ExecutionError,
    },
}

fn worker_loop<'image>(
    mut shard: WorkerShard<'image>,
    commands: Receiver<WorkerCommand<'image>>,
    ingress: OwnerIngress,
    replies: &Sender<WorkerReply<'image>>,
    spin_before_park: u32,
) {
    let mut initial_heap_pops = 0;
    let mut initial_physical_lp_probes = 0;
    let initial_minimum_ns = shard
        .frontier
        .peek_min(&mut initial_heap_pops, &mut initial_physical_lp_probes)
        .map(|entry| entry.time_ns);
    if replies
        .send(WorkerReply::Ready {
            worker: shard.worker,
            minimum_ns: initial_minimum_ns,
        })
        .is_err()
    {
        return;
    }
    while let Ok(command) = receive_spin_then_park(&commands, spin_before_park) {
        let result = match command {
            WorkerCommand::Extract {
                round,
                exclusive_horizon_ns,
            } => {
                let started = Instant::now();
                match shard.extract(exclusive_horizon_ns) {
                    Ok((active, heap_pops, physical_lp_probes)) => {
                        replies.send(WorkerReply::Active {
                            worker: shard.worker,
                            round,
                            active,
                            heap_pops,
                            physical_lp_probes,
                            busy_ns: elapsed_ns(started),
                        })
                    }
                    Err(error) => {
                        let _ = replies.send(WorkerReply::Failed { error });
                        return;
                    }
                }
            }
            WorkerCommand::Execute {
                round,
                round_start,
                exclusive_horizon_ns,
                class,
                first_dispatch_order,
                items,
                outbox_capacity,
                fault,
                workers,
            } => {
                if replies
                    .send(WorkerReply::ChunkStarted {
                        worker: shard.worker,
                        round,
                        class,
                    })
                    .is_err()
                {
                    return;
                }
                let chunk_started = Instant::now();
                let mut executed = Vec::with_capacity(items.len());
                let mut returned = Vec::with_capacity(items.len());
                let mut remote_by_owner = (0..workers).map(|_| Vec::new()).collect::<Vec<_>>();
                for (offset, mut active) in items.into_iter().enumerate() {
                    let lp_started = Instant::now();
                    let started_after_ns =
                        duration_ns(lp_started.saturating_duration_since(round_start));
                    let drain = active.lp.drain(
                        exclusive_horizon_ns,
                        outbox_capacity,
                        fault,
                        shard.worker,
                        round,
                    );
                    let (work, outbox) = match drain {
                        Ok(result) => result,
                        Err(error) => {
                            let _ = replies.send(WorkerReply::Failed { error });
                            return;
                        }
                    };
                    let messages_exchanged = match u64::try_from(outbox.len()) {
                        Ok(count) => count,
                        Err(_) => {
                            let _ = replies.send(WorkerReply::Failed {
                                error: ExecutionError::CounterOverflow(active.lp.node.id),
                            });
                            return;
                        }
                    };
                    let timing = LpExecutionTiming {
                        node: active.lp.node.id,
                        worker: shard.worker,
                        class,
                        dispatch_order: first_dispatch_order.saturating_add(offset as u64),
                        started_after_ns,
                        busy_ns: elapsed_ns(lp_started),
                    };
                    let owner_worker = active.owner_worker;
                    returned.push(ReturnedLp {
                        owner_worker,
                        owner_slot: active.owner_slot,
                        lp: active.lp,
                    });
                    for envelope in outbox {
                        let Ok(target_slot) = usize::try_from(envelope.event.target.0) else {
                            let _ = replies.send(WorkerReply::Failed {
                                error: ExecutionError::UnknownNode(envelope.event.target),
                            });
                            return;
                        };
                        remote_by_owner[target_slot % workers].push(envelope);
                    }
                    executed.push(ExecutedLp {
                        work,
                        messages_exchanged,
                        timing,
                    });
                }
                replies.send(WorkerReply::ChunkComplete {
                    worker: shard.worker,
                    round,
                    class,
                    executed,
                    returned,
                    remote_by_owner,
                    busy_ns: elapsed_ns(chunk_started),
                })
            }
            WorkerCommand::RestoreAndMerge {
                round,
                exclusive_horizon_ns,
                workers,
                returned,
            } => {
                let started = Instant::now();
                match shard.restore_and_merge(&ingress, returned, exclusive_horizon_ns, workers) {
                    Ok((frontier_updates, heap_pops, physical_lp_probes, minimum_ns)) => replies
                        .send(WorkerReply::MergeComplete {
                            worker: shard.worker,
                            round,
                            frontier_updates,
                            heap_pops,
                            physical_lp_probes,
                            minimum_ns,
                            busy_ns: elapsed_ns(started),
                        }),
                    Err(error) => {
                        let _ = replies.send(WorkerReply::Failed { error });
                        return;
                    }
                }
            }
            WorkerCommand::Finish => {
                let mut lps = Vec::with_capacity(shard.lps.len());
                for lp in shard.lps {
                    let Some(lp) = lp else {
                        let _ = replies.send(WorkerReply::Failed {
                            error: ExecutionError::WorkerFailed {
                                worker: shard.worker,
                                round: u64::MAX,
                            },
                        });
                        return;
                    };
                    lps.push(lp);
                }
                let _ = replies.send(WorkerReply::Finished {
                    worker: shard.worker,
                    lps,
                });
                return;
            }
        };
        if result.is_err() {
            return;
        }
    }
}

fn owned_worker_loop<'image>(
    mut shard: WorkerShard<'image>,
    commands: Receiver<OwnedWorkerCommand>,
    replies: &Sender<OwnedWorkerReply<'image>>,
    spin_before_park: u32,
) {
    let mut setup_heap_pops = 0;
    let mut setup_physical_lp_probes = 0;
    let minimum_ns = shard
        .frontier
        .peek_min(&mut setup_heap_pops, &mut setup_physical_lp_probes)
        .map(|entry| entry.time_ns);
    if replies
        .send(OwnedWorkerReply::Ready {
            worker: shard.worker,
            minimum_ns,
        })
        .is_err()
    {
        return;
    }

    while let Ok(command) = receive_spin_then_park(&commands, spin_before_park) {
        match command {
            OwnedWorkerCommand::RunRound {
                round,
                round_start,
                exclusive_horizon_ns,
                remote_floor_ns,
                inbox,
                outbox_capacity,
                fault,
                workers,
            } => {
                let started = Instant::now();
                let (inbound_frontier_updates, inbound_heap_pops, inbound_physical_lp_probes) =
                    match shard.merge_remote(inbox, remote_floor_ns, workers) {
                        Ok(metrics) => metrics,
                        Err(error) => {
                            let _ = replies.send(OwnedWorkerReply::Failed { error });
                            return;
                        }
                    };
                let mut frontier_updates = 0_u64;
                let mut heap_pops = 0_u64;
                let mut physical_lp_probes = 0_u64;
                let (active, extract_pops, extract_probes) =
                    match shard.extract(exclusive_horizon_ns) {
                        Ok(active) => active,
                        Err(error) => {
                            let _ = replies.send(OwnedWorkerReply::Failed { error });
                            return;
                        }
                    };
                heap_pops = heap_pops.saturating_add(extract_pops);
                physical_lp_probes = physical_lp_probes.saturating_add(extract_probes);

                let mut executed = Vec::with_capacity(active.len());
                let mut returned = Vec::with_capacity(active.len());
                let mut work_estimates = Vec::with_capacity(active.len());
                let mut remote_by_owner = (0..workers).map(|_| Vec::new()).collect::<Vec<_>>();
                for (dispatch_order, mut active) in active.into_iter().enumerate() {
                    let lp_started = Instant::now();
                    let started_after_ns =
                        duration_ns(lp_started.saturating_duration_since(round_start));
                    work_estimates.push(work_estimate(&active));
                    let (work, outbox) = match active.lp.drain(
                        exclusive_horizon_ns,
                        outbox_capacity,
                        fault,
                        shard.worker,
                        round,
                    ) {
                        Ok(result) => result,
                        Err(error) => {
                            let _ = replies.send(OwnedWorkerReply::Failed { error });
                            return;
                        }
                    };
                    let messages_exchanged = match u64::try_from(outbox.len()) {
                        Ok(count) => count,
                        Err(_) => {
                            let _ = replies.send(OwnedWorkerReply::Failed {
                                error: ExecutionError::CounterOverflow(active.lp.node.id),
                            });
                            return;
                        }
                    };
                    let timing = LpExecutionTiming {
                        node: active.lp.node.id,
                        worker: shard.worker,
                        class: WorkClass::Bulk,
                        dispatch_order: u64::try_from(dispatch_order).unwrap_or(u64::MAX),
                        started_after_ns,
                        busy_ns: elapsed_ns(lp_started),
                    };
                    returned.push(ReturnedLp {
                        owner_worker: shard.worker,
                        owner_slot: active.owner_slot,
                        lp: active.lp,
                    });
                    for envelope in outbox {
                        let Ok(target_slot) = usize::try_from(envelope.event.target.0) else {
                            let _ = replies.send(OwnedWorkerReply::Failed {
                                error: ExecutionError::UnknownNode(envelope.event.target),
                            });
                            return;
                        };
                        remote_by_owner[target_slot % workers].push(envelope);
                    }
                    executed.push(ExecutedLp {
                        work,
                        messages_exchanged,
                        timing,
                    });
                }

                let (updates, restore_pops, restore_probes, minimum_ns) =
                    match shard.restore_owned(returned, exclusive_horizon_ns) {
                        Ok(metrics) => metrics,
                        Err(error) => {
                            let _ = replies.send(OwnedWorkerReply::Failed { error });
                            return;
                        }
                    };
                frontier_updates = frontier_updates.saturating_add(updates);
                heap_pops = heap_pops.saturating_add(restore_pops);
                physical_lp_probes = physical_lp_probes.saturating_add(restore_probes);
                if replies
                    .send(OwnedWorkerReply::RoundComplete {
                        worker: shard.worker,
                        round,
                        executed,
                        work_estimates,
                        remote_by_owner,
                        inbound_frontier_updates,
                        inbound_heap_pops,
                        inbound_physical_lp_probes,
                        frontier_updates,
                        heap_pops,
                        physical_lp_probes,
                        minimum_ns,
                        busy_ns: elapsed_ns(started),
                    })
                    .is_err()
                {
                    return;
                }
            }
            OwnedWorkerCommand::Finish {
                remote_floor_ns,
                inbox,
                workers,
            } => {
                let worker = shard.worker;
                let (inbound_frontier_updates, inbound_heap_pops, inbound_physical_lp_probes) =
                    match shard.merge_remote(inbox, remote_floor_ns, workers) {
                        Ok(metrics) => metrics,
                        Err(error) => {
                            let _ = replies.send(OwnedWorkerReply::Failed { error });
                            return;
                        }
                    };
                let lps = match shard.into_lps() {
                    Ok(lps) => lps,
                    Err(error) => {
                        let _ = replies.send(OwnedWorkerReply::Failed { error });
                        return;
                    }
                };
                let _ = replies.send(OwnedWorkerReply::Finished {
                    worker,
                    lps,
                    inbound_frontier_updates,
                    inbound_heap_pops,
                    inbound_physical_lp_probes,
                });
                return;
            }
        }
    }
}

struct WorkChunk<'image> {
    class: WorkClass,
    items: Vec<ActiveLp<'image>>,
}

struct RoundAssignment<'image> {
    partition: WorkPartition,
    straggler_chunks: VecDeque<WorkChunk<'image>>,
    bulk_chunks: VecDeque<WorkChunk<'image>>,
    dedicated_workers: Vec<usize>,
    bulk_workers: Vec<usize>,
}

struct RoundExecution<'image> {
    executed: Vec<ExecutedLp>,
    returned_by_owner: Vec<Vec<ReturnedLp<'image>>>,
    remote_by_source_and_owner: Vec<Vec<Vec<RemoteEnvelope>>>,
}

impl RoundExecution<'_> {
    fn new(workers: usize) -> Self {
        Self {
            executed: Vec::new(),
            returned_by_owner: (0..workers).map(|_| Vec::new()).collect(),
            remote_by_source_and_owner: (0..workers)
                .map(|_| (0..workers).map(|_| Vec::new()).collect())
                .collect(),
        }
    }
}

fn run_coordinator<'image>(
    image: &'image SimulationImage,
    config: CpuConfig,
    run_end: u128,
    minimum_lookahead_ns: Option<u64>,
    commands: &[Sender<WorkerCommand<'image>>],
    replies: &Receiver<WorkerReply<'image>>,
    routes: &[OwnerRoutes],
) -> Result<CpuRun, ExecutionError> {
    let mut previous_horizon = None;
    let mut rounds = Vec::new();
    let mut round_number = 0_u64;
    let mut minima = vec![None; config.workers];
    for _ in 0..config.workers {
        match receive_reply(replies)? {
            WorkerReply::Ready { worker, minimum_ns } => minima[worker] = minimum_ns,
            WorkerReply::Failed { error } => return Err(error),
            _ => return Err(ExecutionError::WorkerChannelDisconnected),
        }
    }

    loop {
        let round_start = Instant::now();
        let mut worker_busy_ns = vec![0_u64; config.workers];
        let mut frontier_heap_pops = 0_u64;
        let mut physical_lp_probes = 0_u64;
        let Some(frontier_ns) = minima.iter().copied().flatten().min() else {
            break;
        };
        if u128::from(frontier_ns) >= run_end {
            break;
        }
        let lookahead_end = minimum_lookahead_ns.map_or(TIME_AFTER_U64_MAX, |delay| {
            (u128::from(frontier_ns) + u128::from(delay)).min(TIME_AFTER_U64_MAX)
        });
        let exclusive_horizon_ns = run_end.min(lookahead_end);
        let horizon_advance_ns = exclusive_horizon_ns
            .saturating_sub(previous_horizon.unwrap_or(u128::from(frontier_ns)));

        for command in commands {
            send_command(
                command,
                WorkerCommand::Extract {
                    round: round_number,
                    exclusive_horizon_ns,
                },
            )?;
        }
        let mut active_by_owner = (0..config.workers)
            .map(|_| None)
            .collect::<Vec<Option<Vec<ActiveLp<'image>>>>>();
        for _ in 0..config.workers {
            match receive_reply_spin(replies, config.spin_before_park)? {
                WorkerReply::Active {
                    worker,
                    round,
                    active,
                    heap_pops,
                    physical_lp_probes: probes,
                    busy_ns,
                } if round == round_number => {
                    active_by_owner[worker] = Some(active);
                    worker_busy_ns[worker] = worker_busy_ns[worker].saturating_add(busy_ns);
                    frontier_heap_pops = frontier_heap_pops.saturating_add(heap_pops);
                    physical_lp_probes = physical_lp_probes.saturating_add(probes);
                }
                WorkerReply::Failed { error } => return Err(error),
                _ => return Err(ExecutionError::WorkerChannelDisconnected),
            }
        }
        let active = active_by_owner
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<_>>();
        let assignment = partition_round(active, config)?;
        let partition = assignment.partition.clone();
        let round_chunks = partition
            .stragglers
            .len()
            .saturating_add(partition.bulk_chunks.len());
        let mut execution = execute_assignment(
            assignment,
            round_number,
            round_start,
            exclusive_horizon_ns,
            config,
            commands,
            replies,
            &mut worker_busy_ns,
        )?;

        let mut lp_work = Vec::with_capacity(execution.executed.len());
        let mut lp_timings = Vec::with_capacity(execution.executed.len());
        let mut events_processed = 0_u64;
        let mut messages_exchanged = 0_u64;
        for executed in execution.executed {
            events_processed = events_processed
                .checked_add(executed.work.events_processed)
                .ok_or(ExecutionError::CounterOverflow(executed.work.node))?;
            messages_exchanged = messages_exchanged
                .checked_add(executed.messages_exchanged)
                .ok_or(ExecutionError::CounterOverflow(executed.work.node))?;
            lp_work.push(executed.work);
            lp_timings.push(executed.timing);
        }

        let mut owner_batch_messages = 0_u64;
        for batches_by_owner in execution.remote_by_source_and_owner {
            for (owner, batch) in batches_by_owner.into_iter().enumerate() {
                if batch.is_empty() {
                    continue;
                }
                routes[owner]
                    .remote
                    .send(batch)
                    .map_err(|_| ExecutionError::WorkerChannelDisconnected)?;
                owner_batch_messages = owner_batch_messages.saturating_add(1);
            }
        }
        for (worker, command) in commands.iter().enumerate() {
            send_command(
                command,
                WorkerCommand::RestoreAndMerge {
                    round: round_number,
                    exclusive_horizon_ns,
                    workers: config.workers,
                    returned: std::mem::take(&mut execution.returned_by_owner[worker]),
                },
            )?;
        }
        let mut frontier_updates = 0_u64;
        let mut next_minima = vec![None; config.workers];
        for _ in 0..config.workers {
            match receive_reply_spin(replies, config.spin_before_park)? {
                WorkerReply::MergeComplete {
                    worker,
                    round,
                    frontier_updates: updates,
                    heap_pops,
                    physical_lp_probes: probes,
                    minimum_ns,
                    busy_ns,
                } if round == round_number => {
                    frontier_updates = frontier_updates.saturating_add(updates);
                    frontier_heap_pops = frontier_heap_pops.saturating_add(heap_pops);
                    physical_lp_probes = physical_lp_probes.saturating_add(probes);
                    worker_busy_ns[worker] = worker_busy_ns[worker].saturating_add(busy_ns);
                    next_minima[worker] = minimum_ns;
                }
                WorkerReply::Failed { error } => return Err(error),
                _ => return Err(ExecutionError::WorkerChannelDisconnected),
            }
        }
        minima = next_minima;

        radix_sort_by_node(&mut lp_work, |work| work.node);
        radix_sort_by_node(&mut lp_timings, |timing| timing.node);
        let active_lp_count = lp_work.len();
        let max_work = lp_work
            .iter()
            .map(|work| work.events_processed)
            .max()
            .unwrap_or(0);
        let parallel_efficiency = if active_lp_count == 0 || max_work == 0 {
            1.0
        } else {
            events_processed as f64 / (active_lp_count as f64 * max_work as f64)
        };
        let lp_time_parallel_efficiency = efficiency(
            lp_timings.iter().map(|timing| timing.busy_ns),
            active_lp_count,
        );
        let worker_parallel_efficiency = efficiency(worker_busy_ns.iter().copied(), config.workers);
        let round_wall_time_ns = elapsed_ns(round_start);
        let worker_timings = worker_busy_ns
            .iter()
            .copied()
            .enumerate()
            .map(|(worker, busy_ns)| WorkerRoundTiming {
                worker,
                busy_ns,
                idle_ns: round_wall_time_ns.saturating_sub(busy_ns),
            })
            .collect::<Vec<_>>();
        let total_worker_busy = worker_busy_ns
            .iter()
            .copied()
            .fold(0_u128, |total, busy| total + u128::from(busy));
        let available_worker_time =
            u128::from(round_wall_time_ns).saturating_mul(config.workers as u128);
        let worker_utilization = if available_worker_time == 0 {
            1.0
        } else {
            total_worker_busy as f64 / available_worker_time as f64
        };
        rounds.push(CpuRoundMetrics {
            semantic: RoundMetrics {
                frontier_ns,
                exclusive_horizon_ns,
                horizon_advance_ns,
                events_processed,
                active_lp_count,
                lp_work,
                parallel_efficiency,
                messages_exchanged,
                frontier_updates,
                frontier_heap_pops,
                physical_lp_probes,
            },
            partition,
            owner_batch_messages,
            worker_wake_messages: u64::try_from(
                config
                    .workers
                    .saturating_mul(2)
                    .saturating_add(round_chunks),
            )
            .unwrap_or(u64::MAX),
            worker_completion_messages: u64::try_from(
                config
                    .workers
                    .saturating_mul(2)
                    .saturating_add(round_chunks.saturating_mul(2)),
            )
            .unwrap_or(u64::MAX),
            chunk_request_messages: 0,
            owner_delivery_messages: owner_batch_messages,
            lp_timings,
            worker_timings,
            lp_time_parallel_efficiency,
            worker_parallel_efficiency,
            worker_utilization,
            coordinator_partition_ns: 0,
            worker_wait_ns: 0,
            coordinator_exchange_ns: 0,
            round_wall_time_ns,
        });
        previous_horizon = Some(exclusive_horizon_ns);
        round_number = round_number
            .checked_add(1)
            .ok_or(ExecutionError::CounterOverflow(NodeId(0)))?;
    }

    finish_workers(image, commands, replies, rounds)
}

#[allow(clippy::too_many_arguments)]
fn run_owned_static_coordinator<'image>(
    image: &'image SimulationImage,
    config: CpuConfig,
    run_end: u128,
    minimum_lookahead_ns: Option<u64>,
    commands: &[Sender<OwnedWorkerCommand>],
    replies: &Receiver<OwnedWorkerReply<'image>>,
) -> Result<CpuRun, ExecutionError> {
    let mut minima = vec![None; config.workers];
    let mut ready = vec![false; config.workers];
    for _ in 0..config.workers {
        match receive_owned_reply(replies, 0)? {
            OwnedWorkerReply::Ready { worker, minimum_ns } if !ready[worker] => {
                ready[worker] = true;
                minima[worker] = minimum_ns;
            }
            OwnedWorkerReply::Failed { error } => return Err(error),
            _ => return Err(ExecutionError::WorkerChannelDisconnected),
        }
    }

    let mut inboxes = (0..config.workers).map(|_| Vec::new()).collect::<Vec<_>>();
    let mut previous_horizon = None;
    let mut rounds = Vec::<CpuRoundMetrics>::new();
    let mut round_number = 0_u64;
    while let Some(frontier_ns) = minima.iter().copied().flatten().min() {
        if u128::from(frontier_ns) >= run_end {
            break;
        }
        let round_start = Instant::now();
        let lookahead_end = minimum_lookahead_ns.map_or(TIME_AFTER_U64_MAX, |delay| {
            (u128::from(frontier_ns) + u128::from(delay)).min(TIME_AFTER_U64_MAX)
        });
        let exclusive_horizon_ns = run_end.min(lookahead_end);
        let horizon_advance_ns = exclusive_horizon_ns
            .saturating_sub(previous_horizon.unwrap_or(u128::from(frontier_ns)));
        let remote_floor_ns = previous_horizon.unwrap_or(u128::from(frontier_ns));

        let partition_started = Instant::now();
        for (worker, command) in commands.iter().enumerate() {
            send_command(
                command,
                OwnedWorkerCommand::RunRound {
                    round: round_number,
                    round_start,
                    exclusive_horizon_ns,
                    remote_floor_ns,
                    inbox: std::mem::take(&mut inboxes[worker]),
                    outbox_capacity: config.max_outbox_events_per_lp,
                    fault: config.fault_injection,
                    workers: config.workers,
                },
            )?;
        }
        let coordinator_partition_ns = elapsed_ns(partition_started);

        let wait_started = Instant::now();
        let mut completed = vec![false; config.workers];
        let mut next_local_minima = vec![None; config.workers];
        let mut worker_busy_ns = vec![0_u64; config.workers];
        let mut work_by_worker = (0..config.workers).map(|_| Vec::new()).collect::<Vec<_>>();
        let mut executed_lps = Vec::new();
        let mut remote_runs = Vec::new();
        let mut inbound_frontier_updates = 0_u64;
        let mut inbound_frontier_heap_pops = 0_u64;
        let mut inbound_physical_lp_probes = 0_u64;
        let mut frontier_updates = 0_u64;
        let mut frontier_heap_pops = 0_u64;
        let mut physical_lp_probes = 0_u64;
        let mut owner_batch_messages = 0_u64;
        for _ in 0..config.workers {
            match receive_owned_reply(replies, config.spin_before_park)? {
                OwnedWorkerReply::RoundComplete {
                    worker,
                    round,
                    executed,
                    work_estimates,
                    remote_by_owner,
                    inbound_frontier_updates: inbound_updates,
                    inbound_heap_pops,
                    inbound_physical_lp_probes: inbound_probes,
                    frontier_updates: updates,
                    heap_pops,
                    physical_lp_probes: probes,
                    minimum_ns,
                    busy_ns,
                } if round == round_number && !completed[worker] => {
                    completed[worker] = true;
                    next_local_minima[worker] = minimum_ns;
                    worker_busy_ns[worker] = busy_ns;
                    work_by_worker[worker] = work_estimates;
                    executed_lps.extend(executed);
                    inbound_frontier_updates =
                        inbound_frontier_updates.saturating_add(inbound_updates);
                    inbound_frontier_heap_pops =
                        inbound_frontier_heap_pops.saturating_add(inbound_heap_pops);
                    inbound_physical_lp_probes =
                        inbound_physical_lp_probes.saturating_add(inbound_probes);
                    frontier_updates = frontier_updates.saturating_add(updates);
                    frontier_heap_pops = frontier_heap_pops.saturating_add(heap_pops);
                    physical_lp_probes = physical_lp_probes.saturating_add(probes);
                    for (owner, batch) in remote_by_owner.into_iter().enumerate() {
                        if batch.is_empty() {
                            continue;
                        }
                        owner_batch_messages = owner_batch_messages.saturating_add(1);
                        remote_runs.push((owner, batch));
                    }
                }
                OwnedWorkerReply::Failed { error } => return Err(error),
                _ => return Err(ExecutionError::WorkerChannelDisconnected),
            }
        }
        let worker_wait_ns = elapsed_ns(wait_started);
        // Resident workers merge the preceding barrier's inbox on this wake. Keep the physical
        // timing here, but attribute its deterministic frontier work to the producing round.
        if let Some(previous) = rounds.last_mut() {
            previous.semantic.frontier_updates = previous
                .semantic
                .frontier_updates
                .saturating_add(inbound_frontier_updates);
            previous.semantic.frontier_heap_pops = previous
                .semantic
                .frontier_heap_pops
                .saturating_add(inbound_frontier_heap_pops);
            previous.semantic.physical_lp_probes = previous
                .semantic
                .physical_lp_probes
                .saturating_add(inbound_physical_lp_probes);
        }

        let exchange_started = Instant::now();
        let mut inbox_minima: Vec<Option<u64>> = vec![None; config.workers];
        for (owner, batch) in remote_runs {
            for envelope in &batch {
                inbox_minima[owner] = match inbox_minima[owner] {
                    Some(current) => Some(current.min(envelope.event.key.time_ns)),
                    None => Some(envelope.event.key.time_ns),
                };
            }
            inboxes[owner].extend(batch);
        }
        for worker in 0..config.workers {
            minima[worker] = match (next_local_minima[worker], inbox_minima[worker]) {
                (Some(local), Some(remote)) => Some(local.min(remote)),
                (Some(local), None) => Some(local),
                (None, Some(remote)) => Some(remote),
                (None, None) => None,
            };
        }
        let coordinator_exchange_ns = elapsed_ns(exchange_started);

        let mut lp_work = Vec::with_capacity(executed_lps.len());
        let mut lp_timings = Vec::with_capacity(executed_lps.len());
        let mut events_processed = 0_u64;
        let mut messages_exchanged = 0_u64;
        for executed in executed_lps {
            events_processed = events_processed
                .checked_add(executed.work.events_processed)
                .ok_or(ExecutionError::CounterOverflow(executed.work.node))?;
            messages_exchanged = messages_exchanged
                .checked_add(executed.messages_exchanged)
                .ok_or(ExecutionError::CounterOverflow(executed.work.node))?;
            lp_work.push(executed.work);
            lp_timings.push(executed.timing);
        }
        radix_sort_by_node(&mut lp_work, |work| work.node);
        radix_sort_by_node(&mut lp_timings, |timing| timing.node);
        let active_lp_count = lp_work.len();
        let max_work = lp_work
            .iter()
            .map(|work| work.events_processed)
            .max()
            .unwrap_or(0);
        let parallel_efficiency = if active_lp_count == 0 || max_work == 0 {
            1.0
        } else {
            events_processed as f64 / (active_lp_count as f64 * max_work as f64)
        };
        let lp_time_parallel_efficiency = efficiency(
            lp_timings.iter().map(|timing| timing.busy_ns),
            active_lp_count,
        );
        let worker_parallel_efficiency = efficiency(worker_busy_ns.iter().copied(), config.workers);
        let round_wall_time_ns = elapsed_ns(round_start);
        let worker_timings = worker_busy_ns
            .iter()
            .copied()
            .enumerate()
            .map(|(worker, busy_ns)| WorkerRoundTiming {
                worker,
                busy_ns,
                idle_ns: round_wall_time_ns.saturating_sub(busy_ns),
            })
            .collect::<Vec<_>>();
        let total_worker_busy = worker_busy_ns
            .iter()
            .copied()
            .fold(0_u128, |total, busy| total + u128::from(busy));
        let available_worker_time =
            u128::from(round_wall_time_ns).saturating_mul(config.workers as u128);
        let worker_utilization = if available_worker_time == 0 {
            1.0
        } else {
            total_worker_busy as f64 / available_worker_time as f64
        };
        let partition = WorkPartition {
            stragglers: Vec::new(),
            bulk_chunks: work_by_worker
                .into_iter()
                .filter(|chunk| !chunk.is_empty())
                .collect(),
            reserved_straggler_workers: Vec::new(),
        };
        rounds.push(CpuRoundMetrics {
            semantic: RoundMetrics {
                frontier_ns,
                exclusive_horizon_ns,
                horizon_advance_ns,
                events_processed,
                active_lp_count,
                lp_work,
                parallel_efficiency,
                messages_exchanged,
                frontier_updates,
                frontier_heap_pops,
                physical_lp_probes,
            },
            partition,
            owner_batch_messages,
            worker_wake_messages: config.workers as u64,
            worker_completion_messages: config.workers as u64,
            chunk_request_messages: 0,
            owner_delivery_messages: 0,
            lp_timings,
            worker_timings,
            lp_time_parallel_efficiency,
            worker_parallel_efficiency,
            worker_utilization,
            coordinator_partition_ns,
            worker_wait_ns,
            coordinator_exchange_ns,
            round_wall_time_ns,
        });
        previous_horizon = Some(exclusive_horizon_ns);
        round_number = round_number
            .checked_add(1)
            .ok_or(ExecutionError::CounterOverflow(NodeId(0)))?;
    }

    let remote_floor_ns = previous_horizon.unwrap_or(0);
    for (worker, command) in commands.iter().enumerate() {
        send_command(
            command,
            OwnedWorkerCommand::Finish {
                remote_floor_ns,
                inbox: std::mem::take(&mut inboxes[worker]),
                workers: config.workers,
            },
        )?;
    }
    let mut lps = Vec::with_capacity(image.nodes.len());
    let mut finished = vec![false; config.workers];
    let mut inbound_frontier_updates = 0_u64;
    let mut inbound_frontier_heap_pops = 0_u64;
    let mut inbound_physical_lp_probes = 0_u64;
    for _ in 0..config.workers {
        match receive_owned_reply(replies, 0)? {
            OwnedWorkerReply::Finished {
                worker,
                lps: worker_lps,
                inbound_frontier_updates: inbound_updates,
                inbound_heap_pops,
                inbound_physical_lp_probes: inbound_probes,
            } if !finished[worker] => {
                finished[worker] = true;
                lps.extend(worker_lps);
                inbound_frontier_updates = inbound_frontier_updates.saturating_add(inbound_updates);
                inbound_frontier_heap_pops =
                    inbound_frontier_heap_pops.saturating_add(inbound_heap_pops);
                inbound_physical_lp_probes =
                    inbound_physical_lp_probes.saturating_add(inbound_probes);
            }
            OwnedWorkerReply::Failed { error } => return Err(error),
            _ => return Err(ExecutionError::WorkerChannelDisconnected),
        }
    }
    // Messages at or beyond run_end are merged only for final-state assembly.
    if let Some(last) = rounds.last_mut() {
        last.semantic.frontier_updates = last
            .semantic
            .frontier_updates
            .saturating_add(inbound_frontier_updates);
        last.semantic.frontier_heap_pops = last
            .semantic
            .frontier_heap_pops
            .saturating_add(inbound_frontier_heap_pops);
        last.semantic.physical_lp_probes = last
            .semantic
            .physical_lp_probes
            .saturating_add(inbound_physical_lp_probes);
    }
    Ok(CpuRun {
        result: assemble_result(image, lps)?,
        rounds,
    })
}

fn partition_round<'image>(
    mut active: Vec<ActiveLp<'image>>,
    config: CpuConfig,
) -> Result<RoundAssignment<'image>, ExecutionError> {
    radix_order_lpt(&mut active);
    let mut stragglers = Vec::new();
    let mut bulk = Vec::new();
    for item in active {
        if config
            .straggler_threshold_events
            .is_some_and(|threshold| item.estimated_events > threshold)
        {
            stragglers.push(item);
        } else {
            bulk.push(item);
        }
    }

    let dedicated_count = if stragglers.is_empty() {
        0
    } else {
        let available = if bulk.is_empty() {
            config.workers
        } else {
            config.workers.saturating_sub(1).max(1)
        };
        config
            .dedicated_straggler_workers
            .min(stragglers.len())
            .min(available)
    };
    let dedicated_workers = (0..dedicated_count).collect::<Vec<_>>();
    let mut bulk_workers = (dedicated_count..config.workers).collect::<Vec<_>>();
    if bulk_workers.is_empty() && !bulk.is_empty() {
        bulk_workers.push(0);
    }

    let bulk_chunk_size = match config.granularity {
        ChunkGranularity::Static => {
            let divisor = bulk_workers.len().max(1);
            bulk.len().div_ceil(divisor).max(1)
        }
        ChunkGranularity::Fixed(size) => size,
    };
    let mut bulk_chunks = VecDeque::new();
    let mut bulk = bulk.into_iter();
    loop {
        let items = bulk.by_ref().take(bulk_chunk_size).collect::<Vec<_>>();
        if items.is_empty() {
            break;
        }
        bulk_chunks.push_back(WorkChunk {
            class: WorkClass::Bulk,
            items,
        });
    }
    let straggler_chunks = stragglers
        .into_iter()
        .map(|item| WorkChunk {
            class: WorkClass::Straggler,
            items: vec![item],
        })
        .collect::<VecDeque<_>>();
    let partition = WorkPartition {
        stragglers: straggler_chunks
            .iter()
            .flat_map(|chunk| &chunk.items)
            .map(work_estimate)
            .collect(),
        bulk_chunks: bulk_chunks
            .iter()
            .map(|chunk| chunk.items.iter().map(work_estimate).collect())
            .collect(),
        reserved_straggler_workers: dedicated_workers.clone(),
    };
    Ok(RoundAssignment {
        partition,
        straggler_chunks,
        bulk_chunks,
        dedicated_workers,
        bulk_workers,
    })
}

fn radix_order_lpt(items: &mut Vec<ActiveLp<'_>>) {
    if items.len() < 2 {
        return;
    }
    let mut source = items.drain(..).map(Some).collect::<Vec<_>>();
    let mut scratch = (0..source.len()).map(|_| None).collect::<Vec<_>>();
    for pass in 0..8 {
        let mut counts = [0_usize; 256];
        for item in source.iter().flatten() {
            let inverted = u64::MAX - item.estimated_events;
            counts[((inverted >> (pass * 8)) & 0xff) as usize] += 1;
        }
        let mut offset = 0;
        for count in &mut counts {
            let next = offset + *count;
            *count = offset;
            offset = next;
        }
        for item in &mut source {
            let item = item.take().expect("radix source slots are populated");
            let inverted = u64::MAX - item.estimated_events;
            let byte = ((inverted >> (pass * 8)) & 0xff) as usize;
            scratch[counts[byte]] = Some(item);
            counts[byte] += 1;
        }
        std::mem::swap(&mut source, &mut scratch);
    }
    items.extend(
        source
            .into_iter()
            .map(|item| item.expect("radix output slots are populated")),
    );
}

fn work_estimate(item: &ActiveLp<'_>) -> LpWorkEstimate {
    LpWorkEstimate {
        node: item.lp.node.id,
        estimated_events: item.estimated_events,
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_assignment<'image>(
    mut assignment: RoundAssignment<'image>,
    round: u64,
    round_start: Instant,
    exclusive_horizon_ns: u128,
    config: CpuConfig,
    commands: &[Sender<WorkerCommand<'image>>],
    replies: &Receiver<WorkerReply<'image>>,
    worker_busy_ns: &mut [u64],
) -> Result<RoundExecution<'image>, ExecutionError> {
    let total_chunks = assignment.straggler_chunks.len() + assignment.bulk_chunks.len();
    let mut execution = RoundExecution::new(config.workers);
    if total_chunks == 0 {
        return Ok(execution);
    }
    let mut inflight = vec![None; config.workers];
    let mut bulk_worker = vec![false; config.workers];
    for worker in assignment.bulk_workers.iter().copied() {
        bulk_worker[worker] = true;
    }
    let mut dispatch_order = 0_u64;
    let mut awaiting_straggler_start = vec![false; config.workers];
    let mut awaiting_straggler_starts = 0_usize;
    for worker in assignment.dedicated_workers.iter().copied() {
        if let Some(chunk) = assignment.straggler_chunks.pop_front() {
            awaiting_straggler_start[worker] = true;
            awaiting_straggler_starts += 1;
            dispatch_chunk(
                worker,
                chunk,
                round,
                round_start,
                exclusive_horizon_ns,
                config,
                commands,
                &mut inflight,
                &mut dispatch_order,
            )?;
        }
    }

    let mut deferred_completions = VecDeque::new();
    while awaiting_straggler_starts != 0 {
        match receive_reply_spin(replies, config.spin_before_park)? {
            WorkerReply::ChunkStarted {
                worker,
                round: reply_round,
                class: WorkClass::Straggler,
            } if reply_round == round && awaiting_straggler_start[worker] => {
                awaiting_straggler_start[worker] = false;
                awaiting_straggler_starts -= 1;
            }
            completion @ WorkerReply::ChunkComplete {
                round: reply_round, ..
            } if reply_round == round => deferred_completions.push_back(completion),
            WorkerReply::Failed { error } => return Err(error),
            _ => return Err(ExecutionError::WorkerChannelDisconnected),
        }
    }

    for worker in assignment.bulk_workers.iter().copied() {
        if inflight[worker].is_none() {
            if let Some(chunk) = assignment.bulk_chunks.pop_front() {
                dispatch_chunk(
                    worker,
                    chunk,
                    round,
                    round_start,
                    exclusive_horizon_ns,
                    config,
                    commands,
                    &mut inflight,
                    &mut dispatch_order,
                )?;
            }
        }
    }

    let mut completed_chunks = 0_usize;
    while completed_chunks < total_chunks {
        let reply = deferred_completions
            .pop_front()
            .map_or_else(|| receive_reply_spin(replies, config.spin_before_park), Ok)?;
        match reply {
            WorkerReply::ChunkStarted {
                worker,
                round: reply_round,
                class,
            } if reply_round == round && inflight[worker] == Some(class) => {}
            WorkerReply::ChunkComplete {
                worker,
                round: reply_round,
                class,
                executed,
                returned,
                remote_by_owner,
                busy_ns,
            } if reply_round == round && inflight[worker] == Some(class) => {
                inflight[worker] = None;
                worker_busy_ns[worker] = worker_busy_ns[worker].saturating_add(busy_ns);
                completed_chunks += 1;
                execution.executed.extend(executed);
                for state in returned {
                    execution.returned_by_owner[state.owner_worker].push(state);
                }
                for (owner, batch) in remote_by_owner.into_iter().enumerate() {
                    execution.remote_by_source_and_owner[worker][owner].extend(batch);
                }

                let next = match class {
                    WorkClass::Straggler => assignment.straggler_chunks.pop_front(),
                    WorkClass::Bulk => assignment.bulk_chunks.pop_front(),
                };
                if let Some(chunk) = next {
                    dispatch_chunk(
                        worker,
                        chunk,
                        round,
                        round_start,
                        exclusive_horizon_ns,
                        config,
                        commands,
                        &mut inflight,
                        &mut dispatch_order,
                    )?;
                } else if class == WorkClass::Straggler
                    && assignment.straggler_chunks.is_empty()
                    && !assignment.bulk_chunks.is_empty()
                    && bulk_worker[worker]
                {
                    let chunk = assignment
                        .bulk_chunks
                        .pop_front()
                        .expect("bulk work was established");
                    dispatch_chunk(
                        worker,
                        chunk,
                        round,
                        round_start,
                        exclusive_horizon_ns,
                        config,
                        commands,
                        &mut inflight,
                        &mut dispatch_order,
                    )?;
                }
            }
            WorkerReply::Failed { error } => return Err(error),
            _ => return Err(ExecutionError::WorkerChannelDisconnected),
        }
    }
    Ok(execution)
}

#[allow(clippy::too_many_arguments)]
fn dispatch_chunk<'image>(
    worker: usize,
    chunk: WorkChunk<'image>,
    round: u64,
    round_start: Instant,
    exclusive_horizon_ns: u128,
    config: CpuConfig,
    commands: &[Sender<WorkerCommand<'image>>],
    inflight: &mut [Option<WorkClass>],
    dispatch_order: &mut u64,
) -> Result<(), ExecutionError> {
    if inflight[worker].is_some() {
        return Err(ExecutionError::WorkerFailed { worker, round });
    }
    let item_count =
        u64::try_from(chunk.items.len()).map_err(|_| ExecutionError::CounterOverflow(NodeId(0)))?;
    send_command(
        &commands[worker],
        WorkerCommand::Execute {
            round,
            round_start,
            exclusive_horizon_ns,
            class: chunk.class,
            first_dispatch_order: *dispatch_order,
            items: chunk.items,
            outbox_capacity: config.max_outbox_events_per_lp,
            fault: config.fault_injection,
            workers: config.workers,
        },
    )?;
    inflight[worker] = Some(chunk.class);
    *dispatch_order = dispatch_order
        .checked_add(item_count)
        .ok_or(ExecutionError::CounterOverflow(NodeId(0)))?;
    Ok(())
}

fn build_lps<'image>(
    image: &'image SimulationImage,
    observation_mode: ObservationMode,
) -> Result<Vec<CpuLp<'image>>, ExecutionError> {
    let descriptors = image
        .initial_packets
        .iter()
        .copied()
        .map(|packet| (packet.id, packet))
        .collect::<BTreeMap<_, _>>();
    if descriptors.len() != image.initial_packets.len() {
        let mut seen = BTreeSet::new();
        let duplicate = image
            .initial_packets
            .iter()
            .find(|packet| !seen.insert(packet.id))
            .expect("descriptor count established a duplicate");
        return Err(ExecutionError::DuplicatePayload(duplicate.id));
    }
    let mut futures = (0..image.nodes.len())
        .map(|_| BTreeMap::new())
        .collect::<Vec<_>>();
    let mut packets = (0..image.nodes.len())
        .map(|_| BTreeMap::new())
        .collect::<Vec<BTreeMap<PayloadId, PacketDescriptor>>>();
    let mut pinned_packets = (0..image.nodes.len())
        .map(|_| BTreeSet::new())
        .collect::<Vec<_>>();
    let meaningful_event_payloads = image
        .initial_events
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                crate::EventKind::PacketArrival | crate::EventKind::RemoteArrival
            )
        })
        .map(|event| event.payload)
        .collect::<BTreeSet<_>>();
    let completion_payloads = image
        .initial_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::TxComplete)
        .map(|event| event.payload)
        .collect::<BTreeSet<_>>();
    let mut queued_payloads = BTreeSet::new();
    let mut in_service_payloads = BTreeSet::new();
    let mut pending_keys = BTreeSet::new();
    for event in image.initial_events.iter().copied() {
        if !pending_keys.insert(event.key) {
            return Err(ExecutionError::DuplicateEventKey(event.key));
        }
        let lp_slot =
            node_slot(image, event.target).ok_or(ExecutionError::UnknownNode(event.target))?;
        if futures[lp_slot].insert(event.key, event).is_some() {
            return Err(ExecutionError::DuplicateEventKey(event.key));
        }
        let descriptor = descriptors
            .get(&event.payload)
            .copied()
            .ok_or(ExecutionError::UnknownPacket(event.payload))?;
        packets[lp_slot].insert(descriptor.id, descriptor);
    }
    for (lp_slot, node) in image.nodes.iter().copied().enumerate() {
        let (queue, in_service) = match node.kind {
            NodeKind::Host => {
                let state = image.host_states.get(node.state_slot as usize).ok_or(
                    ExecutionError::InvalidStateSlot {
                        node: node.id,
                        kind: node.kind,
                        state_slot: node.state_slot,
                    },
                )?;
                (
                    state.queue.iter().copied().collect::<Vec<_>>(),
                    state.in_service.into_iter().collect::<Vec<_>>(),
                )
            }
            NodeKind::Switch => {
                let state = image.switch_states.get(node.state_slot as usize).ok_or(
                    ExecutionError::InvalidStateSlot {
                        node: node.id,
                        kind: node.kind,
                        state_slot: node.state_slot,
                    },
                )?;
                (
                    state
                        .queues
                        .iter()
                        .flat_map(|queue| queue.queue.iter().copied())
                        .collect(),
                    state
                        .queues
                        .iter()
                        .filter_map(|queue| queue.in_service)
                        .collect(),
                )
            }
        };
        queued_payloads.extend(queue.iter().copied());
        in_service_payloads.extend(in_service.iter().copied());
        for payload in queue.into_iter().chain(in_service) {
            let descriptor = descriptors
                .get(&payload)
                .copied()
                .ok_or(ExecutionError::UnknownPacket(payload))?;
            packets[lp_slot].insert(payload, descriptor);
        }
    }
    for descriptor in image.initial_packets.iter().copied() {
        let needs_orphan_retention = !meaningful_event_payloads.contains(&descriptor.id)
            && !queued_payloads.contains(&descriptor.id)
            && (!in_service_payloads.contains(&descriptor.id)
                || completion_payloads.contains(&descriptor.id));
        if needs_orphan_retention {
            if let Some(lp_slot) = packets
                .iter()
                .position(|owned| owned.contains_key(&descriptor.id))
            {
                pinned_packets[lp_slot].insert(descriptor.id);
                continue;
            }
            let flow = image
                .flows
                .get(descriptor.flow.0 as usize)
                .filter(|flow| flow.id == descriptor.flow)
                .ok_or(ExecutionError::UnknownFlow(descriptor.flow))?;
            let lp_slot =
                node_slot(image, flow.source).ok_or(ExecutionError::UnknownNode(flow.source))?;
            packets[lp_slot].insert(descriptor.id, descriptor);
            pinned_packets[lp_slot].insert(descriptor.id);
        } else if packets
            .iter()
            .all(|owned| !owned.contains_key(&descriptor.id))
        {
            let flow = image
                .flows
                .get(descriptor.flow.0 as usize)
                .filter(|flow| flow.id == descriptor.flow)
                .ok_or(ExecutionError::UnknownFlow(descriptor.flow))?;
            let lp_slot =
                node_slot(image, flow.source).ok_or(ExecutionError::UnknownNode(flow.source))?;
            packets[lp_slot].insert(descriptor.id, descriptor);
        }
    }

    image
        .nodes
        .iter()
        .copied()
        .enumerate()
        .map(|(lp_slot, node)| {
            Ok(CpuLp {
                lp_slot,
                node,
                transitions: TransitionState::new_local(
                    image,
                    node,
                    std::mem::take(&mut packets[lp_slot]).into_values(),
                    observation_mode,
                )?,
                futures: std::mem::take(&mut futures[lp_slot]),
                pinned_packets: std::mem::take(&mut pinned_packets[lp_slot]),
            })
        })
        .collect()
}

fn partition_initial_ownership<'image>(
    lps: Vec<CpuLp<'image>>,
    workers: usize,
) -> Result<Vec<WorkerShard<'image>>, ExecutionError> {
    let mut owned = (0..workers)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<CpuLp<'image>>>>();
    for lp in lps {
        owned[lp.lp_slot % workers].push(lp);
    }
    owned
        .into_iter()
        .enumerate()
        .map(|(worker, lps)| WorkerShard::new(worker, lps))
        .collect()
}

fn finish_workers<'image>(
    image: &SimulationImage,
    commands: &[Sender<WorkerCommand<'image>>],
    replies: &Receiver<WorkerReply<'image>>,
    rounds: Vec<CpuRoundMetrics>,
) -> Result<CpuRun, ExecutionError> {
    for command in commands {
        send_command(command, WorkerCommand::Finish)?;
    }
    let mut by_worker = (0..commands.len())
        .map(|_| None)
        .collect::<Vec<Option<Vec<CpuLp<'image>>>>>();
    for _ in commands {
        match receive_reply(replies)? {
            WorkerReply::Finished { worker, lps } => by_worker[worker] = Some(lps),
            WorkerReply::Failed { error } => return Err(error),
            _ => return Err(ExecutionError::WorkerChannelDisconnected),
        }
    }
    let lps = by_worker
        .into_iter()
        .flatten()
        .flatten()
        .collect::<Vec<_>>();
    Ok(CpuRun {
        result: assemble_result(image, lps)?,
        rounds,
    })
}

fn assemble_result(
    image: &SimulationImage,
    lps: Vec<CpuLp<'_>>,
) -> Result<RunResult, ExecutionError> {
    let mut host_states = (0..image.host_states.len())
        .map(|_| None)
        .collect::<Vec<_>>();
    let mut switch_states = (0..image.switch_states.len())
        .map(|_| None)
        .collect::<Vec<_>>();
    let mut summary = RunSummary::default();
    let mut resident_packets = BTreeMap::new();
    let mut observed_packets = BTreeMap::new();
    let mut departures = Vec::new();
    let mut arrivals = Vec::new();
    let mut pending_events = Vec::new();

    for lp in lps {
        let mut referenced = lp.pinned_packets;
        referenced.extend(
            lp.futures
                .values()
                .filter(|event| event.kind != crate::EventKind::TxReady)
                .map(|event| event.payload),
        );
        pending_events.extend(lp.futures.into_values());
        let local = lp.transitions.finish_local();
        add_state_payloads(&local, &mut referenced);
        install_local_result(
            local,
            &referenced,
            &mut host_states,
            &mut switch_states,
            &mut summary,
            &mut resident_packets,
            &mut observed_packets,
            &mut departures,
            &mut arrivals,
        )?;
    }
    pending_events.sort_unstable_by_key(|event| event.key);
    departures.sort_unstable_by_key(|(key, _)| *key);
    arrivals.sort_unstable_by_key(|(key, _)| *key);
    Ok(RunResult {
        host_states: host_states
            .into_iter()
            .map(|state| state.expect("every host state has one LP owner"))
            .collect(),
        switch_states: switch_states
            .into_iter()
            .map(|state| state.expect("every switch state has one LP owner"))
            .collect(),
        summary,
        resident_packets: resident_packets.into_values().collect(),
        observed_packets: observed_packets.into_values().collect(),
        departures: departures
            .into_iter()
            .map(|(_, departure)| departure)
            .collect(),
        arrivals: arrivals.into_iter().map(|(_, arrival)| arrival).collect(),
        pending_events,
    })
}

fn add_state_payloads(local: &LocalTransitionResult, referenced: &mut BTreeSet<PayloadId>) {
    match &local.state {
        LocalNodeState::Host(state) => {
            referenced.extend(state.queue.iter().copied());
            referenced.extend(state.in_service);
        }
        LocalNodeState::Switch(state) => {
            for queue in &state.queues {
                referenced.extend(queue.queue.iter().copied());
                referenced.extend(queue.in_service);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn install_local_result(
    local: LocalTransitionResult,
    referenced: &BTreeSet<PayloadId>,
    host_states: &mut [Option<crate::HostState>],
    switch_states: &mut [Option<crate::SwitchState>],
    summary: &mut RunSummary,
    resident_packets: &mut BTreeMap<PayloadId, PacketDescriptor>,
    observed_packets: &mut BTreeMap<PayloadId, PacketDescriptor>,
    departures: &mut Vec<(EventKey, PacketDeparture)>,
    arrivals: &mut Vec<(EventKey, PacketArrivalObservation)>,
) -> Result<(), ExecutionError> {
    match local.state {
        LocalNodeState::Host(state) => {
            let slot = local.node.state_slot as usize;
            if host_states
                .get_mut(slot)
                .and_then(|entry| entry.replace(state))
                .is_some()
            {
                return Err(ExecutionError::InvalidStateSlot {
                    node: local.node.id,
                    kind: local.node.kind,
                    state_slot: local.node.state_slot,
                });
            }
        }
        LocalNodeState::Switch(state) => {
            let slot = local.node.state_slot as usize;
            if switch_states
                .get_mut(slot)
                .and_then(|entry| entry.replace(state))
                .is_some()
            {
                return Err(ExecutionError::InvalidStateSlot {
                    node: local.node.id,
                    kind: local.node.kind,
                    state_slot: local.node.state_slot,
                });
            }
        }
    }
    add_summary_checked(summary, local.summary, local.node.id)?;
    for packet in local
        .resident_packets
        .into_iter()
        .filter(|packet| referenced.contains(&packet.id))
    {
        if let Some(existing) = resident_packets.insert(packet.id, packet) {
            if existing != packet {
                return Err(ExecutionError::DuplicatePayload(packet.id));
            }
        }
    }
    for packet in local.observed_packets {
        if let Some(existing) = observed_packets.insert(packet.id, packet) {
            if existing != packet {
                return Err(ExecutionError::DuplicatePayload(packet.id));
            }
        }
    }
    departures.extend(local.departures);
    arrivals.extend(local.arrivals);
    Ok(())
}

fn add_summary_checked(
    total: &mut RunSummary,
    local: RunSummary,
    node: NodeId,
) -> Result<(), ExecutionError> {
    macro_rules! add {
        ($field:ident) => {
            total.$field = total
                .$field
                .checked_add(local.$field)
                .ok_or(ExecutionError::CounterOverflow(node))?;
        };
    }
    add!(sourced_packets);
    add!(sourced_bytes);
    add!(departed_packets);
    add!(departed_bytes);
    add!(admitted_packets);
    add!(admitted_bytes);
    add!(received_packets);
    add!(received_bytes);
    add!(dropped_packets);
    add!(dropped_bytes);
    add!(feedback_packets);
    add!(feedback_bytes);
    Ok(())
}

fn radix_sort_remote_envelopes(events: &mut Vec<RemoteEnvelope>) {
    if events.len() < 2 {
        return;
    }
    let varying_bytes = remote_envelope_varying_bytes(events);
    let mut scratch = vec![events[0]; events.len()];
    for pass in 0..REMOTE_ORDER_BYTES {
        if varying_bytes & (1_u64 << pass) == 0 {
            continue;
        }
        let mut counts = [0_usize; 256];
        for event in events.iter() {
            counts[usize::from(remote_order_byte(event.event, pass))] += 1;
        }
        let mut offset = 0;
        for count in &mut counts {
            let next = offset + *count;
            *count = offset;
            offset = next;
        }
        for event in events.iter().copied() {
            let byte = usize::from(remote_order_byte(event.event, pass));
            scratch[counts[byte]] = event;
            counts[byte] += 1;
        }
        std::mem::swap(events, &mut scratch);
    }
}

fn remote_envelope_varying_bytes(events: &[RemoteEnvelope]) -> u64 {
    let first = events[0].event;
    let mut target = 0_u64;
    let mut time = 0_u64;
    let mut phase = 0_u16;
    let mut origin_node = 0_u64;
    let mut origin_seq = 0_u64;
    for envelope in &events[1..] {
        let event = envelope.event;
        target |= first.target.0 ^ event.target.0;
        time |= first.key.time_ns ^ event.key.time_ns;
        phase |= first.key.phase ^ event.key.phase;
        origin_node |= first.key.origin_node.0 ^ event.key.origin_node.0;
        origin_seq |= first.key.origin_seq ^ event.key.origin_seq;
    }
    varying_byte_mask(origin_seq, 0)
        | varying_byte_mask(origin_node, 8)
        | varying_byte_mask(u64::from(phase), 16)
        | varying_byte_mask(time, 18)
        | varying_byte_mask(target, 26)
}

fn varying_byte_mask(value: u64, first_pass: usize) -> u64 {
    let mut mask = 0_u64;
    for byte in 0..8 {
        if value & (0xff_u64 << (byte * 8)) != 0 {
            mask |= 1_u64 << (first_pass + byte);
        }
    }
    mask
}

fn radix_sort_by_node<T: Copy>(items: &mut Vec<T>, node: impl Fn(T) -> NodeId + Copy) {
    if items.len() < 2 {
        return;
    }
    let mut scratch = vec![items[0]; items.len()];
    for pass in 0..8 {
        let mut counts = [0_usize; 256];
        for item in items.iter().copied() {
            let byte = ((node(item).0 >> (pass * 8)) & 0xff) as u8;
            counts[usize::from(byte)] += 1;
        }
        let mut offset = 0;
        for count in &mut counts {
            let next = offset + *count;
            *count = offset;
            offset = next;
        }
        for item in items.iter().copied() {
            let byte = ((node(item).0 >> (pass * 8)) & 0xff) as u8;
            let index = usize::from(byte);
            scratch[counts[index]] = item;
            counts[index] += 1;
        }
        std::mem::swap(items, &mut scratch);
    }
}

const fn remote_order_byte(event: Event, pass: usize) -> u8 {
    let (value, byte) = match pass {
        0..=7 => (event.key.origin_seq, pass),
        8..=15 => (event.key.origin_node.0, pass - 8),
        16..=17 => (event.key.phase as u64, pass - 16),
        18..=25 => (event.key.time_ns, pass - 18),
        26..=33 => (event.target.0, pass - 26),
        _ => (0, 0),
    };
    ((value >> (byte * 8)) & 0xff) as u8
}

fn efficiency(values: impl Iterator<Item = u64>, count: usize) -> f64 {
    let (total, maximum) = values.fold((0_u128, 0_u64), |(total, maximum), value| {
        (total + u128::from(value), maximum.max(value))
    });
    if count == 0 || maximum == 0 {
        1.0
    } else {
        total as f64 / (count as f64 * maximum as f64)
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    duration_ns(started.elapsed())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn receive_spin_then_park<T>(
    receiver: &Receiver<T>,
    spin_before_park: u32,
) -> Result<T, RecvError> {
    for _ in 0..spin_before_park {
        match receiver.try_recv() {
            Ok(value) => return Ok(value),
            Err(TryRecvError::Empty) => spin_loop(),
            Err(TryRecvError::Disconnected) => return receiver.recv(),
        }
    }
    receiver.recv()
}

fn send_command<T>(sender: &Sender<T>, command: T) -> Result<(), ExecutionError> {
    sender
        .send(command)
        .map_err(|_| ExecutionError::WorkerChannelDisconnected)
}

fn receive_reply<T>(receiver: &Receiver<T>) -> Result<T, ExecutionError> {
    receiver
        .recv()
        .map_err(|_| ExecutionError::WorkerChannelDisconnected)
}

fn receive_reply_spin<T>(
    receiver: &Receiver<T>,
    spin_before_park: u32,
) -> Result<T, ExecutionError> {
    receive_spin_then_park(receiver, spin_before_park)
        .map_err(|_| ExecutionError::WorkerChannelDisconnected)
}

fn receive_owned_reply<'image>(
    receiver: &Receiver<OwnedWorkerReply<'image>>,
    spin_before_park: u32,
) -> Result<OwnedWorkerReply<'image>, ExecutionError> {
    receive_spin_then_park(receiver, spin_before_park)
        .map_err(|_| ExecutionError::WorkerChannelDisconnected)
}

fn drain_owned_worker_errors(
    receiver: &Receiver<OwnedWorkerReply<'_>>,
    initial: ExecutionError,
) -> ExecutionError {
    let mut selected = initial;
    while let Ok(reply) = receiver.recv() {
        if let OwnedWorkerReply::Failed { error } = reply {
            selected = prefer_worker_error(selected, error);
        }
    }
    selected
}

fn drain_worker_errors(
    receiver: &Receiver<WorkerReply<'_>>,
    initial: ExecutionError,
) -> ExecutionError {
    let mut selected = initial;
    while let Ok(reply) = receiver.recv() {
        if let WorkerReply::Failed { error } = reply {
            selected = prefer_worker_error(selected, error);
        }
    }
    selected
}

fn prefer_worker_error(current: ExecutionError, candidate: ExecutionError) -> ExecutionError {
    match (&current, &candidate) {
        (
            ExecutionError::WorkerPanicked {
                worker: current_worker,
            },
            ExecutionError::WorkerPanicked {
                worker: candidate_worker,
            },
        ) => {
            if candidate_worker < current_worker {
                candidate
            } else {
                current
            }
        }
        (_, ExecutionError::WorkerPanicked { .. }) => candidate,
        (ExecutionError::WorkerPanicked { .. }, _) => current,
        (ExecutionError::WorkerChannelDisconnected, _) => candidate,
        (_, ExecutionError::WorkerChannelDisconnected) => current,
        _ => current,
    }
}

fn node_slot(image: &SimulationImage, node: NodeId) -> Option<usize> {
    usize::try_from(node.0)
        .ok()
        .filter(|slot| image.nodes.get(*slot).is_some_and(|entry| entry.id == node))
}

impl<'image> WorkerShard<'image> {
    fn new(worker: usize, lps: Vec<CpuLp<'image>>) -> Result<Self, ExecutionError> {
        let mut frontier = OwnerFrontierIndex::new(lps.len());
        let mut setup_physical_lp_probes = 0;
        for (owner_slot, lp) in lps.iter().enumerate() {
            frontier.update(
                owner_slot,
                lp.node.id,
                lp.next_key(),
                &mut setup_physical_lp_probes,
            )?;
        }
        Ok(Self {
            worker,
            lps: lps.into_iter().map(Some).collect(),
            frontier,
        })
    }

    fn extract(
        &mut self,
        exclusive_horizon_ns: u128,
    ) -> Result<(Vec<ActiveLp<'image>>, u64, u64), ExecutionError> {
        let mut heap_pops = 0;
        let mut physical_lp_probes = 0_u64;
        let mut active = Vec::new();
        while let Some(entry) = self.frontier.pop_before(
            exclusive_horizon_ns,
            &mut heap_pops,
            &mut physical_lp_probes,
        ) {
            physical_lp_probes = physical_lp_probes.saturating_add(1);
            let lp = self.lps[entry.owner_slot]
                .take()
                .expect("a live frontier entry owns a resident LP");
            let estimated_events = lp.estimated_work(exclusive_horizon_ns)?;
            active.push(ActiveLp {
                owner_worker: self.worker,
                owner_slot: entry.owner_slot,
                estimated_events,
                lp,
            });
        }
        Ok((active, heap_pops, physical_lp_probes))
    }

    fn restore_and_merge(
        &mut self,
        ingress: &OwnerIngress,
        states: Vec<ReturnedLp<'image>>,
        exclusive_horizon_ns: u128,
        workers: usize,
    ) -> Result<(u64, u64, u64, Option<u64>), ExecutionError> {
        let mut frontier_updates = 0_u64;
        let mut heap_pops = 0_u64;
        let mut physical_lp_probes = 0_u64;
        let mut inbox = ingress.remote.try_iter().flatten().collect::<Vec<_>>();
        for state in states {
            let node = state.lp.node.id;
            let next = state.lp.next_key();
            physical_lp_probes = physical_lp_probes.saturating_add(1);
            if self.lps[state.owner_slot].replace(state.lp).is_some() {
                return Err(ExecutionError::WorkerFailed {
                    worker: self.worker,
                    round: 0,
                });
            }
            self.frontier
                .update(state.owner_slot, node, next, &mut physical_lp_probes)?;
            frontier_updates = frontier_updates.saturating_add(1);
        }

        radix_sort_remote_envelopes(&mut inbox);
        let mut offset = 0;
        while offset < inbox.len() {
            let target = inbox[offset].event.target;
            let global_slot =
                usize::try_from(target.0).map_err(|_| ExecutionError::UnknownNode(target))?;
            if global_slot % workers != self.worker {
                return Err(ExecutionError::UnknownNode(target));
            }
            let owner_slot = global_slot / workers;
            physical_lp_probes = physical_lp_probes.saturating_add(1);
            let lp = self
                .lps
                .get_mut(owner_slot)
                .and_then(Option::as_mut)
                .ok_or(ExecutionError::UnknownNode(target))?;
            if lp.node.id != target {
                return Err(ExecutionError::UnknownNode(target));
            }
            let mut end = offset + 1;
            while end < inbox.len() && inbox[end].event.target == target {
                end += 1;
            }
            for envelope in &inbox[offset..end] {
                if u128::from(envelope.event.key.time_ns) < exclusive_horizon_ns {
                    return Err(ExecutionError::RemoteEventBeforeHorizon {
                        key: envelope.event.key,
                        exclusive_horizon_ns,
                    });
                }
                lp.transitions.install_packet(envelope.packet)?;
                if lp
                    .futures
                    .insert(envelope.event.key, envelope.event)
                    .is_some()
                {
                    return Err(ExecutionError::DuplicateEventKey(envelope.event.key));
                }
            }
            self.frontier
                .update(owner_slot, target, lp.next_key(), &mut physical_lp_probes)?;
            frontier_updates = frontier_updates.saturating_add(1);
            offset = end;
        }

        let minimum_ns = if let Some(entry) = self
            .frontier
            .peek_min(&mut heap_pops, &mut physical_lp_probes)
        {
            if u128::from(entry.time_ns) < exclusive_horizon_ns {
                let key = self.lps[entry.owner_slot]
                    .as_ref()
                    .and_then(CpuLp::next_key)
                    .expect("a live frontier entry has a pending event");
                return Err(ExecutionError::EventBelowHorizonAfterDrain {
                    key,
                    exclusive_horizon_ns,
                });
            }
            Some(entry.time_ns)
        } else {
            None
        };
        Ok((frontier_updates, heap_pops, physical_lp_probes, minimum_ns))
    }
}

impl<'image> WorkerShard<'image> {
    fn merge_remote(
        &mut self,
        mut inbox: Vec<RemoteEnvelope>,
        remote_floor_ns: u128,
        workers: usize,
    ) -> Result<(u64, u64, u64), ExecutionError> {
        let mut frontier_updates = 0_u64;
        let heap_pops = 0_u64;
        let mut physical_lp_probes = 0_u64;
        radix_sort_remote_envelopes(&mut inbox);
        let mut offset = 0;
        while offset < inbox.len() {
            let target = inbox[offset].event.target;
            let global_slot =
                usize::try_from(target.0).map_err(|_| ExecutionError::UnknownNode(target))?;
            if global_slot % workers != self.worker {
                return Err(ExecutionError::UnknownNode(target));
            }
            let owner_slot = global_slot / workers;
            physical_lp_probes = physical_lp_probes.saturating_add(1);
            let lp = self
                .lps
                .get_mut(owner_slot)
                .and_then(Option::as_mut)
                .filter(|lp| lp.node.id == target)
                .ok_or(ExecutionError::UnknownNode(target))?;
            let mut end = offset + 1;
            while end < inbox.len() && inbox[end].event.target == target {
                end += 1;
            }
            for envelope in &inbox[offset..end] {
                if u128::from(envelope.event.key.time_ns) < remote_floor_ns {
                    return Err(ExecutionError::RemoteEventBeforeHorizon {
                        key: envelope.event.key,
                        exclusive_horizon_ns: remote_floor_ns,
                    });
                }
                lp.transitions.install_packet(envelope.packet)?;
                if lp
                    .futures
                    .insert(envelope.event.key, envelope.event)
                    .is_some()
                {
                    return Err(ExecutionError::DuplicateEventKey(envelope.event.key));
                }
            }
            self.frontier
                .update(owner_slot, target, lp.next_key(), &mut physical_lp_probes)?;
            frontier_updates = frontier_updates.saturating_add(1);
            offset = end;
        }
        Ok((frontier_updates, heap_pops, physical_lp_probes))
    }

    fn restore_owned(
        &mut self,
        states: Vec<ReturnedLp<'image>>,
        exclusive_horizon_ns: u128,
    ) -> Result<(u64, u64, u64, Option<u64>), ExecutionError> {
        let mut frontier_updates = 0_u64;
        let mut heap_pops = 0_u64;
        let mut physical_lp_probes = 0_u64;
        for state in states {
            let node = state.lp.node.id;
            let next = state.lp.next_key();
            physical_lp_probes = physical_lp_probes.saturating_add(1);
            if state.owner_worker != self.worker
                || self.lps[state.owner_slot].replace(state.lp).is_some()
            {
                return Err(ExecutionError::UnknownNode(node));
            }
            self.frontier
                .update(state.owner_slot, node, next, &mut physical_lp_probes)?;
            frontier_updates = frontier_updates.saturating_add(1);
        }
        let minimum_ns = self
            .frontier
            .peek_min(&mut heap_pops, &mut physical_lp_probes)
            .map(|entry| entry.time_ns);
        if minimum_ns.is_some_and(|time_ns| u128::from(time_ns) < exclusive_horizon_ns) {
            let entry = self
                .frontier
                .peek_min(&mut heap_pops, &mut physical_lp_probes)
                .expect("minimum was established");
            let key = self.lps[entry.owner_slot]
                .as_ref()
                .and_then(CpuLp::next_key)
                .expect("a live owner frontier entry has a pending event");
            return Err(ExecutionError::EventBelowHorizonAfterDrain {
                key,
                exclusive_horizon_ns,
            });
        }
        Ok((frontier_updates, heap_pops, physical_lp_probes, minimum_ns))
    }

    fn into_lps(self) -> Result<Vec<CpuLp<'image>>, ExecutionError> {
        self.lps
            .into_iter()
            .map(|lp| {
                lp.ok_or(ExecutionError::WorkerFailed {
                    worker: self.worker,
                    round: u64::MAX,
                })
            })
            .collect()
    }
}

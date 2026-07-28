//! Persistent whole-LP CPU worker pool for exact safe-horizon rounds.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, bounded, unbounded};

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
    pub lp_timings: Vec<LpExecutionTiming>,
    pub worker_timings: Vec<WorkerRoundTiming>,
    pub lp_time_parallel_efficiency: f64,
    pub worker_parallel_efficiency: f64,
    pub worker_utilization: f64,
    pub round_wall_time_ns: u64,
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
            let (returned_tx, returned_rx) = unbounded();
            let (remote_tx, remote_rx) = unbounded();
            routes.push(OwnerRoutes {
                returned: returned_tx,
                remote: remote_tx,
            });
            ingresses.push(Some(OwnerIngress {
                returned: returned_rx,
                remote: remote_rx,
            }));
        }
        let mut commands = Vec::with_capacity(config.workers);
        for shard in shards {
            let (command_tx, command_rx) = bounded(1);
            commands.push(command_tx);
            let worker_reply = reply_tx.clone();
            let worker_ingress = ingresses[shard.worker]
                .take()
                .expect("each worker owns one ingress receiver");
            let worker_routes = routes.clone();
            scope.spawn(move |_| {
                let worker = shard.worker;
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    worker_loop(
                        shard,
                        command_rx,
                        worker_ingress,
                        worker_routes,
                        &worker_reply,
                    )
                }));
                if outcome.is_err() {
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
        );
        if result.is_err() {
            for command in &commands {
                let _ = command.send(WorkerCommand::Shutdown);
            }
        }
        result
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
        while self
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

            let (_, event) = self
                .futures
                .pop_first()
                .expect("first_key_value established a pending event");
            let preserved_packet = if self.pinned_packets.contains(&event.payload) {
                Some(self.transitions.packet_descriptor(event.payload)?)
            } else {
                None
            };
            self.transitions.dispatch(event, &mut children)?;
            if let Some(packet) = preserved_packet {
                self.transitions.install_packet(packet)?;
            }
            for child in children.drain(..) {
                if child.target == self.node.id {
                    if self.futures.insert(child.key, child).is_some() {
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
    owner_slot: usize,
    lp: CpuLp<'image>,
}

#[derive(Clone)]
struct OwnerRoutes<'image> {
    returned: Sender<ReturnedLp<'image>>,
    remote: Sender<RemoteEnvelope>,
}

struct OwnerIngress<'image> {
    returned: Receiver<ReturnedLp<'image>>,
    remote: Receiver<RemoteEnvelope>,
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
    ) -> Result<(), ExecutionError> {
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

    fn peek_min(&mut self, heap_pops: &mut u64) -> Option<FrontierEntry> {
        loop {
            let entry = self.heap.peek().map(|entry| entry.0)?;
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
    ) -> Option<FrontierEntry> {
        let entry = self.peek_min(heap_pops)?;
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
    },
    Finish,
    Shutdown,
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
        busy_ns: u64,
    },
    MergeComplete {
        worker: usize,
        round: u64,
        frontier_updates: u64,
        heap_pops: u64,
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

fn worker_loop<'image>(
    mut shard: WorkerShard<'image>,
    commands: Receiver<WorkerCommand<'image>>,
    ingress: OwnerIngress<'image>,
    routes: Vec<OwnerRoutes<'image>>,
    replies: &Sender<WorkerReply<'image>>,
) {
    let mut initial_heap_pops = 0;
    let initial_minimum_ns = shard
        .frontier
        .peek_min(&mut initial_heap_pops)
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
    while let Ok(command) = commands.recv() {
        let result = match command {
            WorkerCommand::Extract {
                round,
                exclusive_horizon_ns,
            } => {
                let started = Instant::now();
                match shard.extract(exclusive_horizon_ns) {
                    Ok((active, heap_pops)) => replies.send(WorkerReply::Active {
                        worker: shard.worker,
                        round,
                        active,
                        heap_pops,
                        busy_ns: elapsed_ns(started),
                    }),
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
                    let returned = ReturnedLp {
                        owner_slot: active.owner_slot,
                        lp: active.lp,
                    };
                    if routes[owner_worker].returned.send(returned).is_err() {
                        let _ = replies.send(WorkerReply::Failed {
                            error: ExecutionError::WorkerChannelDisconnected,
                        });
                        return;
                    }
                    for envelope in outbox {
                        let Ok(target_slot) = usize::try_from(envelope.event.target.0) else {
                            let _ = replies.send(WorkerReply::Failed {
                                error: ExecutionError::UnknownNode(envelope.event.target),
                            });
                            return;
                        };
                        if routes[target_slot % workers].remote.send(envelope).is_err() {
                            let _ = replies.send(WorkerReply::Failed {
                                error: ExecutionError::WorkerChannelDisconnected,
                            });
                            return;
                        }
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
                    busy_ns: elapsed_ns(chunk_started),
                })
            }
            WorkerCommand::RestoreAndMerge {
                round,
                exclusive_horizon_ns,
                workers,
            } => {
                let started = Instant::now();
                match shard.restore_and_merge(&ingress, exclusive_horizon_ns, workers) {
                    Ok((frontier_updates, heap_pops, minimum_ns)) => {
                        replies.send(WorkerReply::MergeComplete {
                            worker: shard.worker,
                            round,
                            frontier_updates,
                            heap_pops,
                            minimum_ns,
                            busy_ns: elapsed_ns(started),
                        })
                    }
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
            WorkerCommand::Shutdown => return,
        };
        if result.is_err() {
            return;
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

fn run_coordinator<'image>(
    image: &'image SimulationImage,
    config: CpuConfig,
    run_end: u128,
    minimum_lookahead_ns: Option<u64>,
    commands: &[Sender<WorkerCommand<'image>>],
    replies: &Receiver<WorkerReply<'image>>,
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
            match receive_reply(replies)? {
                WorkerReply::Active {
                    worker,
                    round,
                    active,
                    heap_pops,
                    busy_ns,
                } if round == round_number => {
                    active_by_owner[worker] = Some(active);
                    worker_busy_ns[worker] = worker_busy_ns[worker].saturating_add(busy_ns);
                    frontier_heap_pops = frontier_heap_pops.saturating_add(heap_pops);
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
        let execution = execute_assignment(
            assignment,
            round_number,
            round_start,
            exclusive_horizon_ns,
            config,
            commands,
            replies,
            &mut worker_busy_ns,
        )?;

        let mut lp_work = Vec::with_capacity(execution.len());
        let mut lp_timings = Vec::with_capacity(execution.len());
        let mut events_processed = 0_u64;
        let mut messages_exchanged = 0_u64;
        for executed in execution {
            events_processed = events_processed
                .checked_add(executed.work.events_processed)
                .ok_or(ExecutionError::CounterOverflow(executed.work.node))?;
            messages_exchanged = messages_exchanged
                .checked_add(executed.messages_exchanged)
                .ok_or(ExecutionError::CounterOverflow(executed.work.node))?;
            lp_work.push(executed.work);
            lp_timings.push(executed.timing);
        }

        for command in commands {
            send_command(
                command,
                WorkerCommand::RestoreAndMerge {
                    round: round_number,
                    exclusive_horizon_ns,
                    workers: config.workers,
                },
            )?;
        }
        let mut frontier_updates = 0_u64;
        let mut next_minima = vec![None; config.workers];
        for _ in 0..config.workers {
            match receive_reply(replies)? {
                WorkerReply::MergeComplete {
                    worker,
                    round,
                    frontier_updates: updates,
                    heap_pops,
                    minimum_ns,
                    busy_ns,
                } if round == round_number => {
                    frontier_updates = frontier_updates.saturating_add(updates);
                    frontier_heap_pops = frontier_heap_pops.saturating_add(heap_pops);
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
            },
            partition,
            lp_timings,
            worker_timings,
            lp_time_parallel_efficiency,
            worker_parallel_efficiency,
            worker_utilization,
            round_wall_time_ns,
        });
        previous_horizon = Some(exclusive_horizon_ns);
        round_number = round_number
            .checked_add(1)
            .ok_or(ExecutionError::CounterOverflow(NodeId(0)))?;
    }

    finish_workers(image, commands, replies, rounds)
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
) -> Result<Vec<ExecutedLp>, ExecutionError> {
    let total_chunks = assignment.straggler_chunks.len() + assignment.bulk_chunks.len();
    if total_chunks == 0 {
        return Ok(Vec::new());
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
        match receive_reply(replies)? {
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
    let mut executed_lps = Vec::new();
    while completed_chunks < total_chunks {
        let reply = deferred_completions
            .pop_front()
            .map_or_else(|| receive_reply(replies), Ok)?;
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
                busy_ns,
            } if reply_round == round && inflight[worker] == Some(class) => {
                inflight[worker] = None;
                worker_busy_ns[worker] = worker_busy_ns[worker].saturating_add(busy_ns);
                completed_chunks += 1;
                executed_lps.extend(executed);

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
    Ok(executed_lps)
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
    let mut scratch = vec![events[0]; events.len()];
    for pass in 0..REMOTE_ORDER_BYTES {
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

fn node_slot(image: &SimulationImage, node: NodeId) -> Option<usize> {
    usize::try_from(node.0)
        .ok()
        .filter(|slot| image.nodes.get(*slot).is_some_and(|entry| entry.id == node))
}

impl<'image> WorkerShard<'image> {
    fn new(worker: usize, lps: Vec<CpuLp<'image>>) -> Result<Self, ExecutionError> {
        let mut frontier = OwnerFrontierIndex::new(lps.len());
        for (owner_slot, lp) in lps.iter().enumerate() {
            frontier.update(owner_slot, lp.node.id, lp.next_key())?;
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
    ) -> Result<(Vec<ActiveLp<'image>>, u64), ExecutionError> {
        let mut heap_pops = 0;
        let mut active = Vec::new();
        while let Some(entry) = self
            .frontier
            .pop_before(exclusive_horizon_ns, &mut heap_pops)
        {
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
        Ok((active, heap_pops))
    }

    fn restore_and_merge(
        &mut self,
        ingress: &OwnerIngress<'image>,
        exclusive_horizon_ns: u128,
        workers: usize,
    ) -> Result<(u64, u64, Option<u64>), ExecutionError> {
        let mut frontier_updates = 0_u64;
        let mut heap_pops = 0_u64;
        let states = ingress.returned.try_iter().collect::<Vec<_>>();
        let mut inbox = ingress.remote.try_iter().collect::<Vec<_>>();
        for state in states {
            let node = state.lp.node.id;
            let next = state.lp.next_key();
            if self.lps[state.owner_slot].replace(state.lp).is_some() {
                return Err(ExecutionError::WorkerFailed {
                    worker: self.worker,
                    round: 0,
                });
            }
            self.frontier.update(state.owner_slot, node, next)?;
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
            self.frontier.update(owner_slot, target, lp.next_key())?;
            frontier_updates = frontier_updates.saturating_add(1);
            offset = end;
        }

        let minimum_ns = if let Some(entry) = self.frontier.peek_min(&mut heap_pops) {
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
        Ok((frontier_updates, heap_pops, minimum_ns))
    }
}

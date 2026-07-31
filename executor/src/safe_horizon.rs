//! Single-owner safe-horizon rounds and canonical remote-event exchange.
//!
//! The owner-local frontier index is deliberately separate from transition state. T9 uses one
//! owner on one thread; later backends can partition the same index without changing round
//! semantics or the shared transition handlers.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use crate::event::{EventFelClass, event_fel_class, is_same_time_tx_ready_continuation};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use crate::metal_spike::{
    RealReplayTrace, RealReplayTraceBuilder, RecordedReplayLp, ReplayStep, ReplayTraceCapture,
};
use crate::scalar::{ExecutionError, ObservationMode, RunResult, TransitionState};
use crate::{Event, EventKey, NodeId, SimulationImage};

const TIME_AFTER_U64_MAX: u128 = 1_u128 << 64;
const REMOTE_ORDER_BYTES: usize = 34;

/// Deterministic transition work performed by one active LP in one round.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LpRoundWork {
    pub node: NodeId,
    pub events_processed: u64,
    /// Local `TxComplete` → same-time `TxReady` pairs executed without an LP queue insert/pop.
    pub same_time_continuations: u64,
    /// Local generic-FEL insertions whose event kind is classified as `FallbackHeap`.
    ///
    /// This is a classification counter, not evidence of a physically separate heap: scalar and
    /// CPU round execution currently store every non-continuation local event in the same ordered
    /// `BTreeMap`.
    pub fallback_classified_pushes: u64,
}

/// Cheap, deterministic instrumentation retained for one safe-horizon round.
///
/// Work is measured in transitions rather than wall time. This keeps the normal round mode
/// deterministic and avoids per-event timers; T10 can add separately gated worker timings.
#[derive(Clone, Debug, PartialEq)]
pub struct RoundMetrics {
    pub frontier_ns: u64,
    /// Exclusive round boundary. `u128` represents one nanosecond after `u64::MAX`.
    pub exclusive_horizon_ns: u128,
    pub horizon_advance_ns: u128,
    pub events_processed: u64,
    pub active_lp_count: usize,
    pub lp_work: Vec<LpRoundWork>,
    pub parallel_efficiency: f64,
    pub messages_exchanged: u64,
    /// Frontier updates are sparse: one per executed or message-receiving LP.
    pub frontier_updates: u64,
    /// Valid and stale heap entries removed during this round.
    pub frontier_heap_pops: u64,
    /// Physical LP-table slots visited by frontier, dispatch, and merge machinery.
    pub physical_lp_probes: u64,
}

impl RoundMetrics {
    /// Events processed in this barrier interval.
    pub const fn events_per_round(&self) -> u64 {
        self.events_processed
    }
}

/// Complete scalar round-mode result and its per-round instrumentation.
#[derive(Clone, Debug, PartialEq)]
pub struct ScalarRoundRun {
    pub result: RunResult,
    pub rounds: Vec<RoundMetrics>,
}

/// Contiguous safe-horizon round window retained by the T13e spike harness.
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoundMetricsWindow {
    pub start_round: usize,
    pub rounds: usize,
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
impl RoundMetricsWindow {
    pub(crate) fn end_round(self) -> Result<usize, ExecutionError> {
        if self.rounds == 0 {
            return Err(ExecutionError::InvalidCpuConfig(
                "round metrics window requires at least one round",
            ));
        }
        self.start_round
            .checked_add(self.rounds)
            .ok_or(ExecutionError::InvalidCpuConfig(
                "round metrics window end overflows usize",
            ))
    }

    pub(crate) fn contains(self, round: usize) -> bool {
        round >= self.start_round && round - self.start_round < self.rounds
    }
}

/// Lightweight aggregate for one phase of a windowed full run.
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RoundRunTotals {
    pub rounds: usize,
    pub events_processed: u128,
    pub active_lp_rounds: u128,
    pub maximum_active_lps: usize,
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
impl RoundRunTotals {
    fn observe(&mut self, events_processed: u64, active_lp_count: usize) {
        self.rounds = self.rounds.saturating_add(1);
        self.events_processed = self
            .events_processed
            .saturating_add(u128::from(events_processed));
        self.active_lp_rounds = self
            .active_lp_rounds
            .saturating_add(active_lp_count as u128);
        self.maximum_active_lps = self.maximum_active_lps.max(active_lp_count);
    }
}

/// Whole-run and phase totals retained without keeping per-LP metrics outside the window.
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WindowedRunTotals {
    pub whole_run: RoundRunTotals,
    pub before_window: RoundRunTotals,
    pub retained_window: RoundRunTotals,
    pub after_window: RoundRunTotals,
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
impl WindowedRunTotals {
    pub(crate) fn observe(
        &mut self,
        window: RoundMetricsWindow,
        round: usize,
        events_processed: u64,
        active_lp_count: usize,
    ) {
        self.whole_run.observe(events_processed, active_lp_count);
        if round < window.start_round {
            self.before_window
                .observe(events_processed, active_lp_count);
        } else if window.contains(round) {
            self.retained_window
                .observe(events_processed, active_lp_count);
        } else {
            self.after_window.observe(events_processed, active_lp_count);
        }
    }
}

/// Scalar result with only one requested round-metrics window retained.
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[derive(Clone, Debug, PartialEq)]
pub struct WindowedScalarRoundRun {
    pub result: RunResult,
    pub rounds: Vec<RoundMetrics>,
    pub totals: WindowedRunTotals,
}

/// Runs the single-threaded safe-horizon executor through the configured inclusive stop.
///
/// `exclusive_horizon_ns` is an optional half-open partial-run boundary, exactly as in
/// [`crate::run_scalar`].
pub fn run_scalar_rounds(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
) -> Result<ScalarRoundRun, ExecutionError> {
    run_scalar_rounds_with_observations(image, exclusive_horizon_ns, ObservationMode::Summary)
}

/// Runs the single-threaded safe-horizon executor with explicit observation retention.
pub fn run_scalar_rounds_with_observations(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    observation_mode: ObservationMode,
) -> Result<ScalarRoundRun, ExecutionError> {
    RoundExecutor::new(image, observation_mode)?.run(exclusive_horizon_ns)
}

/// Records a bounded real-image window while executing the canonical safe-horizon CPU path.
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub fn run_scalar_rounds_with_replay_trace(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    capture: ReplayTraceCapture,
) -> Result<(ScalarRoundRun, RealReplayTrace), ExecutionError> {
    if capture.rounds == 0 {
        return Err(ExecutionError::InvalidCpuConfig(
            "real replay trace capture requires at least one round",
        ));
    }
    let mut executor = RoundExecutor::new(image, ObservationMode::Summary)?;
    executor.replay_trace = Some(RealReplayTraceBuilder::new(capture));
    executor.run_with_replay_trace(exclusive_horizon_ns)
}

/// Records and retains only the requested real-image window while executing the full scalar run.
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub fn run_scalar_rounds_with_windowed_replay_trace(
    image: &SimulationImage,
    exclusive_horizon_ns: Option<u64>,
    window: RoundMetricsWindow,
) -> Result<(WindowedScalarRoundRun, RealReplayTrace), ExecutionError> {
    window.end_round()?;
    let mut executor = RoundExecutor::new(image, ObservationMode::Summary)?;
    executor.replay_trace = Some(RealReplayTraceBuilder::new(ReplayTraceCapture {
        start_round: window.start_round,
        rounds: window.rounds,
    }));
    executor.run_with_windowed_replay_trace(exclusive_horizon_ns, window)
}

#[derive(Clone, Copy)]
enum RoundRetention {
    All,
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    Window(RoundMetricsWindow),
}

impl RoundRetention {
    fn retains(self, _round: usize) -> bool {
        match self {
            Self::All => true,
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            Self::Window(window) => window.contains(_round),
        }
    }
}

struct ExecutedRounds {
    retained: Vec<RoundMetrics>,
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    total_rounds: usize,
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    window_totals: Option<WindowedRunTotals>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FrontierEntry {
    time_ns: u64,
    node: NodeId,
    generation: u64,
    lp_slot: usize,
}

/// One future-event index owned by one execution owner.
///
/// Superseded entries remain in the heap and are discarded lazily. A generation prevents an old
/// entry from becoming valid again after an LP's frontier later returns to the same timestamp.
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
        lp_slot: usize,
        node: NodeId,
        next: Option<EventKey>,
        physical_lp_probes: &mut u64,
    ) -> Result<(), ExecutionError> {
        *physical_lp_probes = physical_lp_probes.saturating_add(1);
        let generation = self.generations[lp_slot]
            .checked_add(1)
            .ok_or(ExecutionError::CounterOverflow(node))?;
        self.generations[lp_slot] = generation;
        if let Some(key) = next {
            self.heap.push(Reverse(FrontierEntry {
                time_ns: key.time_ns,
                node,
                generation,
                lp_slot,
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
            if self.generations[entry.lp_slot] == entry.generation {
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

struct RoundExecutor<'image> {
    image: &'image SimulationImage,
    transitions: TransitionState<'image>,
    futures: Vec<BTreeMap<EventKey, Event>>,
    pending_keys: BTreeSet<EventKey>,
    frontier: OwnerFrontierIndex,
    minimum_lookahead_ns: Option<u64>,
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    replay_trace: Option<RealReplayTraceBuilder>,
}

struct DrainedLp {
    work: LpRoundWork,
    outbox: Vec<Event>,
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    replay: Option<RecordedReplayLp>,
}

impl<'image> RoundExecutor<'image> {
    fn new(
        image: &'image SimulationImage,
        observation_mode: ObservationMode,
    ) -> Result<Self, ExecutionError> {
        let transitions = TransitionState::new(image, observation_mode)?;
        let mut futures = (0..image.nodes.len())
            .map(|_| BTreeMap::new())
            .collect::<Vec<_>>();
        let mut pending_keys = BTreeSet::new();
        let mut touched = BTreeSet::new();

        for event in image.initial_events.iter().copied() {
            if !pending_keys.insert(event.key) {
                return Err(ExecutionError::DuplicateEventKey(event.key));
            }
            let lp_slot =
                node_slot(image, event.target).ok_or(ExecutionError::UnknownNode(event.target))?;
            if futures[lp_slot].insert(event.key, event).is_some() {
                return Err(ExecutionError::DuplicateEventKey(event.key));
            }
            touched.insert(lp_slot);
        }

        let minimum_lookahead_ns = image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min();
        if minimum_lookahead_ns == Some(0) {
            return Err(ExecutionError::NonPositiveLookahead);
        }

        let mut frontier = OwnerFrontierIndex::new(image.nodes.len());
        let mut setup_physical_lp_probes = 0;
        for lp_slot in touched {
            let node = image.nodes[lp_slot].id;
            frontier.update(
                lp_slot,
                node,
                futures[lp_slot].first_key_value().map(|(key, _)| *key),
                &mut setup_physical_lp_probes,
            )?;
        }

        Ok(Self {
            image,
            transitions,
            futures,
            pending_keys,
            frontier,
            minimum_lookahead_ns,
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            replay_trace: None,
        })
    }

    fn run(mut self, exclusive_horizon_ns: Option<u64>) -> Result<ScalarRoundRun, ExecutionError> {
        let configured_stop = u128::from(self.image.stop_time_ns) + 1;
        let run_end = exclusive_horizon_ns
            .map(u128::from)
            .unwrap_or(TIME_AFTER_U64_MAX)
            .min(configured_stop);
        let rounds = self.execute_rounds(run_end, RoundRetention::All)?;
        Ok(self.finish(rounds.retained))
    }

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    fn run_with_replay_trace(
        mut self,
        exclusive_horizon_ns: Option<u64>,
    ) -> Result<(ScalarRoundRun, RealReplayTrace), ExecutionError> {
        let configured_stop = u128::from(self.image.stop_time_ns) + 1;
        let run_end = exclusive_horizon_ns
            .map(u128::from)
            .unwrap_or(TIME_AFTER_U64_MAX)
            .min(configured_stop);
        let rounds = self.execute_rounds(run_end, RoundRetention::All)?;
        let trace = self
            .replay_trace
            .take()
            .expect("trace execution installs a trace builder")
            .finish(rounds.total_rounds);
        Ok((self.finish(rounds.retained), trace))
    }

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    fn run_with_windowed_replay_trace(
        mut self,
        exclusive_horizon_ns: Option<u64>,
        window: RoundMetricsWindow,
    ) -> Result<(WindowedScalarRoundRun, RealReplayTrace), ExecutionError> {
        let configured_stop = u128::from(self.image.stop_time_ns) + 1;
        let run_end = exclusive_horizon_ns
            .map(u128::from)
            .unwrap_or(TIME_AFTER_U64_MAX)
            .min(configured_stop);
        let rounds = self.execute_rounds(run_end, RoundRetention::Window(window))?;
        let trace = self
            .replay_trace
            .take()
            .expect("trace execution installs a trace builder")
            .finish(rounds.total_rounds);
        Ok((
            WindowedScalarRoundRun {
                result: self.finish_result(),
                rounds: rounds.retained,
                totals: rounds
                    .window_totals
                    .expect("windowed execution collects phase totals"),
            },
            trace,
        ))
    }

    fn execute_rounds(
        &mut self,
        run_end: u128,
        retention: RoundRetention,
    ) -> Result<ExecutedRounds, ExecutionError> {
        let mut previous_horizon = None;
        let mut rounds = Vec::new();
        let mut total_rounds = 0_usize;
        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        let mut window_totals = match retention {
            RoundRetention::All => None,
            RoundRetention::Window(_) => Some(WindowedRunTotals::default()),
        };

        loop {
            let mut frontier_heap_pops = 0;
            let mut physical_lp_probes = 0_u64;
            let Some(frontier_entry) = self
                .frontier
                .peek_min(&mut frontier_heap_pops, &mut physical_lp_probes)
            else {
                break;
            };
            let frontier_ns = frontier_entry.time_ns;
            if u128::from(frontier_ns) >= run_end {
                break;
            }

            let lookahead_end = self
                .minimum_lookahead_ns
                .map_or(TIME_AFTER_U64_MAX, |delay| {
                    (u128::from(frontier_ns) + u128::from(delay)).min(TIME_AFTER_U64_MAX)
                });
            let horizon = run_end.min(lookahead_end);
            let horizon_advance_ns =
                horizon.saturating_sub(previous_horizon.unwrap_or(u128::from(frontier_ns)));

            let mut active = Vec::new();
            while let Some(entry) =
                self.frontier
                    .pop_before(horizon, &mut frontier_heap_pops, &mut physical_lp_probes)
            {
                active.push(entry);
            }

            let mut children = Vec::new();
            let mut outboxes = Vec::with_capacity(active.len());
            let mut lp_work = Vec::with_capacity(active.len());
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            let capture_replay = self
                .replay_trace
                .as_ref()
                .is_some_and(|trace| trace.captures(total_rounds));
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            let mut replay_rows = Vec::with_capacity(if capture_replay { active.len() } else { 0 });
            let mut events_processed = 0_u64;
            let mut frontier_updates = 0_u64;
            for entry in active {
                physical_lp_probes = physical_lp_probes.saturating_add(1);
                let drained = self.drain_lp(
                    entry.lp_slot,
                    horizon,
                    &mut children,
                    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
                    capture_replay,
                )?;
                events_processed = events_processed.saturating_add(drained.work.events_processed);
                lp_work.push(drained.work);
                if !drained.outbox.is_empty() {
                    outboxes.push(drained.outbox);
                }
                #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
                if let Some(replay) = drained.replay {
                    replay_rows.push(replay);
                }
                self.update_frontier(entry.lp_slot, &mut physical_lp_probes)?;
                frontier_updates = frontier_updates.saturating_add(1);
            }

            if let Some(entry) = self
                .frontier
                .peek_min(&mut frontier_heap_pops, &mut physical_lp_probes)
            {
                if u128::from(entry.time_ns) < horizon {
                    let key = self.futures[entry.lp_slot]
                        .first_key_value()
                        .map(|(key, _)| *key)
                        .expect("a live frontier entry has a pending event");
                    return Err(ExecutionError::EventBelowHorizonAfterDrain {
                        key,
                        exclusive_horizon_ns: horizon,
                    });
                }
            }

            let mut remote_events = outboxes.into_iter().flatten().collect::<Vec<Event>>();
            let messages_exchanged = u64::try_from(remote_events.len()).unwrap_or(u64::MAX);
            radix_sort_remote_events(&mut remote_events);
            let mut offset = 0;
            while offset < remote_events.len() {
                let target = remote_events[offset].target;
                let lp_slot =
                    node_slot(self.image, target).ok_or(ExecutionError::UnknownNode(target))?;
                physical_lp_probes = physical_lp_probes.saturating_add(1);
                let mut end = offset + 1;
                while end < remote_events.len() && remote_events[end].target == target {
                    end += 1;
                }
                for event in &remote_events[offset..end] {
                    if u128::from(event.key.time_ns) < horizon {
                        return Err(ExecutionError::RemoteEventBeforeHorizon {
                            key: event.key,
                            exclusive_horizon_ns: horizon,
                        });
                    }
                    if self.futures[lp_slot].insert(event.key, *event).is_some() {
                        return Err(ExecutionError::DuplicateEventKey(event.key));
                    }
                }
                self.update_frontier(lp_slot, &mut physical_lp_probes)?;
                frontier_updates = frontier_updates.saturating_add(1);
                offset = end;
            }

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
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            if capture_replay {
                self.replay_trace
                    .as_mut()
                    .expect("capture flag requires a trace builder")
                    .push_round(
                        total_rounds,
                        frontier_ns,
                        horizon,
                        events_processed,
                        parallel_efficiency,
                        replay_rows,
                    );
            }
            let metrics = RoundMetrics {
                frontier_ns,
                exclusive_horizon_ns: horizon,
                horizon_advance_ns,
                events_processed,
                active_lp_count,
                lp_work,
                parallel_efficiency,
                messages_exchanged,
                frontier_updates,
                frontier_heap_pops,
                physical_lp_probes,
            };
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            if let (RoundRetention::Window(window), Some(totals)) =
                (retention, window_totals.as_mut())
            {
                totals.observe(
                    window,
                    total_rounds,
                    events_processed,
                    metrics.active_lp_count,
                );
            }
            if retention.retains(total_rounds) {
                rounds.push(metrics);
            }
            total_rounds = total_rounds
                .checked_add(1)
                .ok_or(ExecutionError::CounterOverflow(NodeId(0)))?;
            previous_horizon = Some(horizon);
        }

        Ok(ExecutedRounds {
            retained: rounds,
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            total_rounds,
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            window_totals,
        })
    }

    fn finish(self, rounds: Vec<RoundMetrics>) -> ScalarRoundRun {
        ScalarRoundRun {
            result: self.finish_result(),
            rounds,
        }
    }

    fn finish_result(self) -> RunResult {
        let mut pending_events = self
            .futures
            .into_iter()
            .flat_map(BTreeMap::into_values)
            .collect::<Vec<_>>();
        pending_events.sort_unstable_by_key(|event| event.key);
        self.transitions.finish(pending_events)
    }

    fn drain_lp(
        &mut self,
        lp_slot: usize,
        exclusive_horizon_ns: u128,
        children: &mut Vec<Event>,
        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))] capture_replay: bool,
    ) -> Result<DrainedLp, ExecutionError> {
        let node = self.image.nodes[lp_slot].id;
        let mut events_processed = 0_u64;
        let mut same_time_continuations = 0_u64;
        let mut fallback_classified_pushes = 0_u64;
        let mut outbox = Vec::new();
        let mut continuation = None;
        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        let pending_events_below_horizon = if capture_replay {
            u32::try_from(
                self.futures[lp_slot]
                    .values()
                    .take_while(|event| u128::from(event.key.time_ns) < exclusive_horizon_ns)
                    .count(),
            )
            .map_err(|_| ExecutionError::CounterOverflow(node))?
        } else {
            0
        };
        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        let mut replay_steps = Vec::new();
        while continuation.is_some()
            || self.futures[lp_slot]
                .first_key_value()
                .is_some_and(|(key, _)| u128::from(key.time_ns) < exclusive_horizon_ns)
        {
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            let direct_continuation = continuation.is_some();
            let event = if let Some(event) = continuation.take() {
                event
            } else {
                self.futures[lp_slot]
                    .pop_first()
                    .expect("first_key_value established a pending event")
                    .1
            };
            let removed = self.pending_keys.remove(&event.key);
            debug_assert!(removed, "executing event must own a pending key");

            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            let queue_occupancy = if capture_replay && event.kind == crate::EventKind::TxReady {
                Some(
                    u16::try_from(self.transitions.queue_occupancy(node)?)
                        .map_err(|_| ExecutionError::CounterOverflow(node))?,
                )
            } else {
                None
            };
            self.transitions.dispatch(event, children)?;
            let direct_child = match children.as_slice() {
                [child]
                    if is_same_time_tx_ready_continuation(
                        event,
                        *child,
                        node,
                        self.futures[lp_slot].first_key_value().map(|(key, _)| *key),
                    ) =>
                {
                    Some(*child)
                }
                _ => None,
            };
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            let mut local_fel_pushes = 0_u8;
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            let mut remote_outbox_writes = 0_u8;
            for child in children.drain(..) {
                if !self.pending_keys.insert(child.key) {
                    return Err(ExecutionError::DuplicateEventKey(child.key));
                }
                if child.target == node {
                    if direct_child == Some(child) {
                        continuation = Some(child);
                        same_time_continuations = same_time_continuations.saturating_add(1);
                    } else if self.futures[lp_slot].insert(child.key, child).is_some() {
                        return Err(ExecutionError::DuplicateEventKey(child.key));
                    } else {
                        if event_fel_class(child.kind) == EventFelClass::FallbackHeap {
                            fallback_classified_pushes = fallback_classified_pushes
                                .checked_add(1)
                                .ok_or(ExecutionError::CounterOverflow(node))?;
                        }
                        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
                        if capture_replay {
                            local_fel_pushes = local_fel_pushes
                                .checked_add(1)
                                .ok_or(ExecutionError::CounterOverflow(node))?;
                        }
                    }
                } else {
                    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
                    if capture_replay {
                        remote_outbox_writes = remote_outbox_writes
                            .checked_add(1)
                            .ok_or(ExecutionError::CounterOverflow(node))?;
                    }
                    outbox.push(child);
                }
            }
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            if capture_replay {
                replay_steps.push(
                    ReplayStep::new(
                        event.kind,
                        direct_continuation,
                        local_fel_pushes,
                        remote_outbox_writes,
                        queue_occupancy,
                    )
                    .map_err(|_| ExecutionError::CounterOverflow(node))?,
                );
            }
            events_processed = events_processed.saturating_add(1);
        }

        Ok(DrainedLp {
            work: LpRoundWork {
                node,
                events_processed,
                same_time_continuations,
                fallback_classified_pushes,
            },
            outbox,
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            replay: capture_replay.then(|| RecordedReplayLp {
                node,
                pending_events_below_horizon,
                next_time_ns_after_local_drain: self.futures[lp_slot]
                    .first_key_value()
                    .map_or(u64::MAX, |(key, _)| key.time_ns),
                steps: replay_steps,
            }),
        })
    }

    fn update_frontier(
        &mut self,
        lp_slot: usize,
        physical_lp_probes: &mut u64,
    ) -> Result<(), ExecutionError> {
        *physical_lp_probes = physical_lp_probes.saturating_add(1);
        let node = self.image.nodes[lp_slot].id;
        self.frontier.update(
            lp_slot,
            node,
            self.futures[lp_slot].first_key_value().map(|(key, _)| *key),
            physical_lp_probes,
        )
    }
}

fn node_slot(image: &SimulationImage, node: NodeId) -> Option<usize> {
    usize::try_from(node.0)
        .ok()
        .filter(|slot| image.nodes.get(*slot).is_some_and(|entry| entry.id == node))
}

/// Stable LSD radix ordering for `(target_lp, EventKey)`.
///
/// All components are fixed-width integers. Bytes that are identical across the run are skipped;
/// the remaining passes are the same stable radix operation. This keeps exchange O(messages)
/// without a comparison sort or an LP-count-sized bucket table.
fn radix_sort_remote_events(events: &mut Vec<Event>) {
    if events.len() < 2 {
        return;
    }

    let varying_bytes = remote_event_varying_bytes(events);
    let mut scratch = vec![events[0]; events.len()];
    for pass in 0..REMOTE_ORDER_BYTES {
        if varying_bytes & (1_u64 << pass) == 0 {
            continue;
        }
        let mut counts = [0_usize; 256];
        for event in events.iter() {
            counts[usize::from(remote_order_byte(*event, pass))] += 1;
        }
        let mut offset = 0;
        for count in &mut counts {
            let next = offset + *count;
            *count = offset;
            offset = next;
        }
        for event in events.iter().copied() {
            let byte = usize::from(remote_order_byte(event, pass));
            scratch[counts[byte]] = event;
            counts[byte] += 1;
        }
        std::mem::swap(events, &mut scratch);
    }
}

fn remote_event_varying_bytes(events: &[Event]) -> u64 {
    let first = events[0];
    let mut target = 0_u64;
    let mut time = 0_u64;
    let mut phase = 0_u16;
    let mut origin_node = 0_u64;
    let mut origin_seq = 0_u64;
    for event in &events[1..] {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::hint::black_box;
    use std::time::Instant;

    use crate::{
        EventKind, FlowDescriptor, FlowId, HostState, LinkDescriptor, LinkId, NodeDescriptor,
        NodeKind, PacketDescriptor, PacketKind, PayloadId, RemoteChannel, SwitchState, event_phase,
    };

    fn event(target: u64, time_ns: u64, phase: u16, origin_node: u64, origin_seq: u64) -> Event {
        Event {
            key: EventKey {
                time_ns,
                phase,
                origin_node: NodeId(origin_node),
                origin_seq,
            },
            target: NodeId(target),
            kind: EventKind::RemoteArrival,
            payload: PayloadId(origin_seq),
        }
    }

    #[test]
    fn remote_radix_order_is_target_then_complete_event_key() {
        let mut events = vec![
            event(2, 9, 1, 4, 7),
            event(1, 10, 0, 2, 0),
            event(1, 9, 2, 3, 1),
            event(1, 9, 2, 3, 0),
            event(1, 9, 0, 9, 5),
            event(0, u64::MAX, u16::MAX, u64::MAX, u64::MAX),
        ];
        let mut expected = events.clone();
        expected.sort_unstable_by_key(|event| (event.target, event.key));

        radix_sort_remote_events(&mut events);

        assert_eq!(events, expected);
    }

    #[test]
    fn lazy_frontier_discards_superseded_entries_without_visiting_idle_lps() {
        let mut frontier = OwnerFrontierIndex::new(100_000);
        let mut physical_lp_probes = 0;
        frontier
            .update(
                7,
                NodeId(7),
                Some(EventKey {
                    time_ns: 10,
                    phase: 0,
                    origin_node: NodeId(7),
                    origin_seq: 0,
                }),
                &mut physical_lp_probes,
            )
            .unwrap();
        frontier
            .update(
                7,
                NodeId(7),
                Some(EventKey {
                    time_ns: 20,
                    phase: 0,
                    origin_node: NodeId(7),
                    origin_seq: 1,
                }),
                &mut physical_lp_probes,
            )
            .unwrap();
        let mut heap_pops = 0;

        assert_eq!(
            frontier
                .peek_min(&mut heap_pops, &mut physical_lp_probes)
                .unwrap()
                .time_ns,
            20
        );
        assert_eq!(heap_pops, 1);
        assert_eq!(frontier.heap.len(), 1);
    }

    #[test]
    fn event_exactly_at_horizon_is_not_active() {
        let mut frontier = OwnerFrontierIndex::new(1);
        let mut physical_lp_probes = 0;
        frontier
            .update(
                0,
                NodeId(0),
                Some(EventKey {
                    time_ns: 10,
                    phase: 0,
                    origin_node: NodeId(0),
                    origin_seq: 0,
                }),
                &mut physical_lp_probes,
            )
            .unwrap();
        let mut heap_pops = 0;

        assert_eq!(
            frontier.pop_before(10, &mut heap_pops, &mut physical_lp_probes),
            None
        );
        assert_eq!(
            frontier
                .peek_min(&mut heap_pops, &mut physical_lp_probes)
                .unwrap()
                .time_ns,
            10
        );
        assert_eq!(heap_pops, 0);
    }

    fn idle_cost_image(idle_switches: usize, packet_count: u64) -> SimulationImage {
        let node_count = 2 + idle_switches as u64;
        let link = LinkDescriptor {
            id: LinkId(0),
            source: NodeId(0),
            target: NodeId(1),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        };
        let mut nodes = vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ];
        let mut switch_states = Vec::with_capacity(idle_switches);
        for index in 0..idle_switches {
            nodes.push(NodeDescriptor {
                id: NodeId(2 + index as u64),
                kind: NodeKind::Switch,
                state_slot: index as u32,
            });
            switch_states.push(SwitchState {
                physical_switch: index as u64,
                queues: vec![],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            });
        }
        let mut flows = Vec::with_capacity(packet_count as usize);
        let mut initial_packets = Vec::with_capacity(packet_count as usize);
        let mut initial_events = Vec::with_capacity(packet_count as usize);
        for sequence in 0..packet_count {
            let flow = FlowId(sequence);
            let payload = PayloadId::from_node_sequence(NodeId(0), node_count, sequence).unwrap();
            flows.push(FlowDescriptor {
                id: flow,
                source: NodeId(0),
                target: NodeId(1),
                route: vec![LinkId(0)],
                reverse_route: vec![],
            });
            initial_packets.push(PacketDescriptor {
                id: payload,
                flow,
                size_bytes: 1,
                kind: PacketKind::Data,
            });
            initial_events.push(Event {
                key: EventKey {
                    time_ns: sequence * 3,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(0),
                    origin_seq: sequence,
                },
                target: NodeId(0),
                kind: EventKind::PacketArrival,
                payload,
            });
        }
        SimulationImage {
            stop_time_ns: packet_count * 3 + 5,
            nodes,
            host_states: vec![
                HostState {
                    egress_link: LinkId(0),
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                    generators: vec![],
                    tcp_receivers: vec![],
                    next_origin_seq: packet_count,
                    next_payload_seq: 0,
                    sourced_packets: 0,
                    departed_packets: 0,
                    received_packets: 0,
                },
                HostState {
                    egress_link: LinkId(0),
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                    generators: vec![],
                    tcp_receivers: vec![],
                    next_origin_seq: 0,
                    next_payload_seq: 0,
                    sourced_packets: 0,
                    departed_packets: 0,
                    received_packets: 0,
                },
            ],
            switch_states,
            flows,
            initial_packets,
            links: vec![link],
            channels: vec![RemoteChannel::for_packet_link(link, 1).unwrap()],
            initial_events,
            seed: 1,
        }
    }

    fn idle_cost_sample(idle_switches: usize) -> u128 {
        let image = idle_cost_image(idle_switches, 1_000);
        let run_end = u128::from(image.stop_time_ns) + 1;
        let mut executor = RoundExecutor::new(&image, ObservationMode::Summary).unwrap();
        let start = Instant::now();
        let rounds = executor
            .execute_rounds(run_end, RoundRetention::All)
            .unwrap();
        let elapsed = start.elapsed().as_nanos();
        black_box(rounds);
        elapsed
    }

    #[test]
    #[ignore = "release-only idle-node cost falsification measurement"]
    fn idle_node_round_cost_measurement() {
        const SMALL_IDLE: usize = 1_000;
        const LARGE_IDLE: usize = 100_000;
        for _ in 0..3 {
            black_box(idle_cost_sample(SMALL_IDLE));
            black_box(idle_cost_sample(LARGE_IDLE));
        }

        let mut small = Vec::new();
        let mut large = Vec::new();
        for sample in 0..15 {
            if sample % 2 == 0 {
                small.push(idle_cost_sample(SMALL_IDLE));
                large.push(idle_cost_sample(LARGE_IDLE));
            } else {
                large.push(idle_cost_sample(LARGE_IDLE));
                small.push(idle_cost_sample(SMALL_IDLE));
            }
        }
        small.sort_unstable();
        large.sort_unstable();
        let small_median = small[small.len() / 2];
        let large_median = large[large.len() / 2];
        let ratio = large_median as f64 / small_median as f64;
        println!(
            "idle-node round cost: idle={SMALL_IDLE} median_ns={small_median}; \
             idle={LARGE_IDLE} median_ns={large_median}; ratio={ratio:.3}"
        );
        assert!(
            ratio <= 1.5,
            "100x idle LPs materially changed round-loop time: ratio={ratio:.3}"
        );
    }
}

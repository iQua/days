//! Per-host lookup structures for the Scalar and CPU stage path.
//!
//! A host running a collective owns many stage generators, and before these structures every stage
//! event scanned the whole generator table: to find a flow's generator, a completed stage's
//! successors, or the first releasable stage. [`HostStageIndex`] answers the same questions by key.
//! Each answer is the one the retired scan gave, in the scan's order, on every table the executor
//! can be handed, including tables the validator rejects (duplicate flows, stages already
//! releasable at load). The retained scans in `legacy_scans` and the equality hooks in
//! `crate::scalar` hold the two to that.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Index, IndexMut};

use crate::{CollectiveStage, FlowGeneratorState, FlowId, GeneratorStatus, HostState};

/// Executor-local keyed views of one host's generator and TCP-receiver tables.
///
/// Owned by the host's LP like the rest of its state, derived from it on construction and never
/// serialized. The keyed lists depend only on the tables' shape, which no transition changes: no
/// transition adds, removes or reorders a generator or receiver, rewrites a generator's flow or
/// stage presence, or rewrites a stage's predecessor flows (the two dependency writers store back
/// the predecessors they read). `releasable` is the only view of mutable state; it is refreshed at
/// each of the three writes that can change its predicate.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HostStageIndex {
    /// Every `(flow, position)` of the generator table, sorted by flow and then position, so a
    /// flow's entries are contiguous and in table order.
    generators_by_flow: Vec<(FlowId, usize)>,
    /// The same view of the TCP-receiver table.
    receivers_by_flow: Vec<(FlowId, usize)>,
    /// Positions of the stages whose local predecessor is the key, in table order.
    local_successors: BTreeMap<FlowId, Vec<usize>>,
    /// Positions of the stages whose inbound predecessor is the key, in table order.
    inbound_successors: BTreeMap<FlowId, Vec<usize>>,
    releasable: ReleasableStages,
    /// Test-only visit count. Probes always compare equal: they are not a view of the tables.
    probe: StageScanProbe,
}

/// Positions of the stages that are unreleased with both prerequisites complete.
///
/// A stage is ready to activate exactly when it is in this set and its generator is `Blocked`, so
/// the first ready stage in table order is the first member, in ascending position, whose
/// generator is `Blocked`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReleasableStages(BTreeSet<usize>);

impl ReleasableStages {
    /// Re-evaluates the membership of the stage at `position` after its dependencies or release
    /// flag were written.
    pub(crate) fn refresh(&mut self, position: usize, stage: Option<CollectiveStage>) {
        if releasable(stage) {
            self.0.insert(position);
        } else {
            self.0.remove(&position);
        }
    }
}

/// Whether `stage` is a stage its prerequisites have released but that has not activated.
fn releasable(stage: Option<CollectiveStage>) -> bool {
    stage.is_some_and(|stage| !stage.activated && stage.dependencies.prerequisites_complete())
}

/// The stage record at `position` of a host's stage table, which is empty on a host without
/// stages.
fn stage_at(stages: &[Option<CollectiveStage>], position: usize) -> Option<CollectiveStage> {
    stages.get(position).copied().flatten()
}

/// The retired activation scan: the first stage in table order that is ready to activate.
///
/// Debug builds compare every indexed answer with it, as the switch queue byte counters are.
fn first_ready_by_scan(
    generators: &[FlowGeneratorState],
    stages: &[Option<CollectiveStage>],
) -> Option<usize> {
    generators
        .iter()
        .enumerate()
        .position(|(position, generator)| {
            generator.next_emission.status == GeneratorStatus::Blocked
                && releasable(stage_at(stages, position))
        })
}

/// The contiguous entries of `flow` in a `(flow, position)` list sorted by flow then position.
fn entries_of(entries: &[(FlowId, usize)], flow: FlowId) -> &[(FlowId, usize)] {
    let start = entries.partition_point(|(key, _)| *key < flow);
    let length = entries[start..].partition_point(|(key, _)| *key == flow);
    &entries[start..start + length]
}

fn by_flow(flows: impl Iterator<Item = FlowId>) -> Vec<(FlowId, usize)> {
    let mut entries = flows
        .enumerate()
        .map(|(position, flow)| (flow, position))
        .collect::<Vec<_>>();
    // Positions are unique, so the unstable sort is deterministic.
    entries.sort_unstable();
    entries
}

impl HostStageIndex {
    pub(crate) fn build(state: &HostState) -> Self {
        let mut local_successors = BTreeMap::<FlowId, Vec<usize>>::new();
        let mut inbound_successors = BTreeMap::<FlowId, Vec<usize>>::new();
        let mut releasable = ReleasableStages::default();
        for position in 0..state.generators.len() {
            let Some(dependencies) = state.stage_dependencies(position) else {
                continue;
            };
            if let Some(predecessor) = dependencies.local_predecessor {
                local_successors
                    .entry(predecessor)
                    .or_default()
                    .push(position);
            }
            if let Some(predecessor) = dependencies.inbound_predecessor {
                inbound_successors
                    .entry(predecessor)
                    .or_default()
                    .push(position);
            }
            releasable.refresh(position, state.stage(position));
        }
        Self {
            generators_by_flow: by_flow(state.generators.iter().map(|generator| generator.flow)),
            receivers_by_flow: by_flow(state.tcp_receivers.iter().map(|receiver| receiver.flow)),
            local_successors,
            inbound_successors,
            releasable,
            probe: StageScanProbe::default(),
        }
    }

    /// Every generator position owning `flow`, in table order.
    pub(crate) fn generators_of(&mut self, flow: FlowId) -> &[(FlowId, usize)] {
        let entries = entries_of(&self.generators_by_flow, flow);
        self.probe.note(entries.len().max(1));
        entries
    }

    /// The position `generators.iter().position(|generator| generator.flow == flow)` returns.
    pub(crate) fn first_generator(&mut self, flow: FlowId) -> Option<usize> {
        let first = entries_of(&self.generators_by_flow, flow)
            .first()
            .map(|(_, position)| *position);
        self.probe.note(1);
        first
    }

    /// The position `tcp_receivers.iter().position(|receiver| receiver.flow == flow)` returns.
    pub(crate) fn first_receiver(&mut self, flow: FlowId) -> Option<usize> {
        let first = entries_of(&self.receivers_by_flow, flow)
            .first()
            .map(|(_, position)| *position);
        self.probe.note(1);
        first
    }

    /// The stages whose local predecessor is `completed`, in table order, with the releasable set
    /// the caller refreshes as it writes their dependencies.
    pub(crate) fn local_successors_of(
        &mut self,
        completed: FlowId,
    ) -> (&[usize], &mut ReleasableStages) {
        let successors = self
            .local_successors
            .get(&completed)
            .map_or(&[][..], Vec::as_slice);
        self.probe.note(successors.len().max(1));
        (successors, &mut self.releasable)
    }

    /// The stages whose inbound predecessor is `inbound`, in table order, with the releasable set.
    pub(crate) fn inbound_successors_of(
        &mut self,
        inbound: FlowId,
    ) -> (&[usize], &mut ReleasableStages) {
        let successors = self
            .inbound_successors
            .get(&inbound)
            .map_or(&[][..], Vec::as_slice);
        self.probe.note(successors.len().max(1));
        (successors, &mut self.releasable)
    }

    /// The position `generators.iter().position(stage_ready_to_activate)` returns.
    pub(crate) fn first_ready(
        &mut self,
        generators: &ProbedTable<'_, FlowGeneratorState>,
        stages: &ProbedTable<'_, Option<CollectiveStage>>,
    ) -> Option<usize> {
        self.first_ready_in(generators.items, stages.items)
    }

    fn first_ready_in(
        &mut self,
        generators: &[FlowGeneratorState],
        stages: &[Option<CollectiveStage>],
    ) -> Option<usize> {
        let mut examined = 0_usize;
        let ready = self.releasable.0.iter().copied().find(|position| {
            examined += 1;
            generators[*position].next_emission.status == GeneratorStatus::Blocked
        });
        self.probe.note(examined.max(1));
        debug_assert_eq!(
            ready,
            first_ready_by_scan(generators, stages),
            "the releasable-stage set diverged from the generator table"
        );
        ready
    }

    /// Refreshes the releasable membership of the stage at `position` after its release flag was
    /// set.
    pub(crate) fn refresh_releasable(&mut self, position: usize, stage: Option<CollectiveStage>) {
        self.releasable.refresh(position, stage);
    }

    /// Entries this host's lookups examined.
    #[cfg(feature = "planner-test-hooks")]
    pub(crate) fn visits(&self) -> u64 {
        self.probe.visits()
    }
}

/// One host's stage index together with the read counters of the host's three tables.
///
/// Stored in each host's entry of `TransitionState::hosts`, beside the host's state. The counters
/// sit beside the index, not inside it, so the stage view can borrow the index and both counters
/// at once.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct HostStageSlot {
    index: HostStageIndex,
    generator_reads: StageScanProbe,
    stage_reads: StageScanProbe,
    receiver_reads: StageScanProbe,
}

/// The index and the read counters of a host's generator, stage and TCP-receiver tables.
pub(crate) type HostStageParts<'a> = (
    &'a mut HostStageIndex,
    &'a mut StageScanProbe,
    &'a mut StageScanProbe,
    &'a mut StageScanProbe,
);

impl HostStageSlot {
    pub(crate) fn build(state: &HostState) -> Self {
        Self {
            index: HostStageIndex::build(state),
            generator_reads: StageScanProbe::default(),
            stage_reads: StageScanProbe::default(),
            receiver_reads: StageScanProbe::default(),
        }
    }

    /// The index, and the generator-table, stage-table and TCP-receiver-table read counters.
    pub(crate) fn parts_mut(&mut self) -> HostStageParts<'_> {
        (
            &mut self.index,
            &mut self.generator_reads,
            &mut self.stage_reads,
            &mut self.receiver_reads,
        )
    }

    /// Entries this host's stage path read: index entries and scanned table entries.
    #[cfg(feature = "planner-test-hooks")]
    pub(crate) fn visits(&self) -> u64 {
        self.index
            .visits()
            .saturating_add(self.generator_reads.visits())
            .saturating_add(self.stage_reads.visits())
            .saturating_add(self.receiver_reads.visits())
    }

    #[cfg(feature = "planner-test-hooks")]
    pub(crate) fn index(&self) -> &HostStageIndex {
        &self.index
    }
}

/// One of a host's tables as the stage path sees it: every element a scan yields is counted.
///
/// The stage view hands out host tables only as `ProbedTable`s. Positional access (`table[i]`) is
/// one O(1) element read and is not counted. Every iteration (`iter`, `iter_mut`, or a `for`
/// loop over `&mut table`) counts each element it yields. There is no `Deref` to the slice and no
/// length accessor, so a table cannot be walked without counting. Without the test hooks the
/// counter field does not exist and every method is an inlined slice operation, so the table has
/// the size and code of the `&mut [T]` it wraps.
pub(crate) struct ProbedTable<'a, T> {
    items: &'a mut [T],
    #[cfg(feature = "planner-test-hooks")]
    reads: &'a mut StageScanProbe,
}

impl<'a, T> ProbedTable<'a, T> {
    #[inline(always)]
    pub(crate) fn new(items: &'a mut [T], reads: &'a mut StageScanProbe) -> Self {
        let _ = &reads;
        Self {
            items,
            #[cfg(feature = "planner-test-hooks")]
            reads,
        }
    }

    /// A counted scan. No stage-path function scans a table (the `xtask` stage-path audit
    /// rejects it); a scan the syntactic audit cannot see is still counted here, and the scaling
    /// budget fails on it.
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn iter(&mut self) -> CountedIter<'_, std::slice::Iter<'_, T>> {
        CountedIter {
            inner: self.items.iter(),
            #[cfg(feature = "planner-test-hooks")]
            reads: &mut *self.reads,
            #[cfg(not(feature = "planner-test-hooks"))]
            reads: std::marker::PhantomData,
        }
    }

    /// A counted mutable scan; see [`Self::iter`].
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn iter_mut(&mut self) -> CountedIter<'_, std::slice::IterMut<'_, T>> {
        CountedIter {
            inner: self.items.iter_mut(),
            #[cfg(feature = "planner-test-hooks")]
            reads: &mut *self.reads,
            #[cfg(not(feature = "planner-test-hooks"))]
            reads: std::marker::PhantomData,
        }
    }
}

impl<'b, 'a, T> IntoIterator for &'b mut ProbedTable<'a, T> {
    type Item = &'b mut T;
    type IntoIter = CountedIter<'b, std::slice::IterMut<'b, T>>;

    #[inline(always)]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T> Index<usize> for ProbedTable<'_, T> {
    type Output = T;

    #[inline(always)]
    fn index(&self, position: usize) -> &T {
        &self.items[position]
    }
}

impl<T> IndexMut<usize> for ProbedTable<'_, T> {
    #[inline(always)]
    fn index_mut(&mut self, position: usize) -> &mut T {
        &mut self.items[position]
    }
}

/// A host's stage table as the stage path sees it. The table is empty on a host without stages, so
/// its accessors take a generator position and answer `None` for an ungated generator; each is one
/// O(1) positional read, uncounted like `table[i]`.
impl ProbedTable<'_, Option<CollectiveStage>> {
    /// The stage record of the generator at `position`.
    #[inline(always)]
    pub(crate) fn stage(&self, position: usize) -> Option<CollectiveStage> {
        stage_at(self.items, position)
    }

    /// The stage record of the generator at `position`, to write.
    #[inline(always)]
    pub(crate) fn stage_mut(&mut self, position: usize) -> Option<&mut CollectiveStage> {
        self.items.get_mut(position).and_then(Option::as_mut)
    }
}

// Without the test hooks the stage view costs nothing: the probe is zero-sized, a probed table is
// exactly the slice reference it wraps, a counted iterator is exactly the slice iterator, and a
// host's slot is exactly its index. Checked at compile time in every production build.
#[cfg(not(feature = "planner-test-hooks"))]
const _: () = {
    use std::mem::size_of;
    assert!(size_of::<StageScanProbe>() == 0);
    assert!(
        size_of::<ProbedTable<'static, FlowGeneratorState>>()
            == size_of::<&mut [FlowGeneratorState]>()
    );
    assert!(
        size_of::<CountedIter<'static, std::slice::IterMut<'static, FlowGeneratorState>>>()
            == size_of::<std::slice::IterMut<'static, FlowGeneratorState>>()
    );
    assert!(size_of::<HostStageSlot>() == size_of::<HostStageIndex>());
};

/// A slice iterator that counts each element it yields into a table's read counter.
pub(crate) struct CountedIter<'r, I> {
    inner: I,
    #[cfg(feature = "planner-test-hooks")]
    reads: &'r mut StageScanProbe,
    #[cfg(not(feature = "planner-test-hooks"))]
    reads: std::marker::PhantomData<&'r mut StageScanProbe>,
}

impl<I: Iterator> Iterator for CountedIter<'_, I> {
    type Item = I::Item;

    #[inline(always)]
    fn next(&mut self) -> Option<I::Item> {
        let item = self.inner.next();
        #[cfg(feature = "planner-test-hooks")]
        if item.is_some() {
            self.reads.note(1);
        }
        item
    }

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I: DoubleEndedIterator> DoubleEndedIterator for CountedIter<'_, I> {
    #[inline(always)]
    fn next_back(&mut self) -> Option<I::Item> {
        let item = self.inner.next_back();
        #[cfg(feature = "planner-test-hooks")]
        if item.is_some() {
            self.reads.note(1);
        }
        item
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for CountedIter<'_, I> {}

/// Test-only count of the table entries the stage path examines.
///
/// A visit is one generator-table, TCP-receiver-table, index or pending-cause entry a stage-path
/// lookup reads; a keyed lookup counts one. Without the test hooks the probe is empty and `note`
/// compiles to nothing, so the count can neither cost a production run anything nor influence it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StageScanProbe {
    #[cfg(feature = "planner-test-hooks")]
    counts: StageScanCounts,
}

impl PartialEq for StageScanProbe {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for StageScanProbe {}

/// Events dispatched and table entries examined; the dispatch count is the probe's denominator.
#[cfg(feature = "planner-test-hooks")]
#[derive(Clone, Copy, Debug, Default)]
struct StageScanCounts {
    dispatches: u64,
    visits: u64,
}

impl StageScanProbe {
    /// Records that one lookup examined `entries` table entries.
    #[inline]
    pub(crate) fn note(&mut self, entries: usize) {
        let _ = entries;
        #[cfg(feature = "planner-test-hooks")]
        {
            self.counts.visits = self
                .counts
                .visits
                .saturating_add(u64::try_from(entries).unwrap_or(u64::MAX));
        }
    }

    /// Records one dispatched event.
    #[inline]
    pub(crate) fn note_dispatch(&mut self) {
        #[cfg(feature = "planner-test-hooks")]
        {
            self.counts.dispatches = self.counts.dispatches.saturating_add(1);
        }
    }

    /// Entries examined so far.
    #[cfg(feature = "planner-test-hooks")]
    pub(crate) fn visits(&self) -> u64 {
        self.counts.visits
    }

    /// Events dispatched so far.
    #[cfg(feature = "planner-test-hooks")]
    pub(crate) fn dispatches(&self) -> u64 {
        self.counts.dispatches
    }
}

/// Pre-index stage-path scans, retained as the oracle of the P14 scan equality gate.
///
/// Every function below is the exact expression the keyed path replaced in
/// `executor/src/scalar.rs` as of `e39d3fa`. They are compiled only for the test-hook surface so
/// that the equality hooks in `crate::scalar` can prove, on lowered images, on running checkpoints
/// and after every event of a run, that the index answers every query as the scan did.
#[cfg(feature = "planner-test-hooks")]
pub(crate) mod legacy_scans {
    use crate::{FlowGeneratorState, FlowId, GeneratorStatus, HostState};

    /// `host_packet_arrival` (`any` and `position`), `activate_wrapped_stage`,
    /// `start_compute_stage`, `host_compute_timer`, `push_stage_progress`,
    /// `host_tcp_initial_send`, `host_tcp_ack_arrival` and `prepare_tcp_attempts`.
    pub(crate) fn first_generator(state: &HostState, flow: FlowId) -> Option<usize> {
        state
            .generators
            .iter()
            .position(|generator| generator.flow == flow)
    }

    /// The generators owning `flow`, in table order: the candidates of `host_pacing_timer`'s
    /// `any(|generator| generator.flow == flow && <compute stage>)`.
    pub(crate) fn generators_of(state: &HostState, flow: FlowId) -> Vec<usize> {
        positions_where(state, |_, generator| generator.flow == flow)
    }

    /// `host_tcp_data_arrival`.
    pub(crate) fn first_receiver(state: &HostState, flow: FlowId) -> Option<usize> {
        state
            .tcp_receivers
            .iter()
            .position(|receiver| receiver.flow == flow)
    }

    /// The generators `complete_local_successors` updates for `completed`, in table order.
    pub(crate) fn local_successors(state: &HostState, completed: FlowId) -> Vec<usize> {
        positions_where(state, |position, _| {
            state
                .stage_dependencies(position)
                .is_some_and(|dependencies| dependencies.local_predecessor == Some(completed))
        })
    }

    /// The generators `record_inbound_progress` updates for `inbound`, in table order.
    pub(crate) fn inbound_successors(state: &HostState, inbound: FlowId) -> Vec<usize> {
        positions_where(state, |position, _| {
            state
                .stage_dependencies(position)
                .is_some_and(|dependencies| dependencies.inbound_predecessor == Some(inbound))
        })
    }

    /// `activate_ready_collectives`' `find(stage_ready_to_activate)`.
    pub(crate) fn first_ready(state: &HostState) -> Option<usize> {
        positions_where(state, |position, generator| {
            generator.next_emission.status == GeneratorStatus::Blocked
                && state.stage(position).is_some_and(|stage| {
                    !stage.activated && stage.dependencies.prerequisites_complete()
                })
        })
        .first()
        .copied()
    }

    /// The generator positions, in table order, that satisfy `predicate`. A generator's stage
    /// record lives in the host's stage table and is read by position.
    fn positions_where(
        state: &HostState,
        predicate: impl Fn(usize, &FlowGeneratorState) -> bool,
    ) -> Vec<usize> {
        state
            .generators
            .iter()
            .enumerate()
            .filter(|(position, generator)| predicate(*position, generator))
            .map(|(position, _)| position)
            .collect()
    }
}

/// Compares every keyed answer of `index` with the retained scan over `state`.
///
/// The queries are every flow the host's tables, stage predecessors or `extra_flows` name, and the
/// flow just past the largest of them, so that absent keys are exercised too. `index` must also
/// equal a fresh derivation from `state`, which checks the incrementally maintained releasable set.
#[cfg(feature = "planner-test-hooks")]
pub(crate) fn check_host_index(
    state: &HostState,
    index: &HostStageIndex,
    extra_flows: impl IntoIterator<Item = FlowId>,
) -> Result<(), String> {
    let rebuilt = HostStageIndex::build(state);
    if *index != rebuilt {
        return Err(format!(
            "the maintained index {index:?} differs from the one derived from the host state {rebuilt:?}"
        ));
    }
    let mut index = rebuilt;
    let mut queries = extra_flows.into_iter().collect::<Vec<_>>();
    for (position, generator) in state.generators.iter().enumerate() {
        queries.push(generator.flow);
        if let Some(dependencies) = state.stage_dependencies(position) {
            queries.extend(dependencies.local_predecessor);
            queries.extend(dependencies.inbound_predecessor);
        }
    }
    queries.extend(state.tcp_receivers.iter().map(|receiver| receiver.flow));
    if let Some(largest) = queries.iter().max().copied() {
        queries.push(FlowId(largest.0.saturating_add(1)));
    }
    queries.push(FlowId(0));
    queries.sort_unstable();
    queries.dedup();
    for flow in queries {
        let mismatch =
            |site: &str, indexed: &dyn std::fmt::Debug, scanned: &dyn std::fmt::Debug| {
                Err(format!(
                    "flow {flow:?} {site}: indexed {indexed:?} differs from scanned {scanned:?}"
                ))
            };
        let (indexed, scanned) = (
            index.first_generator(flow),
            legacy_scans::first_generator(state, flow),
        );
        if indexed != scanned {
            return mismatch("first generator", &indexed, &scanned);
        }
        let indexed = index
            .generators_of(flow)
            .iter()
            .map(|(_, position)| *position)
            .collect::<Vec<_>>();
        let scanned = legacy_scans::generators_of(state, flow);
        if indexed != scanned {
            return mismatch("generators", &indexed, &scanned);
        }
        let (indexed, scanned) = (
            index.first_receiver(flow),
            legacy_scans::first_receiver(state, flow),
        );
        if indexed != scanned {
            return mismatch("first receiver", &indexed, &scanned);
        }
        let indexed = index.local_successors_of(flow).0.to_vec();
        let scanned = legacy_scans::local_successors(state, flow);
        if indexed != scanned {
            return mismatch("local successors", &indexed, &scanned);
        }
        let indexed = index.inbound_successors_of(flow).0.to_vec();
        let scanned = legacy_scans::inbound_successors(state, flow);
        if indexed != scanned {
            return mismatch("inbound successors", &indexed, &scanned);
        }
    }
    let indexed = index.first_ready_in(&state.generators, &state.stages);
    let scanned = legacy_scans::first_ready(state);
    if indexed != scanned {
        return Err(format!(
            "first ready stage: indexed {indexed:?} differs from scanned {scanned:?}"
        ));
    }
    Ok(())
}

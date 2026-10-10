//! P17 merge (test hooks only): the device exchange merge's counters and active-set audit.
//!
//! The instrumented kernels (`DAYS_MERGE_AUDIT`, compiled only with `cuda-test-hooks` or
//! `metal-test-hooks`) read and write a region appended to `stream_state` after planning: one row
//! of [`MERGE_AUDIT_WORDS`] words per LP, then one word per flow holding the flow's source LP (the
//! audit's generator-ownership table). Each merge thread writes only its own LP's row, so the
//! counters need no atomics. Production plans carry no such region.
//!
//! After every successful attempt the host checks the audit (zero mismatches, and as many active
//! stream entries as non-empty streams summed over the run; the kernel header of `merge_audit` in
//! `cuda_kernels.cu` explains why that pair proves each round's list is the full rebuild's set) and
//! keeps the rows as this thread's last run's.

/// Words per LP row: `{merge invocations, streams the merge read, entries it appended, audit
/// mismatches, non-empty streams counted by this thread, stream entries in this LP's list}`.
pub(crate) const MERGE_AUDIT_WORDS: usize = 6;

/// Test-only: one LP's merge counters over the run's successful attempt.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MergeAuditRow {
    /// Merge invocations on this LP (one per completed round).
    pub invocations: u64,
    /// Declared-list entries the merge read.
    pub streams_read: u64,
    /// Active entries the merge appended.
    pub appended: u64,
    /// Audit mismatches against invariant E.
    pub mismatches: u64,
    /// Non-empty streams this thread counted in its share of the global sweep.
    pub nonempty_streams: u64,
    /// Non-heap entries in this LP's list after the merge, summed over invocations.
    pub stream_entries: u64,
}

std::thread_local! {
    /// Test-only: the merge rows of this thread's last successful device run.
    static MERGE_AUDIT: std::cell::RefCell<Option<Vec<MergeAuditRow>>> =
        const { std::cell::RefCell::new(None) };
}

/// The hooks region appended to `stream_state`: zeroed LP rows, then each flow's source LP read
/// from the flow plane (`flow_words` words per flow, source first).
pub(crate) fn hook_region(node_count: usize, flows: &[u64], flow_words: usize) -> Vec<u64> {
    let mut region = vec![0_u64; node_count.saturating_mul(MERGE_AUDIT_WORDS)];
    region.extend(flows.chunks_exact(flow_words).map(|flow| flow[0]));
    region
}

/// Checks and keeps the LP rows `words` (`MERGE_AUDIT_WORDS` per LP) of a successful attempt.
///
/// # Panics
///
/// When the device audit found a list that is not the full rebuild's set. This runs only in
/// test-hook builds, where a broken invariant must fail every device test that reaches it.
pub(crate) fn record(words: &[u64]) {
    let rows = words
        .as_chunks::<MERGE_AUDIT_WORDS>()
        .0
        .iter()
        .map(
            |&[
                invocations,
                streams_read,
                appended,
                mismatches,
                nonempty_streams,
                stream_entries,
            ]| MergeAuditRow {
                invocations,
                streams_read,
                appended,
                mismatches,
                nonempty_streams,
                stream_entries,
            },
        )
        .collect::<Vec<_>>();
    let mismatches = rows.iter().map(|row| row.mismatches).sum::<u64>();
    let nonempty = rows.iter().map(|row| row.nonempty_streams).sum::<u64>();
    let entries = rows.iter().map(|row| row.stream_entries).sum::<u64>();
    assert!(
        mismatches == 0 && nonempty == entries,
        "P17 merge audit: the active lists are not the full rebuild's sets: {mismatches} \
         mismatches; {entries} active stream entries against {nonempty} non-empty streams",
    );
    MERGE_AUDIT.with(|audit| *audit.borrow_mut() = Some(rows));
}

/// Test-only: clears the rows kept by the previous run.
pub(crate) fn reset() {
    MERGE_AUDIT.with(|audit| *audit.borrow_mut() = None);
}

/// Test-only: the per-LP merge rows of this thread's last successful Metal or CUDA run; clears
/// them.
#[doc(hidden)]
pub fn take_merge_audit_for_testing() -> Option<Vec<MergeAuditRow>> {
    MERGE_AUDIT.with(|audit| audit.borrow_mut().take())
}

//! Per-host lookup structures for the Scalar and CPU stage path.

/// Test-only count of the table entries the stage path examines.
///
/// A visit is one generator-table, TCP-receiver-table or pending-cause entry that a stage-path
/// lookup reads. Without the test hooks the probe is empty and `note` compiles to nothing, so the
/// count can neither cost a production run anything nor influence it.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StageScanProbe {
    #[cfg(feature = "planner-test-hooks")]
    counts: StageScanCounts,
}

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

/// How many entries a `find`/`position` over `len` entries examined: through the first match, or
/// all of them when nothing matched.
pub(crate) fn scanned_through(position: Option<usize>, len: usize) -> usize {
    position.map_or(len, |position| position + 1)
}

//! Plane construction shared by the device planners (P18 diet).
//!
//! **All-zero planes (c2a).** Ten of a device plan's planes are all zero at upload: the channel and generator stream rings,
//! the remote staging arena, the outbox, the three observation logs, the worklist, the summary
//! counters, the LP state and the merge cursors. A plan holds their lengths, not their words; the
//! device allocates and zeroes them itself, so no host buffer of zeros is built, held beside the
//! rest of the plan, or copied (at the 1,024-GPU flagship, 16.5 GB of the 31.2 GB plan).

/// The length in words of an all-zero plane. Like an uploaded host plane, it is at least one word.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ZeroPlane(usize);

impl ZeroPlane {
    /// `records` records of `words` words each.
    pub(crate) fn records(records: usize, words: usize) -> Option<Self> {
        records.checked_mul(words).map(Self::words)
    }

    /// `words` words.
    pub(crate) fn words(words: usize) -> Self {
        Self(words.max(1))
    }

    pub(crate) fn len(self) -> usize {
        self.0
    }
}

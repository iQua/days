//! Deterministic sizing for the T20l fix-2 device-side readback compaction.
//!
//! # Why this exists
//!
//! T20l phase 1 (`t20l-hostloop-decomposition.md` §4.2, §8) measured the frontier device path
//! copying the **whole** 18.6-19.2 GB result arena back to the host on every successful attempt,
//! to recover 487,958 pending events and 2,931,779 resident packet descriptors — a copy between
//! one and two orders of magnitude more expensive than the decode that consumes it. §8 recorded
//! the reason: the decode phases are the *only* ones that respect occupancy, and the meta planes
//! that bound them are already on the device before the copy starts.
//!
//! This module turns those meta planes into a **plan**: for each striped record arena, the exact
//! set of live regions, in decode order, with the dense destination each is gathered to.
//!
//! # The determinism contract
//!
//! Every number in a [`CompactionPlan`] is a function of words the **device** wrote — an arena
//! offset, a ring capacity, a ring head, a live count — plus the record width the planner fixed.
//! There is no host guess, no high-water estimate, and no threshold: a plan for a given device
//! state is unique, so two runs of the same image compact identically and decode identically.
//! The destination offsets are the exclusive prefix sum of the live counts, taken in the same
//! entity order the decode walks, which is what makes the gathered buffer directly indexable.
//!
//! # Byte-identity
//!
//! `CompactionEntity::live_record_slots` is the host mirror of what the gather kernel computes,
//! and it reproduces the *existing* decode's slot arithmetic exactly: `offset + index` for the
//! linear arenas, `offset + (head + index) mod max(capacity, 1)` for the ring arenas — the same
//! expression `read_queue`, the stream loop and `tcp_ledger_ring::ledger_record_slot` used before
//! this fix. `the_readback_compaction_ring_matches_the_canonical_ledger_slot` pins the ring form
//! against that canonical slot exhaustively, so the gather moves the same words the full readback
//! delivered and the decode reads them in the same order.

use std::num::NonZeroUsize;

/// Words per entity row in the plan buffer handed to the gather kernel.
///
/// `[source word offset, ring capacity in records, ring head, live record count,
/// destination word offset]`.
pub(crate) const COMPACTION_PLAN_ROW_WORDS: usize = 5;

/// How an arena's live records are addressed inside one entity's region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactionShape {
    /// Live records occupy `[offset, offset + count)` — the FEL and observation-log arenas.
    Linear,
    /// Logical record `i` lives at physical slot `(head + i) mod max(capacity, 1)` — the queue,
    /// stream and TCP segment-ledger arenas.
    Ring,
}

/// One entity's live region inside a striped record arena, as the device described it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CompactionEntity {
    /// Absolute **word** offset of the entity's region inside the source plane.
    pub(crate) source_words: u64,
    /// Ring capacity in records. Ignored by [`CompactionShape::Linear`].
    pub(crate) capacity: u64,
    /// Physical slot of logical record 0. Ignored by [`CompactionShape::Linear`].
    pub(crate) head: u64,
    /// Live record count — the device-written occupancy word.
    pub(crate) count: u64,
}

impl CompactionEntity {
    /// The absolute source **word** offsets of this entity's live records, in decode order.
    ///
    /// The host mirror of the gather kernel's inner loop; used by the unit tests and by the
    /// backends' debug assertions rather than on the readback path itself.
    #[cfg(test)]
    pub(crate) fn live_record_slots(
        &self,
        shape: CompactionShape,
        record_words: usize,
    ) -> Vec<u64> {
        (0..self.count)
            .map(|index| {
                let physical = match shape {
                    CompactionShape::Linear => index,
                    CompactionShape::Ring => (self.head + index) % self.capacity.max(1),
                };
                self.source_words + physical * record_words as u64
            })
            .collect()
    }
}

/// Sizing failure while building a compaction plan.
///
/// Every variant means a device-written meta word describes a region the source plane cannot
/// hold, which is a device-state defect rather than a capacity fault: the readback refuses
/// instead of silently truncating complete state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionError(pub(crate) String);

impl std::fmt::Display for CompactionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A gather request for one striped record arena.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompactionPlan {
    /// Diagnostic name of the arena, used in refusal messages.
    pub(crate) arena: &'static str,
    shape: CompactionShape,
    record_words: NonZeroUsize,
    entities: Vec<CompactionEntity>,
    /// Exclusive prefix sum of `entities[..].count`, in records. One entry per entity.
    destinations: Vec<u64>,
    /// Total live records — the length, in records, of the gathered buffer.
    total_records: u64,
}

impl CompactionPlan {
    /// Builds a plan from device-written meta words.
    ///
    /// `entities` must already be in the order the decode walks them, because the destination
    /// offsets are that order's prefix sum.
    pub(crate) fn new(
        arena: &'static str,
        shape: CompactionShape,
        record_words: usize,
        entities: Vec<CompactionEntity>,
        source_plane_words: usize,
    ) -> Result<Self, CompactionError> {
        let record_words = NonZeroUsize::new(record_words).ok_or_else(|| {
            CompactionError(format!("{arena} compaction needs a nonzero record width"))
        })?;
        let mut destinations = Vec::with_capacity(entities.len());
        let mut total_records = 0_u64;
        for entity in &entities {
            if shape == CompactionShape::Ring && entity.count > entity.capacity.max(1) {
                return Err(CompactionError(format!(
                    "{arena} ring reports {} live records in a {}-record ring",
                    entity.count, entity.capacity
                )));
            }
            // The gather reads `source_words + physical * record_words + w` for
            // `physical < max(capacity, count)`, so bound the whole region once.
            let span = match shape {
                CompactionShape::Linear => entity.count,
                CompactionShape::Ring => entity.capacity.max(entity.count),
            };
            let end = span
                .checked_mul(record_words.get() as u64)
                .and_then(|words| words.checked_add(entity.source_words))
                .ok_or_else(|| {
                    CompactionError(format!("{arena} live region overflows a u64 word offset"))
                })?;
            if end > source_plane_words as u64 {
                return Err(CompactionError(format!(
                    "{arena} live region ends at word {end}, past the {source_plane_words}-word \
                     source plane"
                )));
            }
            destinations.push(total_records);
            total_records = total_records.checked_add(entity.count).ok_or_else(|| {
                CompactionError(format!("{arena} live record count overflows a u64"))
            })?;
        }
        total_records
            .checked_mul(record_words.get() as u64)
            .and_then(|words| usize::try_from(words).ok())
            .ok_or_else(|| {
                CompactionError(format!(
                    "{arena} gathered buffer overflows the host address space"
                ))
            })?;
        Ok(Self {
            arena,
            shape,
            record_words,
            entities,
            destinations,
            total_records,
        })
    }

    /// Total live records across every entity — the gathered buffer's record length.
    pub(crate) fn total_records(&self) -> u64 {
        self.total_records
    }

    /// Total live words — the exact number of words the compacted readback transfers.
    pub(crate) fn total_words(&self) -> usize {
        (self.total_records as usize).saturating_mul(self.record_words.get())
    }

    /// The number of entities the gather dispatches over.
    pub(crate) fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Destination **record** index of entity `index`'s logical record 0 in the gathered buffer.
    pub(crate) fn destination(&self, index: usize) -> usize {
        self.destinations
            .get(index)
            .copied()
            .unwrap_or(self.total_records) as usize
    }

    /// Live record count of entity `index`, as the device reported it.
    pub(crate) fn count(&self, index: usize) -> usize {
        self.entities
            .get(index)
            .map_or(0, |entity| entity.count as usize)
    }

    /// The plan buffer uploaded to the device: [`COMPACTION_PLAN_ROW_WORDS`] words per entity.
    pub(crate) fn plan_words(&self) -> Vec<u64> {
        let mut words = Vec::with_capacity(self.entities.len() * COMPACTION_PLAN_ROW_WORDS);
        for (entity, destination) in self.entities.iter().zip(&self.destinations) {
            words.push(entity.source_words);
            words.push(entity.capacity);
            words.push(entity.head);
            words.push(entity.count);
            words.push(destination * self.record_words.get() as u64);
        }
        words
    }

    /// The scalar argument buffer uploaded beside the plan.
    pub(crate) fn argument_words(&self) -> Vec<u64> {
        vec![
            self.entities.len() as u64,
            self.record_words.get() as u64,
            u64::from(self.shape == CompactionShape::Ring),
        ]
    }

    /// Host mirror of the gather, for hardware-free tests.
    #[cfg(test)]
    pub(crate) fn gather(&self, source: &[u64]) -> Vec<u64> {
        let mut destination = vec![0_u64; self.total_words()];
        for (entity, base) in self.entities.iter().zip(&self.destinations) {
            for (index, slot) in entity
                .live_record_slots(self.shape, self.record_words.get())
                .into_iter()
                .enumerate()
            {
                let from = slot as usize;
                let to = (*base as usize + index) * self.record_words.get();
                destination[to..to + self.record_words.get()]
                    .copy_from_slice(&source[from..from + self.record_words.get()]);
            }
        }
        destination
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(source_words: u64, capacity: u64, head: u64, count: u64) -> CompactionEntity {
        CompactionEntity {
            source_words,
            capacity,
            head,
            count,
        }
    }

    #[test]
    fn destinations_are_the_exclusive_prefix_sum_of_the_device_written_counts() {
        let plan = CompactionPlan::new(
            "test",
            CompactionShape::Linear,
            2,
            vec![ring(0, 0, 0, 3), ring(6, 0, 0, 0), ring(8, 0, 0, 5)],
            32,
        )
        .expect("in-range plan");
        assert_eq!(plan.destination(0), 0);
        assert_eq!(plan.destination(1), 3);
        assert_eq!(plan.destination(2), 3);
        assert_eq!(plan.count(0), 3);
        assert_eq!(plan.count(1), 0);
        assert_eq!(plan.count(2), 5);
        assert_eq!(plan.entity_count(), 3);
        assert_eq!(plan.total_records(), 8);
        assert_eq!(plan.total_words(), 16);
        // An index past the last entity reads as "everything is already gathered", so a decode
        // walking more entities than the meta plane described cannot read another entity's
        // records.
        assert_eq!(plan.destination(3), 8);
        assert_eq!(plan.count(3), 0);
    }

    #[test]
    fn the_gathered_buffer_reproduces_the_ring_decode_order() {
        // Two flows, three records each of two words, both rings rotated.
        let mut source = vec![0_u64; 12];
        for slot in 0..6_u64 {
            source[slot as usize * 2] = slot;
            source[slot as usize * 2 + 1] = 100 + slot;
        }
        let plan = CompactionPlan::new(
            "test",
            CompactionShape::Ring,
            2,
            vec![ring(0, 3, 2, 3), ring(6, 3, 1, 2)],
            12,
        )
        .expect("in-range plan");
        // Flow 0 head 2: logical 0,1,2 -> physical 2,0,1. Flow 1 head 1: logical 0,1 -> 1,2.
        assert_eq!(
            plan.gather(&source),
            vec![2, 102, 0, 100, 1, 101, 4, 104, 5, 105]
        );
    }

    #[test]
    fn a_linear_arena_gathers_its_prefix_without_consulting_the_ring_words() {
        let source: Vec<u64> = (0..10).collect();
        let plan = CompactionPlan::new(
            "test",
            CompactionShape::Linear,
            1,
            vec![ring(0, 4, 3, 2), ring(4, 4, 2, 1)],
            10,
        )
        .expect("in-range plan");
        assert_eq!(plan.gather(&source), vec![0, 1, 4]);
        assert_eq!(plan.argument_words(), vec![2, 1, 0]);
    }

    #[test]
    fn the_plan_row_carries_the_device_words_and_the_destination_word_offset() {
        let plan = CompactionPlan::new(
            "test",
            CompactionShape::Ring,
            5,
            vec![ring(0, 4, 1, 3), ring(20, 2, 0, 2)],
            30,
        )
        .expect("in-range plan");
        assert_eq!(
            plan.plan_words(),
            vec![0, 4, 1, 3, 0, /* second row */ 20, 2, 0, 2, 15]
        );
        assert_eq!(plan.argument_words(), vec![2, 5, 1]);
    }

    #[test]
    fn a_live_region_past_the_source_plane_is_refused_rather_than_truncated() {
        let error = CompactionPlan::new(
            "test",
            CompactionShape::Ring,
            2,
            vec![ring(0, 4, 0, 4), ring(8, 4, 0, 4)],
            12,
        )
        .expect_err("out-of-range plan");
        assert!(error.0.contains("past the 12-word source plane"), "{error}");
    }

    #[test]
    fn a_count_above_its_own_ring_capacity_is_refused() {
        let error =
            CompactionPlan::new("test", CompactionShape::Ring, 2, vec![ring(0, 3, 0, 4)], 64)
                .expect_err("over-full ring");
        assert!(
            error.0.contains("4 live records in a 3-record ring"),
            "{error}"
        );
    }

    #[test]
    fn an_empty_arena_plans_a_zero_word_gather() {
        let plan = CompactionPlan::new("test", CompactionShape::Ring, 14, Vec::new(), 1)
            .expect("empty plan");
        assert_eq!(plan.total_records(), 0);
        assert_eq!(plan.total_words(), 0);
        assert_eq!(plan.entity_count(), 0);
        assert!(plan.plan_words().is_empty());
    }
}

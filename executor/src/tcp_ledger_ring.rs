//! Slot arithmetic and canonical decoding for the device TCP segment-ledger ring.
//!
//! # Why a ring
//!
//! The device ledger holds one flow's unacknowledged segments in ascending sequence order. T20i
//! layer 1 measured what that means dynamically: occupancy IS the congestion window, every new
//! segment is appended past the highest sequence, every retransmission replaces an existing record
//! in place, and every cumulative ACK removes a *prefix*. Those are exactly a ring's operations.
//! The shifting array the plane used before paid an O(n) compaction on every acknowledgement and
//! an O(n) scan on every insert; a stalled Reno recovery reached 11,058 records, so both costs are
//! paid 36.7M times at frontier scale.
//!
//! # The canonical-state obligation
//!
//! The ledger is complete state: its decoded records feed the resident-packet map that the frozen
//! fixture hashes cover. The ring is therefore a *physical* representation only. Logical index `i`
//! of a flow's ledger lives at physical slot `(head + i) mod capacity`, and every reader walks
//! `i = 0..count` in that order, so the decoded order and content are identical to what the
//! shifting array produced. [`mirror`] pins that equality against a transliteration of the
//! pre-ring algorithm.
//!
//! T20l fix 2 moved the *readback's* walk onto the device: `days_compact_gather` resolves the ring
//! while gathering the live records, so the host decode sees logical order already. That kernel's
//! slot expression is pinned against [`ledger_record_slot`] by
//! `the_readback_compaction_ring_matches_the_canonical_ledger_slot`, which is why this function is
//! now a test-only oracle rather than a readback-path call.
//!
//! # Metadata layout
//!
//! Each flow owns [`TCP_LEDGER_META_WORDS`] metadata words:
//!
//! | word | meaning |
//! | ---: | --- |
//! | 0 | absolute record-arena offset of the flow's ring, in words |
//! | 1 | ring capacity, in records |
//! | 2 | live record count |
//! | 3 | T20g live-state contract: fallback-heap slot + 1 of the flow's armed timer, 0 if none |
//! | 4 | ring head: physical slot holding logical index 0 |
//! | 5 | T20i high-water mark: the maximum value word 2 has ever held |
//!
//! Words 4 and 5 are T20i additions. Word 5 exists so a capacity fault can report the *whole*
//! per-flow occupancy vector rather than one first-offender tuple: layer 1 showed 377 of 262,144
//! flows exceed the derived floor, so a first-offender retry chain would need up to 377 sequential
//! replans, while one vector sizes every flow at once.

/// Metadata words per flow in the device TCP segment-ledger plane.
pub(crate) const TCP_LEDGER_META_WORDS: usize = 6;

/// Words per ledger record: payload id, size, sequence, sent time, retransmission flag.
pub(crate) const TCP_LEDGER_RECORD_WORDS: usize = 5;

/// Metadata word holding the flow's record-arena offset.
#[cfg(test)]
pub(crate) const LEDGER_META_OFFSET: usize = 0;
/// Metadata word holding the flow's ring capacity.
#[cfg(test)]
pub(crate) const LEDGER_META_CAPACITY: usize = 1;
/// Metadata word holding the flow's live record count.
#[cfg(test)]
pub(crate) const LEDGER_META_COUNT: usize = 2;
/// Metadata word holding the flow's armed fallback-heap slot + 1 (T20g live-state contract).
#[cfg(test)]
pub(crate) const LEDGER_META_TIMER_SLOT: usize = 3;
/// Metadata word holding the ring head: the physical slot of logical index 0.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const LEDGER_META_HEAD: usize = 4;
/// Metadata word holding the per-flow occupancy high-water mark.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const LEDGER_META_HIGH_WATER: usize = 5;

/// Absolute word index of logical ledger record `logical` for a flow whose ring starts at
/// `offset`, spans `capacity` records, and currently begins at physical slot `head`.
///
/// This is the single source of truth for the ring's slot arithmetic; `tcp_ledger_slot` in
/// `metal_kernels.metal` and `cuda_kernels.cu` are line-for-line transliterations of it, and so is
/// the `days_compact_gather` ring expression T20l fix 2 added.
///
/// It is **test-only** since T20l fix 2. The readback used to call it once per resident record;
/// the gather kernel now resolves the ring on the device and hands the host logical order, so the
/// function's remaining job is to be the oracle those kernels are pinned against.
///
/// # Precondition, and why it is asserted rather than assumed
///
/// The `%` below is **total**; the kernels' transliteration is not. Both kernels reduce
/// `head + logical` with a *single conditional subtraction*, which is a complete modulo only while
///
/// ```text
/// head + logical <= 2 * max(capacity, 1) - 1
/// ```
///
/// Every current call site satisfies that — the tightest is R7's `head + keep`, which is exactly
/// `2 * capacity - 1` when a full ring is fully acknowledged from `head = capacity - 1`. But a
/// future call site that violated it would be **correct here and wrong on the device**, and the
/// byte-identity gates would stay green, because they compare a kernel against this mirror only
/// through call sites that both of them make. The `debug_assert!` turns that blind spot into a
/// test failure at the offending call site — in the mirror traces, in the Metal `tcp_semantics`
/// suite, and in the host readback itself, all of which run with debug assertions on. It is
/// deliberately not a hard `assert!`: this runs once per resident record per flow in the frontier
/// readback, and `%` keeps the host answer right even where the device answer would be wrong.
///
/// The residual limitation, stated: a release build does not check the bound, and no static gate
/// can. `the_kernels_single_conditional_subtraction_matches_this_modulo` pins the two forms equal
/// across the whole precondition domain and shows exactly where they part company outside it.
#[cfg(test)]
#[inline]
pub(crate) fn ledger_record_slot(
    offset: usize,
    capacity: usize,
    head: usize,
    logical: usize,
) -> usize {
    let span = capacity.max(1);
    debug_assert!(
        head + logical < 2 * span,
        "ledger ring slot head {head} + logical {logical} exceeds the kernels' \
         single-conditional-subtraction bound {} at capacity {capacity}",
        2 * span - 1
    );
    offset + ((head + logical) % span) * TCP_LEDGER_RECORD_WORDS
}

/// Saturating `u32` view of one flow's high-water metadata word.
///
/// The fault payload is a `u32` vector — 262,144 flows cost 1 MiB, which is why one readback can
/// carry the whole demand distribution instead of a first-offender tuple.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[inline]
pub(crate) fn ledger_high_water_vector(meta: &[u64], meta_offset: usize, flows: usize) -> Vec<u32> {
    (0..flows)
        .map(|flow| {
            meta.get(meta_offset + flow * TCP_LEDGER_META_WORDS + LEDGER_META_HIGH_WATER)
                .copied()
                .unwrap_or(0)
                .try_into()
                .unwrap_or(u32::MAX)
        })
        .collect()
}

/// Host transliterations of the two ledger algorithms, and the gate that pins them equal.
///
/// The device kernels own the production implementations; these mirrors exist so the ring's
/// canonical-decode obligation is testable without a GPU. Every mutation site in the mirrors is
/// numbered, and the T20i report's audit table maps each number to its `metal_kernels.metal` and
/// `cuda_kernels.cu` counterpart.
#[cfg(test)]
pub(crate) mod mirror {
    use super::{
        LEDGER_META_CAPACITY, LEDGER_META_COUNT, LEDGER_META_HEAD, LEDGER_META_HIGH_WATER,
        LEDGER_META_OFFSET, TCP_LEDGER_META_WORDS, TCP_LEDGER_RECORD_WORDS, ledger_record_slot,
    };

    /// One ledger mutation, replayed identically against both representations.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum LedgerOp {
        /// Insert or replace the record at `sequence`.
        Insert {
            id: u64,
            size: u64,
            sequence: u64,
            sent_ns: u64,
            retransmission: bool,
        },
        /// Remove the cumulative-ACK prefix, truncating a partially acknowledged head record.
        Acknowledge { acknowledgment: u64 },
    }

    /// Outcome of one mutation: `Ok(())`, a capacity fault, or a segment-size conflict.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) enum LedgerOutcome {
        Applied,
        CapacityExceeded { capacity: usize, demand: usize },
        SizeConflict,
    }

    /// One flow's ledger plane: `TCP_LEDGER_META_WORDS` metadata words followed by the record arena.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct LedgerPlane {
        pub(crate) words: Vec<u64>,
    }

    impl LedgerPlane {
        /// Seeds a plane with `records` already resident, exactly as the planners do.
        pub(crate) fn seed(capacity: usize, records: &[[u64; TCP_LEDGER_RECORD_WORDS]]) -> Self {
            let offset = TCP_LEDGER_META_WORDS;
            let mut words = vec![0_u64; offset + capacity.max(1) * TCP_LEDGER_RECORD_WORDS];
            words[LEDGER_META_OFFSET] = offset as u64;
            words[LEDGER_META_CAPACITY] = capacity as u64;
            words[LEDGER_META_COUNT] = records.len() as u64;
            words[LEDGER_META_HEAD] = 0;
            words[LEDGER_META_HIGH_WATER] = records.len() as u64;
            for (index, record) in records.iter().enumerate() {
                let slot = offset + index * TCP_LEDGER_RECORD_WORDS;
                words[slot..slot + TCP_LEDGER_RECORD_WORDS].copy_from_slice(record);
            }
            Self { words }
        }

        fn meta(&self, word: usize) -> usize {
            self.words[word] as usize
        }

        pub(crate) fn count(&self) -> usize {
            self.meta(LEDGER_META_COUNT)
        }

        pub(crate) fn high_water(&self) -> usize {
            self.meta(LEDGER_META_HIGH_WATER)
        }

        pub(crate) fn head(&self) -> usize {
            self.meta(LEDGER_META_HEAD)
        }

        /// The canonical decode: logical order 0..count, five words per record.
        ///
        /// The array representation and the ring representation are byte-identical exactly when
        /// this function agrees on them, because it is the only reader the readback path uses.
        pub(crate) fn canonical_bytes(&self, ring: bool) -> Vec<u8> {
            let offset = self.meta(LEDGER_META_OFFSET);
            let capacity = self.meta(LEDGER_META_CAPACITY);
            let count = self.count();
            let head = if ring { self.head() } else { 0 };
            let mut bytes = Vec::with_capacity(count * TCP_LEDGER_RECORD_WORDS * 8);
            for logical in 0..count {
                let slot = if ring {
                    ledger_record_slot(offset, capacity, head, logical)
                } else {
                    offset + logical * TCP_LEDGER_RECORD_WORDS
                };
                for word in 0..TCP_LEDGER_RECORD_WORDS {
                    bytes.extend_from_slice(&self.words[slot + word].to_le_bytes());
                }
            }
            bytes
        }
    }

    fn record_words(op: LedgerOp) -> [u64; TCP_LEDGER_RECORD_WORDS] {
        match op {
            LedgerOp::Insert {
                id,
                size,
                sequence,
                sent_ns,
                retransmission,
            } => [id, size, sequence, sent_ns, u64::from(retransmission)],
            LedgerOp::Acknowledge { .. } => unreachable!("only inserts carry a record"),
        }
    }

    /// The pre-T20i shifting-array algorithm, transliterated from `metal_kernels.metal` at
    /// `e78a2a9`. It is the reference the ring must reproduce.
    pub(crate) fn apply_array(plane: &mut LedgerPlane, op: LedgerOp) -> LedgerOutcome {
        let offset = plane.meta(LEDGER_META_OFFSET);
        let capacity = plane.meta(LEDGER_META_CAPACITY);
        let count = plane.meta(LEDGER_META_COUNT);
        let words = &mut plane.words;
        match op {
            LedgerOp::Insert { size, sequence, .. } => {
                let record = record_words(op);
                let mut insertion = 0;
                while insertion < count
                    && words[offset + insertion * TCP_LEDGER_RECORD_WORDS + 2] < sequence
                {
                    insertion += 1;
                }
                if insertion < count
                    && words[offset + insertion * TCP_LEDGER_RECORD_WORDS + 2] == sequence
                {
                    if words[offset + insertion * TCP_LEDGER_RECORD_WORDS + 1] != size {
                        return LedgerOutcome::SizeConflict;
                    }
                    let slot = offset + insertion * TCP_LEDGER_RECORD_WORDS;
                    words[slot..slot + TCP_LEDGER_RECORD_WORDS].copy_from_slice(&record);
                    return LedgerOutcome::Applied;
                }
                if count >= capacity {
                    return LedgerOutcome::CapacityExceeded {
                        capacity,
                        demand: count + 1,
                    };
                }
                for index in (insertion + 1..=count).rev() {
                    for word in 0..TCP_LEDGER_RECORD_WORDS {
                        words[offset + index * TCP_LEDGER_RECORD_WORDS + word] =
                            words[offset + (index - 1) * TCP_LEDGER_RECORD_WORDS + word];
                    }
                }
                let slot = offset + insertion * TCP_LEDGER_RECORD_WORDS;
                words[slot..slot + TCP_LEDGER_RECORD_WORDS].copy_from_slice(&record);
                words[LEDGER_META_COUNT] = (count + 1) as u64;
                LedgerOutcome::Applied
            }
            LedgerOp::Acknowledge { acknowledgment } => {
                let mut keep = 0;
                while keep < count {
                    let record = offset + keep * TCP_LEDGER_RECORD_WORDS;
                    if words[record + 2] + words[record + 1] > acknowledgment {
                        break;
                    }
                    keep += 1;
                }
                if keep < count {
                    let first = offset + keep * TCP_LEDGER_RECORD_WORDS;
                    let sequence = words[first + 2];
                    if sequence < acknowledgment {
                        let acknowledged = acknowledgment - sequence;
                        if acknowledged < words[first + 1] {
                            words[first + 1] -= acknowledged;
                            words[first + 2] = acknowledgment;
                        } else {
                            keep += 1;
                        }
                    }
                }
                let remaining = count - keep;
                for index in 0..remaining {
                    for word in 0..TCP_LEDGER_RECORD_WORDS {
                        words[offset + index * TCP_LEDGER_RECORD_WORDS + word] =
                            words[offset + (keep + index) * TCP_LEDGER_RECORD_WORDS + word];
                    }
                }
                words[LEDGER_META_COUNT] = remaining as u64;
                LedgerOutcome::Applied
            }
        }
    }

    /// First logical index whose sequence is `>= sequence`, by binary search over the ring window.
    ///
    /// Equivalent to the pre-ring linear scan because the ledger is strictly ascending in sequence
    /// and duplicate-free: inserts replace on an exact sequence match, and a partial cumulative ACK
    /// re-keys the head record to the acknowledgement, which stays below the next record's start.
    fn lower_bound(
        words: &[u64],
        offset: usize,
        capacity: usize,
        head: usize,
        count: usize,
        sequence: u64,
    ) -> usize {
        let mut low = 0;
        let mut high = count;
        while low < high {
            let mid = low + (high - low) / 2;
            if words[ledger_record_slot(offset, capacity, head, mid) + 2] < sequence {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    /// The T20i ring algorithm, transliterated from the post-change kernels.
    pub(crate) fn apply_ring(plane: &mut LedgerPlane, op: LedgerOp) -> LedgerOutcome {
        let offset = plane.meta(LEDGER_META_OFFSET);
        let capacity = plane.meta(LEDGER_META_CAPACITY);
        let count = plane.meta(LEDGER_META_COUNT);
        let head = plane.meta(LEDGER_META_HEAD);
        let words = &mut plane.words;
        match op {
            LedgerOp::Insert { size, sequence, .. } => {
                let record = record_words(op);
                let insertion = lower_bound(words, offset, capacity, head, count, sequence);
                if insertion < count {
                    let slot = ledger_record_slot(offset, capacity, head, insertion);
                    if words[slot + 2] == sequence {
                        if words[slot + 1] != size {
                            return LedgerOutcome::SizeConflict;
                        }
                        // Mutation site R1: in-place replace.
                        words[slot..slot + TCP_LEDGER_RECORD_WORDS].copy_from_slice(&record);
                        return LedgerOutcome::Applied;
                    }
                }
                if count >= capacity {
                    return LedgerOutcome::CapacityExceeded {
                        capacity,
                        demand: count + 1,
                    };
                }
                let slot = if insertion == count {
                    // Mutation site R2: monotone append, O(1).
                    ledger_record_slot(offset, capacity, head, count)
                } else if insertion == 0 {
                    // Mutation site R3: prefix insert retreats the head, O(1).
                    let retreated = if head == 0 { capacity } else { head } - 1;
                    words[LEDGER_META_HEAD] = retreated as u64;
                    offset + retreated * TCP_LEDGER_RECORD_WORDS
                } else {
                    // Mutation site R4: interior insert still shifts, inside the ring window.
                    for index in (insertion + 1..=count).rev() {
                        let target = ledger_record_slot(offset, capacity, head, index);
                        let source = ledger_record_slot(offset, capacity, head, index - 1);
                        for word in 0..TCP_LEDGER_RECORD_WORDS {
                            words[target + word] = words[source + word];
                        }
                    }
                    ledger_record_slot(offset, capacity, head, insertion)
                };
                words[slot..slot + TCP_LEDGER_RECORD_WORDS].copy_from_slice(&record);
                // Mutation site R5: count grows, and the high-water word follows it up only.
                let grown = count + 1;
                words[LEDGER_META_COUNT] = grown as u64;
                if grown as u64 > words[LEDGER_META_HIGH_WATER] {
                    words[LEDGER_META_HIGH_WATER] = grown as u64;
                }
                LedgerOutcome::Applied
            }
            LedgerOp::Acknowledge { acknowledgment } => {
                let mut keep = 0;
                while keep < count {
                    let record = ledger_record_slot(offset, capacity, head, keep);
                    if words[record + 2] + words[record + 1] > acknowledgment {
                        break;
                    }
                    keep += 1;
                }
                if keep < count {
                    let first = ledger_record_slot(offset, capacity, head, keep);
                    let sequence = words[first + 2];
                    if sequence < acknowledgment {
                        let acknowledged = acknowledgment - sequence;
                        if acknowledged < words[first + 1] {
                            // Mutation site R6: partial cumulative ACK re-keys the head record.
                            words[first + 1] -= acknowledged;
                            words[first + 2] = acknowledgment;
                        } else {
                            keep += 1;
                        }
                    }
                }
                // Mutation site R7: prefix removal advances the head. No compaction.
                //
                // This is the tightest consumer of the kernels' reduction precondition: `head` is
                // at most `capacity - 1` and `keep` at most `count <= capacity`, so `head + keep`
                // reaches exactly `2 * capacity - 1` when a full ring is fully acknowledged from
                // the last slot — the largest sum one conditional subtraction still reduces.
                let span = capacity.max(1);
                debug_assert!(
                    head + keep < 2 * span,
                    "R7 head {head} + keep {keep} exceeds the kernels' \
                     single-conditional-subtraction bound {} at capacity {capacity}",
                    2 * span - 1
                );
                words[LEDGER_META_HEAD] = ((head + keep) % span) as u64;
                words[LEDGER_META_COUNT] = (count - keep) as u64;
                LedgerOutcome::Applied
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mirror::{LedgerOp, LedgerOutcome, LedgerPlane, apply_array, apply_ring};
    use super::{
        LEDGER_META_HIGH_WATER, LEDGER_META_TIMER_SLOT, TCP_LEDGER_META_WORDS,
        TCP_LEDGER_RECORD_WORDS, ledger_high_water_vector, ledger_record_slot,
    };

    fn insert(id: u64, sequence: u64, size: u64, retransmission: bool) -> LedgerOp {
        LedgerOp::Insert {
            id,
            size,
            sequence,
            sent_ns: 1_000 + sequence,
            retransmission,
        }
    }

    /// Replays one trace against both representations and asserts the canonical bytes agree at
    /// every step, not merely at the end.
    fn assert_trace_byte_identical(capacity: usize, seed: &[[u64; 5]], ops: &[LedgerOp]) {
        let mut array = LedgerPlane::seed(capacity, seed);
        let mut ring = LedgerPlane::seed(capacity, seed);
        assert_eq!(array.canonical_bytes(false), ring.canonical_bytes(true));
        for (index, op) in ops.iter().copied().enumerate() {
            let array_outcome = apply_array(&mut array, op);
            let ring_outcome = apply_ring(&mut ring, op);
            assert_eq!(
                array_outcome, ring_outcome,
                "op {index} ({op:?}) outcome diverged"
            );
            assert_eq!(
                array.count(),
                ring.count(),
                "op {index} ({op:?}) count diverged"
            );
            assert_eq!(
                array.canonical_bytes(false),
                ring.canonical_bytes(true),
                "op {index} ({op:?}) canonical bytes diverged"
            );
        }
    }

    #[test]
    fn ring_slot_arithmetic_wraps_within_the_capacity_window() {
        assert_eq!(ledger_record_slot(100, 4, 0, 0), 100);
        assert_eq!(ledger_record_slot(100, 4, 0, 3), 115);
        assert_eq!(ledger_record_slot(100, 4, 3, 0), 115);
        assert_eq!(ledger_record_slot(100, 4, 3, 1), 100);
        assert_eq!(ledger_record_slot(100, 4, 3, 3), 110);
        // A zero-capacity ring is never indexed, but the arithmetic must not divide by zero.
        assert_eq!(ledger_record_slot(100, 0, 0, 0), 100);
    }

    /// The kernels' `tcp_ledger_slot`, transliterated back: one conditional subtraction, no `%`.
    fn kernel_slot(offset: usize, capacity: usize, head: usize, logical: usize) -> usize {
        let span = if capacity == 0 { 1 } else { capacity };
        let mut physical = head + logical;
        if physical >= span {
            physical -= span;
        }
        offset + physical * TCP_LEDGER_RECORD_WORDS
    }

    #[test]
    fn the_kernels_single_conditional_subtraction_matches_this_modulo() {
        // Exhaustive over the precondition domain: every capacity up to 12, every head inside the
        // ring, and every logical index up to the `2 * span - 1` bound R7 makes tight. Inside the
        // domain the kernels' cheaper form is the mirror's `%`, which is what lets one host mirror
        // gate two device kernels.
        for capacity in 0..=12_usize {
            let span = capacity.max(1);
            for head in 0..span {
                for logical in 0..=(2 * span - 1 - head) {
                    assert_eq!(
                        kernel_slot(64, capacity, head, logical),
                        ledger_record_slot(64, capacity, head, logical),
                        "capacity {capacity} head {head} logical {logical}"
                    );
                }
            }
        }
        // And immediately outside it they part company: capacity 4 with head 3 and logical 5 sums
        // to 8, one past `2 * 4 - 1`, where one subtraction leaves 4 — a slot outside the ring —
        // while the modulo wraps to 0. This is the blind spot the precondition assertion covers.
        assert_eq!(kernel_slot(64, 4, 3, 5), 64 + 4 * TCP_LEDGER_RECORD_WORDS);
        assert_ne!(kernel_slot(64, 4, 3, 5), 64);
    }

    #[test]
    fn the_readback_compaction_ring_matches_the_canonical_ledger_slot() {
        // T20l fix 2 moved the readback's ring walk into `days_compact_gather`, whose slot
        // expression is `source_words + ((head + index) % max(capacity, 1)) * record_words`.
        // `device_compaction::CompactionEntity::live_record_slots` is that expression's host
        // mirror, and this pins it against the canonical ledger slot over the same exhaustive
        // domain the kernel-equality test uses, so the compacted readback cannot drift from the
        // order the shifting array and the ring both produce.
        for capacity in 0..=12_u64 {
            let span = capacity.max(1);
            for head in 0..span {
                for count in 0..=span {
                    let entity = crate::device_compaction::CompactionEntity {
                        source_words: 64,
                        capacity,
                        head,
                        count,
                    };
                    let gathered = entity.live_record_slots(
                        crate::device_compaction::CompactionShape::Ring,
                        TCP_LEDGER_RECORD_WORDS,
                    );
                    let canonical = (0..count as usize)
                        .map(|logical| {
                            ledger_record_slot(64, capacity as usize, head as usize, logical) as u64
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        gathered, canonical,
                        "capacity {capacity} head {head} count {count}"
                    );
                }
            }
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "single-conditional-subtraction bound")]
    fn a_slot_past_the_kernels_reduction_bound_is_rejected_by_the_mirror() {
        // Non-vacuity for the precondition assertion itself: the one input the kernels would get
        // wrong is the one input the mirror refuses to answer.
        let _ = ledger_record_slot(64, 4, 3, 5);
    }

    #[test]
    fn monotone_append_and_prefix_removal_decode_identically() {
        let ops = (0..8)
            .map(|index| insert(index, index * 256, 256, false))
            .chain([LedgerOp::Acknowledge {
                acknowledgment: 3 * 256,
            }])
            .chain((8..14).map(|index| insert(index, index * 256, 256, false)))
            .chain([LedgerOp::Acknowledge {
                acknowledgment: 9 * 256,
            }])
            .collect::<Vec<_>>();
        assert_trace_byte_identical(8, &[], &ops);
    }

    #[test]
    fn ring_wraps_past_the_arena_end_without_compaction() {
        // Capacity 4 with 12 appends interleaved with single-record acknowledgements forces the
        // head all the way around the arena three times.
        let mut ops = Vec::new();
        for index in 0..12 {
            ops.push(insert(index, index * 256, 256, false));
            if index >= 3 {
                ops.push(LedgerOp::Acknowledge {
                    acknowledgment: (index - 2) * 256,
                });
            }
        }
        assert_trace_byte_identical(4, &[], &ops);
    }

    #[test]
    fn partial_cumulative_ack_rekeys_the_head_record_identically() {
        let ops = vec![
            insert(0, 0, 256, false),
            insert(1, 256, 256, false),
            insert(2, 512, 256, false),
            // Acknowledges 0..320: record 0 leaves, record 1 is truncated to [320, 512).
            LedgerOp::Acknowledge {
                acknowledgment: 320,
            },
            insert(3, 768, 256, false),
            // A retransmission of the truncated head replaces in place.
            insert(4, 320, 192, true),
            LedgerOp::Acknowledge {
                acknowledgment: 512,
            },
        ];
        assert_trace_byte_identical(6, &[], &ops);
    }

    #[test]
    fn interior_and_prefix_inserts_decode_identically_after_the_head_moved() {
        let ops = vec![
            insert(0, 1_024, 256, false),
            insert(1, 1_280, 256, false),
            LedgerOp::Acknowledge {
                acknowledgment: 1_280,
            },
            insert(2, 1_536, 256, false),
            insert(3, 1_792, 256, false),
            // Prefix insert below the current head record.
            insert(4, 1_024, 256, true),
            // Interior insert between two live records.
            insert(5, 1_664, 128, true),
        ];
        assert_trace_byte_identical(8, &[], &ops);
    }

    #[test]
    fn capacity_faults_and_size_conflicts_agree_between_representations() {
        let ops = [
            insert(0, 0, 256, false),
            insert(1, 256, 256, false),
            // Third append overflows a capacity-2 ring.
            insert(2, 512, 256, false),
            // A replacement with a different size is a semantic conflict, not a capacity fault.
            insert(3, 256, 128, true),
        ];
        let mut array = LedgerPlane::seed(2, &[]);
        let mut ring = LedgerPlane::seed(2, &[]);
        let outcomes = ops
            .iter()
            .copied()
            .map(|op| {
                let array_outcome = apply_array(&mut array, op);
                assert_eq!(array_outcome, apply_ring(&mut ring, op));
                array_outcome
            })
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes,
            vec![
                LedgerOutcome::Applied,
                LedgerOutcome::Applied,
                LedgerOutcome::CapacityExceeded {
                    capacity: 2,
                    demand: 3
                },
                LedgerOutcome::SizeConflict,
            ]
        );
        assert_eq!(array.canonical_bytes(false), ring.canonical_bytes(true));
    }

    #[test]
    fn a_pseudorandom_op_trace_stays_byte_identical() {
        // A deterministic linear congruential walk over inserts, retransmissions and ACKs.
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state >> 33
        };
        let mut ops = Vec::new();
        let mut highest = 0_u64;
        let mut acknowledged = 0_u64;
        for id in 0..400_u64 {
            match next() % 4 {
                0 if highest > acknowledged => {
                    acknowledged += 256 * (next() % 3);
                    acknowledged = acknowledged.min(highest);
                    ops.push(LedgerOp::Acknowledge {
                        acknowledgment: acknowledged,
                    });
                }
                1 if highest > acknowledged => {
                    let span = (highest - acknowledged) / 256;
                    let pick = acknowledged + 256 * (next() % span.max(1));
                    ops.push(insert(id, pick, 256, true));
                }
                _ => {
                    ops.push(insert(id, highest, 256, false));
                    highest += 256;
                }
            }
        }
        assert_trace_byte_identical(64, &[], &ops);
    }

    #[test]
    fn the_high_water_word_tracks_the_running_maximum_occupancy() {
        let mut ring = LedgerPlane::seed(16, &[]);
        for index in 0..5 {
            apply_ring(&mut ring, insert(index, index * 256, 256, false));
        }
        assert_eq!(ring.count(), 5);
        assert_eq!(ring.high_water(), 5);
        apply_ring(
            &mut ring,
            LedgerOp::Acknowledge {
                acknowledgment: 4 * 256,
            },
        );
        assert_eq!(ring.count(), 1);
        assert_eq!(ring.high_water(), 5, "acknowledgement must not lower it");
        // A retransmission replaces in place and must not raise it either.
        apply_ring(&mut ring, insert(9, 4 * 256, 256, true));
        assert_eq!(ring.count(), 1);
        assert_eq!(ring.high_water(), 5);
        for index in 5..11 {
            apply_ring(&mut ring, insert(index, index * 256, 256, false));
        }
        assert_eq!(ring.count(), 7);
        assert_eq!(ring.high_water(), 7, "a new peak must raise it");
    }

    #[test]
    fn ledger_mutations_never_disturb_the_armed_timer_slot_word() {
        // The T20g live-state contract parks the flow's fallback-heap slot + 1 in metadata word
        // +3. Ring conversion adds two neighbouring words, so every mutation site must leave that
        // word alone; a stray write would silently unbind a superseded retransmission timeout.
        let mut ring = LedgerPlane::seed(4, &[]);
        ring.words[LEDGER_META_TIMER_SLOT] = 7_919;
        let ops = [
            insert(0, 0, 256, false),
            insert(1, 256, 256, false),
            insert(2, 512, 256, false),
            LedgerOp::Acknowledge {
                acknowledgment: 512,
            },
            insert(3, 768, 256, false),
            insert(4, 1_024, 256, false),
            insert(5, 512, 256, true),
            LedgerOp::Acknowledge {
                acknowledgment: 1_280,
            },
        ];
        for op in ops {
            apply_ring(&mut ring, op);
            assert_eq!(ring.words[LEDGER_META_TIMER_SLOT], 7_919, "after {op:?}");
        }
    }

    #[test]
    fn the_fault_payload_reads_every_flows_high_water_word() {
        // The vector payload is the whole point of T20i layer 2: one readback, every flow.
        let flows = 5;
        let mut meta = vec![0_u64; 3 + flows * TCP_LEDGER_META_WORDS];
        let peaks = [0_u64, 11_058, 520, u64::from(u32::MAX) + 9, 2];
        for (flow, peak) in peaks.iter().copied().enumerate() {
            meta[3 + flow * TCP_LEDGER_META_WORDS + LEDGER_META_HIGH_WATER] = peak;
        }
        assert_eq!(
            ledger_high_water_vector(&meta, 3, flows),
            vec![0, 11_058, 520, u32::MAX, 2]
        );
        // A short plane reads as zero rather than panicking: a fault payload is advisory sizing
        // input, never a correctness input.
        assert_eq!(
            ledger_high_water_vector(&meta, 3, flows + 2).len(),
            flows + 2
        );
    }

    #[test]
    fn seeded_records_initialize_the_high_water_and_head() {
        let seed: [[u64; TCP_LEDGER_RECORD_WORDS]; 3] = [
            [10, 256, 0, 5, 0],
            [11, 256, 256, 6, 0],
            [12, 256, 512, 7, 1],
        ];
        let plane = LedgerPlane::seed(8, &seed);
        assert_eq!(plane.count(), 3);
        assert_eq!(plane.high_water(), 3);
        assert_eq!(plane.head(), 0);
        assert_eq!(plane.words.len(), TCP_LEDGER_META_WORDS + 8 * 5);
        assert_eq!(plane.canonical_bytes(false), plane.canonical_bytes(true));
    }
}

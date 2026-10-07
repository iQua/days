//! The seeded per-pair routing matrix of an imbalanced all-to-all (P16 H1, ruling R7).
//!
//! It is image semantics: the lowering sizes each pair's stage from it, the image keeps the
//! parameters of every seeded collective ([`crate::SimulationImage::seeded_all_to_alls`]), and
//! LeanGuard re-derives the same matrix from the progress certificate (pure `Nat` arithmetic in
//! `lean/LeanGuard/P10c/Collective/SeededMatrix.lean`). Every draw is an integer.

/// The skew of the seeded routing matrix's expert popularity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RoutingSkew {
    /// Every expert equally likely.
    Uniform,
    /// Expert of popularity rank `p` has weight `floor(2^40 / (p + 1))`, an integer Zipf(1) over
    /// a seeded permutation of the experts.
    Zipf1,
}

/// The seeded, deterministic per-pair bytes of an imbalanced all-to-all (ruling R7).
///
/// Each source rank routes `tokens x topk` copies to experts drawn from the popularity weights
/// with an integer SplitMix64 stream; expert `e` lives on rank `e / (experts / n)`. A pair's bytes
/// are its copies times `bytes_per_copy`; copies to the source itself are not sent. `matrix` and
/// `group` distinguish the matrices of one scenario (a layer and microbatch, an EP group); the
/// expert permutation is one per `(seed, matrix, group)`. `transpose` returns the matrix's
/// transpose (an MoE combine sends back what its dispatch sent). Every draw is an integer: no
/// floating point anywhere.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SeededAllToAll {
    pub seed: u64,
    pub matrix: u64,
    pub group: u64,
    pub transpose: bool,
    pub experts: u64,
    pub topk: u64,
    pub tokens: u64,
    pub bytes_per_copy: u64,
    pub skew: RoutingSkew,
}

/// SplitMix64's output function.
const fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A SplitMix64 stream.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    /// A draw in `0..bound` (`bound > 0`), by remainder: deterministic, with a bias below
    /// `bound / 2^64`.
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

impl SeededAllToAll {
    /// The bytes from rank `source` to rank `target` of an `n`-rank group, row-major
    /// (`bytes[source * n + target]`), zero on the diagonal. `None` when `experts` is not a
    /// positive multiple of `n`, or a product overflows.
    pub fn bytes(&self, ranks: u64) -> Option<Vec<u64>> {
        if ranks == 0 || self.experts == 0 || !self.experts.is_multiple_of(ranks) {
            return None;
        }
        let n = usize::try_from(ranks).ok()?;
        let experts = usize::try_from(self.experts).ok()?;
        let per_rank = self.experts / ranks;
        let key = mix(mix(mix(self.seed) ^ self.matrix) ^ self.group);
        // The popularity order: a Fisher-Yates permutation of the experts.
        let mut order = (0..self.experts).collect::<Vec<_>>();
        let mut stream = SplitMix64(mix(key ^ 0x5045_524d));
        for index in (1..experts).rev() {
            let other = usize::try_from(stream.below(index as u64 + 1)).ok()?;
            order.swap(index, other);
        }
        // Cumulative integer weights by expert.
        let mut cumulative = Vec::with_capacity(experts);
        let mut total = 0_u64;
        for &popularity in &order {
            let weight = match self.skew {
                RoutingSkew::Uniform => 1,
                RoutingSkew::Zipf1 => (1_u64 << 40) / (popularity + 1),
            };
            total = total.checked_add(weight)?;
            cumulative.push(total);
        }
        let copies = self.tokens.checked_mul(self.topk)?;
        let mut counts = vec![0_u64; n * n];
        for source in 0..n {
            let mut stream = SplitMix64(mix(key ^ (source as u64 + 1)));
            for _ in 0..copies {
                let draw = stream.below(total);
                let expert = cumulative.partition_point(|&bound| bound <= draw) as u64;
                let target = usize::try_from(expert / per_rank).ok()?;
                if target != source {
                    counts[source * n + target] += 1;
                }
            }
        }
        let mut bytes = vec![0_u64; n * n];
        for source in 0..n {
            for target in 0..n {
                let count = if self.transpose {
                    counts[target * n + source]
                } else {
                    counts[source * n + target]
                };
                bytes[source * n + target] = count.checked_mul(self.bytes_per_copy)?;
            }
        }
        Some(bytes)
    }

    /// The bytes each source routes, its own copies included: `tokens x topk x bytes_per_copy`.
    pub fn routed_bytes_per_source(&self) -> Option<u64> {
        self.tokens
            .checked_mul(self.topk)?
            .checked_mul(self.bytes_per_copy)
    }
}

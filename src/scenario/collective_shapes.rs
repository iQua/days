//! The message shapes of the flagship's collective operations, shared by the lowering and the AICB
//! adapter (P16 H1, `days-gpu/evidence/P16/collops-design.md` §2-§3).
//!
//! Everything here is integer arithmetic on the operation's parameters, so the adapter's
//! pre-flight checks see exactly the messages the lowering emits.
//!
//! - [`simai_ring_channels`]: SimAI's NCCL ring channels (`MockNcclGroup::genringchannels`).
//! - [`uniform_floor_message_bytes`]: SimAI's per-message bytes.
//! - [`SeededAllToAll`]: the imbalanced arm's seeded per-pair routing matrix (ruling R7).

/// One collective algorithm, as a message shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectiveShape {
    AllGather,
    ReduceScatter,
    AllReduce,
    AllToAll,
}

impl CollectiveShape {
    /// Ring steps per channel: `n - 1`, twice that for AllReduce, one for an all-to-all.
    pub const fn steps(self, ranks: u64) -> u64 {
        match self {
            Self::AllGather | Self::ReduceScatter => ranks.saturating_sub(1),
            Self::AllReduce => 2 * ranks.saturating_sub(1),
            Self::AllToAll => 1,
        }
    }
}

/// SimAI's ring channels of a group (`MockNcclGroup::gen_local_ring` and `genringchannels`).
///
/// `ranks` is the group in SimAI's group order (ascending for every MockNcclGroup group) and
/// `server_of` maps a rank to its server. The group spans `nNodes` servers with `nlocal = n /
/// nNodes` ranks each; there are `nlocal` channels. Channel `k`'s local order is the first
/// `nlocal` sorted ranks rotated by `k`; server `i` repeats it shifted by `i * delta`, `delta =
/// ranks[nlocal] - ranks[0]`; the last local rank of each server links to the first of the next,
/// and the last server wraps to the first. Each returned channel lists the ranks in ring order:
/// rank `channel[j]` sends to `channel[(j + 1) % n]`.
///
/// Returns `None` for a group that is not SimAI-regular (servers with unequal rank counts, or a
/// shifted copy that is not the group).
pub fn simai_ring_channels(ranks: &[u64], server_of: impl Fn(u64) -> u64) -> Option<Vec<Vec<u64>>> {
    let n = ranks.len();
    if n == 0 {
        return None;
    }
    let mut servers = ranks
        .iter()
        .map(|&rank| server_of(rank))
        .collect::<Vec<_>>();
    servers.sort_unstable();
    servers.dedup();
    let nodes = servers.len();
    if n % nodes != 0 {
        return None;
    }
    let nlocal = n / nodes;
    let mut sorted = ranks.to_vec();
    sorted.sort_unstable();
    let local = &sorted[..nlocal];
    let delta = if nodes > 1 {
        ranks[nlocal].checked_sub(ranks[0])?
    } else {
        0
    };
    let mut channels = Vec::with_capacity(nlocal);
    for k in 0..nlocal {
        let mut order = Vec::with_capacity(n);
        for i in 0..nodes as u64 {
            for j in 0..nlocal {
                order.push(local[(k + j) % nlocal].checked_add(i.checked_mul(delta)?)?);
            }
        }
        let mut members = order.clone();
        members.sort_unstable();
        if members != sorted {
            return None;
        }
        channels.push(order);
    }
    Some(channels)
}

/// SimAI's bytes per message: `floor(floor(S / n) / c)` for a ring over `c` channels, and
/// `floor(S / n)` per ordered pair for an all-to-all (`channels` is ignored there).
pub const fn uniform_floor_message_bytes(
    shape: CollectiveShape,
    total_bytes: u64,
    ranks: u64,
    channels: u64,
) -> u64 {
    if ranks == 0 || channels == 0 {
        return 0;
    }
    match shape {
        CollectiveShape::AllToAll => total_bytes / ranks,
        _ => total_bytes / ranks / channels,
    }
}

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
        if ranks == 0 || self.experts == 0 || self.experts % ranks != 0 {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// TP2/EP32 at 1,024 GPUs (simai-semantics-facts §4): an EP group is 32 ranks at stride 2
    /// across 8 servers, so 4 channels; channel 1 starts at the second local rank and its
    /// server hop leaves from local rank 0 to the next server's second local rank.
    #[test]
    fn simai_channels_rotate_the_local_ring_and_wrap_across_rails() {
        let ranks = (0..32).map(|l| 2 * l).collect::<Vec<u64>>();
        let channels = simai_ring_channels(&ranks, |rank| rank / 8).unwrap();
        assert_eq!(channels.len(), 4);
        assert_eq!(&channels[0][..8], &[0, 2, 4, 6, 8, 10, 12, 14]);
        assert_eq!(&channels[1][..8], &[2, 4, 6, 0, 10, 12, 14, 8]);
        assert_eq!(channels[1][31], 8 * 7 + 0);
        // One rank per server: a single ring in group order.
        let spread = [3, 11, 19, 27];
        assert_eq!(
            simai_ring_channels(&spread, |rank| rank / 8).unwrap(),
            vec![vec![3, 11, 19, 27]]
        );
        // TP2 inside a server: two channels, one each way.
        assert_eq!(
            simai_ring_channels(&[6, 7], |rank| rank / 8).unwrap(),
            vec![vec![6, 7], vec![7, 6]]
        );
        assert_eq!(simai_ring_channels(&[0, 1, 8], |rank| rank / 8), None);
    }

    #[test]
    fn uniform_floor_drops_the_remainder() {
        assert_eq!(
            uniform_floor_message_bytes(CollectiveShape::ReduceScatter, 5_967_183_872, 512, 4),
            2_913_664
        );
        assert_eq!(
            uniform_floor_message_bytes(CollectiveShape::AllToAll, 67_108_864, 32, 1),
            2_097_152
        );
        assert_eq!(
            uniform_floor_message_bytes(CollectiveShape::AllGather, 7, 4, 2),
            0
        );
    }

    fn seeded(transpose: bool, skew: RoutingSkew) -> SeededAllToAll {
        SeededAllToAll {
            seed: 7,
            matrix: 3,
            group: 1,
            transpose,
            experts: 128,
            topk: 8,
            tokens: 2_048,
            bytes_per_copy: 4_096,
            skew,
        }
    }

    #[test]
    fn the_seeded_matrix_is_deterministic_conserving_and_skewed() {
        let n = 32;
        let dispatch = seeded(false, RoutingSkew::Zipf1).bytes(n).unwrap();
        assert_eq!(
            dispatch,
            seeded(false, RoutingSkew::Zipf1).bytes(n).unwrap()
        );
        let combine = seeded(true, RoutingSkew::Zipf1).bytes(n).unwrap();
        let n = n as usize;
        for source in 0..n {
            assert_eq!(dispatch[source * n + source], 0);
            let sent = (0..n)
                .map(|target| dispatch[source * n + target])
                .sum::<u64>();
            assert!(sent <= 2_048 * 8 * 4_096);
            for target in 0..n {
                assert_eq!(combine[target * n + source], dispatch[source * n + target]);
            }
        }
        // Zipf(1) concentrates traffic on the ranks of the popular experts.
        let received = |matrix: &[u64]| {
            (0..n)
                .map(|target| {
                    (0..n)
                        .map(|source| matrix[source * n + target])
                        .sum::<u64>()
                })
                .collect::<Vec<_>>()
        };
        let skewed = received(&dispatch);
        let uniform = received(&seeded(false, RoutingSkew::Uniform).bytes(32).unwrap());
        let spread = |column: &[u64]| column.iter().max().unwrap() - column.iter().min().unwrap();
        assert!(spread(&skewed) > 4 * spread(&uniform));
        assert_ne!(
            dispatch,
            SeededAllToAll {
                seed: 8,
                ..seeded(false, RoutingSkew::Zipf1)
            }
            .bytes(32)
            .unwrap()
        );
        assert_eq!(seeded(false, RoutingSkew::Zipf1).bytes(30), None);
    }
}

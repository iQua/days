//! SimAI's communication groups (`MockNcclGroup.cc:25-160`) and the Megatron placement.
//!
//! Under the SimAI fidelity, SimAI forces PP to 1 when it forms groups (`Sys.cc:1355-1368`):
//! `DP = W / TP`, `DP_EP = DP / EP`. TP groups are consecutive ranks, DP groups stride TP, an EP
//! group takes slot `k` of `EP` consecutive TP groups, and a DP_EP group takes slot `k` of TP
//! groups at stride `EP`. Under the Megatron fidelity (Megatron-Core's `tp-cp-ep-dp-pp` order,
//! CP = 1) the same formulas apply to each pipeline stage's contiguous block of `W / PP` ranks,
//! and stage `s` rank `r` pairs with `r + W / PP` of stage `s + 1`.
//!
//! A family stores its groups flat (all groups of one family have one size) with a rank-to-group
//! index, so forming the groups of `W` ranks costs a fixed number of allocations.

use super::{AicbError, GroupKind, Header};

/// Which semantics the run reproduces (design note A4).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Fidelity {
    /// SimAI-identical: SimAI's groups with PP forced to 1, every SimAI refusal enforced.
    Simai,
    /// Megatron-faithful: per-stage groups and pipeline Send/Recv.
    Megatron,
}

/// All groups of one kind: disjoint, equal-sized, in SimAI's order (by smallest rank).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupFamily {
    size: u32,
    ranks: Vec<u32>,
    group_of: Vec<u32>,
}

const NO_GROUP: u32 = u32::MAX;

impl GroupFamily {
    #[allow(dead_code)] // RED skeleton
    fn new(world: u32, size: u32, ranks: Vec<u32>) -> Self {
        let mut group_of = vec![NO_GROUP; world as usize];
        if size > 0 {
            for (index, chunk) in ranks.chunks_exact(size as usize).enumerate() {
                for &rank in chunk {
                    debug_assert_eq!(group_of[rank as usize], NO_GROUP, "groups are disjoint");
                    group_of[rank as usize] = index as u32;
                }
            }
        }
        Self {
            size,
            ranks,
            group_of,
        }
    }

    /// Ranks per group (0 for an empty family).
    pub fn group_size(&self) -> u32 {
        self.size
    }

    /// Number of groups.
    pub fn len(&self) -> usize {
        if self.size == 0 {
            0
        } else {
            self.ranks.len() / self.size as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Group `index`, in SimAI's rank order.
    pub fn group(&self, index: usize) -> &[u32] {
        let size = self.size as usize;
        &self.ranks[index * size..(index + 1) * size]
    }

    /// The groups in order.
    pub fn groups(&self) -> impl Iterator<Item = &[u32]> {
        self.ranks.chunks_exact(self.size.max(1) as usize)
    }

    /// The index of the group containing `rank`, if any.
    pub fn group_index_of(&self, rank: u32) -> Option<usize> {
        match self.group_of.get(rank as usize) {
            Some(&index) if index != NO_GROUP => Some(index as usize),
            _ => None,
        }
    }
}

/// Every group of a trace's ranks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Groups {
    pub fidelity: Fidelity,
    pub world: u32,
    pub gpus_per_server: u32,
    /// Pipeline stages that carry traffic (1 under the SimAI fidelity).
    pub stages: u32,
    pub tp: GroupFamily,
    pub dp: GroupFamily,
    pub ep: GroupFamily,
    pub dp_ep: GroupFamily,
    /// `(r, r + W / PP)` for every rank `r` of stages `0..PP-1`, ascending.
    pub pp_pairs: Vec<(u32, u32)>,
}

impl Groups {
    pub fn family(&self, kind: GroupKind) -> &GroupFamily {
        match kind {
            GroupKind::Tp => &self.tp,
            GroupKind::Dp => &self.dp,
            GroupKind::Ep => &self.ep,
            GroupKind::DpEp => &self.dp_ep,
        }
    }

    /// The server of a rank (GPU `i` sits in server `i / gpus_per_server`).
    pub fn server_of(&self, rank: u32) -> u32 {
        rank / self.gpus_per_server
    }

    /// Ranks per pipeline stage.
    pub fn stage_ranks(&self) -> u32 {
        self.world / self.stages
    }
}

/// Forms the trace's groups on `gpus_per_server`-GPU servers.
pub fn form_groups(
    _header: &Header,
    _fidelity: Fidelity,
    _gpus_per_server: u32,
) -> Result<Groups, AicbError> {
    Err(AicbError::new("group formation is not implemented"))
}

/// The groups in the text form of `mockncclgroup_dump` (days-gpu
/// `evidence/P16/aicb-design/tooling`), without its ring-channel lines: the header line, then
/// for TP, DP, EP and DP_EP each group as `group <type> nNodes <n> nRanks <n> ranks …`.
pub fn render_mockncclgroup(_groups: &Groups, _header: &Header) -> String {
    String::new()
}

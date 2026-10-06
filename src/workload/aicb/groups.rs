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

use std::fmt::Write as _;

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
    header: &Header,
    fidelity: Fidelity,
    gpus_per_server: u32,
) -> Result<Groups, AicbError> {
    let world = header.all_gpus;
    let tp = header.tp;
    let ep = header.ep;
    if gpus_per_server == 0 || !world.is_multiple_of(gpus_per_server) {
        return Err(AicbError::new(format!(
            "all_gpus = {world} is not a multiple of {gpus_per_server} GPUs per server"
        )));
    }
    if tp < 2 {
        return Err(AicbError::new(format!(
            "model_parallel_NPU_group = {tp}: SimAI builds no TP or EP group below 2 and reads an \
             empty dimension vector (simai-semantics-facts §4)"
        )));
    }
    // SimAI records a TP group's server count as ceil(TP / GPUs per server); it is the true
    // count only when TP divides the server or the server divides TP.
    if !gpus_per_server.is_multiple_of(tp) && !tp.is_multiple_of(gpus_per_server) {
        return Err(AicbError::new(format!(
            "TP = {tp} neither divides nor is a multiple of {gpus_per_server} GPUs per server"
        )));
    }
    let stages = match fidelity {
        Fidelity::Simai => 1,
        Fidelity::Megatron => header.pp,
    };
    if !world.is_multiple_of(tp * stages) {
        return Err(AicbError::new(format!(
            "TP = {tp} x PP = {stages} does not divide all_gpus = {world}"
        )));
    }
    let stage_world = world / stages;
    let dp = stage_world / tp;
    if !dp.is_multiple_of(ep) {
        return Err(AicbError::new(format!(
            "EP = {ep} does not divide DP = {dp} (MockNcclGroup forms no group, \
             MockNcclGroup.cc:42)"
        )));
    }
    let dp_ep = dp / ep;
    // Each family covers every rank at most once: one allocation of W slots each, whatever W is.
    // (only for the families that exist).
    let present = [true, dp > 1, ep > 1, dp_ep > 1];
    let mut families: [(u32, Vec<u32>); 4] = std::array::from_fn(|kind| {
        let capacity = if present[kind] { world as usize } else { 0 };
        (0, Vec::with_capacity(capacity))
    });
    for stage in 0..stages {
        let base = stage * stage_world;
        let tp_group = |index: u32, slot: u32| base + index * tp + slot;
        let tp_groups = stage_world / tp;
        // TP: consecutive ranks.
        families[0].0 = tp;
        families[0]
            .1
            .extend((0..tp_groups).flat_map(|i| (0..tp).map(move |j| tp_group(i, j))));
        // DP: stride W / DP = TP.
        if dp > 1 {
            families[1].0 = dp;
            let stride = stage_world / dp;
            families[1]
                .1
                .extend((0..stride).flat_map(|i| (0..dp).map(move |j| base + i + j * stride)));
        }
        // EP: slot k of EP consecutive TP groups.
        if ep > 1 {
            families[2].0 = ep;
            for block in 0..tp_groups / ep {
                for slot in 0..tp {
                    families[2]
                        .1
                        .extend((block * ep..(block + 1) * ep).map(|l| tp_group(l, slot)));
                }
            }
        }
        // DP_EP: slot k of TP groups at stride EP.
        if dp_ep > 1 {
            families[3].0 = dp_ep;
            for first in 0..tp_groups / dp_ep {
                for slot in 0..tp {
                    families[3]
                        .1
                        .extend((0..dp_ep).map(|l| tp_group(first + l * ep, slot)));
                }
            }
        }
    }
    let [tp_family, dp_family, ep_family, dp_ep_family] =
        families.map(|(size, ranks)| GroupFamily::new(world, size, ranks));
    let pp_pairs = (0..world - stage_world)
        .map(|rank| (rank, rank + stage_world))
        .collect();
    Ok(Groups {
        fidelity,
        world,
        gpus_per_server,
        stages,
        tp: tp_family,
        dp: dp_family,
        ep: ep_family,
        dp_ep: dp_ep_family,
        pp_pairs,
    })
}

/// The groups in the text form of `mockncclgroup_dump` (days-gpu
/// `evidence/P16/aicb-design/tooling`), without its ring-channel lines: the header line, then
/// for TP, DP, EP and DP_EP each group as `group <type> nNodes <n> nRanks <n> ranks …`.
pub fn render_mockncclgroup(groups: &Groups, header: &Header) -> String {
    let dp = groups.stage_ranks() / header.tp;
    let mut out = format!(
        "mockncclgroup W {} gpus_per_server {} TP {} DP {} PP 1 EP {} DP_EP {}\n",
        groups.world,
        groups.gpus_per_server,
        header.tp,
        dp,
        header.ep,
        dp / header.ep
    );
    for (name, family) in [
        ("TP", &groups.tp),
        ("DP", &groups.dp),
        ("EP", &groups.ep),
        ("DP_EP", &groups.dp_ep),
    ] {
        for group in family.groups().take(family.len()) {
            // MockNcclGroup.cc: a TP group records ceil(TP / GPUs per server) nodes, every other
            // group its distinct servers (equal here: form_groups refuses the other TP shapes).
            let mut nodes = 0;
            let mut last = None;
            for &rank in group {
                let server = groups.server_of(rank);
                if last != Some(server) {
                    nodes += 1;
                    last = Some(server);
                }
            }
            let _ = write!(
                out,
                "group {name} nNodes {nodes} nRanks {} ranks",
                group.len()
            );
            for rank in group {
                let _ = write!(out, " {rank}");
            }
            out.push('\n');
        }
    }
    out
}

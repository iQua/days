//! The typed workload IR the AICB adapter lowers into (P16 H1, ruling R12;
//! `days-gpu/evidence/P16/collops-design.md` §4.2).
//!
//! A [`Workload`] is a scenario's stage groups without their TOML text: rank groups, transport
//! templates, and operations that follow one another. [`compile_config_with_workload`] lowers it
//! with the topology, switch and link configuration of a TOML scenario, through exactly the path a
//! TOML `[[collective]]` or `[[compute]]` takes, so a workload and its TOML rendering lower to the
//! same image.
//!
//! [`compile_config_with_workload`]: super::compile_config_with_workload

use super::collective_shapes::SeededAllToAll;

/// One scenario's operations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Workload {
    /// Rank groups: each the hosts of its ranks, in rank order.
    pub groups: Vec<Vec<u64>>,
    /// Transport templates: each the TOML body of a `[collective.traffic]` table (its `arr_dist`
    /// and `pkt_size_dist`, and its `[tcp]`, or `[dcqcn]` and `[roce]`, subtables). An
    /// operation's own byte size replaces `size`.
    pub transports: Vec<Transport>,
    /// The operations. ECMP port ordinals follow each collective's `issue_ordinal` (ruling C2),
    /// then key content.
    pub operations: Vec<Operation>,
}

/// A transport template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transport {
    /// `TCP` or `RoCE`.
    pub flow_type: String,
    /// The class (IEEE 802.1Q priority, 0..=7).
    pub priority: u8,
    /// The traffic table's TOML body.
    pub traffic: String,
}

/// One operation over one rank group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    /// Index into [`Workload::groups`].
    pub group: usize,
    /// The operations this one follows (indices into [`Workload::operations`]), host-matched: its
    /// rank at host `h` waits, at `h`, for each listed operation that runs on `h`, through that
    /// operation's rank there (equal host lists are the special case). Each listed operation runs
    /// on a host where this one starts (all its hosts; a Send/Recv's sender alone), every such host
    /// runs a listed operation, and a collective follows at least one compute operation.
    pub after: Vec<usize>,
    /// The issue stream (SimAI's queue) the operation runs on at each rank: 0 for the compute
    /// stream, another value for a data queue. The stage-aware sizing charges each stream's widest
    /// operation once (ruling R11 (a)), so an operation must follow the previous operation of its
    /// stream (through `after`) at every rank it shares with it.
    pub stream: u32,
    pub kind: OperationKind,
}

/// What an operation does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationKind {
    /// One delay-only stage per rank. The adapter fuses a rank's consecutive delays (compute,
    /// `process_time` and single-server collectives, H3 ruling A1) into one.
    Compute {
        duration_ns: u64,
    },
    Collective(Collective),
}

/// A collective operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Collective {
    pub algorithm: Algorithm,
    /// The whole payload in bytes (SimAI's `comm_size`).
    pub bytes: u64,
    /// Index into [`Workload::transports`].
    pub transport: usize,
    /// Ring channels as host orders (`simai_ring_channels`); `None` for one ring in rank order,
    /// an all-to-all and a send/recv.
    pub channels: Option<Vec<Vec<u64>>>,
    /// `true` for `UniformFloor` chunks (SimAI's), `false` for `EqualRemainderLast` (one ring).
    pub uniform_floor: bool,
    /// An all-to-all's seeded per-pair sizes; uniform without.
    pub seeded: Option<SeededAllToAll>,
    /// The collective's position in the realized issue order (ruling C2), which orders its flows,
    /// and so SimAI's per-pair ECMP port ordinals, before key content; `None` keeps key order.
    /// The TOML rendering is `issue_ordinal`.
    pub issue_ordinal: Option<u64>,
}

/// A collective algorithm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Algorithm {
    AllGather,
    ReduceScatter,
    AllReduce,
    AllToAll,
    /// Rank 0 of a two-rank group sends to rank 1.
    SendRecv,
}

impl Algorithm {
    pub(crate) const fn collective_type(self) -> &'static str {
        match self {
            Self::AllGather => "AllGather",
            Self::ReduceScatter => "ReduceScatter",
            Self::AllReduce => "RingAllReduce",
            Self::AllToAll => "AllToAll",
            Self::SendRecv => "SendRecv",
        }
    }
}

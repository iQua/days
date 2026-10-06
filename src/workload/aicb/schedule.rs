#![allow(dead_code)] // RED skeleton: the planner is not implemented yet.
//! The per-rank schedule SimAI runs for a trace, planned once (design note §3).
//!
//! SimAI's workload loop (`Workload.cc:791-900`, `Layer.cc:92-114`, `Layer.cc:1213-1222`)
//! runs, at every rank, in nanoseconds:
//!
//! ```text
//! forward, i = 0 .. N-1:   delay fp_compute; [fp collective (blocking); delay process_time]; delay 1
//! backward, i = N-1 .. 0:  delay ig_compute; [ig collective (blocking); delay process_time]; delay 1;
//!                          delay wg_compute; [fork the wg collective onto the data queue]; delay 1
//! ```
//!
//! This module turns a parsed trace and its groups into that chain, applies SimAI's size rules in
//! SimAI's order (the fp clamp, per-message floors, ring elision, the 0 -> 1 B rule, the wire-size
//! hang gate), fuses each rank's delay-only stretches into segments (ruling A1), derives the data
//! queue's LIFO order (R9) and the realized start order the ECMP ordinals follow (A7), and assigns
//! the imbalanced arm's matrices (R7). Everything here is per trace, not per rank: every rank of a
//! pipeline stage runs the same chain, so nothing allocates per rank.

use super::{AicbError, Algorithm, Column, Fidelity, GroupKind, Groups, Trace};

/// SimAI narrows ticks to `int` when it schedules them (`Sys.cc:1973`, `:2110-2115`).
const SIMAI_TICK_MAX: u64 = i32::MAX as u64;
/// SimAI's forward-column clamp (`Workload.cc:811-813`): sizes in (0, 4096) become 4096.
const FP_CLAMP_BYTES: u64 = 4096;
/// Wire bytes SimAI adds to a data packet's payload (PPP 14, IP 20, UDP 8, SeqTs 10).
const WIRE_HEADER_BYTES: u64 = 52;
/// SimAI raises a message's "sent" only when its last packet's wire size is in (60, 9000)
/// (`qbb-net-device.cc:618`).
const SEND_CALLBACK_WIRE: (u64, u64) = (60, 9000);

/// Expert routing of the all-to-all collectives (ruling A4).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpertRouting {
    /// SimAI's equal `floor(S / n)` chunk per ordered pair.
    Uniform,
    /// H1's seeded integer token-routing model (R7); Days-only.
    Imbalanced(ImbalanceParams),
}

/// The routing model's inputs that the trace does not carry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImbalanceParams {
    pub seed: u64,
    pub experts: u32,
    pub topk: u32,
    /// Tokens per rank (`mbs * seq / TP` under sequence parallelism).
    pub tokens_per_rank: u64,
    /// Zipf exponent of expert popularity: 1, or 0 for equal popularity (seeded draws).
    pub zipf: u8,
}

/// SimAI's environment pins (`AS_SEND_LAT`, `AS_NVLS_ENABLE`, `AS_PXN_ENABLE`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimaiEnv {
    pub send_lat_us: u64,
    pub nvls_enable: bool,
    pub pxn_enable: bool,
}

/// What the plan needs beyond the trace and its groups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanOptions {
    pub fidelity: Fidelity,
    pub expert_routing: ExpertRouting,
    /// Packet payload bytes (`PACKET_PAYLOAD_SIZE`).
    pub mtu_bytes: u64,
    /// SimAI's `gpu_type` (the topology file's), which decides NVLS eligibility.
    pub gpu_type: String,
    /// Required under the SimAI fidelity.
    pub simai_env: Option<SimaiEnv>,
}

/// Lower bounds on one ring step's hop, for the R9 sufficient test.
pub trait HopBounds {
    /// A message of `bytes` between GPUs of different servers.
    fn network_hop_ns(&self, bytes: u64) -> u64;
    /// A message of `bytes` between GPUs of one server, whose sender's NVLink port carries
    /// `port_bytes` in that step.
    fn nvlink_hop_ns(&self, bytes: u64, port_bytes: u64) -> u64;
}

/// Bounds from link delays and the NIC rate alone: a network message crosses at least two links
/// and is serialized at least once at the NIC rate; an NVLink message crosses two NVLink hops.
/// Sound for any rail fabric; `ServerLocality` gives tighter NVLink bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PropagationBounds {
    pub link_delay_ns: u64,
    pub nic_rate_bps: u64,
    pub nvlink_delay_ns: u64,
}

impl HopBounds for PropagationBounds {
    fn network_hop_ns(&self, bytes: u64) -> u64 {
        let serialize =
            (u128::from(bytes) * 8 * 1_000_000_000).div_ceil(u128::from(self.nic_rate_bps));
        (2 * u128::from(self.link_delay_ns) + serialize)
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn nvlink_hop_ns(&self, _bytes: u64, _port_bytes: u64) -> u64 {
        2 * self.nvlink_delay_ns
    }
}

/// Index of a collective in [`Plan::ops`].
pub type OpId = usize;

/// The imbalanced arm's routing matrix of an all-to-all (R7).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MatrixRef {
    /// The (dispatch, combine) pair this all-to-all belongs to, in trace order.
    pub matrix: u32,
    /// Dispatch uses `M`, combine `Mᵀ`; a record's ig all-to-all reverses its fp one.
    pub transpose: bool,
    /// `S / (tokens_per_rank * topk)`, exact.
    pub bytes_per_copy: u64,
}

/// One record column's collective, after SimAI's size rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectiveOp {
    /// Index into [`Trace::records`].
    pub record: usize,
    pub column: Column,
    pub algorithm: Algorithm,
    pub group: GroupKind,
    /// Bytes of the whole collective after the fp clamp.
    pub total_bytes: u64,
    /// Bytes of every message (ring: `floor(floor(S/n)/c)`; all-to-all: `floor(S/n)`, or 1 B
    /// under the SimAI fidelity's 0 -> 1 B rule). For the imbalanced arm, the uniform value.
    pub message_bytes: u64,
    pub group_size: u32,
    /// Servers each group spans.
    pub servers: u32,
    /// Ring channels `c = n / servers` (1 for an all-to-all).
    pub channels: u32,
    /// Ring steps (AG, RS `n - 1`; AR `2(n - 1)`; all-to-all 1).
    pub steps: u32,
    pub matrix: Option<MatrixRef>,
}

impl CollectiveOp {
    /// A collective whose every group lies in one server: per-rank delays only (H2-2(a)).
    pub fn single_server(&self) -> bool {
        self.servers == 1
    }

    /// Network messages of one group instance (one per server boundary per ring channel and
    /// step; one per ordered cross-server pair of an all-to-all).
    pub fn network_messages_per_group(&self) -> u64 {
        let (n, servers, steps) = (
            u64::from(self.group_size),
            u64::from(self.servers),
            u64::from(self.steps),
        );
        let per_server = n / servers;
        match self.algorithm {
            Algorithm::AllToAll => n * (n - per_server),
            _ if servers == 1 => 0,
            _ => u64::from(self.channels) * servers * steps,
        }
    }

    /// NVLink messages of one group instance (same-server hops and pairs).
    pub fn nvlink_messages_per_group(&self) -> u64 {
        let (n, servers, steps) = (
            u64::from(self.group_size),
            u64::from(self.servers),
            u64::from(self.steps),
        );
        let per_server = n / servers;
        match self.algorithm {
            Algorithm::AllToAll => n * (per_server - 1),
            _ if servers == 1 => u64::from(self.channels) * n * steps,
            _ => u64::from(self.channels) * (n - servers) * steps,
        }
    }

    /// A weight-gradient collective, which runs on the data queue.
    pub fn data_stream(&self) -> bool {
        self.column == Column::WeightGradient
    }
}

/// A pipeline-parallel transfer of one microbatch block across one stage boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PipelineTransfer {
    /// From stage `boundary` to `boundary + 1` (forward), or back (backward).
    pub boundary: u32,
    pub block: u32,
    pub backward: bool,
}

/// One item of a rank's chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChainItem {
    Delay(u64),
    /// A blocking model-stream collective.
    Collective(OpId),
    /// The issue of a data-stream collective; the chain continues at once.
    Fork(OpId),
    /// Waits for the delivery of a pipeline transfer.
    Receive(PipelineTransfer),
    /// Sends a pipeline transfer and waits for its completion (Megatron's `_communicate`).
    Send(PipelineTransfer),
}

/// What ends a fused segment (a cross-host point of the chain).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentEnd {
    Collective(OpId),
    Fork(OpId),
    Receive(PipelineTransfer),
    Send(PipelineTransfer),
}

/// A maximal delay-only stretch of a rank's chain, fused into one stage (ruling A1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Segment {
    /// The sum of the stretch's compute, `process_time` and `+1` delays.
    pub delay_ns: u64,
    /// Single-server collectives inside the stretch, whose per-rank delays add to `delay_ns`.
    pub single_server_ops: Vec<OpId>,
    pub end: SegmentEnd,
}

/// The chain of every rank of one pipeline stage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageChain {
    pub items: Vec<ChainItem>,
    /// The fused segments, each ending at a cross-host point. Delays after the last one have no
    /// successor and are not lowered.
    pub segments: Vec<Segment>,
    /// Stages the same chain would need without fusion: one compute stage per positive delay
    /// stretch between two non-delay items, plus one delay stage per single-server collective.
    pub unfused_stages: usize,
}

/// How the data queue's order was derived (R9).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataQueueOrder {
    /// The sufficient test held: SimAI's realized LIFO order.
    Lifo,
    /// At most two data ops, so at most one waits: issue order is SimAI's order.
    Fifo,
    /// The test failed; FIFO is a recorded difference.
    FifoFallback,
}

/// The data queue's derived order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataQueue {
    /// Data ops in issue order.
    pub issued: Vec<OpId>,
    /// Issue offsets after the first issue, in ns; `None` past a cross-host point.
    pub issue_offsets_ns: Vec<Option<u64>>,
    /// Lower bound of the first op's duration.
    pub head_lower_bound_ns: u64,
    pub kind: DataQueueOrder,
    /// Data ops in start order.
    pub order: Vec<OpId>,
}

/// An entry of the realized start order: what H1 numbers `issue_ordinal` by (A7).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartItem {
    Collective(OpId),
    Pipeline(PipelineTransfer),
}

/// Counts the manifest reports.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PlanCounters {
    /// fp sizes raised from (0, 4096) to 4096.
    pub fp_clamps: usize,
    /// wg columns of 0 B (a stream-less dataset SimAI never waits on).
    pub elided_zero_wg: usize,
    /// Rings whose per-message floor is 0 (SimAI sends nothing).
    pub elided_ring_floor: usize,
    /// All-to-alls with `S < n` under the Megatron fidelity (every pair is zero).
    pub elided_zero_all_to_all: usize,
    /// All-to-alls raised to 1 B per pair under the SimAI fidelity.
    pub one_byte_all_to_all: usize,
    /// Ops (and pipeline transfer sizes) in the hang window, recorded (Megatron fidelity).
    pub hang_window_recorded: usize,
}

/// The planned schedule of a trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Plan {
    pub fidelity: Fidelity,
    pub ops: Vec<CollectiveOp>,
    /// One chain per pipeline stage (one under the SimAI fidelity).
    pub stages: Vec<StageChain>,
    pub data_queue: DataQueue,
    pub start_order: Vec<StartItem>,
    /// Whether the static ordinals equal SimAI's run-time ports: no cross-host model-stream
    /// message is issued once a data op can be in flight (design note §0.1 item 5).
    pub ecmp_ordinals_exact: bool,
    pub counters: PlanCounters,
}

/// Whether SimAI would never raise "sent" for a message of `bytes` (design note §3.6).
pub fn in_hang_window(bytes: u64, mtu_bytes: u64) -> bool {
    if bytes == 0 {
        return true; // SimAI sends 1 B (`entry.h:140`), a 53 B packet
    }
    let last_payload = (bytes - 1) % mtu_bytes + 1;
    let wire = last_payload + WIRE_HEADER_BYTES;
    !(SEND_CALLBACK_WIRE.0 < wire && wire < SEND_CALLBACK_WIRE.1)
}

/// Plans a trace's schedule (design note §3).
pub fn plan_schedule(
    _trace: &Trace,
    _groups: &Groups,
    _options: &PlanOptions,
    _hops: &dyn HopBounds,
) -> Result<Plan, AicbError> {
    Err(AicbError::new("schedule planning is not implemented"))
}

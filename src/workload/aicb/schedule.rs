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
//! SimAI's order (the fp clamp, per-message floors, a ring that floors to 0 B refused under the
//! SimAI fidelity and dropped under Megatron's, the 0 -> 1 B rule, the wire-size hang gate),
//! fuses each rank's delay-only stretches into segments (ruling A1), derives the data queue's LIFO
//! order (R9) and the realized start order the ECMP ordinals follow (A7), and assigns the
//! imbalanced arm's matrices (R7). Everything here is per trace, not per rank: every rank of a
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
    /// Rings whose per-message floor is 0, dropped under the Megatron fidelity (with their
    /// `process_time`); the SimAI fidelity refuses them (SimAI never finishes such a ring).
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

/// The shape of one family's groups: servers spanned and ranks per server, equal in every group.
fn family_shape(groups: &Groups, kind: GroupKind) -> Result<(u32, u32), AicbError> {
    let family = groups.family(kind);
    let mut shape = None;
    for group in family.groups() {
        let mut servers = 0;
        let mut per_server = 0;
        let mut current = None;
        let mut run = 0;
        for &rank in group {
            let server = groups.server_of(rank);
            if current != Some(server) {
                if current.is_some() {
                    if per_server != 0 && run != per_server {
                        return Err(irregular(kind));
                    }
                    per_server = run;
                }
                servers += 1;
                current = Some(server);
                run = 0;
            }
            run += 1;
        }
        if per_server != 0 && run != per_server {
            return Err(irregular(kind));
        }
        let this = (servers, run);
        if *shape.get_or_insert(this) != this {
            return Err(irregular(kind));
        }
    }
    shape.ok_or_else(|| AicbError::new(format!("no {kind:?} group exists")))
}

fn irregular(kind: GroupKind) -> AicbError {
    AicbError::new(format!(
        "{kind:?} groups are not regular (equal ranks on each spanned server); SimAI's ring \
         channels need that"
    ))
}

/// Plans a trace's schedule (design note §3).
pub fn plan_schedule(
    trace: &Trace,
    groups: &Groups,
    options: &PlanOptions,
    hops: &dyn HopBounds,
) -> Result<Plan, AicbError> {
    let simai = options.fidelity == Fidelity::Simai;
    if options.mtu_bytes == 0 {
        return Err(AicbError::new("the packet payload size must be positive"));
    }
    if groups.fidelity != options.fidelity {
        return Err(AicbError::new(
            "the groups were formed for the other fidelity",
        ));
    }
    let env = match (options.fidelity, options.simai_env) {
        (Fidelity::Simai, None) => {
            return Err(AicbError::new(
                "the SimAI fidelity needs the AS_SEND_LAT, AS_NVLS_ENABLE and AS_PXN_ENABLE pins",
            ));
        }
        (_, env) => env,
    };
    if simai && env.is_some_and(|env| env.pxn_enable) {
        return Err(AicbError::new(
            "AS_PXN_ENABLE = 1 adds PXN forwarding flows, which Days does not model",
        ));
    }
    let imbalance = match options.expert_routing {
        ExpertRouting::Imbalanced(_) if simai => {
            return Err(AicbError::new(
                "imbalanced expert routing is a Days-only arm; use fidelity = \"megatron\"",
            ));
        }
        ExpertRouting::Imbalanced(params) => Some(params),
        ExpertRouting::Uniform => None,
    };
    let nvls = simai
        && env.is_some_and(|env| env.nvls_enable)
        && matches!(options.gpu_type.as_str(), "H100" | "H800");
    let mut counters = PlanCounters::default();
    let mut shapes: [Option<(u32, u32)>; 4] = [None; 4];
    let mut ops = Vec::new();
    // ops[op_of[record][column]]
    let mut op_of = vec![[None::<OpId>; 3]; trace.records.len()];
    for (index, record) in trace.records.iter().enumerate() {
        for (slot, column) in [
            Column::Forward,
            Column::InputGradient,
            Column::WeightGradient,
        ]
        .into_iter()
        .enumerate()
        {
            let entry = record.column(column);
            if simai && entry.compute_ns > SIMAI_TICK_MAX {
                return Err(AicbError::at(
                    record.line,
                    format!(
                        "compute of {} ns is above {SIMAI_TICK_MAX} (SimAI narrows ticks to int)",
                        entry.compute_ns
                    ),
                ));
            }
            let Some(comm) = entry.comm else { continue };
            let mut size = entry.size_bytes;
            if column == Column::Forward && 0 < size && size < FP_CLAMP_BYTES {
                size = FP_CLAMP_BYTES;
                counters.fp_clamps += 1;
            }
            if size == 0 {
                if column == Column::WeightGradient {
                    counters.elided_zero_wg += 1;
                    continue;
                }
                return Err(AicbError::at(
                    record.line,
                    format!(
                        "{column:?} collective of 0 B (SimAI's blocking collective never completes)"
                    ),
                ));
            }
            let family = groups.family(comm.group);
            if family.is_empty() {
                return Err(AicbError::at(
                    record.line,
                    format!(
                        "{column:?} {:?} on {:?}, which has no group of two or more ranks",
                        comm.algorithm, comm.group
                    ),
                ));
            }
            let shape_slot = comm.group as usize;
            let (servers, per_server) = match shapes[shape_slot] {
                Some(shape) => shape,
                None => *shapes[shape_slot].insert(family_shape(groups, comm.group)?),
            };
            let n = family.group_size();
            let n64 = u64::from(n);
            if nvls
                && comm.algorithm == Algorithm::AllReduce
                && comm.group == GroupKind::Tp
                && n >= 8
            {
                return Err(AicbError::at(
                    record.line,
                    "a TP all-reduce of 8 or more ranks on H100/H800 with AS_NVLS_ENABLE = 1 runs \
                     SimAI's NVLS algorithm, which Days does not model",
                ));
            }
            let (channels, steps, mut message_bytes) = match comm.algorithm {
                Algorithm::AllToAll => (1, 1, size / n64),
                Algorithm::AllReduce => {
                    (per_server, 2 * (n - 1), size / n64 / u64::from(per_server))
                }
                Algorithm::AllGather | Algorithm::ReduceScatter => {
                    (per_server, n - 1, size / n64 / u64::from(per_server))
                }
            };
            if message_bytes == 0 {
                match comm.algorithm {
                    Algorithm::AllToAll if simai => {
                        message_bytes = 1;
                        counters.one_byte_all_to_all += 1;
                    }
                    Algorithm::AllToAll => {
                        counters.elided_zero_all_to_all += 1;
                        continue;
                    }
                    // SimAI builds no flow for the ring but still creates and counts its stream,
                    // which never reaches exit(): the collective never finishes (part-1 review
                    // F1; NcclTreeFlowModel.cc:233-265, :439, :608-632).
                    _ if simai => {
                        return Err(AicbError::at(
                            record.line,
                            format!(
                                "{column:?} {:?} of {size} B on {:?} floors to 0 B per message                                  (floor(floor({size} / {n}) / {channels}) with n = {n} ranks,                                  c = {channels} channels): SimAI creates the collective's stream                                  but sends nothing, so it never finishes",
                                comm.algorithm, comm.group
                            ),
                        ));
                    }
                    _ => {
                        counters.elided_ring_floor += 1;
                        continue;
                    }
                }
            }
            if in_hang_window(message_bytes, options.mtu_bytes) {
                if simai {
                    let last = (message_bytes - 1) % options.mtu_bytes + 1;
                    return Err(AicbError::at(
                        record.line,
                        format!(
                            "{column:?} {:?} of {size} B on {:?}: every message is {message_bytes} B, \
                             whose last packet ({last} B payload) SimAI never marks sent, so the \
                             collective never finishes",
                            comm.algorithm, comm.group
                        ),
                    ));
                }
                counters.hang_window_recorded += 1;
            }
            op_of[index][slot] = Some(ops.len());
            ops.push(CollectiveOp {
                record: index,
                column,
                algorithm: comm.algorithm,
                group: comm.group,
                total_bytes: size,
                message_bytes,
                group_size: n,
                servers,
                channels,
                steps,
                matrix: None,
            });
        }
        if simai && record.process_time_ns > SIMAI_TICK_MAX {
            return Err(AicbError::at(
                record.line,
                format!(
                    "process_time of {} ns is above {SIMAI_TICK_MAX} (SimAI narrows ticks to int)",
                    record.process_time_ns
                ),
            ));
        }
    }
    if let Some(params) = imbalance {
        assign_matrices(trace, &mut ops, &op_of, params)?;
    }
    let blocks = if groups.stages > 1 {
        let blocks = block_structure(trace)?;
        if trace.header.pp_comm_bytes > 0
            && in_hang_window(trace.header.pp_comm_bytes, options.mtu_bytes)
        {
            counters.hang_window_recorded += 1;
        }
        Some(blocks)
    } else {
        None
    };
    let generic = build_chain(trace, &op_of, blocks.as_ref(), None, groups.stages);
    let stages = (0..groups.stages)
        .map(|stage| {
            let items = build_chain(trace, &op_of, blocks.as_ref(), Some(stage), groups.stages);
            let (segments, unfused_stages) = segment(&items, &ops)?;
            Ok(StageChain {
                items,
                segments,
                unfused_stages,
            })
        })
        .collect::<Result<Vec<_>, AicbError>>()?;
    let data_queue = derive_data_queue(&generic, &ops, hops)?;
    let (start_order, ecmp_ordinals_exact) = start_order(&generic, &ops, &data_queue);
    Ok(Plan {
        fidelity: options.fidelity,
        ops,
        stages,
        data_queue,
        start_order,
        ecmp_ordinals_exact,
        counters,
    })
}

/// R7's matrices: the fp all-to-alls pair up in trace order as (dispatch, combine); a record's
/// ig all-to-all is its fp one's backward and uses the transposed matrix.
fn assign_matrices(
    trace: &Trace,
    ops: &mut [CollectiveOp],
    op_of: &[[Option<OpId>; 3]],
    params: ImbalanceParams,
) -> Result<(), AicbError> {
    if params.topk == 0 || params.tokens_per_rank == 0 || params.experts == 0 {
        return Err(AicbError::new(
            "experts, topk and tokens_per_rank must be positive",
        ));
    }
    if params.zipf > 1 {
        return Err(AicbError::new("zipf must be 0 or 1"));
    }
    let copies = params
        .tokens_per_rank
        .checked_mul(u64::from(params.topk))
        .ok_or_else(|| AicbError::new("tokens_per_rank x topk overflows u64"))?;
    let mut forward_seen = 0_u32;
    for (index, slots) in op_of.iter().enumerate() {
        let line = trace.records[index].line;
        let forward = slots[0].filter(|&op| ops[op].algorithm == Algorithm::AllToAll);
        let mut forward_ref = None;
        for (slot, op) in slots.iter().enumerate().take(2) {
            let Some(op) = *op else { continue };
            if ops[op].algorithm != Algorithm::AllToAll {
                continue;
            }
            let collective = ops[op];
            if !params.experts.is_multiple_of(collective.group_size) {
                return Err(AicbError::at(
                    line,
                    format!(
                        "{} experts do not divide over the {}-rank EP group",
                        params.experts, collective.group_size
                    ),
                ));
            }
            if !collective.total_bytes.is_multiple_of(copies) {
                return Err(AicbError::at(
                    line,
                    format!(
                        "all-to-all of {} B is not tokens_per_rank x topk = {copies} copies of a whole \
                         number of bytes",
                        collective.total_bytes
                    ),
                ));
            }
            let reference = if slot == 0 {
                let reference = MatrixRef {
                    matrix: forward_seen / 2,
                    transpose: forward_seen % 2 == 1,
                    bytes_per_copy: collective.total_bytes / copies,
                };
                forward_seen += 1;
                forward_ref = Some(reference);
                reference
            } else {
                let Some(fp) = forward.and(forward_ref) else {
                    return Err(AicbError::at(
                        line,
                        "an ig all-to-all on a record whose fp column is not an all-to-all has no \
                         forward routing to reverse",
                    ));
                };
                MatrixRef {
                    matrix: fp.matrix,
                    transpose: !fp.transpose,
                    bytes_per_copy: collective.total_bytes / copies,
                }
            };
            ops[op].matrix = Some(reference);
        }
        if slots[2].is_some_and(|op| ops[op].algorithm == Algorithm::AllToAll) {
            return Err(AicbError::at(
                line,
                "a weight-gradient all-to-all has no routing model in the imbalanced arm",
            ));
        }
    }
    if !forward_seen.is_multiple_of(2) {
        return Err(AicbError::new(format!(
            "{forward_seen} forward all-to-alls do not pair up as (dispatch, combine)"
        )));
    }
    Ok(())
}

/// The microbatch blocks: a prologue of gradient-norm records, `ga` identical blocks, then the
/// loss and optimizer tail (design note §3.9). Returns `(first record, length)` per block.
fn block_structure(trace: &Trace) -> Result<Vec<(usize, usize)>, AicbError> {
    const PROLOGUE: [&str; 8] = [
        "grad_norm",
        "moe_grad_norm1",
        "moe_grad_norm2",
        "grad_gather",
        "grad_param_comm",
        "grad_param_compute",
        "layernorm",
        "embedding_grads",
    ];
    let records = &trace.records;
    let prologue = records
        .iter()
        .take_while(|record| PROLOGUE.contains(&record.name.as_str()))
        .count();
    let is_tail = |name: &str| {
        ["cross_entropy", "optimizer"].iter().any(|prefix| {
            name.strip_prefix(prefix)
                .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()))
        })
    };
    let tail = records
        .iter()
        .rev()
        .take_while(|record| is_tail(&record.name))
        .count();
    let body = records.len().saturating_sub(prologue + tail);
    let ga = trace.header.ga as usize;
    if body == 0 || body % ga != 0 {
        return Err(AicbError::new(format!(
            "the {body} records between the prologue and the tail are not ga = {ga} equal blocks"
        )));
    }
    let length = body / ga;
    for offset in length..body {
        let (a, b) = (
            &records[prologue + offset],
            &records[prologue + offset % length],
        );
        if (
            &a.name,
            a.forward,
            a.input_gradient,
            a.weight_gradient,
            a.process_time_ns,
        ) != (
            &b.name,
            b.forward,
            b.input_gradient,
            b.weight_gradient,
            b.process_time_ns,
        ) {
            return Err(AicbError::at(
                a.line,
                format!("microbatch block {} differs from block 0", offset / length),
            ));
        }
    }
    Ok((0..ga)
        .map(|block| (prologue + block * length, length))
        .collect())
}

/// The chain of one stage (`stage = Some`), or the stage-independent chain with every pipeline
/// transfer at its point (`stage = None`, for the start order and the data-queue test).
fn build_chain(
    trace: &Trace,
    op_of: &[[Option<OpId>; 3]],
    blocks: Option<&Vec<(usize, usize)>>,
    stage: Option<u32>,
    stages: u32,
) -> Vec<ChainItem> {
    let records = &trace.records;
    let mut items = Vec::with_capacity(records.len() * 8);
    let push_delay = |items: &mut Vec<ChainItem>, ns: u64| {
        if ns > 0 {
            items.push(ChainItem::Delay(ns));
        }
    };
    // Pipeline points: (is the record the first or last of block m).
    let block_of = |index: usize| {
        blocks.and_then(|blocks| {
            blocks
                .iter()
                .position(|&(first, length)| first <= index && index < first + length)
                .map(|block| (block as u32, blocks[block]))
        })
    };
    let transfers = |backward: bool, block: u32, receive: bool| -> Vec<ChainItem> {
        // Forward: receive from s-1 (boundary s-1), send to s+1 (boundary s).
        // Backward: receive from s+1 (boundary s), send to s-1 (boundary s-1).
        let mut out = Vec::new();
        match stage {
            Some(s) => {
                let boundary = match (backward, receive) {
                    (false, true) | (true, false) => s.checked_sub(1),
                    (false, false) | (true, true) => (s + 1 < stages).then_some(s),
                };
                if let Some(boundary) = boundary {
                    let transfer = PipelineTransfer {
                        boundary,
                        block,
                        backward,
                    };
                    out.push(if receive {
                        ChainItem::Receive(transfer)
                    } else {
                        ChainItem::Send(transfer)
                    });
                }
            }
            None if !receive => {
                for boundary in 0..stages - 1 {
                    out.push(ChainItem::Send(PipelineTransfer {
                        boundary,
                        block,
                        backward,
                    }));
                }
            }
            None => {}
        }
        out
    };
    for (index, record) in records.iter().enumerate() {
        // (block, is its first record, is its last record)
        let block = block_of(index)
            .map(|(m, (first, length))| (m, index == first, index == first + length - 1));
        if let Some((m, true, _)) = block {
            items.extend(transfers(false, m, true));
        }
        push_delay(&mut items, record.forward.compute_ns);
        if let Some(op) = op_of[index][0] {
            items.push(ChainItem::Collective(op));
            push_delay(&mut items, record.process_time_ns);
        }
        push_delay(&mut items, 1);
        if let Some((m, _, true)) = block {
            items.extend(transfers(false, m, false));
        }
    }
    for (index, record) in records.iter().enumerate().rev() {
        let block = block_of(index)
            .map(|(m, (first, length))| (m, index == first, index == first + length - 1));
        if let Some((m, _, true)) = block {
            items.extend(transfers(true, m, true));
        }
        push_delay(&mut items, record.input_gradient.compute_ns);
        if let Some(op) = op_of[index][1] {
            items.push(ChainItem::Collective(op));
            push_delay(&mut items, record.process_time_ns);
        }
        push_delay(&mut items, 1);
        push_delay(&mut items, record.weight_gradient.compute_ns);
        if let Some(op) = op_of[index][2] {
            items.push(ChainItem::Fork(op));
        }
        push_delay(&mut items, 1);
        if let Some((m, true, _)) = block {
            items.extend(transfers(true, m, false));
        }
    }
    items
}

/// Fuses a chain into segments (A1) and counts the unfused stages.
fn segment(items: &[ChainItem], ops: &[CollectiveOp]) -> Result<(Vec<Segment>, usize), AicbError> {
    let mut segments = Vec::new();
    let mut delay = 0_u64;
    let mut singles = Vec::new();
    let mut unfused = 0;
    let mut run = 0_u64;
    for item in items {
        let end = match *item {
            ChainItem::Delay(ns) => {
                delay = delay
                    .checked_add(ns)
                    .ok_or_else(|| AicbError::new("a fused delay overflows u64"))?;
                run += ns;
                continue;
            }
            ChainItem::Collective(op) if ops[op].single_server() => {
                singles.push(op);
                if run > 0 {
                    unfused += 1;
                }
                unfused += 1;
                run = 0;
                continue;
            }
            ChainItem::Collective(op) => SegmentEnd::Collective(op),
            ChainItem::Fork(op) => SegmentEnd::Fork(op),
            ChainItem::Receive(transfer) => SegmentEnd::Receive(transfer),
            ChainItem::Send(transfer) => SegmentEnd::Send(transfer),
        };
        if run > 0 {
            unfused += 1;
        }
        run = 0;
        if delay > 0 || !singles.is_empty() {
            segments.push(Segment {
                delay_ns: delay,
                single_server_ops: std::mem::take(&mut singles),
                end,
            });
        }
        delay = 0;
    }
    Ok((segments, unfused))
}

/// R9: the data queue's start order, from the generic chain (design note §3.4).
fn derive_data_queue(
    chain: &[ChainItem],
    ops: &[CollectiveOp],
    hops: &dyn HopBounds,
) -> Result<DataQueue, AicbError> {
    let mut issued = Vec::new();
    let mut offsets = Vec::new();
    let mut offset = Some(0_u64);
    let mut started = false;
    for item in chain {
        match *item {
            ChainItem::Fork(op) => {
                issued.push(op);
                offsets.push(offset);
                started = true;
            }
            ChainItem::Delay(ns) if started => {
                offset = offset.and_then(|value| value.checked_add(ns));
            }
            ChainItem::Collective(op) if started && ops[op].single_server() => {
                // Its delay has no static upper bound here (it is H1's formula); treat it as
                // unknown, which only makes the test fail safe.
                offset = None;
            }
            ChainItem::Collective(_) | ChainItem::Receive(_) | ChainItem::Send(_) if started => {
                offset = None;
            }
            _ => {}
        }
    }
    let head_lower_bound_ns = issued
        .first()
        .map_or(0, |&op| lower_bound_ns(&ops[op], hops));
    let waiting = offsets
        .iter()
        .skip(1)
        .all(|offset| offset.is_some_and(|value| value < head_lower_bound_ns));
    let (kind, order) = if issued.len() <= 2 {
        (DataQueueOrder::Fifo, issued.clone())
    } else if waiting {
        let mut order = vec![issued[0]];
        order.extend(issued[1..].iter().rev());
        (DataQueueOrder::Lifo, order)
    } else {
        (DataQueueOrder::FifoFallback, issued.clone())
    };
    // With two ops, LIFO and FIFO agree when the second waits; label the exact case LIFO too.
    Ok(DataQueue {
        issued,
        issue_offsets_ns: offsets,
        head_lower_bound_ns,
        kind,
        order,
    })
}

/// A lower bound of a collective's duration at a rank: its steps, each at least the cheapest
/// hop class the op uses.
fn lower_bound_ns(op: &CollectiveOp, hops: &dyn HopBounds) -> u64 {
    let network = op.servers > 1;
    let nvlink = op.group_size > op.servers; // more than one rank on some server
    let per_step = match (network, nvlink) {
        (true, true) => hops
            .network_hop_ns(op.message_bytes)
            .min(hops.nvlink_hop_ns(op.message_bytes, op.message_bytes)),
        (true, false) => hops.network_hop_ns(op.message_bytes),
        (false, _) => hops.nvlink_hop_ns(op.message_bytes, op.message_bytes),
    };
    u64::from(op.steps).saturating_mul(per_step)
}

/// The realized start order (A7) and whether its ordinals are exact.
fn start_order(
    chain: &[ChainItem],
    ops: &[CollectiveOp],
    queue: &DataQueue,
) -> (Vec<StartItem>, bool) {
    let mut order = Vec::new();
    let mut first_fork_seen = false;
    let mut exact = true;
    let mut next_data = queue.order.iter();
    for item in chain {
        match *item {
            ChainItem::Collective(op) if !ops[op].single_server() => {
                exact &= !first_fork_seen;
                order.push(StartItem::Collective(op));
            }
            // Single-server collectives fuse into compute segments and send no network message.
            ChainItem::Collective(_) => {}
            ChainItem::Send(transfer) => {
                exact &= !first_fork_seen;
                order.push(StartItem::Pipeline(transfer));
            }
            // Each fork takes the next data op in start order: the forks of one LIFO batch are
            // contiguous in the chain apart from delays (the R9 test), so the permutation stays
            // within the batch; under FIFO it is the identity.
            ChainItem::Fork(_) => {
                first_fork_seen = true;
                order.push(StartItem::Collective(
                    *next_data.next().expect("every fork has a data op"),
                ));
            }
            ChainItem::Delay(_) | ChainItem::Receive(_) => {}
        }
    }
    exact &= queue.kind != DataQueueOrder::FifoFallback;
    (order, exact)
}

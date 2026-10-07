//! Immutable, backend-neutral simulation image records.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;

use crate::{
    DcqcnController, DropMarkPolicy, Event, EventKind, FlowId, LinkId, NodeId, NodeKind, PayloadId,
    SchedulerKind, TcpCongestionControl, TimeError, link_arrival_time_ns,
};

/// The plan's default constant propagation delay for a directed link.
pub const fn default_propagation_ns() -> u64 {
    0
}

/// Semantic identity and role-specific state location of one logical process.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub kind: NodeKind,
    /// Index into `SimulationImage::host_states` or `switch_states`, selected by `kind`.
    pub state_slot: u32,
}

/// Host-owned semantic state.
#[derive(Clone, Eq, PartialEq)]
pub struct HostState {
    pub egress_link: LinkId,
    pub queue: VecDeque<PayloadId>,
    pub in_service: Option<PayloadId>,
    pub tx_ready_pending: bool,
    /// Source-owned flow generators in canonical `FlowId` order.
    pub generators: Vec<FlowGeneratorState>,
    /// Collective or compute stage records of the generators, by generator position.
    ///
    /// Empty on a host without a stage generator, so unused collectives cost a host nothing. On a
    /// host that carries a stage it holds exactly one entry per generator, `Some` at each stage
    /// generator; a plain flow sharing that host pays one `Option<CollectiveStage>` entry (136 B),
    /// the size P14 first gave every generator inline. `validate` accepts only these two shapes
    /// (`HostState::stages_are_canonical`), so the per-generator view that `Debug` prints
    /// determines the table.
    pub stages: Vec<Option<CollectiveStage>>,
    /// Target-owned TCP cumulative-ACK state in canonical `FlowId` order.
    pub tcp_receivers: Vec<TcpReceiverState>,
    /// Target-owned DCQCN CNP interval state in canonical `FlowId` order.
    pub dcqcn_receivers: Vec<DcqcnReceiverState>,
    /// Target-owned RoCE queue-pair receivers in canonical `FlowId` order, or `None` on a host
    /// that receives no queue pair.
    ///
    /// Receivers are fixed at lowering, so a boxed slice holds them: a host without one pays the
    /// 16-B pointer and no allocation, and a host with any pays one allocation. `validate` refuses
    /// `Some` of an empty slice, so `None` is the only empty shape.
    pub roce_receivers: Option<Box<[RoceReceiverState]>>,
    /// Egress pause state of a host whose egress link is PFC-controlled (`[link.pfc] host_links`),
    /// or `None`: a host without one pays the 8-B pointer and no allocation.
    pub pfc: Option<Box<HostPfcState>>,
    pub next_origin_seq: u64,
    /// Per-node packet identity cursor. Every generated packet consumes one sequence value.
    pub next_payload_seq: u64,
    pub sourced_packets: u64,
    pub departed_packets: u64,
    pub received_packets: u64,
}

impl HostState {
    /// The stage record of the generator at `position`, or `None` for an ungated generator.
    pub fn stage(&self, position: usize) -> Option<CollectiveStage> {
        self.stages.get(position).copied().flatten()
    }

    /// Dependency state of the stage at `position`, or `None` for an ungated generator.
    pub fn stage_dependencies(&self, position: usize) -> Option<StageDependencies> {
        self.stage(position).map(|stage| stage.dependencies)
    }

    /// Writes dependency state back into the stage record at `position`.
    ///
    /// # Panics
    ///
    /// Panics when the generator at `position` is not a stage; callers obtain `dependencies` from
    /// [`Self::stage_dependencies`].
    pub fn set_stage_dependencies(&mut self, position: usize, dependencies: StageDependencies) {
        self.stages
            .get_mut(position)
            .and_then(Option::as_mut)
            .expect("set_stage_dependencies requires a stage record")
            .dependencies = dependencies;
    }

    /// Each generator with its stage record, in table order.
    pub fn generators_with_stages(
        &self,
    ) -> impl Iterator<Item = (&FlowGeneratorState, Option<CollectiveStage>)> {
        self.generators
            .iter()
            .enumerate()
            .map(|(position, generator)| (generator, self.stage(position)))
    }

    /// Each generator with its stage record, both writable, in table order. On a host whose stage
    /// table is empty every generator pairs with `None`.
    pub fn generators_with_stages_mut(
        &mut self,
    ) -> impl Iterator<Item = (&mut FlowGeneratorState, Option<&mut CollectiveStage>)> {
        self.generators.iter_mut().zip(
            self.stages
                .iter_mut()
                .map(Option::as_mut)
                .chain(std::iter::repeat_with(|| None)),
        )
    }

    /// Whether the stage table has one of its two accepted shapes: empty, or one entry per
    /// generator with at least one stage.
    pub fn stages_are_canonical(&self) -> bool {
        self.stages.is_empty()
            || (self.stages.len() == self.generators.len()
                && self.stages.iter().any(Option::is_some))
    }
}

/// A host's generator table as `Debug` renders it: each generator with its stage record inline.
///
/// The rendering is the one every image and result fingerprint used when the record was a
/// generator field, byte for byte; neither the stage table nor a position appears in it.
struct GeneratorsWithStages<'a>(&'a HostState);

impl fmt::Debug for GeneratorsWithStages<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.0;
        formatter
            .debug_list()
            .entries(
                state
                    .generators
                    .iter()
                    .enumerate()
                    .map(|(position, generator)| GeneratorWithStage {
                        generator,
                        stage: state.stage(position),
                    }),
            )
            .finish()
    }
}

/// One generator and its stage record, rendered as the generator alone was when it held the record.
struct GeneratorWithStage<'a> {
    generator: &'a FlowGeneratorState,
    stage: Option<CollectiveStage>,
}

impl fmt::Debug for GeneratorWithStage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let generator = self.generator;
        let mut debug = formatter.debug_struct("FlowGeneratorState");
        debug
            .field("flow", &generator.flow)
            .field("packets_emitted", &generator.packets_emitted)
            .field("bytes_emitted", &generator.bytes_emitted)
            .field("next_emission", &generator.next_emission)
            .field("rng_state", &generator.rng_state)
            .field("feedback", &generator.feedback)
            .field("kind", &generator.kind);
        // Omitting the absent additive field preserves every frozen pre-P14 image byte.
        if self.stage.is_some() {
            debug.field("stage", &self.stage);
        }
        debug.finish()
    }
}

impl fmt::Debug for HostState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("HostState");
        debug
            .field("egress_link", &self.egress_link)
            .field("queue", &self.queue)
            .field("in_service", &self.in_service)
            .field("tx_ready_pending", &self.tx_ready_pending)
            // Each generator with its stage record inline; the stage table is not printed.
            .field("generators", &GeneratorsWithStages(self))
            .field("tcp_receivers", &self.tcp_receivers);
        // Omitting the empty additive field preserves every frozen pre-T26 image byte.
        if !self.dcqcn_receivers.is_empty() {
            debug.field("dcqcn_receivers", &self.dcqcn_receivers);
        }
        // Omitting the absent additive field preserves every frozen pre-P15 image byte.
        if let Some(receivers) = &self.roce_receivers {
            debug.field("roce_receivers", receivers);
        }
        // Omitting the absent host-link PFC state preserves every image byte without it.
        if let Some(pfc) = &self.pfc {
            debug.field("pfc", pfc);
        }
        debug
            .field("next_origin_seq", &self.next_origin_seq)
            .field("next_payload_seq", &self.next_payload_seq)
            .field("sourced_packets", &self.sourced_packets)
            .field("departed_packets", &self.departed_packets)
            .field("received_packets", &self.received_packets)
            .finish()
    }
}

/// One switch-port-owned TailDrop egress queue with discipline-owned service state.
#[derive(Clone, Eq, PartialEq)]
pub struct SwitchQueueState {
    /// `None` is used only by terminal hand-built fixtures whose flow ends at the switch.
    pub egress_link: Option<LinkId>,
    pub scheduler: SchedulerKind,
    /// Maximum queued packets. A value of zero denotes an unbounded queue.
    pub queue_capacity_packets: u64,
    /// Deterministic admission/marking policy. TailDrop preserves the original capacity field.
    pub drop_mark: DropMarkPolicy,
    /// Optional IEEE 802.1Qbb-style per-priority pause state and ingress monitor.
    pub pfc: Option<PfcQueueState>,
    pub queue: VecDeque<PayloadId>,
    /// Packet committed to the non-preemptive transmission currently in progress.
    pub in_service: Option<PayloadId>,
    /// Whether this queue already owns a future `TxReady` decision point.
    pub tx_ready_pending: bool,
}

impl fmt::Debug for SwitchQueueState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("SwitchQueueState");
        debug
            .field("egress_link", &self.egress_link)
            .field("scheduler", &self.scheduler)
            .field("queue_capacity_packets", &self.queue_capacity_packets);
        if self.drop_mark != DropMarkPolicy::TailDrop {
            debug.field("drop_mark", &self.drop_mark);
        }
        if self.pfc.is_some() {
            debug.field("pfc", &self.pfc);
        }
        debug
            .field("queue", &self.queue)
            .field("in_service", &self.in_service)
            .field("tx_ready_pending", &self.tx_ready_pending)
            .finish()
    }
}

/// Egress pause state of one host whose egress link is PFC-controlled (P15 host-link PFC).
///
/// The switch egress LPs that monitor the host's link assert pause per priority, as they do for a
/// switch queue (`PfcQueueState::paused_by_controller`). A queue pair whose data class is paused
/// parks its pacer at its next tick (ruling H1 (b)); `pause_parked` holds those queue pairs so
/// that the RESUME that ends the pause restarts exactly them, without a scan of the host's
/// generators.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HostPfcState {
    /// Downstream controller LPs currently asserting pause for each priority. A priority remains
    /// paused until every asserting controller has resumed.
    pub paused_by_controller: [BTreeSet<NodeId>; 8],
    /// Generator positions (canonical `FlowId` order) of the queue pairs a paused tick parked, by
    /// data class.
    pub pause_parked: [BTreeSet<usize>; 8],
}

impl HostPfcState {
    pub fn is_paused(&self, priority: usize) -> bool {
        !self.paused_by_controller[priority].is_empty()
    }
}

/// Controller-scoped per-priority pause state owned by one switch egress queue.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PfcQueueState {
    /// Downstream controller LPs currently asserting pause for each priority. A priority remains
    /// paused until every asserting controller has resumed.
    pub paused_by_controller: [BTreeSet<NodeId>; 8],
    /// Downstream buffer monitors, one for each PFC-controlled incoming link feeding this queue.
    pub ingresses: Vec<PfcIngressState>,
}

impl PfcQueueState {
    pub fn is_paused(&self, priority: usize) -> bool {
        !self.paused_by_controller[priority].is_empty()
    }
}

/// Exact byte-accounting state for one PFC-controlled incoming link.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PfcIngressState {
    pub controlled_link: LinkId,
    /// Index of the reverse control lane in `SimulationImage::channels`.
    pub control_channel_index: u32,
    pub buffer_capacity_bytes: [u64; 8],
    /// Maximum reachable frame size for each enabled priority on the controlled link. Disabled or
    /// unreachable priorities use zero.
    pub max_frame_bytes: [u64; 8],
    /// A zero XOFF threshold disables PFC for that priority.
    pub xoff_threshold_bytes: [u64; 8],
    pub xon_threshold_bytes: [u64; 8],
    pub occupancy_bytes: [u64; 8],
    pub pause_asserted: [bool; 8],
}

/// Switch-port-owned state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwitchState {
    /// Stable physical topology identity shared by every egress LP of the same switch.
    pub physical_switch: u64,
    /// Lowered images contain exactly one queue. Hand-built images with zero queues remain useful
    /// for terminal validation fixtures.
    pub queues: Vec<SwitchQueueState>,
    /// Deterministic child-emission sequence owned only by this LP.
    pub next_origin_seq: u64,
    pub arrived_packets: u64,
    pub dropped_packets: u64,
    pub departed_packets: u64,
}

/// Stable endpoints and canonical directed route for one open-loop flow.
#[derive(Clone, Eq, PartialEq)]
pub struct FlowDescriptor {
    pub id: FlowId,
    pub source: NodeId,
    pub target: NodeId,
    /// IEEE 802.1Q priority code point used by link-level eligibility mechanisms.
    pub priority: u8,
    /// Priority code point of the flow's receiver feedback: DCQCN CNPs and RoCE ACKs and NACKs.
    ///
    /// Equal to `priority` unless a DCQCN or RoCE flow names another class; every other packet of
    /// the flow, TCP ACKs included, uses `priority` (see [`Self::packet_priority`]).
    pub feedback_priority: u8,
    /// Canonical data-packet route from source to target.
    pub route: Vec<LinkId>,
    /// Canonical feedback-packet route from target back to source.
    pub reverse_route: Vec<LinkId>,
}

impl fmt::Debug for FlowDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("FlowDescriptor");
        debug
            .field("id", &self.id)
            .field("source", &self.source)
            .field("target", &self.target);
        if self.priority != 0 {
            debug.field("priority", &self.priority);
        }
        // Omitting the default preserves every frozen pre-P15 image byte.
        if self.feedback_priority != self.priority {
            debug.field("feedback_priority", &self.feedback_priority);
        }
        debug
            .field("route", &self.route)
            .field("reverse_route", &self.reverse_route)
            .finish()
    }
}

impl FlowDescriptor {
    /// The PFC priority class of a packet of this flow.
    ///
    /// Receiver feedback (a DCQCN CNP, a RoCE ACK or NACK) rides `feedback_priority`; every other
    /// packet rides `priority`. Derived from the flow and the packet kind, so no packet carries a
    /// priority field. Called on every PFC-monitored switch arrival and service decision, so it
    /// must inline (`scalar::tests` pins the attribute).
    #[inline(always)]
    pub const fn packet_priority(&self, kind: PacketKind) -> u8 {
        match kind {
            PacketKind::DcqcnCnp(_) | PacketKind::RoceAck(_) | PacketKind::RoceNack(_) => {
                self.feedback_priority
            }
            _ => self.priority,
        }
    }
}

/// Whether a generator owns a scheduled emission or is waiting without local work.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorStatus {
    Scheduled = 0,
    Blocked = 1,
    Finished = 2,
    /// Emission remains under the traffic termination but lies beyond the scenario stop.
    Stopped = 3,
}

/// The next packet already produced by a generator for a future emission transition.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledEmission {
    pub status: GeneratorStatus,
    /// Meaningful only when `status == GeneratorStatus::Scheduled`.
    pub departure_time_ns: u64,
    /// Meaningful only when `status == GeneratorStatus::Scheduled`.
    pub payload: PayloadId,
}

/// Feedback-owned transport state reserved by every generator kind.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GeneratorFeedbackState {
    pub arrivals: u64,
    pub outstanding_bytes: u64,
    pub unacknowledged_bytes: u64,
}

/// Closed termination modes supported by the constant generator.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorTermination {
    Bytes(u64),
    DurationNs(u64),
}

/// Fixed-width parameters for the v1 constant generator.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConstantGenerator {
    pub first_departure_ns: u64,
    pub interval_ns: u64,
    pub packet_size_bytes: u64,
    pub termination: GeneratorTermination,
}

/// Collective algorithm tag retained as image data. Runtime execution is driven by resolved stage
/// dependencies rather than algorithm-specific branches.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CollectiveAlgorithm {
    RingAllReduce = 0,
    AllGather = 1,
    /// One ring phase: rank r ends holding the reduced chunk it owns.
    ReduceScatter = 2,
    /// One message per ordered pair of ranks, all independent.
    AllToAll = 3,
    /// One message from the group's rank 0 to its rank 1.
    SendRecv = 4,
}

/// Resolved phase tag for one collective stage.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CollectivePhase {
    ReduceScatter = 0,
    AllGather = 1,
    /// An all-to-all's one phase: `step` is the offset `k` of the destination `rank + k`.
    AllToAll = 2,
    /// A send/recv's one phase.
    SendRecv = 3,
}

/// Chunk partition policy retained explicitly in every stage image.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CollectiveChunkPolicy {
    EqualRemainderLast = 0,
    /// Every message carries `floor(floor(S / n) / c)` bytes (`c` channels), the remainder
    /// unsent: SimAI's NCCL flow model. An all-to-all's pairs carry `floor(S / n)`.
    UniformFloor = 1,
    /// An all-to-all's per-pair bytes from a seeded routing matrix (`scenario::alltoall`).
    Seeded = 2,
}

/// Logical communication-channel policy retained explicitly in every stage image.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CollectiveChannelPolicy {
    RingNext = 0,
    /// Several rings over the same ranks, each its own order; `channel` names the ring.
    Channels = 1,
    /// Every ordered pair of ranks (an all-to-all).
    AllPairs = 2,
    /// The one pair rank 0 to rank 1 (a send/recv).
    Pair = 3,
}

/// The predecessors of one kind (local or inbound) of a dependency-gated stage.
///
/// Most stages wait for at most one stage of each kind, held inline. A join (the stage after an
/// all-to-all or a multi-channel ring, or after several stage groups) waits for several; their
/// flows are the run `[first, first + count)` of [`SimulationImage::stage_joins`], ascending and
/// distinct. The predecessor graph is fixed at lowering, so the run is immutable image data that
/// no host state carries.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StagePredecessors {
    #[default]
    None,
    One(FlowId),
    Join {
        first: u32,
        count: u32,
    },
}

impl StagePredecessors {
    /// How many predecessors of this kind the stage waits for.
    pub const fn count(self) -> u32 {
        match self {
            Self::None => 0,
            Self::One(_) => 1,
            Self::Join { count, .. } => count,
        }
    }

    /// The single predecessor held inline; `None` for no predecessor and for a join.
    pub const fn one(self) -> Option<FlowId> {
        match self {
            Self::One(flow) => Some(flow),
            _ => None,
        }
    }

    /// A join's run of [`SimulationImage::stage_joins`] (`joins`); empty for `None` and `One`.
    ///
    /// A run outside `joins` yields nothing; validation rejects it before any run.
    pub fn join_run(self, joins: &[FlowId]) -> &[FlowId] {
        match self {
            Self::None => &[],
            Self::One(_) => &[],
            Self::Join { first, count } => usize::try_from(first)
                .ok()
                .and_then(|first| {
                    joins.get(first..first.checked_add(usize::try_from(count).ok()?)?)
                })
                .unwrap_or(&[]),
        }
    }

    /// Each predecessor flow, in ascending order.
    pub fn iter(self, joins: &[FlowId]) -> impl Iterator<Item = FlowId> + '_ {
        let one = match self {
            Self::One(flow) => Some(flow),
            _ => None,
        };
        one.into_iter().chain(self.join_run(joins).iter().copied())
    }
}

/// Prerequisite state of one dependency-gated stage, owned by the stage's source host LP.
///
/// Local predecessors run on the same host; the stage counts those that completed. Inbound
/// predecessors are stages on other hosts whose flows target this host; their completion is
/// observed through ordinary delivery at this host, so no cross-LP state is shared, and the stage
/// counts the bytes they delivered in order. A stage is released once every local predecessor
/// completed and every inbound byte arrived.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StageDependencies {
    pub local: StagePredecessors,
    pub inbound: StagePredecessors,
    /// Bytes the inbound predecessors must deliver to this host: their totals, summed.
    pub inbound_predecessor_bytes: u64,
    /// Inbound bytes delivered so far: the sum of the receivers' in-order frontiers of the
    /// inbound predecessors (TCP's next expected sequence, a RoCE queue pair's expected PSN).
    pub inbound_bytes_received: u64,
    /// Local predecessors complete so far.
    pub local_completed: u32,
}

impl StageDependencies {
    /// Whether every local predecessor completed.
    pub const fn local_complete(self) -> bool {
        self.local_completed == self.local.count()
    }

    /// Whether every inbound predecessor delivered its bytes.
    pub const fn inbound_complete(self) -> bool {
        self.inbound_bytes_received == self.inbound_predecessor_bytes
    }

    pub const fn prerequisites_complete(self) -> bool {
        self.local_complete() && self.inbound_complete()
    }
}

/// Resolved position and chunk of one collective stage carried by a transport generator.
///
/// The transport owns the sending discipline; the stage fixes which chunk
/// `[chunk_offset_bytes, chunk_offset_bytes + chunk_bytes)` of the `declared_total_bytes` buffer
/// rank `rank` sends to its ring successor at one-based `step` of `phase`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectiveStageIdentity {
    pub collective_id: u64,
    pub algorithm: CollectiveAlgorithm,
    /// The channel (ring) this stage runs on; 0 for a single-ring collective.
    pub channel: u32,
    pub group_size: u32,
    pub declared_total_bytes: u64,
    pub rank: u32,
    pub phase: CollectivePhase,
    /// One-based phase step.
    pub step: u32,
    pub chunk_policy: CollectiveChunkPolicy,
    pub channel_policy: CollectiveChannelPolicy,
    pub chunk_offset_bytes: u64,
    pub chunk_bytes: u64,
}

/// A delay-only stage: a compute interval of `duration_ns` that sends no bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComputeStage {
    /// Scenario-local identity of the compute group this stage belongs to.
    pub compute_id: u64,
    pub group_size: u32,
    pub rank: u32,
    pub duration_ns: u64,
}

/// What a dependency-gated generator does once its prerequisites complete.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageRole {
    /// One collective stage whose bytes are carried by the generator's transport.
    Collective(CollectiveStageIdentity),
    /// A timer-only compute interval.
    Compute(ComputeStage),
}

/// Dependency record attached to an ordinary transport or timer generator.
///
/// The lowerer resolves algorithms, chunks, channels, and dependencies into these records. A
/// source LP needs no shared mutable collective object: ordinary delivery at this host satisfies
/// the inbound prerequisite of the next local stage.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectiveStage {
    pub role: StageRole,
    pub dependencies: StageDependencies,
    /// Whether the prerequisites have released the stage. Transport generators revisit `Blocked`
    /// while waiting for feedback, so status alone cannot distinguish a stage that never started.
    pub activated: bool,
}

/// Closed generator transition set. New traffic families require an explicit image variant.
#[repr(C, u8)]
// Image state is deliberately pointer-free and Copy across every backend boundary.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowGeneratorKind {
    Constant(ConstantGenerator),
    Tcp(TcpGenerator),
    Rate(RateGenerator),
    Dcqcn(DcqcnGenerator),
    Roce(RoceGenerator),
}

/// Exact rational rate source paced by the M3 fallback-heap timer.
///
/// Credit uses `rate_denominator * 1_000_000_000` quanta per bit. Each pacing tick adds
/// `rate_numerator_bits_per_second * pacing_interval_ns` quanta and one packet is emitted when the
/// accumulated credit covers `packet_size_bytes * 8` bits.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateGenerator {
    pub first_pacing_time_ns: u64,
    pub pacing_interval_ns: u64,
    pub packet_size_bytes: u64,
    pub total_bytes: u64,
    pub rate_numerator_bits_per_second: u64,
    pub rate_denominator: u64,
    pub credit_quanta: u128,
}

/// Exact Mellanox-form DCQCN reaction point over the T25 rate generator: an unreliable DCQCN
/// flow, whose receiver answers CE-marked data with CNP packets. The controller has no timer
/// event (P16 ruling D2); its one live event is the pacing tick.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnGenerator {
    pub rate: RateGenerator,
    pub controller: DcqcnController,
    pub cnp_size_bytes: u64,
}

/// Exact credit pacer of a RoCE queue pair, on the arithmetic of the DCQCN pacer.
///
/// One tick every `pacing_interval_ns` on the grid anchored at `first_pacing_time_ns` adds
/// `controller.current_rate_bps * pacing_interval_ns` quanta; a packet of `size` bytes costs
/// `size * 8 * 10^9` quanta (the DCQCN pacer's `rate_denominator` is always one). The DCQCN
/// pacer's write-only rate mirror and its constant denominator are not stored.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RocePacer {
    pub first_pacing_time_ns: u64,
    pub pacing_interval_ns: u64,
    /// The packet size of every packet but a short last one.
    pub mtu_bytes: u64,
    pub total_bytes: u64,
    pub credit_quanta: u128,
}

/// A RoCE queue pair: a reliable DCQCN flow with Go-back-N.
///
/// The exact Mellanox-form DCQCN reaction point (`controller`, with lazy timers and no timer event)
/// paces a Go-back-N sender. A PSN is the byte offset of a packet's first byte, and packet `psn`
/// is `min(mtu_bytes, total_bytes - psn)` bytes, so a retransmission is a pure function of its
/// PSN and no segment ledger exists. The high-water mark is `FlowGeneratorState::bytes_emitted`,
/// which counts first transmissions only, as TCP's does.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceGenerator {
    pub pacer: RocePacer,
    pub controller: DcqcnController,
    /// Stable zero-byte token of the pacing tick and the retransmission timeout
    /// (`PacketKind::RocePacingTimer`).
    pub pacing_timer_payload: PayloadId,
    /// PSN of the next packet the pacer sends.
    pub next_psn: u64,
    /// Cumulative acknowledgment: every byte below it is acknowledged.
    pub snd_una: u64,
    /// Deadline of the armed retransmission timeout; meaningful only while `rto_ns != 0` and
    /// `snd_una < bytes_emitted`, and zero otherwise.
    pub rto_deadline_ns: u64,
    /// Fixed retransmission timeout (no backoff); zero turns the timeout off, so only a NACK
    /// recovers a loss and a lost last packet stalls the queue pair for the rest of the run.
    pub rto_ns: u64,
    /// P16 ruling D7: the window in bytes (SimAI `m_win`); zero turns it off.
    pub window_bytes: u64,
    /// Whether exactly one pacing tick is pending. A parked pacer has none.
    pub pacer_armed: bool,
    /// P16 ruling D7: the window scales with the controller's rate (SimAI `m_var_win`).
    pub variable_window: bool,
    /// The pacer is parked because a tick found the window closed, and no restart has run since.
    /// Only an ACK or NACK that moves `snd_una`, or a timeout, restarts such a pacer; a host
    /// RESUME does not (it restarts pause-parked pacers), so the bit keeps the two apart.
    pub window_parked: bool,
}

/// Fixed-width result of routing an ordinary feedback packet into a source generator.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeneratorFeedbackAction {
    None,
    Emit { flow: FlowId, size_bytes: u64 },
}

/// Mutable generator state owned exclusively by the source host LP.
#[repr(C)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct FlowGeneratorState {
    pub flow: FlowId,
    pub packets_emitted: u64,
    pub bytes_emitted: u64,
    pub next_emission: ScheduledEmission,
    /// Per-flow deterministic stream state derived from the image seed and semantic flow key.
    pub rng_state: u64,
    pub feedback: GeneratorFeedbackState,
    pub kind: FlowGeneratorKind,
}

/// A generator alone, without its host's stage table, renders with no stage record.
impl fmt::Debug for FlowGeneratorState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        GeneratorWithStage {
            generator: self,
            stage: None,
        }
        .fmt(formatter)
    }
}

/// One active source-owned retransmission timer.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpTimerState {
    pub attempt: PayloadId,
    pub sequence: u64,
    pub deadline_ns: u64,
    pub generation: u64,
    pub rto_ns: u64,
}

/// Fixed-width source-owned TCP transport and congestion state.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpGenerator {
    pub total_bytes: u64,
    pub mss_bytes: u64,
    pub ack_size_bytes: u64,
    pub next_sequence: u64,
    pub highest_ack: u64,
    pub bytes_in_flight: u64,
    pub duplicate_acks: u64,
    pub recovery_high_sequence: u64,
    pub last_attempt: PayloadId,
    pub timer_generation: u64,
    pub active_timer: Option<TcpTimerState>,
    pub srtt_ns: u64,
    pub rtt_var_ns: u64,
    pub rto_ns: u64,
    pub control: TcpCongestionControl,
}

impl TcpGenerator {
    pub const INITIAL_RTO_NS: u64 = 1_000_000_000;

    pub const fn new(
        total_bytes: u64,
        mss_bytes: u64,
        ack_size_bytes: u64,
        control: TcpCongestionControl,
    ) -> Self {
        Self {
            total_bytes,
            mss_bytes,
            ack_size_bytes,
            next_sequence: 0,
            highest_ack: 0,
            bytes_in_flight: 0,
            duplicate_acks: 0,
            recovery_high_sequence: 0,
            last_attempt: PayloadId(0),
            timer_generation: 0,
            active_timer: None,
            srtt_ns: 0,
            rtt_var_ns: 0,
            rto_ns: Self::INITIAL_RTO_NS,
            control,
        }
    }
}

/// One target-side received byte range, represented half-open as `[start, end)`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpReceiveRange {
    pub start: u64,
    pub end: u64,
}

/// Target-owned cumulative ACK state for one TCP flow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpReceiverState {
    pub flow: FlowId,
    pub ack_size_bytes: u64,
    pub next_expected_sequence: u64,
    pub out_of_order: Vec<TcpReceiveRange>,
}

impl TcpReceiverState {
    pub const fn new(flow: FlowId, ack_size_bytes: u64) -> Self {
        Self {
            flow,
            ack_size_bytes,
            next_expected_sequence: 0,
            out_of_order: Vec::new(),
        }
    }
}

/// Target-owned DCQCN notification-point state.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnReceiverState {
    pub flow: FlowId,
    pub cnp_interval_ns: u64,
    pub cnp_size_bytes: u64,
    pub last_cnp_time_ns: Option<u64>,
}

/// The last NACK a RoCE receiver sent: the expected PSN it carried and when.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceNackMark {
    pub expected_psn: u64,
    pub time_ns: u64,
}

/// Target-owned state of one RoCE queue pair: the Go-back-N receiver. Its ACKs and NACKs echo the
/// ECN mark of the data packet that triggered them, so it holds no notification point (P16 ruling
/// D4).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceReceiverState {
    pub flow: FlowId,
    pub total_bytes: u64,
    /// The in-order frontier: every byte below it has arrived in order.
    pub expected_psn: u64,
    /// In-order packets per cumulative ACK; at least one.
    pub ack_every_packets: u64,
    /// In-order packets since the last ACK or NACK.
    pub packets_since_ack: u64,
    /// Wire size of an ACK or NACK.
    pub ack_size_bytes: u64,
    /// Minimum spacing of two NACKs for the same expected PSN.
    pub nack_interval_ns: u64,
    pub last_nack: Option<RoceNackMark>,
    /// Whether a packet below the frontier elicits a cumulative ACK; `false` only with the
    /// sender's retransmission timeout off.
    pub duplicate_ack: bool,
}

/// Immutable per-packet data referenced by a persistent event payload.
///
/// Lowered images retain only the first scheduled packet for each active flow. Later records are
/// produced by the source generator and live only while the packet is scheduled or in flight.
#[repr(C)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PacketDescriptor {
    pub id: PayloadId,
    pub flow: FlowId,
    pub size_bytes: u64,
    /// Congestion-experienced bit carried unchanged across every hop.
    pub ecn_marked: bool,
    pub kind: PacketKind,
}

impl fmt::Debug for PacketDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("PacketDescriptor");
        debug
            .field("id", &self.id)
            .field("flow", &self.flow)
            .field("size_bytes", &self.size_bytes);
        if self.ecn_marked {
            debug.field("ecn_marked", &self.ecn_marked);
        }
        debug.field("kind", &self.kind).finish()
    }
}

/// PFC control payload carried by an ordinary reverse-channel `RemoteArrival`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PfcHeader {
    pub controlled_link: LinkId,
    pub priority: u8,
    pub pause: bool,
}

/// Typed CNP feedback metadata carried by an ordinary reverse-channel `RemoteArrival`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnCnpHeader {
    pub trigger_payload: PayloadId,
}

/// Exact ECN codepoint represented by packet kind plus the persistent congestion bit.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EcnCodepoint {
    NotEct = 0,
    Ect0 = 1,
    Ce = 3,
}

/// TCP data metadata independent of the transmission-attempt `PayloadId`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpDataHeader {
    pub sequence: u64,
    pub sent_time_ns: u64,
    pub retransmission: bool,
}

/// TCP cumulative-ACK metadata carried by a feedback packet.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpAckHeader {
    pub acknowledgment: u64,
    pub acknowledged_bytes: u64,
    pub echoed_sent_time_ns: u64,
}

/// RoCE queue-pair data metadata independent of the transmission-attempt `PayloadId`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceDataHeader {
    /// Byte offset of the packet's first byte.
    pub psn: u64,
    pub sent_time_ns: u64,
    pub retransmission: bool,
}

/// RoCE cumulative ACK or NACK metadata: the receiver's in-order frontier, the send time and size
/// of the data packet that triggered it, and that packet's ECN echo (P16 ruling D4: the queue
/// pair's congestion feedback, as HPCC's and SimAI's ACK `FLAG_CNP`). The size is a packet size,
/// at most the queue pair's MTU, which lowering bounds by `u32::MAX`, so the header keeps the 24 B
/// every packet descriptor reserves.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceAckHeader {
    pub acknowledgment: u64,
    pub echoed_sent_time_ns: u64,
    pub acknowledged_bytes: u32,
    pub ce_echo: bool,
}

/// Closed packet direction used to route ordinary data and feedback packets.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketKind {
    Data = 0,
    Feedback = 1,
    TcpData(TcpDataHeader) = 2,
    TcpAck(TcpAckHeader) = 3,
    Pfc(PfcHeader) = 4,
    DcqcnCnp(DcqcnCnpHeader) = 5,
    // Code 6 was the paper-form DCQCN control tick, removed in P16 (the Mellanox-form controller
    // has no timer events); the remaining codes keep their values.
    RoceData(RoceDataHeader) = 7,
    RoceAck(RoceAckHeader) = 8,
    /// A cumulative NACK: `acknowledgment` is the expected PSN the sender rewinds to.
    RoceNack(RoceAckHeader) = 9,
    /// A source-local zero-byte token of a RoCE pacer. It is never enqueued or transmitted.
    RocePacingTimer = 10,
}

impl PacketKind {
    pub const fn is_data(self) -> bool {
        matches!(self, Self::Data | Self::TcpData(_) | Self::RoceData(_))
    }

    pub const fn is_feedback(self) -> bool {
        matches!(
            self,
            Self::Feedback
                | Self::TcpAck(_)
                | Self::Pfc(_)
                | Self::DcqcnCnp(_)
                | Self::RoceAck(_)
                | Self::RoceNack(_)
        )
    }

    /// A zero-byte source-local timer token (a RoCE pacing tick): never enqueued, transmitted or
    /// delivered.
    pub const fn is_timer_token(self) -> bool {
        matches!(self, Self::RocePacingTimer)
    }

    pub const fn code(self) -> u8 {
        match self {
            Self::Data => 0,
            Self::Feedback => 1,
            Self::TcpData(_) => 2,
            Self::TcpAck(_) => 3,
            Self::Pfc(_) => 4,
            Self::DcqcnCnp(_) => 5,
            Self::RoceData(_) => 7,
            Self::RoceAck(_) => 8,
            Self::RoceNack(_) => 9,
            Self::RocePacingTimer => 10,
        }
    }
}

impl PacketDescriptor {
    pub const fn ecn_codepoint(self) -> EcnCodepoint {
        if !self.kind.is_data() {
            EcnCodepoint::NotEct
        } else if self.ecn_marked {
            EcnCodepoint::Ce
        } else {
            EcnCodepoint::Ect0
        }
    }

    /// Applies CE only to ECN-capable data. NotEct control/feedback remains unchanged.
    pub fn mark_ecn(&mut self) -> bool {
        if self.kind.is_data() {
            self.ecn_marked = true;
            true
        } else {
            false
        }
    }
}

/// Immutable configuration of one constant-rate, directed, non-preemptive link.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkDescriptor {
    pub id: LinkId,
    /// Logical process that owns transmission on this physical directed link.
    pub source: NodeId,
    /// A logical-process representative of the physical receiving node. Packet delivery uses the
    /// route-selected `RemoteChannel::target`, which may be another egress LP at that same switch.
    pub target: NodeId,
    pub rate_bps: u64,
    pub propagation_ns: u64,
}

impl LinkDescriptor {
    /// Computes serialization plus propagation without an absolute start time.
    pub fn delay_ns(&self, bytes: u64) -> Result<u64, TimeError> {
        link_arrival_time_ns(0, bytes, self.rate_bps, self.propagation_ns)
    }

    /// Computes packet arrival using this link's constant rate and propagation delay.
    pub fn arrival_time_ns(&self, start_time_ns: u64, bytes: u64) -> Result<u64, TimeError> {
        link_arrival_time_ns(start_time_ns, bytes, self.rate_bps, self.propagation_ns)
    }
}

/// Declared lower-bound channel for one kind of cross-node event.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteChannel {
    pub source: NodeId,
    /// Route-selected receiver LP after crossing `link`.
    pub target: NodeId,
    /// Directed physical link supplying serialization rate and propagation delay.
    pub link: LinkId,
    pub event_kind: EventKind,
    pub min_delay_ns: u64,
}

impl RemoteChannel {
    /// Constructs the canonical packet channel for a link and its admitted minimum packet size.
    pub fn for_packet_link(
        link: LinkDescriptor,
        min_packet_size_bytes: u64,
    ) -> Result<Self, TimeError> {
        Self::for_packet_link_to(link, link.target, min_packet_size_bytes)
    }

    /// Constructs a packet channel whose physical link feeds a route-selected downstream LP.
    pub fn for_packet_link_to(
        link: LinkDescriptor,
        target: NodeId,
        min_packet_size_bytes: u64,
    ) -> Result<Self, TimeError> {
        Ok(Self {
            source: link.source,
            target,
            link: link.id,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: link.delay_ns(min_packet_size_bytes)?,
        })
    }
}

/// One immutable semantic image containing every host and switch logical process.
#[derive(Clone, Eq, PartialEq)]
pub struct SimulationImage {
    /// Inclusive configured simulation endpoint in integer nanoseconds.
    pub stop_time_ns: u64,
    pub nodes: Vec<NodeDescriptor>,
    pub host_states: Vec<HostState>,
    pub switch_states: Vec<SwitchState>,
    pub flows: Vec<FlowDescriptor>,
    /// Packet records needed by initial events or preloaded mutable state, never a whole-run table.
    pub initial_packets: Vec<PacketDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub channels: Vec<RemoteChannel>,
    pub initial_events: Vec<Event>,
    pub seed: u64,
    /// The predecessor runs of join stages ([`StagePredecessors::Join`]), fixed at lowering and
    /// read-only; empty, with no allocation, in an image without a join.
    pub stage_joins: Vec<FlowId>,
    /// The parameters of every seeded all-to-all, ascending by collective id (P16 H1, ruling R7):
    /// its pairs' stages carry the matrix's bytes, and the progress certificate names the
    /// parameters so LeanGuard can re-derive them. Empty, with no allocation, without one.
    pub seeded_all_to_alls: Vec<SeededCollective>,
}

/// One seeded all-to-all's routing-matrix parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SeededCollective {
    pub collective_id: u64,
    pub matrix: crate::SeededAllToAll,
}

impl fmt::Debug for SimulationImage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("SimulationImage");
        debug
            .field("stop_time_ns", &self.stop_time_ns)
            .field("nodes", &self.nodes)
            .field("host_states", &self.host_states)
            .field("switch_states", &self.switch_states)
            .field("flows", &self.flows)
            .field("initial_packets", &self.initial_packets)
            .field("links", &self.links)
            .field("channels", &self.channels)
            .field("initial_events", &self.initial_events)
            .field("seed", &self.seed);
        // Omitting the empty additive field preserves every image byte without a join.
        if !self.stage_joins.is_empty() {
            debug.field("stage_joins", &self.stage_joins);
        }
        if !self.seeded_all_to_alls.is_empty() {
            debug.field("seeded_all_to_alls", &self.seeded_all_to_alls);
        }
        debug.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CollectiveStage, FlowDescriptor, FlowGeneratorState, HostPfcState, HostState, LinkId,
        PacketKind, RoceGenerator, RoceReceiverState, StageDependencies,
    };
    use std::collections::VecDeque;

    /// P15: `FlowDescriptor::packet_priority` runs on every PFC-monitored switch arrival and
    /// service decision (five Scalar sites), so it stays force-inlined, as the CUDA readback
    /// decoders are after P14 lost 25 ms at E6 60% to an out-of-line decode
    /// (`days-gpu/evidence/P14/cuda-host.md`).
    #[test]
    fn packet_priority_is_force_inlined() {
        let source = include_str!("image.rs");
        assert!(
            source.contains("#[inline(always)]\n    pub const fn packet_priority("),
            "`FlowDescriptor::packet_priority` must be #[inline(always)]"
        );
    }

    /// P15 queue-pair layout budgets. A RoCE queue pair is a variant of `FlowGeneratorKind`, whose
    /// union is 256 B (`TcpGenerator`, 248 B, rounded to the 16-byte alignment of the `u128`
    /// pacing credit): the queue pair must fit it, or every flow's generator grows by 16 B. Its
    /// receiver lives in the host's boxed `roce_receivers` slice, which costs a host without one
    /// the 16-B pointer (200 B at `main` 4174d8a, 216 B now) and no allocation. The flow's
    /// feedback priority uses padding after `priority`, and every new packet header fits the
    /// 24 B the TCP headers already reserve. Layout is the compiler's choice, so these are upper
    /// bounds on 64-bit targets.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn roce_queue_pairs_grow_no_per_flow_record() {
        let checks = [
            ("RoceGenerator", std::mem::size_of::<RoceGenerator>(), 240),
            ("FlowDescriptor", std::mem::size_of::<FlowDescriptor>(), 80),
            ("HostState", std::mem::size_of::<HostState>(), 224),
            (
                "RoceReceiverState",
                std::mem::size_of::<RoceReceiverState>(),
                88,
            ),
            ("PacketKind", std::mem::size_of::<PacketKind>(), 32),
        ];
        for (name, size, bound) in checks {
            assert!(
                size <= bound,
                "{name} grew to {size} B, above its {bound} B budget"
            );
        }
    }

    /// P16 D1 layout budgets (`days-gpu/evidence/P16/dcqcn-design.md` §5.4). The Mellanox-form
    /// controller is 136 B (152 B for the paper form at `main` 9ff20ea): 80 B of configuration and
    /// 56 B of state, four `bool`s packed after the `u32` stage (a stored freeze flag would pad it to
    /// 144 B, so the freeze is derived from the generator). With no control token, an unreliable
    /// DCQCN generator is 208 B (240 B at `main`) and a queue pair 240 B (256 B, the union's full
    /// budget, at `main`), which leaves the window its 16 B. A queue pair's receiver loses its
    /// 40 B notification point (88 B; 120 B at `main`), and the ACK header keeps the 24 B every
    /// packet descriptor reserves although it now carries the ECN echo.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn mellanox_dcqcn_state_fits_its_budgets() {
        let checks = [
            (
                "DcqcnController",
                std::mem::size_of::<super::DcqcnController>(),
                136,
            ),
            (
                "DcqcnGenerator",
                std::mem::size_of::<super::DcqcnGenerator>(),
                208,
            ),
            ("RoceGenerator", std::mem::size_of::<RoceGenerator>(), 240),
            (
                "RoceReceiverState",
                std::mem::size_of::<RoceReceiverState>(),
                88,
            ),
            (
                "RoceAckHeader",
                std::mem::size_of::<super::RoceAckHeader>(),
                24,
            ),
            ("PacketKind", std::mem::size_of::<PacketKind>(), 32),
        ];
        for (name, size, bound) in checks {
            assert!(
                size <= bound,
                "{name} grew to {size} B, above its {bound} B budget"
            );
        }
    }

    /// P15 host-link PFC layout budget. A host's egress pause state lives behind one optional
    /// box, `HostState::pfc`, so a host without a PFC-controlled egress link pays the 8-B
    /// pointer (216 B to 224 B, pinned above) and no allocation, and the switch-side
    /// `PfcQueueState` keeps its layout and its `Debug` bytes.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn host_pfc_state_is_one_optional_box() {
        assert_eq!(std::mem::size_of::<Option<Box<HostPfcState>>>(), 8);
        let host = HostState {
            egress_link: LinkId(0),
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators: Vec::new(),
            stages: Vec::new(),
            tcp_receivers: Vec::new(),
            dcqcn_receivers: Vec::new(),
            roce_receivers: None,
            pfc: None,
            next_origin_seq: 0,
            next_payload_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        };
        assert!(
            !format!("{host:#?}").contains("pfc"),
            "a host without host-link PFC keeps its pre-P15 Debug bytes"
        );
    }

    /// P15 lane R3 layout budget: collective stages over RoCE queue pairs reuse the stage record
    /// as it is. A RoCE stage's gated state lives in fields the queue pair already has (its anchors
    /// at zero, its pacer parked) and its release flag is `CollectiveStage::activated`, so neither
    /// the stage record (136 B at `ade83b4`, one per generator on a stage host) nor its
    /// dependencies (56 B) nor the progress record (280 B; RoCE rows add a `stage_kind` value, not
    /// a field) grows. P16 H1's join counts took the progress record's two single-predecessor
    /// fields (the certificate writer lists predecessors from the image), so it shrank to 256 B.
    /// Layout is the compiler's choice, so these are upper bounds on 64-bit targets.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn roce_collective_stages_grow_no_stage_record() {
        let checks = [
            (
                "CollectiveStage",
                std::mem::size_of::<CollectiveStage>(),
                136,
            ),
            (
                "Option<CollectiveStage>",
                std::mem::size_of::<Option<CollectiveStage>>(),
                136,
            ),
            (
                "StageDependencies",
                std::mem::size_of::<StageDependencies>(),
                56,
            ),
            (
                "CollectiveProgressRecord",
                std::mem::size_of::<crate::CollectiveProgressRecord>(),
                256,
            ),
        ];
        for (name, size, bound) in checks {
            assert!(
                size <= bound,
                "{name} grew to {size} B, above its {bound} B budget"
            );
        }
    }

    /// `FlowGeneratorState` is held once per flow in the image and in every Scalar and CPU host
    /// state, so each byte is a per-flow cost at lowering and at run time: at the 262,144-flow
    /// frontier every 16 B is 4.2 MB. Before P14 (`main` at 948a0e9) it was 352 B. P14 carried the
    /// collective or compute stage record inline as `stage: Option<CollectiveStage>` (136 B, padded
    /// to 144 B by the type's 16-byte alignment), 496 B for every generator though most are not
    /// stages. The record now lives in the host's parallel `HostState::stages` table, so the
    /// generator is back to `main`'s layout. `repr(C)` with the 16-byte alignment `u128` credit
    /// forces gives no spare bytes, so any field added here rounds up to 368 B.
    ///
    /// Layout is the compiler's choice, so the bound is an upper bound on 64-bit targets.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn flow_generator_state_keeps_main_size() {
        const FLOW_GENERATOR_STATE_MAX_BYTES: usize = 352;
        let size = std::mem::size_of::<FlowGeneratorState>();
        assert!(
            size <= FLOW_GENERATOR_STATE_MAX_BYTES,
            "FlowGeneratorState grew to {size} B, above {FLOW_GENERATOR_STATE_MAX_BYTES} B: keep \
             per-generator stage data in the host's `stages` table"
        );
    }
}

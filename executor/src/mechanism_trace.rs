//! Stable exact-integer certificates for stateful mechanism replay.

use std::fmt::{self, Write};

use crate::{
    CollectiveAlgorithm, CollectivePhase, DcqcnTransitionRecord, EventKey, ExactRational, FlowId,
    GeneratorStatus, LinkId, NodeId, PayloadId, WfqSchedulerState,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateReplayConfig {
    pub pacing_interval_ns: u64,
    pub packet_size_bytes: u64,
    pub total_bytes: u64,
    pub rate_numerator_bits_per_second: u64,
    pub rate_denominator: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateReplayState {
    pub packets_emitted: u64,
    pub bytes_emitted: u64,
    pub credit_quanta: u128,
    pub status: GeneratorStatus,
    pub next_time_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub flow: FlowId,
    pub payload: PayloadId,
    pub stop_time_ns: u64,
    pub current_packet_size_bytes: u64,
    pub config: RateReplayConfig,
    pub before: RateReplayState,
    pub after: RateReplayState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PfcOccupancyAction {
    Admit,
    Drain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PfcControlAction {
    Pause,
    Resume,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PfcThresholdTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub queue_id: u64,
    pub controlled_link: LinkId,
    pub priority: u8,
    pub xon_bytes: u64,
    pub xoff_bytes: u64,
    pub buffer_capacity_bytes: u64,
    pub amount_bytes: u64,
    pub action: PfcOccupancyAction,
    pub before_occupancy_bytes: u64,
    pub before_asserted: bool,
    pub after_occupancy_bytes: u64,
    pub after_asserted: bool,
    pub emitted: Option<PfcControlAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PfcControlTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub queue_id: u64,
    pub controlled_link: LinkId,
    pub controller: NodeId,
    pub priority: u8,
    pub action: PfcControlAction,
    pub before_controllers: Vec<NodeId>,
    pub after_controllers: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerPacket {
    pub payload: PayloadId,
    pub flow: FlowId,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrrTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub queue_id: u64,
    pub class_count: u64,
    pub quanta_bytes: Vec<u64>,
    pub before_deficits_bytes: Vec<u64>,
    pub before_current_class: u64,
    pub scan_steps: u128,
    pub eligible_packets: Vec<SchedulerPacket>,
    pub selected_payload: PayloadId,
    pub after_deficits_bytes: Vec<u64>,
    pub after_current_class: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WrrTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub queue_id: u64,
    pub class_count: u64,
    pub weights: Vec<u64>,
    pub before_packets_sent: Vec<u64>,
    pub before_current_class: u64,
    pub eligible_packets: Vec<SchedulerPacket>,
    pub selected_payload: PayloadId,
    pub after_packets_sent: Vec<u64>,
    pub after_current_class: u64,
}

/// Which exact WFQ transition a [`WfqTransitionRecord`] records, by the event that performs it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WfqTransitionKind {
    /// An admitted packet's arrival (phase 0): virtual time advances (or resets on an idle
    /// scheduler) and the packet gets its finish tag.
    Enqueue,
    /// A service start (`TxReady`, phase 2): the scheduler state is unchanged, and the served
    /// packet is the waiting packet with the least finish tag among those PFC does not pause.
    Select,
    /// A service completion (`TxComplete`, phase 1): virtual time advances, the packet's class
    /// leaves the active set when it was the class's last packet, and an idle scheduler resets.
    Complete,
}

/// The exact WFQ scheduler state a transition reads and writes. Virtual time and finish tags are
/// normalized by the queue's link rate (bits, not seconds), as in [`WfqSchedulerState`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WfqReplayState {
    pub virtual_time: ExactRational,
    pub last_updated_ns: u64,
    /// Each class's last finish tag.
    pub finish_times: Vec<ExactRational>,
    /// Each class's queued plus in-service packets.
    pub active_packets: Vec<u64>,
}

impl WfqReplayState {
    pub fn of(state: &WfqSchedulerState) -> Self {
        Self {
            virtual_time: state.virtual_time.clone(),
            last_updated_ns: state.last_updated_ns,
            finish_times: state.finish_times.clone(),
            active_packets: state.active_packets.clone(),
        }
    }
}

/// One packet waiting at a WFQ service decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WfqQueuedPacket {
    pub packet: SchedulerPacket,
    /// The packet's PFC class (its flow's packet priority), which decides whether a paused
    /// priority holds it back. Zero on a queue without a PFC monitor.
    pub pfc_priority: u8,
    pub finish: ExactRational,
}

/// One exact WFQ transition of a switch egress queue (P16 L2; the `wfq` mode of LeanGuard's
/// `p10c_mechanisms_check`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WfqTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub queue_id: u64,
    pub kind: WfqTransitionKind,
    /// The egress link's rate, which converts elapsed nanoseconds into virtual time.
    pub rate_bps: u64,
    pub weights: Vec<u64>,
    /// The packet enqueued, served, or completed.
    pub packet: SchedulerPacket,
    /// The enqueued packet's virtual start, `max(virtual time, its class's last finish tag)`.
    pub virtual_start: Option<ExactRational>,
    /// The packet's finish tag.
    pub finish: ExactRational,
    pub before: WfqReplayState,
    pub after: WfqReplayState,
    /// At a selection: every packet waiting before it, the served one included, in queue order.
    pub queued_packets: Vec<WfqQueuedPacket>,
    /// At a selection: the PFC priorities paused at the queue's egress, ascending.
    pub paused_priorities: Vec<u8>,
}

/// Which Static Priority transition an [`SpTransitionRecord`] records, by the event that performs
/// it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpTransitionKind {
    /// An admitted packet's arrival (phase 0).
    Enqueue,
    /// A service start (`TxReady`, phase 2), with the time its transmission completes.
    Schedule,
    /// A service completion (`TxComplete`, phase 1).
    Depart,
}

/// One Static Priority transition of a switch egress queue (P16 L2; LeanGuard's `sp_check`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpTransitionRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub queue_id: u64,
    pub kind: SpTransitionKind,
    pub class_count: u64,
    pub packet: SchedulerPacket,
    /// `flow % class_count`.
    pub class_id: u64,
    /// The class's priority; a greater value is served first.
    pub priority: u64,
    /// The transmission's completion time, on `Schedule` and `Depart` rows.
    pub departure_time_ns: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectiveActivationCause {
    LocalCompletion,
    InboundArrival,
}

/// Transport or timer that carries a dependency-gated stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectiveStageKind {
    /// A collective stage carried by an ordinary TCP generator.
    Tcp,
    /// A delay-only compute stage.
    Compute,
    /// A collective stage carried by a RoCE queue pair (P15; `qp-schema.md` Amendment 4).
    Roce,
    /// A same-server collective stage carried by a stage notify (P16 H2): its row writes the chunk
    /// as `packet_size_bytes`, the sender's lead as `interval_ns`, and the message delay (lead plus
    /// lane) as `duration_ns`.
    Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectiveProgressRecord {
    pub key: EventKey,
    /// Distinguishes progress transitions caused by the same executor event.
    pub ordinal: u64,
    pub node: NodeId,
    pub flow: FlowId,
    pub cause: CollectiveActivationCause,
    pub cause_flow: FlowId,
    pub arrival_bytes: u64,
    /// Collective identity; the compute-group identity for a compute stage.
    pub collective_id: u64,
    /// `None` for a compute stage.
    pub algorithm: Option<CollectiveAlgorithm>,
    pub group_size: u32,
    pub declared_total_bytes: u64,
    pub rank: u32,
    /// `None` for a compute stage.
    pub phase: Option<CollectivePhase>,
    /// One-based collective step; zero for a compute stage.
    pub step: u32,
    pub chunk_offset_bytes: u64,
    pub chunk_bytes: u64,
    /// A transport stage's MSS or MTU, and a RoCE stage's pacing interval; zero for a compute
    /// stage (the certificate writer names its inbound predecessors' transport).
    pub packet_size_bytes: u64,
    pub interval_ns: u64,
    pub stop_time_ns: u64,
    /// The inbound requirement: the inbound predecessors' summed totals, zero without any.
    pub inbound_predecessor_bytes: u64,
    pub before_local_complete: bool,
    /// Local predecessors complete before this transition (a join counts several).
    pub before_local_completed: u32,
    pub before_inbound_complete: bool,
    pub before_inbound_bytes: u64,
    /// Whether this prerequisite transition unblocked the stage and emitted its first packet.
    pub activated: bool,
    pub after_local_complete: bool,
    pub after_local_completed: u32,
    pub after_inbound_complete: bool,
    pub after_inbound_bytes: u64,
    pub after_packets_emitted: u64,
    pub after_bytes_emitted: u64,
    pub after_status: GeneratorStatus,
    pub after_next_time_ns: u64,
    pub stage_kind: CollectiveStageKind,
    /// Compute interval of a delay-only stage; zero for every data stage.
    pub duration_ns: u64,
    /// Inbound rows: the arriving data segment `[segment_sequence, segment_sequence +
    /// segment_bytes)`, from a TCP sequence number or a RoCE PSN. Every segment of a pending
    /// inbound predecessor is logged, including ones that do not advance the receiver's in-order
    /// frontier. Zero on local rows.
    pub segment_sequence: u64,
    pub segment_bytes: u64,
    /// Local rows caused by a TCP or RoCE stage: the completing ACK's cumulative acknowledgment.
    /// Zero for a compute cause and on inbound rows.
    pub ack_number: u64,
    /// Local rows: when the completing signal originated. TCP: when the segment answered by the
    /// completing ACK was sent (the ACK echoes it). Compute: when the timer was armed. Zero on
    /// inbound rows.
    pub cause_origin_ns: u64,
    /// Local rows: TCP, the unloaded round trip of that segment (forward route) and the ACK
    /// (reverse route), a lower bound on `time - cause_origin_ns`; compute, the timer duration,
    /// met exactly. Zero on inbound rows.
    pub cause_delay_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MechanismTransitionRecord {
    Rate(RateTransitionRecord),
    PfcThreshold(PfcThresholdTransitionRecord),
    PfcControl(PfcControlTransitionRecord),
    Drr(DrrTransitionRecord),
    Wrr(WrrTransitionRecord),
    Dcqcn(DcqcnTransitionRecord),
    Collective(CollectiveProgressRecord),
    /// A RoCE queue pair's reliability transition (P15; schema `qp-schema.md`).
    Roce(crate::RoceTransitionRecord),
    /// An exact WFQ transition (P16 L2), boxed so the enum keeps the size of its other variants.
    Wfq(Box<WfqTransitionRecord>),
    /// A Static Priority transition (P16 L2).
    Sp(SpTransitionRecord),
}

impl MechanismTransitionRecord {
    pub const fn key(&self) -> EventKey {
        match self {
            Self::Rate(record) => record.key,
            Self::PfcThreshold(record) => record.key,
            Self::PfcControl(record) => record.key,
            Self::Drr(record) => record.key,
            Self::Wrr(record) => record.key,
            Self::Dcqcn(record) => record.key,
            Self::Collective(record) => record.key,
            Self::Roce(record) => record.key(),
            Self::Wfq(record) => record.key,
            Self::Sp(record) => record.key,
        }
    }

    pub const fn canonical_order_key(&self) -> (EventKey, u8, u64) {
        let (tag, ordinal) = match self {
            Self::Rate(_) => (0, 0),
            Self::PfcThreshold(_) => (1, 0),
            Self::PfcControl(_) => (2, 0),
            Self::Drr(_) => (3, 0),
            Self::Wrr(_) => (4, 0),
            Self::Dcqcn(_) => (5, 0),
            Self::Collective(record) => (6, record.ordinal),
            // A host RESUME yields one `resume` row per restarted queue pair at one key.
            Self::Roce(record) => (7, record.flow().0),
            Self::Wfq(_) => (8, 0),
            Self::Sp(_) => (9, 0),
        };
        (self.key(), tag, ordinal)
    }
}

/// The collective progress CSV (P14's schema). RoCE stages write `stage_kind = roce` rows under
/// the same columns, as pinned by Amendment 4 of `days-gpu/plans/briefs/p15/qp-schema.md`: the MTU
/// and pacing interval in `packet_size_bytes` and `interval_ns`, the PSN in `segment_sequence`,
/// and the Go-back-N frontier's advance in `arrival_bytes`. A compute stage whose inbound
/// predecessors are RoCE queue pairs names their MTU and pacing interval there (Amendment 5).
///
/// P16 H1 (counted joins) replaces the single `local_predecessor_flow_id` and
/// `inbound_predecessor_flow_id` columns by predecessor lists and appends the operation columns,
/// read from `image`'s stage records: the row stage's `channel`, `chunk_policy` and
/// `channel_policy`; `local_predecessors` and `inbound_predecessors` (`;`-separated flow ids,
/// ascending) and `local_required`; the local completion count before and after; and
/// `cause_total_bytes`, the byte total of the cause flow (zero for a compute stage); and
/// `group_stages`, the stages of the row's collective or compute group in the image (a seeded
/// all-to-all has no stage for a pair of zero bytes); and `seeded_matrix`, a seeded all-to-all's
/// matrix parameters (`seed;matrix;group;transpose;experts;topk;tokens;bytes_per_copy;skew`),
/// from which LeanGuard re-derives every pair's bytes, empty on every other row; and
/// `cause_kind`, what carries the cause flow (`tcp`, `roce`, `notify` for a stage notify, or
/// `compute`), which decides how LeanGuard replays an arrival and binds a completion; and
/// `cause_collective_id`, a stage-notify cause's collective, empty for any other cause.
///
/// P16 H1 fix round 2 (review N1): a stage notify is delay-only, delivered exactly its delay `d`
/// (its timer's lead plus its lane) after its release. Its delivery rows name that release and
/// delay, as timer completions do: `cause_origin_ns` = the delivery time minus `d`, and
/// `cause_delay_ns` = `d`. (The progress record keeps zero there on every inbound row; only the
/// certificate names the notify's.)
/// `inbound_predecessor_bytes` is the stage's
/// inbound requirement: the summed totals of its inbound predecessors, zero without any.
pub fn collective_transitions_csv(
    records: &[MechanismTransitionRecord],
    image: &crate::SimulationImage,
) -> Result<String, CollectiveTraceError> {
    // Every stage record, byte total and carrier by flow id, gathered once.
    let mut stages = vec![None; image.flows.len()];
    let mut totals = vec![(0_u64, Carrier::Compute); image.flows.len()];
    let mut group_stages = std::collections::BTreeMap::<(bool, u64), u64>::new();
    for state in &image.host_states {
        for (generator, stage) in state.generators_with_stages() {
            if let Some(stage) = stage {
                let group = match stage.role {
                    crate::StageRole::Collective(identity) => (false, identity.collective_id),
                    crate::StageRole::Compute(compute) => (true, compute.compute_id),
                };
                *group_stages.entry(group).or_default() += 1;
            }
            let Ok(index) = usize::try_from(generator.flow.0) else {
                continue;
            };
            if index < stages.len() {
                stages[index] = stage;
                totals[index] = match (generator.kind, stage.map(|stage| stage.role)) {
                    (crate::FlowGeneratorKind::Tcp(tcp), _) => (tcp.total_bytes, Carrier::Tcp),
                    (crate::FlowGeneratorKind::Roce(roce), _) => (
                        roce.pacer.total_bytes,
                        Carrier::Roce(roce.pacer.mtu_bytes, roce.pacer.pacing_interval_ns),
                    ),
                    // A stage notify (P16 H2) carries its chunk on a constant timer, delivered its
                    // lead plus its lane after its release.
                    (
                        crate::FlowGeneratorKind::Constant(constant),
                        Some(crate::StageRole::Collective(identity)),
                    ) => (
                        constant.packet_size_bytes,
                        Carrier::Notify {
                            delay_ns: constant
                                .interval_ns
                                .checked_add(constant.first_departure_ns)
                                .ok_or(CollectiveTraceError::NotifyTiming {
                                    flow: generator.flow,
                                })?,
                            collective_id: identity.collective_id,
                        },
                    ),
                    _ => (0, Carrier::Compute),
                };
            }
        }
    }
    let stage_of = |flow: FlowId| {
        usize::try_from(flow.0)
            .ok()
            .and_then(|index| stages.get(index).copied().flatten())
    };
    let totals_of = |flow: FlowId| {
        usize::try_from(flow.0)
            .ok()
            .and_then(|index| totals.get(index).copied())
            .unwrap_or((0, Carrier::Compute))
    };
    // Amendment 5: the one fabric transport of a compute stage's inbound predecessors; a stage
    // notify delivers its chunk whole and names none.
    let inbound_transport = |flow: FlowId, dependencies: crate::StageDependencies| {
        let mut transports = dependencies.inbound.iter(&image.stage_joins).filter_map(
            |predecessor| match totals_of(predecessor).1 {
                Carrier::Roce(mtu, interval) => Some((mtu, interval)),
                Carrier::Tcp => Some((0, 0)),
                Carrier::Notify { .. } | Carrier::Compute => None,
            },
        );
        let first = transports.next().unwrap_or_default();
        if transports.all(|transport| transport == first) {
            Ok(first)
        } else {
            Err(CollectiveTraceError::MixedInboundTransports { flow })
        }
    };
    let list = |predecessors: crate::StagePredecessors| {
        predecessors
            .iter(&image.stage_joins)
            .map(|flow| flow.0.to_string())
            .collect::<Vec<_>>()
            .join(";")
    };
    let mut records = records
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Collective(record) => Some(*record),
            _ => None,
        })
        .collect::<Vec<_>>();
    records.sort_by_key(|record| (record.key, record.ordinal));
    if let Some(duplicate) = records
        .windows(2)
        .find(|pair| (pair[0].key, pair[0].ordinal) == (pair[1].key, pair[1].ordinal))
    {
        return Err(CollectiveTraceError::Duplicate(MechanismTraceError {
            mechanism: "collective",
            duplicate_key: duplicate[0].key,
        }));
    }

    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,ordinal,node_id,flow_id,cause,cause_flow_id,arrival_bytes,collective_id,algorithm,group_size,declared_total_bytes,rank,collective_phase,step,chunk_offset_bytes,chunk_bytes,packet_size_bytes,interval_ns,stop_time_ns,inbound_predecessor_bytes,before_local_complete,before_inbound_complete,before_inbound_bytes,activated,after_local_complete,after_inbound_complete,after_inbound_bytes,after_packets_emitted,after_bytes_emitted,after_status,after_next_time_ns,stage_kind,duration_ns,segment_sequence,segment_bytes,ack_number,cause_origin_ns,cause_delay_ns,channel,chunk_policy,channel_policy,local_predecessors,inbound_predecessors,local_required,before_local_completed,after_local_completed,cause_total_bytes,group_stages,seeded_matrix,cause_kind,cause_collective_id\n",
    );
    for record in records {
        let stage = stage_of(record.flow);
        let dependencies = stage.map(|stage| stage.dependencies);
        let identity = stage.and_then(|stage| match stage.role {
            crate::StageRole::Collective(identity) => Some(identity),
            crate::StageRole::Compute(_) => None,
        });
        let carrier = totals_of(record.cause_flow).1;
        // A stage notify's delivery names its release and delay (review N1).
        let (cause_origin_ns, cause_delay_ns) = match (record.cause, carrier) {
            (CollectiveActivationCause::InboundArrival, Carrier::Notify { delay_ns, .. }) => (
                record.key.time_ns.checked_sub(delay_ns).ok_or(
                    CollectiveTraceError::NotifyTiming {
                        flow: record.cause_flow,
                    },
                )?,
                delay_ns,
            ),
            _ => (record.cause_origin_ns, record.cause_delay_ns),
        };
        let (packet_size_bytes, interval_ns) = match (record.stage_kind, dependencies) {
            (CollectiveStageKind::Compute, Some(dependencies)) => {
                inbound_transport(record.flow, dependencies)?
            }
            _ => (record.packet_size_bytes, record.interval_ns),
        };
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.ordinal,
            record.node.0,
            record.flow.0,
            collective_cause(record.cause),
            record.cause_flow.0,
            record.arrival_bytes,
            record.collective_id,
            record.algorithm.map_or("", collective_algorithm),
            record.group_size,
            record.declared_total_bytes,
            record.rank,
            record.phase.map_or("", collective_phase),
            record.step,
            record.chunk_offset_bytes,
            record.chunk_bytes,
            packet_size_bytes,
            interval_ns,
            record.stop_time_ns,
            record.inbound_predecessor_bytes,
            bit(record.before_local_complete),
            bit(record.before_inbound_complete),
            record.before_inbound_bytes,
            bit(record.activated),
            bit(record.after_local_complete),
            bit(record.after_inbound_complete),
            record.after_inbound_bytes,
            record.after_packets_emitted,
            record.after_bytes_emitted,
            status(record.after_status),
            record.after_next_time_ns,
            collective_stage_kind(record.stage_kind),
            record.duration_ns,
            record.segment_sequence,
            record.segment_bytes,
            record.ack_number,
            cause_origin_ns,
            cause_delay_ns,
            identity.map_or(0, |identity| identity.channel),
            identity.map_or("", |identity| chunk_policy(identity.chunk_policy)),
            identity.map_or("", |identity| channel_policy(identity.channel_policy)),
            dependencies.map_or_else(String::new, |dependencies| list(dependencies.local)),
            dependencies.map_or_else(String::new, |dependencies| list(dependencies.inbound)),
            dependencies.map_or(0, |dependencies| dependencies.local.count()),
            record.before_local_completed,
            record.after_local_completed,
            totals_of(record.cause_flow).0,
            group_stages
                .get(&(
                    record.stage_kind == CollectiveStageKind::Compute,
                    record.collective_id
                ))
                .copied()
                .unwrap_or(0),
            identity
                .and_then(|identity| {
                    image
                        .seeded_all_to_alls
                        .binary_search_by_key(&identity.collective_id, |entry| {
                            entry.collective_id
                        })
                        .ok()
                })
                .map_or_else(String::new, |index| seeded_matrix(
                    &image.seeded_all_to_alls[index].matrix
                )),
            match carrier {
                Carrier::Tcp => "tcp",
                Carrier::Roce(..) => "roce",
                Carrier::Notify { .. } => "notify",
                Carrier::Compute => "compute",
            },
            match carrier {
                Carrier::Notify { collective_id, .. } => collective_id.to_string(),
                _ => String::new(),
            },
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

/// What carries a stage's bytes, as the certificate names a cause's (`cause_kind`).
#[derive(Clone, Copy)]
enum Carrier {
    Tcp,
    /// A RoCE queue pair's MTU and pacing interval.
    Roce(u64, u64),
    /// A stage notify (P16 H2): a same-server message of a collective, delivered whole its delay
    /// (its timer's lead plus its lane) after its release.
    Notify {
        delay_ns: u64,
        collective_id: u64,
    },
    Compute,
}

/// A seeded all-to-all's matrix parameters, `;`-separated:
/// `seed;matrix;group;transpose;experts;topk;tokens;bytes_per_copy;skew`.
fn seeded_matrix(matrix: &crate::SeededAllToAll) -> String {
    format!(
        "{};{};{};{};{};{};{};{};{}",
        matrix.seed,
        matrix.matrix,
        matrix.group,
        u8::from(matrix.transpose),
        matrix.experts,
        matrix.topk,
        matrix.tokens,
        matrix.bytes_per_copy,
        match matrix.skew {
            crate::RoutingSkew::Uniform => "uniform",
            crate::RoutingSkew::Zipf1 => "zipf1",
        }
    )
}

const fn chunk_policy(policy: crate::CollectiveChunkPolicy) -> &'static str {
    match policy {
        crate::CollectiveChunkPolicy::EqualRemainderLast => "equal_remainder_last",
        crate::CollectiveChunkPolicy::UniformFloor => "uniform_floor",
        crate::CollectiveChunkPolicy::Seeded => "seeded",
    }
}

const fn channel_policy(policy: crate::CollectiveChannelPolicy) -> &'static str {
    match policy {
        crate::CollectiveChannelPolicy::RingNext => "ring_next",
        crate::CollectiveChannelPolicy::Channels => "channels",
        crate::CollectiveChannelPolicy::AllPairs => "all_pairs",
        crate::CollectiveChannelPolicy::Pair => "pair",
    }
}

const fn collective_stage_kind(kind: CollectiveStageKind) -> &'static str {
    match kind {
        CollectiveStageKind::Tcp => "tcp",
        CollectiveStageKind::Roce => "roce",
        CollectiveStageKind::Compute => "compute",
        CollectiveStageKind::Notify => "notify",
    }
}

const fn collective_cause(cause: CollectiveActivationCause) -> &'static str {
    match cause {
        CollectiveActivationCause::LocalCompletion => "local_completion",
        CollectiveActivationCause::InboundArrival => "inbound_arrival",
    }
}

const fn collective_algorithm(algorithm: CollectiveAlgorithm) -> &'static str {
    match algorithm {
        CollectiveAlgorithm::RingAllReduce => "ring_allreduce",
        CollectiveAlgorithm::AllGather => "allgather",
        CollectiveAlgorithm::ReduceScatter => "reduce_scatter",
        CollectiveAlgorithm::AllToAll => "all_to_all",
        CollectiveAlgorithm::SendRecv => "send_recv",
    }
}

const fn collective_phase(phase: CollectivePhase) -> &'static str {
    match phase {
        CollectivePhase::ReduceScatter => "reduce_scatter",
        CollectivePhase::AllGather => "allgather",
        CollectivePhase::AllToAll => "all_to_all",
        CollectivePhase::SendRecv => "send_recv",
    }
}

/// The CNP arrivals at DCQCN reaction points, one row per arrival in `(time, flow, payload)`
/// order (P16 D1 fix round 1: the LeanGuard CNP join for unreliable flows, `p10c_dcqcn_check
/// trace`). Built from a full-observation result's arrival and packet planes: every arrival with
/// disposition `Feedback` of a `DcqcnCnp` packet, which the source host consumes whether or not
/// its controller is frozen. Columns: `time_ns,flow_id,payload`.
pub fn dcqcn_cnp_arrivals_csv(
    arrivals: &[crate::PacketArrivalObservation],
    packets: &[crate::PacketDescriptor],
) -> String {
    let cnp_flows = packets
        .iter()
        .filter(|packet| matches!(packet.kind, crate::PacketKind::DcqcnCnp(_)))
        .map(|packet| (packet.id, packet.flow))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut rows = arrivals
        .iter()
        .filter(|arrival| arrival.disposition == crate::ArrivalDisposition::Feedback)
        .filter_map(|arrival| {
            cnp_flows
                .get(&arrival.payload)
                .map(|flow| (arrival.time_ns, flow.0, arrival.payload.0))
        })
        .collect::<Vec<_>>();
    rows.sort_unstable();
    let mut csv = String::from("time_ns,flow_id,payload\n");
    for (time_ns, flow, payload) in rows {
        writeln!(csv, "{time_ns},{flow},{payload}").expect("writing to String cannot fail");
    }
    csv
}

/// The Mellanox-form DCQCN controller transitions, one row per (event, flow) in `EventKey` order
/// (pinned schema `days-gpu/plans/briefs/p16/dcqcn-schema.md`). One event yields at most one row
/// per flow; a host RESUME can yield one `advance` row per queue pair it restarts.
pub fn dcqcn_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let mut records = records
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Dcqcn(record) => Some(*record),
            _ => None,
        })
        .collect::<Vec<_>>();
    records.sort_unstable_by_key(|record| (record.key, record.flow));
    if let Some(pair) = records
        .windows(2)
        .find(|pair| pair[0].key == pair[1].key && pair[0].flow == pair[1].flow)
    {
        return Err(MechanismTraceError {
            mechanism: "DCQCN",
            duplicate_key: pair[0].key,
        });
    }
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,kind,bound_ns,frozen,alpha_ticks,increase_fires,decrease_cuts,initial_rate_bps,minimum_rate_bps,maximum_rate_bps,additive_rate_bps,hyper_rate_bps,g_q63,alpha_interval_ns,decrease_interval_ns,increase_interval_ns,fast_recovery_steps,clamp_target_rate,before_alpha_q63,before_current_rate_bps,before_target_rate_bps,before_next_alpha_ns,before_next_decrease_ns,before_next_increase_ns,before_stage,before_armed,before_alpha_pending,before_decrease_pending,before_increase_armed,after_alpha_q63,after_current_rate_bps,after_target_rate_bps,after_next_alpha_ns,after_next_decrease_ns,after_next_increase_ns,after_stage,after_armed,after_alpha_pending,after_decrease_pending,after_increase_armed\n",
    );
    for record in records {
        let config = record.before.config;
        debug_assert_eq!(config, record.after.config);
        write!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.flow.0,
            record.kind.label(),
            record.bound_ns,
            bit(record.frozen),
            record.advance.alpha_ticks,
            record.advance.increase_fires,
            record.advance.decrease_cuts,
            config.initial_rate_bps,
            config.minimum_rate_bps,
            config.maximum_rate_bps,
            config.additive_rate_bps,
            config.hyper_rate_bps,
            config.g_q63,
            config.alpha_interval_ns,
            config.decrease_interval_ns,
            config.increase_interval_ns,
            config.fast_recovery_steps,
            bit(config.clamp_target_rate),
        )
        .expect("writing to String cannot fail");
        for state in [record.before, record.after] {
            write!(
                csv,
                ",{},{},{},{},{},{},{},{},{},{},{}",
                state.alpha_q63,
                state.current_rate_bps,
                state.target_rate_bps,
                state.next_alpha_ns,
                state.next_decrease_ns,
                state.next_increase_ns,
                state.stage,
                bit(state.armed),
                bit(state.alpha_pending),
                bit(state.decrease_pending),
                bit(state.increase_armed),
            )
            .expect("writing to String cannot fail");
        }
        csv.push('\n');
    }
    Ok(csv)
}

/// The sender transitions of RoCE queue pairs, one row per event in `EventKey` order (pinned
/// schema `days-gpu/plans/briefs/p15/qp-schema.md`, with Amendment 6: the ECN echo and the
/// window).
pub fn roce_sender_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    // Canonical order is (event key, flow): one event yields one sender row, except a host
    // RESUME, which yields one `resume` row per queue pair it restarts (schema Amendment 2).
    let mut records = records
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Roce(crate::RoceTransitionRecord::Sender(record)) => {
                Some(*record)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    records.sort_unstable_by_key(|record| (record.key, record.flow));
    if let Some(pair) = records.windows(2).find(|pair| {
        pair[0].key == pair[1].key
            && (pair[0].flow == pair[1].flow
                || pair[0].node != pair[1].node
                || pair[0].kind != crate::RoceSenderKind::Resume
                || pair[1].kind != crate::RoceSenderKind::Resume)
    }) {
        return Err(MechanismTraceError {
            mechanism: "RoCE sender",
            duplicate_key: pair[0].key,
        });
    }
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,kind,class_paused,window_blocked,data_class,mtu_bytes,total_bytes,pacing_interval_ns,first_pacing_time_ns,rto_ns,window_bytes,variable_window,maximum_rate_bps,initial_rate_bps,rate_bps,input_acknowledgment,input_ce_echo,emitted,emitted_psn,emitted_bytes,emitted_retransmission,emitted_payload,before_next_psn,before_snd_una,before_bytes_emitted,before_packets_emitted,before_credit_quanta,before_rto_deadline_ns,before_pacer,before_next_tick_ns,before_status,after_next_psn,after_snd_una,after_bytes_emitted,after_packets_emitted,after_credit_quanta,after_rto_deadline_ns,after_pacer,after_next_tick_ns,after_status\n",
    );
    for record in records {
        let emitted = record.emitted;
        write!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.flow.0,
            record.kind.label(),
            bit(record.class_paused),
            bit(record.window_blocked),
            record.data_class,
            record.mtu_bytes,
            record.total_bytes,
            record.pacing_interval_ns,
            record.first_pacing_time_ns,
            record.rto_ns,
            record.window_bytes,
            bit(record.variable_window),
            record.maximum_rate_bps,
            record.initial_rate_bps,
            optional_u64(record.rate_bps),
            optional_u64(record.input_acknowledgment),
            optional_bit(record.input_ce_echo),
            bit(emitted.is_some()),
            optional_u64(emitted.map(|emission| emission.psn)),
            optional_u64(emitted.map(|emission| emission.bytes)),
            emitted.map_or_else(String::new, |emission| bit(emission.retransmission)
                .to_string()),
            optional_u64(emitted.map(|emission| emission.payload.0)),
        )
        .expect("writing to String cannot fail");
        for view in [record.before, record.after] {
            write!(
                csv,
                ",{},{},{},{},{},{},{},{},{}",
                view.next_psn,
                view.snd_una,
                view.bytes_emitted,
                view.packets_emitted,
                view.credit_quanta,
                optional_u64(view.rto_deadline_ns),
                view.pacer.label(),
                optional_u64(view.next_tick_ns),
                status(view.status),
            )
            .expect("writing to String cannot fail");
        }
        csv.push('\n');
    }
    Ok(csv)
}

/// The receiver transitions of RoCE queue pairs: one row per data arrival in `EventKey` order
/// (pinned schema `days-gpu/plans/briefs/p15/qp-schema.md`, with Amendment 6: a queue pair's
/// receiver has no notification point, and each ACK or NACK echoes its packet's CE mark).
pub fn roce_receiver_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "RoCE receiver",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Roce(crate::RoceTransitionRecord::Receiver(record)) => {
                    Some((record.key, *record))
                }
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,total_bytes,ack_every_packets,nack_interval_ns,duplicate_ack,ack_size_bytes,packet_psn,packet_bytes,packet_sent_time_ns,packet_retransmission,packet_ce,action,feedback_acknowledgment,feedback_payload,feedback_ce_echo,before_expected_psn,before_packets_since_ack,before_last_nack_psn,before_last_nack_time_ns,after_expected_psn,after_packets_since_ack,after_last_nack_psn,after_last_nack_time_ns\n",
    );
    for record in records {
        write!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.flow.0,
            record.total_bytes,
            record.ack_every_packets,
            record.nack_interval_ns,
            bit(record.duplicate_ack),
            record.ack_size_bytes,
            record.packet_psn,
            record.packet_bytes,
            record.packet_sent_time_ns,
            bit(record.packet_retransmission),
            bit(record.packet_ce),
            record.action.label(),
            optional_u64(record.feedback_acknowledgment),
            optional_u64(record.feedback_payload.map(|payload| payload.0)),
            optional_bit(record.feedback_ce_echo),
        )
        .expect("writing to String cannot fail");
        for view in [record.before, record.after] {
            write!(
                csv,
                ",{},{},{},{}",
                view.expected_psn,
                view.packets_since_ack,
                optional_u64(view.last_nack_psn),
                optional_u64(view.last_nack_time_ns),
            )
            .expect("writing to String cannot fail");
        }
        csv.push('\n');
    }
    Ok(csv)
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn optional_bit(value: Option<bool>) -> &'static str {
    match value {
        None => "",
        Some(false) => "0",
        Some(true) => "1",
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MechanismTraceError {
    pub mechanism: &'static str,
    pub duplicate_key: EventKey,
}

impl fmt::Display for MechanismTraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "duplicate canonical {} transition key {:?}",
            self.mechanism, self.duplicate_key
        )
    }
}

impl std::error::Error for MechanismTraceError {}

/// Why the collective progress CSV cannot be written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectiveTraceError {
    Duplicate(MechanismTraceError),
    /// A compute stage's inbound predecessors use different transports, which its one pair of
    /// Amendment 5 columns cannot name.
    MixedInboundTransports {
        flow: FlowId,
    },
    /// A stage notify's delay overflows, or a delivery precedes it (its release would be negative).
    NotifyTiming {
        flow: FlowId,
    },
}

impl fmt::Display for CollectiveTraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate(error) => error.fmt(formatter),
            Self::MixedInboundTransports { flow } => write!(
                formatter,
                "compute stage {flow:?} has inbound predecessors of different transports"
            ),
            Self::NotifyTiming { flow } => write!(
                formatter,
                "stage notify {flow:?} has a delay or a delivery time its release cannot precede"
            ),
        }
    }
}

impl std::error::Error for CollectiveTraceError {}

fn canonical<T: Clone>(
    mechanism: &'static str,
    mut records: Vec<(EventKey, T)>,
) -> Result<Vec<T>, MechanismTraceError> {
    records.sort_by_key(|record| record.0);
    if let Some(duplicate_key) = records
        .windows(2)
        .find(|pair| pair[0].0 == pair[1].0)
        .map(|pair| pair[0].0)
    {
        return Err(MechanismTraceError {
            mechanism,
            duplicate_key,
        });
    }
    Ok(records.into_iter().map(|(_, record)| record).collect())
}

pub fn rate_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "rate",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Rate(record) => Some((record.key, *record)),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,payload_id,stop_time_ns,current_packet_size_bytes,pacing_interval_ns,packet_size_bytes,total_bytes,rate_numerator_bits_per_second,rate_denominator,before_packets_emitted,before_bytes_emitted,before_credit_quanta,before_status,before_next_time_ns,after_packets_emitted,after_bytes_emitted,after_credit_quanta,after_status,after_next_time_ns\n",
    );
    for record in records {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.flow.0,
            record.payload.0,
            record.stop_time_ns,
            record.current_packet_size_bytes,
            record.config.pacing_interval_ns,
            record.config.packet_size_bytes,
            record.config.total_bytes,
            record.config.rate_numerator_bits_per_second,
            record.config.rate_denominator,
            record.before.packets_emitted,
            record.before.bytes_emitted,
            record.before.credit_quanta,
            status(record.before.status),
            record.before.next_time_ns,
            record.after.packets_emitted,
            record.after.bytes_emitted,
            record.after.credit_quanta,
            status(record.after.status),
            record.after.next_time_ns,
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

pub fn pfc_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "PFC",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::PfcThreshold(record) => {
                    Some((record.key, MechanismTransitionRecord::PfcThreshold(*record)))
                }
                MechanismTransitionRecord::PfcControl(record) => Some((
                    record.key,
                    MechanismTransitionRecord::PfcControl(record.clone()),
                )),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,queue_id,kind,controlled_link,controller,priority,xon_bytes,xoff_bytes,buffer_capacity_bytes,amount_bytes,occupancy_action,before_occupancy_bytes,before_asserted,after_occupancy_bytes,after_asserted,control_action,before_controllers,after_controllers\n",
    );
    for record in records {
        match record {
            MechanismTransitionRecord::PfcThreshold(record) => writeln!(
                csv,
                "{},{},{},{},{},{},threshold,{},,{},{},{},{},{},{},{},{},{},{},{},,",
                record.key.time_ns,
                record.key.phase,
                record.key.origin_node.0,
                record.key.origin_seq,
                record.node.0,
                record.queue_id,
                record.controlled_link.0,
                record.priority,
                record.xon_bytes,
                record.xoff_bytes,
                record.buffer_capacity_bytes,
                record.amount_bytes,
                occupancy_action(record.action),
                record.before_occupancy_bytes,
                bit(record.before_asserted),
                record.after_occupancy_bytes,
                bit(record.after_asserted),
                optional_control(record.emitted),
            ),
            MechanismTransitionRecord::PfcControl(record) => writeln!(
                csv,
                "{},{},{},{},{},{},control,{},{},{},,,,,,,,,,{},{},{}",
                record.key.time_ns,
                record.key.phase,
                record.key.origin_node.0,
                record.key.origin_seq,
                record.node.0,
                record.queue_id,
                record.controlled_link.0,
                record.controller.0,
                record.priority,
                control(record.action),
                controllers(&record.before_controllers),
                controllers(&record.after_controllers),
            ),
            _ => unreachable!("PFC filter is closed"),
        }
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

pub fn drr_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "DRR",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Drr(record) => Some((record.key, record.clone())),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,queue_id,class_count,quanta_bytes,before_deficits_bytes,before_current_class,scan_steps,eligible_packets,selected_payload,after_deficits_bytes,after_current_class\n",
    );
    for record in records {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.queue_id,
            record.class_count,
            naturals(&record.quanta_bytes),
            naturals(&record.before_deficits_bytes),
            record.before_current_class,
            record.scan_steps,
            packets(&record.eligible_packets),
            record.selected_payload.0,
            naturals(&record.after_deficits_bytes),
            record.after_current_class,
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

pub fn wrr_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "WRR",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Wrr(record) => Some((record.key, record.clone())),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,queue_id,class_count,weights,before_packets_sent,before_current_class,eligible_packets,selected_payload,after_packets_sent,after_current_class\n",
    );
    for record in records {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.queue_id,
            record.class_count,
            naturals(&record.weights),
            naturals(&record.before_packets_sent),
            record.before_current_class,
            packets(&record.eligible_packets),
            record.selected_payload.0,
            naturals(&record.after_packets_sent),
            record.after_current_class,
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

/// The WFQ certificate: one row per enqueue, service start and service completion at a WFQ
/// egress queue, in canonical event-key order (one row per event). Exact rationals are written
/// `numerator/denominator` in lowest terms; lists are `;`-separated; a queued packet is
/// `payload:flow:size_bytes:pfc_priority:finish`.
pub fn wfq_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "WFQ",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Wfq(record) => Some((record.key, record.as_ref())),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,kind,node_id,queue_id,rate_bps,weights,payload,flow_id,size_bytes,virtual_start,finish_tag,before_virtual_time,before_last_updated_ns,before_finish_tags,before_active_packets,queued_packets,paused_priorities,after_virtual_time,after_last_updated_ns,after_finish_tags,after_active_packets\n",
    );
    for record in records {
        let kind = match record.kind {
            WfqTransitionKind::Enqueue => "enqueue",
            WfqTransitionKind::Select => "select",
            WfqTransitionKind::Complete => "complete",
        };
        let queued = record
            .queued_packets
            .iter()
            .map(|queued| {
                format!(
                    "{}:{}:{}:{}:{}",
                    queued.packet.payload.0,
                    queued.packet.flow.0,
                    queued.packet.size_bytes,
                    queued.pfc_priority,
                    rational(&queued.finish)
                )
            })
            .collect::<Vec<_>>()
            .join(";");
        let paused = record
            .paused_priorities
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(";");
        writeln!(
            csv,
            "{},{},{},{},{kind},{},{},{},{},{},{},{},{},{},{},{},{},{},{queued},{paused},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.queue_id,
            record.rate_bps,
            naturals(&record.weights),
            record.packet.payload.0,
            record.packet.flow.0,
            record.packet.size_bytes,
            record.virtual_start.as_ref().map_or_else(String::new, rational),
            rational(&record.finish),
            rational(&record.before.virtual_time),
            record.before.last_updated_ns,
            rationals(&record.before.finish_times),
            naturals(&record.before.active_packets),
            rational(&record.after.virtual_time),
            record.after.last_updated_ns,
            rationals(&record.after.finish_times),
            naturals(&record.after.active_packets),
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

/// The Static Priority certificate `sp_check` reads: one row per enqueue, schedule and depart at an
/// SP egress queue, in canonical event-key order. `sp_check` keys a scheduler by one natural, so
/// `scheduler_id` is `node_id * 2^32 + queue_id`; the `node_id` and `queue_id` columns repeat it
/// for readers. A packet is identified by `(flow_id, packet_id)`, its payload.
pub fn sp_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "SP",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Sp(record) => Some((record.key, *record)),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,origin_node,origin_seq,kind,scheduler_id,class_count,packet_id,flow_id,class_id,priority,size_bytes,departure_time_ns,node_id,queue_id\n",
    );
    for record in records {
        let kind = match record.kind {
            SpTransitionKind::Enqueue => "enqueue",
            SpTransitionKind::Schedule => "schedule",
            SpTransitionKind::Depart => "depart",
        };
        let scheduler_id = (u128::from(record.node.0) << 32) + u128::from(record.queue_id);
        writeln!(
            csv,
            "{},{},{},{},{kind},{scheduler_id},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.class_count,
            record.packet.payload.0,
            record.packet.flow.0,
            record.class_id,
            record.priority,
            record.packet.size_bytes,
            record
                .departure_time_ns
                .map_or_else(String::new, |time| time.to_string()),
            record.node.0,
            record.queue_id,
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

/// An exact rational in lowest terms, `numerator/denominator`.
fn rational(value: &ExactRational) -> String {
    format!("{}/{}", value.numer(), value.denom())
}

fn rationals(values: &[ExactRational]) -> String {
    values.iter().map(rational).collect::<Vec<_>>().join(";")
}

const fn status(status: GeneratorStatus) -> &'static str {
    match status {
        GeneratorStatus::Scheduled => "scheduled",
        GeneratorStatus::Blocked => "blocked",
        GeneratorStatus::Finished => "finished",
        GeneratorStatus::Stopped => "stopped",
    }
}

const fn bit(value: bool) -> u8 {
    if value { 1 } else { 0 }
}

const fn occupancy_action(action: PfcOccupancyAction) -> &'static str {
    match action {
        PfcOccupancyAction::Admit => "admit",
        PfcOccupancyAction::Drain => "drain",
    }
}

const fn control(action: PfcControlAction) -> &'static str {
    match action {
        PfcControlAction::Pause => "pause",
        PfcControlAction::Resume => "resume",
    }
}

fn optional_control(action: Option<PfcControlAction>) -> &'static str {
    action.map_or("", control)
}

fn naturals(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(";")
}

fn controllers(values: &[NodeId]) -> String {
    values
        .iter()
        .map(|value| value.0.to_string())
        .collect::<Vec<_>>()
        .join(";")
}

fn packets(values: &[SchedulerPacket]) -> String {
    values
        .iter()
        .map(|packet| {
            format!(
                "{}:{}:{}",
                packet.payload.0, packet.flow.0, packet.size_bytes
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

//! Stable exact-integer certificates for stateful mechanism replay.

use std::fmt::{self, Write};

use crate::{
    CollectiveAlgorithm, CollectivePhase, DcqcnTransitionRecord, EventKey, FlowId, GeneratorStatus,
    LinkId, NodeId, PayloadId,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CollectiveActivationCause {
    LocalCompletion,
    InboundArrival,
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
    pub collective_id: u64,
    pub algorithm: CollectiveAlgorithm,
    pub group_size: u32,
    pub declared_total_bytes: u64,
    pub rank: u32,
    pub phase: CollectivePhase,
    pub step: u32,
    pub chunk_offset_bytes: u64,
    pub chunk_bytes: u64,
    pub packet_size_bytes: u64,
    pub interval_ns: u64,
    pub stop_time_ns: u64,
    pub local_predecessor: Option<FlowId>,
    pub inbound_predecessor: Option<FlowId>,
    pub inbound_predecessor_bytes: u64,
    pub before_local_complete: bool,
    pub before_inbound_complete: bool,
    pub before_inbound_bytes: u64,
    /// Whether this prerequisite transition unblocked the stage and emitted its first packet.
    pub activated: bool,
    pub after_local_complete: bool,
    pub after_inbound_complete: bool,
    pub after_inbound_bytes: u64,
    pub after_packets_emitted: u64,
    pub after_bytes_emitted: u64,
    pub after_status: GeneratorStatus,
    pub after_next_time_ns: u64,
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
        };
        (self.key(), tag, ordinal)
    }
}

pub fn collective_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
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
        return Err(MechanismTraceError {
            mechanism: "collective",
            duplicate_key: duplicate[0].key,
        });
    }

    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,ordinal,node_id,flow_id,cause,cause_flow_id,arrival_bytes,collective_id,algorithm,group_size,declared_total_bytes,rank,collective_phase,step,chunk_offset_bytes,chunk_bytes,packet_size_bytes,interval_ns,stop_time_ns,local_predecessor_flow_id,inbound_predecessor_flow_id,inbound_predecessor_bytes,before_local_complete,before_inbound_complete,before_inbound_bytes,activated,after_local_complete,after_inbound_complete,after_inbound_bytes,after_packets_emitted,after_bytes_emitted,after_status,after_next_time_ns\n",
    );
    for record in records {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
            collective_algorithm(record.algorithm),
            record.group_size,
            record.declared_total_bytes,
            record.rank,
            collective_phase(record.phase),
            record.step,
            record.chunk_offset_bytes,
            record.chunk_bytes,
            record.packet_size_bytes,
            record.interval_ns,
            record.stop_time_ns,
            optional_flow(record.local_predecessor),
            optional_flow(record.inbound_predecessor),
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
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
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
    }
}

const fn collective_phase(phase: CollectivePhase) -> &'static str {
    match phase {
        CollectivePhase::ReduceScatter => "reduce_scatter",
        CollectivePhase::AllGather => "allgather",
    }
}

fn optional_flow(flow: Option<FlowId>) -> String {
    flow.map_or_else(String::new, |flow| flow.0.to_string())
}

pub fn dcqcn_transitions_csv(
    records: &[MechanismTransitionRecord],
) -> Result<String, MechanismTraceError> {
    let records = canonical(
        "DCQCN",
        records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Dcqcn(record) => Some((record.key, *record)),
                _ => None,
            })
            .collect(),
    )?;
    let mut csv = String::from(
        "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,kind,applied,emitted_bytes,initial_rate_bps,minimum_rate_bps,maximum_rate_bps,additive_rate_bps,hyper_rate_bps,g_ppb,decrease_ppb,cnp_interval_ns,control_interval_ns,increase_byte_threshold,before_alpha_ppb,before_current_rate_bps,before_target_rate_bps,before_cnp_seen,before_last_cnp_time_ns,before_stage,before_stage_steps,before_bytes_since_increase,before_next_control_time_ns,after_alpha_ppb,after_current_rate_bps,after_target_rate_bps,after_cnp_seen,after_last_cnp_time_ns,after_stage,after_stage_steps,after_bytes_since_increase,after_next_control_time_ns\n",
    );
    for record in records {
        let config = record.before.config;
        debug_assert_eq!(config, record.after.config);
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.flow.0,
            record.kind.label(),
            bit(record.applied),
            record.emitted_bytes,
            config.initial_rate_bps,
            config.minimum_rate_bps,
            config.maximum_rate_bps,
            config.additive_rate_bps,
            config.hyper_rate_bps,
            config.g_ppb,
            config.decrease_ppb,
            config.cnp_interval_ns,
            config.control_interval_ns,
            config.increase_byte_threshold,
            record.before.alpha_ppb,
            record.before.current_rate_bps,
            record.before.target_rate_bps,
            bit(record.before.cnp_seen),
            optional_u64(record.before.last_cnp_time_ns),
            record.before.stage.label(),
            record.before.stage_steps,
            record.before.bytes_since_increase,
            record.before.next_control_time_ns,
            record.after.alpha_ppb,
            record.after.current_rate_bps,
            record.after.target_rate_bps,
            bit(record.after.cnp_seen),
            optional_u64(record.after.last_cnp_time_ns),
            record.after.stage.label(),
            record.after.stage_steps,
            record.after.bytes_since_increase,
            record.after.next_control_time_ns,
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
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

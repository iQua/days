//! Stable exact-integer certificates for T25 stateful mechanism replay.

use std::fmt::{self, Write};

use crate::{EventKey, FlowId, GeneratorStatus, LinkId, NodeId, PayloadId};

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MechanismTransitionRecord {
    Rate(RateTransitionRecord),
    PfcThreshold(PfcThresholdTransitionRecord),
    PfcControl(PfcControlTransitionRecord),
    Drr(DrrTransitionRecord),
    Wrr(WrrTransitionRecord),
}

impl MechanismTransitionRecord {
    pub const fn key(&self) -> EventKey {
        match self {
            Self::Rate(record) => record.key,
            Self::PfcThreshold(record) => record.key,
            Self::PfcControl(record) => record.key,
            Self::Drr(record) => record.key,
            Self::Wrr(record) => record.key,
        }
    }

    pub const fn canonical_order_key(&self) -> (EventKey, u8) {
        let tag = match self {
            Self::Rate(_) => 0,
            Self::PfcThreshold(_) => 1,
            Self::PfcControl(_) => 2,
            Self::Drr(_) => 3,
            Self::Wrr(_) => 4,
        };
        (self.key(), tag)
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

//! Stable CSV projection for exact LeanGuard RED/ECN enqueue replay.

use std::fmt::{self, Write};

use std::collections::BTreeMap;

use crate::{
    AqmTransitionAction, AqmTransitionRecord, DropMarkPolicy, PacketDescriptor, PacketKind,
    PayloadId, QueueDepthUnit,
};

const HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,queue_id,payload_id,packet_kind,queued_packets_before,queued_bytes_before,packet_size_bytes,ecn_before,ecn_after,policy,depth_unit,capacity,threshold,min_threshold,max_threshold,max_probability_numerator,max_probability_denominator,mark_ecn,before_average_scaled,before_counter,after_average_scaled,after_counter,action\n";

/// Why an AQM certificate cannot be written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AqmTraceError {
    /// Two transitions share one canonical event key.
    DuplicateKey(crate::EventKey),
    /// A transition's packet is not among the run's observed packets, so its kind is unknown.
    UnknownPacket(PayloadId),
}

impl fmt::Display for AqmTraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKey(key) => {
                write!(formatter, "duplicate canonical AQM transition key {key:?}")
            }
            Self::UnknownPacket(payload) => {
                write!(formatter, "AQM transition of unobserved packet {payload:?}")
            }
        }
    }
}

impl std::error::Error for AqmTraceError {}

/// Serializes full-observation scalar/CPU AQM transitions in canonical event-key order.
///
/// `packets` are the run's observed packets (`RunResult::observed_packets`), which name each
/// transition's `packet_kind`: Days AGO marks only data packets (`PacketKind::is_data`), so an ACK,
/// NACK or CNP admitted where a data packet would be marked is enqueued unmarked, and the checker
/// needs the kind to tell the two apart.
pub fn aqm_transitions_csv(
    records: &[AqmTransitionRecord],
    packets: &[PacketDescriptor],
) -> Result<String, AqmTraceError> {
    let mut records = records.to_vec();
    records.sort_by_key(|record| record.key);
    if let Some(duplicate_key) = records
        .windows(2)
        .find(|pair| pair[0].key == pair[1].key)
        .map(|pair| pair[0].key)
    {
        return Err(AqmTraceError::DuplicateKey(duplicate_key));
    }
    let kinds = packets
        .iter()
        .map(|packet| (packet.id, packet.kind))
        .collect::<BTreeMap<_, _>>();

    let mut csv = String::from(HEADER);
    for record in records {
        let kind = packet_kind(
            *kinds
                .get(&record.payload)
                .ok_or(AqmTraceError::UnknownPacket(record.payload))?,
        );
        let (
            policy,
            unit,
            capacity,
            threshold,
            minimum,
            maximum,
            numerator,
            denominator,
            mark_ecn,
            before_average,
            before_counter,
            after_average,
            after_counter,
        ) = match (record.before, record.after) {
            (DropMarkPolicy::EcnThreshold(before), DropMarkPolicy::EcnThreshold(after))
                if before == after =>
            {
                (
                    "threshold",
                    unit(before.unit),
                    before.capacity,
                    Some(before.threshold),
                    None,
                    None,
                    None,
                    None,
                    false,
                    None,
                    None,
                    None,
                    None,
                )
            }
            (DropMarkPolicy::Red(before), DropMarkPolicy::Red(after)) => (
                "red",
                unit(before.unit),
                before.capacity,
                None,
                Some(before.min_threshold),
                Some(before.max_threshold),
                Some(before.max_probability_numerator),
                Some(before.max_probability_denominator),
                before.mark_ecn,
                Some(before.average_scaled),
                Some(before.counter),
                Some(after.average_scaled),
                Some(after.counter),
            ),
            _ => unreachable!("AQM transition records preserve their closed policy variant"),
        };
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{kind},{},{},{},{},{},{policy},{unit},{capacity},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
            record.queue_id,
            record.payload.0,
            record.queued_packets_before,
            record.queued_bytes_before,
            record.packet_size_bytes,
            bit(record.ecn_before),
            bit(record.ecn_after),
            optional(threshold),
            optional(minimum),
            optional(maximum),
            optional(numerator),
            optional(denominator),
            bit(mark_ecn),
            optional(before_average),
            optional(before_counter),
            optional(after_average),
            optional(after_counter),
            action(record.action),
        )
        .expect("writing to String cannot fail");
    }
    Ok(csv)
}

/// The certificate's name for a packet kind; `p10c_aqm_check` treats `data`, `tcp_data` and
/// `roce_data` as data.
const fn packet_kind(kind: PacketKind) -> &'static str {
    match kind {
        PacketKind::Data => "data",
        PacketKind::Feedback => "feedback",
        PacketKind::TcpData(_) => "tcp_data",
        PacketKind::TcpAck(_) => "tcp_ack",
        PacketKind::Pfc(_) => "pfc",
        PacketKind::DcqcnCnp(_) => "dcqcn_cnp",
        PacketKind::RoceData(_) => "roce_data",
        PacketKind::RoceAck(_) => "roce_ack",
        PacketKind::RoceNack(_) => "roce_nack",
        PacketKind::RocePacingTimer => "roce_pacing_timer",
        PacketKind::StageNotify => "stage_notify",
    }
}

const fn unit(unit: QueueDepthUnit) -> &'static str {
    match unit {
        QueueDepthUnit::Packets => "packets",
        QueueDepthUnit::Bytes => "bytes",
    }
}

const fn bit(value: bool) -> u8 {
    if value { 1 } else { 0 }
}

const fn action(action: AqmTransitionAction) -> &'static str {
    match action {
        AqmTransitionAction::Enqueue => "enqueue",
        AqmTransitionAction::Mark => "mark",
        AqmTransitionAction::Drop => "drop",
    }
}

fn optional<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DcqcnCnpHeader, LinkId, PfcHeader, RoceAckHeader, RoceDataHeader, TcpAckHeader,
        TcpDataHeader,
    };

    /// `p10c_aqm_check` treats exactly `data`, `tcp_data` and `roce_data` as data packets
    /// (`dataKind` in `lean/LeanGuard/P10c/AqmEventLog.lean`); the names must follow
    /// `PacketKind::is_data`, the executor's marking rule.
    #[test]
    fn packet_kind_names_follow_the_marking_rule() {
        let tcp_data = TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        };
        let tcp_ack = TcpAckHeader {
            acknowledgment: 0,
            acknowledged_bytes: 0,
            echoed_sent_time_ns: 0,
        };
        let pfc = PfcHeader {
            controlled_link: LinkId(0),
            priority: 0,
            pause: true,
        };
        let cnp = DcqcnCnpHeader {
            trigger_payload: PayloadId(0),
        };
        let roce_data = RoceDataHeader {
            psn: 0,
            sent_time_ns: 0,
            retransmission: false,
        };
        let roce_ack = RoceAckHeader {
            acknowledgment: 0,
            echoed_sent_time_ns: 0,
            acknowledged_bytes: 0,
            ce_echo: false,
        };
        let kinds = [
            PacketKind::Data,
            PacketKind::Feedback,
            PacketKind::TcpData(tcp_data),
            PacketKind::TcpAck(tcp_ack),
            PacketKind::Pfc(pfc),
            PacketKind::DcqcnCnp(cnp),
            PacketKind::RoceData(roce_data),
            PacketKind::RoceAck(roce_ack),
            PacketKind::RoceNack(roce_ack),
            PacketKind::RocePacingTimer,
            PacketKind::StageNotify,
        ];
        for kind in kinds {
            let name = packet_kind(kind);
            assert_eq!(
                matches!(name, "data" | "tcp_data" | "roce_data"),
                kind.is_data(),
                "{name}"
            );
        }
    }
}

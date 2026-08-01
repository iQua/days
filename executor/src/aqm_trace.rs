//! Stable CSV projection for exact LeanGuard RED/ECN enqueue replay.

use std::fmt::{self, Write};

use crate::{AqmTransitionAction, AqmTransitionRecord, DropMarkPolicy, QueueDepthUnit};

const HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,payload_id,queued_packets_before,queued_bytes_before,packet_size_bytes,ecn_before,ecn_after,policy,depth_unit,capacity,threshold,min_threshold,max_threshold,max_probability_numerator,max_probability_denominator,mark_ecn,before_average_scaled,before_counter,after_average_scaled,after_counter,action\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AqmTraceError {
    pub duplicate_key: crate::EventKey,
}

impl fmt::Display for AqmTraceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "duplicate canonical AQM transition key {:?}",
            self.duplicate_key
        )
    }
}

impl std::error::Error for AqmTraceError {}

/// Serializes full-observation scalar/CPU AQM transitions in canonical event-key order.
pub fn aqm_transitions_csv(records: &[AqmTransitionRecord]) -> Result<String, AqmTraceError> {
    let mut records = records.to_vec();
    records.sort_by_key(|record| record.key);
    if let Some(duplicate_key) = records
        .windows(2)
        .find(|pair| pair[0].key == pair[1].key)
        .map(|pair| pair[0].key)
    {
        return Err(AqmTraceError { duplicate_key });
    }

    let mut csv = String::from(HEADER);
    for record in records {
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
            "{},{},{},{},{},{},{},{},{},{},{},{policy},{unit},{capacity},{},{},{},{},{},{},{},{},{},{},{}",
            record.key.time_ns,
            record.key.phase,
            record.key.origin_node.0,
            record.key.origin_seq,
            record.node.0,
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

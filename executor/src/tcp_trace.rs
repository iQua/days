//! Stable CSV projection for LeanGuard TCP transition replay.

use std::fmt::Write;

use crate::{TcpCongestionControl, TcpPhase, TcpTransitionInput, TcpTransitionRecord};

const HEADER: &str = "time_ns,event_id,kind,node_id,flow_id,algorithm,mss_bytes,acked_bytes,rtt_ns,flight_bytes,acknowledgment,recovery_high_input,before_phase,before_cwnd_bytes,before_ssthresh_bytes,before_dupacks,before_recovery_high,before_ca_credit,before_cwnd_scaled,before_ssthresh_scaled,before_w_max_scaled,before_w_last_max_scaled,before_epoch_ns,before_srtt_ns,before_k_ns,after_phase,after_cwnd_bytes,after_ssthresh_bytes,after_dupacks,after_recovery_high,after_ca_credit,after_cwnd_scaled,after_ssthresh_scaled,after_w_max_scaled,after_w_last_max_scaled,after_epoch_ns,after_srtt_ns,after_k_ns\n";

/// Serializes full-observation scalar/CPU TCP records in canonical event-key order.
pub fn tcp_transitions_csv(records: &[TcpTransitionRecord]) -> String {
    let mut records = records.to_vec();
    records.sort_unstable_by_key(|record| record.key);
    let mut csv = String::from(HEADER);
    for (event_id, record) in records.into_iter().enumerate() {
        let (kind, acked, rtt, flight, acknowledgment, recovery_high_input) = match record.input {
            TcpTransitionInput::NewAck {
                acknowledged_bytes,
                rtt_sample_ns,
                flight_size_bytes,
                acknowledgment,
            } => (
                "new_ack",
                Some(acknowledged_bytes),
                Some(rtt_sample_ns),
                flight_size_bytes,
                Some(acknowledgment),
                None,
            ),
            TcpTransitionInput::DuplicateAck {
                flight_size_bytes,
                recovery_high_sequence,
            } => (
                "duplicate_ack",
                None,
                None,
                flight_size_bytes,
                None,
                Some(recovery_high_sequence),
            ),
            TcpTransitionInput::Timeout { flight_size_bytes } => {
                ("timeout", None, None, flight_size_bytes, None, None)
            }
        };
        let algorithm = record.before.label();
        debug_assert_eq!(algorithm, record.after.label());
        write!(
            csv,
            "{},{event_id},{kind},{},{},{algorithm},{},{},{},{flight},{},{},",
            record.key.time_ns,
            record.node.0,
            record.flow.0,
            record.mss_bytes,
            optional(acked),
            optional(rtt),
            optional(acknowledgment),
            optional(recovery_high_input),
        )
        .expect("writing to String cannot fail");
        write_state(&mut csv, record.before, record.mss_bytes);
        csv.push(',');
        write_state(&mut csv, record.after, record.mss_bytes);
        csv.push('\n');
    }
    csv
}

fn write_state(csv: &mut String, control: TcpCongestionControl, mss_bytes: u64) {
    write!(
        csv,
        "{},{},{},{},{},{},{},{},{},{},{},{},{}",
        phase(control.phase()),
        control.cwnd_bytes(mss_bytes),
        control.ssthresh_bytes(),
        control.duplicate_acks(),
        control.recovery_high_sequence(),
        control.ca_credit(),
        control.cwnd_scaled(),
        control.ssthresh_scaled(),
        control.w_max_scaled(),
        control.w_last_max_scaled(),
        optional(control.epoch_start_ns()),
        control.srtt_ns(),
        control.cubic_k_ns(),
    )
    .expect("writing to String cannot fail");
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

const fn phase(phase: TcpPhase) -> &'static str {
    match phase {
        TcpPhase::SlowStart => "slow_start",
        TcpPhase::CongestionAvoidance => "congestion_avoidance",
        TcpPhase::FastRecovery => "fast_recovery",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventKey, FlowId, NodeId};

    #[test]
    fn csv_has_a_stable_fixed_width_schema() {
        let before = TcpCongestionControl::reno(512);
        let mut after = before;
        after.on_new_ack(512, 10, 10, 1024, 512);
        let csv = tcp_transitions_csv(&[TcpTransitionRecord {
            key: EventKey {
                time_ns: 10,
                phase: 0,
                origin_node: NodeId(1),
                origin_seq: 2,
            },
            node: NodeId(0),
            flow: FlowId(0),
            mss_bytes: 512,
            input: TcpTransitionInput::NewAck {
                acknowledged_bytes: 512,
                rtt_sample_ns: 10,
                flight_size_bytes: 1024,
                acknowledgment: 512,
            },
            before,
            after,
        }]);
        let lines = csv.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].split(',').count(), 38);
        assert_eq!(lines[1].split(',').count(), 38);
    }
}

//! P15 lane R1: the RoCE queue-pair transition records replay the design note's semantics.
//!
//! For every `configs/p15` fixture, the Scalar full-observation records of each queue pair are
//! replayed independently of the executor: consecutive records of a flow continue each other's
//! state, and every tick, ACK, NACK, timeout and data arrival obeys its rule (design note
//! `days-gpu/evidence/P15/qp-design.md` §5). The CSV writers emit the pinned schema
//! (`days-gpu/plans/briefs/p15/qp-schema.md`), one row per record.

use std::collections::BTreeMap;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    FlowId, GeneratorStatus, MechanismTransitionRecord, ObservationMode, PacketKind, RoceEmission,
    RocePacerState, RoceReceiverAction, RoceReceiverRecord, RoceSenderKind, RoceSenderRecord,
    RoceTransitionRecord, RunResult, roce_receiver_transitions_csv, roce_sender_transitions_csv,
    run_scalar_with_observations,
};

const FIXTURES: [&str; 7] = [
    "roce_lossless_pfc.toml",
    "roce_gbn_lossy.toml",
    "roce_timeout.toml",
    "roce_nack_only.toml",
    "roce_cnp_under_pfc.toml",
    "roce_feedback_priority.toml",
    "roce_mixed_tcp.toml",
];

const SENDER_HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,kind,mtu_bytes,total_bytes,pacing_interval_ns,first_pacing_time_ns,rto_ns,rate_bps,input_acknowledgment,emitted,emitted_psn,emitted_bytes,emitted_retransmission,emitted_payload,before_next_psn,before_snd_una,before_bytes_emitted,before_packets_emitted,before_credit_quanta,before_rto_deadline_ns,before_pacer,before_next_tick_ns,before_status,after_next_psn,after_snd_una,after_bytes_emitted,after_packets_emitted,after_credit_quanta,after_rto_deadline_ns,after_pacer,after_next_tick_ns,after_status";
const RECEIVER_HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,total_bytes,ack_every_packets,nack_interval_ns,duplicate_ack,ack_size_bytes,cnp_interval_ns,packet_psn,packet_bytes,packet_sent_time_ns,packet_retransmission,packet_ce,action,feedback_acknowledgment,feedback_payload,cnp_sent,cnp_payload,before_expected_psn,before_packets_since_ack,before_last_nack_psn,before_last_nack_time_ns,before_last_cnp_time_ns,after_expected_psn,after_packets_since_ack,after_last_nack_psn,after_last_nack_time_ns,after_last_cnp_time_ns";

fn run(name: &str) -> RunResult {
    let image =
        compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
            .unwrap_or_else(|error| panic!("{name} must lower: {error}"));
    run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name} must run: {error}"))
}

fn records(result: &RunResult) -> &[MechanismTransitionRecord] {
    &result
        .diagnostics
        .as_ref()
        .expect("full observation carries diagnostics")
        .mechanism_transitions
}

fn packet_size(record: &RoceSenderRecord, psn: u64) -> u64 {
    record.mtu_bytes.min(record.total_bytes - psn)
}

fn cost(bytes: u64) -> u128 {
    u128::from(bytes) * 8 * 1_000_000_000
}

/// The reliability state one record's `after` hands to the next record of the same flow. The
/// status is excluded: a CNP or control tick (DCQCN records) can move an armed pacer's prediction.
fn continuation(view: &days_executor::RoceSenderView) -> impl PartialEq + std::fmt::Debug {
    (
        view.next_psn,
        view.snd_una,
        view.bytes_emitted,
        view.packets_emitted,
        view.credit_quanta,
        view.rto_deadline_ns,
        view.pacer,
        view.next_tick_ns,
    )
}

fn check_sender(name: &str, record: &RoceSenderRecord) {
    let (before, after) = (record.before, record.after);
    let now = record.key.time_ns;
    let context = format!("{name}: {record:?}");
    // Common invariants of every view.
    for view in [before, after] {
        assert!(
            view.snd_una <= view.next_psn
                && view.next_psn <= view.bytes_emitted
                && view.bytes_emitted <= record.total_bytes,
            "{context}"
        );
        assert_eq!(
            view.status == GeneratorStatus::Finished,
            view.snd_una == record.total_bytes,
            "{context}"
        );
    }
    let rewound_restart = |time: u64| {
        let first = record.first_pacing_time_ns;
        let interval = record.pacing_interval_ns;
        if time < first {
            first
        } else {
            first + ((time - first) / interval + 1) * interval
        }
    };
    match record.kind {
        RoceSenderKind::Tick => {
            assert_eq!(before.pacer, RocePacerState::Armed, "{context}");
            assert_eq!(before.next_tick_ns, Some(now), "{context}");
            assert_eq!(before.snd_una, after.snd_una, "{context}");
            if before.next_psn >= record.total_bytes {
                // A no-op tick: an ACK moved the next PSN to the end while it was pending.
                assert_eq!(record.emitted, None, "{context}");
                assert_eq!(before.credit_quanta, after.credit_quanta, "{context}");
                assert_eq!(after.pacer, RocePacerState::Parked, "{context}");
                return;
            }
            let rate = record.rate_bps.expect("a crediting tick names its rate");
            let credited =
                before.credit_quanta + u128::from(rate) * u128::from(record.pacing_interval_ns);
            let size = packet_size(record, before.next_psn);
            match record.emitted {
                Some(RoceEmission {
                    psn,
                    bytes,
                    retransmission,
                    ..
                }) => {
                    assert!(credited >= cost(size), "{context}");
                    assert_eq!((psn, bytes), (before.next_psn, size), "{context}");
                    assert_eq!(retransmission, psn < before.bytes_emitted, "{context}");
                    assert_eq!(after.next_psn, psn + bytes, "{context}");
                    assert_eq!(after.credit_quanta, credited - cost(size), "{context}");
                    let high_water = if retransmission {
                        before.bytes_emitted
                    } else {
                        psn + bytes
                    };
                    assert_eq!(after.bytes_emitted, high_water, "{context}");
                    if record.rto_ns != 0 && before.rto_deadline_ns.is_none() {
                        assert_eq!(
                            after.rto_deadline_ns,
                            Some(now + record.rto_ns),
                            "{context}"
                        );
                    } else {
                        assert_eq!(after.rto_deadline_ns, before.rto_deadline_ns, "{context}");
                    }
                }
                None => {
                    assert!(credited < cost(size), "{context}");
                    assert_eq!(after.credit_quanta, credited, "{context}");
                    assert_eq!(after.next_psn, before.next_psn, "{context}");
                }
            }
            match after.pacer {
                RocePacerState::Armed | RocePacerState::Stopped => {
                    assert!(after.next_psn < record.total_bytes, "{context}");
                    assert_eq!(
                        after.next_tick_ns,
                        Some(now + record.pacing_interval_ns),
                        "{context}"
                    );
                }
                RocePacerState::Parked => assert_eq!(after.next_psn, record.total_bytes),
            }
        }
        RoceSenderKind::Ack | RoceSenderKind::Nack => {
            let acknowledgment = record
                .input_acknowledgment
                .expect("feedback names its value");
            let nack = record.kind == RoceSenderKind::Nack;
            let stale = if nack {
                acknowledgment < before.snd_una
            } else {
                acknowledgment <= before.snd_una
            };
            assert_eq!(after.credit_quanta, before.credit_quanta, "{context}");
            if stale {
                assert_eq!(continuation(&before), continuation(&after), "{context}");
                return;
            }
            assert_eq!(after.snd_una, acknowledgment, "{context}");
            let expected_next = if nack {
                acknowledgment
            } else {
                before.next_psn.max(acknowledgment)
            };
            assert_eq!(after.next_psn, expected_next, "{context}");
            let outstanding = after.snd_una < after.bytes_emitted;
            assert_eq!(
                after.rto_deadline_ns,
                (record.rto_ns != 0 && outstanding).then_some(now + record.rto_ns),
                "{context}"
            );
            if before.pacer != RocePacerState::Armed && after.pacer == RocePacerState::Armed {
                assert_eq!(after.next_tick_ns, Some(rewound_restart(now)), "{context}");
            }
        }
        RoceSenderKind::Timeout => {
            assert_eq!(before.rto_deadline_ns, Some(now), "{context}");
            assert_eq!(after.next_psn, before.snd_una, "{context}");
            assert_eq!(
                after.rto_deadline_ns,
                Some(now + record.rto_ns),
                "{context}"
            );
            if before.pacer != RocePacerState::Armed && after.pacer == RocePacerState::Armed {
                assert_eq!(after.next_tick_ns, Some(rewound_restart(now)), "{context}");
            }
        }
    }
}

fn check_receiver(name: &str, record: &RoceReceiverRecord) {
    let (before, after) = (record.before, record.after);
    let context = format!("{name}: {record:?}");
    let in_order = record.packet_psn == before.expected_psn;
    let expected = match record.action {
        RoceReceiverAction::Ack => {
            assert!(in_order, "{context}");
            assert!(
                before.packets_since_ack + 1 >= record.ack_every_packets
                    || after.expected_psn == record.total_bytes,
                "{context}"
            );
            before.expected_psn + record.packet_bytes
        }
        RoceReceiverAction::None if in_order => {
            assert_eq!(
                after.packets_since_ack,
                before.packets_since_ack + 1,
                "{context}"
            );
            before.expected_psn + record.packet_bytes
        }
        RoceReceiverAction::None | RoceReceiverAction::DuplicateAck => {
            assert!(record.packet_psn < before.expected_psn, "{context}");
            assert_eq!(
                record.action == RoceReceiverAction::DuplicateAck,
                record.duplicate_ack,
                "{context}"
            );
            before.expected_psn
        }
        RoceReceiverAction::Nack | RoceReceiverAction::NackSuppressed => {
            assert!(record.packet_psn > before.expected_psn, "{context}");
            let admitted = before.last_nack_psn != Some(before.expected_psn)
                || before
                    .last_nack_time_ns
                    .is_none_or(|last| record.key.time_ns >= last + record.nack_interval_ns);
            assert_eq!(
                record.action == RoceReceiverAction::Nack,
                admitted,
                "{context}"
            );
            before.expected_psn
        }
    };
    assert_eq!(after.expected_psn, expected, "{context}");
    let sends = matches!(
        record.action,
        RoceReceiverAction::Ack | RoceReceiverAction::DuplicateAck | RoceReceiverAction::Nack
    );
    assert_eq!(record.feedback_payload.is_some(), sends, "{context}");
    if sends {
        assert_eq!(after.packets_since_ack, 0, "{context}");
        assert_eq!(record.feedback_acknowledgment, Some(after.expected_psn));
    }
    // The CNP is decided first and allocated first (ordering S1).
    if let (Some(cnp), Some(feedback)) = (record.cnp_payload, record.feedback_payload) {
        assert!(cnp < feedback, "{context}");
    }
    // The notification point: a CNP moves the last-CNP time to now; otherwise it is unchanged.
    if record.cnp_payload.is_some() {
        assert!(record.packet_ce, "{context}");
        assert_eq!(
            after.last_cnp_time_ns,
            Some(record.key.time_ns),
            "{context}"
        );
    } else {
        assert_eq!(after.last_cnp_time_ns, before.last_cnp_time_ns, "{context}");
    }
}

#[test]
fn queue_pair_records_replay_the_go_back_n_semantics() {
    for name in FIXTURES {
        let result = run(name);
        let mut senders = BTreeMap::<FlowId, Vec<RoceSenderRecord>>::new();
        let mut receivers = BTreeMap::<FlowId, Vec<RoceReceiverRecord>>::new();
        for record in records(&result) {
            match record {
                MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(record)) => {
                    senders.entry(record.flow).or_default().push(*record);
                }
                MechanismTransitionRecord::Roce(RoceTransitionRecord::Receiver(record)) => {
                    receivers.entry(record.flow).or_default().push(*record);
                }
                _ => {}
            }
        }
        assert!(!senders.is_empty() && !receivers.is_empty(), "{name}");
        for records in senders.values() {
            for pair in records.windows(2) {
                assert!(
                    pair[0].key < pair[1].key,
                    "{name}: records in EventKey order"
                );
                assert_eq!(
                    continuation(&pair[0].after),
                    continuation(&pair[1].before),
                    "{name}: {:?} then {:?}",
                    pair[0],
                    pair[1]
                );
            }
            for record in records {
                check_sender(name, record);
            }
        }
        for records in receivers.values() {
            for pair in records.windows(2) {
                assert!(pair[0].key < pair[1].key, "{name}");
                assert_eq!(pair[0].after, pair[1].before, "{name}");
            }
            for record in records {
                check_receiver(name, record);
            }
        }
    }
}

/// Ordering S1: when one arrival yields a CNP and an ACK or NACK, the receiver host serves the CNP
/// first, because both enter its FIFO at the same instant and the CNP holds the smaller payload.
#[test]
fn a_cnp_leaves_the_receiver_before_the_ack_of_the_same_arrival() {
    let result = run("roce_cnp_under_pfc.toml");
    let first_departure = result.departures.iter().enumerate().fold(
        BTreeMap::new(),
        |mut first, (index, departure)| {
            first.entry(departure.payload).or_insert(index);
            first
        },
    );
    let mut pairs = 0;
    for record in records(&result) {
        if let MechanismTransitionRecord::Roce(RoceTransitionRecord::Receiver(record)) = record {
            if let (Some(cnp), Some(feedback)) = (record.cnp_payload, record.feedback_payload) {
                assert!(
                    first_departure[&cnp] < first_departure[&feedback],
                    "{record:?}"
                );
                pairs += 1;
            }
        }
    }
    assert!(pairs > 0, "the fixture exercises same-arrival CNP and ACK");
}

#[test]
fn the_csv_writers_emit_the_pinned_schema_one_row_per_record() {
    for name in FIXTURES {
        let result = run(name);
        let records = records(&result);
        let count = |sender: bool| {
            records
                .iter()
                .filter(|record| match record {
                    MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(_)) => sender,
                    MechanismTransitionRecord::Roce(RoceTransitionRecord::Receiver(_)) => !sender,
                    _ => false,
                })
                .count()
        };
        let sender = roce_sender_transitions_csv(records).expect("sender CSV");
        let receiver = roce_receiver_transitions_csv(records).expect("receiver CSV");
        let sender_lines = sender.lines().collect::<Vec<_>>();
        let receiver_lines = receiver.lines().collect::<Vec<_>>();
        assert_eq!(sender_lines[0], SENDER_HEADER, "{name}");
        assert_eq!(receiver_lines[0], RECEIVER_HEADER, "{name}");
        assert_eq!(sender_lines.len() - 1, count(true), "{name}");
        assert_eq!(receiver_lines.len() - 1, count(false), "{name}");
        let columns = |line: &str| line.split(',').count();
        for line in &sender_lines {
            assert_eq!(columns(line), columns(SENDER_HEADER), "{name}: {line}");
        }
        for line in &receiver_lines {
            assert_eq!(columns(line), columns(RECEIVER_HEADER), "{name}: {line}");
        }
        // Every RoCE data packet sent is one emitting tick.
        let emitted = records
            .iter()
            .filter(|record| {
                matches!(record, MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(
                    sender
                )) if sender.emitted.is_some())
            })
            .count();
        let data = result
            .observed_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::RoceData(_)))
            .count();
        assert_eq!(emitted, data, "{name}");
    }
}

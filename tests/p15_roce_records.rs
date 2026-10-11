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
    EventKey, FlowId, GeneratorStatus, MechanismTransitionRecord, NodeId, ObservationMode,
    PacketKind, PfcControlAction, PfcControlTransitionRecord, RoceEmission, RocePacerState,
    RoceReceiverAction, RoceReceiverRecord, RoceSenderKind, RoceSenderRecord, RoceTransitionRecord,
    RunResult, roce_receiver_transitions_csv, roce_sender_transitions_csv,
    run_scalar_with_observations,
};

const FIXTURES: [&str; 9] = [
    "roce_lossless_pfc.toml",
    "roce_gbn_lossy.toml",
    "roce_timeout.toml",
    "roce_nack_only.toml",
    "roce_cnp_under_pfc.toml",
    "roce_feedback_priority.toml",
    "roce_mixed_tcp.toml",
    "hostpfc_incast_lossless.toml",
    "hostpfc_multi_qp_tcp.toml",
];

const SENDER_HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,kind,class_paused,window_blocked,data_class,mtu_bytes,total_bytes,pacing_interval_ns,first_pacing_time_ns,rto_ns,window_bytes,variable_window,maximum_rate_bps,initial_rate_bps,rate_bps,input_acknowledgment,input_ce_echo,emitted,emitted_psn,emitted_bytes,emitted_retransmission,emitted_payload,before_next_psn,before_snd_una,before_bytes_emitted,before_packets_emitted,before_credit_quanta,before_rto_deadline_ns,before_pacer,before_next_tick_ns,before_status,after_next_psn,after_snd_una,after_bytes_emitted,after_packets_emitted,after_credit_quanta,after_rto_deadline_ns,after_pacer,after_next_tick_ns,after_status,congestion_control";
const RECEIVER_HEADER: &str = "time_ns,event_phase,event_origin_node,event_origin_sequence,node_id,flow_id,total_bytes,ack_every_packets,nack_interval_ns,duplicate_ack,ack_size_bytes,packet_psn,packet_bytes,packet_sent_time_ns,packet_retransmission,packet_ce,action,feedback_acknowledgment,feedback_payload,feedback_ce_echo,before_expected_psn,before_packets_since_ack,before_last_nack_psn,before_last_nack_time_ns,after_expected_psn,after_packets_since_ack,after_last_nack_psn,after_last_nack_time_ns";

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

/// The reliability state one record's `after` hands to the next record of the same flow, status
/// included: nothing recomputes a pair's prediction between its transitions (P16 ruling D2; in P15
/// a CNP or control tick could).
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
        view.status,
    )
}

fn check_sender(name: &str, record: &RoceSenderRecord) {
    let (before, after) = (record.before, record.after);
    let now = record.key.time_ns;
    let context = format!("{name}: {record:?}");
    assert!(record.data_class < 8, "{context}");
    assert!(
        !record.class_paused || record.kind == RoceSenderKind::Tick,
        "only a tick finds its class paused (Amendment 1): {context}"
    );
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
        RoceSenderKind::Tick if record.class_paused => {
            // Amendment 1: the tick sends nothing, adds no credit, and parks.
            assert_eq!(before.pacer, RocePacerState::Armed, "{context}");
            assert_eq!(before.next_tick_ns, Some(now), "{context}");
            assert_eq!(
                (record.emitted, record.rate_bps, record.input_acknowledgment),
                (None, None, None),
                "{context}"
            );
            assert_eq!(after.pacer, RocePacerState::Parked, "{context}");
            assert_eq!(
                (
                    after.next_psn,
                    after.snd_una,
                    after.bytes_emitted,
                    after.packets_emitted,
                    after.credit_quanta,
                    after.rto_deadline_ns
                ),
                (
                    before.next_psn,
                    before.snd_una,
                    before.bytes_emitted,
                    before.packets_emitted,
                    before.credit_quanta,
                    before.rto_deadline_ns
                ),
                "{context}"
            );
        }
        RoceSenderKind::Resume => {
            // Amendment 2: a pause-parked pacer with data to send restarts on its next grid
            // point strictly after the RESUME, or stops beyond the stop time.
            assert_eq!(
                (record.emitted, record.rate_bps, record.input_acknowledgment),
                (None, None, None),
                "{context}"
            );
            assert_eq!(before.pacer, RocePacerState::Parked, "{context}");
            assert!(
                before.next_psn < record.total_bytes && before.snd_una < record.total_bytes,
                "{context}"
            );
            assert!(
                matches!(after.pacer, RocePacerState::Armed | RocePacerState::Stopped),
                "{context}"
            );
            assert_eq!(after.next_tick_ns, Some(rewound_restart(now)), "{context}");
            assert_eq!(
                (
                    after.next_psn,
                    after.snd_una,
                    after.bytes_emitted,
                    after.packets_emitted,
                    after.credit_quanta,
                    after.rto_deadline_ns
                ),
                (
                    before.next_psn,
                    before.snd_una,
                    before.bytes_emitted,
                    before.packets_emitted,
                    before.credit_quanta,
                    before.rto_deadline_ns
                ),
                "{context}"
            );
        }
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
                || before.last_nack_time_ns.is_none_or(|last| {
                    // A repeat's earliest time beyond u64::MAX is never reached.
                    last.checked_add(record.nack_interval_ns)
                        .is_some_and(|earliest| record.key.time_ns >= earliest)
                });
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
    // P16 rulings D4 and D5: an ACK or NACK echoes the CE mark of the packet that triggered it;
    // the receiver sends no CNP.
    assert_eq!(
        record.feedback_ce_echo,
        sends.then_some(record.packet_ce),
        "{context}"
    );
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
fn an_ack_or_nack_carries_the_echo_its_receiver_recorded() {
    let result = run("roce_cnp_under_pfc.toml");
    let packets = result
        .observed_packets
        .iter()
        .map(|packet| (packet.id, packet.kind))
        .collect::<BTreeMap<_, _>>();
    let mut echoes = 0;
    for record in records(&result) {
        if let MechanismTransitionRecord::Roce(RoceTransitionRecord::Receiver(record)) = record {
            let Some(payload) = record.feedback_payload else {
                continue;
            };
            let (PacketKind::RoceAck(header) | PacketKind::RoceNack(header)) = packets[&payload]
            else {
                panic!("{record:?}: the feedback is an ACK or NACK");
            };
            assert_eq!(Some(header.ce_echo), record.feedback_ce_echo, "{record:?}");
            echoes += usize::from(header.ce_echo);
        }
    }
    assert!(echoes > 0, "the fixture exercises echoing ACKs");
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

/// Whether `class` is paused at `node` just before `key`, per the PFC control log: the last
/// control of (node, class) before `key` leaves a nonempty controller set. Controls arrive in
/// phase 0, before any tick (phase 1) at the same instant.
fn paused_before(
    controls: &[PfcControlTransitionRecord],
    node: NodeId,
    class: u8,
    key: EventKey,
) -> bool {
    controls
        .iter()
        .rfind(|control| control.node == node && control.priority == class && control.key < key)
        .is_some_and(|control| !control.after_controllers.is_empty())
}

/// Schema Amendment 3 and the `class_paused` writer contract (`evidence/P15/leanguard.md` §12):
/// every tick records whether its queue pair's data class is paused at its host, a no-op tick
/// included; every `resume` row shares the key of a host RESUME of its class at its node; and every
/// such RESUME restarts every restartable pause-parked queue pair of that class there.
#[test]
fn pause_and_resume_rows_agree_with_the_host_pfc_log() {
    let mut paused_ticks = 0;
    let mut resumes = 0;
    for name in FIXTURES {
        let result = run(name);
        let records = records(&result);
        let mut controls = records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::PfcControl(control) => Some(control.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        controls.sort_by_key(|control| control.key);
        let senders = records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender)) => {
                    Some(*sender)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for sender in &senders {
            let context = format!("{name}: {sender:?}");
            match sender.kind {
                RoceSenderKind::Tick => {
                    assert_eq!(
                        sender.class_paused,
                        paused_before(&controls, sender.node, sender.data_class, sender.key),
                        "{context}"
                    );
                    paused_ticks += usize::from(sender.class_paused);
                }
                RoceSenderKind::Resume => {
                    assert!(
                        controls.iter().any(|control| control.key == sender.key
                            && control.node == sender.node
                            && control.priority == sender.data_class
                            && control.action == PfcControlAction::Resume
                            && control.after_controllers.is_empty()),
                        "a resume row needs a host RESUME of its class: {context}"
                    );
                    resumes += 1;
                }
                _ => {}
            }
        }
        // Completeness: at each RESUME that unpauses (node, class), every queue pair of that
        // class at that node whose latest earlier row leaves it parked and restartable has a
        // resume row at the RESUME's key.
        for control in controls.iter().filter(|control| {
            control.action == PfcControlAction::Resume
                && control.after_controllers.is_empty()
                && !control.before_controllers.is_empty()
        }) {
            let mut latest = BTreeMap::<FlowId, RoceSenderRecord>::new();
            for sender in senders.iter().filter(|sender| {
                sender.node == control.node
                    && sender.data_class == control.priority
                    && sender.key < control.key
            }) {
                latest.insert(sender.flow, *sender);
            }
            for (flow, last) in latest {
                let restartable = last.after.pacer == RocePacerState::Parked
                    && last.after.next_psn < last.total_bytes
                    && last.after.snd_una < last.total_bytes;
                let resumed = senders.iter().any(|sender| {
                    sender.flow == flow
                        && sender.key == control.key
                        && sender.kind == RoceSenderKind::Resume
                });
                assert_eq!(
                    resumed, restartable,
                    "{name}: flow {flow:?} at {control:?} after {last:?}"
                );
            }
        }
    }
    assert!(
        paused_ticks > 0 && resumes > 0,
        "the host-PFC fixture pauses and resumes"
    );
}

/// Amendment 2: rows share an event key only as `resume` rows of distinct flows at one node, in
/// ascending `flow_id`. The multi-pair fixture's RESUMEs restart several pairs at one key, so the
/// shared-key case is exercised, not passed vacuously (host-PFC review M1).
#[test]
fn only_resume_rows_share_an_event_key() {
    let result = run("hostpfc_multi_qp_tcp.toml");
    let csv = roce_sender_transitions_csv(records(&result)).expect("sender CSV");
    let rows = csv
        .lines()
        .skip(1)
        .map(|line| line.split(',').map(str::to_owned).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut shared = 0;
    for pair in rows.windows(2) {
        if pair[0][..4] == pair[1][..4] {
            shared += 1;
            assert_eq!(
                (pair[0][6].as_str(), pair[1][6].as_str()),
                ("resume", "resume")
            );
            assert_eq!(pair[0][4], pair[1][4], "one node");
            let flow = |row: &Vec<String>| row[5].parse::<u64>().expect("flow id");
            assert!(flow(&pair[0]) < flow(&pair[1]), "{pair:?}");
        }
    }
    assert!(
        shared > 0,
        "no two sender rows share an event key: the shared-key case went unexercised"
    );
}

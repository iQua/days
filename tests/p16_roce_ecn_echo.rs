//! P16 D1 rulings D4 and D5: a queue pair's receiver echoes ECN on its ACKs and NACKs and sends no
//! CNP, and the sender's Mellanox-form controller takes one feedback per echoing ACK or NACK that
//! reaches it before the pair completes (`days-gpu/evidence/P16/dcqcn-design.md` §3).

use std::collections::BTreeSet;
use std::path::Path;

use days::scenario::compile_config;

use days_executor::{
    DcqcnTransitionKind, MechanismTransitionRecord, ObservationMode, PacketKind, RoceSenderKind,
    RoceTransitionRecord, RunResult, run_scalar_with_observations,
};

/// The P15 queue-pair fixtures whose ACKs see CE (`evidence/P16/dcqcn-impl/
/// ce-echo-census-main-9ff20ea.txt`).
const ECHOING: [&str; 6] = [
    "roce_cnp_under_pfc.toml",
    "roce_feedback_priority.toml",
    "roce_gbn_lossy.toml",
    "roce_mixed_tcp.toml",
    "roce_timeout.toml",
    "roce_nack_only.toml",
];

fn run(name: &str) -> RunResult {
    let image =
        compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
            .unwrap_or_else(|error| panic!("{name} must lower: {error}"));
    run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name} must run: {error}"))
}

fn dcqcn_feedback_rows(result: &RunResult) -> usize {
    result
        .diagnostics
        .as_ref()
        .expect("full observation")
        .mechanism_transitions
        .iter()
        .filter(|record| {
            matches!(record, MechanismTransitionRecord::Dcqcn(row)
                if row.kind == DcqcnTransitionKind::Feedback)
        })
        .count()
}

#[test]
fn queue_pairs_send_no_cnp_and_react_to_echoing_acks() {
    for name in ECHOING {
        let result = run(name);
        let cnps = result
            .observed_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)))
            .count();
        assert_eq!(cnps, 0, "{name}: a queue pair's receiver sends no CNP");
        assert!(
            dcqcn_feedback_rows(&result) > 0,
            "{name}: echoing ACKs reach the controller"
        );
    }
}

/// The join the LeanGuard RoCE checker certifies: the DCQCN feedback rows are exactly the sender
/// ACK and NACK rows that carry an echo and leave the pair incomplete, at the same event key.
#[test]
fn feedback_rows_are_exactly_the_incomplete_echoing_acks() {
    for name in ECHOING {
        let result = run(name);
        let records = &result
            .diagnostics
            .as_ref()
            .expect("full")
            .mechanism_transitions;
        let feedback = records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Dcqcn(row)
                    if row.kind == DcqcnTransitionKind::Feedback =>
                {
                    Some((row.key, row.flow))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let echoing = records
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(row))
                    if matches!(row.kind, RoceSenderKind::Ack | RoceSenderKind::Nack)
                        && row.input_ce_echo == Some(true)
                        && row.after.snd_una < row.total_bytes =>
                {
                    Some((row.key, row.flow))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let unique = feedback.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(
            unique.len(),
            feedback.len(),
            "{name}: one feedback row per ACK"
        );
        assert_eq!(
            unique,
            echoing.iter().copied().collect::<BTreeSet<_>>(),
            "{name}: feedback rows are the incomplete echoing ACKs and NACKs"
        );
        assert!(!echoing.is_empty(), "{name}");
    }
}

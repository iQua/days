//! P15 lane R1: the `configs/p15` RoCE queue-pair fixtures run on Scalar and CPU.
//!
//! Each fixture is pinned two ways:
//!
//! 1. **Mechanism contract**: the counts that make the fixture worth running (Go-back-N
//!    retransmissions, NACKs, CNPs, PFC frames, drops, completions or stalls) are asserted, so an
//!    edit that stops a fixture exercising its mechanism fails here.
//! 2. **Cross-backend identity**: Scalar and CPU at worker counts 1 to 4 return byte-identical
//!    full-observation `RunResult`s. Device backends refuse queue pairs in this phase.

use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, FlowGeneratorKind, GeneratorStatus, MechanismTransitionRecord, ObservationMode,
    PacketKind, RoceSenderKind, RoceTransitionRecord, RunResult, SimulationImage,
    run_cpu_with_observations, run_scalar_with_observations,
};

fn lower(name: &str) -> SimulationImage {
    compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
}

/// The Scalar full-observation result, after checking that every CPU worker count matches it.
fn run_identical(name: &str) -> RunResult {
    let image = lower(name);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name}: Scalar run failed: {error}"));
    for workers in 1..=4 {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("{name}: CPU run with {workers} workers failed: {error}"));
        assert!(
            cpu.result == scalar,
            "{name}: CPU with {workers} workers differs from Scalar"
        );
    }
    scalar
}

#[derive(Debug, Default)]
struct Contract {
    queue_pairs: usize,
    finished_pairs: usize,
    complete_receivers: usize,
    fresh_data: usize,
    retransmissions: usize,
    acks: usize,
    nacks: usize,
    cnps: usize,
    /// PFC pause and resume transitions (the frames are switch-sourced control, not observed).
    pfc_controls: usize,
    timeouts: usize,
    dropped: u128,
}

fn contract(result: &RunResult) -> Contract {
    let mut contract = Contract {
        dropped: result.summary.dropped_packets,
        ..Contract::default()
    };
    for generator in result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
    {
        if let FlowGeneratorKind::Roce(roce) = generator.kind {
            contract.queue_pairs += 1;
            if generator.next_emission.status == GeneratorStatus::Finished {
                assert_eq!(roce.snd_una, roce.pacer.total_bytes);
                contract.finished_pairs += 1;
            }
        }
    }
    for receiver in result
        .host_states
        .iter()
        .filter_map(|state| state.roce_receivers.as_deref())
        .flatten()
    {
        if receiver.expected_psn == receiver.total_bytes {
            contract.complete_receivers += 1;
        }
    }
    for packet in &result.observed_packets {
        match packet.kind {
            PacketKind::RoceData(header) if header.retransmission => contract.retransmissions += 1,
            PacketKind::RoceData(_) => contract.fresh_data += 1,
            PacketKind::RoceAck(_) => contract.acks += 1,
            PacketKind::RoceNack(_) => contract.nacks += 1,
            PacketKind::DcqcnCnp(_) => contract.cnps += 1,
            _ => {}
        }
    }
    for record in &result
        .diagnostics
        .as_ref()
        .expect("full observation carries diagnostics")
        .mechanism_transitions
    {
        match record {
            MechanismTransitionRecord::PfcControl(_) => contract.pfc_controls += 1,
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender))
                if sender.kind == RoceSenderKind::Timeout =>
            {
                contract.timeouts += 1;
            }
            _ => {}
        }
    }
    contract
}

#[test]
fn lossless_pfc_completes_without_loss_or_recovery() {
    let result = run_identical("roce_lossless_pfc.toml");
    let contract = contract(&result);
    assert_eq!(contract.queue_pairs, 2, "{contract:?}");
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    assert_eq!(contract.complete_receivers, 2, "{contract:?}");
    assert_eq!(contract.fresh_data, 400, "{contract:?}");
    assert_eq!(
        (
            contract.dropped,
            contract.nacks,
            contract.retransmissions,
            contract.timeouts
        ),
        (0, 0, 0, 0),
        "{contract:?}"
    );
    assert_eq!(
        contract.acks, 400,
        "one ACK per in-order packet: {contract:?}"
    );
    assert!(contract.pfc_controls > 0, "{contract:?}");
}

#[test]
fn lossy_go_back_n_recovers_every_loss_by_nack() {
    let result = run_identical("roce_gbn_lossy.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    assert_eq!(contract.complete_receivers, 2, "{contract:?}");
    assert!(contract.dropped > 0, "{contract:?}");
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
}

#[test]
fn timeout_arm_completes_through_retransmission_timeouts() {
    let result = run_identical("roce_timeout.toml");
    let contract = contract(&result);
    assert_eq!(
        (contract.queue_pairs, contract.finished_pairs),
        (1, 1),
        "{contract:?}"
    );
    assert!(contract.timeouts > 0, "{contract:?}");
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
}

#[test]
fn nack_only_profile_has_no_timeout_and_can_stall() {
    let result = run_identical("roce_nack_only.toml");
    let contract = contract(&result);
    assert!(contract.nacks > 0, "{contract:?}");
    assert_eq!(contract.timeouts, 0, "{contract:?}");
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.kind != days_executor::EventKind::RetransmissionTimeout),
        "the timeout is off"
    );
    // roce_timeout.toml is this scenario with the timeout on, and completes.
    assert_eq!(
        (
            contract.queue_pairs,
            contract.finished_pairs,
            contract.complete_receivers
        ),
        (1, 0, 0),
        "with the timeout off an unrepaired loss stalls the queue pair: {contract:?}"
    );
    let stalled = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .find_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => Some((generator.next_emission.status, roce)),
            _ => None,
        })
        .expect("the fixture has a queue pair");
    assert_eq!(stalled.0, GeneratorStatus::Blocked);
    assert!(!stalled.1.pacer_armed, "the stalled pair's pacer is parked");
    assert!(stalled.1.snd_una < stalled.1.pacer.total_bytes);
}

#[test]
fn cnps_under_pfc_complete_every_queue_pair() {
    let result = run_identical("roce_cnp_under_pfc.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.cnps > 0, "{contract:?}");
    assert!(contract.pfc_controls > 0, "{contract:?}");
    assert_eq!(contract.dropped, 0, "{contract:?}");
}

#[test]
fn a_separate_feedback_class_changes_the_run() {
    let shared = run_identical("roce_cnp_under_pfc.toml");
    let separate = run_identical("roce_feedback_priority.toml");
    let contract = contract(&separate);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.cnps > 0, "{contract:?}");
    assert_ne!(
        shared.departures, separate.departures,
        "feedback on an unpaused class must change the schedule"
    );
}

#[test]
fn tcp_and_queue_pairs_share_a_bottleneck() {
    let result = run_identical("roce_mixed_tcp.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    let tcp_done = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => Some(tcp.highest_ack == tcp.total_bytes),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tcp_done, [true]);
}

/// FNV-1a64 over the pretty `Debug` rendering, the `result_fnv1a64` the `days` CLI prints.
fn fingerprint(value: &impl std::fmt::Debug) -> (u64, u64) {
    let text = format!("{value:#?}");
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (text.len() as u64, hash)
}

/// The Scalar summary-mode result of a fixture: the complete state every backend must return.
fn scalar_anchor(name: &str) -> (u64, u64) {
    let image = lower(name);
    let result =
        days_executor::run_scalar_with_observations(&image, None, ObservationMode::Summary)
            .unwrap_or_else(|error| panic!("{name}: Scalar run failed: {error}"));
    fingerprint(&result)
}

/// Frozen at authoring (`b448d08`, 2026-10-01, sim; the `days` CLI printed the same values): the
/// anchors the device lane proves Metal and CUDA against.
const ANCHORS: [(&str, u64, u64); 7] = [
    ("roce_lossless_pfc.toml", 45_710, 0x7e1f_a3a8_7997_030c),
    ("roce_gbn_lossy.toml", 46_238, 0x4c94_e615_e09a_e734),
    ("roce_timeout.toml", 34_578, 0x3488_8b67_3127_c7cc),
    ("roce_nack_only.toml", 34_559, 0x9715_1362_f757_31bf),
    ("roce_cnp_under_pfc.toml", 58_280, 0x70b0_1e3d_c0d6_15a8),
    ("roce_feedback_priority.toml", 58_323, 0x6b64_e238_d4d0_3c96),
    ("roce_mixed_tcp.toml", 52_398, 0x7612_b5cd_28d5_5949),
];

#[test]
fn p15_fixtures_match_their_frozen_anchors() {
    for (name, bytes, fnv1a64) in ANCHORS {
        let actual = scalar_anchor(name);
        assert_eq!(
            actual,
            (bytes, fnv1a64),
            "{name}: frozen anchor moved (got bytes={} fnv1a64={:016x})",
            actual.0,
            actual.1
        );
    }
}

/// The HPCC cross-check fixture's anchor; ignored in the default matrix (64 queue pairs at 100
/// Gbps; run it in release: `cargo test --release -p days --test p15_roce_fixtures -- --ignored`).
#[test]
#[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
fn hpcc_fixture_matches_its_frozen_anchor() {
    assert_eq!(
        scalar_anchor("hpcc_incast64_dragonfly.toml"),
        (813_526, 0x1043_7719_b6f6_f177)
    );
}

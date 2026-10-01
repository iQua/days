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
    CpuConfig, FlowGeneratorKind, GeneratorStatus, ObservationMode, PacketKind, RunResult,
    SimulationImage, run_cpu_with_observations, run_scalar_with_observations,
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
    pfc_frames: usize,
    dropped: u128,
}

fn contract(result: &RunResult) -> Contract {
    let mut contract = Contract {
        dropped: result.summary.dropped_packets,
        ..Contract::default()
    };
    for generator in result.host_states.iter().flat_map(|state| &state.generators) {
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
            PacketKind::Pfc(_) => contract.pfc_frames += 1,
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
        (contract.dropped, contract.nacks, contract.retransmissions),
        (0, 0, 0),
        "{contract:?}"
    );
    assert!(contract.acks >= 400, "{contract:?}");
    assert!(contract.pfc_frames > 0, "{contract:?}");
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
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    assert!(contract.dropped > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
}

#[test]
fn nack_only_profile_has_no_timeout_and_can_stall() {
    let result = run_identical("roce_nack_only.toml");
    let contract = contract(&result);
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.kind != days_executor::EventKind::RetransmissionTimeout),
        "the timeout is off"
    );
    assert!(
        contract.finished_pairs < contract.queue_pairs,
        "a tail loss stalls a queue pair with the timeout off: {contract:?}"
    );
}

#[test]
fn cnps_under_pfc_complete_every_queue_pair() {
    let result = run_identical("roce_cnp_under_pfc.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.cnps > 0, "{contract:?}");
    assert!(contract.pfc_frames > 0, "{contract:?}");
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

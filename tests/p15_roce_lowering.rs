//! P15 lane R1: lowering of RoCE queue pairs (`flow_type = "RoCE"`).
//!
//! A queue pair is a reliable DCQCN flow: the `[flow.traffic.dcqcn]` table configures its exact
//! controller and pacer, and `[flow.traffic.roce]` its Go-back-N reliability (design note
//! `days-gpu/evidence/P15/qp-design.md` §6, rulings D5, D7 and D8). These tests pin the lowered
//! records, the defaults, the refusals and the canonical identity of every `configs/p15` fixture.

use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{
    EventKind, FlowGeneratorKind, GeneratorStatus, PacketKind, PayloadId, RoceGenerator,
    RoceReceiverState, SimulationImage,
};
use tempfile::TempDir;

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

/// Every `configs/p15` fixture. Each must lower before any test names its contract.
const FIXTURES: [&str; 8] = [
    "roce_lossless_pfc.toml",
    "roce_gbn_lossy.toml",
    "roce_timeout.toml",
    "roce_nack_only.toml",
    "roce_cnp_under_pfc.toml",
    "roce_feedback_priority.toml",
    "roce_mixed_tcp.toml",
    "hpcc_incast64_dragonfly.toml",
];

fn lower_fixture(name: &str) -> SimulationImage {
    compile_config(repo_path(&format!("configs/p15/{name}")))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
}

fn queue_pairs(image: &SimulationImage) -> Vec<(days_executor::FlowId, RoceGenerator)> {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => Some((generator.flow, roce)),
            _ => None,
        })
        .collect()
}

fn receiver(image: &SimulationImage, flow: days_executor::FlowId) -> RoceReceiverState {
    let descriptor = &image.flows[flow.0 as usize];
    let target = &image.nodes[descriptor.target.0 as usize];
    let receivers = image.host_states[target.state_slot as usize]
        .roce_receivers
        .as_deref()
        .expect("a QP's target host holds RoCE receivers");
    *receivers
        .iter()
        .find(|receiver| receiver.np.flow == flow)
        .expect("the QP has a receiver on its target host")
}

fn packet(image: &SimulationImage, id: PayloadId) -> days_executor::PacketDescriptor {
    *image
        .initial_packets
        .iter()
        .find(|packet| packet.id == id)
        .expect("the token is resident")
}

#[test]
fn every_p15_fixture_lowers_to_queue_pairs_with_receivers_and_two_tokens() {
    for name in FIXTURES {
        let image = lower_fixture(name);
        let pairs = queue_pairs(&image);
        assert!(!pairs.is_empty(), "{name} has queue pairs");
        for (flow, roce) in pairs {
            let descriptor = &image.flows[flow.0 as usize];
            let generator = image
                .host_states
                .iter()
                .flat_map(|state| &state.generators)
                .find(|generator| generator.flow == flow)
                .unwrap();
            assert_eq!(
                (roce.next_psn, roce.snd_una, roce.rto_deadline_ns),
                (0, 0, 0),
                "{name}"
            );
            assert!(roce.pacer_armed, "{name}: the pacer starts armed");
            assert_eq!(roce.pacer.credit_quanta, 0);
            // The status predicts the first tick (validation checks the exact credit rule).
            assert!(
                matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                ),
                "{name}"
            );
            assert_eq!(generator.next_emission.payload, roce.pacing_timer_payload);
            assert_eq!(
                generator.next_emission.departure_time_ns,
                roce.pacer.first_pacing_time_ns
            );
            assert_eq!(
                packet(&image, roce.pacing_timer_payload).kind,
                PacketKind::RocePacingTimer
            );
            assert_eq!(
                packet(&image, roce.control_timer_payload).kind,
                PacketKind::DcqcnControlTimer
            );
            for token in [roce.pacing_timer_payload, roce.control_timer_payload] {
                let token = packet(&image, token);
                assert_eq!((token.flow, token.size_bytes), (flow, 0), "{name}");
            }
            let pacing_events = image
                .initial_events
                .iter()
                .filter(|event| {
                    event.kind == EventKind::PacingTimer
                        && event.payload == roce.pacing_timer_payload
                })
                .count();
            assert_eq!(pacing_events, 1, "{name}: one pending pacing tick");
            let receiver = receiver(&image, flow);
            assert_eq!(receiver.total_bytes, roce.pacer.total_bytes);
            assert_eq!((receiver.expected_psn, receiver.packets_since_ack), (0, 0));
            assert_eq!(receiver.last_nack, None);
            assert_eq!(receiver.np.last_cnp_time_ns, None);
            assert_eq!(
                descriptor.feedback_priority,
                if name == "roce_feedback_priority.toml" {
                    1
                } else if name == "hpcc_incast64_dragonfly.toml" {
                    0
                } else {
                    descriptor.priority
                },
                "{name}"
            );
        }
    }
}

#[test]
fn queue_pair_keys_lower_with_their_defaults_and_the_hpcc_profile() {
    let lossless = lower_fixture("roce_lossless_pfc.toml");
    let (flow, roce) = queue_pairs(&lossless)[0];
    assert_eq!(roce.rto_ns, 200_000);
    assert_eq!(roce.pacer.mtu_bytes, 1_000);
    assert_eq!(roce.pacer.total_bytes, 200_000);
    assert_eq!(roce.pacer.pacing_interval_ns, 1_000);
    assert_eq!(roce.controller.config.control_interval_ns, 50_000);
    let defaults = receiver(&lossless, flow);
    assert_eq!(defaults.ack_every_packets, 1);
    assert_eq!(defaults.nack_interval_ns, 500_000);
    assert_eq!(defaults.ack_size_bytes, 64);
    assert!(defaults.duplicate_ack);
    assert_eq!(defaults.np.cnp_size_bytes, 64);
    assert_eq!(defaults.np.cnp_interval_ns, 10_000);

    let hpcc = lower_fixture("hpcc_incast64_dragonfly.toml");
    let pairs = queue_pairs(&hpcc);
    assert_eq!(pairs.len(), 64);
    for (flow, roce) in pairs {
        assert_eq!(roce.rto_ns, 0, "HPCC has no retransmission timeout");
        let receiver = receiver(&hpcc, flow);
        assert!(!receiver.duplicate_ack);
        assert_eq!(receiver.ack_size_bytes, 60);
        let descriptor = &hpcc.flows[flow.0 as usize];
        assert_eq!(descriptor.target.0 % 6, 0);
        assert_eq!((descriptor.priority, descriptor.feedback_priority), (3, 0));
    }
    // HPCC host n maps to Days host 6 (n - 1): the receiver is host 0 and the senders are
    // hosts 6, 12, ..., 384, all under router 0.
    let mut senders = hpcc
        .flows
        .iter()
        .map(|flow| {
            let source = &hpcc.nodes[flow.source.0 as usize];
            let target = &hpcc.nodes[flow.target.0 as usize];
            (source.state_slot, target.state_slot)
        })
        .collect::<Vec<_>>();
    senders.sort_unstable();
    senders.dedup();
    assert_eq!(senders.len(), 64);
}

fn write(directory: &TempDir, name: &str, text: &str) -> PathBuf {
    let path = directory.path().join(name);
    fs::write(&path, text).unwrap();
    path
}

fn lossless_text() -> String {
    fs::read_to_string(repo_path("configs/p15/roce_lossless_pfc.toml")).unwrap()
}

#[test]
fn queue_pair_options_are_refused_where_they_do_not_apply() {
    let directory = TempDir::new().unwrap();
    let base = lossless_text();
    let refused = |text: String, expected: &str| {
        let path = write(&directory, "refused.toml", &text);
        let error = compile_config(&path).unwrap_err().to_string();
        assert!(
            error.contains(expected),
            "expected `{expected}` in: {error}"
        );
    };
    refused(
        base.replace("retransmit_timeout_ns = 200000\n", ""),
        "retransmit_timeout_ns",
    );
    refused(
        base.replacen("[flow.traffic.roce]\n", "", 1).replacen(
            "retransmit_timeout_ns = 200000\n",
            "",
            1,
        ),
        "[flow.traffic.roce]",
    );
    refused(
        base.replacen(
            "retransmit_timeout_ns = 200000\n",
            "retransmit_timeout_ns = 200000\nack_every_packets = 0\n",
            1,
        ),
        "ack_every_packets",
    );
    refused(
        base.replacen(
            "retransmit_timeout_ns = 200000\n",
            "retransmit_timeout_ns = 200000\nack_size_bytes = 0\n",
            1,
        ),
        "ack_size_bytes",
    );
    refused(
        base.replacen(
            "retransmit_timeout_ns = 200000\n",
            "retransmit_timeout_ns = 200000\nduplicate_ack = false\n",
            1,
        ),
        "duplicate_ack",
    );
    refused(
        base.replacen(
            "retransmit_timeout_ns = 200000\n",
            "retransmit_timeout_ns = 200000\nfeedback_priority = 8\n",
            1,
        ),
        "feedback_priority",
    );
    refused(
        base.replacen(
            "increase_byte_threshold = 100000\n",
            "increase_byte_threshold = 100000\ncnp_priority = 0\n",
            1,
        ),
        "cnp_priority",
    );
    refused(
        base.replacen("flow_type = \"RoCE\"", "flow_type = \"DCQCN\"", 1),
        "RoCE options",
    );
    refused(
        base.replacen(
            "[flow.traffic.roce]\n",
            "[flow.traffic.tcp]\ncc_algorithm = \"Reno\"\n\n[flow.traffic.roce]\n",
            1,
        ),
        "TCP options",
    );
    refused(
        base.replacen("size = 200000\n", "duration = 0.001\n", 1),
        "size",
    );
}

#[test]
fn the_rto_off_profile_lowers_only_explicitly() {
    let directory = TempDir::new().unwrap();
    let base = lossless_text();
    let off = base.replace(
        "retransmit_timeout_ns = 200000\n",
        "retransmit_timeout_ns = 0\nduplicate_ack = false\n",
    );
    let image = compile_config(write(&directory, "off.toml", &off)).expect("RTO off lowers");
    for (flow, roce) in queue_pairs(&image) {
        assert_eq!(roce.rto_ns, 0);
        assert!(!receiver(&image, flow).duplicate_ack);
    }
}

/// Queue-pair keys are interned by an order-isomorphic ordinal, so flow identity depends only on
/// the scenario's content: reordering two flows that differ only in their RoCE keys lowers to the
/// same image.
#[test]
fn queue_pair_identity_does_not_depend_on_file_order() {
    let directory = TempDir::new().unwrap();
    let base = lossless_text();
    let split = base
        .find("\n[[flow]]")
        .expect("the fixture lists its flows after the header");
    let (header, flows) = base.split_at(split);
    let mut blocks = flows
        .split("\n[[flow]]")
        .filter(|block| !block.trim().is_empty())
        .map(|block| format!("\n[[flow]]{block}"))
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), 2);
    blocks[1] = blocks[1].replace(
        "retransmit_timeout_ns = 200000",
        "retransmit_timeout_ns = 300000",
    );
    let forward = format!("{header}{}{}", blocks[0], blocks[1]);
    let reversed = format!("{header}{}{}", blocks[1], blocks[0]);
    let forward = compile_config(write(&directory, "forward.toml", &forward)).unwrap();
    let reversed = compile_config(write(&directory, "reversed.toml", &reversed)).unwrap();
    assert_eq!(format!("{forward:#?}"), format!("{reversed:#?}"));
}

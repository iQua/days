//! P16 ecnbytes (user ruling, Oct 8): the SimAI fidelity lowers SimAI.conf's ECN thresholds and
//! queue capacity in bytes, as SimAI and real switches count queue depth.
//!
//! The ECN step sits at the K-ramp midpoint, `(KMIN + KMAX) / 2` KB: 1,000,000 B on a 100 Gb/s
//! egress and 2,000,000 B on a 400 Gb/s one, against SimAI's 32 MiB `BUFFER_SIZE`. Counting 60 B
//! ACKs as whole packets made marking start at about 748 KB of real queue on the MoE smoke
//! (`days-gpu/evidence/P16/moepin/report.md`, round 2).

#[path = "support/aicb.rs"]
mod aicb;

use days_executor::{DropMarkPolicy, EcnThresholdPolicy, QueueDepthUnit, SimulationImage};

const G: u64 = 1_000_000_000;

/// Every switch queue's ECN policy, by its egress link's rate: `(rate, policy, queues)`.
fn ecn_policies(image: &SimulationImage) -> Vec<(u64, EcnThresholdPolicy, usize)> {
    let mut policies = std::collections::BTreeMap::<(u64, u64, u64, u8), usize>::new();
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        let DropMarkPolicy::EcnThreshold(policy) = queue.drop_mark else {
            panic!("an AICB switch queue marks by an ECN threshold")
        };
        let link = queue
            .egress_link
            .expect("a switch queue has an egress link");
        let rate = image.links[link.0 as usize].rate_bps;
        *policies
            .entry((rate, policy.capacity, policy.threshold, policy.unit as u8))
            .or_default() += 1;
    }
    policies
        .into_iter()
        .map(|((rate, capacity, threshold, unit), queues)| {
            let unit = if unit == QueueDepthUnit::Bytes as u8 {
                QueueDepthUnit::Bytes
            } else {
                QueueDepthUnit::Packets
            };
            (
                rate,
                EcnThresholdPolicy {
                    unit,
                    capacity,
                    threshold,
                },
                queues,
            )
        })
        .collect()
}

fn bytes(threshold: u64) -> EcnThresholdPolicy {
    EcnThresholdPolicy {
        unit: QueueDepthUnit::Bytes,
        capacity: 33_554_432,
        threshold,
    }
}

fn assert_byte_thresholds(name: &str) {
    let image = aicb::lower(name);
    let policies = ecn_policies(&image);
    assert!(!policies.is_empty(), "{name}: switch queues");
    for (rate, policy, queues) in policies {
        let expected = match rate {
            rate if rate == 100 * G => bytes(1_000_000),
            rate if rate == 400 * G => bytes(2_000_000),
            other => panic!("{name}: unexpected egress rate {other}"),
        };
        assert_eq!(policy, expected, "{name}: {queues} queues at {rate} b/s");
    }
}

#[test]
fn b4_marks_by_queue_bytes() {
    assert_byte_thresholds("b4-simai.toml");
}

#[test]
fn the_smoke_marks_by_queue_bytes() {
    assert_byte_thresholds("smoke-simai.toml");
}

#[test]
fn the_reduced_traces_mark_by_queue_bytes() {
    for name in [
        "reduced-dense-simai.toml",
        "reduced-dense-megatron.toml",
        "reduced-moe-simai.toml",
        "reduced-moe-imbalanced.toml",
    ] {
        assert_byte_thresholds(name);
    }
}

/// `configs/p16/rail_mini_roce.toml` with `edit` applied, lowered from a scratch directory.
fn rail_variant(name: &str, edit: impl Fn(String) -> String) -> Result<SimulationImage, String> {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p16/rail_mini_roce.toml");
    let text = edit(std::fs::read_to_string(path).expect("read the rail fixture"));
    let scratch =
        std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("ecn_bytes_{name}.toml"));
    std::fs::write(&scratch, text).expect("write the variant");
    days::scenario::compile_config(&scratch).map_err(|error| error.to_string())
}

fn in_bytes(text: String) -> String {
    text.replace(
        "capacity = 3729\n",
        "capacity = 3729\necn_capacity_bytes = 33554432\n",
    )
    .replace("threshold_packets = 112", "threshold_bytes = 1000000")
    .replace("threshold_packets = 223", "threshold_bytes = 2000000")
}

#[test]
fn ecn_rows_in_bytes_lower_a_byte_policy_and_keep_the_packet_capacity() {
    let image = rail_variant("bytes", in_bytes).unwrap_or_else(|error| panic!("{error}"));
    for (rate, policy, _) in ecn_policies(&image) {
        let threshold = if rate == 100 * G {
            1_000_000
        } else {
            2_000_000
        };
        assert_eq!(policy, bytes(threshold), "{rate} b/s");
    }
    assert!(
        image
            .switch_states
            .iter()
            .flat_map(|state| &state.queues)
            .all(|queue| queue.queue_capacity_packets == 3729)
    );
}

#[test]
fn ecn_rows_refuse_mixed_units_and_thresholds_beyond_the_byte_capacity() {
    for (name, edit, expected) in [
        (
            "mixed",
            Box::new(|text: String| {
                in_bytes(text).replace("threshold_bytes = 2000000", "threshold_packets = 223")
            }) as Box<dyn Fn(String) -> String>,
            "rows are in bytes here",
        ),
        (
            "packets_with_byte_capacity",
            Box::new(|text: String| {
                text.replace(
                    "capacity = 3729\n",
                    "capacity = 3729\necn_capacity_bytes = 33554432\n",
                )
            }),
            "rows are in bytes here",
        ),
        (
            "beyond",
            Box::new(|text: String| {
                in_bytes(text).replace("threshold_bytes = 2000000", "threshold_bytes = 40000000")
            }),
            "1..=33554432 bytes",
        ),
        (
            "no_rows",
            Box::new(|text: String| {
                let mut text = in_bytes(text);
                let start = text.find("ecn_by_rate = [").expect("rows");
                let end = start + text[start..].find("]\n").expect("rows end") + 2;
                text.replace_range(start..end, "ecn_threshold = 0.8\n");
                text
            }),
            "`switch.ecn_capacity_bytes` needs `switch.ecn_by_rate` rows in bytes",
        ),
    ] {
        let error = rail_variant(name, edit).expect_err(name);
        assert!(error.contains(expected), "{name}: {error}");
    }
}

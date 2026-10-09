//! P16 H2 (ruling H2-7): the switch parameters of the SimAI-identical arms, lowered per port.
//!
//! `configs/p16/rail_mini_roce.toml` sets ECN ramp rows by egress link rate, PFC XOFF and XON by
//! switch tier (ASW, PSW), and PFC headroom by controlled-link rate. The image and every backend
//! already hold these per egress LP and per ingress monitor; these tests pin what lowering writes.

use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{DropMarkPolicy, NodeKind, SimulationImage};
use tempfile::TempDir;

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn lower_text(text: &str) -> Result<SimulationImage, String> {
    let directory = TempDir::new().expect("temp dir");
    let path = directory.path().join("rows.toml");
    fs::write(&path, text).expect("write");
    compile_config(&path).map_err(|error| error.to_string())
}

fn fixture_text() -> String {
    fs::read_to_string(repo_path("configs/p16/rail_mini_roce.toml")).expect("fixture")
}

#[test]
fn ecn_rows_follow_the_egress_rate() {
    let image = lower_text(&fixture_text()).expect("lowers");
    let mut seen = std::collections::BTreeMap::<(u64, u64, u64, u64, u64), usize>::new();
    for state in &image.switch_states {
        let queue = &state.queues[0];
        let link = image.links[queue.egress_link.expect("egress").0 as usize];
        let DropMarkPolicy::EcnRamp(policy) = queue.drop_mark else {
            panic!("ECN ramp expected");
        };
        assert_eq!(policy.capacity_bytes, 33_554_432);
        assert_eq!(queue.queue_capacity_packets, 3729);
        *seen
            .entry((
                link.rate_bps,
                policy.kmin_bytes,
                policy.kmax_bytes,
                policy.pmax_numerator,
                policy.pmax_denominator,
            ))
            .or_default() += 1;
    }
    // ASW downlinks to the 8 GPUs at 100G; ASW uplinks and PSW downlinks at 400G (4 x 2 x 2).
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        vec![
            ((100_000_000_000, 400_000, 1_600_000, 1, 5), 8),
            ((400_000_000_000, 800_000, 3_200_000, 1, 5), 16)
        ]
    );
}

#[test]
fn pfc_thresholds_follow_the_tier_and_the_controlled_rate() {
    let image = lower_text(&fixture_text()).expect("lowers");
    let asws = 4;
    let mut checked = 0;
    for state in &image.switch_states {
        let Some(pfc) = &state.queues[0].pfc else {
            continue;
        };
        let asw = state.physical_switch < asws;
        for ingress in &pfc.ingresses {
            let controlled = image.links[ingress.controlled_link.0 as usize];
            let (xoff, xon) = if asw {
                (4_168_818, 4_165_746)
            } else {
                (4_154_756, 4_151_684)
            };
            let headroom = if controlled.rate_bps == 100_000_000_000 {
                30_574
            } else {
                75_000
            };
            assert_eq!(ingress.xoff_threshold_bytes, [0, 0, 0, xoff, 0, 0, 0, 0]);
            assert_eq!(ingress.xon_threshold_bytes, [0, 0, 0, xon, 0, 0, 0, 0]);
            assert_eq!(
                ingress.buffer_capacity_bytes,
                [0, 0, 0, xoff + headroom, 0, 0, 0, 0]
            );
            let host_ingress = image.nodes[controlled.source.0 as usize].kind == NodeKind::Host;
            assert_eq!(host_ingress, controlled.rate_bps == 100_000_000_000);
            checked += 1;
        }
    }
    assert!(checked > 0);
}

#[test]
fn the_rows_are_refused_where_they_are_ambiguous_or_incomplete() {
    let text = fixture_text();
    let cases = [
        (
            text.replace("ecn_capacity_bytes = 33_554_432\n", ""),
            "ecn_capacity_bytes",
        ),
        (
            text.replace(
                "    { rate_bps = 100000000000, kmin_bytes = 400_000, kmax_bytes = 1_600_000, pmax = 0.2 },\n",
                "",
            ),
            "ecn_by_rate",
        ),
        (
            text.replace("host_links = true\n", "host_links = true\nxoff = [0, 0, 0, 1, 0, 0, 0, 0]\n"),
            "by_tier",
        ),
        (
            text.replace("    { rate_bps = 100000000000, bytes = 30574 },\n", ""),
            "headroom_by_rate",
        ),
        (
            text.replace(
                "    { tier = \"psw\", xoff = [0, 0, 0, 4154756, 0, 0, 0, 0], xon = [0, 0, 0, 4151684, 0, 0, 0, 0] },\n",
                "",
            ),
            "by_tier",
        ),
    ];
    for (variant, needle) in cases {
        let error = lower_text(&variant).expect_err(needle);
        assert!(error.contains(needle), "{needle}: {error}");
    }
    // Tiers are rail tiers.
    let off_rail = fs::read_to_string(repo_path("configs/p15/roce_ring_allreduce_lossless.toml"))
        .expect("fixture")
        .replace(
            "xoff = [0, 0, 0, 20000, 0, 0, 0, 0]\nxon = [0, 0, 0, 10000, 0, 0, 0, 0]\n",
            "by_tier = [\n    { tier = \"asw\", xoff = [0, 0, 0, 20000, 0, 0, 0, 0], xon = [0, 0, 0, 10000, 0, 0, 0, 0] },\n    { tier = \"psw\", xoff = [0, 0, 0, 20000, 0, 0, 0, 0], xon = [0, 0, 0, 10000, 0, 0, 0, 0] },\n]\n",
        );
    let error = lower_text(&off_rail).expect_err("tiers off the rail");
    assert!(error.contains("by_tier"), "{error}");
}

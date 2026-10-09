//! P16 ecnramp: the scenario surface of the one ECN marking policy.
//!
//! `switch.drop = "TailDrop"` is the only drop rule. Marking is `switch.ecn = { kmin_bytes,
//! kmax_bytes, pmax }` (every queue) or `switch.ecn_by_rate` rows (one per egress link rate), both
//! with `switch.ecn_capacity_bytes`, the byte capacity an ECN queue tail-drops at. `pmax` is an
//! exact decimal reduced to lowest terms. RED, RED_ECN and the packet-unit ECN threshold are gone.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use days_executor::{DropMarkPolicy, EcnRampPolicy, SimulationImage};

const G: u64 = 1_000_000_000;

fn repo(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn lower_text(name: &str, text: &str) -> Result<SimulationImage, String> {
    let scratch = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("ecn_ramp_{name}.toml"));
    std::fs::write(&scratch, text).expect("write the variant");
    days::scenario::compile_config(&scratch).map_err(|error| error.to_string())
}

/// `path`'s text with every marking or drop key of `[switch]` removed and `lines` added.
fn with_switch(path: &str, lines: &str) -> String {
    let text = std::fs::read_to_string(repo(path)).expect("read the fixture");
    let mut out = String::new();
    let mut in_switch = false;
    let mut skipping_array = false;
    for line in text.lines() {
        if skipping_array {
            if line.trim_start().starts_with(']') {
                skipping_array = false;
            }
            continue;
        }
        if line.starts_with('[') {
            in_switch = line.trim() == "[switch]";
            out.push_str(line);
            out.push('\n');
            if in_switch {
                out.push_str(lines);
                out.push('\n');
            }
            continue;
        }
        let key = line.split('=').next().unwrap_or("").trim();
        if in_switch
            && matches!(
                key,
                "drop" | "ecn_threshold" | "ecn" | "ecn_capacity_bytes" | "ecn_by_rate"
            )
        {
            if key == "ecn_by_rate" && line.trim_end().ends_with('[') {
                skipping_array = true;
            }
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Every switch queue's admission policy, by its egress link's rate, with its queue count.
fn policies(image: &SimulationImage) -> BTreeMap<(u64, String), usize> {
    let mut policies = BTreeMap::new();
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        let rate = queue
            .egress_link
            .map_or(0, |link| image.links[link.0 as usize].rate_bps);
        *policies
            .entry((rate, format!("{:?}", queue.drop_mark)))
            .or_default() += 1;
    }
    policies
}

fn ramp(capacity: u64, kmin: u64, kmax: u64, num: u64, den: u64) -> DropMarkPolicy {
    DropMarkPolicy::EcnRamp(EcnRampPolicy {
        capacity_bytes: capacity,
        kmin_bytes: kmin,
        kmax_bytes: kmax,
        pmax_numerator: num,
        pmax_denominator: den,
    })
}

fn step(capacity: u64, threshold: u64) -> DropMarkPolicy {
    ramp(capacity, threshold, threshold, 1, 1)
}

/// The single policy every switch queue of `image` carries.
fn uniform_policy(image: &SimulationImage) -> DropMarkPolicy {
    let first = image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .next()
        .expect("switch queues")
        .drop_mark;
    for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
        assert_eq!(queue.drop_mark, first, "every queue carries one policy");
    }
    first
}

/// Ruling 6a: every packet-unit ECN config converts to a byte step at `threshold x S` with a byte
/// capacity of `capacity x S`, S its data packet size; `rail_mini_roce` and `aqm_roce_acks_red`
/// become ramps and `fattree` (RED drop) becomes TailDrop.
#[test]
fn every_converted_config_lowers_to_its_byte_policy() {
    let steps: &[(&str, u64, u64)] = &[
        (
            "configs/benchmarks/evaluation/f_aqm_alias_incast32.toml",
            32_768,
            4_096,
        ),
        ("configs/p14/dcqcn_10s_zero_xoff.toml", 512_000, 103_000),
        ("configs/p14/dcqcn_1s_zero_xoff.toml", 512_000, 103_000),
        ("configs/p14/dcqcn_2s_zero_xoff.toml", 512_000, 103_000),
        ("configs/p14/dcqcn_multi_zero_xoff.toml", 512_000, 103_000),
        ("configs/p14/dcqcn_simple_zero_xoff.toml", 512_000, 103_000),
        (
            "configs/p14/leanguard_dcqcn_zero_xoff.toml",
            512_000,
            103_000,
        ),
        ("configs/p14/dcqcn_t26.toml", 1_000, 1_000),
        ("configs/p14/dcqcn_t26_pfc.toml", 1_000, 1_000),
        ("configs/p15/hostpfc_bidir_drr.toml", 300_000, 300_000),
        ("configs/p15/hostpfc_bidir_wrr.toml", 300_000, 300_000),
        ("configs/p15/hostpfc_incast_lossless.toml", 300_000, 300_000),
        ("configs/p15/hostpfc_multi_qp_tcp.toml", 300_000, 300_000),
        (
            "configs/p15/hpcc_incast64_dragonfly.toml",
            104_800_000,
            999_792,
        ),
        ("configs/p15/roce_allgather_lossless.toml", 300_000, 300_000),
        ("configs/p15/roce_cnp_under_pfc.toml", 1_000_000, 50_000),
        ("configs/p15/roce_compute_dag.toml", 300_000, 300_000),
        ("configs/p15/roce_feedback_priority.toml", 1_000_000, 50_000),
        ("configs/p15/roce_lossless_pfc.toml", 1_000_000, 1_000_000),
        ("configs/p15/roce_mixed_tcp.toml", 64_000, 32_000),
        (
            "configs/p15/roce_ring_allreduce_lossless.toml",
            300_000,
            300_000,
        ),
        (
            "configs/p15/roce_ring_release_paused.toml",
            300_000,
            300_000,
        ),
        (
            "configs/p15/roce_tcp_mixed_collectives.toml",
            300_000,
            300_000,
        ),
        ("configs/p16/dcqcn_mlx_blocked.toml", 512_000, 103_000),
        ("configs/p16/dcqcn_mlx_coincident.toml", 512_000, 103_000),
        (
            "configs/p16/dcqcn_mlx_coincident_grid_qp.toml",
            1_000_000,
            50_000,
        ),
        (
            "configs/p16/dcqcn_mlx_coincident_pending.toml",
            512_000,
            103_000,
        ),
        (
            "configs/p16/dcqcn_mlx_coincident_qp.toml",
            1_000_000,
            50_000,
        ),
        ("configs/p16/dcqcn_mlx_window.toml", 300_000, 15_000),
        ("configs/p16/roce_long_flow_cutoff.toml", 300_000, 300_000),
        ("configs/leanguard/aqm_roce_acks.toml", 20_000, 5_000),
    ];
    for &(path, capacity, threshold) in steps {
        let image = days::scenario::compile_config(repo(path))
            .unwrap_or_else(|error| panic!("{path}: {error}"));
        assert_eq!(uniform_policy(&image), step(capacity, threshold), "{path}");
    }

    let red_ecn = days::scenario::compile_config(repo("configs/leanguard/aqm_roce_acks_red.toml"))
        .expect("aqm_roce_acks_red lowers");
    assert_eq!(uniform_policy(&red_ecn), ramp(20_000, 14_000, 18_000, 4, 5));

    let fattree = days::scenario::compile_config(repo("configs/fattree.toml")).expect("fattree");
    assert_eq!(uniform_policy(&fattree), DropMarkPolicy::TailDrop);

    let rail = days::scenario::compile_config(repo("configs/p16/rail_mini_roce.toml"))
        .expect("rail_mini_roce lowers");
    let expected = BTreeMap::from([
        (
            (
                100 * G,
                format!("{:?}", ramp(33_554_432, 400_000, 1_600_000, 1, 5)),
            ),
            16,
        ),
        (
            (
                400 * G,
                format!("{:?}", ramp(33_554_432, 800_000, 3_200_000, 1, 5)),
            ),
            8,
        ),
    ]);
    assert_eq!(policies(&rail), expected);
}

/// The five lossy RoCE fixtures exist for their drops (go-back-N, NACK and timeout paths): each
/// converts to a byte step at its byte capacity, so it marks only a full queue and tail-drops past
/// it. Their own tests assert that the drops still occur.
#[test]
fn the_lossy_roce_fixtures_tail_drop_at_a_byte_capacity() {
    for path in [
        "configs/p15/roce_allgather_compute_lossy.toml",
        "configs/p15/roce_gbn_lossy.toml",
        "configs/p15/roce_nack_only.toml",
        "configs/p15/roce_ring_lossy.toml",
        "configs/p15/roce_timeout.toml",
    ] {
        let image = days::scenario::compile_config(repo(path))
            .unwrap_or_else(|error| panic!("{path}: {error}"));
        let DropMarkPolicy::EcnRamp(policy) = uniform_policy(&image) else {
            panic!("{path}: an ECN queue")
        };
        assert_eq!(policy.kmin_bytes, policy.kmax_bytes, "{path}: a step");
        assert_eq!(
            policy.kmax_bytes, policy.capacity_bytes,
            "{path}: marks only when full"
        );
    }
}

#[test]
fn switch_ecn_lowers_one_ramp_on_every_queue_with_pmax_in_lowest_terms() {
    for (pmax, num, den) in [
        ("0.2", 1, 5),
        ("0.20", 1, 5),
        ("0.8", 4, 5),
        ("0.125", 1, 8),
        ("1", 1, 1),
        ("1.0", 1, 1),
    ] {
        let text = with_switch(
            "configs/p15/roce_ring_lossy.toml",
            &format!(
                "drop = \"TailDrop\"\necn_capacity_bytes = 8000\n\
                 ecn = {{ kmin_bytes = 2000, kmax_bytes = 6000, pmax = {pmax} }}"
            ),
        );
        let image = lower_text("pmax", &text).unwrap_or_else(|error| panic!("{pmax}: {error}"));
        assert_eq!(
            uniform_policy(&image),
            ramp(8_000, 2_000, 6_000, num, den),
            "{pmax}"
        );
    }
}

#[test]
fn switch_ecn_by_rate_lowers_one_ramp_per_egress_rate() {
    let text = with_switch(
        "configs/p16/rail_mini_roce.toml",
        "drop = \"TailDrop\"\necn_capacity_bytes = 33554432\necn_by_rate = [\n    \
         { rate_bps = 100000000000, kmin_bytes = 400000, kmax_bytes = 1600000, pmax = 0.2 },\n    \
         { rate_bps = 400000000000, kmin_bytes = 1000000, kmax_bytes = 1000000, pmax = 1 },\n]",
    );
    let image = lower_text("rows", &text).unwrap_or_else(|error| panic!("{error}"));
    let expected = BTreeMap::from([
        (
            (
                100 * G,
                format!("{:?}", ramp(33_554_432, 400_000, 1_600_000, 1, 5)),
            ),
            16,
        ),
        ((400 * G, format!("{:?}", step(33_554_432, 1_000_000))), 8),
    ]);
    assert_eq!(policies(&image), expected);
}

#[test]
fn the_removed_drop_rules_and_malformed_marking_are_refused() {
    let lossy = "configs/p15/roce_ring_lossy.toml";
    let rail = "configs/p16/rail_mini_roce.toml";
    let row = |rate: &str, kmin: u64, kmax: u64, pmax: &str| {
        format!("{{ rate_bps = {rate}, kmin_bytes = {kmin}, kmax_bytes = {kmax}, pmax = {pmax} }}")
    };
    let cases: Vec<(&str, &str, String, &str)> = vec![
        (
            "red",
            lossy,
            "drop = \"RED\"".to_owned(),
            "only drop rule is TailDrop",
        ),
        (
            "red_ecn",
            lossy,
            "drop = \"RED_ECN\"".to_owned(),
            "only drop rule is TailDrop",
        ),
        (
            "ecn_threshold",
            lossy,
            "drop = \"ECN_THRESHOLD\"".to_owned(),
            "only drop rule is TailDrop",
        ),
        (
            "threshold_key",
            lossy,
            "drop = \"TailDrop\"\necn_threshold = 0.5".to_owned(),
            "unknown field `ecn_threshold`",
        ),
        (
            "no_capacity",
            lossy,
            "drop = \"TailDrop\"\necn = { kmin_bytes = 2000, kmax_bytes = 6000, pmax = 0.5 }"
                .to_owned(),
            "needs `switch.ecn_capacity_bytes`",
        ),
        (
            "capacity_alone",
            lossy,
            "drop = \"TailDrop\"\necn_capacity_bytes = 8000".to_owned(),
            "`switch.ecn_capacity_bytes` needs `switch.ecn` or `switch.ecn_by_rate`",
        ),
        (
            "both",
            rail,
            format!(
                "drop = \"TailDrop\"\necn_capacity_bytes = 33554432\n\
                 ecn = {{ kmin_bytes = 2000, kmax_bytes = 6000, pmax = 0.5 }}\necn_by_rate = [{}, {}]",
                row("100000000000", 1, 2, "0.5"),
                row("400000000000", 1, 2, "0.5")
            ),
            "`switch.ecn` and `switch.ecn_by_rate` are exclusive",
        ),
        (
            "kmin_above_kmax",
            lossy,
            "drop = \"TailDrop\"\necn_capacity_bytes = 8000\n\
             ecn = { kmin_bytes = 6000, kmax_bytes = 2000, pmax = 0.5 }"
                .to_owned(),
            "1 <= kmin <= kmax <= capacity",
        ),
        (
            "kmax_above_capacity",
            lossy,
            "drop = \"TailDrop\"\necn_capacity_bytes = 8000\n\
             ecn = { kmin_bytes = 2000, kmax_bytes = 9000, pmax = 0.5 }"
                .to_owned(),
            "1 <= kmin <= kmax <= capacity",
        ),
        (
            "pmax_zero",
            lossy,
            "drop = \"TailDrop\"\necn_capacity_bytes = 8000\n\
             ecn = { kmin_bytes = 2000, kmax_bytes = 6000, pmax = 0 }"
                .to_owned(),
            "pmax must be a decimal in (0, 1]",
        ),
        (
            "pmax_above_one",
            lossy,
            "drop = \"TailDrop\"\necn_capacity_bytes = 8000\n\
             ecn = { kmin_bytes = 2000, kmax_bytes = 6000, pmax = 1.5 }"
                .to_owned(),
            "pmax must be a decimal in (0, 1]",
        ),
        (
            "step_pmax",
            lossy,
            "drop = \"TailDrop\"\necn_capacity_bytes = 8000\n\
             ecn = { kmin_bytes = 2000, kmax_bytes = 2000, pmax = 0.5 }"
                .to_owned(),
            "step (kmin == kmax) needs pmax = 1",
        ),
        (
            "missing_rate",
            rail,
            format!(
                "drop = \"TailDrop\"\necn_capacity_bytes = 33554432\necn_by_rate = [{}]",
                row("100000000000", 1, 2, "0.5")
            ),
            "has no row for a 400000000000 b/s egress link",
        ),
        (
            "duplicate_rate",
            rail,
            format!(
                "drop = \"TailDrop\"\necn_capacity_bytes = 33554432\necn_by_rate = [{}, {}]",
                row("100000000000", 1, 2, "0.5"),
                row("100000000000", 1, 2, "0.5")
            ),
            "one row per distinct positive rate",
        ),
    ];
    for (name, path, lines, expected) in cases {
        let error = lower_text(name, &with_switch(path, &lines)).expect_err(name);
        assert!(error.contains(expected), "{name}: {error}");
    }
}

/// P16 ecnramp perf discipline: the admission policy every switch queue carries shrinks from the
/// RED state's 96 B (its `u128` average) to five words and a tag.
#[test]
fn the_admission_policy_is_at_most_48_bytes() {
    assert!(
        std::mem::size_of::<DropMarkPolicy>() <= 48,
        "{}",
        std::mem::size_of::<DropMarkPolicy>()
    );
}

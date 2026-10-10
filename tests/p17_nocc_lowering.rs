//! P17 lane nocc (user ruling, Oct 9): RoCE queue pairs without congestion control. Lowering.
//!
//! `[*.traffic.roce] congestion_control` is `"dcqcn"` (the default, the P16 queue pair) or
//! `"none"`. A `"none"` pair has no DCQCN table: it paces at its source host's line rate, one MTU
//! per `ceil(MTU * 8e9 / rate)` ns, and carries the inert fixed-rate controller
//! (`days-gpu/evidence/P17/nocc/design.md`, rulings R1 to R3). Every scenario that lowered before
//! lowers to the same image (pinned below against `main` at 3ebb462).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{FlowGeneratorKind, RoceGenerator, SimulationImage};

fn repository(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn lower(path: &str) -> SimulationImage {
    compile_config(repository(path)).unwrap_or_else(|error| panic!("{path} must lower: {error}"))
}

fn lower_text(config: &str) -> Result<SimulationImage, String> {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p17-nocc-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, config).expect("write scenario");
    let image = compile_config(&path).map_err(|error| error.to_string());
    std::fs::remove_file(&path).expect("remove scenario");
    image
}

fn read(path: &str) -> String {
    std::fs::read_to_string(repository(path)).expect("read scenario")
}

/// `text` with `line` inserted after every line that is exactly a `[*.traffic.roce]` header.
fn after_roce_headers(text: &str, line: &str) -> String {
    let mut out = String::new();
    let mut count = 0;
    for current in text.lines() {
        out.push_str(current);
        out.push('\n');
        let trimmed = current.trim();
        if trimmed.starts_with('[') && trimmed.ends_with(".traffic.roce]") {
            out.push_str(line);
            out.push('\n');
            count += 1;
        }
    }
    assert!(count > 0, "no [*.traffic.roce] header");
    out
}

/// `text` with every `[*.traffic.dcqcn]` table and variable window removed, and every queue pair
/// set to `"none"`.
fn to_nocc(text: &str) -> String {
    let mut out = String::new();
    let mut skipping = false;
    for current in text.lines() {
        let trimmed = current.trim();
        if trimmed.starts_with('[') {
            skipping = trimmed.ends_with(".traffic.dcqcn]") || trimmed == "[dcqcn]";
        }
        if !skipping && !trimmed.starts_with("variable_window") {
            out.push_str(current);
            out.push('\n');
        }
    }
    after_roce_headers(&out, "congestion_control = \"none\"")
}

fn queue_pairs(image: &SimulationImage) -> Vec<RoceGenerator> {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => Some(roce),
            _ => None,
        })
        .collect()
}

/// The inert fixed-rate controller of a pair without congestion control (ruling R1a), checked
/// field by field: pristine, every rate the line rate, no gain, every interval `u64::MAX`.
fn assert_fixed_rate(roce: &RoceGenerator, rate_bps: u64, interval_ns: u64) {
    let controller = roce.controller;
    let config = controller.config;
    assert_eq!(
        (
            config.initial_rate_bps,
            config.minimum_rate_bps,
            config.maximum_rate_bps
        ),
        (rate_bps, rate_bps, rate_bps)
    );
    assert_eq!(
        (
            config.additive_rate_bps,
            config.hyper_rate_bps,
            config.g_q63
        ),
        (0, 0, 0)
    );
    assert_eq!(
        (
            config.alpha_interval_ns,
            config.decrease_interval_ns,
            config.increase_interval_ns
        ),
        (u64::MAX, u64::MAX, u64::MAX)
    );
    assert_eq!(
        (config.fast_recovery_steps, config.clamp_target_rate),
        (0, false)
    );
    assert_eq!(
        (controller.current_rate_bps, controller.target_rate_bps),
        (rate_bps, rate_bps)
    );
    assert!(!controller.armed);
    assert_eq!(roce.pacer.pacing_interval_ns, interval_ns);
    assert!(
        format!("{roce:?}").contains("congestion_control: None"),
        "{roce:?}"
    );
}

#[test]
fn none_paces_at_the_host_line_rate_with_the_inert_controller() {
    let image = lower("configs/p17/nocc_marked.toml");
    let pairs = queue_pairs(&image);
    assert_eq!(pairs.len(), 4);
    for roce in &pairs {
        // 1 Gbps ports, 1,000 B packets: ceil(1000 * 8e9 / 1e9) = 8,000 ns.
        assert_fixed_rate(roce, 1_000_000_000, 8_000);
    }
}

#[test]
fn none_on_the_rail_paces_at_the_nic_rate() {
    // 100 Gbps NICs and 400 Gbps uplinks; the pacer takes the host's NIC link.
    let text = to_nocc(&read("configs/p16/rail_mini_roce.toml"));
    let image = lower_text(&text).expect("rail none lowers");
    let pairs = queue_pairs(&image);
    assert!(!pairs.is_empty());
    for roce in &pairs {
        let mtu = roce.pacer.mtu_bytes;
        let rate = 100_000_000_000_u64;
        let interval = (mtu * 8 * 1_000_000_000).div_ceil(rate);
        assert_fixed_rate(roce, rate, interval);
    }
}

#[test]
fn explicit_dcqcn_lowers_to_the_default_image() {
    for path in [
        "configs/p15/roce_cnp_under_pfc.toml",
        "configs/p15/roce_ring_allreduce_lossless.toml",
    ] {
        let text = read(path);
        let explicit = lower_text(&after_roce_headers(&text, "congestion_control = \"dcqcn\""))
            .unwrap_or_else(|error| panic!("{path} with explicit dcqcn: {error}"));
        assert_eq!(
            format!("{explicit:#?}"),
            format!("{:#?}", lower(path)),
            "{path}"
        );
        assert!(!format!("{explicit:?}").contains("congestion_control"));
    }
}

#[test]
fn none_refuses_a_dcqcn_table() {
    let text = after_roce_headers(
        &read("configs/p15/roce_cnp_under_pfc.toml"),
        "congestion_control = \"none\"",
    );
    let error = lower_text(&text).expect_err("none with [dcqcn] must be refused");
    assert!(
        error.contains("congestion_control = \"none\"")
            && error.contains("traffic.roce")
            && error.contains("traffic.dcqcn"),
        "{error}"
    );
}

#[test]
fn none_refuses_a_variable_window() {
    let text = after_roce_headers(
        &to_nocc(&read("configs/p15/roce_cnp_under_pfc.toml")),
        "window_bytes = 8000\nvariable_window = true",
    );
    let error = lower_text(&text).expect_err("none with a variable window must be refused");
    assert!(
        error.contains("variable_window") && error.contains("congestion_control = \"none\""),
        "{error}"
    );
    // A fixed window is allowed.
    let fixed = after_roce_headers(
        &to_nocc(&read("configs/p15/roce_cnp_under_pfc.toml")),
        "window_bytes = 8000",
    );
    let image = lower_text(&fixed).expect("none with a fixed window lowers");
    assert!(
        queue_pairs(&image)
            .iter()
            .all(|roce| roce.window_bytes == 8_000)
    );
}

#[test]
fn unknown_congestion_control_values_are_refused() {
    for value in ["hpcc", "DCQCN", "None", ""] {
        let text = after_roce_headers(
            &read("configs/p15/roce_cnp_under_pfc.toml"),
            &format!("congestion_control = \"{value}\""),
        );
        let error = lower_text(&text).expect_err("unknown value must be refused");
        assert!(
            error.contains("congestion_control")
                && error.contains(&format!("\"{value}\""))
                && error.contains("traffic.roce"),
            "{value}: {error}"
        );
    }
}

#[test]
fn dcqcn_still_requires_its_table() {
    let text = to_nocc(&read("configs/p15/roce_cnp_under_pfc.toml")).replace(
        "congestion_control = \"none\"",
        "congestion_control = \"dcqcn\"",
    );
    let error = lower_text(&text).expect_err("dcqcn without [dcqcn] must be refused");
    assert!(error.contains("traffic.dcqcn"), "{error}");
}

/// Every RoCE fixture lowers to exactly the image it lowered to on `main` (3ebb462): the same
/// flow ids, seeds, routes and rendering (design note §1 and risk 1: the RoCE key reshaping and
/// the hand-written `Debug` must not move a byte). Values recorded with
/// `days-gpu/evidence/P17/nocc/tooling/image_fingerprints.rs` on 3ebb462.
#[test]
fn every_roce_fixture_lowers_to_the_image_it_lowered_to_on_main() {
    const IMAGES: &[(&str, usize, u64)] = &[
        (
            "configs/p15/hostpfc_bidir_drr.toml",
            122_238,
            0x05a8_c135_6e00_b570,
        ),
        (
            "configs/p15/hostpfc_bidir_wrr.toml",
            122_178,
            0x532d_ff28_1da3_dd38,
        ),
        (
            "configs/p15/hostpfc_incast_lossless.toml",
            85_265,
            0x43e1_d043_9b76_f4d0,
        ),
        (
            "configs/p15/hostpfc_multi_qp_tcp.toml",
            99_363,
            0xc38b_5e8a_c1fd_b1a6,
        ),
        (
            "configs/p15/roce_allgather_compute_lossy.toml",
            227_129,
            0xc0c6_3e8a_5c42_b224,
        ),
        (
            "configs/p15/roce_allgather_lossless.toml",
            147_693,
            0x0115_6e0e_e52a_f78d,
        ),
        (
            "configs/p15/roce_cnp_under_pfc.toml",
            75_237,
            0xfb52_1ffd_7b16_8161,
        ),
        (
            "configs/p15/roce_compute_dag.toml",
            246_845,
            0xeb10_7a23_0b0a_3a6f,
        ),
        (
            "configs/p15/roce_feedback_priority.toml",
            75_374,
            0xb6e8_e52c_bffe_88cc,
        ),
        (
            "configs/p15/roce_gbn_lossy.toml",
            59_942,
            0x845e_ae71_9452_0f53,
        ),
        (
            "configs/p15/roce_lossless_pfc.toml",
            60_166,
            0xafcb_2179_fb97_dca5,
        ),
        (
            "configs/p15/roce_mixed_tcp.toml",
            68_739,
            0xbb85_f1e2_21f8_d77e,
        ),
        (
            "configs/p15/roce_nack_only.toml",
            44_540,
            0xc531_3610_25f7_f455,
        ),
        (
            "configs/p15/roce_ring_allreduce_lossless.toml",
            225_599,
            0x56dd_7a39_3734_a5f7,
        ),
        (
            "configs/p15/roce_ring_lossy.toml",
            208_670,
            0x07f5_58f6_aea3_833f,
        ),
        (
            "configs/p15/roce_ring_release_paused.toml",
            230_900,
            0x311d_0dcd_00ad_7c02,
        ),
        (
            "configs/p15/roce_tcp_mixed_collectives.toml",
            177_930,
            0xe39d_56ac_49c8_b5eb,
        ),
        (
            "configs/p15/roce_timeout.toml",
            44_545,
            0x0b66_b1c1_a195_21a7,
        ),
        (
            "configs/p16/dcqcn_mlx_blocked.toml",
            63_608,
            0x28f7_ddcc_aa6e_f72d,
        ),
        (
            "configs/p16/dcqcn_mlx_coincident_grid_qp.toml",
            75_211,
            0xe86e_621b_e25c_c5d5,
        ),
        (
            "configs/p16/dcqcn_mlx_coincident_pending.toml",
            63_619,
            0xafa4_e5af_33c1_76c9,
        ),
        (
            "configs/p16/dcqcn_mlx_coincident_qp.toml",
            75_229,
            0x2a54_6bf2_59a0_ba93,
        ),
        (
            "configs/p16/dcqcn_mlx_coincident.toml",
            63_614,
            0x0282_68fc_c7b0_d97e,
        ),
        (
            "configs/p16/dcqcn_mlx_window.toml",
            90_282,
            0xac6e_e785_2741_e5cc,
        ),
        (
            "configs/p16/rail_mini_roce.toml",
            775_146,
            0xfd92_6e7a_eb9a_cc76,
        ),
        (
            "configs/p16/rail_mini_tcp_allgather.toml",
            273_344,
            0x309c_0f8d_944e_e696,
        ),
        (
            "configs/p16/roce_long_flow_cutoff.toml",
            236_648,
            0x5b0a_f26f_a7d6_6742,
        ),
        (
            "tests/fixtures/aicb/reduced-dense-megatron.toml",
            1_654_663,
            0xe268_b2b5_71d3_5d8a,
        ),
    ];
    for &(path, bytes, fnv1a64) in IMAGES {
        let text = format!("{:#?}", lower(path));
        let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
        assert_eq!(
            (text.len(), hash),
            (bytes, fnv1a64),
            "{path}: the lowered image moved (got bytes={} fnv1a64={hash:016x})",
            text.len()
        );
    }
}

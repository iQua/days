//! P18 diet-host, the bit-equality gate (design ruling 6): frozen `{image:#?}` fingerprints of
//! the four images the memory diet must not change, captured at `feat/p17` `5c7629ff`.
//!
//! The diet rewrites lowering's transient state (c3) and the device pipeline around the image
//! (c2, c4, c5, c9). None of it may change a lowered image: these pins compare FNV-1a-64 over the
//! pretty Debug rendering and its byte count against values taken before any diet commit.
//! `tests/planner_bit_equal.rs` compares two device planners over one image, so it cannot prove a
//! lowering change bit-equal; this file can.
//!
//! b4 and the MoE smoke lower from committed fixtures. The M2s trace is not committed: set
//! `DAYS_AICB_M2S` (days-gpu `evidence/P17/profile/tooling/scenarios/m2s_tp2ep32.txt`, sha256
//! `fee71765…`) and run in release, one test at a time:
//! `cargo test --release --test p18_image_pins -- --ignored --test-threads=1`.
//! Only b4 (15 MB of Debug text) runs by default; the others are ignored (ruling
//! 6), since the smoke alone formats to 2.5 GB, which a debug CI build would spend minutes
//! hashing.

#[path = "support/aicb.rs"]
mod aicb;
#[path = "support/image_fnv.rs"]
mod image_fnv;

use std::path::Path;

use days::scenario::compile_config;
use days_executor::SimulationImage;
use image_fnv::{ImageFingerprint, fingerprint};

fn check(name: &str, image: &SimulationImage, expected: ImageFingerprint) {
    let print = fingerprint(image);
    println!(
        "record=p18_image_pin image={name} flows={} debug_bytes={} debug_fnv1a64={:#018x}",
        print.flows, print.debug_bytes, print.debug_fnv1a64
    );
    assert_eq!(
        print, expected,
        "{name}: the lowered image must keep its frozen bytes"
    );
}

/// Lowers `scenario` (a fixture under `tests/fixtures/aicb`) from a directory that also holds
/// `SimAI.conf` and the trace named by the environment variable `trace_env`, copied as `trace`.
fn lower_with_trace(scenario: &str, trace_env: &str, trace: &str) -> SimulationImage {
    let source = std::env::var_os(trace_env)
        .unwrap_or_else(|| panic!("{trace_env} names the {trace} trace (not committed)"));
    let directory = tempfile::TempDir::new().expect("temp dir");
    for file in [scenario, "SimAI.conf"] {
        std::fs::copy(aicb::fixture(file), directory.path().join(file)).expect("copy the scenario");
    }
    std::fs::copy(Path::new(&source), directory.path().join(trace)).expect("copy the trace");
    compile_config(directory.path().join(scenario))
        .unwrap_or_else(|error| panic!("{scenario}: {error}"))
}

#[test]
fn b4_lowers_to_its_frozen_image() {
    check(
        "b4",
        &aicb::lower("b4-simai.toml"),
        ImageFingerprint {
            flows: 2_048,
            debug_bytes: 15_207_992,
            debug_fnv1a64: 0xe500_2742_d39e_7d17,
        },
    );
}

#[test]
#[ignore = "P18 image pin; run in release"]
fn the_moe_smoke_lowers_to_its_frozen_image() {
    check(
        "smoke",
        &aicb::lower("smoke-simai.toml"),
        ImageFingerprint {
            flows: 426_112,
            debug_bytes: 2_534_287_452,
            debug_fnv1a64: 0x598b_0d35_986a_34dd,
        },
    );
}

#[test]
#[ignore = "P18 image pin; set DAYS_AICB_M2S, run in release"]
fn m2s_lowers_to_its_frozen_image() {
    check(
        "m2s",
        &lower_with_trace("m2s-simai.toml", "DAYS_AICB_M2S", "m2s_tp2ep32.txt"),
        ImageFingerprint {
            flows: 229_504,
            debug_bytes: 1_368_201_675,
            debug_fnv1a64: 0xc8ea_d6f6_f3fe_6815,
        },
    );
}

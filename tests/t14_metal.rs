#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{MetalConfig, MetalRun, run_metal, run_scalar};

const BASELINE_FIXTURES: [&str; 2] = [
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
    "configs/benchmarks/baseline/fattree_k8_f64_st.toml",
];
const WIDTH_VIA_LOAD_10_FIXTURE: &str =
    "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml";

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn assert_metal_matches_scalar(relative: &str) -> MetalRun {
    let path = fixture_path(relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let scalar = run_scalar(&image, None)
        .unwrap_or_else(|error| panic!("scalar failed for {}: {error}", path.display()));
    let metal = run_metal(&image, None, MetalConfig::default())
        .unwrap_or_else(|error| panic!("Metal failed for {}: {error}", path.display()));

    assert_eq!(
        metal.result,
        scalar,
        "Metal result differs from scalar for {}",
        path.display()
    );
    metal
}

#[test]
fn baseline_fattree_k4_and_k8_match_scalar_complete_result() {
    for fixture in BASELINE_FIXTURES {
        assert_metal_matches_scalar(fixture);
    }
}

#[test]
#[ignore = "~10.6M-event production Metal acceptance fixture"]
fn fattree_k32_load_10_matches_scalar_complete_result() {
    let metal = assert_metal_matches_scalar(WIDTH_VIA_LOAD_10_FIXTURE);
    assert_eq!(metal.transitions, 10_604_109);
}

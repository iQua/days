#![cfg(all(feature = "metal-spike", target_vendor = "apple"))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    MetalConfig, MetalRun, ObservationMode, run_metal, run_metal_with_observations, run_scalar,
};

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
    let run = |streams_enabled| {
        run_metal(
            &image,
            None,
            MetalConfig {
                streams_enabled,
                ..MetalConfig::default()
            },
        )
        .unwrap_or_else(|error| {
            panic!(
                "Metal streams={streams_enabled} failed for {}: {error}",
                path.display()
            )
        })
    };
    let streams = run(true);
    let heap = run(false);

    assert_eq!(
        streams.result,
        scalar,
        "Metal streams=true differs from scalar for {}",
        path.display()
    );
    assert_eq!(
        heap.result,
        scalar,
        "Metal streams=false differs from scalar for {}",
        path.display()
    );
    assert_eq!(streams.result, heap.result);
    assert_eq!(streams.rounds, heap.rounds);
    assert_eq!(streams.transitions, heap.transitions);
    streams
}

#[test]
fn baseline_fattree_k4_and_k8_match_scalar_complete_result() {
    for fixture in BASELINE_FIXTURES {
        assert_metal_matches_scalar(fixture);
    }
}

#[test]
fn fattree_k8_result_is_independent_of_round_threadgroup_geometry() {
    let path = fixture_path(BASELINE_FIXTURES[1]);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let run = |round_threads_per_threadgroup, streams_enabled| {
        run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                round_threads_per_threadgroup,
                streams_enabled,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| {
            panic!(
                "Metal geometry {round_threads_per_threadgroup} failed for {}: {error}",
                path.display()
            )
        })
    };

    for streams_enabled in [true, false] {
        let narrow = run(32, streams_enabled);
        let wide = run(256, streams_enabled);

        assert_eq!(narrow.result, wide.result);
    }
}

#[test]
#[ignore = "~10.6M-event production Metal acceptance fixture"]
fn fattree_k32_load_10_matches_scalar_complete_result() {
    let metal = assert_metal_matches_scalar(WIDTH_VIA_LOAD_10_FIXTURE);
    assert_eq!(metal.transitions, 10_604_109);
}

#![cfg(feature = "cuda")]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    CudaConfig, ObservationMode, run_cuda_with_observations, run_scalar_with_observations,
};

const K4_FIXTURE: &str = "configs/benchmarks/baseline/fattree_k4_f8_st.toml";

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn cuda_fattree_k4_is_byte_exact_end_to_end_and_deterministic() {
    let path = fixture_path(K4_FIXTURE);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("scalar failed for {}: {error}", path.display()));
    let run = || {
        run_cuda_with_observations(&image, None, CudaConfig::default(), ObservationMode::Full)
            .unwrap_or_else(|error| panic!("CUDA failed for {}: {error}", path.display()))
    };
    let first = run();
    let second = run();

    assert_eq!(first.result, scalar);
    assert_eq!(second.result, scalar);
    assert_eq!(first.result, second.result);
    assert_eq!(first.rounds, second.rounds);
    assert_eq!(first.transitions, second.transitions);
}

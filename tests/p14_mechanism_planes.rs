//! P14 Lane B Acceptance 5: DCQCN and PFC state costs nothing in a production device plan when the
//! image carries none.
//!
//! The hooks build the real production plan and measure what it spends on the mechanisms: the PFC
//! region appended to the scheduler plane and the DCQCN-marked per-flow receiver rows. The E5/E6
//! evaluation fixtures, which the zero-cost A/B measures, must plan zero words for both. The P14
//! fixtures must plan a nonzero amount, so the measurement is not vacuous.
#![cfg(all(
    feature = "test",
    any(
        feature = "cuda",
        feature = "cuda-planner-test",
        all(feature = "metal", target_vendor = "apple")
    )
))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{MechanismPlaneWords, SimulationImage};

const EVALUATION: [&str; 8] = [
    "e5_wide_k32_q200.toml",
    "e5_wide_k32_q200_cubic.toml",
    "e5_wide_k32_q256.toml",
    "e5_legacy_k4_loss.toml",
    "e6_cbr_k32_load_01.toml",
    "e6_cbr_k32_load_10.toml",
    "e6_cbr_k32_load_30.toml",
    "e6_cbr_k32_load_60.toml",
];

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

// One entry per backend built into this test binary; each push is feature-gated.
#[allow(clippy::vec_init_then_push)]
fn measure(image: &SimulationImage) -> Vec<(&'static str, MechanismPlaneWords)> {
    #[allow(unused_mut)]
    let mut measured = Vec::new();
    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    measured.push((
        "CUDA",
        days_executor::mechanism_plane_words_cuda_for_testing(
            image,
            days_executor::CudaConfig::default(),
        )
        .expect("CUDA plan must size"),
    ));
    #[cfg(all(feature = "metal", target_vendor = "apple"))]
    measured.push((
        "Metal",
        days_executor::mechanism_plane_words_metal_for_testing(
            image,
            days_executor::MetalConfig::default(),
        )
        .expect("Metal plan must size"),
    ));
    measured
}

#[test]
fn evaluation_fixtures_plan_no_dcqcn_or_pfc_words() {
    for name in EVALUATION {
        let image = lower(&format!("configs/benchmarks/evaluation/{name}"));
        for (backend, words) in measure(&image) {
            assert_eq!(
                words,
                MechanismPlaneWords {
                    pfc_region_words: 0,
                    dcqcn_receiver_rows: 0,
                    pfc_params_words: 1,
                },
                "{backend} {name}"
            );
        }
    }
}

#[test]
fn p14_fixtures_plan_their_mechanism_state() {
    for (name, pfc, dcqcn) in [
        ("dcqcn_t26.toml", false, 1),
        ("dcqcn_t26_pfc.toml", true, 1),
        ("leanguard_pfc_executable.toml", true, 0),
        ("dcqcn_multi_zero_xoff.toml", true, 2),
    ] {
        let image = lower(&format!("configs/p14/{name}"));
        for (backend, words) in measure(&image) {
            assert_eq!(
                words.pfc_region_words != 0,
                pfc,
                "{backend} {name}: {words:?}"
            );
            assert_eq!(words.dcqcn_receiver_rows, dcqcn, "{backend} {name}");
        }
    }
}

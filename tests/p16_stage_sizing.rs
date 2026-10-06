//! P16 lane G2 (stagesize): the host projection's `tcp_state` plane against the production
//! planners' actual allocation (`days-gpu/evidence/P16/colldev-design.md` §0.2 item 3).
//!
//! `device-sizing-report` (`size_default_device_plan`) is the tool the design's fit argument rests
//! on. The production planners allocate `tcp_state` for every image: a receiver row and a ledger
//! row per flow, the TCP receive ranges and ledger records, then the stage region (P16 G1) and the
//! RoCE region (P15). The projection must report the same words.
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
use days_executor::{
    DeviceSizingReport, ObservationMode, SimulationImage, size_default_device_plan,
};

/// One image of each `tcp_state` composition: queue pairs only (with and without a window, and
/// with checkpoint-free resident packets), TCP with queue pairs, DCQCN rows, stages over RoCE, over
/// RoCE and TCP, and with compute stages, and an open-loop image.
const FIXTURES: &[&str] = &[
    "configs/p15/roce_gbn_lossy.toml",
    "configs/p15/roce_lossless_pfc.toml",
    "configs/p16/dcqcn_mlx_window.toml",
    "configs/p15/hostpfc_multi_qp_tcp.toml",
    "configs/p14/dcqcn_1s_zero_xoff.toml",
    "configs/p15/roce_ring_allreduce_lossless.toml",
    "configs/p15/roce_tcp_mixed_collectives.toml",
    "configs/p15/roce_compute_dag.toml",
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
];

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn tcp_state_words(report: &DeviceSizingReport) -> Option<usize> {
    report
        .planes
        .iter()
        .find(|plane| plane.name == "tcp_state")
        .map(|plane| plane.words)
}

// One entry per backend built into this test binary; each push is feature-gated.
#[allow(clippy::vec_init_then_push)]
fn exact_plans(image: &SimulationImage) -> Vec<(&'static str, DeviceSizingReport)> {
    #[allow(unused_mut)]
    let mut plans = Vec::new();
    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    plans.push((
        "CUDA",
        days_executor::size_cuda_plan_for_testing(
            image,
            None,
            days_executor::CudaConfig::default(),
            ObservationMode::Summary,
        )
        .expect("CUDA plan must size"),
    ));
    #[cfg(all(feature = "metal", target_vendor = "apple"))]
    plans.push((
        "Metal",
        days_executor::size_metal_plan_for_testing(
            image,
            None,
            days_executor::MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("Metal plan must size"),
    ));
    plans
}

#[test]
fn projected_tcp_state_is_the_planners_allocation() {
    for relative in FIXTURES {
        let image = lower(relative);
        let projected = size_default_device_plan(&image)
            .unwrap_or_else(|error| panic!("{relative} must project: {error}"));
        for (backend, exact) in exact_plans(&image) {
            let allocated = tcp_state_words(&exact)
                .unwrap_or_else(|| panic!("{backend} {relative}: the plan must carry tcp_state"));
            assert_eq!(
                tcp_state_words(&projected),
                Some(allocated),
                "{backend} {relative}: the projected tcp_state plane must be the planner's"
            );
        }
    }
}

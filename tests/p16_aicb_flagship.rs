//! P16 H3 (aicb): the flagship AICB trace (1,024 H100s, TP2, EP32, gbs 1,024) lowers in full on
//! SimAI's 1024g file and runs to a cutoff on every backend (design note §6.1 and §7, ruling A5).
//!
//! The trace is not committed (ruling A5): set `DAYS_AICB_FLAGSHIP` to its path
//! (`days-gpu/evidence/P16/collops-design/traces/flagship-tp2-ep32-w1024.txt`, sha256
//! `2dfd84f5…`, which the scenario pins) and run in release, one test at a time:
//! `DAYS_AICB_FLAGSHIP=<path> cargo test --release [--features metal|cuda] --test
//! p16_aicb_flagship -- --ignored --test-threads=1`. The lowered image holds 14.7 M stages and
//! needs tens of GB of host memory.
//!
//! The CPU test lowers once and checks the stage counts `aicb_plan.py` reported (11,564,032
//! network messages, 2,749,440 NVLink notifies, 387 fused segments per rank), SimAI's per-pair
//! ECMP counts (29,824 pairs, at most 895 messages each) and exactness condition (b), then runs to
//! a cutoff past the start of the first cross-host collectives: Scalar against CPU (4 workers)
//! under Full and Summary observation. Each device test lowers again and runs to the same cutoff
//! under Summary observation.

#[path = "support/aicb.rs"]
mod aicb;

use aicb::{
    ecmp_pairs, first_segment_end, lower_flagship, max_ring_channels, ring_pairs_across_channels,
    stage_kinds,
};
use days_executor::{
    CollectiveAlgorithm, CpuConfig, ObservationMode, run_cpu_with_observations,
    run_scalar_with_observations,
};

#[test]
#[ignore = "acceptance: the flagship lowers 14.7 M stages; set DAYS_AICB_FLAGSHIP, run in release"]
fn the_flagship_lowers_in_full_and_runs_to_its_cutoff_on_cpu() {
    let image = lower_flagship();

    let (network, notify, computes) = stage_kinds(&image);
    let total: usize = network.iter().map(|(_, count)| count).sum();
    assert_eq!(total, 11_564_032, "{network:?}");
    assert!(
        network
            .iter()
            .any(|(algorithm, _)| *algorithm == CollectiveAlgorithm::AllToAll)
    );
    assert_eq!(notify, 2_749_440);
    assert_eq!(computes, 387 * 1_024);

    let pairs = ecmp_pairs(&image);
    assert_eq!(pairs.len(), 29_824);
    assert_eq!(pairs.values().max(), Some(&895));
    assert!(max_ring_channels(&image) > 1, "multi-channel rings");
    assert_eq!(ring_pairs_across_channels(&image), []);

    let horizon = first_segment_end(&image) + 10_000;
    for mode in [ObservationMode::Full, ObservationMode::Summary] {
        let expected = run_scalar_with_observations(&image, Some(horizon), mode)
            .expect("the Scalar oracle runs");
        assert!(
            !expected.departures.is_empty() || mode == ObservationMode::Summary,
            "the cutoff passes the start of the first cross-host collectives"
        );
        let actual = run_cpu_with_observations(
            &image,
            Some(horizon),
            CpuConfig {
                workers: 4,
                ..CpuConfig::default()
            },
            mode,
        )
        .expect("CPU runs the flagship")
        .result;
        assert_eq!(actual, expected, "flagship@{horizon} {mode:?}");
    }
}

/// The flagship's Scalar result at its cutoff under Summary observation, without diagnostics,
/// and the cutoff.
#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn summary_at_cutoff(image: &days_executor::SimulationImage) -> (days_executor::RunResult, u64) {
    let horizon = first_segment_end(image) + 10_000;
    let mut expected = run_scalar_with_observations(image, Some(horizon), ObservationMode::Summary)
        .expect("the Scalar oracle runs");
    expected.diagnostics = None;
    (expected, horizon)
}

#[cfg(feature = "cuda")]
#[test]
#[ignore = "acceptance: the flagship on CUDA; set DAYS_AICB_FLAGSHIP, run in release"]
fn cuda_runs_the_flagship_to_its_cutoff_as_scalar() {
    let image = lower_flagship();
    let (expected, horizon) = summary_at_cutoff(&image);
    let actual = days_executor::run_cuda_with_observations(
        &image,
        Some(horizon),
        days_executor::CudaConfig::default(),
        ObservationMode::Summary,
    )
    .expect("CUDA runs the flagship")
    .result;
    assert_eq!(actual, expected, "CUDA flagship@{horizon}");
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
#[ignore = "acceptance: the flagship on Metal; set DAYS_AICB_FLAGSHIP, run in release"]
fn metal_runs_the_flagship_to_its_cutoff_as_scalar() {
    let image = lower_flagship();
    let (expected, horizon) = summary_at_cutoff(&image);
    let actual = days_executor::run_metal_with_observations(
        &image,
        Some(horizon),
        days_executor::MetalConfig::default(),
        ObservationMode::Summary,
    )
    .expect("Metal runs the flagship")
    .result;
    assert_eq!(actual, expected, "Metal flagship@{horizon}");
}

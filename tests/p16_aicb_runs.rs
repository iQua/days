//! P16 H3 (aicb): AICB scenarios run byte-identically on every backend (design note §7, test
//! tiers of ruling A5).
//!
//! - **Tier (i):** four reduced traces run to completion on Scalar (every stage finished) and
//!   equal Scalar on CPU (1, 2 and 4 workers), Metal and CUDA, under Full and Summary
//!   observation, at the stop, at stop/2 and from checkpoints: the dense trace (32 GPUs, TP8,
//!   PP2) under the SimAI fidelity and under the Megatron fidelity (per-stage groups and PP
//!   Send/Recv), and the MoE trace (32 GPUs, TP2, EP8) under the SimAI fidelity and under the
//!   Megatron fidelity with imbalanced expert routing.
//! - **Tier (ii):** SimAI's b4 trace (SimAI and Megatron fidelity) and the MoE smoke (SimAI
//!   fidelity, and Megatron fidelity with imbalanced routing) lower in full and run to a cutoff past the start of their first cross-host collectives (the end of the
//!   first fused segment plus 10 us): CPU under Full and Summary, the devices under Summary. Full
//!   observation on a device sizes its logs for the whole run, not the cutoff: b4's 1,920 ring
//!   stages of 26,985 packets each exhaust CUDA's 20 GB on sim (result-plane upload out of memory)
//!   and stall Metal; that device-side finding is recorded in the lane report. The devices' Full
//!   identity on an AICB image is tier (i)'s. The flagship's cutoff runs are
//!   `tests/p16_aicb_flagship.rs`'s.
//! - **Tier (iii):** b4 to completion, every stage finished, Scalar against CPU (`#[ignore]`:
//!   51.8 M data packets; run it explicitly as the acceptance test). Devices: the `#[ignore]`
//!   tests in the device modules.

#[path = "support/aicb.rs"]
mod aicb;

use aicb::{first_segment_end, lower, unfinished};
use days_executor::{
    CpuConfig, ObservationMode, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_with_observations,
};

fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    run_scalar_with_observations(image, horizon, mode).expect("the Scalar oracle runs")
}

#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn without_diagnostics(mut result: RunResult) -> RunResult {
    result.diagnostics = None;
    result
}

fn checkpoint_image(original: &SimulationImage, checkpoint: &RunResult) -> SimulationImage {
    let mut image = original.clone();
    image.host_states.clone_from(&checkpoint.host_states);
    image.switch_states.clone_from(&checkpoint.switch_states);
    image
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    image.initial_events.clone_from(&checkpoint.pending_events);
    image
}

/// Tier (i): the reduced traces.
const TIER_ONE: [&str; 4] = [
    "reduced-dense-simai.toml",
    "reduced-dense-megatron.toml",
    "reduced-moe-simai.toml",
    "reduced-moe-imbalanced.toml",
];

const B4: &str = "b4-simai.toml";

/// Tier (ii): the full traces run to a cutoff.
const TIER_TWO: [&str; 4] = [
    B4,
    "b4-megatron.toml",
    "smoke-simai.toml",
    "smoke-imbalanced.toml",
];

/// Tier (i): each reduced trace and checkpoints at four horizons over its departures.
fn tier_one_images() -> Vec<(String, SimulationImage)> {
    let mut all = Vec::new();
    for name in TIER_ONE {
        let image = lower(name);
        let full = scalar(&image, None, ObservationMode::Full);
        let end = full
            .departures
            .iter()
            .map(|departure| departure.time_ns)
            .max()
            .unwrap_or(1);
        for step in 1..=4 {
            let horizon = end * step / 5 + 1;
            let prefix = scalar(&image, Some(horizon), ObservationMode::Full);
            all.push((
                format!("{name}@{horizon}"),
                checkpoint_image(&image, &prefix),
            ));
        }
        all.push((name.to_owned(), image));
    }
    all
}

/// Tier (ii): a full trace with its cutoff horizon.
fn tier_two(name: &str) -> (SimulationImage, u64) {
    let image = lower(name);
    let horizon = first_segment_end(&image) + 10_000;
    (image, horizon)
}

#[test]
fn the_reduced_traces_run_to_completion_on_scalar() {
    for name in TIER_ONE {
        let image = lower(name);
        let result = scalar(&image, None, ObservationMode::Summary);
        assert_eq!(unfinished(&result), 0, "{name}: every stage finishes");
        assert!(result.pending_events.is_empty(), "{name}: the run drains");
    }
}

#[test]
fn cpu_matches_scalar_on_the_reduced_traces_and_their_checkpoints() {
    for (label, image) in tier_one_images() {
        days_executor::validate(&image, days_executor::Backend::Scalar)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        for mode in [ObservationMode::Full, ObservationMode::Summary] {
            let expected = scalar(&image, None, mode);
            for workers in [1, 2, 4] {
                let actual = run_cpu_with_observations(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    mode,
                )
                .unwrap_or_else(|error| panic!("{label}: {error}"))
                .result;
                assert_eq!(actual, expected, "{label} {mode:?} workers={workers}");
            }
        }
    }
}

#[test]
fn cpu_runs_the_full_traces_to_their_cutoffs_as_scalar() {
    for name in TIER_TWO {
        let (image, horizon) = tier_two(name);
        for mode in [ObservationMode::Full, ObservationMode::Summary] {
            let expected = scalar(&image, Some(horizon), mode);
            assert!(
                !expected.departures.is_empty() || mode == ObservationMode::Summary,
                "{name}: the cutoff passes the start of the first cross-host collectives"
            );
            for workers in [1, 4] {
                let actual = run_cpu_with_observations(
                    &image,
                    Some(horizon),
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    mode,
                )
                .unwrap_or_else(|error| panic!("{name}: {error}"))
                .result;
                assert_eq!(
                    actual, expected,
                    "{name}@{horizon} {mode:?} workers={workers}"
                );
            }
        }
    }
}

/// Tier (iii): SimAI's b4 run to completion (51.8 M data packets).
#[test]
#[ignore = "acceptance: b4 to completion (51.8 M data packets); run explicitly in release"]
fn b4_runs_to_completion() {
    let image = lower(B4);
    let expected = scalar(&image, None, ObservationMode::Summary);
    assert_eq!(unfinished(&expected), 0, "every stage finishes");
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
        ObservationMode::Summary,
    )
    .expect("CPU runs b4")
    .result;
    assert_eq!(actual, expected);
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{
        B4, TIER_TWO, lower, scalar, tier_one_images, tier_two, unfinished, without_diagnostics,
    };

    #[test]
    fn cuda_matches_scalar_on_the_reduced_traces_and_their_checkpoints() {
        for (label, image) in tier_one_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual =
                        run_cuda_with_observations(&image, horizon, CudaConfig::default(), mode)
                            .unwrap_or_else(|error| panic!("{label}: {error}"))
                            .result;
                    assert_eq!(actual, expected, "{label} horizon={horizon:?} {mode:?}");
                }
            }
        }
    }

    #[test]
    fn cuda_runs_the_full_traces_to_their_cutoffs_as_scalar() {
        for name in TIER_TWO {
            let (image, horizon) = tier_two(name);
            let mode = ObservationMode::Summary;
            let expected = without_diagnostics(scalar(&image, Some(horizon), mode));
            let actual =
                run_cuda_with_observations(&image, Some(horizon), CudaConfig::default(), mode)
                    .unwrap_or_else(|error| panic!("{name}: {error}"))
                    .result;
            assert_eq!(actual, expected, "{name}@{horizon} {mode:?}");
        }
    }

    #[test]
    #[ignore = "acceptance: b4 to completion on CUDA; run explicitly in release"]
    fn cuda_runs_b4_to_completion_as_scalar() {
        let image = lower(B4);
        let expected = scalar(&image, None, ObservationMode::Summary);
        assert_eq!(unfinished(&expected), 0);
        let actual = run_cuda_with_observations(
            &image,
            None,
            CudaConfig::default(),
            ObservationMode::Summary,
        )
        .expect("CUDA runs b4")
        .result;
        assert_eq!(actual, without_diagnostics(expected));
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{
        B4, TIER_TWO, lower, scalar, tier_one_images, tier_two, unfinished, without_diagnostics,
    };

    #[test]
    fn metal_matches_scalar_on_the_reduced_traces_and_their_checkpoints() {
        for (label, image) in tier_one_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual =
                        run_metal_with_observations(&image, horizon, MetalConfig::default(), mode)
                            .unwrap_or_else(|error| panic!("{label}: {error}"))
                            .result;
                    assert_eq!(actual, expected, "{label} horizon={horizon:?} {mode:?}");
                }
            }
        }
    }

    #[test]
    fn metal_runs_the_full_traces_to_their_cutoffs_as_scalar() {
        for name in TIER_TWO {
            let (image, horizon) = tier_two(name);
            let mode = ObservationMode::Summary;
            let expected = without_diagnostics(scalar(&image, Some(horizon), mode));
            let actual =
                run_metal_with_observations(&image, Some(horizon), MetalConfig::default(), mode)
                    .unwrap_or_else(|error| panic!("{name}: {error}"))
                    .result;
            assert_eq!(actual, expected, "{name}@{horizon} {mode:?}");
        }
    }

    #[test]
    #[ignore = "acceptance: b4 to completion on Metal; run explicitly in release"]
    fn metal_runs_b4_to_completion_as_scalar() {
        let image = lower(B4);
        let expected = scalar(&image, None, ObservationMode::Summary);
        assert_eq!(unfinished(&expected), 0);
        let actual = run_metal_with_observations(
            &image,
            None,
            MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("Metal runs b4")
        .result;
        assert_eq!(actual, without_diagnostics(expected));
    }
}

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
//!   fidelity, and Megatron fidelity with imbalanced routing) lower in full and run to a cutoff
//!   past the start of their first cross-host collectives (the end of the first fused segment
//!   plus 10 us): CPU under Full and Summary, the devices under Summary. Full
//!   observation on a device sizes its logs for the whole run, not the cutoff: b4's 1,920 ring
//!   stages of 26,985 packets each exhaust CUDA's 20 GB on sim (result-plane upload out of memory)
//!   and stall Metal; that device-side finding is recorded in the lane report. The devices' Full
//!   identity on an AICB image is tier (i)'s. The flagship's cutoff runs are
//!   `tests/p16_aicb_flagship.rs`'s.
//! - **Devices:** every device tier runs under the `days` CLI's stock capacity caps
//!   (`days::STOCK_CAPACITY_CAPS`; user ruling Oct 8, P16 ecnbytes). The byte-unit ECN queues
//!   (32 MiB) make the uncapped default plan bound each switch queue by 32 MiB / 60 B records:
//!   59.3 GB for the smoke and 11.6 GB for b4, against 2.8 GB and 70 MB capped.
//! - **Tier (iii):** b4 to completion, every stage finished, Scalar against CPU (`#[ignore]`:
//!   51.8 M data packets; run it explicitly as the acceptance test). Devices: the `#[ignore]`
//!   tests in the device modules.
//! - **Smoke completion (P16 milestone 3):** the MoE smoke (`smoke-simai.toml`) to completion,
//!   every stage finished: Scalar against CPU (4 workers), and Metal and CUDA against Scalar
//!   (`#[ignore]`; run them explicitly in release). `smoke_finish_time` pins the simulated finish.

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

const SMOKE: &str = "smoke-simai.toml";

/// The smoke's Scalar result to completion, checked finished and drained. The state is printed
/// before the checks, so a run that stops unfinished still reports it.
fn smoke_expected(image: &SimulationImage) -> RunResult {
    let expected = scalar(image, None, ObservationMode::Summary);
    eprintln!(
        "SMOKE scalar: stop_time_ns={} unfinished={} pending={} hosts={} switches={} summary={:?}",
        image.stop_time_ns,
        unfinished(&expected),
        expected.pending_events.len(),
        expected.host_states.len(),
        expected.switch_states.len(),
        expected.summary
    );
    assert_eq!(unfinished(&expected), 0, "every stage finishes");
    assert!(
        expected.pending_events.is_empty(),
        "the event queue drains: {} pending",
        expected.pending_events.len()
    );
    expected
}

/// Compares a backend's smoke result to Scalar field by field (so a failure names the first
/// differing plane), then as a whole.
#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn assert_smoke_identical(backend: &str, actual: &RunResult, expected: &RunResult) {
    assert_eq!(actual.summary, expected.summary, "{backend}: RunSummary");
    assert_eq!(unfinished(actual), 0, "{backend}: every stage finishes");
    assert!(
        actual.pending_events.is_empty(),
        "{backend}: the queue drains"
    );
    assert_eq!(
        actual.host_states, expected.host_states,
        "{backend}: host states"
    );
    assert_eq!(
        actual.switch_states, expected.switch_states,
        "{backend}: switch states"
    );
    assert_eq!(actual, expected, "{backend}: the whole result");
    eprintln!(
        "SMOKE {backend}: identical to Scalar (summary, host states, switch states, whole result)"
    );
}

/// The MoE smoke run to completion: every stage finished, and Scalar equals CPU (4 workers).
#[test]
#[ignore = "acceptance: the MoE smoke to completion; run explicitly in release"]
fn smoke_runs_to_completion_on_scalar_and_cpu() {
    let image = lower(SMOKE);
    let expected = smoke_expected(&image);
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
        ObservationMode::Summary,
    )
    .expect("CPU runs the smoke")
    .result;
    assert_eq!(actual, expected);
    eprintln!("SMOKE cpu4: identical to Scalar");
}

/// The smoke's simulated finish: the smallest exclusive horizon with no unfinished stage, minus
/// 1 ns (the time of the event that finishes the last stage), by bisection on Scalar from the end
/// of the first fused segment (every stage is still unfinished there) to the scenario's stop time.
#[test]
#[ignore = "acceptance: pins the smoke's simulated finish; run explicitly in release"]
fn smoke_finish_time() {
    let image = lower(SMOKE);
    let mut lo = first_segment_end(&image);
    let mut hi = image.stop_time_ns;
    let mut lo_result = scalar(&image, Some(lo), ObservationMode::Summary);
    assert!(unfinished(&lo_result) > 0, "unfinished at {lo}");
    assert_eq!(
        unfinished(&scalar(&image, Some(hi), ObservationMode::Summary)),
        0,
        "finished by {hi}"
    );
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        let result = scalar(
            &checkpoint_image(&image, &lo_result),
            Some(mid),
            ObservationMode::Summary,
        );
        if unfinished(&result) > 0 {
            lo = mid;
            lo_result = result;
        } else {
            hi = mid;
        }
    }
    let next = scalar(
        &checkpoint_image(&image, &lo_result),
        Some(lo + 1),
        ObservationMode::Summary,
    );
    assert_eq!(unfinished(&next), 0);
    eprintln!(
        "SMOKE T_finish_ns={lo} (unfinished at horizon {lo}: {}; at {}: 0; pending at {}: {}) stop_time_ns={}",
        unfinished(&lo_result),
        lo + 1,
        lo + 1,
        next.pending_events.len(),
        image.stop_time_ns
    );
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    fn cuda_config() -> CudaConfig {
        CudaConfig {
            capacity_caps: days::STOCK_CAPACITY_CAPS,
            ..CudaConfig::default()
        }
    }

    use super::{
        B4, SMOKE, TIER_TWO, assert_smoke_identical, lower, scalar, smoke_expected,
        tier_one_images, tier_two, unfinished, without_diagnostics,
    };

    #[test]
    fn cuda_matches_scalar_on_the_reduced_traces_and_their_checkpoints() {
        for (label, image) in tier_one_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual = run_cuda_with_observations(&image, horizon, cuda_config(), mode)
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
            let actual = run_cuda_with_observations(&image, Some(horizon), cuda_config(), mode)
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
        let actual =
            run_cuda_with_observations(&image, None, cuda_config(), ObservationMode::Summary)
                .expect("CUDA runs b4")
                .result;
        assert_eq!(actual, without_diagnostics(expected));
    }

    #[test]
    #[ignore = "acceptance: the MoE smoke to completion on CUDA; run explicitly in release"]
    fn cuda_runs_the_smoke_to_completion_as_scalar() {
        let image = lower(SMOKE);
        let expected = without_diagnostics(smoke_expected(&image));
        let actual =
            run_cuda_with_observations(&image, None, cuda_config(), ObservationMode::Summary)
                .expect("CUDA runs the smoke")
                .result;
        assert_smoke_identical("cuda", &actual, &expected);
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    fn metal_config() -> MetalConfig {
        MetalConfig {
            capacity_caps: days::STOCK_CAPACITY_CAPS,
            ..MetalConfig::default()
        }
    }

    use super::{
        B4, SMOKE, TIER_TWO, assert_smoke_identical, lower, scalar, smoke_expected,
        tier_one_images, tier_two, unfinished, without_diagnostics,
    };

    #[test]
    fn metal_matches_scalar_on_the_reduced_traces_and_their_checkpoints() {
        for (label, image) in tier_one_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual = run_metal_with_observations(&image, horizon, metal_config(), mode)
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
            let actual = run_metal_with_observations(&image, Some(horizon), metal_config(), mode)
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
        let actual =
            run_metal_with_observations(&image, None, metal_config(), ObservationMode::Summary)
                .expect("Metal runs b4")
                .result;
        assert_eq!(actual, without_diagnostics(expected));
    }

    #[test]
    #[ignore = "acceptance: the MoE smoke to completion on Metal; run explicitly in release"]
    fn metal_runs_the_smoke_to_completion_as_scalar() {
        let image = lower(SMOKE);
        let expected = without_diagnostics(smoke_expected(&image));
        let actual =
            run_metal_with_observations(&image, None, metal_config(), ObservationMode::Summary)
                .expect("Metal runs the smoke")
                .result;
        assert_smoke_identical("metal", &actual, &expected);
    }
}

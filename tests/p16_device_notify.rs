//! P16 H2: the stage notify on the device backends, byte-identical to the Scalar oracle.
//!
//! The images are the notify fixtures of `tests/p16_stage_notify.rs` (a RoCE ring all-reduce and
//! a TCP AllGather on the miniature rail, whose same-server hops are notifies) and their
//! checkpoints, which hold notifies unreleased, timed, in flight and delivered. Identity is the
//! complete state under full observation with Scalar's diagnostics stripped, and the summary-mode
//! result, at the end and at mid-run horizons (the device's own readback of in-flight notifies),
//! on every stream and block configuration. Capacity retries are pinned.

#![cfg(any(
    feature = "cuda",
    feature = "cuda-planner-test",
    all(feature = "metal", target_vendor = "apple")
))]

#[path = "p16_stage_notify.rs"]
#[allow(dead_code)]
mod notify;

use days_executor::{ObservationMode, RunResult, SimulationImage, run_scalar_with_observations};

/// Capacity retries per notify fixture on the default plan, MEASURED at P16 H2 (Metal on the
/// M5 Max, CUDA on sim): none.
#[allow(dead_code)]
const PINNED_RETRIES: &[(&str, usize)] = &[("rail_mini_roce", 0), ("rail_mini_tcp_allgather", 0)];

#[allow(dead_code)]
pub fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    let mut expected =
        run_scalar_with_observations(image, horizon, mode).expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

/// The end, and horizons that cut notifies in flight: a quarter, a half and three quarters of
/// the stop, and each checkpoint's own next events. (The helpers are dead under
/// `cuda-planner-test` alone, which builds no device runner.)
#[allow(dead_code)]
fn horizons(image: &SimulationImage) -> Vec<Option<u64>> {
    let stop = image.stop_time_ns;
    let mut horizons = vec![None, Some(stop / 4), Some(stop / 2), Some(stop / 4 * 3)];
    if let Some(first) = image.initial_events.first() {
        horizons.push(Some(first.key.time_ns + 1));
    }
    horizons
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{PINNED_RETRIES, horizons, notify, scalar};

    fn run(
        image: &days_executor::SimulationImage,
        horizon: Option<u64>,
        mode: ObservationMode,
        (streams_enabled, round_threads_per_block): (bool, usize),
    ) -> Result<days_executor::RunResult, String> {
        run_cuda_with_observations(
            image,
            horizon,
            CudaConfig {
                streams_enabled,
                round_threads_per_block,
                ..CudaConfig::default()
            },
            mode,
        )
        .map(|run| run.result)
        .map_err(|error| error.to_string())
    }

    #[test]
    fn cuda_notify_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in notify::notify_images(7) {
            for horizon in horizons(&image) {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = scalar(&image, horizon, mode);
                    for config in [(true, 256), (true, 32), (false, 32)] {
                        let actual = run(&image, horizon, mode, config).unwrap_or_else(|error| {
                            panic!("{name} {horizon:?} {mode:?} {config:?}: {error}")
                        });
                        assert_eq!(actual, expected, "{name} {horizon:?} {mode:?} {config:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn cuda_notify_fixtures_pin_their_capacity_retries() {
        let retries = notify::notify_fixtures()
            .into_iter()
            .map(|(name, image)| {
                let run = run_cuda_with_observations(
                    &image,
                    None,
                    CudaConfig::default(),
                    ObservationMode::Summary,
                )
                .unwrap_or_else(|error| panic!("{name}: {error}"));
                (name, run.capacity_retry_trace.len())
            })
            .collect::<Vec<_>>();
        eprintln!("record=notify_retries backend=cuda {retries:?}");
        assert_eq!(
            retries,
            PINNED_RETRIES
                .iter()
                .map(|(name, count)| (name.to_string(), *count))
                .collect::<Vec<_>>()
        );
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{PINNED_RETRIES, horizons, notify, scalar};

    fn run(
        image: &days_executor::SimulationImage,
        horizon: Option<u64>,
        mode: ObservationMode,
        (streams_enabled, round_threads_per_threadgroup): (bool, usize),
    ) -> Result<days_executor::RunResult, String> {
        run_metal_with_observations(
            image,
            horizon,
            MetalConfig {
                streams_enabled,
                round_threads_per_threadgroup,
                ..MetalConfig::default()
            },
            mode,
        )
        .map(|run| run.result)
        .map_err(|error| error.to_string())
    }

    #[test]
    fn metal_notify_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in notify::notify_images(7) {
            for horizon in horizons(&image) {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = scalar(&image, horizon, mode);
                    for config in [(true, 256), (true, 32), (false, 32)] {
                        let actual = run(&image, horizon, mode, config).unwrap_or_else(|error| {
                            panic!("{name} {horizon:?} {mode:?} {config:?}: {error}")
                        });
                        assert_eq!(actual, expected, "{name} {horizon:?} {mode:?} {config:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn metal_notify_fixtures_pin_their_capacity_retries() {
        let retries = notify::notify_fixtures()
            .into_iter()
            .map(|(name, image)| {
                let run = run_metal_with_observations(
                    &image,
                    None,
                    MetalConfig::default(),
                    ObservationMode::Summary,
                )
                .unwrap_or_else(|error| panic!("{name}: {error}"));
                (name, run.capacity_retry_trace.len())
            })
            .collect::<Vec<_>>();
        eprintln!("record=notify_retries backend=metal {retries:?}");
        assert_eq!(
            retries,
            PINNED_RETRIES
                .iter()
                .map(|(name, count)| (name.to_string(), *count))
                .collect::<Vec<_>>()
        );
    }
}

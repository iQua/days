//! P16 lane D2 (fullobs): a device plan under Full observation sizes its observation logs for the
//! run actually requested, not for the whole lifetime of every flow.
//!
//! `configs/p16/roce_long_flow_cutoff.toml` is b4's shape in miniature (`days-gpu/evidence/P16/
//! aicb-impl/report.md`, finding 1): every queue pair is Blocked behind a long compute, its pacing
//! grid is anchored at 0, and each carries 10 MB, so the whole run holds about 40,000 packets per
//! flow. Run to a cutoff a few microseconds past the compute's end, the run needs a few hundred
//! observation records. The planner used to size the logs from the whole run's packet counts
//! (about 2 GB here, 186 GB for b4, which ran CUDA out of memory and made Metal spin until its
//! round bound ran out).
//!
//! Capacity on the device is refuse-or-run, never semantics: the result must equal Scalar's Full
//! result, and with the horizon-bounded sizing no attempt is discarded for observation capacity.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    DeviceSizingReport, ObservationMode, RunResult, SimulationImage, run_scalar_with_observations,
};

const FIXTURE: &str = "configs/p16/roce_long_flow_cutoff.toml";

/// Offsets past the first event (the end of the 1 ms compute) the tests cut the run at: before
/// any packet departs, while the first packets are in flight, and after 50 departures.
const CUTOFFS_NS: [u64; 4] = [1, 10_000, 50_000, 100_000];

/// The observation logs of a cutoff run fit in 1 MiB: at most `cutoff / 1,000 ns + 2` pacing
/// ticks per queue pair, so at most 102 data packets and as many ACKs per flow, over 24 queue pairs
/// and routes of at most 4 links, at 256 bytes per record slot across the three logs.
const CUTOFF_OBSERVATION_BYTES: usize = 1 << 20;

fn image() -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn first_event_ns(image: &SimulationImage) -> u64 {
    image
        .initial_events
        .iter()
        .map(|event| event.key.time_ns)
        .min()
        .expect("the fixture has initial events")
}

fn observation_bytes(report: &DeviceSizingReport) -> usize {
    report
        .planes
        .iter()
        .filter(|plane| matches!(plane.name, "observed" | "departures" | "arrivals"))
        .map(|plane| plane.bytes)
        .sum()
}

fn scalar_full(image: &SimulationImage, horizon: u64) -> RunResult {
    let mut result = run_scalar_with_observations(image, Some(horizon), ObservationMode::Full)
        .expect("the Scalar oracle runs");
    result.diagnostics = None;
    result
}

#[test]
fn the_fixture_is_a_long_flow_image_cut_early() {
    let image = image();
    let first = first_event_ns(&image);
    assert_eq!(first, 1_000_000, "the first event is the end of `forward`");
    let full = scalar_full(&image, first + 50_000);
    assert_eq!(
        full.departures.len(),
        50,
        "the +50 us cutoff sees the rings start"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{
        MetalConfig, ObservationMode, run_metal_with_observations, size_metal_plan_for_testing,
    };

    use super::{
        CUTOFF_OBSERVATION_BYTES, CUTOFFS_NS, first_event_ns, image, observation_bytes, scalar_full,
    };

    #[test]
    fn metal_full_observation_logs_are_sized_for_the_cutoff() {
        let image = image();
        let first = first_event_ns(&image);
        for cutoff in CUTOFFS_NS {
            let report = size_metal_plan_for_testing(
                &image,
                Some(first + cutoff),
                MetalConfig::default(),
                ObservationMode::Full,
            )
            .expect("the Metal plan sizes");
            let bytes = observation_bytes(&report);
            assert!(
                bytes <= CUTOFF_OBSERVATION_BYTES,
                "+{cutoff} ns: {bytes} observation bytes"
            );
        }
    }

    #[test]
    fn metal_full_matches_scalar_at_each_cutoff_without_a_retry() {
        let image = image();
        let first = first_event_ns(&image);
        for cutoff in CUTOFFS_NS {
            let horizon = first + cutoff;
            let run = run_metal_with_observations(
                &image,
                Some(horizon),
                MetalConfig::default(),
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("+{cutoff} ns: {error}"));
            assert_eq!(run.result, scalar_full(&image, horizon), "+{cutoff} ns");
            assert!(
                run.capacity_retry_trace.is_empty(),
                "+{cutoff} ns: {:?}",
                run.capacity_retry_trace
            );
        }
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{
        CudaConfig, ObservationMode, run_cuda_with_observations, size_cuda_plan_for_testing,
    };

    use super::{
        CUTOFF_OBSERVATION_BYTES, CUTOFFS_NS, first_event_ns, image, observation_bytes, scalar_full,
    };

    #[test]
    fn cuda_full_observation_logs_are_sized_for_the_cutoff() {
        let image = image();
        let first = first_event_ns(&image);
        for cutoff in CUTOFFS_NS {
            let report = size_cuda_plan_for_testing(
                &image,
                Some(first + cutoff),
                CudaConfig::default(),
                ObservationMode::Full,
            )
            .expect("the CUDA plan sizes");
            let bytes = observation_bytes(&report);
            assert!(
                bytes <= CUTOFF_OBSERVATION_BYTES,
                "+{cutoff} ns: {bytes} observation bytes"
            );
        }
    }

    #[test]
    fn cuda_full_matches_scalar_at_each_cutoff_without_a_retry() {
        let image = image();
        let first = first_event_ns(&image);
        for cutoff in CUTOFFS_NS {
            let horizon = first + cutoff;
            let run = run_cuda_with_observations(
                &image,
                Some(horizon),
                CudaConfig::default(),
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("+{cutoff} ns: {error}"));
            assert_eq!(run.result, scalar_full(&image, horizon), "+{cutoff} ns");
            assert!(
                run.capacity_retry_trace.is_empty(),
                "+{cutoff} ns: {:?}",
                run.capacity_retry_trace
            );
        }
    }
}

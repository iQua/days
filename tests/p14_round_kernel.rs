//! P14 spec: the two builds of `days_round`, selected per run from the image.
//!
//! - Selection: every `configs/p14/` fixture and its checkpoints select the mechanisms build; the
//!   evaluation cells and plain scheduler images select the plain build.
//! - Fail closed: forced onto the plain build, every `configs/p14/` fixture stops with
//!   `MechanismsKernelRequired` and produces no result.
//! - Forced onto the mechanisms build, images without DCQCN or PFC state reproduce the plain
//!   build's bytes and the Scalar oracle's, so a selection error can only cost time.

#![cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]

use std::fs;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, GeneratorStatus, ObservationMode, RoundKernel, RunResult, SimulationImage,
    run_scalar_with_observations, validate,
};

const P14_FIXTURES: [&str; 9] = [
    "dcqcn_10s_zero_xoff.toml",
    "dcqcn_1s_zero_xoff.toml",
    "dcqcn_2s_zero_xoff.toml",
    "dcqcn_multi_zero_xoff.toml",
    "dcqcn_simple_zero_xoff.toml",
    "dcqcn_t26_pfc.toml",
    "dcqcn_t26.toml",
    "leanguard_dcqcn_zero_xoff.toml",
    "leanguard_pfc_executable.toml",
];

/// The six evaluation cells in the repository (E5 active's fixture lives in days-gpu).
const PLAIN_CELLS: [&str; 6] = [
    "configs/benchmarks/evaluation/e5_wide_k32_q200.toml",
    "configs/benchmarks/evaluation/e6_cbr_k32_load_01.toml",
    "configs/benchmarks/evaluation/e6_cbr_k32_load_10.toml",
    "configs/benchmarks/evaluation/e6_cbr_k32_load_30.toml",
    "configs/benchmarks/evaluation/e6_cbr_k32_load_60.toml",
    "configs/benchmarks/lookahead/rq9_frontier_closed_k32.toml",
];

const DISCIPLINES: [&str; 5] = ["FIFO", "SP", "WFQ", "DRR", "WRR"];

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn fixture(name: &str) -> SimulationImage {
    lower(&format!("configs/p14/{name}"))
}

/// A three-source incast under `discipline`, without PFC or DCQCN: every scheduler path whose PFC
/// branch the plain build compiles out (DRR and WRR class scans, WFQ removal, SP) runs.
fn scheduler_image(discipline: &str) -> SimulationImage {
    let directory = tempfile::TempDir::new().expect("temporary directory");
    let path = directory.path().join("plain_incast.toml");
    let flow = |source: u32, priority: u8, size: u64, delay: &str| {
        format!(
            r#"
[[flow]]
flow_type = "PacketDistribution"
priority = {priority}
graph = [[{source}, 3]]

[flow.traffic]
initial_delay = {delay}
size = {size}
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}
"#
        )
    };
    let config = format!(
        r#"
seed = 14
edges = [[0, 2], [1, 2], [2, 3]]
hosts = [0, 1, 2, 3]
duration = 0.001

[switch]
port_rate = 1_000_000_000
capacity = 40
weights = [3, 1, 2]
priorities = [3, 2, 1]
discipline = "{discipline}"
drop = "TailDrop"
{}{}{}{}"#,
        flow(0, 3, 100_000, "0.0"),
        flow(1, 1, 100_000, "0.000002"),
        flow(0, 0, 60_000, "0.000001"),
        flow(2, 3, 60_000, "0.000003"),
    );
    fs::write(&path, config).expect("scenario must be written");
    compile_config(&path).unwrap_or_else(|error| panic!("{discipline} incast must lower: {error}"))
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

/// The Scalar oracle's result, for the device tests that run only under the test hooks.
#[cfg(any(
    all(feature = "metal-test-hooks", target_vendor = "apple"),
    feature = "cuda-test-hooks"
))]
fn scalar(image: &SimulationImage, horizon: Option<u64>) -> RunResult {
    let mut expected = run_scalar_with_observations(image, horizon, ObservationMode::Full)
        .expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

/// A DCQCN fixture lowered without its (zero-XOFF, inert) PFC link section: the plain build must
/// then fail closed through the DCQCN checks alone, not the PFC entry check.
fn without_pfc(name: &str) -> Option<SimulationImage> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p14")
        .join(name);
    let source = fs::read_to_string(&path).expect("fixture must be readable");
    let mut kept = Vec::new();
    let mut in_pfc_table = false;
    let mut had_pfc = false;
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_pfc_table = trimmed == "[link.pfc]";
        }
        if in_pfc_table || trimmed == r#"mode = "Pfc""# {
            had_pfc = true;
            continue;
        }
        kept.push(line);
    }
    if !had_pfc {
        return None;
    }
    let directory = tempfile::TempDir::new().expect("temporary directory");
    let stripped = directory.path().join(name);
    fs::write(&stripped, kept.join("\n")).expect("scenario must be written");
    let image = compile_config(&stripped)
        .unwrap_or_else(|error| panic!("{name} without PFC must lower: {error}"));
    assert!(!image_has_pfc_state(&image), "{name}");
    validate(&image, Backend::Scalar)
        .unwrap_or_else(|error| panic!("{name} without PFC must validate: {error}"));
    Some(image)
}

/// The fixtures the plain build must refuse: every `configs/p14/` fixture, plus each DCQCN
/// fixture without its PFC state where that validates.
fn fail_closed_fixtures() -> Vec<(String, SimulationImage)> {
    let mut images = Vec::new();
    for name in P14_FIXTURES {
        let image = fixture(name);
        if name.starts_with("dcqcn") || name.starts_with("leanguard_dcqcn") {
            if let Some(stripped) = without_pfc(name) {
                images.push((format!("{name} without PFC"), stripped));
            }
        }
        images.push((name.to_owned(), image));
    }
    images
}

#[test]
fn the_dcqcn_checks_are_exercised_without_pfc_state() {
    let dcqcn_only = fail_closed_fixtures()
        .into_iter()
        .filter(|(_, image)| !image_has_pfc_state(image))
        .map(|(name, _)| name)
        .collect::<Vec<_>>();
    // dcqcn_t26 has no PFC state; each of the seven DCQCN fixtures with PFC contributes a variant.
    assert_eq!(dcqcn_only.len(), 8, "{dcqcn_only:?}");
}

fn image_has_pfc_state(image: &SimulationImage) -> bool {
    image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .any(|queue| queue.pfc.is_some())
}

/// Every `configs/p14/` fixture, and checkpoints of each taken across its run, down to the tail
/// where every DCQCN generator has finished and only in-flight data and control timers remain.
fn mechanism_images() -> Vec<(String, SimulationImage)> {
    let mut images = Vec::new();
    for (name, image) in fail_closed_fixtures() {
        for step in 1..8 {
            let horizon = image.stop_time_ns / 8 * step;
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            images.push((
                format!("{name}@{horizon}"),
                checkpoint_image(&image, &prefix),
            ));
        }
        images.push((name, image));
    }
    images
}

#[test]
fn every_p14_fixture_and_checkpoint_selects_the_mechanisms_round_kernel() {
    for (name, image) in mechanism_images() {
        assert_eq!(
            RoundKernel::for_image(&image),
            RoundKernel::Mechanisms,
            "{name}"
        );
    }
}

#[test]
fn evaluation_cells_and_plain_scheduler_images_select_the_plain_round_kernel() {
    for relative in PLAIN_CELLS {
        assert_eq!(
            RoundKernel::for_image(&lower(relative)),
            RoundKernel::Plain,
            "{relative}"
        );
    }
    for discipline in DISCIPLINES {
        assert_eq!(
            RoundKernel::for_image(&scheduler_image(discipline)),
            RoundKernel::Plain,
            "{discipline}"
        );
    }
}

/// The receiver-only gap (see `evidence/P14/spec.md`): a checkpoint in which DCQCN data is still
/// in flight to its notification point after the flow's generator has finished. On the plain
/// build nothing but a later control timer would stop such a run, so selection must pick the
/// mechanisms build from the notification point alone.
#[test]
fn a_dcqcn_tail_checkpoint_selects_the_mechanisms_round_kernel() {
    let tails = dcqcn_tail_checkpoints();
    assert!(
        !tails.is_empty(),
        "dcqcn_t26 must have a finished-generator tail"
    );
    for (horizon, tail) in &tails {
        assert_eq!(
            RoundKernel::for_image(tail),
            RoundKernel::Mechanisms,
            "@{horizon}"
        );
    }
}

/// Checkpoints of `dcqcn_t26` taken at each packet arrival of its Scalar run, keeping those in
/// which the DCQCN generator no longer has a pacing timer and DCQCN data is still in flight.
fn dcqcn_tail_checkpoints() -> Vec<(u64, SimulationImage)> {
    let image = fixture("dcqcn_t26.toml");
    let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar oracle must run");
    let mut horizons = full
        .arrivals
        .iter()
        .map(|arrival| arrival.time_ns)
        .collect::<Vec<_>>();
    horizons.sort_unstable();
    horizons.dedup();
    horizons
        .into_iter()
        .filter_map(|horizon| {
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            let timers_done = prefix
                .host_states
                .iter()
                .flat_map(|state| &state.generators)
                .all(|generator| {
                    !matches!(
                        generator.next_emission.status,
                        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                    )
                });
            let data_in_flight = prefix
                .resident_packets
                .iter()
                .any(|packet| packet.kind.is_data());
            if timers_done && data_in_flight {
                Some((horizon, checkpoint_image(&image, &prefix)))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
mod metal {
    use days_executor::{
        MetalConfig, MetalError, ObservationMode, RoundKernel, run_metal_with_observations,
    };

    use super::{DISCIPLINES, fail_closed_fixtures, mechanism_images, scalar, scheduler_image};

    fn forced(round_kernel: RoundKernel, round_threads_per_threadgroup: usize) -> MetalConfig {
        MetalConfig {
            round_threads_per_threadgroup,
            round_kernel_override: Some(round_kernel),
            ..MetalConfig::default()
        }
    }

    #[test]
    fn metal_plain_round_kernel_fails_closed_on_every_p14_fixture_and_checkpoint() {
        for (name, image) in fail_closed_fixtures() {
            let error = run_metal_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 256),
                ObservationMode::Full,
            )
            .expect_err("the plain build must not run DCQCN or PFC state");
            assert!(
                matches!(error, MetalError::MechanismsKernelRequired { .. }),
                "{name}: {error}"
            );
        }
        // A checkpoint with work left must stop; one with no pending event runs no transition,
        // so the plain build's result is the unchanged image, which is Scalar's.
        for (name, image) in mechanism_images() {
            match run_metal_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 32),
                ObservationMode::Full,
            ) {
                Err(MetalError::MechanismsKernelRequired { .. }) => {}
                Ok(run) if image.initial_events.is_empty() => {
                    assert_eq!(run.transitions, 0, "{name}");
                    assert_eq!(run.result, scalar(&image, None), "{name}");
                }
                other => panic!("{name}: expected a fail-closed stop, got {other:?}"),
            }
        }
    }

    #[test]
    fn metal_mechanisms_round_kernel_reproduces_plain_images() {
        for discipline in DISCIPLINES {
            let image = scheduler_image(discipline);
            let expected = scalar(&image, None);
            for round_threads in [32, 256] {
                let selected = run_metal_with_observations(
                    &image,
                    None,
                    MetalConfig {
                        round_threads_per_threadgroup: round_threads,
                        ..MetalConfig::default()
                    },
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| panic!("{discipline}: {error}"));
                assert_eq!(selected.round_kernel, RoundKernel::Plain);
                assert_eq!(selected.result, expected, "{discipline} plain");
                let mechanisms = run_metal_with_observations(
                    &image,
                    None,
                    forced(RoundKernel::Mechanisms, round_threads),
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| panic!("{discipline}: {error}"));
                assert_eq!(mechanisms.round_kernel, RoundKernel::Mechanisms);
                assert_eq!(mechanisms.result, expected, "{discipline} mechanisms");
            }
        }
    }
}

#[cfg(feature = "cuda-test-hooks")]
mod cuda {
    use days_executor::{
        CudaConfig, CudaError, ObservationMode, RoundKernel, run_cuda_with_observations,
    };

    use super::{DISCIPLINES, fail_closed_fixtures, mechanism_images, scalar, scheduler_image};

    fn forced(round_kernel: RoundKernel, round_threads_per_block: usize) -> CudaConfig {
        CudaConfig {
            round_threads_per_block,
            round_kernel_override: Some(round_kernel),
            ..CudaConfig::default()
        }
    }

    #[test]
    fn cuda_plain_round_kernel_fails_closed_on_every_p14_fixture_and_checkpoint() {
        for (name, image) in fail_closed_fixtures() {
            let error = run_cuda_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 256),
                ObservationMode::Full,
            )
            .expect_err("the plain build must not run DCQCN or PFC state");
            assert!(
                matches!(error, CudaError::MechanismsKernelRequired { .. }),
                "{name}: {error}"
            );
        }
        // A checkpoint with work left must stop; one with no pending event runs no transition,
        // so the plain build's result is the unchanged image, which is Scalar's.
        for (name, image) in mechanism_images() {
            match run_cuda_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 32),
                ObservationMode::Full,
            ) {
                Err(CudaError::MechanismsKernelRequired { .. }) => {}
                Ok(run) if image.initial_events.is_empty() => {
                    assert_eq!(run.transitions, 0, "{name}");
                    assert_eq!(run.result, scalar(&image, None), "{name}");
                }
                other => panic!("{name}: expected a fail-closed stop, got {other:?}"),
            }
        }
    }

    #[test]
    fn cuda_mechanisms_round_kernel_reproduces_plain_images() {
        for discipline in DISCIPLINES {
            let image = scheduler_image(discipline);
            let expected = scalar(&image, None);
            for round_threads in [32, 256] {
                let selected = run_cuda_with_observations(
                    &image,
                    None,
                    CudaConfig {
                        round_threads_per_block: round_threads,
                        ..CudaConfig::default()
                    },
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| panic!("{discipline}: {error}"));
                assert_eq!(selected.round_kernel, RoundKernel::Plain);
                assert_eq!(selected.result, expected, "{discipline} plain");
                let mechanisms = run_cuda_with_observations(
                    &image,
                    None,
                    forced(RoundKernel::Mechanisms, round_threads),
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| panic!("{discipline}: {error}"));
                assert_eq!(mechanisms.round_kernel, RoundKernel::Mechanisms);
                assert_eq!(mechanisms.result, expected, "{discipline} mechanisms");
            }
        }
    }
}

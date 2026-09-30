//! P14 spec: the two builds of `days_round`, selected per run from the image.
//!
//! - Selection: every `configs/p14/` fixture and its checkpoints select the mechanisms build; the
//!   evaluation cells and plain scheduler images select the plain build.
//! - Fail closed: forced onto the plain build, every image with DCQCN or PFC state (the
//!   `configs/p14/` fixtures, their DCQCN-only variants, checkpoints, and the finished- and
//!   active-generator checkpoints of review F2) is refused by the host before launch with
//!   `MechanismsKernelRequired`, and produces no result.
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

/// A DCQCN fixture lowered without its (zero-XOFF, inert) PFC link section: the host must then
/// refuse the plain kernel through the plan check's DCQCN conditions (the generator row and the
/// receiver marker) alone, without the PFC region.
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
fn dcqcn_only_variants_exercise_the_plan_check_without_a_pfc_region() {
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

/// The receiver-only gap (see `evidence/P14/spec.md`): on the plain build, only a DCQCN timer event
/// (a pacing tick or a control timer) or a CNP arrival fails closed. Any run window that holds none
/// of them, but in which CE-marked data reaches a notification point, runs to completion on the
/// plain build and silently omits the CNPs. Such windows exist whenever the run's horizon falls
/// inside one pacing interval of an active generator, or after the generator finishes and before
/// the next control timer. Both were measured to diverge from Scalar on the bottleneck variants
/// below. The only guard is static, image-level selection: every checkpoint of a DCQCN image keeps
/// its notification points, so it selects the mechanisms build. These tests pin that for
/// checkpoints taken while the generator is active and after it has finished.
#[test]
fn dcqcn_tail_checkpoints_select_the_mechanisms_round_kernel() {
    for (fixture_name, image) in [
        ("dcqcn_t26", fixture("dcqcn_t26.toml")),
        ("dcqcn_t26 bottleneck", dcqcn_t26_bottleneck(None)),
    ] {
        let tails = dcqcn_checkpoints(&image, CheckpointPhase::GeneratorFinished);
        assert!(
            !tails.is_empty(),
            "{fixture_name} must have a finished-generator tail"
        );
        for (horizon, tail) in &tails {
            assert_eq!(
                RoundKernel::for_image(tail),
                RoundKernel::Mechanisms,
                "{fixture_name}@{horizon}"
            );
        }
    }
}

/// Checkpoints between the ticks of an active DCQCN generator: the lengthened bottleneck variant,
/// checkpointed at every packet arrival and 1 ns before it while the generator is `Scheduled` and
/// data is in flight. These are the windows in which the plain build was measured to return wrong
/// bytes without failing closed (the review's probe, reproduced in `evidence/P14/spec`).
#[test]
fn active_dcqcn_generator_checkpoints_select_the_mechanisms_round_kernel() {
    let image = dcqcn_t26_bottleneck(Some("size = 200_000"));
    let active = dcqcn_checkpoints(&image, CheckpointPhase::GeneratorActive);
    // The variant keeps its generator active across most of its arrivals.
    assert!(
        active.len() >= 100,
        "only {} active-generator checkpoints",
        active.len()
    );
    for (horizon, checkpoint) in &active {
        assert_eq!(
            RoundKernel::for_image(checkpoint),
            RoundKernel::Mechanisms,
            "@{horizon}"
        );
    }
}

/// `dcqcn_t26` behind a 1 Gbps bottleneck with a one-packet ECN threshold: CE-marked data keeps
/// arriving at the notification point, between the 10 Gbps generator's ticks and after it
/// finishes. `flow_size` replaces the flow's 20,000 B `size` line, to lengthen the active phase.
fn dcqcn_t26_bottleneck(flow_size: Option<&str>) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p14/dcqcn_t26.toml");
    let source = fs::read_to_string(&path).expect("fixture must be readable");
    let mut variant = source
        .replace("port_rate = 100_000_000_000", "port_rate = 1_000_000_000")
        .replace("capacity = 1\n", "capacity = 100\n")
        .replace("ecn_threshold = 1.0", "ecn_threshold = 0.01");
    if let Some(size) = flow_size {
        assert!(variant.contains("size = 20_000"));
        variant = variant.replace("size = 20_000", size);
    }
    assert_ne!(variant, source, "the variant must change the fixture");
    let directory = tempfile::TempDir::new().expect("temporary directory");
    let stripped = directory.path().join("dcqcn_t26_bottleneck.toml");
    fs::write(&stripped, variant).expect("scenario must be written");
    compile_config(&stripped).unwrap_or_else(|error| panic!("the variant must lower: {error}"))
}

/// Which DCQCN checkpoints [`dcqcn_checkpoints`] keeps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CheckpointPhase {
    /// A DCQCN generator is still `Scheduled`: windows between its pacing ticks.
    GeneratorActive,
    /// No generator has a pacing timer left: windows before the next control timer.
    GeneratorFinished,
}

/// Checkpoints of `image` taken at each packet arrival of its Scalar run (and, for the active
/// phase, 1 ns before it), keeping those in `phase` with DCQCN data still in flight.
fn dcqcn_checkpoints(
    image: &SimulationImage,
    phase: CheckpointPhase,
) -> Vec<(u64, SimulationImage)> {
    let full = run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar oracle must run");
    let mut horizons = full
        .arrivals
        .iter()
        .flat_map(|arrival| match phase {
            CheckpointPhase::GeneratorActive => {
                vec![arrival.time_ns.saturating_sub(1), arrival.time_ns]
            }
            CheckpointPhase::GeneratorFinished => vec![arrival.time_ns],
        })
        .collect::<Vec<_>>();
    horizons.sort_unstable();
    horizons.dedup();
    horizons
        .into_iter()
        .filter_map(|horizon| {
            let prefix = run_scalar_with_observations(image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            let mut statuses = prefix
                .host_states
                .iter()
                .flat_map(|state| &state.generators)
                .map(|generator| generator.next_emission.status);
            let in_phase = match phase {
                CheckpointPhase::GeneratorActive => {
                    statuses.any(|status| status == GeneratorStatus::Scheduled)
                }
                CheckpointPhase::GeneratorFinished => statuses.all(|status| {
                    !matches!(
                        status,
                        GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                    )
                }),
            };
            let data_in_flight = prefix
                .resident_packets
                .iter()
                .any(|packet| packet.kind.is_data());
            if in_phase && data_in_flight {
                Some((horizon, checkpoint_image(image, &prefix)))
            } else {
                None
            }
        })
        .collect()
}

/// Every image the plain build must refuse before launch: each `configs/p14/` fixture and DCQCN-only
/// variant with seven checkpoints of each, the finished-generator tails of `dcqcn_t26` and its
/// bottleneck variant, and the 132 active-generator checkpoints of the lengthened bottleneck
/// variant, 42 of which the plain build used to run to silently wrong bytes (review F2).
#[cfg(any(
    all(feature = "metal-test-hooks", target_vendor = "apple"),
    feature = "cuda-test-hooks"
))]
fn refused_images() -> Vec<(String, SimulationImage)> {
    let mut images = mechanism_images();
    for (name, image) in [
        ("dcqcn_t26", fixture("dcqcn_t26.toml")),
        ("dcqcn_t26 bottleneck", dcqcn_t26_bottleneck(None)),
    ] {
        for (horizon, tail) in dcqcn_checkpoints(&image, CheckpointPhase::GeneratorFinished) {
            images.push((format!("{name} tail@{horizon}"), tail));
        }
    }
    let lengthened = dcqcn_t26_bottleneck(Some("size = 200_000"));
    let active = dcqcn_checkpoints(&lengthened, CheckpointPhase::GeneratorActive);
    assert_eq!(
        active.len(),
        132,
        "the review's active-generator checkpoint set"
    );
    for (horizon, checkpoint) in active {
        images.push((format!("dcqcn_t26 lengthened active@{horizon}"), checkpoint));
    }
    images
}

#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
mod metal {
    use days_executor::{
        MetalConfig, MetalError, ObservationMode, RoundKernel, run_metal_with_observations,
    };

    use super::{DISCIPLINES, refused_images, scalar, scheduler_image};

    fn forced(round_kernel: RoundKernel, round_threads_per_threadgroup: usize) -> MetalConfig {
        MetalConfig {
            round_threads_per_threadgroup,
            round_kernel_override: Some(round_kernel),
            ..MetalConfig::default()
        }
    }

    /// The host refuses the plain kernel on every image with DCQCN or PFC state, before launch:
    /// the plain kernel carries no device-side stop, so `MechanismsKernelRequired` can only come
    /// from the host's check of the uploaded plan.
    #[test]
    fn metal_plain_round_kernel_is_refused_before_launch_on_every_mechanism_image() {
        for (name, image) in refused_images() {
            match run_metal_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 32),
                ObservationMode::Full,
            ) {
                Err(MetalError::MechanismsKernelRequired { .. }) => {}
                other => panic!("{name}: expected a refusal, got {other:?}"),
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
        CudaConfig, CudaError, CudaExecutor, ObservationMode, RoundKernel,
        run_cuda_with_observations,
    };

    use super::{DISCIPLINES, fixture, refused_images, scalar, scheduler_image};

    /// The kernels of one round module, in attempt-DAG order, then the readback gather.
    fn module_kernels(round: &'static str) -> Vec<&'static str> {
        vec![
            "days_horizon_sweep",
            "days_horizon",
            "days_round_reset",
            "days_round_prepare",
            round,
            "days_round_control_sweep",
            "days_round_control",
            "days_exchange_prefix_sweep",
            "days_exchange_prefix",
            "days_exchange_scatter",
            "days_exchange_merge",
            "days_round_finalize_sweep",
            "days_round_finalize",
            "days_compact_gather",
        ]
    }

    /// P14 round 3: each round-kernel build is its own complete module, holding only its own round
    /// kernel, and every run launches its 14 kernels from that one module. A module holding both
    /// round kernels moved the code after them and cost device time (`evidence/P14/modprobe-ab.md`).
    #[test]
    fn cuda_each_run_launches_one_complete_round_module() {
        let executor = CudaExecutor::on_device(0).expect("CUDA device 0");
        let plain = module_kernels("days_round");
        let mechanisms = module_kernels("days_round_mechanisms");
        assert_eq!(
            executor.round_module_kernels_for_testing(RoundKernel::Plain),
            plain,
            "the plain module holds exactly main's kernels"
        );
        let mut held = executor.round_module_kernels_for_testing(RoundKernel::Mechanisms);
        // The probe lists the attempt kernels in DAG order with the plain round kernel's slot
        // first; put the mechanisms round kernel back in its slot for the comparison.
        if let Some(position) = held
            .iter()
            .position(|name| *name == "days_round_mechanisms")
        {
            let name = held.remove(position);
            held.insert(4, name);
        }
        assert_eq!(
            held, mechanisms,
            "the mechanisms module holds exactly its kernels"
        );

        for (name, image, build, forced_build) in [
            (
                "FIFO incast",
                scheduler_image("FIFO"),
                RoundKernel::Plain,
                None,
            ),
            (
                "dcqcn_t26",
                fixture("dcqcn_t26.toml"),
                RoundKernel::Mechanisms,
                None,
            ),
            (
                "FIFO incast forced onto mechanisms",
                scheduler_image("FIFO"),
                RoundKernel::Mechanisms,
                Some(RoundKernel::Mechanisms),
            ),
        ] {
            let run = run_cuda_with_observations(
                &image,
                None,
                CudaConfig {
                    round_kernel_override: forced_build,
                    ..CudaConfig::default()
                },
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(run.round_kernel, build, "{name}");
            let expected = match build {
                RoundKernel::Plain => &plain,
                RoundKernel::Mechanisms => &mechanisms,
            };
            assert_eq!(
                run.launched_kernels,
                expected
                    .iter()
                    .map(|kernel| (build, *kernel))
                    .collect::<Vec<_>>(),
                "{name}: every launched kernel comes from the {build:?} module"
            );
        }
    }

    fn forced(round_kernel: RoundKernel, round_threads_per_block: usize) -> CudaConfig {
        CudaConfig {
            round_threads_per_block,
            round_kernel_override: Some(round_kernel),
            ..CudaConfig::default()
        }
    }

    /// The host refuses the plain kernel on every image with DCQCN or PFC state, before launch:
    /// the plain kernel carries no device-side stop, so `MechanismsKernelRequired` can only come
    /// from the host's check of the uploaded plan.
    #[test]
    fn cuda_plain_round_kernel_is_refused_before_launch_on_every_mechanism_image() {
        for (name, image) in refused_images() {
            match run_cuda_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 32),
                ObservationMode::Full,
            ) {
                Err(CudaError::MechanismsKernelRequired { .. }) => {}
                other => panic!("{name}: expected a refusal, got {other:?}"),
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

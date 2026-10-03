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
/// where every DCQCN generator has finished and only in-flight data and feedback remain.
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
/// (a pacing tick; in P14 also a control timer, which P16 removed) or a CNP arrival fails closed.
/// Any run window that holds none of them, but in which CE-marked data reaches a notification
/// point, runs to completion on the plain build and silently omits the CNPs. Such windows exist
/// whenever the run's horizon falls inside one pacing interval of an active generator, or after the
/// generator finishes. Both were measured (P14) to diverge from Scalar on the bottleneck variants
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
    /// No generator has a pacing timer left: windows after its last tick.
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
    /// kernel, and every run launches its 14 kernels from that one module. Since round 4 a run
    /// loads only that module, so the mixed-array probe loads the other build's module itself.
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

        // The record identifies each launched handle, not the selection: an attempt array that
        // borrows slots from the other module, as a mixed-module regression would, is recorded as
        // exactly that.
        for build in [RoundKernel::Plain, RoundKernel::Mechanisms] {
            let other = match build {
                RoundKernel::Plain => RoundKernel::Mechanisms,
                RoundKernel::Mechanisms => RoundKernel::Plain,
            };
            let own = match build {
                RoundKernel::Plain => &plain,
                RoundKernel::Mechanisms => &mechanisms,
            };
            let theirs = match build {
                RoundKernel::Plain => &mechanisms,
                RoundKernel::Mechanisms => &plain,
            };
            for borrowed in [vec![0], vec![4], vec![0, 5, 12]] {
                let expected = (0..13)
                    .map(|index| {
                        if borrowed.contains(&index) {
                            (other, theirs[index])
                        } else {
                            (build, own[index])
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    executor.launch_record_for_testing(build, &borrowed),
                    expected,
                    "{build:?} with slots {borrowed:?} borrowed from the {other:?} module"
                );
            }
        }

        // One-record channel and fallback-heap caps force capacity retries (a DCQCN host holds a
        // pacing timer and in-flight CNPs); the record is the final attempt's.
        let retrying = CudaConfig {
            max_channel_events_per_stream: Some(1),
            max_fel_events_per_lp: Some(1),
            ..CudaConfig::default()
        };
        for (name, image, build, forced_build, config, retries) in [
            (
                "FIFO incast",
                scheduler_image("FIFO"),
                RoundKernel::Plain,
                None,
                CudaConfig::default(),
                false,
            ),
            (
                "dcqcn_t26",
                fixture("dcqcn_t26.toml"),
                RoundKernel::Mechanisms,
                None,
                CudaConfig::default(),
                false,
            ),
            (
                "FIFO incast forced onto mechanisms",
                scheduler_image("FIFO"),
                RoundKernel::Mechanisms,
                Some(RoundKernel::Mechanisms),
                CudaConfig::default(),
                false,
            ),
            (
                "FIFO incast after capacity retries",
                scheduler_image("FIFO"),
                RoundKernel::Plain,
                None,
                retrying,
                true,
            ),
            (
                "dcqcn_t26 after capacity retries",
                fixture("dcqcn_t26.toml"),
                RoundKernel::Mechanisms,
                None,
                retrying,
                true,
            ),
        ] {
            let run = run_cuda_with_observations(
                &image,
                None,
                CudaConfig {
                    round_kernel_override: forced_build,
                    ..config
                },
                ObservationMode::Full,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(run.round_kernel, build, "{name}");
            if retries {
                assert!(
                    !run.capacity_retry_trace.is_empty(),
                    "{name}: the capped run must retry capacity"
                );
            }
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

    /// P14 round 4: a run captures its graph with exactly one round module loaded in the context,
    /// its own. Round 3's residue was reproduced with the other build's module loaded at
    /// initialization, and was absent with it never loaded or loaded after graph capture
    /// (retry-free runs; `evidence/P14/diag3-timing.md`). One module per run is the design that
    /// this parity evidence covers, so each run loads its module and no other.
    #[test]
    fn cuda_each_run_captures_its_graph_with_one_round_module_loaded() {
        let executor = CudaExecutor::on_device(0).expect("CUDA device 0");
        // One-record caps force capacity retries: the module is held across every attempt, and
        // the count is the final attempt's.
        let retrying = CudaConfig {
            max_channel_events_per_stream: Some(1),
            max_fel_events_per_lp: Some(1),
            ..CudaConfig::default()
        };
        // Both orders of consecutive builds on one executor: a module left loaded by the previous
        // run would be counted by the next.
        for (name, image, build, config, retries) in [
            (
                "FIFO incast",
                scheduler_image("FIFO"),
                RoundKernel::Plain,
                CudaConfig::default(),
                false,
            ),
            (
                "dcqcn_t26",
                fixture("dcqcn_t26.toml"),
                RoundKernel::Mechanisms,
                CudaConfig::default(),
                false,
            ),
            (
                "FIFO incast forced onto mechanisms",
                scheduler_image("FIFO"),
                RoundKernel::Mechanisms,
                forced(RoundKernel::Mechanisms, 256),
                false,
            ),
            (
                "FIFO incast after capacity retries",
                scheduler_image("FIFO"),
                RoundKernel::Plain,
                retrying,
                true,
            ),
            (
                "dcqcn_t26 after capacity retries",
                fixture("dcqcn_t26.toml"),
                RoundKernel::Mechanisms,
                retrying,
                true,
            ),
        ] {
            let run = executor
                .run_with_observations(&image, None, config, ObservationMode::Full)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(run.round_kernel, build, "{name}");
            if retries {
                assert!(
                    !run.capacity_retry_trace.is_empty(),
                    "{name}: the capped run must retry capacity"
                );
            }
            assert_eq!(
                run.round_modules_at_capture, 1,
                "{name}: only the {build:?} module is loaded while the run captures its graph"
            );
            // The warm start plans the converged capacity at once; it owns one module too.
            let warm = executor
                .run_with_observations_warm_started(
                    &image,
                    None,
                    config,
                    ObservationMode::Full,
                    &run.capacity_warm_start,
                )
                .unwrap_or_else(|error| panic!("{name} warm-started: {error}"));
            assert_eq!(warm.result, run.result, "{name} warm-started");
            assert_eq!(
                warm.round_modules_at_capture, 1,
                "{name} warm-started: only the {build:?} module is loaded at capture"
            );
        }
    }

    /// P14 cuda-host round 3 (option f): a run takes its round module, under the device's guard,
    /// before it plans its first attempt, and holds it across capacity retries. Its steps are one
    /// load, then each attempt's plan, upload, capture and first launch, then one unload when the
    /// run ends. An attempt whose plan is refused for capacity retries before it records a step,
    /// so a run makes at most one recorded attempt per retry plus its final one.
    #[test]
    fn cuda_each_run_loads_its_round_module_before_it_plans() {
        use days_executor::cuda::take_cuda_run_steps_for_testing;
        const ATTEMPT: [&str; 4] = [
            "planned",
            "buffers_allocated",
            "graph_captured",
            "graph_launched",
        ];
        let executor = CudaExecutor::on_device(0).expect("CUDA device 0");
        let retrying = CudaConfig {
            max_channel_events_per_stream: Some(1),
            max_fel_events_per_lp: Some(1),
            ..CudaConfig::default()
        };
        for (name, image, config) in [
            (
                "FIFO incast",
                scheduler_image("FIFO"),
                CudaConfig::default(),
            ),
            (
                "dcqcn_t26",
                fixture("dcqcn_t26.toml"),
                CudaConfig::default(),
            ),
            (
                "FIFO incast after capacity retries",
                scheduler_image("FIFO"),
                retrying,
            ),
            (
                "dcqcn_t26 after capacity retries",
                fixture("dcqcn_t26.toml"),
                retrying,
            ),
        ] {
            take_cuda_run_steps_for_testing();
            let run = executor
                .run_with_observations(&image, None, config, ObservationMode::Full)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let steps = take_cuda_run_steps_for_testing();
            let (Some((&"module_loaded", rest)), Some(&"module_unloaded")) =
                (steps.split_first(), steps.last())
            else {
                panic!("{name}: one module load first and one unload last: {steps:?}");
            };
            let attempts = &rest[..rest.len() - 1];
            assert!(
                !attempts.is_empty()
                    && attempts.len().is_multiple_of(ATTEMPT.len())
                    && attempts
                        .chunks(ATTEMPT.len())
                        .all(|attempt| attempt == ATTEMPT),
                "{name}: every recorded attempt plans, uploads, captures and launches, with the \
                 module held throughout: {steps:?}"
            );
            assert!(
                attempts.len() / ATTEMPT.len() <= run.capacity_retry_trace.len() + 1,
                "{name}: at most one recorded attempt per retry plus the final one: {steps:?}"
            );
        }
    }

    /// P14 round 4: the executor loads no module; each run loads its own and reports what that
    /// cost. Printed for the cost record (`--nocapture`).
    #[test]
    fn cuda_each_run_reports_its_round_module_load_time() {
        let executor = CudaExecutor::on_device(0).expect("CUDA device 0");
        for repeat in 0..3 {
            for (name, image, build) in [
                ("FIFO incast", scheduler_image("FIFO"), RoundKernel::Plain),
                (
                    "dcqcn_t26",
                    fixture("dcqcn_t26.toml"),
                    RoundKernel::Mechanisms,
                ),
            ] {
                let run = executor
                    .run_with_observations(
                        &image,
                        None,
                        CudaConfig::default(),
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| panic!("{name}: {error}"));
                assert_eq!(run.round_kernel, build, "{name}");
                assert!(run.module_load_ns > 0, "{name}: the run loaded its module");
                println!(
                    "record=p14_round_module_load repeat={repeat} round_kernel={build:?} \
                     module_load_ns={} graph_capture_ns={} retries={}",
                    run.module_load_ns,
                    run.graph_capture_ns,
                    run.capacity_retry_trace.len()
                );
            }
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
    ///
    /// P14 cuda-host round 3 (option f): the run takes its module before it plans, so a refused run
    /// loads the module, plans, is refused, and unloads the module before it returns. It allocates
    /// no buffer, captures no graph and launches nothing.
    #[test]
    fn cuda_plain_round_kernel_is_refused_before_launch_on_every_mechanism_image() {
        use days_executor::cuda::take_cuda_run_steps_for_testing;
        for (name, image) in refused_images() {
            take_cuda_run_steps_for_testing();
            match run_cuda_with_observations(
                &image,
                None,
                forced(RoundKernel::Plain, 32),
                ObservationMode::Full,
            ) {
                Err(CudaError::MechanismsKernelRequired { .. }) => {}
                other => panic!("{name}: expected a refusal, got {other:?}"),
            }
            assert_eq!(
                take_cuda_run_steps_for_testing(),
                ["module_loaded", "planned", "module_unloaded"],
                "{name}: a refused run unloads its module and launches nothing"
            );
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

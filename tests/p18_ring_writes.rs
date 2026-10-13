//! P18 ring writes (`days-gpu/evidence/P17/partition/design.md` §1, stage 1).
//!
//! The producer LP writes each remote event as its final 11-word channel record straight into the
//! consumer's channel ring, at slot `(tail0 + b) mod capacity`, where `tail0` is the ring's tail
//! snapshotted by `days_round_reset` and `b` counts the records the producer wrote this round. The
//! merge publishes `count += b`. The staged exchange (`days_exchange_prefix_sweep`,
//! `days_exchange_prefix`, `days_exchange_scatter`) and its `remote_staging` plane leave the
//! streams path; the legacy heap path keeps them.
//!
//! The visibility rule: during a round a channel's count changes only by its consumer's pops, and
//! the records its producer writes become visible only at the publish step. The hooks-only audit
//! checks it for every inbound channel at every merge: the consumer's pops account for the whole
//! change of count since the snapshot, the ring's tail did not move, the round's records fit, and
//! the published records hash to what the producer wrote, in emission order.

#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
use std::path::PathBuf;

#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
use days::scenario::compile_config;
#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
use days_executor::{
    MergeAuditRow, ObservationMode, RunResult, SimulationImage, run_scalar_with_observations,
};

/// The projection the fit arguments rest on (`device-sizing-report`) carries no staging arena: a
/// streams plan's `remote_staging` plane is the one-word placeholder every logically empty plane
/// keeps.
#[test]
fn the_projected_streams_plan_has_no_remote_staging_arena() {
    for relative in [
        "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
        "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
        "configs/p16/rail_mini_roce.toml",
    ] {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
        let image = days::scenario::compile_config(&path)
            .unwrap_or_else(|error| panic!("{relative} must lower: {error}"));
        let report = days_executor::size_default_device_plan(&image)
            .unwrap_or_else(|error| panic!("{relative} must size: {error}"));
        let staging = report
            .planes
            .iter()
            .find(|plane| plane.name == "remote_staging")
            .expect("the plane list keeps its 28 names");
        assert_eq!(
            staging.words, 1,
            "{relative}: a streams plan must not carry a remote staging arena"
        );
    }
}

/// The production planners agree with the projection, and the legacy heap path keeps its arena.
#[cfg(all(
    feature = "test",
    any(
        feature = "cuda",
        feature = "cuda-planner-test",
        all(feature = "metal", target_vendor = "apple")
    )
))]
mod planners {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{DeviceSizingReport, ObservationMode, SimulationImage};

    fn lower(relative: &str) -> SimulationImage {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
        compile_config(&path).unwrap_or_else(|error| panic!("{relative} must lower: {error}"))
    }

    fn staging_words(report: &DeviceSizingReport) -> usize {
        report
            .planes
            .iter()
            .find(|plane| plane.name == "remote_staging")
            .expect("remote_staging plane")
            .words
    }

    fn check(backend: &str, size: impl Fn(&SimulationImage, bool) -> DeviceSizingReport) {
        for relative in [
            "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
            "configs/p15/roce_compute_dag.toml",
        ] {
            let image = lower(relative);
            assert_eq!(
                staging_words(&size(&image, true)),
                1,
                "{backend} {relative}: the streams path writes the rings directly"
            );
            assert!(
                staging_words(&size(&image, false)) > 1,
                "{backend} {relative}: the legacy heap path keeps its staging arena"
            );
        }
    }

    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    #[test]
    fn cuda_streams_plans_carry_no_remote_staging_arena() {
        check("CUDA", |image, streams_enabled| {
            days_executor::size_cuda_plan_for_testing(
                image,
                None,
                days_executor::CudaConfig {
                    streams_enabled,
                    ..days_executor::CudaConfig::default()
                },
                ObservationMode::Summary,
            )
            .expect("CUDA plan must size")
        });
    }

    #[cfg(all(feature = "metal", target_vendor = "apple"))]
    #[test]
    fn metal_streams_plans_carry_no_remote_staging_arena() {
        check("Metal", |image, streams_enabled| {
            days_executor::size_metal_plan_for_testing(
                image,
                None,
                days_executor::MetalConfig {
                    streams_enabled,
                    ..days_executor::MetalConfig::default()
                },
                ObservationMode::Summary,
            )
            .expect("Metal plan must size")
        });
    }
}

#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
const FIXTURES: [&str; 4] = [
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
    "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
    "configs/p15/roce_timeout.toml",
    "configs/p16/rail_mini_roce.toml",
];

#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
fn scalar(image: &SimulationImage) -> RunResult {
    let mut expected = run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

/// Each retry grows the one channel that faulted first, so one-record rings on a fixture with many
/// busy channels take many retries.
#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
const TIGHT_RETRY_BUDGET: usize = 256;

/// One device run's outcome: the result, the capacity retries it took and the audit rows.
#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
struct Audited {
    result: RunResult,
    retries: usize,
    rows: Vec<MergeAuditRow>,
}

/// Runs every fixture at the default capacities and at one-record channel rings (which forces
/// capacity retries through the producer's snapshot check), and checks byte identity and the
/// visibility audit.
#[cfg(any(
    feature = "cuda-test-hooks",
    all(feature = "metal-test-hooks", target_vendor = "apple")
))]
fn check_visibility(backend: &str, run: impl Fn(&SimulationImage, bool) -> Audited) {
    let mut retried = 0;
    for fixture in FIXTURES {
        let image = lower(fixture);
        let expected = scalar(&image);
        for tight in [false, true] {
            let audited = run(&image, tight);
            assert_eq!(
                audited.result, expected,
                "{backend} {fixture} tight={tight}: byte identity"
            );
            retried += usize::from(audited.retries > 0);
            let checks = audited
                .rows
                .iter()
                .map(|row| row.visibility_checks)
                .sum::<u64>();
            let mismatches = audited
                .rows
                .iter()
                .map(|row| row.visibility_mismatches)
                .sum::<u64>();
            let merges = audited.rows.first().map_or(0, |row| row.invocations);
            let inbound = image.channels.len() as u64;
            assert_eq!(
                checks,
                merges * inbound,
                "{backend} {fixture} tight={tight}: the audit checks every inbound channel at \
                 every merge"
            );
            assert!(checks > 0, "{backend} {fixture}: the audit ran");
            assert_eq!(
                mismatches, 0,
                "{backend} {fixture}: the visibility rule held"
            );
            eprintln!(
                "record=ring_visibility backend={backend} fixture={fixture} tight={tight} \
                 merges={merges} channels={inbound} checks={checks} retries={}",
                audited.retries
            );
        }
    }
    assert!(
        retried > 0,
        "{backend}: the one-record rings must force at least one capacity retry"
    );
}

#[cfg(feature = "cuda-test-hooks")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, RoundKernel, run_cuda_with_observations};

    use super::{Audited, FIXTURES, TIGHT_RETRY_BUDGET, check_visibility, lower};

    #[test]
    fn cuda_ring_writes_publish_only_at_the_merge() {
        check_visibility("cuda", |image, tight| {
            let run = run_cuda_with_observations(
                image,
                None,
                CudaConfig {
                    max_channel_events_per_stream: tight.then_some(1),
                    max_capacity_retries: TIGHT_RETRY_BUDGET,
                    ..CudaConfig::default()
                },
                ObservationMode::Full,
            )
            .expect("CUDA run");
            Audited {
                result: run.result,
                retries: run.capacity_retry_trace.len(),
                rows: days_executor::take_merge_audit_for_testing()
                    .expect("the device run recorded its audit rows"),
            }
        });
    }

    /// The streams path launches no staged-exchange kernel; the legacy heap path keeps
    /// `days_exchange_prefix` and `days_exchange_scatter`, and no plan launches the retired
    /// streams-only `days_exchange_prefix_sweep`.
    #[test]
    fn cuda_streams_attempts_launch_no_staged_exchange() {
        let staged = [
            "days_exchange_prefix_sweep",
            "days_exchange_prefix",
            "days_exchange_scatter",
        ];
        let image = lower(FIXTURES[0]);
        for (streams_enabled, mode, attempt_kernels) in [
            (true, ObservationMode::Full, 10),
            (true, ObservationMode::Summary, 9),
            (false, ObservationMode::Full, 12),
            (false, ObservationMode::Summary, 12),
        ] {
            let run = run_cuda_with_observations(
                &image,
                None,
                CudaConfig {
                    streams_enabled,
                    ..CudaConfig::default()
                },
                mode,
            )
            .expect("CUDA run");
            assert_eq!(run.round_kernel, RoundKernel::Plain);
            let launched = run
                .launched_kernels
                .iter()
                .map(|(_, name)| *name)
                .collect::<Vec<_>>();
            // The attempt DAG, then the readback gather.
            assert_eq!(
                launched.len(),
                attempt_kernels + 1,
                "streams={streams_enabled} {mode:?}: {launched:?}"
            );
            for kernel in staged {
                assert_eq!(
                    launched.contains(&kernel),
                    !streams_enabled && kernel != "days_exchange_prefix_sweep",
                    "streams={streams_enabled} {mode:?}: {kernel} in {launched:?}"
                );
            }
        }
    }
}

#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[test]
fn metal_ring_writes_publish_only_at_the_merge() {
    check_visibility("metal", |image, tight| {
        let run = days_executor::run_metal_with_observations(
            image,
            None,
            days_executor::MetalConfig {
                max_channel_events_per_stream: tight.then_some(1),
                max_capacity_retries: TIGHT_RETRY_BUDGET,
                ..days_executor::MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("Metal run");
        Audited {
            result: run.result,
            retries: run.capacity_retry_trace.len(),
            rows: days_executor::take_merge_audit_for_testing()
                .expect("the device run recorded its audit rows"),
        }
    });
}

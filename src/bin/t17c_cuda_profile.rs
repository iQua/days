#[cfg(not(feature = "cuda"))]
fn main() {
    panic!("t17c_cuda_profile requires --features cuda");
}

#[cfg(feature = "cuda")]
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
#[cfg(feature = "cuda")]
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

#[cfg(any(feature = "cuda", test))]
const PREPARE_TOLERANCE_BASIS_POINTS: u128 = 500;

#[cfg(any(feature = "cuda", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PrepareComparison {
    split_prepare_sum_ns: u64,
    split_minus_unsplit_ns: i128,
    split_over_unsplit_numerator_ns: u64,
    split_over_unsplit_denominator_ns: u64,
    within_tolerance: bool,
}

#[cfg(any(feature = "cuda", test))]
fn prepare_comparison(
    split_parts_ns: [u64; 4],
    unsplit_prepare_ns: u64,
) -> Result<PrepareComparison, &'static str> {
    if unsplit_prepare_ns == 0 {
        return Err("unsplit prepare CUDA-event total must be nonzero");
    }
    let split_prepare_sum_ns = split_parts_ns
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .ok_or("split prepare CUDA-event total overflowed u64")?;
    let split_minus_unsplit_ns = i128::from(split_prepare_sum_ns) - i128::from(unsplit_prepare_ns);
    let absolute_difference_ns = split_minus_unsplit_ns.unsigned_abs();
    let within_tolerance = absolute_difference_ns * 10_000
        <= u128::from(unsplit_prepare_ns) * PREPARE_TOLERANCE_BASIS_POINTS;
    Ok(PrepareComparison {
        split_prepare_sum_ns,
        split_minus_unsplit_ns,
        split_over_unsplit_numerator_ns: split_prepare_sum_ns,
        split_over_unsplit_denominator_ns: unsplit_prepare_ns,
        within_tolerance,
    })
}

#[cfg(any(feature = "cuda", test))]
const FIXTURES: [(&str, u64, u64, u64, u64); 7] = [
    (
        "configs/benchmarks/p12/e1_open_k32_load_10.toml",
        18,
        2_227_879,
        70_809_309,
        0x951c_ad2c_3d9f_39f8,
    ),
    (
        "configs/benchmarks/p12/e1_open_k32_load_30.toml",
        18,
        6_333_069,
        131_534_072,
        0x2dbb_d522_d243_3b86,
    ),
    (
        "configs/benchmarks/p12/e1_open_k32_load_60.toml",
        18,
        10_373_881,
        256_696_554,
        0xb1ba_5a9d_872d_abbc,
    ),
    (
        "configs/benchmarks/p12/e1_open_k32_load_90.toml",
        18,
        12_951_185,
        379_505_175,
        0x8ae1_9e3f_4c91_b029,
    ),
    (
        "configs/benchmarks/width_via_load_k48_h16/fattree_k48_h16_load_90_sustained.toml",
        1_002,
        4_245_398_171,
        2_227_821_985,
        0xc04b_51a5_7fc0_d763,
    ),
    (
        "configs/benchmarks/p11/rq9_frontier_closed_k32.toml",
        1_151,
        674_774_349,
        2_274_074_943,
        0x475b_25a5_6369_d8f6,
    ),
    (
        "configs/benchmarks/p12/e5_wide_k32_q200.toml",
        664,
        212_378_014,
        50_572_617,
        0x56f7_b241_57e2_e852,
    ),
];

#[cfg(any(feature = "cuda", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: u64,
    fnv1a64: u64,
}

#[cfg(feature = "cuda")]
struct FingerprintWriter(Fingerprint);

#[cfg(feature = "cuda")]
impl std::fmt::Write for FingerprintWriter {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0.bytes = self
            .0
            .bytes
            .checked_add(value.len() as u64)
            .ok_or(std::fmt::Error)?;
        self.0.fnv1a64 = value.bytes().fold(self.0.fnv1a64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
        });
        Ok(())
    }
}

#[cfg(feature = "cuda")]
fn fingerprint(value: &impl std::fmt::Debug) -> Fingerprint {
    use std::fmt::Write as _;

    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    write!(&mut writer, "{value:#?}").expect("Debug fingerprint must fit in u64 bytes");
    writer.0
}

#[cfg(any(feature = "cuda", test))]
fn fixture_row(path: &str) -> (u64, u64, Fingerprint) {
    FIXTURES
        .iter()
        .find(|(candidate, ..)| path == *candidate)
        .map(|&(_, rounds, transitions, bytes, fnv1a64)| {
            (rounds, transitions, Fingerprint { bytes, fnv1a64 })
        })
        .unwrap_or_else(|| panic!("fixture is not one of T32's seven registered points: {path}"))
}

#[cfg(feature = "cuda")]
fn main() {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor, DeviceCapacityCaps, ObservationMode};

    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(
        arguments.len() == 2,
        "usage: t17c_cuda_profile <registered-fixture> <sample-index-0-through-4>"
    );
    let fixture = arguments[0].clone();
    let sample_index = arguments[1]
        .parse::<usize>()
        .expect("sample index must be an integer from 0 through 4");
    assert!(sample_index < 5, "sample index must be from 0 through 4");
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
    let (expected_rounds, expected_transitions, expected_fingerprint) = fixture_row(&fixture);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let executor = CudaExecutor::new().expect("CUDA profile executor must initialize");
    let config = CudaConfig {
        capacity_caps: DeviceCapacityCaps {
            fallback_fel_events_per_lp: Some(16_384),
            queue_packets_per_lp: Some(2_048),
            channel_events_per_stream: Some(2_048),
            remote_staging_events_per_lp: Some(2_048),
            outbox_events_total: Some(2_000_000),
            tcp_receiver_ranges_per_flow: Some(64),
            tcp_ledger_segments_per_flow: Some(4_096),
            observation_events_per_lp: Some(512),
        },
        max_capacity_retries: 16,
        ..CudaConfig::default()
    };

    let warm = executor
        .run_with_observations(&image, None, config, ObservationMode::Summary)
        .expect("unprofiled CUDA warmup must run");
    let (unsplit, profiled, comparison_order) = if sample_index % 2 == 0 {
        let unsplit = executor
            .run_unsplit_prepare_profiled_with_observations(
                &image,
                None,
                config,
                ObservationMode::Summary,
            )
            .expect("unsplit direct-event CUDA run must run");
        let profiled = executor
            .run_profiled_with_observations(&image, None, config, ObservationMode::Summary)
            .expect("split direct-event CUDA run must run");
        (unsplit, profiled, "unsplit_then_split")
    } else {
        let profiled = executor
            .run_profiled_with_observations(&image, None, config, ObservationMode::Summary)
            .expect("split direct-event CUDA run must run");
        let unsplit = executor
            .run_unsplit_prepare_profiled_with_observations(
                &image,
                None,
                config,
                ObservationMode::Summary,
            )
            .expect("unsplit direct-event CUDA run must run");
        (unsplit, profiled, "split_then_unsplit")
    };
    assert_eq!(
        profiled.run.result, warm.result,
        "profiled and production CUDA results must be byte-identical"
    );
    assert_eq!(
        unsplit.run.result, warm.result,
        "unsplit-direct and production CUDA results must be byte-identical"
    );
    assert_eq!(profiled.run.rounds, warm.rounds);
    assert_eq!(profiled.run.transitions, warm.transitions);
    assert_eq!(unsplit.run.rounds, warm.rounds);
    assert_eq!(unsplit.run.transitions, warm.transitions);
    assert_eq!(unsplit.run.encoded_attempts, profiled.run.encoded_attempts);
    assert_eq!(
        unsplit.run.capacity_retry_trace,
        profiled.run.capacity_retry_trace
    );
    assert_eq!(
        unsplit.run.continuation_relaunches,
        profiled.run.continuation_relaunches
    );
    assert_eq!(warm.rounds, expected_rounds);
    assert_eq!(warm.transitions, expected_transitions);
    let production_fingerprint = fingerprint(&warm.result);
    let profiled_fingerprint = fingerprint(&profiled.run.result);
    assert_eq!(production_fingerprint, expected_fingerprint);
    assert_eq!(profiled_fingerprint, expected_fingerprint);
    assert_eq!(
        profiled.profile.recorded_attempts, profiled.run.encoded_attempts,
        "every directly encoded attempt must have phase timestamps"
    );
    assert_eq!(
        unsplit.profile.recorded_attempts, unsplit.run.encoded_attempts,
        "every unsplit direct attempt must have prepare timestamps"
    );
    assert_eq!(
        unsplit.profile.recorded_attempts, profiled.profile.recorded_attempts,
        "split and unsplit totals must cover the same encoded attempts"
    );

    let profile = profiled.profile;
    assert_eq!(
        profile.recorded_dispatches,
        profile.recorded_attempts.saturating_mul(15),
        "Summary profiling must record exactly the 15 launches that exist"
    );
    assert_eq!(
        unsplit.profile.recorded_dispatches,
        unsplit.profile.recorded_attempts.saturating_mul(12),
        "Summary unsplit profiling must record exactly the 12 production launches that exist"
    );
    let comparison = prepare_comparison(
        [
            profile.prepare_count_ns,
            profile.prepare_prefix_ns,
            profile.prepare_write_ns,
            profile.prepare_combine_ns,
        ],
        unsplit.profile.unsplit_prepare_ns,
    )
    .expect("prepare comparison requires exact nonzero timing totals");
    assert_eq!(comparison.split_prepare_sum_ns, profile.round_prepare_ns);
    let phases: [(&str, u64); 12] = [
        ("horizon", profile.horizon_ns),
        ("reset", profile.round_reset_ns),
        ("split_prepare_count", profile.prepare_count_ns),
        ("split_prepare_prefix", profile.prepare_prefix_ns),
        ("split_prepare_write", profile.prepare_write_ns),
        ("split_prepare_combine", profile.prepare_combine_ns),
        ("drain", profile.drain_ns),
        ("control", profile.control_ns),
        ("exchange_prefix", profile.exchange_prefix_ns),
        ("exchange_scatter", profile.exchange_scatter_ns),
        ("exchange_merge", profile.exchange_merge_ns),
        ("finalize", profile.finalize_ns),
    ];
    let total_kernel_ns = profile.total_kernel_ns();
    assert_eq!(
        phases.iter().map(|(_, elapsed_ns)| elapsed_ns).sum::<u64>(),
        total_kernel_ns,
        "the twelve reported rows must attribute every profile dispatch exactly once"
    );
    let (dominant_phase, dominant_ns) = phases
        .iter()
        .copied()
        .max_by_key(|(_, elapsed_ns)| *elapsed_ns)
        .expect("CUDA profile has twelve phases");

    println!(
        "record=t17c_cuda_phase_profile_identity fixture={fixture} sample_index={sample_index} \
         rounds={} transitions={} result_bytes={} result_fnv1a64={:016x} \
         production_unsplit_split_equal=true instrumentation_off_on_equal=true roster_equal=true",
        warm.rounds, warm.transitions, production_fingerprint.bytes, production_fingerprint.fnv1a64,
    );
    println!(
        "record=t17c_cuda_phase_profile_protocol fixture={fixture} sample_index={sample_index} \
         orchestration=direct_15_dispatch_split_profile_attempts profile_dispatches=15 \
         unsplit_dispatches=12 production_dispatches=12 maximum_profile_dispatches=16 \
         maximum_unsplit_dispatches=13 maximum_production_dispatches=13 \
         timestamps=cuda_device_events \
         production_graph_perturbed=false correctness=complete_RunResult_equality \
         percentage_basis=sum_of_existing_profile_kernel_intervals \
         prepare_attribution=diagnostic_split_path_only"
    );
    println!(
        "record=t17c_cuda_phase_profile_contract fixture={fixture} sample_index={sample_index} \
         capacity_caps=fallback_fel:16384,queue:2048,channel:2048,remote_staging:2048,outbox:2000000,tcp_ranges:64,tcp_ledger:4096,observation:512 \
         max_capacity_retries=16 \
         production_dispatch_names=days_horizon_sweep,days_horizon,days_round_reset,days_round_prepare,days_round,days_round_control_sweep,days_round_control,days_exchange_prefix_sweep,days_exchange_prefix,days_exchange_scatter,days_exchange_merge,days_round_finalize \
         split_dispatch_names=days_horizon_sweep,days_horizon,days_round_reset,days_round_prepare_count_profile,days_round_prepare_prefix_profile,days_round_prepare_write_profile,days_round_prepare_combine_profile,days_round,days_round_control_sweep,days_round_control,days_exchange_prefix_sweep,days_exchange_prefix,days_exchange_scatter,days_exchange_merge,days_round_finalize"
    );
    println!(
        "record=t17c_cuda_prepare_split_perturbation fixture={fixture} \
         added_dispatch_boundaries_per_attempt=3 scratch_u64_stores_per_taken_prepare=2048 \
         scratch_u64_loads_per_taken_prepare=3073 logical_scratch_bytes_per_taken_prepare=40968 \
         production_subcost_attribution=false"
    );
    let attribution = if comparison.within_tolerance {
        "within_bound"
    } else {
        "perturbed_unusable"
    };
    println!(
        "record=t17c_cuda_prepare_comparison fixture={fixture} sample_index={sample_index} \
         clock=cuda_device_events method=direct_launch_events order={comparison_order} \
         recorded_attempts={} split_prepare_sum_ns={} unsplit_prepare_ns={} \
         split_minus_unsplit_ns={} split_over_unsplit_numerator_ns={} \
         split_over_unsplit_denominator_ns={} tolerance_basis_points={} \
         within_tolerance={} split_total_attribution={attribution} \
         production_subcost_attribution=false",
        profile.recorded_attempts,
        comparison.split_prepare_sum_ns,
        unsplit.profile.unsplit_prepare_ns,
        comparison.split_minus_unsplit_ns,
        comparison.split_over_unsplit_numerator_ns,
        comparison.split_over_unsplit_denominator_ns,
        PREPARE_TOLERANCE_BASIS_POINTS,
        comparison.within_tolerance,
    );
    println!(
        "record=t17c_cuda_phase_profile_run fixture={fixture} sample_index={sample_index} \
         rounds={} transitions={} \
         production_encoded_attempts={} production_backend_wall_ns={} production_device_ns={} \
         unsplit_encoded_attempts={} unsplit_recorded_dispatches={} \
         unsplit_backend_wall_ns={} unsplit_device_ns={} \
         profiled_encoded_attempts={} profiled_wave_boundary_syncs={} \
         profiled_backend_wall_ns={} profiled_device_ns={} recorded_dispatches={} total_kernel_ns={} \
         total_kernel_over_profiled_device={:.9}",
        warm.rounds,
        warm.transitions,
        warm.encoded_attempts,
        warm.wall_ns,
        warm.device_ns,
        unsplit.run.encoded_attempts,
        unsplit.profile.recorded_dispatches,
        unsplit.run.wall_ns,
        unsplit.run.device_ns,
        profiled.run.encoded_attempts,
        profiled.run.wave_boundary_syncs,
        profiled.run.wall_ns,
        profiled.run.device_ns,
        profile.recorded_dispatches,
        total_kernel_ns,
        ratio(total_kernel_ns, profiled.run.device_ns),
    );
    for (phase, elapsed_ns) in phases {
        println!(
            "record=t17c_cuda_phase_profile_phase fixture={fixture} sample_index={sample_index} \
             phase={phase} \
             elapsed_ns={elapsed_ns} ns_per_encoded_attempt={} percent={:.6}",
            elapsed_ns / profile.recorded_attempts,
            percent(elapsed_ns, total_kernel_ns),
        );
    }
    println!(
        "record=t17c_cuda_phase_profile_dominant fixture={fixture} sample_index={sample_index} \
         phase={dominant_phase} \
         elapsed_ns={dominant_ns} percent={:.6} credible_tuning_threshold_percent=5.000000",
        percent(dominant_ns, total_kernel_ns),
    );
}

#[cfg(any(feature = "cuda", test))]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    assert!(denominator > 0, "profile ratio denominator must be nonzero");
    numerator as f64 / denominator as f64
}

#[cfg(any(feature = "cuda", test))]
fn percent(part: u64, total: u64) -> f64 {
    ratio(part, total) * 100.0
}

#[cfg(test)]
mod tests {
    use super::{
        FIXTURES, Fingerprint, PREPARE_TOLERANCE_BASIS_POINTS, fixture_row, percent,
        prepare_comparison, ratio,
    };

    #[test]
    fn profile_ratios_use_the_declared_denominator() {
        assert_eq!(ratio(1, 4), 0.25);
        assert_eq!(percent(1, 4), 25.0);
    }

    #[test]
    fn phase_runner_has_frozen_identity_for_all_seven_points() {
        for &(path, rounds, transitions, bytes, fnv1a64) in &FIXTURES {
            assert_eq!(
                fixture_row(path),
                (rounds, transitions, Fingerprint { bytes, fnv1a64 })
            );
        }
    }

    #[test]
    fn phase_runner_rejects_paths_outside_the_frozen_roster() {
        assert!(
            std::panic::catch_unwind(|| fixture_row(
                "other/configs/benchmarks/p12/e1_open_k32_load_10.toml"
            ))
            .is_err()
        );
    }

    #[test]
    fn prepare_comparison_uses_an_inclusive_five_percent_bound() {
        let upper = prepare_comparison([250, 250, 250, 300], 1_000).unwrap();
        assert_eq!(upper.split_prepare_sum_ns, 1_050);
        assert_eq!(upper.split_minus_unsplit_ns, 50);
        assert_eq!(upper.split_over_unsplit_numerator_ns, 1_050);
        assert_eq!(upper.split_over_unsplit_denominator_ns, 1_000);
        assert_eq!(PREPARE_TOLERANCE_BASIS_POINTS, 500);
        assert!(upper.within_tolerance);

        let lower = prepare_comparison([200, 200, 200, 350], 1_000).unwrap();
        assert_eq!(lower.split_minus_unsplit_ns, -50);
        assert!(lower.within_tolerance);

        assert!(
            !prepare_comparison([250, 250, 250, 301], 1_000)
                .unwrap()
                .within_tolerance
        );
        assert!(
            !prepare_comparison([200, 200, 200, 349], 1_000)
                .unwrap()
                .within_tolerance
        );
    }

    #[test]
    fn prepare_comparison_rejects_invalid_exact_arithmetic() {
        assert!(prepare_comparison([1, 0, 0, 0], 0).is_err());
        assert!(prepare_comparison([u64::MAX, 1, 0, 0], 1).is_err());

        let maximum = prepare_comparison([u64::MAX, 0, 0, 0], u64::MAX).unwrap();
        assert_eq!(maximum.split_minus_unsplit_ns, 0);
        assert!(maximum.within_tolerance);
    }
}

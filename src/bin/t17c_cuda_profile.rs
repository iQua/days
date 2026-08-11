#[cfg(not(feature = "cuda"))]
fn main() {
    panic!("t17c_cuda_profile requires --features cuda");
}

#[cfg(feature = "cuda")]
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
#[cfg(feature = "cuda")]
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

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
    use days_executor::{CudaConfig, CudaExecutor, ObservationMode};

    let fixture = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/benchmarks/p12/e1_open_k32_load_10.toml".to_owned());
    assert!(
        std::env::args().nth(2).is_none(),
        "t17c_cuda_profile accepts at most one fixture"
    );
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
    let (expected_rounds, expected_transitions, expected_fingerprint) = fixture_row(&fixture);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let executor = CudaExecutor::new().expect("CUDA profile executor must initialize");
    let config = CudaConfig::default();

    let warm = executor
        .run_with_observations(&image, None, config, ObservationMode::Summary)
        .expect("unprofiled CUDA warmup must run");
    let profiled = executor
        .run_profiled_with_observations(&image, None, config, ObservationMode::Summary)
        .expect("profiled CUDA run must run");
    assert_eq!(
        profiled.run.result, warm.result,
        "profiled and production CUDA results must be byte-identical"
    );
    assert_eq!(profiled.run.rounds, warm.rounds);
    assert_eq!(profiled.run.transitions, warm.transitions);
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

    let profile = profiled.profile;
    assert_eq!(
        profile.recorded_dispatches,
        profile.recorded_attempts.saturating_mul(15),
        "Summary profiling must record exactly the 15 launches that exist"
    );
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
        "record=t17c_cuda_phase_profile_identity fixture={fixture} rounds={} transitions={} \
         result_bytes={} result_fnv1a64={:016x} instrumentation_off_on_equal=true roster_equal=true",
        warm.rounds, warm.transitions, production_fingerprint.bytes, production_fingerprint.fnv1a64,
    );
    println!(
        "record=t17c_cuda_phase_profile_protocol fixture={fixture} \
         orchestration=direct_15_dispatch_split_profile_attempts profile_dispatches=15 \
         production_dispatches=12 maximum_profile_dispatches=16 maximum_production_dispatches=13 \
         timestamps=cuda_device_events \
         production_graph_perturbed=false correctness=complete_RunResult_equality \
         percentage_basis=sum_of_existing_profile_kernel_intervals \
         prepare_attribution=diagnostic_split_path_only"
    );
    println!(
        "record=t17c_cuda_prepare_split_perturbation fixture={fixture} \
         added_dispatch_boundaries_per_attempt=3 scratch_u64_stores_per_taken_prepare=2048 \
         scratch_u64_loads_per_taken_prepare=3073 logical_scratch_bytes_per_taken_prepare=40968 \
         production_subcost_attribution=false"
    );
    println!(
        "record=t17c_cuda_phase_profile_run fixture={fixture} rounds={} transitions={} \
         production_encoded_attempts={} production_backend_wall_ns={} production_device_ns={} \
         profiled_encoded_attempts={} profiled_wave_boundary_syncs={} \
         profiled_backend_wall_ns={} profiled_device_ns={} recorded_dispatches={} total_kernel_ns={} \
         total_kernel_over_profiled_device={:.9}",
        warm.rounds,
        warm.transitions,
        warm.encoded_attempts,
        warm.wall_ns,
        warm.device_ns,
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
            "record=t17c_cuda_phase_profile_phase fixture={fixture} phase={phase} \
             elapsed_ns={elapsed_ns} ns_per_encoded_attempt={} percent={:.6}",
            elapsed_ns / profile.recorded_attempts,
            percent(elapsed_ns, total_kernel_ns),
        );
    }
    println!(
        "record=t17c_cuda_phase_profile_dominant fixture={fixture} phase={dominant_phase} \
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
    use super::{FIXTURES, Fingerprint, fixture_row, percent, ratio};

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
}

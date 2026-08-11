#[cfg(not(feature = "cuda"))]
fn main() {
    panic!("t17c_cuda_profile requires --features cuda");
}

#[cfg(feature = "cuda")]
fn main() {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor, ObservationMode};

    let fixture = std::env::args().nth(1).unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_full/fattree_k32_load_90_sustained.toml".to_owned()
    });
    assert!(
        std::env::args().nth(2).is_none(),
        "t17c_cuda_profile accepts at most one fixture"
    );
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
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
    assert_eq!(
        profiled.profile.recorded_attempts, profiled.run.encoded_attempts,
        "every directly encoded attempt must have phase timestamps"
    );

    let profile = profiled.profile;
    let phases: [(&str, u64); 12] = [
        ("horizon", profile.horizon_ns),
        ("reset", profile.round_reset_ns),
        ("prepare_count", profile.prepare_count_ns),
        ("prepare_prefix", profile.prepare_prefix_ns),
        ("prepare_write", profile.prepare_write_ns),
        ("prepare_combine", profile.prepare_combine_ns),
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
        "record=t17c_cuda_phase_profile_protocol fixture={fixture} \
         orchestration=direct_16_dispatch_profile_attempts production_dispatches=13 \
         timestamps=cuda_device_events \
         production_graph_perturbed=false correctness=complete_RunResult_equality \
         percentage_basis=sum_of_phase_kernel_intervals"
    );
    println!(
        "record=t17c_cuda_phase_profile_run fixture={fixture} rounds={} transitions={} \
         production_encoded_attempts={} production_backend_wall_ns={} production_device_ns={} \
         profiled_encoded_attempts={} profiled_wave_boundary_syncs={} \
         profiled_backend_wall_ns={} profiled_device_ns={} total_kernel_ns={} \
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
    use super::{percent, ratio};

    #[test]
    fn profile_ratios_use_the_declared_denominator() {
        assert_eq!(ratio(1, 4), 0.25);
        assert_eq!(percent(1, 4), 25.0);
    }
}

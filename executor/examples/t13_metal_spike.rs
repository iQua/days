#[cfg(target_vendor = "apple")]
use days_executor::metal_spike::{
    MetalSpikeBenchmarkConfig, SweepPoint, benchmark_metal, run_metal_correctness_suite,
    sweep_geometry,
};

#[cfg(target_vendor = "apple")]
fn main() {
    let correctness = run_metal_correctness_suite().expect("Metal correctness spike failed");
    eprintln!("correctness={correctness:?}");

    let mut config = MetalSpikeBenchmarkConfig::default();
    if let Some(workers) = env_usize("DAYS_METAL_SWEEP_CPU_WORKERS") {
        config.cpu_workers = workers;
    }
    if let Ok(point) = std::env::var("DAYS_METAL_SWEEP_POINT") {
        config.sweep_points = vec![parse_sweep_point(&point)];
    }
    let gpu_cores = env_usize("DAYS_METAL_SWEEP_GPU_CORES").unwrap_or(40);
    let report = benchmark_metal(config).expect("Metal benchmark failed");
    eprintln!(
        "substrate={};pipeline_setup_ns={};rounds_per_encoding={};cpu_workers={};gpu_cores_for_modeled_coverage={};base_workload={:?}",
        report.substrate,
        report.pipeline_setup_ns,
        report.rounds_per_encoding,
        report.cpu_workers,
        gpu_cores,
        report.workload
    );
    println!(
        "width,sample,rounds,warmup_rounds,cpu_workers,padded_lanes,body_threadgroups,reduction_dispatches_per_round,dispatches_per_round,encodings,rounds_per_encoding,events_per_round,modeled_useful_lane_coverage_ppm,modeled_threadgroup_core_coverage_ppm,pipeline_setup_ns,host_encode_submit_ns,device_ns,gpu_wall_ns,matched_cpu_ns,cpu_checksum,gpu_checksum,matched_checksums,no_host_sync_between_rounds"
    );
    for measurement in &report.scales {
        let geometry =
            sweep_geometry(measurement.active_lps).expect("reported sweep geometry is valid");
        for sample in 0..measurement.host_encode_submit_ns.len() {
            println!(
                "{},{sample},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                measurement.active_lps,
                measurement.rounds,
                measurement.warmup_rounds,
                report.cpu_workers,
                measurement.padded_lanes,
                measurement.body_threadgroups,
                measurement.reduction_dispatches_per_round,
                measurement.dispatches_per_round,
                measurement.encodings,
                report.rounds_per_encoding,
                measurement.workload.transitions_per_round,
                geometry.modeled_useful_lane_coverage_ppm(gpu_cores),
                geometry.modeled_threadgroup_core_coverage_ppm(gpu_cores),
                measurement.pipeline_setup_ns,
                measurement.host_encode_submit_ns[sample],
                measurement.device_ns[sample],
                measurement.gpu_wall_ns[sample],
                measurement.matched_cpu_ns[sample],
                measurement.cpu_checksums[sample],
                measurement.gpu_checksums[sample],
                measurement.matched_checksums,
                measurement.no_host_sync_between_rounds,
            );
        }
    }
}

#[cfg(target_vendor = "apple")]
fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().map(|value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be usize"))
    })
}

#[cfg(target_vendor = "apple")]
fn parse_sweep_point(value: &str) -> SweepPoint {
    let mut fields = value.split(':');
    let active_lps = fields
        .next()
        .and_then(|field| field.parse().ok())
        .expect("DAYS_METAL_SWEEP_POINT must be width:rounds:warmup");
    let rounds = fields
        .next()
        .and_then(|field| field.parse().ok())
        .expect("DAYS_METAL_SWEEP_POINT must be width:rounds:warmup");
    let warmup_rounds = fields
        .next()
        .and_then(|field| field.parse().ok())
        .expect("DAYS_METAL_SWEEP_POINT must be width:rounds:warmup");
    assert!(
        fields.next().is_none(),
        "DAYS_METAL_SWEEP_POINT must be width:rounds:warmup"
    );
    SweepPoint {
        active_lps,
        rounds,
        warmup_rounds,
    }
}

#[cfg(not(target_vendor = "apple"))]
fn main() {
    eprintln!("the T13 Metal spike runs only on Apple targets");
}

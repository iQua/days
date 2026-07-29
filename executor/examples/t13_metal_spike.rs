#[cfg(target_vendor = "apple")]
use days_executor::metal_spike::{
    MetalSpikeBenchmarkConfig, benchmark_metal, run_metal_correctness_suite,
};

#[cfg(target_vendor = "apple")]
fn main() {
    let correctness = run_metal_correctness_suite().expect("Metal correctness spike failed");
    eprintln!("correctness={correctness:?}");

    let report =
        benchmark_metal(MetalSpikeBenchmarkConfig::default()).expect("Metal benchmark failed");
    eprintln!(
        "substrate={};pipeline_setup_ns={};rounds_per_encoding={};workload={:?}",
        report.substrate, report.pipeline_setup_ns, report.rounds_per_encoding, report.workload
    );
    println!(
        "scale,sample,rounds,encodings,rounds_per_encoding,host_encode_submit_ns,device_ns,gpu_wall_ns,matched_cpu_ns,checksum"
    );
    for (scale, measurement) in report.scales.iter().enumerate() {
        for sample in 0..measurement.host_encode_submit_ns.len() {
            println!(
                "{scale},{sample},{},{},{},{},{},{},{},{}",
                measurement.rounds,
                measurement.encodings,
                report.rounds_per_encoding,
                measurement.host_encode_submit_ns[sample],
                measurement.device_ns[sample],
                measurement.gpu_wall_ns[sample],
                measurement.matched_cpu_ns[sample],
                measurement.checksums[sample],
            );
        }
    }
}

#[cfg(not(target_vendor = "apple"))]
fn main() {
    eprintln!("the T13 Metal spike runs only on Apple targets");
}

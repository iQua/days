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
    println!("kind,count,dispatches,sample,wall_ns,device_ns");
    for batch in &report.batches {
        for (sample, (wall_ns, device_ns)) in batch
            .wall_time_ns
            .iter()
            .zip(&batch.device_time_ns)
            .enumerate()
        {
            println!(
                "dispatch,{},{},{sample},{wall_ns},{device_ns}",
                batch.dispatches, batch.dispatches
            );
        }
    }
    for rounds in &report.resident_rounds {
        for (sample, (wall_ns, device_ns)) in rounds
            .wall_time_ns
            .iter()
            .zip(&rounds.device_time_ns)
            .enumerate()
        {
            println!(
                "resident_round,{},{},{sample},{wall_ns},{device_ns}",
                rounds.rounds, rounds.dispatches
            );
        }
    }
}

#[cfg(not(target_vendor = "apple"))]
fn main() {
    eprintln!("the T13 Metal spike runs only on Apple targets");
}

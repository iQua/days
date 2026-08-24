#[cfg(any(feature = "cuda", test))]
const WIDTHS: [usize; 3] = [256, 128, 64];
#[cfg(any(feature = "cuda", test))]
const SAMPLES: usize = 2;

#[cfg(any(feature = "cuda", test))]
fn median(mut values: Vec<u128>) -> u128 {
    values.sort_unstable();
    (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2
}

#[cfg(any(feature = "cuda", test))]
fn retained_range(values: &[u128]) -> u128 {
    values.iter().max().unwrap() - values.iter().min().unwrap()
}

#[cfg(any(feature = "cuda", test))]
fn order_for_sample(sample: usize) -> [usize; 3] {
    if sample.is_multiple_of(2) {
        WIDTHS
    } else {
        [64, 128, 256]
    }
}

#[cfg(not(feature = "cuda"))]
fn main() {
    panic!("t17c_cuda_geometry requires --features cuda");
}

#[cfg(feature = "cuda")]
fn main() {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor, RunResult};

    use crate::{SAMPLES, WIDTHS, median, order_for_sample, retained_range};

    #[derive(Clone, Copy)]
    struct Measurement {
        sample: usize,
        order: &'static str,
        width: usize,
        wall_ns: u128,
        device_ns: u128,
        rounds: u64,
        transitions: u64,
    }

    fn measure(
        executor: &CudaExecutor,
        image: &days_executor::SimulationImage,
        width: usize,
        expected: Option<&RunResult>,
    ) -> (Measurement, RunResult) {
        let run = executor
            .run(
                image,
                None,
                CudaConfig {
                    round_threads_per_block: width,
                    ..CudaConfig::default()
                },
            )
            .unwrap_or_else(|error| panic!("CUDA width {width} run failed: {error}"));
        if let Some(expected) = expected {
            assert_eq!(&run.result, expected, "CUDA width {width} result differs");
        }
        let measurement = Measurement {
            sample: 0,
            order: "warmup",
            width,
            wall_ns: u128::from(run.wall_ns),
            device_ns: u128::from(run.device_ns),
            rounds: run.rounds,
            transitions: run.transitions,
        };
        (measurement, run.result)
    }

    fn print_record(kind: &str, fixture: &str, measurement: Measurement) {
        println!(
            "record=t17c_cuda_geometry_{kind} fixture={fixture} sample={} order={} \
             predecessor={} round_threads_per_block={} rounds={} transitions={} \
             backend_wall_ns={} backend_ns_per_round={} device_ns={} device_ns_per_round={}",
            measurement.sample,
            measurement.order,
            if kind == "sample" {
                "same_geometry_discarded"
            } else {
                "none"
            },
            measurement.width,
            measurement.rounds,
            measurement.transitions,
            measurement.wall_ns,
            measurement.wall_ns / u128::from(measurement.rounds),
            measurement.device_ns,
            measurement.device_ns / u128::from(measurement.rounds),
        );
    }

    let fixture = std::env::args().nth(1).unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_full/fattree_k32_load_90_sustained.toml".to_owned()
    });
    assert!(
        std::env::args().nth(2).is_none(),
        "t17c_cuda_geometry accepts at most one fixture"
    );
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let executor = CudaExecutor::new().expect("CUDA geometry executor must initialize");

    println!(
        "record=t17c_cuda_geometry_protocol fixture={fixture} mechanism=threads_per_block \
         widths=256,128,64 samples={SAMPLES} \
         order_schedule=descending,ascending predecessor=same_geometry_discarded \
         correctness=complete_RunResult_equality credible_threshold_percent=5.000000"
    );
    let (baseline_warm, expected) = measure(&executor, &image, WIDTHS[0], None);
    print_record("warmup", &fixture, baseline_warm);
    for width in WIDTHS.into_iter().skip(1) {
        let (warm, _) = measure(&executor, &image, width, Some(&expected));
        assert_eq!(warm.rounds, baseline_warm.rounds);
        assert_eq!(warm.transitions, baseline_warm.transitions);
        print_record("warmup", &fixture, warm);
    }

    let mut measurements = Vec::with_capacity(WIDTHS.len() * SAMPLES);
    for sample in 0..SAMPLES {
        let order = if sample == 0 {
            "descending"
        } else {
            "ascending"
        };
        for width in order_for_sample(sample) {
            let _ = measure(&executor, &image, width, Some(&expected));
            let (mut measurement, _) = measure(&executor, &image, width, Some(&expected));
            measurement.sample = sample;
            measurement.order = order;
            print_record("sample", &fixture, measurement);
            measurements.push(measurement);
        }
    }

    let baseline = measurements
        .iter()
        .filter(|measurement| measurement.width == WIDTHS[0])
        .map(|measurement| measurement.wall_ns)
        .collect::<Vec<_>>();
    let baseline_median = median(baseline.clone());
    for width in WIDTHS {
        let samples = measurements
            .iter()
            .filter(|measurement| measurement.width == width)
            .map(|measurement| measurement.wall_ns)
            .collect::<Vec<_>>();
        let sample_median = median(samples.clone());
        let improvement =
            (baseline_median as f64 - sample_median as f64) / baseline_median as f64 * 100.0;
        let dispersion = retained_range(&baseline).max(retained_range(&samples));
        let difference = baseline_median.abs_diff(sample_median);
        let outcome =
            if baseline_median == sample_median || difference.saturating_mul(2) < dispersion {
                "parity"
            } else if sample_median < baseline_median {
                "beats"
            } else {
                "trails"
            };
        println!(
            "record=t17c_cuda_geometry_summary fixture={fixture} \
             round_threads_per_block={width} samples={SAMPLES} backend_wall_ns={sample_median} \
             backend_ns_per_round={} retained_range_ns={} baseline_wall_ns={baseline_median} \
             improvement_percent={improvement:.6} outcome={outcome} \
             clears_five_percent={}",
            sample_median / u128::from(baseline_warm.rounds),
            retained_range(&samples),
            outcome == "beats" && improvement >= 5.0,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{SAMPLES, WIDTHS, median, order_for_sample, retained_range};

    #[test]
    fn geometry_probe_balances_ascending_and_descending_orders() {
        assert_eq!(SAMPLES, 2);
        assert_eq!(order_for_sample(0), WIDTHS);
        assert_eq!(order_for_sample(1), [64, 128, 256]);
        assert_eq!(median(vec![1, 2, 3, 4]), 2);
        assert_eq!(retained_range(&[1, 4]), 3);
    }
}

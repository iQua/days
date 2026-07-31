#[cfg(any(feature = "cuda", test))]
use std::fmt;

#[cfg(any(feature = "cuda", test))]
const SAMPLES: usize = 4;

#[cfg(any(feature = "cuda", test))]
fn median(mut values: Vec<u128>) -> u128 {
    values.sort_unstable();
    match values.len() {
        0 => 0,
        length if length % 2 == 1 => values[length / 2],
        length => {
            values[length / 2 - 1]
                .checked_add(values[length / 2])
                .expect("median pair sum must fit")
                / 2
        }
    }
}

#[cfg(any(feature = "cuda", test))]
fn retained_range(values: &[u128]) -> u128 {
    values
        .iter()
        .max()
        .expect("retained samples must be nonempty")
        - values
            .iter()
            .min()
            .expect("retained samples must be nonempty")
}

#[cfg(any(feature = "cuda", test))]
fn order_for_sample(sample: usize) -> &'static str {
    if sample.is_multiple_of(2) {
        "baseline_first"
    } else {
        "candidate_first"
    }
}

#[cfg(any(feature = "cuda", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormalOutcome {
    Beats,
    Parity,
    Trails,
}

#[cfg(any(feature = "cuda", test))]
impl fmt::Display for FormalOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Beats => "beats",
            Self::Parity => "parity",
            Self::Trails => "trails",
        })
    }
}

#[cfg(any(feature = "cuda", test))]
fn formal_outcome(candidate: &[u128], baseline: &[u128]) -> FormalOutcome {
    let candidate_median = median(candidate.to_vec());
    let baseline_median = median(baseline.to_vec());
    let dispersion = retained_range(candidate).max(retained_range(baseline));
    let difference = candidate_median.abs_diff(baseline_median);
    if candidate_median == baseline_median || difference.saturating_mul(2) < dispersion {
        FormalOutcome::Parity
    } else if candidate_median < baseline_median {
        FormalOutcome::Beats
    } else {
        FormalOutcome::Trails
    }
}

#[cfg(not(feature = "cuda"))]
fn main() {
    panic!("t17c_cuda_ablation requires --features cuda");
}

#[cfg(feature = "cuda")]
fn main() {
    cuda_app::main();
}

#[cfg(feature = "cuda")]
mod cuda_app {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{CudaConfig, CudaExecutor, RunResult, SimulationImage};

    use super::{SAMPLES, formal_outcome, median, order_for_sample, retained_range};

    const ROUND_THREADS_PER_BLOCK: usize = 256;

    #[derive(Clone, Copy, Debug)]
    struct Measurement {
        sample: usize,
        order: &'static str,
        engine: &'static str,
        end_to_end_ns: u128,
        backend_wall_ns: u128,
        device_ns: u128,
        graph_capture_ns: u128,
        host_submit_ns: u128,
        rounds: u64,
        transitions: u64,
    }

    impl Measurement {
        fn backend_ns_per_round(self) -> u128 {
            self.backend_wall_ns / u128::from(self.rounds)
        }

        fn device_ns_per_round(self) -> u128 {
            self.device_ns / u128::from(self.rounds)
        }
    }

    fn measure(
        executor: &CudaExecutor,
        image: &SimulationImage,
        role_split: bool,
    ) -> (Measurement, RunResult) {
        let started = Instant::now();
        let run = executor
            .run(
                image,
                None,
                CudaConfig {
                    role_split,
                    round_threads_per_block: ROUND_THREADS_PER_BLOCK,
                    ..CudaConfig::default()
                },
            )
            .unwrap_or_else(|error| {
                panic!("CUDA role_split={role_split} benchmark run failed: {error}")
            });
        let end_to_end_ns = started.elapsed().as_nanos();
        let result = run.result;
        (
            Measurement {
                sample: 0,
                order: "warmup",
                engine: if role_split {
                    "role_split"
                } else {
                    "unmodified"
                },
                end_to_end_ns,
                backend_wall_ns: u128::from(run.wall_ns),
                device_ns: u128::from(run.device_ns),
                graph_capture_ns: u128::from(run.graph_capture_ns),
                host_submit_ns: u128::from(run.host_submit_ns),
                rounds: run.rounds,
                transitions: run.transitions,
            },
            result,
        )
    }

    fn checked(
        executor: &CudaExecutor,
        image: &SimulationImage,
        role_split: bool,
        expected: &RunResult,
    ) -> Measurement {
        let (measurement, result) = measure(executor, image, role_split);
        assert_eq!(
            &result, expected,
            "CUDA role_split={role_split} complete result differs from the warm baseline"
        );
        measurement
    }

    fn after_predecessor(
        executor: &CudaExecutor,
        image: &SimulationImage,
        role_split: bool,
        expected: &RunResult,
    ) -> Measurement {
        let _ = checked(executor, image, role_split, expected);
        checked(executor, image, role_split, expected)
    }

    fn print_record(kind: &str, fixture: &str, measurement: Measurement) {
        println!(
            "record=t17c_role_split_{kind} fixture={fixture} sample={} order={} \
             predecessor={} engine={} rounds={} transitions={} round_threads_per_block={} \
             end_to_end_ns={} backend_wall_ns={} backend_ns_per_round={} device_ns={} \
             device_ns_per_round={} graph_capture_ns={} host_submit_ns={}",
            measurement.sample,
            measurement.order,
            if kind == "sample" {
                "same_revision_discarded"
            } else {
                "none"
            },
            measurement.engine,
            measurement.rounds,
            measurement.transitions,
            ROUND_THREADS_PER_BLOCK,
            measurement.end_to_end_ns,
            measurement.backend_wall_ns,
            measurement.backend_ns_per_round(),
            measurement.device_ns,
            measurement.device_ns_per_round(),
            measurement.graph_capture_ns,
            measurement.host_submit_ns,
        );
    }

    fn print_summary(fixture: &str, engine: &str, measurements: &[Measurement]) {
        let selected = measurements
            .iter()
            .copied()
            .filter(|measurement| measurement.engine == engine)
            .collect::<Vec<_>>();
        let rounds = selected[0].rounds;
        println!(
            "record=t17c_role_split_summary fixture={fixture} engine={engine} samples={} \
             rounds={rounds} transitions={} backend_wall_ns={} backend_ns_per_round={} \
             device_ns={} device_ns_per_round={} retained_range_ns={}",
            selected.len(),
            selected[0].transitions,
            median(
                selected
                    .iter()
                    .map(|measurement| measurement.backend_wall_ns)
                    .collect()
            ),
            median(
                selected
                    .iter()
                    .map(|measurement| measurement.backend_ns_per_round())
                    .collect()
            ),
            median(
                selected
                    .iter()
                    .map(|measurement| measurement.device_ns)
                    .collect()
            ),
            median(
                selected
                    .iter()
                    .map(|measurement| measurement.device_ns_per_round())
                    .collect()
            ),
            retained_range(
                &selected
                    .iter()
                    .map(|measurement| measurement.backend_wall_ns)
                    .collect::<Vec<_>>()
            ),
        );
        for order in ["baseline_first", "candidate_first"] {
            let ordered = selected
                .iter()
                .filter(|measurement| measurement.order == order)
                .map(|measurement| measurement.backend_wall_ns)
                .collect::<Vec<_>>();
            println!(
                "record=t17c_role_split_order_summary fixture={fixture} engine={engine} \
                 order={order} samples={} backend_wall_ns={} backend_ns_per_round={}",
                ordered.len(),
                median(ordered.clone()),
                median(ordered) / u128::from(rounds),
            );
        }
    }

    pub fn main() {
        let fixture = std::env::args().nth(1).unwrap_or_else(|| {
            "configs/benchmarks/width_via_load_full/fattree_k32_load_90_sustained.toml".to_owned()
        });
        assert!(
            std::env::args().nth(2).is_none(),
            "t17c_cuda_ablation accepts at most one fixture"
        );
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        let executor = CudaExecutor::new().expect("CUDA executor must initialize");

        println!(
            "record=t17c_role_split_protocol fixture={fixture} samples={SAMPLES} \
             order_schedule=baseline_first,candidate_first,baseline_first,candidate_first \
             predecessor=same_revision_discarded comparison_clock=backend_wall \
             parity_rule=equal_medians_or_twice_abs_median_difference_lt_max_range \
             retention_rule=retain_on_beats_or_parity"
        );

        let (baseline_warmup, expected) = measure(&executor, &image, false);
        let candidate_warmup = checked(&executor, &image, true, &expected);
        assert_eq!(baseline_warmup.rounds, candidate_warmup.rounds);
        assert_eq!(baseline_warmup.transitions, candidate_warmup.transitions);
        print_record("warmup", &fixture, baseline_warmup);
        print_record("warmup", &fixture, candidate_warmup);

        let mut measurements = Vec::with_capacity(SAMPLES * 2);
        for sample in 0..SAMPLES {
            let order = order_for_sample(sample);
            let role_splits = if order == "baseline_first" {
                [false, true]
            } else {
                [true, false]
            };
            for role_split in role_splits {
                let mut measurement = after_predecessor(&executor, &image, role_split, &expected);
                measurement.sample = sample;
                measurement.order = order;
                print_record("sample", &fixture, measurement);
                measurements.push(measurement);
            }
        }

        for engine in ["unmodified", "role_split"] {
            print_summary(&fixture, engine, &measurements);
        }
        let baseline = measurements
            .iter()
            .filter(|measurement| measurement.engine == "unmodified")
            .map(|measurement| measurement.backend_wall_ns)
            .collect::<Vec<_>>();
        let candidate = measurements
            .iter()
            .filter(|measurement| measurement.engine == "role_split")
            .map(|measurement| measurement.backend_wall_ns)
            .collect::<Vec<_>>();
        let baseline_median = median(baseline.clone());
        let candidate_median = median(candidate.clone());
        let outcome = formal_outcome(&candidate, &baseline);
        let paired_wins = candidate
            .iter()
            .zip(&baseline)
            .filter(|(candidate, baseline)| candidate < baseline)
            .count();
        println!(
            "record=t17c_role_split_formal fixture={fixture} baseline_wall_ns={baseline_median} \
             candidate_wall_ns={candidate_median} baseline_ns_per_round={} \
             candidate_ns_per_round={} candidate_over_baseline={:.9} improvement_percent={:.6} \
             baseline_range_ns={} candidate_range_ns={} paired_wins={paired_wins} \
             paired_samples={SAMPLES} outcome={outcome}",
            baseline_median / u128::from(baseline_warmup.rounds),
            candidate_median / u128::from(baseline_warmup.rounds),
            candidate_median as f64 / baseline_median as f64,
            (baseline_median as f64 - candidate_median as f64) / baseline_median as f64 * 100.0,
            retained_range(&baseline),
            retained_range(&candidate),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{FormalOutcome, SAMPLES, formal_outcome, median, order_for_sample};

    #[test]
    fn protocol_balances_two_samples_per_order() {
        assert_eq!(SAMPLES, 4);
        assert_eq!(
            (0..SAMPLES).map(order_for_sample).collect::<Vec<_>>(),
            vec![
                "baseline_first",
                "candidate_first",
                "baseline_first",
                "candidate_first"
            ]
        );
    }

    #[test]
    fn formal_rule_retains_only_beats_or_strict_dispersion_parity() {
        assert_eq!(
            formal_outcome(&[80, 81, 82, 83], &[100, 101, 102, 103]),
            FormalOutcome::Beats
        );
        assert_eq!(
            formal_outcome(&[101, 102, 103, 104], &[100, 102, 104, 106]),
            FormalOutcome::Parity
        );
        assert_eq!(
            formal_outcome(&[103, 104, 105, 106], &[100, 101, 102, 103]),
            FormalOutcome::Trails
        );
        assert_eq!(median(vec![1, 2, 3, 4]), 2);
    }
}

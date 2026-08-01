use std::path::PathBuf;
use std::time::Instant;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, CudaConfig, CudaExecutor, RunResult, RunSummary, SimulationImage, run_cpu,
    run_scalar_rounds,
};

use super::{
    BenchmarkWorkload, SCALAR_SAMPLES, benchmark_workload, compare_retained_samples,
    cpu_engine_name, cuda_crossover_summary, median, order_for_sample, parse_worker_sweep,
    recorded_predecessor_for_engine, split_fixed,
};

const COMPARISON_SAMPLES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Outcome {
    rounds: u64,
    transitions: u64,
    summary: RunSummary,
    resident_packets: usize,
    pending_events: usize,
}

#[derive(Clone, Copy, Debug)]
struct Measurement {
    sample: usize,
    order: &'static str,
    predecessor: &'static str,
    engine: &'static str,
    backend: &'static str,
    workers: usize,
    fixed_method: &'static str,
    separation_quality: &'static str,
    end_to_end_ns: u128,
    cold_end_to_end_ns: u128,
    fixed_ns: u128,
    warm_fixed_ns: u128,
    marginal_ns: u128,
    calibration_ns: u128,
    outcome: Outcome,
    backend_wall_ns: u64,
    device_ns: u64,
    host_submit_ns: u64,
    graph_capture_ns: u64,
    graph_replays: u64,
    encoded_attempts: u64,
    continuation_relaunches: u64,
    wave_boundary_syncs: u64,
    mid_round_wave_boundary_syncs: u64,
}

impl Measurement {
    fn marginal_ns_per_round(self) -> u128 {
        assert!(self.outcome.rounds > 0, "benchmark produced zero rounds");
        self.marginal_ns / u128::from(self.outcome.rounds)
    }

    fn device_ns_per_round(self) -> u128 {
        assert!(self.outcome.rounds > 0, "benchmark produced zero rounds");
        u128::from(self.device_ns) / u128::from(self.outcome.rounds)
    }
}

fn scalar_calibration(image: &SimulationImage) -> u128 {
    let started = Instant::now();
    let calibration =
        run_scalar_rounds(image, Some(0)).expect("scalar zero-round calibration must succeed");
    let calibration_ns = started.elapsed().as_nanos();
    assert!(
        calibration.rounds.is_empty(),
        "exclusive horizon zero must remain a zero-round calibration"
    );
    calibration_ns
}

fn scalar(
    image: &SimulationImage,
    calibration_ns: u128,
    expected_result: &RunResult,
) -> Measurement {
    let started = Instant::now();
    let run = run_scalar_rounds(image, None).expect("scalar benchmark run must succeed");
    let end_to_end_ns = started.elapsed().as_nanos();
    assert_eq!(
        &run.result, expected_result,
        "scalar benchmark RunResult must match the oracle"
    );
    let rounds = u64::try_from(run.rounds.len()).expect("scalar round count must fit in u64");
    let transitions = run.rounds.iter().fold(0_u64, |total, round| {
        total.saturating_add(round.events_processed)
    });
    let outcome = Outcome {
        rounds,
        transitions,
        summary: run.result.summary,
        resident_packets: run.result.resident_packets.len(),
        pending_events: run.result.pending_events.len(),
    };
    drop(run);

    let marginal_ns = end_to_end_ns
        .checked_sub(calibration_ns)
        .expect("scalar full-run wall must cover its zero-round calibration");
    let (warm_fixed_ns, marginal_ns) = split_fixed(end_to_end_ns, marginal_ns);
    Measurement {
        sample: 0,
        order: "warmup",
        predecessor: "none",
        engine: "scalar",
        backend: "scalar",
        workers: 1,
        fixed_method: "zero_round_calibration",
        separation_quality: "calibrated_estimate",
        end_to_end_ns,
        cold_end_to_end_ns: end_to_end_ns,
        fixed_ns: warm_fixed_ns,
        warm_fixed_ns,
        marginal_ns,
        calibration_ns,
        outcome,
        backend_wall_ns: 0,
        device_ns: 0,
        host_submit_ns: 0,
        graph_capture_ns: 0,
        graph_replays: 0,
        encoded_attempts: 0,
        continuation_relaunches: 0,
        wave_boundary_syncs: 0,
        mid_round_wave_boundary_syncs: 0,
    }
}

fn cpu(
    image: &SimulationImage,
    workers: usize,
    best_workers: usize,
    expected_result: &RunResult,
) -> Measurement {
    let engine = cpu_engine_name(workers, best_workers);
    let started = Instant::now();
    let run = run_cpu(
        image,
        None,
        CpuConfig {
            workers,
            ..CpuConfig::default()
        },
    )
    .unwrap_or_else(|error| panic!("{engine} W{workers} benchmark run failed: {error}"));
    let end_to_end_ns = started.elapsed().as_nanos();
    assert_eq!(
        &run.result, expected_result,
        "{engine} W{workers} benchmark RunResult must match the oracle"
    );
    let rounds = u64::try_from(run.rounds.len()).expect("CPU round count must fit in u64");
    let transitions = run.rounds.iter().fold(0_u64, |total, round| {
        total.saturating_add(round.semantic.events_processed)
    });
    let marginal_ns = run.rounds.iter().fold(0_u128, |total, round| {
        total.saturating_add(u128::from(round.round_wall_time_ns))
    });
    let outcome = Outcome {
        rounds,
        transitions,
        summary: run.result.summary,
        resident_packets: run.result.resident_packets.len(),
        pending_events: run.result.pending_events.len(),
    };
    drop(run);

    let (warm_fixed_ns, marginal_ns) = split_fixed(end_to_end_ns, marginal_ns);
    Measurement {
        sample: 0,
        order: "warmup",
        predecessor: "none",
        engine,
        backend: "cpu",
        workers,
        fixed_method: "round_wall_sum",
        separation_quality: "exact_boundary",
        end_to_end_ns,
        cold_end_to_end_ns: end_to_end_ns,
        fixed_ns: warm_fixed_ns,
        warm_fixed_ns,
        marginal_ns,
        calibration_ns: 0,
        outcome,
        backend_wall_ns: 0,
        device_ns: 0,
        host_submit_ns: 0,
        graph_capture_ns: 0,
        graph_replays: 0,
        encoded_attempts: 0,
        continuation_relaunches: 0,
        wave_boundary_syncs: 0,
        mid_round_wave_boundary_syncs: 0,
    }
}

fn cuda(
    executor: &CudaExecutor,
    image: &SimulationImage,
    round_threads_per_block: usize,
    initialization_ns: u128,
    expected_result: &RunResult,
) -> Measurement {
    let started = Instant::now();
    let run = executor
        .run(
            image,
            None,
            CudaConfig {
                round_threads_per_block,
                ..CudaConfig::default()
            },
        )
        .expect("CUDA benchmark run must succeed");
    let end_to_end_ns = started.elapsed().as_nanos();
    assert_eq!(
        &run.result, expected_result,
        "CUDA benchmark RunResult must match the oracle"
    );
    let outcome = Outcome {
        rounds: run.rounds,
        transitions: run.transitions,
        summary: run.result.summary,
        resident_packets: run.result.resident_packets.len(),
        pending_events: run.result.pending_events.len(),
    };
    let marginal_ns = u128::from(run.wall_ns);
    let backend_wall_ns = run.wall_ns;
    let device_ns = run.device_ns;
    let host_submit_ns = run.host_submit_ns;
    let graph_capture_ns = run.graph_capture_ns;
    let graph_replays = run.graph_replays;
    let encoded_attempts = run.encoded_attempts;
    let continuation_relaunches = run.continuation_relaunches;
    let wave_boundary_syncs = run.wave_boundary_syncs;
    let mid_round_wave_boundary_syncs = run.mid_round_wave_boundary_syncs;
    drop(run);

    let (warm_fixed_ns, marginal_ns) = split_fixed(end_to_end_ns, marginal_ns);
    let fixed_ns = warm_fixed_ns
        .checked_add(initialization_ns)
        .expect("cold CUDA fixed time must fit in u128");
    let cold_end_to_end_ns = end_to_end_ns
        .checked_add(initialization_ns)
        .expect("cold CUDA end-to-end time must fit in u128");
    assert_eq!(
        fixed_ns.checked_add(marginal_ns),
        Some(cold_end_to_end_ns),
        "cold CUDA fixed and marginal time must close"
    );

    Measurement {
        sample: 0,
        order: "warmup",
        predecessor: "none",
        engine: "cuda",
        backend: "cuda",
        workers: 0,
        fixed_method: "backend_wall_with_cold_initialization",
        separation_quality: "exact_boundary",
        end_to_end_ns,
        cold_end_to_end_ns,
        fixed_ns,
        warm_fixed_ns,
        marginal_ns,
        calibration_ns: 0,
        outcome,
        backend_wall_ns,
        device_ns,
        host_submit_ns,
        graph_capture_ns,
        graph_replays,
        encoded_attempts,
        continuation_relaunches,
        wave_boundary_syncs,
        mid_round_wave_boundary_syncs,
    }
}

fn cpu_after_predecessor(
    image: &SimulationImage,
    workers: usize,
    best_workers: usize,
    expected_result: &RunResult,
) -> Measurement {
    let _ = cpu(image, workers, best_workers, expected_result);
    let mut measurement = cpu(image, workers, best_workers, expected_result);
    measurement.predecessor = recorded_predecessor_for_engine(measurement.engine);
    measurement
}

fn cuda_after_predecessor(
    executor: &CudaExecutor,
    image: &SimulationImage,
    round_threads_per_block: usize,
    initialization_ns: u128,
    expected_result: &RunResult,
) -> Measurement {
    let _ = cuda(
        executor,
        image,
        round_threads_per_block,
        initialization_ns,
        expected_result,
    );
    let mut measurement = cuda(
        executor,
        image,
        round_threads_per_block,
        initialization_ns,
        expected_result,
    );
    measurement.predecessor = recorded_predecessor_for_engine(measurement.engine);
    measurement
}

fn print_record(
    kind: &str,
    fixture: &str,
    workload: BenchmarkWorkload,
    round_threads_per_block: usize,
    measurement: Measurement,
) {
    println!(
        "record=t17b_sustained_{kind} config={fixture} workload={} rq={} sample={} order={} predecessor={} \
         engine={} backend={} workers={} round_threads_per_block={} rounds={} transitions={} \
         fixed_method={} separation_quality={} end_to_end_ns={} cold_end_to_end_ns={} \
         fixed_ns={} warm_fixed_ns={} marginal_ns={} marginal_ns_per_round={} calibration_ns={} \
         backend_wall_ns={} device_ns={} device_ns_per_round={} host_submit_ns={} \
         graph_capture_ns={} graph_replays={} encoded_attempts={} continuation_relaunches={} \
         wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
        workload.workload(),
        workload.rq(),
        measurement.sample,
        measurement.order,
        measurement.predecessor,
        measurement.engine,
        measurement.backend,
        measurement.workers,
        round_threads_per_block,
        measurement.outcome.rounds,
        measurement.outcome.transitions,
        measurement.fixed_method,
        measurement.separation_quality,
        measurement.end_to_end_ns,
        measurement.cold_end_to_end_ns,
        measurement.fixed_ns,
        measurement.warm_fixed_ns,
        measurement.marginal_ns,
        measurement.marginal_ns_per_round(),
        measurement.calibration_ns,
        measurement.backend_wall_ns,
        measurement.device_ns,
        measurement.device_ns_per_round(),
        measurement.host_submit_ns,
        measurement.graph_capture_ns,
        measurement.graph_replays,
        measurement.encoded_attempts,
        measurement.continuation_relaunches,
        measurement.wave_boundary_syncs,
        measurement.mid_round_wave_boundary_syncs,
    );
}

fn print_summary(
    kind: &str,
    fixture: &str,
    workload: BenchmarkWorkload,
    order: &str,
    round_threads_per_block: usize,
    selected: &[Measurement],
) {
    assert!(!selected.is_empty(), "summary selection must be nonempty");
    let first = selected[0];
    assert!(
        selected.iter().all(|measurement| {
            measurement.engine == first.engine
                && measurement.workers == first.workers
                && measurement.outcome == first.outcome
                && measurement.predecessor == first.predecessor
                && measurement.fixed_method == first.fixed_method
                && measurement.separation_quality == first.separation_quality
        }),
        "summary selection must describe one engine and outcome"
    );
    let end_to_end_ns = median(selected.iter().map(|measurement| measurement.end_to_end_ns));
    let cold_end_to_end_ns = median(
        selected
            .iter()
            .map(|measurement| measurement.cold_end_to_end_ns),
    );
    let marginal_ns = median(selected.iter().map(|measurement| measurement.marginal_ns));
    let (warm_fixed_ns, _) = split_fixed(end_to_end_ns, marginal_ns);
    let (fixed_ns, _) = split_fixed(cold_end_to_end_ns, marginal_ns);
    println!(
        "record=t17b_sustained_{kind} statistic=median \
         aggregation=component_medians_with_derived_fixed_closure config={fixture} workload={} rq={} order={order} \
         predecessor={} engine={} backend={} workers={} samples={} round_threads_per_block={} \
         rounds={} transitions={} fixed_method={} separation_quality={} end_to_end_ns={} \
         cold_end_to_end_ns={} fixed_ns={} warm_fixed_ns={} marginal_ns={} \
         marginal_ns_per_round={} calibration_ns={} backend_wall_ns={} device_ns={} \
         device_ns_per_round={} host_submit_ns={} graph_capture_ns={} graph_replays={} \
         encoded_attempts={} continuation_relaunches={} wave_boundary_syncs={} \
         mid_round_wave_boundary_syncs={}",
        workload.workload(),
        workload.rq(),
        first.predecessor,
        first.engine,
        first.backend,
        first.workers,
        selected.len(),
        round_threads_per_block,
        first.outcome.rounds,
        first.outcome.transitions,
        first.fixed_method,
        first.separation_quality,
        end_to_end_ns,
        cold_end_to_end_ns,
        fixed_ns,
        warm_fixed_ns,
        marginal_ns,
        median(
            selected
                .iter()
                .copied()
                .map(Measurement::marginal_ns_per_round)
        ),
        median(
            selected
                .iter()
                .map(|measurement| measurement.calibration_ns)
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.backend_wall_ns))
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.device_ns))
        ),
        median(
            selected
                .iter()
                .copied()
                .map(Measurement::device_ns_per_round)
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.host_submit_ns))
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.graph_capture_ns))
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.graph_replays))
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.encoded_attempts))
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.continuation_relaunches))
        ),
        median(
            selected
                .iter()
                .map(|measurement| u128::from(measurement.wave_boundary_syncs))
        ),
        median(
            selected
                .iter()
                .map(|measurement| { u128::from(measurement.mid_round_wave_boundary_syncs) })
        ),
    );
}

fn run_worker_sweep(
    image: &SimulationImage,
    fixture: &str,
    workload: BenchmarkWorkload,
    workers: &[usize],
) {
    let mut expected_result = None;
    for &worker_count in workers {
        let started = Instant::now();
        let run = run_cpu(
            image,
            None,
            CpuConfig {
                workers: worker_count,
                ..CpuConfig::default()
            },
        )
        .unwrap_or_else(|error| panic!("W{worker_count} worker sweep run failed: {error}"));
        let end_to_end_ns = started.elapsed().as_nanos();
        if let Some(expected) = &expected_result {
            assert_eq!(
                &run.result, expected,
                "W{worker_count} worker sweep RunResult must match the first sweep run"
            );
        } else {
            expected_result = Some(run.result.clone());
        }
        let rounds =
            u64::try_from(run.rounds.len()).expect("worker sweep round count must fit in u64");
        let transitions = run.rounds.iter().fold(0_u64, |total, round| {
            total.saturating_add(round.semantic.events_processed)
        });
        let marginal_ns = run.rounds.iter().fold(0_u128, |total, round| {
            total.saturating_add(u128::from(round.round_wall_time_ns))
        });
        let (fixed_ns, marginal_ns) = split_fixed(end_to_end_ns, marginal_ns);
        println!(
            "record=t17b_worker_sweep config={fixture} workload={} rq={} workers={worker_count} samples=1 \
             predecessor=none rounds={rounds} transitions={transitions} \
             fixed_ns={fixed_ns} marginal_ns={marginal_ns} \
             marginal_ns_per_round={}",
            workload.workload(),
            workload.rq(),
            marginal_ns / u128::from(rounds)
        );
    }
}

pub fn main() {
    let mut fixture =
        "configs/benchmarks/width_via_load_full/fattree_k32_load_30_sustained.toml".to_owned();
    let mut fixture_was_set = false;
    let mut samples = COMPARISON_SAMPLES;
    let mut best_workers = 19_usize;
    let mut round_threads_per_block = 256_usize;
    let mut worker_sweep = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--samples" => {
                samples = arguments
                    .next()
                    .expect("--samples requires a value")
                    .parse()
                    .expect("--samples must be an integer");
            }
            "--best-workers" => {
                best_workers = arguments
                    .next()
                    .expect("--best-workers requires a value")
                    .parse()
                    .expect("--best-workers must be an integer");
            }
            "--round-threads-per-block" | "--threadgroup-width" => {
                round_threads_per_block = arguments
                    .next()
                    .expect("--round-threads-per-block requires a value")
                    .parse()
                    .expect("--round-threads-per-block must be an integer");
            }
            "--worker-sweep" => {
                let value = arguments.next().expect("--worker-sweep requires a value");
                worker_sweep = Some(
                    parse_worker_sweep(&value)
                        .unwrap_or_else(|error| panic!("invalid --worker-sweep: {error}")),
                );
            }
            unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
            path if !fixture_was_set => {
                fixture = path.to_owned();
                fixture_was_set = true;
            }
            extra => panic!("unexpected second fixture path {extra}"),
        }
    }
    assert_eq!(
        samples, COMPARISON_SAMPLES,
        "the T17b protocol requires exactly four comparison samples, two per order"
    );
    assert!(
        best_workers > 0 && best_workers != 4,
        "--best-workers must be nonzero and distinct from the fixed W4 baseline"
    );
    assert!(
        round_threads_per_block > 0,
        "--round-threads-per-block must be nonzero"
    );

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let workload = benchmark_workload(&image);
    if let Some(workers) = worker_sweep {
        run_worker_sweep(&image, &fixture, workload, &workers);
        return;
    }
    let oracle = run_cpu(
        &image,
        None,
        CpuConfig {
            workers: 4,
            ..CpuConfig::default()
        },
    )
    .expect("W4 benchmark oracle must succeed")
    .result;
    let executor = CudaExecutor::new().expect("CUDA benchmark executor must initialize");
    let initialization = executor.initialization_timings();
    let initialization_ns = u128::from(initialization.context_stream_setup_ns)
        .checked_add(u128::from(initialization.module_function_load_ns))
        .expect("CUDA initialization time must fit in u128");

    println!(
        "record=t17b_sustained_protocol workload={} rq={} scalar_samples={SCALAR_SAMPLES} scalar_predecessor=none \
         comparison_samples={samples} comparison_predecessor=same_kind_discarded \
         order_schedule=balanced_cpu_first_gpu_first comparison_outcome_values=beats,parity,trails \
         comparison_dispersion=maximum_retained_sample_range \
         comparison_parity_rule=equal_medians_or_twice_abs_median_difference_lt_dispersion \
         paired_comparison_rule=same_sample_index best_workers={best_workers} \
         run_multiplicity=2xscalar+10xw4+9xwbest+9xcuda \
         scalar_calibration_multiplicity=2",
        workload.workload(),
        workload.rq(),
    );
    println!(
        "record=t17b_sustained_initialization config={fixture} workload={} rq={} engine=cuda \
         context_stream_setup_ns={} module_function_load_ns={} initialization_ns={} \
         graph_capture_treatment=per_run_marginal_backend_wall",
        workload.workload(),
        workload.rq(),
        initialization.context_stream_setup_ns,
        initialization.module_function_load_ns,
        initialization_ns,
    );

    let warmups = [
        cpu(&image, 4, best_workers, &oracle),
        cpu(&image, best_workers, best_workers, &oracle),
        cuda(
            &executor,
            &image,
            round_threads_per_block,
            initialization_ns,
            &oracle,
        ),
    ];
    let expected = warmups[0].outcome;
    for warmup in warmups {
        assert_eq!(warmup.outcome, expected, "warmup backends must agree");
        print_record(
            "warmup",
            &fixture,
            workload,
            round_threads_per_block,
            warmup,
        );
    }

    let mut measurements =
        Vec::with_capacity(samples.saturating_mul(3).saturating_add(SCALAR_SAMPLES));
    for sample in 0..samples {
        let order = order_for_sample(sample);
        let include_scalar = sample < SCALAR_SAMPLES;
        let mut ordered = Vec::with_capacity(3 + usize::from(include_scalar));
        if order == "cpu_first" {
            if include_scalar {
                ordered.push(scalar(&image, scalar_calibration(&image), &oracle));
            }
            ordered.extend([
                cpu_after_predecessor(&image, 4, best_workers, &oracle),
                cpu_after_predecessor(&image, best_workers, best_workers, &oracle),
                cuda_after_predecessor(
                    &executor,
                    &image,
                    round_threads_per_block,
                    initialization_ns,
                    &oracle,
                ),
            ]);
        } else {
            ordered.push(cuda_after_predecessor(
                &executor,
                &image,
                round_threads_per_block,
                initialization_ns,
                &oracle,
            ));
            if include_scalar {
                ordered.push(scalar(&image, scalar_calibration(&image), &oracle));
            }
            ordered.extend([
                cpu_after_predecessor(&image, 4, best_workers, &oracle),
                cpu_after_predecessor(&image, best_workers, best_workers, &oracle),
            ]);
        }
        for mut measurement in ordered {
            measurement.sample = sample;
            measurement.order = order;
            assert_eq!(
                measurement.outcome, expected,
                "timed sample backends must agree"
            );
            print_record(
                "sample",
                &fixture,
                workload,
                round_threads_per_block,
                measurement,
            );
            measurements.push(measurement);
        }
    }

    for engine in ["scalar", "w4", "wbest", "cuda"] {
        let pooled = measurements
            .iter()
            .filter(|measurement| measurement.engine == engine)
            .copied()
            .collect::<Vec<_>>();
        if pooled.is_empty() {
            continue;
        }
        print_summary(
            "summary",
            &fixture,
            workload,
            "pooled",
            round_threads_per_block,
            &pooled,
        );
        for order in ["cpu_first", "gpu_first"] {
            let ordered = pooled
                .iter()
                .filter(|measurement| measurement.order == order)
                .copied()
                .collect::<Vec<_>>();
            if !ordered.is_empty() {
                print_summary(
                    "order_summary",
                    &fixture,
                    workload,
                    order,
                    round_threads_per_block,
                    &ordered,
                );
            }
        }
    }

    let retained_marginals = |engine| {
        measurements
            .iter()
            .filter(|measurement| measurement.engine == engine)
            .map(|measurement| measurement.marginal_ns)
            .collect::<Vec<_>>()
    };
    let scalar_samples = measurements
        .iter()
        .filter(|measurement| measurement.engine == "scalar")
        .count();
    let w4_samples = retained_marginals("w4");
    let wbest_samples = retained_marginals("wbest");
    let cuda_samples = retained_marginals("cuda");
    let cuda_vs_w4 = compare_retained_samples(&cuda_samples, &w4_samples);
    let cuda_vs_wbest = compare_retained_samples(&cuda_samples, &wbest_samples);
    assert_eq!(cuda_vs_w4.paired_comparisons, samples);
    assert_eq!(cuda_vs_wbest.paired_comparisons, samples);

    let w4_marginal_ns = cuda_vs_w4.reference_median_ns;
    let wbest_marginal_ns = cuda_vs_wbest.reference_median_ns;
    let cuda_marginal_ns = cuda_vs_w4.candidate_median_ns;
    assert!(
        w4_marginal_ns > 0 && wbest_marginal_ns > 0 && cuda_marginal_ns > 0,
        "marginal-ratio inputs must be nonzero"
    );
    let cuda_over_w4 = cuda_marginal_ns as f64 / w4_marginal_ns as f64;
    let cuda_over_wbest = cuda_marginal_ns as f64 / wbest_marginal_ns as f64;
    let w4_over_cuda = w4_marginal_ns as f64 / cuda_marginal_ns as f64;
    let wbest_over_cuda = wbest_marginal_ns as f64 / cuda_marginal_ns as f64;
    let crossover = cuda_crossover_summary(cuda_vs_w4.outcome, cuda_vs_wbest.outcome);
    println!(
        "record=t17b_sustained_pooled_summary statistic=median config={fixture} workload={} rq={} \
         scalar_samples={scalar_samples} comparison_samples={samples} best_workers={best_workers} \
         round_threads_per_block={round_threads_per_block} rounds={} transitions={} \
         ratio_definition=cuda_marginal_div_cpu_marginal \
         speedup_definition=cpu_marginal_div_cuda_marginal \
         w4_marginal_ns={w4_marginal_ns} wbest_marginal_ns={wbest_marginal_ns} \
         cuda_marginal_ns={cuda_marginal_ns} cuda_over_w4={cuda_over_w4:.6} \
         cuda_over_wbest={cuda_over_wbest:.6} w4_over_cuda={w4_over_cuda:.6} \
         wbest_over_cuda={wbest_over_cuda:.6} cuda_retained_range_ns={} \
         w4_retained_range_ns={} wbest_retained_range_ns={} \
         cuda_vs_w4_parity_bound_range_ns={} cuda_vs_wbest_parity_bound_range_ns={} \
         cuda_vs_w4_outcome={} cuda_vs_wbest_outcome={} \
         cuda_wins_vs_w4={} cuda_vs_w4_paired_samples={} \
         cuda_wins_vs_wbest={} cuda_vs_wbest_paired_samples={} \
         crossover={crossover} crossover_basis=dispersion_qualified_marginal",
        workload.workload(),
        workload.rq(),
        expected.rounds,
        expected.transitions,
        cuda_vs_w4.candidate_range_ns,
        cuda_vs_w4.reference_range_ns,
        cuda_vs_wbest.reference_range_ns,
        cuda_vs_w4.parity_bound_range_ns,
        cuda_vs_wbest.parity_bound_range_ns,
        cuda_vs_w4.outcome.as_str(),
        cuda_vs_wbest.outcome.as_str(),
        cuda_vs_w4.candidate_wins,
        cuda_vs_w4.paired_comparisons,
        cuda_vs_wbest.candidate_wins,
        cuda_vs_wbest.paired_comparisons,
    );
}

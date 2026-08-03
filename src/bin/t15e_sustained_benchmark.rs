#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn median(values: impl Iterator<Item = u128>) -> u128 {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    match values.len() {
        0 => 0,
        length if length % 2 == 1 => values[length / 2],
        length => {
            values[length / 2 - 1]
                .checked_add(values[length / 2])
                .expect("median pair sum must fit in u128")
                / 2
        }
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BenchmarkWorkload {
    Empty,
    OpenLoop,
    Tcp,
    MixedTcp,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
impl BenchmarkWorkload {
    const fn workload(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::OpenLoop => "open_loop",
            Self::Tcp => "tcp",
            Self::MixedTcp => "mixed_tcp",
        }
    }

    const fn rq(self) -> &'static str {
        match self {
            Self::Tcp | Self::MixedTcp => "RQ9",
            Self::OpenLoop => "legacy",
            Self::Empty => "na",
        }
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
const fn classify_workload(has_tcp: bool, has_open_loop: bool) -> BenchmarkWorkload {
    match (has_tcp, has_open_loop) {
        (false, false) => BenchmarkWorkload::Empty,
        (false, true) => BenchmarkWorkload::OpenLoop,
        (true, false) => BenchmarkWorkload::Tcp,
        (true, true) => BenchmarkWorkload::MixedTcp,
    }
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn benchmark_workload(image: &days_executor::SimulationImage) -> BenchmarkWorkload {
    let mut has_tcp = false;
    let mut has_open_loop = false;
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        match generator.kind {
            days_executor::FlowGeneratorKind::Tcp(_) => has_tcp = true,
            days_executor::FlowGeneratorKind::Constant(_)
            | days_executor::FlowGeneratorKind::Rate(_)
            | days_executor::FlowGeneratorKind::Collective(_)
            | days_executor::FlowGeneratorKind::Dcqcn(_) => has_open_loop = true,
        }
    }
    classify_workload(has_tcp, has_open_loop)
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ComparisonOutcome {
    Beats,
    Parity,
    Trails,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
impl ComparisonOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Beats => "beats",
            Self::Parity => "parity",
            Self::Trails => "trails",
        }
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetainedSampleComparison {
    candidate_median_ns: u128,
    reference_median_ns: u128,
    candidate_range_ns: u128,
    reference_range_ns: u128,
    parity_bound_range_ns: u128,
    outcome: ComparisonOutcome,
    candidate_wins: usize,
    paired_comparisons: usize,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn retained_range(values: &[u128]) -> u128 {
    let minimum = values
        .iter()
        .min()
        .expect("retained comparison samples must not be empty");
    let maximum = values
        .iter()
        .max()
        .expect("retained comparison samples must not be empty");
    maximum - minimum
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn compare_retained_samples(
    candidate_samples: &[u128],
    reference_samples: &[u128],
) -> RetainedSampleComparison {
    assert!(
        !candidate_samples.is_empty(),
        "retained comparison samples must not be empty"
    );
    assert_eq!(
        candidate_samples.len(),
        reference_samples.len(),
        "paired retained comparisons must have equal sample counts"
    );

    let candidate_median_ns = median(candidate_samples.iter().copied());
    let reference_median_ns = median(reference_samples.iter().copied());
    let candidate_range_ns = retained_range(candidate_samples);
    let reference_range_ns = retained_range(reference_samples);
    let parity_bound_range_ns = candidate_range_ns.max(reference_range_ns);
    let median_difference_ns = candidate_median_ns.abs_diff(reference_median_ns);
    let outcome = if median_difference_ns == 0
        || median_difference_ns.saturating_mul(2) < parity_bound_range_ns
    {
        ComparisonOutcome::Parity
    } else if candidate_median_ns < reference_median_ns {
        ComparisonOutcome::Beats
    } else {
        ComparisonOutcome::Trails
    };
    let candidate_wins = candidate_samples
        .iter()
        .zip(reference_samples)
        .filter(|(candidate, reference)| candidate < reference)
        .count();

    RetainedSampleComparison {
        candidate_median_ns,
        reference_median_ns,
        candidate_range_ns,
        reference_range_ns,
        parity_bound_range_ns,
        outcome,
        candidate_wins,
        paired_comparisons: candidate_samples.len(),
    }
}

#[cfg(any(test, all(feature = "metal-spike", target_vendor = "apple")))]
fn crossover_summary(w4: ComparisonOutcome, w18: ComparisonOutcome) -> &'static str {
    match (w4, w18) {
        (ComparisonOutcome::Beats, ComparisonOutcome::Beats) => "metal_beats_w4_and_w18",
        (ComparisonOutcome::Beats, ComparisonOutcome::Parity) => "metal_beats_w4_parity_w18",
        (ComparisonOutcome::Beats, ComparisonOutcome::Trails) => "metal_beats_w4_trails_w18",
        (ComparisonOutcome::Parity, ComparisonOutcome::Beats) => "metal_parity_w4_beats_w18",
        (ComparisonOutcome::Parity, ComparisonOutcome::Parity) => "metal_parity_w4_and_w18",
        (ComparisonOutcome::Parity, ComparisonOutcome::Trails) => "metal_parity_w4_trails_w18",
        (ComparisonOutcome::Trails, ComparisonOutcome::Beats) => "metal_trails_w4_beats_w18",
        (ComparisonOutcome::Trails, ComparisonOutcome::Parity) => "metal_trails_w4_parity_w18",
        (ComparisonOutcome::Trails, ComparisonOutcome::Trails) => "metal_trails_w4_and_w18",
    }
}

#[cfg(any(
    test,
    all(
        feature = "cuda",
        not(all(feature = "metal-spike", target_vendor = "apple"))
    )
))]
fn cuda_crossover_summary(w4: ComparisonOutcome, wbest: ComparisonOutcome) -> &'static str {
    match (w4, wbest) {
        (ComparisonOutcome::Beats, ComparisonOutcome::Beats) => "cuda_beats_w4_and_wbest",
        (ComparisonOutcome::Beats, ComparisonOutcome::Parity) => "cuda_beats_w4_parity_wbest",
        (ComparisonOutcome::Beats, ComparisonOutcome::Trails) => "cuda_beats_w4_trails_wbest",
        (ComparisonOutcome::Parity, ComparisonOutcome::Beats) => "cuda_parity_w4_beats_wbest",
        (ComparisonOutcome::Parity, ComparisonOutcome::Parity) => "cuda_parity_w4_and_wbest",
        (ComparisonOutcome::Parity, ComparisonOutcome::Trails) => "cuda_parity_w4_trails_wbest",
        (ComparisonOutcome::Trails, ComparisonOutcome::Beats) => "cuda_trails_w4_beats_wbest",
        (ComparisonOutcome::Trails, ComparisonOutcome::Parity) => "cuda_trails_w4_parity_wbest",
        (ComparisonOutcome::Trails, ComparisonOutcome::Trails) => "cuda_trails_w4_and_wbest",
    }
}

#[cfg(any(
    test,
    all(
        feature = "cuda",
        not(all(feature = "metal-spike", target_vendor = "apple"))
    )
))]
fn cpu_engine_name(workers: usize, best_workers: usize) -> &'static str {
    if workers == 4 {
        "w4"
    } else if workers == best_workers {
        "wbest"
    } else {
        "cpu"
    }
}

#[cfg(any(
    test,
    all(
        feature = "cuda",
        not(all(feature = "metal-spike", target_vendor = "apple"))
    )
))]
fn parse_worker_sweep(value: &str) -> Result<Vec<usize>, String> {
    let workers = value
        .split(',')
        .map(|worker| {
            worker
                .parse::<usize>()
                .map_err(|_| "worker sweep counts must be integers".to_owned())
        })
        .collect::<Result<Vec<_>, _>>()?;
    if workers.is_empty() {
        return Err("worker sweep must include at least one count".to_owned());
    }
    if workers.contains(&0) {
        return Err("worker sweep counts must be nonzero".to_owned());
    }
    let mut unique = workers.clone();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() != workers.len() {
        return Err("worker sweep counts must be unique".to_owned());
    }
    Ok(workers)
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn split_fixed(total_ns: u128, marginal_ns: u128) -> (u128, u128) {
    let fixed_ns = total_ns
        .checked_sub(marginal_ns)
        .expect("measured marginal time must not exceed enclosing wall time");
    assert_eq!(
        fixed_ns.checked_add(marginal_ns),
        Some(total_ns),
        "fixed and marginal time must close to enclosing wall time"
    );
    (fixed_ns, marginal_ns)
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn order_for_sample(sample: usize) -> &'static str {
    if sample.is_multiple_of(2) {
        "cpu_first"
    } else {
        "gpu_first"
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
const SCALAR_SAMPLES: usize = 2;

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn recorded_predecessor_for_engine(engine: &str) -> &'static str {
    if engine == "scalar" {
        "none"
    } else {
        "same_kind_discarded"
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{
        CpuConfig, MetalConfig, MetalExecutor, RunResult, RunSummary, SimulationImage, run_cpu,
        run_scalar_rounds,
    };

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
        stream_mode: &'static str,
        metal_mode_order: &'static str,
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
        host_encode_submit_ns: u64,
        encoded_attempts: u64,
        continuation_relaunches: u64,
        wave_boundary_syncs: u64,
        mid_round_wave_boundary_syncs: u64,
    }

    impl Measurement {
        fn marginal_ns_per_round(self) -> u128 {
            assert!(
                self.outcome.rounds > 0,
                "benchmark run produced zero rounds"
            );
            self.marginal_ns / u128::from(self.outcome.rounds)
        }
    }

    fn scalar_calibration(image: &SimulationImage) -> u128 {
        let started = Instant::now();
        let calibration =
            run_scalar_rounds(image, Some(0)).expect("scalar zero-round calibration must succeed");
        let calibration_ns = started.elapsed().as_nanos();
        assert_eq!(
            calibration.rounds.len(),
            0,
            "exclusive horizon zero must remain a zero-round calibration"
        );
        drop(calibration);
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
        assert!(rounds > 0, "scalar benchmark run produced zero rounds");
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
            stream_mode: "na",
            metal_mode_order: "na",
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
            host_encode_submit_ns: 0,
            encoded_attempts: 0,
            continuation_relaunches: 0,
            wave_boundary_syncs: 0,
            mid_round_wave_boundary_syncs: 0,
        }
    }

    fn cpu(image: &SimulationImage, workers: usize, expected_result: &RunResult) -> Measurement {
        let started = Instant::now();
        let run = run_cpu(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
        )
        .unwrap_or_else(|error| panic!("W{workers} benchmark run failed: {error}"));
        let end_to_end_ns = started.elapsed().as_nanos();
        assert_eq!(
            &run.result, expected_result,
            "W{workers} benchmark RunResult must match the oracle"
        );
        let rounds = u64::try_from(run.rounds.len()).expect("CPU round count must fit in u64");
        assert!(rounds > 0, "W{workers} benchmark run produced zero rounds");
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
        let engine = match workers {
            4 => "w4",
            18 => "w18",
            _ => "cpu",
        };
        Measurement {
            sample: 0,
            order: "warmup",
            predecessor: "none",
            engine,
            backend: "cpu",
            stream_mode: "na",
            metal_mode_order: "na",
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
            host_encode_submit_ns: 0,
            encoded_attempts: 0,
            continuation_relaunches: 0,
            wave_boundary_syncs: 0,
            mid_round_wave_boundary_syncs: 0,
        }
    }

    fn metal(
        executor: &MetalExecutor,
        image: &SimulationImage,
        threadgroup_width: usize,
        device_queue_setup_ns: u128,
        pipeline_creation_ns: u128,
        streams_enabled: bool,
        expected_result: &RunResult,
    ) -> Measurement {
        let started = Instant::now();
        let run = executor
            .run(
                image,
                None,
                MetalConfig {
                    streams_enabled,
                    round_threads_per_threadgroup: threadgroup_width,
                    ..MetalConfig::default()
                },
            )
            .expect("Metal benchmark run must succeed");
        let end_to_end_ns = started.elapsed().as_nanos();
        assert_eq!(
            &run.result, expected_result,
            "Metal benchmark RunResult must match the oracle"
        );
        assert!(run.rounds > 0, "Metal benchmark run produced zero rounds");
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
        let host_encode_submit_ns = run.host_encode_submit_ns;
        let encoded_attempts = run.encoded_attempts;
        let continuation_relaunches = run.continuation_relaunches;
        let wave_boundary_syncs = run.wave_boundary_syncs;
        let mid_round_wave_boundary_syncs = run.mid_round_wave_boundary_syncs;
        drop(run);

        let (warm_fixed_ns, marginal_ns) = split_fixed(end_to_end_ns, marginal_ns);
        let initialization_ns = device_queue_setup_ns
            .checked_add(pipeline_creation_ns)
            .expect("Metal initialization time must fit in u128");
        let fixed_ns = warm_fixed_ns
            .checked_add(initialization_ns)
            .expect("cold Metal fixed time must fit in u128");
        let cold_end_to_end_ns = end_to_end_ns
            .checked_add(initialization_ns)
            .expect("cold Metal end-to-end time must fit in u128");
        assert_eq!(
            fixed_ns.checked_add(marginal_ns),
            Some(cold_end_to_end_ns),
            "cold Metal fixed and marginal time must close"
        );

        Measurement {
            sample: 0,
            order: "warmup",
            predecessor: "none",
            engine: if streams_enabled {
                "metal_streams"
            } else {
                "metal_heap"
            },
            backend: "metal",
            stream_mode: if streams_enabled { "streams" } else { "heap" },
            metal_mode_order: "warmup",
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
            host_encode_submit_ns,
            encoded_attempts,
            continuation_relaunches,
            wave_boundary_syncs,
            mid_round_wave_boundary_syncs,
        }
    }

    fn cpu_after_predecessor(
        image: &SimulationImage,
        workers: usize,
        expected_result: &RunResult,
    ) -> Measurement {
        let _ = cpu(image, workers, expected_result);
        let mut measurement = cpu(image, workers, expected_result);
        measurement.predecessor = recorded_predecessor_for_engine(measurement.engine);
        measurement
    }

    fn metal_after_predecessor(
        executor: &MetalExecutor,
        image: &SimulationImage,
        threadgroup_width: usize,
        device_queue_setup_ns: u128,
        pipeline_creation_ns: u128,
        streams_enabled: bool,
        expected_result: &RunResult,
    ) -> Measurement {
        let _ = metal(
            executor,
            image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
            streams_enabled,
            expected_result,
        );
        let mut measurement = metal(
            executor,
            image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
            streams_enabled,
            expected_result,
        );
        measurement.predecessor = recorded_predecessor_for_engine(measurement.engine);
        measurement
    }

    fn print_record(
        kind: &str,
        fixture: &str,
        workload: BenchmarkWorkload,
        threadgroup_width: usize,
        measurement: Measurement,
    ) {
        println!(
            "record=t15e_{kind} config={fixture} workload={} rq={} sample={} order={} predecessor={} \
             engine={} backend={} stream_mode={} metal_mode_order={} workers={} \
             threadgroup_width={} rounds={} transitions={} fixed_method={} separation_quality={} \
             end_to_end_ns={} \
             cold_end_to_end_ns={} fixed_ns={} warm_fixed_ns={} marginal_ns={} \
             marginal_ns_per_round={} calibration_ns={} backend_wall_ns={} device_ns={} \
             host_encode_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
            workload.workload(),
            workload.rq(),
            measurement.sample,
            measurement.order,
            measurement.predecessor,
            measurement.engine,
            measurement.backend,
            measurement.stream_mode,
            measurement.metal_mode_order,
            measurement.workers,
            threadgroup_width,
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
            measurement.host_encode_submit_ns,
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
        threadgroup_width: usize,
        selected: &[Measurement],
    ) {
        assert!(!selected.is_empty(), "summary selection must be nonempty");
        let first = selected[0];
        assert!(
            selected.iter().all(|measurement| {
                measurement.engine == first.engine
                    && measurement.stream_mode == first.stream_mode
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
            "record=t15e_{kind} statistic=median \
             aggregation=component_medians_with_derived_fixed_closure config={fixture} workload={} rq={} \
             order={order} predecessor={} engine={} \
             backend={} stream_mode={} workers={} samples={} threadgroup_width={} rounds={} transitions={} \
             fixed_method={} separation_quality={} end_to_end_ns={} cold_end_to_end_ns={} \
             fixed_ns={} warm_fixed_ns={} marginal_ns={} marginal_ns_per_round={} \
             calibration_ns={} backend_wall_ns={} \
             device_ns={} host_encode_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
            workload.workload(),
            workload.rq(),
            first.predecessor,
            first.engine,
            first.backend,
            first.stream_mode,
            first.workers,
            selected.len(),
            threadgroup_width,
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
                    .map(|measurement| u128::from(measurement.host_encode_submit_ns))
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

    let mut relative =
        "configs/benchmarks/width_via_load_full/fattree_k32_load_30_sustained.toml".to_owned();
    let mut relative_was_set = false;
    let mut samples = 4_usize;
    let mut threadgroup_width = 256_usize;
    let mut skip_scalar = false;
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
            "--threadgroup-width" => {
                threadgroup_width = arguments
                    .next()
                    .expect("--threadgroup-width requires a value")
                    .parse()
                    .expect("--threadgroup-width must be an integer");
            }
            "--skip-scalar" => skip_scalar = true,
            unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
            path if !relative_was_set => {
                relative = path.to_owned();
                relative_was_set = true;
            }
            extra => panic!("unexpected second fixture path {extra}"),
        }
    }
    assert_eq!(
        samples, 4,
        "the T15e protocol requires exactly four samples, two per order"
    );
    assert!(threadgroup_width > 0, "--threadgroup-width must be nonzero");

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let workload = benchmark_workload(&image);
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
    let executor = MetalExecutor::new().expect("Metal benchmark executor must initialize");
    let initialization = executor.initialization_timings();
    let device_queue_setup_ns = u128::from(initialization.device_queue_setup_ns);
    let pipeline_creation_ns = u128::from(initialization.pipeline_creation_ns);
    println!(
        "record=t15e_protocol workload={} rq={} scalar_samples={} scalar_predecessor=none \
         comparison_samples={samples} comparison_predecessor=same_kind_discarded \
         order_schedule=balanced_cpu_first_gpu_first_and_heap_streams \
         comparison_outcome_values=beats,parity,trails \
         comparison_dispersion=maximum_retained_sample_range \
         comparison_parity_rule=equal_medians_or_twice_abs_median_difference_lt_dispersion \
         paired_comparison_rule=same_sample_index \
         scalar_provenance={} scalar_reuse_days_commit={} \
         scalar_reuse_evidence={}",
        workload.workload(),
        workload.rq(),
        if skip_scalar { 0 } else { SCALAR_SAMPLES },
        if skip_scalar {
            "reused_audited_t15e"
        } else {
            "measured"
        },
        if skip_scalar {
            "0fb8f9b881cc5fd542d89f4bd4aa705e4ab7cf1a"
        } else {
            "na"
        },
        if skip_scalar {
            "evidence/P08/t15e-sustained-load30-raw.txt,evidence/P08/t15e-sustained-load90-raw.txt"
        } else {
            "na"
        },
    );
    println!(
        "record=t15e_initialization config={relative} workload={} rq={} engine=metal device_queue_setup_ns={} \
         pipeline_creation_ns={} in_process_reuse=1 archive_saving_status=unmeasured \
         archive_upper_bound_ns={}",
        workload.workload(),
        workload.rq(),
        initialization.device_queue_setup_ns,
        initialization.pipeline_creation_ns,
        initialization.pipeline_creation_ns,
    );

    let warmups = [
        cpu(&image, 4, &oracle),
        cpu(&image, 18, &oracle),
        metal(
            &executor,
            &image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
            false,
            &oracle,
        ),
        metal(
            &executor,
            &image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
            true,
            &oracle,
        ),
    ];
    let expected = warmups[0].outcome;
    for warmup in warmups {
        assert_eq!(warmup.outcome, expected, "warmup backends must agree");
        print_record("warmup", &relative, workload, threadgroup_width, warmup);
    }

    let mut measurements =
        Vec::with_capacity(samples.saturating_mul(4).saturating_add(SCALAR_SAMPLES));
    for sample in 0..samples {
        let order = order_for_sample(sample);
        let include_scalar = !skip_scalar && sample < SCALAR_SAMPLES;
        let heap_first = sample % 4 == 0 || sample % 4 == 3;
        let metal_mode_order = if heap_first {
            "heap_then_streams"
        } else {
            "streams_then_heap"
        };
        let mut metal_pair = if heap_first {
            vec![
                metal_after_predecessor(
                    &executor,
                    &image,
                    threadgroup_width,
                    device_queue_setup_ns,
                    pipeline_creation_ns,
                    false,
                    &oracle,
                ),
                metal_after_predecessor(
                    &executor,
                    &image,
                    threadgroup_width,
                    device_queue_setup_ns,
                    pipeline_creation_ns,
                    true,
                    &oracle,
                ),
            ]
        } else {
            vec![
                metal_after_predecessor(
                    &executor,
                    &image,
                    threadgroup_width,
                    device_queue_setup_ns,
                    pipeline_creation_ns,
                    true,
                    &oracle,
                ),
                metal_after_predecessor(
                    &executor,
                    &image,
                    threadgroup_width,
                    device_queue_setup_ns,
                    pipeline_creation_ns,
                    false,
                    &oracle,
                ),
            ]
        };
        let mut ordered = Vec::with_capacity(4 + usize::from(include_scalar));
        if order == "cpu_first" {
            if include_scalar {
                ordered.push(scalar(&image, scalar_calibration(&image), &oracle));
            }
            ordered.extend([
                cpu_after_predecessor(&image, 4, &oracle),
                cpu_after_predecessor(&image, 18, &oracle),
            ]);
            ordered.append(&mut metal_pair);
        } else {
            ordered.append(&mut metal_pair);
            if include_scalar {
                ordered.push(scalar(&image, scalar_calibration(&image), &oracle));
            }
            ordered.extend([
                cpu_after_predecessor(&image, 4, &oracle),
                cpu_after_predecessor(&image, 18, &oracle),
            ]);
        }
        for mut measurement in ordered {
            measurement.sample = sample;
            measurement.order = order;
            if measurement.backend == "metal" {
                measurement.metal_mode_order = metal_mode_order;
            }
            assert_eq!(
                measurement.outcome, expected,
                "timed sample backends must agree"
            );
            print_record(
                "sample",
                &relative,
                workload,
                threadgroup_width,
                measurement,
            );
            measurements.push(measurement);
        }
    }

    for engine in ["scalar", "w4", "w18", "metal_heap", "metal_streams"] {
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
            &relative,
            workload,
            "pooled",
            threadgroup_width,
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
                    &relative,
                    workload,
                    order,
                    threadgroup_width,
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
    let w18_samples = retained_marginals("w18");
    let metal_heap_samples = retained_marginals("metal_heap");
    let metal_samples = retained_marginals("metal_streams");
    let metal_vs_w4 = compare_retained_samples(&metal_samples, &w4_samples);
    let metal_vs_w18 = compare_retained_samples(&metal_samples, &w18_samples);
    assert_eq!(
        metal_vs_w4.paired_comparisons, samples,
        "W4 comparison must retain the audited sample count"
    );
    assert_eq!(
        metal_vs_w18.paired_comparisons, samples,
        "W18 comparison must retain the audited sample count"
    );
    let w4_marginal_ns = metal_vs_w4.reference_median_ns;
    let w18_marginal_ns = metal_vs_w18.reference_median_ns;
    let metal_heap_marginal_ns = median(metal_heap_samples.iter().copied());
    let metal_marginal_ns = metal_vs_w4.candidate_median_ns;
    assert!(
        w4_marginal_ns > 0
            && w18_marginal_ns > 0
            && metal_heap_marginal_ns > 0
            && metal_marginal_ns > 0,
        "marginal-ratio inputs must be nonzero"
    );
    let metal_over_w4 = metal_marginal_ns as f64 / w4_marginal_ns as f64;
    let metal_over_w18 = metal_marginal_ns as f64 / w18_marginal_ns as f64;
    let metal_over_heap = metal_marginal_ns as f64 / metal_heap_marginal_ns as f64;
    let w4_over_metal = w4_marginal_ns as f64 / metal_marginal_ns as f64;
    let w18_over_metal = w18_marginal_ns as f64 / metal_marginal_ns as f64;
    let crossover = crossover_summary(metal_vs_w4.outcome, metal_vs_w18.outcome);
    println!(
        "record=t15e_pooled_summary statistic=median config={relative} workload={} rq={} \
         scalar_samples={scalar_samples} comparison_samples={samples} \
         threadgroup_width={threadgroup_width} rounds={} transitions={} \
         ratio_definition=metal_marginal_div_cpu_marginal \
         ablation_ratio_definition=metal_streams_marginal_div_metal_heap_marginal \
         speedup_definition=cpu_marginal_div_metal_marginal \
         w4_marginal_ns={w4_marginal_ns} w18_marginal_ns={w18_marginal_ns} \
         metal_heap_marginal_ns={metal_heap_marginal_ns} \
         metal_marginal_ns={metal_marginal_ns} metal_over_w4={metal_over_w4:.6} \
         metal_over_w18={metal_over_w18:.6} streams_over_heap={metal_over_heap:.6} \
         w4_over_metal={w4_over_metal:.6} \
         w18_over_metal={w18_over_metal:.6} \
         metal_retained_range_ns={} w4_retained_range_ns={} w18_retained_range_ns={} \
         metal_vs_w4_parity_bound_range_ns={} metal_vs_w18_parity_bound_range_ns={} \
         metal_vs_w4_outcome={} metal_vs_w18_outcome={} \
         metal_wins_vs_w4={} metal_vs_w4_paired_samples={} \
         metal_wins_vs_w18={} metal_vs_w18_paired_samples={} \
         crossover={crossover} crossover_basis=dispersion_qualified_marginal",
        workload.workload(),
        workload.rq(),
        expected.rounds,
        expected.transitions,
        metal_vs_w4.candidate_range_ns,
        metal_vs_w4.reference_range_ns,
        metal_vs_w18.reference_range_ns,
        metal_vs_w4.parity_bound_range_ns,
        metal_vs_w18.parity_bound_range_ns,
        metal_vs_w4.outcome.as_str(),
        metal_vs_w18.outcome.as_str(),
        metal_vs_w4.candidate_wins,
        metal_vs_w4.paired_comparisons,
        metal_vs_w18.candidate_wins,
        metal_vs_w18.paired_comparisons,
    );
}

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
#[path = "t15e_sustained_benchmark/cuda_app.rs"]
mod cuda_app;

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
fn main() {
    cuda_app::main();
}

#[cfg(not(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
)))]
fn main() {
    eprintln!(
        "t15e_sustained_benchmark requires --features metal-spike on Apple or --features cuda"
    );
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::{
        BenchmarkWorkload, ComparisonOutcome, SCALAR_SAMPLES, classify_workload,
        compare_retained_samples, cpu_engine_name, crossover_summary, cuda_crossover_summary,
        median, order_for_sample, parse_worker_sweep, recorded_predecessor_for_engine, split_fixed,
    };

    #[test]
    fn tcp_workloads_use_the_rq9_record_schema() {
        let tcp = classify_workload(true, false);
        assert_eq!(tcp, BenchmarkWorkload::Tcp);
        assert_eq!(tcp.workload(), "tcp");
        assert_eq!(tcp.rq(), "RQ9");

        let mixed = classify_workload(true, true);
        assert_eq!(mixed.workload(), "mixed_tcp");
        assert_eq!(mixed.rq(), "RQ9");

        let open_loop = classify_workload(false, true);
        assert_eq!(open_loop.workload(), "open_loop");
        assert_eq!(open_loop.rq(), "legacy");
    }

    #[test]
    fn comparison_outcomes_use_the_record_schema_values() {
        assert_eq!(ComparisonOutcome::Beats.as_str(), "beats");
        assert_eq!(ComparisonOutcome::Parity.as_str(), "parity");
        assert_eq!(ComparisonOutcome::Trails.as_str(), "trails");
        assert_eq!(
            crossover_summary(ComparisonOutcome::Beats, ComparisonOutcome::Parity),
            "metal_beats_w4_parity_w18"
        );
    }

    #[test]
    fn cuda_crossover_summary_names_w4_and_selected_best_worker_baselines() {
        assert_eq!(
            cuda_crossover_summary(ComparisonOutcome::Beats, ComparisonOutcome::Parity),
            "cuda_beats_w4_parity_wbest"
        );
        assert_eq!(
            cpu_engine_name(4, 19),
            "w4",
            "the fixed W4 baseline keeps its stable engine name"
        );
        assert_eq!(
            cpu_engine_name(19, 19),
            "wbest",
            "the selected Grace worker count uses the W-best engine name"
        );
    }

    #[test]
    fn retained_sample_comparison_reports_beats_and_paired_wins() {
        let comparison = compare_retained_samples(&[90, 91, 92, 93], &[100, 101, 102, 103]);

        assert_eq!(comparison.outcome, ComparisonOutcome::Beats);
        assert_eq!(comparison.candidate_wins, 4);
        assert_eq!(comparison.paired_comparisons, 4);
    }

    #[test]
    fn retained_sample_comparison_reports_parity_inside_half_max_range() {
        let comparison = compare_retained_samples(&[100, 101, 102, 110], &[103, 104, 105, 106]);

        assert_eq!(comparison.candidate_median_ns, 101);
        assert_eq!(comparison.reference_median_ns, 104);
        assert_eq!(comparison.candidate_range_ns, 10);
        assert_eq!(comparison.reference_range_ns, 3);
        assert_eq!(comparison.parity_bound_range_ns, 10);
        assert_eq!(comparison.outcome, ComparisonOutcome::Parity);
        assert_eq!(comparison.candidate_wins, 3);
    }

    #[test]
    fn retained_sample_comparison_reports_parity_for_equal_zero_range_samples() {
        let comparison = compare_retained_samples(&[100; 4], &[100; 4]);

        assert_eq!(comparison.parity_bound_range_ns, 0);
        assert_eq!(comparison.outcome, ComparisonOutcome::Parity);
        assert_eq!(comparison.candidate_wins, 0);
        assert_eq!(comparison.paired_comparisons, 4);
    }

    #[test]
    fn retained_sample_comparison_reports_trails_at_half_max_range_boundary() {
        let comparison = compare_retained_samples(&[105, 106, 107, 115], &[100, 101, 102, 103]);

        assert_eq!(comparison.candidate_median_ns, 106);
        assert_eq!(comparison.reference_median_ns, 101);
        assert_eq!(comparison.parity_bound_range_ns, 10);
        assert_eq!(comparison.outcome, ComparisonOutcome::Trails);
        assert_eq!(comparison.candidate_wins, 0);
    }

    #[test]
    fn even_median_averages_middle_pair() {
        assert_eq!(median([8, 2, 6, 4].into_iter()), 5);
    }

    #[test]
    fn fixed_split_closes_to_wall_time() {
        let (fixed_ns, marginal_ns) = split_fixed(100, 73);
        assert_eq!(fixed_ns, 27);
        assert_eq!(fixed_ns.checked_add(marginal_ns), Some(100));
    }

    #[test]
    fn four_sample_protocol_balances_orders() {
        let orders = (0..4).map(order_for_sample).collect::<Vec<_>>();
        assert_eq!(
            orders.iter().filter(|&&order| order == "cpu_first").count(),
            2
        );
        assert_eq!(
            orders.iter().filter(|&&order| order == "gpu_first").count(),
            2
        );
        assert_eq!(orders, ["cpu_first", "gpu_first", "cpu_first", "gpu_first"]);
    }

    #[test]
    fn scalar_measurement_exemption_uses_two_samples_without_predecessors() {
        assert_eq!(SCALAR_SAMPLES, 2);
        assert_eq!(recorded_predecessor_for_engine("scalar"), "none");
        for engine in ["w4", "w18", "wbest", "cuda", "metal_heap", "metal_streams"] {
            assert_eq!(
                recorded_predecessor_for_engine(engine),
                "same_kind_discarded"
            );
        }
    }

    #[test]
    fn worker_sweep_parses_the_audited_grace_counts() {
        assert_eq!(
            parse_worker_sweep("4,8,12,16,19").unwrap(),
            [4, 8, 12, 16, 19]
        );
        assert_eq!(
            parse_worker_sweep("4,4").unwrap_err(),
            "worker sweep counts must be unique"
        );
        assert_eq!(
            parse_worker_sweep("4,0").unwrap_err(),
            "worker sweep counts must be nonzero"
        );
    }
}

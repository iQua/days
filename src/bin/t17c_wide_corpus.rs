#[cfg(any(test, feature = "cuda"))]
use std::fmt;

#[cfg(any(test, feature = "cuda"))]
const SAMPLES: usize = 4;

#[cfg(any(test, feature = "cuda"))]
fn median(mut values: Vec<u128>) -> u128 {
    values.sort_unstable();
    match values.len() {
        0 => 0,
        length if length % 2 == 1 => values[length / 2],
        length => (values[length / 2 - 1] + values[length / 2]) / 2,
    }
}

#[cfg(any(test, feature = "cuda"))]
fn retained_range(values: &[u128]) -> u128 {
    values.iter().max().unwrap() - values.iter().min().unwrap()
}

#[cfg(any(test, feature = "cuda"))]
fn order_for_sample(sample: usize) -> &'static str {
    if sample.is_multiple_of(2) {
        "cpu_first"
    } else {
        "gpu_first"
    }
}

#[cfg(any(test, feature = "cuda"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormalOutcome {
    Beats,
    Parity,
    Trails,
}

#[cfg(any(test, feature = "cuda"))]
impl fmt::Display for FormalOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Beats => "beats",
            Self::Parity => "parity",
            Self::Trails => "trails",
        })
    }
}

#[cfg(any(test, feature = "cuda"))]
fn formal_outcome(candidate: &[u128], reference: &[u128]) -> FormalOutcome {
    let candidate_median = median(candidate.to_vec());
    let reference_median = median(reference.to_vec());
    let dispersion = retained_range(candidate).max(retained_range(reference));
    let difference = candidate_median.abs_diff(reference_median);
    if candidate_median == reference_median || difference.saturating_mul(2) < dispersion {
        FormalOutcome::Parity
    } else if candidate_median < reference_median {
        FormalOutcome::Beats
    } else {
        FormalOutcome::Trails
    }
}

fn run_sizing_dry_run_if_requested() -> bool {
    if !std::env::args().any(|argument| argument == "--sizing-dry-run") {
        return false;
    }

    let mut fixture = None;
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--sizing-dry-run" => {}
            "--best-workers" => {
                arguments.next().expect("--best-workers requires a value");
            }
            unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
            path if fixture.is_none() => fixture = Some(path.to_owned()),
            extra => panic!("unexpected second fixture path {extra}"),
        }
    }
    let fixture = fixture.unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_k48_h16/\
         fattree_k48_h16_load_30_sustained.toml"
            .to_owned()
    });
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
    let image = days::scenario::compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let report = days_executor::size_default_device_plan(&image)
        .unwrap_or_else(|error| panic!("failed to size {}: {error}", path.display()));

    println!(
        "record=t17c_wide_sizing_protocol mode=host_arithmetic_only allocates_device=false \
         executes_simulation=false plane_count=27 fixture={fixture}"
    );
    for plane in &report.planes {
        println!(
            "record=t17c_wide_sizing_plane index={} name={} words={} bytes={} fixture={fixture}",
            plane.index, plane.name, plane.words, plane.bytes,
        );
    }
    let arenas = report.event_arenas;
    println!(
        "record=t17c_wide_sizing_arena legacy_heap_event_slots={} \
         fallback_heap_event_slots={} channel_stream_event_slots={} \
         service_stream_event_slots={} generator_stream_event_slots={} heap_arena_bytes={} \
         stream_arena_bytes={} total_event_arena_bytes={} legacy_heap_arena_bytes={} \
         fixture={fixture}",
        arenas.legacy_heap_event_slots,
        arenas.fallback_heap_event_slots,
        arenas.channel_stream_event_slots,
        arenas.service_stream_event_slots,
        arenas.generator_stream_event_slots,
        arenas.heap_arena_bytes,
        arenas.stream_arena_bytes,
        arenas.total_event_arena_bytes(),
        arenas.legacy_heap_arena_bytes,
    );
    println!(
        "record=t17c_wide_sizing_total plane_count=27 total_device_bytes={} \
         total_device_mib={:.6} total_device_gib={:.9} fixture={fixture}",
        report.total_device_bytes,
        report.total_device_bytes as f64 / 1_048_576.0,
        report.total_device_bytes as f64 / 1_073_741_824.0,
    );
    true
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
mod app {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{CpuConfig, NodeKind, RunResult, RunSummary, SimulationImage, run_cpu};

    #[cfg(feature = "cuda")]
    use super::{SAMPLES, formal_outcome, median, order_for_sample, retained_range};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct Outcome {
        rounds: u64,
        transitions: u64,
        summary: RunSummary,
        resident_packets: usize,
        pending_events: usize,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct WidthProfile {
        topology_lps: usize,
        host_lps: usize,
        switch_port_lps: usize,
        rounds: u64,
        transitions: u64,
        active_lp_rounds: u128,
        minimum_active_lps: usize,
        maximum_active_lps: usize,
        host_active_lp_rounds: u128,
        switch_active_lp_rounds: u128,
        maximum_active_host_lps: usize,
        maximum_active_switch_lps: usize,
        maximum_events_per_lp: u64,
        remote_messages: u128,
    }

    #[derive(Clone, Copy, Debug)]
    struct Measurement {
        sample: usize,
        order: &'static str,
        predecessor: &'static str,
        engine: &'static str,
        backend: &'static str,
        workers: usize,
        outcome: Outcome,
        end_to_end_ns: u128,
        fixed_ns: u128,
        marginal_ns: u128,
        backend_wall_ns: u64,
        device_ns: u64,
        host_submit_ns: u64,
        graph_capture_ns: u64,
        encoded_attempts: u64,
        continuation_relaunches: u64,
        graph_replays: u64,
        wave_boundary_syncs: u64,
        mid_round_wave_boundary_syncs: u64,
    }

    impl Measurement {
        fn marginal_ns_per_round(self) -> u128 {
            self.marginal_ns / u128::from(self.outcome.rounds)
        }
    }

    fn outcome(result: &RunResult, rounds: u64, transitions: u64) -> Outcome {
        Outcome {
            rounds,
            transitions,
            summary: result.summary,
            resident_packets: result.resident_packets.len(),
            pending_events: result.pending_events.len(),
        }
    }

    fn width_profile(image: &SimulationImage, run: &days_executor::CpuRun) -> WidthProfile {
        let host_lps = image
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Host)
            .count();
        let mut profile = WidthProfile {
            topology_lps: image.nodes.len(),
            host_lps,
            switch_port_lps: image.nodes.len() - host_lps,
            rounds: run.rounds.len() as u64,
            transitions: 0,
            active_lp_rounds: 0,
            minimum_active_lps: usize::MAX,
            maximum_active_lps: 0,
            host_active_lp_rounds: 0,
            switch_active_lp_rounds: 0,
            maximum_active_host_lps: 0,
            maximum_active_switch_lps: 0,
            maximum_events_per_lp: 0,
            remote_messages: 0,
        };
        for round in &run.rounds {
            let active_lps = round.semantic.active_lp_count;
            assert_eq!(round.semantic.lp_work.len(), active_lps);
            let active_host_lps = round
                .semantic
                .lp_work
                .iter()
                .filter(|work| image.nodes[work.node.0 as usize].kind == NodeKind::Host)
                .count();
            let active_switch_lps = active_lps - active_host_lps;
            profile.transitions = profile
                .transitions
                .saturating_add(round.semantic.events_processed);
            profile.active_lp_rounds = profile.active_lp_rounds.saturating_add(active_lps as u128);
            profile.minimum_active_lps = profile.minimum_active_lps.min(active_lps);
            profile.maximum_active_lps = profile.maximum_active_lps.max(active_lps);
            profile.host_active_lp_rounds = profile
                .host_active_lp_rounds
                .saturating_add(active_host_lps as u128);
            profile.switch_active_lp_rounds = profile
                .switch_active_lp_rounds
                .saturating_add(active_switch_lps as u128);
            profile.maximum_active_host_lps = profile.maximum_active_host_lps.max(active_host_lps);
            profile.maximum_active_switch_lps =
                profile.maximum_active_switch_lps.max(active_switch_lps);
            profile.maximum_events_per_lp = profile.maximum_events_per_lp.max(
                round
                    .semantic
                    .lp_work
                    .iter()
                    .map(|work| work.events_processed)
                    .max()
                    .unwrap_or(0),
            );
            profile.remote_messages = profile
                .remote_messages
                .saturating_add(u128::from(round.semantic.messages_exchanged));
        }
        assert!(profile.rounds > 0);
        profile
    }

    fn cpu(
        image: &SimulationImage,
        workers: usize,
        expected: Option<&RunResult>,
        expected_width: Option<WidthProfile>,
    ) -> (Measurement, RunResult, WidthProfile) {
        let started = Instant::now();
        let run = run_cpu(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
        )
        .unwrap_or_else(|error| panic!("W{workers} wide run failed: {error}"));
        let end_to_end_ns = started.elapsed().as_nanos();
        if let Some(expected) = expected {
            assert_eq!(&run.result, expected, "W{workers} complete result differs");
        }
        let width = width_profile(image, &run);
        if let Some(expected_width) = expected_width {
            assert_eq!(width, expected_width, "W{workers} width profile differs");
        }
        let marginal_ns = run.rounds.iter().fold(0_u128, |total, round| {
            total.saturating_add(u128::from(round.round_wall_time_ns))
        });
        let result = run.result;
        let outcome = outcome(&result, width.rounds, width.transitions);
        (
            Measurement {
                sample: 0,
                order: "warmup",
                predecessor: "none",
                engine: "cpu_best",
                backend: "cpu",
                workers,
                outcome,
                end_to_end_ns,
                fixed_ns: end_to_end_ns
                    .checked_sub(marginal_ns)
                    .expect("CPU marginal must fit in end-to-end time"),
                marginal_ns,
                backend_wall_ns: 0,
                device_ns: 0,
                host_submit_ns: 0,
                graph_capture_ns: 0,
                encoded_attempts: 0,
                continuation_relaunches: 0,
                graph_replays: 0,
                wave_boundary_syncs: 0,
                mid_round_wave_boundary_syncs: 0,
            },
            result,
            width,
        )
    }

    #[cfg(feature = "cuda")]
    fn print_protocol(fixture: &str, workers: usize) {
        println!(
            "record=t17c_wide_protocol fixture={fixture} backend=cuda samples={SAMPLES} \
             order_schedule=cpu_first,gpu_first,cpu_first,gpu_first \
             comparison_predecessor=same_kind_discarded correctness=complete_RunResult_equality \
             cpu_workers={workers} cpu_marginal=round_wall_sum \
             gpu_marginal=backend_wall parity_rule=equal_medians_or_\
             twice_abs_median_difference_lt_max_range"
        );
    }

    fn print_measurement(kind: &str, fixture: &str, measurement: Measurement) {
        println!(
            "record=t17c_wide_{kind} fixture={fixture} sample={} order={} predecessor={} \
             engine={} backend={} workers={} rounds={} transitions={} end_to_end_ns={} \
             fixed_ns={} marginal_ns={} marginal_ns_per_round={} backend_wall_ns={} \
             device_ns={} host_submit_ns={} graph_capture_ns={} encoded_attempts={} \
             continuation_relaunches={} graph_replays={} wave_boundary_syncs={} \
             mid_round_wave_boundary_syncs={}",
            measurement.sample,
            measurement.order,
            measurement.predecessor,
            measurement.engine,
            measurement.backend,
            measurement.workers,
            measurement.outcome.rounds,
            measurement.outcome.transitions,
            measurement.end_to_end_ns,
            measurement.fixed_ns,
            measurement.marginal_ns,
            measurement.marginal_ns_per_round(),
            measurement.backend_wall_ns,
            measurement.device_ns,
            measurement.host_submit_ns,
            measurement.graph_capture_ns,
            measurement.encoded_attempts,
            measurement.continuation_relaunches,
            measurement.graph_replays,
            measurement.wave_boundary_syncs,
            measurement.mid_round_wave_boundary_syncs,
        );
    }

    fn print_width(fixture: &str, profile: WidthProfile, summary: RunSummary) {
        println!(
            "record=t17c_wide_occupancy fixture={fixture} topology_lps={} host_lps={} \
             switch_port_lps={} rounds={} transitions={} active_lp_rounds={} \
             mean_active_lps={:.6} minimum_active_lps={} maximum_active_lps={} \
             mean_active_percent={:.6} maximum_active_percent={:.6} \
             host_active_lp_rounds={} mean_active_host_lps={:.6} \
             maximum_active_host_lps={} switch_active_lp_rounds={} \
             mean_active_switch_lps={:.6} maximum_active_switch_lps={} \
             maximum_events_per_lp={} remote_messages={} sourced_packets={} sourced_bytes={}",
            profile.topology_lps,
            profile.host_lps,
            profile.switch_port_lps,
            profile.rounds,
            profile.transitions,
            profile.active_lp_rounds,
            profile.active_lp_rounds as f64 / profile.rounds as f64,
            profile.minimum_active_lps,
            profile.maximum_active_lps,
            profile.active_lp_rounds as f64 / profile.rounds as f64 / profile.topology_lps as f64
                * 100.0,
            profile.maximum_active_lps as f64 / profile.topology_lps as f64 * 100.0,
            profile.host_active_lp_rounds,
            profile.host_active_lp_rounds as f64 / profile.rounds as f64,
            profile.maximum_active_host_lps,
            profile.switch_active_lp_rounds,
            profile.switch_active_lp_rounds as f64 / profile.rounds as f64,
            profile.maximum_active_switch_lps,
            profile.maximum_events_per_lp,
            profile.remote_messages,
            summary.sourced_packets,
            summary.sourced_bytes,
        );
    }

    #[cfg(feature = "cuda")]
    fn print_summary(fixture: &str, engine: &str, measurements: &[Measurement]) {
        let selected = measurements
            .iter()
            .filter(|measurement| measurement.engine == engine)
            .copied()
            .collect::<Vec<_>>();
        let first = selected[0];
        println!(
            "record=t17c_wide_summary fixture={fixture} engine={engine} backend={} workers={} \
             samples={} rounds={} transitions={} marginal_ns={} marginal_ns_per_round={} \
             retained_range_ns={}",
            first.backend,
            first.workers,
            selected.len(),
            first.outcome.rounds,
            first.outcome.transitions,
            median(
                selected
                    .iter()
                    .map(|measurement| measurement.marginal_ns)
                    .collect()
            ),
            median(
                selected
                    .iter()
                    .map(|measurement| measurement.marginal_ns_per_round())
                    .collect()
            ),
            retained_range(
                &selected
                    .iter()
                    .map(|measurement| measurement.marginal_ns)
                    .collect::<Vec<_>>()
            ),
        );
        for order in ["cpu_first", "gpu_first"] {
            let ordered = selected
                .iter()
                .filter(|measurement| measurement.order == order)
                .map(|measurement| measurement.marginal_ns)
                .collect::<Vec<_>>();
            println!(
                "record=t17c_wide_order_summary fixture={fixture} engine={engine} order={order} \
                 samples={} marginal_ns={} marginal_ns_per_round={}",
                ordered.len(),
                median(ordered.clone()),
                median(ordered) / u128::from(first.outcome.rounds),
            );
        }
    }

    #[cfg(feature = "cuda")]
    pub fn cuda_main() {
        use days_executor::CudaExecutor;

        let (fixture, workers) = parse_arguments(19);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        let executor = CudaExecutor::new().expect("CUDA wide executor must initialize");
        let initialization = executor.initialization_timings();

        print_protocol(&fixture, workers);
        println!(
            "record=t17c_wide_initialization fixture={fixture} backend=cuda \
             context_stream_setup_ns={} module_function_load_ns={}",
            initialization.context_stream_setup_ns, initialization.module_function_load_ns,
        );

        let (cpu_warm, expected, width) = cpu(&image, workers, None, None);
        let (cuda_warm, memory_layout) = cuda(&executor, &image, &expected);
        assert_eq!(cuda_warm.outcome, cpu_warm.outcome);
        print_measurement("warmup", &fixture, cpu_warm);
        print_measurement("warmup", &fixture, cuda_warm);
        print_width(&fixture, width, expected.summary);
        println!(
            "record=t17c_wide_cuda_memory fixture={fixture} streams_enabled={} \
             legacy_heap_event_slots={} fallback_heap_event_slots={} \
             channel_stream_event_slots={} service_stream_event_slots={} \
             generator_stream_event_slots={} heap_arena_bytes={} stream_arena_bytes={} \
             total_event_arena_bytes={} legacy_heap_arena_bytes={} delta_from_legacy_heap_bytes={}",
            memory_layout.streams_enabled,
            memory_layout.legacy_heap_event_slots,
            memory_layout.fallback_heap_event_slots,
            memory_layout.channel_stream_event_slots,
            memory_layout.service_stream_event_slots,
            memory_layout.generator_stream_event_slots,
            memory_layout.heap_arena_bytes,
            memory_layout.stream_arena_bytes,
            memory_layout.total_event_arena_bytes(),
            memory_layout.legacy_heap_arena_bytes,
            memory_layout.delta_from_legacy_heap_bytes(),
        );

        let mut measurements = Vec::with_capacity(SAMPLES * 2);
        for sample in 0..SAMPLES {
            let order = order_for_sample(sample);
            if order == "cpu_first" {
                let mut cpu = cpu_after_predecessor(&image, workers, &expected, width);
                cpu.sample = sample;
                cpu.order = order;
                print_measurement("sample", &fixture, cpu);
                measurements.push(cpu);
                let mut cuda = cuda_after_predecessor(&executor, &image, &expected);
                cuda.sample = sample;
                cuda.order = order;
                print_measurement("sample", &fixture, cuda);
                measurements.push(cuda);
            } else {
                let mut cuda = cuda_after_predecessor(&executor, &image, &expected);
                cuda.sample = sample;
                cuda.order = order;
                print_measurement("sample", &fixture, cuda);
                measurements.push(cuda);
                let mut cpu = cpu_after_predecessor(&image, workers, &expected, width);
                cpu.sample = sample;
                cpu.order = order;
                print_measurement("sample", &fixture, cpu);
                measurements.push(cpu);
            }
        }

        for engine in ["cpu_best", "cuda"] {
            print_summary(&fixture, engine, &measurements);
        }
        let cpu_samples = measurements
            .iter()
            .filter(|measurement| measurement.engine == "cpu_best")
            .map(|measurement| measurement.marginal_ns)
            .collect::<Vec<_>>();
        let cuda_samples = measurements
            .iter()
            .filter(|measurement| measurement.engine == "cuda")
            .map(|measurement| measurement.marginal_ns)
            .collect::<Vec<_>>();
        let cpu_median = median(cpu_samples.clone());
        let cuda_median = median(cuda_samples.clone());
        let dispersion = retained_range(&cpu_samples).max(retained_range(&cuda_samples));
        let twice_difference = cpu_median.abs_diff(cuda_median).saturating_mul(2);
        let outcome = formal_outcome(&cuda_samples, &cpu_samples);
        let paired_wins = cuda_samples
            .iter()
            .zip(&cpu_samples)
            .filter(|(cuda, cpu)| cuda < cpu)
            .count();
        println!(
            "record=t17c_wide_formal fixture={fixture} cpu_workers={workers} \
             cpu_marginal_ns={cpu_median} cuda_marginal_ns={cuda_median} \
             cpu_marginal_ns_per_round={} cuda_marginal_ns_per_round={} \
             cpu_retained_range_ns={} cuda_retained_range_ns={} \
             parity_bound_range_ns={dispersion} twice_median_difference_ns={twice_difference} \
             cuda_over_cpu={:.9} cpu_over_cuda={:.9} paired_wins={paired_wins} \
             paired_samples={SAMPLES} formal_outcome={outcome} \
             exact_complete_result_parity=pass",
            cpu_median / u128::from(cpu_warm.outcome.rounds),
            cuda_median / u128::from(cpu_warm.outcome.rounds),
            retained_range(&cpu_samples),
            retained_range(&cuda_samples),
            cuda_median as f64 / cpu_median as f64,
            cpu_median as f64 / cuda_median as f64,
        );
    }

    #[cfg(feature = "cuda")]
    fn cuda(
        executor: &days_executor::CudaExecutor,
        image: &SimulationImage,
        expected: &RunResult,
    ) -> (Measurement, days_executor::CudaMemoryLayout) {
        let started = Instant::now();
        let run = executor
            .run(image, None, days_executor::CudaConfig::default())
            .expect("CUDA wide run failed");
        let end_to_end_ns = started.elapsed().as_nanos();
        assert_eq!(&run.result, expected, "CUDA complete result differs");
        let outcome = outcome(&run.result, run.rounds, run.transitions);
        let marginal_ns = u128::from(run.wall_ns);
        let measurement = Measurement {
            sample: 0,
            order: "warmup",
            predecessor: "none",
            engine: "cuda",
            backend: "cuda",
            workers: 0,
            outcome,
            end_to_end_ns,
            fixed_ns: end_to_end_ns
                .checked_sub(marginal_ns)
                .expect("CUDA marginal must fit in end-to-end time"),
            marginal_ns,
            backend_wall_ns: run.wall_ns,
            device_ns: run.device_ns,
            host_submit_ns: run.host_submit_ns,
            graph_capture_ns: run.graph_capture_ns,
            encoded_attempts: run.encoded_attempts,
            continuation_relaunches: run.continuation_relaunches,
            graph_replays: run.graph_replays,
            wave_boundary_syncs: run.wave_boundary_syncs,
            mid_round_wave_boundary_syncs: run.mid_round_wave_boundary_syncs,
        };
        (measurement, run.memory_layout)
    }

    #[cfg(feature = "cuda")]
    fn cpu_after_predecessor(
        image: &SimulationImage,
        workers: usize,
        expected: &RunResult,
        width: WidthProfile,
    ) -> Measurement {
        let _ = cpu(image, workers, Some(expected), Some(width));
        let (mut measurement, _, _) = cpu(image, workers, Some(expected), Some(width));
        measurement.predecessor = "same_kind_discarded";
        measurement
    }

    #[cfg(feature = "cuda")]
    fn cuda_after_predecessor(
        executor: &days_executor::CudaExecutor,
        image: &SimulationImage,
        expected: &RunResult,
    ) -> Measurement {
        let _ = cuda(executor, image, expected);
        let (mut measurement, _) = cuda(executor, image, expected);
        measurement.predecessor = "same_kind_discarded";
        measurement
    }

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    pub fn metal_main() {
        use days_executor::{MetalConfig, MetalExecutor};

        let (fixture, workers) = parse_arguments(18);
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&fixture);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        println!(
            "record=t17c_wide_protocol fixture={fixture} backend=metal \
             run_multiplicity=1xcpu_oracle+1xmetal correctness=complete_RunResult_equality \
             cpu_workers={workers} cpu_marginal=round_wall_sum metal_marginal=backend_wall \
             formal_outcome=not_applicable_single_sample"
        );
        let (cpu_measurement, expected, width) = cpu(&image, workers, None, None);
        let initialized = Instant::now();
        let executor = MetalExecutor::new().expect("Metal wide executor must initialize");
        let initialization_wall_ns = initialized.elapsed().as_nanos();
        let initialization = executor.initialization_timings();
        let started = Instant::now();
        let run = executor
            .run(&image, None, MetalConfig::default())
            .expect("Metal wide run must succeed");
        let end_to_end_ns = started.elapsed().as_nanos();
        assert_eq!(run.result, expected, "Metal complete result differs");
        assert_eq!(run.rounds, cpu_measurement.outcome.rounds);
        assert_eq!(run.transitions, cpu_measurement.outcome.transitions);
        let marginal_ns = u128::from(run.wall_ns);

        print_measurement("warmup", &fixture, cpu_measurement);
        print_width(&fixture, width, expected.summary);
        println!(
            "record=t17c_wide_initialization fixture={fixture} backend=metal \
             initialization_wall_ns={initialization_wall_ns} device_queue_setup_ns={} \
             pipeline_creation_ns={}",
            initialization.device_queue_setup_ns, initialization.pipeline_creation_ns,
        );
        println!(
            "record=t17c_wide_metal fixture={fixture} sample=0 order=single predecessor=none \
             engine=metal backend=metal workers=0 rounds={} transitions={} end_to_end_ns={} \
             fixed_ns={} marginal_ns={} marginal_ns_per_round={} device_ns={} \
             host_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={} \
             streams_enabled={} legacy_heap_event_slots={} fallback_heap_event_slots={} \
             channel_stream_event_slots={} service_stream_event_slots={} \
             generator_stream_event_slots={} heap_arena_bytes={} stream_arena_bytes={} \
             total_event_arena_bytes={} legacy_heap_arena_bytes={} \
             delta_from_legacy_heap_bytes={} formal_outcome=not_applicable_single_sample \
             exact_complete_result_parity=pass",
            run.rounds,
            run.transitions,
            end_to_end_ns,
            end_to_end_ns
                .checked_sub(marginal_ns)
                .expect("Metal marginal must fit in end-to-end time"),
            marginal_ns,
            marginal_ns / u128::from(run.rounds),
            run.device_ns,
            run.host_encode_submit_ns,
            run.encoded_attempts,
            run.continuation_relaunches,
            run.wave_boundary_syncs,
            run.mid_round_wave_boundary_syncs,
            run.memory_layout.streams_enabled,
            run.memory_layout.legacy_heap_event_slots,
            run.memory_layout.fallback_heap_event_slots,
            run.memory_layout.channel_stream_event_slots,
            run.memory_layout.service_stream_event_slots,
            run.memory_layout.generator_stream_event_slots,
            run.memory_layout.heap_arena_bytes,
            run.memory_layout.stream_arena_bytes,
            run.memory_layout.total_event_arena_bytes(),
            run.memory_layout.legacy_heap_arena_bytes,
            run.memory_layout.delta_from_legacy_heap_bytes(),
        );
    }

    fn parse_arguments(default_workers: usize) -> (String, usize) {
        let mut fixture = None;
        let mut workers = default_workers;
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--best-workers" => {
                    workers = arguments
                        .next()
                        .expect("--best-workers requires a value")
                        .parse()
                        .expect("--best-workers must be an integer");
                }
                unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
                path if fixture.is_none() => fixture = Some(path.to_owned()),
                extra => panic!("unexpected second fixture path {extra}"),
            }
        }
        assert!(workers > 0, "--best-workers must be nonzero");
        (
            fixture.unwrap_or_else(|| {
                "configs/benchmarks/width_via_load_k48_h16/\
                 fattree_k48_h16_load_30_sustained.toml"
                    .to_owned()
            }),
            workers,
        )
    }
}

#[cfg(feature = "cuda")]
fn main() {
    if run_sizing_dry_run_if_requested() {
        return;
    }
    app::cuda_main();
}

#[cfg(all(
    not(feature = "cuda"),
    feature = "metal-spike",
    target_vendor = "apple"
))]
fn main() {
    if run_sizing_dry_run_if_requested() {
        return;
    }
    app::metal_main();
}

#[cfg(not(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
)))]
fn main() {
    if run_sizing_dry_run_if_requested() {
        return;
    }
    panic!("t17c_wide_corpus requires --features cuda or Apple metal-spike");
}

#[cfg(test)]
mod tests {
    use super::{FormalOutcome, SAMPLES, formal_outcome, median, order_for_sample, retained_range};

    #[test]
    fn protocol_has_nine_runs_per_engine_and_balanced_orders() {
        assert_eq!(1 + SAMPLES * 2, 9);
        assert_eq!(
            (0..SAMPLES).map(order_for_sample).collect::<Vec<_>>(),
            ["cpu_first", "gpu_first", "cpu_first", "gpu_first"]
        );
    }

    #[test]
    fn formal_rule_uses_strict_dispersion_parity() {
        assert_eq!(median(vec![1, 2, 3, 4]), 2);
        assert_eq!(retained_range(&[1, 2, 3, 4]), 3);
        assert_eq!(
            formal_outcome(&[80, 81, 82, 83], &[100, 101, 102, 103]),
            FormalOutcome::Beats
        );
        assert_eq!(
            formal_outcome(&[100, 101, 102, 110], &[103, 104, 105, 106]),
            FormalOutcome::Parity
        );
        assert_eq!(formal_outcome(&[100; 4], &[100; 4]), FormalOutcome::Parity);
        assert_eq!(
            formal_outcome(&[105, 106, 107, 115], &[100, 101, 102, 103]),
            FormalOutcome::Trails
        );
    }
}

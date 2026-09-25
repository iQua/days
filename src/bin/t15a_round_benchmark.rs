#[cfg(any(test, feature = "cuda"))]
const DEFAULT_SHORT_FIXTURE: &str =
    "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml";
#[cfg(any(test, feature = "cuda"))]
const DEFAULT_BEST_WORKERS: usize = 19;

#[cfg(any(test, feature = "cuda"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShortEngine {
    Scalar,
    W4,
    WBest,
    Cuda,
}

#[cfg(any(test, feature = "cuda"))]
fn engine_name(engine: ShortEngine) -> &'static str {
    match engine {
        ShortEngine::Scalar => "scalar",
        ShortEngine::W4 => "w4",
        ShortEngine::WBest => "wbest",
        ShortEngine::Cuda => "cuda",
    }
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
#[derive(Clone, Debug, Eq, PartialEq)]
struct CudaOptions {
    fixture: String,
    samples: usize,
    best_workers: usize,
}

#[cfg(any(test, feature = "cuda"))]
fn parse_cuda_options<I, S>(arguments: I) -> Result<CudaOptions, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut fixture = None;
    let mut samples = 4_usize;
    let mut best_workers = DEFAULT_BEST_WORKERS;
    let mut arguments = arguments
        .into_iter()
        .map(|argument| argument.as_ref().to_owned());

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--samples" => {
                samples = arguments
                    .next()
                    .ok_or_else(|| "--samples requires a value".to_owned())?
                    .parse()
                    .map_err(|_| "--samples must be an integer".to_owned())?;
            }
            "--best-workers" => {
                best_workers = arguments
                    .next()
                    .ok_or_else(|| "--best-workers requires a value".to_owned())?
                    .parse()
                    .map_err(|_| "--best-workers must be an integer".to_owned())?;
            }
            unknown if unknown.starts_with("--") => {
                return Err(format!("unknown argument {unknown}"));
            }
            path if fixture.is_none() => fixture = Some(path.to_owned()),
            extra => return Err(format!("unexpected second fixture path {extra}")),
        }
    }

    if samples != 4 {
        return Err("the T15a protocol requires exactly four samples, two per order".to_owned());
    }
    if best_workers == 0 {
        return Err("--best-workers must be nonzero".to_owned());
    }

    Ok(CudaOptions {
        fixture: fixture.unwrap_or_else(|| DEFAULT_SHORT_FIXTURE.to_owned()),
        samples,
        best_workers,
    })
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

    #[derive(Clone, Copy)]
    struct Measurement {
        sample: usize,
        order: &'static str,
        backend: &'static str,
        stream_mode: &'static str,
        workers: usize,
        elapsed_ns: u128,
        outcome: Outcome,
        backend_wall_ns: u64,
        device_ns: u64,
        host_encode_submit_ns: u64,
        encoded_attempts: u64,
        continuation_relaunches: u64,
        wave_boundary_syncs: u64,
        mid_round_wave_boundary_syncs: u64,
    }

    fn scalar(image: &SimulationImage, expected_result: &RunResult) -> Measurement {
        let started = Instant::now();
        let run = run_scalar_rounds(image, None).expect("scalar benchmark run must succeed");
        let elapsed_ns = started.elapsed().as_nanos();
        assert_eq!(
            &run.result, expected_result,
            "scalar benchmark RunResult must match the oracle"
        );
        let outcome = Outcome {
            rounds: run.rounds.len() as u64,
            transitions: run.rounds.iter().map(|round| round.events_processed).sum(),
            summary: run.result.summary,
            resident_packets: run.result.resident_packets.len(),
            pending_events: run.result.pending_events.len(),
        };
        drop(run);
        Measurement {
            sample: 0,
            order: "warmup",
            backend: "scalar",
            stream_mode: "na",
            workers: 1,
            elapsed_ns,
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
        let elapsed_ns = started.elapsed().as_nanos();
        assert_eq!(
            &run.result, expected_result,
            "W{workers} benchmark RunResult must match the oracle"
        );
        let outcome = Outcome {
            rounds: run.rounds.len() as u64,
            transitions: run
                .rounds
                .iter()
                .map(|round| round.semantic.events_processed)
                .sum(),
            summary: run.result.summary,
            resident_packets: run.result.resident_packets.len(),
            pending_events: run.result.pending_events.len(),
        };
        drop(run);
        Measurement {
            sample: 0,
            order: "warmup",
            backend: "cpu",
            stream_mode: "na",
            workers,
            elapsed_ns,
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
        let elapsed_ns = started.elapsed().as_nanos();
        assert_eq!(
            &run.result, expected_result,
            "Metal benchmark RunResult must match the oracle"
        );
        let outcome = Outcome {
            rounds: run.rounds,
            transitions: run.transitions,
            summary: run.result.summary,
            resident_packets: run.result.resident_packets.len(),
            pending_events: run.result.pending_events.len(),
        };
        let measurement = Measurement {
            sample: 0,
            order: "warmup",
            backend: "metal",
            stream_mode: if streams_enabled { "streams" } else { "heap" },
            workers: 0,
            elapsed_ns,
            outcome,
            backend_wall_ns: run.wall_ns,
            device_ns: run.device_ns,
            host_encode_submit_ns: run.host_encode_submit_ns,
            encoded_attempts: run.encoded_attempts,
            continuation_relaunches: run.continuation_relaunches,
            wave_boundary_syncs: run.wave_boundary_syncs,
            mid_round_wave_boundary_syncs: run.mid_round_wave_boundary_syncs,
        };
        drop(run);
        measurement
    }

    fn print_record(kind: &str, fixture: &str, threadgroup_width: usize, measurement: Measurement) {
        println!(
            "record=t15a_{kind} config={fixture} sample={} order={} backend={} stream_mode={} workers={} \
             threadgroup_width={} rounds={} transitions={} end_to_end_ns={} backend_wall_ns={} \
             device_ns={} host_encode_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
            measurement.sample,
            measurement.order,
            measurement.backend,
            measurement.stream_mode,
            measurement.workers,
            threadgroup_width,
            measurement.outcome.rounds,
            measurement.outcome.transitions,
            measurement.elapsed_ns,
            measurement.backend_wall_ns,
            measurement.device_ns,
            measurement.host_encode_submit_ns,
            measurement.encoded_attempts,
            measurement.continuation_relaunches,
            measurement.wave_boundary_syncs,
            measurement.mid_round_wave_boundary_syncs,
        );
    }

    fn median(values: impl Iterator<Item = u128>) -> u128 {
        let mut values = values.collect::<Vec<_>>();
        values.sort_unstable();
        match values.len() {
            0 => 0,
            length if length % 2 == 1 => values[length / 2],
            length => (values[length / 2 - 1] + values[length / 2]) / 2,
        }
    }

    fn after_same_kind_predecessor(mut run: impl FnMut() -> Measurement) -> Measurement {
        let _ = run();
        run()
    }

    let mut arguments = std::env::args().skip(1);
    let relative = arguments.next().unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml".into()
    });
    let mut samples = 4_usize;
    let mut threadgroup_width = 256_usize;
    let mut streams_enabled = true;
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
            "--streams-disabled" => streams_enabled = false,
            unknown => panic!("unknown argument {unknown}"),
        }
    }
    assert_eq!(
        samples, 4,
        "the T15a protocol requires exactly four samples, two per order"
    );

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let oracle = run_scalar_rounds(&image, None)
        .expect("scalar benchmark oracle must succeed")
        .result;
    let executor = MetalExecutor::new().expect("Metal benchmark executor must initialize");

    let warmups = [
        scalar(&image, &oracle),
        cpu(&image, 4, &oracle),
        cpu(&image, 18, &oracle),
        metal(
            &executor,
            &image,
            threadgroup_width,
            streams_enabled,
            &oracle,
        ),
    ];
    let expected = warmups[0].outcome;
    for warmup in warmups {
        assert_eq!(warmup.outcome, expected, "warmup backends must agree");
        print_record("warmup", &relative, threadgroup_width, warmup);
    }

    let mut measurements = Vec::with_capacity(samples * 4);
    for sample in 0..samples {
        let order = if sample % 2 == 0 {
            "cpu_first"
        } else {
            "gpu_first"
        };
        let ordered = if order == "cpu_first" {
            [
                after_same_kind_predecessor(|| scalar(&image, &oracle)),
                after_same_kind_predecessor(|| cpu(&image, 4, &oracle)),
                after_same_kind_predecessor(|| cpu(&image, 18, &oracle)),
                after_same_kind_predecessor(|| {
                    metal(
                        &executor,
                        &image,
                        threadgroup_width,
                        streams_enabled,
                        &oracle,
                    )
                }),
            ]
        } else {
            [
                after_same_kind_predecessor(|| {
                    metal(
                        &executor,
                        &image,
                        threadgroup_width,
                        streams_enabled,
                        &oracle,
                    )
                }),
                after_same_kind_predecessor(|| scalar(&image, &oracle)),
                after_same_kind_predecessor(|| cpu(&image, 4, &oracle)),
                after_same_kind_predecessor(|| cpu(&image, 18, &oracle)),
            ]
        };
        for mut measurement in ordered {
            measurement.sample = sample;
            measurement.order = order;
            assert_eq!(
                measurement.outcome, expected,
                "timed sample backends must agree"
            );
            print_record("sample", &relative, threadgroup_width, measurement);
            measurements.push(measurement);
        }
    }

    for (backend, workers) in [("scalar", 1), ("cpu", 4), ("cpu", 18), ("metal", 0)] {
        let selected = measurements
            .iter()
            .filter(|measurement| measurement.backend == backend && measurement.workers == workers);
        let elapsed_ns = median(selected.clone().map(|measurement| measurement.elapsed_ns));
        let backend_wall_ns = median(
            selected
                .clone()
                .map(|measurement| u128::from(measurement.backend_wall_ns)),
        );
        let device_ns = median(
            selected
                .clone()
                .map(|measurement| u128::from(measurement.device_ns)),
        );
        let host_ns =
            median(selected.map(|measurement| u128::from(measurement.host_encode_submit_ns)));
        println!(
            "record=t15a_summary config={relative} backend={backend} stream_mode={} workers={workers} samples={} \
             threadgroup_width={threadgroup_width} rounds={} transitions={} \
             median_end_to_end_ns={elapsed_ns} median_backend_wall_ns={backend_wall_ns} \
             median_device_ns={device_ns} median_host_encode_submit_ns={host_ns}",
            if backend == "metal" {
                if streams_enabled { "streams" } else { "heap" }
            } else {
                "na"
            },
            measurements
                .iter()
                .filter(|measurement| {
                    measurement.backend == backend && measurement.workers == workers
                })
                .count(),
            expected.rounds,
            expected.transitions,
        );
        for order in ["cpu_first", "gpu_first"] {
            let selected = measurements.iter().filter(|measurement| {
                measurement.backend == backend
                    && measurement.workers == workers
                    && measurement.order == order
            });
            println!(
                "record=t15a_order_summary config={relative} order={order} backend={backend} \
                 stream_mode={} workers={workers} samples={} threadgroup_width={threadgroup_width} rounds={} \
                 transitions={} median_end_to_end_ns={} median_backend_wall_ns={} \
                 median_device_ns={} median_host_encode_submit_ns={}",
                if backend == "metal" {
                    if streams_enabled { "streams" } else { "heap" }
                } else {
                    "na"
                },
                measurements
                    .iter()
                    .filter(|measurement| {
                        measurement.backend == backend
                            && measurement.workers == workers
                            && measurement.order == order
                    })
                    .count(),
                expected.rounds,
                expected.transitions,
                median(selected.clone().map(|measurement| measurement.elapsed_ns)),
                median(
                    selected
                        .clone()
                        .map(|measurement| u128::from(measurement.backend_wall_ns))
                ),
                median(
                    selected
                        .clone()
                        .map(|measurement| u128::from(measurement.device_ns))
                ),
                median(
                    selected.map(|measurement| { u128::from(measurement.host_encode_submit_ns) })
                ),
            );
        }
    }
}

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
mod cuda_app {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{
        CpuConfig, CudaConfig, CudaExecutor, RunResult, RunSummary, SimulationImage, run_cpu,
        run_scalar_rounds,
    };

    use super::{CudaOptions, ShortEngine, engine_name, order_for_sample, parse_cuda_options};

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
        engine: ShortEngine,
        workers: usize,
        end_to_end_ns: u128,
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
        fn end_to_end_ns_per_round(self) -> u128 {
            per_round(self.end_to_end_ns, self.outcome.rounds)
        }

        fn backend_wall_ns_per_round(self) -> u128 {
            per_round(u128::from(self.backend_wall_ns), self.outcome.rounds)
        }

        fn device_ns_per_round(self) -> u128 {
            per_round(u128::from(self.device_ns), self.outcome.rounds)
        }

        fn host_submit_ns_per_round(self) -> u128 {
            per_round(u128::from(self.host_submit_ns), self.outcome.rounds)
        }
    }

    fn per_round(value: u128, rounds: u64) -> u128 {
        assert!(rounds > 0, "short benchmark run produced zero rounds");
        value / u128::from(rounds)
    }

    fn backend_name(engine: ShortEngine) -> &'static str {
        match engine {
            ShortEngine::Scalar => "scalar",
            ShortEngine::W4 | ShortEngine::WBest => "cpu",
            ShortEngine::Cuda => "cuda",
        }
    }

    fn stream_mode(engine: ShortEngine) -> &'static str {
        if engine == ShortEngine::Cuda {
            "streams"
        } else {
            "na"
        }
    }

    fn scalar(image: &SimulationImage, expected_result: &RunResult) -> Measurement {
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

        Measurement {
            sample: 0,
            order: "warmup",
            predecessor: "none",
            engine: ShortEngine::Scalar,
            workers: 1,
            end_to_end_ns,
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
        engine: ShortEngine,
        workers: usize,
        expected_result: &RunResult,
    ) -> Measurement {
        assert!(
            matches!(engine, ShortEngine::W4 | ShortEngine::WBest),
            "CPU measurement requires a CPU engine role"
        );
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
            "W{workers} benchmark RunResult must match the scalar oracle"
        );
        let rounds = u64::try_from(run.rounds.len()).expect("CPU round count must fit in u64");
        let transitions = run.rounds.iter().fold(0_u64, |total, round| {
            total.saturating_add(round.semantic.events_processed)
        });
        let outcome = Outcome {
            rounds,
            transitions,
            summary: run.result.summary,
            resident_packets: run.result.resident_packets.len(),
            pending_events: run.result.pending_events.len(),
        };
        drop(run);

        Measurement {
            sample: 0,
            order: "warmup",
            predecessor: "none",
            engine,
            workers,
            end_to_end_ns,
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
        config: CudaConfig,
        expected_result: &RunResult,
    ) -> Measurement {
        let started = Instant::now();
        let run = executor
            .run(image, None, config)
            .expect("CUDA benchmark run must succeed");
        let end_to_end_ns = started.elapsed().as_nanos();
        assert_eq!(
            &run.result, expected_result,
            "CUDA benchmark RunResult must match the scalar oracle"
        );
        let outcome = Outcome {
            rounds: run.rounds,
            transitions: run.transitions,
            summary: run.result.summary,
            resident_packets: run.result.resident_packets.len(),
            pending_events: run.result.pending_events.len(),
        };
        let measurement = Measurement {
            sample: 0,
            order: "warmup",
            predecessor: "none",
            engine: ShortEngine::Cuda,
            workers: 0,
            end_to_end_ns,
            outcome,
            backend_wall_ns: run.wall_ns,
            device_ns: run.device_ns,
            host_submit_ns: run.host_submit_ns,
            graph_capture_ns: run.graph_capture_ns,
            graph_replays: run.graph_replays,
            encoded_attempts: run.encoded_attempts,
            continuation_relaunches: run.continuation_relaunches,
            wave_boundary_syncs: run.wave_boundary_syncs,
            mid_round_wave_boundary_syncs: run.mid_round_wave_boundary_syncs,
        };
        drop(run);
        measurement
    }

    fn after_same_kind_predecessor(mut run: impl FnMut() -> Measurement) -> Measurement {
        let _ = run();
        let mut measurement = run();
        measurement.predecessor = "same_kind_discarded";
        measurement
    }

    fn print_record(kind: &str, fixture: &str, block_width: usize, measurement: Measurement) {
        println!(
            "record=t15a_{kind} config={fixture} sample={} order={} predecessor={} \
             engine={} backend={} stream_mode={} workers={} block_width={} rounds={} transitions={} \
             end_to_end_ns={} end_to_end_ns_per_round={} backend_wall_ns={} \
             backend_wall_ns_per_round={} device_ns={} device_ns_per_round={} \
             host_submit_ns={} host_submit_ns_per_round={} graph_capture_ns={} graph_replays={} \
             encoded_attempts={} continuation_relaunches={} wave_boundary_syncs={} \
             mid_round_wave_boundary_syncs={}",
            measurement.sample,
            measurement.order,
            measurement.predecessor,
            engine_name(measurement.engine),
            backend_name(measurement.engine),
            stream_mode(measurement.engine),
            measurement.workers,
            block_width,
            measurement.outcome.rounds,
            measurement.outcome.transitions,
            measurement.end_to_end_ns,
            measurement.end_to_end_ns_per_round(),
            measurement.backend_wall_ns,
            measurement.backend_wall_ns_per_round(),
            measurement.device_ns,
            measurement.device_ns_per_round(),
            measurement.host_submit_ns,
            measurement.host_submit_ns_per_round(),
            measurement.graph_capture_ns,
            measurement.graph_replays,
            measurement.encoded_attempts,
            measurement.continuation_relaunches,
            measurement.wave_boundary_syncs,
            measurement.mid_round_wave_boundary_syncs,
        );
    }

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

    fn print_summary(
        kind: &str,
        fixture: &str,
        order: &str,
        block_width: usize,
        selected: &[Measurement],
    ) {
        assert!(!selected.is_empty(), "summary selection must be nonempty");
        let first = selected[0];
        assert!(
            selected.iter().all(|measurement| {
                measurement.engine == first.engine
                    && measurement.outcome == first.outcome
                    && measurement.predecessor == first.predecessor
            }),
            "summary selection must describe one engine and outcome"
        );
        println!(
            "record=t15a_{kind} statistic=median config={fixture} order={order} predecessor={} \
             engine={} backend={} stream_mode={} workers={} samples={} block_width={block_width} \
             rounds={} transitions={} median_end_to_end_ns={} \
             median_end_to_end_ns_per_round={} median_backend_wall_ns={} \
             median_backend_wall_ns_per_round={} median_device_ns={} \
             median_device_ns_per_round={} median_host_submit_ns={} \
             median_host_submit_ns_per_round={} median_graph_capture_ns={} \
             median_graph_replays={} median_encoded_attempts={} \
             median_continuation_relaunches={} median_wave_boundary_syncs={} \
             median_mid_round_wave_boundary_syncs={}",
            first.predecessor,
            engine_name(first.engine),
            backend_name(first.engine),
            stream_mode(first.engine),
            first.workers,
            selected.len(),
            first.outcome.rounds,
            first.outcome.transitions,
            median(selected.iter().map(|measurement| measurement.end_to_end_ns)),
            median(
                selected
                    .iter()
                    .copied()
                    .map(Measurement::end_to_end_ns_per_round)
            ),
            median(
                selected
                    .iter()
                    .map(|measurement| u128::from(measurement.backend_wall_ns))
            ),
            median(
                selected
                    .iter()
                    .copied()
                    .map(Measurement::backend_wall_ns_per_round)
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
                    .copied()
                    .map(Measurement::host_submit_ns_per_round)
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

    pub fn run() {
        let options =
            parse_cuda_options(std::env::args().skip(1)).unwrap_or_else(|error| panic!("{error}"));
        run_with_options(options);
    }

    fn run_with_options(options: CudaOptions) {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&options.fixture);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        let oracle = run_scalar_rounds(&image, None)
            .expect("scalar benchmark oracle must succeed")
            .result;
        let executor = CudaExecutor::new().expect("CUDA benchmark executor must initialize");
        let initialization = executor.initialization_timings();
        let cuda_config = CudaConfig::default();
        let block_width = cuda_config.round_threads_per_block;

        println!(
            "record=t15a_protocol backend=cuda samples={} samples_per_order=2 \
             comparison_predecessor=same_kind_discarded order_schedule=cpu_first,gpu_first,cpu_first,gpu_first \
             correctness_rule=complete_RunResult_equality best_workers={} block_width={block_width}",
            options.samples, options.best_workers,
        );
        println!(
            "record=t15a_initialization config={} engine=cuda context_stream_setup_ns={} \
             module_function_load_ns={} in_process_reuse=1",
            options.fixture,
            initialization.context_stream_setup_ns,
            initialization.module_function_load_ns,
        );

        let warmups = [
            scalar(&image, &oracle),
            cpu(&image, ShortEngine::W4, 4, &oracle),
            cpu(&image, ShortEngine::WBest, options.best_workers, &oracle),
            cuda(&executor, &image, cuda_config, &oracle),
        ];
        let expected = warmups[0].outcome;
        for warmup in warmups {
            assert_eq!(warmup.outcome, expected, "warmup backends must agree");
            print_record("warmup", &options.fixture, block_width, warmup);
        }

        let mut measurements = Vec::with_capacity(options.samples.saturating_mul(4));
        for sample in 0..options.samples {
            let order = order_for_sample(sample);
            let ordered = if order == "cpu_first" {
                [
                    after_same_kind_predecessor(|| scalar(&image, &oracle)),
                    after_same_kind_predecessor(|| cpu(&image, ShortEngine::W4, 4, &oracle)),
                    after_same_kind_predecessor(|| {
                        cpu(&image, ShortEngine::WBest, options.best_workers, &oracle)
                    }),
                    after_same_kind_predecessor(|| cuda(&executor, &image, cuda_config, &oracle)),
                ]
            } else {
                [
                    after_same_kind_predecessor(|| cuda(&executor, &image, cuda_config, &oracle)),
                    after_same_kind_predecessor(|| scalar(&image, &oracle)),
                    after_same_kind_predecessor(|| cpu(&image, ShortEngine::W4, 4, &oracle)),
                    after_same_kind_predecessor(|| {
                        cpu(&image, ShortEngine::WBest, options.best_workers, &oracle)
                    }),
                ]
            };
            for mut measurement in ordered {
                measurement.sample = sample;
                measurement.order = order;
                assert_eq!(
                    measurement.outcome, expected,
                    "timed sample backends must agree"
                );
                print_record("sample", &options.fixture, block_width, measurement);
                measurements.push(measurement);
            }
        }

        for engine in [
            ShortEngine::Scalar,
            ShortEngine::W4,
            ShortEngine::WBest,
            ShortEngine::Cuda,
        ] {
            let pooled = measurements
                .iter()
                .filter(|measurement| measurement.engine == engine)
                .copied()
                .collect::<Vec<_>>();
            print_summary("summary", &options.fixture, "pooled", block_width, &pooled);
            for order in ["cpu_first", "gpu_first"] {
                let ordered = pooled
                    .iter()
                    .filter(|measurement| measurement.order == order)
                    .copied()
                    .collect::<Vec<_>>();
                print_summary(
                    "order_summary",
                    &options.fixture,
                    order,
                    block_width,
                    &ordered,
                );
            }
        }
    }
}

#[cfg(all(
    feature = "cuda",
    not(all(feature = "metal-spike", target_vendor = "apple"))
))]
fn main() {
    cuda_app::run();
}

#[cfg(not(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
)))]
fn main() {
    eprintln!("t15a_round_benchmark requires --features metal-spike on an Apple target");
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_BEST_WORKERS, DEFAULT_SHORT_FIXTURE, ShortEngine, engine_name, order_for_sample,
        parse_cuda_options,
    };

    #[test]
    fn short_protocol_balances_two_samples_per_order() {
        let orders = (0..4).map(order_for_sample).collect::<Vec<_>>();
        assert_eq!(orders, ["cpu_first", "gpu_first", "cpu_first", "gpu_first"]);
    }

    #[test]
    fn short_engine_names_keep_w4_and_selected_best_distinct() {
        assert_eq!(engine_name(ShortEngine::Scalar), "scalar");
        assert_eq!(engine_name(ShortEngine::W4), "w4");
        assert_eq!(engine_name(ShortEngine::WBest), "wbest");
        assert_eq!(engine_name(ShortEngine::Cuda), "cuda");
    }

    #[test]
    fn cuda_options_parse_fixture_samples_and_best_workers() {
        let options = parse_cuda_options([
            "configs/custom.toml",
            "--samples",
            "4",
            "--best-workers",
            "16",
        ])
        .expect("valid CUDA short options must parse");

        assert_eq!(options.fixture, "configs/custom.toml");
        assert_eq!(options.samples, 4);
        assert_eq!(options.best_workers, 16);
    }

    #[test]
    fn cuda_options_keep_audited_defaults() {
        let options = parse_cuda_options(std::iter::empty::<&str>())
            .expect("default CUDA short options must parse");

        assert_eq!(options.fixture, DEFAULT_SHORT_FIXTURE);
        assert_eq!(options.samples, 4);
        assert_eq!(options.best_workers, DEFAULT_BEST_WORKERS);
    }

    #[test]
    fn cuda_options_reject_zero_best_workers() {
        let error =
            parse_cuda_options(["--best-workers", "0"]).expect_err("zero workers must be rejected");

        assert_eq!(error, "--best-workers must be nonzero");
    }
}

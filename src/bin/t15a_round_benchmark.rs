#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{
        CpuConfig, MetalConfig, MetalExecutor, RunSummary, SimulationImage, run_cpu,
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

    fn scalar(image: &SimulationImage) -> Measurement {
        let started = Instant::now();
        let run = run_scalar_rounds(image, None).expect("scalar benchmark run must succeed");
        let elapsed_ns = started.elapsed().as_nanos();
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

    fn cpu(image: &SimulationImage, workers: usize) -> Measurement {
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
    ) -> Measurement {
        let started = Instant::now();
        let run = executor
            .run(
                image,
                None,
                MetalConfig {
                    round_threads_per_threadgroup: threadgroup_width,
                    ..MetalConfig::default()
                },
            )
            .expect("Metal benchmark run must succeed");
        let elapsed_ns = started.elapsed().as_nanos();
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
            "record=t15a_{kind} config={fixture} sample={} order={} backend={} workers={} \
             threadgroup_width={} rounds={} transitions={} end_to_end_ns={} backend_wall_ns={} \
             device_ns={} host_encode_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
            measurement.sample,
            measurement.order,
            measurement.backend,
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
            unknown => panic!("unknown argument {unknown}"),
        }
    }
    assert!(samples > 0, "--samples must be nonzero");

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let executor = MetalExecutor::new().expect("Metal benchmark executor must initialize");

    let warmups = [
        scalar(&image),
        cpu(&image, 4),
        cpu(&image, 18),
        metal(&executor, &image, threadgroup_width),
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
                after_same_kind_predecessor(|| scalar(&image)),
                after_same_kind_predecessor(|| cpu(&image, 4)),
                after_same_kind_predecessor(|| cpu(&image, 18)),
                after_same_kind_predecessor(|| metal(&executor, &image, threadgroup_width)),
            ]
        } else {
            [
                after_same_kind_predecessor(|| metal(&executor, &image, threadgroup_width)),
                after_same_kind_predecessor(|| scalar(&image)),
                after_same_kind_predecessor(|| cpu(&image, 4)),
                after_same_kind_predecessor(|| cpu(&image, 18)),
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
            "record=t15a_summary config={relative} backend={backend} workers={workers} samples={} \
             threadgroup_width={threadgroup_width} rounds={} transitions={} \
             median_end_to_end_ns={elapsed_ns} median_backend_wall_ns={backend_wall_ns} \
             median_device_ns={device_ns} median_host_encode_submit_ns={host_ns}",
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
                 workers={workers} samples={} threadgroup_width={threadgroup_width} rounds={} \
                 transitions={} median_end_to_end_ns={} median_backend_wall_ns={} \
                 median_device_ns={} median_host_encode_submit_ns={}",
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

#[cfg(not(all(feature = "metal-spike", target_vendor = "apple")))]
fn main() {
    eprintln!("t15a_round_benchmark requires --features metal-spike on an Apple target");
    std::process::exit(2);
}

#[cfg(any(test, all(feature = "metal-spike", target_vendor = "apple")))]
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

#[cfg(any(test, all(feature = "metal-spike", target_vendor = "apple")))]
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

#[cfg(any(test, all(feature = "metal-spike", target_vendor = "apple")))]
fn order_for_sample(sample: usize) -> &'static str {
    if sample.is_multiple_of(2) {
        "cpu_first"
    } else {
        "gpu_first"
    }
}

#[cfg(any(test, all(feature = "metal-spike", target_vendor = "apple")))]
const SCALAR_SAMPLES: usize = 2;

#[cfg(any(test, all(feature = "metal-spike", target_vendor = "apple")))]
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

    fn scalar(image: &SimulationImage, calibration_ns: u128) -> Measurement {
        let started = Instant::now();
        let run = run_scalar_rounds(image, None).expect("scalar benchmark run must succeed");
        let end_to_end_ns = started.elapsed().as_nanos();
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
        let end_to_end_ns = started.elapsed().as_nanos();
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
        let end_to_end_ns = started.elapsed().as_nanos();
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
            engine: "metal",
            backend: "metal",
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

    fn cpu_after_predecessor(image: &SimulationImage, workers: usize) -> Measurement {
        let _ = cpu(image, workers);
        let mut measurement = cpu(image, workers);
        measurement.predecessor = recorded_predecessor_for_engine(measurement.engine);
        measurement
    }

    fn metal_after_predecessor(
        executor: &MetalExecutor,
        image: &SimulationImage,
        threadgroup_width: usize,
        device_queue_setup_ns: u128,
        pipeline_creation_ns: u128,
    ) -> Measurement {
        let _ = metal(
            executor,
            image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
        );
        let mut measurement = metal(
            executor,
            image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
        );
        measurement.predecessor = recorded_predecessor_for_engine(measurement.engine);
        measurement
    }

    fn print_record(kind: &str, fixture: &str, threadgroup_width: usize, measurement: Measurement) {
        println!(
            "record=t15e_{kind} config={fixture} sample={} order={} predecessor={} \
             engine={} backend={} workers={} \
             threadgroup_width={} rounds={} transitions={} fixed_method={} separation_quality={} \
             end_to_end_ns={} \
             cold_end_to_end_ns={} fixed_ns={} warm_fixed_ns={} marginal_ns={} \
             marginal_ns_per_round={} calibration_ns={} backend_wall_ns={} device_ns={} \
             host_encode_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
            measurement.sample,
            measurement.order,
            measurement.predecessor,
            measurement.engine,
            measurement.backend,
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
        order: &str,
        threadgroup_width: usize,
        selected: &[Measurement],
    ) {
        assert!(!selected.is_empty(), "summary selection must be nonempty");
        let first = selected[0];
        assert!(
            selected.iter().all(|measurement| {
                measurement.engine == first.engine
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
             aggregation=component_medians_with_derived_fixed_closure config={fixture} \
             order={order} predecessor={} engine={} \
             backend={} workers={} samples={} threadgroup_width={} rounds={} transitions={} \
             fixed_method={} separation_quality={} end_to_end_ns={} cold_end_to_end_ns={} \
             fixed_ns={} warm_fixed_ns={} marginal_ns={} marginal_ns_per_round={} \
             calibration_ns={} backend_wall_ns={} \
             device_ns={} host_encode_submit_ns={} encoded_attempts={} continuation_relaunches={} \
             wave_boundary_syncs={} mid_round_wave_boundary_syncs={}",
            first.predecessor,
            first.engine,
            first.backend,
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
            unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
            path if !relative_was_set => {
                relative = path.to_owned();
                relative_was_set = true;
            }
            extra => panic!("unexpected second fixture path {extra}"),
        }
    }
    assert!(
        samples >= 2 && samples.is_multiple_of(2),
        "--samples must be a positive even count so both orders are represented"
    );
    assert!(threadgroup_width > 0, "--threadgroup-width must be nonzero");

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let executor = MetalExecutor::new().expect("Metal benchmark executor must initialize");
    let initialization = executor.initialization_timings();
    let device_queue_setup_ns = u128::from(initialization.device_queue_setup_ns);
    let pipeline_creation_ns = u128::from(initialization.pipeline_creation_ns);
    println!(
        "record=t15e_protocol scalar_samples={SCALAR_SAMPLES} scalar_predecessor=none \
         comparison_samples={samples} comparison_predecessor=same_kind_discarded \
         order_schedule=alternating_cpu_first_gpu_first"
    );
    println!(
        "record=t15e_initialization config={relative} engine=metal device_queue_setup_ns={} \
         pipeline_creation_ns={} in_process_reuse=1 archive_saving_status=unmeasured \
         archive_upper_bound_ns={}",
        initialization.device_queue_setup_ns,
        initialization.pipeline_creation_ns,
        initialization.pipeline_creation_ns,
    );

    let warmups = [
        cpu(&image, 4),
        cpu(&image, 18),
        metal(
            &executor,
            &image,
            threadgroup_width,
            device_queue_setup_ns,
            pipeline_creation_ns,
        ),
    ];
    let expected = warmups[0].outcome;
    for warmup in warmups {
        assert_eq!(warmup.outcome, expected, "warmup backends must agree");
        print_record("warmup", &relative, threadgroup_width, warmup);
    }

    let mut measurements =
        Vec::with_capacity(samples.saturating_mul(3).saturating_add(SCALAR_SAMPLES));
    for sample in 0..samples {
        let order = order_for_sample(sample);
        let include_scalar = sample < SCALAR_SAMPLES;
        let mut ordered = Vec::with_capacity(3 + usize::from(include_scalar));
        if order == "cpu_first" {
            if include_scalar {
                ordered.push(scalar(&image, scalar_calibration(&image)));
            }
            ordered.extend([
                cpu_after_predecessor(&image, 4),
                cpu_after_predecessor(&image, 18),
                metal_after_predecessor(
                    &executor,
                    &image,
                    threadgroup_width,
                    device_queue_setup_ns,
                    pipeline_creation_ns,
                ),
            ]);
        } else {
            ordered.push(metal_after_predecessor(
                &executor,
                &image,
                threadgroup_width,
                device_queue_setup_ns,
                pipeline_creation_ns,
            ));
            if include_scalar {
                ordered.push(scalar(&image, scalar_calibration(&image)));
            }
            ordered.extend([
                cpu_after_predecessor(&image, 4),
                cpu_after_predecessor(&image, 18),
            ]);
        }
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

    for engine in ["scalar", "w4", "w18", "metal"] {
        let pooled = measurements
            .iter()
            .filter(|measurement| measurement.engine == engine)
            .copied()
            .collect::<Vec<_>>();
        print_summary("summary", &relative, "pooled", threadgroup_width, &pooled);
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
                    order,
                    threadgroup_width,
                    &ordered,
                );
            }
        }
    }

    let pooled_marginal = |engine| {
        median(
            measurements
                .iter()
                .filter(|measurement| measurement.engine == engine)
                .map(|measurement| measurement.marginal_ns),
        )
    };
    let scalar_samples = measurements
        .iter()
        .filter(|measurement| measurement.engine == "scalar")
        .count();
    let w4_marginal_ns = pooled_marginal("w4");
    let w18_marginal_ns = pooled_marginal("w18");
    let metal_marginal_ns = pooled_marginal("metal");
    assert!(
        w4_marginal_ns > 0 && w18_marginal_ns > 0 && metal_marginal_ns > 0,
        "marginal-ratio inputs must be nonzero"
    );
    let metal_over_w4 = metal_marginal_ns as f64 / w4_marginal_ns as f64;
    let metal_over_w18 = metal_marginal_ns as f64 / w18_marginal_ns as f64;
    let w4_over_metal = w4_marginal_ns as f64 / metal_marginal_ns as f64;
    let w18_over_metal = w18_marginal_ns as f64 / metal_marginal_ns as f64;
    let metal_beats_w4 = metal_marginal_ns < w4_marginal_ns;
    let metal_beats_w18 = metal_marginal_ns < w18_marginal_ns;
    let crossover = match (metal_beats_w4, metal_beats_w18) {
        (true, true) => "metal_below_w4_and_w18",
        (true, false) => "metal_below_w4_only",
        (false, true) => "metal_below_w18_only",
        (false, false) => "metal_below_neither",
    };
    println!(
        "record=t15e_pooled_summary statistic=median config={relative} \
         scalar_samples={scalar_samples} comparison_samples={samples} \
         threadgroup_width={threadgroup_width} rounds={} transitions={} \
         ratio_definition=metal_marginal_div_cpu_marginal \
         speedup_definition=cpu_marginal_div_metal_marginal \
         w4_marginal_ns={w4_marginal_ns} w18_marginal_ns={w18_marginal_ns} \
         metal_marginal_ns={metal_marginal_ns} metal_over_w4={metal_over_w4:.6} \
         metal_over_w18={metal_over_w18:.6} w4_over_metal={w4_over_metal:.6} \
         w18_over_metal={w18_over_metal:.6} metal_beats_w4={} metal_beats_w18={} \
         crossover={crossover} crossover_basis=marginal",
        expected.rounds,
        expected.transitions,
        u8::from(metal_beats_w4),
        u8::from(metal_beats_w18),
    );
}

#[cfg(not(all(feature = "metal-spike", target_vendor = "apple")))]
fn main() {
    eprintln!("t15e_sustained_benchmark requires --features metal-spike on an Apple target");
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::{
        SCALAR_SAMPLES, median, order_for_sample, recorded_predecessor_for_engine, split_fixed,
    };

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
        for engine in ["w4", "w18", "metal"] {
            assert_eq!(
                recorded_predecessor_for_engine(engine),
                "same_kind_discarded"
            );
        }
    }
}

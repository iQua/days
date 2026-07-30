#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{
        MetalConfig, MetalDrainDecomposition, MetalExecutor, MetalFelControlRun, MetalFelProbeRun,
        MetalMergeFanInRun, MetalRun,
    };

    #[derive(Clone, Copy)]
    struct Sample {
        index: usize,
        order: &'static str,
        decomposition: MetalDrainDecomposition,
        production_device_ns: u64,
        production_wall_ns: u64,
        production_profiled_useful_ns: u64,
        production_target_merge_ns: u64,
        diagnostic_pipeline_creation_ns: u64,
    }

    fn median_u64(values: impl Iterator<Item = u64>) -> u64 {
        let mut values = values.collect::<Vec<_>>();
        assert!(!values.is_empty(), "median requires at least one value");
        values.sort_unstable();
        match values.len() {
            length if length.is_multiple_of(2) => {
                let sum = u128::from(values[length / 2 - 1]) + u128::from(values[length / 2]);
                (sum / 2) as u64
            }
            length => values[length / 2],
        }
    }

    fn median_i128(values: impl Iterator<Item = i128>) -> i128 {
        let mut values = values.collect::<Vec<_>>();
        assert!(!values.is_empty(), "median requires at least one value");
        values.sort_unstable();
        match values.len() {
            length if length.is_multiple_of(2) => (values[length / 2 - 1] + values[length / 2]) / 2,
            length => values[length / 2],
        }
    }

    fn baseline(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalRun {
        executor
            .run_profiled(image, None, config)
            .expect("profiled Metal baseline must run")
    }

    fn production(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalRun {
        executor
            .run(image, None, config)
            .expect("ordinary production Metal run must succeed")
    }

    fn control(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalFelControlRun {
        executor
            .run_fel_control_profiled(image, None, config)
            .expect("profiled FEL matched control must run")
    }

    fn probe(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalFelProbeRun {
        executor
            .run_fel_probe_profiled(image, None, config)
            .expect("profiled FEL stress probe must run")
    }

    fn fan_in(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalMergeFanInRun {
        executor
            .run_merge_fan_in_profiled(image, None, config)
            .expect("profiled merge fan-in characterization must run")
    }

    fn after_baseline_predecessor(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalRun {
        let predecessor = baseline(executor, image, config);
        drop(predecessor);
        baseline(executor, image, config)
    }

    fn after_production_predecessor(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalRun {
        let predecessor = production(executor, image, config);
        drop(predecessor);
        production(executor, image, config)
    }

    fn after_control_predecessor(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalFelControlRun {
        let predecessor = control(executor, image, config);
        drop(predecessor);
        control(executor, image, config)
    }

    fn after_probe_predecessor(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalFelProbeRun {
        let predecessor = probe(executor, image, config);
        drop(predecessor);
        probe(executor, image, config)
    }

    fn after_fan_in_predecessor(
        executor: &MetalExecutor,
        image: &days_executor::SimulationImage,
        config: MetalConfig,
    ) -> MetalMergeFanInRun {
        let predecessor = fan_in(executor, image, config);
        drop(predecessor);
        fan_in(executor, image, config)
    }

    fn useful_profiled_ns(run: &MetalRun) -> u64 {
        run.phase_profile
            .as_ref()
            .expect("drain diagnostic runs must be profiled")
            .useful
            .total_ns()
    }

    fn useful_merge_ns(run: &MetalRun) -> u64 {
        run.phase_profile
            .as_ref()
            .expect("drain diagnostic runs must be profiled")
            .useful
            .target_merge_ns
    }

    fn stress_scaled_estimate_ns(
        delta_ns: i128,
        production_operations: u64,
        injected_operations: u64,
    ) -> Option<u64> {
        if delta_ns <= 0 || injected_operations == 0 {
            return None;
        }
        let scaled = (delta_ns as u128)
            .checked_mul(u128::from(production_operations))
            .expect("stress-scaled FEL estimate must not overflow")
            / u128::from(injected_operations);
        Some(u64::try_from(scaled).expect("stress-scaled FEL estimate must fit in u64"))
    }

    fn optional_number(value: Option<u64>) -> String {
        value.map_or_else(|| "unavailable".to_owned(), |value| value.to_string())
    }

    fn per_round_ms(nanoseconds: u64, rounds: u64) -> f64 {
        nanoseconds as f64 / rounds as f64 / 1_000_000.0
    }

    fn print_sample(fixture: &str, sample: Sample) {
        let split = sample.decomposition;
        let estimate = optional_number(split.stress_scaled_fel_estimate_ns);
        let residual = optional_number(split.residual_after_stress_scaled_estimate_ns);
        let share_ppm = split.stress_scaled_fel_estimate_ns.and_then(|estimate| {
            (sample.production_device_ns != 0).then(|| {
                let scaled = u128::from(estimate)
                    .checked_mul(1_000_000)
                    .expect("sample FEL share must not overflow")
                    / u128::from(sample.production_device_ns);
                u64::try_from(scaled).expect("sample FEL share must fit in u64")
            })
        });
        println!(
            "record=t15e_drain_sample config={fixture} stream_mode=heap sample={} order={} rounds={} \
             transitions={} production_drain_execute_ns={} matched_control_drain_execute_ns={} \
             stress_probe_drain_execute_ns={} fel_round_trip_delta_ns={} \
             production_drain_execute_ms_per_round={:.6} \
             matched_control_drain_execute_ms_per_round={:.6} \
             stress_probe_drain_execute_ms_per_round={:.6} \
             injected_fel_round_trips={} injected_fel_operations={} local_fel_pushes={} \
             production_drain_fel_operations={} stress_scaled_fel_estimate_ns={} \
             residual_after_stress_scaled_estimate_ns={} \
             production_device_ns={} production_wall_ns={} \
             production_profiled_useful_ns={} stress_scaled_fel_share_ppm={} \
             production_target_merge_ns={} diagnostic_pipeline_creation_ns={} \
             estimate_method=upper_biased_root_reinsert_stress_scaled_by_operation_count \
             inference_limit=not_representative_and_cannot_indict_calendar_fel",
            sample.index,
            sample.order,
            split.rounds,
            split.transitions,
            split.baseline_drain_execute_ns,
            split.matched_control_drain_execute_ns,
            split.probe_drain_execute_ns,
            split.fel_round_trip_delta_ns,
            per_round_ms(split.baseline_drain_execute_ns, split.rounds),
            per_round_ms(split.matched_control_drain_execute_ns, split.rounds),
            per_round_ms(split.probe_drain_execute_ns, split.rounds),
            split.injected_fel_operations / 2,
            split.injected_fel_operations,
            split.local_fel_pushes,
            split.production_drain_fel_operations,
            estimate,
            residual,
            sample.production_device_ns,
            sample.production_wall_ns,
            sample.production_profiled_useful_ns,
            optional_number(share_ppm),
            sample.production_target_merge_ns,
            sample.diagnostic_pipeline_creation_ns,
        );
    }

    let mut relative = "configs/benchmarks/width_via_load_full/fattree_k32_load_90.toml".to_owned();
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
    assert_eq!(
        samples, 4,
        "the T15e protocol requires exactly four samples, two per order"
    );
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let config = MetalConfig {
        streams_enabled: false,
        round_threads_per_threadgroup: threadgroup_width,
        ..MetalConfig::default()
    };

    {
        let warm_baseline = baseline(&executor, &image, config);
        let warm_control = control(&executor, &image, config);
        let warm_probe = probe(&executor, &image, config);
        let warm_split = warm_probe
            .decompose_against(&warm_baseline, &warm_control)
            .expect("warm FEL probe, control, and baseline must match");
        println!(
            "record=t15e_drain_initialization config={relative} stream_mode=heap \
             all_diagnostic_pipelines_creation_ns={} cached_probe_pipeline_creation_ns={} \
             matched_round_pso=1 warm_rounds={} warm_transitions={} \
             warm_fel_round_trip_delta_ns={}",
            warm_control.diagnostic_pipeline_creation_ns,
            warm_probe.diagnostic_pipeline_creation_ns,
            warm_split.rounds,
            warm_split.transitions,
            warm_split.fel_round_trip_delta_ns,
        );
    }

    let mut measurements = Vec::with_capacity(samples);
    for index in 0..samples {
        let order = if index.is_multiple_of(2) {
            "production_baseline_control_probe"
        } else {
            "probe_control_baseline_production"
        };
        let (production, baseline, control, probe) = if order == "production_baseline_control_probe"
        {
            (
                after_production_predecessor(&executor, &image, config),
                after_baseline_predecessor(&executor, &image, config),
                after_control_predecessor(&executor, &image, config),
                after_probe_predecessor(&executor, &image, config),
            )
        } else {
            let probe = after_probe_predecessor(&executor, &image, config);
            let control = after_control_predecessor(&executor, &image, config);
            let baseline = after_baseline_predecessor(&executor, &image, config);
            let production = after_production_predecessor(&executor, &image, config);
            (production, baseline, control, probe)
        };
        assert_eq!(
            production.result, baseline.result,
            "ordinary and profiled production runs must match"
        );
        assert_eq!(production.rounds, baseline.rounds);
        assert_eq!(production.transitions, baseline.transitions);
        let measurement = Sample {
            index,
            order,
            decomposition: probe
                .decompose_against(&baseline, &control)
                .expect("FEL baseline, matched control, and stress probe must decompose"),
            production_device_ns: production.device_ns,
            production_wall_ns: production.wall_ns,
            production_profiled_useful_ns: useful_profiled_ns(&baseline),
            production_target_merge_ns: useful_merge_ns(&baseline),
            diagnostic_pipeline_creation_ns: probe
                .diagnostic_pipeline_creation_ns
                .checked_add(control.diagnostic_pipeline_creation_ns)
                .expect("diagnostic pipeline creation total must fit"),
        };
        print_sample(&relative, measurement);
        measurements.push(measurement);
    }

    let rounds = measurements[0].decomposition.rounds;
    let transitions = measurements[0].decomposition.transitions;
    let local_fel_pushes = measurements[0].decomposition.local_fel_pushes;
    let production_operations = measurements[0]
        .decomposition
        .production_drain_fel_operations;
    let injected_operations = measurements[0].decomposition.injected_fel_operations;
    assert!(
        measurements.iter().all(|sample| {
            sample.decomposition.rounds == rounds
                && sample.decomposition.transitions == transitions
                && sample.decomposition.local_fel_pushes == local_fel_pushes
                && sample.decomposition.production_drain_fel_operations == production_operations
                && sample.decomposition.injected_fel_operations == injected_operations
        }),
        "all protocol samples must report identical operation counts"
    );
    let baseline_drain_ns = median_u64(
        measurements
            .iter()
            .map(|sample| sample.decomposition.baseline_drain_execute_ns),
    );
    let control_drain_ns = median_u64(
        measurements
            .iter()
            .map(|sample| sample.decomposition.matched_control_drain_execute_ns),
    );
    let probe_drain_ns = median_u64(
        measurements
            .iter()
            .map(|sample| sample.decomposition.probe_drain_execute_ns),
    );
    let median_delta_ns = median_i128(
        measurements
            .iter()
            .map(|sample| sample.decomposition.fel_round_trip_delta_ns),
    );
    let production_device_ns = median_u64(
        measurements
            .iter()
            .map(|sample| sample.production_device_ns),
    );
    let production_wall_ns =
        median_u64(measurements.iter().map(|sample| sample.production_wall_ns));
    let production_profiled_useful_ns = median_u64(
        measurements
            .iter()
            .map(|sample| sample.production_profiled_useful_ns),
    );
    let stress_estimate_ns =
        stress_scaled_estimate_ns(median_delta_ns, production_operations, injected_operations);
    let median_production_target_merge_ns = median_u64(
        measurements
            .iter()
            .map(|sample| sample.production_target_merge_ns),
    );
    let residual_ns =
        stress_estimate_ns.and_then(|estimate| baseline_drain_ns.checked_sub(estimate));
    let stress_share_ppm = stress_estimate_ns.and_then(|estimate| {
        (production_device_ns != 0).then(|| {
            let scaled = u128::from(estimate)
                .checked_mul(1_000_000)
                .expect("summary FEL share must not overflow")
                / u128::from(production_device_ns);
            u64::try_from(scaled).expect("summary FEL share must fit in u64")
        })
    });
    let decision = match (stress_estimate_ns, production_device_ns) {
        (None, _) => "mechanism_selection_unavailable_nonpositive_stress_delta",
        (Some(_), 0) => "mechanism_selection_unavailable_zero_device_denominator",
        (Some(_), _) => "mechanism_selection_unavailable_stress_probe_nonrepresentative",
    };
    println!(
        "record=t15e_drain_summary statistic=median_all_samples config={relative} stream_mode=heap samples={} \
         samples_per_order={} threadgroup_width={threadgroup_width} rounds={rounds} \
         transitions={transitions} production_drain_execute_ns={baseline_drain_ns} \
         matched_control_drain_execute_ns={control_drain_ns} \
         stress_probe_drain_execute_ns={probe_drain_ns} \
         fel_round_trip_delta_ns={median_delta_ns} \
         production_drain_execute_ms_per_round={:.6} \
         matched_control_drain_execute_ms_per_round={:.6} \
         stress_probe_drain_execute_ms_per_round={:.6} \
         local_fel_pushes={local_fel_pushes} \
         production_drain_fel_operations={production_operations} \
         injected_fel_operations={injected_operations} \
         stress_scaled_fel_estimate_ns={} \
         residual_after_stress_scaled_estimate_ns={} \
         production_device_ns={production_device_ns} production_wall_ns={production_wall_ns} \
         production_profiled_useful_ns={production_profiled_useful_ns} \
         stress_scaled_fel_share_ppm={} production_target_merge_ns={} \
         decision={decision} \
         inference_limit=stress_probe_cannot_indict_or_bound_production_fel",
        measurements.len(),
        measurements.len() / 2,
        per_round_ms(baseline_drain_ns, rounds),
        per_round_ms(control_drain_ns, rounds),
        per_round_ms(probe_drain_ns, rounds),
        optional_number(stress_estimate_ns),
        optional_number(residual_ns),
        optional_number(stress_share_ppm),
        median_production_target_merge_ns,
    );

    let fan_in = after_fan_in_predecessor(&executor, &image, config);
    let fan_in_parity = production(&executor, &image, config);
    assert_eq!(
        fan_in.run.result, fan_in_parity.result,
        "separate fan-in characterization must preserve the production result"
    );
    assert_eq!(fan_in.run.rounds, fan_in_parity.rounds);
    assert_eq!(fan_in.run.transitions, fan_in_parity.transitions);
    drop(fan_in_parity);
    assert_eq!(
        fan_in.diagnostic_pipeline_creation_ns, 0,
        "warm diagnostic pipeline cache must be reused"
    );
    let fan_in_counts = fan_in.fan_in;
    let average_active_fan_in = if fan_in_counts.eventful_target_rounds == 0 {
        "unavailable".to_owned()
    } else {
        format!(
            "{:.6}",
            fan_in_counts.active_producer_target_rounds as f64
                / fan_in_counts.eventful_target_rounds as f64
        )
    };
    let average_events_per_target_round = if fan_in_counts.eventful_target_rounds == 0 {
        "unavailable".to_owned()
    } else {
        format!(
            "{:.6}",
            fan_in_counts.remote_events as f64 / fan_in_counts.eventful_target_rounds as f64
        )
    };
    println!(
        "record=t15e_merge_fan_in_characterization config={relative} stream_mode=heap \
         rounds={} transitions={} eventful_target_rounds={} \
         active_producer_target_rounds={} remote_events={} maximum_active_fan_in={} \
         first_maximum_fan_in_target={} maximum_fan_in_target_count={} \
         average_active_fan_in={} average_events_per_target_round={} \
         production_target_merge_ns={} instrumented_target_merge_ns={} \
         diagnostic_pipeline_creation_ns={} \
         timing_interpretation=instrumented_characterization_only_not_sensitivity",
        fan_in.run.rounds,
        fan_in.run.transitions,
        fan_in_counts.eventful_target_rounds,
        fan_in_counts.active_producer_target_rounds,
        fan_in_counts.remote_events,
        fan_in_counts.maximum_active_fan_in,
        fan_in_counts
            .first_maximum_fan_in_target
            .map_or_else(|| "unavailable".to_owned(), |node| node.0.to_string()),
        fan_in_counts.maximum_fan_in_target_count,
        average_active_fan_in,
        average_events_per_target_round,
        median_production_target_merge_ns,
        useful_merge_ns(&fan_in.run),
        fan_in.diagnostic_pipeline_creation_ns,
    );
}

#[cfg(not(all(feature = "metal-spike", target_vendor = "apple")))]
fn main() {
    eprintln!("t15e_drain_benchmark requires --features metal-spike on an Apple target");
    std::process::exit(2);
}

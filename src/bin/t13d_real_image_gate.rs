#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
mod app {
    use std::collections::{BTreeMap, BTreeSet};
    use std::env;
    use std::error::Error;
    use std::io;
    use std::path::Path;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::metal_spike::{
        RealReplayBenchmarkConfig, RealReplayBenchmarkReport, benchmark_real_replay,
    };
    use days_executor::{
        Backend, CpuConfig, EventKind, RealReplayTrace, ReplayTraceCapture, RoundMetrics,
        RoundMetricsWindow, RunResult, RunSummary, SimulationImage, WindowedRunTotals, run_cpu,
        run_cpu_with_metrics_window, run_scalar_rounds_with_replay_trace,
        run_scalar_rounds_with_windowed_replay_trace, validate,
    };

    const LOWER_CPU_VALIDATION_WORKERS: usize = 4;
    const T13E_MIN_EVENTS: u128 = 10_000_000;
    const T13E_MAX_EVENTS: u128 = 50_000_000;

    type BoxError = Box<dyn Error>;

    fn enforce_t13e_event_budget(total_events: u128) -> Result<(), BoxError> {
        if !(T13E_MIN_EVENTS..=T13E_MAX_EVENTS).contains(&total_events) {
            return Err(input_error(&format!(
                "T13e event budget violation: total_events={total_events} is outside the hard \
                 inclusive range [{T13E_MIN_EVENTS}, {T13E_MAX_EVENTS}]"
            )));
        }
        Ok(())
    }

    pub fn main() -> Result<(), BoxError> {
        match parse_args()? {
            Command::Lower { config } => lower(&config),
            Command::ScalarTrace {
                config,
                start_round,
                warmup_rounds,
                measured_rounds,
            } => scalar_trace(&config, start_round, warmup_rounds, measured_rounds),
            Command::Cpu { config, workers } => cpu(&config, workers),
            Command::Gate {
                config,
                start_round,
                warmup_rounds,
                measured_rounds,
                cpu_worker_counts,
            } => gate(
                &config,
                start_round,
                warmup_rounds,
                measured_rounds,
                &cpu_worker_counts,
            ),
            Command::T13eGate {
                config,
                start_round,
                warmup_rounds,
                measured_rounds,
                cpu_worker_counts,
            } => t13e_gate(
                &config,
                start_round,
                warmup_rounds,
                measured_rounds,
                &cpu_worker_counts,
            ),
        }
    }

    enum Command {
        Lower {
            config: String,
        },
        ScalarTrace {
            config: String,
            start_round: usize,
            warmup_rounds: usize,
            measured_rounds: usize,
        },
        Cpu {
            config: String,
            workers: usize,
        },
        Gate {
            config: String,
            start_round: usize,
            warmup_rounds: usize,
            measured_rounds: usize,
            cpu_worker_counts: Vec<usize>,
        },
        T13eGate {
            config: String,
            start_round: usize,
            warmup_rounds: usize,
            measured_rounds: usize,
            cpu_worker_counts: Vec<usize>,
        },
    }

    fn parse_args() -> Result<Command, BoxError> {
        let mut args = env::args().skip(1);
        let mode = args.next().ok_or_else(usage_error)?;
        let command = match mode.as_str() {
            "lower" => Command::Lower {
                config: required_arg(&mut args, "config")?,
            },
            "scalar-trace" => Command::ScalarTrace {
                config: required_arg(&mut args, "config")?,
                start_round: parse_usize(required_arg(&mut args, "start_round")?, "start_round")?,
                warmup_rounds: parse_usize(
                    required_arg(&mut args, "warmup_rounds")?,
                    "warmup_rounds",
                )?,
                measured_rounds: parse_usize(
                    required_arg(&mut args, "measured_rounds")?,
                    "measured_rounds",
                )?,
            },
            "cpu" => Command::Cpu {
                config: required_arg(&mut args, "config")?,
                workers: parse_usize(required_arg(&mut args, "workers")?, "workers")?,
            },
            "gate" => Command::Gate {
                config: required_arg(&mut args, "config")?,
                start_round: parse_usize(required_arg(&mut args, "start_round")?, "start_round")?,
                warmup_rounds: parse_usize(
                    required_arg(&mut args, "warmup_rounds")?,
                    "warmup_rounds",
                )?,
                measured_rounds: parse_usize(
                    required_arg(&mut args, "measured_rounds")?,
                    "measured_rounds",
                )?,
                cpu_worker_counts: parse_worker_counts(required_arg(
                    &mut args,
                    "cpu_worker_counts",
                )?)?,
            },
            "t13e-gate" => Command::T13eGate {
                config: required_arg(&mut args, "config")?,
                start_round: parse_usize(required_arg(&mut args, "start_round")?, "start_round")?,
                warmup_rounds: parse_usize(
                    required_arg(&mut args, "warmup_rounds")?,
                    "warmup_rounds",
                )?,
                measured_rounds: parse_usize(
                    required_arg(&mut args, "measured_rounds")?,
                    "measured_rounds",
                )?,
                cpu_worker_counts: parse_worker_counts(required_arg(
                    &mut args,
                    "cpu_worker_counts",
                )?)?,
            },
            _ => return Err(usage_error()),
        };
        if args.next().is_some() {
            return Err(usage_error());
        }
        match &command {
            Command::ScalarTrace {
                start_round,
                warmup_rounds,
                measured_rounds,
                ..
            }
            | Command::Gate {
                start_round,
                warmup_rounds,
                measured_rounds,
                ..
            }
            | Command::T13eGate {
                start_round,
                warmup_rounds,
                measured_rounds,
                ..
            } => {
                if *measured_rounds == 0 {
                    return Err(input_error("measured_rounds must be greater than zero"));
                }
                let capture_rounds =
                    warmup_rounds.checked_add(*measured_rounds).ok_or_else(|| {
                        input_error("warmup_rounds + measured_rounds overflows usize")
                    })?;
                start_round
                    .checked_add(capture_rounds)
                    .ok_or_else(|| input_error("the requested capture range overflows usize"))?;
            }
            Command::Cpu { workers: 0, .. } => {
                return Err(input_error("workers must be greater than zero"));
            }
            Command::Lower { .. } | Command::Cpu { .. } => {}
        }
        Ok(command)
    }

    fn usage_error() -> BoxError {
        input_error(
            "usage: t13d_real_image_gate lower CONFIG | \
             scalar-trace CONFIG START_ROUND WARMUP_ROUNDS MEASURED_ROUNDS | \
             cpu CONFIG WORKERS | \
             gate CONFIG START_ROUND WARMUP_ROUNDS MEASURED_ROUNDS CPU_WORKERS_CSV | \
             t13e-gate CONFIG START_ROUND WARMUP_ROUNDS MEASURED_ROUNDS CPU_WORKERS_CSV",
        )
    }

    fn required_arg(
        args: &mut impl Iterator<Item = String>,
        name: &str,
    ) -> Result<String, BoxError> {
        args.next()
            .ok_or_else(|| input_error(&format!("missing {name}\n{}", usage_error())))
    }

    fn parse_usize(value: String, name: &str) -> Result<usize, BoxError> {
        value
            .parse()
            .map_err(|error| input_error(&format!("invalid {name}={value}: {error}")))
    }

    fn parse_worker_counts(value: String) -> Result<Vec<usize>, BoxError> {
        let workers = value
            .split(',')
            .map(|field| parse_usize(field.to_owned(), "cpu_worker_counts"))
            .collect::<Result<Vec<_>, _>>()?;
        if workers.is_empty() || workers.contains(&0) || !workers.contains(&4) {
            return Err(input_error(
                "cpu_worker_counts must be a nonempty comma list containing W4",
            ));
        }
        let mut unique = workers.clone();
        unique.sort_unstable();
        unique.dedup();
        if unique.len() != workers.len() {
            return Err(input_error("cpu_worker_counts must not contain duplicates"));
        }
        Ok(workers)
    }

    fn input_error(message: &str) -> BoxError {
        io::Error::new(io::ErrorKind::InvalidInput, message).into()
    }

    fn lower(config: &str) -> Result<(), BoxError> {
        let (image, lowering_wall_ns) = lower_image(config)?;
        let scalar_validation = Instant::now();
        validate(&image, Backend::Scalar)?;
        let scalar_validation_wall_ns = scalar_validation.elapsed().as_nanos();
        let cpu_validation = Instant::now();
        validate(
            &image,
            Backend::Cpu {
                workers: LOWER_CPU_VALIDATION_WORKERS,
            },
        )?;
        let cpu_validation_wall_ns = cpu_validation.elapsed().as_nanos();

        print_image(config, &image, lowering_wall_ns);
        println!(
            "record=validation config={} scalar=ok cpu=ok cpu_workers={} \
             scalar_wall_ns={} cpu_wall_ns={}",
            display_path(config),
            LOWER_CPU_VALIDATION_WORKERS,
            scalar_validation_wall_ns,
            cpu_validation_wall_ns,
        );
        Ok(())
    }

    fn scalar_trace(
        config: &str,
        start_round: usize,
        warmup_rounds: usize,
        measured_rounds: usize,
    ) -> Result<(), BoxError> {
        let capture_rounds = warmup_rounds
            .checked_add(measured_rounds)
            .ok_or_else(|| input_error("warmup_rounds + measured_rounds overflows usize"))?;
        let (image, lowering_wall_ns) = lower_image(config)?;
        eprintln!("validating scalar image");
        validate(&image, Backend::Scalar)?;
        eprintln!(
            "running scalar image to completion and capturing rounds [{}, {})",
            start_round,
            start_round + capture_rounds,
        );
        let timer = Instant::now();
        let (run, trace) = run_scalar_rounds_with_replay_trace(
            &image,
            None,
            ReplayTraceCapture {
                start_round,
                rounds: capture_rounds,
            },
        )?;
        let wall_ns = timer.elapsed().as_nanos();

        trace.validate()?;
        validate_capture(&trace, start_round, capture_rounds)?;
        let stats = summarize_rounds(run.rounds.iter())?;

        print_image(config, &image, lowering_wall_ns);
        print_run(config, "scalar_trace", 1, wall_ns, &stats, None);
        print_result("scalar_trace", 1, &run.result);
        print_trace(&trace, start_round, warmup_rounds, measured_rounds)?;
        Ok(())
    }

    fn cpu(config: &str, workers: usize) -> Result<(), BoxError> {
        let (image, lowering_wall_ns) = lower_image(config)?;
        eprintln!("validating CPU image for workers={workers}");
        validate(&image, Backend::Cpu { workers })?;
        eprintln!("running CPU image to completion with workers={workers}");
        let timer = Instant::now();
        let run = run_cpu(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
        )?;
        let wall_ns = timer.elapsed().as_nanos();
        let stats = summarize_rounds(run.rounds.iter().map(|round| &round.semantic))?;
        let mean_round_wall_ns = mean_u64(run.rounds.iter().map(|round| round.round_wall_time_ns))?;

        print_image(config, &image, lowering_wall_ns);
        print_run(
            config,
            "cpu",
            workers,
            wall_ns,
            &stats,
            Some(mean_round_wall_ns),
        );
        print_result("cpu", workers, &run.result);
        Ok(())
    }

    fn gate(
        config: &str,
        start_round: usize,
        warmup_rounds: usize,
        measured_rounds: usize,
        cpu_worker_counts: &[usize],
    ) -> Result<(), BoxError> {
        if warmup_rounds == 0 {
            return Err(input_error("the real-image gate requires warmup rounds"));
        }
        let capture_rounds = warmup_rounds
            .checked_add(measured_rounds)
            .ok_or_else(|| input_error("warmup_rounds + measured_rounds overflows usize"))?;
        let (image, lowering_wall_ns) = lower_image(config)?;
        validate(&image, Backend::Scalar)?;
        for &workers in cpu_worker_counts {
            validate(&image, Backend::Cpu { workers })?;
        }
        eprintln!(
            "recording real rounds [{}, {}) before resident replay",
            start_round,
            start_round + capture_rounds,
        );
        let scalar_timer = Instant::now();
        let (scalar, trace) = run_scalar_rounds_with_replay_trace(
            &image,
            None,
            ReplayTraceCapture {
                start_round,
                rounds: capture_rounds,
            },
        )?;
        let scalar_wall_ns = scalar_timer.elapsed().as_nanos();
        trace.validate()?;
        validate_capture(&trace, start_round, capture_rounds)?;
        let scalar_stats = summarize_rounds(scalar.rounds.iter())?;

        print_image(config, &image, lowering_wall_ns);
        print_run(
            config,
            "scalar_gate_trace",
            1,
            scalar_wall_ns,
            &scalar_stats,
            None,
        );
        print_result("scalar_gate_trace", 1, &scalar.result);
        print_trace(&trace, start_round, warmup_rounds, measured_rounds)?;
        drop(scalar);

        run_resident_gate(
            config,
            trace,
            warmup_rounds,
            measured_rounds,
            cpu_worker_counts,
        )
    }

    fn t13e_gate(
        config: &str,
        start_round: usize,
        warmup_rounds: usize,
        measured_rounds: usize,
        cpu_worker_counts: &[usize],
    ) -> Result<(), BoxError> {
        if warmup_rounds == 0 {
            return Err(input_error(
                "the T13e real-image gate requires warmup rounds",
            ));
        }
        let capture_rounds = warmup_rounds
            .checked_add(measured_rounds)
            .ok_or_else(|| input_error("warmup_rounds + measured_rounds overflows usize"))?;
        let window = RoundMetricsWindow {
            start_round,
            rounds: capture_rounds,
        };
        let (image, lowering_wall_ns) = lower_image(config)?;
        validate(&image, Backend::Scalar)?;
        for &workers in cpu_worker_counts {
            validate(&image, Backend::Cpu { workers })?;
        }

        eprintln!(
            "running full scalar path with retained rounds [{}, {})",
            start_round,
            start_round + capture_rounds,
        );
        let scalar_timer = Instant::now();
        let (scalar, trace) = run_scalar_rounds_with_windowed_replay_trace(&image, None, window)?;
        let scalar_wall_ns = scalar_timer.elapsed().as_nanos();
        trace.validate()?;
        validate_capture(&trace, start_round, capture_rounds)?;
        let source_activity =
            validate_measured_source_activity(&trace, warmup_rounds, measured_rounds)?;
        let scalar_stats = summarize_rounds(scalar.rounds.iter())?;

        print_image(config, &image, lowering_wall_ns);
        print_run(
            config,
            "t13e_scalar_gate_trace",
            1,
            scalar_wall_ns,
            &scalar_stats,
            None,
        );
        print_window_totals(
            config,
            "t13e_scalar_gate_trace",
            1,
            scalar.rounds.len(),
            &scalar.totals,
        );
        print_result("t13e_scalar_gate_trace", 1, &scalar.result);
        print_trace(&trace, start_round, warmup_rounds, measured_rounds)?;
        println!(
            "record=t13e_source_activity config={} measured_rounds={} \
             source_active_rounds={} minimum_packet_arrivals_per_round={} \
             total_packet_arrivals={}",
            display_path(config),
            measured_rounds,
            source_activity.active_rounds,
            source_activity.minimum_packet_arrivals_per_round,
            source_activity.total_packet_arrivals,
        );

        eprintln!("running full W4 CPU path with the same retained window");
        let cpu_timer = Instant::now();
        let cpu = run_cpu_with_metrics_window(
            &image,
            None,
            CpuConfig {
                workers: 4,
                ..CpuConfig::default()
            },
            window,
        )?;
        let cpu_wall_ns = cpu_timer.elapsed().as_nanos();
        let cpu_stats = summarize_rounds(cpu.rounds.iter().map(|round| &round.semantic))?;
        let mean_round_wall_ns = mean_u64(cpu.rounds.iter().map(|round| round.round_wall_time_ns))?;
        print_run(
            config,
            "t13e_cpu",
            4,
            cpu_wall_ns,
            &cpu_stats,
            Some(mean_round_wall_ns),
        );
        print_window_totals(config, "t13e_cpu", 4, cpu.rounds.len(), &cpu.totals);
        print_result("t13e_cpu", 4, &cpu.result);

        if scalar.result != cpu.result {
            return Err(input_error(
                "T13e full scalar and W4 CPU results differ for the same image",
            ));
        }
        if scalar.totals != cpu.totals {
            return Err(input_error(
                "T13e full scalar and W4 CPU round/event totals differ",
            ));
        }
        let total_events = scalar.totals.whole_run.events_processed;
        let event_budget_status = if (T13E_MIN_EVENTS..=T13E_MAX_EVENTS).contains(&total_events) {
            "pass"
        } else {
            "fail"
        };
        println!(
            "record=t13e_budget config={} contract=hard_event_budget min_events={} \
             max_events={} total_events={} event_budget_status={} scalar_wall_ns={} \
             cpu_wall_ns={} wall_budget_status=reported_not_enforced",
            display_path(config),
            T13E_MIN_EVENTS,
            T13E_MAX_EVENTS,
            total_events,
            event_budget_status,
            scalar_wall_ns,
            cpu_wall_ns,
        );
        enforce_t13e_event_budget(total_events)?;
        println!(
            "record=t13e_path_equality config={} scalar_mode=t13e_scalar_gate_trace \
             cpu_mode=t13e_cpu cpu_workers=4 result_equal=true totals_equal=true \
             scalar_wall_ns={} cpu_wall_ns={}",
            display_path(config),
            scalar_wall_ns,
            cpu_wall_ns,
        );
        drop(cpu);
        drop(scalar);
        drop(image);

        run_resident_gate(
            config,
            trace,
            warmup_rounds,
            measured_rounds,
            cpu_worker_counts,
        )
    }

    struct SourceActivity {
        active_rounds: usize,
        minimum_packet_arrivals_per_round: u64,
        total_packet_arrivals: u128,
    }

    fn validate_measured_source_activity(
        trace: &RealReplayTrace,
        warmup_rounds: usize,
        measured_rounds: usize,
    ) -> Result<SourceActivity, BoxError> {
        let measured_end = warmup_rounds
            .checked_add(measured_rounds)
            .ok_or_else(|| input_error("measured source-activity window overflows usize"))?;
        let rounds = trace
            .rounds
            .get(warmup_rounds..measured_end)
            .ok_or_else(|| input_error("measured source-activity window exceeds trace"))?;
        let packet_arrivals = trace.event_counts_by_round(EventKind::PacketArrival)?;
        let packet_arrivals = packet_arrivals
            .get(warmup_rounds..measured_end)
            .ok_or_else(|| input_error("measured source-activity counts exceed trace"))?;
        let mut active_rounds = 0;
        let mut minimum_packet_arrivals_per_round = u64::MAX;
        let mut total_packet_arrivals = 0_u128;

        for (round, &packet_arrivals) in rounds.iter().zip(packet_arrivals) {
            if packet_arrivals == 0 {
                return Err(input_error(&format!(
                    "T13e measured source round {} has no PacketArrival events",
                    round.source_round
                )));
            }
            active_rounds += 1;
            minimum_packet_arrivals_per_round =
                minimum_packet_arrivals_per_round.min(packet_arrivals);
            total_packet_arrivals = total_packet_arrivals
                .checked_add(u128::from(packet_arrivals))
                .ok_or_else(|| input_error("measured source-activity total overflows u128"))?;
        }

        Ok(SourceActivity {
            active_rounds,
            minimum_packet_arrivals_per_round,
            total_packet_arrivals,
        })
    }

    fn run_resident_gate(
        config: &str,
        trace: RealReplayTrace,
        warmup_rounds: usize,
        measured_rounds: usize,
        cpu_worker_counts: &[usize],
    ) -> Result<(), BoxError> {
        let warmup_indices = (0..warmup_rounds).collect::<Vec<_>>();
        let measured_indices = (warmup_rounds..warmup_rounds + measured_rounds).collect::<Vec<_>>();
        let warmup = trace.selected_rounds(&warmup_indices)?;
        let measured = trace.selected_rounds(&measured_indices)?;
        eprintln!(
            "running resident gate: warmup_rounds={} measured_rounds={} workers={:?}",
            warmup_rounds, measured_rounds, cpu_worker_counts,
        );
        let report = benchmark_real_replay(
            &warmup,
            &measured,
            RealReplayBenchmarkConfig {
                samples: 3,
                rounds_per_encoding: 16_384,
                cpu_worker_counts: cpu_worker_counts.to_vec(),
            },
        )?;
        print_gate_report(config, "main", &report)?;

        let mut skew_buckets = BTreeMap::<u32, Vec<usize>>::new();
        for (index, round) in measured.rounds.iter().enumerate() {
            skew_buckets
                .entry(round.maximum_events_per_lp)
                .or_default()
                .push(index);
        }
        for (maximum_events_per_lp, indices) in skew_buckets {
            if indices.len() < 32 {
                println!(
                    "record=skew_bucket config={} max_lp_events={} rounds={} status=skipped \
                     reason=fewer_than_32_rounds",
                    display_path(config),
                    maximum_events_per_lp,
                    indices.len(),
                );
                continue;
            }
            let bucket = measured.selected_rounds(&indices)?;
            let bucket_warmup_count = bucket.rounds.len().min(256);
            let bucket_warmup =
                bucket.selected_rounds(&(0..bucket_warmup_count).collect::<Vec<_>>())?;
            eprintln!(
                "running skew bucket max_lp_events={} rounds={}",
                maximum_events_per_lp,
                bucket.rounds.len(),
            );
            let bucket_report = benchmark_real_replay(
                &bucket_warmup,
                &bucket,
                RealReplayBenchmarkConfig {
                    samples: 3,
                    rounds_per_encoding: 16_384,
                    cpu_worker_counts: vec![4],
                },
            )?;
            print_gate_report(
                config,
                &format!("skew_max_{maximum_events_per_lp}"),
                &bucket_report,
            )?;
        }
        Ok(())
    }

    fn print_gate_report(
        config: &str,
        label: &str,
        report: &RealReplayBenchmarkReport,
    ) -> Result<(), BoxError> {
        for (sample_index, sample) in report.samples.iter().enumerate() {
            println!(
                "record=gate_gpu_sample config={} label={} sample={} rounds={} \
                 host_encode_submit_ns={} device_ns={} gpu_wall_ns={} gpu_checksum={}",
                display_path(config),
                label,
                sample_index,
                report.rounds,
                sample.host_encode_submit_ns,
                sample.device_ns,
                sample.gpu_wall_ns,
                sample.gpu_checksum,
            );
            for (worker_index, workers) in report.cpu_worker_counts.iter().copied().enumerate() {
                println!(
                    "record=gate_cpu_sample config={} label={} sample={} rounds={} workers={} \
                     cpu_ns={} cpu_checksum={}",
                    display_path(config),
                    label,
                    sample_index,
                    report.rounds,
                    workers,
                    sample.cpu_ns[worker_index],
                    sample.cpu_checksums[worker_index],
                );
            }
        }
        let gpu_wall = report.median_gpu_wall_ns_per_round();
        let mut cpu_medians = report
            .cpu_worker_counts
            .iter()
            .copied()
            .map(|workers| {
                report
                    .median_cpu_ns_per_round(workers)
                    .map(|median| (workers, median))
                    .ok_or_else(|| input_error("reported CPU worker is missing a median"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        cpu_medians.sort_by(|left, right| left.1.total_cmp(&right.1));
        let (fairest_workers, fairest_cpu) = cpu_medians[0];
        let w4_cpu = report
            .median_cpu_ns_per_round(4)
            .ok_or_else(|| input_error("gate report is missing required W4"))?;
        println!(
            "record=gate_summary config={} label={} substrate={} warmup_rounds={} rounds={} \
             transitions={} lp_round_records={} min_active_lps={} max_active_lps={} \
             mean_active_lps={:.9} mean_parallel_efficiency={:.9} \
             mean_achievable_ceiling={:.9} max_lp_events={} padded_lanes={} body_threadgroups={} \
             reduction_dispatches_per_round={} dispatches_per_round={} rounds_per_encoding={} \
             pipeline_setup_ns={} median_host_ns_per_round={:.9} \
             median_device_ns_per_round={:.9} median_gpu_wall_ns_per_round={:.9} \
             w4_cpu_ns_per_round={:.9} gpu_wall_over_w4={:.9} fairest_workers={} \
             fairest_cpu_ns_per_round={:.9} gpu_wall_over_fairest={:.9} \
             matched_checksums={} no_host_sync_between_rounds={} \
             resident_parent_stream_bytes={} local_fel_fused_bytes={} \
             remote_outbox_fused_bytes={} trace_consistent_horizon_dependency={} \
             variable_active_lp_guard={}",
            display_path(config),
            label,
            report.substrate.replace(' ', "_"),
            report.warmup_rounds,
            report.rounds,
            report.profile.transitions,
            report.profile.lp_round_records,
            report.profile.minimum_active_lps,
            report.profile.maximum_active_lps,
            report.profile.mean_active_lps,
            report.profile.mean_parallel_efficiency,
            report.profile.mean_achievable_speedup_ceiling,
            report.profile.maximum_events_per_lp,
            report.padded_lanes,
            report.body_threadgroups,
            report.reduction_dispatches_per_round,
            report.dispatches_per_round,
            report.rounds_per_encoding,
            report.pipeline_setup_ns,
            report.median_host_encode_submit_ns_per_round(),
            report.median_device_ns_per_round(),
            gpu_wall,
            w4_cpu,
            gpu_wall / w4_cpu,
            fairest_workers,
            fairest_cpu,
            gpu_wall / fairest_cpu,
            report.matched_checksums,
            report.no_host_sync_between_rounds,
            report.resident_parent_stream_bytes,
            report.local_fel_fused_bytes,
            report.remote_outbox_fused_bytes,
            report.trace_consistent_horizon_dependency,
            report.variable_active_lp_guard,
        );
        println!(
            "record=gate_event_mix config={} label={} packet_arrival={} tx_ready={} \
             tx_complete={} remote_arrival={} direct_continuations={} local_fel_pushes={} \
             remote_outbox_writes={} tx_ready_queue_depth_sum={} \
             tx_ready_queue_depth_max={} tx_ready_empty_checks={}",
            display_path(config),
            label,
            report.profile.event_kind_counts[0],
            report.profile.event_kind_counts[1],
            report.profile.event_kind_counts[2],
            report.profile.event_kind_counts[3],
            report.profile.direct_continuations,
            report.profile.local_fel_pushes,
            report.profile.remote_outbox_writes,
            report.profile.tx_ready_queue_depth_sum,
            report.profile.tx_ready_queue_depth_max,
            report.profile.tx_ready_empty_checks,
        );
        Ok(())
    }

    fn lower_image(config: &str) -> Result<(SimulationImage, u128), BoxError> {
        eprintln!("lowering {}", display_path(config));
        let timer = Instant::now();
        let image = compile_config(Path::new(config))?;
        Ok((image, timer.elapsed().as_nanos()))
    }

    fn display_path(path: &str) -> String {
        path.replace('\\', "\\\\")
            .replace(' ', "\\ ")
            .replace('\n', "\\n")
    }

    fn print_image(config: &str, image: &SimulationImage, lowering_wall_ns: u128) {
        let route_source_lps = image
            .flows
            .iter()
            .flat_map(|flow| &flow.route)
            .filter_map(|link_id| {
                usize::try_from(link_id.0)
                    .ok()
                    .and_then(|slot| image.links.get(slot))
                    .filter(|link| link.id == *link_id)
                    .map(|link| link.source)
            })
            .collect::<BTreeSet<_>>()
            .len();
        let route_exposed_lps = image
            .flows
            .iter()
            .flat_map(|flow| {
                flow.route
                    .iter()
                    .filter_map(|link_id| {
                        usize::try_from(link_id.0)
                            .ok()
                            .and_then(|slot| image.links.get(slot))
                            .filter(|link| link.id == *link_id)
                            .map(|link| link.source)
                    })
                    .chain(std::iter::once(flow.target))
            })
            .collect::<BTreeSet<_>>()
            .len();
        println!(
            "record=image config={} lowering_wall_ns={} stop_time_ns={} seed={} nodes={} \
             host_states={} switch_states={} flows={} initial_packets={} links={} channels={} \
             initial_events={} route_source_lps={} route_exposed_lps={}",
            display_path(config),
            lowering_wall_ns,
            image.stop_time_ns,
            image.seed,
            image.nodes.len(),
            image.host_states.len(),
            image.switch_states.len(),
            image.flows.len(),
            image.initial_packets.len(),
            image.links.len(),
            image.channels.len(),
            image.initial_events.len(),
            route_source_lps,
            route_exposed_lps,
        );
    }

    #[derive(Default)]
    struct RoundStats {
        rounds: usize,
        events: u128,
        active_lps: u128,
        messages_exchanged: u128,
        same_time_continuations: u128,
        physical_lp_probes: u128,
        maximum_events_per_round: u64,
        maximum_active_lps: usize,
        maximum_events_per_lp: u64,
        efficiency_sum: f64,
        ceiling_sum: f64,
        maximum_ceiling: f64,
    }

    impl RoundStats {
        fn observe(&mut self, round: &RoundMetrics) -> Result<(), BoxError> {
            self.rounds = self
                .rounds
                .checked_add(1)
                .ok_or_else(|| input_error("round count overflow"))?;
            checked_add(
                &mut self.events,
                u128::from(round.events_processed),
                "events",
            )?;
            checked_add(
                &mut self.active_lps,
                round.active_lp_count as u128,
                "active LPs",
            )?;
            checked_add(
                &mut self.messages_exchanged,
                u128::from(round.messages_exchanged),
                "messages exchanged",
            )?;
            checked_add(
                &mut self.physical_lp_probes,
                u128::from(round.physical_lp_probes),
                "physical LP probes",
            )?;
            let maximum_events_per_lp = round
                .lp_work
                .iter()
                .map(|work| work.events_processed)
                .max()
                .unwrap_or(0);
            for work in &round.lp_work {
                checked_add(
                    &mut self.same_time_continuations,
                    u128::from(work.same_time_continuations),
                    "same-time continuations",
                )?;
            }
            let ceiling = if maximum_events_per_lp == 0 {
                0.0
            } else {
                round.events_processed as f64 / maximum_events_per_lp as f64
            };
            self.maximum_events_per_round =
                self.maximum_events_per_round.max(round.events_processed);
            self.maximum_active_lps = self.maximum_active_lps.max(round.active_lp_count);
            self.maximum_events_per_lp = self.maximum_events_per_lp.max(maximum_events_per_lp);
            self.efficiency_sum += round.parallel_efficiency;
            self.ceiling_sum += ceiling;
            self.maximum_ceiling = self.maximum_ceiling.max(ceiling);
            if !self.efficiency_sum.is_finite() || !self.ceiling_sum.is_finite() {
                return Err(input_error("non-finite round statistic"));
            }
            Ok(())
        }

        fn mean_active_lps(&self) -> f64 {
            self.active_lps as f64 / self.divisor()
        }

        fn mean_efficiency(&self) -> f64 {
            self.efficiency_sum / self.divisor()
        }

        fn mean_ceiling(&self) -> f64 {
            self.ceiling_sum / self.divisor()
        }

        fn divisor(&self) -> f64 {
            self.rounds.max(1) as f64
        }
    }

    fn summarize_rounds<'a>(
        rounds: impl Iterator<Item = &'a RoundMetrics>,
    ) -> Result<RoundStats, BoxError> {
        let mut stats = RoundStats::default();
        for round in rounds {
            stats.observe(round)?;
        }
        Ok(stats)
    }

    fn checked_add(total: &mut u128, value: u128, field: &str) -> Result<(), BoxError> {
        *total = total
            .checked_add(value)
            .ok_or_else(|| input_error(&format!("{field} total overflow")))?;
        Ok(())
    }

    fn mean_u64(values: impl Iterator<Item = u64>) -> Result<f64, BoxError> {
        let mut total = 0_u128;
        let mut count = 0_u128;
        for value in values {
            checked_add(&mut total, u128::from(value), "u64 mean")?;
            checked_add(&mut count, 1, "u64 mean sample count")?;
        }
        Ok(total as f64 / count.max(1) as f64)
    }

    fn print_run(
        config: &str,
        mode: &str,
        workers: usize,
        wall_ns: u128,
        stats: &RoundStats,
        mean_round_wall_ns: Option<f64>,
    ) {
        println!(
            "record=run config={} mode={} workers={} wall_ns={} rounds={} events={} \
             mean_events_per_round={:.6} max_events_per_round={} mean_active_lps={:.6} \
             max_active_lps={} mean_parallel_efficiency={:.9} \
             mean_achievable_ceiling={:.6} max_achievable_ceiling={:.6} \
             max_lp_events={} messages_exchanged={} same_time_continuations={} \
             physical_lp_probes={} mean_cpu_instrumented_round_wall_ns={}",
            display_path(config),
            mode,
            workers,
            wall_ns,
            stats.rounds,
            stats.events,
            stats.events as f64 / stats.divisor(),
            stats.maximum_events_per_round,
            stats.mean_active_lps(),
            stats.maximum_active_lps,
            stats.mean_efficiency(),
            stats.mean_ceiling(),
            stats.maximum_ceiling,
            stats.maximum_events_per_lp,
            stats.messages_exchanged,
            stats.same_time_continuations,
            stats.physical_lp_probes,
            mean_round_wall_ns.map_or_else(|| "na".to_owned(), |value| format!("{value:.6}")),
        );
    }

    fn print_window_totals(
        config: &str,
        mode: &str,
        workers: usize,
        retained_metrics: usize,
        totals: &WindowedRunTotals,
    ) {
        println!(
            "record=window_totals config={} mode={} workers={} total_rounds={} total_events={} \
             whole_mean_active_lps={:.9} whole_max_active_lps={} retained_metrics={} \
             retained_run_record_scope=retained_window ramp_rounds={} ramp_events={} \
             ramp_mean_active_lps={:.9} ramp_max_active_lps={} retained_rounds={} \
             retained_events={} retained_mean_active_lps={:.9} retained_max_active_lps={} \
             drain_rounds={} drain_events={} drain_mean_active_lps={:.9} \
             drain_max_active_lps={}",
            display_path(config),
            mode,
            workers,
            totals.whole_run.rounds,
            totals.whole_run.events_processed,
            totals.whole_run.active_lp_rounds as f64 / totals.whole_run.rounds.max(1) as f64,
            totals.whole_run.maximum_active_lps,
            retained_metrics,
            totals.before_window.rounds,
            totals.before_window.events_processed,
            totals.before_window.active_lp_rounds as f64
                / totals.before_window.rounds.max(1) as f64,
            totals.before_window.maximum_active_lps,
            totals.retained_window.rounds,
            totals.retained_window.events_processed,
            totals.retained_window.active_lp_rounds as f64
                / totals.retained_window.rounds.max(1) as f64,
            totals.retained_window.maximum_active_lps,
            totals.after_window.rounds,
            totals.after_window.events_processed,
            totals.after_window.active_lp_rounds as f64 / totals.after_window.rounds.max(1) as f64,
            totals.after_window.maximum_active_lps,
        );
    }

    fn print_result(mode: &str, workers: usize, result: &RunResult) {
        let debug = format!("{result:?}");
        let debug_fnv64 = debug.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
        print_summary(mode, workers, result.summary);
        println!(
            "record=result_state mode={} workers={} host_states={} switch_states={} \
             resident_packets={} observed_packets={} departures={} arrivals={} pending_events={} \
             debug_bytes={} debug_fnv64={}",
            mode,
            workers,
            result.host_states.len(),
            result.switch_states.len(),
            result.resident_packets.len(),
            result.observed_packets.len(),
            result.departures.len(),
            result.arrivals.len(),
            result.pending_events.len(),
            debug.len(),
            debug_fnv64,
        );
    }

    fn print_summary(mode: &str, workers: usize, summary: RunSummary) {
        println!(
            "record=result_summary mode={} workers={} sourced_packets={} sourced_bytes={} \
             departed_packets={} departed_bytes={} admitted_packets={} admitted_bytes={} \
             received_packets={} received_bytes={} dropped_packets={} dropped_bytes={} \
             feedback_packets={} feedback_bytes={}",
            mode,
            workers,
            summary.sourced_packets,
            summary.sourced_bytes,
            summary.departed_packets,
            summary.departed_bytes,
            summary.admitted_packets,
            summary.admitted_bytes,
            summary.received_packets,
            summary.received_bytes,
            summary.dropped_packets,
            summary.dropped_bytes,
            summary.feedback_packets,
            summary.feedback_bytes,
        );
    }

    fn validate_capture(
        trace: &RealReplayTrace,
        start_round: usize,
        capture_rounds: usize,
    ) -> Result<(), BoxError> {
        if trace.rounds.len() != capture_rounds {
            return Err(input_error(&format!(
                "requested {capture_rounds} trace rounds at {start_round}, captured {}; \
                 the image completed before the requested window ended",
                trace.rounds.len(),
            )));
        }
        for (offset, round) in trace.rounds.iter().enumerate() {
            let expected = start_round
                .checked_add(offset)
                .ok_or_else(|| input_error("trace source round overflow"))?;
            if round.source_round != expected {
                return Err(input_error("captured trace rounds are not contiguous"));
            }
        }
        Ok(())
    }

    #[derive(Default)]
    struct TraceStats {
        events_by_kind: [u128; 4],
        direct_continuations: u128,
        local_fel_pushes: u128,
        remote_outbox_writes: u128,
        occupancy_samples: u128,
        occupancy_total: u128,
        occupancy_maximum: u16,
        pending_total: u128,
        pending_maximum: u32,
        active_lps: u128,
        events: u128,
        efficiency_sum: f64,
        ceiling_sum: f64,
        maximum_events_per_lp: u32,
    }

    fn print_trace(
        trace: &RealReplayTrace,
        start_round: usize,
        warmup_rounds: usize,
        measured_rounds: usize,
    ) -> Result<(), BoxError> {
        let mut stats = TraceStats::default();
        for (offset, round) in trace.rounds.iter().enumerate() {
            let rows = &trace.lps[round.lp_start..round.lp_start + round.lp_count];
            let phase = if offset < warmup_rounds {
                "warmup"
            } else {
                "measured"
            };
            let mean_lp_events = if round.active_lp_count == 0 {
                0.0
            } else {
                round.events_processed as f64 / round.active_lp_count as f64
            };
            let achievable_ceiling = if round.maximum_events_per_lp == 0 {
                0.0
            } else {
                round.events_processed as f64 / f64::from(round.maximum_events_per_lp)
            };
            let maximum_to_mean_skew = if mean_lp_events == 0.0 {
                0.0
            } else {
                f64::from(round.maximum_events_per_lp) / mean_lp_events
            };
            let pending_maximum = rows
                .iter()
                .map(|row| row.pending_events_below_horizon)
                .max()
                .unwrap_or(0);
            let pending_total = rows.iter().try_fold(0_u128, |total, row| {
                total
                    .checked_add(u128::from(row.pending_events_below_horizon))
                    .ok_or_else(|| input_error("per-round pending-event total overflow"))
            })?;
            println!(
                "record=trace_round capture_offset={} phase={} source_round={} frontier_ns={} \
                 exclusive_horizon_ns={} events={} active_lps={} max_lp_events={} \
                 mean_lp_events={:.9} parallel_efficiency={:.9} \
                 achievable_ceiling={:.9} max_to_mean_skew={:.9} \
                 pending_events_below_horizon={} max_pending_events_below_horizon={}",
                offset,
                phase,
                round.source_round,
                round.frontier_ns,
                round.exclusive_horizon_ns,
                round.events_processed,
                round.active_lp_count,
                round.maximum_events_per_lp,
                mean_lp_events,
                round.parallel_efficiency,
                achievable_ceiling,
                maximum_to_mean_skew,
                pending_total,
                pending_maximum,
            );
            checked_add(
                &mut stats.active_lps,
                round.active_lp_count as u128,
                "trace active LPs",
            )?;
            checked_add(
                &mut stats.events,
                u128::from(round.events_processed),
                "trace events",
            )?;
            stats.efficiency_sum += round.parallel_efficiency;
            stats.ceiling_sum += achievable_ceiling;
            stats.maximum_events_per_lp =
                stats.maximum_events_per_lp.max(round.maximum_events_per_lp);
        }

        for row in &trace.lps {
            checked_add(
                &mut stats.pending_total,
                u128::from(row.pending_events_below_horizon),
                "trace pending events",
            )?;
            stats.pending_maximum = stats.pending_maximum.max(row.pending_events_below_horizon);
        }
        for step in &trace.steps {
            let kind = match step.kind() {
                EventKind::PacketArrival => 0,
                EventKind::TxReady => 1,
                EventKind::TxComplete => 2,
                EventKind::RemoteArrival => 3,
            };
            checked_add(&mut stats.events_by_kind[kind], 1, "event-kind count")?;
            if step.is_direct_continuation() {
                checked_add(
                    &mut stats.direct_continuations,
                    1,
                    "trace direct continuations",
                )?;
            }
            checked_add(
                &mut stats.local_fel_pushes,
                u128::from(step.local_fel_pushes()),
                "trace local FEL pushes",
            )?;
            checked_add(
                &mut stats.remote_outbox_writes,
                u128::from(step.remote_outbox_writes()),
                "trace remote outbox writes",
            )?;
            if let Some(occupancy) = step.queue_occupancy() {
                checked_add(&mut stats.occupancy_samples, 1, "queue occupancy samples")?;
                checked_add(
                    &mut stats.occupancy_total,
                    u128::from(occupancy),
                    "queue occupancy total",
                )?;
                stats.occupancy_maximum = stats.occupancy_maximum.max(occupancy);
            }
        }

        let round_divisor = trace.rounds.len().max(1) as f64;
        let lp_divisor = trace.lps.len().max(1) as f64;
        let occupancy_divisor = stats.occupancy_samples.max(1) as f64;
        println!(
            "record=trace start_round={} warmup_rounds={} measured_rounds={} \
             source_rounds={} captured_rounds={} lps={} steps={} events={} \
             mean_active_lps={:.9} mean_parallel_efficiency={:.9} \
             mean_achievable_ceiling={:.9} max_lp_events={} \
             mean_pending_events_below_horizon={:.9} \
             max_pending_events_below_horizon={}",
            start_round,
            warmup_rounds,
            measured_rounds,
            trace.source_round_count,
            trace.rounds.len(),
            trace.lps.len(),
            trace.steps.len(),
            stats.events,
            stats.active_lps as f64 / round_divisor,
            stats.efficiency_sum / round_divisor,
            stats.ceiling_sum / round_divisor,
            stats.maximum_events_per_lp,
            stats.pending_total as f64 / lp_divisor,
            stats.pending_maximum,
        );
        println!(
            "record=trace_event_mix packet_arrival={} tx_ready={} tx_complete={} \
             remote_arrival={} direct_continuations={} local_fel_pushes={} \
             remote_outbox_writes={}",
            stats.events_by_kind[0],
            stats.events_by_kind[1],
            stats.events_by_kind[2],
            stats.events_by_kind[3],
            stats.direct_continuations,
            stats.local_fel_pushes,
            stats.remote_outbox_writes,
        );
        println!(
            "record=trace_queue_occupancy samples={} mean={:.9} maximum={}",
            stats.occupancy_samples,
            stats.occupancy_total as f64 / occupancy_divisor,
            stats.occupancy_maximum,
        );
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn t13e_event_budget_accepts_inclusive_bounds() {
            enforce_t13e_event_budget(T13E_MIN_EVENTS).unwrap();
            enforce_t13e_event_budget(T13E_MAX_EVENTS).unwrap();
        }

        #[test]
        fn t13e_event_budget_rejects_out_of_range_counts() {
            let below = enforce_t13e_event_budget(T13E_MIN_EVENTS - 1)
                .expect_err("an event count below the T13e budget must fail")
                .to_string();
            let above = enforce_t13e_event_budget(T13E_MAX_EVENTS + 1)
                .expect_err("an event count above the T13e budget must fail")
                .to_string();

            assert!(below.contains("[10000000, 50000000]"));
            assert!(above.contains("[10000000, 50000000]"));
        }
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::main()
}

#[cfg(not(all(feature = "metal-spike", target_vendor = "apple")))]
fn main() {
    eprintln!("t13d_real_image_gate requires --features metal-spike on an Apple target");
    std::process::exit(2);
}

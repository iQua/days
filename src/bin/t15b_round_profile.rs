#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn main() {
    use std::path::PathBuf;

    use days::scenario::compile_config;
    use days_executor::{
        MetalConfig, MetalExecutor, MetalPhaseProfile, MetalPhaseTimings, NodeKind, RunSummary,
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

    fn metal_outcome(run: &days_executor::MetalRun) -> Outcome {
        Outcome {
            rounds: run.rounds,
            transitions: run.transitions,
            summary: run.result.summary,
            resident_packets: run.result.resident_packets.len(),
            pending_events: run.result.pending_events.len(),
        }
    }

    fn phases(timing: MetalPhaseTimings) -> [(&'static str, u64); 9] {
        [
            ("horizon", timing.horizon_ns),
            ("reset", timing.round_reset_ns),
            ("prepare", timing.round_prepare_ns),
            ("drain_execute", timing.drain_execute_ns),
            ("continuation_control", timing.continuation_control_ns),
            ("exchange_prefix", timing.exchange_prefix_ns),
            ("exchange_scatter", timing.exchange_scatter_ns),
            ("target_merge", timing.target_merge_ns),
            ("final_control", timing.final_control_ns),
        ]
    }

    fn median(mut values: Vec<u64>) -> u64 {
        values.sort_unstable();
        match values.len() {
            0 => 0,
            length if length % 2 == 1 => values[length / 2],
            length => values[length / 2 - 1].saturating_add(values[length / 2]) / 2,
        }
    }

    fn signed_difference(left: u64, right: u64) -> i128 {
        i128::from(left) - i128::from(right)
    }

    fn median_timing(
        profiles: &[MetalPhaseProfile],
        selected: impl Fn(MetalPhaseProfile) -> MetalPhaseTimings,
    ) -> MetalPhaseTimings {
        let rows = profiles
            .iter()
            .copied()
            .map(selected)
            .map(phases)
            .collect::<Vec<_>>();
        let mut values = [0_u64; 9];
        for (index, value) in values.iter_mut().enumerate() {
            *value = median(rows.iter().map(|row| row[index].1).collect());
        }
        MetalPhaseTimings {
            horizon_ns: values[0],
            round_reset_ns: values[1],
            round_prepare_ns: values[2],
            drain_execute_ns: values[3],
            continuation_control_ns: values[4],
            exchange_prefix_ns: values[5],
            exchange_scatter_ns: values[6],
            target_merge_ns: values[7],
            final_control_ns: values[8],
        }
    }

    let mut arguments = std::env::args().skip(1);
    let relative = arguments.next().unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_full/fattree_k32_load_90.toml".into()
    });
    let mut samples = 3_usize;
    let mut max_rounds = None;
    let mut max_transitions_per_lp_per_round = None;
    let mut rounds_per_command_buffer = None;
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
            "--max-rounds" => {
                max_rounds = Some(
                    arguments
                        .next()
                        .expect("--max-rounds requires a value")
                        .parse()
                        .expect("--max-rounds must be an integer"),
                );
            }
            "--max-transitions-per-lp-per-round" => {
                max_transitions_per_lp_per_round = Some(
                    arguments
                        .next()
                        .expect("--max-transitions-per-lp-per-round requires a value")
                        .parse()
                        .expect("--max-transitions-per-lp-per-round must be an integer"),
                );
            }
            "--rounds-per-command-buffer" => {
                rounds_per_command_buffer = Some(
                    arguments
                        .next()
                        .expect("--rounds-per-command-buffer requires a value")
                        .parse()
                        .expect("--rounds-per-command-buffer must be an integer"),
                );
            }
            "--streams-disabled" => streams_enabled = false,
            unknown => panic!("unknown argument {unknown}"),
        }
    }
    assert!(samples > 0, "--samples must be nonzero");
    let run_config = MetalConfig {
        streams_enabled,
        max_rounds,
        max_transitions_per_lp_per_round: max_transitions_per_lp_per_round
            .unwrap_or(MetalConfig::default().max_transitions_per_lp_per_round),
        rounds_per_command_buffer: rounds_per_command_buffer
            .unwrap_or(MetalConfig::default().rounds_per_command_buffer),
        ..MetalConfig::default()
    };
    let stream_mode = if streams_enabled { "streams" } else { "heap" };

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&relative);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let scalar = run_scalar_rounds(&image, None).expect("scalar profile oracle must run");
    let expected = Outcome {
        rounds: scalar.rounds.len() as u64,
        transitions: scalar
            .rounds
            .iter()
            .map(|round| round.events_processed)
            .sum(),
        summary: scalar.result.summary,
        resident_packets: scalar.result.resident_packets.len(),
        pending_events: scalar.result.pending_events.len(),
    };
    let active_lp_rounds = scalar
        .rounds
        .iter()
        .map(|round| round.active_lp_count as u64)
        .sum::<u64>();
    let maximum_active_lps = scalar
        .rounds
        .iter()
        .map(|round| round.active_lp_count)
        .max()
        .unwrap_or(0);
    let maximum_lp_work = scalar
        .rounds
        .iter()
        .flat_map(|round| &round.lp_work)
        .map(|work| work.events_processed)
        .max()
        .unwrap_or(0);
    let efficiency_sum = scalar
        .rounds
        .iter()
        .map(|round| round.parallel_efficiency)
        .sum::<f64>();
    let mut role_events = [0_u64; 2];
    let mut role_lp_rounds = [0_u64; 2];
    let mut direct_continuations = 0_u64;
    let messages = scalar
        .rounds
        .iter()
        .map(|round| round.messages_exchanged)
        .sum::<u64>();
    for work in scalar.rounds.iter().flat_map(|round| &round.lp_work) {
        let role = match image.nodes[work.node.0 as usize].kind {
            NodeKind::Host => 0,
            NodeKind::Switch => 1,
        };
        role_events[role] = role_events[role].saturating_add(work.events_processed);
        role_lp_rounds[role] = role_lp_rounds[role].saturating_add(1);
        direct_continuations = direct_continuations.saturating_add(work.same_time_continuations);
    }
    println!(
        "record=t15b_divergence config={relative} stream_mode={stream_mode} nodes={} rounds={} transitions={} \
         configured_max_rounds={} configured_transition_cap={} \
         active_lp_rounds={active_lp_rounds} mean_active_lps={:.6} \
         maximum_active_lps={maximum_active_lps} maximum_lp_work={maximum_lp_work} \
         mean_parallel_efficiency={:.9} host_events={} switch_events={} \
         host_lp_rounds={} switch_lp_rounds={} direct_continuations={direct_continuations} \
         remote_messages={messages}",
        image.nodes.len(),
        expected.rounds,
        expected.transitions,
        run_config.max_rounds.unwrap_or(0),
        run_config.max_transitions_per_lp_per_round,
        active_lp_rounds as f64 / expected.rounds as f64,
        efficiency_sum / expected.rounds as f64,
        role_events[0],
        role_events[1],
        role_lp_rounds[0],
        role_lp_rounds[1],
    );
    let expected_result = scalar.result;

    let executor = MetalExecutor::new().expect("Metal profile executor must initialize");
    drop(
        executor
            .run_profiled(&image, None, run_config)
            .expect("profile warmup must run"),
    );
    let mut standard_device_ns = Vec::with_capacity(samples);
    let mut profiled_device_ns = Vec::with_capacity(samples);
    let mut profiles = Vec::with_capacity(samples);
    for sample in 0..samples {
        drop(
            executor
                .run(&image, None, run_config)
                .expect("standard predecessor must run"),
        );
        let standard = executor
            .run(&image, None, run_config)
            .expect("standard profile comparison must run");
        assert_eq!(metal_outcome(&standard), expected);
        assert_eq!(standard.result, expected_result);
        if sample == 0 {
            let memory = standard.memory_layout;
            println!(
                "record=t15f_memory config={relative} stream_mode={stream_mode} \
                 legacy_heap_event_slots={} fallback_heap_event_slots={} \
                 checkpoint_fallback_events={} \
                 channel_stream_event_slots={} service_stream_event_slots={} \
                 generator_stream_event_slots={} heap_arena_bytes={} stream_arena_bytes={} \
                 legacy_heap_arena_bytes={} total_event_arena_bytes={} \
                 delta_from_legacy_heap_bytes={}",
                memory.legacy_heap_event_slots,
                memory.fallback_heap_event_slots,
                memory.checkpoint_fallback_events,
                memory.channel_stream_event_slots,
                memory.service_stream_event_slots,
                memory.generator_stream_event_slots,
                memory.heap_arena_bytes,
                memory.stream_arena_bytes,
                memory.legacy_heap_arena_bytes,
                memory.total_event_arena_bytes(),
                memory.delta_from_legacy_heap_bytes(),
            );
        }
        standard_device_ns.push(standard.device_ns);

        drop(
            executor
                .run_profiled(&image, None, run_config)
                .expect("profile predecessor must run"),
        );
        let profiled = executor
            .run_profiled(&image, None, run_config)
            .expect("profile sample must run");
        assert_eq!(metal_outcome(&profiled), expected);
        assert_eq!(profiled.result, expected_result);
        let profile = profiled
            .phase_profile
            .expect("profile sample must contain timestamps");
        if profile.captured_attempts < profile.useful_attempts {
            println!(
                "record=t15b_profile_incomplete config={relative} stream_mode={stream_mode} sample={sample} \
                 rounds={} transitions={} encoded_attempts={} captured_attempts={} useful_attempts={} \
                 captured_prefix_active_ns={} captured_pass_gap_ns={} captured_pass_overlap_ns={} \
                 estimate_complete=0",
                expected.rounds,
                expected.transitions,
                profile.encoded_attempts,
                profile.captured_attempts,
                profile.useful_attempts,
                profile.useful.total_ns(),
                profile.captured_pass_gap_ns,
                profile.captured_pass_overlap_ns,
            );
            return;
        }
        assert!(
            profile.estimate_complete,
            "phase estimate must cover all work"
        );
        let captured_active_ns = profile
            .useful
            .total_ns()
            .saturating_add(profile.termination.total_ns());
        let instrumented_residual_ns = profiled.device_ns.saturating_sub(captured_active_ns);
        println!(
            "record=t15b_profile_sample config={relative} stream_mode={stream_mode} sample={sample} rounds={} \
             transitions={} standard_device_ns={} profiled_device_ns={} frequency_hz={} \
         encoded_attempts={} captured_attempts={} useful_attempts={} idle_sample_attempts={} \
         captured_pass_gap_ns={} captured_pass_overlap_ns={} \
         captured_active_ns={captured_active_ns} \
             instrumented_residual_ns={instrumented_residual_ns} idle_extrapolated_total_ns={}",
            expected.rounds,
            expected.transitions,
            standard.device_ns,
            profiled.device_ns,
            profile.timestamp_frequency_hz,
            profile.encoded_attempts,
            profile.captured_attempts,
            profile.useful_attempts,
            profile.idle_sample_attempts,
            profile.captured_pass_gap_ns,
            profile.captured_pass_overlap_ns,
            profile.estimated_total.total_ns(),
        );
        for (index, (phase, idle_extrapolated_ns)) in
            phases(profile.estimated_total).into_iter().enumerate()
        {
            let useful_ns = phases(profile.useful)
                .into_iter()
                .find(|(candidate, _)| *candidate == phase)
                .expect("phase must exist")
                .1;
            let termination_ns = phases(profile.termination)
                .into_iter()
                .find(|(candidate, _)| *candidate == phase)
                .expect("phase must exist")
                .1;
            let idle_mean_ns = phases(profile.idle_mean)
                .into_iter()
                .find(|(candidate, _)| *candidate == phase)
                .expect("phase must exist")
                .1;
            let captured_active_ns = useful_ns.saturating_add(termination_ns);
            println!(
                "record=t15b_phase_sample config={relative} stream_mode={stream_mode} sample={sample} phase={phase} \
                 captured_active_ns={captured_active_ns} useful_ns={useful_ns} \
                 termination_ns={termination_ns} idle_mean_ns={idle_mean_ns} \
                 idle_extrapolated_ns={idle_extrapolated_ns} phase_index={index}"
            );
        }
        profiled_device_ns.push(profiled.device_ns);
        profiles.push(profile);
    }

    let mut profiled_order = (0..profiles.len()).collect::<Vec<_>>();
    profiled_order.sort_unstable_by_key(|index| profiled_device_ns[*index]);
    let attribution_sample = profiled_order[profiled_order.len() / 2];
    let attribution_profile = profiles[attribution_sample];
    let attribution_standard_ns = standard_device_ns[attribution_sample];
    let attribution_profiled_ns = profiled_device_ns[attribution_sample];
    let attribution_useful_ns = attribution_profile
        .useful
        .total_ns()
        .saturating_add(attribution_profile.termination.total_ns());
    let attribution_all_active_ns = attribution_profile.estimated_total.total_ns();
    let attribution_idle_active_ns =
        attribution_all_active_ns.saturating_sub(attribution_useful_ns);
    let attribution_net_pass_gap_ns = signed_difference(
        attribution_profile.captured_pass_gap_ns,
        attribution_profile.captured_pass_overlap_ns,
    );
    let attribution_envelope_ns = i128::from(attribution_profiled_ns)
        - i128::from(attribution_all_active_ns)
        - attribution_net_pass_gap_ns;
    println!(
        "record=t15d_attribution_summary config={relative} stream_mode={stream_mode} sample={attribution_sample} \
         rounds={} transitions={} captured_all_attempts={} encoded_attempts={} \
         captured_attempts={} useful_attempts={} standard_device_ns={attribution_standard_ns} \
         profiled_device_ns={attribution_profiled_ns} profiling_perturbation_ns={} \
         useful_active_ns={attribution_useful_ns} idle_active_ns={attribution_idle_active_ns} \
         pass_gap_ns={} pass_overlap_ns={} net_pass_gap_ns={attribution_net_pass_gap_ns} \
         envelope_ns={attribution_envelope_ns} instrumented_residual_ns={}",
        expected.rounds,
        expected.transitions,
        u8::from(attribution_profile.encoded_attempts == attribution_profile.captured_attempts),
        attribution_profile.encoded_attempts,
        attribution_profile.captured_attempts,
        attribution_profile.useful_attempts,
        signed_difference(attribution_profiled_ns, attribution_standard_ns),
        attribution_profile.captured_pass_gap_ns,
        attribution_profile.captured_pass_overlap_ns,
        attribution_profiled_ns.saturating_sub(attribution_useful_ns),
    );

    let estimated = median_timing(&profiles, |profile| profile.estimated_total);
    let useful = median_timing(&profiles, |profile| profile.useful);
    let termination = median_timing(&profiles, |profile| profile.termination);
    let idle_mean = median_timing(&profiles, |profile| profile.idle_mean);
    let median_profiled_device_ns = median(profiled_device_ns);
    let captured_active_ns = useful.total_ns().saturating_add(termination.total_ns());
    let instrumented_residual_ns = median_profiled_device_ns.saturating_sub(captured_active_ns);
    println!(
        "record=t15b_profile_summary config={relative} stream_mode={stream_mode} samples={samples} \
         aggregation=component_medians rounds={} transitions={} \
         standard_device_ns={} profiled_device_ns={} frequency_hz={} encoded_attempts={} \
         captured_attempts={} useful_attempts={} idle_sample_attempts={} \
         captured_pass_gap_ns={} captured_pass_overlap_ns={} \
         captured_active_ns={captured_active_ns} \
         instrumented_residual_ns={instrumented_residual_ns} idle_extrapolated_total_ns={}",
        expected.rounds,
        expected.transitions,
        median(standard_device_ns),
        median_profiled_device_ns,
        profiles[0].timestamp_frequency_hz,
        profiles[0].encoded_attempts,
        profiles[0].captured_attempts,
        profiles[0].useful_attempts,
        profiles[0].idle_sample_attempts,
        median(
            profiles
                .iter()
                .map(|profile| profile.captured_pass_gap_ns)
                .collect(),
        ),
        median(
            profiles
                .iter()
                .map(|profile| profile.captured_pass_overlap_ns)
                .collect(),
        ),
        estimated.total_ns(),
    );
    for (index, (phase, idle_extrapolated_ns)) in phases(estimated).into_iter().enumerate() {
        let useful_ns = phases(useful)[index].1;
        let termination_ns = phases(termination)[index].1;
        let captured_active_ns = useful_ns.saturating_add(termination_ns);
        println!(
            "record=t15b_phase_summary config={relative} stream_mode={stream_mode} samples={samples} phase={phase} \
             captured_active_ns={captured_active_ns} useful_ns={useful_ns} \
             termination_ns={termination_ns} idle_mean_ns={} \
             idle_extrapolated_ns={idle_extrapolated_ns}",
            phases(idle_mean)[index].1,
        );
    }
}

#[cfg(not(all(feature = "metal-spike", target_vendor = "apple")))]
fn main() {
    eprintln!("t15b_round_profile requires --features metal-spike on an Apple target");
    std::process::exit(2);
}

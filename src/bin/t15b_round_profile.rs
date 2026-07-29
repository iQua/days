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

    fn phases(timing: MetalPhaseTimings) -> [(&'static str, u64); 8] {
        [
            ("horizon", timing.horizon_ns),
            ("compaction", timing.compaction_ns),
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
        let mut values = [0_u64; 8];
        for (index, value) in values.iter_mut().enumerate() {
            *value = median(rows.iter().map(|row| row[index].1).collect());
        }
        MetalPhaseTimings {
            horizon_ns: values[0],
            compaction_ns: values[1],
            drain_execute_ns: values[2],
            continuation_control_ns: values[3],
            exchange_prefix_ns: values[4],
            exchange_scatter_ns: values[5],
            target_merge_ns: values[6],
            final_control_ns: values[7],
        }
    }

    let mut arguments = std::env::args().skip(1);
    let relative = arguments.next().unwrap_or_else(|| {
        "configs/benchmarks/width_via_load_full/fattree_k32_load_90.toml".into()
    });
    let mut samples = 3_usize;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--samples" => {
                samples = arguments
                    .next()
                    .expect("--samples requires a value")
                    .parse()
                    .expect("--samples must be an integer");
            }
            unknown => panic!("unknown argument {unknown}"),
        }
    }
    assert!(samples > 0, "--samples must be nonzero");

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
        "record=t15b_divergence config={relative} nodes={} rounds={} transitions={} \
         active_lp_rounds={active_lp_rounds} mean_active_lps={:.6} \
         maximum_active_lps={maximum_active_lps} maximum_lp_work={maximum_lp_work} \
         mean_parallel_efficiency={:.9} host_events={} switch_events={} \
         host_lp_rounds={} switch_lp_rounds={} direct_continuations={direct_continuations} \
         remote_messages={messages}",
        image.nodes.len(),
        expected.rounds,
        expected.transitions,
        active_lp_rounds as f64 / expected.rounds as f64,
        efficiency_sum / expected.rounds as f64,
        role_events[0],
        role_events[1],
        role_lp_rounds[0],
        role_lp_rounds[1],
    );
    let expected_result = scalar.result;

    let executor = MetalExecutor::new().expect("Metal profile executor must initialize");
    let profile_config = MetalConfig::default();
    drop(
        executor
            .run_profiled(&image, None, profile_config)
            .expect("profile warmup must run"),
    );
    let mut standard_device_ns = Vec::with_capacity(samples);
    let mut profiled_device_ns = Vec::with_capacity(samples);
    let mut profiles = Vec::with_capacity(samples);
    for sample in 0..samples {
        drop(
            executor
                .run(&image, None, MetalConfig::default())
                .expect("standard predecessor must run"),
        );
        let standard = executor
            .run(&image, None, MetalConfig::default())
            .expect("standard profile comparison must run");
        assert_eq!(metal_outcome(&standard), expected);
        assert_eq!(standard.result, expected_result);
        standard_device_ns.push(standard.device_ns);

        drop(
            executor
                .run_profiled(&image, None, profile_config)
                .expect("profile predecessor must run"),
        );
        let profiled = executor
            .run_profiled(&image, None, profile_config)
            .expect("profile sample must run");
        assert_eq!(metal_outcome(&profiled), expected);
        assert_eq!(profiled.result, expected_result);
        let profile = profiled
            .phase_profile
            .expect("profile sample must contain timestamps");
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
            "record=t15b_profile_sample config={relative} sample={sample} rounds={} \
             transitions={} standard_device_ns={} profiled_device_ns={} frequency_hz={} \
             encoded_attempts={} captured_attempts={} useful_attempts={} idle_sample_attempts={} \
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
                "record=t15b_phase_sample config={relative} sample={sample} phase={phase} \
                 captured_active_ns={captured_active_ns} useful_ns={useful_ns} \
                 termination_ns={termination_ns} idle_mean_ns={idle_mean_ns} \
                 idle_extrapolated_ns={idle_extrapolated_ns} phase_index={index}"
            );
        }
        profiled_device_ns.push(profiled.device_ns);
        profiles.push(profile);
    }

    let estimated = median_timing(&profiles, |profile| profile.estimated_total);
    let useful = median_timing(&profiles, |profile| profile.useful);
    let termination = median_timing(&profiles, |profile| profile.termination);
    let idle_mean = median_timing(&profiles, |profile| profile.idle_mean);
    let median_profiled_device_ns = median(profiled_device_ns);
    let captured_active_ns = useful.total_ns().saturating_add(termination.total_ns());
    let instrumented_residual_ns = median_profiled_device_ns.saturating_sub(captured_active_ns);
    println!(
        "record=t15b_profile_summary config={relative} samples={samples} rounds={} transitions={} \
         standard_device_ns={} profiled_device_ns={} frequency_hz={} encoded_attempts={} \
         captured_attempts={} useful_attempts={} idle_sample_attempts={} \
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
        estimated.total_ns(),
    );
    for (index, (phase, idle_extrapolated_ns)) in phases(estimated).into_iter().enumerate() {
        let useful_ns = phases(useful)[index].1;
        let termination_ns = phases(termination)[index].1;
        let captured_active_ns = useful_ns.saturating_add(termination_ns);
        println!(
            "record=t15b_phase_summary config={relative} samples={samples} phase={phase} \
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

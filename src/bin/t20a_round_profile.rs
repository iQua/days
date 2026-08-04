#[cfg(feature = "p11-profile")]
mod app {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{
        CpuConfig, ObservationMode, P11HorizonPolicy, P11LpProfile, P11RoundProfile, RunResult,
        allocation_profile, record_scoped_allocation, record_scoped_deallocation,
        reset_allocation_profile, run_cpu, run_scalar, run_scalar_rounds_with_p11_horizon_policy,
    };

    pub struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let allocation = unsafe { System.alloc(layout) };
            if !allocation.is_null() {
                record_scoped_allocation(layout.size());
            }
            allocation
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let allocation = unsafe { System.alloc_zeroed(layout) };
            if !allocation.is_null() {
                record_scoped_allocation(layout.size());
            }
            allocation
        }

        unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
            record_scoped_deallocation(layout.size());
            unsafe { System.dealloc(pointer, layout) };
        }

        unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_scoped_deallocation(layout.size());
            let allocation = unsafe { System.realloc(pointer, layout, new_size) };
            if !allocation.is_null() {
                record_scoped_allocation(new_size);
            }
            allocation
        }
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct ProfileSum {
        complete_result_equal: bool,
        end_to_end_ns: u128,
        rounds: u64,
        events: u64,
        active_lps: u64,
        round_wall_ns: u64,
        horizon_ns: u64,
        exchange_merge_ns: u64,
        residual_ns: u64,
        lp: P11LpProfile,
        exchange_targets: u64,
        exchange_fan_in_sum: u64,
        exchange_max_fan_in: u64,
        horizon_bound_min_ns: u128,
        horizon_bound_max_ns: u128,
        horizon_distinct_bounds_max: u64,
    }

    impl ProfileSum {
        fn add_round(&mut self, events: u64, active_lps: usize, profile: P11RoundProfile) {
            self.rounds = self.rounds.saturating_add(1);
            self.events = self.events.saturating_add(events);
            self.active_lps = self.active_lps.saturating_add(active_lps as u64);
            self.round_wall_ns = self.round_wall_ns.saturating_add(profile.round_wall_ns);
            self.horizon_ns = self.horizon_ns.saturating_add(profile.horizon_ns);
            self.exchange_merge_ns = self
                .exchange_merge_ns
                .saturating_add(profile.exchange_merge_ns);
            self.residual_ns = self.residual_ns.saturating_add(profile.residual_ns);
            self.lp = self.lp.saturating_add(profile.lp);
            self.exchange_targets = self
                .exchange_targets
                .saturating_add(profile.exchange_targets);
            self.exchange_fan_in_sum = self
                .exchange_fan_in_sum
                .saturating_add(profile.exchange_fan_in_sum);
            self.exchange_max_fan_in = self.exchange_max_fan_in.max(profile.exchange_max_fan_in);
            if self.rounds == 1 {
                self.horizon_bound_min_ns = profile.horizon_bound_min_ns;
            } else {
                self.horizon_bound_min_ns =
                    self.horizon_bound_min_ns.min(profile.horizon_bound_min_ns);
            }
            self.horizon_bound_max_ns = self.horizon_bound_max_ns.max(profile.horizon_bound_max_ns);
            self.horizon_distinct_bounds_max = self
                .horizon_distinct_bounds_max
                .max(profile.horizon_distinct_bounds);
        }
    }

    fn emit(
        config: &str,
        backend: &str,
        workers: usize,
        sample: usize,
        sum: ProfileSum,
        horizon_policy: &str,
        delay_regime: &str,
    ) {
        let allocation = allocation_profile();
        let rounds = sum.rounds.max(1);
        println!(
            "record=t20a_profile config={config} backend={backend} workers={workers} sample={sample} \
             horizon_policy={horizon_policy} delay_regime={delay_regime} \
             complete_result_equal={} end_to_end_ns={} rounds={} events={} \
             mean_active_lps={:.6} mean_events_per_round={:.6} backend_ns_per_round={} \
             ns_per_transition={:.6} round_wall_ns={} horizon_ns={} fel_pop_ns={} fel_insert_ns={} \
             transition_body_ns={} resident_packet_ns={} outbox_staging_ns={} exchange_merge_ns={} \
             residual_ns={} profiled_events={} same_time_events={} fel_pop_count={} fel_insert_count={} \
             outbox_events={} resident_lookups={} resident_inserts={} resident_generator_inserts={} \
             resident_removes={} resident_alloc_calls={} resident_alloc_bytes={} \
             resident_dealloc_calls={} resident_dealloc_bytes={} generator_alloc_calls={} \
             generator_alloc_bytes={} outbox_key_inversions={} outbox_target_inversions={} \
             outbox_distinct_targets_sum={} exchange_targets_sum={} exchange_fan_in_sum={} \
             exchange_max_fan_in={} horizon_bound_min_ns={} horizon_bound_max_ns={} \
             horizon_distinct_bounds_max={}",
            u8::from(sum.complete_result_equal),
            sum.end_to_end_ns,
            sum.rounds,
            sum.events,
            sum.active_lps as f64 / rounds as f64,
            sum.events as f64 / rounds as f64,
            sum.round_wall_ns / rounds,
            sum.round_wall_ns as f64 / sum.events.max(1) as f64,
            sum.round_wall_ns,
            sum.horizon_ns,
            sum.lp.fel_pop_ns,
            sum.lp.fel_insert_ns,
            sum.lp.transition_body_ns,
            sum.lp.resident_packet_ns,
            sum.lp.outbox_staging_ns,
            sum.exchange_merge_ns,
            sum.residual_ns,
            sum.lp.profiled_events,
            sum.lp.consecutive_same_time_events,
            sum.lp.fel_pop_count,
            sum.lp.fel_insert_count,
            sum.lp.outbox_events,
            sum.lp.resident_packet_lookups,
            sum.lp.resident_packet_inserts,
            sum.lp.resident_packet_generator_inserts,
            sum.lp.resident_packet_removes,
            allocation.allocation_calls,
            allocation.allocated_bytes,
            allocation.deallocation_calls,
            allocation.deallocated_bytes,
            allocation.generator_allocation_calls,
            allocation.generator_allocated_bytes,
            sum.lp.outbox_key_inversions,
            sum.lp.outbox_target_inversions,
            sum.lp.outbox_distinct_targets,
            sum.exchange_targets,
            sum.exchange_fan_in_sum,
            sum.exchange_max_fan_in,
            sum.horizon_bound_min_ns,
            sum.horizon_bound_max_ns,
            sum.horizon_distinct_bounds_max,
        );
    }

    fn run_scalar_sample(
        config: &str,
        image: &days_executor::SimulationImage,
        oracle: Option<&RunResult>,
        sample: usize,
        policy: P11HorizonPolicy,
        delay_regime: &str,
    ) {
        reset_allocation_profile();
        let started = Instant::now();
        let run = run_scalar_rounds_with_p11_horizon_policy(
            image,
            None,
            ObservationMode::Summary,
            policy,
        )
        .expect("profiled scalar run must succeed");
        let end_to_end_ns = started.elapsed().as_nanos();
        if let Some(oracle) = oracle {
            assert_eq!(&run.result, oracle, "profiled scalar result changed");
        }
        let mut sum = ProfileSum {
            complete_result_equal: oracle.is_some(),
            end_to_end_ns,
            ..ProfileSum::default()
        };
        for round in &run.rounds {
            sum.add_round(
                round.events_processed,
                round.active_lp_count,
                round.p11_profile,
            );
        }
        let policy_name = match policy {
            P11HorizonPolicy::Global => "global",
            P11HorizonPolicy::TransitivePerLp => "transitive_per_lp",
        };
        emit(config, "scalar", 1, sample, sum, policy_name, delay_regime);
    }

    fn run_cpu_sample(
        config: &str,
        image: &days_executor::SimulationImage,
        oracle: Option<&RunResult>,
        workers: usize,
        sample: usize,
    ) {
        reset_allocation_profile();
        let started = Instant::now();
        let run = run_cpu(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
        )
        .expect("profiled CPU run must succeed");
        let end_to_end_ns = started.elapsed().as_nanos();
        if let Some(oracle) = oracle {
            assert_eq!(&run.result, oracle, "profiled CPU result changed");
        }
        let mut sum = ProfileSum {
            complete_result_equal: oracle.is_some(),
            end_to_end_ns,
            ..ProfileSum::default()
        };
        for round in &run.rounds {
            sum.add_round(
                round.semantic.events_processed,
                round.semantic.active_lp_count,
                round.semantic.p11_profile,
            );
        }
        emit(
            config,
            "cpu",
            workers,
            sample,
            sum,
            "global",
            "as_configured",
        );
    }

    pub fn main() {
        let mut config = None;
        let mut backend = "cpu".to_owned();
        let mut workers = 4_usize;
        let mut samples = 4_usize;
        let mut oracle_mode = "scalar".to_owned();
        let mut warmup = true;
        let mut horizon_policy = P11HorizonPolicy::Global;
        let mut delay_regime = "as_configured".to_owned();
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--backend" => backend = arguments.next().expect("--backend requires a value"),
                "--workers" => {
                    workers = arguments
                        .next()
                        .expect("--workers requires a value")
                        .parse()
                        .expect("--workers must be an integer")
                }
                "--samples" => {
                    samples = arguments
                        .next()
                        .expect("--samples requires a value")
                        .parse()
                        .expect("--samples must be an integer")
                }
                "--oracle" => oracle_mode = arguments.next().expect("--oracle requires a value"),
                "--warmup" => {
                    warmup = match arguments
                        .next()
                        .expect("--warmup requires a value")
                        .as_str()
                    {
                        "0" => false,
                        "1" => true,
                        _ => panic!("--warmup must be 0 or 1"),
                    }
                }
                "--horizon-policy" => {
                    horizon_policy = match arguments
                        .next()
                        .expect("--horizon-policy requires a value")
                        .as_str()
                    {
                        "global" => P11HorizonPolicy::Global,
                        "per-lp" => P11HorizonPolicy::TransitivePerLp,
                        value => panic!("unsupported horizon policy {value}"),
                    }
                }
                "--delay-regime" => {
                    delay_regime = arguments.next().expect("--delay-regime requires a value")
                }
                unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
                path if config.is_none() => config = Some(path.to_owned()),
                extra => panic!("unexpected argument {extra}"),
            }
        }
        assert!(matches!(backend.as_str(), "scalar" | "cpu"));
        assert!(matches!(oracle_mode.as_str(), "scalar" | "cpu" | "none"));
        assert!(workers > 0);
        assert!(samples > 0);
        let config = config.expect(
            "usage: t20a_round_profile CONFIG [--backend scalar|cpu] \
             [--oracle scalar|cpu|none] [--warmup 0|1]",
        );
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&config);
        let mut image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        match delay_regime.as_str() {
            "as_configured" | "uniform" => {}
            "heterogeneous" => {
                let old_propagation = image
                    .links
                    .iter()
                    .map(|link| link.propagation_ns)
                    .collect::<Vec<_>>();
                for link in &mut image.links {
                    link.propagation_ns = if link.id.0.is_multiple_of(8) {
                        1
                    } else {
                        10_000
                    };
                }
                for channel in &mut image.channels {
                    let link_slot = channel.link.0 as usize;
                    channel.min_delay_ns = channel
                        .min_delay_ns
                        .saturating_sub(old_propagation[link_slot])
                        .saturating_add(image.links[link_slot].propagation_ns);
                }
            }
            value => panic!("unsupported delay regime {value}"),
        }
        let oracle = match oracle_mode.as_str() {
            "scalar" => Some(run_scalar(&image, None).expect("scalar oracle must succeed")),
            "cpu" => Some(
                run_cpu(
                    &image,
                    None,
                    CpuConfig {
                        workers: 4,
                        ..CpuConfig::default()
                    },
                )
                .expect("W4 CPU oracle must succeed")
                .result,
            ),
            "none" => None,
            _ => unreachable!(),
        };
        let warmup_label = if warmup { "one_discarded" } else { "none" };
        println!(
            "record=t20a_protocol config={config} backend={backend} workers={workers} samples={samples} \
             horizon_policy={horizon_policy:?} delay_regime={delay_regime} \
             instrumentation=compile_time_optional timing_use=diagnostic_only warmup={warmup_label} \
             oracle={oracle_mode} \
             complete_result_gate=assert_eq"
        );
        if warmup {
            match backend.as_str() {
                "scalar" => {
                    let warmup = run_scalar_rounds_with_p11_horizon_policy(
                        &image,
                        None,
                        ObservationMode::Summary,
                        horizon_policy,
                    )
                    .expect("profiled scalar warmup must succeed");
                    if let Some(oracle) = &oracle {
                        assert_eq!(&warmup.result, oracle, "profiled scalar warmup changed");
                    }
                }
                "cpu" => {
                    let warmup = run_cpu(
                        &image,
                        None,
                        CpuConfig {
                            workers,
                            ..CpuConfig::default()
                        },
                    )
                    .expect("profiled CPU warmup must succeed");
                    if let Some(oracle) = &oracle {
                        assert_eq!(&warmup.result, oracle, "profiled CPU warmup changed");
                    }
                }
                _ => unreachable!(),
            }
        }
        for sample in 0..samples {
            match backend.as_str() {
                "scalar" => run_scalar_sample(
                    &config,
                    &image,
                    oracle.as_ref(),
                    sample,
                    horizon_policy,
                    &delay_regime,
                ),
                "cpu" => run_cpu_sample(&config, &image, oracle.as_ref(), workers, sample),
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(feature = "p11-profile")]
#[global_allocator]
static ALLOCATOR: app::CountingAllocator = app::CountingAllocator;

#[cfg(feature = "p11-profile")]
fn main() {
    app::main();
}

#[cfg(not(feature = "p11-profile"))]
fn main() {
    panic!("t20a_round_profile requires --features p11-profile");
}

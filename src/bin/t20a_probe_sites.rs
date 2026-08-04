#[cfg(feature = "p11-probe-sites")]
mod app {
    use std::path::PathBuf;
    use std::time::Instant;

    use days::scenario::compile_config;
    use days_executor::{
        CpuConfig, ObservationMode, RunResult, p11_probe_site_totals, reset_p11_probe_sites,
        run_cpu, run_scalar, run_scalar_rounds_with_observations,
    };

    struct MeasuredRun {
        result: RunResult,
        transitions: u64,
    }

    fn run_backend(
        image: &days_executor::SimulationImage,
        backend: &str,
        workers: usize,
    ) -> MeasuredRun {
        match backend {
            "scalar" => {
                let run =
                    run_scalar_rounds_with_observations(image, None, ObservationMode::Summary)
                        .expect("scalar probe-site run must succeed");
                let transitions = run.rounds.iter().map(|round| round.events_processed).sum();
                MeasuredRun {
                    result: run.result,
                    transitions,
                }
            }
            "cpu" => {
                let run = run_cpu(
                    image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                )
                .expect("CPU probe-site run must succeed");
                let transitions = run
                    .rounds
                    .iter()
                    .map(|round| round.semantic.events_processed)
                    .sum();
                MeasuredRun {
                    result: run.result,
                    transitions,
                }
            }
            _ => unreachable!(),
        }
    }

    pub fn main() {
        let mut config = None;
        let mut backend = "cpu".to_owned();
        let mut workers = 4_usize;
        let mut oracle_mode = "scalar".to_owned();
        let mut warmup = true;
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
                unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
                path if config.is_none() => config = Some(path.to_owned()),
                extra => panic!("unexpected argument {extra}"),
            }
        }
        assert!(matches!(backend.as_str(), "scalar" | "cpu"));
        assert!(matches!(oracle_mode.as_str(), "scalar" | "cpu" | "none"));
        assert!(workers > 0);
        let config = config.expect(
            "usage: t20a_probe_sites CONFIG [--backend scalar|cpu] [--workers N] \
             [--oracle scalar|cpu|none] [--warmup 0|1]",
        );
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&config);
        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
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

        if warmup {
            let warmup = run_backend(&image, &backend, workers);
            if let Some(oracle) = oracle.as_ref() {
                assert_eq!(&warmup.result, oracle, "probe-site warmup result changed");
            }
        }
        reset_p11_probe_sites();

        let started = Instant::now();
        let run = run_backend(&image, &backend, workers);
        let end_to_end_ns = started.elapsed().as_nanos();
        let complete_result_equal = if let Some(oracle) = oracle.as_ref() {
            assert_eq!(
                &run.result, oracle,
                "probe-site instrumented result changed"
            );
            true
        } else {
            false
        };

        let report_workers = if backend == "scalar" { 1 } else { workers };
        println!(
            "record=t20a_probe_protocol config={config} backend={backend} workers={report_workers} \
             instrumentation=counters_only timing_use=none oracle={oracle_mode} \
             complete_result_gate=assert_eq complete_result_equal={}",
            u8::from(complete_result_equal),
        );

        let mut totals = p11_probe_site_totals();
        let total_probes = totals.iter().map(|total| total.probes).sum::<u64>();
        let resident_probes = totals
            .iter()
            .filter(|total| total.map == "resident")
            .map(|total| total.probes)
            .sum::<u64>();
        let observed_probes = totals
            .iter()
            .filter(|total| total.map == "observed")
            .map(|total| total.probes)
            .sum::<u64>();
        let tcp_ledger_probes = totals
            .iter()
            .filter(|total| total.map == "tcp_ledger")
            .map(|total| total.probes)
            .sum::<u64>();
        let unused = totals
            .iter()
            .filter(|total| total.probes == 0)
            .map(|total| total.name)
            .collect::<Vec<_>>();

        totals.sort_by_key(|total| std::cmp::Reverse(total.probes));
        for total in totals
            .iter()
            .filter(|total| total.probes != 0 || total.calls != 0)
        {
            println!(
                "record=t20a_probe_site config={config} backend={backend} workers={report_workers} \
                 site={} map={} probes={} calls={} probes_per_event={:.6} probes_per_call={:.6} \
                 share_percent={:.6}",
                total.name,
                total.map,
                total.probes,
                total.calls,
                total.probes as f64 / run.transitions.max(1) as f64,
                total.probes as f64 / total.calls.max(1) as f64,
                total.probes as f64 * 100.0 / total_probes.max(1) as f64,
            );
        }
        println!(
            "record=t20a_probe_total config={config} backend={backend} workers={report_workers} \
             transitions={} total_probes={total_probes} resident_probes={resident_probes} \
             observed_probes={observed_probes} tcp_ledger_probes={tcp_ledger_probes} \
             probes_per_event={:.6} end_to_end_ns={end_to_end_ns}",
            run.transitions,
            total_probes as f64 / run.transitions.max(1) as f64,
        );
        println!("record=t20a_probe_unused sites={}", unused.join(","));
    }
}

#[cfg(feature = "p11-probe-sites")]
fn main() {
    app::main();
}

#[cfg(not(feature = "p11-probe-sites"))]
fn main() {
    panic!("t20a_probe_sites requires --features p11-probe-sites");
}

use std::path::PathBuf;
use std::time::Instant;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, ObservationMode, run_cpu, run_scalar, run_scalar_rounds_with_observations,
};

struct Timing {
    end_to_end_ns: u128,
    backend_ns: u128,
    rounds: usize,
    transitions: u64,
    active_lp_rounds: u128,
}

fn emit(
    config: &str,
    backend: &str,
    workers: usize,
    sample: usize,
    complete_result_equal: bool,
    timing: Timing,
) {
    let measured_rounds = timing.rounds.max(1) as u128;
    println!(
        "record=t20a_timing config={config} backend={backend} workers={workers} sample={sample} \
         instrumentation=off complete_result_equal={} end_to_end_ns={} backend_ns={} \
         rounds={} transitions={} mean_active_lps={:.6} mean_events_per_round={:.6} \
         backend_ns_per_round={} ns_per_transition={:.6}",
        u8::from(complete_result_equal),
        timing.end_to_end_ns,
        timing.backend_ns,
        timing.rounds,
        timing.transitions,
        timing.active_lp_rounds as f64 / measured_rounds as f64,
        timing.transitions as f64 / measured_rounds as f64,
        timing.backend_ns / measured_rounds,
        timing.backend_ns as f64 / timing.transitions.max(1) as f64,
    );
}

fn scalar_run(image: &days_executor::SimulationImage) -> (days_executor::ScalarRoundRun, u128) {
    let started = Instant::now();
    let run = run_scalar_rounds_with_observations(image, None, ObservationMode::Summary);
    let backend_ns = started.elapsed().as_nanos();
    (run.expect("scalar round run must succeed"), backend_ns)
}

fn main() {
    let mut config = None;
    let mut backend = "cpu".to_owned();
    let mut workers = 4_usize;
    let mut samples = 4_usize;
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
        "usage: t20a_round_timing CONFIG [--backend scalar|cpu] \
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
    let warmup_label = if warmup { "one_discarded" } else { "none" };

    println!(
        "record=t20a_timing_protocol config={config} backend={backend} workers={workers} \
         samples={samples} instrumentation=off warmup={warmup_label} oracle={oracle_mode} \
         complete_result_gate=assert_eq"
    );
    if warmup {
        match backend.as_str() {
            "scalar" => {
                let (run, _) = scalar_run(&image);
                if let Some(oracle) = &oracle {
                    assert_eq!(&run.result, oracle, "scalar warmup changed");
                }
            }
            "cpu" => {
                let run = run_cpu(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                )
                .expect("CPU warmup must succeed");
                if let Some(oracle) = &oracle {
                    assert_eq!(&run.result, oracle, "CPU warmup changed");
                }
            }
            _ => unreachable!(),
        }
    }

    for sample in 0..samples {
        let started = Instant::now();
        match backend.as_str() {
            "scalar" => {
                let (run, backend_ns) = scalar_run(&image);
                let end_to_end_ns = started.elapsed().as_nanos();
                if let Some(oracle) = &oracle {
                    assert_eq!(&run.result, oracle, "scalar timing result changed");
                }
                let transitions = run.rounds.iter().map(|round| round.events_processed).sum();
                let active_lp_rounds = run
                    .rounds
                    .iter()
                    .map(|round| round.active_lp_count as u128)
                    .sum();
                emit(
                    &config,
                    "scalar",
                    1,
                    sample,
                    oracle.is_some(),
                    Timing {
                        end_to_end_ns,
                        backend_ns,
                        rounds: run.rounds.len(),
                        transitions,
                        active_lp_rounds,
                    },
                );
            }
            "cpu" => {
                let run = run_cpu(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                )
                .expect("CPU timing run must succeed");
                let end_to_end_ns = started.elapsed().as_nanos();
                if let Some(oracle) = &oracle {
                    assert_eq!(&run.result, oracle, "CPU timing result changed");
                }
                let backend_ns = run
                    .rounds
                    .iter()
                    .map(|round| u128::from(round.round_wall_time_ns))
                    .sum();
                let transitions = run
                    .rounds
                    .iter()
                    .map(|round| round.semantic.events_processed)
                    .sum();
                let active_lp_rounds = run
                    .rounds
                    .iter()
                    .map(|round| round.semantic.active_lp_count as u128)
                    .sum();
                emit(
                    &config,
                    "cpu",
                    workers,
                    sample,
                    oracle.is_some(),
                    Timing {
                        end_to_end_ns,
                        backend_ns,
                        rounds: run.rounds.len(),
                        transitions,
                        active_lp_rounds,
                    },
                );
            }
            _ => unreachable!(),
        }
    }
}

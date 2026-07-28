use std::{env, error::Error, time::Instant};

use days::scenario::compile_config;
use days_executor::{
    ChunkGranularity, CpuConfig, CpuRoundMetrics, RoundMetrics, run_cpu, run_scalar_rounds,
};

#[derive(Clone, Copy)]
enum Mode {
    Scalar,
    Cpu(CpuConfig),
}

fn main() -> Result<(), Box<dyn Error>> {
    let (path, mode, repetitions) = parse_args()?;
    let image = compile_config(&path)?;

    for repetition in 0..repetitions {
        let timer = Instant::now();
        match mode {
            Mode::Scalar => {
                let run = run_scalar_rounds(&image, None)?;
                let wall_ns = timer.elapsed().as_nanos();
                print_result(
                    &path,
                    repetition,
                    "scalar",
                    1,
                    "scalar",
                    None,
                    wall_ns,
                    run.rounds.iter(),
                    &[],
                    run.result.summary,
                );
            }
            Mode::Cpu(config) => {
                let run = run_cpu(&image, None, config)?;
                let wall_ns = timer.elapsed().as_nanos();
                let chunk = match config.granularity {
                    ChunkGranularity::Static => "static".to_owned(),
                    ChunkGranularity::Fixed(size) => size.to_string(),
                };
                print_result(
                    &path,
                    repetition,
                    "cpu",
                    config.workers,
                    &chunk,
                    config.straggler_threshold_events,
                    wall_ns,
                    run.rounds.iter().map(|round| &round.semantic),
                    &run.rounds,
                    run.result.summary,
                );
            }
        }
    }
    Ok(())
}

fn parse_args() -> Result<(String, Mode, usize), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let path = args.next().ok_or(
        "usage: round_benchmark CONFIG [--workers N] [--chunk static|N] \
         [--straggler-threshold none|N] [--dedicated N] [--repetitions N]",
    )?;
    let mut workers = None;
    let mut granularity = ChunkGranularity::Static;
    let mut straggler_threshold_events = None;
    let mut dedicated_straggler_workers = 1;
    let mut repetitions = 1;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {flag}"))?;
        match flag.as_str() {
            "--workers" => workers = Some(value.parse()?),
            "--chunk" if value == "static" => granularity = ChunkGranularity::Static,
            "--chunk" => granularity = ChunkGranularity::Fixed(value.parse()?),
            "--straggler-threshold" if value == "none" => {
                straggler_threshold_events = None;
            }
            "--straggler-threshold" => straggler_threshold_events = Some(value.parse()?),
            "--dedicated" => dedicated_straggler_workers = value.parse()?,
            "--repetitions" => repetitions = value.parse()?,
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    let mode = workers.map_or(Mode::Scalar, |workers| {
        Mode::Cpu(CpuConfig {
            workers,
            granularity,
            straggler_threshold_events,
            dedicated_straggler_workers,
            ..CpuConfig::default()
        })
    });
    Ok((path, mode, repetitions))
}

#[allow(clippy::too_many_arguments)]
fn print_result<'round>(
    path: &str,
    repetition: usize,
    mode: &str,
    workers: usize,
    chunk: &str,
    straggler_threshold: Option<u64>,
    wall_ns: u128,
    rounds: impl Iterator<Item = &'round RoundMetrics> + Clone,
    cpu_rounds: &[CpuRoundMetrics],
    summary: days_executor::RunSummary,
) {
    let round_count = rounds.clone().count();
    let total_events = rounds
        .clone()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    let total_active_lps = rounds
        .clone()
        .map(|round| round.active_lp_count as u128)
        .sum::<u128>();
    let total_horizon_advance = rounds
        .clone()
        .map(|round| round.horizon_advance_ns)
        .sum::<u128>();
    let maximum_events = rounds
        .clone()
        .map(|round| round.events_processed)
        .max()
        .unwrap_or(0);
    let maximum_active_lps = rounds
        .clone()
        .map(|round| round.active_lp_count)
        .max()
        .unwrap_or(0);
    let maximum_lp_events = rounds
        .clone()
        .flat_map(|round| &round.lp_work)
        .map(|work| work.events_processed)
        .max()
        .unwrap_or(0);
    let mean_efficiency = mean(rounds.clone().map(|round| round.parallel_efficiency));
    let minimum_efficiency = rounds
        .map(|round| round.parallel_efficiency)
        .reduce(f64::min)
        .unwrap_or(1.0);
    let mean_lp_time_efficiency = mean(
        cpu_rounds
            .iter()
            .map(|round| round.lp_time_parallel_efficiency),
    );
    let mean_worker_efficiency = mean(
        cpu_rounds
            .iter()
            .map(|round| round.worker_parallel_efficiency),
    );
    let mean_worker_utilization = mean(cpu_rounds.iter().map(|round| round.worker_utilization));
    let straggler_lps = cpu_rounds
        .iter()
        .map(|round| round.partition.stragglers.len() as u128)
        .sum::<u128>();
    let bulk_chunks = cpu_rounds
        .iter()
        .map(|round| round.partition.bulk_chunks.len() as u128)
        .sum::<u128>();
    let worker_busy_ns = cpu_rounds
        .iter()
        .flat_map(|round| &round.worker_timings)
        .map(|worker| u128::from(worker.busy_ns))
        .sum::<u128>();
    let worker_idle_ns = cpu_rounds
        .iter()
        .flat_map(|round| &round.worker_timings)
        .map(|worker| u128::from(worker.idle_ns))
        .sum::<u128>();
    let round_divisor = u128::try_from(round_count.max(1)).expect("round count must fit u128");

    println!(
        "config={path} repetition={repetition} mode={mode} workers={workers} chunk={chunk} \
         straggler_threshold={} rounds={round_count} events={total_events} \
         mean_events_per_round={:.3} max_events_per_round={maximum_events} \
         mean_active_lps={:.3} max_active_lps={maximum_active_lps} \
         max_lp_events={maximum_lp_events} \
         mean_horizon_advance_ns={:.3} mean_parallel_efficiency={mean_efficiency:.6} \
         min_parallel_efficiency={minimum_efficiency:.6} \
         mean_lp_time_efficiency={mean_lp_time_efficiency:.6} \
         mean_worker_efficiency={mean_worker_efficiency:.6} \
         mean_worker_utilization={mean_worker_utilization:.6} \
         straggler_lps={straggler_lps} bulk_chunks={bulk_chunks} \
         worker_busy_ns={worker_busy_ns} worker_idle_ns={worker_idle_ns} \
         sourced_packets={} received_packets={} dropped_packets={} wall_ns={wall_ns}",
        straggler_threshold.map_or_else(|| "none".to_owned(), |value| value.to_string()),
        total_events as f64 / round_divisor as f64,
        total_active_lps as f64 / round_divisor as f64,
        total_horizon_advance as f64 / round_divisor as f64,
        summary.sourced_packets,
        summary.received_packets,
        summary.dropped_packets,
    );
}

fn mean(values: impl Iterator<Item = f64>) -> f64 {
    let (total, count) = values.fold((0.0, 0_u64), |(total, count), value| {
        (total + value, count + 1)
    });
    if count == 0 {
        1.0
    } else {
        total / count as f64
    }
}

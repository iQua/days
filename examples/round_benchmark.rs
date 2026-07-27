use std::{env, error::Error, time::Instant};

use days::scenario::compile_config;
use days_executor::run_scalar_rounds;

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args().nth(1).ok_or("usage: round_benchmark CONFIG")?;
    let image = compile_config(&path)?;

    let timer = Instant::now();
    let run = run_scalar_rounds(&image, None)?;
    let elapsed = timer.elapsed();

    let round_count = run.rounds.len();
    let total_events = run
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    let total_active_lps = run
        .rounds
        .iter()
        .map(|round| round.active_lp_count as u128)
        .sum::<u128>();
    let total_horizon_advance = run
        .rounds
        .iter()
        .map(|round| round.horizon_advance_ns)
        .sum::<u128>();
    let maximum_events = run
        .rounds
        .iter()
        .map(|round| round.events_processed)
        .max()
        .unwrap_or(0);
    let maximum_active_lps = run
        .rounds
        .iter()
        .map(|round| round.active_lp_count)
        .max()
        .unwrap_or(0);
    let mean_efficiency = if round_count == 0 {
        1.0
    } else {
        run.rounds
            .iter()
            .map(|round| round.parallel_efficiency)
            .sum::<f64>()
            / round_count as f64
    };
    let minimum_efficiency = run
        .rounds
        .iter()
        .map(|round| round.parallel_efficiency)
        .reduce(f64::min)
        .unwrap_or(1.0);
    let round_divisor = u128::try_from(round_count.max(1)).expect("round count must fit u128");
    let summary = run.result.summary;

    println!(
        "config={path} rounds={round_count} events={total_events} \
         mean_events_per_round={:.3} max_events_per_round={maximum_events} \
         mean_active_lps={:.3} max_active_lps={maximum_active_lps} \
         mean_horizon_advance_ns={:.3} \
         mean_parallel_efficiency={mean_efficiency:.6} \
         min_parallel_efficiency={minimum_efficiency:.6} \
         sourced_packets={} received_packets={} dropped_packets={} wall_ns={}",
        total_events as f64 / round_divisor as f64,
        total_active_lps as f64 / round_divisor as f64,
        total_horizon_advance as f64 / round_divisor as f64,
        summary.sourced_packets,
        summary.received_packets,
        summary.dropped_packets,
        elapsed.as_nanos()
    );
    Ok(())
}

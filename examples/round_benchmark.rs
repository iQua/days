use std::{env, error::Error, time::Instant};

use days::scenario::compile_config;
use days_executor::{
    ChunkGranularity, CpuConfig, CpuRoundMetrics, RoundMetrics, StaticPartitionPolicy, run_cpu,
    run_scalar_rounds,
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
                    "none",
                    None,
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
                let static_partition = match config.static_partition {
                    StaticPartitionPolicy::Modulo => "modulo",
                    StaticPartitionPolicy::RouteLoad => "route-load",
                };
                print_result(
                    &path,
                    repetition,
                    "cpu",
                    config.workers,
                    &chunk,
                    static_partition,
                    config.straggler_threshold_events,
                    Some(config.spin_before_park),
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
         [--straggler-threshold none|N] [--dedicated N] [--spin-before-park N] \
         [--static-partition modulo|route-load] [--repetitions N]",
    )?;
    let mut workers = None;
    let mut granularity = ChunkGranularity::Static;
    let mut static_partition = CpuConfig::default().static_partition;
    let mut straggler_threshold_events = None;
    let mut dedicated_straggler_workers = 1;
    let mut spin_before_park = CpuConfig::default().spin_before_park;
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
            "--spin-before-park" => spin_before_park = value.parse()?,
            "--static-partition" if value == "modulo" => {
                static_partition = StaticPartitionPolicy::Modulo;
            }
            "--static-partition" if value == "route-load" => {
                static_partition = StaticPartitionPolicy::RouteLoad;
            }
            "--repetitions" => repetitions = value.parse()?,
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    let mode = workers.map_or(Mode::Scalar, |workers| {
        Mode::Cpu(CpuConfig {
            workers,
            granularity,
            static_partition,
            straggler_threshold_events,
            dedicated_straggler_workers,
            spin_before_park,
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
    static_partition: &str,
    straggler_threshold: Option<u64>,
    spin_before_park: Option<u32>,
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
        .clone()
        .map(|round| round.parallel_efficiency)
        .reduce(f64::min)
        .unwrap_or(1.0);
    let remote_events = rounds
        .clone()
        .map(|round| u128::from(round.messages_exchanged))
        .sum::<u128>();
    let same_time_continuations = rounds
        .clone()
        .flat_map(|round| &round.lp_work)
        .map(|work| u128::from(work.same_time_continuations))
        .sum::<u128>();
    let physical_lp_probes = rounds
        .clone()
        .map(|round| u128::from(round.physical_lp_probes))
        .sum::<u128>();
    let round_divisor = u128::try_from(round_count.max(1)).expect("round count must fit u128");
    let cpu_fields = if cpu_rounds.is_empty() {
        String::new()
    } else {
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
        let owner_batches = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_batch_messages))
            .sum::<u128>();
        let worker_wakes = cpu_rounds
            .iter()
            .map(|round| u128::from(round.worker_wake_messages))
            .sum::<u128>();
        let worker_completions = cpu_rounds
            .iter()
            .map(|round| u128::from(round.worker_completion_messages))
            .sum::<u128>();
        let chunk_requests = cpu_rounds
            .iter()
            .map(|round| u128::from(round.chunk_request_messages))
            .sum::<u128>();
        let owner_deliveries = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_delivery_messages))
            .sum::<u128>();
        let owner_batches_merged = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_batches_merged))
            .sum::<u128>();
        let early_owner_batches_merged = cpu_rounds
            .iter()
            .map(|round| u128::from(round.early_owner_batches_merged))
            .sum::<u128>();
        let owner_merge_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.owner_merge_ns))
            .sum::<u128>();
        let early_owner_merge_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.early_owner_merge_ns))
            .sum::<u128>();
        let pool_messages = cpu_rounds
            .iter()
            .map(|round| u128::from(round.pool_messages()))
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
        let lp_busy_ns = cpu_rounds
            .iter()
            .flat_map(|round| &round.lp_timings)
            .map(|lp| u128::from(lp.busy_ns))
            .sum::<u128>();
        let worker_machinery_ns = worker_busy_ns.saturating_sub(lp_busy_ns);
        let coordinator_partition_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.coordinator_partition_ns))
            .sum::<u128>();
        let worker_wait_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.worker_wait_ns))
            .sum::<u128>();
        let coordinator_exchange_ns = cpu_rounds
            .iter()
            .map(|round| u128::from(round.coordinator_exchange_ns))
            .sum::<u128>();
        let chunks = straggler_lps.saturating_add(bulk_chunks);
        let legacy_protocol_messages_estimate = 4_u128
            .saturating_mul(workers as u128)
            .saturating_mul(round_count as u128)
            .saturating_add(3_u128.saturating_mul(chunks))
            .saturating_add(owner_batches);
        format!(
            " spin_before_park={} mean_lp_time_efficiency={mean_lp_time_efficiency:.6} \
             mean_worker_efficiency={mean_worker_efficiency:.6} \
             mean_worker_utilization={mean_worker_utilization:.6} \
             straggler_lps={straggler_lps} bulk_chunks={bulk_chunks} \
             owner_batches={owner_batches} worker_wakes={worker_wakes} \
             worker_completions={worker_completions} chunk_requests={chunk_requests} \
             owner_deliveries={owner_deliveries} owner_batches_merged={owner_batches_merged} \
             early_owner_batches_merged={early_owner_batches_merged} \
             owner_merge_ns={owner_merge_ns} early_owner_merge_ns={early_owner_merge_ns} \
             legacy_protocol_messages_estimate={legacy_protocol_messages_estimate} \
             pool_messages_actual={pool_messages} \
             mean_pool_messages_per_round={:.3} \
             lp_busy_ns={lp_busy_ns} worker_machinery_ns={worker_machinery_ns} \
             lp_busy_ns_per_active_lp={:.3} machinery_ns_per_physical_probe={:.3} \
             wall_ns_per_physical_probe={:.3} \
             worker_busy_ns={worker_busy_ns} worker_idle_ns={worker_idle_ns} \
             coordinator_partition_ns={coordinator_partition_ns} \
             worker_wait_ns={worker_wait_ns} coordinator_exchange_ns={coordinator_exchange_ns} \
             mean_coordinator_partition_ns={:.3} mean_worker_wait_ns={:.3} \
             mean_coordinator_exchange_ns={:.3}",
            spin_before_park.expect("CPU rows provide a spin bound"),
            pool_messages as f64 / round_divisor as f64,
            lp_busy_ns as f64 / total_active_lps.max(1) as f64,
            worker_machinery_ns as f64 / physical_lp_probes.max(1) as f64,
            wall_ns as f64 / physical_lp_probes.max(1) as f64,
            coordinator_partition_ns as f64 / round_divisor as f64,
            worker_wait_ns as f64 / round_divisor as f64,
            coordinator_exchange_ns as f64 / round_divisor as f64,
        )
    };

    println!(
        "config={path} repetition={repetition} mode={mode} workers={workers} chunk={chunk} \
         static_partition={static_partition} straggler_threshold={} \
         rounds={round_count} events={total_events} \
         mean_events_per_round={:.3} max_events_per_round={maximum_events} \
         mean_active_lps={:.3} max_active_lps={maximum_active_lps} \
         max_lp_events={maximum_lp_events} \
         mean_horizon_advance_ns={:.3} mean_parallel_efficiency={mean_efficiency:.6} \
         min_parallel_efficiency={minimum_efficiency:.6} \
         remote_events={remote_events} same_time_continuations={same_time_continuations} \
         physical_lp_probes={physical_lp_probes} \
         {cpu_fields} \
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

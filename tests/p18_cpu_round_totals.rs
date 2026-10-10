//! P18 cpu-levers M: `CpuRunTotals` equals the `days` CLI's old fold over the per-round records,
//! bit for bit, and the workers' tallies under `CpuRoundRecords::Totals` equal the per-LP lists of
//! `CpuRoundRecords::PerRound`.
//!
//! Until P18 the CLI kept every round's `CpuRoundMetrics` and folded them when it printed the
//! `days_cpu` record. `old_cli_fold` below is that fold, transcribed from `print_cpu_record` at
//! `571d708c`: sums and maxima over rounds, the f64 means as round-ordered sums, and the "timed"
//! sums over the rounds whose LP wall clocks were sampled. Two checks:
//!
//! - On a `PerRound` run, `CpuRun::totals` equals `old_cli_fold(CpuRun::rounds())` exactly, every
//!   field, wall-clock fields included (both come from the same records).
//! - A `Totals` run of the same image and configuration has the same deterministic fields: the
//!   same rounds, events, LP maxima, continuations, probes, partition counts and message counts.
//!   The wall-clock fields differ between any two runs and are excluded.
//!
//! The configurations cover the three CPU paths: static owned (1, 2 and 4 workers), classified
//! static (a straggler threshold) and dynamic (fixed chunks), in summary and full observation.

use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{
    ChunkGranularity, CpuConfig, CpuRoundMetrics, CpuRoundRecords, CpuRunTotals, ObservationMode,
    SimulationImage, run_cpu_with_observations,
};

const E1LONG_FIXTURE: &str = "configs/benchmarks/evaluation/e1long_open_k32_load_30.toml";
const SMOKE_FIXTURE: &str = "configs/ci/executor_smoke.toml";

/// The `days` CLI's fold over per-round records at `571d708c` (`print_cpu_record`), as totals.
fn old_cli_fold(rounds: &[CpuRoundMetrics]) -> CpuRunTotals {
    let semantic = || rounds.iter().map(|round| &round.semantic);
    let mean_sum = |values: &mut dyn Iterator<Item = f64>| values.fold(0.0, |total, v| total + v);
    let timed = rounds
        .iter()
        .filter(|round| !round.lp_timings.is_empty())
        .collect::<Vec<_>>();
    CpuRunTotals {
        rounds: rounds.len() as u64,
        events: semantic().map(|r| u128::from(r.events_processed)).sum(),
        active_lps: semantic().map(|r| r.active_lp_count as u128).sum(),
        horizon_advance_ns: semantic().map(|r| r.horizon_advance_ns).sum(),
        max_events_per_round: semantic().map(|r| r.events_processed).max().unwrap_or(0),
        max_active_lps: semantic().map(|r| r.active_lp_count).max().unwrap_or(0),
        max_lp_events: semantic()
            .flat_map(|r| &r.lp_work)
            .map(|work| work.events_processed)
            .max()
            .unwrap_or(0),
        parallel_efficiency_sum: mean_sum(&mut semantic().map(|r| r.parallel_efficiency)),
        min_parallel_efficiency: semantic().map(|r| r.parallel_efficiency).reduce(f64::min),
        remote_events: semantic().map(|r| u128::from(r.messages_exchanged)).sum(),
        same_time_continuations: semantic()
            .flat_map(|r| &r.lp_work)
            .map(|work| u128::from(work.same_time_continuations))
            .sum(),
        physical_lp_probes: semantic().map(|r| u128::from(r.physical_lp_probes)).sum(),
        lp_time_parallel_efficiency_sum: mean_sum(
            &mut rounds.iter().map(|r| r.lp_time_parallel_efficiency),
        ),
        worker_parallel_efficiency_sum: mean_sum(
            &mut rounds.iter().map(|r| r.worker_parallel_efficiency),
        ),
        worker_utilization_sum: mean_sum(&mut rounds.iter().map(|r| r.worker_utilization)),
        straggler_lps: rounds
            .iter()
            .map(|r| r.partition.stragglers.len() as u128)
            .sum(),
        bulk_chunks: rounds
            .iter()
            .map(|r| r.partition.bulk_chunks.len() as u128)
            .sum(),
        owner_batch_messages: rounds
            .iter()
            .map(|r| u128::from(r.owner_batch_messages))
            .sum(),
        worker_wake_messages: rounds
            .iter()
            .map(|r| u128::from(r.worker_wake_messages))
            .sum(),
        worker_completion_messages: rounds
            .iter()
            .map(|r| u128::from(r.worker_completion_messages))
            .sum(),
        chunk_request_messages: rounds
            .iter()
            .map(|r| u128::from(r.chunk_request_messages))
            .sum(),
        classification_presence_messages: rounds
            .iter()
            .map(|r| u128::from(r.classification_presence_messages))
            .sum(),
        classification_work_messages: rounds
            .iter()
            .map(|r| u128::from(r.classification_work_messages))
            .sum(),
        classification_return_messages: rounds
            .iter()
            .map(|r| u128::from(r.classification_return_messages))
            .sum(),
        owner_delivery_messages: rounds
            .iter()
            .map(|r| u128::from(r.owner_delivery_messages))
            .sum(),
        owner_batches_merged: rounds
            .iter()
            .map(|r| u128::from(r.owner_batches_merged))
            .sum(),
        early_owner_batches_merged: rounds
            .iter()
            .map(|r| u128::from(r.early_owner_batches_merged))
            .sum(),
        owner_merge_ns: rounds.iter().map(|r| u128::from(r.owner_merge_ns)).sum(),
        early_owner_merge_ns: rounds
            .iter()
            .map(|r| u128::from(r.early_owner_merge_ns))
            .sum(),
        pool_messages: rounds.iter().map(|r| u128::from(r.pool_messages())).sum(),
        worker_busy_ns: rounds
            .iter()
            .flat_map(|r| &r.worker_timings)
            .map(|w| u128::from(w.busy_ns))
            .sum(),
        worker_idle_ns: rounds
            .iter()
            .flat_map(|r| &r.worker_timings)
            .map(|w| u128::from(w.idle_ns))
            .sum(),
        timed_rounds: timed.len() as u64,
        timed_active_lps: timed.iter().map(|r| r.lp_timings.len() as u128).sum(),
        timed_physical_lp_probes: timed
            .iter()
            .map(|r| u128::from(r.semantic.physical_lp_probes))
            .sum(),
        timed_worker_busy_ns: timed
            .iter()
            .flat_map(|r| &r.worker_timings)
            .map(|w| u128::from(w.busy_ns))
            .sum(),
        lp_busy_ns: timed
            .iter()
            .flat_map(|r| &r.lp_timings)
            .map(|lp| u128::from(lp.busy_ns))
            .sum(),
        coordinator_partition_ns: rounds
            .iter()
            .map(|r| u128::from(r.coordinator_partition_ns))
            .sum(),
        worker_wait_ns: rounds.iter().map(|r| u128::from(r.worker_wait_ns)).sum(),
        coordinator_exchange_ns: rounds
            .iter()
            .map(|r| u128::from(r.coordinator_exchange_ns))
            .sum(),
        round_wall_time_ns: rounds
            .iter()
            .map(|r| u128::from(r.round_wall_time_ns))
            .sum(),
    }
}

/// The totals with every wall-clock-derived field cleared: what two runs must share.
fn deterministic(totals: CpuRunTotals) -> CpuRunTotals {
    CpuRunTotals {
        lp_time_parallel_efficiency_sum: 0.0,
        worker_parallel_efficiency_sum: 0.0,
        worker_utilization_sum: 0.0,
        early_owner_batches_merged: 0,
        owner_merge_ns: 0,
        early_owner_merge_ns: 0,
        worker_busy_ns: 0,
        worker_idle_ns: 0,
        timed_worker_busy_ns: 0,
        lp_busy_ns: 0,
        coordinator_partition_ns: 0,
        worker_wait_ns: 0,
        coordinator_exchange_ns: 0,
        round_wall_time_ns: 0,
        ..totals
    }
}

/// `Debug` prints every f64 in its shortest round-trip form, so equal renderings are equal bits.
fn bits(totals: &CpuRunTotals) -> String {
    format!("{totals:?}")
}

fn e1long_k4() -> PathBuf {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(E1LONG_FIXTURE))
        .expect("read the E1-long fixture");
    let text = [
        ("\nk = 32\n", "\nk = 4\n"),
        ("\nhosts_per_edge = 16\n", "\nhosts_per_edge = 2\n"),
        ("\nflow_count = 8192\n", "\nflow_count = 16\n"),
    ]
    .iter()
    .fold(text, |text, (from, to)| {
        assert_eq!(text.matches(from).count(), 1, "{from:?}");
        text.replace(from, to)
    });
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p18_cpu_round_totals_k4_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived scenario");
    path
}

fn lower(path: &Path) -> SimulationImage {
    compile_config_with_route_workers(path, RouteWorkers::serial()).expect("the scenario lowers")
}

fn configurations() -> Vec<CpuConfig> {
    let base = CpuConfig::default();
    let mut configs = [1, 2, 4]
        .into_iter()
        .map(|workers| CpuConfig { workers, ..base })
        .collect::<Vec<_>>();
    configs.push(CpuConfig {
        workers: 3,
        straggler_threshold_events: Some(2),
        dedicated_straggler_workers: 1,
        ..base
    });
    configs.push(CpuConfig {
        workers: 2,
        granularity: ChunkGranularity::Fixed(3),
        ..base
    });
    configs
}

fn check(image: &SimulationImage, horizon_ns: Option<u64>, label: &str) {
    for config in configurations() {
        for observation in [ObservationMode::Summary, ObservationMode::Full] {
            let per_round = run_cpu_with_observations(
                image,
                horizon_ns,
                CpuConfig {
                    round_records: CpuRoundRecords::PerRound,
                    ..config
                },
                observation,
            )
            .expect("the PerRound run succeeds");
            assert!(
                per_round.totals.rounds > 1,
                "{label}: the run must have rounds to compare"
            );
            assert_eq!(
                bits(&per_round.totals),
                bits(&old_cli_fold(per_round.rounds())),
                "{label} {config:?} {observation:?}: totals against the CLI's old fold"
            );
            let totals = run_cpu_with_observations(
                image,
                horizon_ns,
                CpuConfig {
                    round_records: CpuRoundRecords::Totals,
                    ..config
                },
                observation,
            )
            .expect("the Totals run succeeds");
            assert_eq!(totals.result, per_round.result, "{label} {config:?}");
            assert_eq!(
                bits(&deterministic(totals.totals)),
                bits(&deterministic(per_round.totals)),
                "{label} {config:?} {observation:?}: Totals tallies against PerRound lists"
            );
            if observation == ObservationMode::Full || per_round.totals.rounds > 64 {
                assert!(
                    per_round.totals.timed_rounds > 0,
                    "{label}: some rounds sample LP wall clocks"
                );
            }
        }
    }
}

#[test]
fn round_totals_equal_the_cli_fold_on_the_ci_smoke_fixture() {
    let image = lower(&Path::new(env!("CARGO_MANIFEST_DIR")).join(SMOKE_FIXTURE));
    check(&image, None, "executor_smoke");
}

#[test]
fn round_totals_equal_the_cli_fold_on_a_k4_e1long_fabric() {
    let path = e1long_k4();
    let image = lower(&path);
    let _ = fs::remove_file(&path);
    check(&image, Some(300_000), "e1long_k4_300us");
}

#[test]
fn per_round_records_are_refused_when_not_requested() {
    let image = lower(&Path::new(env!("CARGO_MANIFEST_DIR")).join(SMOKE_FIXTURE));
    let run =
        run_cpu_with_observations(&image, None, CpuConfig::default(), ObservationMode::Summary)
            .expect("the run succeeds");
    assert!(run.totals.rounds > 1);
    let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run.rounds().len()));
    assert!(
        refused.is_err(),
        "a Totals run must not hand out an empty round list"
    );
}

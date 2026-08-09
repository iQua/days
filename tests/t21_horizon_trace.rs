//! T21 (P12) horizon-trace instrument gates.
//!
//! Two things are pinned here.
//!
//! 1. **The structural facts the instrument reports are properties of the executor, not of the
//!    instrument.** The safe horizon is `frontier + min channel delay`, the next round's frontier
//!    never precedes the previous horizon, and therefore a round count can never exceed the
//!    equivalent slot count of a pipeline quantised at that same minimum delay. The
//!    rounds-vs-equivalent-slots metric rests on those three facts, so they are asserted directly.
//!
//! 2. **The instrument is output-invariant.** `src/bin/t21_horizon_trace.rs` reads counters the
//!    ordinary build already exposes and writes them to a file; running it with the dump on and
//!    with the dump off must produce the same complete-state fingerprint, and round-mode execution
//!    must produce the same `RunResult` as plain scalar execution.
//!
//! NO TIMING ASSERTION APPEARS IN THIS FILE. Every quantity is a count, a nanosecond of *simulated*
//! time, or a fingerprint.

use std::path::PathBuf;

use assert_cmd::cargo::cargo_bin_cmd;
use days::scenario::compile_config;
use days_executor::{
    ObservationMode, SimulationImage, run_scalar_rounds_with_observations,
    run_scalar_with_observations,
};

/// Cheap fully-drained fixture: k=4 fat tree, 8 flows, single-threaded.
const CHEAP_FIXTURE: &str = "configs/benchmarks/baseline/fattree_k4_f8_st.toml";

fn lower(relative: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

fn minimum_channel_delay_ns(image: &SimulationImage) -> u64 {
    image
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min()
        .expect("a lowered fixture has at least one channel")
}

/// The horizon is exactly `frontier + minimum channel delay`, clamped by the configured stop.
///
/// This is the definition the equivalent-slot denominator is derived from: a pipeline that must
/// quantise at the shortest delay anywhere in the fabric needs one tick per `min_delay_ns`.
#[test]
fn every_round_horizon_is_the_frontier_plus_the_minimum_channel_delay() {
    let image = lower(CHEAP_FIXTURE);
    let quantum = u128::from(minimum_channel_delay_ns(&image));
    let run_end = u128::from(image.stop_time_ns) + 1;
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    assert!(!run.rounds.is_empty(), "fixture must execute rounds");
    for (index, round) in run.rounds.iter().enumerate() {
        assert_eq!(
            round.exclusive_horizon_ns,
            run_end.min(u128::from(round.frontier_ns) + quantum),
            "round {index} horizon is not the frontier plus the lookahead quantum",
        );
    }
}

/// Round boundaries never move backwards, and no round starts before the previous one ended.
///
/// Without this the per-round trace could not be read as a partition of simulated time, and the
/// horizon-width-over-simulated-time artifact would be meaningless.
#[test]
fn round_boundaries_partition_simulated_time_forwards() {
    let image = lower(CHEAP_FIXTURE);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    for pair in run.rounds.windows(2) {
        assert!(
            u128::from(pair[1].frontier_ns) >= pair[0].exclusive_horizon_ns,
            "round frontier {} precedes the previous horizon {}",
            pair[1].frontier_ns,
            pair[0].exclusive_horizon_ns,
        );
        assert!(
            pair[1].exclusive_horizon_ns > pair[0].exclusive_horizon_ns,
            "round horizon did not advance past {}",
            pair[0].exclusive_horizon_ns,
        );
    }
}

/// The hardware-independent claim: our round count is bounded by the equivalent slot count.
///
/// A slot pipeline quantised at the fabric's shortest delay needs `ceil(span / quantum)` ticks to
/// cover the same simulated span. Because the horizon advances by at least one quantum per round,
/// our round count can only meet or beat that bound. The rounds-vs-slots ratio published for a
/// fixture is therefore a structural statement, never a hardware one.
#[test]
fn rounds_never_exceed_the_equivalent_slot_count_at_the_minimum_delay_quantum() {
    let image = lower(CHEAP_FIXTURE);
    let quantum = u128::from(minimum_channel_delay_ns(&image));
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    let span_ns = u128::from(image.stop_time_ns);
    let equivalent_slots = span_ns.div_ceil(quantum);
    assert!(
        run.rounds.len() as u128 <= equivalent_slots,
        "{} rounds exceed the {equivalent_slots} equivalent slots of a {quantum} ns quantum",
        run.rounds.len(),
    );
}

/// Round-mode execution and plain scalar execution agree on complete state.
///
/// The frozen fixture anchors are recorded from `run_scalar_with_observations`; the instrument
/// reads `run_scalar_rounds_with_observations`. If those two paths could disagree, a fingerprint
/// printed by the instrument would not be comparable with the anchor it is checked against.
#[test]
fn round_mode_execution_matches_plain_scalar_execution() {
    let image = lower(CHEAP_FIXTURE);
    let plain = run_scalar_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar run must succeed");
    let rounds = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    assert_eq!(rounds.result, plain);
}

fn instrument_stdout(arguments: &[&str]) -> String {
    let output = cargo_bin_cmd!("t21_horizon_trace")
        .args(arguments)
        .output()
        .expect("horizon trace instrument must launch");
    assert!(
        output.status.success(),
        "horizon trace instrument failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("instrument output must be UTF-8")
}

fn record_line<'stdout>(stdout: &'stdout str, record: &str) -> &'stdout str {
    stdout
        .lines()
        .find(|line| line.starts_with(&format!("record={record} ")))
        .unwrap_or_else(|| panic!("instrument must emit a {record} record:\n{stdout}"))
}

fn field<'line>(line: &'line str, name: &str) -> &'line str {
    line.split_whitespace()
        .find_map(|entry| entry.strip_prefix(&format!("{name}=")))
        .unwrap_or_else(|| panic!("record must carry {name}: {line}"))
}

/// Turning the dump on cannot move complete state: it is a file sink, not an instrument.
#[test]
fn the_per_round_dump_does_not_change_complete_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let trace = directory.path().join("trace.csv");
    let histogram = directory.path().join("histogram.csv");

    let without_dump = instrument_stdout(&[CHEAP_FIXTURE]);
    let with_dump = instrument_stdout(&[
        CHEAP_FIXTURE,
        "--per-round-csv",
        trace.to_str().expect("temporary path is UTF-8"),
        "--width-histogram-csv",
        histogram.to_str().expect("temporary path is UTF-8"),
    ]);

    let quiet = record_line(&without_dump, "t21_horizon_identity");
    let dumped = record_line(&with_dump, "t21_horizon_identity");
    assert_eq!(
        field(quiet, "result_fnv1a64"),
        field(dumped, "result_fnv1a64")
    );
    assert_eq!(field(quiet, "result_bytes"), field(dumped, "result_bytes"));
    assert_eq!(field(quiet, "dump"), "off");
    assert_eq!(field(dumped, "dump"), "on");

    assert_eq!(
        record_line(&without_dump, "t21_horizon_rounds"),
        record_line(&with_dump, "t21_horizon_rounds"),
        "the round summary must not depend on whether a file was written",
    );

    let csv = std::fs::read_to_string(&trace).expect("per-round CSV must be written");
    let mut lines = csv.lines();
    let header = lines.next().expect("CSV has a header");
    assert_eq!(
        header,
        "round,frontier_ns,exclusive_horizon_ns,horizon_span_ns,horizon_advance_ns,\
         channel_slots_advanced,cumulative_channel_slots,propagation_slots_advanced,\
         cumulative_propagation_slots,active_lp_count,host_active_lps,switch_active_lps,\
         transitions,messages_exchanged,same_time_continuations"
    );
    let columns = header.split(',').count();
    let rows = lines
        .inspect(|line| assert_eq!(line.split(',').count(), columns))
        .count();
    assert_eq!(
        rows.to_string(),
        field(record_line(&with_dump, "t21_horizon_rounds"), "rounds"),
        "the CSV must carry one row per reported round",
    );

    let histogram_csv = std::fs::read_to_string(&histogram).expect("histogram CSV must be written");
    let mut histogram_lines = histogram_csv.lines();
    assert_eq!(
        histogram_lines.next().expect("histogram has a header"),
        "active_lp_count,rounds"
    );
    let histogram_rounds = histogram_lines
        .map(|line| {
            line.split(',')
                .nth(1)
                .expect("histogram row has a round count")
                .parse::<u64>()
                .expect("histogram round count is an integer")
        })
        .sum::<u64>();
    assert_eq!(
        histogram_rounds.to_string(),
        field(record_line(&with_dump, "t21_horizon_rounds"), "rounds"),
        "the histogram must account for every round",
    );
}

/// The equivalent-slot decomposition telescopes: per-round advances sum to the traversed total.
#[test]
fn per_round_slot_advances_sum_to_the_traversed_slot_count() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let trace = directory.path().join("trace.csv");
    let stdout = instrument_stdout(&[
        CHEAP_FIXTURE,
        "--per-round-csv",
        trace.to_str().expect("temporary path is UTF-8"),
    ]);

    let csv = std::fs::read_to_string(&trace).expect("per-round CSV must be written");
    let column_sum = |column: usize| {
        csv.lines()
            .skip(1)
            .map(|line| {
                line.split(',')
                    .nth(column)
                    .expect("row has the requested slot column")
                    .parse::<u128>()
                    .expect("slot advance is an integer")
            })
            .sum::<u128>()
    };

    let summary = record_line(&stdout, "t21_horizon_rounds");
    assert_eq!(
        column_sum(5).to_string(),
        field(summary, "channel_slots_traversed")
    );
    assert_eq!(
        column_sum(7).to_string(),
        field(summary, "propagation_slots_traversed")
    );
}

/// A slot pipeline quantised at link propagation alone never needs fewer ticks than one quantised
/// at the executor's own lookahead, which is propagation PLUS minimum serialization.
///
/// Both denominators are published so that no table can quietly pick the flattering one.
#[test]
fn the_propagation_quantum_never_reports_fewer_slots_than_the_channel_quantum() {
    let stdout = instrument_stdout(&[CHEAP_FIXTURE]);
    let summary = record_line(&stdout, "t21_horizon_rounds");
    let value = |name: &str| {
        field(summary, name)
            .parse::<u128>()
            .expect("slot counts are integers")
    };

    assert!(value("min_link_propagation_ns") <= value("min_channel_delay_ns"));
    // A fabric may declare no propagation at all, and a slot pipeline cannot have a zero-width
    // slot, so the effective quantum falls back to the channel delay and says so.
    assert_eq!(
        value("propagation_slot_quantum_ns"),
        if value("min_link_propagation_ns") == 0 {
            value("min_channel_delay_ns")
        } else {
            value("min_link_propagation_ns")
        },
    );
    assert!(value("propagation_slots_configured") >= value("channel_slots_configured"));
    assert!(value("propagation_slots_traversed") >= value("channel_slots_traversed"));
    assert!(
        value("rounds") <= value("channel_slots_configured"),
        "the round count must respect the conservative denominator",
    );
}

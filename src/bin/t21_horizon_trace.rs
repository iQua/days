//! P12 T21 horizon trace: per-round safe-horizon structure of a scalar run.
//!
//! **WHAT THIS IS.** A disclosed *consumer* of counters the ordinary executor build already
//! exposes. `RoundMetrics` (`executor/src/safe_horizon.rs`) has always carried the round frontier,
//! the exclusive horizon, the horizon advance, the active-LP width and the transition count;
//! `run_scalar_rounds_with_observations` has always returned the whole vector. This binary lowers a
//! config, runs that ordinary scalar round path once, and projects the vector into records and an
//! optional CSV. It adds **zero bytes** to any execution path, so it cannot move complete state —
//! the same reason `src/bin/t20a_lp_shape.rs` states in its own header.
//!
//! **NO TIMING.** Nothing here is timed and nothing here may be reported as a wall-clock claim.
//! Every quantity below is a count, a nanosecond of *simulated* time, or a fingerprint. The
//! wave-4 honesty constraint applies to anything derived from it: per-round device cost carries
//! width-fixed terms, so a lower round count is a mechanism, not a speedup.
//!
//! **THE HARDWARE-INDEPENDENT METRIC.** The safe horizon is `frontier + min channel delay` and the
//! next round's frontier never precedes the previous horizon, so the round count is bounded above
//! by `ceil(span / quantum)` — the ticks a pipeline quantised at the fabric's shortest delay needs
//! to cover the same simulated span. `tests/t21_horizon_trace.rs` pins those facts.
//!
//! **TWO DENOMINATORS, BOTH PRINTED.** "Shortest delay" is not one number. `channel_slots_*` uses
//! the executor's own lookahead — minimum link propagation PLUS the serialization of the smallest
//! admitted packet — which is the quantum our round count is provably bounded by, and is the
//! conservative choice. `propagation_slots_*` uses the minimum link propagation alone, which is how
//! a slot pipeline that charges serialization inside its tick is built. The second is never smaller
//! than the first, so a table that quoted only it would be flattering us; both are on every record
//! so the choice has to be made in the open.
//!
//! Usage:
//!
//! ```text
//! t21_horizon_trace CONFIG [--label NAME] [--horizon-ns N]
//!                          [--per-round-csv PATH] [--width-histogram-csv PATH]
//! ```
//!
//! `--horizon-ns` is the ordinary partial-run exclusive boundary of [`run_scalar`], used to take a
//! cheap horizon on a fixture whose own horizon is long. It is reported on every record.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Write as _};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    NodeId, NodeKind, ObservationMode, RoundMetrics, RunResult, SimulationImage,
    run_scalar_rounds_with_observations,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

const CSV_HEADER: &str = "round,frontier_ns,exclusive_horizon_ns,horizon_span_ns,\
                          horizon_advance_ns,channel_slots_advanced,cumulative_channel_slots,\
                          propagation_slots_advanced,cumulative_propagation_slots,\
                          active_lp_count,host_active_lps,switch_active_lps,transitions,\
                          messages_exchanged,same_time_continuations";

const USAGE: &str = "usage: t21_horizon_trace CONFIG [--label NAME] [--horizon-ns N] \
                     [--per-round-csv PATH] [--width-histogram-csv PATH]";

struct Cli {
    config: String,
    label: String,
    horizon_ns: Option<u64>,
    per_round_csv: Option<PathBuf>,
    width_histogram_csv: Option<PathBuf>,
}

impl Cli {
    fn parse() -> Self {
        let mut config = None;
        let mut label = None;
        let mut horizon_ns = None;
        let mut per_round_csv = None;
        let mut width_histogram_csv = None;
        let mut arguments = std::env::args().skip(1);

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--label" => label = Some(arguments.next().expect("--label requires a value")),
                "--horizon-ns" => {
                    horizon_ns = Some(
                        arguments
                            .next()
                            .expect("--horizon-ns requires a value")
                            .parse()
                            .expect("--horizon-ns must be an integer nanosecond count"),
                    );
                }
                "--per-round-csv" => {
                    per_round_csv = Some(PathBuf::from(
                        arguments.next().expect("--per-round-csv requires a value"),
                    ));
                }
                "--width-histogram-csv" => {
                    width_histogram_csv = Some(PathBuf::from(
                        arguments
                            .next()
                            .expect("--width-histogram-csv requires a value"),
                    ));
                }
                unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
                path if config.is_none() => config = Some(path.to_owned()),
                extra => panic!("unexpected argument {extra}"),
            }
        }

        let config = config.expect(USAGE);
        if let Some(horizon) = horizon_ns {
            assert!(horizon > 0, "--horizon-ns must be greater than zero");
        }
        let label = label.unwrap_or_else(|| config.clone());

        Self {
            config,
            label,
            horizon_ns,
            per_round_csv,
            width_histogram_csv,
        }
    }

    fn dump_state(&self) -> &'static str {
        if self.per_round_csv.is_some() || self.width_histogram_csv.is_some() {
            "on"
        } else {
            "off"
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: u64,
    fnv1a64: u64,
}

struct FingerprintWriter(Fingerprint);

impl fmt::Write for FingerprintWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.bytes = self
            .0
            .bytes
            .checked_add(value.len() as u64)
            .ok_or(fmt::Error)?;
        self.0.fnv1a64 = value.bytes().fold(self.0.fnv1a64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
        });
        Ok(())
    }
}

/// FNV-1a64 over the pretty `Debug` rendering of a complete result.
///
/// Identical to `src/bin/t20f_frontier.rs` and `tests/t21_p12_fixtures.rs`, so a fingerprint from
/// this instrument is directly comparable with a frozen fixture anchor.
fn fingerprint(value: &impl Debug) -> Fingerprint {
    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    write!(&mut writer, "{value:#?}").expect("debug serialization length must fit in u64");
    writer.0
}

/// Ticks a pipeline quantised at `quantum_ns` needs to reach simulated time `time_ns`.
///
/// Ceiling division, so the decomposition telescopes exactly: the slots a round advances are
/// `slot_index(horizon) - slot_index(previous boundary)`, and those differences sum to
/// `slot_index(last horizon) - slot_index(first frontier)` with no rounding drift.
const fn slot_index(time_ns: u128, quantum_ns: u64) -> u128 {
    time_ns.div_ceil(quantum_ns as u128)
}

/// The two quanta a slot pipeline can be built on for the same fabric.
///
/// Both are reported on every record so that no table ever has to pick the denominator silently.
#[derive(Clone, Copy)]
struct Quanta {
    /// Minimum `RemoteChannel::min_delay_ns`: link propagation PLUS the serialization of the
    /// smallest admitted packet. This is the executor's own lookahead, so it is the quantum our
    /// round count is provably bounded by, and it is the CONSERVATIVE denominator.
    channel_ns: u64,
    /// Minimum `LinkDescriptor::propagation_ns`, reported raw — it is legitimately **zero** for a
    /// fabric that declares no propagation delay, where the whole channel delay is serialization.
    min_link_propagation_ns: u64,
    /// The quantum a pipeline that ticks link traversal and charges serialization inside the tick
    /// would use: `min_link_propagation_ns`, or `channel_ns` when that is zero, because a slot
    /// pipeline cannot have a zero-width slot. Never larger than `channel_ns`, so it never yields
    /// fewer slots.
    propagation_ns: u64,
}

impl Quanta {
    fn new(image: &SimulationImage) -> Self {
        let channel_ns = image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min()
            .expect("a lowered image has at least one channel");
        let min_link_propagation_ns = image
            .links
            .iter()
            .map(|link| link.propagation_ns)
            .min()
            .expect("a lowered image has at least one link");
        assert!(
            channel_ns > 0,
            "a zero minimum channel delay has no equivalent slot count"
        );
        assert!(
            min_link_propagation_ns <= channel_ns,
            "a channel delay cannot be shorter than its link propagation"
        );
        Self {
            channel_ns,
            min_link_propagation_ns,
            propagation_ns: if min_link_propagation_ns == 0 {
                channel_ns
            } else {
                min_link_propagation_ns
            },
        }
    }
}

#[derive(Clone, Copy)]
struct RoundRow {
    index: usize,
    frontier_ns: u64,
    exclusive_horizon_ns: u128,
    horizon_span_ns: u128,
    horizon_advance_ns: u128,
    channel_slots_advanced: u128,
    cumulative_channel_slots: u128,
    propagation_slots_advanced: u128,
    cumulative_propagation_slots: u128,
    active_lp_count: usize,
    host_active_lps: usize,
    switch_active_lps: usize,
    transitions: u64,
    messages_exchanged: u64,
    same_time_continuations: u64,
}

fn write_row(writer: &mut impl Write, row: RoundRow) -> io::Result<()> {
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        row.index,
        row.frontier_ns,
        row.exclusive_horizon_ns,
        row.horizon_span_ns,
        row.horizon_advance_ns,
        row.channel_slots_advanced,
        row.cumulative_channel_slots,
        row.propagation_slots_advanced,
        row.cumulative_propagation_slots,
        row.active_lp_count,
        row.host_active_lps,
        row.switch_active_lps,
        row.transitions,
        row.messages_exchanged,
        row.same_time_continuations,
    )
}

#[derive(Default)]
struct Trace {
    rounds: usize,
    transitions: u128,
    messages: u128,
    same_time_continuations: u128,
    active_lp_rounds: u128,
    host_active_lp_rounds: u128,
    first_frontier_ns: u128,
    last_horizon_ns: u128,
    channel_slots_traversed: u128,
    propagation_slots_traversed: u128,
    rounds_wider_than_one_quantum: usize,
    widths: Vec<usize>,
    horizon_spans: Vec<u128>,
    horizon_advances: Vec<u128>,
    width_histogram: BTreeMap<usize, u64>,
}

/// Projects the round vector into the trace, optionally writing the per-round CSV.
///
/// The CSV is a pure sink: every summary quantity is computed the same way whether or not a writer
/// is supplied, which is what `the_per_round_dump_does_not_change_complete_state` checks.
fn project(
    rounds: &[RoundMetrics],
    quanta: Quanta,
    node_kinds: &HashMap<NodeId, NodeKind>,
    mut csv: Option<&mut BufWriter<File>>,
) -> io::Result<Trace> {
    let mut trace = Trace {
        widths: Vec::with_capacity(rounds.len()),
        horizon_spans: Vec::with_capacity(rounds.len()),
        horizon_advances: Vec::with_capacity(rounds.len()),
        ..Trace::default()
    };
    if let Some(writer) = csv.as_mut() {
        writeln!(*writer, "{CSV_HEADER}")?;
    }

    let Some(first) = rounds.first() else {
        return Ok(trace);
    };
    let first_channel_boundary = slot_index(u128::from(first.frontier_ns), quanta.channel_ns);
    let first_propagation_boundary =
        slot_index(u128::from(first.frontier_ns), quanta.propagation_ns);
    trace.first_frontier_ns = u128::from(first.frontier_ns);
    let mut previous_boundary = u128::from(first.frontier_ns);

    for (index, round) in rounds.iter().enumerate() {
        assert_eq!(
            round.active_lp_count,
            round.lp_work.len(),
            "round {index} active LP count disagrees with lp_work",
        );
        let host_active_lps = round
            .lp_work
            .iter()
            .filter(|work| {
                node_kinds.get(&work.node).copied().unwrap_or_else(|| {
                    panic!("round {index} references unknown node {:?}", work.node)
                }) == NodeKind::Host
            })
            .count();
        let same_time_continuations = round
            .lp_work
            .iter()
            .map(|work| work.same_time_continuations)
            .sum::<u64>();

        let cumulative_channel_slots =
            slot_index(round.exclusive_horizon_ns, quanta.channel_ns) - first_channel_boundary;
        let channel_slots_advanced = slot_index(round.exclusive_horizon_ns, quanta.channel_ns)
            - slot_index(previous_boundary, quanta.channel_ns);
        let cumulative_propagation_slots =
            slot_index(round.exclusive_horizon_ns, quanta.propagation_ns)
                - first_propagation_boundary;
        let propagation_slots_advanced =
            slot_index(round.exclusive_horizon_ns, quanta.propagation_ns)
                - slot_index(previous_boundary, quanta.propagation_ns);
        let horizon_span_ns = round.exclusive_horizon_ns - u128::from(round.frontier_ns);

        trace.rounds += 1;
        trace.transitions += u128::from(round.events_processed);
        trace.messages += u128::from(round.messages_exchanged);
        trace.same_time_continuations += u128::from(same_time_continuations);
        trace.active_lp_rounds += round.active_lp_count as u128;
        trace.host_active_lp_rounds += host_active_lps as u128;
        trace.last_horizon_ns = round.exclusive_horizon_ns;
        trace.channel_slots_traversed = cumulative_channel_slots;
        trace.propagation_slots_traversed = cumulative_propagation_slots;
        if channel_slots_advanced > 1 {
            trace.rounds_wider_than_one_quantum += 1;
        }
        trace.widths.push(round.active_lp_count);
        trace.horizon_spans.push(horizon_span_ns);
        trace.horizon_advances.push(round.horizon_advance_ns);
        *trace
            .width_histogram
            .entry(round.active_lp_count)
            .or_default() += 1;

        if let Some(writer) = csv.as_mut() {
            write_row(
                *writer,
                RoundRow {
                    index,
                    frontier_ns: round.frontier_ns,
                    exclusive_horizon_ns: round.exclusive_horizon_ns,
                    horizon_span_ns,
                    horizon_advance_ns: round.horizon_advance_ns,
                    channel_slots_advanced,
                    cumulative_channel_slots,
                    propagation_slots_advanced,
                    cumulative_propagation_slots,
                    active_lp_count: round.active_lp_count,
                    host_active_lps,
                    switch_active_lps: round.active_lp_count - host_active_lps,
                    transitions: round.events_processed,
                    messages_exchanged: round.messages_exchanged,
                    same_time_continuations,
                },
            )?;
        }

        previous_boundary = round.exclusive_horizon_ns;
    }

    Ok(trace)
}

fn median(sorted: &[u128]) -> f64 {
    match sorted.len() {
        0 => 0.0,
        length if length % 2 == 1 => sorted[length / 2] as f64,
        length => (sorted[length / 2 - 1] as f64 + sorted[length / 2] as f64) / 2.0,
    }
}

struct Distribution {
    minimum: u128,
    median: f64,
    maximum: u128,
}

fn distribution(values: &[u128]) -> Distribution {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    Distribution {
        minimum: sorted.first().copied().unwrap_or(0),
        median: median(&sorted),
        maximum: sorted.last().copied().unwrap_or(0),
    }
}

fn ratio(numerator: u128, denominator: u128) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

/// Simulated span the run was asked to cover, in nanoseconds.
///
/// `stop_time_ns` is the fixture's INCLUSIVE endpoint, so the executor's exclusive end is one
/// nanosecond later; that extra nanosecond is never a whole quantum and is excluded here so that a
/// 20 us fixture on a 1,000 ns fabric reports 20 equivalent slots rather than 21.
const fn span_ns(image: &SimulationImage, horizon_ns: Option<u64>) -> u64 {
    match horizon_ns {
        Some(horizon) if horizon < image.stop_time_ns => horizon,
        _ => image.stop_time_ns,
    }
}

fn print_identity(cli: &Cli, result: &RunResult, quanta: Quanta, lp_count: usize) {
    let fingerprint = fingerprint(result);
    println!(
        "record=t21_horizon_identity config={} label={} horizon_ns={} min_channel_delay_ns={} \
         min_link_propagation_ns={} propagation_slot_quantum_ns={} lp_count={lp_count} dump={} \
         engine=scalar_rounds observations=summary result_bytes={} result_fnv1a64={:016x} \
         pending_events={} resident_packets={} sourced_packets={} departed_packets={} \
         received_packets={} dropped_packets={}",
        cli.config,
        cli.label,
        cli.horizon_ns
            .map_or_else(|| "none".to_owned(), |horizon| horizon.to_string()),
        quanta.channel_ns,
        quanta.min_link_propagation_ns,
        quanta.propagation_ns,
        cli.dump_state(),
        fingerprint.bytes,
        fingerprint.fnv1a64,
        result.pending_events.len(),
        result.resident_packets.len(),
        result.summary.sourced_packets,
        result.summary.departed_packets,
        result.summary.received_packets,
        result.summary.dropped_packets,
    );
}

fn print_rounds(cli: &Cli, trace: &Trace, quanta: Quanta, span_ns: u64, lp_count: usize) {
    let widths = trace
        .widths
        .iter()
        .map(|&width| width as u128)
        .collect::<Vec<_>>();
    let width = distribution(&widths);
    let horizon_span = distribution(&trace.horizon_spans);
    let horizon_advance = distribution(&trace.horizon_advances);
    let channel_slots_configured = u128::from(span_ns).div_ceil(u128::from(quanta.channel_ns));
    let propagation_slots_configured =
        u128::from(span_ns).div_ceil(u128::from(quanta.propagation_ns));
    let rounds = trace.rounds as u128;
    let traversed_span_ns = trace
        .last_horizon_ns
        .saturating_sub(trace.first_frontier_ns);

    println!(
        "record=t21_horizon_rounds config={} label={} horizon_ns={} min_channel_delay_ns={} \
         min_link_propagation_ns={} propagation_slot_quantum_ns={} lp_count={lp_count} \
         stop_time_ns_span={span_ns} \
         channel_slots_configured={channel_slots_configured} \
         propagation_slots_configured={propagation_slots_configured} rounds={} \
         collapse_vs_channel_slots={:.6} collapse_vs_propagation_slots={:.6} \
         first_frontier_ns={} last_horizon_ns={} traversed_span_ns={traversed_span_ns} \
         channel_slots_traversed={} propagation_slots_traversed={} \
         collapse_vs_channel_slots_traversed={:.6} \
         rounds_advancing_more_than_one_quantum={} transitions={} messages_exchanged={} \
         same_time_continuations={} mean_active_lps={:.6} active_lps_min={} \
         active_lps_median={:.1} active_lps_max={} mean_host_active_lps={:.6} \
         horizon_span_ns_min={} horizon_span_ns_median={:.1} horizon_span_ns_max={} \
         horizon_advance_ns_min={} horizon_advance_ns_median={:.1} horizon_advance_ns_max={} \
         transitions_per_round={:.6} distinct_widths={}",
        cli.config,
        cli.label,
        cli.horizon_ns
            .map_or_else(|| "none".to_owned(), |horizon| horizon.to_string()),
        quanta.channel_ns,
        quanta.min_link_propagation_ns,
        quanta.propagation_ns,
        trace.rounds,
        ratio(channel_slots_configured, rounds),
        ratio(propagation_slots_configured, rounds),
        trace.first_frontier_ns,
        trace.last_horizon_ns,
        trace.channel_slots_traversed,
        trace.propagation_slots_traversed,
        ratio(trace.channel_slots_traversed, rounds),
        trace.rounds_wider_than_one_quantum,
        trace.transitions,
        trace.messages,
        trace.same_time_continuations,
        ratio(trace.active_lp_rounds, rounds),
        width.minimum,
        width.median,
        width.maximum,
        ratio(trace.host_active_lp_rounds, rounds),
        horizon_span.minimum,
        horizon_span.median,
        horizon_span.maximum,
        horizon_advance.minimum,
        horizon_advance.median,
        horizon_advance.maximum,
        ratio(trace.transitions, rounds),
        trace.width_histogram.len(),
    );
}

fn write_width_histogram(path: &PathBuf, trace: &Trace) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writeln!(writer, "active_lp_count,rounds")?;
    for (width, rounds) in &trace.width_histogram {
        writeln!(writer, "{width},{rounds}")?;
    }
    writer.flush()
}

fn main() {
    let cli = Cli::parse();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&cli.config);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));

    let quanta = Quanta::new(&image);
    let span_ns = span_ns(&image, cli.horizon_ns);

    let mut node_kinds = HashMap::with_capacity(image.nodes.len());
    for node in &image.nodes {
        assert!(
            node_kinds.insert(node.id, node.kind).is_none(),
            "image contains duplicate node {:?}",
            node.id
        );
    }

    let run = run_scalar_rounds_with_observations(&image, cli.horizon_ns, ObservationMode::Summary)
        .expect("scalar round run must succeed");

    let mut csv = cli.per_round_csv.as_ref().map(|csv_path| {
        BufWriter::new(
            File::create(csv_path)
                .unwrap_or_else(|error| panic!("failed to create {}: {error}", csv_path.display())),
        )
    });
    let trace = project(&run.rounds, quanta, &node_kinds, csv.as_mut())
        .expect("failed to write the per-round CSV");
    if let Some(writer) = csv.as_mut() {
        writer.flush().expect("failed to flush the per-round CSV");
    }
    if let Some(histogram_path) = cli.width_histogram_csv.as_ref() {
        write_width_histogram(histogram_path, &trace).unwrap_or_else(|error| {
            panic!("failed to write {}: {error}", histogram_path.display())
        });
    }

    print_identity(&cli, &run.result, quanta, image.nodes.len());
    print_rounds(&cli, &trace, quanta, span_ns, image.nodes.len());
    for (width, rounds) in &trace.width_histogram {
        println!(
            "record=t21_horizon_width_bin config={} label={} active_lp_count={width} rounds={rounds}",
            cli.config, cli.label,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{Trace, distribution, median, slot_index};

    #[test]
    fn slot_index_is_ceiling_division_by_the_quantum() {
        assert_eq!(slot_index(0, 100), 0);
        assert_eq!(slot_index(1, 100), 1);
        assert_eq!(slot_index(100, 100), 1);
        assert_eq!(slot_index(101, 100), 2);
        assert_eq!(slot_index(200_000, 100), 2_000);
    }

    /// Per-round slot advances telescope exactly, so a sparse round that jumps a silence is
    /// credited with every slot the pipeline would have ticked through.
    #[test]
    fn slot_advances_telescope_across_a_silence() {
        let quantum = 1_000;
        let boundaries: [u128; 4] = [1_000, 2_000, 900_000, 1_000_000];
        let first_frontier = 0_u128;

        let mut previous = first_frontier;
        let mut advances = 0_u128;
        for boundary in boundaries {
            advances += slot_index(boundary, quantum) - slot_index(previous, quantum);
            previous = boundary;
        }

        assert_eq!(
            advances,
            slot_index(*boundaries.last().expect("nonempty"), quantum)
                - slot_index(first_frontier, quantum)
        );
        assert_eq!(advances, 1_000);
    }

    #[test]
    fn median_averages_the_two_middle_values() {
        assert_eq!(median(&[1, 3, 7, 9]), 5.0);
        assert_eq!(median(&[1, 3, 7]), 3.0);
        assert_eq!(median(&[]), 0.0);
    }

    #[test]
    fn distribution_reports_the_extremes_of_an_unsorted_series() {
        let summary = distribution(&[7, 1, 9, 3]);

        assert_eq!(summary.minimum, 1);
        assert_eq!(summary.maximum, 9);
        assert_eq!(summary.median, 5.0);
    }

    #[test]
    fn an_empty_trace_is_well_defined() {
        let trace = Trace::default();

        assert_eq!(trace.rounds, 0);
        assert_eq!(trace.channel_slots_traversed, 0);
        assert_eq!(trace.propagation_slots_traversed, 0);
        assert!(trace.width_histogram.is_empty());
    }
}

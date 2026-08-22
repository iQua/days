//! Measurement-only analysis of safe-horizon LP work shape.
//!
//! This binary runs separately from timed experiments. It consumes counters already exposed by
//! ordinary executor builds and neither instruments nor changes simulator execution semantics.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, CpuRoundMetrics, NodeId, NodeKind, ObservationMode, RoundMetrics, run_cpu,
    run_scalar_rounds_with_observations,
};

const DEFAULT_LANE_WIDTHS: [usize; 3] = [8, 32, 64];
const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

struct Cli {
    config: String,
    backend: String,
    workers: usize,
    lane_widths: Vec<usize>,
    per_round_csv: Option<PathBuf>,
    exclusive_horizon_ns: Option<u64>,
    json: bool,
}

impl Cli {
    fn parse() -> Self {
        let mut config = None;
        let mut backend = "cpu".to_owned();
        let mut workers = 18_usize;
        let mut lane_widths = Vec::new();
        let mut per_round_csv = None;
        let mut exclusive_horizon_ns = None;
        let mut json = false;
        let mut arguments = std::env::args().skip(1);

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--backend" => backend = arguments.next().expect("--backend requires a value"),
                "--workers" => {
                    workers = arguments
                        .next()
                        .expect("--workers requires a value")
                        .parse()
                        .expect("--workers must be an integer");
                }
                "--lane-width" => {
                    let width = arguments
                        .next()
                        .expect("--lane-width requires a value")
                        .parse()
                        .expect("--lane-width must be an integer");
                    assert!(width > 0, "--lane-width must be greater than zero");
                    if !lane_widths.contains(&width) {
                        lane_widths.push(width);
                    }
                }
                "--per-round-csv" => {
                    per_round_csv = Some(PathBuf::from(
                        arguments.next().expect("--per-round-csv requires a value"),
                    ));
                }
                "--exclusive-horizon-ns" => {
                    exclusive_horizon_ns = Some(
                        arguments
                            .next()
                            .expect("--exclusive-horizon-ns requires a value")
                            .parse()
                            .expect("--exclusive-horizon-ns must be an integer"),
                    );
                }
                "--json" => json = true,
                unknown if unknown.starts_with("--") => panic!("unknown argument {unknown}"),
                path if config.is_none() => config = Some(path.to_owned()),
                extra => panic!("unexpected argument {extra}"),
            }
        }

        assert!(
            matches!(backend.as_str(), "scalar" | "cpu"),
            "--backend must be scalar or cpu"
        );
        assert!(workers > 0, "--workers must be greater than zero");
        if lane_widths.is_empty() {
            lane_widths.extend(DEFAULT_LANE_WIDTHS);
        }

        Self {
            config: config.expect(
                "usage: t20a_lp_shape CONFIG [--backend scalar|cpu] [--workers N] \
                 [--lane-width W]... [--exclusive-horizon-ns N] [--per-round-csv PATH] [--json]",
            ),
            backend,
            workers,
            lane_widths,
            per_round_csv,
            exclusive_horizon_ns,
            json,
        }
    }

    fn data_workers(&self) -> usize {
        if self.backend == "scalar" {
            1
        } else {
            self.workers
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

fn fingerprint(value: &impl Debug) -> Fingerprint {
    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    fmt::write(&mut writer, format_args!("{value:#?}"))
        .expect("debug serialization length must fit in u64");
    writer.0
}

#[derive(Clone, Copy)]
struct ActiveEntry {
    slot: usize,
    kind: NodeKind,
    events_processed: u64,
}

impl ActiveEntry {
    const fn new(slot: usize, kind: NodeKind, events_processed: u64) -> Self {
        Self {
            slot,
            kind,
            events_processed,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LaneRoundSummary {
    lane_utilisation: f64,
    role_homogeneous_groups: usize,
    groups: usize,
    mean_kinds_per_group: f64,
    max_over_mean: f64,
    capacity: u128,
    kind_count_sum: usize,
    max_over_mean_sum: f64,
    max_over_mean_groups: usize,
}

fn ratio(numerator: u128, denominator: u128, zero_value: f64) -> f64 {
    if denominator == 0 {
        zero_value
    } else {
        numerator as f64 / denominator as f64
    }
}

fn summarize_lane(entries: &[ActiveEntry], width: usize) -> LaneRoundSummary {
    assert!(width > 0, "lane width must be greater than zero");

    let mut capacity = 0_u128;
    let mut role_homogeneous_groups = 0_usize;
    let mut groups = 0_usize;
    let mut kind_count_sum = 0_usize;
    let mut max_over_mean_sum = 0.0;
    let mut max_over_mean_groups = 0_usize;

    // The final short group uses its actual length: inactive/padded lanes are intentionally absent
    // from the lockstep denominator requested by this measurement.
    for group in entries.chunks(width) {
        groups += 1;
        let group_events = group
            .iter()
            .map(|entry| u128::from(entry.events_processed))
            .sum::<u128>();
        let group_max = group
            .iter()
            .map(|entry| entry.events_processed)
            .max()
            .expect("chunks are nonempty");
        capacity += group.len() as u128 * u128::from(group_max);

        let first_kind = group[0].kind;
        let homogeneous = group.iter().all(|entry| entry.kind == first_kind);
        role_homogeneous_groups += usize::from(homogeneous);
        kind_count_sum += if homogeneous { 1 } else { 2 };

        if group_events > 0 {
            max_over_mean_sum += group_max as f64 * group.len() as f64 / group_events as f64;
            max_over_mean_groups += 1;
        }
    }

    let useful = entries
        .iter()
        .map(|entry| u128::from(entry.events_processed))
        .sum::<u128>();

    LaneRoundSummary {
        lane_utilisation: ratio(useful, capacity, 1.0),
        role_homogeneous_groups,
        groups,
        mean_kinds_per_group: ratio(kind_count_sum as u128, groups as u128, 0.0),
        max_over_mean: if max_over_mean_groups == 0 {
            0.0
        } else {
            max_over_mean_sum / max_over_mean_groups as f64
        },
        capacity,
        kind_count_sum,
        max_over_mean_sum,
        max_over_mean_groups,
    }
}

#[derive(Default)]
struct Series {
    values: Vec<f64>,
}

impl Series {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: impl Into<f64>) {
        self.values.push(value.into());
    }

    fn summary(&mut self) -> DistributionSummary {
        self.values.sort_by(f64::total_cmp);
        if self.values.is_empty() {
            return DistributionSummary::default();
        }
        DistributionSummary {
            median: median_sorted(&self.values),
            min: self.values[0],
            max: self.values[self.values.len() - 1],
        }
    }
}

#[derive(Clone, Copy, Default)]
struct DistributionSummary {
    median: f64,
    min: f64,
    max: f64,
}

#[derive(Clone, Copy, Default)]
struct CpuRoundCounterSample {
    round_wall_time_ns: u64,
    owner_merge_ns: u64,
    early_owner_merge_ns: u64,
    coordinator_exchange_ns: u64,
    coordinator_partition_ns: u64,
    worker_wait_ns: u64,
    messages_exchanged: u64,
    owner_batches_merged: u64,
    early_owner_batches_merged: u64,
}

impl From<&CpuRoundMetrics> for CpuRoundCounterSample {
    fn from(round: &CpuRoundMetrics) -> Self {
        Self {
            round_wall_time_ns: round.round_wall_time_ns,
            owner_merge_ns: round.owner_merge_ns,
            early_owner_merge_ns: round.early_owner_merge_ns,
            coordinator_exchange_ns: round.coordinator_exchange_ns,
            coordinator_partition_ns: round.coordinator_partition_ns,
            worker_wait_ns: round.worker_wait_ns,
            messages_exchanged: round.semantic.messages_exchanged,
            owner_batches_merged: round.owner_batches_merged,
            early_owner_batches_merged: round.early_owner_batches_merged,
        }
    }
}

impl CpuRoundCounterSample {
    fn scalar(round: &RoundMetrics) -> Self {
        Self {
            messages_exchanged: round.messages_exchanged,
            ..Self::default()
        }
    }
}

#[derive(Default)]
struct CpuRoundCounterSummary {
    rounds: usize,
    round_wall_ns_total: u128,
    owner_merge_ns_total: u128,
    early_owner_merge_ns_total: u128,
    coordinator_exchange_ns_total: u128,
    coordinator_partition_ns_total: u128,
    worker_wait_ns_total: u128,
    messages_exchanged_total: u128,
    owner_batches_merged_total: u128,
    early_owner_batches_merged_total: u128,
    owner_merge_ns: DistributionSummary,
    coordinator_exchange_ns: DistributionSummary,
}

fn summarize_cpu_round_counters(
    samples: impl IntoIterator<Item = CpuRoundCounterSample>,
) -> CpuRoundCounterSummary {
    let mut summary = CpuRoundCounterSummary::default();
    let mut owner_merge_ns = Series::default();
    let mut coordinator_exchange_ns = Series::default();

    for sample in samples {
        summary.rounds += 1;
        summary.round_wall_ns_total += u128::from(sample.round_wall_time_ns);
        summary.owner_merge_ns_total += u128::from(sample.owner_merge_ns);
        summary.early_owner_merge_ns_total += u128::from(sample.early_owner_merge_ns);
        summary.coordinator_exchange_ns_total += u128::from(sample.coordinator_exchange_ns);
        summary.coordinator_partition_ns_total += u128::from(sample.coordinator_partition_ns);
        summary.worker_wait_ns_total += u128::from(sample.worker_wait_ns);
        summary.messages_exchanged_total += u128::from(sample.messages_exchanged);
        summary.owner_batches_merged_total += u128::from(sample.owner_batches_merged);
        summary.early_owner_batches_merged_total += u128::from(sample.early_owner_batches_merged);
        owner_merge_ns.push(sample.owner_merge_ns as f64);
        coordinator_exchange_ns.push(sample.coordinator_exchange_ns as f64);
    }

    summary.owner_merge_ns = owner_merge_ns.summary();
    summary.coordinator_exchange_ns = coordinator_exchange_ns.summary();
    summary
}

#[cfg(test)]
fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    median_sorted(values)
}

fn median_sorted(values: &[f64]) -> f64 {
    match values.len() {
        0 => 0.0,
        len if len % 2 == 1 => values[len / 2],
        len => (values[len / 2 - 1] + values[len / 2]) / 2.0,
    }
}

struct LaneAggregate {
    width: usize,
    lane_utilisation: Series,
    role_homogeneous_groups: Series,
    groups: Series,
    mean_kinds_per_group: Series,
    max_over_mean: Series,
    total_capacity: u128,
    total_role_homogeneous_groups: u128,
    total_groups: u128,
    total_kind_count: u128,
    total_max_over_mean_sum: f64,
    total_max_over_mean_groups: u128,
}

impl LaneAggregate {
    fn new(width: usize, round_capacity: usize) -> Self {
        Self {
            width,
            lane_utilisation: Series::with_capacity(round_capacity),
            role_homogeneous_groups: Series::with_capacity(round_capacity),
            groups: Series::with_capacity(round_capacity),
            mean_kinds_per_group: Series::with_capacity(round_capacity),
            max_over_mean: Series::with_capacity(round_capacity),
            total_capacity: 0,
            total_role_homogeneous_groups: 0,
            total_groups: 0,
            total_kind_count: 0,
            total_max_over_mean_sum: 0.0,
            total_max_over_mean_groups: 0,
        }
    }

    fn observe(&mut self, summary: LaneRoundSummary) {
        self.lane_utilisation.push(summary.lane_utilisation);
        self.role_homogeneous_groups
            .push(summary.role_homogeneous_groups as f64);
        self.groups.push(summary.groups as f64);
        self.mean_kinds_per_group.push(summary.mean_kinds_per_group);
        self.max_over_mean.push(summary.max_over_mean);
        self.total_capacity += summary.capacity;
        self.total_role_homogeneous_groups += summary.role_homogeneous_groups as u128;
        self.total_groups += summary.groups as u128;
        self.total_kind_count += summary.kind_count_sum as u128;
        self.total_max_over_mean_sum += summary.max_over_mean_sum;
        self.total_max_over_mean_groups += summary.max_over_mean_groups as u128;
    }
}

struct Analysis {
    rounds: usize,
    total_events: u128,
    total_active_lp_rounds: u128,
    total_host_active_lps: u128,
    total_switch_active_lps: u128,
    total_fallback_classified_pushes: u128,
    total_same_time_continuations: u128,
    active_lps: Series,
    events_processed: Series,
    host_active_lps: Series,
    switch_active_lps: Series,
    fallback_classified_pushes: Series,
    same_time_coverage: Series,
    lanes: Vec<LaneAggregate>,
}

impl Analysis {
    fn new(lane_widths: &[usize], round_capacity: usize) -> Self {
        Self {
            rounds: 0,
            total_events: 0,
            total_active_lp_rounds: 0,
            total_host_active_lps: 0,
            total_switch_active_lps: 0,
            total_fallback_classified_pushes: 0,
            total_same_time_continuations: 0,
            active_lps: Series::with_capacity(round_capacity),
            events_processed: Series::with_capacity(round_capacity),
            host_active_lps: Series::with_capacity(round_capacity),
            switch_active_lps: Series::with_capacity(round_capacity),
            fallback_classified_pushes: Series::with_capacity(round_capacity),
            same_time_coverage: Series::with_capacity(round_capacity),
            lanes: lane_widths
                .iter()
                .map(|&width| LaneAggregate::new(width, round_capacity))
                .collect(),
        }
    }
}

fn write_csv_header(writer: &mut impl Write, lane_widths: &[usize]) -> io::Result<()> {
    write!(
        writer,
        "round,events_processed,active_lp_count,host_active_lps,switch_active_lps,\
         same_time_continuations,same_time_coverage,fallback_classified_pushes"
    )?;
    for width in lane_widths {
        write!(
            writer,
            ",lane_utilisation_w{width},role_homogeneous_groups_w{width},groups_w{width},\
             mean_kinds_per_group_w{width},max_over_mean_w{width}"
        )?;
    }
    writeln!(writer)
}

struct CsvRound {
    index: usize,
    events_processed: u64,
    active_lp_count: usize,
    host_active_lps: usize,
    switch_active_lps: usize,
    same_time_continuations: u128,
    same_time_coverage: f64,
    fallback_classified_pushes: u128,
}

fn write_csv_round(
    writer: &mut impl Write,
    round: CsvRound,
    lanes: &[LaneRoundSummary],
) -> io::Result<()> {
    write!(
        writer,
        "{},{},{},{},{},{},{:.6},{}",
        round.index,
        round.events_processed,
        round.active_lp_count,
        round.host_active_lps,
        round.switch_active_lps,
        round.same_time_continuations,
        round.same_time_coverage,
        round.fallback_classified_pushes,
    )?;
    for lane in lanes {
        write!(
            writer,
            ",{:.6},{},{},{:.6},{:.6}",
            lane.lane_utilisation,
            lane.role_homogeneous_groups,
            lane.groups,
            lane.mean_kinds_per_group,
            lane.max_over_mean,
        )?;
    }
    writeln!(writer)
}

fn analyze_rounds<'a>(
    rounds: impl Iterator<Item = &'a RoundMetrics>,
    round_capacity: usize,
    node_slots: &HashMap<NodeId, usize>,
    node_kinds: &[NodeKind],
    lane_widths: &[usize],
    mut csv: Option<&mut BufWriter<File>>,
) -> io::Result<Analysis> {
    let mut analysis = Analysis::new(lane_widths, round_capacity);
    let mut active = Vec::with_capacity(node_slots.len());
    let mut lane_rounds = Vec::with_capacity(lane_widths.len());

    if let Some(writer) = csv.as_mut() {
        write_csv_header(*writer, lane_widths)?;
    }

    for (round_index, round) in rounds.enumerate() {
        assert_eq!(
            round.active_lp_count,
            round.lp_work.len(),
            "round {round_index} active LP count disagrees with lp_work"
        );

        active.clear();
        active.extend(round.lp_work.iter().map(|work| {
            let slot = *node_slots.get(&work.node).unwrap_or_else(|| {
                panic!(
                    "round {round_index} references unknown node {:?}",
                    work.node
                )
            });
            ActiveEntry::new(slot, node_kinds[slot], work.events_processed)
        }));
        // GPU active-worklist compaction is in ascending LP-slot order; this reproduces that lane
        // assignment independently of the order in which a backend returned `lp_work`.
        active.sort_unstable_by_key(|entry| entry.slot);
        assert!(
            active.windows(2).all(|pair| pair[0].slot != pair[1].slot),
            "round {round_index} contains duplicate LP work"
        );

        let events_processed = round
            .lp_work
            .iter()
            .map(|work| u128::from(work.events_processed))
            .sum::<u128>();
        assert_eq!(
            u128::from(round.events_processed),
            events_processed,
            "round {round_index} event total disagrees with lp_work"
        );
        let same_time_continuations = round
            .lp_work
            .iter()
            .map(|work| u128::from(work.same_time_continuations))
            .sum::<u128>();
        let fallback_classified_pushes = round
            .lp_work
            .iter()
            .map(|work| u128::from(work.fallback_classified_pushes))
            .sum::<u128>();
        let host_active_lps = active
            .iter()
            .filter(|entry| entry.kind == NodeKind::Host)
            .count();
        let switch_active_lps = active.len() - host_active_lps;
        let same_time_coverage = ratio(same_time_continuations, events_processed, 0.0);

        analysis.rounds += 1;
        analysis.total_events += events_processed;
        analysis.total_active_lp_rounds += round.active_lp_count as u128;
        analysis.total_host_active_lps += host_active_lps as u128;
        analysis.total_switch_active_lps += switch_active_lps as u128;
        analysis.total_fallback_classified_pushes += fallback_classified_pushes;
        analysis.total_same_time_continuations += same_time_continuations;
        analysis.active_lps.push(round.active_lp_count as f64);
        analysis
            .events_processed
            .push(round.events_processed as f64);
        analysis.host_active_lps.push(host_active_lps as f64);
        analysis.switch_active_lps.push(switch_active_lps as f64);
        analysis
            .fallback_classified_pushes
            .push(fallback_classified_pushes as f64);
        analysis.same_time_coverage.push(same_time_coverage);

        lane_rounds.clear();
        for lane in &mut analysis.lanes {
            let summary = summarize_lane(&active, lane.width);
            lane.observe(summary);
            lane_rounds.push(summary);
        }

        if let Some(writer) = csv.as_mut() {
            write_csv_round(
                *writer,
                CsvRound {
                    index: round_index,
                    events_processed: round.events_processed,
                    active_lp_count: round.active_lp_count,
                    host_active_lps,
                    switch_active_lps,
                    same_time_continuations,
                    same_time_coverage,
                    fallback_classified_pushes,
                },
                &lane_rounds,
            )?;
        }
    }

    Ok(analysis)
}

#[derive(Clone, Copy)]
struct CommonSummaries {
    active_lps: DistributionSummary,
    events_processed: DistributionSummary,
    host_active_lps: DistributionSummary,
    switch_active_lps: DistributionSummary,
    fallback_classified_pushes: DistributionSummary,
}

fn distribution_json(summary: DistributionSummary) -> serde_json::Value {
    serde_json::json!({
        "median": summary.median,
        "min": summary.min,
        "max": summary.max,
    })
}

fn count(value: u128) -> u64 {
    u64::try_from(value).expect("analysis count must fit in u64")
}

fn emit_json(cli: &Cli, mut analysis: Analysis, complete_state: Fingerprint) {
    let common = CommonSummaries {
        active_lps: analysis.active_lps.summary(),
        events_processed: analysis.events_processed.summary(),
        host_active_lps: analysis.host_active_lps.summary(),
        switch_active_lps: analysis.switch_active_lps.summary(),
        fallback_classified_pushes: analysis.fallback_classified_pushes.summary(),
    };
    let same_time_coverage = analysis.same_time_coverage.summary();
    let lanes = analysis
        .lanes
        .iter_mut()
        .map(|lane| {
            let lane_utilisation = lane.lane_utilisation.summary();
            let role_homogeneous_groups = lane.role_homogeneous_groups.summary();
            let groups = lane.groups.summary();
            let mean_kinds_per_group = lane.mean_kinds_per_group.summary();
            let max_over_mean = lane.max_over_mean.summary();
            let max_over_mean_overall = if lane.total_max_over_mean_groups == 0 {
                0.0
            } else {
                lane.total_max_over_mean_sum / lane.total_max_over_mean_groups as f64
            };

            serde_json::json!({
                "width": lane.width,
                "lane_utilization": {
                    "median": lane_utilisation.median,
                    "min": lane_utilisation.min,
                    "max": lane_utilisation.max,
                    "overall": ratio(analysis.total_events, lane.total_capacity, 1.0),
                },
                "group_max_over_mean_work": {
                    "median": max_over_mean.median,
                    "min": max_over_mean.min,
                    "max": max_over_mean.max,
                    "overall": max_over_mean_overall,
                },
                "groups_per_round": {
                    "median": groups.median,
                    "min": groups.min,
                    "max": groups.max,
                    "mean": ratio(lane.total_groups, analysis.rounds as u128, 0.0),
                },
                "role_homogeneous_groups": {
                    "median": role_homogeneous_groups.median,
                    "min": role_homogeneous_groups.min,
                    "max": role_homogeneous_groups.max,
                    "fraction_overall": ratio(
                        lane.total_role_homogeneous_groups,
                        lane.total_groups,
                        0.0,
                    ),
                },
                "mean_kinds_per_group": {
                    "median": mean_kinds_per_group.median,
                    "min": mean_kinds_per_group.min,
                    "max": mean_kinds_per_group.max,
                    "overall": ratio(lane.total_kind_count, lane.total_groups, 0.0),
                },
            })
        })
        .collect::<Vec<_>>();

    let report = serde_json::json!({
        "record": "s0_lpshape",
        "measurement_class": "deterministic_simulation_semantics_counts_only",
        "timing_measurement": false,
        "config": cli.config,
        "backend": cli.backend,
        "workers": cli.data_workers(),
        "exclusive_horizon_ns": cli.exclusive_horizon_ns,
        "rounds": analysis.rounds,
        "total_transitions": count(analysis.total_events),
        "complete_state": {
            "debug_bytes": complete_state.bytes,
            "fnv1a64": format!("{:016x}", complete_state.fnv1a64),
        },
        "active_lps_per_round": {
            "median": common.active_lps.median,
            "min": common.active_lps.min,
            "max": common.active_lps.max,
            "mean": ratio(
                analysis.total_active_lp_rounds,
                analysis.rounds as u128,
                0.0,
            ),
            "total": count(analysis.total_active_lp_rounds),
        },
        "transitions_per_round": {
            "median": common.events_processed.median,
            "min": common.events_processed.min,
            "max": common.events_processed.max,
            "mean": ratio(analysis.total_events, analysis.rounds as u128, 0.0),
        },
        "host_active_lps_per_round": distribution_json(common.host_active_lps),
        "switch_active_lps_per_round": distribution_json(common.switch_active_lps),
        "fallback_classified_pushes": {
            "per_round": distribution_json(common.fallback_classified_pushes),
            "total": count(analysis.total_fallback_classified_pushes),
        },
        "same_time_continuations": {
            "total": count(analysis.total_same_time_continuations),
            "fraction_of_transitions": ratio(
                analysis.total_same_time_continuations,
                analysis.total_events,
                0.0,
            ),
            "per_round_fraction": distribution_json(same_time_coverage),
        },
        "lanes": lanes,
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("LP-shape report must serialize")
    );
}

fn emit(cli: &Cli, mut analysis: Analysis) {
    let lane_widths = cli
        .lane_widths
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "record=t20a_lp_shape_protocol config={} backend={} workers={} lane_widths={} rounds={} \
         instrumentation=off features=none",
        cli.config, cli.backend, cli.workers, lane_widths, analysis.rounds,
    );

    let common = CommonSummaries {
        active_lps: analysis.active_lps.summary(),
        events_processed: analysis.events_processed.summary(),
        host_active_lps: analysis.host_active_lps.summary(),
        switch_active_lps: analysis.switch_active_lps.summary(),
        fallback_classified_pushes: analysis.fallback_classified_pushes.summary(),
    };
    let mean_active_lps = ratio(
        analysis.total_active_lp_rounds,
        analysis.rounds as u128,
        0.0,
    );

    for lane in &mut analysis.lanes {
        let lane_utilisation = lane.lane_utilisation.summary();
        let role_homogeneous_groups = lane.role_homogeneous_groups.summary();
        let groups = lane.groups.summary();
        let mean_kinds_per_group = lane.mean_kinds_per_group.summary();
        let max_over_mean = lane.max_over_mean.summary();
        let lane_utilisation_overall = ratio(analysis.total_events, lane.total_capacity, 1.0);
        let role_homogeneous_fraction_overall =
            ratio(lane.total_role_homogeneous_groups, lane.total_groups, 0.0);
        let mean_kinds_per_group_overall = ratio(lane.total_kind_count, lane.total_groups, 0.0);
        let mean_groups = ratio(lane.total_groups, analysis.rounds as u128, 0.0);
        let max_over_mean_overall = if lane.total_max_over_mean_groups == 0 {
            0.0
        } else {
            lane.total_max_over_mean_sum / lane.total_max_over_mean_groups as f64
        };

        println!(
            "record=t20a_lp_shape config={} backend={} workers={} lane_width={} rounds={} \
             total_events={} active_lps_median={:.6} active_lps_min={:.0} active_lps_max={:.0} \
             total_active_lp_rounds={} mean_active_lps={:.6} events_processed_median={:.6} \
             events_processed_min={:.0} events_processed_max={:.0} host_active_lps_median={:.6} \
             host_active_lps_min={:.0} host_active_lps_max={:.0} total_host_active_lps={} \
             switch_active_lps_median={:.6} switch_active_lps_min={:.0} \
             switch_active_lps_max={:.0} total_switch_active_lps={} \
             fallback_classified_pushes_median={:.6} fallback_classified_pushes_min={:.0} \
             fallback_classified_pushes_max={:.0} total_fallback_classified_pushes={} \
             lane_utilisation_median={:.6} lane_utilisation_min={:.6} \
             lane_utilisation_max={:.6} lane_utilisation_overall={:.6} \
             role_homogeneous_groups_median={:.6} role_homogeneous_groups_min={:.0} \
             role_homogeneous_groups_max={:.0} total_role_homogeneous_groups={} \
             role_homogeneous_fraction_overall={:.6} groups_median={:.6} groups_min={:.0} \
             groups_max={:.0} total_groups={} mean_groups={:.6} mean_kinds_per_group_median={:.6} \
             mean_kinds_per_group_min={:.6} mean_kinds_per_group_max={:.6} \
             mean_kinds_per_group_overall={:.6} max_over_mean_median={:.6} \
             max_over_mean_min={:.6} max_over_mean_max={:.6} max_over_mean_overall={:.6}",
            cli.config,
            cli.backend,
            cli.data_workers(),
            lane.width,
            analysis.rounds,
            analysis.total_events,
            common.active_lps.median,
            common.active_lps.min,
            common.active_lps.max,
            analysis.total_active_lp_rounds,
            mean_active_lps,
            common.events_processed.median,
            common.events_processed.min,
            common.events_processed.max,
            common.host_active_lps.median,
            common.host_active_lps.min,
            common.host_active_lps.max,
            analysis.total_host_active_lps,
            common.switch_active_lps.median,
            common.switch_active_lps.min,
            common.switch_active_lps.max,
            analysis.total_switch_active_lps,
            common.fallback_classified_pushes.median,
            common.fallback_classified_pushes.min,
            common.fallback_classified_pushes.max,
            analysis.total_fallback_classified_pushes,
            lane_utilisation.median,
            lane_utilisation.min,
            lane_utilisation.max,
            lane_utilisation_overall,
            role_homogeneous_groups.median,
            role_homogeneous_groups.min,
            role_homogeneous_groups.max,
            lane.total_role_homogeneous_groups,
            role_homogeneous_fraction_overall,
            groups.median,
            groups.min,
            groups.max,
            lane.total_groups,
            mean_groups,
            mean_kinds_per_group.median,
            mean_kinds_per_group.min,
            mean_kinds_per_group.max,
            mean_kinds_per_group_overall,
            max_over_mean.median,
            max_over_mean.min,
            max_over_mean.max,
            max_over_mean_overall,
        );
    }

    let same_time_coverage = analysis.same_time_coverage.summary();
    println!(
        "record=t20a_same_time config={} backend={} workers={} rounds={} \
         coverage_median={:.6} coverage_min={:.6} coverage_max={:.6} coverage_overall={:.6} \
         total_continuations={} total_events={}",
        cli.config,
        cli.backend,
        cli.data_workers(),
        analysis.rounds,
        same_time_coverage.median,
        same_time_coverage.min,
        same_time_coverage.max,
        ratio(
            analysis.total_same_time_continuations,
            analysis.total_events,
            0.0,
        ),
        analysis.total_same_time_continuations,
        analysis.total_events,
    );
}

fn cpu_round_counter_record(cli: &Cli, summary: &CpuRoundCounterSummary) -> String {
    let rounds = summary.rounds as u128;
    let owner_merge_ns_per_message = ratio(
        summary.owner_merge_ns_total,
        summary.messages_exchanged_total,
        0.0,
    );
    let coordinator_exchange_ns_per_message = ratio(
        summary.coordinator_exchange_ns_total,
        summary.messages_exchanged_total,
        0.0,
    );
    // owner_merge_ns sums CPU time across worker threads, while round_wall_time_ns is wall time.
    // Its share can legitimately exceed 1.0 at high worker counts, so emit it without clamping.
    let owner_merge_share_of_round_wall = ratio(
        summary.owner_merge_ns_total,
        summary.round_wall_ns_total,
        0.0,
    );
    let coordinator_exchange_share_of_round_wall = ratio(
        summary.coordinator_exchange_ns_total,
        summary.round_wall_ns_total,
        0.0,
    );
    let backend_note = if cli.backend == "scalar" {
        " backend_note=scalar_has_no_cpu_pool_counters"
    } else {
        ""
    };

    format!(
        "record=t20a_cpu_round_counters config={} backend={} workers={} rounds={} \
         round_wall_ns_total={} round_wall_ns_per_round={:.6} \
         owner_merge_ns_total={} owner_merge_ns_per_round={:.6} \
         owner_merge_ns_median={:.6} owner_merge_ns_min={:.0} owner_merge_ns_max={:.0} \
         early_owner_merge_ns_total={} early_owner_merge_ns_per_round={:.6} \
         coordinator_exchange_ns_total={} coordinator_exchange_ns_per_round={:.6} \
         coordinator_exchange_ns_median={:.6} coordinator_exchange_ns_min={:.0} \
         coordinator_exchange_ns_max={:.0} coordinator_partition_ns_total={} \
         coordinator_partition_ns_per_round={:.6} worker_wait_ns_total={} \
         worker_wait_ns_per_round={:.6} messages_exchanged_total={} \
         messages_exchanged_per_round={:.6} owner_batches_merged_total={} \
         early_owner_batches_merged_total={} owner_merge_ns_per_message={:.6} \
         coordinator_exchange_ns_per_message={:.6} owner_merge_share_of_round_wall={:.6} \
         coordinator_exchange_share_of_round_wall={:.6}{}",
        cli.config,
        cli.backend,
        cli.data_workers(),
        summary.rounds,
        summary.round_wall_ns_total,
        ratio(summary.round_wall_ns_total, rounds, 0.0),
        summary.owner_merge_ns_total,
        ratio(summary.owner_merge_ns_total, rounds, 0.0),
        summary.owner_merge_ns.median,
        summary.owner_merge_ns.min,
        summary.owner_merge_ns.max,
        summary.early_owner_merge_ns_total,
        ratio(summary.early_owner_merge_ns_total, rounds, 0.0),
        summary.coordinator_exchange_ns_total,
        ratio(summary.coordinator_exchange_ns_total, rounds, 0.0),
        summary.coordinator_exchange_ns.median,
        summary.coordinator_exchange_ns.min,
        summary.coordinator_exchange_ns.max,
        summary.coordinator_partition_ns_total,
        ratio(summary.coordinator_partition_ns_total, rounds, 0.0),
        summary.worker_wait_ns_total,
        ratio(summary.worker_wait_ns_total, rounds, 0.0),
        summary.messages_exchanged_total,
        ratio(summary.messages_exchanged_total, rounds, 0.0),
        summary.owner_batches_merged_total,
        summary.early_owner_batches_merged_total,
        owner_merge_ns_per_message,
        coordinator_exchange_ns_per_message,
        owner_merge_share_of_round_wall,
        coordinator_exchange_share_of_round_wall,
        backend_note,
    )
}

fn main() {
    let cli = Cli::parse();
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&cli.config);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));

    let mut node_slots = HashMap::with_capacity(image.nodes.len());
    let mut node_kinds = Vec::with_capacity(image.nodes.len());
    for (slot, node) in image.nodes.iter().enumerate() {
        assert!(
            node_slots.insert(node.id, slot).is_none(),
            "image contains duplicate node {:?}",
            node.id
        );
        node_kinds.push(node.kind);
    }

    let mut csv = cli.per_round_csv.as_ref().map(|csv_path| {
        BufWriter::new(
            File::create(csv_path)
                .unwrap_or_else(|error| panic!("failed to create {}: {error}", csv_path.display())),
        )
    });

    let (analysis, cpu_round_counters, complete_state) = match cli.backend.as_str() {
        "scalar" => {
            let run = run_scalar_rounds_with_observations(
                &image,
                cli.exclusive_horizon_ns,
                ObservationMode::Summary,
            )
            .expect("scalar round run must succeed");
            let complete_state = fingerprint(&run.result);
            let analysis = analyze_rounds(
                run.rounds.iter(),
                run.rounds.len(),
                &node_slots,
                &node_kinds,
                &cli.lane_widths,
                csv.as_mut(),
            );
            let counters =
                summarize_cpu_round_counters(run.rounds.iter().map(CpuRoundCounterSample::scalar));
            analysis.map(|analysis| (analysis, counters, complete_state))
        }
        "cpu" => {
            let run = run_cpu(
                &image,
                cli.exclusive_horizon_ns,
                CpuConfig {
                    workers: cli.workers,
                    ..CpuConfig::default()
                },
            )
            .expect("CPU round run must succeed");
            let complete_state = fingerprint(&run.result);
            let analysis = analyze_rounds(
                run.rounds.iter().map(|round| &round.semantic),
                run.rounds.len(),
                &node_slots,
                &node_kinds,
                &cli.lane_widths,
                csv.as_mut(),
            );
            let counters =
                summarize_cpu_round_counters(run.rounds.iter().map(CpuRoundCounterSample::from));
            analysis.map(|analysis| (analysis, counters, complete_state))
        }
        _ => unreachable!(),
    }
    .expect("failed to write per-round CSV");

    if let Some(writer) = csv.as_mut() {
        writer.flush().expect("failed to flush per-round CSV");
    }
    if cli.json {
        emit_json(&cli, analysis, complete_state);
    } else {
        emit(&cli, analysis);
        println!(
            "record=t20a_complete_state config={} backend={} workers={} result_bytes={} \
             result_fnv1a64={:016x}",
            cli.config,
            cli.backend,
            cli.data_workers(),
            complete_state.bytes,
            complete_state.fnv1a64,
        );
        let counter_record = cpu_round_counter_record(&cli, &cpu_round_counters);
        println!("{counter_record}");
    }
}

#[cfg(test)]
mod tests {
    use days_executor::NodeKind;

    use super::{
        ActiveEntry, Cli, CpuRoundCounterSample, cpu_round_counter_record, fingerprint, median,
        summarize_cpu_round_counters, summarize_lane,
    };

    #[test]
    fn complete_state_fingerprint_matches_the_frontier_contract() {
        let value = vec![1_u64, 2, 3];

        let actual = fingerprint(&value);

        assert_eq!(actual.bytes, "[\n    1,\n    2,\n    3,\n]".len() as u64);
        assert_eq!(actual.fnv1a64, 0x11d1_ee2a_8792_4525);
    }

    #[test]
    fn lane_summary_uses_actual_short_group_length() {
        let entries = [
            ActiveEntry::new(0, NodeKind::Host, 1),
            ActiveEntry::new(1, NodeKind::Host, 3),
            ActiveEntry::new(2, NodeKind::Switch, 2),
        ];

        let summary = summarize_lane(&entries, 2);

        assert_eq!(summary.groups, 2);
        assert_eq!(summary.role_homogeneous_groups, 2);
        assert_eq!(summary.capacity, 8);
        assert_eq!(summary.lane_utilisation, 0.75);
        assert_eq!(summary.mean_kinds_per_group, 1.0);
        assert_eq!(summary.max_over_mean, 1.25);
    }

    #[test]
    fn lane_summary_counts_mixed_roles() {
        let entries = [
            ActiveEntry::new(0, NodeKind::Host, 2),
            ActiveEntry::new(1, NodeKind::Switch, 2),
        ];

        let summary = summarize_lane(&entries, 2);

        assert_eq!(summary.role_homogeneous_groups, 0);
        assert_eq!(summary.mean_kinds_per_group, 2.0);
        assert_eq!(summary.max_over_mean, 1.0);
    }

    #[test]
    fn zero_work_has_defined_lane_values() {
        let entries = [
            ActiveEntry::new(0, NodeKind::Host, 0),
            ActiveEntry::new(1, NodeKind::Switch, 0),
        ];

        let summary = summarize_lane(&entries, 2);

        assert_eq!(summary.lane_utilisation, 1.0);
        assert_eq!(summary.max_over_mean, 0.0);
        assert_eq!(summary.max_over_mean_groups, 0);
    }

    #[test]
    fn median_averages_the_two_middle_values() {
        let mut values = vec![9.0, 1.0, 7.0, 3.0];

        assert_eq!(median(&mut values), 5.0);
    }

    #[test]
    fn cpu_round_counter_summary_aggregates_totals_means_and_medians() {
        let summary = summarize_cpu_round_counters([
            CpuRoundCounterSample {
                round_wall_time_ns: 100,
                owner_merge_ns: 30,
                early_owner_merge_ns: 5,
                coordinator_exchange_ns: 2,
                coordinator_partition_ns: 7,
                worker_wait_ns: 20,
                messages_exchanged: 3,
                owner_batches_merged: 2,
                early_owner_batches_merged: 1,
            },
            CpuRoundCounterSample {
                round_wall_time_ns: 200,
                owner_merge_ns: 50,
                early_owner_merge_ns: 7,
                coordinator_exchange_ns: 6,
                coordinator_partition_ns: 9,
                worker_wait_ns: 40,
                messages_exchanged: 5,
                owner_batches_merged: 3,
                early_owner_batches_merged: 2,
            },
        ]);

        assert_eq!(summary.rounds, 2);
        assert_eq!(summary.round_wall_ns_total, 300);
        assert_eq!(summary.owner_merge_ns_total, 80);
        assert_eq!(summary.early_owner_merge_ns_total, 12);
        assert_eq!(summary.coordinator_exchange_ns_total, 8);
        assert_eq!(summary.coordinator_partition_ns_total, 16);
        assert_eq!(summary.worker_wait_ns_total, 60);
        assert_eq!(summary.messages_exchanged_total, 8);
        assert_eq!(summary.owner_batches_merged_total, 5);
        assert_eq!(summary.early_owner_batches_merged_total, 3);
        assert_eq!(summary.owner_merge_ns.median, 40.0);
        assert_eq!(summary.owner_merge_ns.min, 30.0);
        assert_eq!(summary.owner_merge_ns.max, 50.0);
        assert_eq!(summary.coordinator_exchange_ns.median, 4.0);
        assert_eq!(summary.coordinator_exchange_ns.min, 2.0);
        assert_eq!(summary.coordinator_exchange_ns.max, 6.0);
    }

    #[test]
    fn scalar_round_counter_record_zeroes_cpu_pool_fields() {
        let cli = Cli {
            config: "example.toml".to_owned(),
            backend: "scalar".to_owned(),
            workers: 18,
            lane_widths: vec![8],
            per_round_csv: None,
            exclusive_horizon_ns: None,
            json: false,
        };
        let summary = summarize_cpu_round_counters([CpuRoundCounterSample {
            messages_exchanged: 7,
            ..CpuRoundCounterSample::default()
        }]);

        let record = cpu_round_counter_record(&cli, &summary);

        assert!(record.starts_with(
            "record=t20a_cpu_round_counters config=example.toml backend=scalar workers=1 rounds=1"
        ));
        assert!(record.contains("owner_merge_ns_total=0 owner_merge_ns_per_round=0.000000"));
        assert!(
            record.contains("messages_exchanged_total=7 messages_exchanged_per_round=7.000000")
        );
        assert!(record.contains("owner_merge_ns_per_message=0.000000"));
        assert!(record.ends_with("backend_note=scalar_has_no_cpu_pool_counters"));
    }
}

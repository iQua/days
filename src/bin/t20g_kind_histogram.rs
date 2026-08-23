//! Analysis-only scalar transition histogram for the Stage 0 O0.4 probe.
//!
//! This binary intentionally records no clock. It executes the canonical safe-horizon scalar path
//! with opt-in counters and refuses to emit JSON unless the frozen transition and complete-state
//! gates match.

use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;

use clap::Parser;
use days::scenario::compile_config;
use days_executor::{
    RunResult, ScalarTransitionHistogram, TransitionKindCounts,
    run_scalar_rounds_with_transition_histogram,
};
use serde::Serialize;

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Parser)]
#[command(about = "Emit an untimed scalar transition-kind and LP-round histogram")]
struct Cli {
    fixture: PathBuf,
    #[arg(long)]
    label: String,
    #[arg(long)]
    exclusive_horizon_ns: Option<u64>,
    #[arg(long)]
    expected_rounds: u64,
    #[arg(long)]
    expected_transitions: u64,
    #[arg(long)]
    expected_result_bytes: u64,
    #[arg(long, value_parser = parse_hex_u64)]
    expected_result_fnv1a64: u64,
}

fn parse_hex_u64(value: &str) -> Result<u64, String> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|error| format!("expected a hexadecimal u64: {error}"))
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
    write!(&mut writer, "{value:#?}").expect("debug serialization length must fit in u64");
    writer.0
}

#[derive(Serialize)]
struct ExactRatio {
    numerator: u128,
    denominator: u64,
    decimal: f64,
}

impl ExactRatio {
    fn new(numerator: u128, denominator: u64) -> Self {
        Self {
            numerator,
            denominator,
            decimal: if denominator == 0 {
                0.0
            } else {
                numerator as f64 / denominator as f64
            },
        }
    }
}

#[derive(Serialize)]
struct KindCount {
    kind: &'static str,
    discriminant: u16,
    count: u64,
    share: ExactRatio,
}

#[derive(Serialize)]
struct HistogramBucket {
    transitions_per_lp_round: u64,
    lp_rounds: u64,
}

#[derive(Serialize)]
struct CompleteStateGate {
    debug_bytes: u64,
    fnv1a64: String,
    expected_debug_bytes: u64,
    expected_fnv1a64: String,
    passed: bool,
}

#[derive(Serialize)]
struct Output {
    schema: &'static str,
    probe: &'static str,
    measurement_class: &'static str,
    fixture: String,
    label: String,
    exclusive_horizon_ns: Option<u64>,
    rounds: u64,
    expected_rounds: u64,
    total_transitions: u64,
    expected_transitions: u64,
    transition_total_gate_passed: bool,
    complete_state: CompleteStateGate,
    event_kind_counts: Vec<KindCount>,
    tx_ready_share: ExactRatio,
    active_lp_rounds: u64,
    lp_round_transition_histogram: Vec<HistogramBucket>,
    worklist_order: &'static str,
    lane_width: usize,
    final_group: &'static str,
    literal_sum_warp_max_ratio: ExactRatio,
    divergence_cap_ratio: ExactRatio,
}

fn kind_counts(counts: TransitionKindCounts, total: u64) -> Vec<KindCount> {
    [
        ("PACKET_ARRIVAL", 0, counts.packet_arrival),
        ("TX_READY", 1, counts.tx_ready),
        ("TX_COMPLETE", 2, counts.tx_complete),
        ("REMOTE_ARRIVAL", 3, counts.remote_arrival),
        ("RETRANSMISSION_TIMEOUT", 4, counts.retransmission_timeout),
        ("PACING_TIMER", 5, counts.pacing_timer),
    ]
    .into_iter()
    .map(|(kind, discriminant, count)| KindCount {
        kind,
        discriminant,
        count,
        share: ExactRatio::new(u128::from(count), total),
    })
    .collect()
}

fn output(cli: &Cli, result: &RunResult, histogram: ScalarTransitionHistogram) -> Output {
    let complete_state = fingerprint(result);
    assert_eq!(
        histogram.rounds, cli.expected_rounds,
        "frozen round-count gate failed"
    );
    assert_eq!(
        histogram.total_transitions, cli.expected_transitions,
        "frozen transition-count gate failed"
    );
    assert_eq!(
        complete_state.bytes, cli.expected_result_bytes,
        "frozen complete-state Debug-byte gate failed"
    );
    assert_eq!(
        complete_state.fnv1a64, cli.expected_result_fnv1a64,
        "frozen complete-state FNV-1a64 gate failed"
    );

    let tx_ready = histogram.kind_counts.tx_ready;
    let transition_histogram = histogram
        .lp_round_transition_histogram
        .into_iter()
        .map(|(transitions_per_lp_round, lp_rounds)| HistogramBucket {
            transitions_per_lp_round,
            lp_rounds,
        })
        .collect();
    Output {
        schema: "days-stage0-kind-histogram-v1",
        probe: "O0.4",
        measurement_class: "simulation semantics only; no clock recorded",
        fixture: cli.fixture.display().to_string(),
        label: cli.label.clone(),
        exclusive_horizon_ns: cli.exclusive_horizon_ns,
        rounds: histogram.rounds,
        expected_rounds: cli.expected_rounds,
        total_transitions: histogram.total_transitions,
        expected_transitions: cli.expected_transitions,
        transition_total_gate_passed: true,
        complete_state: CompleteStateGate {
            debug_bytes: complete_state.bytes,
            fnv1a64: format!("{:016x}", complete_state.fnv1a64),
            expected_debug_bytes: cli.expected_result_bytes,
            expected_fnv1a64: format!("{:016x}", cli.expected_result_fnv1a64),
            passed: true,
        },
        event_kind_counts: kind_counts(histogram.kind_counts, histogram.total_transitions),
        tx_ready_share: ExactRatio::new(u128::from(tx_ready), histogram.total_transitions),
        active_lp_rounds: histogram.active_lp_rounds,
        lp_round_transition_histogram: transition_histogram,
        worklist_order: "ascending NodeId within each round",
        lane_width: 32,
        final_group: "actual active-lane count (not padded to 32), matching T20a",
        literal_sum_warp_max_ratio: ExactRatio::new(
            histogram.warp_max_transitions,
            histogram.total_transitions,
        ),
        divergence_cap_ratio: ExactRatio::new(
            histogram.warp_padded_transitions,
            histogram.total_transitions,
        ),
    }
}

fn main() {
    let cli = Cli::parse();
    let image = compile_config(&cli.fixture)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", cli.fixture.display()));
    let run = run_scalar_rounds_with_transition_histogram(&image, cli.exclusive_horizon_ns)
        .expect("scalar histogram run must succeed");
    let output = output(&cli, &run.result, run.histogram);
    serde_json::to_writer_pretty(std::io::stdout().lock(), &output)
        .expect("histogram JSON must serialize");
    println!();
}

#[cfg(test)]
mod tests {
    use super::parse_hex_u64;

    #[test]
    fn parses_frozen_hex_fingerprints() {
        assert_eq!(parse_hex_u64("0x00ff").unwrap(), 255);
        assert_eq!(
            parse_hex_u64("765408a1fba2d5e6").unwrap(),
            0x765408a1fba2d5e6
        );
        assert!(parse_hex_u64("not-hex").is_err());
    }
}

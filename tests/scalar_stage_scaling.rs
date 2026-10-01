//! P14 scan budget: Scalar stage progress examines a bounded number of table entries per event.
//!
//! A host running a ring all-reduce over `n` ranks owns `2 (n - 1)` stage generators. Before this
//! budget, every stage event scanned the host's whole generator table: activation searched it for
//! the first releasable stage, a completion visited every generator to find its successors, and
//! each segment and ACK of a stage's TCP transport found its generator and receiver by a linear
//! `find`. The work per event therefore grew with the rank count, and a run with it.
//!
//! The measure is deterministic. A test-only probe
//! (`days_executor::scalar::run_scalar_counting_stage_scans_for_testing`) counts the events the
//! run dispatched and the stage-path *visits*, which are exactly:
//! * every element yielded by an iteration of a host's generator or TCP-receiver table. The
//!   stage-path functions reach a host only through the stage view, whose two tables count every
//!   element an `iter`, `iter_mut` or `for` loop yields, so any scan of them is counted, whether
//!   or not it uses the stage index. Positional access (`table[i]`) is O(1) and is not counted;
//! * every entry the stage index reads: one per keyed lookup (the O(log G) comparisons of a
//!   binary search or B-tree descent are not counted), plus each successor-list entry and each
//!   releasable-set member examined;
//! * every pending-cause entry examined by a take, plus the causes left for the progress-only
//!   records.
//!
//! Which functions are on the stage path is fixed by `STAGE_PATH_FUNCTIONS` in `xtask/src/main.rs`.
//! `cargo xtask audit` rejects, inside them, any table scan and any raw access that would bypass
//! the view (`host_state`, `host_state_mut`, `host_states`, `HostState`, raw table slices, a raw
//! `Vec` of causes). The audit is syntactic; this budget counts any scan the audit cannot see.
//! Work outside those functions (switch transitions, retransmission timeouts, DCQCN) is not
//! counted.
//!
//! The gate is the ratio of visits per dispatched event between [`LARGE_RANKS`] and
//! [`SMALL_RANKS`], a 4x step: a per-event scan of the host's table grows with the rank count
//! (about 4x), a keyed lookup does not (about 1x). The bound [`MAX_RATIO`] is their geometric
//! midpoint, 2.
//!
//! How to run each case (both are fast, un-ignored and need the `test` feature):
//! * `cargo test -p days --features test --test scalar_stage_scaling ring_all_reduce`
//! * `cargo test -p days --features test --test scalar_stage_scaling compute_chain`
//! * `cargo test -p days --features test --test scalar_stage_scaling roce_ring`
#![cfg(feature = "test")]

#[path = "collective_tcp.rs"]
#[allow(dead_code)]
mod tcp;

use days_executor::scalar::run_scalar_counting_stage_scans_for_testing;
use days_executor::{
    Backend, GeneratorStatus, HostState, ObservationMode, SimulationImage,
    run_scalar_with_observations, validate,
};

const SMALL_RANKS: u64 = 8;
const LARGE_RANKS: u64 = 32;
/// Geometric midpoint of the keyed (1x) and per-event-scan (4x) ratios for a 4x rank step.
const MAX_RATIO: f64 = 2.0;

/// One ring all-reduce with a 1,000-byte chunk per rank (two 500-byte segments per stage).
fn ring_config(ranks: u64) -> String {
    tcp::tcp_collective_config("RingAllReduce", ranks, ranks * 1_000, 100)
}

/// The same ring over RoCE queue pairs (P15 lane R3): two 500-byte packets per stage, paced at the
/// 8 Gb/s port rate, with a 1 ms retransmission timeout.
fn roce_ring_config(ranks: u64) -> String {
    ring_config(ranks)
        .replace("flow_type = \"TCP\"", "flow_type = \"RoCE\"")
        .replace(
            "[collective.traffic.tcp]\ncc_algorithm = \"TCPReno\"\n",
            r#"[collective.traffic.dcqcn]
rate_gbps = 8.0
min_rate_gbps = 0.01
max_rate_gbps = 8.0
g = 0.00390625
ai_rate_gbps = 0.04
hai_rate_gbps = 0.4
mi_factor = 0.5
rtt_ns = 50000
cnp_interval_ns = 10000
pacing_interval_ns = 500
increase_byte_threshold = 100000

[collective.traffic.roce]
retransmit_timeout_ns = 1000000
"#,
        )
}

/// A forward compute stage, then the ring all-reduce, then a backward compute stage, on every rank.
fn compute_chain_config(ranks: u64) -> String {
    let hosts = (0..ranks)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    ring_config(ranks).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + &format!(
        r#"
[[compute]]
name = "forward"
hosts = [{hosts}]
duration_ns = 5000

[[compute]]
name = "backward"
hosts = [{hosts}]
duration_ns = 7000
after = "grad"
"#
    )
}

struct Probe {
    dispatches: u64,
    visits: u64,
}

impl Probe {
    fn per_event(&self) -> f64 {
        self.visits as f64 / self.dispatches.max(1) as f64
    }
}

fn probe(label: &str, image: &SimulationImage, expected_stages: u64) -> Probe {
    validate(image, Backend::Scalar).unwrap_or_else(|error| panic!("{label}: {error}"));
    let stages = image
        .host_states
        .iter()
        .flat_map(|state| &state.stages)
        .flatten()
        .count() as u64;
    assert_eq!(stages, expected_stages, "{label}: stage count");
    let (result, dispatches, visits) =
        run_scalar_counting_stage_scans_for_testing(image, ObservationMode::Full)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
    // The probe only counts: the run is the ordinary Scalar run.
    let plain = run_scalar_with_observations(image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{label}: {error}"));
    assert!(
        result == plain,
        "{label}: the counting run changed the result"
    );
    let unfinished = result
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
        .filter(|(_, stage)| stage.is_some())
        .filter(|(generator, _)| generator.next_emission.status != GeneratorStatus::Finished)
        .count();
    assert_eq!(unfinished, 0, "{label}: every stage must finish");
    Probe { dispatches, visits }
}

fn check(label: &str, small: Probe, large: Probe) {
    let ratio = large.per_event() / small.per_event();
    println!(
        "record=scalar_stage_scaling case={label} small_ranks={SMALL_RANKS} \
         large_ranks={LARGE_RANKS} small_dispatches={} small_visits={} large_dispatches={} \
         large_visits={} small_per_event={:.3} large_per_event={:.3} ratio={ratio:.3} \
         max_ratio={MAX_RATIO}",
        small.dispatches,
        small.visits,
        large.dispatches,
        large.visits,
        small.per_event(),
        large.per_event(),
    );
    assert!(
        ratio < MAX_RATIO,
        "{label}: stage-path table visits per event grew {ratio:.2}x from {SMALL_RANKS} to \
         {LARGE_RANKS} ranks ({:.2} -> {:.2}); the stage path scans per-host tables per event",
        small.per_event(),
        large.per_event(),
    );
}

#[test]
fn ring_all_reduce_stage_path_visits_per_event_do_not_grow_with_ranks() {
    let run = |ranks: u64| {
        let image = tcp::compile_text("scan-ring", &ring_config(ranks));
        probe("ring", &image, 2 * ranks * (ranks - 1))
    };
    check("ring_all_reduce", run(SMALL_RANKS), run(LARGE_RANKS));
}

#[test]
fn compute_chain_stage_path_visits_per_event_do_not_grow_with_ranks() {
    let run = |ranks: u64| {
        let image = tcp::compile_text("scan-chain", &compute_chain_config(ranks));
        probe("chain", &image, 2 * ranks * (ranks - 1) + 2 * ranks)
    };
    check("compute_chain", run(SMALL_RANKS), run(LARGE_RANKS));
}

/// P15 lane R3: a RoCE stage's data arrivals, releases and completions read the host's tables by
/// key, as TCP's do.
#[test]
fn roce_ring_all_reduce_stage_path_visits_per_event_do_not_grow_with_ranks() {
    let run = |ranks: u64| {
        let config = roce_ring_config(ranks);
        assert!(config.contains("flow_type = \"RoCE\""));
        let image = tcp::compile_text("scan-roce-ring", &config);
        probe("roce ring", &image, 2 * ranks * (ranks - 1))
    };
    check("roce_ring_all_reduce", run(SMALL_RANKS), run(LARGE_RANKS));
}

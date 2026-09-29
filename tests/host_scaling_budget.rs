//! P14 perf budget contract: host lowering, validation and planning stay near-linear in the flow
//! and stage counts.
//!
//! Twice now a per-flow linear scan inside the load-time validator made the host path quadratic at
//! frontier scale: T20j removed ten per-generator rescans of the initial tables, and P14 added a
//! per-flow and per-packet `find` over every generator (`stage_generator`), which took the
//! 262,144-flow frontier's lowering from 3.5 s to 124 s and repeated the cost when the device
//! backends validated the image again. Byte-identity gates cannot see this regression: the output
//! is unchanged, only its cost grows.
//!
//! The contract is a scaling ratio, not a wall-clock threshold. Each case measures a phase at a
//! small and a large size, taking the minimum of a few repetitions, and the large/small ratio must
//! stay below the case's bound. Machine speed cancels in the ratio. The bound is the geometric
//! midpoint of the ratios a linear and a quadratic phase give for the case's size step, so a
//! factor-of-two noise swing on either side still separates the two regimes.
//!
//! A ratio at or above the bound is measured once more, at both sizes, and the phase fails only if
//! the second measurement breaches the bound too. Every measurement, the re-measure included,
//! prints one `record=host_scaling_budget` line.
//!
//! # Frontier cases
//!
//! Scenarios derived from the frontier fixture itself: the same topology, seed, switch, link and
//! traffic block, keeping only the first few of its 32 identical 8,192-flow sets. Phases:
//! * `lowering`: `compile_config`, which includes the Scalar-backend `validate`;
//! * `device_validation`: `validate` for the CUDA backend, which every device run repeats at
//!   executor entry (the Metal plan construction validates for Metal the same way);
//! * `default_sizing`: `size_default_device_plan`;
//! * `metal_plan`: the full Metal plan construction (`size_metal_plan_for_testing`) under the
//!   frontier run protocol's capacity caps. It needs the `metal` feature on Apple hardware; the
//!   hook builds the plan on the host and never creates a Metal device. It is not in CI: the test
//!   process peaked above 10 GB resident at either candidate CI size (1 to 8 and 2 to 16 sets),
//!   beyond a hosted `macos-15` runner's 7 GB (`evidence/P14/ci-scaling.md`).
//!
//! The regression is a quadratic term beside a linear one, `T(n) = a n + b n^2`. With `q = b n / a`
//! at the small size and a size step `k`, the ratio is `(k + k^2 q) / (1 + q)`: it rises from `k`
//! towards `k^2` as the quadratic share grows, so the gate needs a small size where that share is
//! already large. The gate catches the term when `q` reaches about `1 / sqrt(k)`, so a larger step
//! also lowers the share it needs. The CI case steps 2 to 32 sets (16,384 to 262,144 flows, the
//! whole frontier), a 16x step with bound 64. At 2 to 16 sets (8x, bound 22.6) the regression
//! (`c1ec354`) cleared the bound by only 1.4x in lowering on an x86 host, whose scan is cheaper
//! relative to its linear work than on Apple M-series hosts (`evidence/P14/ci-scaling.md`). The
//! full case steps 4 to 32 sets.
//!
//! # Collective case
//!
//! Scalar stage validation of one ring all-reduce at 96 and 192 ranks (see
//! [`ci_scaling_collective_stage_validation`]).
//!
//! # Running
//!
//! Every timing case is `#[ignore]`d, so the default debug matrix runs only the derivation check.
//! The CI-sized cases run in the `scaling` CI job:
//! `cargo test --release -p days --features test --test host_scaling_budget -- --ignored --exact
//! --test-threads=1 --nocapture ci_scaling_frontier_host_phases ci_scaling_collective_stage_validation`
//! On Apple hardware, `--features test,metal` and `frontier_metal_plan_at_ci_sizes` time the Metal
//! plan at the CI sizes. The full frontier case is an explicit run only:
//! `cargo test --release -p days --features test,metal --test host_scaling_budget -- --ignored
//! --exact --nocapture full_frontier_host_phases`.
//! Timing cases must run one at a time (`--test-threads=1`), or they time each other.
#![cfg(feature = "test")]

use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use days::scenario::compile_config;
use days_executor::{Backend, SimulationImage, size_default_device_plan, validate};

const FRONTIER_FIXTURE: &str = "configs/benchmarks/lookahead/rq9_frontier_closed_k32.toml";
const FLOW_SET_HEADER: &str = "[[flow_set]]";
/// Flows per stacked set in the frontier fixture.
const FLOWS_PER_SET: usize = 8_192;
/// Stacked flow sets in the frontier fixture.
const FIXTURE_SETS: usize = 32;
/// The frontier fixture's fat-tree arity, with `k / 2` hosts per edge switch.
const FRONTIER_K: usize = 32;

/// Geometric midpoint of the linear (16x) and quadratic (256x) ratios for a 16x size step, in
/// thousandths.
const SIXTEENFOLD_STEP_MAX_RATIO_MILLI: u128 = 64_000;
/// Geometric midpoint of the linear (8x) and quadratic (64x) ratios for an 8x size step, in
/// thousandths.
const EIGHTFOLD_STEP_MAX_RATIO_MILLI: u128 = 22_600;
/// Geometric midpoint of the linear (4x) and quadratic (16x) ratios for a 4x size step, in
/// thousandths.
const FOURFOLD_STEP_MAX_RATIO_MILLI: u128 = 8_000;

/// A phase effectively free at the small size must not divide by noise.
const RATIO_FLOOR: Duration = Duration::from_millis(1);

/// A sample longer than this ends the repetitions at its size; the phase keeps the minimum of the
/// samples taken, and the record line reports how many there were. No passing sample comes near
/// it: the longest in the verification runs, the whole frontier's lowering on four x86 cores,
/// took 14.8 s, a quarter of the budget. On a regressed tree a large-size sample takes minutes,
/// so the cap bounds how long the failing job runs. Capping only drops samples, so the kept
/// minimum can only rise: at a large size that raises the ratio, towards failing, so the cap can
/// never turn a failure into a pass. The small sizes take about a second and never reach it.
const SAMPLE_BUDGET: Duration = Duration::from_secs(60);

/// One scaling case: the two sizes, their repetitions, and the ratio bound.
struct Case {
    name: &'static str,
    /// What `small` and `large` count.
    unit: &'static str,
    small: usize,
    large: usize,
    small_repetitions: usize,
    large_repetitions: usize,
    max_ratio_milli: u128,
}

/// The CI-sized frontier case: 16,384 and 262,144 flows. The large size runs twice: one run can
/// land in a burst of load from other processes, and on a loaded host a single large lowering
/// once took 2.7x its usual time (`evidence/P14/ci-scaling.md`).
const CI_FRONTIER: Case = Case {
    name: "frontier_ci",
    unit: "flow_sets",
    small: 2,
    large: FIXTURE_SETS,
    small_repetitions: 5,
    large_repetitions: 2,
    max_ratio_milli: SIXTEENFOLD_STEP_MAX_RATIO_MILLI,
};

/// The full frontier case: 32,768 and 262,144 flows.
const FULL_FRONTIER: Case = Case {
    name: "frontier_full",
    unit: "flow_sets",
    small: 4,
    large: FIXTURE_SETS,
    small_repetitions: 3,
    large_repetitions: 1,
    max_ratio_milli: EIGHTFOLD_STEP_MAX_RATIO_MILLI,
};

/// The collective case: one ring all-reduce at 96 and 192 ranks, 18,240 and 73,152 stages. At 48
/// ranks validation took only 3 to 10 ms, so a millisecond of scheduler noise moved the ratio; at
/// 96 ranks it takes tens of milliseconds on a 4-core x86 host (`evidence/P14/ci-scaling.md`).
const CI_COLLECTIVE: Case = Case {
    name: "collective_ci",
    unit: "ranks",
    small: 96,
    large: 192,
    small_repetitions: 5,
    large_repetitions: 3,
    max_ratio_milli: FOURFOLD_STEP_MAX_RATIO_MILLI,
};

/// The topology case: the frontier's traffic on k=16 and k=32 fat-trees with 16 flows per host,
/// 16,384 flows over 1,024 hosts and 131,072 flows over 8,192 hosts (see
/// [`ci_scaling_topology_host_phases`]).
const CI_TOPOLOGY: Case = Case {
    name: "topology_ci",
    unit: "fat_tree_k",
    small: 16,
    large: FRONTIER_K,
    small_repetitions: 5,
    large_repetitions: 2,
    max_ratio_milli: EIGHTFOLD_STEP_MAX_RATIO_MILLI,
};

/// Flows per host in the topology case, at both fabric sizes.
const TOPOLOGY_FLOWS_PER_HOST: usize = 16;

/// The topology case's phases and whether each is gated. Lowering is recorded but not gated
/// until per-flow route search stops being O(flows x switches) (the `p14/route` lane and
/// `tests/route_scaling_budget.rs`); gating it then is a one-line change here.
const TOPOLOGY_PHASES: [(Phase, bool); 3] = [
    (Phase::Lowering, false),
    (Phase::DeviceValidation, true),
    (Phase::DefaultSizing, true),
];

/// A lowered scenario and the config it came from.
struct Scenario {
    path: PathBuf,
    image: SimulationImage,
}

impl Scenario {
    /// Writes and lowers `config`; this untimed lowering also warms the allocator and caches.
    fn lower(name: &str, config: String) -> Self {
        let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}.toml"));
        fs::write(&path, config).expect("write the derived scenario");
        let image = compile_config(&path).unwrap_or_else(|error| panic!("lower {name}: {error}"));
        Self { path, image }
    }

    fn frontier(sets: usize) -> Self {
        let scenario = Self::lower(
            &format!("host_scaling_budget_frontier_{sets}_sets"),
            frontier_with_flow_sets(sets),
        );
        assert_eq!(
            scenario.image.flows.len(),
            sets * FLOWS_PER_SET,
            "{sets} sets: flow count"
        );
        scenario
    }

    fn fat_tree(k: usize) -> Self {
        let scenario = Self::lower(
            &format!("host_scaling_budget_fat_tree_k{k}"),
            fat_tree_frontier(k, TOPOLOGY_FLOWS_PER_HOST),
        );
        let hosts = fat_tree_hosts(k);
        assert_eq!(scenario.image.host_states.len(), hosts, "k={k}: host count");
        assert_eq!(
            scenario.image.flows.len(),
            hosts * TOPOLOGY_FLOWS_PER_HOST,
            "k={k}: flow count"
        );
        scenario
    }

    fn ring_all_reduce(ranks: usize) -> Self {
        let scenario = Self::lower(
            &format!("host_scaling_budget_ring_{ranks}_ranks"),
            ring_all_reduce_config(ranks),
        );
        let stages = scenario
            .image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .filter(|generator| generator.stage.is_some())
            .count();
        assert_eq!(
            stages,
            2 * ranks * (ranks - 1),
            "{ranks} ranks: stage count"
        );
        scenario
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Lowering,
    DeviceValidation,
    ScalarValidation,
    DefaultSizing,
    #[cfg(all(feature = "metal", target_vendor = "apple"))]
    MetalPlan,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Lowering => "lowering",
            Self::DeviceValidation => "device_validation",
            Self::ScalarValidation => "scalar_validation",
            Self::DefaultSizing => "default_sizing",
            #[cfg(all(feature = "metal", target_vendor = "apple"))]
            Self::MetalPlan => "metal_plan",
        }
    }

    /// Up to `repetitions` runs of this phase on `scenario` (see [`SAMPLE_BUDGET`]).
    fn time(self, scenario: &Scenario, repetitions: usize) -> Timing {
        match self {
            Self::Lowering => sample(repetitions, || {
                compile_config(&scenario.path).expect("lower the scenario")
            }),
            Self::DeviceValidation => sample(repetitions, || {
                validate(&scenario.image, Backend::Cuda).expect("CUDA validation")
            }),
            Self::ScalarValidation => sample(repetitions, || {
                validate(&scenario.image, Backend::Scalar).expect("Scalar validation")
            }),
            Self::DefaultSizing => sample(repetitions, || {
                size_default_device_plan(&scenario.image).expect("default device plan")
            }),
            #[cfg(all(feature = "metal", target_vendor = "apple"))]
            Self::MetalPlan => metal_plan_time(&scenario.image, repetitions),
        }
    }
}

/// The samples of one phase at one size.
#[derive(Clone, Copy)]
struct Timing {
    min: Duration,
    max: Duration,
    samples: usize,
}

/// Times up to `repetitions` calls, stopping after the first sample longer than
/// [`SAMPLE_BUDGET`]; each result is dropped outside the timed region.
fn sample<T>(repetitions: usize, mut phase: impl FnMut() -> T) -> Timing {
    assert!(repetitions > 0, "at least one repetition");
    let mut timing = Timing {
        min: Duration::MAX,
        max: Duration::ZERO,
        samples: 0,
    };
    while timing.samples < repetitions {
        let started = Instant::now();
        let result = black_box(phase());
        let elapsed = started.elapsed();
        drop(result);
        timing.min = timing.min.min(elapsed);
        timing.max = timing.max.max(elapsed);
        timing.samples += 1;
        if elapsed > SAMPLE_BUDGET {
            break;
        }
    }
    timing
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
fn metal_plan_time(image: &SimulationImage, repetitions: usize) -> Timing {
    use days_executor::{
        DeviceCapacityCaps, MetalConfig, ObservationMode, size_metal_plan_for_testing,
    };
    // The frontier run protocol's caps: the `days` CLI's defaults with
    // `--channel-events-per-stream 256`. Without caps (the executor-level default),
    // `derived_remote_capacities` reserves remote staging per flow and per route link, and
    // `prepare_streams` writes one state word per staging slot. That plan is linear in the flow
    // count but hundreds of GB at the frontier, so its planning time then most likely depends on
    // memory pressure rather than on the planner's work per flow. The gate plans under the caps.
    let config = MetalConfig {
        capacity_caps: DeviceCapacityCaps {
            fallback_fel_events_per_lp: Some(16_384),
            queue_packets_per_lp: Some(2_048),
            channel_events_per_stream: Some(256),
            remote_staging_events_per_lp: Some(2_048),
            outbox_events_total: Some(2_000_000),
            tcp_receiver_ranges_per_flow: Some(64),
            tcp_ledger_segments_per_flow: Some(4_096),
            observation_events_per_lp: Some(512),
        },
        ..MetalConfig::default()
    };
    sample(repetitions, || {
        size_metal_plan_for_testing(image, None, config, ObservationMode::Summary)
            .expect("Metal plan")
    })
}

/// Prints one measurement's record and returns whether it breaches the case's bound.
fn record(
    case: &Case,
    phase: &str,
    attempt: u32,
    gated: bool,
    (small, large): (Timing, Timing),
) -> bool {
    let ratio_milli = large.min.as_nanos() * 1_000 / small.min.max(RATIO_FLOOR).as_nanos();
    let breach = ratio_milli >= case.max_ratio_milli;
    println!(
        "record=host_scaling_budget case={} phase={phase} attempt={attempt} unit={} small={} \
         large={} small_ns={} large_ns={} ratio={} max_ratio={} gated={gated} breach={breach} \
         small_samples={} large_samples={} small_max_ns={} large_max_ns={}",
        case.name,
        case.unit,
        case.small,
        case.large,
        small.min.as_nanos(),
        large.min.as_nanos(),
        milli(ratio_milli),
        milli(case.max_ratio_milli),
        small.samples,
        large.samples,
        small.max.as_nanos(),
        large.max.as_nanos(),
    );
    breach
}

fn milli(value: u128) -> String {
    format!("{}.{:03}", value / 1_000, value % 1_000)
}

/// Gates one phase: a breach is measured once more at both sizes, and only a repeated breach
/// records a failure. An ungated phase is timed once per size and only recorded.
fn gate(
    case: &Case,
    (phase, gated): (Phase, bool),
    small: &Scenario,
    large: &Scenario,
    failures: &mut Vec<String>,
) {
    if !gated {
        record(
            case,
            phase.label(),
            1,
            false,
            (phase.time(small, 1), phase.time(large, 1)),
        );
        return;
    }
    let measure = || {
        (
            phase.time(small, case.small_repetitions),
            phase.time(large, case.large_repetitions),
        )
    };
    let first = measure();
    if !record(case, phase.label(), 1, true, first) {
        return;
    }
    let second = measure();
    if record(case, phase.label(), 2, true, second) {
        failures.push(format!(
            "{} {}: {}/{} {} time ratio breached {} twice ({} ms -> {} ms, then {} ms -> {} ms); \
             the phase is super-linear",
            case.name,
            phase.label(),
            case.large,
            case.small,
            case.unit,
            milli(case.max_ratio_milli),
            first.0.min.as_millis(),
            first.1.min.as_millis(),
            second.0.min.as_millis(),
            second.1.min.as_millis(),
        ));
    }
}

fn run_frontier(case: &Case, phases: &[Phase]) {
    let small = Scenario::frontier(case.small);
    let large = Scenario::frontier(case.large);
    let mut failures = Vec::new();
    for &phase in phases {
        gate(case, (phase, true), &small, &large, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const FRONTIER_HOST_PHASES: [Phase; 3] = [
    Phase::Lowering,
    Phase::DeviceValidation,
    Phase::DefaultSizing,
];

/// The frontier fixture with only its first `sets` stacked flow sets.
fn frontier_with_flow_sets(sets: usize) -> String {
    let text = fs::read_to_string(repo_path(FRONTIER_FIXTURE)).expect("read the frontier fixture");
    let mut blocks = text.split(FLOW_SET_HEADER);
    let prefix = blocks.next().expect("the fixture has a prefix");
    let blocks = blocks.collect::<Vec<_>>();
    assert_eq!(
        blocks.len(),
        FIXTURE_SETS,
        "the frontier fixture must stack exactly {FIXTURE_SETS} flow sets"
    );
    assert!(
        blocks
            .windows(2)
            .all(|pair| pair[0].trim_end() == pair[1].trim_end()),
        "the frontier fixture's stacked flow sets must be identical"
    );
    let mut derived = prefix.to_owned();
    for block in &blocks[..sets] {
        derived.push_str(FLOW_SET_HEADER);
        derived.push_str(block);
    }
    derived
}

/// Hosts of a k-ary fat-tree with `k / 2` hosts per edge switch.
fn fat_tree_hosts(k: usize) -> usize {
    k * k * k / 4
}

/// The frontier fixture on a k-ary fat-tree with `flows_per_host` stacked flow sets, each with one
/// flow per host. At the fixture's own arity this is the fixture's first `flows_per_host` sets.
fn fat_tree_frontier(k: usize, flows_per_host: usize) -> String {
    let text = frontier_with_flow_sets(flows_per_host);
    let substitutions = [
        (format!("k = {FRONTIER_K}\n"), format!("k = {k}\n"), 1),
        (
            format!("hosts_per_edge = {}\n", FRONTIER_K / 2),
            format!("hosts_per_edge = {}\n", k / 2),
            1,
        ),
        (
            format!("flow_count = {FLOWS_PER_SET}\n"),
            format!("flow_count = {}\n", fat_tree_hosts(k)),
            flows_per_host,
        ),
    ];
    substitutions
        .iter()
        .fold(text, |text, (from, to, expected)| {
            assert_eq!(text.matches(from.as_str()).count(), *expected, "{from:?}");
            text.replace(from.as_str(), to)
        })
}

fn repo_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

#[test]
fn derived_scenarios_are_the_frontier_with_fewer_flow_sets() {
    let full = fs::read_to_string(repo_path(FRONTIER_FIXTURE)).expect("read the frontier fixture");
    assert_eq!(frontier_with_flow_sets(FIXTURE_SETS), full);
    for sets in [
        CI_FRONTIER.small,
        CI_FRONTIER.large,
        FULL_FRONTIER.small,
        FULL_FRONTIER.large,
    ] {
        let derived = frontier_with_flow_sets(sets);
        assert_eq!(derived.matches(FLOW_SET_HEADER).count(), sets);
        assert!(
            full.starts_with(&derived),
            "{sets} sets: a prefix of the fixture"
        );
    }
    assert_eq!(
        fat_tree_frontier(FRONTIER_K, TOPOLOGY_FLOWS_PER_HOST),
        frontier_with_flow_sets(TOPOLOGY_FLOWS_PER_HOST)
    );
    let small = fat_tree_frontier(CI_TOPOLOGY.small, TOPOLOGY_FLOWS_PER_HOST);
    assert!(small.contains("k = 16\nhosts_per_edge = 8\n"));
    assert_eq!(
        small.matches("flow_count = 1024\n").count(),
        TOPOLOGY_FLOWS_PER_HOST
    );
}

#[test]
#[ignore = "CI scaling gate (the `scaling` job): run with --release --test-threads=1"]
fn ci_scaling_frontier_host_phases() {
    run_frontier(&CI_FRONTIER, &FRONTIER_HOST_PHASES);
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
#[ignore = "explicit Metal-plan scaling budget on Apple hardware: run with --release"]
fn frontier_metal_plan_at_ci_sizes() {
    run_frontier(&CI_FRONTIER, &[Phase::MetalPlan]);
}

#[test]
#[ignore = "explicit P14 perf frontier-scale host budget: run with --release"]
fn full_frontier_host_phases() {
    run_frontier(
        &FULL_FRONTIER,
        &[
            Phase::Lowering,
            Phase::DeviceValidation,
            Phase::DefaultSizing,
            #[cfg(all(feature = "metal", target_vendor = "apple"))]
            Phase::MetalPlan,
        ],
    );
}

/// Host phases as the fabric grows with the flows. The flow-only frontier case keeps one k=32
/// fabric at both sizes, so a per-flow scan over hosts, nodes or links costs it only linear extra
/// work and passes; here k=16 to k=32 multiplies hosts, switch-port nodes, links and flows by 8,
/// with 16 flows per host at both sizes.
///
/// Bounds, from `T = a n + b n^2` with every entity count `n` growing 8x:
/// * `device_validation`: its checks are per flow (routes of at most six links), per node, per
///   link and per initial packet, so work linear in the entities grows 8x, while a term pairing
///   two of them (flows x hosts, links x links) grows 64x. The bound is their geometric midpoint,
///   22.6.
/// * `default_sizing`: per-flow packet counts and per-node, per-link and per-channel capacities,
///   the same 8x against 64x, so the same bound. (Switches grow only 4x, as `5 k^2 / 4`; a flows x
///   switches term gives 32x, still above the bound.)
///
/// The fixed tree measures 11 to 14x rather than 8x for both phases (the k=16 working set fits in
/// cache and the maps add a logarithmic factor), which leaves the gate a margin of about 1.6x.
///
/// **Detection floor.** A term is caught only once its share `q` of the phase at k=16 reaches
/// `(22.6 - R) / (64 - 22.6)`, where `R` is the fixed tree's ratio: about 0.25 for validation.
/// On four x86 cores that is a per-flow scan over hosts costing about 3 us per flow at 1,024
/// hosts, which at the frontier (262,144 flows over 8,192 hosts) adds about 6 s to every
/// validation, more than the whole fixed validation. A scratch mutant repeating a per-flow
/// `host_states` scan confirms it: at 4.9 s per frontier validation the case passed, at 9.9 s it
/// failed twice, and the flow-only case passed it. Realistic per-flow scans over hosts cost far
/// less: a single `position` over `nodes` or over `host_states` added 0.36 to 1.5 s per
/// validation at the frontier, and both pass this case (`evidence/P14/ci-scaling.md`).
/// A ratio gate catches gross complexity regressions only. The deterministic budgets
/// (`scalar_stage_scaling`, `collective_lowering_budget` and `route_scaling_budget`) are the
/// fine-grained guards.
///
/// **Lowering** is recorded (`gated=false`) but not gated: lowering routes every flow with its own
/// search over the switch graph, which allocates and scans O(switches) per flow, so it grows 17
/// to 18x here today, and 30x from k=32 to k=64. The `p14/route` lane fixes that; gating lowering
/// afterwards is a one-line change in [`TOPOLOGY_PHASES`].
#[test]
#[ignore = "CI scaling gate (the `scaling` job): run with --release --test-threads=1"]
fn ci_scaling_topology_host_phases() {
    let case = &CI_TOPOLOGY;
    let small = Scenario::fat_tree(case.small);
    let large = Scenario::fat_tree(case.large);
    let mut failures = Vec::new();
    for phase in TOPOLOGY_PHASES {
        gate(case, phase, &small, &large, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// One ring all-reduce over `ranks` hosts on a single switch: `2 * ranks * (ranks - 1)` TCP stages.
fn ring_all_reduce_config(ranks: usize) -> String {
    let switch = ranks;
    let edges = (0..ranks)
        .map(|host| format!("[{host}, {switch}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let hosts = (0..ranks)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sinks = (0..ranks)
        .map(|host| ((host + 1) % ranks).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
seed = 26
edges = [{edges}]
hosts = [{hosts}]
duration = 0.05

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[collective]]
collective_type = "RingAllReduce"
flow_type = "TCP"
flow_count = {ranks}
sources = [{hosts}]
sinks = [{sinks}]

[collective.traffic]
initial_delay = 0.0
size = {size}
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 500, high = 500 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"
"#,
        size = ranks * 1_000,
    )
}

/// The stage validators look up predecessors by collective position and by flow. A per-stage scan
/// of every generator makes validation quadratic in the stage count, which grows as the square
/// of the rank count: doubling the ranks quadruples the stages, so linear work gives about 4x and
/// a quadratic term 16x. The bound is their geometric midpoint.
///
/// Only validation is gated. Collective lowering is recorded beside it (`gated=false`) but not
/// bounded: every stage's flow key embeds a clone of its collective's key, rank-length source and
/// sink lists included, so the canonical flow sort and the predecessor lookups cost O(ranks) per
/// comparison and lowering grows as stages x ranks (`evidence/P14/perf-fix.md`, open finding).
/// Being informational, it is timed once per size.
#[test]
#[ignore = "CI scaling gate (the `scaling` job): run with --release --test-threads=1"]
fn ci_scaling_collective_stage_validation() {
    let case = &CI_COLLECTIVE;
    let small = Scenario::ring_all_reduce(case.small);
    let large = Scenario::ring_all_reduce(case.large);
    let mut failures = Vec::new();
    for phase in [(Phase::Lowering, false), (Phase::ScalarValidation, true)] {
        gate(case, phase, &small, &large, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

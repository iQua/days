//! P14 perf budget contract: host lowering and planning stay near-linear in the flow count.
//!
//! Twice now a per-flow linear scan inside the load-time validator made the host path quadratic at
//! frontier scale: T20j removed ten per-generator rescans of the initial tables, and P14 added a
//! per-flow and per-packet `find` over every generator (`stage_generator`), which took the
//! 262,144-flow frontier's lowering from 3.5 s to 124 s and repeated the cost when the device
//! backends validated the image again. Byte-identity gates cannot see this regression: the output
//! is unchanged, only its cost grows.
//!
//! The contract is a scaling ratio, not a wall-clock threshold. Two frontier-shaped scenarios are
//! derived from the frontier fixture itself: the same topology, seed, switch, link and traffic
//! block, with 4 and with all 32 of its stacked 8,192-flow sets (32,768 and 262,144 TCP flows, an
//! 8x step). Each host phase is timed at both sizes, taking the minimum of a few repetitions, and
//! the large/small ratio must stay below [`MAX_RATIO`]. The large size runs once: its phases take
//! seconds, so their relative noise is far smaller than the small size's. Linear work gives about 8x and `n log n`
//! work about 9.6x (log2 262,144 / log2 32,768 = 18/15); a quadratic term gives 64x. The bound
//! sits at the geometric midpoint, so machine speed cancels and a factor-2 noise swing on either
//! side still separates the two regimes.
//!
//! Phases:
//! * **lowering**: `compile_config`, which includes the Scalar-backend `validate`;
//! * **device validation**: `validate` for the CUDA backend, which every device run repeats at
//!   executor entry (the Metal plan construction below validates for Metal the same way);
//! * **host planning**: `size_default_device_plan`, and the full Metal plan construction
//!   (`size_metal_plan_for_testing`) where that hook is compiled.
//!
//! The frontier sizes are expensive in an unoptimized build, so the gate is explicit:
//! `cargo test --release -p days --features test --test host_scaling_budget -- --ignored`
//! (add `metal` on Apple hardware to include the Metal plan construction).
#![cfg(feature = "test")]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use days::scenario::compile_config;
use days_executor::{Backend, SimulationImage, size_default_device_plan, validate};

const FRONTIER_FIXTURE: &str = "configs/benchmarks/lookahead/rq9_frontier_closed_k32.toml";
const FLOW_SET_HEADER: &str = "[[flow_set]]";
/// Flows per stacked set in the frontier fixture.
const FLOWS_PER_SET: usize = 8_192;
const SMALL_SETS: usize = 4;
const LARGE_SETS: usize = 32;
/// Geometric midpoint of the linear (8x) and quadratic (64x) ratios for an 8x size step.
const MAX_RATIO: f64 = 22.6;
const SMALL_REPETITIONS: usize = 3;
const LARGE_REPETITIONS: usize = 1;

/// The frontier fixture with only its first `sets` stacked flow sets.
fn frontier_with_flow_sets(sets: usize) -> String {
    let text = fs::read_to_string(repo_path(FRONTIER_FIXTURE)).expect("read the frontier fixture");
    let mut blocks = text.split(FLOW_SET_HEADER);
    let prefix = blocks.next().expect("the fixture has a prefix");
    let blocks = blocks.collect::<Vec<_>>();
    assert_eq!(
        blocks.len(),
        LARGE_SETS,
        "the frontier fixture must stack exactly {LARGE_SETS} flow sets"
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

fn repo_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn write_scenario(sets: usize) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("host_scaling_budget_frontier_{sets}_sets.toml"));
    fs::write(&path, frontier_with_flow_sets(sets)).expect("write the derived scenario");
    path
}

/// Minimum wall time of `repetitions` calls, and the last call's value.
fn min_time<T>(repetitions: usize, mut phase: impl FnMut() -> T) -> (Duration, T) {
    let mut best = Duration::MAX;
    let mut value = None;
    for _ in 0..repetitions {
        let started = Instant::now();
        let result = phase();
        best = best.min(started.elapsed());
        value = Some(result);
    }
    (best, value.expect("at least one repetition"))
}

struct Phases {
    lowering: Duration,
    device_validation: Duration,
    sizing: Duration,
    metal_plan: Option<Duration>,
}

fn measure(sets: usize, repetitions: usize) -> (Phases, SimulationImage) {
    let path = write_scenario(sets);
    let (lowering, image) = min_time(repetitions, || {
        compile_config(&path).unwrap_or_else(|error| panic!("lower {sets} sets: {error}"))
    });
    assert_eq!(
        image.flows.len(),
        sets * FLOWS_PER_SET,
        "{sets} sets: flow count"
    );
    let (device_validation, ()) = min_time(repetitions, || {
        validate(&image, Backend::Cuda).expect("CUDA validation");
    });
    let (sizing, _) = min_time(repetitions, || {
        size_default_device_plan(&image).expect("default device plan")
    });
    let metal_plan = metal_plan_time(&image, repetitions);
    (
        Phases {
            lowering,
            device_validation,
            sizing,
            metal_plan,
        },
        image,
    )
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
fn metal_plan_time(image: &SimulationImage, repetitions: usize) -> Option<Duration> {
    use days_executor::{MetalConfig, ObservationMode, size_metal_plan_for_testing};
    let (time, _) = min_time(repetitions, || {
        size_metal_plan_for_testing(
            image,
            None,
            MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("Metal plan")
    });
    Some(time)
}

#[cfg(not(all(feature = "metal", target_vendor = "apple")))]
fn metal_plan_time(_image: &SimulationImage, _repetitions: usize) -> Option<Duration> {
    None
}

fn check(label: &str, small: Duration, large: Duration, failures: &mut Vec<String>) {
    // A floor keeps a phase that is effectively free at the small size from dividing by noise.
    let floor = Duration::from_millis(1);
    let ratio = large.as_secs_f64() / small.max(floor).as_secs_f64();
    println!(
        "record=host_scaling_budget phase={label} small_sets={SMALL_SETS} large_sets={LARGE_SETS} \
         small_ns={} large_ns={} ratio={ratio:.3} max_ratio={MAX_RATIO}",
        small.as_nanos(),
        large.as_nanos(),
    );
    if ratio >= MAX_RATIO {
        failures.push(format!(
            "{label}: {LARGE_SETS}/{SMALL_SETS} flow-set time ratio {ratio:.1} >= {MAX_RATIO} \
             ({} ms -> {} ms); the phase is super-linear in the flow count",
            small.as_millis(),
            large.as_millis(),
        ));
    }
}

#[test]
fn derived_scenarios_are_the_frontier_with_fewer_flow_sets() {
    let full = fs::read_to_string(repo_path(FRONTIER_FIXTURE)).expect("read the frontier fixture");
    assert_eq!(frontier_with_flow_sets(LARGE_SETS), full);
    let small = frontier_with_flow_sets(SMALL_SETS);
    assert_eq!(small.matches(FLOW_SET_HEADER).count(), SMALL_SETS);
    assert!(full.starts_with(&small));
}

#[test]
#[ignore = "explicit P14 perf frontier-scale host budget: run with --release"]
fn frontier_host_lowering_and_planning_scale_near_linearly_in_flows() {
    let (small, _) = measure(SMALL_SETS, SMALL_REPETITIONS);
    let (large, _) = measure(LARGE_SETS, LARGE_REPETITIONS);
    let mut failures = Vec::new();
    check("lowering", small.lowering, large.lowering, &mut failures);
    check(
        "device_validation",
        small.device_validation,
        large.device_validation,
        &mut failures,
    );
    check("default_sizing", small.sizing, large.sizing, &mut failures);
    if let (Some(small_plan), Some(large_plan)) = (small.metal_plan, large.metal_plan) {
        check("metal_plan", small_plan, large_plan, &mut failures);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

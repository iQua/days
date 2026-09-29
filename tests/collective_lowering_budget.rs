//! P14 coll budget contract: collective lowering grows linearly in its stage count, in memory, in
//! allocation work and in time.
//!
//! One ring all-reduce over `n` ranks lowers to `2n(n - 1)` stages. Two costs used to grow as
//! stages x ranks, i.e. as `n^3`:
//! * every stage's flow key embedded a clone of its collective's key, rank-length `sources` and
//!   `sinks` included, and each stage held about four such keys (its own, two predecessors, and the
//!   dense-id map's copy), so key storage and every flow-sort comparison and predecessor lookup
//!   scaled with the rank count;
//! * every stage searched for its route, and the search on the ring's star topology expands every
//!   leaf, so routing cost O(ranks) per stage although a rank's stages all share one endpoint pair.
//!
//! The contract is a scaling ratio between two sizes, 32 and 256 ranks: 1,984 and 130,560 stages,
//! a 65.8x stage step for an 8x rank step. Linear-in-stages growth gives 65.8x; stages x ranks
//! gives 526x. [`max_ratio`] is their geometric midpoint, 186x, the convention of
//! `tests/host_scaling_budget.rs`. Fixed per-stage costs (flow inputs, routes, generator state)
//! dilute a super-linear term at small rank counts, which is why the small size is not smaller.
//! Measured ratios before and after each fix are in `days-gpu/evidence/P14/coll-lowering.md`.
//!
//! Every case lowers with `RouteWorkers::serial()`, which computes routes inline on the calling
//! thread and lowers the same image bytes as every other route budget (`RouteWorkers`). Parallel
//! route workers divide per-flow work by the core count, which would hide a per-flow super-linear
//! term and make the ratio depend on the machine.
//!
//! * **Memory** runs in the default test matrix and is deterministic: a pure function of the code
//!   and the scenario. This binary's global allocator counts, on the lowering thread only, the peak
//!   of live heap bytes and the cumulative bytes allocated. Serial routes put every allocation of
//!   the lowering on that thread, and no other test in the binary can perturb the thread-local
//!   counts. The cumulative count tracks work that allocates as it goes, such as route searches.
//! * **Time** is lowering wall time, the minimum of a few repetitions at each size. It is noisy on a
//!   shared machine, so it is an explicit case.
//!
//! How to run each case:
//! * memory (deterministic; default matrix, any profile):
//!   `cargo test -p days --test collective_lowering_budget`
//! * time (explicit; release, one test thread so the memory case cannot share the CPU):
//!   `cargo test --release -p days --test collective_lowering_budget -- --ignored --test-threads=1`
//! * path identity (default matrix): `serial_route_lowering_is_the_compile_config_image` checks
//!   that the serial-route image equals `compile_config`'s for both budget scenarios.
//! * both, as a scaling job runs them:
//!   `cargo test --release -p days --test collective_lowering_budget -- --include-ignored --test-threads=1`
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use days::scenario::{compile_config, compile_config_with_route_workers};
use days::topos::route::RouteWorkers;

/// Live and peak heap bytes allocated by the current thread.
///
/// The counters are thread-local, so tests that run beside this one in the same binary cannot
/// perturb them, and nothing is shared between threads.
struct ThreadCountingAllocator;

thread_local! {
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
    static PEAK_BYTES: Cell<isize> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation(bytes: usize) {
    let _ = ALLOCATED_BYTES.try_with(|total| total.set(total.get().wrapping_add(bytes)));
}

fn record(delta: isize) {
    let _ = LIVE_BYTES.try_with(|live| {
        let now = live.get().wrapping_add(delta);
        live.set(now);
        let _ = PEAK_BYTES.try_with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        record(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            record(new_size as isize - layout.size() as isize);
            record_allocation(new_size);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

/// One ring all-reduce over `ranks` hosts on a single switch: `2 * ranks * (ranks - 1)` TCP stages.
///
/// The same scenario as `tests/host_scaling_budget.rs`'s ring case. Each test passes its own
/// `test` label, so tests running in parallel never write the same file.
fn ring_all_reduce_scenario(test: &str, ranks: u64) -> PathBuf {
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
    let config = format!(
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
    );
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "collective_lowering_budget_{test}_ring_{ranks}_ranks.toml"
    ));
    fs::write(&path, config).expect("write the ring scenario");
    path
}

fn stages(ranks: u64) -> u64 {
    2 * ranks * (ranks - 1)
}

/// Heap use of one serial lowering.
struct HeapUse {
    /// Peak live bytes above the bytes live when the lowering starts.
    peak_bytes: u64,
    /// Bytes requested by every allocation and reallocation, freed or not.
    allocated_bytes: u64,
}

fn lowering_heap_use(ranks: u64) -> HeapUse {
    let path = ring_all_reduce_scenario("heap", ranks);
    let baseline = LIVE_BYTES.with(Cell::get);
    PEAK_BYTES.with(|peak| peak.set(baseline));
    let allocated_before = ALLOCATED_BYTES.with(Cell::get);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .unwrap_or_else(|error| panic!("lower {ranks} ranks: {error}"));
    let peak = PEAK_BYTES.with(Cell::get);
    let allocated = ALLOCATED_BYTES.with(Cell::get) - allocated_before;
    let generators = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| generator.stage.is_some())
        .count() as u64;
    assert_eq!(generators, stages(ranks), "{ranks} ranks: stage count");
    drop(image);
    HeapUse {
        peak_bytes: u64::try_from(peak - baseline).expect("the peak is at least the baseline"),
        allocated_bytes: allocated as u64,
    }
}

const SMALL_RANKS: u64 = 32;
const LARGE_RANKS: u64 = 256;
const TIME_REPETITIONS: usize = 3;

/// Geometric midpoint of the linear-in-stages ratio and the stages x ranks ratio.
fn max_ratio() -> f64 {
    let stage_ratio = stages(LARGE_RANKS) as f64 / stages(SMALL_RANKS) as f64;
    let rank_ratio = LARGE_RANKS as f64 / SMALL_RANKS as f64;
    stage_ratio * rank_ratio.sqrt()
}

/// Records one phase's ratio and returns a failure message when it reaches [`max_ratio`].
fn check(label: &str, unit: &str, small: f64, large: f64) -> Option<String> {
    let ratio = large / small;
    let max_ratio = max_ratio();
    println!(
        "record=collective_lowering_budget phase={label} small_ranks={SMALL_RANKS} \
         large_ranks={LARGE_RANKS} small_stages={} large_stages={} small_{unit}={small:.0} \
         large_{unit}={large:.0} ratio={ratio:.3} max_ratio={max_ratio:.3}",
        stages(SMALL_RANKS),
        stages(LARGE_RANKS),
    );
    (ratio >= max_ratio).then(|| {
        format!(
            "{label}: {LARGE_RANKS}/{SMALL_RANKS}-rank ratio {ratio:.1} >= {max_ratio:.1} \
             ({small:.0} -> {large:.0} {unit}); collective lowering is super-linear in its stage \
             count"
        )
    })
}

#[test]
fn collective_lowering_heap_scales_linearly_in_stages() {
    let small = lowering_heap_use(SMALL_RANKS);
    let large = lowering_heap_use(LARGE_RANKS);
    let failures = [
        check(
            "lowering_peak_heap",
            "bytes",
            small.peak_bytes as f64,
            large.peak_bytes as f64,
        ),
        check(
            "lowering_allocated",
            "bytes",
            small.allocated_bytes as f64,
            large.allocated_bytes as f64,
        ),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Minimum serial-route lowering wall time over [`TIME_REPETITIONS`] lowerings.
fn lowering_time(ranks: u64) -> Duration {
    let path = ring_all_reduce_scenario("time", ranks);
    (0..TIME_REPETITIONS)
        .map(|_| {
            let started = Instant::now();
            let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
                .unwrap_or_else(|error| panic!("lower {ranks} ranks: {error}"));
            let elapsed = started.elapsed();
            drop(image);
            elapsed
        })
        .min()
        .expect("at least one repetition")
}

#[test]
#[ignore = "explicit P14 coll collective lowering time budget: run with --release --test-threads=1"]
fn collective_lowering_time_scales_linearly_in_stages() {
    let small = lowering_time(SMALL_RANKS);
    let large = lowering_time(LARGE_RANKS);
    if let Some(failure) = check(
        "lowering_time",
        "ns",
        small.as_nanos() as f64,
        large.as_nanos() as f64,
    ) {
        panic!("{failure}");
    }
}

/// The measured path is production's: for each budget scenario, the serial-route lowering the
/// cases above measure produces the same image as `compile_config`, which uses the host's route
/// workers.
#[test]
fn serial_route_lowering_is_the_compile_config_image() {
    for ranks in [SMALL_RANKS, LARGE_RANKS] {
        let path = ring_all_reduce_scenario("path_identity", ranks);
        let production =
            compile_config(&path).unwrap_or_else(|error| panic!("lower {ranks} ranks: {error}"));
        let measured = compile_config_with_route_workers(&path, RouteWorkers::serial())
            .unwrap_or_else(|error| panic!("lower {ranks} ranks serially: {error}"));
        assert!(
            production == measured,
            "{ranks} ranks: the serial-route image differs from compile_config's"
        );
    }
}

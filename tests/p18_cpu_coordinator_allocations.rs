//! P18 cpu-levers C1: the CPU coordinator allocates nothing per round.
//!
//! The coordinator of the static CPU path runs on the calling thread. Until C1 it allocated
//! seven vectors every round (completion flags, local minima, busy times, the workers' outbox
//! metadata, inbound busy times, next batch counts and inbox minima), and freed each worker's
//! per-round outbox-metadata vector, allocated on the worker's thread. With 16 workers that cost
//! about 10 k instructions per round on dp1, a sixth of the serial coordinator
//! (`days-gpu/evidence/P18/dp1-profile/raw/cg-tables/coord-cpu16-dp1-30ms.txt`).
//!
//! **What is measured.** The allocations this thread makes during a two-worker `run_cpu` (run
//! totals, the default) of an E1-long-shaped scenario on a k = 4 fat tree, at two exclusive
//! horizons, `SHORT_NS` and `2 * SHORT_NS`: about 1,000 more rounds. The counters are
//! thread-local, so the workers' threads and other tests cannot perturb them.
//!
//! **The cap.** `MAX_MARGINAL_ALLOCATIONS` on the difference: well under one allocation per added
//! round. Before C1 the difference is about seven per added round.
//!
//! Run: `cargo test -p days --test p18_cpu_coordinator_allocations` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, run_cpu};

struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
}

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            record_allocation();
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

const E1LONG_FIXTURE: &str = "configs/benchmarks/evaluation/e1long_open_k32_load_30.toml";
/// About 1,000 rounds at the fixture's 1,000-ns propagation lookahead.
const SHORT_NS: u64 = 1_000_000;
/// The cap on the allocation difference between the two horizons, for about 1,000 added rounds.
const MAX_MARGINAL_ALLOCATIONS: u64 = 64;

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
        "p18_cpu_coordinator_allocations_k4_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived scenario");
    path
}

/// Allocations this (the coordinator's) thread makes during a two-worker run to `horizon_ns`.
fn coordinator_allocations(image: &days_executor::SimulationImage, horizon_ns: u64) -> u64 {
    let config = CpuConfig {
        workers: 2,
        ..CpuConfig::default()
    };
    let before = allocations();
    let run = run_cpu(image, Some(horizon_ns), config).expect("the CPU run succeeds");
    let after = allocations();
    drop(run);
    after - before
}

#[test]
fn the_cpu_coordinator_allocates_nothing_per_round() {
    let path = e1long_k4();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived scenario lowers");
    let _ = fs::remove_file(&path);
    let short = coordinator_allocations(&image, SHORT_NS);
    let long = coordinator_allocations(&image, 2 * SHORT_NS);
    let marginal = long.saturating_sub(short);
    println!(
        "coordinator allocations: {short} to {SHORT_NS} ns, {long} to {} ns, marginal {marginal}",
        2 * SHORT_NS
    );
    assert!(
        marginal <= MAX_MARGINAL_ALLOCATIONS,
        "the coordinator made {marginal} more allocations over about 1,000 more rounds"
    );
}

//! P18 cpu-levers W2: CPU workers reuse their per-round buffers.
//!
//! Until W2 a worker allocated, every round: three vectors per LP it drained (the children of an
//! event, the LP's outbox, superseded timers), one outbox vector per owner, and the owner freed
//! each batch vector on its own thread. The libc and std residue grew with the worker count, from
//! 176 to 382 instructions per event at 1 to 16 workers on dp1
//! (`days-gpu/evidence/P18/dp1-profile/tables/worker-scaling.md`).
//!
//! **What is measured.** Allocations on every thread (process-wide atomics; this binary holds one
//! test) during a two-worker `run_cpu` of an E1-long-shaped scenario on a k = 4 fat tree at two
//! exclusive horizons, `SHORT_NS` and `2 * SHORT_NS`: about 1,000 more rounds and 230,000 more
//! events. Some allocation per round remains by design elsewhere (a packet store that empties
//! drops its slab, FP2), so the cap is a rate, not zero.
//!
//! Run: `cargo test -p days --test p18_cpu_worker_allocations` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, run_cpu};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwarded with the caller's pointer, layout and size.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const E1LONG_FIXTURE: &str = "configs/benchmarks/evaluation/e1long_open_k32_load_30.toml";
const SHORT_NS: u64 = 1_000_000;
/// The cap on allocations per added round, every thread together: one. Before W2 a run made
/// about 172 per round (two workers, about 74 active LPs per round).
const MAX_ALLOCATIONS_PER_ADDED_ROUND: u64 = 1;
/// About 1,000 rounds per added millisecond at the fixture's 1,000-ns lookahead.
const ADDED_ROUNDS: u64 = 1_000;

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
        "p18_cpu_worker_allocations_k4_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived scenario");
    path
}

fn run_allocations(image: &days_executor::SimulationImage, horizon_ns: u64) -> u64 {
    let config = CpuConfig {
        workers: 2,
        ..CpuConfig::default()
    };
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    let run = run_cpu(image, Some(horizon_ns), config).expect("the CPU run succeeds");
    let after = ALLOCATIONS.load(Ordering::Relaxed);
    drop(run);
    after - before
}

#[test]
fn cpu_workers_reuse_their_round_buffers() {
    let path = e1long_k4();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived scenario lowers");
    let _ = fs::remove_file(&path);
    let short = run_allocations(&image, SHORT_NS);
    let long = run_allocations(&image, 2 * SHORT_NS);
    let marginal = long.saturating_sub(short);
    println!(
        "allocations, every thread: {short} to {SHORT_NS} ns, {long} to {} ns, marginal {marginal}",
        2 * SHORT_NS
    );
    assert!(
        marginal <= MAX_ALLOCATIONS_PER_ADDED_ROUND * ADDED_ROUNDS,
        "{marginal} more allocations over about {ADDED_ROUNDS} more rounds"
    );
}

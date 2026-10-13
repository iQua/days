//! P18 cpu-levers M: a default CPU run keeps no per-round records, so its peak live heap does not
//! grow with the number of rounds it runs.
//!
//! Until P18 every CPU run kept one `CpuRoundMetrics` per round, with a list of every active LP's
//! work, a list reserved for LP wall clocks and a worker-timing list, until the caller dropped the
//! run (`CpuMetricsRetention::All`, the only mode). The `days` CLI and the speed harnesses only
//! fold those records into run totals; on the corpus the retained bytes were derived at 1.4 GB
//! (dp1) to 27 GB (M2s full) (`days-gpu/evidence/P18/cpu-levers/design.md`, §2.1).
//!
//! **What is measured.** The process-wide peak live heap during a one-worker `run_cpu` of an
//! E1-long-shaped scenario on a k = 4 fat tree, at two exclusive horizons, `SHORT_NS` and
//! `2 * SHORT_NS`. The open-loop load is steady, so the run's own state (LPs, queues, the result)
//! is about the same size at both horizons, and the difference is what the run keeps per round.
//! This binary holds one test, so nothing else allocates while it measures; the worker thread's
//! allocations are counted too (the counters are process-wide atomics).
//!
//! **The cap.** `MAX_MARGINAL_BYTES` on the peak difference. With per-round records the
//! difference is at least the 400-B record of each added round (about 1,000 added rounds at a
//! 1,000-ns lookahead) plus its lists, so the base fails by a wide margin; without them it is the
//! drift of the steady-state queues between the two horizons.
//!
//! Run: `cargo test -p days --test p18_cpu_round_records_budget` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, run_cpu};

struct PeakCountingAllocator;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grew(bytes: usize) {
    let live = LIVE.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

fn shrank(bytes: usize) {
    LIVE.fetch_sub(bytes as u64, Ordering::Relaxed);
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for PeakCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grew(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grew(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        shrank(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            shrank(layout.size());
            grew(new_size);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: PeakCountingAllocator = PeakCountingAllocator;

const E1LONG_FIXTURE: &str = "configs/benchmarks/evaluation/e1long_open_k32_load_30.toml";
const K: u64 = 4;
const HOSTS_PER_EDGE: u64 = 2;
/// The shorter horizon: about 1,000 rounds at the fixture's 1,000-ns propagation lookahead.
const SHORT_NS: u64 = 1_000_000;
/// The cap on the peak-live difference between the two horizons: 64 KiB, under one quarter of
/// the 400-B records alone of the ~1,000 added rounds.
const MAX_MARGINAL_BYTES: u64 = 64 * 1024;

fn e1long_k4() -> PathBuf {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(E1LONG_FIXTURE))
        .expect("read the E1-long fixture");
    let substitutions = [
        ("\nk = 32\n", format!("\nk = {K}\n")),
        (
            "\nhosts_per_edge = 16\n",
            format!("\nhosts_per_edge = {HOSTS_PER_EDGE}\n"),
        ),
        (
            "\nflow_count = 8192\n",
            format!("\nflow_count = {}\n", K * K / 2 * HOSTS_PER_EDGE),
        ),
    ];
    let text = substitutions.iter().fold(text, |text, (from, to)| {
        assert_eq!(text.matches(from).count(), 1, "{from:?}");
        text.replace(from, to)
    });
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p18_cpu_round_records_budget_k{K}_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived scenario");
    path
}

/// Peak live heap above the starting live heap during a one-worker CPU run to `horizon_ns`.
fn run_peak_bytes(image: &days_executor::SimulationImage, horizon_ns: u64) -> u64 {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let config = CpuConfig {
        workers: 1,
        ..CpuConfig::default()
    };
    let run = run_cpu(image, Some(horizon_ns), config).expect("the CPU run succeeds");
    let peak = PEAK.load(Ordering::Relaxed) - before;
    drop(run);
    peak
}

#[test]
fn a_default_cpu_run_keeps_no_per_round_records() {
    let path = e1long_k4();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived scenario lowers");
    let _ = fs::remove_file(&path);
    let short = run_peak_bytes(&image, SHORT_NS);
    let long = run_peak_bytes(&image, 2 * SHORT_NS);
    let marginal = long.saturating_sub(short);
    println!(
        "peak live heap: {short} B at {SHORT_NS} ns, {long} B at {} ns, marginal {marginal} B",
        2 * SHORT_NS
    );
    assert!(
        marginal <= MAX_MARGINAL_BYTES,
        "the peak live heap grew by {marginal} B ({short} B at {SHORT_NS} ns, {long} B at {} ns): \
         the run keeps per-round records",
        2 * SHORT_NS
    );
}

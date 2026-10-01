//! Scratch harness (evidence tooling, never committed to `days`): per-phase instructions and
//! cycles of one process, read with `proc_pid_rusage(RUSAGE_INFO_V4)` around each phase.
//!
//! Phases: `lower` = `compile_config_with_route_workers(RouteWorkers::serial())` (single-threaded,
//! includes its own `validate`); `validate` = a second `validate(&image, Backend::Scalar)`;
//! `cpu_w1` = `run_cpu(workers = 1)`; `scalar` = `run_scalar`. Copied to `examples/` of a scratch
//! worktree and run with `cargo run --release --example e1_phase_icount -- <fixture> <reps>`.
use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{Backend, CpuConfig, run_cpu, run_scalar, validate};

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Process-wide allocation counters (scratch tooling only: the simulator itself shares nothing).
struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
/// Allocation count and bytes by requested size (sizes 0..4096 in 1 B bins, larger in one bin).
static SIZE_COUNT: [AtomicU64; 4097] = [const { AtomicU64::new(0) }; 4097];
static SIZE_BYTES: [AtomicU64; 4097] = [const { AtomicU64::new(0) }; 4097];
/// Requested sizes of the first 4,096 allocations above 4,096 B, in allocation order.
static LARGE: [AtomicU64; 4096] = [const { AtomicU64::new(0) }; 4096];
static LARGE_NEXT: AtomicU64 = AtomicU64::new(0);
fn note_size(size: usize) {
    if size > 4096 {
        let slot = LARGE_NEXT.fetch_add(1, Relaxed) as usize;
        if slot < LARGE.len() {
            LARGE[slot].store(size as u64, Relaxed);
        }
    }
    let bin = size.min(4096);
    SIZE_COUNT[bin].fetch_add(1, Relaxed);
    SIZE_BYTES[bin].fetch_add(size as u64, Relaxed);
}
fn size_snapshot() -> Vec<(u64, u64)> {
    (0..4097)
        .map(|bin| (SIZE_COUNT[bin].load(Relaxed), SIZE_BYTES[bin].load(Relaxed)))
        .collect()
}
fn size_report(rep: usize, phase: &str, before: &[(u64, u64)]) {
    for (bin, now) in size_snapshot().iter().enumerate() {
        let count = now.0 - before[bin].0;
        if count > 0 {
            println!(
                "record=size rep={rep} phase={phase} size={bin} count={count} bytes={}",
                now.1 - before[bin].1
            );
        }
    }
}
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOCATED.fetch_add(layout.size() as u64, Relaxed);
        note_size(layout.size());
        let live = LIVE.fetch_add(layout.size() as u64, Relaxed) + layout.size() as u64;
        PEAK.fetch_max(live, Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as u64, Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOCATED.fetch_add(new_size as u64, Relaxed);
        note_size(new_size);
        let live = LIVE.fetch_add(new_size as u64, Relaxed) + new_size as u64;
        PEAK.fetch_max(live, Relaxed);
        LIVE.fetch_sub(layout.size() as u64, Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}
#[global_allocator]
static GLOBAL: Counting = Counting;

/// (allocations, bytes allocated, live bytes) now; resets the peak to the live bytes.
fn heap_mark() -> (u64, u64, u64) {
    let live = LIVE.load(Relaxed);
    PEAK.store(live, Relaxed);
    (ALLOCS.load(Relaxed), ALLOCATED.load(Relaxed), live)
}

fn heap_report(rep: usize, phase: &str, mark: (u64, u64, u64)) {
    let peak = PEAK.load(Relaxed);
    let live = LIVE.load(Relaxed);
    println!(
        "record=heap rep={rep} phase={phase} allocs={} bytes_allocated={} peak_over_start={} live_delta={}",
        ALLOCS.load(Relaxed) - mark.0,
        ALLOCATED.load(Relaxed) - mark.1,
        peak - mark.2,
        live as i64 - mark.2 as i64
    );
}

const RUSAGE_INFO_V4: i32 = 4;
unsafe extern "C" {
    fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut u64) -> i32;
    fn getrusage(who: i32, buffer: *mut i64) -> i32;
}

/// Minor page faults of this process so far (`getrusage(RUSAGE_SELF).ru_minflt`).
fn minor_faults() -> i64 {
    // `struct rusage` on 64-bit Darwin: two `timeval`s (32 B), then 14 `long`s; `ru_minflt` is
    // the fifth long.
    let mut buffer = [0_i64; 18];
    // SAFETY: the buffer is at least `sizeof(struct rusage)` (144 B).
    let rc = unsafe { getrusage(0, buffer.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage failed");
    buffer[4 + 4]
}

/// (instructions, cycles) of this process so far.
fn counters() -> (u64, u64) {
    let mut buffer = [0_u64; 64];
    // SAFETY: the buffer is larger than `rusage_info_v4`.
    let rc = unsafe {
        proc_pid_rusage(
            std::process::id() as i32,
            RUSAGE_INFO_V4,
            buffer.as_mut_ptr(),
        )
    };
    assert_eq!(rc, 0, "proc_pid_rusage failed");
    // 16-byte uuid, then 29 u64 fields before ri_instructions and ri_cycles.
    (buffer[2 + 29], buffer[2 + 30])
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("fixture path");
    let reps: usize = args.next().map_or(1, |value| value.parse().expect("reps"));
    let phases = args
        .next()
        .unwrap_or_else(|| "lower,validate,cpu_w1,scalar".to_owned());
    for rep in 0..reps {
        let faults = minor_faults();
        let heap = heap_mark();
        let start = counters();
        let image =
            compile_config_with_route_workers(&path, RouteWorkers::serial()).expect("lower");
        let lowered = counters();
        heap_report(rep, "lower", heap);
        println!(
            "record=faults rep={rep} phase=lower minor_faults={}",
            minor_faults() - faults
        );
        println!(
            "record=phase rep={rep} phase=lower instructions={} cycles={}",
            lowered.0 - start.0,
            lowered.1 - start.1
        );
        if phases.contains("validate") {
            let before = counters();
            validate(&image, Backend::Scalar).expect("valid");
            let after = counters();
            println!(
                "record=phase rep={rep} phase=validate instructions={} cycles={}",
                after.0 - before.0,
                after.1 - before.1
            );
        }
        if let Some(loops) = phases.strip_prefix("lower_loop=") {
            let loops: usize = loops.parse().expect("loop count");
            for iteration in 0..loops {
                let before = counters();
                let again = compile_config_with_route_workers(&path, RouteWorkers::serial())
                    .expect("lower");
                let after = counters();
                drop(again);
                println!(
                    "record=phase rep={rep} phase=lower_iter{iteration} instructions={} cycles={}",
                    after.0 - before.0,
                    after.1 - before.1
                );
            }
        }
        if let Some(loops) = phases.strip_prefix("validate_loop=") {
            let loops: usize = loops.parse().expect("loop count");
            let before = counters();
            for _ in 0..loops {
                validate(&image, Backend::Scalar).expect("valid");
            }
            let after = counters();
            println!(
                "record=phase rep={rep} phase=validate_loop instructions={} cycles={}",
                (after.0 - before.0) / loops as u64,
                (after.1 - before.1) / loops as u64
            );
        }
        if phases.contains("cpu_w1") {
            let sizes = size_snapshot();
            let large_start = LARGE_NEXT.load(Relaxed) as usize;
            let heap = heap_mark();
            let faults = minor_faults();
            let before = counters();
            let run = run_cpu(
                &image,
                None,
                CpuConfig {
                    workers: 1,
                    ..CpuConfig::default()
                },
            )
            .expect("cpu");
            let after = counters();
            println!(
                "record=phase rep={rep} phase=cpu_w1 instructions={} cycles={} sourced={}",
                after.0 - before.0,
                after.1 - before.1,
                run.result.summary.sourced_packets
            );
            heap_report(rep, "cpu_w1", heap);
            println!(
                "record=faults rep={rep} phase=cpu_w1 minor_faults={}",
                minor_faults() - faults
            );
            size_report(rep, "cpu_w1", &sizes);
            let large_end = (LARGE_NEXT.load(Relaxed) as usize).min(LARGE.len());
            for (order, slot) in LARGE[large_start.min(large_end)..large_end]
                .iter()
                .enumerate()
            {
                println!(
                    "record=large rep={rep} phase=cpu_w1 order={order} size={}",
                    slot.load(Relaxed)
                );
            }
            let dropped = counters();
            drop(run);
            let freed = counters();
            println!(
                "record=phase rep={rep} phase=cpu_w1_drop instructions={} cycles={}",
                freed.0 - dropped.0,
                freed.1 - dropped.1
            );
        }
        if phases.contains("scalar") {
            let heap = heap_mark();
            let before = counters();
            let result = run_scalar(&image, None).expect("scalar");
            let after = counters();
            heap_report(rep, "scalar", heap);
            println!(
                "record=phase rep={rep} phase=scalar instructions={} cycles={} sourced={}",
                after.0 - before.0,
                after.1 - before.1,
                result.summary.sourced_packets
            );
        }
    }
}

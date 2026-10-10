//! P17 FP2: a CPU LP's packet store keeps nothing once it drains.
//!
//! Each CPU LP owns a packet store (`executor/src/packet_store.rs`). Above 16 packets the store is
//! a slab behind an index, and both keep their high-water size. Switch ports fill and drain at
//! different times, so with every drained LP still holding its high-water slab the run's peak
//! heap is the sum of the LPs' own peaks, not the peak of their sum: on the E5-wide TCP fixtures
//! (k = 32, 9,472 LPs) the CPU peak rose from 1,141 MB at `5c7629ff` to 1,284 MB at `2c97d6a8`
//! (`days-gpu/evidence/P17/fp2/tables/retention-arms.md`). A store that drops its slab when it
//! empties holds nothing on an idle LP.
//!
//! **What is measured.** The peak of the live heap bytes, above the start of the window, of a
//! one-worker CPU run of an E5-wide-shaped scenario scaled down to k = 8 (4 hosts per edge
//! switch, one 256 KiB CUBIC flow per host, 128 hosts and 80 switches), derived from
//! `e5_wide_k32_q200_cubic.toml` in the test. The allocator counts every thread (the CPU worker
//! runs on its own thread); this binary holds this one test, so nothing else allocates in the
//! window. With one worker the LPs drain in a fixed order; what can vary is when the harness's and
//! the worker's own small allocations interleave with the run's, which moved the measured peak by
//! up to 58 kB (0.6%) between runs.
//!
//! | tree (Mac, rustc 1.99.0, debug and release) | peak above start, bytes |
//! |---|---:|
//! | `2c97d6a8`: the slab is kept when the store empties | 10,126,178 to 10,185,114 |
//! | the slab is dropped when the store empties | 8,936,386 to 8,993,962 |
//!
//! Run: `cargo test -p days --test p17_fp2_cpu_store_budget` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, run_cpu};

/// Live and peak heap bytes of the whole process. Counters only: the allocator itself is the
/// system allocator, and this binary runs one test, so no other test shares the counters.
struct ProcessCountingAllocator;

static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_BYTES: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes as u64, Relaxed) + bytes as u64;
    PEAK_BYTES.fetch_max(live, Relaxed);
}

fn shrink(bytes: usize) {
    LIVE_BYTES.fetch_sub(bytes as u64, Relaxed);
}

unsafe impl GlobalAlloc for ProcessCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        shrink(layout.size());
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            shrink(layout.size());
            grow(new_size);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: ProcessCountingAllocator = ProcessCountingAllocator;

const E5_FIXTURE: &str = "configs/benchmarks/evaluation/e5_wide_k32_q200_cubic.toml";

/// `E5_FIXTURE` at k = 8 with 4 hosts per edge switch and one 256 KiB flow per host, written to
/// a file private to this process.
fn e5_k8() -> std::path::PathBuf {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(E5_FIXTURE))
        .expect("read the E5 fixture");
    let substitutions = [
        ("\nk = 32\n", "\nk = 8\n"),
        ("\nhosts_per_edge = 16\n", "\nhosts_per_edge = 4\n"),
        ("\nflow_count = 8192\n", "\nflow_count = 128\n"),
        ("\nsize = 1048576\n", "\nsize = 262144\n"),
    ];
    let text = substitutions.iter().fold(text, |text, (from, to)| {
        assert_eq!(text.matches(from).count(), 1, "{from:?}");
        text.replace(from, to)
    });
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p17_fp2_cpu_store_budget_e5_k8_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived E5 scenario");
    path
}

/// The cap: the largest measured 8,993,962 B plus about 5% for interleaving and container drift
/// between platforms and toolchains, below the 10,126,178 B of stores that keep their slab when
/// they empty.
const MAX_PEAK_ABOVE_START_BYTES: u64 = 9_450_000;

#[test]
fn a_drained_cpu_packet_store_keeps_no_slab() {
    let path = e5_k8();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived E5 scenario lowers");
    let _ = fs::remove_file(&path);
    let config = CpuConfig {
        workers: 1,
        ..CpuConfig::default()
    };
    let start = LIVE_BYTES.load(Relaxed);
    PEAK_BYTES.store(start, Relaxed);
    let run = run_cpu(&image, None, config).expect("the CPU run succeeds");
    let peak_above_start = PEAK_BYTES.load(Relaxed) - start;
    let departed = run.result.summary.departed_packets;
    drop(run);
    println!(
        "record=p17_fp2_cpu_store_budget departed_packets={departed} \
         peak_above_start_bytes={peak_above_start} cap={MAX_PEAK_ABOVE_START_BYTES}"
    );
    assert!(
        peak_above_start <= MAX_PEAK_ABOVE_START_BYTES,
        "a one-worker CPU run of E5 at k = 8 peaked {peak_above_start} B above its start, above \
         the cap of {MAX_PEAK_ABOVE_START_BYTES} B: drop a packet store's slab when it empties"
    );
}

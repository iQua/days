//! P18 diet-host c2: the host heap a device run's plan holds.
//!
//! A device plan held every plane as a host `Vec<u64>` until upload, the all-zero ones included:
//! the stream rings, the remote staging arena, the outbox, the observation logs, the worklist,
//! the summary counters, the LP state and the merge cursors. On the b4 fixture under the stock
//! caps that was the whole 70 MB plan, half of it the zero stream rings; at the 1,024-GPU flagship
//! 16.5 GB of the 31.2 GB plan. c2a keeps only their lengths and zeroes them on the device.
//!
//! **What is measured.** The executor's phase marks (`days_executor::phase_marks`, a
//! `planner-test-hooks` hook) split one device run; this binary's allocator counts live heap
//! bytes on the run's thread. The plan's host bytes are the live heap at the end of planning
//! less the live heap when validation ended; the device bytes are what the run allocated on the
//! device (`device_alloc` marks up to the end of upload). Both are deterministic.
//!
//! **The cap:** the host plan is at most 60% of the device plan on b4. On CUDA (sim) it was all
//! of it before c2a, 70,777,720 B against a 70,074,936 B device plan (with planner scratch still
//! live), and is 34,678,512 B (49%) after.
//!
//! Run: `cargo test --release -p days --features test,metal --test p18_device_plan_memory` on
//! the Mac, `--features test,cuda` on a CUDA host.
#![cfg(all(
    feature = "test",
    any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))
))]
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{ObservationMode, SimulationImage};

struct ThreadCountingAllocator;

thread_local! {
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
    static AT_VALIDATED: Cell<isize> = const { Cell::new(0) };
    static AT_PLANNED: Cell<isize> = const { Cell::new(0) };
    static DEVICE_BYTES: Cell<usize> = const { Cell::new(0) };
    static UPLOADED: Cell<bool> = const { Cell::new(false) };
}

fn record(delta: isize) {
    let _ = LIVE_BYTES.try_with(|live| live.set(live.get().wrapping_add(delta)));
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size() as isize);
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
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn phase(name: &'static str, device_bytes: usize) {
    let live = LIVE_BYTES.with(Cell::get);
    match name {
        "validated" => AT_VALIDATED.set(live),
        "planned" => AT_PLANNED.set(live),
        "uploaded" => UPLOADED.set(true),
        "device_alloc" if !UPLOADED.get() => DEVICE_BYTES.set(DEVICE_BYTES.get() + device_bytes),
        _ => {}
    }
}

fn b4() -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/b4-simai.toml");
    compile_config(&path).expect("b4 lowers")
}

/// `(host plan bytes, device plan bytes)` of one run of `run`.
fn plan_bytes(run: impl FnOnce()) -> (u64, u64) {
    days_executor::phase_marks::set_phase_hook_for_testing(Some(phase));
    run();
    days_executor::phase_marks::set_phase_hook_for_testing(None);
    let host = AT_PLANNED.with(Cell::get) - AT_VALIDATED.with(Cell::get);
    (
        u64::try_from(host).expect("planning does not shrink the heap"),
        DEVICE_BYTES.with(Cell::get) as u64,
    )
}

fn check(backend: &str, (host, device): (u64, u64)) {
    println!(
        "record=p18_device_plan_memory backend={backend} fixture=b4 host_plan_bytes={host} \
         device_plan_bytes={device}"
    );
    assert!(device > 0, "{backend}: the run uploaded its plan");
    assert!(
        5 * host <= 3 * device,
        "{backend}: the host plan holds {host} B against a {device} B device plan: an all-zero \
         plane is built on the host again"
    );
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn a_metal_plan_holds_no_all_zero_plane_on_the_host() {
    use days_executor::{MetalConfig, MetalExecutor};
    let image = b4();
    let executor = MetalExecutor::new().expect("Metal executor");
    check(
        "metal",
        plan_bytes(|| {
            executor
                .run_with_observations(
                    &image,
                    Some(29_000_000),
                    MetalConfig {
                        capacity_caps: days::STOCK_CAPACITY_CAPS,
                        ..MetalConfig::default()
                    },
                    ObservationMode::Summary,
                )
                .expect("Metal runs b4");
        }),
    );
}

#[cfg(feature = "cuda")]
#[test]
fn a_cuda_plan_holds_no_all_zero_plane_on_the_host() {
    use days_executor::{CudaConfig, CudaExecutor};
    let image = b4();
    let executor = CudaExecutor::new().expect("CUDA executor");
    check(
        "cuda",
        plan_bytes(|| {
            executor
                .run_with_observations(
                    &image,
                    Some(29_000_000),
                    CudaConfig {
                        capacity_caps: days::STOCK_CAPACITY_CAPS,
                        ..CudaConfig::default()
                    },
                    ObservationMode::Summary,
                )
                .expect("CUDA runs b4");
        }),
    );
}

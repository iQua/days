//! P16 L2 zero-cost guard: the WFQ and SP certificate records are built only under full
//! observation.
//!
//! A Summary-mode Scalar run of each WFQ and SP scenario of `configs/leanguard/` must allocate no more
//! than it did before the WFQ and SP emitters existed (`5839da3`, measured by
//! `days-gpu/evidence/P16/lgci/tooling/zz_lgci_probe.rs` and this test at `5839da3`; the counts
//! are the same in debug and release builds, with and without the `test` feature's byte offset). The counters are thread-local, so tests running beside these cannot perturb
//! them.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::{ObservationMode, run_scalar_with_observations};

struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
}

fn record_allocation(bytes: usize) {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
    let _ = ALLOCATED_BYTES.try_with(|total| total.set(total.get().wrapping_add(bytes as u64)));
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
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
            record_allocation(new_size);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

/// Allocations and bytes of one Summary-mode Scalar run of `configs/leanguard/<config>.toml`.
fn summary_run_allocations(config: &str) -> (u64, u64) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("configs/leanguard")
        .join(format!("{config}.toml"));
    let image = compile_config(&path).unwrap();
    let before = (ALLOCATIONS.get(), ALLOCATED_BYTES.get());
    let result = run_scalar_with_observations(&image, None, ObservationMode::Summary).unwrap();
    let after = (ALLOCATIONS.get(), ALLOCATED_BYTES.get());
    assert!(result.diagnostics.is_none());
    drop(result);
    (after.0 - before.0, after.1 - before.1)
}

#[test]
fn summary_wfq_and_sp_runs_allocate_no_more_than_before_their_certificates() {
    // (scenario, allocations, bytes) at 5839da3, the same in debug and release. The `test`
    // feature's probes add a few bytes to each run.
    let caps = if cfg!(feature = "test") {
        [
            ("sched_wfq", 88, 33_520),
            ("wfq_pfc", 169, 74_808),
            ("sched_sp", 66, 29_984),
        ]
    } else {
        [
            ("sched_wfq", 88, 33_328),
            ("wfq_pfc", 169, 74_552),
            ("sched_sp", 66, 29_792),
        ]
    };
    for (config, allocations, bytes) in caps {
        let (actual_allocations, actual_bytes) = summary_run_allocations(config);
        assert!(
            actual_allocations <= allocations && actual_bytes <= bytes,
            "{config}: {actual_allocations} allocations, {actual_bytes} bytes; the cap is \
             {allocations} allocations, {bytes} bytes"
        );
    }
}

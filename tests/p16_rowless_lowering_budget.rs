//! P16 H2 (review F2): lowering an image without rail switch rows costs what `feat/p16` cost.
//!
//! The per-egress-LP ECN-row decision of `switch.ecn_by_rate` once made every image's switch-state
//! map fallible and collected it through a `Result` with no size hint, so the switch-state vector
//! grew by doubling: on E1 (40,960 switch LPs) +13 allocations, +5.77 MB allocated, +1.57 MB peak
//! and 1.5 MiB of unused capacity kept in the image. The rows are now decided once, outside the
//! map. This budget lowers the k = 16 fat-tree TCP fixture (320 switches, 5,120 switch LPs), which
//! has no ECN rows, on one thread (serial routing) and checks, against `feat/p16` (`67d6dcd`)
//! MEASURED with this binary's counting allocator:
//! * allocations, bytes allocated and peak live heap of lowering at most the base's plus a small
//!   allowance for container-layout drift between toolchains (it is far below one doubling chain
//!   of the switch-state vector, 5,120 x 64 B);
//! * no unused capacity in the lowered switch-state and host-state vectors.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;

struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static ALLOCATED: Cell<u64> = const { Cell::new(0) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

fn record(allocated: usize, live_delta: i64) {
    let _ = ALLOCATIONS.try_with(|count| {
        if allocated != 0 {
            count.set(count.get() + 1);
        }
    });
    let _ = ALLOCATED.try_with(|total| total.set(total.get() + allocated as u64));
    let _ = LIVE.try_with(|live| {
        let now = live.get() + live_delta;
        live.set(now);
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(now)));
    });
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size(), layout.size() as i64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size(), layout.size() as i64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
        record(0, -(layout.size() as i64));
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            record(new_size, new_size as i64 - layout.size() as i64);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

const FIXTURE: &str = "configs/benchmarks/tcp/fattree_k16_tcp_cubic_f1024.toml";

/// `feat/p16` (`67d6dcd`), MEASURED by this test's own counters (Mac, rustc 1.98.1).
const BASE_ALLOCATIONS: u64 = 42_864;
const BASE_ALLOCATED_BYTES: u64 = 24_945_849;
const BASE_PEAK_BYTES: i64 = 8_452_067;
/// Allowance for toolchain container drift: well below one doubling chain of the switch-state
/// vector (13 reallocations; 5,120 x 64 B = 327,680 B).
const ALLOCATION_SLACK: u64 = 2;
const BYTE_SLACK: u64 = 16_384;

#[test]
fn rowless_lowering_costs_what_feat_p16_did() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let (allocations, allocated, peak_start) = (
        ALLOCATIONS.with(Cell::get),
        ALLOCATED.with(Cell::get),
        LIVE.with(Cell::get),
    );
    PEAK.with(|peak| peak.set(peak_start));
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial()).expect("lowers");
    let allocations = ALLOCATIONS.with(Cell::get) - allocations;
    let allocated = ALLOCATED.with(Cell::get) - allocated;
    let peak = PEAK.with(Cell::get) - peak_start;
    eprintln!(
        "record=rowless_lowering allocations={allocations} allocated_bytes={allocated} \
         peak_bytes={peak} switch_states={} capacity={}",
        image.switch_states.len(),
        image.switch_states.capacity()
    );
    assert_eq!(image.switch_states.len(), 5_120);
    assert_eq!(
        image.switch_states.capacity(),
        image.switch_states.len(),
        "the lowered image keeps unused switch-state capacity"
    );
    assert_eq!(image.host_states.capacity(), image.host_states.len());
    assert!(
        allocations <= BASE_ALLOCATIONS + ALLOCATION_SLACK,
        "lowering made {allocations} allocations, base {BASE_ALLOCATIONS}"
    );
    assert!(
        allocated <= BASE_ALLOCATED_BYTES + BYTE_SLACK,
        "lowering allocated {allocated} B, base {BASE_ALLOCATED_BYTES} B"
    );
    assert!(
        peak <= BASE_PEAK_BYTES + BYTE_SLACK as i64,
        "lowering peaked at {peak} B, base {BASE_PEAK_BYTES} B"
    );
}

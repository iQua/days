//! P18 diet-host c3 Stage A: the lowering transient of a stage-heavy image.
//!
//! Lowering an AICB MoE trace held, at its peak, the lowered image plus a transient that grew
//! with the flows: the canonical `FlowInput` table with its stage sidecars, each predecessor
//! carried as a full 192 B `FlowKey`; a `FlowKey -> FlowId` B-tree cloning every key; the SimAI
//! ECMP forward and reverse route maps over every routed flow; a RoCE receiver scratch table; and
//! growth slack in the per-host generator, stage and receiver tables. On the P16 MoE smoke the
//! peak was 2,606 B per flow against a 913 B/flow image (days-gpu
//! `evidence/P18/diet-host/`). Stage A drops the id map (flow `i` of the sorted table is
//! `FlowId(i)`), keeps predecessors as 32 B stage keys, routes SimAI flows one at a time, reserves
//! the flow table at its exact length, sorts in place, sizes every per-host table exactly, and
//! builds receivers in place.
//!
//! **What is measured.** One serial lowering (`RouteWorkers::serial()` keeps every allocation on
//! this thread) of `tests/fixtures/aicb/reduced-moe-simai.toml`: 4,128 flows, all-to-all joins,
//! SimAI ECMP routes and RoCE queue pairs. This binary's global allocator counts live and peak
//! heap bytes on the lowering thread only, so the figures are deterministic: functions of the
//! code, the scenario and the toolchain's container layouts. The peak includes `compile_config`'s
//! validation.
//!
//! **The caps,** per flow, from the measured values at `feat/p17` `5c7629ff` (before) and after
//! Stage A, each between the two so the test fails on the old lowering:
//! * the lowering peak: 2,988 B before, 1,369 B after; cap 1,500 B, which re-adding the id map's
//!   key clones alone (about 250 B per flow) would break;
//! * the retained image: 1,320 B before, 792 B after (exact per-host tables, no initial-packet or
//!   join slack); cap 840 B.
//!
//! Run: `cargo test -p days --test p18_lowering_transient_budget` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::PathBuf;

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;

/// Live and peak heap bytes allocated by the current thread.
struct ThreadCountingAllocator;

thread_local! {
    static LIVE_BYTES: Cell<isize> = const { Cell::new(0) };
    static PEAK_BYTES: Cell<isize> = const { Cell::new(0) };
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

const FIXTURE: &str = "tests/fixtures/aicb/reduced-moe-simai.toml";
const FLOWS: u64 = 4_128;
/// The cap on the lowering peak per flow (2,988 B before Stage A, 1,369 B after).
const MAX_PEAK_BYTES_PER_FLOW: u64 = 1_500;
/// The cap on the retained image per flow (1,320 B before Stage A, 792 B after).
const MAX_RETAINED_BYTES_PER_FLOW: u64 = 840;

#[test]
fn a_stage_heavy_lowering_peaks_near_its_image() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let baseline = LIVE_BYTES.with(Cell::get);
    PEAK_BYTES.with(|peak| peak.set(baseline));
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .unwrap_or_else(|error| panic!("lower {FIXTURE}: {error}"));
    let peak = u64::try_from(PEAK_BYTES.with(Cell::get) - baseline).expect("peak >= baseline");
    let retained = u64::try_from(LIVE_BYTES.with(Cell::get) - baseline).expect("retained >= 0");
    let flows = image.flows.len() as u64;
    assert_eq!(flows, FLOWS, "{FIXTURE}: flow count");
    drop(image);
    let (peak_per_flow, retained_per_flow) = (peak / flows, retained / flows);
    println!(
        "record=p18_lowering_transient fixture={FIXTURE} flows={flows} peak_bytes={peak} \
         retained_bytes={retained} peak_bytes_per_flow={peak_per_flow} \
         retained_bytes_per_flow={retained_per_flow} max_peak_bytes_per_flow={MAX_PEAK_BYTES_PER_FLOW} \
         max_retained_bytes_per_flow={MAX_RETAINED_BYTES_PER_FLOW}"
    );
    assert!(
        peak_per_flow <= MAX_PEAK_BYTES_PER_FLOW,
        "lowering peaks at {peak_per_flow} B per flow, above {MAX_PEAK_BYTES_PER_FLOW} B: a \
         per-flow lowering transient (an id map, route maps, receiver scratch, full predecessor \
         keys) came back"
    );
    assert!(
        retained_per_flow <= MAX_RETAINED_BYTES_PER_FLOW,
        "the image retains {retained_per_flow} B per flow, above {MAX_RETAINED_BYTES_PER_FLOW} B: a \
         per-host table carries growth slack again"
    );
}

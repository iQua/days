//! P14 slim memory contract: lowering an image without stages keeps its per-flow peak heap near
//! what the flows themselves need, not what P14's collective and compute stages need.
//!
//! P14 gave every lowered flow inline collective and compute sidecars (`FlowInput`, private to
//! `src/scenario/compile.rs`), 1,224 B per flow where main held 480 B, although most flows have
//! neither sidecar. The sidecars are now boxed and `FlowInput` is 376 B. This test bounds the
//! consequence on the lowering peak, so a per-flow transient that regrows is caught even if it is
//! not `FlowInput` itself.
//!
//! **What is measured.** The marginal peak of live heap bytes per flow between the frontier
//! fixture's first one and first two stacked 8,192-flow sets (`peak(2 sets) - peak(1 set)`, over
//! 8,192 flows). The marginal removes the fixed cost of the k = 32 fat-tree, about 64 MB, which
//! would otherwise dominate a per-flow figure at this size. This binary's global allocator counts
//! live and peak bytes on the lowering thread only; `RouteWorkers::serial()` keeps every
//! allocation of the lowering (routes and `validate` included) on that thread. The measurement is
//! deterministic: a pure function of the code, the scenario and the toolchain's container layouts.
//!
//! **The cap**, per flow, from the struct sizes:
//! * `FlowInput`, 376 B: the bound `compile.rs` asserts in
//!   `flow_input_carries_its_stage_sidecars_out_of_line`, and the whole `FlowInput` table is live
//!   at the peak (slimming it from 1,224 B lowered this marginal by exactly the 848 B difference);
//! * everything else live at the peak, 597 B: measured as the marginal minus `FlowInput`, and the
//!   same 597 B before and after the slimming (1,821 - 1,224 and 973 - 376), so it is not a
//!   by-product of the change. It is the lowered image's own per-flow storage (295 B: descriptor,
//!   routes, initial packet and event) plus lowering's transients, chiefly the flow-id map's
//!   `FlowKey` clone (192 B) in its B-tree;
//! * 128 B of headroom for container-layout drift between toolchains. It is below the 416 B of
//!   the smaller stage sidecar, so re-inlining either sidecar fails the test.
//!
//! Per-host generator vectors do not appear in this marginal: `Vec` allocates at least four slots
//! for elements of at most 1,024 B, so one and two generators per host occupy the same capacity.
//!
//! Before the slimming (6cc395c) the marginal is 1,821 B per flow and this test fails; after it,
//! 973 B. Raw logs are in `days-gpu/evidence/P14/slim/`.
//!
//! # Retained image bytes per flow
//!
//! The second test closes that blind spot. It bounds the bytes the lowered image itself retains
//! per flow, the marginal between four and eight stacked flow sets of one flow per host on a
//! k = 16 fat-tree (1,024 hosts; 4,096 and 8,192 flows). From four generators per host on, a
//! per-host vector's capacity doubles as its length does, so every per-host vector holds exactly
//! one more slot per added flow and each per-flow image type appears in the marginal at its full
//! size, `FlowGeneratorState` included. Per-host fixed costs cancel.
//!
//! **The cap**, per flow: `FlowGeneratorState` at main's 352 B (the bound
//! `executor/src/image.rs` asserts), 421 B of the rest of the retained image (measured as the
//! marginal minus `FlowGeneratorState`, 917 - 496 at 32dafb3; descriptors, routes, receivers, the
//! initial packet and events), and 64 B of headroom, below the 144 B the inline stage record cost.
//! With the stage record inline (32dafb3) the marginal is 917 B per flow and this test fails.
//!
//! Run: `cargo test -p days --test stageless_lowering_heap_budget` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;

/// Live and peak heap bytes allocated by the current thread.
///
/// The counters are thread-local, so tests that run beside this one in the same binary cannot
/// perturb them, and nothing is shared between threads.
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

const FRONTIER_FIXTURE: &str = "configs/benchmarks/lookahead/rq9_frontier_closed_k32.toml";
const FLOW_SET_HEADER: &str = "[[flow_set]]";
/// Flows per stacked set in the frontier fixture.
const FLOWS_PER_SET: u64 = 8_192;
/// Stacked flow sets in the frontier fixture.
const FIXTURE_SETS: usize = 32;

/// `FlowInput`'s bound, asserted in `src/scenario/compile.rs`.
const FLOW_INPUT_BYTES: u64 = 376;
/// Every other per-flow byte live at the lowering peak, measured before and after the slimming.
const OTHER_PEAK_BYTES_PER_FLOW: u64 = 597;
/// Headroom for container-layout drift; below the smaller stage sidecar's 416 B.
const HEADROOM_BYTES_PER_FLOW: u64 = 128;
/// The cap on the marginal peak heap per stageless flow: 1,101 B.
const MAX_PEAK_BYTES_PER_FLOW: u64 =
    FLOW_INPUT_BYTES + OTHER_PEAK_BYTES_PER_FLOW + HEADROOM_BYTES_PER_FLOW;

/// `FlowGeneratorState`'s bound, asserted in `executor/src/image.rs`.
const FLOW_GENERATOR_STATE_BYTES: u64 = 352;
/// Every other retained image byte per flow, measured with the stage record inline.
const OTHER_RETAINED_BYTES_PER_FLOW: u64 = 421;
/// Headroom for container-layout drift; below the inline stage record's 144 B.
const RETAINED_HEADROOM_BYTES_PER_FLOW: u64 = 64;
/// The cap on the marginal retained image bytes per stageless flow: 837 B.
const MAX_RETAINED_BYTES_PER_FLOW: u64 =
    FLOW_GENERATOR_STATE_BYTES + OTHER_RETAINED_BYTES_PER_FLOW + RETAINED_HEADROOM_BYTES_PER_FLOW;
/// Fat-tree arity of the retained-bytes scenarios, and its host count.
const RETAINED_K: usize = 16;
const RETAINED_HOSTS: usize = RETAINED_K * RETAINED_K * RETAINED_K / 4;

/// The frontier fixture with only its first `sets` stacked flow sets, written to a file private to
/// this process.
fn frontier_with_flow_sets(sets: usize) -> PathBuf {
    write_scenario(&format!("{sets}_sets"), frontier_text(sets))
}

/// The frontier fixture on a k = 16 fat-tree with `sets` stacked flow sets of one flow per host.
fn fat_tree_k16_with_flow_sets(sets: usize) -> PathBuf {
    let substitutions = [
        ("k = 32\n".to_owned(), format!("k = {RETAINED_K}\n"), 1),
        (
            "hosts_per_edge = 16\n".to_owned(),
            format!("hosts_per_edge = {}\n", RETAINED_K / 2),
            1,
        ),
        (
            format!("flow_count = {FLOWS_PER_SET}\n"),
            format!("flow_count = {RETAINED_HOSTS}\n"),
            sets,
        ),
    ];
    let text = substitutions
        .iter()
        .fold(frontier_text(sets), |text, (from, to, expected)| {
            assert_eq!(text.matches(from.as_str()).count(), *expected, "{from:?}");
            text.replace(from.as_str(), to)
        });
    write_scenario(&format!("k{RETAINED_K}_{sets}_sets"), text)
}

fn write_scenario(label: &str, text: String) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "stageless_lowering_heap_budget_{label}_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived frontier scenario");
    path
}

/// The frontier fixture's text with only its first `sets` stacked flow sets.
fn frontier_text(sets: usize) -> String {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(FRONTIER_FIXTURE))
        .expect("read the frontier fixture");
    let mut blocks = text.split(FLOW_SET_HEADER);
    let prefix = blocks.next().expect("the fixture has a prefix");
    let blocks = blocks.collect::<Vec<_>>();
    assert_eq!(
        blocks.len(),
        FIXTURE_SETS,
        "the frontier fixture must stack exactly {FIXTURE_SETS} flow sets"
    );
    let mut derived = prefix.to_owned();
    for block in &blocks[..sets] {
        derived.push_str(FLOW_SET_HEADER);
        derived.push_str(block);
    }
    derived
}

/// Peak live heap bytes of one serial lowering of the first `sets` frontier flow sets, above the
/// bytes live when it starts.
fn lowering_peak_bytes(sets: usize) -> u64 {
    let path = frontier_with_flow_sets(sets);
    let baseline = LIVE_BYTES.with(Cell::get);
    PEAK_BYTES.with(|peak| peak.set(baseline));
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .unwrap_or_else(|error| panic!("lower {sets} frontier sets: {error}"));
    let peak = PEAK_BYTES.with(Cell::get);
    assert_eq!(
        image.flows.len() as u64,
        sets as u64 * FLOWS_PER_SET,
        "{sets} sets: flow count"
    );
    assert!(
        image
            .host_states
            .iter()
            .all(|state| state.stages.is_empty()),
        "the frontier fixture has no stages"
    );
    drop(image);
    u64::try_from(peak - baseline).expect("the peak is at least the baseline")
}

#[test]
fn stageless_lowering_peak_heap_per_flow_stays_within_the_slim_flow_input_budget() {
    let one_set = lowering_peak_bytes(1);
    let two_sets = lowering_peak_bytes(2);
    let per_flow = two_sets.saturating_sub(one_set) / FLOWS_PER_SET;
    println!(
        "record=stageless_lowering_heap_budget one_set_peak_bytes={one_set} \
         two_sets_peak_bytes={two_sets} marginal_peak_bytes_per_flow={per_flow} \
         max_peak_bytes_per_flow={MAX_PEAK_BYTES_PER_FLOW}"
    );
    assert!(
        per_flow <= MAX_PEAK_BYTES_PER_FLOW,
        "lowering a stageless flow peaks at {per_flow} heap bytes, above the {MAX_PEAK_BYTES_PER_FLOW} B \
         budget ({FLOW_INPUT_BYTES} B FlowInput + {OTHER_PEAK_BYTES_PER_FLOW} B else live at the \
         peak + {HEADROOM_BYTES_PER_FLOW} B headroom): a per-flow lowering structure grew"
    );
}

/// Bytes the lowered image retains, and its flow count, for `sets` flow sets on the k = 16 tree.
fn retained_image_bytes(sets: usize) -> (u64, u64) {
    let path = fat_tree_k16_with_flow_sets(sets);
    let baseline = LIVE_BYTES.with(Cell::get);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .unwrap_or_else(|error| panic!("lower {sets} k={RETAINED_K} sets: {error}"));
    let retained = LIVE_BYTES.with(Cell::get) - baseline;
    let flows = image.flows.len() as u64;
    assert_eq!(
        flows,
        (sets * RETAINED_HOSTS) as u64,
        "{sets} sets: flow count"
    );
    assert!(
        image
            .host_states
            .iter()
            .all(|state| state.stages.is_empty()),
        "the derived scenario has no stages"
    );
    drop(image);
    (
        u64::try_from(retained).expect("the image retains a non-negative byte count"),
        flows,
    )
}

#[test]
fn stageless_image_retains_at_most_main_generator_size_per_flow() {
    let (four_bytes, four_flows) = retained_image_bytes(4);
    let (eight_bytes, eight_flows) = retained_image_bytes(8);
    let per_flow = eight_bytes.saturating_sub(four_bytes) / (eight_flows - four_flows);
    println!(
        "record=stageless_lowering_heap_budget four_sets_retained_bytes={four_bytes} \
         eight_sets_retained_bytes={eight_bytes} marginal_retained_bytes_per_flow={per_flow} \
         max_retained_bytes_per_flow={MAX_RETAINED_BYTES_PER_FLOW}"
    );
    assert!(
        per_flow <= MAX_RETAINED_BYTES_PER_FLOW,
        "a stageless lowered image retains {per_flow} bytes per flow, above the \
         {MAX_RETAINED_BYTES_PER_FLOW} B budget ({FLOW_GENERATOR_STATE_BYTES} B FlowGeneratorState + \
         {OTHER_RETAINED_BYTES_PER_FLOW} B rest of the image + {RETAINED_HEADROOM_BYTES_PER_FLOW} B \
         headroom): a per-flow image type grew"
    );
}

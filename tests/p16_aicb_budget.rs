//! P16 H3 (aicb): the adapter's allocation budgets (perf-discipline; design note §6.2).
//!
//! The adapter's work is per trace, not per rank: the parser allocates the record vector and one
//! name per record; group formation allocates a fixed number of vectors per family whatever the
//! world size; the schedule allocates nothing that grows with the world size. A per-rank or
//! per-group allocation (P14's rank-length-key pattern) fails these tests. The counts are
//! deterministic: this binary's global allocator counts allocation calls (fresh allocations and
//! reallocations) on the current thread only.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::PathBuf;

use days::workload::aicb::{
    ExpertRouting, Fidelity, PlanOptions, PropagationBounds, SimaiEnv, form_groups, parse_trace,
    plan_schedule,
};

/// Allocation calls made by the current thread (thread-local: nothing is shared between threads).
struct ThreadCountingAllocator;

thread_local! {
    static CALLS: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarded with the caller's layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: forwarded with the caller's pointer, layout and size.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn calls_during<T>(work: impl FnOnce() -> T) -> (T, u64) {
    let before = CALLS.with(Cell::get);
    let value = work();
    (value, CALLS.with(Cell::get) - before)
}

fn smoke_text() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aicb/smoke-moe-w128-tp2-ep32.txt");
    std::fs::read_to_string(path).unwrap()
}

/// The smoke trace's records on a world eight times larger (EP32 groups stay; DP grows 8x).
fn smoke_at_1024() -> String {
    smoke_text().replacen("all_gpus: 128", "all_gpus: 1024", 1)
}

fn options() -> PlanOptions {
    PlanOptions {
        fidelity: Fidelity::Simai,
        expert_routing: ExpertRouting::Uniform,
        mtu_bytes: 9000,
        gpu_type: "H100".to_owned(),
        simai_env: Some(SimaiEnv {
            send_lat_us: 3,
            nvls_enable: true,
            pxn_enable: false,
        }),
    }
}

const BOUNDS: PropagationBounds = PropagationBounds {
    link_delay_ns: 500,
    nic_rate_bps: 400_000_000_000,
    nvlink_delay_ns: 25,
};

#[test]
fn the_parser_allocates_the_records_and_one_name_each() {
    let text = smoke_text();
    let (trace, calls) = calls_during(|| parse_trace(&text).unwrap());
    let records = trace.records.len() as u64;
    assert_eq!(records, 184);
    assert!(
        calls <= records + 1,
        "parse made {calls} allocation calls for {records} records (budget: records + 1)"
    );
}

#[test]
fn group_formation_allocates_independently_of_the_world_size() {
    let small = parse_trace(&smoke_text()).unwrap();
    let large = parse_trace(&smoke_at_1024()).unwrap();
    let (_, small_calls) = calls_during(|| form_groups(&small.header, Fidelity::Simai, 8).unwrap());
    let (groups, large_calls) =
        calls_during(|| form_groups(&large.header, Fidelity::Simai, 8).unwrap());
    assert_eq!(groups.world, 1024);
    assert_eq!(
        small_calls, large_calls,
        "group formation: {small_calls} allocation calls at W 128, {large_calls} at W 1,024"
    );
    assert!(
        large_calls <= 10,
        "{large_calls} allocation calls (budget 10: two per family, pairs)"
    );
    // The Megatron placement of a PP 2 header: the same budget across its stages.
    let megatron = small.header;
    let megatron = days::workload::aicb::Header {
        pp: 2,
        pp_comm_bytes: 1,
        ..megatron
    };
    let (_, calls) = calls_during(|| form_groups(&megatron, Fidelity::Megatron, 8).unwrap());
    assert!(
        calls <= 10,
        "{calls} allocation calls for two pipeline stages"
    );
}

#[test]
fn the_schedule_allocates_independently_of_the_world_size() {
    let mut calls = Vec::new();
    for text in [smoke_text(), smoke_at_1024()] {
        let trace = parse_trace(&text).unwrap();
        let groups = form_groups(&trace.header, Fidelity::Simai, 8).unwrap();
        let (plan, made) =
            calls_during(|| plan_schedule(&trace, &groups, &options(), &BOUNDS).unwrap());
        assert_eq!(plan.ops.len(), 279);
        calls.push(made);
    }
    assert_eq!(
        calls[0], calls[1],
        "the schedule made {} allocation calls at W 128 and {} at W 1,024",
        calls[0], calls[1]
    );
}

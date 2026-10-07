//! P16 H2 budgets: rail lowering, SimAI ECMP routing and stage notifies cost work in proportion
//! to what they lower, not to flows times the fabric or notifies times the channels.
//!
//! **Scaling** (the `tests/host_scaling_budget.rs` convention). Two rail scenarios grow every
//! entity 8x: `s` servers of 8 GPUs (`s` = 4 and 32), one 8-rank all-reduce ring inside each server
//! (whose 112 stages per server are all stage notifies), and one TCP flow from every GPU to the
//! GPU 9 places on, wrapping (the next server on the next rail, so through a PSW by SimAI's ECMP). The PSW count is fixed (2) so
//! the ASW count grows with the GPUs. Lowering plus validation must grow by at most
//! `sqrt(8 x 64) = 22.6` in allocated bytes, the midpoint between linear (8x) and a per-item term
//! proportional to the fabric (64x). The count is a pure function of the code and the scenario: a
//! thread-local counting allocator, serial routing.
//!
//! **Zero cost when unused** is pinned elsewhere: the stageless heap and allocation budgets
//! (`tests/stageless_lowering_heap_budget.rs`, `tests/cpu_host_lp_heap_budget.rs`) and
//! `tests/planner_bit_equal.rs` run unchanged on non-rail images.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fmt::Write as _;

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{Backend, FlowGeneratorKind, validate};

struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
}

fn record(bytes: usize) {
    let _ = ALLOCATED_BYTES.try_with(|total| total.set(total.get().wrapping_add(bytes as u64)));
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record(layout.size());
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
            record(new_size);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn allocated() -> u64 {
    ALLOCATED_BYTES.with(Cell::get)
}

/// `servers` servers of 8 GPUs, 8 NICs per ASW (one segment per server group of 8), 2 PSWs.
fn scenario(servers: u64) -> String {
    let gpus = servers * 8;
    let mut text = format!(
        r#"seed = 5
duration = 0.001

[topology]
category = "SpectrumX"

[topology.spectrum_x]
gpus = {gpus}
gpus_per_server = 8
nics_per_asw = 8
psws = 2
gpu_type = "H100"
nic_rate_bps = 400000000000
uplink_rate_bps = 400000000000
nvlink_rate_bps = 2880000000000
link_delay_ns = 500
nvlink_delay_ns = 25

[routing]
policy = "SimAiEcmp"

[switch]
capacity = 100
discipline = "FIFO"
drop = "TailDrop"
"#
    );
    for server in 0..servers {
        let ranks = (0..8).map(|rank| server * 8 + rank).collect::<Vec<_>>();
        let list = |values: &[u64]| {
            values
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let sinks = ranks
            .iter()
            .map(|rank| server * 8 + (rank + 1) % 8)
            .collect::<Vec<_>>();
        writeln!(
            text,
            r#"
[[collective]]
collective_type = "RingAllReduce"
flow_type = "TCP"
flow_count = 8
sources = [{}]
sinks = [{}]

[collective.traffic]
initial_delay = 0.0
size = 80000
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 9000, high = 9000 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"
"#,
            list(&ranks),
            list(&sinks)
        )
        .expect("write");
    }
    for gpu in 0..gpus {
        writeln!(
            text,
            r#"
[[flow]]
flow_type = "TCP"
graph = [[{gpu}, {}]]

[flow.traffic]
initial_delay = 0.0
size = 9000
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 9000, high = 9000 }}

[flow.traffic.tcp]
cc_algorithm = "TCPReno"
"#,
            (gpu + 9) % gpus
        )
        .expect("write");
    }
    text
}

/// Bytes allocated by lowering and validating `servers`' scenario, and its notify count.
fn lowering_bytes(servers: u64) -> (u64, usize) {
    let directory = tempfile::TempDir::new().expect("temp dir");
    let path = directory.path().join("rail.toml");
    std::fs::write(&path, scenario(servers)).expect("write");
    let before = allocated();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial()).expect("lowers");
    validate(&image, Backend::Scalar).expect("valid");
    let bytes = allocated() - before;
    let notifies = image
        .host_states
        .iter()
        .flat_map(|state| {
            state
                .generators
                .iter()
                .enumerate()
                .filter(move |(position, generator)| {
                    matches!(generator.kind, FlowGeneratorKind::Constant(_))
                        && state.stage(*position).is_some()
                })
        })
        .count();
    (bytes, notifies)
}

#[test]
fn rail_lowering_and_notifies_scale_with_what_they_lower() {
    let (small, small_notifies) = lowering_bytes(4);
    let (large, large_notifies) = lowering_bytes(32);
    assert_eq!((small_notifies, large_notifies), (4 * 112, 32 * 112));
    let ratio = large as f64 / small as f64;
    eprintln!("record=rail_lowering_bytes small={small} large={large} ratio={ratio:.2}");
    assert!(
        ratio <= 22.6,
        "8x the rail scenario allocates {ratio:.2}x the bytes (bound 22.6)"
    );
}

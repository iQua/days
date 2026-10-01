//! P14 route budget contract: shortest-path route search on a fat tree does work in proportion to
//! the sources it routes from, not to every flow times the whole fabric.
//!
//! Lowering routes each flow between its hosts' edge switches over the switch graph of a k-ary fat
//! tree: `N = 5k^2/4` switches, `E = k^3/2` links, `k^2/2` edge switches, and `k^3/4` hosts. The
//! scenarios are the frontier's traffic (`configs/benchmarks/lookahead/rq9_frontier_closed_k32.toml`)
//! on a k=16 and a k=32 fat tree, one flow per host: 1,024 and 8,192 flows. Every entity grows 8x
//! except the switch graph's nodes (4x), and its links and each search's work (8x).
//!
//! A search from an edge switch to an edge switch in another pod pushes every switch and examines
//! about `1.8 E` candidate neighbours before it pops its target at depth four, so searching once
//! per flow costs about `F x 1.8 E` examinations. Two models of the step:
//!
//! * **per-flow search** (what lowering did before P14 route): examinations per routed flow grow as
//!   `E`, i.e. **8x**; allocated bytes grow as `F x N` (a score and predecessor slot per switch and a
//!   heap per search), i.e. **32x**;
//! * **one search tree per source edge switch** (P14 route): the search from `s` runs to exhaustion
//!   once, and every flow from `s` reads its route from that tree. An exhaustive search pops every
//!   switch once and examines each of its neighbours, `2E` examinations. With every edge switch a
//!   source, examinations total `(k^2/2) x 2E = k^5/2`, so per routed flow they grow as
//!   `k^5 / k^3 = k^2`, i.e. **4x**; allocated bytes grow as `F` plus one search's scratch, i.e.
//!   **8x**.
//!
//! Each bound is the geometric midpoint of the two models, the convention of
//! `tests/host_scaling_budget.rs`: `sqrt(8 x 4) = 5.66` for examinations per routed flow and
//! `sqrt(32 x 8) = 16` for allocated bytes. Examinations also have an absolute bound at each size:
//! with one route worker there is at most one exhaustive tree per edge switch, `k^5/2`
//! examinations. The count is a pure function of the code and the scenario.
//!
//! Every case lowers with `RouteWorkers::serial()`, which routes on the calling thread and lowers
//! the same image bytes as every other route budget (`serial_route_lowering_is_the_compile_config_image`).
//! Parallel workers would divide per-flow work by the core count, and a source whose flows are
//! split between two workers is searched by both, so the count is only a function of the scenario
//! with one worker.
//!
//! * **Examinations** count, through a test-only probe, every candidate neighbour Days' fat-tree
//!   search considers. The probe is zero-sized without the `test` feature.
//! * **Allocated bytes** are counted on the lowering thread by this binary's global allocator.
//! * **Time** is lowering wall time, the minimum of a few repetitions at each size. It is noisy on
//!   a shared machine, so it is an explicit case. Its bound is the allocated-bytes midpoint, 16x,
//!   between flows-linear growth (8x) and flows x switches (32x).
//!
//! **The route table alone** (`fat_tree_route_table_allocates_and_writes_per_source_not_per_flow`).
//! The whole lowering allocates about 27 MB at k=16 and 216 MB at k=32 besides routing, which
//! dilutes a per-flow term: a per-flow allocation below about 42 B per switch still kept the
//! lowering ratio under 16x. This case therefore calls `compute_shortest_path_route_table_with`
//! directly, serially, on the switch graph `build_graph` builds for the same scenario, with one flow
//! per host: the host at rack ordinal `o` of edge switch `s` sends to edge switch
//! `(s + S/2 + o) mod S` (`S = k^2/2`), which is always in another pod. It checks two
//! deterministic measures against their models:
//! * **scratch cells written** by the search (the test-only probe): every write to the search's
//!   node-indexed `scores` and `came_from`, the reset that clears them included. One exhaustive
//!   tree writes exactly `5N - 1` cells (`2N` reset, one for the source, two per push over `N - 1`
//!   pushes, one per pop over `N` pops), so the table writes at most `S x (5N - 1)`, the cap; per
//!   routed flow that grows as `S N / F ~ k`, **2x**, while any per-flow reset or search of the
//!   scratch grows as `N`, **4x**; the ratio bound is `sqrt(2 x 4) = 2.83`;
//! * **allocated bytes** of the table: per flow a route (52 B for five hops), its slots in the
//!   grouping, part, result and key vectors, and the result and duplicate-key B-trees, about
//!   207 B in all; once per table the canonical graph and its sorted edge list (`16N + 32E`) and one
//!   worker's search state (`40N` of `scores` and `came_from`, at most `128N` of heap growth). The
//!   absolute cap allows `384 B` per routed flow, `256 B` per switch and `64 B` per link, about
//!   1.9x, 1.4x and 2x the model (measured: 348 and 331 B per flow in all, at k=16 and k=32).
//!   The ratio bound is again the midpoint of flows-linear growth (8x: flows and links grow 8x) and
//!   flows x switches (32x): 16x. A per-flow allocation of `b` bytes per switch breaks the cap once
//!   `b N` exceeds the cap's per-flow slack, about 0.8 B per switch at k=16 and 0.2 B at k=32.
//!
//! **Known limit.** Both counters see only the search's own state and the allocator. A separate
//! node-length buffer, allocated once per worker and cleared for every flow, allocates nothing per
//! flow and is not the search's scratch, so neither counter sees it, and it is far below what the
//! time case can resolve. At the frontier (k=32: 1,280 switches, 262,144 routes) one byte per switch
//! cleared per route is 335.5 MB of stores summed over all route workers, against the 29,584
//! neighbour examinations per inter-pod route that per-flow search cost. Closing the gap would take
//! a structural guard: a counted wrapper type for every node-length routing buffer (zero-sized in
//! production, as `p14/scan` pinned its stage view) plus an `xtask audit` rule that rejects
//! node-count-sized buffers in `src/topos/route.rs` outside that type.
//!
//! How to run each case:
//! * examinations, lowering allocated bytes, and the route table's scratch writes and allocated
//!   bytes (deterministic; default matrix, any profile):
//!   `cargo test -p days --features test --test route_scaling_budget`
//! * time (explicit; release, one test thread so the deterministic cases cannot share the CPU):
//!   `cargo test --release -p days --features test --test route_scaling_budget -- --ignored --test-threads=1`
//! * all, as a scaling job runs them:
//!   `cargo test --release -p days --features test --test route_scaling_budget -- --include-ignored --test-threads=1`
#![cfg(feature = "test")]
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use std::collections::BTreeMap;

use days::scenario::{compile_config, compile_config_with_route_workers};
use days::topos::build::build_graph;
use days::topos::route::{
    RouteWorkers, compute_shortest_path_route_table_with,
    route_table_neighbour_examinations_for_testing, route_table_scratch_cells_written_for_testing,
};
use petgraph::graph::NodeIndex;

/// Bytes allocated by the current thread.
///
/// The counter is thread-local, so tests that run beside this one in the same binary cannot
/// perturb it, and nothing is shared between threads.
struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
}

fn record_allocation(bytes: usize) {
    let _ = ALLOCATED_BYTES.try_with(|total| total.set(total.get().wrapping_add(bytes)));
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

const SMALL_K: u64 = 16;
const LARGE_K: u64 = 32;
const TIME_REPETITIONS: usize = 3;

fn hosts(k: u64) -> u64 {
    k * k * k / 4
}

fn edge_switches(k: u64) -> u64 {
    k * k / 2
}

fn switch_links(k: u64) -> u64 {
    k * k * k / 2
}

/// The frontier's traffic on a k-ary fat tree with `k/2` hosts per edge switch, one flow per host.
///
/// Each test passes its own `test` label, so tests running in parallel never write the same file.
fn frontier_traffic_scenario(test: &str, k: u64) -> PathBuf {
    let config = format!(
        r#"
seed = 13032
duration = 0.001152

[topology]
category = "FatTree"

[topology.fat_tree]
k = {k}
hosts_per_edge = {hosts_per_edge}

[switch]
port_rate = 100_000_000_000
capacity = 1024
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 1000

[[flow_set]]
flow_type = "TCP"
flow_count = {flows}

[flow_set.traffic]
initial_delay = 0.0
size = 16_777_216
arr_dist = {{ type = "Uniform", low = 1.0, high = 1.0 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 256, high = 256 }}

[flow_set.traffic.tcp]
cc_algorithm = "TCPReno"
"#,
        hosts_per_edge = k / 2,
        flows = hosts(k),
    );
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("route_scaling_budget_{test}_k{k}.toml"));
    fs::write(&path, config).expect("write the fat-tree scenario");
    path
}

/// Route-search work and allocation of one serial lowering.
struct RouteWork {
    flows: u64,
    neighbour_examinations: u64,
    allocated_bytes: u64,
}

fn lowering_route_work(k: u64) -> RouteWork {
    let path = frontier_traffic_scenario("work", k);
    let examinations_before = route_table_neighbour_examinations_for_testing();
    let allocated_before = ALLOCATED_BYTES.with(Cell::get);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .unwrap_or_else(|error| panic!("lower k={k}: {error}"));
    let allocated = ALLOCATED_BYTES.with(Cell::get) - allocated_before;
    let examinations = route_table_neighbour_examinations_for_testing() - examinations_before;
    let flows = image.flows.len() as u64;
    assert_eq!(flows, hosts(k), "k={k}: one routed flow per host");
    drop(image);
    RouteWork {
        flows,
        neighbour_examinations: examinations,
        allocated_bytes: allocated as u64,
    }
}

/// Geometric midpoint of the per-source-tree and per-flow-search models of examinations per
/// routed flow across the step: `4x` and `8x`.
fn max_examinations_ratio() -> f64 {
    let per_source_tree = 4.0_f64;
    let per_flow_search = (switch_links(LARGE_K) / switch_links(SMALL_K)) as f64;
    (per_source_tree * per_flow_search).sqrt()
}

/// Geometric midpoint of flows-linear growth (`8x`) and flows x switches (`32x`).
fn max_allocation_ratio() -> f64 {
    let flow_ratio = (hosts(LARGE_K) / hosts(SMALL_K)) as f64;
    let switch_ratio = 4.0_f64;
    flow_ratio * switch_ratio.sqrt()
}

/// Records one phase's ratio and returns a failure message when it reaches `max_ratio`.
fn check(label: &str, unit: &str, small: f64, large: f64, max_ratio: f64) -> Option<String> {
    let ratio = large / small;
    println!(
        "record=route_scaling_budget phase={label} small_k={SMALL_K} large_k={LARGE_K} \
         small_flows={} large_flows={} small_{unit}={small:.1} large_{unit}={large:.1} \
         ratio={ratio:.3} max_ratio={max_ratio:.3}",
        hosts(SMALL_K),
        hosts(LARGE_K),
    );
    (ratio >= max_ratio).then(|| {
        format!(
            "{label}: k={LARGE_K}/k={SMALL_K} ratio {ratio:.2} >= {max_ratio:.2} \
             ({small:.1} -> {large:.1} {unit}); route search grows with flows x fabric"
        )
    })
}

#[test]
fn fat_tree_route_search_scales_with_sources_not_flows_times_fabric() {
    let small = lowering_route_work(SMALL_K);
    let large = lowering_route_work(LARGE_K);
    let mut failures = Vec::new();
    for (k, work) in [(SMALL_K, &small), (LARGE_K, &large)] {
        assert!(
            work.neighbour_examinations > 0,
            "k={k}: the probe saw no search; the gate would pass vacuously"
        );
        // One exhaustive tree per edge switch: (k^2/2) x 2E examinations.
        let one_tree_per_edge_switch = edge_switches(k) * 2 * switch_links(k);
        println!(
            "record=route_scaling_budget phase=route_examinations k={k} flows={} \
             neighbour_examinations={} one_tree_per_edge_switch={one_tree_per_edge_switch}",
            work.flows, work.neighbour_examinations,
        );
        if work.neighbour_examinations > one_tree_per_edge_switch {
            failures.push(format!(
                "k={k}: {} neighbour examinations exceed one exhaustive search tree per edge \
                 switch ({one_tree_per_edge_switch})",
                work.neighbour_examinations
            ));
        }
    }
    failures.extend(check(
        "route_examinations_per_flow",
        "examinations",
        small.neighbour_examinations as f64 / small.flows as f64,
        large.neighbour_examinations as f64 / large.flows as f64,
        max_examinations_ratio(),
    ));
    failures.extend(check(
        "lowering_allocated",
        "bytes",
        small.allocated_bytes as f64,
        large.allocated_bytes as f64,
        max_allocation_ratio(),
    ));
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Scratch writes and allocation of one serial route table, on the switch graph of the scenario.
struct RouteTableWork {
    flows: u64,
    switches: u64,
    links: u64,
    scratch_cells_written: u64,
    allocated_bytes: u64,
}

/// Per-flow bytes the absolute cap allows: about 1.9x the modelled 207 B.
const TABLE_BYTES_PER_FLOW: u64 = 384;
/// Per-switch bytes the absolute cap allows: about 1.4x the modelled 184 B (graph and one search).
const TABLE_BYTES_PER_SWITCH: u64 = 256;
/// Per-link bytes the absolute cap allows: 2x the modelled 32 B (graph and sorted edge list).
const TABLE_BYTES_PER_LINK: u64 = 64;

fn route_table_work(k: u64) -> RouteTableWork {
    let path = frontier_traffic_scenario("table", k);
    let (graph, attachments) = build_graph(path.to_str().expect("a UTF-8 scenario path"))
        .unwrap_or_else(|error| panic!("build the k={k} switch graph: {error}"));
    let edge_switch_count = edge_switches(k) as usize;
    let mut ordinals = BTreeMap::<usize, usize>::new();
    let flows = attachments
        .iter()
        .enumerate()
        .map(|(key, attachment)| {
            assert!(
                attachment.switch_id < edge_switch_count,
                "k={k}: hosts attach to edge switches"
            );
            let ordinal = ordinals.entry(attachment.switch_id).or_insert(0);
            let target =
                (attachment.switch_id + edge_switch_count / 2 + *ordinal) % edge_switch_count;
            *ordinal += 1;
            (
                key,
                NodeIndex::new(attachment.switch_id),
                NodeIndex::new(target),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(flows.len() as u64, hosts(k), "k={k}: one flow per host");

    let written_before = route_table_scratch_cells_written_for_testing();
    let allocated_before = ALLOCATED_BYTES.with(Cell::get);
    let table = compute_shortest_path_route_table_with(
        &graph,
        flows.iter().copied(),
        RouteWorkers::serial(),
    )
    .unwrap_or_else(|error| panic!("route k={k}: {error:?}"));
    let allocated = ALLOCATED_BYTES.with(Cell::get) - allocated_before;
    let written = route_table_scratch_cells_written_for_testing() - written_before;
    assert!(
        table.values().all(|route| route.len() == 5),
        "k={k}: every flow crosses pods"
    );
    drop(table);
    RouteTableWork {
        flows: flows.len() as u64,
        switches: graph.node_count() as u64,
        links: graph.edge_count() as u64,
        scratch_cells_written: written,
        allocated_bytes: allocated as u64,
    }
}

#[test]
fn fat_tree_route_table_allocates_and_writes_per_source_not_per_flow() {
    let small = route_table_work(SMALL_K);
    let large = route_table_work(LARGE_K);
    let mut failures = Vec::new();
    for (k, work) in [(SMALL_K, &small), (LARGE_K, &large)] {
        assert!(
            work.scratch_cells_written > 0,
            "k={k}: the probe saw no scratch write; the gate would pass vacuously"
        );
        // One exhaustive tree per edge switch, 5N - 1 cells each.
        let one_tree_per_edge_switch = edge_switches(k) * (5 * work.switches - 1);
        let byte_cap = TABLE_BYTES_PER_FLOW * work.flows
            + TABLE_BYTES_PER_SWITCH * work.switches
            + TABLE_BYTES_PER_LINK * work.links;
        println!(
            "record=route_scaling_budget phase=route_table k={k} flows={} switches={} links={} \
             scratch_cells_written={} one_tree_per_edge_switch={one_tree_per_edge_switch} \
             allocated_bytes={} bytes_per_flow={:.1} byte_cap={byte_cap}",
            work.flows,
            work.switches,
            work.links,
            work.scratch_cells_written,
            work.allocated_bytes,
            work.allocated_bytes as f64 / work.flows as f64,
        );
        if work.scratch_cells_written > one_tree_per_edge_switch {
            failures.push(format!(
                "k={k}: {} scratch cells written exceed one exhaustive search tree per edge \
                 switch ({one_tree_per_edge_switch})",
                work.scratch_cells_written
            ));
        }
        if work.allocated_bytes > byte_cap {
            failures.push(format!(
                "k={k}: the route table allocated {} bytes, over the cap of {byte_cap} \
                 ({TABLE_BYTES_PER_FLOW} per flow, {TABLE_BYTES_PER_SWITCH} per switch, \
                 {TABLE_BYTES_PER_LINK} per link)",
                work.allocated_bytes
            ));
        }
    }
    failures.extend(check(
        "route_table_scratch_writes_per_flow",
        "cells",
        small.scratch_cells_written as f64 / small.flows as f64,
        large.scratch_cells_written as f64 / large.flows as f64,
        (2.0_f64 * 4.0).sqrt(),
    ));
    failures.extend(check(
        "route_table_allocated",
        "bytes",
        small.allocated_bytes as f64,
        large.allocated_bytes as f64,
        max_allocation_ratio(),
    ));
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Minimum serial-route lowering wall time over [`TIME_REPETITIONS`] lowerings.
fn lowering_time(k: u64) -> Duration {
    let path = frontier_traffic_scenario("time", k);
    (0..TIME_REPETITIONS)
        .map(|_| {
            let started = Instant::now();
            let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
                .unwrap_or_else(|error| panic!("lower k={k}: {error}"));
            let elapsed = started.elapsed();
            drop(image);
            elapsed
        })
        .min()
        .expect("at least one repetition")
}

#[test]
#[ignore = "explicit P14 route lowering time budget: run with --release --test-threads=1"]
fn fat_tree_lowering_time_scales_with_flows() {
    let small = lowering_time(SMALL_K);
    let large = lowering_time(LARGE_K);
    if let Some(failure) = check(
        "lowering_time",
        "ns",
        small.as_nanos() as f64,
        large.as_nanos() as f64,
        max_allocation_ratio(),
    ) {
        panic!("{failure}");
    }
}

/// The measured path is production's: for each size, the serial-route lowering the cases above
/// measure produces the same image as `compile_config`, which uses the host's route workers.
#[test]
fn serial_route_lowering_is_the_compile_config_image() {
    for k in [SMALL_K, LARGE_K] {
        let path = frontier_traffic_scenario("path_identity", k);
        let production =
            compile_config(&path).unwrap_or_else(|error| panic!("lower k={k}: {error}"));
        let measured = compile_config_with_route_workers(&path, RouteWorkers::serial())
            .unwrap_or_else(|error| panic!("lower k={k} serially: {error}"));
        assert!(
            production == measured,
            "k={k}: the serial-route image differs from compile_config's"
        );
    }
}

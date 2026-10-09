//! P15 lane R1: deterministic heap budgets of RoCE queue pairs.
//!
//! A queue pair's per-host state is its generator (inline in the 352-B generator record, which
//! `image::tests` holds) and, on its target host, one receiver in the host's boxed
//! `roce_receivers` slice: one allocation per receiving host, none on a host without one (ruling
//! D1). These budgets hold that through lowering and both executors, against the same scenario
//! with open-loop flows, which `cpu_host_lp_heap_budget` pins to `main`.
//!
//! **What is measured.** As in `cpu_host_lp_heap_budget`: the allocations this thread makes while
//! lowering, and during one-worker CPU and Scalar runs that stop before their first event, for two
//! E1-shaped scenarios on the same k = 8 fat tree that differ only in hosts per edge switch (64 and
//! 128 hosts, one flow each), so the switches and every fixed cost cancel. The counts are a pure
//! function of the code, the scenario and the toolchain's container implementations.
//!
//! Run: `cargo test -p days --test p15_roce_budgets` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, SimulationImage, run_cpu, run_scalar};

struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

const E1_FIXTURE: &str = "configs/benchmarks/evaluation/e1_open_k32_load_10.toml";
const K: u64 = 8;
const EDGE_SWITCHES: u64 = K * K / 2;
const SMALL_HOSTS_PER_EDGE: u64 = 2;
const LARGE_HOSTS_PER_EDGE: u64 = 4;
/// Hosts the larger scenario adds; every one sources one flow and receives one.
const ADDED_HOSTS: u64 = EDGE_SWITCHES * (LARGE_HOSTS_PER_EDGE - SMALL_HOSTS_PER_EDGE);

/// The E1 flow set's open-loop traffic, and the queue-pair traffic that replaces it.
const OPEN_LOOP_TRAFFIC: &str = "traffic = { initial_delay = 0.0, size = 1_540_000, arr_dist = { type = \"Uniform\", low = 0.000001232, high = 0.000001232 }, pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 } }";
const TCP_TRAFFIC: &str = "traffic = { initial_delay = 0.0, size = 1_540_000, arr_dist = { type = \"Uniform\", low = 1, high = 1 }, pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, tcp = { cc_algorithm = \"Reno\" } }";
const QUEUE_PAIR_TRAFFIC: &str = "traffic = { initial_delay = 0.0, size = 1_540_000, arr_dist = { type = \"Uniform\", low = 1, high = 1 }, pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, dcqcn = { rate_gbps = 100.0, min_rate_gbps = 0.1, max_rate_gbps = 100.0, g = 0.00390625, ai_rate_gbps = 0.02, hai_rate_gbps = 0.2, rp_timer_ns = 50000, pacing_interval_ns = 100 }, roce = { retransmit_timeout_ns = 1_000_000 } }";

/// E1 on a k = 8 fat tree with `hosts_per_edge` hosts per edge switch and one flow per host,
/// open-loop or queue pairs, written to a file private to this process and test.
/// The transport of every flow of a derived scenario.
#[derive(Clone, Copy, Debug)]
enum Transport {
    OpenLoop,
    Tcp,
    QueuePair,
}

fn e1_k8(test: &str, hosts_per_edge: u64, transport: Transport) -> PathBuf {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(E1_FIXTURE))
        .expect("read the E1 fixture");
    let mut substitutions = vec![
        ("\nk = 32\n".to_owned(), format!("\nk = {K}\n")),
        (
            "\nhosts_per_edge = 16\n".to_owned(),
            format!("\nhosts_per_edge = {hosts_per_edge}\n"),
        ),
        (
            "\nflow_count = 8192\n".to_owned(),
            format!("\nflow_count = {}\n", EDGE_SWITCHES * hosts_per_edge),
        ),
    ];
    let closed_loop = match transport {
        Transport::OpenLoop => None,
        Transport::Tcp => Some(("TCP", TCP_TRAFFIC)),
        Transport::QueuePair => Some(("RoCE", QUEUE_PAIR_TRAFFIC)),
    };
    if let Some((flow_type, traffic)) = closed_loop {
        substitutions.push((
            "flow_type = \"PacketDistribution\"".to_owned(),
            format!("flow_type = \"{flow_type}\""),
        ));
        substitutions.push((OPEN_LOOP_TRAFFIC.to_owned(), traffic.to_owned()));
    }
    let text = substitutions.iter().fold(text, |text, (from, to)| {
        assert_eq!(text.matches(from.as_str()).count(), 1, "{from:?}");
        text.replace(from.as_str(), to)
    });
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p15_roce_budgets_{test}_h{hosts_per_edge}_{transport:?}_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived scenario");
    path
}

/// The allocations of lowering, and of one-worker CPU and Scalar runs stopping before the first
/// event, for one derived scenario.
fn phase_allocations(hosts_per_edge: u64, transport: Transport) -> [u64; 3] {
    let path = e1_k8("phases", hosts_per_edge, transport);
    let before = allocations();
    let image: SimulationImage = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived scenario lowers");
    let lowering = allocations() - before;
    let _ = fs::remove_file(&path);
    let before = allocations();
    let run = run_cpu(
        &image,
        Some(1),
        CpuConfig {
            workers: 1,
            ..CpuConfig::default()
        },
    )
    .expect("the CPU run succeeds");
    let cpu = allocations() - before;
    drop(run);
    let before = allocations();
    let result = run_scalar(&image, Some(1)).expect("the Scalar run succeeds");
    let scalar = allocations() - before;
    drop(result);
    [lowering, cpu, scalar]
}

/// Allocations each added queue-pair host may make beyond an added open-loop host, per phase.
///
/// - Lowering, measured at 3.98 per host when this budget was set: the receiver slice, and the
///   reverse-route feedback a closed-loop flow adds (channel bounds and their derived delays, as
///   TCP's ACKs add), plus one for container drift. Lowering builds each slice at its exact length
///   and the validator checks a pair with fixed-width arithmetic, so neither reallocates nor
///   allocates per pair.
/// - CPU and Scalar runs, measured below the open-loop marginal (no pre-produced data packet):
///   one for container drift.
const MAX_EXTRA_ALLOCATIONS_PER_HOST: [u64; 3] = [5, 1, 1];

#[test]
fn a_queue_pair_host_costs_one_receiver_table_and_bounded_bookkeeping() {
    let marginal = |transport| {
        let small = phase_allocations(SMALL_HOSTS_PER_EDGE, transport);
        let large = phase_allocations(LARGE_HOSTS_PER_EDGE, transport);
        [0, 1, 2].map(|phase| large[phase] - small[phase])
    };
    let tcp = marginal(Transport::Tcp);
    println!("record=p15_roce_budget_reference tcp_marginals={tcp:?}");
    let open_loop = [[0; 3], marginal(Transport::OpenLoop)];
    let queue_pairs = [[0; 3], marginal(Transport::QueuePair)];
    let phases = ["lowering", "cpu", "scalar"]
        .into_iter()
        .enumerate()
        .map(|(phase, name)| {
            // A queue pair is a closed-loop transport; it must cost no more than TCP.
            assert!(
                queue_pairs[1][phase] <= tcp[phase],
                "{ADDED_HOSTS} more queue-pair hosts made {} more {name} allocations, above \
                 TCP's {}",
                queue_pairs[1][phase],
                tcp[phase]
            );
            let open_marginal = open_loop[1][phase] - open_loop[0][phase];
            let pair_marginal = queue_pairs[1][phase] - queue_pairs[0][phase];
            let cap = open_marginal + ADDED_HOSTS * MAX_EXTRA_ALLOCATIONS_PER_HOST[phase];
            println!(
                "record=p15_roce_budget phase={name} open_loop_marginal={open_marginal} \
                 queue_pair_marginal={pair_marginal} added_hosts={ADDED_HOSTS} cap={cap}"
            );
            (phase, name, open_marginal, pair_marginal, cap)
        })
        .collect::<Vec<_>>();
    for (phase, name, open_marginal, pair_marginal, cap) in phases {
        assert!(
            pair_marginal <= cap,
            "{ADDED_HOSTS} more queue-pair hosts made {pair_marginal} more {name} allocations, \
             above {cap} ({open_marginal} for open-loop hosts plus {} per host)",
            MAX_EXTRA_ALLOCATIONS_PER_HOST[phase]
        );
    }
}

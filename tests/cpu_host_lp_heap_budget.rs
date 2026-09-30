//! P14 E1 residue contract: a CPU host LP carries no more heap objects than a Scalar host does.
//!
//! P14 gave every host an executor-local stage index (`HostStageSlot`, `executor/src/stage_index.rs`)
//! and kept it in `TransitionState::host_indices`, a vector parallel to `host_states`. Scalar holds
//! one `TransitionState` for every host, so the parallel vector is one allocation for the whole
//! run. The CPU executor builds one `TransitionState` per LP, so the parallel vector became one more
//! heap object per host LP, and its 24-B handle grew every LP of the CPU executor from 592 B to
//! 624 B (`TransitionState` is 16-byte aligned by `RunSummary`'s `u128` counters). On E1
//! (8,192 host LPs, one generator each) that is 16,384 extra allocations and 13.8 MB more allocated
//! per CPU run, against 8,192 and 1.3 MB for Scalar; the CPU `--workers 1` run measured about 2%
//! slower than `main` on madrid while Scalar stayed at parity
//! (`days-gpu/evidence/P14/e1-residue.md`).
//!
//! **What is measured.** The allocations this thread makes during `run_cpu` with one worker and
//! a 1 ns exclusive horizon, so no event runs: LP construction (`build_lps`, on the calling
//! thread), their placement, and the result assembly. The per-host figure is the marginal between
//! two E1-shaped scenarios on the same k = 8 fat-tree that differ only in hosts per edge switch
//! (2 and 4: 64 and 128 hosts, one flow each), so the switches and every fixed cost cancel. This
//! binary's global allocator counts on the calling thread only, so the worker thread and the other
//! tests of this binary cannot perturb it. The count is a pure function of the code, the scenario
//! and the toolchain's container implementations.
//!
//! **The cap.** The marginal at `main` (948a0e9), 882 allocations for the 64 added hosts, plus
//! one allocation per added host for the 16-B `generators_by_flow` table the stage index gives
//! every host (the per-host cost the Scalar executor carries at parity), plus half an allocation
//! per added host of headroom for container drift between toolchains: 978. Before the fix
//! (10d99fa) every host LP makes one more allocation, its own index vector, and the marginal is
//! 1,010, so this test fails; with each host's index kept in the host's own entry it is 946.
//!
//! Run: `cargo test -p days --test cpu_host_lp_heap_budget` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, run_cpu, run_scalar};

/// Allocations made by the current thread.
///
/// The counter is thread-local, so tests that run beside this one in the same binary, and the CPU
/// executor's worker thread, cannot perturb it; nothing is shared between threads.
struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
}

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

// SAFETY: every method forwards to `System` unchanged and only adds bookkeeping.
unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's layout.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation();
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's pointer and layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's pointer, layout and size.
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

const E1_FIXTURE: &str = "configs/benchmarks/evaluation/e1_open_k32_load_10.toml";
/// Fat-tree arity of the derived scenarios; `K * K / 2` edge switches.
const K: u64 = 8;
const EDGE_SWITCHES: u64 = K * K / 2;
/// Hosts per edge switch of the smaller and the larger scenario.
const SMALL_HOSTS_PER_EDGE: u64 = 2;
const LARGE_HOSTS_PER_EDGE: u64 = 4;

/// Hosts the larger scenario adds.
const ADDED_HOSTS: u64 = EDGE_SWITCHES * (LARGE_HOSTS_PER_EDGE - SMALL_HOSTS_PER_EDGE);

/// The marginal at `main` (948a0e9) for these two scenarios, measured by this test.
const MAIN_MARGINAL_ALLOCATIONS: u64 = 882;
/// The stage index's `generators_by_flow` table, which every Scalar and CPU host carries.
const STAGE_INDEX_ALLOCATIONS_PER_HOST: u64 = 1;
/// Headroom for container drift: half an allocation per added host, below the one per host that
/// a separate per-LP index vector costs.
const HEADROOM_ALLOCATIONS: u64 = ADDED_HOSTS / 2;
/// The cap on the marginal allocations of the added hosts: 978.
const MAX_MARGINAL_ALLOCATIONS: u64 = MAIN_MARGINAL_ALLOCATIONS
    + ADDED_HOSTS * STAGE_INDEX_ALLOCATIONS_PER_HOST
    + HEADROOM_ALLOCATIONS;

/// E1 on a k = 8 fat-tree with `hosts_per_edge` hosts under every edge switch, one flow per host,
/// written to a file private to this process and to the calling test, named by `test`: the tests
/// of this binary run in parallel, so a shared file would be rewritten while another test reads it.
fn e1_k8(test: &str, hosts_per_edge: u64) -> PathBuf {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(E1_FIXTURE))
        .expect("read the E1 fixture");
    let substitutions = [
        ("\nk = 32\n", format!("\nk = {K}\n")),
        (
            "\nhosts_per_edge = 16\n",
            format!("\nhosts_per_edge = {hosts_per_edge}\n"),
        ),
        (
            "\nflow_count = 8192\n",
            format!("\nflow_count = {}\n", EDGE_SWITCHES * hosts_per_edge),
        ),
    ];
    let text = substitutions.iter().fold(text, |text, (from, to)| {
        assert_eq!(text.matches(from).count(), 1, "{from:?}");
        text.replace(from, to)
    });
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "cpu_host_lp_heap_budget_{test}_k{K}_h{hosts_per_edge}_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived E1 scenario");
    path
}

/// Allocations this thread makes during a one-worker CPU run of the derived scenario that stops
/// before its first event.
fn cpu_run_allocations(hosts_per_edge: u64) -> u64 {
    let path = e1_k8("allocations", hosts_per_edge);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived E1 scenario lowers");
    let _ = fs::remove_file(&path);
    let config = CpuConfig {
        workers: 1,
        ..CpuConfig::default()
    };
    let before = allocations();
    let run = run_cpu(&image, Some(1), config).expect("the CPU run succeeds");
    let after = allocations();
    drop(run);
    after - before
}

#[test]
fn cpu_host_lp_allocates_no_more_than_a_scalar_host() {
    let small = cpu_run_allocations(SMALL_HOSTS_PER_EDGE);
    let large = cpu_run_allocations(LARGE_HOSTS_PER_EDGE);
    let marginal = large - small;
    println!(
        "record=cpu_host_lp_heap_budget small={small} large={large} added_hosts={ADDED_HOSTS} \
         marginal={marginal} cap={MAX_MARGINAL_ALLOCATIONS}"
    );
    assert!(
        marginal <= MAX_MARGINAL_ALLOCATIONS,
        "{ADDED_HOSTS} more host LPs made {marginal} more allocations, above the cap of \
         {MAX_MARGINAL_ALLOCATIONS}: keep a host's stage index in the host's own entry"
    );
}

/// The Scalar result's host table holds its hosts and no spare capacity.
///
/// The executor keeps each host's stage index beside its state in one entry per host and hands
/// the states back when the run finishes. Collecting the states out of the entries in place would
/// keep the entries' larger buffer, 120 B of spare capacity per host (983 kB on E1) retained with
/// the result for as long as the caller holds it; `main` returned an exact clone.
#[test]
fn scalar_result_host_states_carry_no_spare_capacity() {
    let path = e1_k8("capacity", SMALL_HOSTS_PER_EDGE);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived E1 scenario lowers");
    let _ = fs::remove_file(&path);
    let result = run_scalar(&image, Some(1)).expect("the Scalar run succeeds");
    assert_eq!(result.host_states.len(), image.host_states.len());
    assert_eq!(
        result.host_states.capacity(),
        result.host_states.len(),
        "the Scalar result's host states keep spare capacity"
    );
}

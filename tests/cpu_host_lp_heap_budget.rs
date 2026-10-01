//! P14 E1 residue contract: a CPU host LP carries no more heap objects than a Scalar host does.
//!
//! P14 gave every host an executor-local stage index (`HostStageSlot`, `executor/src/stage_index.rs`)
//! and kept it in `TransitionState::host_indices`, a vector parallel to `host_states`. Scalar holds
//! one `TransitionState` for every host, so the parallel vector is one allocation for the whole
//! run. The CPU executor builds one `TransitionState` per LP, so the parallel vector became one more
//! heap object per host LP, and its 24-B handle grew every LP of the CPU executor from 592 B to
//! 624 B (`TransitionState` is 16-byte aligned by `RunSummary`'s `u128` counters). On E1
//! (8,192 host LPs, one generator each) that is 16,384 extra allocations and 13.8 MB more allocated
//! per CPU run, against 8,192 and 1.3 MB for Scalar. Removing that growth is what the fix targets.
//! madrid's re-timing measured the CPU `--workers 1` E1 run recover from +2.3% over `main` to
//! +0.22% (median paired; -1.96% against the unfixed tip, 39 of 40 pairs). The recovery is
//! measured; that the memory footprint is its mechanism is the fix's premise, not something
//! measured directly (`days-gpu/evidence/P14/e1-residue.md`, `e1-retime.md`).
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

/// Allocations made by the current thread, and the bytes it obtained from the allocator.
///
/// The counters are thread-local, so tests that run beside this one in the same binary, and the
/// CPU executor's worker thread, cannot perturb them; nothing is shared between threads. Bytes
/// obtained are the sizes of fresh allocations plus the new sizes of reallocations: a block
/// resized in place is counted again, since its contents may be moved or compacted.
struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
    static BYTES_OBTAINED: Cell<u64> = const { Cell::new(0) };
}

fn record_allocation(bytes: usize) {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
    record_bytes(bytes);
}

fn record_bytes(bytes: usize) {
    let _ = BYTES_OBTAINED.try_with(|total| total.set(total.get().wrapping_add(bytes as u64)));
}

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

fn bytes_obtained() -> u64 {
    BYTES_OBTAINED.with(Cell::get)
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
            record_bytes(new_size);
        }
        resized
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
/// The Scalar executor clones the image's host-state table exactly and hands it back when the run
/// finishes. A table built any other way can keep spare capacity: collecting the states out of
/// per-host entries in place kept the entries' larger buffer (`4c57401`), 120 B per host (983 kB
/// on E1) retained with the result for as long as the caller holds it.
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

/// Bytes the Scalar run obtained per added host at the unfixed tip (10d99fa), measured by this
/// test: the host states arrive as the image clone the result hands back.
const HEAD_MARGINAL_SCALAR_BYTES: u64 = 213_048;
/// Headroom for container drift: half a `HostState` per added host, below the whole `HostState`
/// per host that a copy of the host-state table costs.
const HEADROOM_SCALAR_BYTES: u64 = ADDED_HOSTS * 100;
/// The cap on the bytes the Scalar run obtains for the added hosts: 219,448.
const MAX_MARGINAL_SCALAR_BYTES: u64 = HEAD_MARGINAL_SCALAR_BYTES + HEADROOM_SCALAR_BYTES;

/// Bytes this thread obtains from the allocator during a Scalar run of the derived scenario that
/// stops before its first event.
fn scalar_run_bytes(hosts_per_edge: u64) -> u64 {
    let path = e1_k8("scalar_bytes", hosts_per_edge);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived E1 scenario lowers");
    let _ = fs::remove_file(&path);
    let before = bytes_obtained();
    let result = run_scalar(&image, Some(1)).expect("the Scalar run succeeds");
    let after = bytes_obtained();
    drop(result);
    after - before
}

/// The Scalar run hands back the host-state table it cloned from the image: it neither allocates
/// a fresh table for the result nor reallocates one.
///
/// **What is measured.** The bytes this thread obtains from the allocator during `run_scalar`
/// with a 1 ns exclusive horizon (construction, the events at t = 0 and `finish`), fresh
/// allocations and reallocations alike, as the marginal between the two E1-shaped scenarios above
/// (64 added hosts). A fresh result table adds one `HostState` (200 B) per host to the marginal,
/// and compacting the states in place into a narrower buffer adds its reallocation, so either
/// fails the cap. The count is a pure function of the code, the scenario and the toolchain's
/// container implementations.
///
/// **The cap.** The unfixed tip's marginal, 213,048 B, plus half a `HostState` per added host:
/// 219,448 B. With one entry per host holding state and index together, `finish` moved the states
/// into a fresh exact table (`acfeb82`, 225,848 B) or compacted them in place (`4c57401`,
/// 233,448 B), and this test fails.
#[test]
fn scalar_run_hands_back_its_host_states_without_a_copy() {
    let small = scalar_run_bytes(SMALL_HOSTS_PER_EDGE);
    let large = scalar_run_bytes(LARGE_HOSTS_PER_EDGE);
    let marginal = large - small;
    println!(
        "record=scalar_run_bytes small={small} large={large} added_hosts={ADDED_HOSTS} \
         marginal={marginal} cap={MAX_MARGINAL_SCALAR_BYTES}"
    );
    assert!(
        marginal <= MAX_MARGINAL_SCALAR_BYTES,
        "{ADDED_HOSTS} more hosts made the Scalar run obtain {marginal} more bytes, above the cap \
         of {MAX_MARGINAL_SCALAR_BYTES}: hand back the cloned host-state table, not a copy"
    );
}

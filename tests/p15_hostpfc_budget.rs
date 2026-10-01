//! P15 host-link PFC: the deterministic heap cost of a host's egress pause state.
//!
//! `HostState::pfc` is one optional box (8 B per host; `image::tests` pins the size). Here the
//! allocations it costs are pinned: a host without a PFC-controlled egress link allocates nothing
//! for it, and a host with one costs one block, in the image and in every executor's copy of the
//! host state. The HPCC fixture (`configs/p15/hpcc_incast64_dragonfly.toml`) is measured with and
//! without `host_links`: the 64 sender hosts and the receiver own pause state; its 325 idle hosts
//! do not. One-worker CPU and Scalar runs stop before their first event, so the counts are those
//! of building the run's state.
//!
//! Run: `cargo test -p days --test p15_hostpfc_budget` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::Path;

use days::scenario::compile_config;
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

const FIXTURE: &str = "configs/p15/hpcc_incast64_dragonfly.toml";

/// The HPCC fixture, with or without `host_links`.
fn lower(host_links: bool) -> SimulationImage {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    if host_links {
        return compile_config(&path).expect("the HPCC fixture lowers");
    }
    let text = fs::read_to_string(&path).expect("read the HPCC fixture");
    assert_eq!(text.matches("\nhost_links = true\n").count(), 1);
    let edited = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("p15_hostpfc_budget_{}.toml", std::process::id()));
    fs::write(&edited, text.replace("\nhost_links = true\n", "\n")).expect("write variant");
    let image = compile_config(&edited).expect("the switch-only variant lowers");
    let _ = fs::remove_file(&edited);
    image
}

/// Allocations of cloning the image, and of one-worker CPU and Scalar runs that stop before the
/// first event.
fn state_allocations(image: &SimulationImage) -> [u64; 3] {
    let before = allocations();
    let copy = image.clone();
    let clone = allocations() - before;
    drop(copy);
    let before = allocations();
    let run = run_cpu(
        image,
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
    let result = run_scalar(image, Some(1)).expect("the Scalar run succeeds");
    let scalar = allocations() - before;
    drop(result);
    [clone, cpu, scalar]
}

#[test]
fn a_host_pays_one_block_for_egress_pause_state_and_an_idle_host_none() {
    let with = lower(true);
    let without = lower(false);
    let paused_hosts = with
        .host_states
        .iter()
        .filter(|state| state.pfc.is_some())
        .count() as u64;
    assert_eq!(paused_hosts, 65, "64 senders and the receiver");
    let idle_without_pfc = with
        .host_states
        .iter()
        .filter(|state| state.generators.is_empty() && state.roce_receivers.is_none())
        .all(|state| state.pfc.is_none());
    assert!(
        idle_without_pfc,
        "a host without a controlled link owns no pause state"
    );
    // Switch egress queues that monitor a host link own a PFC record with its ingress table, as
    // a switch-to-switch monitor's queue does: one block per monitoring queue.
    let monitoring_queues = |image: &SimulationImage| {
        image
            .switch_states
            .iter()
            .flat_map(|state| &state.queues)
            .filter(|queue| {
                queue
                    .pfc
                    .as_ref()
                    .is_some_and(|pfc| !pfc.ingresses.is_empty())
            })
            .count() as u64
    };
    let added_queues = monitoring_queues(&with) - monitoring_queues(&without);
    assert_eq!(
        added_queues, 65,
        "router 0's egress to the receiver and to each sender"
    );
    let with_counts = state_allocations(&with);
    let without_counts = state_allocations(&without);
    // Measured when this budget was set: 130 / 131 / 130. The CPU run's one extra block is a
    // per-image container that grows with the channel table, not a per-host cost.
    let caps = [
        paused_hosts + added_queues,
        paused_hosts + added_queues + 1,
        paused_hosts + added_queues,
    ];
    for (phase, name) in ["image clone", "cpu", "scalar"].into_iter().enumerate() {
        let marginal = with_counts[phase] - without_counts[phase];
        println!(
            "record=p15_hostpfc_budget phase={name} with={} without={} marginal={marginal} \
             paused_hosts={paused_hosts} monitoring_queues={added_queues} cap={}",
            with_counts[phase], without_counts[phase], caps[phase]
        );
        assert!(
            marginal <= caps[phase],
            "{name}: host-link PFC made {marginal} more allocations, above {} (one per host \
             with pause state, one per monitoring switch queue)",
            caps[phase]
        );
    }
}

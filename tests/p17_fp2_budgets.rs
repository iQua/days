//! P17 FP2: deterministic heap budgets of the host future-event list and packet store.
//!
//! Scalar's run loop popped and inserted every event through one `BTreeMap<EventKey, Event>`,
//! and every packet lookup went through one `BTreeMap<PayloadId, ResidentPacket>`. A B-tree
//! allocates and frees nodes as it splits and merges, so a run's allocation count grew with its
//! event count (`days-gpu/evidence/P17/perevent/tables/memory.txt`: 3.14 M allocations on the
//! Scalar a2a twin at `5c7629ff`). The FP2 future-event list is a binary heap, whose one buffer
//! keeps its high-water capacity, and the FP2 packet store is a slab with a free list behind an
//! open-addressing index, so a run allocates when a structure reaches a new high-water size and
//! not per event.
//!
//! **What is measured.** The allocations this thread makes during one whole Scalar run, which runs
//! on the calling thread, of `configs/p17/nocc_gbn_lossy.toml`: RoCE queue pairs on a lossy
//! fabric, 21,615 departed packets. The fixture has no PFC queue, so no debug-only check
//! allocates, and the count is the same in debug and release builds (measured at all three trees
//! below). The count is a pure function
//! of the code, the scenario and the toolchain's container implementations.
//!
//! | tree | allocations |
//! |---|---:|
//! | `5c7629ff`: ordered-map future-event list and packet store | 8,659 |
//! | heap future-event list, ordered-map packet store | 901 |
//! | heap future-event list, slab packet store | 103 |
//!
//! Each structure has its own cap. `5c7629ff` fails both tests; the tree with only the heap
//! future-event list passes the first and fails the second.
//!
//! Run: `cargo test -p days --test p17_fp2_budgets` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::run_scalar;

/// Allocations made by the current thread.
///
/// The counter is thread-local, so tests that run beside these in the same binary cannot perturb
/// it; nothing is shared between threads. A reallocation counts as an allocation.
struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn count_allocation() {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
}

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count_allocation();
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

const FIXTURE: &str = "configs/p17/nocc_gbn_lossy.toml";

/// The departed packets and the allocations of a whole Scalar run of `FIXTURE` on this thread.
fn scalar_run_allocations() -> (u128, u64) {
    let image = compile_config_with_route_workers(
        Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE),
        RouteWorkers::serial(),
    )
    .expect("the fixture lowers");
    let before = ALLOCATIONS.with(Cell::get);
    let result = run_scalar(&image, None).expect("the Scalar run succeeds");
    let allocations = ALLOCATIONS.with(Cell::get) - before;
    let departed = result.summary.departed_packets;
    drop(result);
    println!(
        "record=p17_fp2_scalar_run_allocations fixture={FIXTURE} departed_packets={departed} \
         allocations={allocations}"
    );
    assert_eq!(departed, 21_615, "the fixture's run changed");
    (departed, allocations)
}

/// The cap with a heap future-event list: its measured 901, plus about 1,100 of headroom for
/// unrelated allocations, far below the 8,659 of a B-tree future-event list, which allocates and
/// frees nodes as events pass through it.
const MAX_ALLOCATIONS_WITH_A_HEAP_FUTURE_EVENT_LIST: u64 = 2_000;

/// The cap with a slab packet store as well: its measured 103, plus about 100 of headroom, far
/// below the 901 of a B-tree packet store, which allocates and frees nodes as packets pass
/// through it.
const MAX_ALLOCATIONS_WITH_A_SLAB_PACKET_STORE: u64 = 200;

/// The future-event list allocates when it reaches a new high-water size, not per event.
#[test]
fn the_future_event_list_does_not_allocate_per_event() {
    let (departed, allocations) = scalar_run_allocations();
    assert!(
        allocations <= MAX_ALLOCATIONS_WITH_A_HEAP_FUTURE_EVENT_LIST,
        "a Scalar run of {FIXTURE} ({departed} departed packets) made {allocations} allocations, \
         above the cap of {MAX_ALLOCATIONS_WITH_A_HEAP_FUTURE_EVENT_LIST}: keep the future-event \
         list in one heap buffer"
    );
}

/// The packet store allocates when it reaches a new high-water size, not per packet.
#[test]
fn the_packet_store_does_not_allocate_per_packet() {
    let (departed, allocations) = scalar_run_allocations();
    assert!(
        allocations <= MAX_ALLOCATIONS_WITH_A_SLAB_PACKET_STORE,
        "a Scalar run of {FIXTURE} ({departed} departed packets) made {allocations} allocations, \
         above the cap of {MAX_ALLOCATIONS_WITH_A_SLAB_PACKET_STORE}: keep the packet store in a \
         slab with a free list"
    );
}

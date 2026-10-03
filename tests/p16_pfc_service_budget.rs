//! P16 PFC service budget: a service decision at a PFC switch queue does work, and allocates,
//! independently of the queue's depth.
//!
//! Until P16 every `TxReady` decision at a PFC queue built the whole-queue eligible-packet plan:
//! for every queued entry it looked up the packet, its flow's PFC class and its incoming link, and
//! it pushed four vectors (`queue_serves_head` returned false whenever a queue had PFC). On the
//! HPCC incast (`configs/p15/hpcc_incast64_dragonfly.toml`) the mean queue depth at a decision is
//! 676, so the run read 173 M entries over 256,000 decisions, about 1.39x `main`'s Scalar
//! instructions at about 217 instructions per entry (`days-gpu/evidence/P16/dcqcn-review.md`,
//! finding F2). A completed transmission at a PFC queue and every PFC control frame searched the
//! queue in the same way.
//!
//! **What is counted.** A test-only probe
//! (`days_executor::scalar::run_scalar_counting_pfc_service_for_testing`) counts the `TxReady`
//! decisions made at PFC queues and the queued entries the PFC service paths *read*: an entry is
//! read when its packet record is looked up, for its PFC class or its incoming link. The paths are
//! the `TxReady` decision, the first-eligible search after a transmission completes, and the
//! search on a PFC control frame. The allocations are those the calling thread makes during the
//! Scalar run, counted by this binary's thread-local allocator.
//!
//! **The budgets.** The fixtures here use FIFO queues, the discipline every RoCE and HPCC fixture
//! uses. Serving the head of such a queue reads one entry; with the head's class paused, the
//! per-class order index finds the first eligible packet with one more read. Building that index
//! reads the queue once, when the queue is first paused. [`MAX_READS_PER_DECISION`] (2) is the
//! bound; the whole-queue plan read 8.7 to 10.1 entries per decision on these fixtures and 676 on
//! the HPCC incast.
//!
//! Run: `cargo test -p days --features test --test p16_pfc_service_budget` (debug or release);
//! the HPCC case is release-only: add `--release -- --ignored`.
#![cfg(feature = "test")]
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::path::Path;

use days::scenario::compile_config;
use days_executor::scalar::run_scalar_counting_pfc_service_for_testing;
use days_executor::{ObservationMode, SimulationImage, run_scalar_with_observations};

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

/// Queued entries the PFC service paths may read per `TxReady` decision at a PFC queue.
const MAX_READS_PER_DECISION: u64 = 2;

fn lower(name: &str) -> SimulationImage {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{name} must lower: {error}"))
}

/// The run's PFC decisions, the entries they read, and the thread's allocations during the run.
fn measure(image: &SimulationImage) -> (u64, u64, u64) {
    let before = allocations();
    let (result, decisions, reads) =
        run_scalar_counting_pfc_service_for_testing(image, ObservationMode::Summary)
            .expect("the Scalar run succeeds");
    let allocated = allocations() - before;
    // The probe only counts: the run is the ordinary Scalar run.
    assert_eq!(
        result,
        run_scalar_with_observations(image, None, ObservationMode::Summary)
            .expect("the Scalar run succeeds"),
    );
    (decisions, reads, allocated)
}

fn assert_reads_within_budget(name: &str) -> (u64, u64, u64) {
    let image = lower(name);
    let (decisions, reads, allocated) = measure(&image);
    println!(
        "record=p16_pfc_service case={name} decisions={decisions} reads={reads} \
         allocations={allocated}"
    );
    assert!(decisions > 0, "{name}: the fixture must serve PFC queues");
    assert!(
        reads <= MAX_READS_PER_DECISION * decisions,
        "{name}: the PFC service paths read {reads} queued entries over {decisions} decisions, \
         above {MAX_READS_PER_DECISION} per decision"
    );
    (decisions, reads, allocated)
}

#[test]
fn roce_lossless_pfc_reads_a_bounded_number_of_entries_per_decision() {
    assert_reads_within_budget("configs/p15/roce_lossless_pfc.toml");
}

#[test]
fn hostpfc_incast_reads_a_bounded_number_of_entries_per_decision() {
    assert_reads_within_budget("configs/p15/hostpfc_incast_lossless.toml");
}

#[test]
fn hostpfc_multi_qp_tcp_reads_a_bounded_number_of_entries_per_decision() {
    assert_reads_within_budget("configs/p15/hostpfc_multi_qp_tcp.toml");
}

#[test]
fn dcqcn_t26_pfc_reads_a_bounded_number_of_entries_per_decision() {
    assert_reads_within_budget("configs/p14/dcqcn_t26_pfc.toml");
}

/// The HPCC incast: 256,000 decisions at a mean depth of 676 before P16. Release-only.
#[test]
#[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
fn hpcc_incast_reads_a_bounded_number_of_entries_per_decision() {
    assert_reads_within_budget("configs/p15/hpcc_incast64_dragonfly.toml");
}

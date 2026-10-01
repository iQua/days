//! P14 E1b: the node-ID check of `validate`, pinned by its messages and their precedence.
//!
//! `validate_node_ids` is the first check `validate` runs, so for a crafted node table the first
//! error `validate` returns is that check's, if it rejects the table. Its rule, descriptor by
//! descriptor in table order, is: a repeated ID is a duplicate; else an ID at or above the table
//! length is out of range; else an ID different from its position does not match the dense index.
//! The first failing descriptor decides, and at that descriptor the first failing rule.
//!
//! A table the check accepts reaches the next check, `validate_link_ids`. The rows below that must
//! pass therefore also corrupt the first link's ID and expect that check's message, which proves
//! the node check let the table through.
//!
//! Duplicate and out of range cannot both hold at one descriptor (a repeated ID equals an earlier
//! position, which is below the length), so no row can order those two rules.
//!
//! The check also builds no set of the IDs it has seen: every earlier descriptor passed, so the
//! IDs seen are exactly the positions before the current one
//! (`node_id_check_allocates_nothing_per_node`).
//!
//! Run: `cargo test -p days --test validate_node_ids` (default matrix, any profile).
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use days::scenario::compile_config;
use days_executor::{Backend, LinkId, NodeDescriptor, NodeId, NodeKind, SimulationImage, validate};

const FIXTURE: &str = "configs/ci/executor_smoke.toml";

fn image() -> SimulationImage {
    compile_config(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE))
        .expect("the smoke fixture lowers")
}

/// A node table holding `ids` in order; only the IDs matter to the check.
fn nodes(ids: &[u64]) -> Vec<NodeDescriptor> {
    ids.iter()
        .map(|&id| NodeDescriptor {
            id: NodeId(id),
            kind: NodeKind::Host,
            state_slot: 0,
        })
        .collect()
}

/// The image with `ids` as its node table and its first link's ID moved out of range, so a table
/// the node check accepts fails at the link check instead.
fn crafted(ids: &[u64]) -> SimulationImage {
    let mut image = image();
    image.nodes = nodes(ids);
    let links = image.links.len() as u64;
    image.links[0].id = LinkId(links);
    image
}

fn link_rejection(image: &SimulationImage) -> String {
    let links = image.links.len();
    format!("link ID LinkId({links}) at descriptor 0 is outside dense range 0..{links}")
}

fn rejection(ids: &[u64]) -> (String, String) {
    let image = crafted(ids);
    let error = validate(&image, Backend::Scalar)
        .expect_err("the crafted image is rejected")
        .to_string();
    (error, link_rejection(&image))
}

/// Every malformed node table gets the same first error, with the same message.
#[test]
fn node_id_rejections_keep_their_messages_and_precedence() {
    // (case, node IDs in table order, the expected first error; `None` = the node check passes).
    let cases: &[(&str, &[u64], Option<&str>)] = &[
        ("empty table", &[], None),
        ("one node", &[0], None),
        ("dense", &[0, 1, 2, 3], None),
        (
            "duplicate",
            &[0, 1, 1, 3],
            Some("duplicate node ID NodeId(1) at descriptor 2"),
        ),
        (
            "duplicate of the first ID",
            &[0, 1, 2, 0],
            Some("duplicate node ID NodeId(0) at descriptor 3"),
        ),
        (
            "out of range",
            &[0, 1, 9, 3],
            Some("node ID NodeId(9) at descriptor 2 is outside dense range 0..4"),
        ),
        (
            "out of range at the largest ID",
            &[0, 1, u64::MAX],
            Some(
                "node ID NodeId(18446744073709551615) at descriptor 2 is outside dense range 0..3",
            ),
        ),
        (
            "out of order",
            &[0, 2, 1, 3],
            Some("node ID NodeId(2) at descriptor 1 does not match dense table index 1"),
        ),
        (
            "gap",
            &[0, 1, 3, 4],
            Some("node ID NodeId(3) at descriptor 2 does not match dense table index 2"),
        ),
        (
            "first descriptor mismatched",
            &[1, 0],
            Some("node ID NodeId(1) at descriptor 0 does not match dense table index 0"),
        ),
        (
            "duplicate before mismatch at one descriptor",
            &[0, 0],
            Some("duplicate node ID NodeId(0) at descriptor 1"),
        ),
        (
            "out of range before mismatch at one descriptor",
            &[0, 7],
            Some("node ID NodeId(7) at descriptor 1 is outside dense range 0..2"),
        ),
        (
            "the first failing descriptor decides: range before a later duplicate",
            &[5, 5],
            Some("node ID NodeId(5) at descriptor 0 is outside dense range 0..2"),
        ),
        (
            "the first failing descriptor decides: duplicate before a later range",
            &[0, 1, 2, 2, 9],
            Some("duplicate node ID NodeId(2) at descriptor 3"),
        ),
    ];
    for &(case, ids, expected) in cases {
        let (error, link_error) = rejection(ids);
        let expected = expected.map_or(link_error, str::to_owned);
        assert_eq!(error, expected, "{case}: node IDs {ids:?}");
    }
}

/// The unmodified image validates.
#[test]
fn the_lowered_image_validates() {
    validate(&image(), Backend::Scalar).expect("the lowered smoke image validates");
}

/// Fresh allocations made by the current thread (reallocations are not counted).
///
/// The counter is thread-local, so the other test of this binary cannot perturb it.
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

/// Node-table sizes of the two crafted images.
const SMALL_NODES: u64 = 1_000;
const LARGE_NODES: u64 = 11_000;

/// Allocations this thread makes while `validate` checks a dense node table of `count` IDs and then
/// stops at the first link, whose ID is out of range.
fn node_check_allocations(count: u64) -> u64 {
    let ids = (0..count).collect::<Vec<_>>();
    let image = crafted(&ids);
    let expected = link_rejection(&image);
    let before = allocations();
    let result = validate(&image, Backend::Scalar);
    let after = allocations();
    assert_eq!(
        result
            .expect_err("the crafted image is rejected")
            .to_string(),
        expected,
        "the dense node table passes the node check"
    );
    after - before
}

/// The node-ID check allocates nothing per node.
///
/// **What is measured.** The fresh allocations this thread makes during `validate` on two images
/// that differ only in their dense node tables (1,000 and 11,000 IDs) and whose first link ID is
/// out of range, so `validate` runs the node check and then stops at the link check's first
/// descriptor. Everything but the node check is the same in both, so the marginal is the node
/// check's own allocations for 10,000 more nodes. Reallocations are not counted: the two error
/// messages differ in their digits. The count is a pure function of the code and the toolchain's
/// container implementations.
///
/// **The cap.** Zero. A set of the IDs seen allocates a B-tree node every few insertions (E1's
/// 49,152 nodes took 8,192 allocations per `validate`).
#[test]
fn node_id_check_allocates_nothing_per_node() {
    let small = node_check_allocations(SMALL_NODES);
    let large = node_check_allocations(LARGE_NODES);
    let marginal = large - small;
    println!(
        "record=node_id_check_allocations small={small} large={large} \
         added_nodes={} marginal={marginal} cap=0",
        LARGE_NODES - SMALL_NODES
    );
    assert_eq!(
        marginal,
        0,
        "{} more nodes made validate allocate {marginal} more times: check each node ID against \
         its position instead of a set of the IDs seen",
        LARGE_NODES - SMALL_NODES
    );
}

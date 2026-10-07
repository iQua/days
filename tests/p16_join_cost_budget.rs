//! P16 H1 ruling R1's hard gate, as deterministic budgets: counted joins must not make ordinary
//! collectives cost more to simulate. On four images without a join (a TCP ring and a RoCE ring,
//! each alone and between two compute groups), the allocations this thread makes while lowering,
//! validating and running Scalar (Full and Summary), and planning on Metal, may not exceed
//! `feat/p16`'s (669e16b, measured by this test there), and the image carries an empty
//! `stage_joins` table that owns no heap block. The
//! CPU executor's LP construction (a 1 ns horizon, so no event runs) is printed but not capped:
//! its calling-thread count varies by a few allocations with thread timing (227 to 230 and 233 to
//! 234 at the base); the paired instruction and allocation counters cover CPU runs.
//!
//! The counters are thread-local, so the other tests of this binary and the CPU executor's worker
//! thread cannot perturb them. A count is a pure function of the code, the scenario and the
//! toolchain's container implementations; the caps add no headroom, so a toolchain change that
//! moves them is re-measured at the base, not absorbed.
//!
//! Run: `cargo test -p days --test p16_join_cost_budget` (default matrix, any profile). The
//! Metal plan costs one allocation more in a debug build than in release, at `feat/p16` as here, so
//! its test caps each profile at that profile's own measurement.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{
    Backend, CpuConfig, ObservationMode, SimulationImage, run_cpu, run_scalar_with_observations,
    validate,
};

struct ThreadCountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().wrapping_add(1)));
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

/// A ring on four hosts of one switch, between two compute groups when `computes`; `transport`
/// is the traffic's transport tables.
fn scenario(flow_type: &str, transport: &str, computes: bool) -> String {
    let text = format!(
        r#"
seed = 26
edges = [[0, 4], [1, 4], [2, 4], [3, 4]]
hosts = [0, 1, 2, 3]
duration = 0.05

[switch]
port_rate = 8000000000
capacity = 200
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "fwd"
hosts = [0, 1, 2, 3]
duration_ns = 2000

[[collective]]
name = "ring"
collective_type = "RingAllReduce"
flow_type = "{flow_type}"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]
after = "fwd"

[collective.traffic]
initial_delay = 0.0
size = 12000
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 500, high = 500 }}
{transport}
[[compute]]
name = "bwd"
hosts = [0, 1, 2, 3]
duration_ns = 1000
after = "ring"
"#
    );
    if computes {
        text
    } else {
        let ring = text.find("[[collective]]").expect("a collective");
        let end = text
            .find("[[compute]]\nname = \"bwd\"")
            .expect("a successor");
        let (head, rest) = text.split_at(ring);
        let head = &head[..head.find("[[compute]]").expect("a predecessor")];
        format!(
            "{head}{}",
            rest[..end - ring].replace("after = \"fwd\"\n", "")
        )
    }
}

const TCP: &str = "\n[collective.traffic.tcp]\ncc_algorithm = \"TCPReno\"\n";
const ROCE: &str = "\n[collective.traffic.dcqcn]\nmax_rate_gbps = 8.0\npacing_interval_ns = 500\n\n[collective.traffic.roce]\nretransmit_timeout_ns = 1000000\n";

/// Allocations of each phase: lowering, validation, Scalar (Full), Scalar (Summary), CPU LP
/// construction.
fn phases(label: &str, config: &str) -> ([u64; 5], SimulationImage) {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p16-join-cost-{label}-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, config).expect("write fixture");
    let mut counts = [0; 5];
    let before = allocations();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial()).expect("lowers");
    counts[0] = allocations() - before;
    std::fs::remove_file(&path).expect("remove fixture");
    let before = allocations();
    validate(&image, Backend::Scalar).expect("validates");
    counts[1] = allocations() - before;
    for (slot, mode) in [(2, ObservationMode::Full), (3, ObservationMode::Summary)] {
        let before = allocations();
        let result = run_scalar_with_observations(&image, None, mode).expect("runs");
        counts[slot] = allocations() - before;
        drop(result);
    }
    let before = allocations();
    let result = run_cpu(
        &image,
        Some(1),
        CpuConfig {
            workers: 1,
            ..CpuConfig::default()
        },
    )
    .expect("runs");
    counts[4] = allocations() - before;
    drop(result);
    (counts, image)
}

/// The four images, with `feat/p16`'s counts (669e16b, measured by this test there): lowering,
/// validation, Scalar (Full), Scalar (Summary), and the Metal plan in a release build.
fn cases() -> [(&'static str, String, [u64; 5]); 4] {
    [
        (
            "tcp-ring",
            scenario("TCP", TCP, true),
            [786, 170, 2125, 2072, 311],
        ),
        (
            "roce-ring",
            scenario("RoCE", ROCE, true),
            [837, 174, 1132, 1080, 319],
        ),
        (
            "tcp-ring-alone",
            scenario("TCP", TCP, false),
            [675, 178, 2053, 2000, 326],
        ),
        (
            "roce-ring-alone",
            scenario("RoCE", ROCE, false),
            [713, 169, 1056, 1004, 314],
        ),
    ]
}

#[test]
fn ordinary_collectives_allocate_no_more_than_feat_p16() {
    for (label, config, base) in cases() {
        let (counts, image) = phases(label, &config);
        println!(
            "{label}: allocations [lower, validate, scalar full, scalar summary, cpu] = {counts:?}"
        );
        assert!(image.stage_joins.is_empty() && image.stage_joins.capacity() == 0);
        for (phase, (count, cap)) in ["lower", "validate", "scalar-full", "scalar-summary"]
            .iter()
            .zip(counts.iter().zip(base))
        {
            assert!(
                *count <= cap,
                "{label} {phase}: {count} allocations, above feat/p16's {cap}"
            );
        }
    }
}

/// The Metal plan's allocations in a debug build, in `cases()` order: one more than in release on
/// every image (measured at `feat/p16` a305455 and at da74da4, equal in each profile).
#[cfg(all(feature = "test", feature = "metal", target_vendor = "apple"))]
const METAL_PLAN_DEBUG: [u64; 4] = [312, 320, 327, 315];

/// The Metal planner (the CUDA planner shares its stage sizing) allocates no more than at
/// `feat/p16` for the same images, under the cap of the build's profile.
#[cfg(all(feature = "test", feature = "metal", target_vendor = "apple"))]
#[test]
fn ordinary_collectives_plan_with_no_more_allocations_than_feat_p16() {
    for (case, (label, config, base)) in cases().into_iter().enumerate() {
        // `cfg(debug_assertions)`: the debug build's own measured cap, release's otherwise.
        let cap = if cfg!(debug_assertions) {
            METAL_PLAN_DEBUG[case]
        } else {
            base[4]
        };
        let (_, image) = phases(label, &config);
        let before = allocations();
        let report = days_executor::size_metal_plan_for_testing(
            &image,
            None,
            days_executor::MetalConfig::default(),
            ObservationMode::Summary,
        )
        .expect("plans");
        let count = allocations() - before;
        drop(report);
        println!("{label}: Metal plan allocations = {count}");
        assert!(
            count <= cap,
            "{label} Metal plan: {count} allocations, above feat/p16's {cap}"
        );
    }
}

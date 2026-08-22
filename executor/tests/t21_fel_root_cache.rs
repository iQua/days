//! T21 fix 2 — the gate for `evidence/P12/perround-upperbound.md` §5 item 3(a): the per-node FEL
//! root query is evaluated ONCE per round, not three times.
//!
//! This is a *source* gate. It cannot prove device semantics — the frozen fixture anchors and the
//! four-backend byte-identity suites do that — but it pins the recompute count the analysis
//! measured the cost of, so a later change cannot silently reintroduce the two redundant sweeps.
//!
//! **Item 3(b), the re-grid of the control phases, is gated in `t21_control_regrid.rs`**, not
//! here. This file keeps only the recompute count, because that count is the thing the re-grid
//! must not disturb: after the re-grid the single evaluation lives in `days_horizon_sweep` rather
//! than `days_horizon`, and `days_round_prepare` still reads the cache twice.

use days_executor::device_sizing::{ROUND_SCRATCH_CACHE_WORDS, round_scratch_words};

#[path = "support/kernel_span.rs"]
mod kernel_span;

use kernel_span::{CUDA_MARKER, METAL_MARKER, kernel_body, kernel_names};

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");

fn cuda_kernel(entry: &str) -> &'static str {
    kernel_body(CUDA_KERNELS, CUDA_MARKER, entry)
}

fn metal_kernel(entry: &str) -> &'static str {
    kernel_body(METAL_KERNELS, METAL_MARKER, entry)
}

/// The extraction every other test in this file depends on sees each kernel and *only* that kernel.
///
/// The first version of the helper split on the literal `extern "C" __global__ void `, which does
/// did not match `days_round`'s attributed `__launch_bounds__` form, so
/// `days_round_prepare`'s span ran on
/// through the whole of `days_round`. Two assertions below were consequently evaluated over the
/// wrong text, and one of them could not fail at all. This test is the floor under both.
#[test]
fn each_kernel_span_stops_at_the_next_entry_point_whatever_form_it_takes() {
    let cuda = kernel_names(CUDA_KERNELS, CUDA_MARKER);
    assert!(
        cuda.contains(&"days_round".to_string()),
        "the `__launch_bounds__` entry form must be recognized, or the kernel before it absorbs \
         `days_round`: saw {cuda:?}",
    );
    // The pair the regression was about: `days_round_prepare` is declared immediately before
    // `days_round`, so a miss on the latter's form is exactly what widened the former's span.
    let prepare = cuda_kernel("days_round_prepare");
    assert!(
        !prepare.contains("days_round(") && !prepare.contains("active_index"),
        "CUDA `days_round_prepare`'s span must stop before `days_round`",
    );
    assert!(
        !prepare.contains("__launch_bounds__"),
        "no attribute from the NEXT kernel's declaration may appear in this span",
    );
    for (backend, names) in [
        ("CUDA", cuda),
        ("Metal", kernel_names(METAL_KERNELS, METAL_MARKER)),
    ] {
        for named in [
            "days_horizon_sweep",
            "days_horizon",
            "days_round_reset",
            "days_round_prepare",
            "days_round",
            "days_round_control_sweep",
            "days_round_control",
            "days_exchange_prefix_sweep",
            "days_exchange_prefix",
            "days_exchange_scatter",
            "days_exchange_merge",
            "days_round_finalize_sweep",
            "days_round_finalize",
            "days_compact_gather",
        ] {
            assert!(
                names.contains(&named.to_string()),
                "{backend} extraction must see `{named}`; saw {names:?}",
            );
        }
    }
}

/// `evidence/P12/perround-upperbound.md` §1.5.2 measured the per-node FEL root query at three
/// full-width evaluations per round — once in `days_horizon`, twice in `days_round_prepare` — over
/// a read set nothing between the call sites writes: ≈79 % of the transition-independent term's
/// memory traffic, two thirds of it redundant. After the fix the query runs once, in
/// `days_horizon`, and `days_round_prepare` reads the cached answer twice.
#[test]
fn the_fel_root_query_is_evaluated_once_per_node_per_round() {
    for (backend, horizon, prepare) in [
        (
            "CUDA",
            cuda_kernel("days_horizon_sweep"),
            cuda_kernel("days_round_prepare"),
        ),
        (
            "Metal",
            metal_kernel("days_horizon_sweep"),
            metal_kernel("days_round_prepare"),
        ),
    ] {
        assert_eq!(
            horizon.matches("fel_root_time(").count(),
            1,
            "{backend} `days_horizon_sweep` must evaluate the FEL root exactly once per node",
        );
        assert_eq!(
            horizon.matches("store_fel_root(").count(),
            1,
            "{backend} `days_horizon_sweep` must publish that evaluation to the round scratch",
        );
        assert_eq!(
            prepare.matches("fel_root_time(").count(),
            0,
            "{backend} `days_round_prepare` must not re-evaluate the FEL root; \
             its two passes read the cached answer",
        );
        assert_eq!(
            prepare.matches("load_fel_root(").count(),
            2,
            "{backend} `days_round_prepare`'s count pass and write pass both read the cache",
        );
    }
}

/// The cache is two words per LP — the root event's time and a validity flag — and nothing else.
/// A single-word encoding would have to spend a sentinel, and `u64::MAX` is a legal event time on
/// this simulator (`t21_p12_fixtures` runs a real event at the inclusive stop), so validity is
/// carried separately rather than folded into the value.
///
/// The round scratch region's *total* size is gated in `t21_control_regrid.rs`: fix 1 appends the
/// per-block reduction partials to the same region.
#[test]
fn the_cache_is_two_words_per_lp() {
    assert_eq!(ROUND_SCRATCH_CACHE_WORDS, 2);
    let empty = round_scratch_words(0).expect("an empty image still sizes");
    // E1 k=32, the fixture the analysis measured: 49,152 LPs.
    assert_eq!(round_scratch_words(49_152), Some(98_304 + empty));
    assert_eq!(round_scratch_words(1), Some(2 + empty));
    assert_eq!(round_scratch_words(usize::MAX), None);
}

/// The two node-ordered scans that fix 1 deliberately did NOT re-grid.
///
/// `days_round_prepare`'s worklist compaction is a Hillis-Steele prefix over a contiguous,
/// ascending, node-ordered partition of the LPs, and `days_exchange_prefix`'s legacy path is the
/// same shape over `remote_meta`. Both write outputs that depend on the partition itself — the
/// worklist's order, and each producer's staging base — so unlike every reduced quantity in
/// `t21_control_regrid.rs` they are *not* partition-free, and they stay in a width-1 dispatch over
/// the retained 1,024 lanes. This test exists so that a later widening cannot happen quietly.
///
/// **It has to be able to fail.** The first version asserted only that each body contained the
/// substrings `"ulong chunk = "`, `"ulong remainder = "` and `"1024"`, and the adversarial review
/// re-gridded Metal's `days_round_prepare` across the whole grid — the exact change this test
/// forbids — while keeping all three, and every gate stayed green. The two assertions below are the
/// ones that break under that probe:
///
/// 1. **The divisor is the literal 1,024**, not a grid-derived count. A re-grid must compute the
///    partition from `threadgroups_per_grid × threads_per_threadgroup` (or `gridDim.x × blockDim.x`)
///    and cannot leave `nodes / 1024` in place.
/// 2. **The body consults no grid-wide quantity at all.** These two kernels are the only ones in
///    the attempt DAG that may not know how wide the grid is; reading a grid attribute is the
///    prerequisite for every re-grid, so its absence is the property worth pinning.
///
/// The host side is gated separately, in
/// `t21_control_regrid::the_two_partition_dependent_scans_are_dispatched_at_width_one`: a kernel
/// that keeps its 1,024-lane partition is still wrong if the host hands it 128 threadgroups.
#[test]
fn the_two_node_ordered_scans_keep_the_retained_single_block_partition() {
    // Every way either language has of asking how wide the grid is.
    const GRID_WIDE: [&str; 6] = [
        "gridDim",
        "blockIdx",
        "blockDim",
        "threadgroups_per_grid",
        "threadgroup_position_in_grid",
        "threads_per_threadgroup",
    ];
    for (backend, prepare, prefix) in [
        (
            "CUDA",
            cuda_kernel("days_round_prepare"),
            cuda_kernel("days_exchange_prefix"),
        ),
        (
            "Metal",
            metal_kernel("days_round_prepare"),
            metal_kernel("days_exchange_prefix"),
        ),
    ] {
        for (name, body) in [
            ("days_round_prepare", prepare),
            ("days_exchange_prefix", prefix),
        ] {
            assert!(
                body.contains("ulong chunk = ") && body.contains("ulong remainder = "),
                "{backend} `{name}` keeps the contiguous node-ordered chunk partition",
            );
            assert!(
                body.contains("/ 1024") && body.contains("% 1024"),
                "{backend} `{name}` must partition by the LITERAL 1,024 lanes; a divisor derived \
                 from the grid is a re-grid of a scan whose output depends on the partition",
            );
            for attribute in GRID_WIDE {
                assert!(
                    !body.contains(attribute),
                    "{backend} `{name}` must not consult `{attribute}`: it runs at width 1 and its \
                     output depends on the partition, so it may not learn how wide the grid is",
                );
            }
        }
    }
}

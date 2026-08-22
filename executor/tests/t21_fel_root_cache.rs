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
//! than `days_horizon_sweep`; O1.4's reset/count and prepare/write dispatches read it once each.

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
/// not match `days_round`'s `__launch_bounds__(1024)` form, so `days_round_prepare`'s span ran on
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
/// full-width evaluations per round. The query now runs once in `days_horizon_sweep`; O1.4's
/// reset/count and prepare/write passes consume the cached answer once each.
#[test]
fn the_fel_root_query_is_evaluated_once_per_node_per_round() {
    for (backend, horizon, reset, prepare) in [
        (
            "CUDA",
            cuda_kernel("days_horizon_sweep"),
            cuda_kernel("days_round_reset"),
            cuda_kernel("days_round_prepare"),
        ),
        (
            "Metal",
            metal_kernel("days_horizon_sweep"),
            metal_kernel("days_round_reset"),
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
            reset.matches("fel_root_time(").count() + prepare.matches("fel_root_time(").count(),
            0,
            "{backend} O1.4 compaction must not re-evaluate the FEL root",
        );
        assert_eq!(
            reset.matches("load_fel_root(").count(),
            1,
            "{backend} `days_round_reset` must read one cached root while counting",
        );
        assert_eq!(
            prepare.matches("load_fel_root(").count(),
            1,
            "{backend} `days_round_prepare` must read one cached root while writing",
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
    const PARTIAL_WORDS: usize = 128 * 8;
    assert_eq!(round_scratch_words(0), Some(PARTIAL_WORDS + 1));
    // E1 k=32, the fixture the analysis measured: 49,152 LPs.
    assert_eq!(round_scratch_words(49_152), Some(148_480));
    assert_eq!(round_scratch_words(1), Some(2 + PARTIAL_WORDS + 1));
    assert_eq!(round_scratch_words(usize::MAX), None);
}

/// O1.4 replaces the old prepare geometry pin with an actual-device output gate in
/// `worklist_compaction.rs`. The legacy exchange prefix remains genuinely partition-dependent and
/// retains its single-block, literal-1,024 partition.
#[test]
fn the_legacy_exchange_prefix_keeps_its_single_block_partition() {
    // Every way either language has of asking how wide the grid is.
    const GRID_WIDE: [&str; 6] = [
        "gridDim",
        "blockIdx",
        "blockDim",
        "threadgroups_per_grid",
        "threadgroup_position_in_grid",
        "threads_per_threadgroup",
    ];
    for (backend, prefix) in [
        ("CUDA", cuda_kernel("days_exchange_prefix")),
        ("Metal", metal_kernel("days_exchange_prefix")),
    ] {
        assert!(
            prefix.contains("ulong chunk = ") && prefix.contains("ulong remainder = "),
            "{backend} exchange prefix keeps the contiguous node-ordered chunk partition",
        );
        assert!(
            prefix.contains("/ 1024") && prefix.contains("% 1024"),
            "{backend} exchange prefix partitions by the literal 1,024 lanes",
        );
        for attribute in GRID_WIDE {
            assert!(
                !prefix.contains(attribute),
                "{backend} exchange prefix must not consult `{attribute}`",
            );
        }
    }
}

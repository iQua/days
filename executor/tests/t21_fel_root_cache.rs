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

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");

/// The body of one kernel, from its entry signature to the start of the next entry point.
fn kernel_body<'a>(source: &'a str, entry: &str, opener: &str) -> &'a str {
    let signature = format!("{opener}{entry}(");
    let start = source
        .find(&signature)
        .unwrap_or_else(|| panic!("`{entry}` must exist in the kernel source"));
    let rest = &source[start + signature.len()..];
    let end = rest.find(opener).unwrap_or(rest.len());
    &rest[..end]
}

fn cuda_kernel(entry: &str) -> &'static str {
    kernel_body(CUDA_KERNELS, entry, "extern \"C\" __global__ void ")
}

fn metal_kernel(entry: &str) -> &'static str {
    kernel_body(METAL_KERNELS, entry, "kernel void ")
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
#[test]
fn the_two_node_ordered_scans_keep_the_retained_single_block_partition() {
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
                body.contains("1024"),
                "{backend} `{name}` keeps the retained 1,024-lane width",
            );
        }
    }
}

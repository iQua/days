//! T21 fix 2 — the gate for `evidence/P12/perround-upperbound.md` §5 item 3(a): the per-node FEL
//! root query is evaluated ONCE per round, not three times.
//!
//! This is a *source* gate. It cannot prove device semantics — the frozen fixture anchors and the
//! four-backend byte-identity suites do that — but it pins the recompute count the analysis
//! measured the cost of, so a later change cannot silently reintroduce the two redundant sweeps.
//!
//! **Item 3(b), the re-grid of the five single-block control phases, is NOT in this file** and is
//! not in the tree: an attempt at it is retained in the T21 progress record with the bisect that
//! blocked it (a reproducible, compiler-sensitive `ARENA_OUTBOX` capacity fault at k=32 width that
//! no smaller fixture exposes). The single-block geometry is therefore still live, and
//! `the_control_phase_geometry_is_still_the_single_block_one_the_analysis_measured` records that
//! as a *fact about the tree*, not as an endorsement — it is there so the next round can see at a
//! glance that the lever is still unspent.

use days_executor::device_sizing::{ROUND_SCRATCH_CACHE_WORDS, round_scratch_words};

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");
const CUDA_BACKEND: &str = include_str!("../src/cuda.rs");
const METAL_BACKEND: &str = include_str!("../src/metal.rs");

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
            cuda_kernel("days_horizon"),
            cuda_kernel("days_round_prepare"),
        ),
        (
            "Metal",
            metal_kernel("days_horizon"),
            metal_kernel("days_round_prepare"),
        ),
    ] {
        assert_eq!(
            horizon.matches("fel_root_time(").count(),
            1,
            "{backend} `days_horizon` must evaluate the FEL root exactly once per node",
        );
        assert_eq!(
            horizon.matches("store_fel_root(").count(),
            1,
            "{backend} `days_horizon` must publish that evaluation to the round scratch",
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
#[test]
fn the_round_scratch_is_exactly_the_cache() {
    assert_eq!(ROUND_SCRATCH_CACHE_WORDS, 2);
    assert_eq!(round_scratch_words(0), Some(0));
    assert_eq!(round_scratch_words(1), Some(2));
    // E1 k=32, the fixture the analysis measured: 49,152 LPs.
    assert_eq!(round_scratch_words(49_152), Some(98_304));
    assert_eq!(round_scratch_words(usize::MAX), None);
}

/// The state of item 3(b), recorded as a fact so the next round does not have to rediscover it.
///
/// `evidence/P12/perround-upperbound.md` §1.5.2 read the geometry from source: five of the eight
/// phases launch `grid_dim: (1, 1, 1)` / `MTLSize { width: 1 }` and sweep Θ(nodes + channels) from
/// a single block — one SM of 128 on the measured RTX 4090 — while holding 69.70 % of profiled
/// kernel time. **That is still true.** When the re-grid lands, this test is the one that flips.
#[test]
fn the_control_phase_geometry_is_still_the_single_block_one_the_analysis_measured() {
    assert!(
        CUDA_BACKEND.contains("grid_dim: (1, 1, 1)"),
        "if the CUDA control grid has been widened, retire this test and gate the new geometry",
    );
    assert!(
        METAL_BACKEND.contains("let control_grid = MTLSize {\n            width: 1,"),
        "if the Metal control grid has been widened, retire this test and gate the new geometry",
    );
    // The five phases the analysis named, each still sweeping with the single-block stride.
    for phase in [
        "days_horizon",
        "days_round_prepare",
        "days_round_control",
        "days_exchange_prefix",
        "days_round_finalize",
    ] {
        assert!(
            cuda_kernel(phase).contains("1024"),
            "CUDA `{phase}` still carries the 1,024-lane single-block sweep",
        );
        assert!(
            metal_kernel(phase).contains("1024"),
            "Metal `{phase}` still carries the 1,024-lane single-block sweep",
        );
    }
    // And the two kernel sources still contain no atomics at all — the property the re-grid would
    // have had to argue its way past, and the reason this file records the geometry rather than
    // half-changing it.
    assert_eq!(CUDA_KERNELS.matches("atomicAdd(").count(), 0);
    assert_eq!(
        METAL_KERNELS.matches("atomic_fetch_add_explicit(").count(),
        0
    );
}

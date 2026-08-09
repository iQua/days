//! T21 fix 1, second attempt — the gate for `evidence/P12/perround-upperbound.md` §5 item 3(b):
//! the control phases that sweep Θ(nodes + channels) no longer do it from a grid of one block.
//!
//! **Read `evidence/P12/aterm-fixes.md` §3 before changing anything here.** The first attempt at
//! this item was written in full and reverted: it elected a last-arriving block to combine
//! per-block partials *inside one dispatch*, and the bisect ended at a result that changed when an
//! untaken branch was added — i.e. undefined behaviour, with `volatile device ulong *` partial
//! accesses and MSL's relaxed-only device-scope memory model as the leading suspects.
//!
//! This attempt spends dispatches instead. Each reducing phase is a full-grid `_sweep` that
//! publishes one partial per block, followed by a width-1 combine that reads them. The barrier
//! between the two is the **dispatch boundary** — CUDA stream ordering between launches, Metal
//! `MTLDispatchType::Serial` ordering between dispatches — which is the same guarantee the
//! pre-existing 8-phase DAG already depends on (`days_round_prepare` writes `worklist`; the next
//! dispatch, `days_round`, reads it). No atomic, no `volatile`, no fence, and no reliance on
//! cross-block memory ordering *within* a dispatch appears anywhere in either kernel source.
//!
//! These are *source* gates. They cannot prove device semantics — the frozen fixture anchors, the
//! four-backend byte-identity suites and `t20b3_queue_bytes`'
//! `k32_byte_policy_strict_run_is_retry_free` (the only gate that caught the first attempt) do
//! that. They pin the shape, so the single-block geometry cannot creep back and a future edit
//! cannot reintroduce in-dispatch cross-block synchronization without this file going red.

use days_executor::device_sizing::{ROUND_SCRATCH_CACHE_WORDS, round_scratch_words};

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");
const CUDA_BACKEND: &str = include_str!("../src/cuda.rs");
const METAL_BACKEND: &str = include_str!("../src/metal.rs");
const DEVICE_SIZING: &str = include_str!("../src/device_sizing.rs");

/// Threadgroups/blocks in every re-gridded control sweep. Spelled out here rather than imported so
/// that this gate pins the number independently of the declaration it is checking.
const CONTROL_SWEEP_BLOCKS: usize = 128;
/// Words each sweep block publishes into the round scratch region.
const ROUND_SCRATCH_PARTIAL_WORDS: usize = 8;

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

/// Every `(backend, sweep body, combine body)` triple for one phase.
fn phase(sweep: &str, combine: &str) -> [(&'static str, &'static str, &'static str); 2] {
    [
        ("CUDA", cuda_kernel(sweep), cuda_kernel(combine)),
        ("Metal", metal_kernel(sweep), metal_kernel(combine)),
    ]
}

/// Does this kernel body assign to a `control[...]` word?
///
/// The sweeps are allowed to *read* control — they replay their phase's guard — but a sweep that
/// wrote a control word would make the combine's identical guard unsound, because the guard is the
/// one thing that must evaluate the same in both dispatches.
fn writes_control(body: &str) -> bool {
    let mut rest = body;
    while let Some(open) = rest.find("control[") {
        rest = &rest[open + "control[".len()..];
        let Some(close) = rest.find(']') else {
            return false;
        };
        let after = rest[close + 1..].trim_start();
        if let Some(tail) = after.strip_prefix('=') {
            if !tail.starts_with('=') {
                return true;
            }
        }
    }
    false
}

/// Item 2 of `aterm-fixes.md` §3.4, and the cheapest real slice of 3(b): `days_round_prepare`'s
/// Θ(N) LP-error reset and Θ(C) channel-batch reset need no cross-block communication at all —
/// they are per-word-disjoint from the compaction that used to run beside them — so they become
/// their own full-grid dispatch and leave the compaction alone.
#[test]
fn the_round_prepare_resets_run_on_the_whole_grid() {
    for (backend, reset, prepare) in [
        (
            "CUDA",
            cuda_kernel("days_round_reset"),
            cuda_kernel("days_round_prepare"),
        ),
        (
            "Metal",
            metal_kernel("days_round_reset"),
            metal_kernel("days_round_prepare"),
        ),
    ] {
        assert!(
            reset.contains("L_ERROR_DEMAND] = 0"),
            "{backend} `days_round_reset` owns the per-LP error reset",
        );
        assert!(
            reset.contains("remote_meta["),
            "{backend} `days_round_reset` owns the per-LP remote-count reset",
        );
        assert!(
            reset.contains("CHANNEL_BATCH_WORDS"),
            "{backend} `days_round_reset` owns the per-channel batch reset",
        );
        assert!(
            !writes_control(reset),
            "{backend} `days_round_reset` must not write a control word",
        );
        assert!(
            !prepare.contains("L_ERROR_DEMAND] = 0"),
            "{backend} `days_round_prepare` no longer performs the Θ(N) reset",
        );
        assert!(
            !prepare.contains("CHANNEL_BATCH_WORDS"),
            "{backend} `days_round_prepare` no longer performs the Θ(C) reset",
        );
    }
    assert!(
        CUDA_BACKEND.contains("\"days_round_reset\""),
        "the CUDA attempt DAG dispatches the reset",
    );
    assert!(
        METAL_BACKEND.contains("AttemptKernel::RoundReset"),
        "the Metal attempt DAG dispatches the reset",
    );
}

/// `days_horizon`'s Θ(N) FEL-root sweep — the most expensive of the five, since it is the one that
/// pays `fel_peek` per LP — moves to `days_horizon_sweep`, which publishes a per-block
/// `{minimum, validity}` partial. The combine reduces those partials and keeps every control
/// write, so the frontier and the horizon are still decided in one place.
#[test]
fn the_horizon_scan_runs_on_the_whole_grid() {
    for (backend, sweep, combine) in phase("days_horizon_sweep", "days_horizon") {
        assert_eq!(
            sweep.matches("fel_root_time(").count(),
            1,
            "{backend} `days_horizon_sweep` still evaluates the FEL root exactly once per node",
        );
        assert_eq!(
            sweep.matches("store_fel_root(").count(),
            1,
            "{backend} `days_horizon_sweep` still publishes the T21 fix 2 cache",
        );
        assert!(
            sweep.contains("store_round_partial("),
            "{backend} `days_horizon_sweep` publishes one partial per block",
        );
        assert!(
            !writes_control(sweep),
            "{backend} `days_horizon_sweep` must not write a control word",
        );
        assert_eq!(
            combine.matches("fel_root_time(").count(),
            0,
            "{backend} `days_horizon` combines partials; it does not re-scan the FEL",
        );
        assert!(
            combine.contains("load_round_partial("),
            "{backend} `days_horizon` reads the partials the sweep published",
        );
    }
}

/// `days_round_control`'s two Θ(·) scans — first errored LP, and any unfinished active LP — reduce
/// under `min` and `OR`, so they are partition-free and split cleanly.
#[test]
fn the_round_control_scan_runs_on_the_whole_grid() {
    for (backend, sweep, combine) in phase("days_round_control_sweep", "days_round_control") {
        assert!(
            sweep.contains("L_FINISHED") && sweep.contains("L_ERROR"),
            "{backend} `days_round_control_sweep` owns both scans",
        );
        assert!(
            sweep.contains("store_round_partial("),
            "{backend} `days_round_control_sweep` publishes one partial per block",
        );
        assert!(
            !writes_control(sweep),
            "{backend} `days_round_control_sweep` must not write a control word",
        );
        assert!(
            combine.contains("load_round_partial("),
            "{backend} `days_round_control` reads the partials the sweep published",
        );
    }
}

/// `days_exchange_prefix`'s **streams** path sums Θ(C) per-channel batch counts and picks the
/// lowest invalid channel: a clamped sum, an `OR` and a `min`. Its **legacy** path is a
/// node-ordered prefix scan whose output depends on the partition, so it does not move: it stays
/// in the width-1 combine over the retained 1,024-lane contiguous chunks.
#[test]
fn the_exchange_prefix_stream_scan_runs_on_the_whole_grid() {
    for (backend, sweep, combine) in phase("days_exchange_prefix_sweep", "days_exchange_prefix") {
        assert!(
            sweep.contains("P_STREAMS_ENABLED"),
            "{backend} `days_exchange_prefix_sweep` runs only on the streams path",
        );
        assert!(
            sweep.contains("store_round_partial("),
            "{backend} `days_exchange_prefix_sweep` publishes one partial per block",
        );
        assert!(
            !writes_control(sweep),
            "{backend} `days_exchange_prefix_sweep` must not write a control word",
        );
        assert!(
            combine.contains("load_round_partial("),
            "{backend} `days_exchange_prefix` reads the partials the sweep published",
        );
        assert!(
            combine.contains("remote_meta[base + 2] = write"),
            "{backend} `days_exchange_prefix` keeps the legacy node-ordered prefix scan, which is \
             NOT partition-free and therefore does NOT move",
        );
    }
}

/// `days_round_finalize` reduces a first-error `min` and three clamped observation-log totals with
/// their capacity flags. Seven words per block.
#[test]
fn the_round_finalize_scan_runs_on_the_whole_grid() {
    for (backend, sweep, combine) in phase("days_round_finalize_sweep", "days_round_finalize") {
        assert!(
            sweep.contains("P_FULL_OBSERVATIONS") && sweep.contains("observation_meta["),
            "{backend} `days_round_finalize_sweep` owns the observation-log totals",
        );
        assert!(
            sweep.contains("store_round_partial("),
            "{backend} `days_round_finalize_sweep` publishes one partial per block",
        );
        assert!(
            !writes_control(sweep),
            "{backend} `days_round_finalize_sweep` must not write a control word",
        );
        assert!(
            combine.contains("load_round_partial("),
            "{backend} `days_round_finalize` reads the partials the sweep published",
        );
    }
}

/// The property the first attempt lost, stated as a gate.
///
/// `aterm-fixes.md` §3.3: the first attempt's cross-block combine ran inside one dispatch, elected
/// by an arrival ticket, and read the partials through `volatile device ulong *`. It produced a
/// result that changed when an untaken branch was added. Nothing in this attempt synchronizes
/// across blocks inside a dispatch, so there is no ordering to get wrong: a sweep only ever writes
/// the slot belonging to its own block, and the combine only ever reads slots written by a
/// *previous dispatch*.
#[test]
fn no_kernel_synchronizes_across_blocks_inside_a_dispatch() {
    for (backend, source) in [("CUDA", CUDA_KERNELS), ("Metal", METAL_KERNELS)] {
        for forbidden in [
            "atomic_",
            "atomicAdd(",
            "atomicExch(",
            "atomicInc(",
            "volatile ",
            "__threadfence",
            "memory_order",
        ] {
            assert_eq!(
                source.matches(forbidden).count(),
                0,
                "{backend} kernels must contain no `{forbidden}`: the only barrier this design \
                 uses is the dispatch boundary",
            );
        }
    }
    // Metal's device-scope fence is still used where it always was — inside a *threadgroup*, to
    // order that threadgroup's own device writes — and nowhere else. `days_round_reset` and the
    // four sweeps add none.
    for sweep in [
        "days_round_reset",
        "days_horizon_sweep",
        "days_round_control_sweep",
        "days_exchange_prefix_sweep",
        "days_round_finalize_sweep",
    ] {
        assert_eq!(
            metal_kernel(sweep).matches("mem_flags::mem_device").count(),
            0,
            "Metal `{sweep}` must not reach for a device-scope fence",
        );
    }
}

/// The dispatch count, disclosed. Eight phases become thirteen dispatches; the eight *reported*
/// profile buckets are unchanged, so `t15b_round_profile` and `t17c_cuda_profile` still compare
/// before and after like for like.
///
/// `perround-upperbound.md` Lever 2a budgeted "adds 2-5 dispatches per round — free at ≤0.11 %".
/// This is +5, at that bound.
#[test]
fn the_attempt_dag_is_thirteen_dispatches_over_eight_reported_phases() {
    assert!(
        CUDA_BACKEND.contains("const KERNEL_NAMES: [&str; 13]"),
        "the CUDA attempt DAG is thirteen launches",
    );
    assert!(
        CUDA_BACKEND.contains("const DISPATCH_PHASE: [usize; 13]"),
        "each CUDA launch is attributed to one of the eight reported phases",
    );
    assert!(
        METAL_BACKEND.contains(
            "const ATTEMPT_DISPATCHES: [(AttemptKernel, AttemptPhase, DispatchGeometry); 13]"
        ),
        "the Metal attempt DAG is thirteen dispatches over the eight reported phases",
    );
    assert!(
        METAL_BACKEND.contains("DispatchGeometry::ControlSweep"),
        "the Metal backend has a full-grid control geometry",
    );
    for name in [
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
    ] {
        assert!(
            CUDA_BACKEND.contains(&format!("\"{name}\"")),
            "the CUDA DAG loads `{name}`",
        );
        assert!(
            METAL_BACKEND.contains(&format!("\"{name}\"")),
            "the Metal DAG compiles a pipeline for `{name}`",
        );
    }
}

/// The sweep width is a fixed constant, shared by both backends and both kernel sources.
///
/// Fixing it is what makes the combine sound without a params word or a host/device agreement to
/// get wrong: every one of the `CONTROL_SWEEP_BLOCKS` partial slots is written by its own block on
/// every dispatch, so the combine never reads a slot left over from a previous round.
#[test]
fn the_sweep_width_and_its_scratch_are_one_shared_constant() {
    assert_eq!(ROUND_SCRATCH_CACHE_WORDS, 2);
    assert!(
        DEVICE_SIZING.contains("CONTROL_SWEEP_BLOCKS: usize = 128"),
        "the host allocates for a 128-block sweep",
    );
    assert!(
        DEVICE_SIZING.contains("ROUND_SCRATCH_PARTIAL_WORDS: usize = 8"),
        "the host allocates eight words per sweep block",
    );
    let partials = CONTROL_SWEEP_BLOCKS * ROUND_SCRATCH_PARTIAL_WORDS;
    assert_eq!(round_scratch_words(0), Some(partials));
    assert_eq!(round_scratch_words(1), Some(2 + partials));
    // E1 k=32, the fixture the analysis measured: 49,152 LPs.
    assert_eq!(round_scratch_words(49_152), Some(98_304 + partials));
    assert_eq!(round_scratch_words(usize::MAX), None);
    for (backend, source) in [("CUDA", CUDA_KERNELS), ("Metal", METAL_KERNELS)] {
        assert!(
            source.contains("CONTROL_SWEEP_BLOCKS = 128"),
            "{backend} kernels pin the same sweep width the host allocates for",
        );
        assert!(
            source.contains("ROUND_SCRATCH_PARTIAL_WORDS = 8"),
            "{backend} kernels pin the same partial stride the host allocates for",
        );
    }
    assert!(
        CUDA_BACKEND.contains("CONTROL_SWEEP_BLOCKS"),
        "the CUDA sweep grid is that constant",
    );
    assert!(
        METAL_BACKEND.contains("CONTROL_SWEEP_BLOCKS"),
        "the Metal sweep grid is that constant",
    );
}

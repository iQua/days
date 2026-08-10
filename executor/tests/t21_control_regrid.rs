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

#[path = "support/kernel_span.rs"]
mod kernel_span;

use kernel_span::{CUDA_MARKER, METAL_MARKER, kernel_body};

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

fn cuda_kernel(entry: &str) -> &'static str {
    kernel_body(CUDA_KERNELS, CUDA_MARKER, entry)
}

fn metal_kernel(entry: &str) -> &'static str {
    kernel_body(METAL_KERNELS, METAL_MARKER, entry)
}

/// The contents of a `const NAME: [...] = [ … ];` array literal in a Rust source, as raw text.
fn const_array<'a>(source: &'a str, name: &str) -> &'a str {
    let declaration = format!("const {name}: [");
    let start = source
        .find(&declaration)
        .unwrap_or_else(|| panic!("`{name}` must be declared in the backend source"));
    let rest = &source[start..];
    let open = rest
        .find("= [")
        .unwrap_or_else(|| panic!("`{name}` must be an array literal"));
    let body = &rest[open + "= [".len()..];
    let close = body
        .find("];")
        .unwrap_or_else(|| panic!("`{name}`'s array literal must be closed"));
    &body[..close]
}

/// The `usize` elements of a `const NAME: [usize; N] = [...]` array.
fn const_indices(source: &str, name: &str) -> Vec<usize> {
    const_array(source, name)
        .split(',')
        .filter_map(|element| element.trim().parse::<usize>().ok())
        .collect()
}

/// The double-quoted elements of a `const NAME: [&str; N] = [...]` array, in order.
fn const_strings(source: &str, name: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut rest = const_array(source, name);
    while let Some(open) = rest.find('"') {
        rest = &rest[open + 1..];
        let Some(close) = rest.find('"') else { break };
        values.push(rest[..close].to_string());
        rest = &rest[close + 1..];
    }
    values
}

/// Every `(kernel, geometry)` pair of Metal's `ATTEMPT_DISPATCHES`, in encode order, read out of
/// the backend source rather than out of the backend's own unit tests.
fn metal_dispatch_geometry() -> Vec<(String, String)> {
    let table = const_array(METAL_BACKEND, "ATTEMPT_DISPATCHES");
    let mut pairs = Vec::new();
    let mut rest = table;
    while let Some(offset) = rest.find("AttemptKernel::") {
        rest = &rest[offset + "AttemptKernel::".len()..];
        let kernel: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let Some(geometry_at) = rest.find("DispatchGeometry::") else {
            break;
        };
        let after = &rest[geometry_at + "DispatchGeometry::".len()..];
        let geometry: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        pairs.push((kernel, geometry));
    }
    pairs
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

/// The maximum dispatch count, disclosed. Full and legacy plans retain thirteen dispatches; T24's
/// exact streams+Summary specialization omits only `FinalControlSweep`. The eight *reported*
/// profile buckets are unchanged, so `t15b_round_profile` and `t17c_cuda_profile` still compare.
///
/// `perround-upperbound.md` Lever 2a budgeted "adds 2-5 dispatches per round — free at ≤0.11 %".
/// This is +5, at that bound.
#[test]
fn the_full_and_legacy_attempt_dag_is_thirteen_dispatches_over_eight_reported_phases() {
    assert!(
        CUDA_BACKEND.contains("const KERNEL_NAMES: [&str; 13]"),
        "the CUDA Full/legacy attempt DAG is thirteen launches",
    );
    assert!(
        CUDA_BACKEND.contains("const DISPATCH_PHASE: [usize; 13]"),
        "each CUDA launch is attributed to one of the eight reported phases",
    );
    assert!(
        METAL_BACKEND.contains(
            "const ATTEMPT_DISPATCHES: [(AttemptKernel, AttemptPhase, DispatchGeometry); 13]"
        ),
        "the Metal Full/legacy attempt DAG is thirteen dispatches over eight reported phases",
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

/// Every launch's geometry, read out of the two backends' dispatch tables, with **no kernel
/// unaccounted for**.
///
/// The adversarial review found the previous coverage one kernel short in both places: the host
/// table was gated only for the five sweeps and three of the four width-1 combines, and
/// `days_round_prepare` — the one width-1 kernel whose *output order* depends on the partition —
/// appeared in neither list. Combined with a source gate that could not fail, nothing in the tree
/// pinned it to width 1. This test enumerates the whole table and asserts an expected geometry for
/// every entry, so a new dispatch cannot be added without a decision recorded here.
#[test]
fn the_two_partition_dependent_scans_are_dispatched_at_width_one() {
    // The complete expected geometry of the attempt DAG, in encode order. `FixedControl` is one
    // threadgroup; `ControlSweep` is `CONTROL_SWEEP_BLOCKS` of them.
    const EXPECTED: [(&str, &str); 13] = [
        ("HorizonSweep", "ControlSweep"),
        ("Horizon", "FixedControl"),
        ("RoundReset", "ControlSweep"),
        // The worklist compaction. Its output is the ORDER of `worklist`, which depends on the
        // partition, so it may never be widened.
        ("Compaction", "FixedControl"),
        ("DrainExecute", "ActiveWorklist"),
        ("ContinuationControlSweep", "ControlSweep"),
        ("ContinuationControl", "FixedControl"),
        ("ExchangePrefixSweep", "ControlSweep"),
        // The legacy node-ordered prefix. Its output is each producer's staging base, which depends
        // on the partition, so it may never be widened either.
        ("ExchangePrefix", "FixedControl"),
        ("ExchangeScatter", "Parallel"),
        ("TargetMerge", "Parallel"),
        ("FinalControlSweep", "ControlSweep"),
        ("FinalControl", "FixedControl"),
    ];
    let observed = metal_dispatch_geometry();
    assert_eq!(
        observed.len(),
        EXPECTED.len(),
        "every Metal dispatch must have an expected geometry recorded here; saw {observed:?}",
    );
    for (index, ((kernel, geometry), (expected_kernel, expected_geometry))) in
        observed.iter().zip(EXPECTED).enumerate()
    {
        assert_eq!(
            (kernel.as_str(), geometry.as_str()),
            (expected_kernel, expected_geometry),
            "Metal dispatch {index} must be `{expected_kernel}` at `{expected_geometry}`",
        );
    }

    // CUDA carries the same decision as three index sets over `KERNEL_NAMES`. A kernel in neither
    // `SWEEP_KERNELS` nor `PARALLEL_KERNELS` is launched at `grid_dim: (1, 1, 1)`.
    let names = const_strings(CUDA_BACKEND, "KERNEL_NAMES");
    let sweeps = const_indices(CUDA_BACKEND, "SWEEP_KERNELS");
    let parallel = const_indices(CUDA_BACKEND, "PARALLEL_KERNELS");
    assert_eq!(
        names.len(),
        13,
        "the CUDA DAG is thirteen launches: {names:?}"
    );
    for (kernel, expected) in [
        ("days_round_prepare", "FixedControl"),
        ("days_exchange_prefix", "FixedControl"),
        ("days_horizon_sweep", "ControlSweep"),
        ("days_round_reset", "ControlSweep"),
        ("days_round_control_sweep", "ControlSweep"),
        ("days_exchange_prefix_sweep", "ControlSweep"),
        ("days_round_finalize_sweep", "ControlSweep"),
        ("days_horizon", "FixedControl"),
        ("days_round_control", "FixedControl"),
        ("days_round_finalize", "FixedControl"),
        ("days_round", "Parallel"),
        ("days_exchange_scatter", "Parallel"),
        ("days_exchange_merge", "Parallel"),
    ] {
        let index = names
            .iter()
            .position(|name| name == kernel)
            .unwrap_or_else(|| panic!("CUDA `KERNEL_NAMES` must contain `{kernel}`"));
        let geometry = if sweeps.contains(&index) {
            "ControlSweep"
        } else if parallel.contains(&index) {
            "Parallel"
        } else {
            "FixedControl"
        };
        assert_eq!(
            geometry, expected,
            "CUDA `{kernel}` (index {index}) must launch at `{expected}`",
        );
    }
    assert!(
        CUDA_BACKEND.contains("grid_dim: (1, 1, 1)"),
        "the width-1 CUDA control geometry must still exist for the combines and the two scans",
    );
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

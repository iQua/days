//! T24 lever 2 — only a streams+Summary lowered plan may omit the finalize sweep.
//!
//! Runtime byte identity remains the job of the frozen T21/P12 anchors. This source gate pins the
//! plan selector and the ordered boundary so it cannot silently widen to Full or legacy plans.

use days_executor::device_sizing::finalize_sweep_required;

#[path = "support/kernel_span.rs"]
mod kernel_span;

use kernel_span::{CUDA_MARKER, METAL_MARKER, kernel_body};

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");
const CUDA_BACKEND: &str = include_str!("../src/cuda.rs");
const METAL_BACKEND: &str = include_str!("../src/metal.rs");

fn cuda_kernel(entry: &str) -> &'static str {
    kernel_body(CUDA_KERNELS, CUDA_MARKER, entry)
}

fn metal_kernel(entry: &str) -> &'static str {
    kernel_body(METAL_KERNELS, METAL_MARKER, entry)
}

#[test]
fn only_streams_summary_omits_the_finalize_sweep() {
    assert!(!finalize_sweep_required(false, true));
    assert!(
        finalize_sweep_required(true, true),
        "Full observations retain cumulative whole-LP totals even with streams",
    );
    assert!(
        finalize_sweep_required(false, false),
        "legacy Summary merge can still write an LP error",
    );
    assert!(finalize_sweep_required(true, false));

    for (backend, source) in [("CUDA", CUDA_BACKEND), ("Metal", METAL_BACKEND)] {
        assert!(
            source.contains("crate::device_sizing::finalize_sweep_required(")
                && source.contains("plan.params[8] != 0")
                && source.contains("plan.params[14] != 0"),
            "{backend} must derive the finalize policy from the lowered Full and streams words",
        );
    }
    assert_eq!(
        CUDA_BACKEND
            .matches("!buffers.finalize_sweep_required")
            .count(),
        1,
        "CUDA production encoding must apply the lowered-plan policy directly",
    );
    assert_eq!(
        METAL_BACKEND
            .matches("!buffers.finalize_sweep_required")
            .count(),
        1,
        "Metal ordinary encoding must apply the lowered-plan policy directly",
    );
    assert!(
        CUDA_BACKEND.contains("index == FINALIZE_SWEEP_KERNEL_INDEX"),
        "CUDA must apply the policy only to the finalize-sweep entry",
    );
    for (backend, finalize) in [
        ("CUDA", cuda_kernel("days_round_finalize")),
        ("Metal", metal_kernel("days_round_finalize")),
    ] {
        assert!(
            finalize.contains("P_STREAMS_ENABLED") && finalize.contains("P_FULL_OBSERVATIONS"),
            "{backend} ordered finalize must have an explicit streams+Summary fast path",
        );
        assert!(
            finalize
                .find("params[P_STREAMS_ENABLED]")
                .expect("streams+Summary predicate")
                < finalize
                    .find("load_round_partial(")
                    .expect("Full/legacy partial reduction"),
            "{backend} streams+Summary finalize must return before reading omitted-sweep partials",
        );
        assert!(
            finalize.contains("control[C_CONTINUATION] = 0")
                && finalize.contains("control[C_ROUNDS] += 1"),
            "{backend} ordered finalize must publish round completion before the next horizon",
        );
    }
}

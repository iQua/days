//! T24 lever 1 — continuation control scans only the exact current worklist.

#[path = "support/kernel_span.rs"]
mod kernel_span;

use kernel_span::{CUDA_MARKER, METAL_MARKER, kernel_body};

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");

fn cuda_kernel(entry: &str) -> &'static str {
    kernel_body(CUDA_KERNELS, CUDA_MARKER, entry)
}

fn metal_kernel(entry: &str) -> &'static str {
    kernel_body(METAL_KERNELS, METAL_MARKER, entry)
}

#[test]
fn continuation_control_scans_only_the_canonical_current_worklist() {
    for (backend, sweep) in [
        ("CUDA", cuda_kernel("days_round_control_sweep")),
        ("Metal", metal_kernel("days_round_control_sweep")),
    ] {
        assert!(
            !sweep.contains("P_NODE_COUNT"),
            "{backend} continuation control must not scan LPs outside the current worklist",
        );
        assert_eq!(
            sweep.matches("worklist[active]").count(),
            1,
            "{backend} continuation control must load each selected LP once from the canonical \
             worklist",
        );
        assert!(
            sweep.contains("active < control[C_ACTIVE]"),
            "{backend} continuation control membership must be the deterministic current active \
             count",
        );
        assert!(
            sweep.contains("L_ERROR") && sweep.contains("L_FINISHED"),
            "{backend} continuation control must retain both reductions",
        );
    }
}

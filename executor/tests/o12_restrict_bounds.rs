//! O1.2 source contracts for the disjoint device-plane ABI and production geometry.

#[path = "support/kernel_span.rs"]
mod kernel_span;

use kernel_span::{METAL_MARKER, kernel_body};

const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const CUDA_HOST: &str = include_str!("../src/cuda.rs");
const CUDA_CONFORMANCE: &str = include_str!("cuda_conformance.rs");
const METAL: &str = include_str!("../src/metal_kernels.metal");

const ATTEMPT_KERNELS: [&str; 13] = [
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
];

#[test]
fn cuda_uniform_plane_abi_is_restrict_qualified() {
    let (_, tail) = CUDA
        .split_once("#define DAYS_BUFFERS \\\n")
        .expect("CUDA must declare the uniform plane ABI");
    let (abi, _) = tail
        .split_once("\n\nconstexpr uint EVENT_WORDS")
        .expect("CUDA plane ABI must precede the layout constants");

    assert_eq!(abi.matches('*').count(), 29, "CUDA ABI must keep 29 planes");
    assert_eq!(
        abi.matches("__restrict__").count(),
        29,
        "every separately allocated CUDA plane must be restricted",
    );
    assert_eq!(
        abi.matches("const ulong").count(),
        6,
        "the six globally read-only CUDA planes must remain const",
    );
    for name in [
        "params",
        "flows",
        "routes",
        "links",
        "inbound_meta",
        "inbound_producers",
    ] {
        assert!(
            abi.contains(&format!("const ulong *__restrict__ {name}")),
            "globally read-only CUDA plane `{name}` must also be const",
        );
    }
}

#[test]
fn cuda_round_bound_and_geometry_sweep_match_the_production_limit() {
    assert!(
        CUDA.contains(
            "extern \"C\" __global__ __launch_bounds__(256) void days_round(DAYS_BUFFERS)",
        ),
        "days_round must be compiled for the production 256-thread geometry",
    );
    assert!(
        CUDA_HOST.contains("const DEFAULT_ROUND_THREADS_PER_BLOCK: usize = 256;"),
        "the CUDA production default must remain 256 threads",
    );
    assert!(
        CUDA_HOST
            .contains("round_threads_per_block {parallel_threads} exceeds the supported maximum",),
        "oversized CUDA geometry must retain its pre-launch validation error",
    );
    assert!(
        CUDA_CONFORMANCE.contains("[1, 32, 64, 128, 256]"),
        "CUDA geometry independence must cover five legal block sizes",
    );
}

#[test]
fn metal_attempt_kernel_buffers_are_restrict_qualified() {
    let mut pointer_count = 0;
    for kernel in ATTEMPT_KERNELS {
        let body = kernel_body(METAL, METAL_MARKER, kernel);
        let arguments = body
            .split_once(") {")
            .unwrap_or_else(|| panic!("Metal kernel `{kernel}` must have an argument list"))
            .0;
        for line in arguments.lines().filter(|line| line.contains("[[buffer(")) {
            pointer_count += 1;
            assert!(
                line.contains("* __restrict "),
                "Metal kernel `{kernel}` buffer is not restricted: {line}",
            );
        }
    }
    assert_eq!(pointer_count, 96, "the 13-kernel Metal ABI shape moved");
}

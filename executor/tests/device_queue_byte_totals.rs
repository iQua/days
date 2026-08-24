fn assert_running_queue_byte_total(kernel: &str, metadata_declaration: &str) {
    assert!(
        kernel.contains(metadata_declaration),
        "queue metadata must reserve one u64 for the running byte total"
    );

    let admission_start = kernel
        .find("switch_admission_action(")
        .expect("device kernel must define switch admission");
    let admission = &kernel[admission_start..];
    let admission_end = admission
        .find("local_rational_zero")
        .expect("switch admission must precede the scheduler helpers");
    let admission = &admission[..admission_end];

    assert!(
        admission.contains("queue_meta[meta_base + 4]"),
        "byte-unit admission must read the running queue byte total"
    );
    assert!(
        !admission.contains("for (ulong logical = 0; logical < waiting; ++logical)"),
        "the per-arrival queue byte walk must be deleted, not gated"
    );
}

#[test]
fn metal_uses_a_running_queue_byte_total_without_an_admission_walk() {
    assert_running_queue_byte_total(
        include_str!("../src/metal_kernels.metal"),
        "constant uint QUEUE_META_WORDS = 5;",
    );
}

#[test]
fn cuda_uses_a_running_queue_byte_total_without_an_admission_walk() {
    assert_running_queue_byte_total(
        include_str!("../src/cuda_kernels.cu"),
        "constexpr uint QUEUE_META_WORDS = 5;",
    );
}

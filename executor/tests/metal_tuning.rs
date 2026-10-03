//! Source contracts for MT1's Metal-only Stage 1 specializations.

const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const METAL: &str = include_str!("../src/metal_kernels.metal");

#[test]
fn metal_omits_the_o13_continuation_fast_path() {
    for token in [
        "DAYS_ENABLE_SAME_TIME_CONTINUATION_FAST_PATH",
        "L_SAME_TIME_CONTINUATIONS",
        "is_same_time_tx_ready_continuation",
        "counted_continuation",
        "has_continuation",
        "retain_continuation",
        "child_precedes_lp_next_key",
        "thread_stored_key_less",
        "build_child",
    ] {
        assert!(
            !METAL.contains(token),
            "Metal must omit O1.3 token `{token}` instead of retaining dead continuation plumbing",
        );
    }
}

#[test]
fn cuda_retains_the_independently_tuned_o13_fast_path() {
    for token in [
        "L_SAME_TIME_CONTINUATIONS",
        "is_same_time_tx_ready_continuation",
        "counted_continuation",
        "has_continuation",
        "retain_continuation",
        "child_precedes_lp_next_key",
        "thread_stored_key_less",
        "build_child",
    ] {
        assert!(
            CUDA.contains(token),
            "CUDA must retain its independently tuned O1.3 token `{token}`",
        );
    }
}

#[test]
fn metal_forces_the_timer_heap_removal_inline_as_cuda_does() {
    // P15 R4: the queue-pair ACK/NACK driver is the third call site of `heap_remove_timer`. Left to
    // Apple's inliner heuristics, that third site moved the mechanisms pipeline's run time on
    // DCQCN images that never execute it (host instructions on `dcqcn_1s_zero_xoff`, interleaved:
    // 811M with plain `inline`, 780M forced, 777M before queue pairs). CUDA already declares it
    // `__forceinline__`; Metal must match.
    assert!(
        CUDA.contains("__device__ __forceinline__ bool heap_remove_timer("),
        "CUDA's timer-heap removal must stay force-inlined",
    );
    assert!(
        METAL.contains("[[gnu::always_inline]] inline bool heap_remove_timer("),
        "Metal's timer-heap removal must be force-inlined like CUDA's",
    );
}

/// The PACING_TIMER dispatch block of one kernel, from the arm's opening test to the plain pacing
/// path's ownership test.
fn pacing_dispatch(kernel: &str) -> &str {
    let start = kernel
        .find("if (kind == PACING_TIMER && role == HOST) {")
        .expect("the PACING_TIMER arm is present");
    let rest = &kernel[start..];
    let end = rest
        .find("generators[generator + G_KIND] != 2")
        .expect("the plain pacing path follows the mechanism arms");
    &rest[..end]
}

#[test]
fn pacing_dispatch_tests_queue_pairs_after_dcqcn_in_both_kernels() {
    // P15 R4 (user ruling, option b): DCQCN images must not pay for the queue-pair PACING_TIMER
    // arms. The queue-pair control tick is folded inside the one DCQCN control-tag test, and the
    // queue-pair pacing token is tested after DCQCN pacing.
    for (backend, kernel) in [("Metal", METAL), ("CUDA", CUDA)] {
        let block = pacing_dispatch(kernel);
        assert_eq!(
            block.matches("== DCQCN_CONTROL_TIMER_PACKET").count(),
            1,
            "{backend}: one DCQCN control-tag test serves both control drivers",
        );
        let at = |token: &str| {
            block
                .find(token)
                .unwrap_or_else(|| panic!("{backend}: `{token}` is missing from the dispatch"))
        };
        let control_tag = at("== DCQCN_CONTROL_TIMER_PACKET");
        assert!(
            control_tag < at("roce_control_tick(")
                && at("roce_control_tick(") < at("dcqcn_control_timer("),
            "{backend}: the queue-pair control tick sits inside the DCQCN control arm, first",
        );
        assert!(
            at("dcqcn_pacing_timer(") < at("roce_pacing_tick("),
            "{backend}: the queue-pair pacing token is tested after DCQCN pacing",
        );
    }
}

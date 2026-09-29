//! P14 perf: an unused P14 mechanism costs no per-transition device work.
//!
//! The zero-cost A/B (`evidence/P14/zero-cost-ab.md`) found the P14 kernels slower on images that
//! use neither DCQCN nor PFC. Presence is image data, planned into the params plane
//! (`device_mechanism::mechanism_flags` and the PFC region offset), so each round kernel reads it
//! once per launch into one uniform value and the transition code tests that value instead of
//! re-reading params or plane rows. These are source-level gates on both kernels: the fingerprint
//! suites prove the gated paths are byte-identical, and these prove the gates exist on both
//! backends and stay in the launch-uniform form.

const CUDA: &str = include_str!("../src/cuda_kernels.cu");
const METAL: &str = include_str!("../src/metal_kernels.metal");

fn kernels() -> [(&'static str, &'static str); 2] {
    [("CUDA", CUDA), ("Metal", METAL)]
}

#[test]
fn the_mechanisms_word_is_read_from_params_once_per_launch() {
    for (backend, source) in kernels() {
        assert_eq!(
            source.matches("params[P_MECHANISMS]").count(),
            1,
            "{backend}: the mechanisms word must be read in exactly one place"
        );
        assert_eq!(
            source.matches("launch_mechanisms(params)").count(),
            1,
            "{backend}: only the round kernel builds the launch-uniform mechanisms value"
        );
    }
}

#[test]
fn the_receiver_marker_is_read_only_when_the_image_has_dcqcn_receivers() {
    for (backend, source) in kernels() {
        assert!(
            source.contains(
                "if ((mechanisms & MECHANISM_DCQCN_RECEIVERS) != 0 &&\n            \
                 packet_kind == DATA_PACKET && event[PK_FLOW] < params[P_FLOW_COUNT] &&"
            ),
            "{backend}: a plain DATA delivery must test the launch-uniform receiver bit before \
             it reads the receiver-row marker"
        );
        assert!(
            !source.contains("if (packet_kind == DATA_PACKET && event[PK_FLOW] < params"),
            "{backend}: the ungated marker test must be gone"
        );
    }
}

#[test]
fn pfc_rows_are_looked_up_only_when_the_image_has_a_pfc_region() {
    for (backend, source) in kernels() {
        // TX_READY and TX_COMPLETE at a switch, and a switch's data arrival.
        assert_eq!(
            source
                .matches("(mechanisms & MECHANISM_PFC_REGION) != 0\n")
                .count(),
            3,
            "{backend}: the three per-transition PFC row lookups must test the launch-uniform \
             PFC bit before they read params[P_PFC_OFFSET]"
        );
        assert!(
            !source.contains("ulong pfc_row = role == SWITCH ? pfc_queue_row(")
                && !source.contains("ulong pfc_row = pfc_queue_row("),
            "{backend}: the ungated PFC row lookups must be gone"
        );
    }
}

#[test]
fn dcqcn_branches_are_taken_only_when_the_image_has_dcqcn_state() {
    for (backend, source) in kernels() {
        for gated in [
            "if ((mechanisms & MECHANISM_DCQCN) != 0 &&\n            \
             (event[PK_KIND] & PK_KIND_MASK) == DCQCN_CONTROL_TIMER_PACKET) {",
            "if ((mechanisms & MECHANISM_DCQCN) != 0 &&\n            \
             flow < params[P_FLOW_COUNT] && generators[generator + G_VALID] != 0 &&",
            "if ((mechanisms & MECHANISM_DCQCN) != 0 && packet_kind == DCQCN_CNP_PACKET) {",
        ] {
            assert!(
                source.contains(gated),
                "{backend}: missing the launch-uniform DCQCN gate `{gated}`"
            );
        }
    }
}

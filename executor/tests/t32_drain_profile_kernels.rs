//! T32 read-only drain-counter source gates.
//!
//! The counter kernels deliberately duplicate the production drain entry point. These tests keep
//! that duplication honest: the production body is snapshotted byte-for-byte, the diagnostic
//! entries retain the semantic call order, and every diagnostic write stays single-owner and
//! free of cross-thread synchronization.

#[path = "support/kernel_span.rs"]
mod kernel_span;

use kernel_span::{CUDA_MARKER, METAL_MARKER, kernel_body, kernel_names};

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const METAL_KERNELS: &str = include_str!("../src/metal_kernels.metal");

fn cuda_kernel(entry: &str) -> &'static str {
    kernel_body(CUDA_KERNELS, CUDA_MARKER, entry)
}

fn metal_kernel(entry: &str) -> &'static str {
    kernel_body(METAL_KERNELS, METAL_MARKER, entry)
}

const fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut index = 0;
    while index < bytes.len() {
        hash = (hash ^ bytes[index] as u64).wrapping_mul(0x0000_0100_0000_01b3);
        index += 1;
    }
    hash
}

fn exact_entry<'source>(source: &'source str, entry: &str) -> &'source str {
    let name = format!("void {entry}(");
    let name_at = source
        .find(&name)
        .unwrap_or_else(|| panic!("`{entry}` must exist"));
    let declaration = source[..name_at]
        .rfind('\n')
        .map_or(0, |newline| newline + 1);
    let open = source[name_at..]
        .find('{')
        .map(|offset| name_at + offset)
        .expect("entry has a body");
    let mut depth = 0_usize;
    for (offset, byte) in source.as_bytes()[open..].iter().copied().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[declaration..=open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("`{entry}` body must close")
}

#[test]
fn production_drain_bodies_remain_the_t32_baseline() {
    assert_eq!(
        fnv1a64(exact_entry(CUDA_KERNELS, "days_round").as_bytes()),
        12_869_438_600_818_256_392
    );
    assert_eq!(
        fnv1a64(exact_entry(METAL_KERNELS, "days_round").as_bytes()),
        779_174_371_007_826_252
    );
}

#[test]
fn both_profile_only_drain_entries_exist_at_the_declared_abi() {
    assert!(
        kernel_names(CUDA_KERNELS, CUDA_MARKER)
            .iter()
            .any(|name| name == "days_round_drain_profile")
    );
    assert!(
        kernel_names(METAL_KERNELS, METAL_MARKER)
            .iter()
            .any(|name| name == "days_round_drain_profile")
    );

    let cuda = cuda_kernel("days_round_drain_profile");
    let metal = metal_kernel("days_round_drain_profile");
    assert!(cuda.contains("DAYS_BUFFERS, ulong *diagnostics"));
    assert!(metal.contains("device ulong *diagnostics [[buffer(28)]]"));

    let cuda_declaration = CUDA_KERNELS
        .find("void days_round_drain_profile")
        .expect("CUDA T32 entry exists");
    let cuda_guard = CUDA_KERNELS[..cuda_declaration]
        .rfind("#if defined(DAYS_T32_PROFILE)")
        .expect("CUDA T32 entry is compiled only for the opt-in diagnostic fatbin");
    let cuda_end = CUDA_KERNELS[cuda_declaration..]
        .find("#endif")
        .map(|offset| cuda_declaration + offset)
        .expect("CUDA T32 entry has a closing feature guard");
    assert!(cuda_guard < cuda_declaration && cuda_declaration < cuda_end);

    let declaration = METAL_KERNELS
        .find("kernel void days_round_drain_profile")
        .expect("Metal T32 entry exists");
    let guard = METAL_KERNELS[..declaration]
        .rfind("#if defined(DAYS_T32_DRAIN_PROFILE)")
        .expect("Metal T32 entry is compiled only for the opt-in diagnostic library");
    let end = METAL_KERNELS[declaration..]
        .find("#endif")
        .map(|offset| declaration + offset)
        .expect("Metal T32 entry has a closing feature guard");
    assert!(guard < declaration && declaration < end);
}

fn assert_in_order(body: &str, markers: &[&str]) {
    let mut cursor = 0;
    for marker in markers {
        let offset = body[cursor..]
            .find(marker)
            .unwrap_or_else(|| panic!("`{marker}` must occur after byte {cursor}"));
        cursor += offset + marker.len();
    }
}

#[test]
fn diagnostic_drain_keeps_the_production_semantic_order() {
    const ORDER: [&str; 8] = [
        "fel_peek(",
        "before_horizon(",
        "fel_pop_selected(",
        "dispatch_event(",
        "state[L_TRANSITIONS] == NONE",
        "state[L_TRANSITIONS] += 1",
        "dispatch_transitions += 1",
        "while (dispatch_transitions < params[P_TRANSITION_CAPACITY])",
    ];
    // The loop marker opens production's sequence, so assert the seven body markers separately
    // and pin the loop itself independently.
    for body in [cuda_kernel("days_round"), metal_kernel("days_round")] {
        assert!(body.contains(ORDER[7]));
        assert_in_order(body, &ORDER[..7]);
    }
    for body in [
        cuda_kernel("days_round_drain_profile"),
        metal_kernel("days_round_drain_profile"),
    ] {
        assert!(body.contains(ORDER[7]));
        assert_in_order(body, &ORDER[..7]);
        assert_in_order(
            body,
            &[
                "dispatch_event(",
                "state[L_TRANSITIONS] == NONE",
                "t32_record_head_visit(",
                "t32_record_remote_emissions(",
                "state[L_TRANSITIONS] += 1",
            ],
        );
    }
}

#[test]
fn diagnostic_writes_are_disjoint_and_never_synchronize_or_steer() {
    for (backend, source, body) in [
        (
            "CUDA",
            CUDA_KERNELS,
            cuda_kernel("days_round_drain_profile"),
        ),
        (
            "Metal",
            METAL_KERNELS,
            metal_kernel("days_round_drain_profile"),
        ),
    ] {
        for forbidden in ["atomic", "volatile", "threadfence", "simdgroup_barrier"] {
            assert!(
                !body.contains(forbidden),
                "{backend} diagnostic drain must not contain `{forbidden}`"
            );
        }
        assert_eq!(body.matches("worklist[active_index]").count(), 1);
        assert!(body.contains("t32_record_head_visit("));
        assert!(body.contains("t32_record_remote_emissions("));

        let helpers = source
            .find("t32_diagnostic_flag(")
            .map(|start| &source[start..])
            .expect("diagnostic helpers exist");
        let helpers = &helpers[..helpers
            .find("days_round_drain_profile")
            .expect("helpers end")];
        let head = source
            .find("t32_record_head_visit(")
            .map(|start| &source[start..])
            .expect("head helper exists");
        let head = &head[..head
            .find("t32_record_remote_emissions")
            .expect("head helper ends")];
        let remote = source
            .find("t32_record_remote_emissions(")
            .map(|start| &source[start..])
            .expect("remote helper exists");
        let remote = &remote[..remote
            .find("days_round_drain_profile")
            .expect("helper ends")];

        assert!(helpers.contains("diagnostics[node]"));
        assert!(head.contains("params[P_NODE_COUNT] + row + active_count - 1"));
        assert!(head.contains("t32_diagnostic_flag("));
        assert!(remote.contains("t32_diagnostic_flag("));
        assert!(remote.contains("channel_emissions_offset + channel"));
        for helper in [helpers, head, remote] {
            assert!(!helper.contains("control["));
            assert!(!helper.contains("lp_state["));
            assert!(!helper.contains("set_semantic_error("));
            assert!(!helper.contains("atomic"));
        }
    }
}

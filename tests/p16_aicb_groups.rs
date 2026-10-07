//! P16 H3 (aicb): communication groups, byte for byte against SimAI's own `MockNcclGroup`
//! (days-gpu `evidence/P16/aicb-design.md` §2).
//!
//! The golden files under `tests/fixtures/aicb/` were printed by SimAI's `MockNcclGroup.cc`
//! (SimAI `f5efb5a`) linked standalone (`evidence/P16/aicb-design/tooling/mockncclgroup_dump.cc`).
//! They also list each group's ring channels; this test compares the groups (the channel lines
//! are H1's `simai_ring_channels` and are compared once it lands).

use std::path::PathBuf;

use days::workload::aicb::{Fidelity, GroupKind, Header, form_groups, render_mockncclgroup};

fn golden_groups(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aicb")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap();
    text.lines()
        .filter(|line| !line.starts_with("  channel"))
        .fold(String::new(), |mut out, line| {
            out.push_str(line);
            out.push('\n');
            out
        })
}

fn header(all_gpus: u32, tp: u32, ep: u32, pp: u32) -> Header {
    Header {
        tp,
        ep,
        pp,
        vpp: 1,
        ga: 1,
        all_gpus,
        pp_comm_bytes: if pp == 1 { 0 } else { 5_242_880 },
    }
}

#[test]
fn simai_groups_equal_mockncclgroup_byte_for_byte() {
    for (golden, header) in [
        (
            "mockncclgroup-flagship-w1024-tp2-ep32.txt",
            header(1024, 2, 32, 1),
        ),
        (
            "mockncclgroup-smoke-w128-tp2-ep32.txt",
            header(128, 2, 32, 1),
        ),
        // b4's header says PP 2; SimAI forces PP to 1 when it forms groups (Sys.cc:1363).
        ("mockncclgroup-b4-w128-tp8-ep1.txt", header(128, 8, 1, 2)),
    ] {
        let groups = form_groups(&header, Fidelity::Simai, 8).expect(golden);
        assert_eq!(groups.stages, 1);
        assert!(groups.pp_pairs.is_empty());
        let rendered = render_mockncclgroup(&groups, &header);
        let expected = golden_groups(golden);
        assert_eq!(
            rendered.lines().count(),
            expected.lines().count(),
            "{golden}"
        );
        assert!(
            rendered == expected,
            "{golden} differs from SimAI's MockNcclGroup"
        );
    }
}

#[test]
fn flagship_group_shapes() {
    let groups = form_groups(&header(1024, 2, 32, 1), Fidelity::Simai, 8).unwrap();
    for (kind, count, size) in [
        (GroupKind::Tp, 512, 2),
        (GroupKind::Dp, 2, 512),
        (GroupKind::Ep, 32, 32),
        (GroupKind::DpEp, 64, 16),
    ] {
        let family = groups.family(kind);
        assert_eq!(
            (family.len(), family.group_size()),
            (count, size),
            "{kind:?}"
        );
    }
    // EP group of rank 70: block 1 (ranks 64..127), slot 0: 64, 66, ..., 126.
    let ep = &groups.ep;
    let group = ep.group(ep.group_index_of(70).unwrap());
    assert_eq!(group, (0..32).map(|l| 64 + 2 * l).collect::<Vec<_>>());
    // DP_EP group of rank 3: slot 1 of TP groups 1, 33, 65, ...: ranks 3, 67, 131, ...
    let dp_ep = &groups.dp_ep;
    let group = dp_ep.group(dp_ep.group_index_of(3).unwrap());
    assert_eq!(group, (0..16).map(|l| 3 + 64 * l).collect::<Vec<_>>());
}

#[test]
fn megatron_groups_are_simai_groups_per_pipeline_stage() {
    // b4 faithful: TP8, PP2, so DP 8 per stage at stride 8; PP pairs (r, r + 64).
    let header = header(128, 8, 1, 2);
    let groups = form_groups(&header, Fidelity::Megatron, 8).unwrap();
    assert_eq!(groups.stages, 2);
    assert_eq!((groups.tp.len(), groups.tp.group_size()), (16, 8));
    assert_eq!((groups.dp.len(), groups.dp.group_size()), (16, 8));
    assert_eq!(groups.dp.group(0), [0, 8, 16, 24, 32, 40, 48, 56]);
    assert_eq!(groups.dp.group(8), [64, 72, 80, 88, 96, 104, 112, 120]);
    assert_eq!(
        groups.dp.group(groups.dp.group_index_of(77).unwrap()),
        [69, 77, 85, 93, 101, 109, 117, 125]
    );
    assert!(groups.ep.is_empty());
    assert_eq!((groups.dp_ep.len(), groups.dp_ep.group_size()), (16, 8));
    assert_eq!(groups.pp_pairs.len(), 64);
    assert_eq!(groups.pp_pairs[0], (0, 64));
    assert_eq!(groups.pp_pairs[63], (63, 127));
    // With PP 1 both fidelities form the same groups (design note §2.3).
    let flat = header_pp1();
    let simai = form_groups(&flat, Fidelity::Simai, 8).unwrap();
    let megatron = form_groups(&flat, Fidelity::Megatron, 8).unwrap();
    assert_eq!(
        render_mockncclgroup(&simai, &flat),
        render_mockncclgroup(&megatron, &flat)
    );
    assert_eq!(
        (simai.tp, simai.dp, simai.ep, simai.dp_ep),
        (megatron.tp, megatron.dp, megatron.ep, megatron.dp_ep)
    );
}

fn header_pp1() -> Header {
    header(128, 2, 32, 1)
}

#[test]
fn group_formation_refusals() {
    for (header, fidelity, gps, expected) in [
        (
            header(128, 1, 1, 1),
            Fidelity::Simai,
            8,
            "builds no TP or EP group",
        ),
        (
            header(100, 2, 1, 1),
            Fidelity::Simai,
            8,
            "not a multiple of 8 GPUs per server",
        ),
        (
            header(96, 3, 1, 1),
            Fidelity::Simai,
            8,
            "neither divides nor is a multiple",
        ),
        (
            header(128, 2, 3, 1),
            Fidelity::Simai,
            8,
            "EP = 3 does not divide DP = 64",
        ),
        (
            header(128, 8, 1, 3),
            Fidelity::Megatron,
            8,
            "TP = 8 x PP = 3 does not divide",
        ),
        (
            header(128, 8, 3, 2),
            Fidelity::Megatron,
            8,
            "EP = 3 does not divide DP = 8",
        ),
    ] {
        let error = form_groups(&header, fidelity, gps).expect_err(expected);
        assert!(
            error.message.contains(expected),
            "`{}` lacks `{expected}`",
            error.message
        );
    }
    // The same PP 3 header is fine under the SimAI fidelity, which ignores PP.
    assert!(form_groups(&header(128, 8, 1, 3), Fidelity::Simai, 8).is_ok());
}

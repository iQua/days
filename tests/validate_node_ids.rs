//! P14 E1b: the node-ID check of `validate`, pinned by its messages and their precedence.
//!
//! `validate_node_ids` is the first check `validate` runs, so for a crafted node table the first
//! error `validate` returns is that check's, if it rejects the table. Its rule, descriptor by
//! descriptor in table order, is: a repeated ID is a duplicate; else an ID at or above the table
//! length is out of range; else an ID different from its position does not match the dense index.
//! The first failing descriptor decides, and at that descriptor the first failing rule.
//!
//! A table the check accepts reaches the next check, `validate_link_ids`. The rows below that must
//! pass therefore also corrupt the first link's ID and expect that check's message, which proves
//! the node check let the table through.
//!
//! Duplicate and out of range cannot both hold at one descriptor (a repeated ID equals an earlier
//! position, which is below the length), so no row can order those two rules.
//!
//! Run: `cargo test -p days --test validate_node_ids` (default matrix, any profile).

use days::scenario::compile_config;
use days_executor::{Backend, LinkId, NodeDescriptor, NodeId, NodeKind, SimulationImage, validate};

const FIXTURE: &str = "configs/ci/executor_smoke.toml";

fn image() -> SimulationImage {
    compile_config(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE))
        .expect("the smoke fixture lowers")
}

/// A node table holding `ids` in order; only the IDs matter to the check.
fn nodes(ids: &[u64]) -> Vec<NodeDescriptor> {
    ids.iter()
        .map(|&id| NodeDescriptor {
            id: NodeId(id),
            kind: NodeKind::Host,
            state_slot: 0,
        })
        .collect()
}

/// The image with `ids` as its node table and its first link's ID moved out of range, so a table
/// the node check accepts fails at the link check instead.
fn crafted(ids: &[u64]) -> SimulationImage {
    let mut image = image();
    image.nodes = nodes(ids);
    let links = image.links.len() as u64;
    image.links[0].id = LinkId(links);
    image
}

fn link_rejection(image: &SimulationImage) -> String {
    let links = image.links.len();
    format!("link ID LinkId({links}) at descriptor 0 is outside dense range 0..{links}")
}

fn rejection(ids: &[u64]) -> (String, String) {
    let image = crafted(ids);
    let error = validate(&image, Backend::Scalar)
        .expect_err("the crafted image is rejected")
        .to_string();
    (error, link_rejection(&image))
}

/// Every malformed node table gets the same first error, with the same message.
#[test]
fn node_id_rejections_keep_their_messages_and_precedence() {
    // (case, node IDs in table order, the expected first error; `None` = the node check passes).
    let cases: &[(&str, &[u64], Option<&str>)] = &[
        ("empty table", &[], None),
        ("one node", &[0], None),
        ("dense", &[0, 1, 2, 3], None),
        (
            "duplicate",
            &[0, 1, 1, 3],
            Some("duplicate node ID NodeId(1) at descriptor 2"),
        ),
        (
            "duplicate of the first ID",
            &[0, 1, 2, 0],
            Some("duplicate node ID NodeId(0) at descriptor 3"),
        ),
        (
            "out of range",
            &[0, 1, 9, 3],
            Some("node ID NodeId(9) at descriptor 2 is outside dense range 0..4"),
        ),
        (
            "out of range at the largest ID",
            &[0, 1, u64::MAX],
            Some(
                "node ID NodeId(18446744073709551615) at descriptor 2 is outside dense range 0..3",
            ),
        ),
        (
            "out of order",
            &[0, 2, 1, 3],
            Some("node ID NodeId(2) at descriptor 1 does not match dense table index 1"),
        ),
        (
            "gap",
            &[0, 1, 3, 4],
            Some("node ID NodeId(3) at descriptor 2 does not match dense table index 2"),
        ),
        (
            "first descriptor mismatched",
            &[1, 0],
            Some("node ID NodeId(1) at descriptor 0 does not match dense table index 0"),
        ),
        (
            "duplicate before mismatch at one descriptor",
            &[0, 0],
            Some("duplicate node ID NodeId(0) at descriptor 1"),
        ),
        (
            "out of range before mismatch at one descriptor",
            &[0, 7],
            Some("node ID NodeId(7) at descriptor 1 is outside dense range 0..2"),
        ),
        (
            "the first failing descriptor decides: range before a later duplicate",
            &[5, 5],
            Some("node ID NodeId(5) at descriptor 0 is outside dense range 0..2"),
        ),
        (
            "the first failing descriptor decides: duplicate before a later range",
            &[0, 1, 2, 2, 9],
            Some("duplicate node ID NodeId(2) at descriptor 3"),
        ),
    ];
    for &(case, ids, expected) in cases {
        let (error, link_error) = rejection(ids);
        let expected = expected.map_or(link_error, str::to_owned);
        assert_eq!(error, expected, "{case}: node IDs {ids:?}");
    }
}

/// The unmodified image validates.
#[test]
fn the_lowered_image_validates() {
    validate(&image(), Backend::Scalar).expect("the lowered smoke image validates");
}

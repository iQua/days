use std::cmp::Ordering;
use std::collections::BTreeSet;

use days_executor::{EventKey, NodeId};

fn key(time_ns: u64, phase: u16, origin_node: u64, origin_seq: u64) -> EventKey {
    EventKey {
        time_ns,
        phase,
        origin_node: NodeId(origin_node),
        origin_seq,
    }
}

fn compare_by_contract(left: &EventKey, right: &EventKey) -> Ordering {
    left.time_ns
        .cmp(&right.time_ns)
        .then_with(|| left.phase.cmp(&right.phase))
        .then_with(|| left.origin_node.cmp(&right.origin_node))
        .then_with(|| left.origin_seq.cmp(&right.origin_seq))
}

#[test]
fn event_key_uses_every_tie_break_level() {
    let base = key(10, 2, 3, 4);

    assert!(base < key(11, 0, 0, 0));
    assert!(base < key(10, 3, 0, 0));
    assert!(base < key(10, 2, 4, 0));
    assert!(base < key(10, 2, 3, 5));
}

#[test]
fn derived_order_matches_the_canonical_lexicographic_contract() {
    let keys = vec![
        key(u64::MAX, u16::MAX, u64::MAX, u64::MAX),
        key(1, 0, 0, 0),
        key(0, u16::MAX, u64::MAX, u64::MAX),
        key(0, 0, 0, 0),
        key(7, 3, 9, 1),
        key(7, 2, u64::MAX, u64::MAX),
        key(7, 3, 8, u64::MAX),
        key(7, 3, 9, 0),
    ];

    let mut derived = keys.clone();
    derived.sort();

    let mut expected = keys;
    expected.sort_by(compare_by_contract);

    assert_eq!(derived, expected);
    assert_eq!(
        derived.iter().copied().collect::<BTreeSet<_>>().len(),
        derived.len(),
        "the boundary fixture must contain unique canonical keys"
    );
    assert!(
        derived
            .windows(2)
            .all(|pair| compare_by_contract(&pair[0], &pair[1]) == Ordering::Less)
    );
}

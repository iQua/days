//! P16 H3 (aicb): SimAI's ECMP ports are exact on the lowered AICB images (design note §0.1
//! item 5 and §3.7; rulings A7 and C2).
//!
//! SimAI's `sport` is the per-pair count of earlier messages, so a message's port is exact when
//! the per-pair static order (op, then channel, then step) is SimAI's run-time order. Conditions
//! (a) and (c) are the plan's (blocking model-stream collectives; no data-stream op in flight
//! while a model-stream op issues network messages; the manifest records `ecmp_ordinals`).
//! Condition (b), no network pair repeated across the channels of one ring instance, is checked
//! here on the lowered images, together with the per-pair message counts `aicb_plan.py`
//! reported (design note §3.7: b4 128 pairs of 15 messages, the smoke 3,728 pairs of at most
//! 159; the flagship's 29,824 pairs of at most 895 are `tests/p16_aicb_flagship.rs`'s).

#[path = "support/aicb.rs"]
mod aicb;

use aicb::{ecmp_pairs, lower, max_ring_channels, ring_pairs_across_channels};

#[test]
fn b4_has_128_network_pairs_of_15_messages_and_no_ring_pair_on_two_channels() {
    let image = lower("b4-simai.toml");
    let pairs = ecmp_pairs(&image);
    assert_eq!(pairs.len(), 128);
    assert!(pairs.values().all(|&count| count == 15), "{pairs:?}");
    // One channel per ring on b4 (16 servers), so (b) holds trivially there.
    assert_eq!(max_ring_channels(&image), 1);
    assert_eq!(ring_pairs_across_channels(&image), []);
}

#[test]
fn the_smoke_has_3728_network_pairs_of_at_most_159_messages_and_no_ring_pair_on_two_channels() {
    let image = lower("smoke-simai.toml");
    let pairs = ecmp_pairs(&image);
    assert_eq!(pairs.len(), 3_728);
    assert_eq!(pairs.values().max(), Some(&159));
    // The DP rings run on four channels.
    assert_eq!(max_ring_channels(&image), 4);
    assert_eq!(ring_pairs_across_channels(&image), []);
}

//! The ECN ramp's exact decision and stateless draw (P16 ecnramp), checked against independent
//! re-statements: the draw against a SplitMix64 written out here, and the ramp test against the
//! rational form `u * span < b * 2^64` in `u128`, with no high multiply.

use days_executor::EcnRampPolicy;
use days_executor::ecn_ramp::{
    ECN_RAMP_DOMAIN, EcnRampAction, ecn_draw, ecn_queue_key, ecn_ramp_decision, ecn_ramp_span,
};

fn splitmix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn reference_key(seed: u64, node: u64, queue: u64) -> u64 {
    splitmix(splitmix(splitmix(seed ^ 0x4543_4e5f_5241_4d50) ^ node) ^ queue)
}

/// Independent statement of the rule (rational form, no `mulhi`).
fn reference(policy: &EcnRampPolicy, queued: u64, size: u64, data: bool, u: u64) -> EcnRampAction {
    let Some(depth) = queued.checked_add(size) else {
        return EcnRampAction::Drop;
    };
    if depth > policy.capacity_bytes {
        return EcnRampAction::Drop;
    }
    if !data || depth < policy.kmin_bytes {
        return EcnRampAction::Enqueue;
    }
    if depth >= policy.kmax_bytes {
        return EcnRampAction::Mark;
    }
    let left = u128::from(u)
        * u128::from(policy.pmax_denominator)
        * u128::from(policy.kmax_bytes - policy.kmin_bytes);
    let right = (u128::from(policy.pmax_numerator) * u128::from(depth - policy.kmin_bytes)) << 64;
    if left < right {
        EcnRampAction::Mark
    } else {
        EcnRampAction::Enqueue
    }
}

const SIMAI_100G: EcnRampPolicy = EcnRampPolicy {
    capacity_bytes: 33_554_432,
    kmin_bytes: 400_000,
    kmax_bytes: 1_600_000,
    pmax_numerator: 1,
    pmax_denominator: 5,
};

fn step(threshold: u64, capacity: u64) -> EcnRampPolicy {
    EcnRampPolicy {
        capacity_bytes: capacity,
        kmin_bytes: threshold,
        kmax_bytes: threshold,
        pmax_numerator: 1,
        pmax_denominator: 1,
    }
}

#[test]
fn the_domain_is_ecn_ramp_in_ascii() {
    assert_eq!(ECN_RAMP_DOMAIN.to_be_bytes(), *b"ECN_RAMP");
}

#[test]
fn the_draw_is_splitmix_of_the_queue_key_and_the_payload() {
    for (seed, node, queue, payload) in [
        (0, 0, 0, 0),
        (7, 1_234, 0, 99_999),
        (u64::MAX, u64::MAX, 3, u64::MAX),
    ] {
        let key = ecn_queue_key(seed, node, queue);
        assert_eq!(key, reference_key(seed, node, queue));
        assert_eq!(ecn_draw(key, payload), splitmix(key ^ payload));
    }
    // Distinct queues and distinct seeds give distinct keys.
    assert_ne!(ecn_queue_key(7, 1, 0), ecn_queue_key(7, 2, 0));
    assert_ne!(ecn_queue_key(7, 1, 0), ecn_queue_key(8, 1, 0));
    assert_ne!(ecn_queue_key(7, 1, 0), ecn_queue_key(7, 1, 1));
}

#[test]
fn the_step_marks_exactly_at_and_above_its_threshold() {
    let policy = step(4_096, 32_768);
    let never = || -> u64 { panic!("a step never draws") };
    for (queued, size, action) in [
        (0, 512, EcnRampAction::Enqueue),
        (3_072, 512, EcnRampAction::Enqueue),
        (3_583, 512, EcnRampAction::Enqueue),
        (3_584, 512, EcnRampAction::Mark),
        (3_585, 512, EcnRampAction::Mark),
        (32_256, 512, EcnRampAction::Mark),
        (32_257, 512, EcnRampAction::Drop),
    ] {
        assert_eq!(
            ecn_ramp_decision(&policy, queued, size, true, never),
            action,
            "queued {queued} + {size}"
        );
    }
}

#[test]
fn the_ramp_boundaries_are_exact() {
    let policy = SIMAI_100G;
    let size = 9_000;
    // d = kmin: probability 0, even for the smallest draw.
    assert_eq!(
        ecn_ramp_decision(&policy, 400_000 - size, size, true, || 0),
        EcnRampAction::Enqueue
    );
    // Below kmin: no draw.
    assert_eq!(
        ecn_ramp_decision(&policy, 399_999 - size, size, true, || panic!("no draw")),
        EcnRampAction::Enqueue
    );
    // d = kmin + 1: only the smallest draws mark.
    assert_eq!(
        ecn_ramp_decision(&policy, 400_001 - size, size, true, || 0),
        EcnRampAction::Mark
    );
    assert_eq!(
        ecn_ramp_decision(&policy, 400_001 - size, size, true, || u64::MAX),
        EcnRampAction::Enqueue
    );
    // d = kmax - 1: P just below Pmax = 1/5.
    let just_below = |u: u64| ecn_ramp_decision(&policy, 1_599_999 - size, size, true, || u);
    let fifth = u64::MAX / 5;
    assert_eq!(just_below(fifth - (1 << 50)), EcnRampAction::Mark);
    assert_eq!(just_below(fifth + 1), EcnRampAction::Enqueue);
    // d = kmax: always, without a draw.
    assert_eq!(
        ecn_ramp_decision(&policy, 1_600_000 - size, size, true, || panic!("no draw")),
        EcnRampAction::Mark
    );
    // Capacity and overflow drop before anything else.
    assert_eq!(
        ecn_ramp_decision(&policy, 33_554_432 - size + 1, size, true, || 0),
        EcnRampAction::Drop
    );
    assert_eq!(
        ecn_ramp_decision(&policy, u64::MAX, 1, false, || 0),
        EcnRampAction::Drop
    );
}

#[test]
fn packets_that_are_not_ecn_capable_data_never_mark_or_draw() {
    let policy = SIMAI_100G;
    for queued in [0, 500_000, 1_000_000, 2_000_000, 33_554_000] {
        assert_eq!(
            ecn_ramp_decision(&policy, queued, 60, false, || panic!("no draw for an ACK")),
            EcnRampAction::Enqueue
        );
    }
    assert_eq!(
        ecn_ramp_decision(&policy, 33_554_400, 60, false, || 0),
        EcnRampAction::Drop
    );
}

#[test]
fn the_high_multiply_equals_the_rational_reference() {
    let policies = [
        SIMAI_100G,
        EcnRampPolicy {
            capacity_bytes: 33_554_432,
            kmin_bytes: 800_000,
            kmax_bytes: 3_200_000,
            pmax_numerator: 1,
            pmax_denominator: 5,
        },
        EcnRampPolicy {
            capacity_bytes: 20_000,
            kmin_bytes: 14_000,
            kmax_bytes: 18_000,
            pmax_numerator: 4,
            pmax_denominator: 5,
        },
        EcnRampPolicy {
            capacity_bytes: u64::MAX,
            kmin_bytes: 1,
            kmax_bytes: u64::MAX / 3,
            pmax_numerator: 2,
            pmax_denominator: 3,
        },
        step(1_000_000, 33_554_432),
    ];
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut compared = 0_u64;
    let mut ramp_marks = 0_u64;
    let mut ramp_enqueues = 0_u64;
    for policy in policies {
        assert!(ecn_ramp_span(&policy).is_some());
        for _ in 0..200_000 {
            state = splitmix(state);
            let u = state;
            state = splitmix(state);
            let size = 1 + state % 9_000;
            state = splitmix(state);
            let top = policy
                .kmax_bytes
                .saturating_add(policy.kmax_bytes / 8)
                .max(1);
            let queued = state % top;
            state = splitmix(state);
            let data = !state.is_multiple_of(8);
            let expected = reference(&policy, queued, size, data, u);
            let actual = ecn_ramp_decision(&policy, queued, size, data, || u);
            assert_eq!(
                actual, expected,
                "{policy:?} queued {queued} size {size} u {u}"
            );
            compared += 1;
            let depth = queued.saturating_add(size);
            if data && depth > policy.kmin_bytes && depth < policy.kmax_bytes {
                match actual {
                    EcnRampAction::Mark => ramp_marks += 1,
                    _ => ramp_enqueues += 1,
                }
            }
        }
    }
    assert_eq!(compared, 1_000_000);
    assert!(
        ramp_marks > 10_000 && ramp_enqueues > 10_000,
        "{ramp_marks} {ramp_enqueues}"
    );
}

/// The draw over consecutive payload ids marks at the ramp's probability: the counts are exact
/// (deterministic), equal to the reference's, and within 0.5% of `P(d)`.
#[test]
fn the_draw_marks_at_the_ramp_probability() {
    let key = ecn_queue_key(7, 42, 0);
    let samples = 1_000_000_u64;
    for (policy, depth, expected_ppm) in [
        (SIMAI_100G, 1_000_000_u64, 100_000_u64),
        (SIMAI_100G, 1_600_000 - 1, 199_999),
        (
            EcnRampPolicy {
                pmax_numerator: 1,
                pmax_denominator: 2,
                ..SIMAI_100G
            },
            1_600_000 - 1,
            499_999,
        ),
        (
            EcnRampPolicy {
                pmax_numerator: 4,
                pmax_denominator: 5,
                ..SIMAI_100G
            },
            1_000_000,
            400_000,
        ),
    ] {
        let marks = (0..samples)
            .filter(|payload| {
                ecn_ramp_decision(&policy, depth - 9_000, 9_000, true, || {
                    ecn_draw(key, *payload)
                }) == EcnRampAction::Mark
            })
            .count() as u64;
        let tolerance = expected_ppm / 200;
        assert!(
            marks.abs_diff(expected_ppm) <= tolerance,
            "{policy:?} at {depth}: {marks} marks, expected {expected_ppm} +- {tolerance}"
        );
        let reference_marks = (0..samples)
            .filter(|payload| {
                reference(
                    &policy,
                    depth - 9_000,
                    9_000,
                    true,
                    splitmix(key ^ *payload),
                ) == EcnRampAction::Mark
            })
            .count() as u64;
        assert_eq!(marks, reference_marks);
    }
}

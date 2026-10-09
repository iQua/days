//! The ECN ramp: one marking policy for every switch queue that marks (P16 ecnramp).
//!
//! For an arrival of `s` bytes at a queue holding `q` bytes (the packet in service excluded),
//! the post-admission depth is `d = q + s`, and with `span = pmax_den * (kmax - kmin)`:
//!
//! 1. `q + s` overflows or `d > capacity`: drop (tail drop at capacity);
//! 2. the packet is not ECN-capable data: enqueue, and no draw is computed;
//! 3. `d < kmin`: enqueue;
//! 4. `d >= kmax`: mark;
//! 5. otherwise mark iff `mulhi64(u, span) < pmax_num * (d - kmin)`, where `u` is the arrival's
//!    draw.
//!
//! Step 5 is exact: for an integer `b`, `floor(x) < b` iff `x < b`, so it is `u * span < b * 2^64`,
//! that is `u / 2^64 < P(d) = Pmax * (d - kmin) / (kmax - kmin)`. Nothing rounds `P(d)`; over a
//! uniform 64-bit `u` the mark probability is `ceil(P(d) * 2^64) / 2^64`. At `d = kmin` the ramp
//! never marks. With `kmin == kmax == T` the rule is the step "mark iff `d >= T`".
//!
//! The draw is stateless: `u = mix(queue_key ^ payload)`, with `queue_key = mix(mix(mix(seed ^
//! ECN_RAMP_DOMAIN) ^ node) ^ queue)`, `mix` the SplitMix64 output function. Every input is a
//! fixed semantic identity (the image seed, the switch LP, the queue slot, the transmission
//! attempt's payload id), so the draw does not depend on event order, partitioning or allocation,
//! and nothing (an ACK included) can consume or reset it. LeanGuard recomputes it
//! (`lean/LeanGuard/P10c/Semantics.lean`, namespace `Aqm`).

use crate::EcnRampPolicy;
use crate::splitmix::mix;

/// Separates the ramp's draw from the image seed's other uses (`"ECN_RAMP"` in ASCII).
pub const ECN_RAMP_DOMAIN: u64 = 0x4543_4e5f_5241_4d50;

/// One enqueue decision of the ECN ramp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EcnRampAction {
    Enqueue,
    Mark,
    Drop,
}

/// The per-queue key of the draw.
pub const fn ecn_queue_key(seed: u64, node: u64, queue: u64) -> u64 {
    mix(mix(mix(seed ^ ECN_RAMP_DOMAIN) ^ node) ^ queue)
}

/// The draw of one arrival at the queue with key `key`.
pub const fn ecn_draw(key: u64, payload: u64) -> u64 {
    mix(key ^ payload)
}

/// `pmax_den * (kmax - kmin)`, the ramp's span; `None` when it does not fit `u64` (validation
/// refuses such a policy).
pub const fn ecn_ramp_span(policy: &EcnRampPolicy) -> Option<u64> {
    policy
        .pmax_denominator
        .checked_mul(policy.kmax_bytes.saturating_sub(policy.kmin_bytes))
}

/// The decision for an arrival of `size_bytes` at a queue holding `queued_bytes`. `draw` is
/// called at most once, and only inside the ramp for ECN-capable data.
pub fn ecn_ramp_decision(
    policy: &EcnRampPolicy,
    queued_bytes: u64,
    size_bytes: u64,
    ecn_capable: bool,
    draw: impl FnOnce() -> u64,
) -> EcnRampAction {
    let Some(depth) = queued_bytes.checked_add(size_bytes) else {
        return EcnRampAction::Drop;
    };
    if depth > policy.capacity_bytes {
        return EcnRampAction::Drop;
    }
    if !ecn_capable || depth < policy.kmin_bytes {
        return EcnRampAction::Enqueue;
    }
    if depth >= policy.kmax_bytes {
        return EcnRampAction::Mark;
    }
    // kmin <= depth < kmax, so kmin < kmax; validation bounds `span` by `u64::MAX`, and
    // `pmax_num * (depth - kmin) < pmax_num * (kmax - kmin) <= span`.
    let span =
        u128::from(policy.pmax_denominator) * u128::from(policy.kmax_bytes - policy.kmin_bytes);
    let bound = u128::from(policy.pmax_numerator) * u128::from(depth - policy.kmin_bytes);
    if (u128::from(draw()) * span) >> 64 < bound {
        EcnRampAction::Mark
    } else {
        EcnRampAction::Enqueue
    }
}

/// The ECN ramp's well-formedness: a positive byte capacity,
/// `1 <= kmin <= kmax <= capacity`, `0 < Pmax <= 1` in lowest terms, `Pmax = 1` for a step
/// (`kmin == kmax`, so a step has one representation), and a span `pmax_den * (kmax - kmin)` that
/// fits `u64` (the device's high multiply takes it as one word).
pub fn ecn_ramp_policy_check(policy: &crate::EcnRampPolicy) -> Result<(), &'static str> {
    if policy.capacity_bytes == 0 {
        return Err("needs a positive byte capacity");
    }
    if policy.kmin_bytes == 0
        || policy.kmin_bytes > policy.kmax_bytes
        || policy.kmax_bytes > policy.capacity_bytes
    {
        return Err("needs 1 <= kmin <= kmax <= capacity bytes");
    }
    let (numerator, denominator) = (policy.pmax_numerator, policy.pmax_denominator);
    if numerator == 0 || numerator > denominator {
        return Err("needs 0 < pmax <= 1");
    }
    if gcd(numerator, denominator) != 1 {
        return Err("needs pmax in lowest terms");
    }
    if policy.kmin_bytes == policy.kmax_bytes && numerator != denominator {
        return Err("step (kmin == kmax) needs pmax = 1");
    }
    if ecn_ramp_span(policy).is_none() {
        return Err("span pmax_denominator * (kmax - kmin) exceeds u64");
    }
    Ok(())
}

const fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

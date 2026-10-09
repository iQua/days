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
pub const fn ecn_queue_key(_seed: u64, _node: u64, _queue: u64) -> u64 {
    0
}

/// The draw of one arrival at the queue with key `key`.
pub const fn ecn_draw(_key: u64, _payload: u64) -> u64 {
    0
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
    _policy: &EcnRampPolicy,
    _queued_bytes: u64,
    _size_bytes: u64,
    _ecn_capable: bool,
    _draw: impl FnOnce() -> u64,
) -> EcnRampAction {
    EcnRampAction::Enqueue
}

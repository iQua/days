//! Exact Mellanox-form DCQCN reaction point (the controller of HPCC's ns-3 `CC_MODE 1` and SimAI).
//!
//! Specification: `days-gpu/evidence/P16/simai-dcqcn-spec.md` section 7; design and rulings:
//! `days-gpu/evidence/P16/dcqcn-design.md` (D1-D17).
//!
//! Arithmetic is exact integer arithmetic. Rates are integral bits per second. The congestion
//! estimate alpha is a Q63 fraction (`alpha = 1` is `2^63`); each alpha step floors once, and a cut
//! is `R - ceil(R * alpha / 2^64)`, which equals `floor(R * (1 - alpha / 2))` exactly. The rate
//! average is `floor(R_C / 2) + floor(R_T / 2)`, as HPCC's `DataRate` divisions truncate each half.
//!
//! Timers are lazy (ruling D2): the controller owns three periodic timers (the alpha update, the
//! rate-decrease check and the rate-increase timer) but no event. Their instants are applied, in
//! the eager machine's order, by [`DcqcnController::materialize`] at the flow's next transition:
//! with an exclusive `bound` of `now` at a phase-0 transition (a feedback arrival precedes every
//! controller instant at its time) and `now + 1` at a phase-1 transition (a controller instant at
//! `now` precedes the flow's pacing tick or timeout at `now`). At equal time the order is alpha,
//! then rate increase, then rate decrease. Alpha ticks are applied only where alpha is read or
//! written (at a feedback, at a cut and at the freeze), so `alpha_q63` is alpha as of
//! `next_alpha_ns - alpha_interval_ns`. An instant at `u64::MAX` is never reached.

use std::error::Error;
use std::fmt;

/// `alpha = 1` in Q63.
pub const DCQCN_ALPHA_ONE: u64 = 1 << 63;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnControllerConfig {
    /// R_C = R_T at creation.
    pub initial_rate_bps: u64,
    /// The floor of a cut (HPCC `MIN_RATE`); increases are not clamped below.
    pub minimum_rate_bps: u64,
    /// The clamp of the target rate on additive and hyper increases (the NIC rate).
    pub maximum_rate_bps: u64,
    /// HPCC `RATE_AI`.
    pub additive_rate_bps: u64,
    /// HPCC `RATE_HAI`.
    pub hyper_rate_bps: u64,
    /// HPCC `EWMA_GAIN` in Q63.
    pub g_q63: u64,
    /// HPCC `ALPHA_RESUME_INTERVAL`.
    pub alpha_interval_ns: u64,
    /// HPCC `RATE_DECREASE_INTERVAL`.
    pub decrease_interval_ns: u64,
    /// HPCC `RP_TIMER`.
    pub increase_interval_ns: u64,
    /// HPCC `FAST_RECOVERY_TIMES`.
    pub fast_recovery_steps: u32,
    /// HPCC `CLAMP_TARGET_RATE`.
    pub clamp_target_rate: bool,
}

/// Mellanox-form reaction-point state. Before its first feedback (`armed == false`) a controller
/// is pristine: every timer field is zero and alpha is one.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnController {
    pub config: DcqcnControllerConfig,
    pub alpha_q63: u64,
    pub current_rate_bps: u64,
    pub target_rate_bps: u64,
    /// The first alpha tick not yet applied.
    pub next_alpha_ns: u64,
    /// An instant of the decrease grid `t0 + D + 1 + m * D`: the next check while
    /// `decrease_pending`.
    pub next_decrease_ns: u64,
    /// The next rate-increase timer instant while `increase_armed`.
    pub next_increase_ns: u64,
    /// HPCC `m_rpTimeStage`, saturated at `fast_recovery_steps + 1` (observably equal).
    pub stage: u32,
    /// Some feedback has arrived (HPCC `!m_first_cnp`).
    pub armed: bool,
    /// HPCC `m_alpha_cnp_arrived`.
    pub alpha_pending: bool,
    /// HPCC `m_decrease_cnp_arrived`.
    pub decrease_pending: bool,
    /// The rate-increase timer runs: from the first cut on.
    pub increase_armed: bool,
}

/// What one materialization applied (diagnostic counts carried by the transition record).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DcqcnAdvance {
    pub alpha_ticks: u64,
    pub increase_fires: u64,
    pub decrease_cuts: u64,
}

impl DcqcnAdvance {
    pub const fn applied_rate_change(self) -> bool {
        self.increase_fires != 0 || self.decrease_cuts != 0
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DcqcnTransitionKind {
    /// A feedback: a CNP arrival, or an ACK or NACK carrying the ECN echo (bound `now`).
    Feedback = 0,
    /// A pacing tick of an unreliable DCQCN flow, emitting or not (bound `now + 1`); the rate the
    /// tick read is the row's `after.current_rate_bps` (ruling D17).
    Tick = 1,
    /// Any other transition of the flow whose materialization applied a rate-increase or
    /// rate-decrease instant, or that froze the controller.
    Advance = 2,
}

impl DcqcnTransitionKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Feedback => "feedback",
            Self::Tick => "tick",
            Self::Advance => "advance",
        }
    }
}

/// One exact controller transition (schema `days-gpu/plans/briefs/p16/dcqcn-schema.md`), consumed
/// by the LeanGuard replay checker: `after` is `on_feedback(before, bound)` for a feedback, else
/// `settle(before, bound)` when `frozen` and `materialize(before, bound)` otherwise.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnTransitionRecord {
    pub key: crate::EventKey,
    pub node: crate::NodeId,
    pub flow: crate::FlowId,
    pub kind: DcqcnTransitionKind,
    /// The exclusive bound of the controller instants this transition applied.
    pub bound_ns: u64,
    pub advance: DcqcnAdvance,
    /// The flow is complete after this transition, and its controller frozen (ruling D11).
    pub frozen: bool,
    pub before: DcqcnController,
    pub after: DcqcnController,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DcqcnArithmeticError {
    InvalidConfiguration(&'static str),
}

impl fmt::Display for DcqcnArithmeticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
        }
    }
}

impl Error for DcqcnArithmeticError {}

/// The lowest rate a controller can reach: `2 * floor((minimum - 1) / 2)`. The cut floors at the
/// minimum, a target clamp copies the current rate, and the average `floor(R_C/2) + floor(R_T/2)`
/// of two rates at or above this even floor stays at or above it. Validation requires a minimum
/// of at least 3 bit/s, so the floor is positive and pacing always progresses.
pub const fn dcqcn_rate_floor_bps(minimum_rate_bps: u64) -> u64 {
    2 * (minimum_rate_bps.saturating_sub(1) / 2)
}

/// `floor(a * b / 2^63)` for `a, b <= 2^63`.
#[inline(always)]
const fn mul_shr63(a: u64, b: u64) -> u64 {
    ((a as u128 * b as u128) >> 63) as u64
}

impl DcqcnControllerConfig {
    pub fn validate(self) -> Result<(), DcqcnArithmeticError> {
        let invalid = DcqcnArithmeticError::InvalidConfiguration;
        if self.minimum_rate_bps < 3 {
            return Err(invalid(
                "DCQCN minimum rate must be at least 3 bit/s: the rate average truncates each half, so the rate can fall to 2 * floor((minimum - 1) / 2)",
            ));
        }
        if self.minimum_rate_bps > self.initial_rate_bps
            || self.initial_rate_bps > self.maximum_rate_bps
        {
            return Err(invalid(
                "DCQCN rates must satisfy minimum <= initial <= maximum",
            ));
        }
        if self.g_q63 > DCQCN_ALPHA_ONE {
            return Err(invalid("DCQCN g must be in 0..=1"));
        }
        if self.alpha_interval_ns == 0
            || self.decrease_interval_ns == 0
            || self.increase_interval_ns == 0
        {
            return Err(invalid(
                "DCQCN alpha, rate-decrease and rate-increase intervals must be positive",
            ));
        }
        if self.fast_recovery_steps == u32::MAX {
            return Err(invalid(
                "DCQCN fast recovery times must be below 4294967295",
            ));
        }
        Ok(())
    }
}

impl DcqcnController {
    pub fn new(config: DcqcnControllerConfig) -> Result<Self, DcqcnArithmeticError> {
        config.validate()?;
        Ok(Self::pristine(config))
    }

    /// The controller before its first feedback, for a configuration already validated.
    pub const fn pristine(config: DcqcnControllerConfig) -> Self {
        Self {
            config,
            alpha_q63: DCQCN_ALPHA_ONE,
            current_rate_bps: config.initial_rate_bps,
            target_rate_bps: config.initial_rate_bps,
            next_alpha_ns: 0,
            next_decrease_ns: 0,
            next_increase_ns: 0,
            stage: 0,
            armed: false,
            alpha_pending: false,
            decrease_pending: false,
            increase_armed: false,
        }
    }

    /// The earliest rate-changing instant (an armed rate-increase timer or a pending decrease
    /// check), or `u64::MAX` when there is none. Alpha ticks never change the rate.
    #[inline(always)]
    pub const fn due_ns(&self) -> u64 {
        let increase = if self.increase_armed {
            self.next_increase_ns
        } else {
            u64::MAX
        };
        let decrease = if self.decrease_pending {
            self.next_decrease_ns
        } else {
            u64::MAX
        };
        if increase < decrease {
            increase
        } else {
            decrease
        }
    }

    /// Applies, in the eager machine's order, every rate-increase and rate-decrease instant with
    /// time `< bound_ns`, and the alpha ticks each cut reads. Inline callers test
    /// [`Self::due_ns`] first; this body runs only when an instant is due.
    pub fn materialize(&mut self, bound_ns: u64) -> DcqcnAdvance {
        let mut advance = DcqcnAdvance::default();
        loop {
            let increase = if self.increase_armed {
                self.next_increase_ns
            } else {
                u64::MAX
            };
            let decrease = if self.decrease_pending {
                self.next_decrease_ns
            } else {
                u64::MAX
            };
            let instant = increase.min(decrease);
            if instant >= bound_ns {
                return advance;
            }
            if increase <= decrease {
                self.increase_fire(increase);
                advance.increase_fires += 1;
            } else {
                // Alpha ticks at the cut's instant precede it.
                advance.alpha_ticks += self.alpha_through(decrease);
                self.decrease_check(decrease);
                advance.decrease_cuts += 1;
            }
        }
    }

    /// One feedback (an echoing ACK or NACK, or a CNP) at `now_ns`, a phase-0 transition: every
    /// controller instant before `now_ns` applies first.
    pub fn on_feedback(&mut self, now_ns: u64) -> DcqcnAdvance {
        if !self.armed {
            // HPCC `cnp_received_mlx` on the first CNP: alpha is (still) one, the first CNP does not
            // count for the first alpha step, the first decrease check always cuts, and the alpha
            // and decrease timers start; the decrease grid carries HPCC's persistent +1 ns.
            self.armed = true;
            self.alpha_q63 = DCQCN_ALPHA_ONE;
            self.alpha_pending = false;
            self.decrease_pending = true;
            self.next_alpha_ns = now_ns.saturating_add(self.config.alpha_interval_ns);
            self.next_decrease_ns = now_ns
                .saturating_add(self.config.decrease_interval_ns)
                .saturating_add(1);
            return DcqcnAdvance::default();
        }
        let mut advance = self.materialize(now_ns);
        if let Some(last) = now_ns.checked_sub(1) {
            advance.alpha_ticks += self.alpha_through(last);
        }
        self.alpha_pending = true;
        if !self.decrease_pending {
            self.decrease_pending = true;
            self.next_decrease_ns = self.first_decrease_at_or_after(now_ns);
        }
        advance
    }

    /// The freeze at the transition that completes the flow (ruling D11), with the same exclusive
    /// bound: every instant before `bound_ns` applies, alpha included, and the decrease grid moves
    /// to its first instant at or after the bound, so the frozen state equals the eager machine's.
    pub fn settle(&mut self, bound_ns: u64) -> DcqcnAdvance {
        if !self.armed {
            return DcqcnAdvance::default();
        }
        let mut advance = self.materialize(bound_ns);
        if let Some(last) = bound_ns.checked_sub(1) {
            advance.alpha_ticks += self.alpha_through(last);
        }
        self.next_decrease_ns = self.first_decrease_at_or_after(bound_ns);
        advance
    }

    /// Applies every alpha tick with time `<= time_ns`; returns how many.
    fn alpha_through(&mut self, time_ns: u64) -> u64 {
        if self.next_alpha_ns > time_ns || self.next_alpha_ns == u64::MAX {
            return 0;
        }
        let interval = self.config.alpha_interval_ns;
        let ticks = (time_ns - self.next_alpha_ns) / interval + 1;
        let retained = DCQCN_ALPHA_ONE - self.config.g_q63;
        // floor(((S - g) * alpha + g * S) / S) = floor((S - g) * alpha / S) + g exactly.
        let mut alpha = mul_shr63(retained, self.alpha_q63);
        if self.alpha_pending {
            alpha += self.config.g_q63;
        }
        self.alpha_pending = false;
        // Pure decays; floor makes alpha reach zero, after which every step is a no-op.
        let mut remaining = ticks - 1;
        while remaining != 0 && alpha != 0 {
            alpha = mul_shr63(retained, alpha);
            remaining -= 1;
        }
        self.alpha_q63 = alpha;
        let advanced = u128::from(ticks) * u128::from(interval) + u128::from(self.next_alpha_ns);
        self.next_alpha_ns = u64::try_from(advanced).unwrap_or(u64::MAX);
        ticks
    }

    /// HPCC `CheckRateDecreaseMlx` at an instant where a decrease is pending.
    fn decrease_check(&mut self, time_ns: u64) {
        self.next_decrease_ns = time_ns.saturating_add(self.config.decrease_interval_ns);
        if self.config.clamp_target_rate || self.stage != 0 {
            self.target_rate_bps = self.current_rate_bps;
        }
        let rate = self.current_rate_bps;
        let product = u128::from(rate) * u128::from(self.alpha_q63);
        let reduction = (product >> 64) as u64 + u64::from(product as u64 != 0);
        self.current_rate_bps = (rate - reduction).max(self.config.minimum_rate_bps);
        self.stage = 0;
        self.decrease_pending = false;
        self.increase_armed = true;
        self.next_increase_ns = time_ns.saturating_add(self.config.increase_interval_ns);
    }

    /// HPCC `RateIncEventTimerMlx` and `RateIncEventMlx`.
    fn increase_fire(&mut self, time_ns: u64) {
        self.next_increase_ns = time_ns.saturating_add(self.config.increase_interval_ns);
        let steps = self.config.fast_recovery_steps;
        if self.stage == steps {
            self.target_rate_bps = self
                .target_rate_bps
                .saturating_add(self.config.additive_rate_bps)
                .min(self.config.maximum_rate_bps);
        } else if self.stage > steps {
            self.target_rate_bps = self
                .target_rate_bps
                .saturating_add(self.config.hyper_rate_bps)
                .min(self.config.maximum_rate_bps);
        }
        self.current_rate_bps = self.current_rate_bps / 2 + self.target_rate_bps / 2;
        if self.stage <= steps {
            self.stage += 1;
        }
    }

    /// The first instant of the decrease grid at or after `time_ns`.
    fn first_decrease_at_or_after(&self, time_ns: u64) -> u64 {
        let anchor = self.next_decrease_ns;
        if anchor >= time_ns || anchor == u64::MAX {
            return anchor;
        }
        let interval = u128::from(self.config.decrease_interval_ns);
        let steps = (u128::from(time_ns - anchor)).div_ceil(interval);
        u64::try_from(u128::from(anchor) + steps * interval).unwrap_or(u64::MAX)
    }

    /// Range checks of a controller held in an image: alpha in `0..=1`, the stage saturated, a
    /// pristine controller before its first feedback.
    pub fn validate_state(&self) -> Result<(), &'static str> {
        self.config
            .validate()
            .map_err(|_| "has an invalid DCQCN configuration")?;
        if self.alpha_q63 > DCQCN_ALPHA_ONE {
            return Err("has a DCQCN alpha above one");
        }
        if self.stage > self.config.fast_recovery_steps + 1 {
            return Err("has a DCQCN stage above fast_recovery_times + 1");
        }
        if !self.armed && *self != Self::pristine(self.config) {
            return Err("has a DCQCN controller that moved before its first feedback");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> DcqcnControllerConfig {
        DcqcnControllerConfig {
            initial_rate_bps: 100_000_000_000,
            minimum_rate_bps: 100_000_000,
            maximum_rate_bps: 100_000_000_000,
            additive_rate_bps: 50_000_000,
            hyper_rate_bps: 100_000_000,
            g_q63: DCQCN_ALPHA_ONE >> 8,
            alpha_interval_ns: 1_000,
            decrease_interval_ns: 4_000,
            increase_interval_ns: 900_000,
            fast_recovery_steps: 1,
            clamp_target_rate: false,
        }
    }

    #[test]
    fn the_alpha_update_identity_matches_the_u128_form() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let alpha = state >> 1;
            let g = (state.rotate_left(17) >> 1).min(DCQCN_ALPHA_ONE);
            let s = u128::from(DCQCN_ALPHA_ONE);
            let direct = ((s - u128::from(g)) * u128::from(alpha) + u128::from(g) * s) / s;
            assert_eq!(
                u128::from(mul_shr63(DCQCN_ALPHA_ONE - g, alpha) + g),
                direct
            );
        }
    }

    #[test]
    fn the_cut_identity_matches_the_u128_form() {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut controller = DcqcnController::pristine(config());
        for _ in 0..10_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let rate = state;
            let alpha = (state.rotate_left(29) >> 1).min(DCQCN_ALPHA_ONE);
            let direct = (u128::from(rate) * ((1_u128 << 64) - u128::from(alpha))) >> 64;
            controller.current_rate_bps = rate;
            controller.alpha_q63 = alpha;
            controller.config.minimum_rate_bps = 1;
            controller.decrease_check(0);
            assert_eq!(u128::from(controller.current_rate_bps), direct.max(1));
        }
    }

    #[test]
    fn the_first_feedback_arms_and_the_first_check_halves_the_rate() {
        let mut controller = DcqcnController::new(config()).unwrap();
        controller.on_feedback(10_000);
        assert!(controller.armed && controller.decrease_pending && !controller.alpha_pending);
        assert_eq!(controller.next_alpha_ns, 11_000);
        assert_eq!(controller.next_decrease_ns, 14_001);
        // No feedback in the first alpha window: alpha = (1 - g) at 11, 12, 13 and 14 us, and the
        // check at 14,001 ns reads it.
        let advance = controller.materialize(14_002);
        assert_eq!(advance.decrease_cuts, 1);
        assert_eq!(advance.alpha_ticks, 4);
        let mut alpha = DCQCN_ALPHA_ONE;
        for _ in 0..4 {
            alpha = mul_shr63(DCQCN_ALPHA_ONE - (DCQCN_ALPHA_ONE >> 8), alpha);
        }
        assert_eq!(controller.alpha_q63, alpha);
        assert_eq!(controller.target_rate_bps, 100_000_000_000);
        assert_eq!(controller.stage, 0);
        assert_eq!(controller.next_increase_ns, 914_001);
    }

    #[test]
    fn the_average_truncates_each_half() {
        let mut controller = DcqcnController::pristine(config());
        controller.current_rate_bps = 3;
        controller.target_rate_bps = 5;
        controller.increase_fire(0);
        // floor(3/2) + floor(5/2) = 3, where floor((3+5)/2) = 4.
        assert_eq!(controller.current_rate_bps, 3);
    }

    #[test]
    fn the_stage_saturates_and_hyper_increase_clamps_at_the_maximum() {
        let mut controller = DcqcnController::pristine(config());
        controller.current_rate_bps = 1_000_000_000;
        controller.target_rate_bps = 99_950_000_000;
        controller.increase_fire(0); // stage 0: fast recovery
        assert_eq!(controller.target_rate_bps, 99_950_000_000);
        controller.increase_fire(1); // stage 1 = F: one additive step
        assert_eq!(controller.target_rate_bps, 100_000_000_000);
        controller.increase_fire(2); // stage 2 > F: hyper, clamped
        assert_eq!(controller.target_rate_bps, 100_000_000_000);
        for time in 3..10 {
            controller.increase_fire(time);
        }
        assert_eq!(controller.stage, 2);
    }

    #[test]
    fn a_long_idle_gap_decays_alpha_to_exactly_zero() {
        let mut controller = DcqcnController::new(config()).unwrap();
        controller.on_feedback(0);
        controller.materialize(4_002);
        let ticks = controller.alpha_through(1_000_000_000);
        // Ticks at 5 us .. 1 s inclusive (the cut at 4,001 ns consumed 1..4 us).
        assert_eq!(ticks, 999_996);
        assert_eq!(controller.alpha_q63, 0);
        assert_eq!(controller.next_alpha_ns, 1_000_001_000);
    }
}

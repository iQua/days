//! Test-only eager oracle of the Mellanox-form DCQCN reaction point (P16 D1, condition 1). Never
//! shipped. It is the spec's machine as written (`days-gpu/evidence/P16/simai-dcqcn-spec.md`
//! section 7): three periodic timers, every alpha tick applied when it fires, the decrease check
//! re-armed whether or not a decrease is pending, an unsaturated stage counter, and the direct u128
//! forms of the arithmetic (no algebraic identity shared with the lazy controller). At equal time
//! the timers fire in the order alpha, rate increase, rate decrease, and a feedback arrival at `t`
//! precedes every timer at `t`.
#![allow(dead_code)]

use days_executor::{DcqcnController, DcqcnControllerConfig};

const ONE: u128 = 1 << 63;

#[derive(Clone, Debug)]
pub struct EagerDcqcn {
    pub config: DcqcnControllerConfig,
    pub alpha: u128,
    pub current: u64,
    pub target: u64,
    pub stage: u64,
    pub armed: bool,
    pub alpha_pending: bool,
    pub decrease_pending: bool,
    pub increase_armed: bool,
    pub next_alpha: Option<u64>,
    pub next_decrease: Option<u64>,
    pub next_increase: Option<u64>,
}

/// `t + interval`, where an instant at or beyond `u64::MAX` never fires.
fn later(time: u64, interval: u64) -> Option<u64> {
    time.checked_add(interval).filter(|&next| next != u64::MAX)
}

impl EagerDcqcn {
    pub fn new(config: DcqcnControllerConfig) -> Self {
        Self {
            config,
            alpha: ONE,
            current: config.initial_rate_bps,
            target: config.initial_rate_bps,
            stage: 0,
            armed: false,
            alpha_pending: false,
            decrease_pending: false,
            increase_armed: false,
            next_alpha: None,
            next_decrease: None,
            next_increase: None,
        }
    }

    /// Fires every timer with time `< bound`, in time order, alpha before increase before decrease
    /// at equal time.
    pub fn advance_to(&mut self, bound: u64) {
        loop {
            let candidates = [
                (self.next_alpha, 0_u8),
                (self.next_increase, 1),
                (self.next_decrease, 2),
            ];
            let Some((time, which)) = candidates
                .iter()
                .filter_map(|&(time, which)| time.map(|time| (time, which)))
                .min()
            else {
                return;
            };
            if time >= bound {
                return;
            }
            match which {
                0 => self.alpha_tick(time),
                1 => self.increase_timer(time),
                _ => self.decrease_timer(time),
            }
        }
    }

    pub fn feedback(&mut self, now: u64) {
        self.alpha_pending = true;
        self.decrease_pending = true;
        if !self.armed {
            self.armed = true;
            self.alpha = ONE;
            self.alpha_pending = false;
            self.next_alpha = later(now, self.config.alpha_interval_ns);
            self.next_decrease =
                later(now, self.config.decrease_interval_ns).and_then(|time| later(time, 1));
        }
    }

    fn alpha_tick(&mut self, time: u64) {
        let g = u128::from(self.config.g_q63);
        let marked = if self.alpha_pending { g * ONE } else { 0 };
        self.alpha = ((ONE - g) * self.alpha + marked) / ONE;
        self.alpha_pending = false;
        self.next_alpha = later(time, self.config.alpha_interval_ns);
    }

    fn decrease_timer(&mut self, time: u64) {
        self.next_decrease = later(time, self.config.decrease_interval_ns);
        if !self.decrease_pending {
            return;
        }
        if self.config.clamp_target_rate || self.stage != 0 {
            self.target = self.current;
        }
        let factor = (1_u128 << 64) - self.alpha;
        let cut = ((u128::from(self.current) * factor) >> 64) as u64;
        self.current = cut.max(self.config.minimum_rate_bps);
        self.stage = 0;
        self.decrease_pending = false;
        self.increase_armed = true;
        self.next_increase = later(time, self.config.increase_interval_ns);
    }

    fn increase_timer(&mut self, time: u64) {
        self.next_increase = later(time, self.config.increase_interval_ns);
        let steps = u64::from(self.config.fast_recovery_steps);
        if self.stage == steps {
            self.target = (u128::from(self.target) + u128::from(self.config.additive_rate_bps))
                .min(u128::from(self.config.maximum_rate_bps)) as u64;
        } else if self.stage > steps {
            self.target = (u128::from(self.target) + u128::from(self.config.hyper_rate_bps))
                .min(u128::from(self.config.maximum_rate_bps)) as u64;
        }
        self.current = self.current / 2 + self.target / 2;
        self.stage += 1;
    }

    /// The oracle's state in the lazy controller's representation: the stage saturated at
    /// `fast_recovery_steps + 1`; a timer not yet started as zero (pristine), a timer whose next
    /// instant would be at or beyond `u64::MAX` as `u64::MAX` (never fires).
    pub fn as_controller(&self) -> DcqcnController {
        let instant = |started: bool, time: Option<u64>| {
            if started { time.unwrap_or(u64::MAX) } else { 0 }
        };
        DcqcnController {
            config: self.config,
            alpha_q63: u64::try_from(self.alpha).expect("alpha <= 1"),
            current_rate_bps: self.current,
            target_rate_bps: self.target,
            next_alpha_ns: instant(self.armed, self.next_alpha),
            next_decrease_ns: instant(self.armed, self.next_decrease),
            next_increase_ns: instant(self.increase_armed, self.next_increase),
            stage: u32::try_from(
                self.stage
                    .min(u64::from(self.config.fast_recovery_steps) + 1),
            )
            .expect("saturated stage fits"),
            armed: self.armed,
            alpha_pending: self.alpha_pending,
            decrease_pending: self.decrease_pending,
            increase_armed: self.increase_armed,
        }
    }
}

/// Field-by-field comparison of the rate-relevant state: everything except alpha (applied lazily)
/// and an idle decrease grid (kept stale while no decrease is pending).
pub fn rate_state_matches(lazy: &DcqcnController, eager: &DcqcnController) -> bool {
    lazy.config == eager.config
        && lazy.current_rate_bps == eager.current_rate_bps
        && lazy.target_rate_bps == eager.target_rate_bps
        && lazy.stage == eager.stage
        && lazy.armed == eager.armed
        && lazy.decrease_pending == eager.decrease_pending
        && lazy.increase_armed == eager.increase_armed
        && (!lazy.increase_armed || lazy.next_increase_ns == eager.next_increase_ns)
        && (!lazy.decrease_pending || lazy.next_decrease_ns == eager.next_decrease_ns)
}

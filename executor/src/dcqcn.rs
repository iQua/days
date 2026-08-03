//! Exact fixed-point DCQCN reaction-point controller.
//!
//! Alpha and rational parameters use one part-per-billion scale. Rates remain integral bits per
//! second. Every rational transition evaluates its complete numerator in `u128` and floors once at
//! the final division; no semantic path uses floating point.

use std::error::Error;
use std::fmt;

pub const DCQCN_FRACTION_SCALE: u64 = 1_000_000_000;
pub const DCQCN_STAGE_STEPS: u8 = 5;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnControllerConfig {
    pub initial_rate_bps: u64,
    pub minimum_rate_bps: u64,
    pub maximum_rate_bps: u64,
    pub additive_rate_bps: u64,
    pub hyper_rate_bps: u64,
    pub g_ppb: u64,
    pub decrease_ppb: u64,
    pub cnp_interval_ns: u64,
    pub control_interval_ns: u64,
    pub increase_byte_threshold: u64,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DcqcnIncreaseStage {
    FastRecovery = 0,
    Additive = 1,
    Hyper = 2,
}

impl DcqcnIncreaseStage {
    pub const fn label(self) -> &'static str {
        match self {
            Self::FastRecovery => "fast_recovery",
            Self::Additive => "additive",
            Self::Hyper => "hyper",
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnController {
    pub config: DcqcnControllerConfig,
    pub alpha_ppb: u64,
    pub current_rate_bps: u64,
    pub target_rate_bps: u64,
    pub cnp_seen: bool,
    pub last_cnp_time_ns: Option<u64>,
    pub stage: DcqcnIncreaseStage,
    pub stage_steps: u8,
    pub bytes_since_increase: u64,
    pub next_control_time_ns: u64,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DcqcnTransitionKind {
    Cnp = 0,
    Control = 1,
    Bytes = 2,
}

impl DcqcnTransitionKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Cnp => "cnp",
            Self::Control => "control",
            Self::Bytes => "bytes",
        }
    }
}

/// Full-observation exact controller transition consumed by the T26 LeanGuard replay checker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DcqcnTransitionRecord {
    pub key: crate::EventKey,
    pub node: crate::NodeId,
    pub flow: crate::FlowId,
    pub kind: DcqcnTransitionKind,
    /// CNP accepted, or control/byte increase opportunity applied.
    pub applied: bool,
    /// Nonzero only for the byte-counter transition.
    pub emitted_bytes: u64,
    pub before: DcqcnController,
    pub after: DcqcnController,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DcqcnArithmeticError {
    InvalidConfiguration(&'static str),
    Overflow(&'static str),
    UnexpectedControlTimer { expected_ns: u64, actual_ns: u64 },
}

impl fmt::Display for DcqcnArithmeticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => formatter.write_str(message),
            Self::Overflow(operation) => {
                write!(formatter, "DCQCN arithmetic overflow in {operation}")
            }
            Self::UnexpectedControlTimer {
                expected_ns,
                actual_ns,
            } => write!(
                formatter,
                "DCQCN control timer fired at {actual_ns} ns, expected {expected_ns} ns"
            ),
        }
    }
}

impl Error for DcqcnArithmeticError {}

impl DcqcnControllerConfig {
    pub fn validate(self) -> Result<(), DcqcnArithmeticError> {
        if self.minimum_rate_bps == 0 {
            return Err(DcqcnArithmeticError::InvalidConfiguration(
                "DCQCN minimum rate must be positive",
            ));
        }
        if self.minimum_rate_bps > self.initial_rate_bps
            || self.initial_rate_bps > self.maximum_rate_bps
        {
            return Err(DcqcnArithmeticError::InvalidConfiguration(
                "DCQCN rates must satisfy minimum <= initial <= maximum",
            ));
        }
        if self.g_ppb > DCQCN_FRACTION_SCALE || self.decrease_ppb > DCQCN_FRACTION_SCALE {
            return Err(DcqcnArithmeticError::InvalidConfiguration(
                "DCQCN g and decrease factors must be at most the ppb scale",
            ));
        }
        if self.control_interval_ns == 0 {
            return Err(DcqcnArithmeticError::InvalidConfiguration(
                "DCQCN control interval must be positive",
            ));
        }
        if self.increase_byte_threshold == 0 {
            return Err(DcqcnArithmeticError::InvalidConfiguration(
                "DCQCN increase byte threshold must be positive",
            ));
        }
        Ok(())
    }
}

impl DcqcnController {
    pub fn new(
        config: DcqcnControllerConfig,
        first_control_time_ns: u64,
    ) -> Result<Self, DcqcnArithmeticError> {
        config.validate()?;
        first_control_time_ns
            .checked_add(config.control_interval_ns)
            .ok_or(DcqcnArithmeticError::Overflow("control timer deadline"))?;
        Ok(Self {
            config,
            alpha_ppb: 0,
            current_rate_bps: config.initial_rate_bps,
            target_rate_bps: config.initial_rate_bps,
            cnp_seen: false,
            last_cnp_time_ns: None,
            stage: DcqcnIncreaseStage::Hyper,
            stage_steps: 0,
            bytes_since_increase: 0,
            next_control_time_ns: first_control_time_ns,
        })
    }

    /// Applies one source-side interval-gated CNP. `Ok(false)` is an exact early-CNP no-op.
    pub fn on_cnp(&mut self, now_ns: u64) -> Result<bool, DcqcnArithmeticError> {
        if let Some(last_ns) = self.last_cnp_time_ns {
            let earliest_ns = last_ns
                .checked_add(self.config.cnp_interval_ns)
                .ok_or(DcqcnArithmeticError::Overflow("CNP interval deadline"))?;
            if now_ns < earliest_ns {
                return Ok(false);
            }
        }

        let scale = u128::from(DCQCN_FRACTION_SCALE);
        let retained = u128::from(DCQCN_FRACTION_SCALE - self.config.g_ppb)
            .checked_mul(u128::from(self.alpha_ppb))
            .ok_or(DcqcnArithmeticError::Overflow("alpha retained term"))?;
        let marked = u128::from(self.config.g_ppb)
            .checked_mul(scale)
            .ok_or(DcqcnArithmeticError::Overflow("alpha marked term"))?;
        let next_alpha = retained
            .checked_add(marked)
            .ok_or(DcqcnArithmeticError::Overflow("alpha update"))?
            / scale;
        self.alpha_ppb = u64::try_from(next_alpha)
            .map_err(|_| DcqcnArithmeticError::Overflow("alpha projection"))?;

        self.target_rate_bps = self.current_rate_bps;
        let scale_squared = scale * scale;
        let reduction = u128::from(self.config.decrease_ppb)
            .checked_mul(u128::from(self.alpha_ppb))
            .ok_or(DcqcnArithmeticError::Overflow("rate decrease factor"))?;
        let retained_factor = scale_squared
            .checked_sub(reduction)
            .ok_or(DcqcnArithmeticError::Overflow("rate decrease subtraction"))?;
        let decreased = u128::from(self.current_rate_bps)
            .checked_mul(retained_factor)
            .ok_or(DcqcnArithmeticError::Overflow("rate decrease numerator"))?
            / scale_squared;
        self.current_rate_bps = u64::try_from(decreased)
            .map_err(|_| DcqcnArithmeticError::Overflow("rate decrease projection"))?
            .max(self.config.minimum_rate_bps);
        self.cnp_seen = true;
        self.last_cnp_time_ns = Some(now_ns);
        self.stage = DcqcnIncreaseStage::FastRecovery;
        self.stage_steps = 0;
        self.bytes_since_increase = 0;
        Ok(true)
    }

    /// Applies the canonical periodic controller tick.
    ///
    /// The first tick after an admitted CNP preserves the legacy gate: it only clears `cnp_seen`.
    /// Later ticks decay alpha and advance exactly one staged rate-increase opportunity.
    pub fn on_control_timer(&mut self, now_ns: u64) -> Result<bool, DcqcnArithmeticError> {
        if now_ns != self.next_control_time_ns {
            return Err(DcqcnArithmeticError::UnexpectedControlTimer {
                expected_ns: self.next_control_time_ns,
                actual_ns: now_ns,
            });
        }
        self.next_control_time_ns = now_ns
            .checked_add(self.config.control_interval_ns)
            .ok_or(DcqcnArithmeticError::Overflow("control timer successor"))?;
        if self.cnp_seen {
            self.cnp_seen = false;
            return Ok(false);
        }

        let retained = u128::from(DCQCN_FRACTION_SCALE - self.config.g_ppb)
            .checked_mul(u128::from(self.alpha_ppb))
            .ok_or(DcqcnArithmeticError::Overflow("alpha decay numerator"))?;
        self.alpha_ppb = u64::try_from(retained / u128::from(DCQCN_FRACTION_SCALE))
            .map_err(|_| DcqcnArithmeticError::Overflow("alpha decay projection"))?;
        self.apply_increase()?;
        Ok(true)
    }

    /// Charges emitted data bytes and applies one independent byte-triggered increase opportunity.
    pub fn on_bytes_emitted(&mut self, bytes: u64) -> Result<bool, DcqcnArithmeticError> {
        self.bytes_since_increase = self
            .bytes_since_increase
            .checked_add(bytes)
            .ok_or(DcqcnArithmeticError::Overflow("increase byte counter"))?;
        if self.cnp_seen || self.bytes_since_increase < self.config.increase_byte_threshold {
            return Ok(false);
        }
        self.bytes_since_increase = 0;
        self.apply_increase()?;
        Ok(true)
    }

    fn apply_increase(&mut self) -> Result<(), DcqcnArithmeticError> {
        match self.stage {
            DcqcnIncreaseStage::FastRecovery => {
                self.average_with_target()?;
                self.stage_steps =
                    self.stage_steps
                        .checked_add(1)
                        .ok_or(DcqcnArithmeticError::Overflow(
                            "fast-recovery stage counter",
                        ))?;
                if self.stage_steps == DCQCN_STAGE_STEPS {
                    self.stage = DcqcnIncreaseStage::Additive;
                    self.stage_steps = 0;
                }
            }
            DcqcnIncreaseStage::Additive => {
                self.target_rate_bps = self
                    .target_rate_bps
                    .saturating_add(self.config.additive_rate_bps)
                    .min(self.config.maximum_rate_bps);
                self.average_with_target()?;
                self.stage_steps = self
                    .stage_steps
                    .checked_add(1)
                    .ok_or(DcqcnArithmeticError::Overflow("additive stage counter"))?;
                if self.stage_steps == DCQCN_STAGE_STEPS {
                    self.stage = DcqcnIncreaseStage::Hyper;
                    self.stage_steps = 0;
                }
            }
            DcqcnIncreaseStage::Hyper => {
                self.target_rate_bps = self
                    .target_rate_bps
                    .saturating_add(self.config.hyper_rate_bps)
                    .min(self.config.maximum_rate_bps);
                self.average_with_target()?;
            }
        }
        Ok(())
    }

    fn average_with_target(&mut self) -> Result<(), DcqcnArithmeticError> {
        let average = u128::from(self.current_rate_bps)
            .checked_add(u128::from(self.target_rate_bps))
            .ok_or(DcqcnArithmeticError::Overflow("rate average numerator"))?
            / 2;
        self.current_rate_bps = u64::try_from(average)
            .map_err(|_| DcqcnArithmeticError::Overflow("rate average projection"))?
            .clamp(self.config.minimum_rate_bps, self.config.maximum_rate_bps);
        Ok(())
    }
}

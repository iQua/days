//! Exact, fixed-width TCP congestion-control semantics.
//!
//! The legacy simulator represents Reno's congestion-avoidance credit and all CUBIC state with
//! `f64`.  The executor deliberately does not: elapsed time is integral nanoseconds, while CUBIC
//! windows are decimal fixed point with [`CUBIC_WINDOW_SCALE`] nanosegments per segment.  Every
//! division rounds toward zero.  `K` is the floor of the exact nonnegative cube root.  These rules
//! define the byte-replayable executor lattice; they are a documented numerical divergence from
//! the real-valued RFC equations and from legacy `f64` trajectories near lattice boundaries.

use num_bigint::BigUint;

/// Number of fixed-point CUBIC window units in one MSS-sized segment.
pub const CUBIC_WINDOW_SCALE: u64 = 1_000_000_000;

#[cfg(test)]
const NANOS_PER_SECOND: u64 = 1_000_000_000;
const CUBIC_MAX_SEGMENTS: u64 = 2_000_000;
const CUBIC_MAX_WINDOW_SCALED: u64 = CUBIC_MAX_SEGMENTS * CUBIC_WINDOW_SCALE;
const INITIAL_SSTHRESH_BYTES: u64 = 65_535;
const MIN_RTO_NS: u64 = 1_000_000_000;
const MAX_RTO_NS: u64 = 60_000_000_000;
const RTO_CLOCK_GRANULARITY_NS: u64 = 1_000_000;

/// Observable sender phase shared by Reno and CUBIC.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcpPhase {
    SlowStart = 0,
    CongestionAvoidance = 1,
    FastRecovery = 2,
}

/// Fixed-width Reno state.
///
/// `ca_credit` is Appropriate Byte Counting credit. Congestion avoidance consumes one current
/// window of newly acknowledged bytes to add exactly one MSS. This fixed-width form gives the
/// closed-form Reno sawtooth its exact one-segment-per-window slope; it intentionally replaces the
/// legacy controller's `f64` accumulation of `MSS * acknowledged_bytes / cwnd`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpReno {
    pub mss_bytes: u64,
    pub cwnd_bytes: u64,
    pub ssthresh_bytes: u64,
    pub phase: TcpPhase,
    pub duplicate_acks: u64,
    pub recovery_high_sequence: u64,
    pub ca_credit: u64,
}

/// Fixed-width CUBIC state using decimal nanosegments.
///
/// RFC defaults are represented as exact ratios: beta is `7/10`, C is `2/5`, and fast
/// convergence multiplies by `(1 + beta) / 2 = 17/20`. `epoch_start_ns == u64::MAX` denotes no
/// active epoch; no floating-point sentinel is involved.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpCubic {
    pub mss_bytes: u64,
    pub cwnd_scaled: u64,
    pub ssthresh_scaled: u64,
    pub phase: TcpPhase,
    pub duplicate_acks: u64,
    pub recovery_high_sequence: u64,
    pub w_max_scaled: u64,
    pub w_last_max_scaled: u64,
    pub epoch_start_ns: u64,
    pub srtt_ns: u64,
    pub k_ns: u64,
}

/// Copyable, byte-comparable congestion-control state embedded in a simulation image.
#[repr(C, u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcpCongestionControl {
    Reno(TcpReno) = 0,
    Cubic(TcpCubic) = 1,
}

impl TcpCongestionControl {
    /// Stable controller name used by capability and trace diagnostics.
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Reno(_) => "Reno",
            Self::Cubic(_) => "CUBIC",
        }
    }

    /// Constructs Reno with the legacy initial window of two MSS and the legacy initial threshold.
    pub const fn reno(mss_bytes: u64) -> Self {
        let mss_bytes = nonzero_mss(mss_bytes);
        Self::Reno(TcpReno {
            mss_bytes,
            cwnd_bytes: mss_bytes.saturating_mul(2),
            ssthresh_bytes: INITIAL_SSTHRESH_BYTES,
            phase: TcpPhase::SlowStart,
            duplicate_acks: 0,
            recovery_high_sequence: 0,
            ca_credit: 0,
        })
    }

    /// Constructs CUBIC with the legacy initial window of one MSS.
    pub const fn cubic(mss_bytes: u64) -> Self {
        let mss_bytes = nonzero_mss(mss_bytes);
        let threshold = bytes_to_scaled_const(INITIAL_SSTHRESH_BYTES, mss_bytes);
        Self::Cubic(TcpCubic {
            mss_bytes,
            cwnd_scaled: CUBIC_WINDOW_SCALE,
            ssthresh_scaled: threshold,
            phase: TcpPhase::SlowStart,
            duplicate_acks: 0,
            recovery_high_sequence: 0,
            w_max_scaled: 0,
            w_last_max_scaled: 0,
            epoch_start_ns: u64::MAX,
            srtt_ns: 0,
            k_ns: 0,
        })
    }

    pub const fn phase(self) -> TcpPhase {
        match self {
            Self::Reno(state) => state.phase,
            Self::Cubic(state) => state.phase,
        }
    }

    /// Returns the integral byte window. CUBIC discards sub-byte fixed-point residue.
    ///
    /// `mss_bytes` is explicit because callers already carry the frozen transport parameter. The
    /// controller's frozen MSS is authoritative; a mismatching argument is ignored so malformed
    /// input cannot make otherwise identical controller states behave differently.
    pub const fn cwnd_bytes(self, mss_bytes: u64) -> u64 {
        match self {
            Self::Reno(state) => state.cwnd_bytes,
            Self::Cubic(state) => {
                let _ = mss_bytes;
                scaled_to_bytes_const(state.cwnd_scaled, state.mss_bytes)
            }
        }
    }

    pub const fn ssthresh_bytes(self) -> u64 {
        match self {
            Self::Reno(state) => state.ssthresh_bytes,
            Self::Cubic(state) => scaled_to_bytes_const(state.ssthresh_scaled, state.mss_bytes),
        }
    }

    pub const fn duplicate_acks(self) -> u64 {
        match self {
            Self::Reno(state) => state.duplicate_acks,
            Self::Cubic(state) => state.duplicate_acks,
        }
    }

    pub const fn recovery_high_sequence(self) -> u64 {
        match self {
            Self::Reno(state) => state.recovery_high_sequence,
            Self::Cubic(state) => state.recovery_high_sequence,
        }
    }

    pub const fn ca_credit(self) -> u64 {
        match self {
            Self::Reno(state) => state.ca_credit,
            Self::Cubic(_) => 0,
        }
    }

    pub const fn cwnd_scaled(self) -> u64 {
        match self {
            Self::Reno(state) => bytes_to_scaled_const(state.cwnd_bytes, state.mss_bytes),
            Self::Cubic(state) => state.cwnd_scaled,
        }
    }

    pub const fn ssthresh_scaled(self) -> u64 {
        match self {
            Self::Reno(state) => bytes_to_scaled_const(state.ssthresh_bytes, state.mss_bytes),
            Self::Cubic(state) => state.ssthresh_scaled,
        }
    }

    pub const fn w_max_scaled(self) -> u64 {
        match self {
            Self::Reno(_) => 0,
            Self::Cubic(state) => state.w_max_scaled,
        }
    }

    pub const fn w_last_max_scaled(self) -> u64 {
        match self {
            Self::Reno(_) => 0,
            Self::Cubic(state) => state.w_last_max_scaled,
        }
    }

    pub const fn epoch_start_ns(self) -> Option<u64> {
        match self {
            Self::Cubic(state) if state.epoch_start_ns != u64::MAX => Some(state.epoch_start_ns),
            _ => None,
        }
    }

    pub const fn srtt_ns(self) -> u64 {
        match self {
            Self::Reno(_) => 0,
            Self::Cubic(state) => state.srtt_ns,
        }
    }

    pub const fn cubic_k_ns(self) -> u64 {
        match self {
            Self::Reno(_) => 0,
            Self::Cubic(state) => state.k_ns,
        }
    }

    pub const fn set_recovery_high_sequence(&mut self, sequence: u64) {
        match self {
            Self::Reno(state) => state.recovery_high_sequence = sequence,
            Self::Cubic(state) => state.recovery_high_sequence = sequence,
        }
    }

    /// Applies one cumulative ACK that advances the sender's ACK sequence.
    ///
    /// The last two arguments make recovery handling replayable without hidden packet state:
    /// `flight_size_bytes` is the flight before applying the ACK, and `acknowledgment` is the new
    /// cumulative transport sequence.
    pub fn on_new_ack(
        &mut self,
        acknowledged_bytes: u64,
        now_ns: u64,
        rtt_sample_ns: u64,
        flight_size_bytes: u64,
        acknowledgment: u64,
    ) {
        match self {
            Self::Reno(state) => state.on_new_ack(acknowledged_bytes, acknowledgment),
            Self::Cubic(state) => state.on_new_ack(
                acknowledged_bytes,
                now_ns,
                rtt_sample_ns,
                flight_size_bytes,
                acknowledgment,
            ),
        }
    }

    /// Counts a duplicate ACK and enters fast recovery exactly on the third one.
    ///
    /// The `true` return is the fast-retransmit signal. Later duplicate ACKs only inflate the
    /// recovery window and return `false`.
    pub fn on_duplicate_ack(&mut self, flight_size_bytes: u64, now_ns: u64) -> bool {
        let duplicate_acks = match self {
            Self::Reno(state) => {
                state.duplicate_acks = state.duplicate_acks.saturating_add(1);
                state.duplicate_acks
            }
            Self::Cubic(state) => {
                state.duplicate_acks = state.duplicate_acks.saturating_add(1);
                state.duplicate_acks
            }
        };
        if duplicate_acks == 3 {
            self.on_fast_retransmit(flight_size_bytes, now_ns);
            true
        } else {
            if duplicate_acks > 3 {
                self.on_more_duplicate_ack();
            }
            false
        }
    }

    /// Enters fast recovery after a fast-retransmit loss signal.
    pub fn on_fast_retransmit(&mut self, flight_size_bytes: u64, now_ns: u64) {
        match self {
            Self::Reno(state) => state.on_fast_retransmit(flight_size_bytes),
            Self::Cubic(state) => state.on_fast_retransmit(flight_size_bytes, now_ns),
        }
    }

    /// Applies recovery-window inflation for the fourth and later duplicate ACKs.
    pub fn on_more_duplicate_ack(&mut self) {
        match self {
            Self::Reno(state) => state.on_more_duplicate_ack(),
            Self::Cubic(state) => state.on_more_duplicate_ack(),
        }
    }

    /// Alias used by scalar call sites that classify a non-timeout loss directly.
    pub fn on_loss(&mut self, flight_size_bytes: u64, now_ns: u64) {
        self.on_fast_retransmit(flight_size_bytes, now_ns);
    }

    /// Completes fast recovery after a full cumulative ACK.
    pub fn on_recovery_exit(&mut self) {
        match self {
            Self::Reno(state) => state.on_recovery_exit(),
            Self::Cubic(state) => state.on_recovery_exit(),
        }
    }

    /// Applies an RFC-style retransmission timeout reduction.
    pub fn on_timeout(&mut self, flight_size_bytes: u64, _now_ns: u64) {
        match self {
            Self::Reno(state) => state.on_timeout(flight_size_bytes),
            Self::Cubic(state) => state.on_timeout(flight_size_bytes),
        }
    }

    /// Test/certificate hook that installs an exact CUBIC epoch.
    pub fn force_cubic_epoch_for_test(&mut self, w_max_scaled: u64, epoch_start_ns: u64) {
        if let Self::Cubic(state) = self {
            state.phase = TcpPhase::CongestionAvoidance;
            state.w_max_scaled = w_max_scaled.min(CUBIC_MAX_WINDOW_SCALED);
            state.cwnd_scaled = state.w_max_scaled.max(CUBIC_WINDOW_SCALE);
            state.epoch_start_ns = epoch_start_ns;
            state.k_ns = cubic_k_ns(state.w_max_scaled);
        }
    }

    /// Returns the exact fixed-point CUBIC target at `now + rtt` for fixture generation.
    pub fn cubic_target_for_test(self, now_ns: u64, rtt_ns: u64) -> u64 {
        let Self::Cubic(state) = self else {
            return 0;
        };
        let elapsed_ns = now_ns
            .saturating_sub(state.epoch_start_ns)
            .saturating_add(rtt_ns);
        cubic_window_scaled(state.w_max_scaled, state.k_ns, elapsed_ns)
    }
}

impl TcpReno {
    fn on_new_ack(&mut self, acknowledged_bytes: u64, acknowledgment: u64) {
        self.duplicate_acks = 0;
        match self.phase {
            TcpPhase::SlowStart => {
                self.cwnd_bytes = self
                    .cwnd_bytes
                    .saturating_add(self.mss_bytes.min(acknowledged_bytes));
                if self.cwnd_bytes >= self.ssthresh_bytes {
                    self.cwnd_bytes = self.ssthresh_bytes;
                    self.phase = TcpPhase::CongestionAvoidance;
                    self.ca_credit = 0;
                }
            }
            TcpPhase::CongestionAvoidance => {
                self.ca_credit = self.ca_credit.saturating_add(acknowledged_bytes);
                while self.ca_credit >= self.cwnd_bytes.max(1) {
                    self.ca_credit -= self.cwnd_bytes.max(1);
                    self.cwnd_bytes = self.cwnd_bytes.saturating_add(self.mss_bytes);
                }
            }
            TcpPhase::FastRecovery => {
                if self.recovery_high_sequence != 0 && acknowledgment >= self.recovery_high_sequence
                {
                    self.on_recovery_exit();
                } else {
                    // NewReno partial ACK: deflate to the threshold and retain one segment of
                    // recovery headroom for the next retransmission.
                    self.cwnd_bytes = self.ssthresh_bytes.saturating_add(self.mss_bytes);
                }
            }
        }
    }

    fn on_fast_retransmit(&mut self, flight_size_bytes: u64) {
        let minimum = self.mss_bytes.saturating_mul(2);
        self.ssthresh_bytes = (flight_size_bytes / 2).max(minimum);
        self.cwnd_bytes = self
            .ssthresh_bytes
            .saturating_add(self.mss_bytes.saturating_mul(3));
        self.phase = TcpPhase::FastRecovery;
        self.ca_credit = 0;
    }

    fn on_more_duplicate_ack(&mut self) {
        if self.phase == TcpPhase::FastRecovery {
            self.cwnd_bytes = self.cwnd_bytes.saturating_add(self.mss_bytes);
        }
    }

    fn on_recovery_exit(&mut self) {
        self.cwnd_bytes = self.ssthresh_bytes.max(self.mss_bytes);
        self.phase = TcpPhase::CongestionAvoidance;
        self.duplicate_acks = 0;
        self.recovery_high_sequence = 0;
        self.ca_credit = 0;
    }

    fn on_timeout(&mut self, flight_size_bytes: u64) {
        let minimum = self.mss_bytes.saturating_mul(2);
        self.ssthresh_bytes = (flight_size_bytes / 2).max(minimum);
        self.cwnd_bytes = self.mss_bytes;
        self.phase = TcpPhase::SlowStart;
        self.duplicate_acks = 0;
        self.recovery_high_sequence = 0;
        self.ca_credit = 0;
    }
}

impl TcpCubic {
    fn on_new_ack(
        &mut self,
        acknowledged_bytes: u64,
        now_ns: u64,
        rtt_sample_ns: u64,
        _flight_size_bytes: u64,
        acknowledgment: u64,
    ) {
        self.duplicate_acks = 0;
        self.update_srtt(rtt_sample_ns);
        match self.phase {
            TcpPhase::SlowStart => {
                let acknowledged_segments = acknowledged_bytes.div_ceil(self.mss_bytes);
                self.cwnd_scaled = self
                    .cwnd_scaled
                    .saturating_add(acknowledged_segments.saturating_mul(CUBIC_WINDOW_SCALE))
                    .min(CUBIC_MAX_WINDOW_SCALED);
                if self.cwnd_scaled >= self.ssthresh_scaled {
                    self.phase = TcpPhase::CongestionAvoidance;
                    self.epoch_start_ns = now_ns;
                    if self.w_max_scaled == 0 {
                        self.w_max_scaled = self.cwnd_scaled;
                        self.k_ns = 0;
                    }
                }
            }
            TcpPhase::CongestionAvoidance => self.update_cubic_window(now_ns),
            TcpPhase::FastRecovery => {
                if self.recovery_high_sequence != 0 && acknowledgment >= self.recovery_high_sequence
                {
                    self.on_recovery_exit();
                }
            }
        }
    }

    fn update_srtt(&mut self, sample_ns: u64) {
        let sample_ns = sample_ns.max(1);
        if self.srtt_ns == 0 {
            self.srtt_ns = sample_ns;
        } else {
            self.srtt_ns = ((u128::from(self.srtt_ns) * 7 + u128::from(sample_ns)) / 8)
                .min(u128::from(u64::MAX)) as u64;
        }
    }

    fn ensure_epoch(&mut self, now_ns: u64) {
        if self.epoch_start_ns == u64::MAX {
            self.epoch_start_ns = now_ns;
            if self.w_max_scaled == 0 {
                self.w_max_scaled = self.cwnd_scaled;
                self.k_ns = 0;
            } else {
                self.k_ns = cubic_k_ns(self.w_max_scaled);
            }
        }
    }

    fn update_cubic_window(&mut self, now_ns: u64) {
        self.ensure_epoch(now_ns);
        let elapsed_ns = now_ns.saturating_sub(self.epoch_start_ns);
        let cubic_now = cubic_window_scaled(self.w_max_scaled, self.k_ns, elapsed_ns);
        let friendly =
            tcp_friendly_window_scaled(self.w_max_scaled, elapsed_ns, self.srtt_ns.max(1));

        if cubic_now < friendly {
            self.cwnd_scaled = friendly.min(CUBIC_MAX_WINDOW_SCALED);
            return;
        }

        let target = cubic_window_scaled(
            self.w_max_scaled,
            self.k_ns,
            elapsed_ns.saturating_add(self.srtt_ns.max(1)),
        );
        self.cwnd_scaled = cubic_ack_step(self.cwnd_scaled, target)
            .clamp(CUBIC_WINDOW_SCALE, CUBIC_MAX_WINDOW_SCALED);
    }

    fn on_fast_retransmit(&mut self, flight_size_bytes: u64, now_ns: u64) {
        let previous_max = self.w_last_max_scaled;
        let current = self.cwnd_scaled;
        if previous_max > 0 && current < previous_max {
            self.w_last_max_scaled = current;
            self.w_max_scaled = mul_ratio(current, 17, 20);
        } else {
            self.w_last_max_scaled = current;
            self.w_max_scaled = current;
        }

        let flight_scaled = bytes_to_scaled(flight_size_bytes, self.mss_bytes);
        let reduced = mul_ratio(flight_scaled, 7, 10).max(CUBIC_WINDOW_SCALE);
        self.ssthresh_scaled = reduced.max(2 * CUBIC_WINDOW_SCALE);
        self.cwnd_scaled = reduced.min(CUBIC_MAX_WINDOW_SCALED);
        self.phase = TcpPhase::FastRecovery;
        self.epoch_start_ns = now_ns;
        self.k_ns = cubic_k_ns(self.w_max_scaled);
    }

    fn on_more_duplicate_ack(&mut self) {
        if self.phase == TcpPhase::FastRecovery {
            self.cwnd_scaled = self
                .cwnd_scaled
                .saturating_add(CUBIC_WINDOW_SCALE)
                .min(CUBIC_MAX_WINDOW_SCALED);
        }
    }

    fn on_recovery_exit(&mut self) {
        self.cwnd_scaled = self
            .ssthresh_scaled
            .clamp(CUBIC_WINDOW_SCALE, CUBIC_MAX_WINDOW_SCALED);
        self.phase = TcpPhase::CongestionAvoidance;
        self.duplicate_acks = 0;
        self.recovery_high_sequence = 0;
    }

    fn on_timeout(&mut self, flight_size_bytes: u64) {
        let flight_scaled = bytes_to_scaled(flight_size_bytes, self.mss_bytes);
        self.ssthresh_scaled =
            mul_ratio(flight_scaled, 7, 10).clamp(2 * CUBIC_WINDOW_SCALE, CUBIC_MAX_WINDOW_SCALED);
        self.cwnd_scaled = CUBIC_WINDOW_SCALE;
        self.w_max_scaled = 0;
        self.w_last_max_scaled = 0;
        self.epoch_start_ns = u64::MAX;
        self.k_ns = 0;
        self.phase = TcpPhase::SlowStart;
        self.duplicate_acks = 0;
        self.recovery_high_sequence = 0;
    }
}

/// Updates RFC 6298 integer estimators and returns a bounded integral-nanosecond RTO.
///
/// All fractional EWMA results round down. This is deterministic and deliberately differs from
/// the legacy source's binary floating-point EWMA at sub-nanosecond boundaries.
pub fn update_rto_ns(srtt_ns: &mut u64, rtt_var_ns: &mut u64, sample_ns: u64) -> u64 {
    let sample_ns = sample_ns.max(1);
    if *srtt_ns == 0 {
        *srtt_ns = sample_ns;
        *rtt_var_ns = sample_ns / 2;
    } else {
        let deviation = srtt_ns.abs_diff(sample_ns);
        *rtt_var_ns = ((u128::from(*rtt_var_ns) * 3 + u128::from(deviation)) / 4)
            .min(u128::from(u64::MAX)) as u64;
        *srtt_ns = ((u128::from(*srtt_ns) * 7 + u128::from(sample_ns)) / 8)
            .min(u128::from(u64::MAX)) as u64;
    }
    srtt_ns
        .saturating_add(rtt_var_ns.saturating_mul(4).max(RTO_CLOCK_GRANULARITY_NS))
        .clamp(MIN_RTO_NS, MAX_RTO_NS)
}

const fn nonzero_mss(mss_bytes: u64) -> u64 {
    if mss_bytes == 0 { 1 } else { mss_bytes }
}

const fn bytes_to_scaled_const(bytes: u64, mss_bytes: u64) -> u64 {
    let product = (bytes as u128) * (CUBIC_WINDOW_SCALE as u128);
    let result = product / (nonzero_mss(mss_bytes) as u128);
    if result > u64::MAX as u128 {
        u64::MAX
    } else {
        result as u64
    }
}

const fn scaled_to_bytes_const(scaled: u64, mss_bytes: u64) -> u64 {
    let product = (scaled as u128) * (nonzero_mss(mss_bytes) as u128);
    let result = product / (CUBIC_WINDOW_SCALE as u128);
    if result > u64::MAX as u128 {
        u64::MAX
    } else {
        result as u64
    }
}

fn bytes_to_scaled(bytes: u64, mss_bytes: u64) -> u64 {
    bytes_to_scaled_const(bytes, mss_bytes).min(CUBIC_MAX_WINDOW_SCALED)
}

fn mul_ratio(value: u64, numerator: u64, denominator: u64) -> u64 {
    ((u128::from(value) * u128::from(numerator)) / u128::from(denominator))
        .min(u128::from(u64::MAX)) as u64
}

fn cubic_k_ns(w_max_scaled: u64) -> u64 {
    if w_max_scaled == 0 {
        return 0;
    }
    // K^3 = Wmax * (1-beta) / C seconds^3 = Wmax * 3/4 seconds^3.
    // Wmax is scaled by S and K is in ns, so the exact radicand is
    // Wmax_scaled * 3 * 10^27 / (4*S) = Wmax_scaled * 3 * 10^18 / 4.
    let radicand = (BigUint::from(w_max_scaled)
        * BigUint::from(3_u8)
        * BigUint::from(1_000_000_000_000_000_000_u64))
        / BigUint::from(4_u8);
    floor_cube_root(&radicand)
}

fn floor_cube_root(value: &BigUint) -> u64 {
    if *value == BigUint::from(0_u8) {
        return 0;
    }
    let mut low = 0_u64;
    let mut high = 1_u64;
    while cube(high) <= *value && high <= u64::MAX / 2 {
        high *= 2;
    }
    if cube(high) <= *value {
        return u64::MAX;
    }
    while low + 1 < high {
        let middle = low + (high - low) / 2;
        if cube(middle) <= *value {
            low = middle;
        } else {
            high = middle;
        }
    }
    low
}

fn cube(value: u64) -> BigUint {
    let value = BigUint::from(value);
    &value * &value * value
}

fn cubic_window_scaled(w_max_scaled: u64, k_ns: u64, elapsed_ns: u64) -> u64 {
    let negative = elapsed_ns < k_ns;
    let distance = elapsed_ns.abs_diff(k_ns);
    // C*(d_ns/1e9)^3*S, with C=2/5 and S=1e9, is
    // 2*d_ns^3/(5*10^18) scaled segments. BigUint is transient only.
    let distance = BigUint::from(distance);
    let magnitude =
        distance.pow(3_u32) * BigUint::from(2_u8) / BigUint::from(5_000_000_000_000_000_000_u64);
    let magnitude = big_to_u64_saturating(&magnitude);
    if negative {
        w_max_scaled
            .saturating_sub(magnitude)
            .max(CUBIC_WINDOW_SCALE)
    } else {
        w_max_scaled
            .saturating_add(magnitude)
            .clamp(CUBIC_WINDOW_SCALE, CUBIC_MAX_WINDOW_SCALED)
    }
}

fn tcp_friendly_window_scaled(w_max_scaled: u64, elapsed_ns: u64, rtt_ns: u64) -> u64 {
    // W_est = beta*Wmax + 3*(1-beta)/(1+beta) * t/RTT = 7/10*Wmax + 9/17*t/RTT.
    let base = mul_ratio(w_max_scaled, 7, 10);
    let growth =
        (BigUint::from(CUBIC_WINDOW_SCALE) * BigUint::from(9_u8) * BigUint::from(elapsed_ns))
            / (BigUint::from(17_u8) * BigUint::from(rtt_ns.max(1)));
    base.saturating_add(big_to_u64_saturating(&growth))
        .clamp(CUBIC_WINDOW_SCALE, CUBIC_MAX_WINDOW_SCALED)
}

fn cubic_ack_step(cwnd_scaled: u64, target_scaled: u64) -> u64 {
    if target_scaled >= cwnd_scaled {
        let increment = (u128::from(target_scaled - cwnd_scaled) * u128::from(CUBIC_WINDOW_SCALE)
            / u128::from(cwnd_scaled.max(CUBIC_WINDOW_SCALE)))
        .min(u128::from(u64::MAX)) as u64;
        cwnd_scaled.saturating_add(increment)
    } else {
        let decrement = (u128::from(cwnd_scaled - target_scaled) * u128::from(CUBIC_WINDOW_SCALE)
            / u128::from(cwnd_scaled.max(CUBIC_WINDOW_SCALE)))
        .min(u128::from(u64::MAX)) as u64;
        cwnd_scaled.saturating_sub(decrement)
    }
}

fn big_to_u64_saturating(value: &BigUint) -> u64 {
    let digits = value.to_u64_digits();
    match digits.as_slice() {
        [] => 0,
        [only] => *only,
        _ => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MSS: u64 = 512;

    #[test]
    fn exact_cubic_k_is_floored() {
        let k = cubic_k_ns(100 * CUBIC_WINDOW_SCALE);
        let radicand = BigUint::from(75_u8) * BigUint::from(NANOS_PER_SECOND).pow(3_u32);
        assert!(cube(k) <= radicand);
        assert!(cube(k + 1) > radicand);
    }

    #[test]
    fn required_slow_start_and_cubic_fixture_api_is_exact() {
        let mut reno = TcpCongestionControl::reno(MSS);
        reno.on_new_ack(MSS, 100, 100, 2 * MSS, 2 * MSS);
        reno.on_new_ack(MSS, 100, 100, MSS, 2 * MSS);
        assert_eq!(reno.phase(), TcpPhase::SlowStart);
        assert_eq!(reno.cwnd_bytes(MSS), 4 * MSS);

        let mut cubic = TcpCongestionControl::cubic(MSS);
        cubic.force_cubic_epoch_for_test(100 * CUBIC_WINDOW_SCALE, 0);
        let first = cubic.cubic_target_for_test(1_000_000_000, 100_000_000);
        let second = cubic.cubic_target_for_test(2_000_000_000, 100_000_000);
        assert!(first > 0);
        assert!(second > first);
    }

    #[test]
    fn reno_fast_recovery_and_timeout_are_exact() {
        let mut control = TcpCongestionControl::reno(MSS);
        assert!(!control.on_duplicate_ack(8 * MSS, 1));
        assert!(!control.on_duplicate_ack(8 * MSS, 2));
        assert!(control.on_duplicate_ack(8 * MSS, 3));
        assert_eq!(control.phase(), TcpPhase::FastRecovery);
        assert_eq!(control.cwnd_bytes(MSS), 7 * MSS);
        control.on_timeout(8 * MSS, 4);
        assert_eq!(control.phase(), TcpPhase::SlowStart);
        assert_eq!(control.cwnd_bytes(MSS), MSS);
    }

    #[test]
    fn reno_ca_adds_exactly_one_mss_per_acknowledged_window() {
        let mut control = TcpCongestionControl::reno(MSS);
        let TcpCongestionControl::Reno(ref mut state) = control else {
            unreachable!();
        };
        state.phase = TcpPhase::CongestionAvoidance;
        state.cwnd_bytes = 4 * MSS;
        for acknowledgment in 1..=4 {
            control.on_new_ack(MSS, 0, 1, 4 * MSS, acknowledgment * MSS);
        }
        assert_eq!(control.cwnd_bytes(MSS), 5 * MSS);
        assert_eq!(control.ca_credit(), 0);
    }

    #[test]
    fn rto_uses_integer_rfc_6298_bounds() {
        let mut srtt = 0;
        let mut variation = 0;
        assert_eq!(
            update_rto_ns(&mut srtt, &mut variation, 100_000_000),
            MIN_RTO_NS
        );
        assert_eq!(srtt, 100_000_000);
        assert_eq!(variation, 50_000_000);
    }
}

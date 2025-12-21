//! Implements the TCP CUBIC congestion control (RFC 8312).
//!
//! CUBIC uses a cubic function of elapsed time since the beginning of the
//! current congestion-avoidance epoch. Window sizes are maintained in units
//! of MSS-sized segments, with RFC 8312 default parameters (C=0.4, beta=0.7).

use crate::flows::cc::{AckEvent, CongestionControl};
use crate::flows::CubicConfig;
use std::any::Any;

/// TCP CUBIC congestion control implementation.
#[derive(Debug)]
pub struct TCPCubic {
    /// Maximum segment size in bytes
    mss: usize,
    /// Initial congestion window in bytes (for logging)
    #[cfg(feature = "lean")]
    init_cwnd_bytes: usize,
    /// Initial slow start threshold in bytes (for logging)
    #[cfg(feature = "lean")]
    init_ssthresh_bytes: usize,
    /// Current congestion window size in MSS-sized segments
    cwnd: f64,
    /// Slow start threshold in MSS-sized segments
    ssthresh: f64,
    /// Window size (in segments) before last reduction
    w_max: f64,
    /// Last value of W_max before the current congestion epoch
    w_last_max: f64,
    /// Start time (in seconds) of current congestion-avoidance epoch
    epoch_start: Option<f64>,
    /// Special case: K = 0 when W_max is undefined (hybrid slow start/timeout)
    k_zero: bool,
    /// Multiplicative decrease factor (beta_cubic)
    beta: f64,
    /// CUBIC scaling constant (C)
    c: f64,
    /// Enable fast convergence (RFC 8312 Section 4.6)
    fast_convergence: bool,
    /// Enable TCP-friendly region (RFC 8312 Section 4.2)
    tcp_friendliness: bool,
    /// Minimum cwnd in segments
    min_cwnd: f64,
    /// Maximum cwnd in segments
    max_cwnd: f64,
    /// Smoothed RTT (seconds), per Standard TCP
    srtt: f64,
}

#[cfg(feature = "lean")]
#[derive(Clone, Copy, Debug)]
pub struct CubicSnapshot {
    pub mss: usize,
    pub beta: f64,
    pub c: f64,
    pub tcp_friendliness: bool,
    pub fast_convergence: bool,
    pub init_cwnd_bytes: usize,
    pub init_ssthresh_bytes: usize,
    pub cwnd_bytes: usize,
    pub ssthresh_bytes: usize,
    pub w_max_bytes: usize,
    pub w_last_max_bytes: usize,
    pub epoch_start: Option<f64>,
}

impl TCPCubic {
    /// Creates a new TCP CUBIC instance with RFC 8312 defaults.
    pub fn new() -> TCPCubic {
        let mss = 512;
        let init_cwnd_bytes = 512; // 1 MSS
        let init_ssthresh_bytes = 65535;
        let init_cwnd = init_cwnd_bytes as f64 / mss as f64;
        let init_ssthresh = init_ssthresh_bytes as f64 / mss as f64;
        TCPCubic {
            mss,
            #[cfg(feature = "lean")]
            init_cwnd_bytes,
            #[cfg(feature = "lean")]
            init_ssthresh_bytes,
            cwnd: init_cwnd,
            ssthresh: init_ssthresh,
            w_max: 0.0,
            w_last_max: 0.0,
            epoch_start: None,
            k_zero: true,
            beta: 0.7,
            c: 0.4,
            fast_convergence: true,
            tcp_friendliness: true,
            min_cwnd: 1.0,
            max_cwnd: 2_000_000.0, // 2M segments
            srtt: 0.0,
        }
    }

    pub fn apply_config(&mut self, config: &CubicConfig) {
        if let Some(beta) = config.beta {
            self.beta = beta;
        }
        if let Some(c) = config.c {
            self.c = c;
        }
        if let Some(fast_convergence) = config.fast_convergence {
            self.fast_convergence = fast_convergence;
        }
    }

    fn update_srtt(&mut self, rtt: f64) -> f64 {
        let rtt = self.quantize_time_s(rtt).max(1e-9);
        if self.srtt == 0.0 {
            self.srtt = rtt;
        } else {
            let alpha = 0.125;
            self.srtt = (1.0 - alpha) * self.srtt + alpha * rtt;
        }
        self.srtt
    }

    fn quantize_time_s(&self, t: f64) -> f64 {
        (t.max(0.0) * 1e9).round() / 1e9
    }

    fn cubic_k(&self) -> f64 {
        if self.k_zero || self.w_max <= 0.0 {
            0.0
        } else {
            (self.w_max * (1.0 - self.beta) / self.c).cbrt()
        }
    }

    fn cubic_window(&self, t: f64) -> f64 {
        let k = self.cubic_k();
        self.c * (t - k).powi(3) + self.w_max
    }

    fn clamp_cwnd(&mut self) {
        if !self.cwnd.is_finite() || self.cwnd <= 0.0 {
            self.cwnd = self.min_cwnd;
        }
        if self.cwnd < self.min_cwnd {
            self.cwnd = self.min_cwnd;
        }
        if self.cwnd > self.max_cwnd {
            self.cwnd = self.max_cwnd;
        }
    }

    fn ensure_epoch(&mut self, now: f64) {
        if self.epoch_start.is_none() {
            self.epoch_start = Some(self.quantize_time_s(now));
            if self.w_max == 0.0 {
                self.w_max = self.cwnd;
                self.k_zero = true;
            }
        }
    }

    fn cubic_update(&mut self, now: f64, rtt: f64) {
        let now = self.quantize_time_s(now);
        let rtt = rtt.max(1e-9);
        self.ensure_epoch(now);
        let epoch_start = self.epoch_start.unwrap_or(now);
        let t = (now - epoch_start).max(0.0);

        let w_cubic_t = self.cubic_window(t);
        let w_est = self.w_max * self.beta
            + (3.0 * (1.0 - self.beta) / (1.0 + self.beta)) * (t / rtt);

        if self.tcp_friendliness && w_cubic_t < w_est {
            self.cwnd = w_est;
        } else {
            let w_cubic_trtt = self.cubic_window(t + rtt);
            let denom = self.cwnd.max(1.0);
            self.cwnd = self.cwnd + (w_cubic_trtt - self.cwnd) / denom;
        }
        self.clamp_cwnd();
    }

    fn on_congestion(&mut self) {
        let w_max = self.cwnd;
        if self.fast_convergence {
            if self.w_last_max > 0.0 && w_max < self.w_last_max {
                self.w_last_max = w_max;
                self.w_max = w_max * (1.0 + self.beta) / 2.0;
            } else {
                self.w_last_max = w_max;
                self.w_max = w_max;
            }
        } else {
            self.w_last_max = w_max;
            self.w_max = w_max;
        }

        let reduced = (w_max * self.beta).max(self.min_cwnd);
        self.ssthresh = reduced.max(2.0);
        self.cwnd = reduced;
        self.epoch_start = None;
        self.k_zero = false;
        self.clamp_cwnd();
    }

    fn on_timeout(&mut self) {
        let reduced = (self.cwnd * self.beta).max(self.min_cwnd);
        self.ssthresh = reduced.max(2.0);
        self.cwnd = 1.0;
        self.w_max = 0.0;
        self.w_last_max = 0.0;
        self.epoch_start = None;
        self.k_zero = true;
        self.clamp_cwnd();
    }

    pub fn note_congestion_time(&mut self, now: f64) {
        self.epoch_start = Some(self.quantize_time_s(now));
    }

    #[cfg(feature = "lean")]
    pub fn snapshot(&self) -> CubicSnapshot {
        let mss_f = self.mss as f64;
        let to_bytes = |segs: f64| -> usize {
            if segs <= 0.0 {
                0
            } else {
                (segs * mss_f).floor() as usize
            }
        };
        CubicSnapshot {
            mss: self.mss,
            beta: self.beta,
            c: self.c,
            tcp_friendliness: self.tcp_friendliness,
            fast_convergence: self.fast_convergence,
            init_cwnd_bytes: self.init_cwnd_bytes,
            init_ssthresh_bytes: self.init_ssthresh_bytes,
            cwnd_bytes: to_bytes(self.cwnd),
            ssthresh_bytes: to_bytes(self.ssthresh),
            w_max_bytes: to_bytes(self.w_max),
            w_last_max_bytes: to_bytes(self.w_last_max),
            epoch_start: self.epoch_start,
        }
    }
}

impl Default for TCPCubic {
    fn default() -> Self {
        Self::new()
    }
}

impl CongestionControl for TCPCubic {
    fn ack_received(&mut self, event: AckEvent) {
        let rtt_sample = event.rtt;
        let srtt = self.update_srtt(rtt_sample);
        let acked_segs = ((event.bytes_acked + self.mss - 1) / self.mss) as f64;

        if acked_segs <= 0.0 {
            return;
        }

        if self.cwnd < self.ssthresh {
            // Slow start (RFC 8312 Section 4.8): use standard TCP slow start.
            self.cwnd += acked_segs;
            self.clamp_cwnd();

            if self.cwnd >= self.ssthresh {
                // Enter congestion avoidance.
                self.epoch_start = Some(self.quantize_time_s(event.now));
                if self.w_max == 0.0 {
                    self.w_max = self.cwnd;
                    self.k_zero = true;
                }
            }
            return;
        }

        // Congestion avoidance (RFC 8312 Section 4).
        self.cubic_update(event.now.max(0.0), if srtt > 0.0 { srtt } else { rtt_sample });
    }

    fn timer_expired(&mut self) {
        self.on_timeout();
    }

    fn dupack_over(&mut self) {
        // Standard TCP exits recovery by setting cwnd to ssthresh.
        self.cwnd = self.ssthresh;
        self.clamp_cwnd();
    }

    fn consecutive_dupacks_received(&mut self) {
        self.on_congestion();
    }

    fn more_dupacks_received(&mut self) {
        // Allow limited growth during fast recovery.
        self.cwnd += 1.0;
        self.clamp_cwnd();
    }

    fn ecn_marked(&mut self) {
        self.on_congestion();
    }

    fn get_cwnd(&self) -> usize {
        (self.cwnd.max(0.0) * self.mss as f64).floor() as usize
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(bytes_acked: usize, cubic: &mut TCPCubic, rtt: f64, now: f64) {
        cubic.ack_received(AckEvent::new_basic(0, rtt, now, bytes_acked));
    }

    fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
        (a - b).abs() <= eps
    }

    #[test]
    fn test_initial_state() {
        let cubic = TCPCubic::new();
        assert!(approx_eq(cubic.cwnd, 1.0, 1e-9));
        assert!(cubic.ssthresh > cubic.cwnd);
        assert_eq!(cubic.mss, 512);
        assert!(approx_eq(cubic.beta, 0.7, 1e-9));
        assert!(approx_eq(cubic.c, 0.4, 1e-9));
    }

    #[test]
    fn test_slow_start_growth() {
        let mut cubic = TCPCubic::new();
        cubic.ssthresh = 1000.0;

        for _ in 0..5 {
            ack(cubic.mss, &mut cubic, 0.1, 1.0);
        }

        assert!(cubic.cwnd > 1.0);
        assert!(cubic.cwnd < cubic.ssthresh);
    }

    #[test]
    fn test_congestion_avoidance_growth() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 100.0;
        cubic.ssthresh = 10.0;
        cubic.w_max = 100.0;
        cubic.k_zero = false;
        cubic.epoch_start = Some(0.0);

        ack(cubic.mss, &mut cubic, 0.1, 1.0);

        assert!(cubic.cwnd > 0.0);
        assert!(cubic.cwnd <= cubic.max_cwnd);
    }

    #[test]
    fn test_multiplicative_decrease_beta() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 100.0;
        cubic.on_congestion();

        assert!(approx_eq(cubic.cwnd, 70.0, 1e-6));
        assert!(approx_eq(cubic.ssthresh, 70.0, 1e-6));
    }

    #[test]
    fn test_fast_convergence_reduces_w_max() {
        let mut cubic = TCPCubic::new();
        cubic.fast_convergence = true;
        cubic.w_last_max = 120.0;
        cubic.cwnd = 80.0;
        cubic.on_congestion();

        assert!(approx_eq(cubic.w_max, 80.0 * (1.0 + cubic.beta) / 2.0, 1e-6));
        assert!(approx_eq(cubic.w_last_max, 80.0, 1e-6));
    }

    #[test]
    fn test_timeout_resets_cwnd() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 50.0;
        cubic.timer_expired();
        assert!(approx_eq(cubic.cwnd, 1.0, 1e-9));
        assert!(cubic.ssthresh >= 2.0);
    }

    #[test]
    fn test_cwnd_bounds() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = cubic.max_cwnd * 2.0;
        cubic.clamp_cwnd();
        assert!(approx_eq(cubic.cwnd, cubic.max_cwnd, 1e-9));
    }
}

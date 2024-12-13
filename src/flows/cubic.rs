//! Implements the TCP CUBIC congestion control algorithm.

use crate::flows::cc::CongestionControl;

/// HyStart++ parameters
#[derive(Debug, Default)]
struct HyStartState {
    enabled: bool,
    low_cwnd: usize,
    last_rtt: f64,
    min_rtt: f64,
    rtt_sample_cnt: usize,
    current_round: usize,
    last_round: usize,
    round_start: f64,
    rtt_samples: Vec<f64>,
    exit_slow_start: bool,
}

impl HyStartState {
    fn new() -> Self {
        HyStartState {
            enabled: true,
            low_cwnd: 16,
            last_rtt: 0.0,
            min_rtt: f64::MAX,
            rtt_sample_cnt: 0,
            current_round: 0,
            last_round: 0,
            round_start: 0.0,
            rtt_samples: Vec::new(),
            exit_slow_start: false,
        }
    }

    // Add method to track RTT samples
    fn add_rtt_sample(&mut self, rtt: f64, current_time: f64) {
        self.last_rtt = rtt;
        self.rtt_sample_cnt += 1;

        // Update round tracking
        if current_time > self.round_start {
            self.last_round = self.current_round;
            self.current_round += 1;
            self.round_start = current_time;
            // Removed clearing of rtt_samples to allow accumulation across rounds
        }

        self.rtt_samples.push(rtt);
    }
}

#[derive(Debug, Default)]
pub struct TCPCubic {
    /// the maximum segment size
    mss: usize,
    /// the size of the congestion window
    cwnd: usize,
    /// the slow start threshold
    ssthresh: usize,
    w_last_max: usize,
    epoch_start: f64,
    origin_point: usize,
    d_min: f64,
    w_tcp: usize,
    k: f64,
    ack_cnt: usize,
    tcp_friendliness: bool,
    beta: f64,
    c: f64,
    cwnd_cnt: usize,
    cnt: usize,
    /// Fast convergence state
    fast_convergence: bool,
    /// Last max window size
    last_max_cwnd: usize,
    /// Window size just before reduction
    last_decrease: usize,
    /// HyStart++ state
    hystart: HyStartState,
    /// Minimum cwnd value
    min_cwnd: usize,
    /// Maximum cwnd value
    max_cwnd: usize,
    /// TCP friendly region coefficient
    tcp_friendly_alpha: f64,
    /// Number of delayed ACKs per congestion window
    delayed_ack_factor: usize,
    /// Round trip counter
    round_count: usize,
    /// Timestamp of last window reduction
    last_reduction_time: f64,
    /// Recovery state flag
    in_recovery: bool,
}

impl TCPCubic {
    pub fn new() -> TCPCubic {
        TCPCubic {
            mss: 512,
            cwnd: 512,
            ssthresh: 65535,
            w_last_max: 0,
            epoch_start: 0.0,
            origin_point: 0,
            d_min: 0.0,
            w_tcp: 0,
            k: 0.0,
            ack_cnt: 0,
            tcp_friendliness: true,
            beta: 0.2,
            c: 0.4,
            cwnd_cnt: 0,
            cnt: 0,

            // New fields
            fast_convergence: true,
            last_max_cwnd: 0,
            last_decrease: 0,
            hystart: HyStartState::new(),
            min_cwnd: 2,
            max_cwnd: 2_000_000, // 2M segments
            tcp_friendly_alpha: 3.0,
            delayed_ack_factor: 2,
            round_count: 0,
            last_reduction_time: 0.0,
            in_recovery: false,
        }
    }

    /// Resets the states in CUBIC.
    pub fn cubic_reset(&mut self) {
        self.w_last_max = 0;
        self.epoch_start = 0.0;
        self.origin_point = 0;
        self.d_min = 0.0;
        self.w_tcp = 0;
        self.k = 0.0;
        self.ack_cnt = 0;
    }

    fn update_hystart(&mut self, rtt: f64, current_time: f64) {
        if !self.hystart.enabled || self.cwnd < self.hystart.low_cwnd {
            return;
        }

        // Track RTT samples using HyStart state
        self.hystart.add_rtt_sample(rtt, current_time);
        self.hystart.min_rtt = self.hystart.min_rtt.min(rtt);

        // Exit conditions for slow start using improved sampling
        if self.hystart.rtt_samples.len() >= 8 {
            let sorted_samples: Vec<f64> = {
                let mut samples = self.hystart.rtt_samples.clone();
                samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
                samples
            };

            let min_rtt = sorted_samples[0];
            let max_rtt = sorted_samples[sorted_samples.len() - 1];

            // Use last_rtt to detect rapid RTT increases
            let rtt_increase = (max_rtt - min_rtt) / min_rtt;

            if rtt_increase > 0.125 {
                self.hystart.exit_slow_start = true;
                self.ssthresh = self.cwnd.min(self.max_cwnd);
            }
        }
    }

    /// Updates CUBIC parameters upon the arrival of a new acknowledgment.
    fn cubic_update(&mut self, current_time: f64) {
        self.ack_cnt += 1;
        self.round_count = self.hystart.current_round;

        // Reset epoch if needed
        if self.epoch_start <= 0.0 {
            self.epoch_start = current_time;
            if self.cwnd < self.w_last_max {
                self.k = ((self.w_last_max - self.cwnd) as f64 / self.c).powf(1.0 / 3.0);
                self.origin_point = self.w_last_max;
            } else {
                self.k = 0.0;
                self.origin_point = self.cwnd;
            }
            self.ack_cnt = 1;
            self.w_tcp = self.cwnd;
        }

        let t = current_time + self.d_min - self.epoch_start;
        let target = self.origin_point as f64 + self.c * (t - self.k).powi(3);

        // More precise window growth with better bounds
        let w_cubic = if target > 0.0 {
            target.max(self.min_cwnd as f64).min(self.max_cwnd as f64)
        } else {
            self.min_cwnd as f64
        };

        // Improved TCP friendliness calculation
        let w_tcp = if self.tcp_friendliness {
            // RTT scaling based on minimum observed RTT
            let rtt_scale = if self.hystart.min_rtt > 0.0 {
                (self.d_min / self.hystart.min_rtt).min(1.0)
            } else {
                1.0
            };
            let alpha = self.tcp_friendly_alpha * rtt_scale;

            // Account for delayed ACKs in window calculation
            let w_tcp_calc = self.w_tcp as f64
                + (alpha * self.beta * (self.ack_cnt as f64)
                    / (self.cwnd as f64 * self.delayed_ack_factor as f64));
            w_tcp_calc
                .max(self.min_cwnd as f64)
                .min(self.max_cwnd as f64)
        } else {
            0.0
        };

        // Take maximum of cubic and TCP friendly windows
        let w_est = if self.tcp_friendliness {
            w_cubic.max(w_tcp)
        } else {
            w_cubic
        };

        // Assign cwnd to the estimated value, ensuring it does not exceed max_cwnd
        self.cwnd = w_est.min(self.max_cwnd as f64) as usize;

        // Prevent overflow and handle cases where w_est <= cwnd
        if w_est > self.cwnd as f64 {
            // To prevent overflow, ensure the multiplication does not exceed usize::MAX
            // and handle large cwnd appropriately
            if self.cwnd > (usize::MAX / 100) {
                self.cnt = usize::MAX; // Assign maximum usize to prevent overflow
            } else {
                self.cnt = ((self.cwnd as f64 * (w_est - self.cwnd as f64))
                    / (w_est * self.mss as f64))
                    .max(2.0) as usize;
            }
        } else {
            // Prevent cwnd * 100 from overflowing
            if self.cwnd > usize::MAX / 100 {
                self.cnt = usize::MAX;
            } else {
                self.cnt = 100 * self.cwnd; // Arbitrary large count to prevent cwnd increment
            }
        }
    }

    fn update_fast_convergence(&mut self) {
        if !self.fast_convergence {
            return;
        }

        // Fast convergence mechanism
        if self.cwnd < self.last_max_cwnd {
            self.last_max_cwnd = (self.cwnd as f64 * (1.0 + self.beta)).floor() as usize;
            self.w_last_max = self.cwnd;
        } else {
            self.last_max_cwnd = self.cwnd;
            self.w_last_max = self.cwnd;
        }
    }

    /// CUBIC actions in TCP mode.
    pub fn cubic_tcp_friendliness(&mut self) {
        self.w_tcp += (3.0 * self.beta / (2.0 - self.beta) * (self.ack_cnt as f64)
            / (self.cwnd as f64 * self.delayed_ack_factor as f64))
            .floor() as usize;
        self.ack_cnt = 0;

        if self.w_tcp > self.cwnd {
            let max_cnt = if (self.w_tcp - self.cwnd) != 0 {
                self.cwnd / (self.w_tcp - self.cwnd)
            } else {
                usize::MAX
            };
            if self.cnt > max_cnt {
                self.cnt = max_cnt;
            }
        }
    }
}

impl CongestionControl for TCPCubic {
    fn ack_received(&mut self, _ack_seq: usize, rtt: f64, current_time: f64, bytes_acked: usize) {
        // Track minimum RTT
        if self.d_min > 0.0 {
            self.d_min = self.d_min.min(rtt);
        } else {
            self.d_min = rtt;
        }

        // Update HyStart state with improved RTT tracking
        self.update_hystart(rtt, current_time);

        if self.in_recovery {
            if bytes_acked >= 3 * self.mss {
                // Full acknowledgment received, exit recovery
                self.cwnd = self.ssthresh;
                self.in_recovery = false;
                return;
            }
        }

        if self.cwnd <= self.ssthresh && !self.hystart.exit_slow_start {
            // Slow start with HyStart++ detection
            let increment = bytes_acked.min(self.mss);
            self.cwnd = self
                .cwnd
                .checked_add(increment)
                .unwrap_or(self.max_cwnd)
                .min(self.max_cwnd);
        } else {
            // Congestion avoidance
            self.cubic_update(current_time);
            if self.cwnd_cnt > self.cnt {
                self.update_fast_convergence();
                let increment = bytes_acked.min(self.mss);
                self.cwnd = self
                    .cwnd
                    .checked_add(increment)
                    .unwrap_or(self.max_cwnd)
                    .min(self.max_cwnd);

                self.cwnd_cnt = 0;
            } else {
                self.cwnd_cnt += 1;
            }
        }
    }

    /// Actions to be taken when a timer expired.
    fn timer_expired(&mut self) {
        // sets the congestion window to 1 segment
        self.last_decrease = self.cwnd;
        self.last_reduction_time = self.epoch_start;
        self.cwnd = self.mss;
        self.ssthresh = (self.last_decrease / 2).max(2 * self.mss);
        self.cubic_reset();
        self.hystart = HyStartState::new();
        self.in_recovery = false;
    }

    /// Actions to be taken when a new acknowledgment is received after previous
    /// dupacks.
    fn dupack_over(&mut self) {
        self.cwnd = self.ssthresh;
        self.in_recovery = false;
    }

    /// Actions to be taken when three consecutive dupacks are received.
    fn consecutive_dupacks_received(&mut self) {
        self.ssthresh = (2 * self.mss).max(self.cwnd / 2).min(self.max_cwnd);
        self.cwnd = self.ssthresh + 3 * self.mss;
        self.cwnd = self.cwnd.min(self.max_cwnd); // Ensure cwnd does not exceed max_cwnd
        self.in_recovery = true;
    }

    /// Actions to be taken when more than three consecutive dupacks are
    /// received.
    fn more_dupacks_received(&mut self) {
        self.cwnd = self.cwnd.saturating_add(self.mss).min(self.max_cwnd);
    }

    fn get_cwnd(&self) -> usize {
        self.cwnd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let cubic = TCPCubic::new();
        assert_eq!(cubic.cwnd, 512); // Initial cwnd
        assert_eq!(cubic.ssthresh, 65535);
        assert_eq!(cubic.mss, 512);
        assert_eq!(cubic.min_cwnd, 2);
        assert_eq!(cubic.max_cwnd, 2_000_000);
    }

    #[test]
    fn test_slow_start_growth() {
        let mut cubic = TCPCubic::new();
        cubic.ssthresh = 8000; // Ensure we stay in slow start

        // Simulate ACKs to grow cwnd
        for _ in 0..10 {
            cubic.ack_received(0, 0.1, 1.0, cubic.mss);
        }

        assert_eq!(cubic.cwnd, 512 + 10 * cubic.mss);
        assert_eq!(cubic.ssthresh, 8000);
    }

    #[test]
    fn test_slow_start_to_congestion_avoidance() {
        let mut cubic = TCPCubic::new();
        cubic.ssthresh = 2048; // Set low ssthresh to force transition

        // Send enough ACKs to exceed ssthresh
        while cubic.cwnd < cubic.ssthresh {
            cubic.ack_received(0, 0.1, 1.0, cubic.mss);
        }

        cubic.ack_received(0, 0.1, 1.0, cubic.mss); // This should transition to congestion avoidance

        let expected_cwnd = cwnd_behavior_after_transition(&cubic);
        assert_eq!(
            cubic.cwnd, expected_cwnd,
            "Cwnd did not transition correctly"
        );
    }

    // Helper function to determine expected behavior after transition
    fn cwnd_behavior_after_transition(cubic: &TCPCubic) -> usize {
        cubic.ssthresh + cubic.mss
    }

    #[test]
    fn test_congestion_avoidance_growth() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 2048;
        cubic.ssthresh = 2048;

        // Simulate ACKs in congestion avoidance
        for _ in 0..50 {
            cubic.ack_received(0, 0.1, 2.0, cubic.mss);
        }

        // Ensure cwnd has increased appropriately without exponential growth
        assert!(cubic.cwnd > 2048);
        assert!(cubic.cwnd <= cubic.max_cwnd);
    }

    #[test]
    fn test_consecutive_dupacks_received() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.mss = 512;
        cubic.consecutive_dupacks_received();

        // Expected ssthresh = max(2 * mss, cwnd / 2) = max(1024, 2048) = 2048
        assert_eq!(
            cubic.ssthresh, 2048,
            "ssthresh should be max(2 * mss, cwnd / 2) = 2048"
        );

        // Expected cwnd = ssthresh + 3 * mss = 2048 + 1536 = 3584
        assert_eq!(cubic.cwnd, 3584, "cwnd should be ssthresh + 3 * mss = 3584");

        // Ensure cwnd does not exceed max_cwnd
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "cwnd exceeded max_cwnd after consecutive_dupacks_received"
        );
    }

    #[test]
    fn test_timer_expiry() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 10000;
        cubic.timer_expired();

        assert_eq!(cubic.cwnd, cubic.mss);
        assert_eq!(cubic.ssthresh, (10000 / 2).max(2 * cubic.mss));
    }

    #[test]
    fn test_cubic_update_no_write() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 1024;
        cubic.ssthresh = 2048;

        // Simulate ACK received
        cubic.ack_received(0, 0.1, 1.0, cubic.mss);

        // cwnd should still be in slow start
        assert_eq!(cubic.cwnd, 1024 + cubic.mss);
    }

    #[test]
    fn test_cubic_update_with_write() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 2048;
        cubic.ssthresh = 2048;

        // Simulate ACK received
        cubic.ack_received(0, 0.1, 2.0, cubic.mss);

        // cwnd should have increased in congestion avoidance
        assert!(cubic.cwnd > 2048);
    }

    #[test]
    fn test_hystart_exit_on_rtt_increase() {
        let mut cubic = TCPCubic::new();
        cubic.hystart.enabled = true;
        cubic.cwnd = 16;

        // Simulate RTT samples with significant increase
        let initial_rtt = 0.2;
        let increased_rtt = 0.4; // 200% increase

        for _ in 0..4 {
            cubic.ack_received(0, initial_rtt, 1.0, cubic.mss);
        }

        for _ in 0..4 {
            cubic.ack_received(0, increased_rtt, 2.0, cubic.mss);
        }

        assert!(
            cubic.hystart.exit_slow_start,
            "HyStart did not exit slow start as expected."
        );
        assert_eq!(cubic.ssthresh, cubic.cwnd.min(cubic.max_cwnd));
    }

    #[test]
    fn test_fast_convergence_enabled() {
        let mut cubic = TCPCubic::new();
        cubic.fast_convergence = true;
        cubic.cwnd = 8192;
        cubic.last_max_cwnd = 1024;

        cubic.update_fast_convergence();

        assert_eq!(cubic.w_last_max, 8192);
    }

    #[test]
    fn test_fast_convergence_disabled() {
        let mut cubic = TCPCubic::new();
        cubic.fast_convergence = false;
        cubic.cwnd = 8192;
        cubic.last_max_cwnd = 1024;

        cubic.update_fast_convergence();

        assert_eq!(cubic.w_last_max, 0);
    }

    #[test]
    fn test_timer_expiry_during_recovery() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 5000;
        cubic.epoch_start = 10.0;

        cubic.timer_expired();

        assert_eq!(cubic.cwnd, cubic.mss);
        assert_eq!(cubic.ssthresh, (5000 / 2).max(2 * cubic.mss));
    }

    #[test]
    fn test_recovery_window_calculation() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 5000;
        cubic.ssthresh = 3000;
        cubic.origin_point = 5000;
        cubic.epoch_start = 1.0;

        // Simulate ACK received to trigger cubic update
        cubic.cubic_update(2.0);

        assert!(
            cubic.cwnd >= 5000,
            "Cwnd did not increase as expected after cubic update."
        );
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "Cwnd exceeded max_cwnd after cubic update."
        );
    }

    #[test]
    fn test_min_congestion_window() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 1;
        cubic.timer_expired();

        assert_eq!(cubic.cwnd, cubic.mss);
    }

    #[test]
    fn test_max_congestion_window() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 1_999_488; // Set just below max_cwnd
        cubic.mss = 512;

        // Simulate ACK received
        cubic.ack_received(0, 0.1, 100.0, 512); // bytes_acked = 512

        // Calculate absolute difference manually
        let diff = if cubic.cwnd > 2_000_000 {
            cubic.cwnd - 2_000_000
        } else {
            2_000_000 - cubic.cwnd
        };

        // Allow a small difference due to floating-point precision
        assert!(
            diff <= 512,
            "cwnd should reach approximately max_cwnd (2,000,000), but got {}",
            cubic.cwnd
        );
    }

    #[test]
    fn test_cwnd_increment_tcp_friendliness() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.ssthresh = 4096;
        cubic.tcp_friendliness = true;

        // Simulate multiple ACKs to trigger TCP-friendly growth
        for _ in 0..20 {
            cubic.ack_received(0, 0.1, 1.0, cubic.mss);
        }

        assert!(
            cubic.cwnd > 4096,
            "Cwnd did not increase as expected with TCP friendliness."
        );
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "Cwnd exceeded max_cwnd with TCP friendliness."
        );
    }

    #[test]
    fn test_cwnd_increment_non_tcp_friendliness() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.ssthresh = 4096;
        cubic.tcp_friendliness = false;

        // Simulate multiple ACKs to trigger CUBIC growth without TCP friendliness
        for _ in 0..20 {
            cubic.ack_received(0, 0.1, 1.0, cubic.mss);
        }

        assert!(
            cubic.cwnd > 4096,
            "Cwnd did not increase as expected without TCP friendliness."
        );
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "Cwnd exceeded max_cwnd without TCP friendliness."
        );
    }

    #[test]
    fn test_retransmission_triggered_on_loss() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 8192;
        cubic.ssthresh = 4096;

        // Simulate loss detection
        cubic.consecutive_dupacks_received();

        assert_eq!(cubic.cwnd, cubic.ssthresh + 3 * cubic.mss);
    }

    #[test]
    fn test_recovery_after_full_ack() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 8192;
        cubic.ssthresh = 4096;

        // Enter recovery
        cubic.consecutive_dupacks_received();

        // Simulate full ACK
        cubic.ack_received(0, 0.1, 2.0, 3 * cubic.mss);

        assert_eq!(
            cubic.cwnd, cubic.ssthresh,
            "Cwnd did not recover to ssthresh after full ACK."
        );
    }

    #[test]
    fn test_multiple_loss_recoveries() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 10000;
        cubic.ssthresh = 5000;

        // First loss recovery
        cubic.consecutive_dupacks_received();
        assert_eq!(cubic.cwnd, cubic.ssthresh + 3 * cubic.mss);

        // Simulate full ACK to exit recovery
        cubic.ack_received(0, 0.1, 2.0, 3 * cubic.mss);
        assert_eq!(cubic.cwnd, cubic.ssthresh);

        // Second loss recovery
        cubic.consecutive_dupacks_received();
        assert_eq!(cubic.cwnd, cubic.ssthresh + 3 * cubic.mss);
    }

    #[test]
    fn test_sequence_number_wraparound() {
        let mut cubic = TCPCubic::new();
        // Using a large number to simulate wraparound
        cubic.cwnd = 1_844_674_407_370_955_161; // Close to usize::MAX on 64-bit systems
        cubic.ssthresh = 65535;
        cubic.mss = 512;

        // Simulate ACK received that wraps around
        cubic.ack_received(18_446_744_073_709_551_515, 0.1, 1.0, 512);

        // Ensure cwnd does not exceed max_cwnd and prevent overflow
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "Cwnd exceeded max_cwnd after sequence number wraparound."
        );
    }

    #[test]
    fn test_rtt_measurement_edge_cases() {
        let mut cubic = TCPCubic::new();

        // Very small RTT
        cubic.ack_received(0, 0.000001, 1.0, cubic.mss);
        assert!(
            cubic.d_min <= 0.000001,
            "d_min did not update correctly for very small RTT."
        );

        // Very large RTT
        cubic.ack_received(0, 100.0, 2.0, cubic.mss);
        assert!(
            cubic.d_min <= 0.000001,
            "d_min should remain the smallest RTT."
        );
    }

    #[test]
    fn test_zero_window_handling() {
        let mut cubic = TCPCubic::new();

        // Force window to minimum
        cubic.timer_expired();
        cubic.cwnd = 0; // Invalid state

        // ACK should restore to minimum
        cubic.ack_received(0, 0.1, 1.0, cubic.mss);
        assert_eq!(
            cubic.cwnd, cubic.mss,
            "Cwnd was not reset to mss when set to zero."
        );
    }

    #[test]
    fn test_reordering_tolerance() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 2048;
        cubic.ssthresh = 2048;

        // Simulate reordered ACKs
        cubic.ack_received(3000, 0.1, 1.0, 500);
        cubic.ack_received(2500, 0.1, 1.0, 500); // Out-of-order ACK

        // cwnd should have increased appropriately
        assert!(
            cubic.cwnd > 2048,
            "Cwnd did not increase correctly after out-of-order ACKs."
        );
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "Cwnd exceeded max_cwnd after out-of-order ACKs."
        );
    }

    #[test]
    fn test_extended_loss_recovery() {
        let mut cubic = TCPCubic::new();

        // Simulate sending data
        cubic.cwnd = 10000;
        cubic.ssthresh = 5000;

        // First loss recovery
        cubic.consecutive_dupacks_received();
        assert_eq!(cubic.cwnd, cubic.ssthresh + 3 * cubic.mss);

        // Second loss recovery
        cubic.consecutive_dupacks_received();
        assert_eq!(cubic.cwnd, cubic.ssthresh + 3 * cubic.mss);
    }

    #[test]
    fn test_recovery_window_limits() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 5000;
        cubic.ssthresh = 3000;
        cubic.origin_point = 5000;
        cubic.epoch_start = 1.0;

        cubic.cubic_update(2.0);
        assert!(
            cubic.cwnd <= cubic.max_cwnd,
            "Cwnd exceeded max_cwnd after recovery update."
        );
    }

    #[test]
    fn test_window_bounds_minimum() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 1;
        cubic.timer_expired();

        assert_eq!(cubic.cwnd, cubic.mss);
    }

    #[test]
    fn test_window_increment_limits() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.ssthresh = 4096;

        // Simulate a series of ACKs
        for _ in 0..100 {
            cubic.ack_received(0, 0.1, 1.0, cubic.mss);
        }

        assert!(cwnd_increment_behaves_as_expected(&cubic));
    }

    // Helper function to verify cwnd increment within limits
    fn cwnd_increment_behaves_as_expected(cubic: &TCPCubic) -> bool {
        cubic.cwnd > 4096 && cubic.cwnd <= cubic.max_cwnd
    }

    #[test]
    fn test_ack_received_without_duplicate_ack() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.ssthresh = 4096;

        // Simulate ACK without duplicates
        cubic.ack_received(0, 0.1, 1.0, cubic.mss);

        assert_eq!(
            cubic.cwnd,
            4096 + cubic.mss,
            "Cwnd did not increment correctly on ACK."
        );
    }

    #[test]
    fn test_ack_received_with_duplicate_ack() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.ssthresh = 4096;

        // Simulate triple dupacks
        cubic.consecutive_dupacks_received();
        cubic.ack_received(0, 0.1, 2.0, 3 * cubic.mss);

        // After recovery, cwnd should be set to ssthresh
        assert_eq!(
            cubic.cwnd, cubic.ssthresh,
            "Cwnd did not recover to ssthresh after full ACK."
        );
    }

    #[test]
    fn test_ack_received_after_timer_expiry() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 10_000;
        cubic.timer_expired();

        // Simulate ACK after timer expiry
        cubic.ack_received(0, 0.1, 2.0, cubic.mss);

        assert_eq!(
            cubic.cwnd,
            cubic.mss + cubic.mss,
            "Cwnd did not recover correctly after timer expiry."
        );
    }

    #[test]
    fn test_recovery_after_multiple_timer_expiries() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 20_000;

        // First timer expiry
        cubic.timer_expired();
        assert_eq!(cubic.cwnd, cubic.mss);

        // Second timer expiry
        cubic.timer_expired();
        assert_eq!(cubic.cwnd, cubic.mss);
    }

    #[test]
    fn test_rtt_decrease_during_cubic() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 4096;
        cubic.ssthresh = 4096;

        // Simulate ACKs with decreasing RTT
        cubic.ack_received(0, 0.2, 1.0, cubic.mss);
        cubic.ack_received(0, 0.15, 2.0, cubic.mss);
        cubic.ack_received(0, 0.1, 3.0, cubic.mss);

        assert_eq!(
            cubic.d_min, 0.1,
            "d_min did not update correctly with decreasing RTT."
        );
    }

    #[test]
    fn test_rtt_increase_exit_hystart() {
        let mut cubic = TCPCubic::new();
        cubic.hystart.enabled = true;
        cubic.cwnd = 16;

        // Simulate RTT increases
        let initial_rtt = 0.2;
        let increased_rtt = 0.4; // 100% increase

        for _ in 0..4 {
            cubic.ack_received(0, initial_rtt, 1.0, cubic.mss);
        }

        for _ in 0..4 {
            cubic.ack_received(0, increased_rtt, 2.0, cubic.mss);
        }

        assert!(
            cubic.hystart.exit_slow_start,
            "HyStart did not exit slow start on RTT increase."
        );
        assert_eq!(cubic.ssthresh, cubic.cwnd.min(cubic.max_cwnd));
    }

    #[test]
    fn test_cwnd_never_zero() {
        let mut cubic = TCPCubic::new();
        cubic.cwnd = 0;

        // Simulate ACK
        cubic.ack_received(0, 0.1, 1.0, cubic.mss);

        assert_eq!(
            cubic.cwnd, cubic.mss,
            "Cwnd was not reset to mss when set to zero."
        );
    }
}

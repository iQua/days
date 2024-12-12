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
            self.rtt_samples.clear();
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
                self.ssthresh = self.cwnd;
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
            let rtt_scale = (self.d_min / 0.1).min(1.0);
            let alpha = self.tcp_friendly_alpha * rtt_scale;

            // Account for delayed ACKs in window calculation
            let w_tcp = self.w_tcp as f64
                + (alpha * self.beta * (self.ack_cnt as f64)
                    / (self.cwnd as f64 * self.delayed_ack_factor as f64));

            w_tcp.max(self.min_cwnd as f64).min(self.max_cwnd as f64)
        } else {
            0.0
        };

        // Take maximum of cubic and TCP friendly windows
        let w_est = if self.tcp_friendliness {
            w_cubic.max(w_tcp)
        } else {
            w_cubic
        };

        // More precise count calculation
        if w_est > self.cwnd as f64 {
            self.cnt = ((self.cwnd as f64 * (w_est - self.cwnd as f64)) / (w_est * self.mss as f64))
                .max(2.0) as usize;
        } else {
            self.cnt = 100 * self.cwnd;
        }
    }

    fn update_fast_convergence(&mut self) {
        if !self.fast_convergence {
            return;
        }

        // Fast convergence mechanism
        if self.cwnd < self.last_max_cwnd {
            self.last_max_cwnd = self.cwnd * (1.0 + self.beta) as usize;
            self.w_last_max = self.cwnd;
        } else {
            self.last_max_cwnd = self.cwnd;
            self.w_last_max = self.cwnd;
        }
    }

    /// CUBIC actions in TCP mode.
    pub fn cubic_tcp_friendliness(&mut self) {
        self.w_tcp += (3.0 * self.beta / (2.0 - self.beta) * (self.ack_cnt / self.cwnd) as f64)
            .floor() as usize;
        self.ack_cnt = 0;

        if self.w_tcp > self.cwnd {
            let max_cnt = self.cwnd / (self.w_tcp - self.cwnd);
            if self.cnt > max_cnt {
                self.cnt = max_cnt;
            }
        }
    }
}

impl CongestionControl for TCPCubic {
    /// Actions to be taken when a new acknowledgment has been received.
    fn ack_received(&mut self, rtt: f64, current_time: f64, bytes_acked: usize) {
        // Track minimum RTT
        if self.d_min > 0.0 {
            self.d_min = self.d_min.min(rtt);
        } else {
            self.d_min = rtt;
        }

        // Update HyStart state with improved RTT tracking
        self.update_hystart(rtt, current_time);

        if self.cwnd <= self.ssthresh && !self.hystart.exit_slow_start {
            // Slow start with HyStart++ detection
            self.cwnd += bytes_acked.min(self.mss); // Use bytes_acked instead of just mss
        } else {
            // Congestion avoidance
            self.cubic_update(current_time);
            if self.cwnd_cnt > self.cnt {
                self.update_fast_convergence();
                self.cwnd += bytes_acked.min(self.mss); // Use bytes_acked
                self.cwnd = self.cwnd.min(self.max_cwnd);
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
    }

    /// Actions to be taken when a new acknowledgment is received after previous
    /// dupacks.
    fn dupack_over(&mut self) {
        self.cwnd = self.ssthresh;
    }

    /// Actions to be taken when three consecutive dupacks are received.
    fn consecutive_dupacks_received(&mut self) {
        self.ssthresh = (2 * self.mss).max(self.cwnd / 2);
        self.cwnd = self.ssthresh + 3 * self.mss;
    }

    /// Actions to be taken when more than three consecutive dupacks are
    /// received.
    fn more_dupacks_received(&mut self) {
        self.cwnd += self.mss;
    }

    fn get_cwnd(&self) -> usize {
        self.cwnd
    }
}

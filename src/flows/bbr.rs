//! Implements the TCP BBR congestion control algorithm.

use crate::flows::cc::CongestionControl;

#[derive(Debug, Default)]
pub struct BBRState {
    pub mode: BBRMode,
    /// Maximum bandwidth measured, in bytes/second
    pub bandwidth_max: f64,
    /// Minimum round-trip time measured, in seconds
    pub rtt_min: f64,
    /// The amount of data in flight
    pub inflight: usize,
    /// Bandwidth filter
    pub bandwidth_latest: f64,
    /// Round trip time filter
    pub rtt_latest: f64,
    /// Packet pacing rate
    pub pacing_rate: f64,
    /// Congestion window
    pub cwnd: usize,
    /// Gain used for bandwidth probing
    pub gain_cycle: usize,
    /// Startup gain sequence
    pub startup_gain: [f64; 4],
    /// Drain gain sequence
    pub drain_gain: [f64; 2],
    /// Probe bandwidth sequence
    pub probe_bw_gain: [f64; 8],
    /// Probe round trip time sequence
    pub probe_rtt_gain: [f64; 4],
    /// Packet loss rate estimator
    pub loss_rate: f64,
    /// Estimated round trip count for probing
    pub round_count: usize,
    /// Timestamp of last cycle start
    pub cycle_start_time: f64,
    /// Minimum segment size in bytes
    pub mss: usize,
    /// Maximum congestion window
    pub max_cwnd: usize,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BBRMode {
    Startup,
    Drain,
    ProbeBW,
    ProbeRTT,
}

impl Default for BBRMode {
    fn default() -> Self {
        BBRMode::Startup
    }
}

impl BBRState {
    pub fn new(mss: usize) -> Self {
        BBRState {
            mss,
            max_cwnd: 2_000_000 * mss, // 2M segments
            startup_gain: [1.25, 1.25, 1.25, 1.25],
            drain_gain: [0.75, 1.0],
            probe_bw_gain: [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.25, 0.75],
            probe_rtt_gain: [1.0, 1.0, 1.0, 1.0],
            // Initialize rtt_min to a large value
            rtt_min: f64::INFINITY, // Or a very large number
            ..Default::default()
        }
    }

    fn update_bw_and_rtt(&mut self, bytes_acked: usize, rtt: f64, now: f64) {
        // Bandwidth calculation, filtered with EWMA
        self.bandwidth_latest = (bytes_acked as f64 / rtt).max(self.bandwidth_latest * 0.875);
        self.bandwidth_max = self.bandwidth_max.max(self.bandwidth_latest);

        // RTT calculation, filtered with min
        self.rtt_latest = rtt;
        self.rtt_min = self.rtt_min.min(rtt);

        // Update BBR cycle
        self.round_count += 1;

        //  Use a separate cycle_start_time check:
        if self.cycle_start_time == 0.0 {
            self.cycle_start_time = now;
        }

        if now - self.cycle_start_time > self.rtt_min {
            self.cycle_start_time = now;

            match self.mode {
                BBRMode::Startup => {
                    // Check for bandwidth increase, transition to Drain if no increase
                    if self.round_count >= 4 || self.bandwidth_latest < self.bandwidth_max {
                        self.mode = BBRMode::Drain;
                    }
                }
                BBRMode::Drain => {
                    // Transition to ProbeBW
                    self.mode = BBRMode::ProbeBW;
                    self.round_count = 0;
                    self.gain_cycle = 0;
                }
                BBRMode::ProbeBW => {
                    // Cycle through probe_bw_gain
                    self.gain_cycle = (self.gain_cycle + 1) % self.probe_bw_gain.len();
                }
                BBRMode::ProbeRTT => {
                    // Transition back to ProbeBW or Drain based on loss rate and bandwidth
                    self.mode = BBRMode::ProbeBW; // Simplify, no ProbeRTT
                }
            }
        }
    }

    fn calculate_cwnd(&mut self) {
        // Cwnd calculation based on bandwidth and RTT
        let bdp = self.bandwidth_max * self.rtt_min;
        let gain = match self.mode {
            BBRMode::Startup => self.startup_gain[self.round_count % self.startup_gain.len()],
            BBRMode::Drain => self.drain_gain[self.round_count % self.drain_gain.len()],
            BBRMode::ProbeBW => self.probe_bw_gain[self.gain_cycle],
            BBRMode::ProbeRTT => self.probe_rtt_gain[self.round_count % self.probe_rtt_gain.len()],
        };

        self.cwnd = (bdp * gain).min(self.max_cwnd as f64) as usize;
    }

    fn calculate_pacing_rate(&mut self) {
        // Pacing rate calculation based on bandwidth and gain
        let gain = match self.mode {
            BBRMode::Startup => self.startup_gain[self.round_count % self.startup_gain.len()],
            BBRMode::Drain => self.drain_gain[self.round_count % self.drain_gain.len()],
            BBRMode::ProbeBW => self.probe_bw_gain[self.gain_cycle],
            BBRMode::ProbeRTT => self.probe_rtt_gain[self.round_count % self.probe_rtt_gain.len()],
        };

        self.pacing_rate = self.bandwidth_max * gain;
    }
}

#[derive(Debug)]
pub struct TCPBBR {
    state: BBRState,
}

impl TCPBBR {
    pub fn new() -> Self {
        let default_mss = 512;

        TCPBBR {
            state: BBRState::new(default_mss),
        }
    }
}

impl CongestionControl for TCPBBR {
    fn ack_received(&mut self, _ack_seq: usize, rtt: f64, now: f64, bytes_acked: usize) {
        self.state.update_bw_and_rtt(bytes_acked, rtt, now);
        self.state.calculate_cwnd();
        self.state.calculate_pacing_rate();
    }

    fn timer_expired(&mut self) {
        // Handle RTO event, halve bandwidth_max
        self.state.bandwidth_max /= 2.0;
        self.state.calculate_cwnd();

        // Should enter startup after a timeout.
        self.state.mode = BBRMode::Startup;
    }

    // These methods don't have a direct BBR equivalent, implement them as no-ops or simple reactions
    fn dupack_over(&mut self) {} // BBR doesn't use dupacks explicitly
    fn consecutive_dupacks_received(&mut self) {}
    fn more_dupacks_received(&mut self) {}

    fn get_cwnd(&self) -> usize {
        self.state.cwnd
    }
}

// Tests (add more as needed)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let bbr = TCPBBR::new();
        assert_eq!(bbr.state.mode, BBRMode::Startup);
    }

    #[test]
    fn test_bandwidth_update() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1; // Set a non-zero rtt_min to avoid division by zero
        bbr.ack_received(0, 0.1, 1.0, 1024);
        assert!(bbr.state.bandwidth_latest > 0.0);
        assert_eq!(bbr.state.bandwidth_max, bbr.state.bandwidth_latest);
    }

    #[test]
    fn test_rtt_update() {
        let mut bbr = TCPBBR::new();

        // Simulate an ACK being received:
        bbr.ack_received(0, 0.2, 1.0, 1024);

        // Assert that rtt_min has been updated correctly
        assert_eq!(bbr.state.rtt_min, 0.2);
        assert_eq!(bbr.state.rtt_latest, 0.2);

        // Assert for a second ACK
        bbr.ack_received(0, 0.1, 2.0, 1024); // Lower RTT
        assert_eq!(bbr.state.rtt_min, 0.1);
        assert_eq!(bbr.state.rtt_latest, 0.1);

        bbr.ack_received(0, 0.3, 3.0, 1024); // Higher RTT, shouldn't change rtt_min
        assert_eq!(bbr.state.rtt_min, 0.1);
        assert_eq!(bbr.state.rtt_latest, 0.3);
    }
}

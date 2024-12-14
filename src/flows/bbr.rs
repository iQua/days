//! Implements the TCP BBR congestion control algorithm (RFC 8961).

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
    /// Current gain based on mode and cycle
    pub current_gain: f64,
    /// Indicator for RTT probing completion
    pub rtt_probe_done: bool,
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
            max_cwnd: 2_000_000 * mss,                  // 2M segments
            startup_gain: [2.885, 3.157, 3.429, 3.700], // Typical startup gains
            drain_gain: [0.875, 0.875],                 // Drain to drain the queue
            probe_bw_gain: [0.875, 1.0, 1.15, 1.0, 1.0, 1.15, 1.0, 0.875], // ProbeBW cycle
            probe_rtt_gain: [1.0, 1.0, 1.0, 1.0],       // Maintain current state during ProbeRTT
            // Initialize rtt_min to a large value
            rtt_min: f64::INFINITY, // Or a very large number
            bandwidth_max: 0.0,
            bandwidth_latest: 0.0,
            rtt_latest: 0.0,
            pacing_rate: 0.0,
            cwnd: 10 * mss, // Initial cwnd
            gain_cycle: 0,
            loss_rate: 0.0,
            round_count: 0,
            cycle_start_time: 0.0,
            current_gain: 1.0,
            rtt_probe_done: false,
            inflight: 0,
            ..Default::default()
        }
    }

    pub fn update_bw_and_rtt(&mut self, bytes_acked: usize, rtt: f64, now: f64) {
        // Bandwidth calculation, filtered with EWMA
        self.bandwidth_latest = (bytes_acked as f64 / rtt).max(self.bandwidth_latest * 0.875);
        self.bandwidth_max = self.bandwidth_max.max(self.bandwidth_latest);

        // RTT calculation, filtered with min
        self.rtt_latest = rtt;
        self.rtt_min = self.rtt_min.min(rtt);

        // Update BBR cycle
        self.round_count += 1;

        // Use a separate cycle_start_time check:
        if self.cycle_start_time == 0.0 {
            self.cycle_start_time = now;
        }

        if now - self.cycle_start_time >= self.rtt_min {
            self.cycle_start_time = now;

            match self.mode {
                BBRMode::Startup => {
                    // Check for bandwidth increase, transition to Drain if no increase
                    if self.round_count >= self.startup_gain.len()
                        || self.bandwidth_latest < self.bandwidth_max
                    {
                        self.mode = BBRMode::Drain;
                        self.round_count = 0;
                        self.current_gain = self.drain_gain[0];
                    } else {
                        // Continue in Startup with increasing gain
                        self.current_gain =
                            self.startup_gain[self.round_count % self.startup_gain.len()];
                    }
                }
                BBRMode::Drain => {
                    // Transition to ProbeBW after draining the queue
                    // Fixed by casting mss to f64
                    if self.inflight
                        < (self.bandwidth_max * self.rtt_min / (self.mss as f64)) as usize
                    {
                        self.mode = BBRMode::ProbeBW;
                        self.round_count = 0;
                        self.gain_cycle = 0;
                        self.current_gain = self.probe_bw_gain[self.gain_cycle];
                    } else {
                        // Continue draining
                        self.current_gain =
                            self.drain_gain[self.round_count % self.drain_gain.len()];
                    }
                }
                BBRMode::ProbeBW => {
                    // Cycle through probe_bw_gain
                    self.current_gain = self.probe_bw_gain[self.gain_cycle];
                    self.gain_cycle = (self.gain_cycle + 1) % self.probe_bw_gain.len();

                    // After completing a full cycle, potentially enter ProbeRTT
                    if self.gain_cycle == 0 && self.round_count >= self.probe_bw_gain.len() {
                        self.mode = BBRMode::ProbeRTT;
                        self.round_count = 0;
                        self.rtt_probe_done = false;
                        // Fixed by casting mss to f64
                        self.inflight = ((self.bandwidth_max * self.rtt_min / (self.mss as f64)
                            * 0.5) as usize)
                            .max(10 * self.mss);
                    }
                }
                BBRMode::ProbeRTT => {
                    if !self.rtt_probe_done {
                        // Temporarily reduce pacing and cwnd to measure RTT
                        self.current_gain = self.probe_rtt_gain[0];
                        self.rtt_probe_done = true;
                    } else {
                        // After RTT measurement, transition back to ProbeBW
                        self.mode = BBRMode::ProbeBW;
                        self.round_count = 0;
                        self.gain_cycle = 0;
                        self.current_gain = self.probe_bw_gain[self.gain_cycle];
                    }
                }
            }
        }
    }

    pub fn calculate_cwnd(&mut self) {
        // Cwnd calculation based on bandwidth and RTT
        let bdp = self.bandwidth_max * self.rtt_min;
        self.cwnd = (bdp * self.current_gain).min(self.max_cwnd as f64) as usize;

        // Enforce a minimum cwnd to prevent underutilization
        let min_cwnd = 10 * self.mss;
        if self.cwnd < min_cwnd {
            self.cwnd = min_cwnd;
        }
    }

    pub fn calculate_pacing_rate(&mut self) {
        // Pacing rate calculation based on bandwidth and gain
        self.pacing_rate = self.bandwidth_max * self.current_gain;
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
        // Updating inflight: assuming bytes_acked have been acknowledged
        self.state.inflight = self.state.inflight.saturating_sub(bytes_acked);
        self.state.calculate_cwnd();
        self.state.calculate_pacing_rate();
    }

    fn timer_expired(&mut self) {
        // Handle RTO event, halve bandwidth_max to respond to congestion
        self.state.bandwidth_max /= 2.0;
        self.state.calculate_cwnd();
        self.state.calculate_pacing_rate();

        // Enter Startup mode after a timeout
        self.state.mode = BBRMode::Startup;
        self.state.round_count = 0;
        self.state.current_gain = self.state.startup_gain[0];
        self.state.inflight = 0; // Reset inflight due to timeout
    }

    // These methods don't have a direct BBR equivalent, implement them as no-ops or simple reactions
    fn dupack_over(&mut self) {} // BBR doesn't use dupacks explicitly
    fn consecutive_dupacks_received(&mut self) {}
    fn more_dupacks_received(&mut self) {}

    fn get_cwnd(&self) -> usize {
        self.state.cwnd
    }

    fn get_pacing_rate(&self) -> f64 {
        self.state.pacing_rate
    }
}

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

    // Helper function to simulate the passage of time and receiving ACKs
    fn simulate_ack(bbr: &mut TCPBBR, ack_seq: usize, rtt: f64, now: f64, bytes_acked: usize) {
        bbr.ack_received(ack_seq, rtt, now, bytes_acked);
    }

    // Test transition from Startup to Drain
    #[test]
    fn test_startup_to_drain_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;

        // Simulate enough ACKs without bandwidth increase to trigger transition
        for i in 0..5 {
            simulate_ack(&mut bbr, i, 0.1, i as f64, 1024);
        }

        assert_eq!(bbr.state.mode, BBRMode::Drain);
    }

    // Test transition from Drain to ProbeBW
    #[test]
    fn test_drain_to_probe_bw_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;
        bbr.state.mode = BBRMode::Drain;
        bbr.state.inflight = 500; // Set inflight below the threshold

        // Simulate ACKs to trigger transition
        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeBW);
    }

    // Test transition from ProbeBW to ProbeRTT
    #[test]
    fn test_probe_bw_to_probe_rtt_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;
        bbr.state.mode = BBRMode::ProbeBW;
        bbr.state.gain_cycle = bbr.state.probe_bw_gain.len() - 1;
        bbr.state.round_count = bbr.state.probe_bw_gain.len();

        // Simulate ACKs to complete a ProbeBW cycle
        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeRTT);
    }

    // Test transition from ProbeRTT to ProbeBW
    #[test]
    fn test_probe_rtt_to_probe_bw_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;
        bbr.state.mode = BBRMode::ProbeRTT;
        bbr.state.rtt_probe_done = false;

        // Simulate ACK to perform RTT probing
        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeBW);
        assert!(bbr.state.rtt_probe_done);
    }

    // Test that cwnd does not exceed max_cwnd
    #[test]
    fn test_cwnd_max_boundary() {
        let mut bbr = TCPBBR::new();
        bbr.state.bandwidth_max = 1_000_000.0;
        bbr.state.rtt_min = 0.1;
        bbr.state.current_gain = 10.0; // Intentionally high gain to exceed max_cwnd

        bbr.state.calculate_cwnd();

        assert_eq!(bbr.state.cwnd, bbr.state.max_cwnd);
    }

    // Test that cwnd does not drop below minimum threshold
    #[test]
    fn test_cwnd_min_boundary() {
        let mut bbr = TCPBBR::new();
        bbr.state.bandwidth_max = 100.0;
        bbr.state.rtt_min = 0.1;
        bbr.state.current_gain = 0.5; // Intentionally low gain

        bbr.state.calculate_cwnd();

        let min_cwnd = 10 * bbr.state.mss;
        assert_eq!(bbr.state.cwnd, min_cwnd);
    }

    // Test inflight data reduction
    #[test]
    fn test_inflight_reduction() {
        let mut bbr = TCPBBR::new();
        bbr.state.inflight = 2048;
        bbr.ack_received(1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.inflight, 1024);
    }

    // Test timer_expired handling (simulating packet loss)
    #[test]
    fn test_timer_expired_packet_loss() {
        let mut bbr = TCPBBR::new();
        bbr.state.bandwidth_max = 1000.0;
        bbr.state.mode = BBRMode::ProbeBW;
        bbr.state.inflight = 5000;

        bbr.timer_expired();

        assert_eq!(bbr.state.bandwidth_max, 500.0); // Halved
        assert_eq!(bbr.state.mode, BBRMode::Startup);
        assert_eq!(bbr.state.current_gain, bbr.state.startup_gain[0]);
        assert_eq!(bbr.state.inflight, 0);
    }

    // Simulate RTT spikes and observe BBR's response
    #[test]
    fn test_rtt_spike() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;
        bbr.state.bandwidth_max = 1000.0;
        bbr.state.current_gain = 1.0;

        // Simulate a spike in RTT
        simulate_ack(&mut bbr, 1, 0.5, 1.0, 1024);

        assert!(bbr.state.rtt_min < 0.5); // rtt_min should remain the minimum
        assert_eq!(bbr.state.rtt_latest, 0.5);
    }

    // Simulate varying bandwidth and observe adaptive behavior
    #[test]
    fn test_varying_bandwidth() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;

        // Simulate increasing bandwidth
        for i in 0..10 {
            simulate_ack(&mut bbr, i, 0.1, i as f64, 2048);
        }

        assert!(bbr.state.bandwidth_max > 0.0);

        // Simulate decreasing bandwidth
        for i in 10..20 {
            simulate_ack(&mut bbr, i, 0.2, i as f64, 1024);
        }

        // bandwidth_max should remain the maximum observed
        assert_eq!(bbr.state.bandwidth_max, 2048.0 / 0.1);
    }

    // Simulate packet loss and ensure BBR enters appropriate state
    #[test]
    fn test_packet_loss_recovery() {
        let mut bbr = TCPBBR::new();
        bbr.state.mode = BBRMode::ProbeBW;
        bbr.state.bandwidth_max = 1000.0;
        bbr.state.inflight = 1500;

        // Simulate packet loss via timer_expired
        bbr.timer_expired();

        assert_eq!(bbr.state.mode, BBRMode::Startup);
        assert_eq!(bbr.state.bandwidth_max, 500.0);
    }

    // Test handling of multiple consecutive timer expirations
    #[test]
    fn test_consecutive_timer_expired() {
        let mut bbr = TCPBBR::new();
        bbr.state.bandwidth_max = 1000.0;

        // First timer expiration
        bbr.timer_expired();
        assert_eq!(bbr.state.bandwidth_max, 500.0);
        assert_eq!(bbr.state.mode, BBRMode::Startup);

        // Second timer expiration
        bbr.timer_expired();
        assert_eq!(bbr.state.bandwidth_max, 250.0);
        assert_eq!(bbr.state.mode, BBRMode::Startup);
    }

    // Test that BBR does not transition to ProbeRTT prematurely
    #[test]
    fn test_probe_rtt_not_premature() {
        let mut bbr = TCPBBR::new();
        bbr.state.mode = BBRMode::ProbeBW;
        bbr.state.gain_cycle = 4; // Not completing a full cycle
        bbr.state.round_count = 4;

        // Simulate ACKs without completing the probe_bw_gain cycle
        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeBW);
        assert!(!bbr.state.rtt_probe_done);
    }

    // Test that ProbeRTT sets inflight correctly
    #[test]
    fn test_probe_rtt_inflight_set() {
        let mut bbr = TCPBBR::new();
        bbr.state.mode = BBRMode::ProbeRTT;
        bbr.state.bandwidth_max = 1000.0;
        bbr.state.rtt_min = 0.1;
        bbr.state.mss = 512;
        bbr.state.inflight = 1024;

        simulate_ack(&mut bbr, 1, 0.1, 1.0, 512);

        let expected_inflight = ((1000.0 * 0.1 / 512.0) * 0.5) as usize;
        let min_inflight = 10 * 512;
        let final_inflight = expected_inflight.max(min_inflight);
        assert_eq!(bbr.state.inflight, final_inflight);
    }
}

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
    /// Previous maximum bandwidth
    pub prev_bandwidth_max: f64,
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
            max_cwnd: 2_000_000 * mss,                // 2M segments
            startup_gain: [2.885, 3.157, 3.429, 3.7], // Typical startup gains
            drain_gain: [0.875, 0.875],               // Drain to drain the queue
            probe_bw_gain: [0.875, 1.0, 1.15, 1.0, 1.0, 1.15, 1.0, 0.875], // ProbeBW cycle
            probe_rtt_gain: [0.5, 0.5, 0.5, 0.5],     // Reduce inflight during ProbeRTT
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
            prev_bandwidth_max: 0.0,
            ..Default::default()
        }
    }

    fn min_cwnd(&self) -> usize {
        4 * self.mss
    }

    pub fn update_bw_and_rtt(&mut self, bytes_acked: usize, rtt: f64, now: f64) {
        // Bandwidth calculation
        let bw_sample = bytes_acked as f64 / rtt;
        self.bandwidth_latest = bw_sample;

        // Update maximum bandwidth
        if self.bandwidth_latest > self.bandwidth_max {
            self.bandwidth_max = self.bandwidth_latest;
        }

        // RTT calculation
        self.rtt_latest = rtt;
        if self.rtt_latest < self.rtt_min {
            self.rtt_min = self.rtt_latest;
        }

        // Update BBR cycle
        if now - self.cycle_start_time >= self.rtt_min {
            self.cycle_start_time = now;
            self.round_count += 1;

            match self.mode {
                BBRMode::Startup => {
                    if self.bandwidth_max > self.prev_bandwidth_max {
                        self.prev_bandwidth_max = self.bandwidth_max;
                    } else {
                        self.mode = BBRMode::Drain;
                        self.current_gain = self.drain_gain[0];
                    }
                }
                BBRMode::Drain => {
                    // Target inflight is BDP
                    let target_inflight = (self.bandwidth_max * self.rtt_min) as usize;
                    if self.inflight <= target_inflight {
                        self.mode = BBRMode::ProbeBW;
                        self.gain_cycle = 0;
                        self.current_gain = self.probe_bw_gain[self.gain_cycle];
                    }
                }
                BBRMode::ProbeBW => {
                    self.gain_cycle = (self.gain_cycle + 1) % self.probe_bw_gain.len();
                    self.current_gain = self.probe_bw_gain[self.gain_cycle];
                    if self.round_count >= 10 {
                        self.mode = BBRMode::ProbeRTT;
                        self.rtt_probe_done = false;
                    }
                }
                BBRMode::ProbeRTT => {
                    if !self.rtt_probe_done {
                        self.current_gain = self.probe_rtt_gain[0];
                        self.inflight = self.min_cwnd();
                        self.rtt_probe_done = true;
                    } else {
                        self.mode = BBRMode::ProbeBW;
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
        let cwnd = bdp * self.current_gain;
        self.cwnd = cwnd.min(self.max_cwnd as f64) as usize;

        // Enforce a minimum cwnd
        let min_cwnd = self.min_cwnd();
        if self.cwnd < min_cwnd {
            self.cwnd = min_cwnd;
        }
    }

    pub fn calculate_pacing_rate(&mut self) {
        // Pacing rate calculation
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
        // Updating inflight
        self.state.inflight = self.state.inflight.saturating_sub(bytes_acked);
        self.state.calculate_cwnd();
        self.state.calculate_pacing_rate();
    }

    fn timer_expired(&mut self) {
        // Handle RTO event
        self.state.bandwidth_max /= 2.0;
        self.state.calculate_cwnd();
        self.state.calculate_pacing_rate();

        // Enter Startup mode after a timeout
        self.state.mode = BBRMode::Startup;
        self.state.prev_bandwidth_max = 0.0;
        self.state.current_gain = self.state.startup_gain[0];
        self.state.inflight = 0;
    }

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

    // Helper function to simulate the passage of time and receiving ACKs
    fn simulate_ack(bbr: &mut TCPBBR, ack_seq: usize, rtt: f64, now: f64, bytes_acked: usize) {
        bbr.ack_received(ack_seq, rtt, now, bytes_acked);
        // Increase inflight by bytes sent
        bbr.state.inflight += bytes_acked;
    }

    #[test]
    fn test_initial_state() {
        let bbr = TCPBBR::new();
        assert_eq!(bbr.state.mode, BBRMode::Startup);
    }

    #[test]
    fn test_bandwidth_update() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1; // Set a non-zero rtt_min
        simulate_ack(&mut bbr, 0, 0.1, 1.0, 1024);
        assert!(bbr.state.bandwidth_latest > 0.0);
        assert_eq!(bbr.state.bandwidth_max, bbr.state.bandwidth_latest);
    }

    #[test]
    fn test_rtt_update() {
        let mut bbr = TCPBBR::new();

        // Simulate an ACK being received:
        simulate_ack(&mut bbr, 0, 0.2, 1.0, 1024);

        // Assert that rtt_min has been updated correctly
        assert_eq!(bbr.state.rtt_min, 0.2);
        assert_eq!(bbr.state.rtt_latest, 0.2);

        // Assert for a second ACK
        simulate_ack(&mut bbr, 0, 0.1, 2.0, 1024); // Lower RTT
        assert_eq!(bbr.state.rtt_min, 0.1);
        assert_eq!(bbr.state.rtt_latest, 0.1);

        simulate_ack(&mut bbr, 0, 0.3, 3.0, 1024); // Higher RTT
        assert_eq!(bbr.state.rtt_min, 0.1);
        assert_eq!(bbr.state.rtt_latest, 0.3);
    }

    // Test transition from Startup to Drain
    #[test]
    fn test_startup_to_drain_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;

        // Simulate ACKs with no bandwidth increase
        simulate_ack(&mut bbr, 0, 0.1, 1.0, 1024);
        bbr.state.prev_bandwidth_max = bbr.state.bandwidth_max;

        // Simulate next ACK with same bandwidth
        simulate_ack(&mut bbr, 1, 0.1, 2.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::Drain);
    }

    // Test transition from Drain to ProbeBW
    #[test]
    fn test_drain_to_probe_bw_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.mode = BBRMode::Drain;
        bbr.state.rtt_min = 0.1;
        bbr.state.bandwidth_max = 1000.0;
        bbr.state.inflight = 500; // Less than target_inflight

        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeBW);
    }

    // Test transition from ProbeBW to ProbeRTT
    #[test]
    fn test_probe_bw_to_probe_rtt_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.mode = BBRMode::ProbeBW;
        bbr.state.rtt_min = 0.1;
        bbr.state.round_count = 10;

        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeRTT);
    }

    // Test transition from ProbeRTT to ProbeBW
    #[test]
    fn test_probe_rtt_to_probe_bw_transition() {
        let mut bbr = TCPBBR::new();
        bbr.state.mode = BBRMode::ProbeRTT;
        bbr.state.rtt_min = 0.1;
        bbr.state.rtt_probe_done = false;

        simulate_ack(&mut bbr, 1, 0.1, 1.0, 1024);

        assert!(bbr.state.rtt_probe_done);
        simulate_ack(&mut bbr, 2, 0.1, 2.0, 1024);

        assert_eq!(bbr.state.mode, BBRMode::ProbeBW);
    }

    // Test that cwnd does not exceed max_cwnd
    #[test]
    fn test_cwnd_max_boundary() {
        let mut bbr = TCPBBR::new();
        bbr.state.bandwidth_max = 2_000_000_000.0; // Increased bandwidth to exceed max_cwnd
        bbr.state.rtt_min = 0.1;
        bbr.state.current_gain = 10.0; // High gain

        bbr.state.calculate_cwnd();

        assert_eq!(bbr.state.cwnd, bbr.state.max_cwnd);
    }

    // Test that cwnd does not drop below minimum threshold
    #[test]
    fn test_cwnd_min_boundary() {
        let mut bbr = TCPBBR::new();
        bbr.state.bandwidth_max = 100.0;
        bbr.state.rtt_min = 0.1;
        bbr.state.current_gain = 0.5;

        bbr.state.calculate_cwnd();

        let min_cwnd = bbr.state.min_cwnd();
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

        let min_cwnd = bbr.state.min_cwnd();
        assert_eq!(bbr.state.inflight, min_cwnd);
    }

    // Simulate varying bandwidth and observe adaptive behavior
    #[test]
    fn test_varying_bandwidth() {
        let mut bbr = TCPBBR::new();
        bbr.state.rtt_min = 0.1;

        // Simulate increasing bandwidth
        for i in 0..10 {
            simulate_ack(&mut bbr, i, 0.1, i as f64, 2048 + i * 100);
        }

        assert!(bbr.state.bandwidth_max > 0.0);

        // Simulate decreasing bandwidth
        for i in 10..20 {
            simulate_ack(&mut bbr, i, 0.2, i as f64, 1024);
        }

        // bandwidth_max should reflect the highest observed
        assert!(bbr.state.bandwidth_max >= bbr.state.bandwidth_latest);
    }
}

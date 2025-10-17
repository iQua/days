//! Implements the TCP BBRv3 congestion control algorithm.
//!
//! Based on the BBRv3 IETF Draft:
//! https://ietf-wg-ccwg.github.io/draft-ietf-ccwg-bbr/draft-ietf-ccwg-bbr.html

use crate::flows::cc::CongestionControl;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct BBRState {
    pub mode: BBRMode,
    /// Maximum filtered bandwidth estimate, in bytes/second
    pub max_bw: f64,
    /// Minimum filtered round-trip time estimate, in seconds
    pub min_rtt: f64,
    /// The amount of data in flight
    pub inflight: usize,
    /// Recent bandwidth samples
    pub bw_samples: VecDeque<f64>,
    /// Recent RTT samples
    pub rtt_samples: VecDeque<(f64, f64)>, // (rtt_sample, timestamp)
    /// Pacing rate
    pub pacing_rate: f64,
    /// Congestion window
    pub cwnd: usize,
    /// Pacing gain
    pub pacing_gain: f64,
    /// Congestion window gain
    pub cwnd_gain: f64,
    /// Inflight upper bound
    pub inflight_hi: usize,
    /// Inflight lower bound
    pub inflight_lo: usize,
    /// Loss event flag
    pub loss_in_round: bool,
    /// ECN event flag
    pub ecn_in_round: bool,
    /// Round-trip counter
    pub round_count: usize,
    /// Packet sequence number of the next round-trip boundary
    pub next_round_delivered: usize,
    /// Minimum segment size in bytes
    pub mss: usize,
    /// Maximum congestion window
    pub max_cwnd: usize,
    /// Timestamp of last RTT sample
    pub last_rtt_sample_time: f64,
    /// Start time of the current round
    pub round_start_time: f64,
    /// Total data delivered so far
    pub total_data_delivered: usize,
    /// Number of rounds spent in ProbeUp
    pub probe_up_rounds: usize,
    /// Target number of rounds to stay in ProbeUp
    pub target_probe_up_rounds: usize,
    /// Number of rounds spent in ProbeCruise
    pub cruise_rounds: usize,
    /// Timestamp when entered ProbeCruise
    pub cruise_start_time: f64,
    /// Minimum time to stay in ProbeCruise before ProbeUp (seconds)
    pub min_cruise_time: f64,
    /// Maximum time to stay in ProbeCruise before ProbeUp (seconds)
    pub max_cruise_time: f64,
    /// Random cruise duration within min/max range
    pub current_cruise_duration: f64,
    /// Number of RTTs without loss/ECN needed before ProbeUp
    pub stable_rounds_needed: usize,
    /// Counter for rounds without loss/ECN
    pub stable_rounds: usize,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum BBRMode {
    #[default]
    Startup,
    Drain,
    ProbeUp,
    ProbeDown,
    ProbeCruise,
    ProbeRTT,
    Stall,
}

impl BBRState {
    pub fn new(mss: usize) -> Self {
        let mut state = BBRState {
            mode: BBRMode::Startup,
            max_bw: 0.0,
            min_rtt: f64::INFINITY,
            inflight: 0,
            bw_samples: VecDeque::with_capacity(10),
            rtt_samples: VecDeque::with_capacity(10),
            pacing_rate: 0.0,
            cwnd: 10 * mss,   // Initial cwnd
            pacing_gain: 2.0, // Initial pacing gain in Startup
            cwnd_gain: 2.0,   // Initial cwnd gain in Startup
            inflight_hi: usize::MAX,
            inflight_lo: 0,
            loss_in_round: false,
            ecn_in_round: false,
            round_count: 0,
            next_round_delivered: 0,
            round_start_time: 0.0,
            last_rtt_sample_time: 0.0,
            mss,
            max_cwnd: 2_000_000 * mss, // 2M segments
            total_data_delivered: 0,
            probe_up_rounds: 0,
            target_probe_up_rounds: 8, // As per BBRv3
            cruise_rounds: 0,
            cruise_start_time: 0.0,
            min_cruise_time: 2.0,  // Minimum 2 seconds in cruise
            max_cruise_time: 10.0, // Maximum 10 seconds in cruise
            current_cruise_duration: 0.0,
            stable_rounds_needed: 4, // Need 4 stable rounds before ProbeUp
            stable_rounds: 0,
        };

        // Initialize random cruise duration
        state.randomize_cruise_duration();

        state
    }

    fn randomize_cruise_duration(&mut self) {
        // Simple random duration between min and max cruise time
        // In production, use a proper random number generator
        let random_factor = 0.5; // 0.0 to 1.0
        self.current_cruise_duration =
            self.min_cruise_time + (self.max_cruise_time - self.min_cruise_time) * random_factor;
    }

    pub fn min_cwnd(&self) -> usize {
        4 * self.mss
    }

    pub fn update_bandwidth(&mut self, bytes_acked: usize, rtt: f64) {
        // Calculate sample bandwidth
        let bw_sample = bytes_acked as f64 / rtt;

        // Add to bandwidth samples
        self.bw_samples.push_back(bw_sample);
        if self.bw_samples.len() > 10 {
            self.bw_samples.pop_front();
        }

        // Update max_bw as windowed maximum over bw_samples
        self.max_bw = self.bw_samples.iter().cloned().fold(0.0, f64::max);
    }

    pub fn update_min_rtt(&mut self, rtt: f64, now: f64) {
        // Update RTT samples
        self.rtt_samples.push_back((rtt, now));

        // Remove old samples outside the window
        let window_duration = 10.0; // seconds
        self.rtt_samples
            .retain(|&(_, t)| now - t <= window_duration);

        // Update min_rtt as windowed minimum over specified time
        self.min_rtt = self
            .rtt_samples
            .iter()
            .map(|&(rtt_sample, _)| rtt_sample)
            .fold(f64::INFINITY, f64::min);
    }

    pub fn update_round(&mut self, ack_seq: usize) {
        // Initialize next_round_delivered if it's zero
        if self.next_round_delivered == 0 {
            self.next_round_delivered = self.total_data_delivered;
        }

        // A new round trip has started if the ACKed sequence is beyond next_round_delivered
        if ack_seq >= self.next_round_delivered {
            self.round_count += 1;
            self.next_round_delivered = self.total_data_delivered;

            // Reset per-round variables
            self.loss_in_round = false;
            self.ecn_in_round = false;
            self.round_start_time = self.last_rtt_sample_time;

            // Increment probe_up_rounds if in ProbeUp
            if self.mode == BBRMode::ProbeUp {
                self.probe_up_rounds += 1;
            }
        }

        if self.mode == BBRMode::ProbeCruise {
            self.cruise_rounds += 1;

            // Update stable rounds counter
            if !self.loss_in_round && !self.ecn_in_round {
                self.stable_rounds += 1;
            } else {
                self.stable_rounds = 0;
            }
        }
    }

    pub fn on_ack_received(
        &mut self,
        ack_seq: usize,
        bytes_acked: usize,
        rtt: f64,
        now: f64,
        loss_occurred: bool,
        ecn_marked: bool,
    ) {
        self.total_data_delivered = ack_seq;
        self.last_rtt_sample_time = now;
        self.update_round(ack_seq);
        self.update_bandwidth(bytes_acked, rtt);
        self.update_min_rtt(rtt, now);

        if loss_occurred {
            self.loss_in_round = true;
        }

        if ecn_marked {
            self.ecn_in_round = true;
        }

        // Adjust inflight_hi if loss or ECN occurs
        if self.loss_in_round || self.ecn_in_round {
            let bdp = self.max_bw * self.min_rtt;
            self.inflight_hi = (bdp * self.cwnd_gain) as usize;
            self.inflight_hi = self.inflight_hi.min(self.cwnd);
        }

        // Update pacing rate and cwnd
        self.calculate_pacing_rate();
        self.calculate_cwnd();

        // Mode transitions
        self.check_mode_transitions();

        // Update inflight
        self.inflight = self.inflight.saturating_sub(bytes_acked);

        // Reset stable rounds on loss/ECN
        if loss_occurred || ecn_marked {
            self.stable_rounds = 0;
        }

        // Update cruise start time when entering ProbeCruise
        if self.mode == BBRMode::ProbeCruise
            && (self.cruise_start_time == 0.0 || self.cruise_rounds == 0)
        {
            self.cruise_start_time = now;
            self.cruise_rounds = 0;
            self.stable_rounds = 0;
        }
    }

    pub fn calculate_pacing_rate(&mut self) {
        self.pacing_rate = self.max_bw * self.pacing_gain;
    }

    pub fn calculate_cwnd(&mut self) {
        let bdp = self.max_bw * self.min_rtt;
        let target_cwnd = (bdp * self.cwnd_gain) as usize;

        if self.inflight_hi != usize::MAX {
            self.cwnd = target_cwnd.min(self.inflight_hi);
        } else {
            self.cwnd = target_cwnd;
        }

        // Enforce minimum and maximum cwnd
        self.cwnd = self.cwnd.clamp(self.min_cwnd(), self.max_cwnd);
    }

    pub fn check_mode_transitions(&mut self) {
        match self.mode {
            BBRMode::Startup => {
                // Check if exiting Startup
                if self.loss_in_round || self.ecn_in_round {
                    self.mode = BBRMode::Drain;
                    self.pacing_gain = 1.0; // Set to 1.0 in Drain
                    self.cwnd_gain = 1.0;
                }
            }
            BBRMode::Drain => {
                // Transition to ProbeUp when inflight <= BDP
                let bdp = self.max_bw * self.min_rtt;
                if self.inflight <= (bdp as usize) {
                    self.mode = BBRMode::ProbeUp;
                    self.pacing_gain = 1.25;
                    self.cwnd_gain = 1.25;
                }
            }
            BBRMode::ProbeUp => {
                // Transition to ProbeDown if loss or ECN occurs
                if self.loss_in_round || self.ecn_in_round {
                    self.mode = BBRMode::ProbeDown;
                    self.pacing_gain = 0.75;
                    self.cwnd_gain = 0.75;
                }
                // Otherwise, stay in ProbeUp
            }
            BBRMode::ProbeDown => {
                let bdp = self.max_bw * self.min_rtt;
                // Transition to ProbeCruise when inflight <= BDP
                if self.inflight <= (bdp as usize) {
                    self.mode = BBRMode::ProbeCruise;
                    self.pacing_gain = 1.0;
                    self.cwnd_gain = 1.0;
                }
            }
            BBRMode::ProbeCruise => {
                let time_in_cruise = self.last_rtt_sample_time - self.cruise_start_time;

                // Check if we've been in cruise mode long enough
                if time_in_cruise >= self.current_cruise_duration {
                    // Check if network is stable enough for ProbeUp
                    if self.stable_rounds >= self.stable_rounds_needed {
                        // Transition to ProbeUp
                        self.mode = BBRMode::ProbeUp;
                        self.pacing_gain = 1.25;
                        self.cwnd_gain = 1.25;
                        self.probe_up_rounds = 0;
                        self.randomize_cruise_duration(); // Prepare for next cruise
                    }
                }
            }
            BBRMode::ProbeRTT => {
                // Reduce inflight to minimal cwnd
                self.cwnd = self.min_cwnd();
                // Logic to exit ProbeRTT could be added here
            }
            BBRMode::Stall => {
                // Logic for Stall mode if applicable
                // For simplicity, transition back to Startup
                self.mode = BBRMode::Startup;
                self.pacing_gain = 2.0;
                self.cwnd_gain = 2.0;
            }
        }
    }

    pub fn on_packet_sent(&mut self, _seq: usize, now: f64) {
        self.last_rtt_sample_time = now;
    }

    pub fn on_loss_detected(&mut self) {
        self.loss_in_round = true;
    }

    pub fn on_timer_expired(&mut self) {
        // Handle RTO event
        self.mode = BBRMode::Startup;
        self.pacing_gain = 2.0;
        self.cwnd_gain = 2.0;
        self.inflight_hi = usize::MAX;
    }

    pub fn get_cwnd(&self) -> usize {
        self.cwnd
    }

    pub fn get_pacing_rate(&self) -> f64 {
        self.pacing_rate
    }
}

#[derive(Debug)]
pub struct TCPBBR {
    state: BBRState,
}

impl Default for TCPBBR {
    fn default() -> Self {
        Self::new()
    }
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
    fn ack_received(&mut self, ack_seq: usize, rtt: f64, now: f64, bytes_acked: usize) {
        let loss_occurred = false; // Placeholder, should be detected from network layer
        let ecn_marked = false; // Placeholder, should be detected from network layer

        self.state
            .on_ack_received(ack_seq, bytes_acked, rtt, now, loss_occurred, ecn_marked);
    }

    fn timer_expired(&mut self) {
        self.state.on_timer_expired();
    }

    fn dupack_over(&mut self) {} // Handling of duplicate ACKs if needed
    fn consecutive_dupacks_received(&mut self) {}
    fn more_dupacks_received(&mut self) {}

    fn get_cwnd(&self) -> usize {
        self.state.get_cwnd()
    }

    fn get_pacing_rate(&self) -> f64 {
        self.state.get_pacing_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to simulate the passage of time and receiving ACKs
    fn simulate_ack(
        bbr: &mut TCPBBR,
        ack_seq: usize,
        rtt: f64,
        now: f64,
        bytes_acked: usize,
        loss_occurred: bool,
        ecn_marked: bool,
    ) {
        bbr.state
            .on_ack_received(ack_seq, bytes_acked, rtt, now, loss_occurred, ecn_marked);
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
        simulate_ack(&mut bbr, 1024, 0.1, 1.0, 1024, false, false);
        assert!(bbr.state.max_bw > 0.0);
    }

    #[test]
    fn test_mode_transitions() {
        let mut bbr = TCPBBR::new();
        bbr.state.min_rtt = 0.1;

        // Simulate ACKs with loss occurring to trigger transitions
        for i in 1..100 {
            let now = i as f64 * 0.1;
            let rtt = 0.1;
            let bytes_acked = 1024;
            let loss_occurred = i == 10; // Simulate loss at i == 10
            let ecn_marked = false;
            let ack_seq = i * bytes_acked;

            simulate_ack(
                &mut bbr,
                ack_seq,
                rtt,
                now,
                bytes_acked,
                loss_occurred,
                ecn_marked,
            );

            // Check mode transitions
            if i == 10 {
                assert_eq!(
                    bbr.state.mode,
                    BBRMode::Drain,
                    "At i==10: Expected Drain, got {:?}",
                    bbr.state.mode
                );
            }
            if i == 20 {
                assert_eq!(
                    bbr.state.mode,
                    BBRMode::ProbeUp,
                    "At i==20: Expected ProbeUp, got {:?}",
                    bbr.state.mode
                );
            }
        }
    }

    #[test]
    fn test_cwnd_adjustment() {
        let mut bbr = TCPBBR::new();
        bbr.state.min_rtt = 0.1;

        // Simulate network conditions
        for i in 1..50 {
            let now = i as f64 * 0.1;
            let rtt = 0.1 + (i as f64 * 0.001); // Increasing RTT
            let bytes_acked = 1024;
            let loss_occurred = i == 25;
            let ecn_marked = false;
            let ack_seq = i * bytes_acked;

            simulate_ack(
                &mut bbr,
                ack_seq,
                rtt,
                now,
                bytes_acked,
                loss_occurred,
                ecn_marked,
            );
        }

        // Check that cwnd is adjusted appropriately
        assert!(bbr.state.cwnd >= bbr.state.min_cwnd());
        assert!(bbr.state.cwnd <= bbr.state.max_cwnd);
    }

    #[test]
    fn test_pacing_rate_adjustment() {
        let mut bbr = TCPBBR::new();
        bbr.state.min_rtt = 0.1;

        simulate_ack(&mut bbr, 1024, 0.1, 1.0, 1024, false, false);
        let initial_pacing_rate = bbr.state.get_pacing_rate();

        // Increase bandwidth
        simulate_ack(&mut bbr, 2048, 0.1, 1.1, 2048, false, false);

        assert!(bbr.state.get_pacing_rate() > initial_pacing_rate);
    }
}

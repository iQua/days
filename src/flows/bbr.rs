//! Implements the TCP BBRv3 congestion control algorithm.
//!
//! Based on the BBRv3 IETF Draft:
//! https://ietf-wg-ccwg.github.io/draft-ietf-ccwg-bbr/draft-ietf-ccwg-bbr.html

use std::collections::VecDeque;

use crate::flows::cc::{AckEvent, CongestionControl, RateSample};

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
    pub bw_samples: VecDeque<(f64, u64)>,
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
    /// Short-term inflight cap based on recent loss
    pub inflight_shortterm: usize,
    /// Long-term inflight cap based on persistent loss
    pub inflight_longterm: usize,
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
    /// Most recent RTT sample
    pub last_rtt: f64,
    /// Start time of the current round
    pub round_start_time: f64,
    /// Total data delivered so far
    pub total_data_delivered: usize,
    /// Bytes delivered in the current round
    pub round_delivered: usize,
    /// Bytes lost in the current round
    pub round_lost: usize,
    /// Current ProbeBW cycle index
    pub probe_bw_cycle: u64,
    /// ProbeBW phase (only valid in ProbeBW mode)
    pub probe_bw_phase: ProbeBWPhase,
    /// Timestamp when current ProbeBW phase began
    pub probe_bw_phase_start: f64,
    /// Round count when current ProbeBW phase began
    pub probe_bw_phase_start_round: usize,
    /// Max bandwidth at last full-bw check
    pub full_bw: f64,
    /// Number of rounds without sufficient bandwidth growth
    pub full_bw_count: usize,
    /// Whether full bandwidth is reached
    pub full_bw_reached: bool,
    /// Timestamp of last min_rtt update
    pub min_rtt_stamp: f64,
    /// Estimated extra ACKed bytes (ACK aggregation)
    pub extra_acked: f64,
    /// Recent extra_acked samples
    pub extra_acked_samples: VecDeque<(f64, f64)>, // (extra_acked, timestamp)
    /// Timestamp when ProbeRTT started
    pub probe_rtt_start: f64,
    /// When ProbeRTT can exit (0 if not scheduled)
    pub probe_rtt_done_stamp: f64,
    /// Whether we've completed a ProbeRTT round
    pub probe_rtt_round_done: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum BBRMode {
    #[default]
    Startup,
    Drain,
    ProbeBW,
    ProbeRTT,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum ProbeBWPhase {
    #[default]
    Down,
    Cruise,
    Refill,
    Up,
}

impl BBRState {
    const STARTUP_PACING_GAIN: f64 = 2.77;
    const DRAIN_PACING_GAIN: f64 = 0.35;
    const PROBE_BW_PACING_GAIN_DOWN: f64 = 0.90;
    const PROBE_BW_PACING_GAIN_CRUISE: f64 = 1.0;
    const PROBE_BW_PACING_GAIN_REFILL: f64 = 1.0;
    const PROBE_BW_PACING_GAIN_UP: f64 = 1.25;
    const PROBE_BW_CWND_GAIN_UP: f64 = 2.25;
    const CWND_GAIN: f64 = 2.0;
    const PROBE_RTT_CWND_GAIN: f64 = 0.5;
    const MIN_RTT_FILTER_SEC: f64 = 10.0;
    const PROBE_RTT_INTERVAL_SEC: f64 = 5.0;
    const PROBE_RTT_DURATION_SEC: f64 = 0.2;
    const FULL_BW_THRESH: f64 = 1.25;
    const FULL_BW_CNT: usize = 3;
    const LOSS_THRESH: f64 = 0.02;
    const BETA: f64 = 0.7;

    pub fn new(mss: usize) -> Self {
        BBRState {
            mode: BBRMode::Startup,
            max_bw: 0.0,
            min_rtt: f64::INFINITY,
            inflight: 0,
            bw_samples: VecDeque::with_capacity(32),
            rtt_samples: VecDeque::with_capacity(10),
            pacing_rate: 0.0,
            cwnd: 10 * mss,   // Initial cwnd
            pacing_gain: Self::STARTUP_PACING_GAIN,
            cwnd_gain: Self::CWND_GAIN,
            inflight_hi: usize::MAX,
            inflight_lo: 0,
            inflight_shortterm: 0,
            inflight_longterm: 0,
            loss_in_round: false,
            ecn_in_round: false,
            round_count: 0,
            next_round_delivered: 0,
            round_start_time: 0.0,
            last_rtt_sample_time: 0.0,
            last_rtt: 0.0,
            mss,
            max_cwnd: 2_000_000 * mss, // 2M segments
            total_data_delivered: 0,
            round_delivered: 0,
            round_lost: 0,
            probe_bw_cycle: 0,
            probe_bw_phase: ProbeBWPhase::Down,
            probe_bw_phase_start: 0.0,
            probe_bw_phase_start_round: 0,
            full_bw: 0.0,
            full_bw_count: 0,
            full_bw_reached: false,
            min_rtt_stamp: 0.0,
            extra_acked: 0.0,
            extra_acked_samples: VecDeque::with_capacity(10),
            probe_rtt_start: 0.0,
            probe_rtt_done_stamp: 0.0,
            probe_rtt_round_done: false,
        }
    }

    pub fn min_cwnd(&self) -> usize {
        4 * self.mss
    }

    pub fn update_bandwidth(&mut self, bytes_acked: usize, rtt: f64, rate_sample: &RateSample) {
        // Calculate sample bandwidth (prefer delivery-rate sample when available)
        let bw_sample = if rate_sample.interval > 0.0 && rate_sample.delivered > 0 {
            rate_sample.delivered as f64 / rate_sample.interval
        } else {
            bytes_acked as f64 / rtt
        };

        // Skip app-limited samples unless they exceed current max
        if rate_sample.is_app_limited && bw_sample <= self.max_bw {
            return;
        }

        // Add to bandwidth samples with cycle index
        self.bw_samples.push_back((bw_sample, self.probe_bw_cycle));

        // Keep samples from the last 2 ProbeBW cycles
        let oldest_cycle = self.probe_bw_cycle.saturating_sub(1);
        self.bw_samples
            .retain(|&(_, cycle)| cycle >= oldest_cycle);

        // Update max_bw as windowed maximum over bw_samples
        self.max_bw = self
            .bw_samples
            .iter()
            .map(|&(bw, _)| bw)
            .fold(0.0, f64::max);
    }

    pub fn update_min_rtt(&mut self, rtt: f64, now: f64) {
        // Update RTT samples
        self.rtt_samples.push_back((rtt, now));

        // Remove old samples outside the window
        let window_duration = Self::MIN_RTT_FILTER_SEC;
        self.rtt_samples
            .retain(|&(_, t)| now - t <= window_duration);

        // Update min_rtt as windowed minimum over specified time
        let prev_min_rtt = self.min_rtt;
        self.min_rtt = self
            .rtt_samples
            .iter()
            .map(|&(rtt_sample, _)| rtt_sample)
            .fold(f64::INFINITY, f64::min);

        if self.min_rtt < prev_min_rtt {
            self.min_rtt_stamp = now;
        }
    }

    pub fn update_extra_acked(&mut self, rate_sample: &RateSample, now: f64) {
        if !self.min_rtt.is_finite() || self.max_bw == 0.0 || rate_sample.interval <= 0.0 {
            return;
        }

        let expected = self.max_bw * rate_sample.interval;
        let delivered = rate_sample.delivered as f64;
        let extra = (delivered - expected).max(0.0);

        self.extra_acked_samples.push_back((extra, now));
        if self.extra_acked_samples.len() > 10 {
            self.extra_acked_samples.pop_front();
        }

        // Keep extra_acked samples within the min RTT filter window
        self.extra_acked_samples
            .retain(|&(_, t)| now - t <= Self::MIN_RTT_FILTER_SEC);
        self.extra_acked = self
            .extra_acked_samples
            .iter()
            .map(|&(val, _)| val)
            .fold(0.0, f64::max);
    }

    pub fn update_round(&mut self, ack_seq: usize) -> bool {
        let mut new_round = false;
        // Initialize next_round_delivered if it's zero
        if self.next_round_delivered == 0 {
            self.next_round_delivered = self.total_data_delivered;
        }

        // A new round trip has started if the ACKed sequence is beyond next_round_delivered
        if ack_seq >= self.next_round_delivered {
            new_round = true;
            self.round_count += 1;
            self.next_round_delivered = self.total_data_delivered;

            // Reset per-round variables
            self.loss_in_round = false;
            self.ecn_in_round = false;
            self.round_start_time = self.last_rtt_sample_time;
        }

        new_round
    }

    pub fn on_ack_received(&mut self, event: &AckEvent) {
        let ack_seq = event.ack_seq;
        let bytes_acked = event.bytes_acked;
        let rtt = event.rtt;
        let now = event.now;
        let loss_occurred = event.rate_sample.lost > 0;
        let ecn_marked = event.rate_sample.ecn_marked;

        self.total_data_delivered = ack_seq;
        self.last_rtt_sample_time = now;
        self.last_rtt = rtt;
        let new_round = self.update_round(ack_seq);
        if new_round {
            self.handle_round_end(now, event.rate_sample.prior_inflight);
        }
        self.update_bandwidth(bytes_acked, rtt, &event.rate_sample);
        self.update_min_rtt(rtt, now);
        self.update_extra_acked(&event.rate_sample, now);

        self.round_delivered = self.round_delivered.saturating_add(bytes_acked);
        self.round_lost = self
            .round_lost
            .saturating_add(event.rate_sample.lost);

        if new_round {
            self.update_full_bw();
            if self.mode == BBRMode::ProbeBW {
                self.maybe_advance_probe_bw_phase(now, event.rate_sample.prior_inflight);
            }
            if self.mode == BBRMode::ProbeRTT {
                self.probe_rtt_round_done = true;
            }
        }

        if loss_occurred {
            self.loss_in_round = true;
        }

        if ecn_marked {
            self.ecn_in_round = true;
        }

        // Update pacing rate and cwnd
        self.calculate_pacing_rate();
        self.calculate_cwnd();

        // Mode transitions
        self.check_mode_transitions(now);

        // Update inflight
        self.inflight = self.inflight.saturating_sub(bytes_acked);
    }

    pub fn calculate_pacing_rate(&mut self) {
        if self.pacing_rate == 0.0 && self.last_rtt > 0.0 && self.cwnd > 0 {
            self.pacing_rate = self.cwnd as f64 / self.last_rtt;
        }

        let target_rate = self.max_bw * self.pacing_gain;
        if target_rate > 0.0 && (self.full_bw_reached || target_rate > self.pacing_rate) {
            self.pacing_rate = target_rate;
        }
    }

    pub fn calculate_cwnd(&mut self) {
        let bdp = self.max_bw * self.min_rtt;
        let target_cwnd = (bdp * self.cwnd_gain + self.extra_acked) as usize;

        let mut inflight_cap = usize::MAX;
        if self.inflight_longterm > 0 {
            inflight_cap = inflight_cap.min(self.inflight_longterm);
        }
        if self.inflight_shortterm > 0 {
            inflight_cap = inflight_cap.min(self.inflight_shortterm);
        }
        if self.inflight_hi != usize::MAX {
            inflight_cap = inflight_cap.min(self.inflight_hi);
        }

        if inflight_cap != usize::MAX {
            self.cwnd = target_cwnd.min(inflight_cap);
        } else {
            self.cwnd = target_cwnd;
        }

        // Enforce minimum and maximum cwnd
        self.cwnd = self.cwnd.clamp(self.min_cwnd(), self.max_cwnd);
    }

    pub fn check_mode_transitions(&mut self, now: f64) {
        if self.mode != BBRMode::ProbeRTT
            && self.min_rtt.is_finite()
            && self.min_rtt_stamp > 0.0
            && now - self.min_rtt_stamp > Self::PROBE_RTT_INTERVAL_SEC
        {
            self.enter_probe_rtt(now);
            return;
        }

        match self.mode {
            BBRMode::Startup => {
                if self.full_bw_reached {
                    self.enter_drain();
                }
            }
            BBRMode::Drain => {
                let bdp = self.max_bw * self.min_rtt;
                if self.inflight <= (bdp as usize) {
                    self.enter_probe_bw(now);
                }
            }
            BBRMode::ProbeBW => {}
            BBRMode::ProbeRTT => {
                self.handle_probe_rtt(now);
            }
        }
    }

    fn update_full_bw(&mut self) {
        if self.mode != BBRMode::Startup {
            return;
        }

        if self.full_bw == 0.0 {
            self.full_bw = self.max_bw;
            self.full_bw_count = 0;
            return;
        }

        if self.max_bw >= self.full_bw * Self::FULL_BW_THRESH {
            self.full_bw = self.max_bw;
            self.full_bw_count = 0;
        } else {
            self.full_bw_count += 1;
            if self.full_bw_count >= Self::FULL_BW_CNT {
                self.full_bw_reached = true;
            }
        }
    }

    fn target_inflight(&self) -> usize {
        let bdp = self.max_bw * self.min_rtt;
        let target = (bdp * self.cwnd_gain + self.extra_acked) as usize;
        target.clamp(self.min_cwnd(), self.max_cwnd)
    }

    fn handle_round_end(&mut self, now: f64, prior_inflight: usize) {
        if self.round_delivered == 0 && self.round_lost == 0 {
            return;
        }

        let loss_rate = self.round_lost as f64
            / (self.round_lost + self.round_delivered).max(1) as f64;

        if loss_rate > Self::LOSS_THRESH {
            let target = self.target_inflight() as f64;
            let candidate = (target * Self::BETA)
                .max(prior_inflight as f64)
                .max(self.min_cwnd() as f64) as usize;

            if self.inflight_longterm == 0 {
                self.inflight_longterm = candidate;
            } else {
                self.inflight_longterm = self.inflight_longterm.min(candidate);
            }

            if self.inflight_shortterm == 0 {
                self.inflight_shortterm = candidate;
            } else {
                self.inflight_shortterm = self.inflight_shortterm.min(candidate);
            }

            if self.mode == BBRMode::ProbeBW && self.probe_bw_phase == ProbeBWPhase::Up {
                self.probe_bw_phase = ProbeBWPhase::Down;
                self.probe_bw_phase_start = now;
                self.probe_bw_phase_start_round = self.round_count;
                self.pacing_gain = Self::PROBE_BW_PACING_GAIN_DOWN;
                self.cwnd_gain = Self::CWND_GAIN;
                self.probe_bw_cycle = self.probe_bw_cycle.saturating_add(1);
            }
        }

        self.round_delivered = 0;
        self.round_lost = 0;
    }

    fn enter_startup(&mut self) {
        self.mode = BBRMode::Startup;
        self.pacing_gain = Self::STARTUP_PACING_GAIN;
        self.cwnd_gain = Self::CWND_GAIN;
        self.full_bw = 0.0;
        self.full_bw_count = 0;
        self.full_bw_reached = false;
        self.inflight_shortterm = 0;
        self.inflight_longterm = 0;
    }

    fn enter_drain(&mut self) {
        self.mode = BBRMode::Drain;
        self.pacing_gain = Self::DRAIN_PACING_GAIN;
        self.cwnd_gain = Self::CWND_GAIN;
    }

    fn enter_probe_bw(&mut self, now: f64) {
        self.mode = BBRMode::ProbeBW;
        self.probe_bw_phase = ProbeBWPhase::Down;
        self.probe_bw_cycle = self.probe_bw_cycle.saturating_add(1);
        self.probe_bw_phase_start = now;
        self.probe_bw_phase_start_round = self.round_count;
        self.pacing_gain = Self::PROBE_BW_PACING_GAIN_DOWN;
        self.cwnd_gain = Self::CWND_GAIN;
        self.inflight_shortterm = 0;
    }

    fn maybe_advance_probe_bw_phase(&mut self, now: f64, prior_inflight: usize) {
        let rounds_in_phase = self.round_count.saturating_sub(self.probe_bw_phase_start_round);
        let target = self.target_inflight();

        let next_phase = match self.probe_bw_phase {
            ProbeBWPhase::Down => {
                if prior_inflight <= target || rounds_in_phase >= 1 {
                    Some(ProbeBWPhase::Cruise)
                } else {
                    None
                }
            }
            ProbeBWPhase::Cruise => {
                if rounds_in_phase >= 1 {
                    Some(ProbeBWPhase::Refill)
                } else {
                    None
                }
            }
            ProbeBWPhase::Refill => {
                if rounds_in_phase >= 1 {
                    Some(ProbeBWPhase::Up)
                } else {
                    None
                }
            }
            ProbeBWPhase::Up => {
                if rounds_in_phase >= 1 {
                    Some(ProbeBWPhase::Down)
                } else {
                    None
                }
            }
        };

        if let Some(next_phase) = next_phase {
            self.probe_bw_phase = next_phase;
            if self.probe_bw_phase == ProbeBWPhase::Down {
                self.probe_bw_cycle = self.probe_bw_cycle.saturating_add(1);
                self.inflight_shortterm = 0;
            }
            self.probe_bw_phase_start = now;
            self.probe_bw_phase_start_round = self.round_count;
            self.pacing_gain = match self.probe_bw_phase {
                ProbeBWPhase::Down => Self::PROBE_BW_PACING_GAIN_DOWN,
                ProbeBWPhase::Cruise => Self::PROBE_BW_PACING_GAIN_CRUISE,
                ProbeBWPhase::Refill => Self::PROBE_BW_PACING_GAIN_REFILL,
                ProbeBWPhase::Up => Self::PROBE_BW_PACING_GAIN_UP,
            };
            self.cwnd_gain = match self.probe_bw_phase {
                ProbeBWPhase::Up => Self::PROBE_BW_CWND_GAIN_UP,
                _ => Self::CWND_GAIN,
            };
        }
    }

    fn enter_probe_rtt(&mut self, now: f64) {
        self.mode = BBRMode::ProbeRTT;
        self.pacing_gain = 1.0;
        self.cwnd_gain = Self::PROBE_RTT_CWND_GAIN;
        self.probe_rtt_start = now;
        self.probe_rtt_done_stamp = 0.0;
        self.probe_rtt_round_done = false;
    }

    fn handle_probe_rtt(&mut self, now: f64) {
        let bdp = self.max_bw * self.min_rtt;
        let target_inflight = self
            .min_cwnd()
            .max((bdp * Self::PROBE_RTT_CWND_GAIN) as usize);

        if self.probe_rtt_done_stamp == 0.0 {
            if self.inflight <= target_inflight {
                self.probe_rtt_done_stamp = now + Self::PROBE_RTT_DURATION_SEC;
            }
            return;
        }

        if self.inflight > target_inflight {
            self.probe_rtt_done_stamp = 0.0;
            self.probe_rtt_round_done = false;
            return;
        }

        if now >= self.probe_rtt_done_stamp && self.probe_rtt_round_done {
            self.min_rtt_stamp = now;
            if self.full_bw_reached {
                self.enter_probe_bw(now);
            } else {
                self.enter_startup();
            }
        }
    }

    pub fn on_packet_sent(&mut self, bytes: usize, now: f64) {
        self.last_rtt_sample_time = now;
        self.inflight = self.inflight.saturating_add(bytes);
    }

    pub fn on_loss_detected(&mut self) {
        self.loss_in_round = true;
    }

    pub fn on_timer_expired(&mut self) {
        // Handle RTO event
        self.enter_startup();
        self.inflight_hi = usize::MAX;
        self.inflight_shortterm = 0;
        self.inflight_longterm = 0;
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
    fn ack_received(&mut self, event: AckEvent) {
        self.state.on_ack_received(&event);
    }

    fn packet_sent(&mut self, bytes: usize, now: f64) {
        self.state.on_packet_sent(bytes, now);
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
        let rate_sample = RateSample {
            delivered: bytes_acked,
            interval: rtt,
            rtt,
            acked: bytes_acked,
            lost: if loss_occurred { bytes_acked } else { 0 },
            ecn_marked,
            ..RateSample::default()
        };
        let event = AckEvent {
            ack_seq,
            rtt,
            now,
            bytes_acked,
            rate_sample,
        };
        bbr.state.on_ack_received(&event);
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

        // Drive full_bw detection to exit Startup and enter ProbeBW
        for i in 1..=5 {
            let now = i as f64 * 0.1;
            let rtt = 0.1;
            let bytes_acked = 1024;
            let ack_seq = i * bytes_acked;

            simulate_ack(&mut bbr, ack_seq, rtt, now, bytes_acked, false, false);
        }

        assert_eq!(bbr.state.mode, BBRMode::ProbeBW);
        assert_eq!(bbr.state.probe_bw_phase, ProbeBWPhase::Down);

        // Next ACK should advance ProbeBW phase
        simulate_ack(&mut bbr, 6 * 1024, 0.1, 0.6, 1024, false, false);
        assert_eq!(bbr.state.probe_bw_phase, ProbeBWPhase::Cruise);
    }

    #[test]
    fn test_probe_rtt_entry() {
        let mut bbr = TCPBBR::new();
        bbr.state.min_rtt = 0.1;
        bbr.state.min_rtt_stamp = 1.0;
        bbr.state.mode = BBRMode::ProbeBW;

        // Force min_rtt expiration
        simulate_ack(&mut bbr, 1024, 0.1, 7.0, 1024, false, false);
        assert_eq!(bbr.state.mode, BBRMode::ProbeRTT);
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

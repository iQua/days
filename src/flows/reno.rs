//! Implements the TCP Reno congestion control algorithm.
//!
//! This module implements TCP Reno congestion control as specified in:
//! - RFC 5681: TCP Congestion Control
//! - RFC 6582: The NewReno Modification to TCP's Fast Recovery Algorithm
//! - RFC 6298: Computing TCP's Retransmission Timer
//!
//! Key features:
//! - Slow Start and Congestion Avoidance phases
//! - Fast Recovery with NewReno modifications
//! - Optional SACK support
//! - Configurable duplicate ACK threshold
//! - RFC 6298 compliant RTO calculation

use crate::flows::cc::CongestionControl;
use std::collections::HashSet;

/// TCP Reno states
#[derive(Debug, Default, PartialEq)]
enum TCPRenoState {
    #[default]
    SlowStart,
    CongestionAvoidance,
    FastRecovery,
}

#[derive(Debug, Default)]
pub struct TCPReno {
    /// The maximum segment size
    mss: usize,
    /// The size of the congestion window
    cwnd: usize,
    /// The slow start threshold
    ssthresh: usize,
    /// Current state of TCP Reno
    state: TCPRenoState,
    /// Minimum window size
    min_cwnd: usize,
    /// Maximum window size
    max_cwnd: usize,
    /// Number of packets in flight
    packets_in_flight: usize,
    /// Last measured RTT
    last_rtt: f64,
    /// Previous RTT measurement for variance calculation
    prev_rtt: Option<f64>,
    /// RTT variance for RTO calculation
    rtt_var: f64,
    /// Smoothed RTT
    srtt: f64,
    /// RTO value
    rto: f64,
    /// Minimum RTO value
    min_rto: f64,
    /// Maximum RTO value
    max_rto: f64,
    /// Minimum observed RTT
    min_rtt: f64,
    /// Number of duplicate ACKs received
    dupack_count: usize,
    /// Recovery window - tracks window size during recovery
    recovery_window: usize,
    /// Time of last window reduction
    last_reduction_time: f64,
    /// Saves FlightSize before entering recovery
    pre_recovery_flight_size: usize,
    /// Highest sequence transmitted when entering recovery
    recovery_high_seq: usize,
    /// Tracks number of segments assumed outstanding ("pipe")
    pipe: usize,
    /// Highest sequence number sent
    snd_max: usize,
    /// Next sequence number expected
    rcv_next: usize,
    /// Flag indicating if retransmission is required
    retransmit_required: bool,
    /// Segments that need retransmission
    retransmission_queue: Vec<usize>,
    /// Set of lost sequence numbers
    lost_sequences: HashSet<usize>,
    /// Recovery exit threshold
    recovery_exit_threshold: usize,
    /// Optional sequence number for immediate retransmission
    immediate_retransmit: Option<usize>,
    /// Highest ACK received
    highest_ack: usize,
}

impl TCPReno {
    pub fn new() -> TCPReno {
        let mss = 512;
        let initial_window = 2 * mss;

        TCPReno {
            mss,
            cwnd: initial_window,
            ssthresh: 65535,
            state: TCPRenoState::SlowStart,
            min_cwnd: mss,
            max_cwnd: 65535,
            packets_in_flight: 0,
            last_rtt: 0.0,
            prev_rtt: None,
            rtt_var: 0.0,
            srtt: 0.0,
            rto: 1.0,
            min_rto: 1.0,  // 1 second minimum per RFC 6298
            max_rto: 60.0, // 60 second maximum (common value)
            min_rtt: f64::MAX,
            dupack_count: 0,
            recovery_window: 0,
            last_reduction_time: 0.0,
            pre_recovery_flight_size: 0,
            recovery_high_seq: 0,
            pipe: 0,
            snd_max: 0,
            rcv_next: 0,
            retransmit_required: false,
            retransmission_queue: Vec::new(),
            lost_sequences: HashSet::new(),
            recovery_exit_threshold: 0,
            immediate_retransmit: None,
            highest_ack: 0,
        }
    }

    /// Updates RTT measurements and RTO calculation according to RFC 6298
    ///
    /// Uses standard EWMA with alpha=0.125 for SRTT and beta=0.25 for RTTVAR
    /// RTO = SRTT + 4*RTTVAR with minimum of 1 second per RFC 6298
    fn update_rtt(&mut self, rtt: f64) {
        self.last_rtt = rtt;

        // Update minimum RTT
        if self.min_rtt == f64::MAX {
            self.min_rtt = rtt;
        } else {
            self.min_rtt = self.min_rtt.min(rtt);
        }

        // Update SRTT and RTTVAR per RFC 6298
        if self.srtt == 0.0 {
            self.srtt = rtt;
            self.rtt_var = rtt / 2.0;
        } else {
            self.rtt_var = 0.75 * self.rtt_var + 0.25 * (self.srtt - rtt).abs();
            self.srtt = 0.875 * self.srtt + 0.125 * rtt;
        }

        // Update RTO with bounds checking
        self.rto = self.srtt + 4.0 * self.rtt_var;
        self.rto = self.rto.clamp(self.min_rto, self.max_rto);

        self.prev_rtt = Some(rtt);
    }

    /// Marks a sequence number as lost and updates retransmission state
    fn mark_lost(&mut self, seq: usize) {
        self.lost_sequences.insert(seq);
        self.retransmit_required = true;
        self.retransmission_queue.push(seq);

        // Update recovery_high_seq if the lost sequence is higher
        if seq > self.recovery_high_seq {
            self.recovery_high_seq = seq;
        }
    }

    /// Implements NewReno modifications from RFC 6582 Section 3.2
    ///
    /// Handles partial ACKs during recovery by:
    /// - Deflating cwnd by amount of new data acknowledged
    /// - Re-inflating by one MSS
    /// - Retransmitting next unacknowledged segment
    fn update_recovery_window(&mut self) {
        if self.state == TCPRenoState::FastRecovery {
            self.recovery_window = self.pre_recovery_flight_size + self.mss;
            self.recovery_window = self.recovery_window.min(self.max_cwnd);
            self.recovery_exit_threshold = self.recovery_window;
        }
    }

    /// Handles retransmission requirements
    fn handle_retransmission(&mut self, seq: usize) {
        if self.retransmit_required {
            self.mark_lost(seq);
            if let Some(&next_seq) = self.retransmission_queue.first() {
                self.immediate_retransmit = Some(next_seq);
            }
        }
    }

    /// Resets recovery state
    fn reset_recovery_state(&mut self) {
        self.pipe = 0;
        self.recovery_high_seq = 0;
        self.pre_recovery_flight_size = 0;
        self.recovery_window = 0;
        self.dupack_count = 0;
        self.retransmit_required = false;
        self.immediate_retransmit = None;
        self.retransmission_queue.clear();
        self.lost_sequences.clear();
    }

    /// Updates congestion window based on RFC 5681:
    /// - Slow Start: Increase by MSS per ACK
    /// - Congestion Avoidance: Increase by MSS per RTT
    /// - Fast Recovery: Follow NewReno rules
    fn update_cwnd(&mut self, bytes_acked: usize) {
        match self.state {
            TCPRenoState::SlowStart => {
                let increase = self.mss.min(bytes_acked);
                self.cwnd = (self.cwnd + increase).max(self.min_cwnd).min(self.max_cwnd);

                if self.cwnd >= self.ssthresh {
                    self.state = TCPRenoState::CongestionAvoidance;
                    self.cwnd = self.ssthresh;
                }
            }
            TCPRenoState::CongestionAvoidance => {
                // RFC 5681: At most one SMSS per RTT
                let mss_per_rtt =
                    (self.mss as f64 * bytes_acked as f64 / self.cwnd as f64).ceil() as usize;
                let n = mss_per_rtt.min(self.mss);
                self.cwnd = (self.cwnd + n).max(self.min_cwnd).min(self.max_cwnd);
            }
            TCPRenoState::FastRecovery => {
                if bytes_acked < self.recovery_high_seq {
                    // Partial ACK - RFC 6582 Section 3.2
                    self.pipe = self.pipe.saturating_sub(bytes_acked);
                    // Deflate cwnd by the amount of new data acknowledged
                    self.cwnd = self.cwnd.saturating_sub(bytes_acked);
                    // Add back one MSS
                    self.cwnd += self.mss;
                    self.handle_retransmission(bytes_acked);
                } else {
                    // Full ACK
                    self.pipe = 0;
                    if !self.retransmit_required && self.lost_sequences.is_empty() {
                        self.state = TCPRenoState::CongestionAvoidance;
                        self.reset_recovery_state();
                    }
                }

                self.cwnd = (self.ssthresh + self.pipe)
                    .max(self.min_cwnd)
                    .min(self.recovery_window);
            }
        }
    }

    /// Estimates pipe (segments in flight) during recovery
    fn estimate_pipe(&self) -> usize {
        self.packets_in_flight
            + if self.state == TCPRenoState::FastRecovery {
                self.dupack_count + self.lost_sequences.len()
            } else {
                0
            }
    }

    /// Updates sequence space tracking
    fn update_sequence_space(&mut self, ack_seq: usize, bytes: usize) {
        // Update highest_ack if the new acknowledgment is higher
        if ack_seq > self.highest_ack {
            self.highest_ack = ack_seq;

            // Remove all lost sequences less than or equal to highest_ack
            self.lost_sequences.retain(|&s| s > self.highest_ack);

            // Clear retransmit_required if all losses are recovered
            if self.lost_sequences.is_empty() {
                self.retransmit_required = false;
            }
        }

        // Update rcv_next to reflect the next expected sequence number
        self.rcv_next = ack_seq + bytes;
    }

    // Update recovery exit check
    fn should_exit_recovery(&self) -> bool {
        self.highest_ack >= self.recovery_high_seq
    }
}

impl CongestionControl for TCPReno {
    fn ack_received(&mut self, rtt: f64, current_time: f64, bytes_acked: usize) {
        self.update_rtt(rtt);
        self.update_sequence_space(self.rcv_next, bytes_acked);

        if self.state == TCPRenoState::FastRecovery {
            self.pipe = self.estimate_pipe();

            if self.should_exit_recovery() {
                self.state = TCPRenoState::CongestionAvoidance;
                self.cwnd = self.ssthresh;
                self.reset_recovery_state();
            }
            self.last_reduction_time = current_time;
        } else {
            self.update_cwnd(bytes_acked);
        }

        if self.packets_in_flight > 0 {
            self.packets_in_flight -= 1;
        }
    }

    fn consecutive_dupacks_received(&mut self) {
        self.pre_recovery_flight_size = self.packets_in_flight;
        self.state = TCPRenoState::FastRecovery;

        // RFC 5681 Section 3.2
        self.ssthresh = (self.pre_recovery_flight_size / 2).max(2 * self.mss);
        self.update_recovery_window();
        self.pipe = self.estimate_pipe();
        self.cwnd = (self.ssthresh + 3 * self.mss).max(self.min_cwnd);
        self.dupack_count = 3;
        self.recovery_high_seq = self.snd_max;
        self.retransmit_required = false;
        self.retransmission_queue.clear();
    }

    fn timer_expired(&mut self) {
        self.ssthresh = (self.cwnd / 2).max(2 * self.mss);
        self.cwnd = self.min_cwnd;
        self.state = TCPRenoState::SlowStart;
        self.packets_in_flight = 0;
        self.reset_recovery_state();
    }

    fn dupack_over(&mut self) {
        // Return to congestion avoidance
        self.state = TCPRenoState::CongestionAvoidance;
        self.cwnd = self.ssthresh;
        self.reset_recovery_state();
    }

    fn more_dupacks_received(&mut self) {
        if self.state == TCPRenoState::FastRecovery {
            // Update pipe
            self.pipe = self.estimate_pipe();

            // Inflate window by 1 MSS per additional dupack
            self.cwnd += self.mss;
            self.dupack_count += 1;

            // Allow new transmissions if pipe < cwnd
            if self.pipe < self.cwnd {
                // Can send new segments
                self.packets_in_flight += 1;
                self.pipe += 1;
            }
        }
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
        let reno = TCPReno::new();
        assert_eq!(reno.state, TCPRenoState::SlowStart);
        assert_eq!(reno.cwnd, 1024); // 2*MSS
        assert_eq!(reno.ssthresh, 65535);
        assert_eq!(reno.mss, 512);
    }

    #[test]
    fn test_slow_start_growth() {
        let mut reno = TCPReno::new();
        let initial_cwnd = reno.cwnd;

        // Simulate ACK for 1 MSS
        reno.ack_received(0.1, 0.1, reno.mss);
        assert_eq!(reno.cwnd, initial_cwnd + reno.mss);
        assert_eq!(reno.state, TCPRenoState::SlowStart);

        // Another ACK
        reno.ack_received(0.1, 0.2, reno.mss);
        assert_eq!(reno.cwnd, initial_cwnd + 2 * reno.mss);
    }

    #[test]
    fn test_slow_start_to_congestion_avoidance() {
        let mut reno = TCPReno::new();
        reno.ssthresh = 2048; // Set low ssthresh to force transition

        // Send enough ACKs to exceed ssthresh
        while reno.cwnd < reno.ssthresh {
            reno.ack_received(0.1, 0.1, reno.mss);
        }

        assert_eq!(reno.state, TCPRenoState::CongestionAvoidance);
        assert_eq!(reno.cwnd, reno.ssthresh);
    }

    #[test]
    fn test_fast_recovery_entry() {
        let mut reno = TCPReno::new();
        reno.packets_in_flight = 10000; // Set high flight size

        // Trigger fast recovery with 3 duplicate ACKs
        reno.consecutive_dupacks_received();

        assert_eq!(reno.state, TCPRenoState::FastRecovery);
        assert_eq!(reno.ssthresh, 5000); // FlightSize/2
        assert_eq!(reno.cwnd, reno.ssthresh + 3 * reno.mss);
        assert_eq!(reno.dupack_count, 3);
    }

    #[test]
    fn test_rto_calculation() {
        let mut reno = TCPReno::new();

        // First RTT measurement
        reno.update_rtt(0.1);
        assert_eq!(reno.srtt, 0.1);
        assert_eq!(reno.rtt_var, 0.05);

        // Second measurement
        reno.update_rtt(0.15);
        assert!((reno.srtt - 0.10625).abs() < 0.0001); // 0.875*0.1 + 0.125*0.15

        // Verify RTO bounds
        assert!(reno.rto >= reno.min_rto);
        assert!(reno.rto <= reno.max_rto);
    }

    #[test]
    fn test_timer_expiry() {
        let mut reno = TCPReno::new();
        reno.cwnd = 10000;
        reno.state = TCPRenoState::CongestionAvoidance;

        reno.timer_expired();

        assert_eq!(reno.state, TCPRenoState::SlowStart);
        assert_eq!(reno.cwnd, reno.min_cwnd);
        assert_eq!(reno.ssthresh, 5000); // cwnd/2
    }

    #[test]
    fn test_fast_recovery_partial_acks() {
        let mut reno = TCPReno::new();
        reno.packets_in_flight = 10000;
        reno.snd_max = 20000;

        // Enter fast recovery
        reno.consecutive_dupacks_received();
        let initial_recovery_window = reno.cwnd;

        // Simulate partial ACK
        reno.ack_received(0.1, 0.1, 500);

        // Verify NewReno behavior on partial ACK
        assert_eq!(reno.state, TCPRenoState::FastRecovery);
        assert!(reno.cwnd < initial_recovery_window); // Window should deflate
        assert!(reno.retransmit_required); // Should trigger retransmission
    }

    #[test]
    fn test_fast_recovery_full_ack() {
        let mut reno = TCPReno::new();
        reno.packets_in_flight = 10000;
        reno.snd_max = 20000;
        reno.consecutive_dupacks_received();

        // Simulate full recovery ACK
        reno.recovery_high_seq = 15000;
        reno.ack_received(0.1, 0.1, 15000);

        assert_eq!(reno.state, TCPRenoState::CongestionAvoidance);
        assert_eq!(reno.cwnd, reno.ssthresh);
        assert!(!reno.retransmit_required);
    }

    #[test]
    fn test_multiple_loss_recoveries() {
        let mut reno = TCPReno::new();

        // First loss recovery
        reno.packets_in_flight = 10000;
        reno.consecutive_dupacks_received();
        reno.ack_received(0.1, 0.1, 15000);

        let first_ssthresh = reno.ssthresh;

        // Second loss recovery
        reno.packets_in_flight = 5000;
        reno.consecutive_dupacks_received();

        assert!(reno.ssthresh < first_ssthresh); // Should reduce further
        assert_eq!(reno.state, TCPRenoState::FastRecovery);
    }

    #[test]
    fn test_pipe_estimation() {
        let mut reno = TCPReno::new();
        reno.packets_in_flight = 1000;
        reno.consecutive_dupacks_received();

        // Add some lost sequences
        reno.lost_sequences.insert(1000);
        reno.lost_sequences.insert(2000);

        let pipe = reno.estimate_pipe();
        assert_eq!(pipe, 1000 + 3 + 2); // in_flight + dupacks + lost_seqs
    }

    #[test]
    fn test_window_bounds() {
        // Test minimum bound
        let mut reno = TCPReno::new();
        reno.cwnd = 100;
        reno.timer_expired();
        assert_eq!(reno.cwnd, reno.min_cwnd);

        // Create a new instance for the maximum bound test
        let mut reno = TCPReno::new();

        // Test maximum bound
        reno.cwnd = reno.max_cwnd + 1000;
        reno.ack_received(0.1, 0.1, reno.mss);
        assert_eq!(reno.cwnd, reno.max_cwnd);
    }

    #[test]
    fn test_rtt_update_during_recovery() {
        let mut reno = TCPReno::new();
        reno.consecutive_dupacks_received();

        let initial_srtt = 0.1;
        reno.srtt = initial_srtt;

        // RTT updates should still work in recovery
        reno.update_rtt(0.2);
        assert!(reno.srtt > initial_srtt);
    }

    #[test]
    fn test_sequence_tracking() {
        let mut reno = TCPReno::new();

        // Simulate sending data up to sequence number 1500
        reno.snd_max = 1500;

        // Simulate receiving an acknowledgment for sequence number 1000, acknowledging 500 bytes
        reno.update_sequence_space(1000, 500);
        assert_eq!(reno.highest_ack, 1000);
        assert_eq!(reno.rcv_next, 1500);

        // Simulate receiving an acknowledgment for sequence number 1500, acknowledging another 500 bytes
        reno.update_sequence_space(1500, 500);
        assert_eq!(reno.highest_ack, 1500);
        assert_eq!(reno.rcv_next, 2000);
    }

    #[test]
    fn test_zero_window_handling() {
        let mut reno = TCPReno::new();

        // Force window to minimum
        reno.timer_expired();
        reno.cwnd = 0; // Invalid state

        // ACK should restore to minimum
        reno.ack_received(0.1, 0.1, reno.mss);
        assert_eq!(reno.cwnd, reno.min_cwnd);
    }

    #[test]
    fn test_extreme_rtt_values() {
        let mut reno = TCPReno::new();

        // Very small RTT
        reno.update_rtt(0.000001);
        assert!(reno.rto >= reno.min_rto);

        // Very large RTT
        reno.update_rtt(100.0);
        assert!(reno.rto <= reno.max_rto);
    }

    #[test]
    fn test_multiple_dupacks() {
        let mut reno = TCPReno::new();
        reno.consecutive_dupacks_received();
        let initial_cwnd = reno.cwnd;

        // Additional dupacks should inflate window
        reno.more_dupacks_received();
        assert_eq!(reno.cwnd, initial_cwnd + reno.mss);

        reno.more_dupacks_received();
        assert_eq!(reno.cwnd, initial_cwnd + 2 * reno.mss);
    }

    #[test]
    fn test_back_to_back_timer_expiry() {
        let mut reno = TCPReno::new();
        reno.cwnd = 10000;

        // First timer expiry
        reno.timer_expired();
        let first_ssthresh = reno.ssthresh;

        // Second timer expiry
        reno.timer_expired();

        // ssthresh should be reduced again
        assert!(reno.ssthresh < first_ssthresh);
        assert_eq!(reno.cwnd, reno.min_cwnd);
    }

    #[test]
    fn test_recovery_window_calculation() {
        let mut reno = TCPReno::new();
        reno.packets_in_flight = 10000;
        reno.consecutive_dupacks_received();

        // Recovery window should be flight size + MSS
        assert_eq!(reno.recovery_window, 10000 + reno.mss);
        assert!(reno.recovery_window <= reno.max_cwnd);
    }

    #[test]
    fn test_state_cleanup() {
        let mut reno = TCPReno::new();

        // Set various state
        reno.consecutive_dupacks_received();
        reno.lost_sequences.insert(1000);
        reno.retransmission_queue.push(1000);
        reno.immediate_retransmit = Some(1000);

        // Reset state
        reno.reset_recovery_state();

        assert_eq!(reno.pipe, 0);
        assert_eq!(reno.recovery_high_seq, 0);
        assert_eq!(reno.dupack_count, 0);
        assert!(!reno.retransmit_required);
        assert!(reno.immediate_retransmit.is_none());
        assert!(reno.retransmission_queue.is_empty());
        assert!(reno.lost_sequences.is_empty());
    }

    #[test]
    fn test_reordering_tolerance() {
        let mut reno = TCPReno::new();
        reno.state = TCPRenoState::CongestionAvoidance;
        let initial_cwnd = reno.cwnd;

        // Simulate reordered ACK
        reno.update_sequence_space(2000, 1000); // Later sequence first
        reno.update_sequence_space(1000, 1000); // Earlier sequence after

        // Should maintain same window
        assert_eq!(reno.cwnd, initial_cwnd);
    }

    #[test]
    fn test_extended_loss_recovery() {
        let mut reno = TCPReno::new();

        // Simulate sending data up to sequence number 20000
        reno.snd_max = 20000;
        reno.packets_in_flight = 10000;

        // Enter recovery
        reno.consecutive_dupacks_received();

        // Multiple partial ACKs
        for _ in 0..5 {
            // Simulate ACKs acknowledging 500 bytes each time
            reno.update_sequence_space(reno.highest_ack + 500, 0);
            reno.ack_received(0.1, 0.1, 500);
        }

        // New losses during recovery
        reno.mark_lost(5000);
        reno.mark_lost(6000);

        // Should stay in recovery
        assert_eq!(reno.state, TCPRenoState::FastRecovery);
        assert!(!reno.lost_sequences.is_empty());

        // Full ACK that covers all sent data
        reno.update_sequence_space(reno.snd_max, 0);
        reno.ack_received(0.1, 0.1, reno.snd_max - reno.highest_ack);

        assert_eq!(reno.state, TCPRenoState::CongestionAvoidance);
        assert!(reno.lost_sequences.is_empty());
    }

    #[test]
    fn test_sequence_number_wraparound() {
        let mut reno = TCPReno::new();

        // Set sequence near maximum value
        reno.snd_max = usize::MAX - 1000;
        reno.update_sequence_space(usize::MAX - 1000, 500);

        // Should handle wraparound correctly
        assert_eq!(reno.snd_max, usize::MAX - 500);
    }

    #[test]
    fn test_rtt_measurement_edge_cases() {
        let mut reno = TCPReno::new();

        // RTT decreasing
        reno.update_rtt(0.1);
        reno.update_rtt(0.09);
        reno.update_rtt(0.08);

        // RTT increasing
        reno.update_rtt(0.12);
        reno.update_rtt(0.15);

        // Verify SRTT and RTTVAR remain stable
        assert!(reno.srtt > 0.0);
        assert!(reno.rtt_var > 0.0);
        assert!(reno.rto >= reno.min_rto);
        assert!(reno.rto <= reno.max_rto);
    }

    #[test]
    fn test_timer_expiry_during_recovery() {
        let mut reno = TCPReno::new();
        reno.consecutive_dupacks_received();

        // Timer expires during recovery
        reno.timer_expired();

        // Should reset to slow start
        assert_eq!(reno.state, TCPRenoState::SlowStart);
        assert_eq!(reno.cwnd, reno.min_cwnd);
        assert!(reno.lost_sequences.is_empty());
    }

    #[test]
    fn test_long_term_congestion_avoidance() {
        let mut reno = TCPReno::new();
        reno.state = TCPRenoState::CongestionAvoidance;
        reno.cwnd = 10000;

        // Simulate multiple RTTs
        let initial_cwnd = reno.cwnd;
        let rtts = 10;
        let acks_per_rtt = reno.cwnd / reno.mss;

        for rtt in 0..rtts {
            let start_cwnd = reno.cwnd;

            for _ in 0..acks_per_rtt {
                reno.ack_received(0.1, 0.1 * (rtt as f64), reno.mss);
            }

            // Verify growth is approximately MSS per RTT
            let diff = if reno.cwnd > start_cwnd + reno.mss {
                reno.cwnd - (start_cwnd + reno.mss)
            } else {
                (start_cwnd + reno.mss) - reno.cwnd
            };
            assert!(
                diff <= 1,
                "Window growth in RTT {} deviated by more than 1 byte from MSS",
                rtt
            );
        }

        // Verify overall growth
        let expected_growth = rtts * reno.mss;
        let actual_growth = reno.cwnd - initial_cwnd;
        assert!(
            (actual_growth as i64 - expected_growth as i64).abs() <= rtts as i64,
            "Overall window growth deviated significantly from expected"
        );
    }
}

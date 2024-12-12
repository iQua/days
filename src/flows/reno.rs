//! Implements the TCP Reno congestion control algorithm.

use crate::flows::cc::CongestionControl;
use std::collections::HashSet;

/// TCP Reno states
#[derive(Debug, PartialEq)]
enum TCPRenoState {
    SlowStart,
    CongestionAvoidance,
    FastRecovery,
}

impl Default for TCPRenoState {
    fn default() -> Self {
        TCPRenoState::SlowStart
    }
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
        }
    }

    /// Updates RTT measurements and RTO calculation per RFC 6298
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
    }

    /// Updates sequence space tracking
    fn update_sequence_space(&mut self, seq: usize, bytes: usize) {
        self.snd_max = self.snd_max.max(seq + bytes);
        if seq == self.rcv_next {
            self.rcv_next = seq + bytes;
            // Remove acknowledged sequences from lost set
            self.lost_sequences.remove(&seq);
        }
    }

    /// Updates recovery window management
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

    /// Updates the congestion window based on current state
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
}

impl CongestionControl for TCPReno {
    fn ack_received(&mut self, rtt: f64, current_time: f64, bytes_acked: usize) {
        self.update_rtt(rtt);
        self.update_sequence_space(self.rcv_next, bytes_acked);

        if self.state == TCPRenoState::FastRecovery {
            self.pipe = self.estimate_pipe();

            if self.pipe <= self.ssthresh && bytes_acked >= self.recovery_high_seq {
                if !self.retransmit_required && self.retransmission_queue.is_empty() {
                    self.state = TCPRenoState::CongestionAvoidance;
                    self.cwnd = self.ssthresh;
                    self.reset_recovery_state();
                }
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

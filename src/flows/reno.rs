//! Implements the TCP Reno congestion control algorithm.

use crate::flows::cc::CongestionControl;

/// TCP Reno states
#[derive(Debug, PartialEq)]
enum TCPRenoState {
    SlowStart,
    CongestionAvoidance,
    FastRecovery,
}

impl Default for TCPRenoState {
    fn default() -> Self {
        TCPRenoState::SlowStart // Default to slow start state
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
}

impl TCPReno {
    pub fn new() -> TCPReno {
        let mss = 512;
        let initial_window = 2 * mss;

        TCPReno {
            mss,
            cwnd: initial_window, // Use initial_window directly here
            ssthresh: 65535,
            state: TCPRenoState::SlowStart,
            min_cwnd: mss,
            max_cwnd: 65535,
            packets_in_flight: 0,
            last_rtt: 0.0,
            min_rtt: f64::MAX,
            dupack_count: 0,
            recovery_window: 0,
            last_reduction_time: 0.0,
            pre_recovery_flight_size: 0,
            recovery_high_seq: 0,
            pipe: 0,
        }
    }

    /// Updates the congestion window based on current state
    fn update_cwnd(&mut self, bytes_acked: usize) {
        match self.state {
            TCPRenoState::SlowStart => {
                let increase = self.mss.min(bytes_acked);
                self.cwnd = (self.cwnd + increase).max(self.min_cwnd).min(self.max_cwnd);

                // More precise exit condition
                if self.cwnd >= self.ssthresh {
                    self.state = TCPRenoState::CongestionAvoidance;
                    // Ensure smooth transition
                    self.cwnd = self.ssthresh;
                }

                if self.cwnd >= self.ssthresh {
                    let k = (self.cwnd / self.mss) / 2;
                    self.cwnd = (self.cwnd + self.mss * self.mss / k)
                        .max(self.min_cwnd)
                        .min(self.max_cwnd);
                }
            }
            TCPRenoState::CongestionAvoidance => {
                // RFC 5681: At most one SMSS per RTT
                let n = (bytes_acked * self.mss / self.cwnd).min(self.mss);
                self.cwnd = (self.cwnd + n).max(self.min_cwnd).min(self.max_cwnd);
            }
            TCPRenoState::FastRecovery => {
                // Track new data received during recovery
                if self.pipe > bytes_acked {
                    self.pipe -= bytes_acked;
                } else {
                    self.pipe = 0;
                }

                // Deflate window by amount of new data received
                self.cwnd = (self.ssthresh + self.pipe).max(self.min_cwnd);
            }
        }
    }

    /// Updates RTT tracking
    fn update_rtt(&mut self, rtt: f64) {
        self.last_rtt = rtt;
        self.min_rtt = self.min_rtt.min(rtt);
    }

    /// Estimates pipe (segments in flight) during recovery
    fn estimate_pipe(&self) -> usize {
        self.packets_in_flight
    }
}

impl CongestionControl for TCPReno {
    fn ack_received(&mut self, rtt: f64, current_time: f64) {
        self.update_rtt(rtt);

        if self.state == TCPRenoState::FastRecovery {
            // Update pipe estimate
            self.pipe = self.estimate_pipe();

            // Check if we can exit recovery
            if self.pipe <= self.ssthresh {
                // Exit recovery if we've processed enough data
                self.state = TCPRenoState::CongestionAvoidance;
                self.cwnd = self.ssthresh;
                self.dupack_count = 0;
                self.recovery_window = 0;
                self.pipe = 0;
            } else {
                // Still in recovery - maintain window
                self.cwnd = self.ssthresh;
            }
            self.last_reduction_time = current_time;
        } else {
            self.update_cwnd(self.mss);
        }

        if self.packets_in_flight > 0 {
            self.packets_in_flight -= 1;
        }
    }

    fn consecutive_dupacks_received(&mut self) {
        // Save state before entering recovery
        self.pre_recovery_flight_size = self.packets_in_flight;

        // Enter fast recovery state
        self.state = TCPRenoState::FastRecovery;

        // Set ssthresh per RFC 5681 section 3.2
        // ssthresh = max (FlightSize/2, 2*MSS)
        self.ssthresh = (self.pre_recovery_flight_size / 2).max(2 * self.mss);

        // Save recovery window - should be FlightSize + 1*MSS
        self.recovery_window = self.pre_recovery_flight_size + self.mss;

        // Initial pipe estimate
        self.pipe = self.estimate_pipe();

        // Set cwnd per RFC 5681:
        // cwnd = ssthresh + 3*MSS (to account for the 3 dupacks)
        self.cwnd = (self.ssthresh + 3 * self.mss).max(self.min_cwnd);

        // Record duplicate ACK count
        self.dupack_count = 3;

        // Save highest sequence transmitted
        self.recovery_high_seq = self.packets_in_flight;
    }

    fn timer_expired(&mut self) {
        self.ssthresh = (self.cwnd / 2).max(2 * self.mss);
        // Reset to minimum window size after timeout
        self.cwnd = self.min_cwnd;
        self.state = TCPRenoState::SlowStart;
        self.dupack_count = 0;
        self.packets_in_flight = 0;
    }

    fn dupack_over(&mut self) {
        // Return to congestion avoidance
        self.state = TCPRenoState::CongestionAvoidance;
        self.cwnd = self.ssthresh;
        self.dupack_count = 0;
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

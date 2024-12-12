//! Implements the TCP Reno congestion control algorithm.

use crate::flows::cc::CongestionControl;

#[derive(Debug, Default)]
pub struct TCPReno {
    /// the maximum segment size
    mss: usize,
    /// the size of the congestion window
    cwnd: usize,
    /// the slow start threshold
    ssthresh: usize,
}

impl TCPReno {
    pub fn new() -> TCPReno {
        TCPReno {
            mss: 512,
            cwnd: 512,
            ssthresh: 65535,
        }
    }
}

impl CongestionControl for TCPReno {
    /// Actions to be taken when a new acknowledgment has been received.
    fn ack_received(&mut self, _rtt: f64, _current_time: f64) {
        if self.cwnd <= self.ssthresh {
            // slow start
            self.cwnd += self.mss;
        } else {
            // congestion avoidance
            self.cwnd += self.mss * self.mss / self.cwnd;
        }
    }

    /// Actions to be taken when a timer expired.
    fn timer_expired(&mut self) {
        self.ssthresh = (2 * self.mss).max(self.cwnd / 2);
        // sets the congestion window to 1 segment
        self.cwnd = self.mss;
    }

    /// Actions to be taken when a new acknowledgment is received after previous
    /// dupacks.
    fn dupack_over(&mut self) {
        // RFC 2001 and TCP Reno
        self.cwnd = self.ssthresh;
    }

    /// Actions to be taken when three consecutive dupacks are received.
    fn consecutive_dupacks_received(&mut self) {
        // fast retransmit in RFC 2001 and TCP Reno
        self.ssthresh = (2 * self.mss).max(self.cwnd / 2);
        self.cwnd = self.ssthresh + 3 * self.mss;
    }

    /// Actions to be taken when more than three consecutive dupacks are
    /// received.
    fn more_dupacks_received(&mut self) {
        // fast retransmit in RFC 2001 and TCP Reno
        self.cwnd += self.mss;
    }

    fn get_cwnd(&self) -> usize {
        self.cwnd
    }
}

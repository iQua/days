//! Implements TCP Reno and TCP CUBIC congestion control algorithms, designed to supply
//! the TCPPacketSource struct with congestion control decisions.

use serde::Deserialize;

/// The congestion control algorithms.
#[derive(Clone, Copy, Debug, Deserialize)]
pub enum CCAlgorithm {
    TCPReno,
    TCPCubic,
}

/// Defines the interface for all congestion control algorithms.
pub trait CongestionControl {
    fn ack_received(&mut self, rtt: f64, current_time: f64);
    fn timer_expired(&mut self);
    fn dupack_over(&mut self);
    fn consecutive_dupacks_received(&mut self);
    fn more_dupacks_received(&mut self);
    fn get_cwnd(&self) -> usize;
}

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

    /// Updates CUBIC parameters upon the arrival of a new acknowledgment.
    pub fn cubic_update(&mut self, current_time: f64) {
        self.ack_cnt += 1;

        if self.epoch_start <= 0.0 {
            self.epoch_start = current_time;
            if self.cwnd < self.w_last_max {
                self.k = ((self.w_last_max - self.cwnd) as f64 / self.c).powf(1.0 / 3.0);
            } else {
                self.k = 0.0;
                self.origin_point = self.cwnd;
            }
            self.ack_cnt = 1;
            self.w_tcp = self.cwnd;
        }

        let t = current_time + self.d_min - self.epoch_start;
        let target = self.origin_point as f64 + self.c * (t - self.k).powi(3);

        if target > self.cwnd as f64 {
            self.cnt = (self.cwnd as f64 / (target - self.cwnd as f64)).floor() as usize;
        } else {
            self.cnt = 100 * self.cwnd;
        }

        if self.tcp_friendliness {
            self.cubic_tcp_friendliness();
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
    fn ack_received(&mut self, rtt: f64, current_time: f64) {
        if self.d_min > 0.0 {
            self.d_min = self.d_min.min(rtt);
        } else {
            self.d_min = rtt;
        }

        if self.cwnd <= self.ssthresh {
            // slow start
            self.cwnd += self.mss;
        } else {
            // congestion avoidance
            self.cubic_update(current_time);
            if self.cwnd_cnt > self.cnt {
                self.cwnd += self.mss;
                self.cwnd_cnt = 0;
            } else {
                self.cwnd_cnt += 1;
            }
        }
    }

    /// Actions to be taken when a timer expired.
    fn timer_expired(&mut self) {
        // sets the congestion window to 1 segment
        self.cwnd = self.mss;
        self.cubic_reset();
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

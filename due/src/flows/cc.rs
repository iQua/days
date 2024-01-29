//! Congestion control algorithms, designed to supply the TCPPacketSource class
//! with congestion control decisions.

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
    fn get_cwnd(&self) -> f64;
}

#[derive(Debug)]
pub struct TCPReno {
    /// the maximum segment size
    mss: f64,
    /// the size of the congestion window
    cwnd: f64,
    /// the slow start threshold
    ssthresh: f64,
}

impl TCPReno {
    pub fn new() -> TCPReno {
        TCPReno {
            mss: 512.0,
            cwnd: 512.0,
            ssthresh: 65535.0,
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
        self.ssthresh = (2.0 * self.mss).max(self.cwnd / 2.0);
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
        self.ssthresh = (2.0 * self.mss).max(self.cwnd / 2.0);
        self.cwnd = self.ssthresh + 3.0 * self.mss;
    }

    /// Actions to be taken when more than three consecutive dupacks are
    /// received.
    fn more_dupacks_received(&mut self) {
        // fast retransmit in RFC 2001 and TCP Reno
        self.cwnd += self.mss;
    }

    fn get_cwnd(&self) -> f64 {
        self.cwnd
    }
}

#[derive(Debug)]
pub struct TCPCubic {
    /// the maximum segment size
    mss: f64,
    /// the size of the congestion window
    cwnd: f64,
    /// the slow start threshold
    ssthresh: f64,
    w_last_max: f64,
    epoch_start: f64,
    origin_point: f64,
    d_min: f64,
    w_tcp: f64,
    k: f64,
    ack_cnt: usize,
    tcp_friendliness: bool,
    beta: f64,
    c: f64,
    cwnd_cnt: usize,
    cnt: f64,
}

impl TCPCubic {
    pub fn new() -> TCPCubic {
        TCPCubic {
            mss: 512.0,
            cwnd: 512.0,
            ssthresh: 65535.0,
            w_last_max: 0.0,
            epoch_start: 0.0,
            origin_point: 0.0,
            d_min: 0.0,
            w_tcp: 0.0,
            k: 0.0,
            ack_cnt: 0,
            tcp_friendliness: true,
            beta: 0.2,
            c: 0.4,
            cwnd_cnt: 0,
            cnt: 0.0,
        }
    }

    /// Resets the states in CUBIC.
    pub fn cubic_reset(&mut self) {
        self.w_last_max = 0.0;
        self.epoch_start = 0.0;
        self.origin_point = 0.0;
        self.d_min = 0.0;
        self.w_tcp = 0.0;
        self.k = 0.0;
        self.ack_cnt = 0;
    }

    /// Updates CUBIC parameters upon the arrival of a new acknowledgment.
    pub fn cubic_update(&mut self, current_time: f64) {
        self.ack_cnt += 1;
        if self.epoch_start <= 0.0 {
            self.epoch_start = current_time;
            if self.cwnd < self.w_last_max {
                self.k = ((self.w_last_max - self.cwnd) / self.c).powf(1.0 / 3.0);
            } else {
                self.k = 0.0;
                self.origin_point = self.cwnd;
            }
            self.ack_cnt = 1;
            self.w_tcp = self.cwnd;
        }
        let t = current_time + self.d_min - self.epoch_start;
        let target = self.origin_point + self.c * (t - self.k).powi(3);
        if target > self.cwnd {
            self.cnt = self.cwnd / (target - self.cwnd);
        } else {
            self.cnt = 100.0 * self.cwnd;
        }
        if self.tcp_friendliness {
            self.cubic_tcp_friendliness();
        }
    }

    /// CUBIC actions in TCP mode.
    pub fn cubic_tcp_friendliness(&mut self) {
        self.w_tcp += 3.0 * self.beta / (2.0 - self.beta) * (self.ack_cnt as f64 / self.cwnd);
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
            if self.cwnd_cnt as f64 > self.cnt {
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
        self.ssthresh = (2.0 * self.mss).max(self.cwnd / 2.0);
        self.cwnd = self.ssthresh + 3.0 * self.mss;
    }

    /// Actions to be taken when more than three consecutive dupacks are
    /// received.
    fn more_dupacks_received(&mut self) {
        self.cwnd += self.mss;
    }

    fn get_cwnd(&self) -> f64 {
        self.cwnd
    }
}

//! Implements the general struct for congestion control algorithms, designed to supply
//! the TCPPacketSource struct with congestion control decisions.

use serde::Deserialize;
use std::any::Any;

/// A delivery-rate sample for rate-based congestion control.
#[derive(Clone, Debug, Default)]
pub struct RateSample {
    /// Bytes delivered in the sample interval.
    pub delivered: usize,
    /// Duration of the sample interval in seconds.
    pub interval: f64,
    /// Time between the last delivered packet and this ACK, in seconds.
    pub ack_elapsed: f64,
    /// Time between the first and last packet sent in the sample, in seconds.
    pub send_elapsed: f64,
    /// RTT sample in seconds.
    pub rtt: f64,
    /// Bytes acknowledged by this ACK.
    pub acked: usize,
    /// Whether sender was app-limited when the sample was taken.
    pub is_app_limited: bool,
    /// Inflight at the time of send for the sampled packet.
    pub prior_inflight: usize,
    /// Bytes considered lost in this sample.
    pub lost: usize,
    /// Whether this sample was ECN-marked.
    pub ecn_marked: bool,
}

/// An ACK event used by congestion control algorithms.
#[derive(Clone, Debug)]
pub struct AckEvent {
    pub ack_seq: usize,
    pub rtt: f64,
    pub now: f64,
    pub bytes_acked: usize,
    pub rate_sample: RateSample,
}

impl AckEvent {
    pub fn new_basic(ack_seq: usize, rtt: f64, now: f64, bytes_acked: usize) -> Self {
        AckEvent {
            ack_seq,
            rtt,
            now,
            bytes_acked,
            rate_sample: RateSample {
                delivered: bytes_acked,
                interval: rtt,
                ack_elapsed: rtt,
                send_elapsed: rtt,
                rtt,
                acked: bytes_acked,
                ..RateSample::default()
            },
        }
    }
}

/// A congestion signal with the sender-side flight size at the time of reaction.
#[derive(Clone, Copy, Debug)]
pub struct CongestionEvent {
    pub now: f64,
    pub flight_size_bytes: usize,
}

/// The congestion control algorithms.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq)]
pub enum CCAlgorithm {
    #[serde(alias = "RENO", alias = "Reno", alias = "reno")]
    TCPReno,
    #[serde(alias = "CUBIC", alias = "Cubic", alias = "cubic")]
    TCPCubic,
    #[serde(alias = "BBR", alias = "Bbr", alias = "bbr")]
    TCPBBR,
}

/// Defines the interface for all congestion control algorithms.
pub trait CongestionControl {
    fn ack_received(&mut self, event: AckEvent);
    fn packet_sent(&mut self, _bytes: usize, _now: f64) {}
    fn timer_expired(&mut self);
    fn dupack_over(&mut self);
    fn consecutive_dupacks_received(&mut self);
    fn more_dupacks_received(&mut self);
    fn ecn_marked(&mut self) {}
    fn congestion_event(&mut self, _event: CongestionEvent) {
        self.consecutive_dupacks_received();
    }
    fn ecn_congestion_event(&mut self, _event: CongestionEvent) {
        self.ecn_marked();
    }
    fn timeout_event(&mut self, _event: CongestionEvent) {
        self.timer_expired();
    }
    fn get_cwnd(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn get_pacing_rate(&self) -> f64 {
        0.0 // Default pacing rate for algorithms that do not utilize it
    }
}

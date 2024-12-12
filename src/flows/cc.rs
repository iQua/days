//! Implements the general struct for congestion control algorithms, designed to supply
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
    fn ack_received(&mut self, rtt: f64, current_time: f64, bytes_acked: usize);
    fn timer_expired(&mut self);
    fn dupack_over(&mut self);
    fn consecutive_dupacks_received(&mut self);
    fn more_dupacks_received(&mut self);
    fn get_cwnd(&self) -> usize;
}

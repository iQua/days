//! Packet scheduler implementations and reporting primitives.

pub mod drop;
pub mod drr;
pub mod port;
pub mod sp;
pub mod state;
pub mod vc;
pub mod wfq;
pub mod wrr;

use crate::flows::packet::Packet;

pub use days::utils::logger::SchedulerReport;

/// Defines the interface for all schedulers to update statistics in their periodic
/// reports.
pub trait ReportStatistics {
    fn update_stats_on_packet_received(&mut self, packet: &Packet);
    fn update_stats_on_packet_forwarded(&mut self, packet: &Packet);
    fn prepare_report(&self, now: f64) -> SchedulerReport;
    fn reset_stats(&mut self, now: f64);
}

pub mod drop;
pub mod drr;
pub mod port;
pub mod sp;
pub mod vc;
pub mod wfq;

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct SchedulerReport {
    pub id: usize,
    /// the start time of this report interval
    pub start_time: f64,
    /// the end time of this report interval
    pub end_time: f64,
    /// the number of received packets in this report interval
    pub received_packets: usize,
    pub dropped_packets: usize,
    pub forwarded_packets: usize,
    pub queue_length: usize,
    /// the size of received packets in this report interval
    pub received_sizes: usize,
    pub forwarded_sizes: usize,
    pub throughput_mean: f64,
    /// the mean of queueing delays of the packets
    pub queueing_delay_mean: f64,
}

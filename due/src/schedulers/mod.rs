pub mod drop;
pub mod drr;
pub mod port;
pub mod sp;
pub mod vc;
pub mod wfq;

use crate::flows::packet::Packet;

#[derive(Clone, Debug)]
pub struct SchedulerReport {
    pub id: u32,
    /// the start time of this report interval
    pub start_time: f64,
    /// the end time of this report interval
    pub end_time: f64,
    /// the number of received packets in this report interval
    pub received_packets: u32,
    pub dropped_packets: u32,
    pub forwarded_packets: u32,
    pub queue_length: u32,
    /// the size of received packets in this report interval
    pub received_sizes: u32,
    pub forwarded_sizes: u32,
    pub throughput_mean: f64,
    /// the mean of queueing delays of the packets
    pub queueing_delay_mean: f64,
}

impl SchedulerReport {
    pub fn new(id: u32, start_time: f64) -> Self {
        SchedulerReport {
            id,
            start_time,
            end_time: 0.0,
            received_packets: 0,
            dropped_packets: 0,
            forwarded_packets: 0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
        }
    }

    pub fn receive_update(&mut self, packet: &Packet) {
        self.received_packets += 1;
        self.received_sizes += packet.size as u32;
    }

    pub fn forward_update(&mut self, packet: &Packet) {
        self.forwarded_packets += 1;
        self.forwarded_sizes += packet.size as u32;
    }

    pub fn drop_update(&mut self) {
        self.dropped_packets += 1;
    }
}

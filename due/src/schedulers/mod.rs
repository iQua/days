pub mod drop;
pub mod drr;
pub mod port;
pub mod sp;
pub mod vc;
pub mod wfq;

use serde::Serialize;
use struct_field_names_as_array::FieldNamesAsSlice;

use crate::flows::packet::Packet;

#[derive(Clone, Debug, Serialize, FieldNamesAsSlice)]
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

    pub fn reset(&mut self, start_time: f64) -> SchedulerReport {
        let queue_length = self.queue_length;
        let mut new_report = SchedulerReport::new(self.id, start_time);
        new_report.queue_length = queue_length;
        new_report
    }

    /// Updates the report when receiving a packet.
    pub fn receive_update(&mut self, packet: &Packet) {
        self.received_packets += 1;
        self.received_sizes += packet.size as u32;
        self.queue_length += packet.size as u32;
    }

    /// Updates the report when forwarding a packet.
    pub fn forward_update(&mut self, packet: &Packet) {
        let num_packets = self.forwarded_packets as f64;
        self.queueing_delay_mean =
            (self.queueing_delay_mean * num_packets + packet.queueing_delay) / (num_packets + 1.0);

        self.forwarded_packets += 1;
        self.forwarded_sizes += packet.size as u32;
        self.queue_length -= packet.size as u32;
        self.throughput_mean = self.forwarded_sizes as f64 / (packet.time - self.start_time);
    }
}

//! Implements a TCPSink, designed to send ack packets back to the
//! TCPPacketSource.

use std::fmt::Debug;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::sink::{PacketStatistics, RandomVar};
use crate::next_endpoint_id;

#[derive(Debug, Default)]
pub struct TCPPacketSink {
    endpoint_id: usize,
    flow_id: usize,
    /// the arrival times of the packets
    arrival_times: RandomVar,
    /// the last arrival time
    last_arrival_time: f64,
    /// the inter-arrival times of the packets
    inter_arrival_times: RandomVar,
    /// the one-way end-to-end delays of the packets
    one_way_delays: RandomVar,
    /// the total time spent waiting in queues
    queueing_delays: RandomVar,
    /// the size of the packets
    packet_sizes: RandomVar,
    /// output: packet statistics
    pub statistics: Output<PacketStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
}

impl TCPPacketSink {
    pub fn new(flow_id: usize) -> TCPPacketSink {
        TCPPacketSink {
            endpoint_id: next_endpoint_id(),
            flow_id,
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
            statistics: Output::default(),
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.endpoint_id
    }

    pub fn flow_id(&self) -> usize {
        self.flow_id
    }

    pub fn statistics(&self) -> PacketStatistics {
        PacketStatistics {
            endpoint_id: self.endpoint_id,
            flow_id: self.flow_id,
            arrival_times: self.arrival_times.clone(),
            inter_arrival_times: self.inter_arrival_times.clone(),
            one_way_delays: self.one_way_delays.clone(),
            queueing_delays: self.queueing_delays.clone(),
            packet_sizes: self.packet_sizes.clone(),
        }
    }

    pub async fn report(&mut self, endpoint_id: usize) {
        assert_eq!(endpoint_id, self.endpoint_id);
        debug!("TCPPacketSink {} reporting upon request.", endpoint_id);
        self.statistics.send(self.statistics()).await;
    }

    pub fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.arrival_times.tabulate(arrival_time);
        self.inter_arrival_times
            .tabulate(arrival_time - self.last_arrival_time);
        self.last_arrival_time = arrival_time;
        self.one_way_delays
            .tabulate(arrival_time - packet.creation_time);
        self.queueing_delays.tabulate(packet.queueing_delay);
        self.packet_sizes.tabulate(packet.size as u32);

        debug!(
            "TCPPacketSink {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, packet.packet_id, packet.size, packet.flow_id, arrival_time,
        );
    }
}

impl Model for TCPPacketSink {}

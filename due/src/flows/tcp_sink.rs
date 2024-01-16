//! Implements a TCPSink, designed to send ack packets back to the
//! TCPPacketSource.

use std::fmt::Debug;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::{Packet, TCPAck};
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
    /// the receive buffer, which is a priority queue that is sorted based on
    /// the sequence number of the packet (packet_id)
    recv_buffer: Vec<(usize, usize)>,
    /// the next sequence number expected to be received
    next_seq_expected: usize,
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
            recv_buffer: Vec::new(),
            next_seq_expected: 0,
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

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
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

        // inserts the packet into the receive buffer and sorts based on the
        // sequence number of the packet (packet_id)
        let sequence_num = packet.packet_id;
        self.recv_buffer
            .push((sequence_num, sequence_num + packet.size));
        self.recv_buffer.sort();

        let mut merged_stats: Vec<(usize, usize)> = Vec::new();
        for (start, end) in self.recv_buffer.iter() {
            if merged_stats.last().is_some() & (start <= &merged_stats.last().unwrap_or(&(0, 0)).1)
            {
                let last = merged_stats.last_mut().unwrap();
                *last = (last.0, *end.max(&last.1));
            } else {
                merged_stats.push((*start, *end));
            }
        }

        self.recv_buffer = merged_stats;

        if self.recv_buffer.len() == 1 {
            // in-order delivery: all data up to but not including
            // `next_seq_expected` have been received
            self.next_seq_expected = packet.packet_id + packet.size;
        } else {
            // out-of-order delivery or retransmissions: needs to go through the
            // receive buffer and find out what the last in-order packet's
            // sequence number is
            self.next_seq_expected = self.recv_buffer[0].1;
        }

        let acknowledgment = Packet {
            time: packet.time,
            creation_time: packet.creation_time,
            size: 40,
            packet_id: packet.packet_id,
            flow_id: packet.flow_id,
            queueing_delay: packet.queueing_delay,
            ack: Some(TCPAck {
                sequence_num: self.next_seq_expected,
            }),
        };

        // sends the Ack packet out to the TCPPacketSource now
        self.output.send(acknowledgment.clone()).await;

        debug!(
            "TCPPacketSink {} sent Ack packet {} ({} bytes) at time {:.3}.",
            self.endpoint_id, acknowledgment.packet_id, acknowledgment.size, arrival_time,
        );
    }
}

impl Model for TCPPacketSink {}

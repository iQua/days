//! Implements a PacketSink, designed to record both arrival times and waiting
//! times from the incoming packets.

//! The PacketSink records a variety of statistics, including absolute arrival
//! times, inter-arrival times, the total number of packets and bytes received,
//! the one-way end-to-end delays, and the total time spent waiting in queues.
//! These statistics are indexed by either the flow identifier or the source of
//! each packet.
use crate::packets::packet::Packet;
use crate::Shared;
use sim::SimContext;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

pub struct PacketSink {
    element_id: u32,
    /// the number of packets received
    packets_received: u32,
    /// the number of bytes received
    bytes_received: u32,
    /// the arrival times of the packets
    arrival_times: Vec<f64>,
    /// the last arrival time
    last_arrival_time: f64,
    /// the inter-arrival times of the packets
    inter_arrival_times: Vec<f64>,
    /// the one-way end-to-end delays of the packets
    one_way_delays: Vec<f64>,
    /// the total time spent waiting in queues
    queueing_delays: f64,
    /// the size of the packets
    packet_sizes: Vec<u32>,
    /// a receiver for receiving incoming packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl PacketSink {
    pub fn new(element_id: u32) -> PacketSink {
        PacketSink {
            element_id,
            packets_received: 0,
            bytes_received: 0,
            arrival_times: Vec::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: Vec::new(),
            one_way_delays: Vec::new(),
            queueing_delays: 0.0,
            packet_sizes: Vec::new(),
            receiver: unbounded_channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_received += 1;

        println!(
            "Sink {} received packet {} ({} bytes) from flow {} at time {:.3}. {} packets received.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        while let Some(packet) = self.receiver.recv().await {
            self.packet_received(packet, sim);
        }
    }
}

//! Implements a PacketSink, designed to record both arrival times and waiting
//! times from the incoming packets.

//! The PacketSink records a variety of statistics, including absolute arrival
//! times, inter-arrival times, the total number of packets and bytes received,
//! the one-way end-to-end delays, and the total time spent waiting in queues.
//! These statistics are indexed by either the flow identifier or the source of
//! each packet.

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::sim::{RandomVar, SimContext};
use crate::{get_id, Element, Shared};

pub struct PacketSink {
    element_id: usize,
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
    /// a sender for sending outbound packets
    sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl Element for PacketSink {
    fn id(&mut self) -> usize {
        self.element_id
    }

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl Default for PacketSink {
    fn default() -> Self {
        PacketSink {
            element_id: get_id(),
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }
}

impl Clone for PacketSink {
    fn clone(&self) -> Self {
        PacketSink {
            element_id: get_id(),
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }
}

impl PacketSink {
    pub fn new() -> PacketSink {
        Default::default()
    }

    /// Creates a PacketSink without occupying ids.
    pub fn new_without_id() -> PacketSink {
        PacketSink {
            element_id: usize::MAX,
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.arrival_times.tabulate(sim.now());
        self.inter_arrival_times
            .tabulate(sim.now() - self.last_arrival_time);
        self.last_arrival_time = sim.now();
        self.one_way_delays
            .tabulate(sim.now() - packet.creation_time);
        self.queueing_delays.tabulate(packet.queueing_delay);
        self.packet_sizes.tabulate(packet.size as u32);

        // Update global statistics about packet sizes
        sim.shared().queueing_delay.tabulate(packet.queueing_delay);

        println!(
            "Sink {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        while let Some(packet) = self.receiver.recv().await {
            self.packet_received(packet, sim);
        }

        println!(
            "Sink {} finished running at time {:.3}. Statistics: \n\
            Arrival times: {:#.3} \n\
            Inter-arrival times: {:#.3} \n\
            One-way delays: {:#.3} \n\
            Queueing delays: {:#.3} \n\
            Packet sizes: {:#.3} \n",
            self.element_id,
            sim.now(),
            self.arrival_times,
            self.inter_arrival_times,
            self.one_way_delays,
            self.queueing_delays,
            self.packet_sizes,
        );
    }
}

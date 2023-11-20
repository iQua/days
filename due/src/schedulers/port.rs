//! A simple FIFO port with only one receiver.

use std::collections::VecDeque;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::sim::SimContext;

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::{Element, Shared};

pub struct Port {
    element_id: usize,
    /// the bit rate of the port
    rate: f64,
    /// a closure that determines whether an inbound packet should be dropped or not
    pub drop_strategy: Box<dyn PacketDrop>,
    /// the number of packets received
    packets_received: usize,
    /// the number of dropped packets
    packets_dropped: usize,
    /// the number of packets in the queue
    packets_in_queue: usize,
    /// the total byte sizes in the queue
    bytes_in_queue: usize,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
    /// a sender for sending outbound packets
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl Element for Port {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl Port {
    pub fn new(
        element_id: usize,
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        drop_strategy: DropStrategy,
    ) -> Port {
        let packet_drop = match drop_strategy {
            DropStrategy::TailDrop => TailDrop::new(capacity, capacity_unit),
            _ => unimplemented!(),
        };

        Port {
            element_id,
            rate,
            drop_strategy: Box::new(packet_drop),
            packets_received: 0,
            packets_dropped: 0,
            packets_in_queue: 0,
            bytes_in_queue: 0,
            queue: VecDeque::new(),
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        // drops the packet if the buffer is full
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.size, self.bytes_in_queue, self.queue.len());

        // the case that this packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            println! {
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.element_id,
                packet.packet_id,
                packet.flow_id,
                sim.now()
            }
            return;
        }

        // the case that this packet will not be dropped.
        self.packets_received += 1;
        self.queue.push_back(packet.clone());
        self.packets_in_queue += 1;
        self.bytes_in_queue += packet.size;

        println!(
            "Port {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in queue.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.packets_in_queue
        );
    }

    fn packet_sent(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_in_queue -= 1;
        self.bytes_in_queue -= packet.size;

        println!(
            "Port {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in queue.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_in_queue
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // trying to receive all the packets accumulated in the channel
            while let Ok(packet) = self.receiver.try_recv() {
                self.packet_received(packet, sim);
            }

            while let Some(mut packet) = self.queue.pop_front() {
                sim.advance(packet.size as f64 * 8.0 / self.rate).await;

                packet.send(sim.now());
                let _ = self.sender.send(packet.clone());
                self.packet_sent(packet, sim);
            }

            if let Some(packet) = self.receiver.recv().await {
                self.packet_received(packet, sim);
            } else {
                break;
            }
        }

        println!(
            "Port {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

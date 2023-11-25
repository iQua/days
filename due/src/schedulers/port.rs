//! Implements a simple FIFO scheduler with only one queue.

use std::collections::VecDeque;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::sim::SimContext;
use crate::{Scheduler, Shared};

pub struct Port {
    scheduler_id: usize,
    /// the bit rate of the port
    rate: f64,
    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop>,
    /// the number of packets received
    packets_received: usize,
    /// the number of dropped packets
    packets_dropped: usize,
    /// the total byte sizes in the queue
    bytes_in_queue: usize,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
    /// a sender for sending outbound packets
    sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl Scheduler for Port {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl Port {
    pub fn new(
        scheduler_id: usize,
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
            scheduler_id,
            rate,
            drop_strategy: Box::new(packet_drop),
            packets_received: 0,
            packets_dropped: 0,
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
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                sim.now()
            }
            return;
        }

        // the case that this packet will not be dropped.
        self.packets_received += 1;
        self.queue.push_back(packet.clone());
        self.bytes_in_queue += packet.size;

        println!(
            "Port {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.queue.len()
        );
    }

    fn packet_sent(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.bytes_in_queue -= packet.size;

        println!(
            "Port {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.queue.len()
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // trying to receive all the packets accumulated in the channel
            while let Ok(packet) = self.receiver.try_recv() {
                self.packet_received(packet, sim);
            }

            if let Some(mut packet) = self.queue.pop_front() {
                sim.advance(packet.size as f64 * 8.0 / self.rate).await;

                packet.send(sim.now());
                let _ = self.sender.send(packet.clone());
                self.packet_sent(packet, sim);
            }

            if !self.queue.is_empty() {
                // if there are packets in the queue, continue the loop
                continue;
            } else if let Some(packet) = self.receiver.recv().await {
                // waits for the packet from the upstream element
                self.packet_received(packet, sim);
            } else {
                break;
            }
        }

        println!(
            "Port {} finished running at time {}.",
            self.scheduler_id,
            sim.now()
        );
    }
}

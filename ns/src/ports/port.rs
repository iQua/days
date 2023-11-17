//! A simple FIFO port with only one receiver.
use std::collections::VecDeque;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use sim::SimContext;

use crate::packets::packet::Packet;
use crate::Shared;

pub struct Port {
    element_id: u32,
    /// the bit rate of the port
    rate: f64,
    /// a queue limit in bytes or packets
    qlimit: u32,
    /// if true, qlimit will be based on bytes
    limit_bytes: bool,
    /// the number of packets sent
    packets_sent: u32,
    /// the number of packets received
    packets_received: u32,
    /// the number of dropped packets
    packets_dropped: u32,
    /// the number of packets in the queue
    packets_in_queue: u32,
    /// the total byte sizes in the queue
    bytes_in_queue: u32,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
    /// a sender for sending packets
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl Port {
    pub fn new(element_id: u32, rate: f64, qlimit: u32, limit_bytes: bool) -> Port {
        Port {
            element_id,
            rate,
            qlimit,
            limit_bytes,
            packets_sent: 0,
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
        self.packets_received += 1;

        let byte_count = self.bytes_in_queue + packet.size;
        let should_drop_packet = (self.limit_bytes && byte_count > self.qlimit)
            || (!self.limit_bytes && self.queue.len() >= self.qlimit as usize);

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
        self.packets_sent += 1;
        self.packets_in_queue -= 1;
        self.bytes_in_queue -= packet.size;

        println!(
            "Port {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets sent, {} packets in queue.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_sent,
            self.packets_in_queue
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // trying to receive all the packets accumulated in the channel
            loop {
                match self.receiver.try_recv() {
                    Ok(packet) => {
                        self.packet_received(packet, sim);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }

            // sending all packets in an FIFO order to the downstream element
            loop {
                if let Some(mut packet) = self.queue.pop_front() {
                    sim.advance(packet.size as f64 * 8.0 / self.rate).await;

                    packet.time = sim.now();
                    self.sender.send(packet.clone()).unwrap();
                    self.packet_sent(packet, sim);
                } else {
                    break;
                }
            }

            // waiting for the next packet to arrive from the upstream elements
            if let Some(packet) = self.receiver.recv().await {
                self.packet_received(packet, sim);
            } else {
                panic!(
                    "Port {}: an upstream element may have closed its channel.",
                    self.element_id
                );
            }
        }
    }
}

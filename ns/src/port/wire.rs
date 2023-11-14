//! A very simple FIFO Wire with only one sender and one receiver.
//! It seems that port can have only one receiver with mutiple senders in the
//! future versions?
use std::collections::VecDeque;

use crate::{packets::packet::Packet, Shared};
use sim::{channel, Sender, Receiver, SimContext, select};

pub struct Wire {
    element_id: u32,
    // the number of packets sent
    packets_sent: u32,
    // the number of packets received
    packets_received: u32,
    // the number of packets in the queue
    packets_in_queue: u32,
    // the packet queue of the port
    queue: VecDeque<Packet>,
    // a sender for sending packets
    pub sender: Sender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: Receiver<Packet>,
}

impl Wire {
    pub fn new(element_id: u32) -> Wire {
        Wire {
            element_id: element_id,
            packets_sent: 0,
            packets_received: 0,
            packets_in_queue: 0,
            queue: VecDeque::new(),
            sender: channel().0,
            receiver: channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.queue.push_back(packet.clone());
        self.packets_received += 1;
        self.packets_in_queue += 1;

        println!(
            "Port {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now()
        );

        println!(
            "\t{} packets received, {} packets in queue.",
            self.packets_received,
            self.packets_in_queue
        )
    }

    fn packet_sent(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_sent += 1;
        self.packets_in_queue -= 1;
        println!(
            "Port {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now()
        );

        println!(
            "\t{} packets sent, {} packets in queue.",
            self.packets_sent,
            self.packets_in_queue
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            let receive_action = self.receiver.recv();
            let send_action = async {
                sim.advance(1.0).await;
                None
            };
            match select(sim, receive_action, send_action).await {
                Some(packet) => {
                    self.packet_received(packet, sim);
                },
                None => {
                    if let Some(packet) = self.queue.pop_front() {
                        self.packet_sent(packet, sim);
                    }
                }
            }
        }
    }

}
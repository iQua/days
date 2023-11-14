//! A very simple FIFO Wire with only one sender and one receiver.
//! It seems that port can have only one receiver with mutiple senders in the
//! future versions?
use std::collections::VecDeque;

use crate::{packets::packet::Packet, Shared};
use sim::{channel, Sender, Receiver, SimContext, select, Time};

pub struct Port {
    element_id: u32,
    // the bit rate of the port
    rate: f64,
    // a queue limit in bytes or packets
    qlimit: u32,
    // if true, qlimit will be based on bytes
    limit_bytes: bool,
    // the number of packets sent
    packets_sent: u32,
    // the number of packets received
    packets_received: u32,
    // the number of dropped packets
    packets_dropped: u32,
    // the number of packets in the queue
    packets_in_queue: u32,
    // the total byte sizes in the queue
    bytes_in_queue: u32,
    // the packet queue of the port
    queue: VecDeque<(Packet, Time)>,
    // a sender for sending packets
    pub sender: Sender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: Receiver<Packet>,
}

impl Port {
    pub fn new(element_id: u32, rate: f64, qlimit: u32, limit_bytes: bool) -> Port {
        Port {
            element_id: element_id,
            rate: rate,
            qlimit: qlimit,
            limit_bytes: limit_bytes,
            packets_sent: 0,
            packets_received: 0,
            packets_dropped: 0,
            packets_in_queue: 0,
            bytes_in_queue: 0,
            queue: VecDeque::new(),
            sender: channel().0,
            receiver: channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_received += 1;
        let byte_count = self.bytes_in_queue + packet.size;
        let should_drop_packet = 
            (self.limit_bytes && byte_count > self.qlimit) || 
            (!self.limit_bytes && self.queue.len() >= self.qlimit as usize);
        
        // the case that the packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            println!{
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.element_id,
                packet.packet_id,
                packet.flow_id,
                sim.now()
            }
            return;
        }
        
        // the case that packet will not be dropped.
        self.queue.push_back((packet.clone(), sim.now()));
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
            let receive_action = self.receiver.recv();
            let send_action = async {
                if let Some((packet, arrival_time)) = self.queue.front() {
                    let delay = (packet.size as f64) * 8.0 / self.rate;
                    let wait_time = arrival_time + delay - sim.now();
                    sim.advance(wait_time).await;
                } else {
                    sim.advance(1.0).await;
                }
                None
            };
            match select(sim, receive_action, send_action).await {
                Some(packet) => {
                    self.packet_received(packet, sim);
                },
                None => {
                    if let Some((packet, _)) = self.queue.pop_front() {
                        self.sender
                            .send(packet.clone())
                            .await
                            .expect("no receiving element in the simulation");
                        self.packet_sent(packet, sim);
                    }
                }
            }
        }
    }

}
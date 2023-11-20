//! A simple FIFO port with only one receiver.

use std::collections::VecDeque;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::sim::SimContext;

use crate::packets::packet::Packet;
use crate::{Element, Shared};

pub struct Port {
    element_id: usize,
    /// the bit rate of the port
    rate: f64,
    /// a queue limit in bytes or packets
    qlimit: usize,
    /// if true, qlimit will be based on bytes
    limit_bytes: bool,
    /// the number of packets received
    packets_received: usize,
    /// the number of dropped packets
    packets_dropped: usize,
    /// the number of packets in the queue
    packets_in_queue: usize,
    /// the total byte sizes in the queue
    bytes_in_queue: usize,
    /// if True, assume that the downstream element does not have any buffers,
    /// and backpressure is in effect so that all waiting packets queue up in
    /// this element's buffer.
    zero_downstream_buffer: bool,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
    /// the packet queue of the downstream element
    downstream_queue: VecDeque<Packet>,
    /// a receiver for receving packet sending messages from the downstream element
    pub receiver_from_downstream: UnboundedReceiver<Packet>,
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
        qlimit: usize,
        limit_bytes: bool,
        zero_downstream_buffer: bool,
    ) -> Port {
        Port {
            element_id,
            rate,
            qlimit,
            limit_bytes,
            packets_received: 0,
            packets_dropped: 0,
            packets_in_queue: 0,
            bytes_in_queue: 0,
            zero_downstream_buffer,
            queue: VecDeque::new(),
            downstream_queue: VecDeque::new(),
            receiver_from_downstream: unbounded_channel().1,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_received += 1;

        let byte_count = self.bytes_in_queue + packet.size;
        let should_drop_packet = (self.limit_bytes && byte_count > self.qlimit)
            || (!self.limit_bytes && self.queue.len() >= self.qlimit);

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
        if self.zero_downstream_buffer {
            self.downstream_queue.push_back(packet.clone());
        }
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

        // deletes the packet in self.queue
        // TODO: need to think about better data structure to avoid loop, and
        // also effective for the !zero_downstream_buffer case.
        if self.zero_downstream_buffer {
            self.queue
                .retain(|pkt| packet.flow_id != pkt.flow_id || packet.packet_id != pkt.packet_id)
        }

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

            // trying to receive packet sending messages from the downstream
            // element
            if self.zero_downstream_buffer {
                while let Ok(packet) = self.receiver_from_downstream.try_recv() {
                    self.packet_sent(packet, sim);
                }
            }

            // sending all packets in an FIFO order to the downstream element
            // TODO: Optimize this part!
            if self.zero_downstream_buffer {
                while let Some(mut packet) = self.downstream_queue.pop_front() {
                    sim.advance(packet.size as f64 * 8.0 / self.rate).await;

                    packet.send(sim.now());
                    let _ = self.sender.send(packet.clone());
                }
            } else {
                while let Some(mut packet) = self.queue.pop_front() {
                    sim.advance(packet.size as f64 * 8.0 / self.rate).await;

                    packet.send(sim.now());
                    let _ = self.sender.send(packet.clone());
                    self.packet_sent(packet, sim);
                }
            }

            tokio::select! {
                packet = self.receiver.recv() => {
                    match packet {
                        Some(packet) => self.packet_received(packet, sim),
                        None => break,
                    }
                }
                packet = self.receiver_from_downstream.recv(), if self.zero_downstream_buffer => {
                    match packet {
                        Some(packet) => self.packet_sent(packet, sim),
                        None => break,
                    }
                }
            }
        }

        println!(
            "Port {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

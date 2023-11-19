//! Implements a Deficit Round Robin (DRR) server.

use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use sim::SimContext;

use crate::packets::packet::Packet;
use crate::{Element, Shared};

pub struct DRRServer {
    element_id: u32,
    /// the bit rate of the port
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based DRR.
    pub flow_classes: Box<dyn Fn(u32) -> u32>,

    /// class_id -> deficit
    deficit: HashMap<u32, u32>,
    /// class_id -> quantum
    quantum: HashMap<u32, u32>,
    /// class_id -> the head-of-line packet in its queue
    head_of_line: HashMap<u32, Packet>,

    /// the number of packets received and in the queues waiting to be sent
    packets_received: u32,
    packets_waiting: u32,

    /// class_id -> the number of bytes in its queue
    byte_sizes: HashMap<u32, u32>,

    /// class_id -> its FIFO queue
    queues: HashMap<u32, VecDeque<Packet>>,

    /// a sender for sending outbound packets to the downstream element
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets from upstream elements
    pub receiver: UnboundedReceiver<Packet>,
}

impl Element for DRRServer {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl DRRServer {
    pub fn new(element_id: u32, rate: f64, weights: HashMap<u32, u32>) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = HashMap::new();
        let mut quantum = HashMap::new();
        let mut byte_sizes = HashMap::new();
        let (sender, receiver) = unbounded_channel();

        let min_weight = weights.values().min().unwrap();
        for (class_id, weight) in &weights {
            deficit.insert(*class_id, 0);
            quantum.insert(*class_id, min_quantum * weight / min_weight);
            byte_sizes.insert(*class_id, 0);
        }

        DRRServer {
            element_id,
            rate,
            flow_classes: Box::new(|flow_id| flow_id),
            deficit,
            quantum,
            head_of_line: HashMap::new(),
            packets_received: 0,
            packets_waiting: 0,
            byte_sizes,
            queues: HashMap::new(),
            sender,
            receiver,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_waiting += 1;
        self.packets_received += 1;

        self.queues
            .entry(packet.flow_id)
            .or_default()
            .push_back(packet.clone());

        self.byte_sizes
            .entry(packet.flow_id)
            .and_modify(|byte_size| *byte_size += packet.size);

        println!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in class queue {}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.queues.get(&packet.flow_id).unwrap().len(),
            packet.flow_id
        );
    }

    fn poll_packets(&mut self, queue_id: u32, sim: SimContext<'_, Shared>) -> u32 {
        while let Ok(packet) = self.receiver.try_recv() {
            self.packet_received(packet, sim);
        }

        self.queues.get(&queue_id).unwrap().len() as u32
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // counts the number of packets in each queue
            let mut flow_queue_count: HashMap<u32, u32> = HashMap::new();
            for (&queue_id, queue) in &self.queues {
                flow_queue_count.insert(queue_id, queue.len() as u32);
            }

            // schedules packets by going through each queue
            for (&queue_id, &count) in &flow_queue_count {
                let mut current_length = count;

                // increases the deficit of the current queue if it is non-empty
                if current_length > 0 || self.head_of_line.contains_key(&queue_id) {
                    self.deficit.entry(queue_id).and_modify(|deficit| {
                        *deficit += self.quantum.get(&queue_id).unwrap();
                    });
                } else {
                    // resets to zero if the queue is empty
                    self.deficit.entry(queue_id).and_modify(|deficit| {
                        *deficit = 0;
                    });
                }

                let mut current_deficit = *self.deficit.get(&queue_id).unwrap();

                while (current_deficit > 0 && current_length > 0)
                    || self.head_of_line.contains_key(&queue_id)
                {
                    let mut packet;

                    if let Some(head_packet) = self.head_of_line.remove(&queue_id) {
                        packet = head_packet;
                    } else {
                        packet = self.queues.get_mut(&queue_id).unwrap().pop_front().unwrap();
                    }

                    if packet.size <= current_deficit {
                        // sends the packet out to the next element
                        self.byte_sizes.entry(queue_id).and_modify(|byte_size| {
                            *byte_size -= packet.size;
                        });

                        let timeout = (packet.size as f64) * 8.0 / self.rate;
                        sim.advance(timeout).await;
                        packet.time = sim.now();
                        let _ = self.sender.send(packet.clone());

                        self.packets_waiting -= 1;
                        current_deficit -= packet.size;

                        // polls for and receives all outstanding packets from
                        // DDRServer while sending the previous packets to the
                        // downstream element
                        // updates the length of the current queue
                        current_length = self.poll_packets(queue_id, sim);

                        println!(
                            "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
                                    {} packets in the flow queue.",
                            self.element_id,
                            packet.packet_id,
                            packet.size,
                            packet.flow_id,
                            sim.now(),
                            self.queues.get(&packet.flow_id).unwrap().len(),
                        );
                    } else {
                        self.head_of_line.insert(queue_id, packet);
                        break;
                    }
                }

                self.deficit
                    .entry(queue_id)
                    .and_modify(|deficit| *deficit = current_deficit);
            } // finishes going through each queue in one round

            // waits for inbound packets from the upstream element
            if self.packets_waiting == 0 {
                if let Some(packet) = self.receiver.recv().await {
                    self.packet_received(packet, sim);
                } else {
                    break;
                }
            }
        }
    }
}

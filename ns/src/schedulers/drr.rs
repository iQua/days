//! Implements a Deficit Round Robin (DRR) server.

use crate::packets::packet::Packet;
use crate::Shared;
use sim::{Receiver, Sender, SimContext};
use std::collections::{HashMap, VecDeque};

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
    /// class_id -> the number of packets in its queue
    flow_queue_count: HashMap<u32, u32>,
    /// class_id -> quantum
    quantum: HashMap<u32, u32>,
    /// class_id -> the head-of-line packet in its queue
    head_of_line: HashMap<u32, Packet>,

    /// the number of packets received and in the queues waiting to be sent
    packets_received: u32,
    packets_waiting: u32,
    packets_in_transit: Vec<Packet>,

    /// class_id -> the number of bytes in its queue
    byte_sizes: HashMap<u32, u32>,

    /// class_id -> its FIFO queue
    queues: HashMap<u32, VecDeque<Packet>>,

    /// a sender for sending packets to downstream elements
    pub sender: Sender<Packet>,
    /// a receiver for receiving incoming packets from upstream elements
    pub receiver: Receiver<Packet>,
}

impl DRRServer {
    pub fn new(element_id: u32, rate: f64, weights: HashMap<u32, u32>) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = HashMap::new();
        let mut quantum = HashMap::new();
        let mut flow_queue_count = HashMap::new();
        let mut byte_sizes = HashMap::new();

        let min_weight = weights.values().min().unwrap();
        for (class_id, weight) in &weights {
            deficit.insert(*class_id, 0);
            quantum.insert(*class_id, min_quantum * weight / min_weight);
            flow_queue_count.insert(*class_id, 0);
            byte_sizes.insert(*class_id, 0);
        }

        println!("weights: {:?};\nquantum: {:?}", weights, quantum);
        // TODO: add the usage of flow_classes!
        DRRServer {
            element_id,
            rate,
            flow_classes: Box::new(|flow_id| flow_id),
            deficit,
            flow_queue_count,
            quantum,
            head_of_line: HashMap::new(),
            packets_received: 0,
            packets_waiting: 0,
            packets_in_transit: Vec::new(),
            byte_sizes,
            queues: HashMap::new(),
            sender: channel().0,
            receiver: channel().1,
        }
        // Q: do we need packet_available?
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_waiting += 1;
        self.queues
            .entry(packet.flow_id)
            .or_insert_with(VecDeque::new)
            .push_back(packet.clone());

        self.packets_received += 1;

        self.byte_sizes
            .entry(packet.flow_id)
            .and_modify(|byte_size| *byte_size += packet.size);
        self.flow_queue_count
            .entry(packet.flow_id)
            .and_modify(|flow_id| *flow_id += 1);

        println!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in the flow queue.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.flow_queue_count.get(&packet.flow_id).unwrap(),
        );
    }

    fn packets_sent(&mut self, sim: SimContext<'_, Shared>) {
        for packet in self.packets_in_transit.drain(..) {
            self.packets_waiting -= 1;

            self.byte_sizes
                .entry(packet.flow_id)
                .and_modify(|byte_size| {
                    *byte_size -= packet.size;
                });

            self.deficit
                .entry(packet.flow_id)
                .and_modify(|deficit| *deficit -= packet.size);

            self.flow_queue_count
                .entry(packet.flow_id)
                .and_modify(|packet_count| *packet_count -= 1);

            if *self.flow_queue_count.get(&packet.flow_id).unwrap() == 0 {
                self.deficit
                    .entry(packet.flow_id)
                    .and_modify(|deficit| *deficit = 0);
            }

            println!(
                "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in the flow queue.",
                self.element_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                sim.now(),
                self.flow_queue_count.get(&packet.flow_id).unwrap(),
            );
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            let drr_scheduler = async {
                if self.packets_waiting == 0 {
                    sim.advance(1.0).await;
                    return None;
                }

                for (queue_id, &count) in self.flow_queue_count.iter() {
                    if count > 0 {
                        self.deficit.entry(*queue_id).and_modify(|deficit| {
                            *deficit += self.quantum.get(queue_id).unwrap();
                        });
                    }

                    while *self.deficit.get(queue_id).unwrap() > 0
                        && *self.flow_queue_count.get(queue_id).unwrap() > 0
                    {
                        let packet;
                        if let Some(head_packet) = self.head_of_line.remove(queue_id) {
                            packet = head_packet;
                        } else {
                            packet = self.queues.get_mut(queue_id).unwrap().pop_front().unwrap();
                        }

                        if packet.size < *self.deficit.get(queue_id).unwrap() {
                            let timeout = (packet.size as f64) * 8.0 / self.rate;
                            self.packets_in_transit.push(packet);
                            sim.advance(timeout).await;
                        } else {
                            self.head_of_line.insert(*queue_id, packet);
                            break;
                        }
                    }
                }

                None
            };

            match select(sim, self.receiver.recv(), drr_scheduler).await {
                Some(packet) => {
                    self.packet_received(packet, sim);
                }
                None => {
                    for packet in &self.packets_in_transit {
                        self.sender
                            .send(packet.clone())
                            .await
                            .expect("no receiving element in the simulation");
                    }

                    self.packets_sent(sim);
                }
            }
        }
    }
}

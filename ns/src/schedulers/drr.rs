//! Implements a Deficit Round Robin (DRR) server.

use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use sim::SimContext;

use crate::packets::packet::Packet;
use crate::Shared;
pub struct DRRScheduler {
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

    /// a sender for sending packets to the DRR server
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving incoming packets from the DRR server
    pub receiver: UnboundedReceiver<Packet>,
}

impl DRRScheduler {
    pub fn new(
        element_id: u32,
        rate: f64,
        weights: HashMap<u32, u32>,
        sender: UnboundedSender<Packet>,
        receiver: UnboundedReceiver<Packet>,
    ) -> DRRScheduler {
        let min_quantum = 1500;
        let mut deficit = HashMap::new();
        let mut quantum = HashMap::new();
        let mut byte_sizes = HashMap::new();

        let min_weight = weights.values().min().unwrap();
        for (class_id, weight) in &weights {
            deficit.insert(*class_id, 0);
            quantum.insert(*class_id, min_quantum * weight / min_weight);
            byte_sizes.insert(*class_id, 0);
        }

        println!("weights: {:?};\nquantum: {:?}", weights, quantum);
        // TODO: add the usage of flow_classes!
        DRRScheduler {
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
            .or_insert_with(VecDeque::new)
            .push_back(packet.clone());

        self.byte_sizes
            .entry(packet.flow_id)
            .and_modify(|byte_size| *byte_size += packet.size);

        println!(
            "DRRScheduler {} received packet {} ({} bytes) from flow {} at time {:.3}. \
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

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // receiving all outstanding packets from DDRServer while sending
            // the previous packets to the downstream element
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

            // counting the number of packets in each queue
            let mut flow_queue_count: HashMap<u32, u32> = HashMap::new();
            for (&queue_id, queue) in &self.queues {
                flow_queue_count.insert(queue_id, queue.len() as u32);
            }

            // scheduling packets by going through each queue
            for (&queue_id, &count) in &flow_queue_count {
                let mut current_length = count;

                // increase the deficit of the current queue if it is non-empty
                if current_length > 0 || self.head_of_line.contains_key(&queue_id) {
                    self.deficit.entry(queue_id).and_modify(|deficit| {
                        *deficit += self.quantum.get(&queue_id).unwrap();
                    });
                } else {
                    self.deficit.entry(queue_id).and_modify(|deficit| {
                        *deficit = 0;
                    });
                }

                let mut current_deficit = *self.deficit.get(&queue_id).unwrap();

                while (current_deficit > 0 && current_length > 0)
                    || self.head_of_line.contains_key(&queue_id)
                {
                    let packet;

                    if let Some(head_packet) = self.head_of_line.remove(&queue_id) {
                        packet = head_packet;
                    } else {
                        packet = self.queues.get_mut(&queue_id).unwrap().pop_front().unwrap();
                        current_length -= 1;
                    }

                    if packet.size <= current_deficit {
                        // sending the packet out to the next element
                        self.byte_sizes.entry(queue_id).and_modify(|byte_size| {
                            *byte_size -= packet.size;
                        });

                        let timeout = (packet.size as f64) * 8.0 / self.rate;
                        sim.advance(timeout).await;
                        let _ = self.sender.send(packet.clone());

                        self.packets_waiting -= 1;
                        current_deficit -= packet.size;

                        println!(
                            "DRRScheduler {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
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
            } // finished going through each queue in one round

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

pub struct DRRServer {
    element_id: u32,
    /// a packet scheduler using the Deficit Round Robin algorithm
    drr_scheduler: DRRScheduler,

    server_tx: UnboundedSender<Packet>,
    server_rx: UnboundedReceiver<Packet>,

    /// a sender for sending packets
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl DRRServer {
    pub fn new(element_id: u32, rate: f64, weights: HashMap<u32, u32>) -> DRRServer {
        let (server_tx, scheduler_rx) = unbounded_channel();
        let (scheduler_tx, server_rx) = unbounded_channel();
        DRRServer {
            element_id,
            drr_scheduler: DRRScheduler::new(element_id, rate, weights, scheduler_tx, scheduler_rx),
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
            server_tx,
            server_rx,
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        sim.activate(self.drr_scheduler.run(sim));

        loop {
            // waiting for the next packet to arrive from either upstream elements or DRRScheduler
            tokio::select! {
                Some(packet) = self.receiver.recv() => {
                    println!(
                        "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}.",
                        self.element_id,
                        packet.packet_id,
                        packet.size,
                        packet.flow_id,
                        sim.now(),
                    );
                    let _ = self.server_tx.send(packet.clone());
                }
                Some(mut packet) = self.server_rx.recv() => {
                    packet.time = sim.now();
                    let _ = self.sender.send(packet.clone());
                    println!(
                        "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
                        self.element_id,
                        packet.packet_id,
                        packet.size,
                        packet.flow_id,
                        sim.now(),
                    );
                }
                else => {
                    panic!(
                        "Port {}: an upstream element may have closed its channel.",
                        self.element_id
                    );
                }
            }
        }
    }
}

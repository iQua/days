//! Implements a Deficit Round Robin (DRR) server.

use crate::packets::packet::Packet;
use crate::Shared;
use sim::{select, SimContext};
use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
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
            // If we do not care about the packet receive time for the
            // DRRScheduler, it works properly with packet sending time.
            if self.packets_waiting == 0 {
                let packet = self.receiver.recv().await.unwrap();
                self.packet_received(packet, sim);
                loop {
                    match self.receiver.try_recv() {
                        Ok(packet) => {
                            self.packet_received(packet, sim);
                        },
                        Err(_) => {
                            break;
                        } 
                        
                    }
                }
            }

            let mut flow_queue_count: HashMap<u32, u32> = HashMap::new();

            // Updating the deficit counters
            for (queue_id, queue) in &self.queues {
                if queue.len() > 0 {
                    self.deficit.entry(*queue_id).and_modify(|deficit| {
                        *deficit += self.quantum.get(&queue_id).unwrap();
                    });
                } else {
                    self.deficit
                        .entry(*queue_id)
                        .and_modify(|deficit| *deficit = 0);
                }

                flow_queue_count.insert(*queue_id, queue.len() as u32);
            }

            // Scheduling packets
            for (queue_id, &count) in &flow_queue_count {
                let mut current_length = count;
                let deficit = *self.deficit.get(&queue_id).unwrap();

                while deficit > 0 && current_length > 0 {
                    let packet;
                    if let Some(head_packet) = self.head_of_line.remove(&queue_id) {
                        packet = head_packet;
                    } else {
                        packet = self.queues.get_mut(queue_id).unwrap().pop_front().unwrap();
                        current_length -= 1;
                    }

                    if packet.size < deficit {
                        // sending the packet out to the next element
                        self.byte_sizes
                            .entry(packet.flow_id)
                            .and_modify(|byte_size| {
                                *byte_size -= packet.size;
                            });

                        self.deficit
                            .entry(packet.flow_id)
                            .and_modify(|deficit| *deficit -= packet.size);

                        self.packets_waiting -= 1;

                        let timeout = (packet.size as f64) * 8.0 / self.rate;
                        sim.advance(timeout).await;
                        self.sender.send(packet.clone()).unwrap();

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
                        self.head_of_line.insert(*queue_id, packet);
                        break;
                    }
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

        let mut packet = Packet {
            production_time: sim.now(),
            time: sim.now(),
            size: 0,
            flow_id: 0,
            packet_id: 0,
            src: "source".to_string(),
            dst: "destination".to_string(),
        };

        loop {
            let drr_scheduler = async {
                if let Some(inbound_packet) = self.server_rx.recv().await {
                    println!(
                        "DRRServer received packet from scheduler at {:.3}",
                        sim.now()
                    );
                    packet = inbound_packet.clone();
                }

                None
            };

            match select(sim, drr_scheduler, self.receiver.recv()).await {
                Some(packet) => {
                    println!("DRRServer received packet at {:.3}", sim.now());
                    self.server_tx.send(packet).unwrap();
                }
                None => {
                    println!("Before DRRServer send at time {}", sim.now());
                    self.sender
                        .send(packet.clone())
                        .unwrap();

                    println!(
                        "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
                        self.element_id,
                        packet.packet_id,
                        packet.size,
                        packet.flow_id,
                        sim.now(),
                    );
                }
            }
        }
    }
}

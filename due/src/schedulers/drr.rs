//! Implements a Deficit Round Robin (DRR) server.

use std::collections::{HashMap, VecDeque};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::sim::SimContext;
use crate::{Element, Shared};

pub struct DRRServer {
    element_id: usize,
    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based DRR.
    pub flow_classes: Box<dyn Fn(usize) -> usize>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop>,

    /// deficit of classes, which are consecutive and start from 0
    deficit: Vec<usize>,
    /// quantum of classes, which are consecutive and start from 0
    quantum: Vec<usize>,
    /// class_id -> the head-of-line packet in its queue
    head_of_line: HashMap<usize, Packet>,

    /// the number of packets received, dropped, and in the queues waiting to be sent
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<usize>,

    /// FIFO queues of classes, which are consecutive and start from 0
    queues: Vec<VecDeque<Packet>>,

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
    pub fn new(
        element_id: usize,
        capacity: usize,
        capacity_unit: CapacityUnit,
        rate: f64,
        drop_strategy: DropStrategy,
        weights: Vec<usize>,
    ) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = Vec::new();
        let mut quantum = Vec::new();
        let mut byte_sizes = Vec::new();
        let mut queues = Vec::new();
        let (sender, receiver) = unbounded_channel();

        let min_weight = weights.iter().min().unwrap();

        for class_id in 0..weights.len() {
            deficit.push(0);
            quantum.push(min_quantum * weights[class_id] / min_weight);
            byte_sizes.push(0);
            queues.push(VecDeque::new());
        }

        let packet_drop = match drop_strategy {
            DropStrategy::TailDrop => TailDrop::new(capacity, capacity_unit),
            _ => unimplemented!(),
        };

        DRRServer {
            element_id,
            rate,
            flow_classes: Box::new(|flow_id| flow_id),
            drop_strategy: Box::new(packet_drop),
            deficit,
            quantum,
            head_of_line: HashMap::new(),
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            byte_sizes,
            queues,
            sender,
            receiver,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.iter().sum(),
            self.queues.iter().map(|q| q.len()).sum(),
        );

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

        self.packets_waiting += 1;
        self.packets_received += 1;

        let class_id = (self.flow_classes)(packet.flow_id);

        self.queues[class_id].push_back(packet.clone());
        self.byte_sizes[class_id] += packet.size;

        println!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in class queue {}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.queues[packet.flow_id].len(),
            packet.flow_id
        );
    }

    fn poll_packets(&mut self, class_id: usize, sim: SimContext<'_, Shared>) -> usize {
        while let Ok(packet) = self.receiver.try_recv() {
            self.packet_received(packet, sim);
        }

        self.queues[class_id].len()
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // counts the number of packets in each queue
            let mut flow_queue_count: Vec<usize> = Vec::new();
            for queue in &self.queues {
                flow_queue_count.push(queue.len());
            }

            // schedules packets by going through each queue
            for (class_id, &count) in flow_queue_count.iter().enumerate() {
                let mut current_length = count;

                // increases the deficit of the current queue if it is non-empty
                if current_length > 0 || self.head_of_line.contains_key(&class_id) {
                    self.deficit[class_id] += self.quantum[class_id];
                } else {
                    // resets to zero if the queue is empty
                    self.deficit[class_id] = 0;
                }

                let mut current_deficit = self.deficit[class_id];

                while (current_deficit > 0 && current_length > 0)
                    || self.head_of_line.contains_key(&class_id)
                {
                    let mut packet;

                    if let Some(head_packet) = self.head_of_line.remove(&class_id) {
                        packet = head_packet;
                    } else {
                        packet = self.queues[class_id].pop_front().unwrap();
                    }

                    if packet.size <= current_deficit {
                        // sends the packet out to the next element
                        self.byte_sizes[class_id] -= packet.size;

                        let timeout = (packet.size as f64) * 8.0 / self.rate;
                        sim.advance(timeout).await;
                        packet.send(sim.now());
                        let _ = self.sender.send(packet.clone());

                        self.packets_waiting -= 1;
                        current_deficit -= packet.size;

                        // polls for and receives all outstanding packets from
                        // DDRServer while sending the previous packets to the
                        // downstream element
                        // updates the length of the current queue
                        current_length = self.poll_packets(class_id, sim);

                        println!(
                            "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
                                    {} packets in the class queue.",
                            self.element_id,
                            packet.packet_id,
                            packet.size,
                            packet.flow_id,
                            sim.now(),
                            current_length,
                        );
                    } else {
                        self.head_of_line.insert(class_id, packet);
                        break;
                    }
                }

                self.deficit[class_id] = current_deficit;
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
        println!(
            "DRRServer {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

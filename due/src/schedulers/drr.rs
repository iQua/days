//! Implements a Deficit Round Robin (DRR) server.

use std::collections::{HashMap, VecDeque};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::sim::SimContext;
use crate::{Element, Shared};

pub struct DRRServer {
    element_id: u32,
    /// the bit rate of the port
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based DRR.
    pub flow_classes: Box<dyn Fn(u32) -> u32>,

    /// deficit of classes, which are consecutive and start from 0
    deficit: Vec<u32>,
    /// quantum of classes, which are consecutive and start from 0
    quantum: Vec<u32>,
    /// class_id -> the head-of-line packet in its queue
    head_of_line: HashMap<u32, Packet>,

    /// the number of packets received and in the queues waiting to be sent
    packets_received: u32,
    packets_waiting: u32,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<u32>,

    /// FIFO queues of classes, which are consecutive and start from 0
    queues: Vec<VecDeque<Packet>>,

    /// Does this server have a zero-length buffer? This is useful when multiple
    /// basic elements need to be put together to construct a more complex
    /// element with a unified buffer.
    zero_buffer: bool,

    // a sender for indicating the upstream element to send a packet
    pub sender_to_upstream: UnboundedSender<Packet>,

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
    pub fn new(element_id: u32, rate: f64, weights: Vec<u32>, zero_buffer: bool) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = Vec::new();
        let mut quantum = Vec::new();
        let mut byte_sizes = Vec::new();
        let mut queues = Vec::new();
        let (sender, receiver) = unbounded_channel();

        let min_weight = weights.iter().min().unwrap();


        for class_id in 0..weights.len() {
            deficit.push(0);
            quantum.push(min_quantum * weights[class_id] as u32 / min_weight);
            byte_sizes.push(0);
            queues.push(VecDeque::new());
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
            queues,
            zero_buffer,
            sender_to_upstream: unbounded_channel().0,
            sender,
            receiver,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.packets_waiting += 1;
        self.packets_received += 1;

        let queue_id = (self.flow_classes)(packet.flow_id) as usize;

        self.queues
            .get_mut(queue_id)
            .unwrap()
            .push_back(packet.clone());
        *self.byte_sizes.get_mut(queue_id).unwrap() += packet.size;

        println!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in class queue {}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.queues[packet.flow_id as usize].len(),
            packet.flow_id
        );
    }

    fn poll_packets(&mut self, queue_id: usize, sim: SimContext<'_, Shared>) -> u32 {
        while let Ok(packet) = self.receiver.try_recv() {
            self.packet_received(packet, sim);
        }

        self.queues[queue_id].len() as u32
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // counts the number of packets in each queue
            let mut flow_queue_count: Vec<u32> = Vec::new();
            for queue in &self.queues {
                flow_queue_count.push(queue.len() as u32);
            }

            // schedules packets by going through each queue
            for (queue_id, &count) in flow_queue_count.iter().enumerate() {
                let mut current_length = count;

                // increases the deficit of the current queue if it is non-empty
                if current_length > 0 || self.head_of_line.contains_key(&(queue_id as u32)) {
                    *self.deficit.get_mut(queue_id).unwrap() += self.quantum[queue_id];
                } else {
                    // resets to zero if the queue is empty
                    *self.deficit.get_mut(queue_id).unwrap() = 0;
                }

                let mut current_deficit = self.deficit[queue_id];

                while (current_deficit > 0 && current_length > 0)
                    || self.head_of_line.contains_key(&(queue_id as u32))
                {
                    let mut packet;

                    if let Some(head_packet) = self.head_of_line.remove(&(queue_id as u32)) {
                        packet = head_packet;
                    } else {
                        packet = self.queues.get_mut(queue_id).unwrap().pop_front().unwrap();
                    }

                    if packet.size <= current_deficit {
                        // sends the packet out to the next element
                        *self.byte_sizes.get_mut(queue_id).unwrap() -= packet.size;

                        let timeout = (packet.size as f64) * 8.0 / self.rate;
                        sim.advance(timeout).await;
                        packet.send(sim.now());
                        let _ = self.sender.send(packet.clone());

                        // indicates the upstream device to delete the packet
                        // its buffer.
                        let _ = self.sender_to_upstream.send(packet.clone());

                        self.packets_waiting -= 1;
                        current_deficit -= packet.size;

                        // polls for and receives all outstanding packets from
                        // DDRServer while sending the previous packets to the
                        // downstream element
                        // updates the length of the current queue
                        current_length = self.poll_packets(queue_id, sim);

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
                        self.head_of_line.insert(queue_id as u32, packet);
                        break;
                    }
                }

                *self.deficit.get_mut(queue_id).unwrap() = current_deficit;
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
        println!("DRRServer {} finished running at time {}.", self.element_id, sim.now());
    }
}

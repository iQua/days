//! Implements a Deficit Round Robin (DRR) server.

use crate::packets::packet::Packet;
use crate::Shared;
use sim::{channel, select, Receiver, Sender, SimContext, Time};
use std::collections::{HashMap, VecDeque};

pub struct DRRServer {
    element_id: u32,
    /// the bit rate of the port
    rate: f64,
    /// the following packets to be sent
    packet_to_send: Vec<Packet>,

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

    /// the number of packets received
    packets_received: u32,
    /// the current packet being sent to the downstream element, if any
    current_packet: Option<Packet>,
    /// class_id -> the number of bytes in its queue
    byte_sizes: HashMap<u32, u32>,

    /// class_id -> its FIFO queue
    queues: HashMap<u32, VecDeque<(Packet, Time)>>,

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

        let min_weight = weights.values().min().unwrap();
        for (class_id, weight) in &weights {
            deficit.insert(*class_id, 0);
            quantum.insert(*class_id, min_quantum * weight / min_weight);
            flow_queue_count.insert(*class_id, 0);
        }

        println!("weights: {:?};\nquantum: {:?}", weights, quantum);

        DRRServer {
            element_id,
            rate,
            packet_to_send: Vec::new(),
            flow_classes: Box::new(|flow_id| flow_id),
            deficit: deficit,
            flow_queue_count: flow_queue_count,
            quantum: quantum,
            head_of_line: HashMap::new(),
            packets_received: 0,
            current_packet: None,
            byte_sizes: HashMap::new(),
            queues: HashMap::new(),
            sender: channel().0,
            receiver: channel().1,
        }
        // Q: do we need packet_available?
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.queues
            .entry(packet.flow_id)
            .or_insert_with(VecDeque::new)
            .push_back((packet.clone(), sim.now()));
        self.packets_received += 1;
        *self.byte_sizes.entry(packet.flow_id).or_insert(0) += packet.size;
        *self.flow_queue_count.entry(packet.flow_id).or_insert(0) += 1;

        println!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in the flow queue.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received,
            self.flow_queue_count.get(&packet.flow_id).unwrap()
        );
    }

    async fn fetch_packet_to_send(&mut self, sim: SimContext<'_, Shared>) {
        for (flow_id, count) in self.flow_queue_count.iter() {
            if *count > 0 {
                *self.deficit.entry(*flow_id).or_insert(0) += self.quantum.get(&flow_id).unwrap();
            }
            
            // TODO!
            // The current design want to get only one packet to be sent.
            // However, if we use 'match select' between this function and
            // 'self.receiver.recv()', we can work fine here. That is, we can
            // receive packet and find the next packet to be sent.
            // However, the problem is, if we only fetch one packet to be sent
            // at a time, then after sending this packet, we will bach to this
            // function and iterate again, rather than iterate for this specific
            // flow until the deficit is not enough.
        } 
    }
}

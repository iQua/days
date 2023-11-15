//! Implements a Deficit Round Robin (DRR) server.

use crate::packets::packet::Packet;
use crate::Shared;
use sim::{channel, select, Receiver, Sender, SimContext, Time};
use std::collections::{HashMap, VecDeque};

pub struct DRRServer {
    element_id: u32,
    // the bit rate of the port
    rate: f64,
    // the number of packets sent
    packets_sent: u32,
    // the number of packets received
    packets_received: u32,
    // the following packets to be sent
    packet_to_send: Vec<Packet>,
    // the number of packets of each flow
    flow_queue_count: HashMap<u32, u32>,
    // the bytes of each flow
    bytes_sizes: HashMap<u32, u32>,
    // the head of line packet for each flow
    head_of_line: HashMap<u32, Packet>,
    // the quantum counter for all flows
    quantum: HashMap<u32, u32>,
    // the deficit counter for all flows
    deficits: HashMap<u32, u32>,
    // one FIFO queue for each flow
    queues: HashMap<u32, VecDeque<(Packet, Time)>>,
    // a sender for sending packets
    pub sender: Sender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: Receiver<Packet>,
}

impl DRRServer {
    pub fn new(element_id: u32, rate: f64, weights: HashMap<u32, u32>) -> DRRServer {
        let min_quantum = 1500;
        let min_weight = weights.values().min_by(|a, b| a.cmp(b)).unwrap_or(&1);
        let quantum: HashMap<u32, u32> = weights
            .iter()
            .map(|(&key, &value)| (key, min_quantum * value / min_weight))
            .collect();

        println!("weights: {:?};\nquantum: {:?}", weights, quantum);
        DRRServer {
            element_id,
            rate,
            packets_sent: 0,
            packets_received: 0,
            packet_to_send: Vec::new(), 
            flow_queue_count: HashMap::new(),
            bytes_sizes: HashMap::new(),
            head_of_line: HashMap::new(),
            quantum: quantum,
            deficits: HashMap::new(),
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
        *self.bytes_sizes.entry(packet.flow_id).or_insert(0) += packet.size;
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
                *self.deficits.entry(*flow_id).or_insert(0) += self.quantum.get(&flow_id).unwrap();
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

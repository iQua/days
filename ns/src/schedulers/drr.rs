//! Implements a Deficit Round Robin (DRR) server.

use std::collections::{HashMap, VecDeque};
use std::cmp::min_by;
use crate::packets::packet::Packet;
use sim::{Sender, Receiver, Time, channel};

pub struct DRRServer {
    element_id: u32,
    // the bit rate of the port
    rate: f64,
    // a HashMap for weights of flows
    weights: HashMap<String, u32>,
    // the number of packets sent
    packets_sent: u32,
    // the number of packets received
    packets_received: u32,
    // the number of packets of each flow
    packets_in_queue: HashMap<u32, u32>,
    // the bytes of each flow
    bytes_in_queue: HashMap<u32, u32>,
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
    pub fn new(element_id: u32, rate: f64, weights: HashMap<String, u32>) -> DRRServer {
        let min_quantum = 1500;
        let min_weight = weights.values().min_by(|a, b| a.cmp(b)).unwrap_or(&1);
        let quantum: HashMap<u32, u32> = weights.iter()
            .filter_map(|(key, &value)| {
                key.parse::<u32>().ok().map(|parsed_key| (parsed_key, min_quantum * value / min_weight))
            })
            .collect();
        println!("weights: {:?};\nquantum: {:?}", weights, quantum);
        DRRServer {
            element_id,
            rate,
            weights,
            packets_sent: 0,
            packets_received: 0,
            packets_in_queue: HashMap::new(),
            bytes_in_queue: HashMap::new(),
            quantum: quantum,
            deficits: HashMap::new(),
            queues: HashMap::new(),
            sender: channel().0,
            receiver: channel().1
        }
    }

    

}
//! Implements a Deficit Round Robin (DRR) server.

use crate::packets::packet::Packet;
use sim::{channel, Receiver, Sender, Time};
use std::collections::{HashMap, VecDeque};

pub struct DRRServer {
    element_id: u32,
    /// the bit rate of the port
    rate: f64,
    /// a hash map: class_id -> weight
    weights: HashMap<u32, u32>,

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

    // class_id -> its FIFO queue
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
            weights,
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
    }
}

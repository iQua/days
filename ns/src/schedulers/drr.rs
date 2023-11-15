//! Implements a Deficit Round Robin (DRR) server.

use std::collections::{HashMap, VecDeque};
use crate::packets::packet::Packet;
use sim::{Sender, Receiver, Time, channel};

pub struct DRRServer {
    element_id: u32,
    // the bit rate of the port
    rate: f64,
    // a HashMap for weights of flows
    weights: HashMap<String, u32>,
    // the packet queue of the server
    queue: VecDeque<(Packet, Time)>,
    // a sender for sending packets
    pub sender: Sender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: Receiver<Packet>,
}

impl DRRServer {
    pub fn new(element_id: u32, rate: f64, weights: HashMap<String, u32>) -> DRRServer {
        DRRServer {
            element_id,
            rate,
            weights,
            queue: VecDeque::new(),
            sender: channel().0,
            receiver: channel().1
        }
    }
}
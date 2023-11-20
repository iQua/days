//! Implements a packet switch with a FIFO bounded buffer on each of the outgoing ports.


use std::collections::HashMap;
use crate::Shared;
use crate::{Element, sim::SimContext};
use crate::ports::port::Port;
use crate::packets::packet::Packet;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};


pub struct SimplePacketSwitch {
    element_id: u32,
    /// the number of packets received by the switch
    packets_received: u32,
    /// the fib demux of the switch, 
    pub fib: Vec<u32>,
    /// a closure that maps a flow_id to a class_id
    pub flow_classes: Box<dyn Fn(u32) -> usize>,
    /// the output ports of the switch
    pub ports: HashMap<u32, Port>,
    /// senders for sending outbound packets, port_id -> sender
    pub senders: HashMap<u32, UnboundedSender<Packet>>,
    /// a receiver for receiving inbound packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl Element for SimplePacketSwitch {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        // self.sender = sender;

        //TODO!!!
        self.senders = HashMap::new();
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl SimplePacketSwitch {
    pub fn new(element_id: u32, nports: u32, port_rate: f64, buffer_size: u32) -> SimplePacketSwitch {
        let mut ports = HashMap::new();
        for i in 0..nports {
            let port = Port::new(i, port_rate, buffer_size, false);
            ports.insert(i, port);
        };
        let senders = HashMap::new();
        let fib = Vec::new();
        SimplePacketSwitch {
            element_id,
            packets_received: 0,
            fib,
            flow_classes: Box::new(|flow_id| flow_id as usize),
            ports,
            senders,
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        for (_, port) in self.ports {
            sim.activate(port.run(sim))
        }
        loop {
            if let Some(packet) = self.receiver.recv().await {
                self.packets_received += 1;
                let flow_class = (self.flow_classes)(packet.flow_id);
                // forwards packets to their corresponding ports
                if let Some(port_id) = self.fib.get(flow_class) {
                    let _ = self.senders.get_mut(port_id).unwrap().send(packet);
                } else {
                    println!("Wrong fib demux in SimplePacketSwitch {}.", self.element_id);
                }
                
            } else {
                break;
            }
        }

        println!("SimplePacketSwitch {} finished running.", self.element_id);
    }
}
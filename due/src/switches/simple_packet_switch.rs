//! Implements a packet switch with a FIFO bounded buffer on each of the outgoing ports.

use crate::packets::packet::Packet;
use crate::ports::port::Port;
use crate::Shared;
use crate::{sim::SimContext, Element};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct SimplePacketSwitch {
    element_id: u32,
    /// the number of packets received by the switch
    packets_received: u32,
    /// the fib demux of the switch,
    pub fib: Vec<u32>,
    /// a closure that maps a flow_id to a class_id
    pub flow_classes: Box<dyn Fn(u32) -> u32>,
    /// the output ports of the switch, with consecutive port ids start from 0
    pub ports: Vec<Port>,
    /// senders for sending outbound packets to ports or schedulers
    pub senders: Vec<UnboundedSender<Packet>>,
    /// a receiver for receiving inbound packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl Element for SimplePacketSwitch {
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    fn connect_senders(&mut self, senders: Vec<UnboundedSender<Packet>>) {
        assert_eq!(
            self.ports.len(),
            senders.len(),
            "The number of senders is not equal to the number of ports."
        );
        for (port, sender) in self.ports.iter_mut().zip(senders.iter()) {
            port.connect_sender(sender.clone());
        }
    }
}

impl SimplePacketSwitch {
    pub fn new(
        element_id: u32,
        nports: u32,
        port_rate: f64,
        buffer_size: u32,
    ) -> SimplePacketSwitch {
        let mut ports = Vec::new();
        let mut senders = Vec::new();
        for i in 0..nports {
            let (sender, receiver) = unbounded_channel();
            let mut port = Port::new(i, port_rate, buffer_size, false);
            port.connect_receiver(receiver);
            senders.push(sender);
            ports.push(port);
        }
        let fib = Vec::new();
        SimplePacketSwitch {
            element_id,
            packets_received: 0,
            fib,
            flow_classes: Box::new(|flow_id| flow_id),
            ports,
            senders,
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        for port in self.ports {
            sim.activate(port.run(sim))
        }
        loop {
            if let Some(packet) = self.receiver.recv().await {
                self.packets_received += 1;
                let flow_class = (self.flow_classes)(packet.flow_id);

                // forwards packets to their corresponding ports
                if let Some(&port_id) = self.fib.get(flow_class as usize) {
                    let _ = self.senders.get_mut(port_id as usize).unwrap().send(packet);
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

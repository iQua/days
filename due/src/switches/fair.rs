//! Implements a fair packet switch with various schedulers, as well as bounded
//! buffers, on each of the outgoing ports.

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::Shared;
use crate::{sim::SimContext, Element};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct FairPacketSwitch {
    element_id: usize,
    /// the number of packets received by the switch
    packets_received: usize,
    /// the fib demux of the switch,
    fib: Vec<usize>,
    /// a closure that maps a flow_id to a class_id
    pub flow_classes: Box<dyn Fn(usize) -> usize>,
    /// the schedulers of the switch, with consecutive ids start from 0
    pub ports: Vec<DRRServer>,
    /// senders for sending outbound packets to ports or schedulers
    pub senders: Vec<UnboundedSender<Packet>>,
    /// a receiver for receiving inbound packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl Element for FairPacketSwitch {
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

impl FairPacketSwitch {
    pub fn new(
        element_id: usize,
        nports: usize,
        port_rate: f64,
        buffer_size: usize,
        weights: Vec<usize>,
    ) -> FairPacketSwitch {
        let mut ports = Vec::new();

        // the senders from the FairPacketSwitch to ports
        let mut senders = Vec::new();

        for i in 0..nports {
            let (sender, receiver) = unbounded_channel();

            let mut scheduler = DRRServer::new(
                i,
                buffer_size,
                CapacityUnit::Packets,
                port_rate,
                DropStrategy::TailDrop,
                weights.clone(),
            );

            scheduler.connect_receiver(receiver);
            senders.push(sender);
            ports.push(scheduler);
        }
        let fib = Vec::new();
        FairPacketSwitch {
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
        for scheduler in self.ports {
            sim.activate(scheduler.run(sim))
        }

        loop {
            if let Some(packet) = self.receiver.recv().await {
                self.packets_received += 1;
                let flow_class = (self.flow_classes)(packet.flow_id);

                // forwards packets to their corresponding ports
                if let Some(&port_id) = self.fib.get(flow_class) {
                    let _ = self.senders[port_id].send(packet);
                } else {
                    println!("Wrong fib demux in SimplePacketSwitch {}.", self.element_id);
                }
            } else {
                break;
            }
        }

        println!(
            "FairPacketSwitch {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

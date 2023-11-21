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
    /// senders for sending inbound packets to ports
    port_senders: Vec<UnboundedSender<Packet>>,
    /// senders for sending outbound packets to ports or schedulers
    senders: Vec<UnboundedSender<Packet>>,
    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl Element for FairPacketSwitch {
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.senders.push(sender.clone());
    }
}

impl FairPacketSwitch {
    pub fn new(
        element_id: usize,
        nports: usize,
        port_rate: f64,
        capacity: usize,
        weights: Vec<usize>,
        fib: Vec<usize>,
    ) -> FairPacketSwitch {
        let mut ports = Vec::new();

        // the senders from the FairPacketSwitch to ports
        let mut port_senders = Vec::new();

        for i in 0..nports {
            let (sender, receiver) = unbounded_channel();

            let mut scheduler = DRRServer::new(
                i,
                capacity,
                CapacityUnit::Packets,
                port_rate,
                DropStrategy::TailDrop,
                weights.clone(),
            );

            scheduler.connect_receiver(receiver);
            port_senders.push(sender);
            ports.push(scheduler);
        }

        FairPacketSwitch {
            element_id,
            packets_received: 0,
            fib,
            flow_classes: Box::new(|flow_id| flow_id),
            ports,
            port_senders,
            senders: Vec::new(),
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        // connects ports to outbound senders
        let mut i = 0;

        for mut scheduler in self.ports {
            scheduler.connect_sender(self.senders[i].clone());
            sim.activate(scheduler.run(sim));
            i += 1;
        }

        loop {
            if let Some(packet) = self.receiver.recv().await {
                self.packets_received += 1;
                let flow_class = (self.flow_classes)(packet.flow_id);

                println!(
                    "FairPacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                    {} packets received.",
                    self.element_id,
                    packet.packet_id,
                    packet.size,
                    packet.flow_id,
                    sim.now(),
                    self.packets_received
                );

                // forwards packets to their corresponding ports
                let port_id = self.fib[flow_class];
                let _ = self.port_senders[port_id].send(packet);
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

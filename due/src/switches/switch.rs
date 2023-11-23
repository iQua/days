//! Implements a fair packet switch with various schedulers, as well as bounded
//! buffers, on each of the outgoing ports.

use std::any::Any;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::switches::SchedulingDiscipline;
use crate::Shared;
use crate::{sim::SimContext, Element};

pub struct PacketSwitch {
    element_id: usize,
    /// Scheduling discipline
    discipline: SchedulingDiscipline,
    /// the number of packets received by the switch
    packets_received: usize,
    /// the flow information base (FIB) of the switch
    /// class_id -> the outbound port_id
    fib: Vec<usize>,
    /// a closure that maps flow_id -> class_id
    pub flow_classes: Box<dyn Fn(usize) -> usize>,
    /// the outbound ports, with consecutive ids starting from 0
    /// each of these ports is governed by a DRR or FIFO scheduler
    ports: Vec<Box<dyn Any>>,
    /// senders for sending inbound packets to outbound ports
    port_senders: Vec<UnboundedSender<Packet>>,

    /// senders for sending outbound packets to downstream elements
    senders: Vec<UnboundedSender<Packet>>,
    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl Element for PacketSwitch {
    fn id(&mut self) -> usize {
        self.element_id
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.senders.push(sender.clone());
    }
}

impl PacketSwitch {
    pub fn new(
        element_id: usize,
        nports: usize,
        port_rate: f64,
        capacity: usize,
        weights: Vec<usize>,
        fib: Vec<usize>,
        discipline: SchedulingDiscipline,
    ) -> PacketSwitch {
        let mut ports: Vec<Box<dyn Any>> = Vec::new();

        // the senders from the demultiplexer to ports inside the switch
        let mut port_senders = Vec::new();

        for i in 0..nports {
            let (sender, receiver) = unbounded_channel();

            match discipline {
                SchedulingDiscipline::DRR => {
                    let mut port = DRRServer::new(
                        i,
                        port_rate,
                        capacity,
                        CapacityUnit::Packets,
                        DropStrategy::TailDrop,
                        weights.clone(),
                    );

                    port.connect_receiver(receiver);
                    port_senders.push(sender);
                    ports.push(Box::new(port));
                }
                SchedulingDiscipline::FIFO => {
                    let mut port = Port::new(
                        i,
                        port_rate,
                        capacity,
                        CapacityUnit::Packets,
                        DropStrategy::TailDrop,
                    );

                    port.connect_receiver(receiver);
                    port_senders.push(sender);
                    ports.push(Box::new(port));
                }
            }
        }

        PacketSwitch {
            element_id,
            packets_received: 0,
            discipline,
            fib,
            flow_classes: Box::new(|flow_id| flow_id),
            ports,
            port_senders,
            senders: Vec::new(),
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        // connects ports to outbound senders and activates them for execution
        let mut i = 0;

        match self.discipline {
            SchedulingDiscipline::DRR => {
                for port in self.ports {
                    let mut p = port.downcast::<DRRServer>().unwrap();
                    p.connect_sender(self.senders[i].clone());
                    sim.activate(p.run(sim));

                    i += 1;
                }
            }
            SchedulingDiscipline::FIFO => {
                for port in self.ports {
                    let mut p = port.downcast::<Port>().unwrap();
                    p.connect_sender(self.senders[i].clone());
                    sim.activate(p.run(sim));

                    i += 1;
                }
            }
        }

        while let Some(packet) = self.receiver.recv().await {
            self.packets_received += 1;

            println!(
                "PacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                    {} packets received.",
                self.element_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                sim.now(),
                self.packets_received
            );

            // forwards packets to their corresponding outbound ports
            let port_id = self.fib[packet.flow_id];
            let _ = self.port_senders[port_id].send(packet);
        }

        println!(
            "PacketSwitch {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

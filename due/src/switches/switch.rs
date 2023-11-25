//! Implements a fair packet switch with various schedulers, as well as bounded
//! buffers, on each of the outgoing ports.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::sim::SimContext;
use crate::switches::SchedulingDiscipline;
use crate::{get_id, Scheduler, Shared};

pub struct PacketSwitch {
    element_id: usize,
    /// Scheduling discipline
    discipline: SchedulingDiscipline,
    /// the number of packets received by the switch
    packets_received: usize,
    /// the flow information base (FIB) of the switch
    /// class_id -> the outbound port_id
    fib: Vec<usize>,
    /// the outbound ports, with consecutive ids starting from 0
    /// each of these ports is governed by a DRR or FIFO scheduler
    ports: Vec<Box<dyn Any>>,
    /// senders for sending inbound packets to outbound ports
    port_senders: Vec<UnboundedSender<Packet>>,

    /// senders for sending outbound packets to downstream elements
    /// element_id -> UnboundedSender<Packet>
    senders: HashMap<usize, UnboundedSender<Packet>>,

    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl PacketSwitch {
    pub fn new(
        nports: usize,
        port_rate: f64,
        capacity: usize,
        weights: Vec<usize>,
        fib: Vec<usize>,
        discipline: SchedulingDiscipline,
        flow_classes: Arc<dyn Fn(usize) -> usize>,
    ) -> PacketSwitch {
        let mut ports: Vec<Box<dyn Any>> = Vec::new();

        // the senders from the demultiplexer to ports inside the switch
        let mut port_senders = Vec::new();

        for scheduler_id in 0..nports {
            let (sender, receiver) = unbounded_channel();

            match discipline {
                SchedulingDiscipline::DRR => {
                    let mut port = DRRServer::new(
                        scheduler_id,
                        port_rate,
                        capacity,
                        CapacityUnit::Packets,
                        flow_classes.clone(),
                        DropStrategy::TailDrop,
                        weights.clone(),
                    );

                    port.connect_receiver(receiver);
                    port_senders.push(sender);
                    ports.push(Box::new(port));
                }
                SchedulingDiscipline::FIFO => {
                    let mut port = Port::new(
                        scheduler_id,
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
            element_id: get_id(),
            packets_received: 0,
            discipline,
            fib,
            ports,
            port_senders,
            senders: HashMap::new(),
            receiver: unbounded_channel().1,
        }
    }

    pub fn get_sender(&self, element_id: usize) -> Option<UnboundedSender<Packet>> {
        if let Some(sender) = self.senders.get(&element_id) {
            return Some(sender.clone());
        }

        None
    }

    pub fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    pub fn connect_sender(&mut self, element_id: usize, sender: UnboundedSender<Packet>) {
        self.senders.insert(element_id, sender.clone());
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        // connects ports to outbound senders and activates them for execution
        match self.discipline {
            SchedulingDiscipline::DRR => {
                let mut senders_iter = self.senders.iter();

                for port in self.ports {
                    let mut p = port.downcast::<DRRServer>().unwrap();

                    if let Some((_, sender)) = senders_iter.next() {
                        p.connect_sender(sender.clone());
                    } else {
                        panic!("Not enough senders for ports.");
                    }

                    sim.activate(p.run(sim));
                }
            }
            SchedulingDiscipline::FIFO => {
                let mut senders_iter = self.senders.iter();

                for port in self.ports {
                    let mut p = port.downcast::<Port>().unwrap();

                    if let Some((_, sender)) = senders_iter.next() {
                        p.connect_sender(sender.clone());
                    } else {
                        panic!("Not enough senders for ports.");
                    }
                    sim.activate(p.run(sim));
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

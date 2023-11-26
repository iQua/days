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
use crate::schedulers::Scheduler;
use crate::sim::SimContext;
use crate::switches::SchedulingDiscipline;
use crate::{next_element_id, Shared};

pub struct PacketSwitch {
    element_id: usize,
    /// the bit rate of each outbound port
    port_rate: f64,
    /// the capacity of each outbound port
    capacity: usize,
    /// flow_id -> class_id
    flow_classes: Arc<dyn Fn(usize) -> usize>,
    /// the weights of the classes
    weights: Vec<usize>,
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
    /// element_id -> Scheduler
    port_senders: HashMap<usize, UnboundedSender<Packet>>,

    /// senders for sending outbound packets to downstream elements
    /// element_id -> UnboundedSender<Packet>
    senders: HashMap<usize, UnboundedSender<Packet>>,

    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl PacketSwitch {
    pub fn new(
        port_rate: f64,
        capacity: usize,
        weights: Vec<usize>,
        fib: Vec<usize>,
        discipline: SchedulingDiscipline,
        flow_classes: Arc<dyn Fn(usize) -> usize>,
    ) -> PacketSwitch {
        // outbound ports
        let ports: Vec<Box<dyn Any>> = Vec::new();
        // the senders from the demultiplexer to ports inside the switch
        let port_senders = HashMap::new();

        PacketSwitch {
            element_id: next_element_id(),
            port_rate,
            capacity,
            flow_classes,
            weights,
            discipline,
            fib,
            ports,
            packets_received: 0,
            port_senders,
            senders: HashMap::new(),
            receiver: unbounded_channel().1,
        }
    }

    pub fn id(&self) -> usize {
        self.element_id
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
        // if element_id is u32::MAX, then the sender is an endpoint (i.e., a source or sink)
        let (port_sender, port_receiver) = unbounded_channel();
        let port_id = self.ports.len() + 1;

        // creates a port with the specified scheduling discipline
        match self.discipline {
            SchedulingDiscipline::DRR => {
                let mut port = DRRServer::new(
                    port_id,
                    self.port_rate,
                    self.capacity,
                    CapacityUnit::Packets,
                    self.flow_classes.clone(),
                    DropStrategy::TailDrop,
                    self.weights.clone(),
                );

                port.connect_receiver(port_receiver);
                self.port_senders.insert(port_id, port_sender);
                self.ports.push(Box::new(port));
            }
            SchedulingDiscipline::FIFO => {
                let mut port = Port::new(
                    port_id,
                    self.port_rate,
                    self.capacity,
                    CapacityUnit::Packets,
                    DropStrategy::TailDrop,
                );

                port.connect_receiver(port_receiver);
                self.port_senders.insert(port_id, port_sender);
                self.ports.push(Box::new(port));
            }
        }

        self.senders.insert(element_id, sender.clone());

        println!(
            "PacketSwitch {} connected its sender to element {}.",
            self.element_id, element_id
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        // connects ports to outbound senders and activates them for execution
        match self.discipline {
            SchedulingDiscipline::DRR => {
                println!("drr length of senders: {}", self.senders.len());
                println!("drr length of ports: {}", self.ports.len());
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
            if let Some(port_sender) = self.port_senders.get(&port_id) {
                let _ = port_sender.send(packet);
            }
        }

        println!(
            "PacketSwitch {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

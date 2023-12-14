//! Implements a fair packet switch with various schedulers, as well as bounded
//! buffers, on each of the outgoing ports.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use log::{debug, info};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::flows::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::schedulers::wfq::WFQServer;
use crate::schedulers::Scheduler;
use crate::sim::SimContext;
use crate::switches::SchedulingDiscipline;
use crate::{next_element_id, num_elements, Shared};

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
    /// flow_id -> element_id
    fib: HashMap<usize, usize>,

    /// the outbound ports, each of which is governed by a DRR, FIFO, or WFQ
    /// scheduler
    /// element_id -> Scheduler
    ports: HashMap<usize, Box<dyn Any>>,

    /// senders for sending inbound packets to outbound ports
    /// element_id -> Scheduler
    pub port_senders: HashMap<usize, UnboundedSender<Packet>>,

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
        fib: HashMap<usize, usize>,
        discipline: SchedulingDiscipline,
        flow_classes: Arc<dyn Fn(usize) -> usize>,
    ) -> PacketSwitch {
        // outbound ports
        let ports: HashMap<usize, Box<dyn Any>> = HashMap::new();
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

    pub fn set_fib(&mut self, flow_id: usize, next_id: usize) {
        self.fib.insert(flow_id, next_id);
    }

    pub fn get_fib(&self) -> &HashMap<usize, usize> {
        &self.fib
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
        let (port_sender, port_receiver) = unbounded_channel();
        // creates a port with the specified scheduling discipline
        match self.discipline {
            SchedulingDiscipline::DRR => {
                let mut port;
                if element_id < num_elements() {
                    // sends to another network element
                    port = DRRServer::new(
                        self.port_rate,
                        self.capacity,
                        CapacityUnit::Packets,
                        self.flow_classes.clone(),
                        DropStrategy::TailDrop,
                        self.weights.clone(),
                    );
                } else {
                    port = DRRServer::new(
                        0.0,
                        0,
                        CapacityUnit::Packets,
                        self.flow_classes.clone(),
                        DropStrategy::TailDrop,
                        self.weights.clone(),
                    );
                }

                port.connect_receiver(port_receiver);
                self.port_senders.insert(element_id, port_sender);
                self.ports.insert(element_id, Box::new(port));
            }
            SchedulingDiscipline::FIFO => {
                let mut port;
                if element_id < num_elements() {
                    // sends to another network element
                    port = Port::new(
                        self.port_rate,
                        self.capacity,
                        CapacityUnit::Packets,
                        DropStrategy::TailDrop,
                    );
                } else {
                    port = Port::new(0.0, 0, CapacityUnit::Packets, DropStrategy::TailDrop);
                }

                port.connect_receiver(port_receiver);
                self.port_senders.insert(element_id, port_sender);
                self.ports.insert(element_id, Box::new(port));
            }
            SchedulingDiscipline::WFQ => {
                let mut port;
                if element_id < num_elements() {
                    // sends to another network element
                    port = WFQServer::new(
                        self.port_rate,
                        self.capacity,
                        CapacityUnit::Packets,
                        self.flow_classes.clone(),
                        DropStrategy::TailDrop,
                        self.weights.clone(),
                    );
                } else {
                    port = WFQServer::new(
                        0.0,
                        0,
                        CapacityUnit::Packets,
                        self.flow_classes.clone(),
                        DropStrategy::TailDrop,
                        self.weights.clone(),
                    );
                }

                port.connect_receiver(port_receiver);
                self.port_senders.insert(element_id, port_sender);
                self.ports.insert(element_id, Box::new(port));
            }
        }

        self.senders.insert(element_id, sender.clone());
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        // connects ports to outbound senders and activates them for execution
        match self.discipline {
            SchedulingDiscipline::DRR => {
                for (element_id, port) in self.ports {
                    let mut p = port.downcast::<DRRServer>().unwrap();

                    if let Some(sender) = self.senders.get(&element_id) {
                        p.connect_sender(sender.clone());
                    } else {
                        panic!("Not enough senders for ports.");
                    }

                    sim.activate(p.run(sim));
                }
            }
            SchedulingDiscipline::FIFO => {
                for (element_id, port) in self.ports {
                    let mut p = port.downcast::<Port>().unwrap();

                    if let Some(sender) = self.senders.get(&element_id) {
                        p.connect_sender(sender.clone());
                    } else {
                        panic!("Not enough senders for ports.");
                    }
                    sim.activate(p.run(sim));
                }
            }
            SchedulingDiscipline::WFQ => {
                for (element_id, port) in self.ports {
                    let mut p = port.downcast::<WFQServer>().unwrap();

                    if let Some(sender) = self.senders.get(&element_id) {
                        p.connect_sender(sender.clone());
                    } else {
                        panic!("Not enough senders for ports.");
                    }

                    sim.activate(p.run(sim));
                }
            }
        }

        while let Some(mut packet) = self.receiver.recv().await {
            self.packets_received += 1;
            packet.arrival_update(sim.now());

            debug!(
                "PacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                    {} packets received.",
                self.element_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                sim.now(),
                self.packets_received
            );

            // forwards packets to their corresponding downstream elements
            let element_id = self.fib[&packet.flow_id];
            if let Some(port_sender) = self.port_senders.get(&element_id) {
                let _ = port_sender.send(packet);
            }
        }

        info!(
            "PacketSwitch {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}

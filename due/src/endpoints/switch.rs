//! Implements a fair packet switch with various schedulers, as well as bounded
//! buffers, on each of the outgoing ports.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::simulation::Mailbox;
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::endpoints::drop::{CapacityUnit, DropStrategy};
use crate::endpoints::drr::DRRServer;
use crate::endpoints::packet::Packet;
use crate::endpoints::port::Port;
use crate::endpoints::SchedulingDiscipline;
use crate::{next_element_id, num_elements};

pub struct PacketSwitch {
    element_id: usize,
    /// the bit rate of each outbound port
    port_rate: f64,
    /// the capacity of each outbound port
    capacity: usize,
    /// flow_id -> class_id
    flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
    /// the weights of the classes
    weights: Vec<usize>,
    /// Scheduling discipline
    discipline: SchedulingDiscipline,
    /// the number of packets received by the switch
    packets_received: usize,
    /// the flow information base (FIB) of the switch
    /// flow_id -> element_id
    fib: HashMap<usize, usize>,

    /// the outbound ports, each of which is governed by a DRR or FIFO scheduler
    /// element_id -> Scheduler
    ports: HashMap<usize, Arc<Mutex<dyn Any + Send>>>,

    /// senders for sending inbound packets to outbound ports
    /// element_id -> scheduler
    pub port_senders: HashMap<usize, Output<Packet>>,

    /// senders for sending outbound packets to downstream elements
    /// element_id -> the sender to a downstream element
    pub senders: HashMap<usize, Output<Packet>>,
}

impl PacketSwitch {
    pub fn new(
        port_rate: f64,
        capacity: usize,
        weights: Vec<usize>,
        fib: HashMap<usize, usize>,
        discipline: SchedulingDiscipline,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
    ) -> PacketSwitch {
        // outbound ports
        let ports: HashMap<usize, Arc<Mutex<dyn Any + Send>>> = HashMap::new();
        // the senders from the demultiplexer to ports inside the switch
        let port_senders = HashMap::new();
        let senders = HashMap::new();

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
            senders,
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

    pub fn connect_sender(&mut self, element_id: usize) {
        // creates a port with the specified scheduling discipline
        match self.discipline {
            SchedulingDiscipline::DRR => {
                let port;
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

                let drr_mbox = Mailbox::new();
                let mut port_sender = Output::default();

                port_sender.connect(DRRServer::packet_received, &drr_mbox);
                self.port_senders.insert(element_id, port_sender);
                self.ports.insert(element_id, Arc::new(Mutex::new(port)));
            }
            SchedulingDiscipline::FIFO => {
                let port;
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

                let port_mbox = Mailbox::new();
                let mut port_sender = Output::default();

                port_sender.connect(Port::packet_received, &port_mbox);
                self.port_senders.insert(element_id, port_sender);
                self.ports.insert(element_id, Arc::new(Mutex::new(port)));
            }
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        self.packets_received += 1;

        debug!(
            "PacketSwitch {} received packet {} ({} bytes) from flow {} at time {:.3}. \
                {} packets received.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            arrival_time,
            self.packets_received
        );

        // forwards packets to their corresponding downstream elements
        let element_id = self.fib[&packet.flow_id];
        if let Some(port_sender) = self.port_senders.get_mut(&element_id) {
            port_sender.send(packet).await;
        }
    }

    // pub async fn get_sender(mut self, element_id: usize) -> &'a Output<Packet> {
    //     match self.discipline {
    //         SchedulingDiscipline::DRR => {
    //             if let Some(port) = self.ports.get_mut(&element_id) {
    //                 let p = port.lock().unwrap().downcast_ref::<DRRServer>().unwrap();
    //                 &p.output
    //             } else {
    //                 panic!("No sender found for element {}.", element_id);
    //             }
    //         }
    //         SchedulingDiscipline::FIFO => {
    //             if let Some(port) = self.ports.get_mut(&element_id) {
    //                 let p = port.lock().unwrap().downcast_ref::<Port>().unwrap();
    //                 &p.output
    //             } else {
    //                 panic!("No sender found for element {}.", element_id);
    //             }
    //         }
    //     }
    // }
}

impl Model for PacketSwitch {}

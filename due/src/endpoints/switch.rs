//! Implements a fair packet switch with various schedulers, as well as bounded
//! buffers, on each of the outgoing ports.

use std::collections::HashMap;
use std::sync::Arc;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::endpoints::packet::Packet;
use crate::next_element_id;

pub struct PacketSwitch {
    element_id: usize,
    /// flow_id -> class_id
    flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
    /// the number of packets received by the switch
    packets_received: usize,
    /// the flow information base (FIB) of the switch
    /// flow_id -> element_id
    fib: HashMap<usize, usize>,

    /// senders for sending inbound packets to outbound ports
    /// element_id -> scheduler
    pub outputs: HashMap<usize, Output<Packet>>,
}

impl PacketSwitch {
    pub fn new(
        fib: HashMap<usize, usize>,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
    ) -> PacketSwitch {
        // the senders from the demultiplexer to ports inside the switch
        let mut outputs = HashMap::new();

        for element_id in fib.values() {
            if !outputs.contains_key(element_id) {
                outputs.insert(*element_id, Output::default());
            }
        }

        PacketSwitch {
            element_id: next_element_id(),
            flow_classes,
            fib,
            packets_received: 0,
            outputs,
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
        let flow_class = (self.flow_classes)(packet.flow_id);
        let element_id = self.fib[&flow_class];

        if let Some(output) = self.outputs.get_mut(&element_id) {
            output.send(packet).await;
        }
    }
}

impl Model for PacketSwitch {}

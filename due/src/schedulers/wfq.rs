//! Implements a Weighted Fair Queueing (WFQ) scheduler.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use rand::distributions::weighted;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::schedulers::Scheduler;
use crate::sim::{SimContext, Time};
use crate::{next_scheduler_id, Shared};

#[derive(Debug, Clone)]
pub struct TaggedPacket {
    pub packet: Packet,
    /// tag is the finish time of the packet
    pub tag: f64,
}

impl Ord for TaggedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.tag > other.tag {
            Ordering::Greater;
        } else if self.tag < other.tag {
            Ordering::Less;
        }
        Ordering::Equal
    }
}

impl PartialOrd for TaggedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TaggedPacket {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl Eq for TaggedPacket {}

pub struct WFQServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based WFQ. The default uses a packet's flow_id as its class_id,
    /// which is equivalent to flow-based WFQ.
    pub flow_classes: Arc<dyn Fn(usize) -> usize>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop>,

    weights: Vec<f64>,
    /// finish time of the last packet served in each class
    finish_times: Vec<f64>,
    flow_queue_count: Vec<usize>,
    active_set: Vec<usize>,
    vtime: f64,
    last_update: f64,

    /// the number of packets received, dropped, and in the queues waiting to be sent
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<usize>,

    /// priority queue of packets from all the classes, where packets are sorted according to their finish times
    scheduler_queue: BinaryHeap<TaggedPacket>,

    /// a sender for sending outbound packets to the downstream element
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets from upstream elements
    pub receiver: UnboundedReceiver<Packet>,
}

impl Scheduler for WFQServer {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl WFQServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize>,
        drop_strategy: DropStrategy,
        weights: Vec<f64>,
    ) -> WFQServer {
        let mut finish_times = Vec::new();
        let mut flow_queue_count = Vec::new();
        let mut active_set = Vec::new();
        let mut vtime = 0.0;
        let mut last_update = 0.0;
        let mut byte_sizes = Vec::new();
        let mut scheduler_queue = BinaryHeap::new();
        let (sender, receiver) = unbounded_channel();

        for (class_id, _) in weights.iter().enumerate() {
            finish_times.push(0.0);
            flow_queue_count.push(0);
            byte_sizes.push(0);
        }

        let packet_drop = match drop_strategy {
            DropStrategy::TailDrop => TailDrop::new(capacity, capacity_unit),
            _ => unimplemented!(),
        };

        WFQServer {
            scheduler_id: next_scheduler_id(),
            rate,
            flow_classes,
            drop_strategy: Box::new(packet_drop),
            weights,
            finish_times,
            flow_queue_count,
            active_set,
            vtime,
            last_update,
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            byte_sizes,
            scheduler_queue,
            sender,
            receiver,
        }
    }

    fn packet_received(&mut self, packet: TaggedPacket, now: Time) {
        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.packet.size,
            self.byte_sizes.iter().sum(),
            self.scheduler_queue.len(),
        );

        // the case that this packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            println! {
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet.packet_id,
                packet.packet.flow_id,
                now
            }
            return;
        }

        self.packets_waiting += 1;
        self.packets_received += 1;

        let class_id = (self.flow_classes)(packet.packet.flow_id);

        self.scheduler_queue.push(packet.clone());

        self.byte_sizes[class_id] += packet.packet.size;

        println!(
            "WFQServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in queue {}.",
            self.scheduler_id,
            packet.packet.packet_id,
            packet.packet.size,
            packet.packet.flow_id,
            now,
            self.packets_received,
            self.scheduler_queue.len(),
            class_id
        );
    }

    fn update_stats(&mut self, packet: TaggedPacket, now: Time) {
        let mut weight_sum = 0.0;

        // updates the virtual time based on the current set of active flow classes
        for i in self.active_set {
            weight_sum += self.weights[i];
        }

        self.vtime += (now - self.last_update) / weight_sum;

        // computes the new set of active flow classes
        let class_id = (self.flow_classes)(packet.packet.flow_id);

        self.flow_queue_count[class_id] -= 1;
        if self.flow_queue_count[class_id] == 0 {
            let index = self.active_set.iter().position(|x| *x == class_id).unwrap();
            self.active_set.remove(index);
        }

        if self.active_set.is_empty() {
            self.vtime = 0.0;
            self.finish_times[class_id] = 0.0;
        }

        self.last_update = now;

        self.byte_sizes[class_id] -= packet.packet.size;
    }
}

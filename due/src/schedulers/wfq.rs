//! Implements a Weighted Fair Queueing (WFQ) scheduler.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use log::{debug, info};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::flows::packet::Packet;
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

impl PartialOrd for TaggedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.tag.partial_cmp(&self.tag)
    }
}

impl PartialEq for TaggedPacket {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl Ord for TaggedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
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

    /// weights of classes
    weights: Vec<usize>,
    /// finish time of the last packet served in each class
    finish_times: Vec<f64>,
    /// number of to-be-sent packets of each class
    flow_queue_count: Vec<usize>,

    /// set of active flow classes
    active_set: Vec<usize>,

    vtime: f64,
    last_update: f64,

    /// the number of packets received, dropped, and in the queues waiting to be sent
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<usize>,

    /// min-heap of packets from all the classes, where packets are sorted according to their finish times
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
        weights: Vec<usize>,
    ) -> WFQServer {
        let mut finish_times = Vec::new();
        let mut flow_queue_count = Vec::new();
        let mut active_set = Vec::new();
        let mut vtime = 0.0;
        let mut last_update = 0.0;
        let mut byte_sizes = Vec::new();
        let mut scheduler_queue = BinaryHeap::new();
        let (sender, receiver) = unbounded_channel();

        for _ in weights.iter().enumerate() {
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

    fn packet_received(&mut self, packet: Packet, now: Time) {
        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.iter().sum(),
            self.scheduler_queue.len(),
        );

        // the case that this packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                now
            }
            return;
        }

        self.packets_waiting += 1;
        self.packets_received += 1;

        let class_id = (self.flow_classes)(packet.flow_id);

        // adds tag (finish time) to the packet before push to the queue (a min-heap)
        let tagged_packet = self.add_tag(packet.clone(), now);
        self.scheduler_queue.push(tagged_packet.clone());

        self.byte_sizes[class_id] += packet.size;
        self.flow_queue_count[class_id] += 1;
        self.active_set.push(class_id);

        debug!(
            "WFQServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            now,
            self.packets_received,
            self.scheduler_queue.len()
        );
    }

    fn add_tag(&mut self, packet: Packet, now: Time) -> TaggedPacket {
        let mut finish_time = 0.0;
        // updates the virtual time and the finish time for each flow class
        if self.active_set.is_empty() {
            self.vtime = 0.0;
            for i in 1..self.finish_times.len() {
                self.finish_times[i] = 0.0;
            }
        } else {
            let mut weight_sum = 0.0;
            for class_id in self.active_set.clone() {
                weight_sum += self.weights[class_id] as f64;
            }

            self.vtime += (now - self.last_update) / weight_sum;
            let class_id = (self.flow_classes)(packet.flow_id);
            finish_time = self.vtime.max(self.finish_times[class_id])
                + packet.size as f64 * 8.0 / (self.rate * self.weights[class_id] as f64);
            self.finish_times[class_id] = finish_time;
        }

        TaggedPacket {
            packet: packet,
            tag: finish_time,
        }
    }

    fn update_stats(&mut self, packet: Packet, now: Time) {
        let mut weight_sum = 0.0;

        // updates the virtual time based on the current set of active flow classes
        for class_id in self.active_set.clone() {
            weight_sum += self.weights[class_id] as f64;
        }

        self.vtime += (now - self.last_update) / weight_sum;

        // computes the new set of active flow classes
        let class_id = (self.flow_classes)(packet.flow_id);

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
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // schedules packets by going through the queue
            while !self.scheduler_queue.is_empty() {
                let packet = self.scheduler_queue.peek().unwrap().packet.clone();
                let class_id = (self.flow_classes)(packet.flow_id);

                self.byte_sizes[class_id] -= packet.size;

                let timeout = (packet.size as f64) * 8.0 / self.rate;
                sim.advance(timeout).await;
                let mut outbound = self.scheduler_queue.pop().unwrap();
                outbound.packet.send(sim.now());
                let _ = self.sender.send(packet.clone());

                self.packets_waiting -= 1;

                self.update_stats(packet.clone(), sim.now());

                // polls for and receives all outstanding packets
                // recently sent to WFQServer while sending the previous
                // packet
                while let Ok(packet) = self.receiver.try_recv() {
                    self.packet_received(packet, sim.now());
                }

                debug!(
                    "WFQServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
                            {} packets in the queue.",
                    self.scheduler_id,
                    packet.packet_id,
                    packet.size,
                    packet.flow_id,
                    sim.now(),
                    self.active_set.len(),
                );
            } // finishes going through the queue in one round

            // waits for inbound packets from the upstream element
            if self.packets_waiting == 0 {
                if let Some(packet) = self.receiver.recv().await {
                    self.packet_received(packet, sim.now());
                } else {
                    break;
                }
            }
        }
        info!(
            "WFQServer {} finished running at time {}.",
            self.scheduler_id,
            sim.now()
        );
    }
}

//! Implements a Weighted Fair Queueing (WFQ) scheduler.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};

pub struct TaggedPacket {
    pub packet: Packet,
    /// tag is the finish time of the packet
    pub tag: f64,
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

impl Ord for TaggedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag
            .partial_cmp(&other.tag)
            .unwrap_or(Ordering::Equal)
            .reverse()
    }
}

impl Eq for TaggedPacket {}

pub struct WFQServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Weighted Fair Queueing. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based WFQ.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// weights of classes
    weights: Vec<usize>,
    /// finish time of the last packet served in each class
    finish_times: Vec<f64>,
    /// number of to-be-sent packets of each class
    flow_queue_count: Vec<usize>,

    /// set of active flow classes
    active_set: HashSet<usize>,

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

    /// The server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,
}

impl WFQServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        weights: Vec<usize>,
    ) -> WFQServer {
        let mut finish_times = Vec::new();
        let mut flow_queue_count = Vec::new();
        let mut byte_sizes = Vec::new();

        for _ in &weights {
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
            active_set: HashSet::new(),
            vtime: 0.0,
            last_update: 0.0,
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            byte_sizes,
            scheduler_queue: BinaryHeap::new(),
            busy_until: 0.0,
            output: Output::default(),
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

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
                arrival_time
            }
            return;
        }

        self.packets_waiting += 1;
        self.packets_received += 1;

        let class_id = (self.flow_classes)(packet.flow_id);

        // adds tag (finish time) to the packet before push to the queue (a min-heap)
        let (finish_time, tagged_packet) = self.add_tag(packet.clone(), arrival_time);
        self.scheduler_queue.push(tagged_packet);

        self.byte_sizes[class_id] += packet.size;
        self.flow_queue_count[class_id] += 1;
        self.active_set.insert(class_id);

        debug!(
            "WFQServer {} received packet {} ({} bytes, finish time {:.3}) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            finish_time,
            packet.flow_id,
            arrival_time,
            self.packets_received,
            self.scheduler_queue.len(),
        );

        if arrival_time > self.busy_until {
            self.run((), scheduler);
        }
    }

    fn add_tag(&mut self, packet: Packet, now: f64) -> (f64, TaggedPacket) {
        let mut finish_time = 0.0;
        // updates the virtual time and the finish time for each flow class
        if self.active_set.is_empty() {
            self.vtime = 0.0;
            for time in &mut self.finish_times {
                *time = 0.0;
            }
        } else {
            let mut weight_sum = 0.0;
            for class_id in &self.active_set {
                weight_sum += self.weights[*class_id] as f64;
            }

            self.vtime += (now - self.last_update) / weight_sum;
            let class_id = (self.flow_classes)(packet.flow_id);
            finish_time = self.vtime.max(self.finish_times[class_id])
                + packet.size as f64 * 8.0 / (self.rate * self.weights[class_id] as f64);
            self.finish_times[class_id] = finish_time;
        }

        (
            finish_time,
            TaggedPacket {
                packet,
                tag: finish_time,
            },
        )
    }

    fn update_stats(&mut self, packet: &Packet, now: f64) {
        let mut weight_sum = 0.0;

        // updates the virtual time based on the current set of active flow classes
        for class_id in &self.active_set {
            weight_sum += self.weights[*class_id] as f64;
        }

        self.vtime += (now - self.last_update) / weight_sum;

        // computes the new set of active flow classes
        let class_id = (self.flow_classes)(packet.flow_id);

        self.flow_queue_count[class_id] -= 1;
        if self.flow_queue_count[class_id] == 0 {
            self.active_set.remove(&class_id);
        }

        if self.active_set.is_empty() {
            self.vtime = 0.0;
            self.finish_times[class_id] = 0.0;
        }

        self.last_update = now;
    }

    pub async fn send(&mut self, packet: Packet) {
        self.output.send(packet).await;
    }

    pub fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
        let now = current_time.as_secs_f64();

        // schedules packets in the current packet class being served
        loop {
            if self.packets_waiting == 0 {
                // all packets in the queues have been processed
                return;
            }

            if !self.scheduler_queue.is_empty() {
                let mut outbound = self.scheduler_queue.pop().unwrap().packet;
                let class_id = (self.flow_classes)(outbound.flow_id);
                self.byte_sizes[class_id] -= outbound.size;
                outbound.departure_update(now);

                self.packets_waiting -= 1;
                self.update_stats(&outbound, now);

                // sends the packet out to the next element after a timeout
                let timeout = outbound.size as f64 * 8.0 / self.rate;

                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(timeout),
                        Self::send,
                        outbound.clone(),
                    )
                    .unwrap();

                // schedules the next run
                scheduler
                    .schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
                    .unwrap();

                self.busy_until = now + timeout;

                debug!(
                    "WFQServer {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
                            {} packets in the queue.",
                    self.scheduler_id,
                    outbound.packet_id,
                    outbound.size,
                    outbound.flow_id,
                    now + timeout,
                    self.scheduler_queue.len(),
                );
            }
        }
    }
}

impl Model for WFQServer {}

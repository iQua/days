//! Implements a Deficit Round Robin (DRR) scheduler.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::endpoints::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::endpoints::packet::Packet;
use crate::next_scheduler_id;

pub struct DRRServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a self.current_class, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its self.current_class, which is equivalent to flow-based DRR.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// deficit of classes, which are consecutive and start from 0
    deficit: Vec<usize>,
    /// quantum of classes, which are consecutive and start from 0
    quantum: Vec<usize>,

    /// the number of packets received, dropped, and in the queues waiting to be sent
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<usize>,

    /// FIFO queues of classes, which are consecutive and start from 0
    queues: Vec<VecDeque<Packet>>,

    /// The server is considered busy sending the current packet until this time
    busy_until: f64,
    /// The current packet class being served
    current_class: usize,

    pub output: Output<Packet>,
}

impl DRRServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        weights: Vec<usize>,
    ) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = Vec::new();
        let mut quantum = Vec::new();
        let mut byte_sizes = Vec::new();
        let mut queues = Vec::new();

        let min_weight = weights.iter().min().unwrap();

        for (class_id, _) in weights.iter().enumerate() {
            deficit.push(0);
            quantum.push(min_quantum * weights[class_id] / min_weight);
            byte_sizes.push(0);
            queues.push(VecDeque::new());
        }

        let packet_drop = match drop_strategy {
            DropStrategy::TailDrop => TailDrop::new(capacity, capacity_unit),
            _ => unimplemented!(),
        };

        DRRServer {
            scheduler_id: next_scheduler_id(),
            rate,
            flow_classes,
            drop_strategy: Box::new(packet_drop),
            deficit,
            quantum,
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            byte_sizes,
            queues,
            busy_until: 0.0,
            current_class: 0,
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.iter().sum(),
            self.queues.iter().map(|q| q.len()).sum(),
        );

        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

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

        self.queues[class_id].push_back(packet.clone());
        self.byte_sizes[class_id] += packet.size;

        debug!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in flow class {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            arrival_time,
            self.packets_received,
            self.queues[class_id].len(),
            class_id
        );

        debug!("arrival_time: {}", arrival_time);
        debug!("busy_until: {}", self.busy_until);

        if arrival_time >= self.busy_until {
            self.run(scheduler).await;
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.output.send(packet).await;
    }

    pub async fn run(&mut self, scheduler: &Scheduler<Self>) {
        let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
        let now = current_time.as_secs_f64();

        // schedules packets in the current packet class being served
        // increases the deficit of the current queue if it is non-empty

        if !self.queues[self.current_class].is_empty() {
            self.deficit[self.current_class] += self.quantum[self.current_class];
        } else {
            // resets to zero if the queue is empty
            self.deficit[self.current_class] = 0;
            return;
        }

        let packet = self.queues[self.current_class].front().unwrap().clone();

        if self.deficit[self.current_class] > 0 && packet.size <= self.deficit[self.current_class] {
            self.byte_sizes[self.current_class] -= packet.size;
            let outbound = self.queues[self.current_class].pop_front().unwrap();

            self.packets_waiting -= 1;
            self.deficit[self.current_class] -= packet.size;

            // sends the packet out to the next element after a timeout
            let timeout = (packet.size as f64) * 8.0 / self.rate;
            scheduler
                .schedule_event(Duration::from_secs_f64(timeout), Self::send, outbound)
                .unwrap();
            self.busy_until = now + timeout;

            debug!(
                "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
                        {} packets in the class queue.",
                self.scheduler_id,
                packet.packet_id,
                packet.size,
                packet.flow_id,
                now + timeout,
                self.queues[self.current_class].len(),
            );
        } else {
            self.current_class += 1;
        }
    }
}

impl Model for DRRServer {}

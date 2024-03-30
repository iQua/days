//! Implements a Weighted Fair Queueing (WFQ) scheduler.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::{ReportStatistics, SchedulerReport};
use crate::utils::logger::{Report, ReportLogger};

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
    /// class-based Weighted Fair Queueing. The default uses a packet's flow_id
    /// as its class_id, which is equivalent to flow-based WFQ.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or
    /// not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// weights of classes
    weights: Vec<usize>,
    /// class_id -> finish_time
    finish_times: HashMap<usize, f64>,
    /// number of queued packets of each flow class
    flow_queue_count: HashMap<usize, usize>,

    /// the set of active flow classes
    active_set: HashSet<usize>,

    vtime: f64,
    last_updated: f64,
    time_packet_sent: f64,

    /// the number of packets received, dropped, and forwarded
    packets_received: usize,
    packets_dropped: usize,
    packets_forwarded: usize,

    /// the number of bytes currently queued in each flow class
    byte_sizes: HashMap<usize, usize>,

    /// a min-heap of packets from all the classes, where packets are sorted
    /// according to their finish times
    scheduler_queue: BinaryHeap<TaggedPacket>,

    /// the server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,

    /// the statistics of a preiodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,
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
        let mut finish_times = HashMap::new();

        for (class_id, _) in weights.iter().enumerate() {
            finish_times.insert(class_id, 0.0);
        }

        let scheduler_id = next_scheduler_id();

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::RED => Box::new(RED::new(
                capacity,
                capacity_unit,
                0.7,
                0.9,
                0.8,
                scheduler_id,
            )),
        };

        WFQServer {
            scheduler_id,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            weights,
            finish_times,
            flow_queue_count: HashMap::new(),
            active_set: HashSet::new(),
            vtime: 0.0,
            last_updated: 0.0,
            time_packet_sent: 0.0,
            packets_received: 0,
            packets_dropped: 0,
            packets_forwarded: 0,
            byte_sizes: HashMap::new(),
            scheduler_queue: BinaryHeap::new(),
            busy_until: 0.0,
            output: Output::default(),
            report_start_time: 0.0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.values().sum(),
            self.scheduler_queue.len(),
        );

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "WFQServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.on_packet_forwarded(&packet);

        // computes a finish time and adds it as a tag to the packet
        let tagged_packet = self.tag(packet.clone(), arrival_time);
        let finish_time = tagged_packet.tag;

        // pushes the packet into a min-heap according to the packet's finish time
        self.scheduler_queue.push(tagged_packet);

        let class_id = (self.flow_classes)(packet.flow_id);
        let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
        *byte_size += packet.size;
        let flow_queue_count = self.flow_queue_count.entry(class_id).or_insert(0);
        *flow_queue_count += 1;
        self.active_set.insert(class_id);
        self.last_updated = arrival_time;

        debug!(
            "WFQServer {} received packet {} ({} bytes with finish time {:.3}) from flow {} at time {:.3}. \
            {} packet(s) in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            finish_time,
            packet.flow_id,
            arrival_time,
            self.scheduler_queue.len(),
        );

        if arrival_time >= self.busy_until {
            self.run((), scheduler);
        }
    }

    fn tag(&mut self, packet: Packet, arrival_time: f64) -> TaggedPacket {
        let mut finish_time = 0.0;

        // updates the virtual time and the finish time for each flow class
        if self.active_set.is_empty() {
            self.vtime = 0.0;

            for (class_id, _) in self.weights.iter().enumerate() {
                self.finish_times.insert(class_id, 0.0);
            }
        } else {
            // computes the sum of weights for flow classes in the active set
            let weight_sum: f64 = self
                .active_set
                .iter()
                .map(|class_id| self.weights[*class_id] as f64)
                .sum();

            self.vtime += (arrival_time - self.last_updated) / weight_sum;
            let class_id = (self.flow_classes)(packet.flow_id);
            finish_time = self.vtime.max(self.finish_times[&class_id])
                + packet.size as f64 * 8.0 / (self.rate * self.weights[class_id] as f64);
            self.finish_times.insert(class_id, finish_time);
        }

        TaggedPacket {
            packet,
            tag: finish_time,
        }
    }

    fn update_stats(&mut self, packet: &Packet, arrival_time: f64) {
        // updates the virtual time based on the current set of active flow classes
        let weight_sum: f64 = self
            .active_set
            .iter()
            .map(|class_id| self.weights[*class_id] as f64)
            .sum();
        self.vtime += (arrival_time - self.last_updated) / weight_sum;

        // computes the new set of active flow classes
        let class_id = (self.flow_classes)(packet.flow_id);

        let flow_queue_count = self.flow_queue_count.entry(class_id).or_insert(0);
        *flow_queue_count -= 1;

        if *flow_queue_count == 0 {
            self.active_set.remove(&class_id);
        }

        if self.active_set.is_empty() {
            self.vtime = 0.0;
            self.finish_times.insert(class_id, 0.0);
        }

        self.last_updated = arrival_time;
    }

    pub async fn send(&mut self, packet: Packet) {
        self.on_packet_forwarded(&packet);
        self.output.send(packet.clone()).await;
        self.update_stats(&packet, self.time_packet_sent);
    }

    pub fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        // schedules one packet with the smallest finish time
        if !self.scheduler_queue.is_empty() {
            let mut outbound = self.scheduler_queue.pop().unwrap().packet;
            let class_id = (self.flow_classes)(outbound.flow_id);
            let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
            *byte_size -= outbound.size;
            outbound.queueing_delay_update(now);

            // sends the packet out to the next element after a timeout
            let timeout = outbound.size as f64 * 8.0 / self.rate;

            outbound.departure_update(now + timeout);

            self.time_packet_sent = now + timeout;
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

    fn log_report<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            let report = self.generate_report(now);

            ReportLogger::log_report(Report::SchedulerReport(report));
            debug!(
                "WFQServer {} logged a periodic report at time {:.3}.",
                self.scheduler_id, now
            );

            self.reset_stats(now);

            scheduler
                .schedule_event(
                    Duration::from_secs_f64(ReportLogger::get_report_interval()),
                    Self::log_report,
                    (),
                )
                .unwrap();
        }
    }
}

impl ReportStatistics for WFQServer {
    fn on_packet_received(&mut self, packet: &Packet) {
        self.packets_received += 1;
        self.received_sizes += packet.size;
        self.queue_length += packet.size;
    }

    fn on_packet_forwarded(&mut self, packet: &Packet) {
        let num_packets = self.packets_forwarded as f64;
        self.queueing_delay_mean =
            (self.queueing_delay_mean * num_packets + packet.queueing_delay) / (num_packets + 1.0);
        self.packets_forwarded += 1;
        self.forwarded_sizes += packet.size;
        self.queue_length -= packet.size;
        self.throughput_mean = self.forwarded_sizes as f64 / (packet.time - self.report_start_time);
    }

    fn generate_report(&self, now: f64) -> SchedulerReport {
        SchedulerReport {
            id: self.scheduler_id,
            start_time: self.report_start_time,
            end_time: now,
            received_packets: self.packets_received,
            dropped_packets: self.packets_dropped,
            forwarded_packets: self.packets_forwarded,
            queue_length: self.queue_length,
            received_sizes: self.received_sizes,
            forwarded_sizes: self.forwarded_sizes,
            throughput_mean: self.throughput_mean,
            queueing_delay_mean: self.queueing_delay_mean,
        }
    }

    fn reset_stats(&mut self, now: f64) {
        self.report_start_time = now;
        self.packets_received = 0;
        self.packets_dropped = 0;
        self.packets_forwarded = 0;
        self.received_sizes = 0;
        self.forwarded_sizes = 0;
        self.throughput_mean = 0.0;
        self.queueing_delay_mean = 0.0;
    }
}

impl Model for WFQServer {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            let report_interval = ReportLogger::get_report_interval();
            if report_interval < f64::MAX {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(report_interval),
                        Self::log_report,
                        (),
                    )
                    .unwrap();
            }

            self.into()
        })
    }
}

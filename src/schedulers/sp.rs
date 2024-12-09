//! Implements a Static Priority (SP) scheduler.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::{ReportStatistics, SchedulerReport};
use crate::utils::ui::{Report, ReportTiming};
use crate::{get_update_interval, next_scheduler_id};

pub struct SPServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Static Priority. The default uses a packet's flow_id as its
    /// class_id, which is equivalent to flow-based SP.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// the number of packets received, dropped, and forwarded
    packets_received: usize,
    packets_dropped: usize,
    packets_forwarded: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    /// flow_class -> byte_size
    byte_sizes: HashMap<usize, usize>,

    /// FIFO queues of classes
    /// priority -> queue
    queues: BTreeMap<usize, VecDeque<Packet>>,

    /// flow_class -> priority
    priorities: HashMap<usize, usize>,

    /// The server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,
    pub report_output: Output<Report>,

    /// the statistics of a preiodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,
}

impl SPServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        priorities: HashMap<usize, usize>,
    ) -> SPServer {
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

        SPServer {
            scheduler_id,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            packets_forwarded: 0,
            byte_sizes: HashMap::new(),
            queues: BTreeMap::new(),
            priorities,
            busy_until: 0.0,
            output: Output::default(),
            report_output: Output::default(),
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

    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        let now = cx.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.values().sum(),
            self.queues.values().map(|q| q.len()).sum(),
        );

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "SPServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.on_packet_received(&packet);

        let class_id = (self.flow_classes)(packet.flow_id);

        // pushes the packet to the back of its priority queue
        let priority = self.priorities[&class_id];

        let queue = self.queues.entry(priority).or_default();
        queue.push_back(packet.clone());

        let byte_size = self.byte_sizes.entry(priority).or_insert(0);
        *byte_size += packet.size;

        debug!(
            "SPServer {} received packet {} ({} bytes) from flow {} belonging to class {} at time {:.3}. \
            {} packet(s) in flow class {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            class_id,
            arrival_time,
            self.queues[&priority].len(),
            class_id
        );

        if arrival_time >= self.busy_until {
            self.run((), cx);
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.on_packet_forwarded(&packet);
        self.output.send(packet).await;
    }

    /// Moves on to the next non-empty priority queue if the current queue is empty.
    fn next_priority(&mut self) -> Option<usize> {
        for (&priority, queue) in self.queues.iter().rev() {
            if !queue.is_empty() {
                return Some(priority);
            }
        }

        None
    }

    pub fn run(&mut self, _: (), cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        // schedules one packet with the highest priority
        if let Some(current_priority) = self.next_priority() {
            let queue = self.queues.entry(current_priority).or_default();
            let mut packet = queue.pop_front().unwrap();
            let outbound = packet.clone();

            let byte_size = self.byte_sizes.entry(current_priority).or_insert(0);
            *byte_size -= packet.size;

            packet.queueing_delay_update(now);

            // sends the packet out to the next element after a timeout
            let timeout = packet.size as f64 * 8.0 / self.rate;

            packet.departure_update(now + timeout);

            cx.schedule_event(Duration::from_secs_f64(timeout), Self::send, packet)
                .unwrap();

            // schedules the next run
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
                .unwrap();

            self.busy_until = now + timeout;

            debug!(
                "SPServer {} will send packet {} ({} bytes, priority {}) from flow {} at time {:.3}. {} packets in the priority queue.",
                self.scheduler_id,
                outbound.packet_id,
                outbound.size,
                current_priority,
                outbound.flow_id,
                now + timeout,
                self.queues[&current_priority].len(),
            );
        }
    }

    fn update_ui<'a>(
        &'a mut self,
        _: (),
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            let report = self.prepare_report(now, ReportTiming::InProgress);
            self.report_output
                .send(Report::SchedulerReport(report))
                .await;

            debug!(
                "SPServer {} logged a periodic report at time {:.3}.",
                self.scheduler_id, now
            );

            self.reset_stats(now);

            cx.schedule_event(
                Duration::from_secs_f64(get_update_interval()),
                Self::update_ui,
                (),
            )
            .unwrap();
        }
    }
}

impl ReportStatistics for SPServer {
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

    fn prepare_report(&self, now: f64, timing: ReportTiming) -> SchedulerReport {
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
            timing,
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

impl Model for SPServer {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        let update_interval = get_update_interval();
        if update_interval < f64::MAX {
            cx.schedule_event(
                Duration::from_secs_f64(update_interval),
                Self::update_ui,
                (),
            )
            .unwrap();
        }

        self.into()
    }
}

//! Implements a Deficit Round Robin (DRR) scheduler.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::progress::Report;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::SchedulerReport;

pub struct DRRServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based DRR.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or
    /// not
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

    /// the current packet class being served
    current_queue: usize,

    /// the server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,

    /// the report of a report interval
    pub report: SchedulerReport,
    /// the interval of sending a periodic report to the progress coroutine
    report_interval: f64,
    /// the sender for sedning reports
    pub report_output: Output<Report>,
}

impl DRRServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        weights: Vec<usize>,
        report_interval: f64,
    ) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = Vec::new();
        let mut quantum = Vec::new();
        let mut byte_sizes = Vec::new();
        let mut queues = Vec::new();

        let min_weight = weights.iter().min().unwrap();

        for weight in weights.iter() {
            let quantum_value = min_quantum * weight / min_weight;
            quantum.push(quantum_value);
            deficit.push(0);
            byte_sizes.push(0);
            queues.push(VecDeque::new());
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

        DRRServer {
            scheduler_id,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            deficit,
            quantum,
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            byte_sizes,
            queues,
            current_queue: 0,
            busy_until: 0.0,
            output: Output::default(),
            report: SchedulerReport::new(scheduler_id as u32, 0.0),
            report_interval,
            report_output: Output::default(),
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
            self.byte_sizes.iter().sum(),
            self.queues.iter().map(|q| q.len()).sum(),
        );

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            self.report.dropped_packets += 1;
            debug! {
                "DRRServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.packets_waiting += 1;
        self.packets_received += 1;

        self.report.receive_update(&packet);

        let class_id = (self.flow_classes)(packet.flow_id);

        // pushes the packet to the back of its class queue
        self.queues[class_id].push_back(packet.clone());

        self.byte_sizes[class_id] += packet.size;

        debug!(
            "DRRServer {} received packet {} ({} bytes) from flow {} belonging to class {} at time {:.3}. \
            {} packets received, {} packet(s) in flow class {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            class_id,
            arrival_time,
            self.packets_received,
            self.queues[class_id].len(),
            class_id
        );

        if arrival_time >= self.busy_until {
            self.run((), scheduler);
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.report.forward_update(&packet);
        self.output.send(packet).await;
    }

    /// Moves on to the next queue if the current queue is empty.
    fn next_queue(&mut self) {
        self.current_queue += 1;

        if self.current_queue >= self.queues.len() {
            // updates the deficit of each queue
            for (class_id, queue) in self.queues.iter().enumerate() {
                if !queue.is_empty() {
                    self.deficit[class_id] += self.quantum[class_id];
                } else {
                    // resets to zero if the queue is empty
                    self.deficit[class_id] = 0;
                }
            }

            self.current_queue = 0;
        }
    }

    pub fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        // schedules packets in the current packet class being served
        loop {
            if self.packets_waiting == 0 {
                // all packets in the queues have been processed
                return;
            }

            if !self.queues[self.current_queue].is_empty() {
                let packet = self.queues[self.current_queue].front().unwrap().clone();

                if self.deficit[self.current_queue] > 0
                    && packet.size <= self.deficit[self.current_queue]
                {
                    self.byte_sizes[self.current_queue] -= packet.size;
                    let mut outbound = self.queues[self.current_queue].pop_front().unwrap();
                    outbound.queueing_delay_update(now);

                    self.packets_waiting -= 1;
                    self.deficit[self.current_queue] -= packet.size;

                    // sends the packet out to the next element after a timeout
                    let timeout = packet.size as f64 * 8.0 / self.rate;

                    outbound.departure_update(now + timeout);

                    scheduler
                        .schedule_event(Duration::from_secs_f64(timeout), Self::send, outbound)
                        .unwrap();

                    // schedules the next run
                    scheduler
                        .schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
                        .unwrap();

                    self.busy_until = now + timeout;

                    debug!(
                        "DRRServer {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
                                {} packets in the class queue.",
                        self.scheduler_id,
                        packet.packet_id,
                        packet.size,
                        packet.flow_id,
                        now + timeout,
                        self.queues[self.current_queue].len(),
                    );

                    return;
                } else {
                    self.next_queue();
                }
            } else {
                self.next_queue();
            }
        }
    }

    /// Sends a perioid report of current statistics to the progress coroutine.
    fn send_report<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            self.report.end_time = now;
            let report = Report::SchedulerReport(self.report.clone());
            self.report_output.send(report).await;
            debug!(
                "DRRServer {} sent a periodic report at time {:.3}.",
                self.scheduler_id, now
            );

            // resets the report
            self.report = SchedulerReport::new(self.scheduler_id as u32, now);

            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval),
                    Self::send_report,
                    (),
                )
                .unwrap();
        }
    }
}

impl Model for DRRServer {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval),
                    Self::send_report,
                    (),
                )
                .unwrap();

            self.into()
        })
    }
}

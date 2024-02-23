//! Implements a simple FIFO scheduler with only one queue.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::progress::Report;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::SchedulerReport;

pub struct Port {
    scheduler_id: usize,
    /// the bit rate of the port (0 for unlimited)
    rate: f64,
    /// a closure that determines whether an inbound packet should be dropped or
    /// not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    /// the number of packets received
    packets_received: usize,
    /// the number of dropped packets
    packets_dropped: usize,
    /// the total byte sizes in the queue
    bytes_in_queue: usize,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
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

impl Port {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        drop_strategy: DropStrategy,
        report_interval: f64,
    ) -> Port {
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

        Port {
            scheduler_id,
            rate,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            bytes_in_queue: 0,
            queue: VecDeque::new(),
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
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.size, self.bytes_in_queue, self.queue.len());

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            self.report.dropped_packets += 1;
            debug!(
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id, packet.packet_id, packet.flow_id, arrival_time
            );
            return;
        }

        // the case that this packet will not be dropped
        self.packets_received += 1;
        self.queue.push_back(packet.clone());
        self.bytes_in_queue += packet.size;

        self.report.receive_update(&packet);

        debug!(
            "Port {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            arrival_time,
            self.packets_received,
            self.queue.len()
        );

        if arrival_time >= self.busy_until {
            self.run((), scheduler).await;
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.report.forward_update(&packet);
        self.output.send(packet).await;
    }

    fn packet_sent(&mut self, now: f64, packet: Packet) {
        self.bytes_in_queue -= packet.size;
        self.busy_until = now;

        debug!(
            "Port {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            now,
            self.queue.len()
        );
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            if let Some(mut packet) = self.queue.pop_front() {
                packet.queueing_delay_update(now);
                let timeout = packet.size as f64 * 8.0 / self.rate;
                packet.departure_update(now + timeout);

                scheduler
                    .schedule_event(Duration::from_secs_f64(timeout), Self::send, packet.clone())
                    .unwrap();

                scheduler
                    .schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
                    .unwrap();

                self.packet_sent(now + timeout, packet);
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

impl Model for Port {
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

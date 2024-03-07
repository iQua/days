//! Implements a Virtual Clock scheduler.
//!
//! Reference:
//!
//! L. Zhang, "Virtual Clock: A New Traffic Control Algorithm for Packet
//! Switching Networks," in ACM SIGCOMM Computer Communication Review, vol. 20,
//! pp. 19, 1990.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::logger::{Report, ReportLogger};
use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::SchedulerReport;

pub struct TaggedPacket {
    pub packet: Packet,
    /// tag is the virtual clock finish time of the packet
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

pub struct VirtualClockServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Virtual Clock. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based Virtual Clock.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or
    /// not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// the number of packets received and dropped
    packets_received: usize,
    packets_dropped: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    /// flow_class -> byte_size
    byte_sizes: HashMap<usize, usize>,

    /// min-heap of packets from all the classes, where packets are sorted
    /// according to their virtual clock finish times
    scheduler_queue: BinaryHeap<TaggedPacket>,

    /// flow_class -> vtick (inverse of the desired rates for the corresponding
    /// flows, in bits per second)
    vticks: HashMap<usize, usize>,

    /// number of queued packets of each flow class
    flow_queue_count: HashMap<usize, usize>,

    /// virtual clocks for the corresponding flows
    /// flow_class -> virtual clock
    v_clocks: HashMap<usize, f64>,

    /// flow_class -> virtual clock finish time
    aux_vc: HashMap<usize, f64>,

    /// the server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,

    /// the report of a report interval
    pub report: SchedulerReport,
    /// the interval of generating a periodic report
    pub report_interval: f64,
    /// a report logger used for logging periodic reports to a SQLite database
    /// or a JSON file
    pub report_logger: ReportLogger,
}

impl VirtualClockServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        vticks: HashMap<usize, usize>,
    ) -> VirtualClockServer {
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

        VirtualClockServer {
            scheduler_id,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            byte_sizes: HashMap::new(),
            scheduler_queue: BinaryHeap::new(),
            vticks,
            flow_queue_count: HashMap::new(),
            v_clocks: HashMap::new(),
            aux_vc: HashMap::new(),
            busy_until: 0.0,
            output: Output::default(),
            report: SchedulerReport::new(scheduler_id as u32, 0.0),
            report_interval: f64::MAX,
            report_logger: ReportLogger::default(),
        }
    }

    pub fn set_report_logger(&mut self, report_logger: ReportLogger, report_interval: f64) {
        self.report_logger = report_logger;
        self.report_interval = report_interval;
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
            self.report.dropped_packets += 1;
            debug! {
                "VirtualClockServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.packets_received += 1;

        self.report.receive_update(&packet);

        // computes a virtual clock finish time and adds it as a tag to the
        // packet
        let tagged_packet = self.tag(packet.clone(), arrival_time);
        let aux_vc = tagged_packet.tag;

        // pushes the packet into a min-heap according to the packet's virtual
        // clock finish time
        self.scheduler_queue.push(tagged_packet);

        let class_id = (self.flow_classes)(packet.flow_id);
        let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
        *byte_size += packet.size;
        let flow_queue_count = self.flow_queue_count.entry(class_id).or_insert(0);
        *flow_queue_count += 1;

        debug!(
            "VirtualClockServer {} received packet {} ({} bytes with virtual clock {} aux_vc {:.3}) from flow {} belonging to class {} at time {:.3}. \
            {} packets received, {} packet(s) in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            self.v_clocks.get(&class_id).unwrap(),
            aux_vc,
            packet.flow_id,
            class_id,
            arrival_time,
            self.packets_received,
            self.scheduler_queue.len(),
        );

        if arrival_time >= self.busy_until {
            self.run((), scheduler);
        }
    }

    fn tag(&mut self, packet: Packet, arrival_time: f64) -> TaggedPacket {
        let class_id = (self.flow_classes)(packet.flow_id);

        // upon receiving the first packet from this flow_class, sets its
        // virtual clock to the current real time
        let v_clock = self.v_clocks.entry(class_id).or_insert(arrival_time);

        // updates the virtual clock for the corresponding flow_class by
        // multiplying vtick (the desired bit time, i.e., the inverse of the
        // desired bits per second data rate) by the size of the packet in bits
        let vtick = self.vticks.get(&class_id).unwrap();
        *v_clock += *vtick as f64 * packet.size as f64 * 8.0;

        let aux_vc = self.aux_vc.entry(class_id).or_insert(0.0);
        *aux_vc = arrival_time.max(*aux_vc);
        *aux_vc += *vtick as f64;

        TaggedPacket {
            packet,
            tag: *aux_vc,
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.report.forward_update(&packet);
        self.output.send(packet).await;
    }

    pub fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();
        // schedules one packet with the smallest virtual clock finish time
        if !self.scheduler_queue.is_empty() {
            let mut outbound = self.scheduler_queue.pop().unwrap().packet;
            let class_id = (self.flow_classes)(outbound.flow_id);
            let flow_queue_count = self.flow_queue_count.entry(class_id).or_insert(0);
            *flow_queue_count -= 1;
            let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
            *byte_size -= outbound.size;

            outbound.queueing_delay_update(now);

            // sends the packet out to the next element after a timeout
            let timeout = outbound.size as f64 * 8.0 / self.rate;

            outbound.departure_update(now + timeout);

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
                "VirtualClockServer {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
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

            self.report.end_time = now;
            self.report_logger
                .log_report(Report::SchedulerReport(self.report.clone()));
            debug!(
                "VirtualClockServer {} logged a periodic report at time {:.3}.",
                self.scheduler_id, now
            );

            self.report = self.report.reset(now);

            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval),
                    Self::log_report,
                    (),
                )
                .unwrap();
        }
    }
}

impl Model for VirtualClockServer {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval),
                    Self::log_report,
                    (),
                )
                .unwrap();

            self.into()
        })
    }
}

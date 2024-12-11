//! Implements a Virtual Clock scheduler.
//!
//! Reference:
//!
//! L. Zhang, "Virtual Clock: A New Traffic Control Algorithm for Packet
//! Switching Networks," in ACM SIGCOMM Computer Communication Review, vol. 20,
//! pp. 19, 1990.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::{ReportStatistics, SchedulerReport};
use crate::utils::logger::{CsvLogger, Report, ReportTiming};

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

    /// the number of packets received, dropped, and forwarded
    packets_received: usize,
    packets_dropped: usize,
    packets_forwarded: usize,

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

    /// the statistics of a preiodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,
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
            packets_forwarded: 0,
            byte_sizes: HashMap::new(),
            scheduler_queue: BinaryHeap::new(),
            vticks,
            flow_queue_count: HashMap::new(),
            v_clocks: HashMap::new(),
            aux_vc: HashMap::new(),
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

    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        let now = cx.time();
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
                "VirtualClockServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.on_packet_received(&packet);

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
            {} packet(s) in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            self.v_clocks.get(&class_id).unwrap(),
            aux_vc,
            packet.flow_id,
            class_id,
            arrival_time,
            self.scheduler_queue.len(),
        );

        if arrival_time >= self.busy_until {
            self.run((), cx);
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
        self.on_packet_forwarded(&packet);
        self.output.send(packet).await;
    }

    pub fn run(&mut self, _: (), cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
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

            cx.schedule_event(
                Duration::from_secs_f64(timeout),
                Self::send,
                outbound.clone(),
            )
            .unwrap();

            // schedules the next run
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
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

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        let report = self.prepare_report(now);
        CsvLogger::log_report(Report::SchedulerReport(report), ReportTiming::InProgress);

        debug!(
            "VirtualClockServer {} logged a periodic report at time {:.3}.",
            self.scheduler_id, now
        );

        self.reset_stats(now);
    }
}

impl ReportStatistics for VirtualClockServer {
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

    fn prepare_report(&self, now: f64) -> SchedulerReport {
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

impl Model for VirtualClockServer {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        let report_interval = CsvLogger::get_instance().get_report_interval();
        if report_interval < f64::MAX {
            cx.schedule_periodic_event(
                Duration::from_secs_f64(report_interval),
                Duration::from_secs_f64(report_interval),
                Self::log_report,
                (),
            )
            .unwrap();
        }

        self.into()
    }
}

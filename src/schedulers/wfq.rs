//! Implements a Weighted Fair Queueing (WFQ) scheduler.
//!
//! Reference:
//!
//! A. K. Parekh, R. G. Gallager, "A Generalized Processor Sharing Approach to Flow Control
//! in Integrated Services Networks: The Single-Node Case," IEEE/ACM Trans. Networking,
//! vol. 1, no. 3, pp. 344-357, June 1993.
//!
//! https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=234856

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tracing::instrument;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{
    CapacityUnit, DEFAULT_ECN_THRESHOLD, DropAction, DropStrategy, EcnThreshold, PacketDrop, RED,
    TailDrop,
};
use crate::schedulers::state::QueueState;
use crate::schedulers::{ReportStatistics, SchedulerReport};
use crate::utils::logger::{CsvLogger, Report, ReportTiming};

#[derive(Clone, Debug)]
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

    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

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

    queue_state: Option<std::sync::Arc<QueueState>>,

    /// the statistics of a periodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,

    /// a vector of packets that have been sent out, only used for unit testing
    #[cfg(test)]
    sent_packets: Vec<TaggedPacket>,
}

impl WFQServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        ecn_threshold: f64,
        weights: Vec<usize>,
    ) -> WFQServer {
        let mut finish_times = HashMap::new();

        for (class_id, _) in weights.iter().enumerate() {
            finish_times.insert(class_id, 0.0);
        }

        let scheduler_id = next_scheduler_id();
        let ecn_threshold = if ecn_threshold > 0.0 {
            ecn_threshold
        } else {
            DEFAULT_ECN_THRESHOLD
        };

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::RED => Box::new(RED::new(
                capacity,
                capacity_unit,
                0.7,
                0.9,
                0.8,
                scheduler_id,
                false,
            )),
            DropStrategy::RedEcn => Box::new(RED::new(
                capacity,
                capacity_unit,
                0.7,
                0.9,
                0.8,
                scheduler_id,
                true,
            )),
            DropStrategy::EcnThreshold => {
                Box::new(EcnThreshold::new(capacity, capacity_unit, ecn_threshold))
            }
        };

        WFQServer {
            scheduler_id,
            time: 0.0,
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
            queue_state: None,
            report_start_time: 0.0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
            #[cfg(test)]
            sent_packets: Vec::new(),
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    pub fn set_queue_state(&mut self, state: std::sync::Arc<QueueState>) {
        self.queue_state = Some(state);
    }

    pub fn on_packet_received(&mut self, packet: Packet) {
        let mut packet = packet;
        let drop_action = self.drop_strategy.action(
            packet.size,
            self.byte_sizes.values().sum(),
            self.scheduler_queue.len(),
        );

        match drop_action {
            DropAction::Drop => {
                self.packets_dropped += 1;
                debug! {
                    "WFQServer {} dropped packet {} from flow {} at time {:.3}",
                    self.scheduler_id,
                    packet.packet_id,
                    packet.flow_id,
                    packet.time
                }
                return;
            }
            DropAction::MarkEcn => {
                if !packet.mark_ce() {
                    self.packets_dropped += 1;
                    debug! {
                        "WFQServer {} dropped non-ECT packet {} from flow {} at time {:.3}",
                        self.scheduler_id,
                        packet.packet_id,
                        packet.flow_id,
                        packet.time
                    }
                    return;
                }
            }
            DropAction::Enqueue => {}
        }

        // the case that this packet will not be dropped
        self.update_stats_on_packet_received(&packet);

        // computes a finish time and adds it as a tag to the packet
        let tagged_packet = self.tag(packet.clone(), packet.time);
        let finish_time = tagged_packet.tag;

        // pushes the packet into a min-heap according to the packet's finish time
        self.scheduler_queue.push(tagged_packet);

        let class_id = (self.flow_classes)(packet.flow_id);
        let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
        *byte_size += packet.size;
        let flow_queue_count = self.flow_queue_count.entry(class_id).or_insert(0);
        *flow_queue_count += 1;
        self.active_set.insert(class_id);
        self.last_updated = packet.time;

        debug!(
            "WFQServer {} received packet {} ({} bytes with finish time {:.3}) from flow {} at time {:.3}. \
            {} packet(s) in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            finish_time,
            packet.flow_id,
            packet.time,
            self.scheduler_queue.len(),
        );
    }

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                (packet.time - global_time).abs() <= 1e-7,
                "Timing mismatch: packet.time = {}, global_time = {}",
                packet.time,
                global_time
            );
        }

        let packet_time = packet.time;
        self.on_packet_received(packet);

        if packet_time >= self.busy_until {
            self.run(packet_time, cx);
        }
    }

    fn tag(&mut self, packet: Packet, arrival_time: f64) -> TaggedPacket {
        let mut finish_time = 0.0;

        // updates the virtual time and the finish time for each flow class
        if self.active_set.is_empty() {
            self.vtime = 0.0;
            self.finish_times.clear();
        } else {
            // computes the sum of weights for flow classes in the active set
            let weight_sum: f64 = self
                .active_set
                .iter()
                .map(|class_id| self.weights[*class_id] as f64)
                .sum();

            self.vtime += (arrival_time - self.last_updated) / weight_sum;

            let flow_id = packet.flow_id;
            let class_id = (self.flow_classes)(flow_id);

            // gets previous finish time for this flow class, defaulting to 0
            let prev_finish = *self.finish_times.get(&class_id).unwrap_or(&0.0);

            // calculates virtual start time as max(vtime, prev_finish)
            let virtual_start = self.vtime.max(prev_finish);

            finish_time = virtual_start
                + packet.size as f64 * 8.0 / (self.rate * self.weights[class_id] as f64);

            self.finish_times.insert(class_id, finish_time);
        }

        TaggedPacket {
            packet,
            tag: finish_time,
        }
    }

    fn update_internal_states(&mut self, packet: &Packet, arrival_time: f64) {
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

    #[instrument(skip(self))]
    pub async fn send(&mut self, packet: Packet) {
        self.time = packet.time;

        self.output.send(packet.clone()).await;
        self.update_stats_on_packet_forwarded(&packet);
        self.update_internal_states(&packet, self.time_packet_sent);
    }

    /// Schedules a packet by accepting a closure to handle packet sending based on context.
    fn schedule_packet<F>(&mut self, mut schedule_event: F)
    where
        F: FnMut(f64, f64, TaggedPacket),
    {
        // schedules one packet with the smallest finish time
        if !self.scheduler_queue.is_empty() {
            let mut tagged_outbound = self.scheduler_queue.pop().unwrap();
            let outbound = tagged_outbound.packet.clone();
            let class_id = (self.flow_classes)(outbound.flow_id);
            let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
            *byte_size -= outbound.size;
            tagged_outbound.packet.queueing_delay_update(self.time);

            // sends the packet out to the next element after a timeout
            let timeout = outbound.size as f64 * 8.0 / self.rate;

            tagged_outbound.packet.departure_update(self.time + timeout);

            self.time_packet_sent = self.time + timeout;
            schedule_event(self.time, timeout, tagged_outbound.clone());

            self.busy_until = self.time + timeout;

            debug!(
                "WFQServer {} will send packet {} ({} bytes) from flow {} at time {:.8e}. \
                        {} packets in the queue.",
                self.scheduler_id,
                outbound.packet_id,
                outbound.size,
                outbound.flow_id,
                self.time + timeout,
                self.scheduler_queue.len(),
            );
        }
    }

    #[instrument(skip(self, cx))]
    pub fn run(&mut self, now: f64, cx: &mut Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                (now - global_time).abs() <= 1e-7,
                "Timing mismatch: now = {}, global_time = {}",
                now,
                global_time
            );
        }

        self.time = now;

        if self.time == 0.0 {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            self.time = global_time;
        }

        self.schedule_packet(|now, timeout, outbound| {
            // schedules the send event
            cx.schedule_event(
                Duration::from_secs_f64(timeout),
                Self::send,
                outbound.packet,
            )
            .unwrap();

            // schedules the next run
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::run, now + timeout)
                .unwrap();
        });
    }

    #[cfg(test)]
    pub fn test_run(&mut self, now: f64) {
        // creates a vector to collect events inside the closure
        let mut events = Vec::new();

        // calls schedule_packets() without borrowing self inside the closure
        self.schedule_packet(|now, timeout, mut outbound| {
            // simulates sending the packet
            outbound.packet.departure_update(now + timeout);

            // collects the outbound packet and timeout
            events.push((timeout, outbound));
        });

        // processes collected events after schedule_packets returns
        for (timeout, outbound) in events {
            // updates the sent_packets vector
            self.sent_packets.push(outbound.clone());

            // updates statistics and internal states
            self.update_stats_on_packet_forwarded(&outbound.packet);
            self.update_internal_states(&outbound.packet, self.time_packet_sent);

            // updates busy_until
            self.busy_until = now + timeout;

            // schedules the next run by calling test_run recursively
            self.test_run(now + timeout);
        }
    }

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        let report = self.prepare_report(now);
        CsvLogger::log_report(Report::SchedulerReport(report), ReportTiming::InProgress);

        debug!(
            "WFQServer {} logged a periodic report at time {:.3}.",
            self.scheduler_id, now
        );

        self.reset_stats(now);
    }
}

impl ReportStatistics for WFQServer {
    fn update_stats_on_packet_received(&mut self, packet: &Packet) {
        self.packets_received += 1;
        self.received_sizes += packet.size;
        self.queue_length += packet.size;
        if let Some(state) = &self.queue_state {
            state.record_enqueue(packet.size);
        }
    }

    fn update_stats_on_packet_forwarded(&mut self, packet: &Packet) {
        let num_packets = self.packets_forwarded as f64;

        self.queueing_delay_mean =
            (self.queueing_delay_mean * num_packets + packet.queueing_delay) / (num_packets + 1.0);
        self.packets_forwarded += 1;
        self.forwarded_sizes += packet.size;
        self.queue_length -= packet.size;
        self.throughput_mean = self.forwarded_sizes as f64 / (packet.time - self.report_start_time);
        if let Some(state) = &self.queue_state {
            state.record_dequeue(packet.size);
        }
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

impl Model for WFQServer {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::packet::Packet;
    use crate::schedulers::drop::{CapacityUnit, DropStrategy};
    use std::sync::Arc;

    #[test]
    fn test_single_packet() {
        // tests sending a single packet through the WFQServer.
        let mut wfq = WFQServer::new(
            1e6, // server rate: 1 Mbps
            10,  // capacity: 10 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id), // flow_classes mapping
            DropStrategy::TailDrop,
            0.0,
            vec![1], // weights for one class
        );

        // creates a packet
        let packet = Packet::new(1024, 1, 0, 0.0); // packet_size, packet_id, flow_id, time

        // sends packet to WFQServer
        wfq.on_packet_received(packet.clone());

        // checks that the packet is in the queue
        assert_eq!(wfq.scheduler_queue.len(), 1);
        assert_eq!(wfq.packets_received, 1);

        // runs the scheduler
        wfq.test_run(0.0);

        // since the server is not busy, it should schedule the packet immediately
        assert!(wfq.busy_until > 0.0);
    }

    #[test]
    fn test_multiple_flows() {
        // tests packets from multiple flows.
        let mut wfq = WFQServer::new(
            8.0, // server rate: 8 bits/second
            4,   // capacity: 4 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id), // maps flow ids to class ids directly
            DropStrategy::TailDrop,
            0.0,
            vec![1, 1, 1], // equal weights for three connections
        );

        // simulates packets of size 1, 2, and 2 units arrive at time 0, on equally weighted connections
        // 0, 1, and 2, respectively.
        let packet1 = Packet::new(1, 1, 0, 0.0);
        let packet2 = Packet::new(2, 2, 1, 0.0);
        let packet3 = Packet::new(2, 3, 2, 0.0);
        wfq.on_packet_received(packet1);
        wfq.on_packet_received(packet2);
        wfq.on_packet_received(packet3);

        // simulates a packet of size 2 arrives at connection 0 at time 4
        let packet4 = Packet::new(2, 4, 0, 4.0);
        wfq.on_packet_received(packet4);

        // checks that all four packets are in the queue
        assert_eq!(wfq.scheduler_queue.len(), 4);
        assert_eq!(wfq.packets_received, 4);

        // prints the scheduled_packets queue
        let mut scheduled_packets: Vec<_> = wfq.scheduler_queue.clone().into_sorted_vec();
        scheduled_packets.reverse();
        for packet in scheduled_packets.iter() {
            println!(
                "Packet: {} Flow: {} Tag: {}",
                packet.packet.packet_id, packet.packet.flow_id, packet.tag
            );
        }

        // runs the scheduler
        wfq.test_run(0.0);

        // packets should be sent in the order of their finish tags
        assert!(wfq.sent_packets[0].tag <= wfq.sent_packets[1].tag);
        assert!(wfq.sent_packets[1].tag <= wfq.sent_packets[2].tag);
        assert!(wfq.sent_packets[2].tag <= wfq.sent_packets[3].tag);
    }

    #[test]
    fn test_queue_overflow() {
        // tests handling when queue is full (capacity reached).
        let mut wfq = WFQServer::new(
            1e6,
            2, // capacity: 2 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 1],
        );

        // creates three packets
        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 1, 0.0);
        let packet3 = Packet::new(1024, 3, 0, 0.0);

        // sends packets to WFQServer
        wfq.on_packet_received(packet1.clone());
        wfq.on_packet_received(packet2.clone());
        wfq.on_packet_received(packet3.clone());

        // checks that only two packets should be in the queue due to capacity limit
        assert_eq!(wfq.scheduler_queue.len(), 2);
        assert_eq!(wfq.packets_received, 2);
        assert_eq!(wfq.packets_dropped, 1);
    }

    #[test]
    fn test_unlimited_capacity_queue() {
        // tests behavior when capacity is unlimited (no packets should be dropped).
        let mut wfq = WFQServer::new(
            1e6,
            0, // unlimited capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1],
        );

        let packet = Packet::new(1024, 1, 0, 0.0);

        wfq.on_packet_received(packet.clone());

        // verifies unlimited capacity: no packet should be dropped
        assert_eq!(wfq.packets_dropped, 0);
    }

    #[test]
    fn test_large_packet_size() {
        // tests handling of a packet larger than capacity (should be dropped).
        let mut wfq = WFQServer::new(
            1e6,
            1500, // capacity in bytes
            CapacityUnit::Bytes,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1],
        );

        let packet = Packet::new(1501, 1, 0, 0.0); // packet size greater than capacity

        wfq.on_packet_received(packet.clone());

        // verifies queue should be empty, packet should be dropped
        assert_eq!(wfq.packets_dropped, 1);
    }

    #[test]
    fn test_packet_ordering_with_same_weights() {
        // tests that packets from different flows but same weight are scheduled fairly.
        let mut wfq = WFQServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 1], // same weights
        );

        // creates packets from two flows
        let packet1 = Packet::new(1024, 1, 0, 0.0); // flow_id 0
        let packet2 = Packet::new(1024, 2, 1, 0.1); // flow_id 1

        // sends packets to WFQServer
        wfq.on_packet_received(packet1.clone());
        wfq.on_packet_received(packet2.clone());

        // runs the scheduler
        wfq.test_run(0.0);

        // verifies that packets are scheduled fairly (tags should reflect arrival times)
        let sent_packet_ids: Vec<usize> = wfq
            .sent_packets
            .iter()
            .map(|p| p.packet.packet_id)
            .collect();
        assert_eq!(sent_packet_ids, vec![1, 2]);
    }

    #[test]
    fn test_packet_departure_time() {
        // tests that the departure time of packets is calculated correctly.
        let mut wfq = WFQServer::new(
            1e6, // 1 Mbps
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1],
        );

        // creates a packet
        let packet = Packet::new(1000, 1, 0, 0.0); // 1000 bytes

        // calculates expected transmission time = (size * 8) / rate
        let expected_transmission_time = (1000.0 * 8.0) / 1e6; // 0.008 seconds

        wfq.on_packet_received(packet.clone());

        // runs the scheduler
        wfq.test_run(0.0);

        // verifies that time_packet_sent is correct
        assert!((wfq.time_packet_sent - expected_transmission_time).abs() < 1e-6);
    }

    #[test]
    fn test_red_drop_strategy() {
        // tests using the RED packet drop strategy.
        let mut wfq = WFQServer::new(
            1e6,
            10, // capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::RED, // uses RED
            0.0,
            vec![1],
        );

        // sends multiple packets to fill the queue
        for i in 0..20 {
            let packet = Packet::new(1024, i, 0, 0.0);
            wfq.on_packet_received(packet.clone());
        }

        // verifies with RED, some packets should be randomly dropped before reaching capacity
        assert!(wfq.packets_dropped >= 10);
    }

    #[test]
    fn test_flow_class_mapping() {
        // tests custom flow_classes mapping
        let mut wfq = WFQServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id % 3), // maps flow_ids to 3 classes
            DropStrategy::TailDrop,
            0.0,
            vec![1, 2, 3], // different weights
        );

        // creates packets from different flows
        let packet1 = Packet::new(1024, 1, 1, 0.0); // flow_id 1 -> class 1
        let packet2 = Packet::new(1024, 2, 2, 0.0); // flow_id 2 -> class 2
        let packet3 = Packet::new(1024, 3, 3, 0.0); // flow_id 3 -> class 0

        // sends packets
        wfq.on_packet_received(packet2.clone());
        wfq.on_packet_received(packet1.clone());
        wfq.on_packet_received(packet3.clone());

        // verifies that flow_class mapping works
        assert_eq!((wfq.flow_classes)(1), 1);
        assert_eq!((wfq.flow_classes)(2), 2);
        assert_eq!((wfq.flow_classes)(3), 0);

        // runs the scheduler
        wfq.test_run(0.0);

        // verifies expected packet send order based on weights
        let sent_packet_ids: Vec<usize> = wfq
            .sent_packets
            .iter()
            .map(|p| p.packet.packet_id)
            .collect();
        // due to the weights (1,2,3), the scheduling order should be [2, 1, 3]
        assert_eq!(sent_packet_ids, vec![2, 1, 3]);
    }

    #[test]
    fn test_virtual_time_accuracy() {
        // tests the accuracy of virtual time calculations
        let mut wfq = WFQServer::new(
            100.0, // 100 bps for easy calculation
            10,    // capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 2], // weights 1:2
        );

        // verifies initial state
        assert_eq!(wfq.vtime, 0.0);

        // sends packet to flow 0 (weight 1)
        let packet1 = Packet::new(10, 1, 0, 0.0);
        wfq.on_packet_received(packet1);

        // sends packet to flow 1 (weight 2)
        let packet2 = Packet::new(10, 2, 1, 0.0);
        wfq.on_packet_received(packet2);

        // processes first packet (size 10, weight 1)
        // the virtual time should advance by 10 / 1 = 10 units
        wfq.test_run(0.0);

        // processes all packets and verify system goes idle
        assert_eq!(wfq.scheduler_queue.len(), 0);
        assert_eq!(wfq.vtime, 0.0); // Should reset when idle
    }

    #[test]
    fn test_start_finish_times() {
        let mut wfq = WFQServer::new(
            100.0,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 1], // Equal weights
        );

        // sends two packets to same flow
        let packet1 = Packet::new(10, 1, 0, 0.0);
        let packet2 = Packet::new(10, 2, 0, 0.0);

        wfq.on_packet_received(packet1);
        wfq.on_packet_received(packet2);

        // retrieves finish times from queue
        let mut packets: Vec<_> = wfq.scheduler_queue.clone().into_sorted_vec();
        packets.reverse();

        // first packet: start = 0.0, finish = 0.0
        assert!((packets[0].tag - 0.0).abs() < 1e-6);

        // second packet: start = 0.0, finish = 0.8
        assert!((packets[1].tag - 0.8).abs() < 1e-6);
    }

    #[test]
    fn test_flow_isolation() {
        let mut wfq = WFQServer::new(
            1000.0,
            100,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 1], // Equal weights
        );

        // sends many packets to both flows
        for i in 0..50 {
            let packet1 = Packet::new(10, i * 2, 0, 0.0);
            let packet2 = Packet::new(10, i * 2 + 1, 1, 0.0);
            wfq.on_packet_received(packet1);
            wfq.on_packet_received(packet2);
        }

        wfq.test_run(0.0);

        // counts packets sent from each flow
        let flow0_packets = wfq
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 0)
            .count();
        let flow1_packets = wfq
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 1)
            .count();

        // with equal weights, should be roughly equal
        assert!((flow0_packets as i32 - flow1_packets as i32).abs() <= 1);
    }

    #[test]
    fn test_multiple_weight_ratios() {
        let mut wfq = WFQServer::new(
            1000.0,
            120,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 2, 4], // 1:2:4 weight ratio
        );

        // sends packets to all three flows at fixed intervals
        let arrival_interval = 0.001;
        let mut arrival_time = 0.0;

        for i in 0..40 {
            // sends one packet to each flow in sequence
            let packet1 = Packet::new(10, i * 3, 0, arrival_time);
            wfq.on_packet_received(packet1);

            let packet2 = Packet::new(10, i * 3 + 1, 1, arrival_time);
            wfq.on_packet_received(packet2);

            let packet3 = Packet::new(10, i * 3 + 2, 2, arrival_time);
            wfq.on_packet_received(packet3);

            arrival_time += arrival_interval;
        }

        wfq.test_run(0.0);

        // calculates bytes sent per flow
        let bytes: Vec<usize> = (0..3)
            .map(|flow_id| {
                wfq.sent_packets
                    .iter()
                    .take(40) // Only consider first 40 packets sent
                    .filter(|p| p.packet.flow_id == flow_id)
                    .map(|p| p.packet.size)
                    .sum()
            })
            .collect();

        println!("Flow 0 (weight 1): {} bytes", bytes[0]);
        println!("Flow 1 (weight 2): {} bytes", bytes[1]);
        println!("Flow 2 (weight 4): {} bytes", bytes[2]);
        println!("Ratio flow 1/flow 0: {}", bytes[1] as f64 / bytes[0] as f64);
        println!("Ratio flow 2/flow 0: {}", bytes[2] as f64 / bytes[0] as f64);

        // Check ratios between flows match weights (within 20% tolerance)
        assert!((bytes[1] as f64 / bytes[0] as f64 - 2.0).abs() < 0.2);
        assert!((bytes[2] as f64 / bytes[0] as f64 - 4.0).abs() < 0.2);
    }

    #[test]
    fn test_dynamic_flows() {
        let mut wfq = WFQServer::new(
            1000.0,
            100,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1, 1], // Equal weights
        );

        // initially send packets only to flow 0
        for i in 0..10 {
            let packet = Packet::new(10, i, 0, 0.0);
            wfq.on_packet_received(packet);
        }

        // then send to both flows
        for i in 10..20 {
            let packet1 = Packet::new(10, i * 2, 0, 1.0);
            let packet2 = Packet::new(10, i * 2 + 1, 1, 1.0);
            wfq.on_packet_received(packet1);
            wfq.on_packet_received(packet2);
        }

        wfq.test_run(0.0);

        // count packets sent from each flow
        let flow0_packets = wfq
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 0)
            .count();
        let flow1_packets = wfq
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 1)
            .count();

        // should be roughly equal after both flows active
        assert!((flow0_packets as isize - flow1_packets as isize).abs() <= 10);
    }
}

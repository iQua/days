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

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};
use crate::schedulers::{ReportStatistics, SchedulerReport};
use crate::utils::logger::{CsvLogger, Report, ReportTiming};

#[derive(Clone)]
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
            #[cfg(test)]
            sent_packets: Vec::new(),
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    pub fn on_packet_received(&mut self, packet: Packet, arrival_time: f64) {
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
        self.update_stats_on_packet_received(&packet);

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
    }

    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        let now = cx.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.on_packet_received(packet, arrival_time);

        if arrival_time >= self.busy_until {
            self.run((), cx);
        }
    }

    fn tag(&mut self, packet: Packet, arrival_time: f64) -> TaggedPacket {
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
        }

        let flow_id = packet.flow_id;
        let class_id = (self.flow_classes)(flow_id);
        let weight = self.weights[class_id] as f64;

        // Get previous finish time for this flow, defaulting to 0
        let prev_finish = *self.finish_times.get(&flow_id).unwrap_or(&0.0);

        // Calculate virtual start time as max(vtime, prev_finish)
        let virtual_start = self.vtime.max(prev_finish);

        // Calculate virtual finish time
        let virtual_finish = virtual_start + packet.size as f64 / weight;

        // Store virtual finish time for this flow
        self.finish_times.insert(flow_id, virtual_finish);

        TaggedPacket {
            packet,
            tag: virtual_finish,
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

    pub async fn send(&mut self, packet: Packet) {
        self.output.send(packet.clone()).await;
        self.update_stats_on_packet_forwarded(&packet);
        self.update_internal_states(&packet, self.time_packet_sent);
    }

    pub fn run(&mut self, _: (), cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

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

    #[cfg(test)]
    pub fn test_run(&mut self, now: f64) {
        // schedules one packet with the smallest finish time
        if !self.scheduler_queue.is_empty() {
            let tagged_outbound = self.scheduler_queue.pop().unwrap();
            let mut outbound = tagged_outbound.packet;
            let class_id = (self.flow_classes)(outbound.flow_id);
            let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
            *byte_size -= outbound.size;
            outbound.queueing_delay_update(now);

            // sends the packet out to the next element after a timeout
            let timeout = outbound.size as f64 * 8.0 / self.rate;

            outbound.departure_update(now + timeout);

            self.time_packet_sent = now + timeout;
            self.sent_packets.push(tagged_outbound.clone());

            // schedules the next run
            self.test_run(now + timeout);
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
    }

    fn update_stats_on_packet_forwarded(&mut self, packet: &Packet) {
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
        // Test sending a single packet through the WFQServer.
        let mut wfq = WFQServer::new(
            1e6, // server rate: 1 Mbps
            10,  // capacity: 10 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id), // flow_classes mapping
            DropStrategy::TailDrop,
            vec![1], // weights for one class
        );

        // Create a packet
        let packet = Packet::new(1024, 1, 0, 0.0); // packet_size, packet_id, flow_id, time

        // Send packet to WFQServer
        wfq.on_packet_received(packet.clone(), 0.0);

        // Check that the packet is in the queue
        assert_eq!(wfq.scheduler_queue.len(), 1);
        assert_eq!(wfq.packets_received, 1);

        // Run the scheduler
        wfq.test_run(0.0);

        // Since the server is not busy, it should schedule the packet immediately
        assert!(wfq.busy_until > 0.0);
    }

    #[test]
    fn test_multiple_flows() {
        // Test packets from multiple flows.
        let mut wfq = WFQServer::new(
            8.0, // server rate: 8 bits/second
            4,   // capacity: 4 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id), // Map flow ids to class ids directly
            DropStrategy::TailDrop,
            vec![1, 1, 1], // equal weights for three connections
        );

        // Packets of size 1, 2, and 2 units arrive at time 0, on equally weighted connections
        // 0, 1, and 2, respectively.
        let packet1 = Packet::new(1, 1, 0, 0.0);
        let packet2 = Packet::new(2, 2, 1, 0.0);
        let packet3 = Packet::new(2, 3, 2, 0.0);
        wfq.on_packet_received(packet1, 0.0);
        wfq.on_packet_received(packet2, 0.0);
        wfq.on_packet_received(packet3, 0.0);

        // A packet of size 2 arrives at connection 0 at time 4
        let packet4 = Packet::new(2, 4, 0, 4.0);
        wfq.on_packet_received(packet4, 4.0);

        // Check that all four packets are in the queue
        assert_eq!(wfq.scheduler_queue.len(), 4);
        assert_eq!(wfq.packets_received, 4);
        // Prints the scheduled_packets queue
        // Check that packets are scheduled according to weights
        let mut scheduled_packets: Vec<_> = wfq.scheduler_queue.clone().into_sorted_vec();
        scheduled_packets.reverse();
        for packet in scheduled_packets.iter() {
            println!(
                "Packet: {} Flow: {} Tag: {}",
                packet.packet.packet_id, packet.packet.flow_id, packet.tag
            );
        }
        // Run the scheduler
        wfq.test_run(0.0);

        // Packet should be sent in the order of their finish tags
        assert!(wfq.sent_packets[0].tag <= wfq.sent_packets[1].tag);
        assert!(wfq.sent_packets[1].tag <= wfq.sent_packets[2].tag);
        assert!(wfq.sent_packets[2].tag <= wfq.sent_packets[3].tag);
    }

    #[test]
    fn test_queue_overflow() {
        // Test handling when queue is full (capacity reached).
        let mut wfq = WFQServer::new(
            1e6,
            2, // capacity: 2 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1, 1],
        );

        // Create three packets
        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 1, 0.0);
        let packet3 = Packet::new(1024, 3, 0, 0.0);

        // Send packets to WFQServer
        wfq.on_packet_received(packet1.clone(), 0.0);
        wfq.on_packet_received(packet2.clone(), 0.0);
        wfq.on_packet_received(packet3.clone(), 0.0);

        // Only two packets should be in the queue due to capacity limit
        assert_eq!(wfq.scheduler_queue.len(), 2);
        assert_eq!(wfq.packets_received, 2);
        assert_eq!(wfq.packets_dropped, 1);
    }

    #[test]
    fn test_unlimited_capacity_queue() {
        // Test behavior when capacity is unlimited (no packets should be dropped).
        let mut wfq = WFQServer::new(
            1e6,
            0, // unlimited capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1],
        );

        let packet = Packet::new(1024, 1, 0, 0.0);

        wfq.on_packet_received(packet.clone(), 0.0);

        // Unlimited capacity: no packet should be dropped
        assert_eq!(wfq.packets_dropped, 0);
    }

    #[test]
    fn test_large_packet_size() {
        // Test handling of a packet larger than capacity (should be dropped).
        let mut wfq = WFQServer::new(
            1e6,
            1500, // capacity in bytes
            CapacityUnit::Bytes,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1],
        );

        let packet = Packet::new(1501, 1, 0, 0.0); // Packet size greater than capacity

        wfq.on_packet_received(packet.clone(), 0.0);

        // Queue should be empty, packet should be dropped
        assert_eq!(wfq.packets_dropped, 1);
    }

    #[test]
    fn test_packet_ordering_with_same_weights() {
        // Test that packets from different flows but same weight are scheduled fairly.
        let mut wfq = WFQServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1, 1], // Same weights
        );

        // Create packets from two flows
        let packet1 = Packet::new(1024, 1, 0, 0.0); // flow_id 0
        let packet2 = Packet::new(1024, 2, 1, 0.1); // flow_id 1

        // Send packets to WFQServer
        wfq.on_packet_received(packet1.clone(), 0.0);
        wfq.on_packet_received(packet2.clone(), 0.0);

        // Run the scheduler
        wfq.test_run(0.0);

        // Check that packets are scheduled fairly (tags should reflect arrival times)
        assert!(wfq.sent_packets[0].tag <= wfq.sent_packets[1].tag);
    }

    #[test]
    fn test_packet_departure_time() {
        // Test that the departure time of packets is calculated correctly.
        let mut wfq = WFQServer::new(
            1e6, // 1 Mbps
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1],
        );

        // Create a packet
        let packet = Packet::new(1000, 1, 0, 0.0); // 1000 bytes

        // Expected transmission time = (size * 8) / rate
        let expected_transmission_time = (1000.0 * 8.0) / 1e6; // 0.008 seconds

        wfq.on_packet_received(packet.clone(), 0.0);

        // Run the scheduler
        wfq.test_run(0.0);

        // Send the packet (simulate the event)
        wfq.update_internal_states(&packet, wfq.time_packet_sent);

        // Check that time_packet_sent is correct
        assert!((wfq.time_packet_sent - expected_transmission_time).abs() < 1e-6);
    }

    #[test]
    fn test_red_drop_strategy() {
        // Test using RED drop strategy.
        let mut wfq = WFQServer::new(
            1e6,
            10, // capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::RED, // Use RED
            vec![1],
        );

        // Send multiple packets to fill the queue
        for i in 0..20 {
            let packet = Packet::new(1024, i, 0, 0.0);
            wfq.on_packet_received(packet.clone(), 0.0);
        }

        // With RED, some packets should be randomly dropped before reaching capacity
        assert_eq!(wfq.packets_dropped, 10);
    }

    #[test]
    fn test_flow_class_mapping() {
        // Test custom flow_classes mapping.
        let mut wfq = WFQServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| (flow_id % 3) as usize), // Map flow_ids to 3 classes
            DropStrategy::TailDrop,
            vec![1, 2, 3], // Different weights
        );

        // Create packets from different flows
        let packet1 = Packet::new(1024, 1, 1, 0.0); // flow_id 1 -> class 1
        let packet2 = Packet::new(1024, 2, 2, 0.0); // flow_id 2 -> class 2
        let packet3 = Packet::new(1024, 3, 3, 0.0); // flow_id 3 -> class 0

        // Send packets
        wfq.on_packet_received(packet2.clone(), 0.0);
        wfq.on_packet_received(packet1.clone(), 0.0);
        wfq.on_packet_received(packet3.clone(), 0.0);

        // Check that flow_class mapping works
        assert_eq!((wfq.flow_classes)(1), 1);
        assert_eq!((wfq.flow_classes)(2), 2);
        assert_eq!((wfq.flow_classes)(3), 0);

        // Run the scheduler
        wfq.test_run(0.0);

        // Check that packets are scheduled according to weights
        // Packet from class 2 (weight 3) should have smallest tag
        assert_eq!(wfq.sent_packets[0].packet.flow_id, 2);
    }
}

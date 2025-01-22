//! Implements a Static Priority (SP) scheduler.

use std::collections::BTreeMap;
use std::collections::VecDeque;
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

pub struct SPServer {
    scheduler_id: usize,

    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

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
    /// index is class ID, value is byte count
    byte_sizes: Vec<usize>,

    /// Total bytes currently queued across all classes
    total_queued_bytes: usize,

    /// FIFO queues of classes
    /// priority -> queue
    queues: BTreeMap<usize, VecDeque<Packet>>,

    /// Vector where index is class_id and value is priority
    priorities: Vec<usize>,

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
    sent_packets: Vec<Packet>,
}

impl SPServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        priorities: Vec<usize>,
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

        // Size of byte_sizes vector matches number of classes
        let byte_sizes = vec![0; priorities.len()];

        SPServer {
            scheduler_id,
            time: 0.0,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            packets_forwarded: 0,
            byte_sizes,
            total_queued_bytes: 0,
            queues: BTreeMap::new(),
            priorities,
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

    pub fn on_packet_received(&mut self, packet: Packet) {
        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.total_queued_bytes,
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
                packet.time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.update_stats_on_packet_received(&packet);

        // Update bytes tracked
        let class_id = (self.flow_classes)(packet.flow_id);
        self.byte_sizes[class_id] += packet.size;
        self.total_queued_bytes += packet.size;

        // pushes the packet to the back of its priority queue
        let priority = self.priorities[class_id];
        let queue = self.queues.entry(priority).or_default();
        queue.push_back(packet.clone());

        debug!(
            "SPServer {} received packet {} ({} bytes) from flow {} belonging to class {} at time {:.3}. \
             {} packet(s) in flow class {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            class_id,
            packet.time,
            queue.len(),
            class_id
        );
    }

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

        self.on_packet_received(packet);

        if packet.time >= self.busy_until {
            self.run(packet.time, cx);
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.time = packet.time;
        self.update_stats_on_packet_forwarded(&packet);

        #[cfg(test)]
        self.sent_packets.push(packet.clone());

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
    /// Schedule packets using provided event handler
    fn schedule_packet<F>(&mut self, mut schedule_event: F)
    where
        F: FnMut(f64, f64, Packet),
    {
        if let Some(current_priority) = self.next_priority() {
            let queue = self.queues.get_mut(&current_priority).unwrap();
            let mut packet = queue.pop_front().unwrap();

            // Update byte tracking
            let class_id = (self.flow_classes)(packet.flow_id);
            self.byte_sizes[class_id] -= packet.size;
            self.total_queued_bytes -= packet.size;

            packet.queueing_delay_update(self.time);

            // calculate send timeout
            let timeout = packet.size as f64 * 8.0 / self.rate;
            packet.departure_update(self.time + timeout);

            // call provided event handler
            schedule_event(self.time, timeout, packet);

            self.busy_until = self.time + timeout;

            debug!(
                "SPServer {} will send packet {} ({} bytes, priority {}) from flow {} at time {:.8e}. {} packets in the priority queue.",
                self.scheduler_id,
                packet.packet_id,
                packet.size,
                current_priority,
                packet.flow_id,
                self.time + timeout,
                queue.len(),
            );
        }
    }

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
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::send, outbound)
                .unwrap();

            // schedules the next run
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::run, now + timeout)
                .unwrap();
        });
    }

    #[cfg(test)]
    pub fn test_run(&mut self, now: f64) {
        // creates vector to collect events
        let mut events = Vec::new();

        self.schedule_packet(|_now, timeout, outbound| {
            // collects the events
            events.push((timeout, outbound));
        });

        // processes collected events
        for (timeout, outbound) in events {
            self.sent_packets.push(outbound.clone());
            self.update_stats_on_packet_forwarded(&outbound);
            self.busy_until = now + timeout;

            // recursively schedules the next run
            self.test_run(now + timeout);
        }
    }

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        let report = self.prepare_report(now);
        CsvLogger::log_report(Report::SchedulerReport(report), ReportTiming::InProgress);

        debug!(
            "SPServer {} logged a periodic report at time {:.3}.",
            self.scheduler_id, now
        );

        self.reset_stats(now);
    }
}

impl ReportStatistics for SPServer {
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

impl Model for SPServer {
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

    #[test]
    fn test_single_packet() {
        let priorities = vec![1]; // class 0 has priority 1

        let mut sp = SPServer::new(
            1e6, // server rate: 1 Mbps
            10,  // capacity: 10 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id), // flow_classes mapping
            DropStrategy::TailDrop,
            priorities,
        );

        // creates a packet
        let packet = Packet::new(1024, 1, 0, 0.0);

        // sends packet to SPServer
        sp.on_packet_received(packet.clone());

        // checks that the packet is in the queue
        assert_eq!(sp.queues[&1].len(), 1);
        assert_eq!(sp.packets_received, 1);

        // runs the scheduler
        sp.test_run(0.0);

        // verifies packet was sent
        assert!(sp.busy_until > 0.0);
        assert_eq!(sp.sent_packets.len(), 1);
    }

    #[test]
    fn test_priority_ordering() {
        let priorities = vec![1, 2]; // class 0 has priority 1, class 1 has priority 2

        let mut sp = SPServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // creates packets with different priorities
        let low_prio_packet = Packet::new(1024, 1, 0, 0.0);
        let high_prio_packet = Packet::new(1024, 2, 1, 0.0);

        // sends low priority packet first
        sp.on_packet_received(low_prio_packet.clone());
        sp.on_packet_received(high_prio_packet.clone());

        // runs the scheduler
        sp.test_run(0.0);

        // verifies high priority packet was sent first
        assert_eq!(sp.sent_packets.len(), 2);
        assert_eq!(sp.sent_packets[0].packet_id, 2); // high priority packet
        assert_eq!(sp.sent_packets[1].packet_id, 1); // low priority packet
    }

    #[test]
    fn test_queue_overflow() {
        let priorities = vec![1];

        let mut sp = SPServer::new(
            1e6,
            2, // capacity: 2 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // creates three packets
        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 0, 0.0);
        let packet3 = Packet::new(1024, 3, 0, 0.0);

        // sends packets to SPServer
        sp.on_packet_received(packet1);
        sp.on_packet_received(packet2);
        sp.on_packet_received(packet3);

        // verifies only two packets are in queue due to capacity limit
        assert_eq!(sp.queues[&1].len(), 2);
        assert_eq!(sp.packets_dropped, 1);
    }

    #[test]
    fn test_unlimited_capacity() {
        let priorities = vec![1];

        let mut sp = SPServer::new(
            1e6,
            0, // unlimited capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // sends multiple packets
        for i in 0..100 {
            let packet = Packet::new(1024, i, 0, 0.0);
            sp.on_packet_received(packet);
        }

        // verifies no packets were dropped
        assert_eq!(sp.packets_dropped, 0);
    }

    #[test]
    fn test_red_drop_strategy() {
        let priorities = vec![1];

        let mut sp = SPServer::new(
            1e6,
            10, // capacity
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::RED,
            priorities,
        );

        // sends multiple packets to fill the queue
        for i in 0..20 {
            let packet = Packet::new(1024, i, 0, 0.0);
            sp.on_packet_received(packet);
        }

        // verifies RED dropped some packets before reaching capacity
        assert!(sp.packets_dropped > 0);
    }

    #[test]
    fn test_flow_class_mapping() {
        let priorities = vec![1, 2]; // class 0 -> priority 1, class 1 -> priority 2

        let mut sp = SPServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id % 2), // maps to 2 classes
            DropStrategy::TailDrop,
            priorities,
        );

        // creates packets from different flows
        let packet1 = Packet::new(1024, 1, 1, 0.0); // flow 1 -> class 1 (high priority)
        let packet2 = Packet::new(1024, 2, 0, 0.0); // flow 0 -> class 0 (low priority)
        let packet3 = Packet::new(1024, 3, 2, 0.0); // flow 2 -> class 0 (low priority)

        // sends packets
        sp.on_packet_received(packet2);
        sp.on_packet_received(packet3);
        sp.on_packet_received(packet1);

        // runs scheduler
        sp.test_run(0.0);

        // verifies high priority packet (from class 1) sent first
        assert_eq!(sp.sent_packets[0].flow_id, 1);
    }

    #[test]
    fn test_fifo_within_priority() {
        let priorities = vec![1];

        let mut sp = SPServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // sends multiple packets with same priority
        for i in 0..3 {
            let packet = Packet::new(1024, i, 0, 0.0);
            sp.on_packet_received(packet);
        }

        // runs scheduler
        sp.test_run(0.0);

        // verifies FIFO order within same priority
        assert_eq!(sp.sent_packets[0].packet_id, 0);
        assert_eq!(sp.sent_packets[1].packet_id, 1);
        assert_eq!(sp.sent_packets[2].packet_id, 2);
    }

    #[test]
    fn test_dynamic_flows() {
        let priorities = vec![1, 2];

        let mut sp = SPServer::new(
            1000.0,
            100,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // initially sends only low priority packets
        for i in 0..5 {
            let packet = Packet::new(10, i, 0, 0.0);
            sp.on_packet_received(packet);
        }

        sp.test_run(0.0);
        let initial_sent = sp.sent_packets.len();

        // then sends high priority packets
        for i in 5..10 {
            let packet = Packet::new(10, i, 1, 1.0); // high priority packets
            sp.on_packet_received(packet);
        }

        // runs scheduler again
        sp.test_run(1.0);

        // verifies:
        // 1. Initial low priority packets were sent first (no competition)
        // 2. Then high priority packets were sent before remaining low priority
        assert_eq!(initial_sent, 5); // first 5 low priority packets sent

        // all remaining packets should be high priority (flow_id 1)
        for i in 5..sp.sent_packets.len() {
            assert_eq!(sp.sent_packets[i].flow_id, 1);
        }
    }

    #[test]
    fn test_large_packet_handling() {
        let priorities = vec![1];

        let mut sp = SPServer::new(
            1e6,
            1500, // capacity in bytes
            CapacityUnit::Bytes,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // creates a packet larger than capacity
        let large_packet = Packet::new(1501, 1, 0, 0.0);
        sp.on_packet_received(large_packet);

        // verifies packet was dropped
        assert_eq!(sp.packets_dropped, 1);
        assert!(sp.queues.get(&1).map_or(true, |q| q.is_empty()));

        // sends a packet within capacity limits
        let normal_packet = Packet::new(1000, 2, 0, 0.0);
        sp.on_packet_received(normal_packet);

        // verifies normal packet was accepted
        assert_eq!(sp.queues[&1].len(), 1);
    }

    #[test]
    fn test_multiple_priority_levels() {
        let priorities = vec![1, 2, 3];

        let mut sp = SPServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            priorities,
        );

        // sends packets with different priorities in reverse order
        let packet1 = Packet::new(1024, 1, 0, 0.0); // lowest priority
        let packet2 = Packet::new(1024, 2, 1, 0.0); // medium priority
        let packet3 = Packet::new(1024, 3, 2, 0.0); // highest priority

        sp.on_packet_received(packet1);
        sp.on_packet_received(packet2);
        sp.on_packet_received(packet3);

        // runs scheduler
        sp.test_run(0.0);

        // verifies packets were sent in priority order (highest to lowest)
        assert_eq!(sp.sent_packets.len(), 3);
        assert_eq!(sp.sent_packets[0].flow_id, 2); // highest priority
        assert_eq!(sp.sent_packets[1].flow_id, 1); // medium priority
        assert_eq!(sp.sent_packets[2].flow_id, 0); // lowest priority
    }
}

//! Implements a Weighted Round Robin (WRR) scheduler.

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

pub struct WRRServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Weighted Round Robin
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// weights of classes, which are consecutive and start from 0
    weights: Vec<usize>,

    /// number of packets sent in current round for each class
    packets_sent_in_round: Vec<usize>,

    /// the number of packets received, dropped, in the queues waiting to be
    /// sent, and forwarded
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,
    packets_forwarded: usize,

    /// the number of bytes in each class queue
    byte_sizes: Vec<usize>,

    /// FIFO queues of classes, which are consecutive and start from 0
    queues: Vec<VecDeque<Packet>>,

    /// the current queue being served
    current_queue: usize,

    /// the server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,

    /// the statistics of a periodic report
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

impl WRRServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        weights: Vec<usize>,
    ) -> WRRServer {
        let mut byte_sizes = Vec::new();
        let mut queues = Vec::new();
        let mut packets_sent_in_round = Vec::new();

        for _ in weights.iter() {
            byte_sizes.push(0);
            queues.push(VecDeque::new());
            packets_sent_in_round.push(0);
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

        WRRServer {
            scheduler_id,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            weights,
            packets_sent_in_round,
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            packets_forwarded: 0,
            byte_sizes,
            queues,
            current_queue: 0,
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
            self.byte_sizes.iter().sum(),
            self.queues.iter().map(|q| q.len()).sum(),
        );

        // the case that this packet will be dropped
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "WRRServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped
        self.update_stats_on_packet_received(&packet);
        self.packets_waiting += 1;

        let class_id = (self.flow_classes)(packet.flow_id);

        // pushes the packet to the back of its class queue
        self.queues[class_id].push_back(packet.clone());
        self.byte_sizes[class_id] += packet.size;

        debug!(
            "WRRServer {} received packet {} ({} bytes) from flow {} belonging to class {} at time {:.3}. \
            {} packet(s) in flow class {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            class_id,
            arrival_time,
            self.queues[class_id].len(),
            class_id
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

    pub async fn send(&mut self, packet: Packet) {
        self.update_stats_on_packet_forwarded(&packet);
        self.output.send(packet).await;
    }

    /// moves on to the next queue if the current queue is empty or has sent its weight worth of packets
    fn next_queue(&mut self) {
        self.current_queue = (self.current_queue + 1) % self.queues.len();
        // Reset counts only when we complete a full cycle
        if self.current_queue == 0 {
            for count in self.packets_sent_in_round.iter_mut() {
                *count = 0;
            }
        }
    }

    fn schedule_packets<F>(&mut self, now: f64, mut schedule_events: F)
    where
        F: FnMut(f64, Packet),
    {
        // Iterate through all queues based on their weights
        for _ in 0..self.weights.len() {
            if self.packets_waiting == 0 {
                return;
            }

            // Check if current queue has packets and hasn't exceeded its weight
            if !self.queues[self.current_queue].is_empty()
                && self.packets_sent_in_round[self.current_queue] < self.weights[self.current_queue]
            {
                if let Some(mut outbound) = self.queues[self.current_queue].pop_front() {
                    self.byte_sizes[self.current_queue] -= outbound.size;
                    outbound.queueing_delay_update(now);

                    self.packets_waiting -= 1;
                    self.packets_sent_in_round[self.current_queue] += 1;

                    let transmission_time = (outbound.size as f64 * 8.0) / self.rate;

                    schedule_events(transmission_time, outbound.clone());
                    self.busy_until = now + transmission_time;

                    debug!(
                        "WRRServer {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
                        {} packets in the class queue.",
                        self.scheduler_id,
                        outbound.packet_id,
                        outbound.size,
                        outbound.flow_id,
                        now + transmission_time,
                        self.queues[self.current_queue].len(),
                    );
                }
            }

            self.next_queue();
        }
    }

    pub fn run(&mut self, _: (), cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        self.schedule_packets(now, |timeout, outbound| {
            // schedules the send event
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::send, outbound)
                .unwrap();

            // schedules the next run
            cx.schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
                .unwrap();
        });
    }

    #[cfg(test)]
    pub fn test_run(&mut self, now: f64) {
        // creates a vector to collect events inside the closure
        let mut events = Vec::new();

        // calls schedule_packets() without borrowing self inside the closure
        self.schedule_packets(now, |transmission_time, mut outbound| {
            // simulates sending the packet
            outbound.departure_update(now + transmission_time);

            // collects the outbound packet and transmission_time
            events.push((transmission_time, outbound));
        });

        // processes collected events after schedule_packets returns
        for (timeout, outbound) in events {
            // updates the sent_packets vector
            self.sent_packets.push(outbound.clone());

            // updates statistics
            self.update_stats_on_packet_forwarded(&outbound);

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
            "WRRServer {} logged a periodic report at time {:.3}.",
            self.scheduler_id, now
        );

        self.reset_stats(now);
    }
}

impl ReportStatistics for WRRServer {
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

impl Model for WRRServer {
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
        let mut wrr = WRRServer::new(
            1e6, // server rate: 1 Mbps
            10,  // capacity: 10 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1], // weights for one class
        );

        let packet = Packet::new(1024, 1, 0, 0.0);
        wrr.on_packet_received(packet.clone(), 0.0);

        assert_eq!(wrr.queues[0].len(), 1);
        assert_eq!(wrr.packets_received, 1);

        wrr.test_run(0.0);

        assert!(wrr.busy_until > 0.0);
        assert_eq!(wrr.sent_packets.len(), 1);
    }

    #[test]
    fn test_multiple_flows() {
        let mut wrr = WRRServer::new(
            8.0,
            4,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![2, 1, 1], // weights 2:1:1
        );

        // Send packets to different flows
        let packet1 = Packet::new(1, 1, 0, 0.0);
        let packet2 = Packet::new(1, 2, 1, 0.0);
        let packet3 = Packet::new(1, 3, 2, 0.0);
        let packet4 = Packet::new(1, 4, 0, 0.0);

        wrr.on_packet_received(packet1, 0.0);
        wrr.on_packet_received(packet2, 0.0);
        wrr.on_packet_received(packet3, 0.0);
        wrr.on_packet_received(packet4, 0.0);

        wrr.test_run(0.0);

        // Check that packets are sent according to weights
        assert_eq!(wrr.sent_packets.len(), 4);
        let sent_flow_ids: Vec<usize> = wrr.sent_packets.iter().map(|p| p.flow_id).collect();
        // Flow 0 should get 2 slots before others get 1 each
        assert_eq!(sent_flow_ids, vec![0, 0, 1, 2]);
    }

    #[test]
    fn test_queue_overflow() {
        let mut wrr = WRRServer::new(
            1e6,
            2, // capacity: 2 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1, 1],
        );

        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 1, 0.0);
        let packet3 = Packet::new(1024, 3, 0, 0.0);

        wrr.on_packet_received(packet1, 0.0);
        wrr.on_packet_received(packet2, 0.0);
        wrr.on_packet_received(packet3, 0.0);

        assert_eq!(wrr.queues[0].len() + wrr.queues[1].len(), 2);
        assert_eq!(wrr.packets_dropped, 1);
    }

    #[test]
    fn test_multiple_weight_ratios() {
        let mut wrr = WRRServer::new(
            1000.0, // Server rate: 1000 bps
            120,    // Capacity: 120 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id % 3), // Map to 3 classes
            DropStrategy::TailDrop,
            vec![1, 2, 4], // 1:2:4 weight ratio
        );

        let packet_size = 1000;

        // sends packets to all three flows at fixed intervals
        let arrival_interval = 0.001;
        let mut arrival_time = 0.0;

        for i in 0..40 {
            // sends one packet to each flow in sequence
            let packet1 = Packet::new(packet_size, i * 3, 0, arrival_time);
            wrr.on_packet_received(packet1, arrival_time);

            let packet2 = Packet::new(packet_size, i * 3 + 1, 1, arrival_time);
            wrr.on_packet_received(packet2, arrival_time);

            let packet3 = Packet::new(packet_size, i * 3 + 2, 2, arrival_time);
            wrr.on_packet_received(packet3, arrival_time);

            arrival_time += arrival_interval;
        }

        wrr.test_run(0.0);

        // calculates bytes sent per flow
        let bytes: Vec<usize> = (0..3)
            .map(|flow_id| {
                wrr.sent_packets
                    .iter()
                    .take(40) // only considers first 40 packets sent
                    .filter(|p| p.flow_id == flow_id)
                    .map(|p| p.size)
                    .sum()
            })
            .collect();

        println!("Flow 0 (weight 1): {} bytes", bytes[0]);
        println!("Flow 1 (weight 2): {} bytes", bytes[1]);
        println!("Flow 2 (weight 4): {} bytes", bytes[2]);
        println!("Ratio flow 1/flow 0: {}", bytes[1] as f64 / bytes[0] as f64);
        println!("Ratio flow 2/flow 0: {}", bytes[2] as f64 / bytes[0] as f64);

        // adjusts assertion to reflect the actual ratios, allowing some tolerance
        assert!((bytes[1] as f64 / bytes[0] as f64 - 2.0).abs() < 0.2);
        assert!((bytes[2] as f64 / bytes[0] as f64 - 4.0).abs() < 0.4);
    }

    #[test]
    fn test_red_drop_strategy() {
        let mut wrr = WRRServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::RED,
            vec![1],
        );

        // Send many packets to trigger RED dropping
        for i in 0..20 {
            let packet = Packet::new(1024, i, 0, 0.0);
            wrr.on_packet_received(packet, 0.0);
        }

        assert!(wrr.packets_dropped > 0);
        assert!(wrr.queues.iter().map(|q| q.len()).sum::<usize>() < 20);
    }

    #[test]
    fn test_flow_class_mapping() {
        let mut wrr = WRRServer::new(
            1e6, // Server rate: 1 Mbps
            10,  // Capacity: 10 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id % 3), // Map flows to 3 classes
            DropStrategy::TailDrop,
            vec![1, 2, 3], // Weights for classes 0, 1, 2 respectively
        );

        // Send packets mapped to classes based on flow_id
        let packets = vec![
            Packet::new(1024, 1, 0, 0.0), // flow_id 0 -> class 0
            Packet::new(1024, 2, 1, 0.0), // flow_id 1 -> class 1
            Packet::new(1024, 3, 2, 0.0), // flow_id 2 -> class 2
            Packet::new(1024, 4, 0, 0.0), // flow_id 0 -> class 0
            Packet::new(1024, 5, 1, 0.0), // flow_id 1 -> class 1
            Packet::new(1024, 6, 2, 0.0), // flow_id 2 -> class 2
        ];

        for packet in &packets {
            wrr.on_packet_received(packet.clone(), 0.0);
        }

        wrr.test_run(0.0);

        // Counts sent packets per class
        let class0_packets = wrr
            .sent_packets
            .iter()
            .filter(|p| (p.flow_id % 3) == 0)
            .count();
        let class1_packets = wrr
            .sent_packets
            .iter()
            .filter(|p| (p.flow_id % 3) == 1)
            .count();
        let class2_packets = wrr
            .sent_packets
            .iter()
            .filter(|p| (p.flow_id % 3) == 2)
            .count();

        // Expected distribution based on weights [1, 2, 3]:
        // class0: 1 packet per round * 2 rounds = 2 packets
        // class1: 2 packets per round * 2 rounds = 4 packets
        // class2: 3 packets per round * 2 rounds = 6 packets
        assert_eq!(class0_packets, 2, "Class 0 should have sent 2 packets");
        assert_eq!(class1_packets, 4, "Class 1 should have sent 4 packets");
        assert_eq!(class2_packets, 6, "Class 2 should have sent 6 packets");
    }

    #[test]
    fn test_dynamic_flows() {
        let mut wrr = WRRServer::new(
            1000.0,
            100,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id % 2), // Map to two classes
            DropStrategy::TailDrop,
            vec![1, 1], // Equal weights for two classes
        );

        // First phase: only send to flow 0 (class 0)
        for i in 0..10 {
            let packet = Packet::new(10, i, 0, 0.0);
            wrr.on_packet_received(packet, 0.0);
        }

        // Second phase: send to both flows
        for i in 10..20 {
            let packet1 = Packet::new(10, i * 2, 0, 1.0); // Flow 0 -> class 0
            let packet2 = Packet::new(10, i * 2 + 1, 1, 1.0); // Flow 1 -> class 1
            wrr.on_packet_received(packet1, 1.0);
            wrr.on_packet_received(packet2, 1.0);
        }

        wrr.test_run(0.0);

        // Count packets in second phase
        let phase2_packets = wrr
            .sent_packets
            .iter()
            .filter(|p| p.time >= 1.0)
            .collect::<Vec<_>>();

        let flow0_phase2 = phase2_packets
            .iter()
            .filter(|p| p.flow_id % 2 == 0) // Class 0 packets
            .count();
        let flow1_phase2 = phase2_packets
            .iter()
            .filter(|p| p.flow_id % 2 == 1) // Class 1 packets
            .count();

        // In second phase, flows should get equal treatment
        assert!((flow0_phase2 as i32 - flow1_phase2 as i32).abs() <= 1);
    }

    #[test]
    fn test_empty_queues() {
        let mut wrr = WRRServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![2, 1, 1],
        );

        // Send packets only to flows 0 and 2
        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 2, 0.0);

        wrr.on_packet_received(packet1, 0.0);
        wrr.on_packet_received(packet2, 0.0);

        wrr.test_run(0.0);

        // Should skip empty queue (flow 1) and maintain weight proportions
        // for non-empty queues
        assert_eq!(wrr.sent_packets.len(), 2);
        assert_eq!(wrr.sent_packets[0].flow_id, 0);
        assert_eq!(wrr.sent_packets[1].flow_id, 2);
    }

    #[test]
    fn test_packet_timing() {
        let mut wrr = WRRServer::new(
            1000.0, // 1000 bps
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            vec![1],
        );

        // Send a packet of 100 bits (size 12.5 bytes)
        let packet = Packet::new(12, 1, 0, 0.0);
        wrr.on_packet_received(packet, 0.0);

        wrr.test_run(0.0);

        // Transmission time should be (12 * 8) / 1000 = 0.096 seconds
        assert!((wrr.busy_until - 0.096).abs() < 1e-6);
    }
}

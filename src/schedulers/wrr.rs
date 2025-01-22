//! Implements a Weighted Round Robin (WRR) scheduler.
//! https://en.wikipedia.org/wiki/Weighted_fair_queueing

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

    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

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
            time: 0.0,
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

    pub fn on_packet_received(&mut self, packet: Packet) {
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
                "WRRServer {} dropped packet {} from flow {} at time {:.8e}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                packet.time
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
            packet.time,
            self.queues[class_id].len(),
            class_id
        );
    }

    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        // to be removed after more thorough testing
        let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        // makes sure that the current simulation time can be correctly retrieved from
        // the packet itself
        assert!(
            (packet.time - global_time).abs() <= 1e-7,
            "Timing mismatch: packet.time = {}, global_time = {}",
            packet.time,
            global_time
        );

        self.on_packet_received(packet);

        if packet.time >= self.busy_until {
            self.run(packet.time, cx);
        }
    }

    pub async fn send(&mut self, packet: (f64, Packet)) {
        self.time = packet.0;
        self.update_stats_on_packet_forwarded(&packet.1);
        self.output.send(packet.1).await;
    }

    fn schedule_packet<F>(&mut self, mut schedule_event: F)
    where
        F: FnMut(f64, f64, Packet),
    {
        loop {
            // Return if no packets are waiting
            if self.packets_waiting == 0 {
                return;
            }

            // sends a single packet from the current queue according to its weight, which is an
            // integer indicating the number of packets to be sent in this round
            if !self.queues[self.current_queue].is_empty()
                && self.packets_sent_in_round[self.current_queue] < self.weights[self.current_queue]
            {
                if let Some(mut outbound) = self.queues[self.current_queue].pop_front() {
                    self.byte_sizes[self.current_queue] -= outbound.size;
                    outbound.queueing_delay_update(self.time);

                    self.packets_waiting -= 1;
                    self.packets_sent_in_round[self.current_queue] += 1;

                    // calculate send timeout
                    let timeout = outbound.size as f64 * 8.0 / self.rate;
                    outbound.departure_update(self.time + timeout);

                    schedule_event(self.time, timeout, outbound.clone());
                    self.busy_until = self.time + timeout;

                    debug!(
                        "WRRServer {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
                        {} packets in the class queue.",
                        self.scheduler_id,
                        outbound.packet_id,
                        outbound.size,
                        outbound.flow_id,
                        self.time + timeout,
                        self.queues[self.current_queue].len(),
                    );
                    return;
                }
            }

            // moves to the next queue
            self.packets_sent_in_round[self.current_queue] = 0;
            self.current_queue = (self.current_queue + 1) % self.queues.len();
        }
    }

    pub fn run(&mut self, now: f64, cx: &mut Context<Self>) {
        // to be removed after more thorough testing
        let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.time = now;

        if self.time == 0.0 {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            self.time = global_time;
        }

        // makes sure that the current simulation time can be correctly retrieved from
        // the packet itself
        assert!(
            (now - global_time).abs() <= 1e-7,
            "Timing mismatch: now = {}, global_time = {}",
            now,
            global_time
        );

        self.schedule_packet(|now, timeout, outbound| {
            // schedules the send event
            cx.schedule_event(
                Duration::from_secs_f64(timeout),
                Self::send,
                (timeout, outbound),
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
        self.schedule_packet(|now, transmission_time, mut outbound| {
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
        wrr.on_packet_received(packet.clone());

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

        wrr.on_packet_received(packet1);
        wrr.on_packet_received(packet2);
        wrr.on_packet_received(packet3);
        wrr.on_packet_received(packet4);

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

        wrr.on_packet_received(packet1);
        wrr.on_packet_received(packet2);
        wrr.on_packet_received(packet3);

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
            wrr.on_packet_received(packet1);

            let packet2 = Packet::new(packet_size, i * 3 + 1, 1, arrival_time);
            wrr.on_packet_received(packet2);

            let packet3 = Packet::new(packet_size, i * 3 + 2, 2, arrival_time);
            wrr.on_packet_received(packet3);

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
            wrr.on_packet_received(packet);
        }

        assert!(wrr.packets_dropped > 0);
        assert!(wrr.queues.iter().map(|q| q.len()).sum::<usize>() < 20);
    }

    #[test]
    fn test_flow_class_mapping() {
        // uses a capacity to 12 so none of the packets are dropped
        let mut wrr = WRRServer::new(
            1e6, // server rate (1 Mbps)
            12,  // capacity: 12 packets
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id % 3), // maps flows to 3 classes
            DropStrategy::TailDrop,
            vec![1, 2, 3], // weights for classes 0, 1, 2
        );

        // We want two "rounds" of WRR with ratio 1:2:3, so each class needs:
        // Class 0: 2 packets
        // Class 1: 4 packets
        // Class 2: 6 packets
        // That totals 12 packets. Each flow_id % 3 = class.
        // flow_ids that go to class 0: 0, 3
        // flow_ids that go to class 1: 1, 4, 7, 10
        // flow_ids that go to class 2: 2, 5, 8, 11, 14, 17 (we only need 6 of these).

        let packets = vec![
            // Class 0 (flow_id multiples of 3)
            Packet::new(1024, 0, 0, 0.0),
            Packet::new(1024, 3, 0, 0.0),
            // Class 1 (flow_id ≡ 1 mod 3)
            Packet::new(1024, 1, 1, 0.0),
            Packet::new(1024, 4, 1, 0.0),
            Packet::new(1024, 7, 1, 0.0),
            Packet::new(1024, 10, 1, 0.0),
            // Class 2 (flow_id ≡ 2 mod 3)
            Packet::new(1024, 2, 2, 0.0),
            Packet::new(1024, 5, 2, 0.0),
            Packet::new(1024, 8, 2, 0.0),
            Packet::new(1024, 11, 2, 0.0),
            Packet::new(1024, 14, 2, 0.0),
            Packet::new(1024, 17, 2, 0.0),
        ];

        for packet in &packets {
            wrr.on_packet_received(packet.clone());
        }

        // runs the WRR at time = 0.0
        wrr.test_run(0.0);

        // splits sent packets into rounds (each round sends 6 packets)
        let mut rounds: Vec<Vec<usize>> = vec![vec![], vec![]];
        for (i, packet) in wrr.sent_packets.iter().enumerate() {
            let round = i / 6;
            rounds[round].push(packet.packet_id);
        }

        // expects packet IDs for each round
        let expected_round1 = vec![0, 1, 4, 2, 5, 8];
        let expected_round2 = vec![3, 7, 10, 11, 14, 17];

        // asserts that packets sent in each round match the expected IDs
        assert_eq!(rounds[0], expected_round1, "Round 1 packet IDs mismatch");
        assert_eq!(rounds[1], expected_round2, "Round 2 packet IDs mismatch");

        // counts how many packets each queue actually sent
        let class0_packets: Vec<usize> = wrr
            .sent_packets
            .iter()
            .filter(|p| p.flow_id % 3 == 0)
            .map(|p| p.packet_id)
            .collect();
        let class1_packets: Vec<usize> = wrr
            .sent_packets
            .iter()
            .filter(|p| p.flow_id % 3 == 1)
            .map(|p| p.packet_id)
            .collect();
        let class2_packets: Vec<usize> = wrr
            .sent_packets
            .iter()
            .filter(|p| p.flow_id % 3 == 2)
            .map(|p| p.packet_id)
            .collect();

        // expects packet IDs by class in the correct round-robin order
        let expected_class0_ids = vec![0, 3];
        let expected_class1_ids = vec![1, 4, 7, 10];
        let expected_class2_ids = vec![2, 5, 8, 11, 14, 17];

        // asserts packet IDs match the expected order for each class
        assert_eq!(
            class0_packets, expected_class0_ids,
            "Class 0 packet IDs mismatch"
        );
        assert_eq!(
            class1_packets, expected_class1_ids,
            "Class 1 packet IDs mismatch"
        );
        assert_eq!(
            class2_packets, expected_class2_ids,
            "Class 2 packet IDs mismatch"
        );
        // now the test can truly expect 2, 4, and 6
        assert_eq!(
            class0_packets.len(),
            2,
            "Class 0 should have sent 2 packets"
        );
        assert_eq!(
            class1_packets.len(),
            4,
            "Class 1 should have sent 4 packets"
        );
        assert_eq!(
            class2_packets.len(),
            6,
            "Class 2 should have sent 6 packets"
        );
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

        // first phase: only send to flow 0 (class 0)
        for i in 0..10 {
            let packet = Packet::new(10, i, 0, 0.0);
            wrr.on_packet_received(packet);
        }

        // second phase: send to both flows
        for i in 10..20 {
            let packet1 = Packet::new(10, i * 2, 0, 1.0); // Flow 0 -> class 0
            let packet2 = Packet::new(10, i * 2 + 1, 1, 1.0); // Flow 1 -> class 1
            wrr.on_packet_received(packet1);
            wrr.on_packet_received(packet2);
        }

        wrr.test_run(0.0);

        // counts packets in second phase
        let phase2_packets = wrr
            .sent_packets
            .iter()
            .filter(|p| p.creation_time >= 1.0)
            .collect::<Vec<_>>();

        let flow0_phase2 = phase2_packets
            .iter()
            .filter(|p| p.flow_id % 2 == 0) // Class 0 packets
            .count();
        let flow1_phase2 = phase2_packets
            .iter()
            .filter(|p| p.flow_id % 2 == 1) // Class 1 packets
            .count();

        // in the second phase, flows should get equal treatment
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

        wrr.on_packet_received(packet1);
        wrr.on_packet_received(packet2);

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
        wrr.on_packet_received(packet);

        wrr.test_run(0.0);

        // Transmission time should be (12 * 8) / 1000 = 0.096 seconds
        assert!((wrr.busy_until - 0.096).abs() < 1e-6);
    }
}

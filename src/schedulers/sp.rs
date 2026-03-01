//! Implements a Static Priority (SP) scheduler.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tracing::instrument;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
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

#[cfg(feature = "lean")]
use crate::schedulers::drop::DropDecision;
#[cfg(feature = "lean")]
use crate::utils::logger::{AqmEventKind, AqmEventRow, AqmLoggedEcnField};
use crate::utils::time::{quantize_after, quantize_time};

#[cfg(feature = "lean")]
fn to_ns(time_s: f64) -> u64 {
    (time_s.max(0.0) * 1e9).round() as u64
}

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

    queue_state: Option<std::sync::Arc<QueueState>>,

    /// the statistics of a preiodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,
    scheduled_departures: VecDeque<Packet>,

    /// a vector of packets that have been sent out, only used for unit testing
    #[cfg(test)]
    sent_packets: Vec<Packet>,
}

impl SPServer {
    const SEND_AND_RUN_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const LOG_REPORT_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);

    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        ecn_threshold: f64,
        priorities: Vec<usize>,
    ) -> SPServer {
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
            queue_state: None,
            report_start_time: 0.0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
            scheduled_departures: VecDeque::new(),
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

    #[cfg(feature = "lean")]
    fn log_aqm_event(
        &self,
        event_time: f64,
        queue_id: usize,
        packet: &Packet,
        action: DropAction,
        decision: &DropDecision,
        ecn_before: crate::flows::packet::EcnField,
    ) {
        let event = AqmEventRow {
            time_ns: to_ns(event_time),
            event_id: CsvLogger::next_aqm_event_id(),
            kind: AqmEventKind::Decision,
            scheduler_id: self.scheduler_id as u64,
            queue_id: queue_id as u64,
            packet_id: packet.packet_id as u64,
            flow_id: packet.flow_id as u64,
            size_bytes: packet.size as u64,
            action,
            capacity: decision.witness.capacity as u64,
            capacity_unit: decision.witness.capacity_unit,
            queue_length: decision.witness.queue_length as u64,
            byte_length: decision.witness.byte_length as u64,
            ecn_before: AqmLoggedEcnField::from(ecn_before),
            ecn_after: AqmLoggedEcnField::from(packet.ecn),
            drop_strategy: decision.witness.strategy,
            ecn_threshold_ppb: decision.witness.ecn_threshold_ppb,
            red_min_threshold_ppb: decision.witness.red_min_threshold_ppb,
            red_max_threshold_ppb: decision.witness.red_max_threshold_ppb,
            red_max_probability_ppb: decision.witness.red_max_probability_ppb,
            red_avg_queue_length: decision.witness.red_avg_queue_length.map(|v| v as u64),
            red_rand_max_ppb: decision.witness.red_rand_max_ppb,
            red_rand_min_ppb: decision.witness.red_rand_min_ppb,
        };
        CsvLogger::try_log_report(Report::AqmEventRow(event), ReportTiming::InProgress);
    }

    pub fn on_packet_received(&mut self, packet: Packet) {
        let mut packet = packet;
        let queue_len = self.queues.values().map(|q| q.len()).sum();
        let decision = self
            .drop_strategy
            .decision(packet.size, self.total_queued_bytes, queue_len);
        let class_id = (self.flow_classes)(packet.flow_id);
        #[cfg(feature = "lean")]
        let ecn_before = packet.ecn;
        let drop_action = decision.action;

        match drop_action {
            DropAction::Drop => {
                #[cfg(feature = "lean")]
                self.log_aqm_event(
                    packet.time,
                    class_id,
                    &packet,
                    drop_action,
                    &decision,
                    ecn_before,
                );
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
            DropAction::MarkEcn => {
                if !packet.mark_ce() {
                    #[cfg(feature = "lean")]
                    self.log_aqm_event(
                        packet.time,
                        class_id,
                        &packet,
                        DropAction::Drop,
                        &decision,
                        ecn_before,
                    );
                    self.packets_dropped += 1;
                    debug! {
                        "SPServer {} dropped non-ECT packet {} from flow {} at time {:.3}",
                        self.scheduler_id,
                        packet.packet_id,
                        packet.flow_id,
                        packet.time
                    }
                    return;
                }
                #[cfg(feature = "lean")]
                self.log_aqm_event(
                    packet.time,
                    class_id,
                    &packet,
                    drop_action,
                    &decision,
                    ecn_before,
                );
            }
            DropAction::Enqueue => {
                #[cfg(feature = "lean")]
                self.log_aqm_event(
                    packet.time,
                    class_id,
                    &packet,
                    drop_action,
                    &decision,
                    ecn_before,
                );
            }
        }

        // the case that this packet will not be dropped
        self.update_stats_on_packet_received(&packet);

        // Update bytes tracked
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

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, packet: Packet, cx: &Context<Self>) {
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

    #[instrument(skip(self))]
    pub async fn send(&mut self, packet: Packet) {
        self.time = packet.time;
        self.update_stats_on_packet_forwarded(&packet);

        #[cfg(test)]
        self.sent_packets.push(packet.clone());

        self.output.send(packet).await;
    }

    pub async fn send_and_run(&mut self, _: (), cx: &Context<Self>) {
        let Some(packet) = self.scheduled_departures.pop_front() else {
            debug_assert!(
                false,
                "SPServer {} scheduled departure queue underflow",
                self.scheduler_id
            );
            return;
        };
        self.send(packet).await;
        self.run(self.time, cx);
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
            let departure_time = quantize_after(self.time, timeout);
            packet.departure_update(departure_time);

            // call provided event handler
            let delay = (departure_time - self.time).max(0.0);
            let packet_id = packet.packet_id;
            let packet_size = packet.size;
            let packet_flow_id = packet.flow_id;
            schedule_event(self.time, delay, packet);

            self.busy_until = departure_time;

            debug!(
                "SPServer {} will send packet {} ({} bytes, priority {}) from flow {} at time {:.8e}. {} packets in the priority queue.",
                self.scheduler_id,
                packet_id,
                packet_size,
                current_priority,
                packet_flow_id,
                departure_time,
                queue.len(),
            );
        }
    }

    #[instrument(skip(self, cx))]
    pub fn run(&mut self, now: f64, cx: &Context<Self>) {
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

        let run_time = quantize_time(now);
        self.time = run_time;

        let mut events = Vec::with_capacity(1);
        self.schedule_packet(|_now, delay, outbound| {
            events.push((delay, outbound));
        });

        for (delay, outbound) in events {
            self.scheduled_departures.push_back(outbound);
            cx.schedule_event_fast(
                Duration::from_secs_f64(delay),
                &Self::SEND_AND_RUN_SID,
                Self::send_and_run,
                (),
            )
            .unwrap();
        }
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

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a Context<Self>) {
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

impl Model for SPServer {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::send_and_run));
        registry.add(cx.register_schedulable(Self::log_report));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        let report_interval = CsvLogger::get_instance().get_report_interval();

        if report_interval < f64::MAX {
            cx.schedule_periodic_event(
                Duration::from_secs_f64(report_interval),
                Duration::from_secs_f64(report_interval),
                &Self::LOG_REPORT_SID,
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
            0.0,
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
            0.0,
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
            0.0,
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
            0.0,
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
    fn test_ecn_threshold_marks_and_drops() {
        let priorities = vec![1];

        let mut sp = SPServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::EcnThreshold,
            0.8,
            priorities,
        );

        for i in 0..8 {
            let packet = Packet::new(100, i, 0, 0.0);
            sp.on_packet_received(packet);
        }

        let mut ect_packet = Packet::new(100, 100, 0, 0.0);
        ect_packet.ecn = crate::flows::packet::EcnField::Ect0;
        sp.on_packet_received(ect_packet);

        assert_eq!(sp.queues[&1].len(), 9);
        let marked = sp.queues[&1]
            .iter()
            .find(|packet| packet.packet_id == 100)
            .expect("ECT packet should be enqueued");
        assert_eq!(marked.ecn, crate::flows::packet::EcnField::Ce);

        let non_ect_packet = Packet::new(100, 101, 0, 0.0);
        sp.on_packet_received(non_ect_packet);

        assert_eq!(sp.packets_dropped, 1);
        assert_eq!(sp.queues[&1].len(), 9);
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
            0.0,
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
            0.0,
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
            0.0,
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
            0.0,
            priorities,
        );

        // creates a packet larger than capacity
        let large_packet = Packet::new(1501, 1, 0, 0.0);
        sp.on_packet_received(large_packet);

        // verifies packet was dropped
        assert_eq!(sp.packets_dropped, 1);
        assert!(sp.queues.get(&1).is_none_or(|q| q.is_empty()));

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
            0.0,
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

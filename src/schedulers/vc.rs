//! Implements a Virtual Clock scheduler.
//!
//! Reference:
//!
//! L. Zhang, "Virtual Clock: A New Traffic Control Algorithm for Packet
//! Switching Networks," in ACM Computer Communication Review, vol. 20,
//! pp. 19, 1990.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
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

#[cfg(feature = "lean")]
use crate::schedulers::drop::DropDecision;
#[cfg(feature = "lean")]
use crate::utils::logger::{AqmEventKind, AqmEventRow, AqmLoggedEcnField};
use crate::utils::time::{quantize_after, quantize_time};

#[cfg(feature = "lean")]
fn to_ns(time_s: f64) -> u64 {
    (time_s.max(0.0) * 1e9).round() as u64
}

#[derive(Clone, Debug)]
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

    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

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

    /// Vector of vtick values (inverse of the desired rates for the corresponding
    /// flows, in bits per second) using the flow class as the index
    vticks: Vec<f64>,

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

    queue_state: Option<std::sync::Arc<QueueState>>,

    /// the statistics of a preiodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,

    time_packet_sent: f64,

    /// a vector of packets that have been sent out, only used for unit testing
    #[cfg(test)]
    sent_packets: Vec<TaggedPacket>,
}

impl VirtualClockServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        ecn_threshold: f64,
        vticks: Vec<f64>,
    ) -> VirtualClockServer {
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

        VirtualClockServer {
            scheduler_id,
            time: 0.0,
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
            queue_state: None,
            report_start_time: 0.0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
            time_packet_sent: 0.0,
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
        let queue_len = self.scheduler_queue.len();
        let byte_len: usize = self.byte_sizes.values().sum();
        let decision = self
            .drop_strategy
            .decision(packet.size, byte_len, queue_len);
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
                    "VirtualClockServer {} dropped packet {} from flow {} at time {:.3}",
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
                        "VirtualClockServer {} dropped non-ECT packet {} from flow {} at time {:.3}",
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

        // computes a virtual clock finish time and adds it as a tag to the
        // packet
        let tagged_packet = self.tag(packet.clone(), packet.time);
        let aux_vc = tagged_packet.tag;

        // pushes the packet into a min-heap according to the packet's virtual
        // clock finish time
        self.scheduler_queue.push(tagged_packet);

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
        let class_id = (self.flow_classes)(packet.flow_id);

        // upon receiving the first packet from this flow_class, sets its
        // virtual clock to the current real time
        let v_clock = self.v_clocks.entry(class_id).or_insert(arrival_time);

        // updates the virtual clock for the corresponding flow_class by
        // multiplying vtick (the desired bit time, i.e., the inverse of the
        // desired bits per second data rate) by the size of the packet in bits
        let vtick = self.vticks[class_id];
        *v_clock += vtick * packet.size as f64 * 8.0;

        let aux_vc = self.aux_vc.entry(class_id).or_insert(0.0);
        *aux_vc = arrival_time.max(*aux_vc);
        *aux_vc += vtick * packet.size as f64 * 8.0;

        TaggedPacket {
            packet,
            tag: *aux_vc,
        }
    }

    #[instrument(skip(self))]
    pub async fn send(&mut self, packet: Packet) {
        self.time = packet.time;
        self.update_stats_on_packet_forwarded(&packet);
        self.output.send(packet).await;
    }

    pub async fn send_and_run(&mut self, packet: Packet, cx: &mut Context<Self>) {
        self.send(packet).await;
        self.run(self.time, cx);
    }

    fn schedule_packet<F>(&mut self, mut schedule_event: F)
    where
        F: FnMut(f64, f64, TaggedPacket),
    {
        if !self.scheduler_queue.is_empty() {
            let mut tagged_outbound = self.scheduler_queue.pop().unwrap();

            let outbound = tagged_outbound.packet.clone();
            let class_id = (self.flow_classes)(outbound.flow_id);
            let flow_queue_count = self.flow_queue_count.entry(class_id).or_insert(0);
            *flow_queue_count -= 1;
            let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
            *byte_size -= outbound.size;

            tagged_outbound.packet.queueing_delay_update(self.time);

            // sends the packet out to the next element after a timeout
            let timeout = outbound.size as f64 * 8.0 / self.rate;
            let departure_time = quantize_after(self.time, timeout);
            tagged_outbound.packet.departure_update(departure_time);
            self.time_packet_sent = departure_time;

            // schedules the future send event with TaggedPacket to facilitate testing
            let delay = (departure_time - self.time).max(0.0);
            schedule_event(self.time, delay, tagged_outbound);

            self.busy_until = departure_time;

            debug!(
                "VirtualClockServer {} will send packet {} ({} bytes) from flow {} at time {:.8e}. \
                        {} packets in the queue.",
                self.scheduler_id,
                outbound.packet_id,
                outbound.size,
                outbound.flow_id,
                departure_time,
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

        let run_time = quantize_time(now);
        self.time = run_time;

        self.schedule_packet(|_now, delay, outbound| {
            cx.schedule_event(
                Duration::from_secs_f64(delay),
                Self::send_and_run,
                outbound.packet,
            )
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

            // updates statistics
            self.update_stats_on_packet_forwarded(&outbound.packet);

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
            "VirtualClockServer {} logged a periodic report at time {:.3}.",
            self.scheduler_id, now
        );

        self.reset_stats(now);
    }
}

impl ReportStatistics for VirtualClockServer {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::packet::Packet;
    use crate::schedulers::drop::{CapacityUnit, DropStrategy};
    use std::sync::Arc;

    #[test]
    fn test_single_packet() {
        let mut vc = VirtualClockServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1.0],
        );

        let packet = Packet::new(1024, 1, 0, 0.0);
        vc.on_packet_received(packet.clone());

        assert_eq!(vc.scheduler_queue.len(), 1);
        assert_eq!(vc.packets_received, 1);

        vc.test_run(0.0);
        assert!(vc.busy_until > 0.0);
    }

    #[test]
    fn test_multiple_flows_different_weights() {
        let flow_classes = Arc::new(|flow_id| flow_id % 2);
        let mut vc = VirtualClockServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 2.0, 3.0],
        );

        let packet1 = Packet::new(1024, 1, 0, 0.0); // flow_id 0, class 0
        let packet2 = Packet::new(1024, 2, 1, 0.0); // flow_id 1, class 1
        let packet3 = Packet::new(1024, 3, 2, 0.0); // flow_id 2, class 0

        vc.on_packet_received(packet1);
        vc.on_packet_received(packet2);
        vc.on_packet_received(packet3);

        vc.test_run(0.0);

        let sent_packet_ids: Vec<usize> =
            vc.sent_packets.iter().map(|p| p.packet.packet_id).collect();
        // Expect packets to be scheduled based on class weights
        assert_eq!(sent_packet_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_queue_overflow() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1e6,
            2,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0],
        );

        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 0, 0.0);
        let packet3 = Packet::new(1024, 3, 0, 0.0);

        vc.on_packet_received(packet1);
        vc.on_packet_received(packet2);
        vc.on_packet_received(packet3);

        assert_eq!(vc.packets_dropped, 1);
    }

    #[test]
    fn test_unlimited_capacity_queue() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1e6,
            0,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0],
        );

        let packet = Packet::new(1024, 1, 0, 0.0);
        vc.on_packet_received(packet);

        assert_eq!(vc.packets_dropped, 0);
    }

    #[test]
    fn test_large_packet_size() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1e6,
            1500,
            CapacityUnit::Bytes,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0],
        );

        let packet = Packet::new(1501, 1, 0, 0.0);
        vc.on_packet_received(packet);

        assert_eq!(vc.packets_dropped, 1);
    }

    #[test]
    fn test_packet_ordering_with_same_weights() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 1.0],
        );

        let packet1 = Packet::new(1024, 1, 0, 0.0);
        let packet2 = Packet::new(1024, 2, 1, 0.1);

        vc.on_packet_received(packet1);
        vc.on_packet_received(packet2);

        vc.test_run(0.0);

        let sent_packet_ids: Vec<usize> =
            vc.sent_packets.iter().map(|p| p.packet.packet_id).collect();
        assert_eq!(sent_packet_ids, vec![1, 2]);
    }

    #[test]
    fn test_packet_departure_time() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0],
        );

        let packet = Packet::new(1000, 1, 0, 0.0);
        vc.on_packet_received(packet);

        vc.test_run(0.0);

        let expected_transmission_time = (1000.0 * 8.0) / 1e6;
        assert!((vc.time_packet_sent - expected_transmission_time).abs() < 1e-6);
    }

    #[test]
    fn test_ecn_threshold_marks_and_drops() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::EcnThreshold,
            0.8,
            vec![1.0],
        );

        for i in 0..8 {
            let packet = Packet::new(100, i, 0, 0.0);
            vc.on_packet_received(packet);
        }

        let mut ect_packet = Packet::new(100, 100, 0, 0.0);
        ect_packet.ecn = crate::flows::packet::EcnField::Ect0;
        vc.on_packet_received(ect_packet);

        assert_eq!(vc.scheduler_queue.len(), 9);
        let marked = vc
            .scheduler_queue
            .iter()
            .find(|packet| packet.packet.packet_id == 100)
            .expect("ECT packet should be enqueued");
        assert_eq!(marked.packet.ecn, crate::flows::packet::EcnField::Ce);

        let non_ect_packet = Packet::new(100, 101, 0, 0.0);
        vc.on_packet_received(non_ect_packet);

        assert_eq!(vc.packets_dropped, 1);
        assert_eq!(vc.scheduler_queue.len(), 9);
    }

    #[test]
    fn test_flow_class_mapping() {
        let flow_classes = Arc::new(|flow_id| flow_id % 3);
        let mut vc = VirtualClockServer::new(
            1e6,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 2.0, 3.0],
        );

        let packet1 = Packet::new(1024, 1, 1, 0.0); // flow_id 1 -> class 1
        let packet2 = Packet::new(1024, 2, 2, 0.0); // flow_id 2 -> class 2
        let packet3 = Packet::new(1024, 3, 3, 0.0); // flow_id 3 -> class 3

        vc.on_packet_received(packet2);
        vc.on_packet_received(packet1);
        vc.on_packet_received(packet3);

        vc.test_run(0.0);

        let sent_packet_ids: Vec<usize> =
            vc.sent_packets.iter().map(|p| p.packet.packet_id).collect();
        assert_eq!(sent_packet_ids, vec![3, 1, 2]);
    }

    #[test]
    fn test_virtual_time_accuracy() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            100.0,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 2.0],
        );

        let packet1 = Packet::new(10, 1, 0, 0.0);
        let packet2 = Packet::new(10, 2, 1, 0.0);

        vc.on_packet_received(packet1);
        vc.on_packet_received(packet2);

        vc.test_run(0.0);

        println!("Packet 1 finish time: {:.3}", vc.sent_packets[0].tag);
        assert!((vc.sent_packets[0].tag - 80.0).abs() < 1e-6);
    }

    #[test]
    fn test_start_finish_times() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            100.0,
            10,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 1.0],
        );

        let packet1 = Packet::new(10, 1, 0, 0.0);
        let packet2 = Packet::new(10, 2, 1, 0.0);

        vc.on_packet_received(packet1);
        vc.on_packet_received(packet2);

        vc.test_run(0.0);

        let finish_times: Vec<f64> = vc.sent_packets.iter().map(|p| p.tag).collect();

        println!("Packet 1 finish time: {:.3}", finish_times[0]);
        println!("Packet 2 finish time: {:.3}", finish_times[1]);
        assert!((finish_times[0] - 80.0).abs() < 1e-6);
        assert!((finish_times[1] - 80.0).abs() < 1e-6);
    }

    #[test]
    fn test_flow_isolation() {
        let flow_classes = Arc::new(|flow_id| flow_id);
        let mut vc = VirtualClockServer::new(
            1000.0,
            100,
            CapacityUnit::Packets,
            flow_classes,
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 1.0],
        );

        for i in 0..50 {
            let packet1 = Packet::new(10, i * 2, 0, 0.0);
            let packet2 = Packet::new(10, i * 2 + 1, 1, 0.0);
            vc.on_packet_received(packet1);
            vc.on_packet_received(packet2);
        }

        vc.test_run(0.0);

        let flow0_packets = vc
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 0)
            .count();
        let flow1_packets = vc
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 1)
            .count();
        assert!((flow0_packets as i32 - flow1_packets as i32).abs() <= 1);
    }

    #[test]
    fn test_multiple_weight_ratios() {
        let mut vc = VirtualClockServer::new(
            1000.0, // 1 Mbps
            1000,   // Large queue capacity to prevent drops
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![4.0, 2.0, 1.0], // vticks for 1:2:4 weight ratio
        );

        // Send bursts of packets from each flow with different arrival patterns
        let mut arrival_time = 0.0;

        // First, create backlog for all flows
        for i in 0..100 {
            // Send more packets for flows with higher weights
            let packet0 = Packet::new(100, i * 3, 0, arrival_time);
            let packet1 = Packet::new(100, i * 3 + 1, 1, arrival_time);
            let packet2 = Packet::new(100, i * 3 + 2, 2, arrival_time);

            vc.on_packet_received(packet0);
            vc.on_packet_received(packet1);
            vc.on_packet_received(packet2);

            // Small time increment between bursts
            arrival_time += 0.0001;
        }

        // Run the scheduler for enough time to process packets
        vc.test_run(0.0);

        // Count bytes transmitted per flow
        let bytes_per_flow: Vec<usize> = (0..3)
            .map(|flow_id| {
                vc.sent_packets
                    .iter()
                    .take(40) // Only consider first 40 packets sent
                    .filter(|p| p.packet.flow_id == flow_id)
                    .map(|p| p.packet.size)
                    .sum()
            })
            .collect();

        println!("Flow 0 (weight 1): {} bytes", bytes_per_flow[0]);
        println!("Flow 1 (weight 2): {} bytes", bytes_per_flow[1]);
        println!("Flow 2 (weight 4): {} bytes", bytes_per_flow[2]);
        println!(
            "Ratio flow 1/flow 0: {}",
            bytes_per_flow[1] as f64 / bytes_per_flow[0] as f64
        );
        println!(
            "Ratio flow 2/flow 0: {}",
            bytes_per_flow[2] as f64 / bytes_per_flow[0] as f64
        );

        assert!((bytes_per_flow[1] as f64 / bytes_per_flow[0] as f64 - 2.0).abs() < 1.0);
        assert!((bytes_per_flow[2] as f64 / bytes_per_flow[0] as f64 - 4.0).abs() < 1.0);
    }

    #[test]
    fn test_dynamic_flows() {
        let mut vc = VirtualClockServer::new(
            1000.0,
            100,
            CapacityUnit::Packets,
            Arc::new(|flow_id| flow_id),
            DropStrategy::TailDrop,
            0.0,
            vec![1.0, 1.0],
        );

        for i in 0..10 {
            let packet = Packet::new(10, i, 0, 0.0);
            vc.on_packet_received(packet);
        }

        for i in 10..20 {
            let packet1 = Packet::new(10, i * 2, 0, 1.0);
            let packet2 = Packet::new(10, i * 2 + 1, 1, 1.0);
            vc.on_packet_received(packet1);
            vc.on_packet_received(packet2);
        }

        vc.test_run(0.0);

        let flow0_packets = vc
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 0)
            .count();
        let flow1_packets = vc
            .sent_packets
            .iter()
            .filter(|p| p.packet.flow_id == 1)
            .count();
        assert!((flow0_packets as isize - flow1_packets as isize).abs() <= 10);
    }
}

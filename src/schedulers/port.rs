//! Implements a First-In-First-Out (FIFO) scheduler with only one queue.

use std::collections::VecDeque;
use std::future::Future;
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
use crate::utils::logger::{AqmEventKind, AqmEventRow, AqmLoggedEcnField};
#[cfg(feature = "lean")]
use crate::schedulers::drop::DropDecision;
use crate::utils::time::{quantize_after, quantize_time};

#[cfg(feature = "lean")]
fn to_ns(time_s: f64) -> u64 {
    (time_s.max(0.0) * 1e9).round() as u64
}

pub struct Port {
    scheduler_id: usize,

    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

    /// the bit rate of the port (0 for unlimited)
    rate: f64,
    /// a closure that determines whether an inbound packet should be dropped or
    /// not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    /// the number of packets received
    packets_received: usize,
    /// the number of dropped packets
    packets_dropped: usize,
    /// the number of forwarded packets
    packets_forwarded: usize,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
    /// the server is considered busy sending the current packet until this time
    busy_until: f64,

    /// number of packets which have been dequeued for transmission but have not yet been forwarded
    /// (includes the packet currently being transmitted)
    in_flight: usize,

    pub output: Output<Packet>,

    queue_state: Option<std::sync::Arc<QueueState>>,

    /// the statistics of a preiodic report
    report_start_time: f64,
    queue_length: usize,
    received_sizes: usize,
    forwarded_sizes: usize,
    throughput_mean: f64,
    queueing_delay_mean: f64,
    run_batch_size: usize,

    /// a vector of packets that have been sent out, only used for unit testing
    #[cfg(test)]
    sent_packets: Vec<Packet>,
}

impl Port {
    const DEFAULT_RUN_BATCH_SIZE: usize = 64;

    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        drop_strategy: DropStrategy,
        ecn_threshold: f64,
        run_batch_size: Option<usize>,
    ) -> Port {
        let scheduler_id = next_scheduler_id();
        let ecn_threshold = if ecn_threshold > 0.0 {
            ecn_threshold
        } else {
            DEFAULT_ECN_THRESHOLD
        };
        let run_batch_size = run_batch_size
            .unwrap_or(Self::DEFAULT_RUN_BATCH_SIZE)
            .max(1);

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

        Port {
            scheduler_id,
            time: 0.0,
            rate,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            packets_forwarded: 0,
            queue: VecDeque::new(),
            busy_until: 0.0,
            in_flight: 0,
            output: Output::default(),
            queue_state: None,
            report_start_time: 0.0,
            queue_length: 0,
            received_sizes: 0,
            forwarded_sizes: 0,
            throughput_mean: 0.0,
            queueing_delay_mean: 0.0,
            run_batch_size,
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
            queue_id: 0,
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

    #[cfg(test)]
    pub fn on_packet_received(&mut self, packet: Packet) {
        let mut packet = packet;
        let queue_length_for_drop = self.queue.len() + self.in_flight.saturating_sub(1);
        let decision = self
            .drop_strategy
            .decision(packet.size, self.queue_length, queue_length_for_drop);
        let ecn_before = packet.ecn;
        let mut drop_action = decision.action;

        match drop_action {
            DropAction::Drop => {
                #[cfg(feature = "lean")]
                self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
                self.packets_dropped += 1;
                return;
            }
            DropAction::MarkEcn => {
                if !packet.mark_ce() {
                    drop_action = DropAction::Drop;
                    #[cfg(feature = "lean")]
                    self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
                    self.packets_dropped += 1;
                    return;
                }
                #[cfg(feature = "lean")]
                self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
            }
            DropAction::Enqueue => {
                #[cfg(feature = "lean")]
                self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
            }
        }

        self.update_stats_on_packet_received(&packet);
        self.queue.push_back(packet);
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

        let mut packet = packet;
        let queue_length_for_drop = self.queue.len() + self.in_flight.saturating_sub(1);
        let decision = self
            .drop_strategy
            .decision(packet.size, self.queue_length, queue_length_for_drop);
        let ecn_before = packet.ecn;
        let mut drop_action = decision.action;

        match drop_action {
            DropAction::Drop => {
                #[cfg(feature = "lean")]
                self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
                self.packets_dropped += 1;
                debug!(
                    "Port {} dropped packet {} from flow {} at time {:.8e}",
                    self.scheduler_id, packet.packet_id, packet.flow_id, packet.time
                );
                return;
            }
            DropAction::MarkEcn => {
                if !packet.mark_ce() {
                    drop_action = DropAction::Drop;
                    #[cfg(feature = "lean")]
                    self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
                    self.packets_dropped += 1;
                    debug!(
                        "Port {} dropped non-ECT packet {} from flow {} at time {:.8e}",
                        self.scheduler_id, packet.packet_id, packet.flow_id, packet.time
                    );
                    return;
                }
                #[cfg(feature = "lean")]
                self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
            }
            DropAction::Enqueue => {
                #[cfg(feature = "lean")]
                self.log_aqm_event(packet.time, &packet, drop_action, &decision, ecn_before);
            }
        }

        // the case that this packet will not be dropped
        self.update_stats_on_packet_received(&packet);

        debug!(
            "Port {} received packet {} ({} bytes) from flow {} at time {:.8e}. \
            {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            packet.time,
            self.queue.len()
        );

        let packet_time = packet.time;
        self.queue.push_back(packet);

        if packet_time >= self.busy_until && self.in_flight == 0 {
            self.run(packet_time, cx).await;
        }
    }

    #[instrument(skip(self))]
    pub async fn send(&mut self, packet: Packet) {
        self.time = packet.time;
        self.update_stats_on_packet_forwarded(&packet);
        self.output.send(packet).await;
    }

    pub async fn send_and_run(&mut self, packet: Packet, cx: &mut Context<Self>) {
        self.send_scheduled(packet, cx).await;
    }

    async fn send_scheduled(&mut self, packet: Packet, cx: &mut Context<Self>) {
        self.send(packet).await;

        self.in_flight = self.in_flight.saturating_sub(1);
        if self.in_flight == 0 {
            self.run(self.time, cx).await;
        }
    }

    fn packet_sent(&mut self, now: f64, packet: &Packet) {
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

    #[instrument(skip(self, cx))]
    pub fn run<'a>(
        &'a mut self,
        now: f64,
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            #[cfg(feature = "test")]
            {
                let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

                assert!(
                    (now - global_time).abs() <= 1e-7,
                    "Timing mismatch: now = {}, global_time = {}",
                    now,
                    global_time
                );
            }

            let run_time = quantize_time(now);
            self.time = run_time;

            if self.in_flight != 0 {
                return;
            }

            let mut schedule = Vec::with_capacity(self.run_batch_size);
            let mut service_start = run_time;

            for _ in 0..self.run_batch_size {
                let Some(mut packet) = self.queue.pop_front() else {
                    break;
                };

                packet.queueing_delay_update(service_start);
                let timeout = packet.size as f64 * 8.0 / self.rate;
                let departure_time = quantize_after(service_start, timeout);
                packet.departure_update(departure_time);

                self.packet_sent(departure_time, &packet);

                let delay = (departure_time - run_time).max(0.0);
                schedule.push((Duration::from_secs_f64(delay), packet));

                self.in_flight += 1;
                service_start = departure_time;
            }

            if !schedule.is_empty() {
                cx.schedule_event_batch(schedule, Self::send_scheduled)
                    .unwrap();
            }
        }
    }

    #[cfg(test)]
    pub fn test_run(&mut self, now: f64) {
        let run_time = quantize_time(now);
        self.time = run_time;

        let mut service_start = run_time;
        while let Some(mut packet) = self.queue.pop_front() {
            packet.queueing_delay_update(service_start);
            let timeout = packet.size as f64 * 8.0 / self.rate;
            let departure_time = quantize_after(service_start, timeout);
            packet.departure_update(departure_time);

            self.busy_until = departure_time;
            self.sent_packets.push(packet.clone());
            self.update_stats_on_packet_forwarded(&packet);
            service_start = departure_time;
        }
    }

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        let report = self.prepare_report(now);
        CsvLogger::log_report(Report::SchedulerReport(report), ReportTiming::InProgress);

        debug!(
            "Port {} logged a periodic report at time {:.3}.",
            self.scheduler_id, now
        );

        self.reset_stats(now);
    }
}

impl ReportStatistics for Port {
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

impl Model for Port {
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
    use crate::flows::packet::EcnField;

    #[test]
    fn test_fifo_ordering() {
        let mut port = Port::new(
            1e6,
            10,
            CapacityUnit::Packets,
            DropStrategy::TailDrop,
            0.0,
            None,
        );

        for i in 0..3 {
            let packet = Packet::new(100, i, 0, 0.0);
            port.on_packet_received(packet);
        }

        port.test_run(0.0);

        let sent_ids: Vec<usize> = port.sent_packets.iter().map(|p| p.packet_id).collect();
        assert_eq!(sent_ids, vec![0, 1, 2]);
    }

    #[test]
    fn test_queue_overflow() {
        let mut port = Port::new(
            1e6,
            2,
            CapacityUnit::Packets,
            DropStrategy::TailDrop,
            0.0,
            None,
        );

        for i in 0..3 {
            let packet = Packet::new(100, i, 0, 0.0);
            port.on_packet_received(packet);
        }

        assert_eq!(port.queue.len(), 2);
        assert_eq!(port.packets_dropped, 1);
    }

    #[test]
    fn test_ecn_threshold_marks_and_drops() {
        let mut port = Port::new(
            1e6,
            10,
            CapacityUnit::Packets,
            DropStrategy::EcnThreshold,
            0.8,
            None,
        );

        for i in 0..8 {
            let packet = Packet::new(100, i, 0, 0.0);
            port.on_packet_received(packet);
        }

        let mut ect_packet = Packet::new(100, 100, 0, 0.0);
        ect_packet.ecn = EcnField::Ect0;
        port.on_packet_received(ect_packet);

        assert_eq!(port.queue.len(), 9);
        let marked = port
            .queue
            .iter()
            .find(|packet| packet.packet_id == 100)
            .expect("ECT packet should be enqueued");
        assert_eq!(marked.ecn, EcnField::Ce);

        let non_ect_packet = Packet::new(100, 101, 0, 0.0);
        port.on_packet_received(non_ect_packet);

        assert_eq!(port.packets_dropped, 1);
        assert_eq!(port.queue.len(), 9);
    }
}

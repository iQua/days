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

pub struct Port {
    scheduler_id: usize,

    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,
    /// the current simulation time in integer nanoseconds
    time_ns: u64,

    /// the bit rate of the port in bps (0 for unlimited)
    rate_bps: u64,
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
    busy_until_ns: u64,

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
}

#[inline]
fn s_to_ns_round(t_s: f64) -> u64 {
    (t_s * 1e9).round().max(0.0) as u64
}

#[inline]
fn ns_to_s(t_ns: u64) -> f64 {
    (t_ns as f64) * 1e-9
}

impl Port {
    const DEFAULT_RUN_BATCH_SIZE: usize = 64;

    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        drop_strategy: DropStrategy,
        ecn_threshold: f64,
    ) -> Port {
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

        let rate_bps = if rate > 0.0 {
            rate.round() as u64
        } else {
            0
        };

        Port {
            scheduler_id,
            time: 0.0,
            time_ns: 0,
            rate_bps,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            packets_forwarded: 0,
            queue: VecDeque::new(),
            busy_until_ns: 0,
            in_flight: 0,
            output: Output::default(),
            queue_state: None,
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

    pub fn set_queue_state(&mut self, state: std::sync::Arc<QueueState>) {
        self.queue_state = Some(state);
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
        let drop_action =
            self.drop_strategy
                .action(packet.size, self.queue_length, queue_length_for_drop);

        match drop_action {
            DropAction::Drop => {
                self.packets_dropped += 1;
                debug!(
                    "Port {} dropped packet {} from flow {} at time {:.8e}",
                    self.scheduler_id, packet.packet_id, packet.flow_id, packet.time
                );
                return;
            }
            DropAction::MarkEcn => {
                if !packet.mark_ce() {
                    self.packets_dropped += 1;
                    debug!(
                        "Port {} dropped non-ECT packet {} from flow {} at time {:.8e}",
                        self.scheduler_id, packet.packet_id, packet.flow_id, packet.time
                    );
                    return;
                }
            }
            DropAction::Enqueue => {}
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
        let packet_time_ns = s_to_ns_round(packet_time);
        self.queue.push_back(packet);

        if packet_time_ns >= self.busy_until_ns && self.in_flight == 0 {
            self.run(packet_time, cx).await;
        }
    }

    #[instrument(skip(self))]
    pub async fn send(&mut self, packet: Packet) {
        self.time = packet.time;
        self.time_ns = s_to_ns_round(packet.time);
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

    fn packet_sent(&mut self, now_ns: u64, now_s: f64, packet: &Packet) {
        self.busy_until_ns = now_ns;

        debug!(
            "Port {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            now_s,
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

            self.time = now;
            let mut now_ns = s_to_ns_round(now);

            if self.time == 0.0 {
                let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
                self.time = global_time;
                now_ns = s_to_ns_round(global_time);
            }

            if self.time_ns == 0 || self.time_ns < now_ns {
                self.time_ns = now_ns;
            }

            if self.in_flight != 0 {
                return;
            }

            let mut schedule = Vec::with_capacity(Self::DEFAULT_RUN_BATCH_SIZE);
            let mut depart_ns = self.time_ns;

            for _ in 0..Self::DEFAULT_RUN_BATCH_SIZE {
                let Some(mut packet) = self.queue.pop_front() else {
                    break;
                };

                let start_s = ns_to_s(depart_ns);
                packet.queueing_delay_update(start_s);

                let tx_ns = if self.rate_bps == 0 {
                    0
                } else {
                    let bits = (packet.size as u128) * 8u128;
                    let numerator = bits.saturating_mul(1_000_000_000u128);
                    let tx_ns = (numerator + (self.rate_bps as u128) / 2)
                        / (self.rate_bps as u128);
                    tx_ns as u64
                };

                depart_ns = depart_ns.saturating_add(tx_ns);
                let depart_s = ns_to_s(depart_ns);
                packet.departure_update(depart_s);

                self.packet_sent(depart_ns, depart_s, &packet);

                let delay_ns = depart_ns.saturating_sub(self.time_ns);
                schedule.push((Duration::from_nanos(delay_ns), packet));

                self.in_flight += 1;
            }

            if !schedule.is_empty() {
                cx.schedule_event_batch(schedule, Self::send_scheduled)
                    .unwrap();
            }
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

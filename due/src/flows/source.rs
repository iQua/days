//! Implements a general packet source that provides interfaces of all kinds of
//! packet sources.

use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;
use rand::rngs::SmallRng;
use rand::SeedableRng;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};
use serde::Serialize;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::flow::FlowType;
use crate::flows::packet::Packet;
use crate::flows::tcp_source::TCPPacketSource;
use crate::flows::TrafficCharacteristics;
use crate::get_seed;
use crate::utils::logger::ReportLogger;
use crate::utils::progress::FinishMsg;

#[derive(Clone, Debug, Serialize)]
pub struct PacketSourceReport {
    pub id: usize,
    /// the start time of this report interval
    pub start_time: f64,
    /// the end time of this report interval
    pub end_time: f64,
    /// the number of sent packets in this report interval
    pub sent_packets: usize,
    /// the size of sent packets in this report interval
    pub packet_sizes: usize,
    /// the number of acknowledged bytes in this report interval
    pub ack_bytes: usize,
}

#[derive(Debug)]
pub enum PacketSource {
    DistPacketSource(DistPacketSource),
    TCPPacketSource(TCPPacketSource),
}

impl std::fmt::Display for PacketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PacketSource::DistPacketSource(_) => write!(f, "DistPacketSource {}", self.id()),
            PacketSource::TCPPacketSource(_) => write!(f, "TCPPacketSource {}", self.id()),
        }
    }
}

impl PacketSource {
    pub fn new(
        flow_id: usize,
        flow_type: FlowType,
        traffic: TrafficCharacteristics,
        seed: usize,
    ) -> Self {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        match flow_type {
            FlowType::PacketDistribution => {
                PacketSource::DistPacketSource(DistPacketSource::new(flow_id, traffic, rng))
            }
            FlowType::TCP => {
                PacketSource::TCPPacketSource(TCPPacketSource::new(flow_id, traffic, rng))
            }
        }
    }

    pub fn output(&mut self) -> &mut Output<Packet> {
        match self {
            PacketSource::DistPacketSource(source) => source.output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.output.borrow_mut(),
        }
    }

    pub fn finish_msg_output(&mut self) -> &mut Output<FinishMsg> {
        match self {
            PacketSource::DistPacketSource(source) => source.finish_msg_output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.finish_msg_output.borrow_mut(),
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.endpoint_id,
            PacketSource::TCPPacketSource(source) => source.endpoint_id,
        }
    }

    pub fn flow_id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.flow_id,
            PacketSource::TCPPacketSource(source) => source.flow_id,
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();
        match self {
            PacketSource::DistPacketSource(source) => source.packet_received(packet, now),
            PacketSource::TCPPacketSource(source) => {
                if source.ack_packet_received(packet, now).await {
                    self.run((), scheduler).await;
                }
            }
        }
    }

    fn prepare_run(&mut self, initial_delay: f64, scheduler: &Scheduler<Self>) {
        match self {
            PacketSource::DistPacketSource(source) => {
                source.report_start_time = initial_delay;
            }
            PacketSource::TCPPacketSource(source) => {
                source.report_start_time = initial_delay;

                // schedules a periodic timer to notify TCPPacketSource to
                // check if any of its sent packet reaches timeout

                // as suggested by RFC 6298, the clock granuarity, i.e., the
                // interval of this periodic timer, is always 100 msec
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(initial_delay + 0.1),
                        Self::periodic_timer_event,
                        (),
                    )
                    .unwrap();

                // lets AppDataSource to send data to TCPPacketSource
                let (data, interval) = source.datasource.produce_data(initial_delay);

                // TCPPacketSource now owns the data from the application
                source.send_buffer += data.size;
                source.busy_until = initial_delay;

                // schedules AppDataSource to send next data
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(initial_delay) + interval,
                        Self::fetch_app_data,
                        (),
                    )
                    .unwrap();
            }
        }
    }

    fn fetch_app_data<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match self {
                PacketSource::DistPacketSource(_) => (),
                PacketSource::TCPPacketSource(source) => {
                    let now = scheduler
                        .time()
                        .duration_since(MonotonicTime::EPOCH)
                        .as_secs_f64();

                    let (data, interval) = source.datasource.produce_data(now);

                    // TCPPacketSource now owns the data from the application
                    source.send_buffer += data.size;

                    if !source.datasource.traffic_exceeded(now) {
                        // schedules AppDataSource to send next data
                        scheduler
                            .schedule_event(interval, Self::fetch_app_data, ())
                            .unwrap();
                    } else {
                        source.traffic_exceeded = true;
                    }

                    if source.next_seq < source.send_buffer {
                        // the TCPPacketSource could send a new packet at this
                        // point, if the size of the congestion window
                        // allows
                        self.run((), scheduler).await;
                    } else {
                        // the TCPPacketSource is considered busy retrieving
                        // the next packet from the (application-layer) flow
                        source.busy_until = now + interval.as_secs_f64();
                    }
                }
            }
        }
    }

    fn periodic_timer_event<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match self {
                PacketSource::DistPacketSource(_) => (),
                PacketSource::TCPPacketSource(source) => {
                    let now = scheduler
                        .time()
                        .duration_since(MonotonicTime::EPOCH)
                        .as_secs_f64();

                    source.timer_tick(now).await;

                    // schedules the next periodic timer event
                    scheduler
                        .schedule_event(
                            Duration::from_secs_f64(0.05),
                            Self::periodic_timer_event,
                            (),
                        )
                        .unwrap();
                }
            }
        }
    }

    async fn send_packet(&mut self, scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        match self {
            PacketSource::DistPacketSource(source) => {
                let interval = source.send_packet(now).await;
                if !source.traffic_exceeded(now) {
                    scheduler.schedule_event(interval, Self::run, ()).unwrap();
                }
            }
            PacketSource::TCPPacketSource(source) => source.send_packet(now).await,
        }
    }

    fn log_report<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            if !self.stop_run(now) {
                match self {
                    PacketSource::DistPacketSource(source) => {
                        source.log_report(now);
                    }
                    PacketSource::TCPPacketSource(source) => {
                        source.log_report(now);
                    }
                };

                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(ReportLogger::get_report_interval()),
                        Self::log_report,
                        (),
                    )
                    .unwrap();
            }
        }
    }

    /// Returns whether PacketSource should stop running.
    fn stop_run(&self, now: f64) -> bool {
        match self {
            PacketSource::DistPacketSource(source) => source.traffic_exceeded(now),
            PacketSource::TCPPacketSource(source) => {
                source.traffic_exceeded
                    && source.next_seq + source.mss > source.send_buffer
                    && source.next_seq == source.last_ack
            }
        }
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            self.send_packet(scheduler).await;

            if self.stop_run(now) {
                let name = format!("{self}");

                if ReportLogger::get_report_interval() < f64::MAX {
                    match self {
                        PacketSource::DistPacketSource(source) => {
                            source.log_report(now);
                        }
                        PacketSource::TCPPacketSource(source) => {
                            source.log_report(now);
                        }
                    };
                }

                // notifies the Progress coroutine that the packet source
                // finished running
                self.finish_msg_output().send(FinishMsg {}).await;

                debug!("{} finished running at {:.3}.", name, now);
            }
        }
    }

    fn advance_initial_delay(&self) -> f64 {
        let initial_delay = match &self {
            PacketSource::DistPacketSource(source) => source.traffic.initial_delay,
            PacketSource::TCPPacketSource(source) => source.traffic.initial_delay,
        };

        debug!(
            "{} will be waiting for {:.3} sec(s) at the beginning.",
            format!("{self}"),
            initial_delay
        );

        initial_delay
    }
}

impl Model for PacketSource {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            let initial_delay = self.advance_initial_delay();

            self.prepare_run(initial_delay, scheduler);

            if initial_delay > 0.0 {
                scheduler
                    .schedule_event(Duration::from_secs_f64(initial_delay), Self::run, ())
                    .unwrap();
            } else {
                self.run((), scheduler).await;
            }

            let report_interval = ReportLogger::get_report_interval();
            if report_interval < f64::MAX {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(initial_delay + report_interval),
                        Self::log_report,
                        (),
                    )
                    .unwrap();
            }

            self.into()
        })
    }
}

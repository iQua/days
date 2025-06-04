//! Implements a general packet source that provides interfaces of all kinds of
//! packet sources.

use crate::flows::app_source::AppSourceHandle;
use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use log::debug;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use tracing::instrument;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::Serialize;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::flow::FlowType;
use crate::flows::packet::Packet;
use crate::flows::tcp_source::TCPPacketSource;
use crate::flows::{FlowFinishMsg, TrafficCharacteristics};
use crate::get_seed;
use crate::utils::logger::{CsvLogger, ReportTiming};

#[derive(Clone, Default, Debug, Serialize)]
pub struct PacketSourceReport {
    pub id: usize,
    pub flow_id: usize,
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
        flow_start_after: Vec<usize>,
        flow_type: FlowType,
        traffic: TrafficCharacteristics,
        seed: usize,
        app_source: Option<AppSourceHandle>,
    ) -> Self {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_os_rng(),
        };

        match flow_type {
            FlowType::PacketDistribution => PacketSource::DistPacketSource(DistPacketSource::new(
                flow_id,
                flow_start_after,
                traffic,
                rng,
            )),
            FlowType::TCP => {
                // let handle = app_source.expect("TCP flow must provide AppSourceHandle");
                PacketSource::TCPPacketSource(TCPPacketSource::new(
                    flow_id,
                    flow_start_after,
                    traffic,
                    rng,
                    app_source,
                ))
            }
        }
    }

    pub fn output(&mut self) -> &mut Output<Packet> {
        match self {
            PacketSource::DistPacketSource(source) => source.output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.output.borrow_mut(),
        }
    }

    pub fn connect_flow_finish_output(&mut self, flow_finish_output: Output<FlowFinishMsg>) {
        match self {
            PacketSource::DistPacketSource(_) => {}
            PacketSource::TCPPacketSource(source) => {
                source.flow_finish_outputs.push(flow_finish_output);
            }
        }
    }

    pub fn ui_output(&mut self) -> &mut Output<FlowFinishMsg> {
        match self {
            PacketSource::DistPacketSource(source) => source.ui_output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.ui_output.borrow_mut(),
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

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        #[cfg(test)]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            let local_time = match self {
                PacketSource::DistPacketSource(source) => source.time,
                PacketSource::TCPPacketSource(source) => source.time,
            };

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                (packet.time - global_time).abs() <= 1e-7,
                "Timing mismatch: packet.time = {}, global_time = {}",
                packet.time,
                global_time
            );

            // makes sure that the simulation advances in time
            assert!((packet.time - local_time).abs() <= 1e-7 || packet.time > local_time);
        }

        let now = packet.time;

        match self {
            PacketSource::DistPacketSource(source) => source.packet_received(packet, now),
            PacketSource::TCPPacketSource(source) => {
                if source.ack_packet_received(packet, now).await {
                    self.run((), cx).await;
                }
            }
        }
    }

    async fn prepare_run(&mut self, now: f64, initial_delay: f64, cx: &Context<Self>) {
        match self {
            PacketSource::DistPacketSource(source) => {
                source.report_start_time = now + initial_delay;
                source.flow_start_time = now + initial_delay;
            }
            PacketSource::TCPPacketSource(source) => {
                source.report_start_time = now + initial_delay;

                // schedules a periodic timer to notify TCPPacketSource to
                // check if any of its sent packet reaches timeout

                // as suggested by RFC 6298, the clock granuarity, i.e., the
                // interval of this periodic timer, is always 100 msec
                cx.schedule_periodic_event(
                    Duration::from_secs_f64(initial_delay + 0.1),
                    Duration::from_secs_f64(0.1),
                    Self::periodic_timer_event,
                    (),
                )
                .unwrap();

                // TCPPacketSource now owns the data from the application
                source.busy_until = now + initial_delay;
                let cwnd_limit = source.get_cwnd_limit();
                // On flow start, proactively pull packets from the AppSourceHandle
                // according to current congestion window size (cwnd).
                // This populates the initial packets to be sent as soon as allowed.
                source
                    .try_pull_from_appsource(now + initial_delay, cwnd_limit)
                    .await;
            }
        }
    }

    #[instrument(skip(self, cx))]
    fn fetch_app_data<'a>(
        &'a mut self,
        current_time: f64,
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match self {
                PacketSource::DistPacketSource(source) => source.time = current_time,
                PacketSource::TCPPacketSource(source) => {
                    source.time = current_time;

                    #[cfg(test)]
                    {
                        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
                        assert!((now - source.time).abs() <= 1e-7);
                    }

                    if source.next_seq < source.send_buffer {
                        // the TCPPacketSource could send a new packet at this
                        // point, if the size of the congestion window
                        // allows
                        self.run((), cx).await;
                    }
                }
            }
        }
    }
    async fn periodic_timer_event<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        match self {
            PacketSource::DistPacketSource(_) => (),
            PacketSource::TCPPacketSource(source) => {
                let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
                source.time = now;
                source.timer_tick(now).await;
            }
        }
    }

    async fn send_packet(&mut self, cx: &Context<Self>, now: f64) {
        match self {
            PacketSource::DistPacketSource(source) => {
                if !source.traffic_exceeded(now) {
                    let interval = source.send_packet(now).await;
                    // updates the locally maintained simulation time
                    source.time = now + interval;
                    // schedules the next packet to be sent
                    cx.schedule_event(Duration::from_secs_f64(interval), Self::run, ())
                        .unwrap();
                }
            }
            PacketSource::TCPPacketSource(source) => source.send_packet(now).await,
        }
    }

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        match self {
            PacketSource::DistPacketSource(source) => {
                source.log_report(now, ReportTiming::InProgress);
            }
            PacketSource::TCPPacketSource(source) => {
                source.log_report(now, ReportTiming::InProgress);
            }
        };
    }

    /// Returns whether PacketSource should stop running.
    async fn stop_run(&mut self, now: f64) -> bool {
        match self {
            PacketSource::DistPacketSource(source) => source.traffic_exceeded(now),
            PacketSource::TCPPacketSource(source) => {
                if source.traffic_exceeded
                    && source.next_seq + source.mss > source.send_buffer
                    && source.next_seq == source.last_ack
                {
                    source.wrap_up(now).await;
                    return true;
                }
                false
            }
        }
    }

    #[instrument(skip(self, cx))]
    pub fn run<'a>(
        &'a mut self,
        _: (),
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            #[cfg(feature = "test")]
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            // retrieves the current simulation time from the locally stored simulation time
            let mut now = match self {
                PacketSource::DistPacketSource(source) => source.time,
                PacketSource::TCPPacketSource(source) => source.time,
            };

            if now == 0.0 {
                let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

                // updates the locally maintained simulation time
                match self {
                    PacketSource::DistPacketSource(source) => {
                        source.time = global_time;
                    }
                    PacketSource::TCPPacketSource(source) => {
                        source.time = global_time;
                    }
                };

                now = global_time;
            }

            #[cfg(feature = "test")]
            assert!(
                (now - global_time).abs() <= 1e-7,
                "Timing mismatch: now = {}, global_time = {}",
                now,
                global_time
            );

            self.send_packet(cx, now).await;

            if self.stop_run(now).await {
                let name = format!("{self}");

                // logs the final report
                match self {
                    PacketSource::DistPacketSource(source) => {
                        source.log_report(now, ReportTiming::Final);
                    }
                    PacketSource::TCPPacketSource(source) => {
                        source.log_report(now, ReportTiming::Final);
                    }
                };

                // notifies the Progress coroutine that the packet source finished running
                self.ui_output().send(FlowFinishMsg { flow_id: 0 }).await;

                debug!("{} finished running at {:.3}.", name, now);
            }
        }
    }

    pub async fn flow_finished(&mut self, flow_finish_msg: FlowFinishMsg, cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        debug!(
            "{} of flow {} received notification that flow {} ended at time {:.3}.",
            format!("{self}"),
            self.flow_id(),
            flow_finish_msg.flow_id,
            now
        );

        match self {
            PacketSource::DistPacketSource(source) => {
                source.flow_start_after.remove(&flow_finish_msg.flow_id);

                if source.flow_start_after.is_empty() {
                    self.prepare_run(now, 0.0, cx).await;
                    self.run((), cx).await;
                    self.start_report_logger(0.0, cx);

                    debug!(
                        "{} of flow {} started sending packets at time {:.3}.",
                        format!("{self}"),
                        self.flow_id(),
                        now
                    );
                } else {
                    debug!(
                        "Flow {} still waits for {} flow(s) before it can start.",
                        source.flow_id,
                        source.flow_start_after.len()
                    );
                }
            }
            PacketSource::TCPPacketSource(source) => {
                source.flow_start_after.remove(&flow_finish_msg.flow_id);
                debug!(
                    "Flow {} still waits for {} flow(s) before it can start.",
                    source.flow_id,
                    source.flow_start_after.len()
                );

                if source.flow_start_after.is_empty() {
                    self.prepare_run(now, 0.0, cx).await;
                    self.run((), cx).await;
                    self.start_report_logger(0.0, cx);

                    debug!(
                        "{} of flow {} started sending packets at time {:.3}.",
                        format!("{self}"),
                        self.flow_id(),
                        now
                    );
                }
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

    /// Returns whether PacketSource should start now or wait for other flows to
    /// end due to dependencies.
    fn start_now(&self) -> bool {
        match self {
            PacketSource::DistPacketSource(source) => {
                if source.flow_start_after.is_empty() {
                    return true;
                }
                false
            }
            PacketSource::TCPPacketSource(source) => {
                if source.flow_start_after.is_empty() {
                    return true;
                }
                false
            }
        }
    }

    fn start_report_logger(&self, initial_delay: f64, cx: &mut Context<Self>) {
        let report_interval = CsvLogger::get_instance().get_report_interval();
        if report_interval < f64::MAX {
            cx.schedule_periodic_event(
                Duration::from_secs_f64(initial_delay + report_interval),
                Duration::from_secs_f64(report_interval),
                Self::log_report,
                (),
            )
            .unwrap();
        }
    }
}

impl Model for PacketSource {
    async fn init(mut self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        if self.start_now() {
            let initial_delay = self.advance_initial_delay();
            self.prepare_run(0.0, initial_delay, cx).await;

            if initial_delay > 0.0 {
                cx.schedule_event(Duration::from_secs_f64(initial_delay), Self::run, ())
                    .unwrap();
            } else {
                self.run((), cx).await;
            }

            self.start_report_logger(initial_delay, cx);
        }

        self.into()
    }
}

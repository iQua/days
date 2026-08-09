//! Implements a general packet source that provides interfaces of all kinds of
//! packet sources.

use std::borrow::BorrowMut;
use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

pub use days::utils::logger::PacketSourceReport;
use log::debug;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use tracing::instrument;

use crate::flows::app_source::AppSourceBufferHandle;
#[cfg(feature = "dcqcn")]
use crate::flows::dcqcn_source::DcqcnPacketSource;
use crate::flows::dist_source::DistPacketSource;
use crate::flows::flow::FlowType;
use crate::flows::packet::Packet;
use crate::flows::tcp_source::TCPPacketSource;
use crate::flows::{FlowFinishMsg, TrafficCharacteristics};
use crate::get_seed;
use crate::utils::exact_time::{behavior_delay_ns, clock_ns, scenario_seconds_ns, seconds_view};
use crate::utils::logger::{CsvLogger, ReportTiming};
use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;

#[derive(Debug)]
pub enum PacketSource {
    DistPacketSource(Box<DistPacketSource>),
    TCPPacketSource(Box<TCPPacketSource>),
    #[cfg(feature = "dcqcn")]
    DcqcnPacketSource(Box<DcqcnPacketSource>),
}

impl std::fmt::Display for PacketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PacketSource::DistPacketSource(_) => write!(f, "DistPacketSource {}", self.id()),
            PacketSource::TCPPacketSource(_) => write!(f, "TCPPacketSource {}", self.id()),
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(_) => write!(f, "DCQCNPacketSource {}", self.id()),
        }
    }
}

impl PacketSource {
    const RUN_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);
    const FETCH_APP_DATA_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(1);
    const PERIODIC_TIMER_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(2);
    const LOG_REPORT_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(3);

    pub fn new(
        flow_id: usize,
        flow_start_after: Vec<usize>,
        flow_type: FlowType,
        traffic: TrafficCharacteristics,
        priority: u8,
        seed: usize,
        app_source: Option<AppSourceBufferHandle>,
    ) -> Self {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => {
                let mut rng = rand::rng();
                SmallRng::from_rng(&mut rng)
            }
        };

        match flow_type {
            FlowType::PacketDistribution => PacketSource::DistPacketSource(Box::new(
                DistPacketSource::new(flow_id, flow_start_after, traffic, priority, rng),
            )),
            FlowType::TCP => PacketSource::TCPPacketSource(Box::new(TCPPacketSource::new(
                flow_id,
                flow_start_after,
                traffic,
                priority,
                app_source,
                rng,
            ))),
            #[cfg(feature = "dcqcn")]
            FlowType::DCQCN => PacketSource::DcqcnPacketSource(Box::new(DcqcnPacketSource::new(
                flow_id,
                flow_start_after,
                traffic,
                priority,
                rng,
            ))),
        }
    }

    pub fn output(&mut self) -> &mut Output<Packet> {
        match self {
            PacketSource::DistPacketSource(source) => source.output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.output.borrow_mut(),
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => source.output.borrow_mut(),
        }
    }

    pub fn connect_flow_finish_output(&mut self, flow_finish_output: Output<FlowFinishMsg>) {
        match self {
            PacketSource::DistPacketSource(_) => {}
            PacketSource::TCPPacketSource(source) => {
                source.flow_finish_outputs.push(flow_finish_output);
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                source.flow_finish_outputs.push(flow_finish_output);
            }
        }
    }

    pub fn ui_output(&mut self) -> &mut Output<FlowFinishMsg> {
        match self {
            PacketSource::DistPacketSource(source) => source.ui_output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.ui_output.borrow_mut(),
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => source.ui_output.borrow_mut(),
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.endpoint_id,
            PacketSource::TCPPacketSource(source) => source.endpoint_id,
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => source.endpoint_id,
        }
    }

    pub fn flow_id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.flow_id,
            PacketSource::TCPPacketSource(source) => source.flow_id,
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => source.flow_id,
        }
    }

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, mut packet: Packet, cx: &Context<Self>) {
        let now_ns = clock_ns(cx.time());
        let now = seconds_view(now_ns);
        #[cfg(test)]
        {
            let global_time = now;

            let local_time = match self {
                PacketSource::DistPacketSource(source) => source.time,
                PacketSource::TCPPacketSource(source) => source.time,
                #[cfg(feature = "dcqcn")]
                PacketSource::DcqcnPacketSource(source) => source.time,
            };

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                packet.time <= global_time + 1e-7,
                "Timing mismatch: packet.time = {}, global_time = {}",
                packet.time,
                global_time
            );

            // makes sure that the simulation advances in time
            assert!((global_time - local_time).abs() <= 1e-7 || global_time > local_time);
        }

        packet.departure_update(now);

        match self {
            PacketSource::DistPacketSource(source) => source.packet_received(packet, now),
            PacketSource::TCPPacketSource(source) => {
                if source.ack_packet_received(packet, now_ns).await {
                    self.run((), cx).await;
                }
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                source.packet_received(packet, now);
            }
        }
    }

    async fn prepare_run(&mut self, now_ns: u64, initial_delay_ns: u64, cx: &Context<Self>) {
        let start_ns = now_ns
            .checked_add(initial_delay_ns)
            .expect("source start time exceeds the u64 nanosecond clock range");
        let start_time = seconds_view(start_ns);
        match self {
            PacketSource::DistPacketSource(source) => {
                source.report_start_time = start_time;
                source.flow_start_time = start_time;
            }
            PacketSource::TCPPacketSource(source) => {
                source.report_start_time = start_time;
                source.time = start_time;

                // schedules a periodic timer to notify TCPPacketSource to
                // check if any of its sent packet reaches timeout

                // as suggested by RFC 6298, the clock granuarity, i.e., the
                // interval of this periodic timer, is always 100 msec
                let timer_interval = Duration::from_millis(100);
                let timer_start_ns = initial_delay_ns
                    .checked_add(100_000_000)
                    .expect("TCP timer start exceeds the u64 nanosecond clock range");
                cx.schedule_periodic_event(
                    Duration::from_nanos(timer_start_ns),
                    timer_interval,
                    &Self::PERIODIC_TIMER_SID,
                    (),
                )
                .unwrap();

                // TCPPacketSource now owns the data from the application
                source.busy_until_ns = start_ns;

                if source.app_source.is_some() {
                    // On flow start, proactively pull packets from the AppSourceBufferHandle according to the current
                    // congestion window size (cwnd). This populates the initial packets to be sent as soon as
                    // they are allowed.
                    source.pull_from_appsource(start_time).await;
                } else if source.has_synthetic_source() {
                    let (size, interval) = {
                        let fallback = source
                            .synthetic_source_mut()
                            .expect("fallback source missing");
                        fallback.set_flow_start_time(start_time);
                        let (data, interval) = fallback.produce_data(start_time);
                        (data.size, interval)
                    };

                    source.send_buffer += size;
                    source.busy_until_ns = start_ns;

                    let delay_ns = behavior_delay_ns(interval, "TCP application arrival sample")
                        .expect("TCP arrival distribution must produce a positive finite delay");
                    cx.schedule_event_fast(
                        Duration::from_nanos(delay_ns),
                        &Self::FETCH_APP_DATA_SID,
                        Self::fetch_app_data,
                        (),
                    )
                    .unwrap();
                }
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                source.report_start_time = start_time;
                source.flow_start_time = start_time;
                source.time = start_time;

                let interval = source.timer_interval();
                let interval_ns = behavior_delay_ns(interval, "DCQCN timer interval")
                    .expect("DCQCN timer interval must be finite and positive");
                let timer_start_ns = initial_delay_ns
                    .checked_add(interval_ns)
                    .expect("DCQCN timer start exceeds the u64 nanosecond clock range");
                cx.schedule_periodic_event(
                    Duration::from_nanos(timer_start_ns),
                    Duration::from_nanos(interval_ns),
                    &Self::PERIODIC_TIMER_SID,
                    (),
                )
                .unwrap();
            }
        }
    }

    #[instrument(skip(self, cx))]
    fn fetch_app_data<'a>(
        &'a mut self,
        _: (),
        cx: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let current_ns = clock_ns(cx.time());
            let current_time = seconds_view(current_ns);
            match self {
                PacketSource::DistPacketSource(source) => source.time = current_time,
                PacketSource::TCPPacketSource(source) => {
                    source.time = current_time;

                    #[cfg(test)]
                    {
                        let now = seconds_view(clock_ns(cx.time()));
                        assert!((now - source.time).abs() <= 1e-7);
                    }

                    if source.has_synthetic_source() {
                        let timestamp = source.time;
                        let (size, interval, exceeded) = {
                            let fallback = source
                                .synthetic_source_mut()
                                .expect("fallback source missing");
                            let (data, interval) = fallback.produce_data(timestamp);
                            let exceeded = fallback.traffic_exceeded(timestamp);
                            (data.size, interval, exceeded)
                        };

                        if !exceeded {
                            source.send_buffer += size;
                            let delay_ns = behavior_delay_ns(
                                interval,
                                "TCP application arrival sample",
                            )
                            .expect(
                                "TCP arrival distribution must produce a positive finite delay",
                            );
                            cx.schedule_event_fast(
                                Duration::from_nanos(delay_ns),
                                &Self::FETCH_APP_DATA_SID,
                                Self::fetch_app_data,
                                (),
                            )
                            .unwrap();
                        } else {
                            source.traffic_exceeded = true;
                        }

                        if source.next_seq < source.send_buffer {
                            self.run((), cx).await;
                        } else {
                            let delay_ns = behavior_delay_ns(
                                interval,
                                "TCP application arrival sample",
                            )
                            .expect(
                                "TCP arrival distribution must produce a positive finite delay",
                            );
                            source.busy_until_ns = current_ns
                                .checked_add(delay_ns)
                                .expect("TCP application arrival exceeds the u64 nanosecond range");
                        }
                    } else if source.next_seq < source.send_buffer {
                        // For handle-backed sources, simply resume sending if there is pending data.
                        self.run((), cx).await;
                    }
                }
                #[cfg(feature = "dcqcn")]
                PacketSource::DcqcnPacketSource(source) => {
                    source.time = current_time;
                }
            }
        }
    }
    async fn periodic_timer_event<'a>(&'a mut self, _: (), cx: &'a Context<Self>) {
        let now_ns = clock_ns(cx.time());
        let now = seconds_view(now_ns);
        match self {
            PacketSource::DistPacketSource(_) => (),
            PacketSource::TCPPacketSource(source) => {
                source.time = now;
                source.timer_tick(now_ns).await;
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                source.timer_tick(now);
            }
        }
    }

    async fn send_packet(&mut self, cx: &Context<Self>, now_ns: u64) {
        let now = seconds_view(now_ns);
        match self {
            PacketSource::DistPacketSource(source) => {
                if !source.traffic_exceeded(now) {
                    let interval = source.send_packet(now).await;
                    // updates the locally maintained simulation time
                    let delay_ns = behavior_delay_ns(interval, "packet arrival sample")
                        .expect("arrival distribution must produce a positive finite delay");
                    let next_ns = now_ns
                        .checked_add(delay_ns)
                        .expect("packet arrival exceeds the u64 nanosecond clock range");
                    source.time = seconds_view(next_ns);
                    // schedules the next packet to be sent
                    cx.schedule_event_fast(
                        Duration::from_nanos(delay_ns),
                        &Self::RUN_SID,
                        Self::run,
                        (),
                    )
                    .unwrap();
                }
            }
            PacketSource::TCPPacketSource(source) => {
                if let Some(interval) = source.send_packet(now_ns).await {
                    if interval > 0.0 {
                        let delay_ns = behavior_delay_ns(interval, "TCP pacing interval")
                            .expect("TCP pacing must produce a positive finite delay");
                        let next_ns = now_ns
                            .checked_add(delay_ns)
                            .expect("TCP pacing exceeds the u64 nanosecond clock range");
                        source.time = seconds_view(next_ns);
                        cx.schedule_event_fast(
                            Duration::from_nanos(delay_ns),
                            &Self::RUN_SID,
                            Self::run,
                            (),
                        )
                        .unwrap();
                    }
                }
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                if let Some(interval) = source.send_packet(now).await {
                    if interval > 0.0 {
                        let delay_ns = behavior_delay_ns(interval, "DCQCN pacing interval")
                            .expect("DCQCN pacing must produce a positive finite delay");
                        let next_ns = now_ns
                            .checked_add(delay_ns)
                            .expect("DCQCN pacing exceeds the u64 nanosecond clock range");
                        source.time = seconds_view(next_ns);
                        cx.schedule_event_fast(
                            Duration::from_nanos(delay_ns),
                            &Self::RUN_SID,
                            Self::run,
                            (),
                        )
                        .unwrap();
                    }
                }
            }
        }
    }

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a Context<Self>) {
        let now = seconds_view(clock_ns(cx.time()));

        match self {
            PacketSource::DistPacketSource(source) => {
                source.log_report(now, ReportTiming::InProgress);
            }
            PacketSource::TCPPacketSource(source) => {
                source.log_report(now, ReportTiming::InProgress);
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
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
                    && source.next_seq >= source.send_buffer
                    && source.next_seq == source.last_ack
                {
                    source.wrap_up(now).await;
                    return true;
                }
                false
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                if source.traffic_exceeded(now) {
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
        cx: &'a Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now_ns = clock_ns(cx.time());
            let now = seconds_view(now_ns);
            match self {
                PacketSource::DistPacketSource(source) => source.time = now,
                PacketSource::TCPPacketSource(source) => source.time = now,
                #[cfg(feature = "dcqcn")]
                PacketSource::DcqcnPacketSource(source) => source.time = now,
            }

            #[cfg(feature = "test")]
            {
                let global_time = seconds_view(clock_ns(cx.time()));
                assert!(
                    now <= global_time + 1e-7,
                    "Timing mismatch: now = {}, global_time = {}",
                    now,
                    global_time
                );
            }

            self.send_packet(cx, now_ns).await;

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
                    #[cfg(feature = "dcqcn")]
                    PacketSource::DcqcnPacketSource(source) => {
                        source.log_report(now, ReportTiming::Final);
                    }
                };

                // notifies the Progress coroutine that the packet source finished running
                let flow_id = self.flow_id();
                self.ui_output().send(FlowFinishMsg { flow_id }).await;

                debug!("{} finished running at {:.3}.", name, now);
            }
        }
    }

    pub async fn flow_finished(&mut self, flow_finish_msg: FlowFinishMsg, cx: &Context<Self>) {
        let now_ns = clock_ns(cx.time());
        let now = seconds_view(now_ns);

        debug!(
            "{} of flow {} received notification that flow {} ended at time {:.3}.",
            self,
            self.flow_id(),
            flow_finish_msg.flow_id,
            now
        );

        match self {
            PacketSource::DistPacketSource(source) => {
                source.flow_start_after.remove(&flow_finish_msg.flow_id);

                if source.flow_start_after.is_empty() {
                    self.prepare_run(now_ns, 0, cx).await;
                    self.run((), cx).await;
                    self.start_report_logger(0, cx);

                    debug!(
                        "{} of flow {} started sending packets at time {:.3}.",
                        self,
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
                    self.prepare_run(now_ns, 0, cx).await;
                    self.run((), cx).await;
                    self.start_report_logger(0, cx);

                    debug!(
                        "{} of flow {} started sending packets at time {:.3}.",
                        self,
                        self.flow_id(),
                        now
                    );
                }
            }
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                source.flow_start_after.remove(&flow_finish_msg.flow_id);
                debug!(
                    "Flow {} still waits for {} flow(s) before it can start.",
                    source.flow_id,
                    source.flow_start_after.len()
                );

                if source.flow_start_after.is_empty() {
                    self.prepare_run(now_ns, 0, cx).await;
                    self.run((), cx).await;
                    self.start_report_logger(0, cx);

                    debug!(
                        "{} of flow {} started sending packets at time {:.3}.",
                        self,
                        self.flow_id(),
                        now
                    );
                }
            }
        }
    }

    fn advance_initial_delay(&self) -> u64 {
        let initial_delay = match &self {
            PacketSource::DistPacketSource(source) => source.traffic.initial_delay,
            PacketSource::TCPPacketSource(source) => source.traffic.initial_delay,
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => source.traffic.initial_delay,
        };

        debug!(
            "{} will be waiting for {:.3} sec(s) at the beginning.",
            self, initial_delay
        );

        scenario_seconds_ns(initial_delay, "flow initial delay")
            .expect("flow initial delay must use exact integer nanoseconds")
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
            #[cfg(feature = "dcqcn")]
            PacketSource::DcqcnPacketSource(source) => {
                if source.flow_start_after.is_empty() {
                    return true;
                }
                false
            }
        }
    }

    fn start_report_logger(&self, initial_delay_ns: u64, cx: &Context<Self>) {
        let report_interval = CsvLogger::get_instance().get_report_interval();
        if report_interval < f64::MAX {
            let report_interval_ns = scenario_seconds_ns(report_interval, "source report interval")
                .expect("source report interval must use exact integer nanoseconds");
            let first_report_ns = initial_delay_ns
                .checked_add(report_interval_ns)
                .expect("source report start exceeds the u64 nanosecond clock range");
            cx.schedule_periodic_event(
                Duration::from_nanos(first_report_ns),
                Duration::from_nanos(report_interval_ns),
                &Self::LOG_REPORT_SID,
                (),
            )
            .unwrap();
        }
    }
}

impl Model for PacketSource {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::run));
        registry.add(cx.register_schedulable(Self::fetch_app_data));
        registry.add(cx.register_schedulable(Self::periodic_timer_event));
        registry.add(cx.register_schedulable(Self::log_report));
        registry
    }

    async fn init(mut self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        if self.start_now() {
            let initial_delay = self.advance_initial_delay();
            self.prepare_run(0, initial_delay, cx).await;

            if initial_delay > 0 {
                cx.schedule_event_fast(
                    Duration::from_nanos(initial_delay),
                    &Self::RUN_SID,
                    Self::run,
                    (),
                )
                .unwrap();
            } else {
                self.run((), cx).await;
            }

            self.start_report_logger(initial_delay, cx);
        }

        self.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::cc::CCAlgorithm;
    use crate::flows::flow::FlowType;
    use crate::flows::{DistributionInfo, TCPCharacteristics};
    use futures::executor::block_on;

    fn make_tcp_packet_source() -> PacketSource {
        let traffic = TrafficCharacteristics::new(
            0.0,
            Some(1.0),
            None,
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            Some(TCPCharacteristics {
                cc_algorithm: CCAlgorithm::TCPReno,
                ecn: false,
                cubic: None,
            }),
        );

        PacketSource::new(0, Vec::new(), FlowType::TCP, traffic, 0, 0, None)
    }

    #[test]
    fn test_tcp_stop_run_waits_for_sub_mss_segment_to_be_sent() {
        let mut source = make_tcp_packet_source();
        let PacketSource::TCPPacketSource(tcp) = &mut source else {
            panic!("expected TCP packet source");
        };
        tcp.send_buffer = 128;
        tcp.traffic_exceeded = true;

        assert!(!block_on(source.stop_run(0.0)));
    }

    #[test]
    fn test_tcp_stop_run_finishes_after_all_buffered_bytes_are_acked() {
        let mut source = make_tcp_packet_source();
        let PacketSource::TCPPacketSource(tcp) = &mut source else {
            panic!("expected TCP packet source");
        };
        tcp.send_buffer = 128;
        tcp.next_seq = 128;
        tcp.last_ack = 128;
        tcp.traffic_exceeded = true;

        assert!(block_on(source.stop_run(0.0)));
    }
}

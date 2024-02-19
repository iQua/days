//! Implements a packet sink, designed to compute vital statistics from incoming
//! packets.

//! The packet sink records a variety of statistics, including absolute arrival
//! times, inter-arrival times, the total number of packets and bytes received,
//! the one-way end-to-end delays, and the total time spent waiting in queues.

use std::borrow::BorrowMut;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::basic_sink::BasicPacketSink;
use crate::flows::packet::Packet;
use crate::flows::progress::{PacketStatistics, Report};
use crate::flows::source::PacketSource;
use crate::flows::statistics::RandomVar;
use crate::flows::tcp_sink::TCPPacketSink;

#[derive(Clone, Debug)]
pub struct PacketSinkStatistics {
    sink_name: String,
    /// the arrival times of the packets
    arrival_times: RandomVar,
    /// the last arrival time
    last_arrival_time: f64,
    /// the inter-arrival times of the packets
    inter_arrival_times: RandomVar,
    /// the one-way end-to-end delays of the packets
    one_way_delays: RandomVar,
    /// the total time spent waiting in queues
    queueing_delays: RandomVar,
    /// the size of the packets
    packet_sizes: RandomVar,
}

impl Display for PacketSinkStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} recorded statistics: \n\
            Arrival times: {:#.3} \n\
            Inter-arrival times: {:#.3} \n\
            One-way delays: {:#.3} \n\
            Queueing delays: {:#.3} \n\
            Packet sizes: {:#.3} \n",
            self.sink_name,
            self.arrival_times,
            self.inter_arrival_times,
            self.one_way_delays,
            self.queueing_delays,
            self.packet_sizes,
        )
    }
}

impl PacketSinkStatistics {
    pub fn new(sink_name: String) -> Self {
        PacketSinkStatistics {
            sink_name,
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
        }
    }

    pub fn update(&mut self, packet: &Packet, now: f64) {
        self.arrival_times.tabulate(now);
        self.inter_arrival_times
            .tabulate(now - self.last_arrival_time);
        self.last_arrival_time = now;
        self.one_way_delays.tabulate(now - packet.creation_time);
        self.queueing_delays.tabulate(packet.queueing_delay);
        self.packet_sizes.tabulate(packet.size as u32);
    }
}

#[derive(Debug)]
pub enum PacketSink {
    BasicPacketSink(BasicPacketSink),
    TCPPacketSink(TCPPacketSink),
}

impl std::fmt::Display for PacketSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PacketSink::BasicPacketSink(_) => write!(f, "PacketSink {}", self.id()),
            PacketSink::TCPPacketSink(_) => write!(f, "TCPPacketSink {}", self.id()),
        }
    }
}

impl PacketSink {
    pub fn new(source: &PacketSource) -> Self {
        match source {
            PacketSource::DistPacketSource(source) => {
                PacketSink::BasicPacketSink(BasicPacketSink::new(source.report_interval))
            }
            PacketSource::TCPPacketSource(source) => {
                PacketSink::TCPPacketSink(TCPPacketSink::new(source.report_interval))
            }
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.endpoint_id,
            PacketSink::TCPPacketSink(sink) => sink.endpoint_id,
        }
    }

    pub fn statistics(&mut self) -> &mut Output<PacketSinkStatistics> {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.statistics.borrow_mut(),
            PacketSink::TCPPacketSink(sink) => sink.statistics.borrow_mut(),
        }
    }

    pub fn output(&mut self) -> &mut Output<Packet> {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.output.borrow_mut(),
            PacketSink::TCPPacketSink(sink) => sink.output.borrow_mut(),
        }
    }

    pub fn report_output(&mut self) -> &mut Output<Report> {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.report_output.borrow_mut(),
            PacketSink::TCPPacketSink(sink) => sink.report_output.borrow_mut(),
        }
    }

    pub fn report_interval(&self) -> f64 {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.report_interval,
            PacketSink::TCPPacketSink(sink) => sink.report_interval,
        }
    }

    pub async fn report(&mut self, endpoint_id: usize) {
        assert_eq!(endpoint_id, self.id());
        debug!("{} reporting upon request.", format!("{self}"));
        match self {
            PacketSink::BasicPacketSink(sink) => {
                sink.statistics.send(sink.packet_statistics.clone()).await
            }
            PacketSink::TCPPacketSink(sink) => {
                sink.statistics.send(sink.packet_statistics.clone()).await
            }
        }
    }

    async fn wrap_up(&mut self, packet: Packet, now: f64) {
        match self {
            PacketSink::BasicPacketSink(_) => (),
            PacketSink::TCPPacketSink(sink) => sink.wrap_up(packet, now).await,
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        match self {
            PacketSink::BasicPacketSink(sink) => {
                sink.packet_statistics.update(&packet, arrival_time)
            }
            PacketSink::TCPPacketSink(sink) => sink.packet_statistics.update(&packet, arrival_time),
        };

        debug!(
            "{} received packet {} ({} bytes) from flow {} at time {:.3}.",
            format!("{self}"),
            packet.packet_id,
            packet.size,
            packet.flow_id,
            arrival_time,
        );

        self.wrap_up(packet, arrival_time).await;
    }

    /// Sends a perioid report of current statistics to the progress coroutine.
    fn send_report<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let name = format!("{self}");
            let statistics = match self {
                PacketSink::BasicPacketSink(sink) => {
                    PacketStatistics::PacketSinkStatistics(sink.packet_statistics.clone())
                }
                PacketSink::TCPPacketSink(sink) => {
                    PacketStatistics::PacketSinkStatistics(sink.packet_statistics.clone())
                }
            };
            self.report_output()
                .send(Report {
                    name,
                    statistics,
                    finished: false,
                })
                .await;

            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval()),
                    Self::send_report,
                    (),
                )
                .unwrap();
        }
    }
}

impl Model for PacketSink {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            scheduler
                .schedule_event(
                    Duration::from_secs_f64(self.report_interval()),
                    Self::send_report,
                    (),
                )
                .unwrap();

            self.into()
        })
    }
}

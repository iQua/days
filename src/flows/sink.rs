//! Implements a packet sink, designed to compute vital statistics from incoming
//! packets.

//! The packet sink records a variety of statistics, including absolute arrival
//! times, inter-arrival times, the total number of packets and bytes received,
//! the one-way end-to-end delays, and the total time spent waiting in queues.

use std::borrow::BorrowMut;
use std::cell::Cell;
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

use log::debug;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::Serialize;

use crate::flows::basic_sink::BasicPacketSink;
use crate::flows::packet::Packet;
use crate::flows::source::PacketSource;
use crate::flows::tcp_sink::TCPPacketSink;
use crate::flows::FlowFinishMsg;
use crate::utils::logger::{CsvLogger, ReportTiming};

#[derive(Clone, Default, Debug, Serialize)]
pub struct PacketSinkReport {
    pub id: usize,
    pub flow_id: usize,
    /// the start time of this report interval
    pub start_time: f64,
    /// the end time of this report interval
    pub end_time: f64,
    /// the number of received packets in this report interval
    pub received_packets: usize,
    /// the size of received packets in this report interval
    pub received_sizes: usize,
    /// the mean of queueing delays of received packets in this report interval
    pub queueing_delay_mean: f64,
    /// the mean of one-way end-to-end delays of received packets in this report interval
    pub one_way_delay_mean: f64,
}

/// A simple collector for statistical data.
#[derive(Clone, Debug)]
pub struct RandomVar {
    total: Cell<u32>,
    sum: Cell<f64>,
    sqr: Cell<f64>,
    min: Cell<f64>,
    max: Cell<f64>,
}

impl RandomVar {
    /// Creates a new random variable.
    #[inline]
    pub fn new() -> Self {
        RandomVar::default()
    }

    /// Resets all stored statistical data.
    pub fn clear(&self) {
        self.total.set(0);
        self.sum.set(0.0);
        self.sqr.set(0.0);
        self.min.set(f64::INFINITY);
        self.max.set(f64::NEG_INFINITY);
    }

    /// Adds another packet to the statistical collection.
    pub fn tabulate<T: Into<f64>>(&self, val: T) {
        let val: f64 = val.into();

        self.total.set(self.total.get() + 1);
        self.sum.set(self.sum.get() + val);
        self.sqr.set(self.sqr.get() + val * val);

        if self.min.get() > val {
            self.min.set(val);
        }
        if self.max.get() < val {
            self.max.set(val);
        }
    }

    /// Combines the statistical collection of two random variables into one.
    pub fn merge(&self, other: &Self) {
        self.total.set(self.total.get() + other.total.get());
        self.sum.set(self.sum.get() + other.sum.get());
        self.sqr.set(self.sqr.get() + other.sqr.get());

        if self.min.get() > other.min.get() {
            self.min.set(other.min.get());
        }
        if self.max.get() < other.max.get() {
            self.max.set(other.max.get());
        }
    }
}

impl Default for RandomVar {
    fn default() -> Self {
        RandomVar {
            total: Cell::default(),
            sum: Cell::default(),
            sqr: Cell::default(),
            min: Cell::new(f64::INFINITY),
            max: Cell::new(f64::NEG_INFINITY),
        }
    }
}

impl Display for RandomVar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let total = self.total.get();
        let mean = self.sum.get() / f64::from(total);
        let variance = self.sqr.get() / f64::from(total) - mean * mean;
        let std_dev = variance.sqrt();

        f.debug_struct("RandomVar")
            .field("total", &total)
            .field("mean", &mean)
            .field("std_dev", &std_dev)
            .field("min", &self.min.get())
            .field("max", &self.max.get())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct PacketStatistics {
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
    #[cfg(feature = "test")]
    pub packets: Vec<Packet>,
}

impl Display for PacketStatistics {
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

impl PacketStatistics {
    pub fn new(sink_name: String) -> Self {
        PacketStatistics {
            sink_name,
            arrival_times: RandomVar::new(),
            last_arrival_time: 0.0,
            inter_arrival_times: RandomVar::new(),
            one_way_delays: RandomVar::new(),
            queueing_delays: RandomVar::new(),
            packet_sizes: RandomVar::new(),
            #[cfg(feature = "test")]
            packets: Vec::new(),
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
        #[cfg(feature = "test")]
        self.packets.push(packet.clone());
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
            PacketSource::DistPacketSource(_) => {
                PacketSink::BasicPacketSink(BasicPacketSink::new(source.flow_id()))
            }
            PacketSource::TCPPacketSource(_) => {
                PacketSink::TCPPacketSink(TCPPacketSink::new(source.flow_id()))
            }
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.endpoint_id,
            PacketSink::TCPPacketSink(sink) => sink.endpoint_id,
        }
    }

    pub fn statistics(&mut self) -> &mut Output<PacketStatistics> {
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

    pub fn connect_flow_finish_output(&mut self, flow_finish_output: Output<FlowFinishMsg>) {
        match self {
            PacketSink::BasicPacketSink(sink) => {
                sink.flow_finish_outputs.push(flow_finish_output);
            }
            PacketSink::TCPPacketSink(_) => {}
        }
    }

    pub async fn report(&mut self, endpoint_id: usize, cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        assert_eq!(endpoint_id, self.id());
        debug!("{} reporting upon request.", format!("{self}"));

        match self {
            PacketSink::BasicPacketSink(sink) => {
                sink.log_report(now, ReportTiming::Final);
                sink.statistics.send(sink.packet_statistics.clone()).await
            }
            PacketSink::TCPPacketSink(sink) => {
                sink.log_report(now, ReportTiming::Final);
                sink.statistics.send(sink.packet_statistics.clone()).await;
            }
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
        // to be removed after more thorough testing
        let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        let local_time = match self {
            PacketSink::BasicPacketSink(sink) => sink.time,
            PacketSink::TCPPacketSink(sink) => sink.time,
        };

        println!(
            "TEST packet.time {:?}, global_time {:?}, local_time {:?}",
            packet.time, global_time, local_time
        );
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

        let now = packet.time;

        debug!(
            "{} received packet {} ({} bytes) from flow {} at time {:.3}.",
            format!("{self}"),
            packet.packet_id,
            packet.size,
            packet.flow_id,
            now,
        );

        match self {
            PacketSink::BasicPacketSink(sink) => sink.process(packet, now).await,
            PacketSink::TCPPacketSink(sink) => sink.process(packet, now).await,
        }
    }

    async fn log_report<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        match self {
            PacketSink::BasicPacketSink(sink) => {
                sink.log_report(now, ReportTiming::InProgress);
            }
            PacketSink::TCPPacketSink(sink) => {
                sink.log_report(now, ReportTiming::InProgress);
            }
        }
    }
}

impl Model for PacketSink {
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

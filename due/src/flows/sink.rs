//! Implements a packet sink, designed to compute vital statistics from incoming
//! packets.

//! The packet sink records a variety of statistics, including absolute arrival
//! times, inter-arrival times, the total number of packets and bytes received,
//! the one-way end-to-end delays, and the total time spent waiting in queues.

use std::cell::Cell;
use std::fmt::{Debug, Display, Formatter};

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::basic_sink::BasicPacketSink;
use crate::flows::packet::Packet;
use crate::flows::source::PacketSource;
use crate::flows::tcp_sink::TCPPacketSink;

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

impl std::fmt::Display for PacketStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Statistics: \n\
            Arrival times: {:#.3} \n\
            Inter-arrival times: {:#.3} \n\
            One-way delays: {:#.3} \n\
            Queueing delays: {:#.3} \n\
            Packet sizes: {:#.3} \n",
            self.arrival_times,
            self.inter_arrival_times,
            self.one_way_delays,
            self.queueing_delays,
            self.packet_sizes,
        )
    }
}

impl PacketStatistics {
    pub fn new() -> Self {
        PacketStatistics {
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
            PacketSource::DistPacketSource(_) => {
                PacketSink::BasicPacketSink(BasicPacketSink::new())
            }
            PacketSource::TCPPacketSource(_) => PacketSink::TCPPacketSink(TCPPacketSink::new()),
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.endpoint_id,
            PacketSink::TCPPacketSink(sink) => sink.endpoint_id,
        }
    }

    pub fn packet_statistics(&self) -> PacketStatistics {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.packet_statistics,
            PacketSink::TCPPacketSink(sink) => sink.packet_statistics,
        }
    }

    pub fn statistics(&self) -> Output<PacketStatistics> {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.statistics,
            PacketSink::TCPPacketSink(sink) => sink.statistics,
        }
    }

    pub fn output(&self) -> Output<Packet> {
        match self {
            PacketSink::BasicPacketSink(sink) => sink.output,
            PacketSink::TCPPacketSink(sink) => sink.output,
        }
    }

    pub async fn report(&mut self, endpoint_id: usize) {
        assert_eq!(endpoint_id, self.id());
        debug!("{} reporting upon request.", format!("{self}"));
        self.statistics()
            .send(self.packet_statistics().clone())
            .await;
    }

    async fn wrap_up(&mut self, packet: Packet, now: f64) {
        match self {
            PacketSink::BasicPacketSink(_) => (),
            PacketSink::TCPPacketSink(sink) => sink.wrap_up(packet, now).await,
        }
    }

    pub fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
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

        self.wrap_up(packet, arrival_time);
    }
}

impl Model for PacketSink {}

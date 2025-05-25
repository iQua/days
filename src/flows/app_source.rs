//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource.

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use futures::future::join_all;
use futures_executor::ThreadPool;
use rand::rngs::SmallRng;
use tachyonix::{channel, Receiver, Sender};

/// Trait for application-level data sources (not a simulation model).
pub trait AppSource: Send {
    fn produce_data(&mut self, now: f64) -> Vec<Packet>;
    fn total_size(&self) -> usize;
    fn set_flow_start_time(&mut self, _t: f64) {}
}

#[derive(Debug, Clone)]
pub struct BufferedAppDataSource {
    total_size: usize,
    packets: Vec<Packet>,
}

impl BufferedAppDataSource {
    pub fn new(packets: Vec<Packet>) -> Self {
        let total_size = packets.iter().map(|p| p.size).sum();
        Self {
            total_size,
            packets,
        }
    }

    pub fn clone_packets(&self) -> Vec<Packet> {
        self.packets.clone()
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }
}

impl AppSource for BufferedAppDataSource {
    fn produce_data(&mut self, _now: f64) -> Vec<Packet> {
        self.clone_packets()
    }

    fn total_size(&self) -> usize {
        self.total_size()
    }
}

pub enum AppDataSource {
    DistDataSource(DistPacketSource),
    Dummy,
}

pub enum AppDataType {
    DistData,
}

impl AppDataSource {
    pub fn dummy() -> Self {
        AppDataSource::Dummy
    }

    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        AppDataSource::DistDataSource(DistPacketSource::new(flow_id, Vec::new(), traffic, rng))
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            AppDataSource::DistDataSource(source) => source.traffic_exceeded(now),
            AppDataSource::Dummy => true,
        }
    }
}

impl AppSource for AppDataSource {
    fn produce_data(&mut self, now: f64) -> Vec<Packet> {
        match self {
            AppDataSource::DistDataSource(source) => {
                let (packet, _) = source.produce_packet(now);
                source.packet_sent(&packet, now);
                vec![packet]
            }
            AppDataSource::Dummy => panic!("Dummy source should not produce data"),
        }
    }

    fn total_size(&self) -> usize {
        match self {
            AppDataSource::DistDataSource(_source) => 0, // TODO: implement real total size
            AppDataSource::Dummy => 0,
        }
    }

    fn set_flow_start_time(&mut self, t: f64) {
        match self {
            AppDataSource::DistDataSource(source) => source.flow_start_time = t,
            AppDataSource::Dummy => {}
        }
    }
}

/// Spawns a background task that pushes packets from the given AppSource into a channel.
/// Returns `n` receivers cloned from the same channel.
pub fn spawn_appsource_channel(
    mut source: Box<dyn AppSource + Send>,
    n_receivers: usize,
) -> Vec<Receiver<Packet>> {
    let (senders, receivers): (Vec<Sender<Packet>>, Vec<Receiver<Packet>>) =
        (0..n_receivers).map(|_| channel::<Packet>(128)).unzip();

    let pool = ThreadPool::new().expect("Failed to create thread pool");

    // Share AppSource logic into a thread
    pool.spawn_ok(async move {
        let packets = source.produce_data(0.0); // dummy timestamp
        for packet in packets {
            let sends = senders
                .iter()
                .map(|tx| tx.send(packet.clone()))
                .collect::<Vec<_>>();

            let _ = join_all(sends).await;
        }
    });

    receivers
}

//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource.

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use futures::future::join_all;
use futures_executor::ThreadPool;
use rand::rngs::SmallRng;
use tachyonix::{channel, Receiver, Sender};

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

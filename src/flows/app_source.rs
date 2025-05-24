//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource.

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use rand::rngs::SmallRng;
use tachyonix::channel::{self, Receiver};
use tachyonix::spawn;

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
    // The data source from the application generates packets based on probability distributions,
    // but it can be trace-driven as well in the future.
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

    /// Creates an AppDataSource and spawns a coroutine to produce packets into a channel.
    /// Returns the receiver side of the channel to be consumed by TCPPacketSource.
    pub fn spawn_and_channel(
        flow_id: usize,
        traffic: TrafficCharacteristics,
        rng: SmallRng,
    ) -> Receiver<Packet> {
        let mut source = match AppDataType::DistData {
            AppDataType::DistData => AppDataSource::DistDataSource(DistPacketSource::new(
                flow_id,
                Vec::new(),
                traffic,
                rng,
            )),
        };

        let (tx, rx) = channel::<Packet>(128);

        spawn(async move {
            loop {
                let packets = source.produce_data(0.0); // dummy timestamp
                for packet in packets {
                    if tx.send(packet).await.is_err() {
                        return; // Receiver dropped
                    }
                }
                break; // Exit after sending one burst, or loop to simulate a stream
            }
        });

        rx
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

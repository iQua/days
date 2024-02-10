//! An application packet source.

use rand::rngs::SmallRng;
use std::time::Duration;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;

pub enum AppDataSource {
    // the data source from the application is implemented as a distribution-based packet source,
    // but it can be trace-driven, etc., in the future
    DistDataSource(DistPacketSource),
}

pub enum AppDataType {
    DistData,
}

impl AppDataSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        let app_type = AppDataType::DistData;

        match app_type {
            AppDataType::DistData => {
                AppDataSource::DistDataSource(DistPacketSource::new(flow_id, traffic, rng))
            }
        }
    }

    pub fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        let (packet, duration) = match self {
            AppDataSource::DistDataSource(source) => source.produce_packet(now),
        };

        // the packet has just been produced, update statistics about traffic production
        match self {
            AppDataSource::DistDataSource(source) => source.packet_sent(&packet, now),
        };

        (packet, duration)
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            AppDataSource::DistDataSource(source) => source.traffic_exceeded(now),
        }
    }
}

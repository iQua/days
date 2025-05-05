//! Implements an application data source used by TCP.

use rand::rngs::SmallRng;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;

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

    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        let app_type = AppDataType::DistData;

        match app_type {
            AppDataType::DistData => AppDataSource::DistDataSource(DistPacketSource::new(
                flow_id,
                Vec::new(),
                traffic,
                rng,
            )),
        }
    }

    pub fn set_flow_start_time(&mut self, flow_start_time: f64) {
        match self {
            AppDataSource::DistDataSource(source) => source.flow_start_time = flow_start_time,
            AppDataSource::Dummy => {}
        }
    }

    pub fn produce_data(&mut self, now: f64) -> (Packet, f64) {
        let (packet, duration) = match self {
            AppDataSource::DistDataSource(source) => source.produce_packet(now),
        };

        // the packet has just been produced, update statistics about traffic production
        match self {
            AppDataSource::DistDataSource(source) => source.packet_sent(&packet, now),
            AppDataSource::Dummy => {
                panic!("Dummy AppDataSource should not produce data")
            }
        };

        (packet, duration)
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            AppDataSource::DistDataSource(source) => source.traffic_exceeded(now),
            AppDataSource::Dummy => true,
        }
    }
}

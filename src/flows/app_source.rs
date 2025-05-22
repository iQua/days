//! Implements an application data source used by TCP.

use rand::rngs::SmallRng;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;

use crate::flows::buffered_app_source::AppDataSourceTrait;

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
        match self {
            AppDataSource::DistDataSource(source) => {
                let (packet, duration) = source.produce_packet(now);
                source.packet_sent(&packet, now);
                (packet, duration)
            }
            AppDataSource::Dummy => {
                panic!("Dummy AppDataSource should not produce data")
            }
        }
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            AppDataSource::DistDataSource(source) => source.traffic_exceeded(now),
            AppDataSource::Dummy => true,
        }
    }
}

impl AppDataSourceTrait for AppDataSource {
    fn produce_data(&mut self, now: f64) -> Vec<Packet> {
        match self {
            AppDataSource::DistDataSource(source) => {
                let (packet, _) = source.produce_packet(now);
                source.packet_sent(&packet, now);
                vec![packet]
            }
            AppDataSource::Dummy => {
                panic!("Dummy source should not produce data");
            }
        }
    }

    fn total_size(&self) -> usize {
        match self {
            AppDataSource::DistDataSource(source) => source.total_size(),
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

//! Implements an application data source used by TCP.

use rand::rngs::SmallRng;
use std::sync::Arc;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;

pub enum AppDataSource {
    // Generates packets based on probability distributions.
    DistDataSource(DistPacketSource),

    // Shares a buffer across multiple flows (e.g., for broadcast), reads in fixed-size chunks.
    SharedDataSource {
        data: Arc<[u8]>,
        offset: usize,
        chunk_size: usize,
        flow_id: usize,
    },
}

pub enum AppDataType {
    DistData,
}

impl AppDataSource {
    /// Constructor for standard probabilistic data generation.
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

    /// Constructor for shared app data, used in broadcast-like scenarios.
    pub fn from_bytes(flow_id: usize, data: Arc<[u8]>, chunk_size: usize) -> Self {
        AppDataSource::SharedDataSource {
            data,
            offset: 0,
            chunk_size,
            flow_id,
        }
    }

    pub fn set_flow_start_time(&mut self, flow_start_time: f64) {
        match self {
            AppDataSource::DistDataSource(source) => {
                source.flow_start_time = flow_start_time;
            }
            AppDataSource::SharedDataSource { .. } => {
                // No-op for shared data
            }
        }
    }

    /// Produce next data packet. Returns None if no more data.
    pub fn produce_data(&mut self, now: f64) -> Option<(Packet, f64)> {
        match self {
            AppDataSource::DistDataSource(source) => {
                let (packet, duration) = source.produce_packet(now);
                source.packet_sent(&packet, now);
                Some((packet, duration))
            }

            AppDataSource::SharedDataSource {
                data,
                offset,
                chunk_size,
                flow_id,
            } => {
                if *offset >= data.len() {
                    return None;
                }

                let end = (*offset + *chunk_size).min(data.len());
                let payload = data[*offset..end].to_vec();
                *offset = end;

                Some((Packet::new(payload, *flow_id), 0.0))
            }
        }
    }

    /// Whether all data has been sent.
    pub fn traffic_exceeded(&self, _now: f64) -> bool {
        match self {
            AppDataSource::DistDataSource(source) => source.traffic_exceeded(_now),
            AppDataSource::SharedDataSource { offset, data, .. } => *offset >= data.len(),
        }
    }
}

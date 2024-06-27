pub mod app_source;
pub mod basic_sink;
pub mod cc;
pub mod collective;
pub mod dist_source;
pub mod flow;
pub mod packet;
pub mod route;
pub mod sink;
pub mod source;
pub mod tcp_sink;
pub mod tcp_source;
pub mod wire;

use serde::Deserialize;

use crate::flows::cc::CCAlgorithm;

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type")]
pub enum DistributionInfo {
    DiscreteUniform { low: i64, high: i64 },
    Exp { lambda: f64 },
    Uniform { low: f64, high: f64 },
}

#[derive(Debug, Clone, Copy)]
pub enum FlowSize {
    Bytes(usize),
    Duration(f64),
}

impl FlowSize {
    pub fn exceeded(&self, sent_size: usize, flow_start_time: f64, now: f64) -> bool {
        match self {
            FlowSize::Duration(duration) => (now - flow_start_time) >= *duration,
            FlowSize::Bytes(size) => sent_size >= *size,
        }
    }
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct TomlTrafficCharacteristics {
    pub initial_delay: Option<f64>,
    pub duration: Option<f64>,
    pub size: Option<usize>,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub tcp: Option<TCPCharacteristics>,
}

#[derive(Debug, Clone, Copy)]
pub struct TrafficCharacteristics {
    pub initial_delay: f64,
    pub size: FlowSize,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub tcp: Option<TCPCharacteristics>,
}

impl TrafficCharacteristics {
    pub fn new(
        initial_delay: f64,
        duration: Option<f64>,
        size: Option<usize>,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
        tcp: Option<TCPCharacteristics>,
    ) -> Self {
        let size = match size {
            Some(size) => FlowSize::Bytes(size),
            None => match duration {
                Some(duration) => FlowSize::Duration(duration),
                None => panic!("Must specify duration or size of the flow."),
            },
        };
        Self {
            initial_delay,
            size,
            arr_dist,
            pkt_size_dist,
            tcp,
        }
    }

    pub fn clone(traffic: &TomlTrafficCharacteristics) -> Self {
        Self {
            initial_delay: traffic.initial_delay.unwrap_or_default(),
            size: match traffic.size {
                Some(size) => FlowSize::Bytes(size),
                None => match traffic.duration {
                    Some(duration) => FlowSize::Duration(duration),
                    None => panic!("Must specify duration or size of the flow."),
                },
            },
            arr_dist: traffic.arr_dist,
            pkt_size_dist: traffic.pkt_size_dist,
            tcp: traffic.tcp,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FlowFinishMsg {
    pub flow_id: usize,
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct TCPCharacteristics {
    pub cc_algorithm: CCAlgorithm,
}

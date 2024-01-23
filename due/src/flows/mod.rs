pub mod collective;
use serde::Deserialize;

pub mod cc;
pub mod flow;
pub mod packet;
pub mod route;
pub mod sink;
pub mod source;
pub mod tcp_sink;
pub mod tcp_source;
pub mod wire;

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
    pub fn exceeded(&self, sent_size: usize, now: f64) -> bool {
        match self {
            FlowSize::Duration(duration) => now >= *duration,
            FlowSize::Bytes(size) => sent_size >= *size,
        }
    }
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct TomlTrafficCharacteristics {
    pub initial_delay: f64,
    pub duration: Option<f64>,
    pub size: Option<usize>,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
}

#[derive(Debug, Clone, Copy)]
pub struct TrafficCharacteristics {
    pub initial_delay: f64,
    pub size: FlowSize,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
}

impl TrafficCharacteristics {
    pub fn new(
        initial_delay: f64,
        duration: Option<f64>,
        size: Option<usize>,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
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
        }
    }

    pub fn clone(traffic: &TomlTrafficCharacteristics) -> Self {
        Self {
            initial_delay: traffic.initial_delay,
            size: match traffic.size {
                Some(size) => FlowSize::Bytes(size),
                None => match traffic.duration {
                    Some(duration) => FlowSize::Duration(duration),
                    None => panic!("Must specify duration or size of the flow."),
                },
            },
            arr_dist: traffic.arr_dist,
            pkt_size_dist: traffic.pkt_size_dist,
        }
    }
}

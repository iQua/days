//! Flow models, packet types, and traffic generation utilities.

pub mod app_source;
pub mod basic_sink;
pub mod bbr;
pub mod cc;
pub mod collective;
pub mod cubic;
#[cfg(feature = "dcqcn")]
pub mod dcqcn_sink;
#[cfg(feature = "dcqcn")]
pub mod dcqcn_source;
pub mod dist_source;
pub mod flow;
pub mod packet;
pub mod reno;
pub mod route;
pub mod sink;
pub mod source;
pub mod tcp_sink;
pub mod tcp_source;
pub mod wire;

use serde::Deserialize;

use crate::flows::cc::CCAlgorithm;

pub use days::scenario::DistributionInfo;

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Deserialize, Debug, Clone)]
pub struct TomlTrafficCharacteristics {
    pub initial_delay: Option<f64>,
    pub duration: Option<f64>,
    pub size: Option<usize>,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub tcp: Option<TCPCharacteristics>,
    #[cfg(feature = "dcqcn")]
    pub dcqcn: Option<DcqcnCharacteristics>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrafficCharacteristics {
    pub initial_delay: f64,
    pub size: FlowSize,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub tcp: Option<TCPCharacteristics>,
    #[cfg(feature = "dcqcn")]
    pub dcqcn: Option<DcqcnCharacteristics>,
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
            #[cfg(feature = "dcqcn")]
            dcqcn: None,
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
            arr_dist: traffic.arr_dist.clone(),
            pkt_size_dist: traffic.pkt_size_dist.clone(),
            tcp: traffic.tcp.clone(),
            #[cfg(feature = "dcqcn")]
            dcqcn: traffic.dcqcn.clone(),
        }
    }
}

impl Default for TrafficCharacteristics {
    fn default() -> Self {
        Self::new(
            1.,
            Some(10.),
            None,
            DistributionInfo::Exp { lambda: 1. },
            DistributionInfo::DiscreteUniform {
                low: 1000,
                high: 1000,
            },
            None,
        )
    }
}

#[derive(Clone, Debug)]
pub struct FlowFinishMsg {
    pub flow_id: usize,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct CubicConfig {
    pub beta: Option<f64>,
    pub c: Option<f64>,
    pub fast_convergence: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct TCPCharacteristics {
    pub cc_algorithm: CCAlgorithm,
    #[serde(default)]
    pub ecn: bool,
    #[serde(default)]
    pub cubic: Option<CubicConfig>,
}

#[cfg(feature = "dcqcn")]
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct DcqcnCharacteristics {
    pub rate_gbps: f64,
    pub min_rate_gbps: f64,
    pub max_rate_gbps: f64,
    pub g: f64,
    pub ai_rate_gbps: f64,
    pub hai_rate_gbps: f64,
    pub mi_factor: f64,
    pub rtt_ns: Option<f64>,
    pub cnp_interval_ns: Option<f64>,
    pub pacing_interval_ns: Option<f64>,
    pub cnp_priority: Option<u8>,
}

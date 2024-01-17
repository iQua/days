pub mod collective;
use serde::Deserialize;

pub mod flow;
pub mod packet;
pub mod route;
pub mod sink;
pub mod source;

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type")]
pub enum DistributionInfo {
    DiscreteUniform { low: i64, high: i64 },
    Exp { lambda: f64 },
    Uniform { low: f64, high: f64 },
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub struct TomlTrafficCharacteristics {
    pub initial_delay: f64,
    pub duration: Option<f64>,
    pub size: Option<usize>,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
}

/// A struct to define the traffic characterististics, which is used for flow and
/// source.
#[derive(Debug, Clone, Copy)]
pub struct TrafficCharacteristics {
    pub initial_delay: f64,
    pub duration: f64,
    pub size: usize,
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
        if duration.is_none() & size.is_none() {
            panic!("Must speific duration or size of the flow.");
        }

        Self {
            initial_delay,
            duration: duration.unwrap_or(f64::MAX),
            size: size.unwrap_or(usize::MAX),
            arr_dist,
            pkt_size_dist,
        }
    }
}

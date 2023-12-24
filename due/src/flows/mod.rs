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
pub struct TrafficCharacteristics {
    pub initial_delay: f64,
    pub duration: f64,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
}

impl TrafficCharacteristics {
    pub fn new(
        initial_delay: f64,
        duration: f64,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> Self {
        Self {
            initial_delay,
            duration,
            arr_dist,
            pkt_size_dist,
        }
    }
}

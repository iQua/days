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
    Exp { lambda: f64 },
    Uniform { low: i64, high: i64 },
}

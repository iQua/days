//! Backend-neutral lowering from supported Days configuration into one semantic image.

mod compile;
mod ids;

use serde::Deserialize;

pub use compile::{CompileError, compile_config};

/// Distribution configuration shared by legacy traffic models and exact lowering.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum DistributionInfo {
    DiscreteUniform { low: i64, high: i64 },
    Exp { lambda: f64 },
    Uniform { low: f64, high: f64 },
}

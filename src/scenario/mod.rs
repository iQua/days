//! Backend-neutral lowering from supported Days configuration into one semantic image.

pub mod collective_shapes;
mod compile;
mod ids;
pub mod workload;

use serde::Deserialize;

pub use compile::{
    CompileError, FatTreeEcmpTermination, FatTreeEcmpTrafficKey, FatTreeEcmpTransport,
    compile_config, compile_config_with_manifest, compile_config_with_route_workers,
    compile_config_with_workload, fat_tree_ecmp_explicit_flow_hash,
    fat_tree_ecmp_flow_set_member_hash,
};

/// Distribution configuration shared by legacy traffic models and exact lowering.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum DistributionInfo {
    DiscreteUniform { low: i64, high: i64 },
    Exp { lambda: f64 },
    Uniform { low: f64, high: f64 },
}

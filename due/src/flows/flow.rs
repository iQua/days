use petgraph::graph::DiGraph;
use serde::Deserialize;

use crate::sim::Time;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum FlowType {
    PacketDistribution,
    TCP,
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type")]
pub enum DistributionInfo {
    Exp { lambda: f64 },
    Uniform { low: i64, high: i64 },
}

#[derive(Debug)]
pub struct Flow {
    pub id: usize,
    pub flow_type: FlowType,
    pub graph: DiGraph<usize, ()>,
    pub initial_delay: Time,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
}

impl Clone for Flow {
    fn clone(&self) -> Self {
        Flow {
            id: self.id,
            flow_type: self.flow_type.clone(),
            graph: self.graph.clone(),
            initial_delay: self.initial_delay,
            arr_dist: self.arr_dist,
            pkt_size_dist: self.pkt_size_dist,
        }
    }
}

impl Flow {
    pub fn new(
        id: usize,
        flow_type: FlowType,
        graph: DiGraph<usize, ()>,
        initial_delay: Time,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> Flow {
        Flow {
            id,
            flow_type,
            graph,
            initial_delay,
            arr_dist,
            pkt_size_dist,
        }
    }
}

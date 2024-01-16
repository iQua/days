use std::fs;

use petgraph::graph::DiGraph;
use serde::Deserialize;

use crate::flows::flow::FlowType;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::next_collective_id;

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum CollectiveType {
    Broadcast,
    Gather,
    AllReduce,
}

#[derive(Deserialize, Debug)]
struct TomlCollective {
    collective_type: CollectiveType,
    flow_type: FlowType,
    graph: Vec<(u32, u32)>,
    sources: Vec<usize>,
    sinks: Vec<usize>,
    traffic: TrafficCharacteristics,
}

#[derive(Debug)]
pub struct Collective {
    pub id: usize,
    pub collective_type: CollectiveType,
    pub flow_type: FlowType,
    pub graph: DiGraph<usize, ()>,

    /// host ids that sources and sinks attach to
    pub sources: Vec<usize>,
    pub sinks: Vec<usize>,

    pub traffic: TrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct CollectiveConfig {
    collective: Option<Vec<TomlCollective>>,
}

impl Collective {
    pub fn new(
        id: usize,
        collective_type: CollectiveType,
        flow_type: FlowType,
        graph: DiGraph<usize, ()>,
        sources: Vec<usize>,
        sinks: Vec<usize>,
        traffic: TrafficCharacteristics,
    ) -> Collective {
        Collective {
            id,
            collective_type,
            flow_type,
            graph,
            sources,
            sinks,
            traffic,
        }
    }

    // Initializes collectives from a vector of directed graphs, each graph
    // corresponding to one collective.
    pub fn collectives_from_graph(
        graphs: Vec<Vec<(u32, u32)>>,
        sources: Vec<Vec<usize>>,
        sinks: Vec<Vec<usize>>,
    ) -> Vec<Collective> {
        let mut collectives = Vec::new();

        for (index, graph) in graphs.iter().enumerate() {
            let collective_graph = DiGraph::<usize, ()>::from_edges(graph);
            let collective_sources = sources[index].clone();
            let collective_sinks = sinks[index].clone();

            collectives.push(Collective::new(
                next_collective_id(),
                CollectiveType::AllReduce,
                FlowType::PacketDistribution,
                collective_graph,
                collective_sources,
                collective_sinks,
                TrafficCharacteristics::new(
                    1.,
                    Some(10.),
                    DistributionInfo::Exp { lambda: 1. },
                    DistributionInfo::DiscreteUniform {
                        low: 1000,
                        high: 1000,
                    },
                ),
            ));
        }

        collectives
    }

    // Initializes collectives from a configuration file.
    pub fn collectives_from_config(file_path: &str) -> Vec<Collective> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: CollectiveConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut collectives = Vec::new();

        if let Some(collectives_vec) = config.collective {
            for collective in collectives_vec {
                let graph = DiGraph::<usize, ()>::from_edges(collective.graph);

                collectives.push(Collective::new(
                    next_collective_id(),
                    collective.collective_type,
                    collective.flow_type,
                    graph,
                    collective.sources,
                    collective.sinks,
                    collective.traffic,
                ));
            }
        }

        collectives
    }
}

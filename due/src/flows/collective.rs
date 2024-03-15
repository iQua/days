use std::fs;

use petgraph::graph::DiGraph;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::Deserialize;

use crate::flows::flow::FlowType;
use crate::flows::{DistributionInfo, TomlTrafficCharacteristics, TrafficCharacteristics};
use crate::{next_collective_id, seed_from_config};

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
    flow_count: usize,
    graph: Option<Vec<(u32, u32)>>,
    sources: Option<Vec<usize>>,
    sinks: Option<Vec<usize>>,
    traffic: TomlTrafficCharacteristics,
}

#[derive(Debug)]
pub struct Collective {
    pub id: usize,
    pub collective_type: CollectiveType,
    pub flow_type: FlowType,
    pub flow_count: usize,
    pub graph: Option<DiGraph<usize, ()>>,

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
        flow_count: usize,
        graph: Option<DiGraph<usize, ()>>,
        sources: Vec<usize>,
        sinks: Vec<usize>,
        traffic: TrafficCharacteristics,
    ) -> Collective {
        Collective {
            id,
            collective_type,
            flow_type,
            flow_count,
            graph,
            sources,
            sinks,
            traffic,
        }
    }

    /// Initializes collectives from a vector of directed graphs, each graph
    /// corresponding to one collective.
    pub fn collectives_from_graph(
        flow_count: usize,
        graphs: Vec<Vec<(u32, u32)>>,
        sources: Vec<Vec<usize>>,
        sinks: Vec<Vec<usize>>,
    ) -> Vec<Collective> {
        let mut collectives = Vec::new();

        for (index, graph) in graphs.iter().enumerate() {
            let collective_graph = Some(DiGraph::<usize, ()>::from_edges(graph));
            let collective_sources = sources[index].clone();
            let collective_sinks = sinks[index].clone();

            collectives.push(Collective::new(
                next_collective_id(),
                CollectiveType::AllReduce,
                FlowType::PacketDistribution,
                flow_count,
                collective_graph,
                collective_sources,
                collective_sinks,
                TrafficCharacteristics::new(
                    1.,
                    Some(10.),
                    None,
                    DistributionInfo::Exp { lambda: 1. },
                    DistributionInfo::DiscreteUniform {
                        low: 1000,
                        high: 1000,
                    },
                    None,
                ),
            ));
        }

        collectives
    }

    /// Initializes collectives from a configuration file.
    pub fn collectives_from_config(file_path: &str, hosts: &Vec<usize>) -> Vec<Collective> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: CollectiveConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut collectives = Vec::new();

        if let Some(collectives_vec) = config.collective {
            for collective in collectives_vec {
                let mut graph = None;
                if let Some(config_graph) = collective.graph {
                    graph = Some(DiGraph::<usize, ()>::from_edges(config_graph));
                }

                let mut sources = collective.sources.unwrap_or_default();
                let mut sinks = collective.sinks.unwrap_or_default();

                if sources.is_empty() && sinks.is_empty() {
                    let mut rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);
                    match collective.collective_type {
                        CollectiveType::Broadcast => {
                            let source = hosts.choose(&mut rng).unwrap().clone();
                            let mut sink_hosts = hosts.clone();
                            sink_hosts.retain(|&x| x != source);
                            for _ in 0..collective.flow_count {
                                sources.push(source);
                                sinks.push(sink_hosts.choose(&mut rng).unwrap().clone());
                            }
                        }
                        CollectiveType::Gather => {
                            let sink = hosts.choose(&mut rng).unwrap().clone();
                            let mut source_hosts = hosts.clone();
                            source_hosts.retain(|&x| x != sink);
                            for _ in 0..collective.flow_count {
                                sources.push(source_hosts.choose(&mut rng).unwrap().clone());
                                sinks.push(sink);
                            }
                        }
                        CollectiveType::AllReduce => {
                            let sink = hosts.choose(&mut rng).unwrap().clone();
                            let mut source_hosts = hosts.clone();
                            source_hosts.retain(|&x| x != sink);
                            for _ in 0..collective.flow_count {
                                sources.push(source_hosts.choose(&mut rng).unwrap().clone());
                                sinks.push(sink);
                            }
                        }
                    }
                }

                let traffic = TrafficCharacteristics::clone(&collective.traffic);
                collectives.push(Collective::new(
                    next_collective_id(),
                    collective.collective_type,
                    collective.flow_type,
                    collective.flow_count,
                    graph,
                    sources,
                    sinks,
                    traffic,
                ));
            }
        }

        collectives
    }
}

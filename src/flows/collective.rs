//! Implements collective communication operations in machine learning training workloads.

use std::fs;

use petgraph::graph::DiGraph;
use rand::prelude::IndexedRandom;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::Deserialize;

use crate::flows::flow::FlowType;
use crate::flows::route::RoutingConfig;
use crate::flows::{TomlTrafficCharacteristics, TrafficCharacteristics};
use crate::{next_collective_id, next_flow_id, seed_from_config, update_next_flow_id};

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum CollectiveType {
    Broadcast,
    Gather,
    AllReduce,
}

#[derive(Deserialize, Debug)]
struct TomlCollective {
    collective_type: CollectiveType,
    first_flow_id: Option<usize>,
    flow_type: FlowType,
    flow_count: usize,
    graph: Option<Vec<(u32, u32)>>,
    paths: Option<Vec<Vec<usize>>>,
    sources: Option<Vec<usize>>,
    sinks: Option<Vec<usize>>,
    routing: Option<RoutingConfig>,
    traffic: TomlTrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct TomlCollectiveSet {
    collective_type: CollectiveType,
    collective_count: usize,
    first_flow_id: Option<usize>,
    flow_type: FlowType,
    flow_count: usize,
    sources: Option<Vec<Vec<usize>>>,
    sinks: Option<Vec<Vec<usize>>>,
    routing: Option<RoutingConfig>,
    traffic: TomlTrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct CollectiveConfig {
    collective: Option<Vec<TomlCollective>>,
    collective_set: Option<Vec<TomlCollectiveSet>>,
}

#[derive(Debug)]
pub struct Collective {
    pub id: usize,
    pub collective_type: CollectiveType,
    pub first_flow_id: usize,
    pub flow_type: FlowType,
    pub flow_count: usize,
    pub graph: Option<DiGraph<usize, ()>>,
    pub paths: Option<Vec<Vec<usize>>>,

    /// host ids that sources and sinks attach to
    pub sources: Vec<usize>,
    pub sinks: Vec<usize>,

    pub routing: Option<RoutingConfig>,
    pub traffic: TrafficCharacteristics,
}

// Struct to hold parameters for collectives
pub struct CollectiveParams {
    id: usize,
    collective_type: CollectiveType,
    first_flow_id: usize,
    flow_type: FlowType,
    flow_count: usize,
    graph: Option<DiGraph<usize, ()>>,
    paths: Option<Vec<Vec<usize>>>,
    sources: Vec<usize>,
    sinks: Vec<usize>,
    routing: Option<RoutingConfig>,
    traffic: TrafficCharacteristics,
}

impl Collective {
    pub fn new(params: CollectiveParams) -> Self {
        Self {
            id: params.id,
            collective_type: params.collective_type,
            first_flow_id: params.first_flow_id,
            flow_type: params.flow_type,
            flow_count: params.flow_count,
            graph: params.graph,
            paths: params.paths,
            sources: params.sources,
            sinks: params.sinks,
            routing: params.routing,
            traffic: params.traffic,
        }
    }

    /// Initializes collectives from a vector of directed graphs, each graph
    /// corresponding to one collective.
    pub fn collectives_from_graph(
        collective_type: CollectiveType,
        graphs: Vec<Vec<(u32, u32)>>,
        paths: Option<Vec<Vec<usize>>>,
        sources: Vec<Vec<usize>>,
        sinks: Vec<Vec<usize>>,
    ) -> Vec<Collective> {
        let mut collectives = Vec::new();

        for (index, graph) in graphs.iter().enumerate() {
            let collective_graph = DiGraph::<usize, ()>::from_edges(graph);
            let flow_count = collective_graph.edge_count();
            let collective_sources = sources[index].clone();
            let collective_sinks = sinks[index].clone();
            let collective_paths = paths.clone();

            let first_flow_id = next_flow_id();
            update_next_flow_id(first_flow_id + flow_count);

            let params = CollectiveParams {
                id: next_collective_id(),
                collective_type,
                first_flow_id,
                flow_type: FlowType::PacketDistribution,
                flow_count,
                graph: Some(collective_graph),
                paths: collective_paths,
                sources: collective_sources,
                sinks: collective_sinks,
                routing: None,
                traffic: TrafficCharacteristics::default(),
            };
            collectives.push(Collective::new(params));
        }

        collectives
    }

    /// Generates sources and sinks of flows of a collective.
    fn generate_endpoints(
        collective_type: CollectiveType,
        flow_count: usize,
        paths: &Option<Vec<Vec<usize>>>,
        sources: Vec<usize>,
        sinks: Vec<usize>,
        hosts: &[usize],
        mut rng: SmallRng,
    ) -> (Vec<usize>, Vec<usize>) {
        if !sources.is_empty() && !sinks.is_empty() {
            assert_eq!(
                sources.len(),
                flow_count,
                "A collective whose specified flow_count is {} was specified {} sources",
                flow_count,
                sources.len()
            );
            assert_eq!(
                sinks.len(),
                flow_count,
                "A collective whose specified flow_count is {} was specified {} sinks",
                flow_count,
                sinks.len()
            );

            match collective_type {
                CollectiveType::Broadcast => {
                    assert!(
                        sources.iter().all(|&x| x == sources[0]),
                        "Please make specified sources of a Broadcast operation the same"
                    );
                }
                CollectiveType::Gather => {
                    assert!(
                        sinks.iter().all(|&x| x == sinks[0]),
                        "Please make specified sinks of a Gather operation the same"
                    );
                }
                CollectiveType::AllReduce => {}
            }

            return (sources, sinks);
        }

        if let Some(flow_paths) = paths {
            assert_eq!(
                flow_paths.len(),
                flow_count,
                "The number of specified paths ({}) should be the same as flow count {}",
                flow_paths.len(),
                flow_count
            );
            let sources = flow_paths.iter().map(|path| path[0]).collect();
            let sinks = flow_paths.iter().map(|path| path[path.len() - 1]).collect();

            return (sources, sinks);
        }

        match collective_type {
            CollectiveType::Broadcast => {
                let source = *hosts.choose(&mut rng).unwrap();
                let sink_hosts: Vec<_> = hosts.iter().filter(|&&x| x != source).copied().collect();
                let sources = vec![source; flow_count];
                let sinks = (0..flow_count)
                    .map(|_| *sink_hosts.choose(&mut rng).unwrap())
                    .collect();

                (sources, sinks)
            }
            CollectiveType::Gather | CollectiveType::AllReduce => {
                let sink = *hosts.choose(&mut rng).unwrap();
                let source_hosts: Vec<_> = hosts.iter().filter(|&&x| x != sink).copied().collect();
                let sinks = vec![sink; flow_count];
                let sources = (0..flow_count)
                    .map(|_| *source_hosts.choose(&mut rng).unwrap())
                    .collect();

                (sources, sinks)
            }
        }
    }

    /// Initializes collectives from a configuration file.
    pub fn collectives_from_config(file_path: &str, hosts: &[usize]) -> Vec<Collective> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: CollectiveConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);

        let mut collectives = Vec::new();

        if let Some(collectives_vec) = config.collective {
            for collective in collectives_vec {
                let graph = collective.graph.map(DiGraph::<usize, ()>::from_edges);

                let (sources, sinks) = Self::generate_endpoints(
                    collective.collective_type,
                    collective.flow_count,
                    &collective.paths,
                    collective.sources.unwrap_or_default(),
                    collective.sinks.unwrap_or_default(),
                    hosts,
                    rng.clone(),
                );

                let traffic = TrafficCharacteristics::clone(&collective.traffic);

                let mut first_flow_id = next_flow_id();
                if collective.first_flow_id.is_some() {
                    let new_first_flow_id = collective.first_flow_id.unwrap();
                    assert!(
                        new_first_flow_id >= first_flow_id,
                        "The specified first flow id {} of the collective should be at least {}",
                        new_first_flow_id,
                        first_flow_id
                    );
                    first_flow_id = new_first_flow_id;
                }
                update_next_flow_id(first_flow_id + collective.flow_count);

                let params = CollectiveParams {
                    id: next_collective_id(),
                    collective_type: collective.collective_type,
                    first_flow_id,
                    flow_type: collective.flow_type,
                    flow_count: collective.flow_count,
                    graph,
                    paths: collective.paths,
                    sources,
                    sinks,
                    routing: collective.routing,
                    traffic,
                };

                collectives.push(Collective::new(params));
            }
        }

        if let Some(collective_set_vec) = config.collective_set {
            for collective_set in collective_set_vec {
                let mut sources_list = collective_set.sources.unwrap_or_default();
                let mut sinks_list = collective_set.sinks.unwrap_or_default();
                if sources_list.is_empty() && sinks_list.is_empty() {
                    for _ in 0..collective_set.collective_count {
                        sources_list.push(Vec::default());
                        sinks_list.push(Vec::default());
                    }
                } else {
                    assert_eq!(
                        sources_list.len(),
                        collective_set.collective_count,
                        "Please specify {} sets of PacketSources for the collective set in the configuration file.",
                        collective_set.collective_count
                    );
                    assert_eq!(
                        sinks_list.len(),
                        collective_set.collective_count,
                        "Please specify {} sets of PacketSinks for the collective set in the configuration file.",
                        collective_set.collective_count
                    );
                }

                let mut first_flow_id = next_flow_id();
                if collective_set.first_flow_id.is_some() {
                    let new_first_flow_id = collective_set.first_flow_id.unwrap();
                    assert!(
                        new_first_flow_id >= first_flow_id,
                        "The specified first flow id {} of the collective set should be at least {}",
                        new_first_flow_id,
                        first_flow_id
                    );
                    first_flow_id = new_first_flow_id;
                }
                update_next_flow_id(
                    first_flow_id + collective_set.collective_count * collective_set.flow_count,
                );

                for index in 0..collective_set.collective_count {
                    let (sources, sinks) = Self::generate_endpoints(
                        collective_set.collective_type,
                        collective_set.flow_count,
                        &None,
                        sources_list.remove(0),
                        sinks_list.remove(0),
                        hosts,
                        rng.clone(),
                    );

                    let traffic = TrafficCharacteristics::clone(&collective_set.traffic);

                    let params = CollectiveParams {
                        id: next_collective_id(),
                        collective_type: collective_set.collective_type,
                        first_flow_id: first_flow_id + index * collective_set.flow_count,
                        flow_type: collective_set.flow_type,
                        flow_count: collective_set.flow_count,
                        graph: None,
                        paths: None,
                        sources,
                        sinks,
                        routing: collective_set.routing,
                        traffic,
                    };

                    collectives.push(Collective::new(params));
                }
            }
        }

        collectives
    }
}

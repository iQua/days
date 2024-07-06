use std::fs;

use petgraph::graph::DiGraph;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::Deserialize;

use crate::flows::flow::FlowType;
use crate::flows::{DistributionInfo, TomlTrafficCharacteristics, TrafficCharacteristics};
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

    pub traffic: TrafficCharacteristics,
}

impl Collective {
    pub fn new(
        id: usize,
        collective_type: CollectiveType,
        first_flow_id: usize,
        flow_type: FlowType,
        flow_count: usize,
        graph: Option<DiGraph<usize, ()>>,
        paths: Option<Vec<Vec<usize>>>,
        sources: Vec<usize>,
        sinks: Vec<usize>,
        traffic: TrafficCharacteristics,
    ) -> Collective {
        Collective {
            id,
            collective_type,
            first_flow_id,
            flow_type,
            flow_count,
            graph,
            paths,
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
        paths: Option<Vec<Vec<usize>>>,
        sources: Vec<Vec<usize>>,
        sinks: Vec<Vec<usize>>,
    ) -> Vec<Collective> {
        let mut collectives = Vec::new();

        for (index, graph) in graphs.iter().enumerate() {
            let collective_graph = Some(DiGraph::<usize, ()>::from_edges(graph));
            let collective_sources = sources[index].clone();
            let collective_sinks = sinks[index].clone();

            let first_flow_id = next_flow_id();
            update_next_flow_id(first_flow_id + flow_count);

            collectives.push(Collective::new(
                next_collective_id(),
                CollectiveType::AllReduce,
                first_flow_id,
                FlowType::PacketDistribution,
                flow_count,
                collective_graph,
                paths.clone(),
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

    /// Generates sources and sinks of flows of a collective.
    fn generate_endpoints(
        collective_type: CollectiveType,
        flow_count: usize,
        paths: &Option<Vec<Vec<usize>>>,
        mut sources: Vec<usize>,
        mut sinks: Vec<usize>,
        hosts: &Vec<usize>,
        mut rng: SmallRng,
    ) -> (Vec<usize>, Vec<usize>) {
        if let Some(flow_paths) = paths {
            assert_eq!(
                flow_paths.len(),
                flow_count,
                "The number of specified paths ({}) should be the same as flow count {}",
                flow_paths.len(),
                flow_count
            );

            let mut sources = Vec::new();
            let mut sinks = Vec::new();

            for path in flow_paths {
                sources.push(path[0]);
                sinks.push(path[path.len() - 1]);
            }
        } else if !sources.is_empty() && !sinks.is_empty() {
            match collective_type {
                CollectiveType::Broadcast => {
                    assert_eq!(
                                sources.len(),
                                1,
                                "Please only specify 1 PacketSource for flows of a Broadcast operation in the configuration file."
                            );
                    assert_eq!(
                                sinks.len(),
                                flow_count,
                                "Please specify {} PacketSinks for flows of a Broadcast operation in the configuration file.",
                                flow_count
                            );
                    for _ in 1..flow_count {
                        sources.push(sources[0]);
                    }
                }

                CollectiveType::Gather => {
                    assert_eq!(
                                sinks.len(),
                                1,
                                "Please only specify 1 PacketSink for flows of a Gather operation in the configuration file."
                            );
                    assert_eq!(
                                sources.len(),
                                flow_count,
                                "Please specify {} PacketSources for flows of a Gather operation in the configuration file.",
                                flow_count
                            );
                    for _ in 1..flow_count {
                        sinks.push(sinks[0]);
                    }
                }
                CollectiveType::AllReduce => {
                    assert_eq!(
                                sources.len(),
                                flow_count,
                                "Please specify {} PacketSources for flows of a AllReduce operation in the configuration file.",
                                flow_count
                            );
                    assert_eq!(
                                sinks.len(),
                                flow_count,
                                "Please specify {} PacketSinks for flows of a AllReduce operation in the configuration file.",
                                flow_count
                            );
                }
            }
        } else {
            match collective_type {
                CollectiveType::Broadcast => {
                    let source = hosts.choose(&mut rng).unwrap().clone();
                    let mut sink_hosts = hosts.clone();
                    sink_hosts.retain(|&x| x != source);
                    for _ in 0..flow_count {
                        sources.push(source);
                        sinks.push(sink_hosts.choose(&mut rng).unwrap().clone());
                    }
                }
                CollectiveType::Gather => {
                    let sink = hosts.choose(&mut rng).unwrap().clone();
                    let mut source_hosts = hosts.clone();
                    source_hosts.retain(|&x| x != sink);
                    for _ in 0..flow_count {
                        sources.push(source_hosts.choose(&mut rng).unwrap().clone());
                        sinks.push(sink);
                    }
                }
                CollectiveType::AllReduce => {
                    let sink = hosts.choose(&mut rng).unwrap().clone();
                    let mut source_hosts = hosts.clone();
                    source_hosts.retain(|&x| x != sink);
                    for _ in 0..flow_count {
                        sources.push(source_hosts.choose(&mut rng).unwrap().clone());
                        sinks.push(sink);
                    }
                }
            }
        }

        (sources, sinks)
    }

    /// Initializes collectives from a configuration file.
    pub fn collectives_from_config(file_path: &str, hosts: &Vec<usize>) -> Vec<Collective> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: CollectiveConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);

        let mut collectives = Vec::new();

        if let Some(collectives_vec) = config.collective {
            for collective in collectives_vec {
                let mut graph = None;
                if let Some(config_graph) = collective.graph {
                    graph = Some(DiGraph::<usize, ()>::from_edges(config_graph));
                }

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
                    if new_first_flow_id < first_flow_id {
                        panic!(
                            "The specified first flow id {} of the collective should be at least {}",
                            new_first_flow_id, first_flow_id
                        );
                    }
                    first_flow_id = new_first_flow_id;
                }
                update_next_flow_id(first_flow_id + collective.flow_count);

                collectives.push(Collective::new(
                    next_collective_id(),
                    collective.collective_type,
                    first_flow_id,
                    collective.flow_type,
                    collective.flow_count,
                    graph,
                    collective.paths,
                    sources,
                    sinks,
                    traffic,
                ));
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
                    if new_first_flow_id < first_flow_id {
                        panic!(
                            "The specified first flow id {} of the collective set should be at least {}",
                            new_first_flow_id, first_flow_id
                        );
                    }
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

                    collectives.push(Collective::new(
                        next_collective_id(),
                        collective_set.collective_type,
                        first_flow_id + index * collective_set.flow_count,
                        collective_set.flow_type,
                        collective_set.flow_count,
                        None,
                        None,
                        sources,
                        sinks,
                        traffic,
                    ));
                }
            }
        }

        collectives
    }
}

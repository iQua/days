use std::collections::HashMap;
use std::fs;

use petgraph::graph::{DiGraph, UnGraph};
use serde::Deserialize;

use crate::flows::flow::Flow;
use crate::flows::route::RandomSimplePath;
use crate::flows::DistributionInfo;
use crate::next_collective_id;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum CollectiveType {
    Broadcast,
    Gather,
    AllReduce,
}

#[derive(Deserialize, Debug)]
struct TomlCollective {
    collective_type: CollectiveType,
    graph: Vec<(u32, u32)>,
    sources: Vec<usize>,
    sinks: Vec<usize>,
    initial_delay: f64,
    duration: f64,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
}

#[derive(Debug)]
pub struct Collective {
    pub id: usize,
    pub collective_type: CollectiveType,
    pub graph: DiGraph<usize, ()>,
    pub sources: Vec<usize>,
    pub sinks: Vec<usize>,
    pub initial_delay: f64,
    pub duration: f64,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub flows: Vec<Flow>,
    pub routing: RandomSimplePath,
}

#[derive(Deserialize, Debug)]
struct CollectiveConfig {
    collective: Option<Vec<TomlCollective>>,
}

impl Collective {
    pub fn new(
        id: usize,
        collective_type: CollectiveType,
        graph: DiGraph<usize, ()>,
        sources: Vec<usize>,
        sinks: Vec<usize>,
        initial_delay: f64,
        duration: f64,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> Collective {
        let routing = RandomSimplePath::new(UnGraph::<usize, ()>::new_undirected().clone());

        Collective {
            id,
            collective_type,
            graph,
            sources,
            sinks,
            initial_delay,
            duration,
            arr_dist,
            pkt_size_dist,
            routing,
            flows: Vec::new(),
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
                collective_graph,
                collective_sources,
                collective_sinks,
                0.,
                10.,
                DistributionInfo::Exp { lambda: 1. },
                DistributionInfo::Uniform {
                    low: 1000,
                    high: 1000,
                },
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
                    graph,
                    collective.sources,
                    collective.sinks,
                    collective.initial_delay,
                    collective.duration,
                    collective.arr_dist,
                    collective.pkt_size_dist,
                ));
            }
        }

        collectives
    }
}

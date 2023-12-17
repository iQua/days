use std::collections::HashMap;
use std::fs;

use petgraph::graph::{DiGraph, NodeIndex, UnGraph};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;

use crate::flows::route::{RandomSimplePath, RoutingProtocol};
use crate::topos::build::FatTreeConfig;
use crate::{next_flow_id, seed_from_config};

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

#[derive(Deserialize, Debug)]
struct TomlCollectiveSet {
    collective_type: CollectiveType,
    collective_size: usize,
    collective_count: u32,
    initial_delay: f64,
    duration: f64,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
}

#[derive(Deserialize, Debug)]
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

    /// flow_id -> Flow
    pub flows: HashMap<usize, Flow>,
    /// routing protocol
    pub routing: RandomSimplePath,
}

#[derive(Deserialize, Debug)]
struct CollectiveConfig {
    collective: Option<Vec<TomlCollective>>,
    collective_set: Option<Vec<TomlCollectiveSet>>,
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
            flow: HashMap::new(),
        }
    }

    // Initializes collectives from a vector of directed graphs, each graph
    // corresponding to one collective.
    pub fn collectives_from_graph(
        graphs: Vec<Vec<(u32, u32)>>,
        sources: Vec<Vec<usize>>,
        sinks: Vec<Vec<usize>>,
    ) -> Vec<Flow> {
        let mut collectives = Vec::new();

        for (collective_id, graph) in graphs.iter().enumerate() {
            let collective_graph = DiGraph::<usize, ()>::from_edges(graph);
            let collective_sources = sources[flow_index].clone();
            let collective_sinks = sinks[flow_index].clone();

            collectives.push(Flow::new(
                collective_id,
                FlowType::PacketDistribution,
                flow_graph,
                flow_sources,
                flow_sinks,
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
}

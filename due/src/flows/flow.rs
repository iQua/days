use std::collections::HashMap;
use std::fs;

use petgraph::graph::{DiGraph, NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;

use crate::flows::route::{RandomSimplePath, RoutingProtocol};
use crate::topos::build::FatTreeConfig;
use crate::{next_flow_id, seed_from_config};

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum FlowType {
    PacketDistribution,
    TCP,
}

#[derive(Deserialize, Debug)]
struct TomlFlow {
    flow_type: FlowType,
    graph: Vec<(u32, u32)>,
    sources: Option<Vec<usize>>,
    sinks: Option<Vec<usize>>,
    initial_delay: f64,
    duration: f64,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
}

#[derive(Deserialize, Debug)]
struct TomlFlowSet {
    flow_type: FlowType,
    flow_count: u32,
    initial_delay: f64,
    duration: f64,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
}

#[derive(Deserialize, Debug)]
struct FlowConfig {
    flow: Option<Vec<TomlFlow>>,
    flow_set: Option<Vec<TomlFlowSet>>,
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
    pub sources: Option<Vec<usize>>,
    pub sinks: Option<Vec<usize>>,
    pub initial_delay: f64,
    pub duration: f64,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub routing: RandomSimplePath,

    // edge index -> sink id
    pub sink_ids: HashMap<usize, usize>,
}

impl Flow {
    pub fn new(
        id: usize,
        flow_type: FlowType,
        graph: DiGraph<usize, ()>,
        sources: Option<Vec<usize>>,
        sinks: Option<Vec<usize>>,
        initial_delay: f64,
        duration: f64,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> Flow {
        let routing = RandomSimplePath::new(UnGraph::<usize, ()>::new_undirected().clone());
        Flow {
            id,
            flow_type,
            graph,
            sources,
            sinks,
            initial_delay,
            duration,
            arr_dist,
            pkt_size_dist,
            routing,
            sink_ids: HashMap::new(),
        }
    }

    // Initializes flows from a vector of directed graphs.
    pub fn flows_from_graph(graphs: Vec<Vec<(u32, u32)>>) -> Vec<Flow> {
        let mut flows = Vec::new();

        for graph in graphs {
            let flow_graph = DiGraph::<usize, ()>::from_edges(&graph);

            flows.push(Flow::new(
                next_flow_id(),
                FlowType::PacketDistribution,
                flow_graph,
                None,
                None,
                0.,
                10.,
                DistributionInfo::Exp { lambda: 1. },
                DistributionInfo::Uniform {
                    low: 1000,
                    high: 1000,
                },
            ));
        }

        flows
    }

    // Initializes flows from a configuration file.
    pub fn flows_from_config(file_path: &str) -> Vec<Flow> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: FlowConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut flows = Vec::new();

        if let Some(flows_vec) = config.flow {
            for flow in flows_vec {
                let graph = DiGraph::<usize, ()>::from_edges(flow.graph);

                flows.push(Flow::new(
                    next_flow_id(),
                    flow.flow_type,
                    graph,
                    flow.sources,
                    flow.sinks,
                    flow.initial_delay,
                    flow.duration,
                    flow.arr_dist,
                    flow.pkt_size_dist,
                ));
            }
        }

        if let Some(flow_set_vec) = config.flow_set {
            let fattree_config: FatTreeConfig =
                toml::from_str(&content).expect("Failed to deserialize the configuration");
            let num_edge_switches = fattree_config.k.pow(2) / 2;
            let mut rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);

            for flow_set in flow_set_vec {
                for _ in 0..flow_set.flow_count {
                    let start = rng.gen_range(0..num_edge_switches);
                    let end = {
                        let mut range = (0..start).chain((start + 1)..num_edge_switches);
                        range.nth(rng.gen_range(0..num_edge_switches - 1)).unwrap()
                    };
                    let graph = DiGraph::<usize, ()>::from_edges(vec![(
                        NodeIndex::new(start),
                        NodeIndex::new(end),
                    )]);

                    flows.push(Flow::new(
                        next_flow_id(),
                        flow_set.flow_type,
                        graph,
                        None,
                        None,
                        flow_set.initial_delay,
                        flow_set.duration,
                        flow_set.arr_dist,
                        flow_set.pkt_size_dist,
                    ));
                }
            }
        }

        flows
    }

    // Gets the simple paths for all edges of the flow
    pub fn compute_paths(&mut self, graph: UnGraph<usize, ()>) -> Vec<Vec<NodeIndex>> {
        // sets the routing protocol
        self.routing = RandomSimplePath::new(graph);

        let mut paths = Vec::new();

        for (edge_index, edge) in self.graph.edge_references().enumerate() {
            let mut path = self.routing.compute_route(edge.source(), edge.target());
            let sink_id = self.sink_ids[&edge_index];
            path.push(NodeIndex::new(sink_id));
            paths.push(path);
        }

        paths
    }
}

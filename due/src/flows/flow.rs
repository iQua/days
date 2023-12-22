use std::fs;

use petgraph::graph::{DiGraph, NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::Deserialize;

use crate::flows::route::{RandomSimplePath, RoutingProtocol};
use crate::flows::{DistributionInfo, TrafficCharacteristics};
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
    traffic: TrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct TomlFlowSet {
    flow_type: FlowType,
    flow_count: u32,
    traffic: TrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct FlowConfig {
    flow: Option<Vec<TomlFlow>>,
    flow_set: Option<Vec<TomlFlowSet>>,
}

/// A flow represents a directed edge with one packet source and one packet sink.
#[derive(Debug)]
pub struct Flow {
    pub id: usize,
    pub flow_type: FlowType,
    /// the id of the host switch that the source attaches to
    pub source_host: usize,
    /// the id of the host switch that the sink attaches to
    pub sink_host: usize,
    /// the id of PacketSink
    pub sink_id: usize,
    pub traffic: TrafficCharacteristics,
    /// random seed for the packet source
    pub seed: usize,
    pub routing: RandomSimplePath,
}

impl Flow {
    pub fn new(
        id: usize,
        flow_type: FlowType,
        source_host: usize,
        sink_host: usize,
        traffic: TrafficCharacteristics,
        seed: usize,
    ) -> Flow {
        let routing = RandomSimplePath::new(UnGraph::<usize, ()>::new_undirected().clone());

        Flow {
            id,
            flow_type,
            source_host,
            sink_host,
            sink_id: 0,
            traffic,
            seed,
            routing,
        }
    }

    // Initializes flows from a vector of directed graphs. Each directed graph
    // only has one edge from the packet source to the packet sink.
    pub fn flows_from_graph(graphs: Vec<Vec<(u32, u32)>>) -> Vec<Flow> {
        let mut flows = Vec::new();

        for (_, graph) in graphs.iter().enumerate() {
            let flow_graph = DiGraph::<usize, ()>::from_edges(graph);
            assert!(flow_graph.edge_references().len() == 1);

            for (_, edge) in flow_graph.edge_references().enumerate() {
                flows.push(Flow::new(
                    next_flow_id(),
                    FlowType::PacketDistribution,
                    edge.source().index(),
                    edge.target().index(),
                    TrafficCharacteristics::new(
                        1.,
                        10.,
                        DistributionInfo::Exp { lambda: 1. },
                        DistributionInfo::Uniform {
                            low: 1000,
                            high: 1000,
                        },
                    ),
                    0,
                ));
            }
        }

        flows
    }

    // Initializes flows from a configuration file.
    pub fn flows_from_config(file_path: &str, switch_count: usize) -> Vec<Flow> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: FlowConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut flows = Vec::new();

        if let Some(flows_vec) = config.flow {
            for flow in flows_vec {
                let graph = DiGraph::<usize, ()>::from_edges(flow.graph);
                assert!(graph.edge_references().len() == 1);

                for (_, edge) in graph.edge_references().enumerate() {
                    let flow_id = next_flow_id();

                    flows.push(Flow::new(
                        flow_id,
                        flow.flow_type,
                        edge.source().index(),
                        edge.target().index(),
                        flow.traffic,
                        // uses flow_id as the random seed (added to the global seed)
                        flow_id,
                    ));
                }
            }
        }

        if let Some(flow_set_vec) = config.flow_set {
            let mut rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);

            for flow_set in flow_set_vec {
                for _ in 0..flow_set.flow_count {
                    let start = rng.gen_range(0..switch_count);
                    let end = {
                        let mut range = (0..start).chain((start + 1)..switch_count);
                        range.nth(rng.gen_range(0..switch_count - 1)).unwrap()
                    };

                    let flow_id = next_flow_id();
                    flows.push(Flow::new(
                        flow_id,
                        flow_set.flow_type,
                        start,
                        end,
                        flow_set.traffic,
                        // uses flow_id as the random seed (added to the global seed)
                        flow_id,
                    ));
                }
            }
        }

        flows
    }

    // Given the network graph, computes the path from the packet source to the sink in the flow
    pub fn compute_path(&mut self, graph: UnGraph<usize, ()>) -> Vec<NodeIndex> {
        self.routing = RandomSimplePath::new(graph);

        let mut path = self.routing.compute_route(
            NodeIndex::new(self.source_host),
            NodeIndex::new(self.sink_host),
        );
        path.push(NodeIndex::new(self.sink_id));

        path
    }
}

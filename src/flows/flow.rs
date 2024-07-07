use std::fs;

use petgraph::graph::{DiGraph, NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use serde::Deserialize;

use crate::flows::route::{PathFromConfig, Routing, RoutingProtocol, ShortestPath};
use crate::flows::{DistributionInfo, TomlTrafficCharacteristics, TrafficCharacteristics};
use crate::{next_flow_id, seed_from_config, update_next_flow_id};

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum FlowType {
    PacketDistribution,
    TCP,
}

#[derive(Deserialize, Debug)]
struct TomlFlow {
    flow_id: Option<usize>,
    starts_before: Option<Vec<usize>>,
    starts_after: Option<Vec<usize>>,
    flow_type: FlowType,
    graph: Vec<(u32, u32)>,
    path: Option<Vec<usize>>,
    traffic: TomlTrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct TomlFlowSet {
    first_flow_id: Option<usize>,
    starts_before: Option<Vec<usize>>,
    starts_after: Option<Vec<usize>>,
    flow_type: FlowType,
    flow_count: u32,
    traffic: TomlTrafficCharacteristics,
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
    /// the ids of flows that can only start after this flow ends
    pub starts_before: Vec<usize>,
    /// the ids of flows that this flow must wait for them to finish before it
    /// starts
    pub starts_after: Vec<usize>,
    pub flow_type: FlowType,
    /// the id of the host switch that the source attaches to
    pub source_host: usize,
    /// the id of the host switch that the sink attaches to
    pub sink_host: usize,
    /// the id of PacketSource
    pub source_id: usize,
    /// the id of PacketSink
    pub sink_id: usize,
    /// traffic characteristics of the flow
    pub traffic: TrafficCharacteristics,
    /// random seed for the packet source
    pub seed: usize,
    /// routing protocol
    pub routing: Routing,
}

impl Flow {
    pub fn new(
        id: usize,
        path: Option<Vec<usize>>,
        starts_before: Vec<usize>,
        starts_after: Vec<usize>,
        flow_type: FlowType,
        source_host: usize,
        sink_host: usize,
        traffic: TrafficCharacteristics,
        seed: usize,
    ) -> Flow {
        let mut routing = Routing::ShortestPath(ShortestPath::new(
            UnGraph::<usize, ()>::new_undirected().clone(),
        ));
        if let Some(path_from_config) = path {
            routing = Routing::PathFromConfig(PathFromConfig::new(path_from_config));
        }

        Flow {
            id,
            starts_before,
            starts_after,
            flow_type,
            source_host,
            sink_host,
            source_id: 0,
            sink_id: 0,
            traffic,
            seed,
            routing,
        }
    }

    /// Initializes flows from a vector of directed graphs. Each directed graph
    /// only has one edge from the packet source to the packet sink.
    pub fn flows_from_graph(graphs: Vec<Vec<(u32, u32)>>) -> Vec<Flow> {
        let mut flows = Vec::new();

        for (_, graph) in graphs.iter().enumerate() {
            let flow_graph = DiGraph::<usize, ()>::from_edges(graph);
            assert!(flow_graph.edge_references().len() == 1);

            for (_, edge) in flow_graph.edge_references().enumerate() {
                flows.push(Flow::new(
                    next_flow_id(),
                    None,
                    Vec::new(),
                    Vec::new(),
                    FlowType::PacketDistribution,
                    edge.source().index(),
                    edge.target().index(),
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
                    0,
                ));
            }
        }

        flows
    }

    /// Initializes flows from a configuration file.
    pub fn flows_from_config(file_path: &str, hosts: &Vec<usize>) -> Vec<Flow> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let flow_config: FlowConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut flows = Vec::new();

        if let Some(flows_vec) = flow_config.flow {
            for flow in flows_vec {
                let graph = DiGraph::<usize, ()>::from_edges(&flow.graph);
                assert!(graph.edge_references().len() == 1);

                for (_, edge) in graph.edge_references().enumerate() {
                    let mut flow_id = next_flow_id();
                    if flow.flow_id.is_some() {
                        let new_id = flow.flow_id.unwrap();
                        assert!(
                            new_id >= flow_id,
                            "The specified flow id {} should be at least {}",
                            new_id,
                            flow_id
                        );
                        update_next_flow_id(new_id + 1);
                        flow_id = new_id;
                    }

                    if let Some(ref path) = flow.path {
                        let (source_host, sink_host) = &flow.graph[0];
                        assert!(
                            path[0] == *source_host as usize,
                            "Flow {}'s source specified in path ({}) should be the same as it in graph ({})",
                            flow_id,
                            path[0],
                            source_host
                        );
                        assert!(
                            path[path.len() - 1] == *sink_host as usize,
                            "Flow {}'s sink specified in path ({}) should be the same as it in graph ({})",
                            flow_id,
                            path[path.len() - 1],
                            sink_host
                        );
                    }

                    let starts_before = flow.starts_before.clone().unwrap_or_default();
                    let starts_after = flow.starts_after.clone().unwrap_or_default();
                    let traffic = TrafficCharacteristics::clone(&flow.traffic);

                    flows.push(Flow::new(
                        flow_id,
                        flow.path.clone(),
                        starts_before,
                        starts_after,
                        flow.flow_type,
                        edge.source().index(),
                        edge.target().index(),
                        traffic,
                        // uses flow_id as the random seed (added to the global seed)
                        flow_id,
                    ));
                }
            }
        }

        if let Some(flow_set_vec) = flow_config.flow_set {
            let mut rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);

            for flow_set in flow_set_vec {
                let mut first_flow_id = next_flow_id();
                if flow_set.first_flow_id.is_some() {
                    let new_first_flow_id = flow_set.first_flow_id.unwrap();
                    assert!(
                        new_first_flow_id >= first_flow_id,
                        "The specified first flow id {} of the flow set should be at least {}",
                        new_first_flow_id,
                        first_flow_id
                    );
                    first_flow_id = new_first_flow_id;
                }

                for id_counter in 0..flow_set.flow_count {
                    let host_pair: Vec<usize> =
                        hosts.choose_multiple(&mut rng, 2).cloned().collect();

                    let flow_id = first_flow_id + id_counter as usize;
                    let starts_before = flow_set.starts_before.clone().unwrap_or_default();
                    let starts_after = flow_set.starts_after.clone().unwrap_or_default();
                    let traffic = TrafficCharacteristics::clone(&flow_set.traffic);

                    flows.push(Flow::new(
                        flow_id,
                        None,
                        starts_before,
                        starts_after,
                        flow_set.flow_type,
                        host_pair[0],
                        host_pair[1],
                        traffic,
                        // uses flow_id as the random seed (added to the global seed)
                        flow_id,
                    ));
                }
                update_next_flow_id(first_flow_id + flow_set.flow_count as usize);
            }
        }

        flows
    }

    /// Given the network graph, computes the path from the PacketSource to the
    /// PacketSink in the flow.
    pub fn compute_path(&mut self, graph: UnGraph<usize, ()>) -> Vec<NodeIndex> {
        match &self.routing {
            Routing::ShortestPath(_) => {
                let mut routing = ShortestPath::new(graph);
                let mut path = vec![NodeIndex::new(self.source_id)];

                path.append(&mut routing.compute_route(
                    NodeIndex::new(self.source_host),
                    NodeIndex::new(self.sink_host),
                ));

                path.push(NodeIndex::new(self.sink_id));

                path
            }
            Routing::PathFromConfig(routing) => {
                let mut path = vec![NodeIndex::new(self.source_id)];
                path.append(&mut routing.path.clone());
                path.push(NodeIndex::new(self.sink_id));

                path
            }
        }
    }
}

//! Implements a network flow with a source and a sink, and with its configurations
//! and traffic characteristics.

use std::fs;

use petgraph::graph::{DiGraph, NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::SmallRng;
use serde::Deserialize;

use crate::flows::route::{ECMP, PathFromConfig, Routing, RoutingConfig, ShortestPath};
use crate::flows::{TomlTrafficCharacteristics, TrafficCharacteristics};
use crate::{next_flow_id, seed_from_config, update_next_flow_id};

/// Represents the type of a flow.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum FlowType {
    /// Represents a packet distribution flow.
    PacketDistribution,
    /// Represents a TCP flow.
    TCP,
    /// Represents a DCQCN flow.
    #[cfg(feature = "dcqcn")]
    DCQCN,
}

#[derive(Deserialize, Debug)]
struct TomlFlow {
    flow_id: Option<usize>,
    starts_before: Option<Vec<usize>>,
    starts_after: Option<Vec<usize>>,
    flow_type: FlowType,
    priority: Option<u8>,
    graph: Vec<(u32, u32)>,
    routing: Option<RoutingConfig>,
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
    priority: Option<u8>,
    routing: Option<RoutingConfig>,
    traffic: TomlTrafficCharacteristics,
}

#[derive(Deserialize, Debug)]
struct FlowConfig {
    flow: Option<Vec<TomlFlow>>,
    flow_set: Option<Vec<TomlFlowSet>>,
}

/// Parameters required to initialize a `Flow`.
#[derive(Debug, Clone)]
pub struct FlowParams {
    /// Unique identifier for the flow.
    pub id: usize,
    /// Optional path for the flow.
    pub path: Option<Vec<usize>>,
    /// List of flow IDs that must start before this flow.
    pub starts_before: Vec<usize>,
    /// List of flow IDs that this flow must wait for before starting.
    pub starts_after: Vec<usize>,
    /// Type of the flow.
    pub flow_type: FlowType,
    /// ID of the source host.
    pub source_host: usize,
    /// ID of the sink host.
    pub sink_host: usize,
    /// Optional routing configuration.
    pub routing: Option<RoutingConfig>,
    /// Traffic characteristics of the flow.
    pub traffic: TrafficCharacteristics,
    /// 802.1Q priority code point (0-7).
    pub priority: u8,
    /// Random seed for the packet source.
    pub seed: usize,
}

/// Represents a network flow with its configurations and characteristics.
#[derive(Debug)]
pub struct Flow {
    /// Unique identifier for the flow.
    pub id: usize,
    /// IDs of flows that can only start after this flow ends.
    pub starts_before: Vec<usize>,
    /// IDs of flows that this flow must wait for before starting.
    pub starts_after: Vec<usize>,
    /// Type of the flow.
    pub flow_type: FlowType,
    /// ID of the host switch that the source attaches to.
    pub source_host: usize,
    /// ID of the host switch that the sink attaches to.
    pub sink_host: usize,
    /// ID of the PacketSource.
    pub source_id: usize,
    /// ID of the PacketSink.
    pub sink_id: usize,
    /// Routing protocol used by the flow.
    pub routing: Routing,
    /// Traffic characteristics of the flow.
    pub traffic: TrafficCharacteristics,
    /// 802.1Q priority code point (0-7).
    pub priority: u8,
    /// Random seed for the packet source.
    pub seed: usize,
}

fn checked_priority(priority: Option<u8>) -> u8 {
    let priority = priority.unwrap_or(0);
    assert!(
        priority <= 7,
        "Flow priority must be within 0..=7, got {}",
        priority
    );
    priority
}

impl Flow {
    /// Creates a new `Flow` instance based on the provided parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - A `FlowParams` struct containing initialization parameters.
    ///
    /// # Returns
    ///
    /// * A new `Flow` instance.
    pub fn new(params: FlowParams) -> Flow {
        let mut routing;

        match params.routing {
            Some(RoutingConfig::ShortestPath) => {
                routing = Routing::ShortestPath(ShortestPath::new(
                    UnGraph::<usize, ()>::new_undirected().clone(),
                ));
            }
            Some(RoutingConfig::ECMP) => {
                routing = Routing::ECMP(ECMP::new(
                    UnGraph::<usize, ()>::new_undirected().clone(),
                    params.id,
                    params.source_host,
                    params.sink_host,
                ));
            }
            _ => {
                routing = Routing::ShortestPath(ShortestPath::new(
                    UnGraph::<usize, ()>::new_undirected().clone(),
                ));
            }
        }

        if let Some(path_from_config) = params.path {
            routing = Routing::PathFromConfig(PathFromConfig::new(path_from_config));
        }

        Flow {
            id: params.id,
            starts_before: params.starts_before,
            starts_after: params.starts_after,
            flow_type: params.flow_type,
            source_host: params.source_host,
            sink_host: params.sink_host,
            source_id: 0,
            sink_id: 0,
            traffic: params.traffic,
            priority: params.priority,
            seed: params.seed,
            routing,
        }
    }

    /// Initializes flows from a vector of directed graphs.
    ///
    /// Each directed graph should contain only one edge from the packet source to the packet sink.
    ///
    /// # Arguments
    ///
    /// * `graphs` - A vector of directed graphs represented as vectors of edge tuples.
    ///
    /// # Returns
    ///
    /// * A vector of initialized `Flow` instances.
    pub fn flows_from_graph(graphs: Vec<Vec<(u32, u32)>>) -> Vec<Flow> {
        let mut flows = Vec::new();

        for graph in graphs.iter() {
            let flow_graph = DiGraph::<usize, ()>::from_edges(graph);
            assert!(
                flow_graph.edge_references().len() == 1,
                "Each graph should contain exactly one edge."
            );

            for edge in flow_graph.edge_references() {
                let params = FlowParams {
                    id: next_flow_id(),
                    path: None,
                    starts_before: Vec::new(),
                    starts_after: Vec::new(),
                    flow_type: FlowType::PacketDistribution,
                    source_host: edge.source().index(),
                    sink_host: edge.target().index(),
                    routing: Some(RoutingConfig::PathFromConfig),
                    traffic: TrafficCharacteristics::default(),
                    priority: 0,
                    seed: 0,
                };
                flows.push(Flow::new(params));
            }
        }

        flows
    }

    /// Initializes flows from a configuration file.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the configuration file.
    /// * `hosts` - Slice of host IDs available in the network.
    ///
    /// # Returns
    ///
    /// * A vector of initialized `Flow` instances.
    pub fn flows_from_config(file_path: &str, hosts: &[usize]) -> Vec<Flow> {
        let content =
            fs::read_to_string(file_path).expect("The configuration file could not be read.");

        let flow_config: FlowConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration.");

        let mut flows = Vec::new();

        if let Some(flows_vec) = flow_config.flow {
            for flow in flows_vec {
                let graph = DiGraph::<usize, ()>::from_edges(&flow.graph);
                assert!(
                    graph.edge_references().len() == 1,
                    "Each graph should contain exactly one edge."
                );

                for edge in graph.edge_references() {
                    let mut flow_id = next_flow_id();
                    if let Some(new_id) = flow.flow_id {
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
                            "Flow {}'s source specified in path ({}) should match graph ({})",
                            flow_id,
                            path[0],
                            source_host
                        );
                        assert!(
                            path[path.len() - 1] == *sink_host as usize,
                            "Flow {}'s sink specified in path ({}) should match graph ({})",
                            flow_id,
                            path[path.len() - 1],
                            sink_host
                        );
                    }

                    let starts_before = flow.starts_before.clone().unwrap_or_default();
                    let starts_after = flow.starts_after.clone().unwrap_or_default();
                    let traffic = TrafficCharacteristics::clone(&flow.traffic);
                    let priority = checked_priority(flow.priority);

                    flows.push(Flow::new(FlowParams {
                        id: flow_id,
                        path: flow.path.clone(),
                        starts_before,
                        starts_after,
                        flow_type: flow.flow_type.clone(),
                        source_host: edge.source().index(),
                        sink_host: edge.target().index(),
                        routing: flow.routing.clone(),
                        traffic,
                        priority,
                        seed: flow_id,
                    }));
                }
            }
        }

        if let Some(flow_set_vec) = flow_config.flow_set {
            let mut rng = SmallRng::seed_from_u64(seed_from_config(file_path) as u64);

            for flow_set in flow_set_vec {
                let mut first_flow_id = next_flow_id();
                if let Some(new_first_flow_id) = flow_set.first_flow_id {
                    assert!(
                        new_first_flow_id >= first_flow_id,
                        "The specified first flow id {} of the flow set should be at least {}",
                        new_first_flow_id,
                        first_flow_id
                    );
                    first_flow_id = new_first_flow_id;
                }
                let priority = checked_priority(flow_set.priority);

                for id_counter in 0..flow_set.flow_count {
                    let host_pair: Vec<usize> = hosts.sample(&mut rng, 2).cloned().collect();

                    let flow_id = first_flow_id + id_counter as usize;
                    let starts_before = flow_set.starts_before.clone().unwrap_or_default();
                    let starts_after = flow_set.starts_after.clone().unwrap_or_default();
                    let traffic = TrafficCharacteristics::clone(&flow_set.traffic);

                    flows.push(Flow::new(FlowParams {
                        id: flow_id,
                        path: None,
                        starts_before,
                        starts_after,
                        flow_type: flow_set.flow_type.clone(),
                        source_host: host_pair[0],
                        sink_host: host_pair[1],
                        routing: flow_set.routing.clone(),
                        traffic,
                        priority,
                        seed: flow_id,
                    }));
                }
                update_next_flow_id(first_flow_id + flow_set.flow_count as usize);
            }
        }

        flows
    }

    /// Computes the routing path for the flow based on the provided network graph.
    ///
    /// Available routing protocols:
    /// - Shortest path routing
    /// - Custom routing using the path from the configuration file
    /// - Equal-Cost Multi-Path (ECMP) routing
    ///
    /// # Arguments
    ///
    /// * `graph` - The network graph as an undirected graph.
    ///
    /// # Returns
    ///
    /// * A vector of `NodeIndex` representing the path from source to sink.
    pub fn compute_path(&self, graph: &UnGraph<usize, ()>) -> Vec<NodeIndex> {
        match &self.routing {
            Routing::ShortestPath(_) => {
                let mut path = vec![NodeIndex::new(self.source_id)];

                path.append(&mut ShortestPath::compute_route_in(
                    graph,
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
            Routing::ECMP(_) => {
                let mut path = vec![NodeIndex::new(self.source_id)];

                path.append(&mut ECMP::compute_route_in(
                    graph,
                    self.id,
                    self.source_host,
                    self.sink_host,
                    NodeIndex::new(self.source_host),
                    NodeIndex::new(self.sink_host),
                ));

                path.push(NodeIndex::new(self.sink_id));

                path
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::UnGraph;

    /// Helper function to create a simple undirected graph.
    fn create_graph(edges: &[(u32, u32)]) -> UnGraph<usize, ()> {
        UnGraph::<usize, ()>::from_edges(edges)
    }

    #[test]
    fn test_flows_from_graph_single_edge() {
        let graphs = vec![vec![(0, 1)], vec![(2, 3)], vec![(4, 5)]];

        let flows = Flow::flows_from_graph(graphs.clone());

        assert_eq!(flows.len(), graphs.len());

        for (i, flow) in flows.iter().enumerate() {
            assert_eq!(flow.source_host, graphs[i][0].0 as usize);
            assert_eq!(flow.sink_host, graphs[i][0].1 as usize);
            assert_eq!(flow.flow_type, FlowType::PacketDistribution);
        }
    }

    #[test]
    #[should_panic(expected = "Each graph should contain exactly one edge.")]
    fn test_flows_from_graph_multiple_edges() {
        let graphs = vec![
            vec![(0, 1), (1, 2)], // This graph has two edges and should panic
        ];

        Flow::flows_from_graph(graphs);
    }

    #[test]
    fn test_compute_shortest_path() {
        let flow = Flow {
            id: 1,
            starts_before: vec![],
            starts_after: vec![],
            flow_type: FlowType::PacketDistribution,
            source_host: 0,
            sink_host: 3,
            source_id: 0,
            sink_id: 3,
            routing: Routing::ShortestPath(ShortestPath::new(
                UnGraph::<usize, ()>::new_undirected(),
            )),
            traffic: TrafficCharacteristics::default(),
            priority: 0,
            seed: 0,
        };

        let graph = create_graph(&[(0, 1), (1, 2), (2, 3), (0, 3)]);

        let path = flow.compute_path(&graph);

        // Assuming the shortest path is direct from 0 to 3
        assert_eq!(path.len(), 4); // source_id, nodes en route, sink_id
        assert_eq!(path[0], NodeIndex::new(0));
        assert_eq!(path[path.len() - 1], NodeIndex::new(3));
    }

    #[test]
    fn test_compute_ecmp_path() {
        let flow = Flow {
            id: 2,
            starts_before: vec![],
            starts_after: vec![],
            flow_type: FlowType::TCP,
            source_host: 0,
            sink_host: 3,
            source_id: 0,
            sink_id: 3,
            routing: Routing::ECMP(ECMP::new(
                create_graph(&[(0, 1), (1, 3), (0, 2), (2, 3)]),
                2,
                0,
                3,
            )),
            traffic: TrafficCharacteristics::default(),
            priority: 0,
            seed: 0,
        };

        let graph = create_graph(&[(0, 1), (1, 3), (0, 2), (2, 3)]);

        let path = flow.compute_path(&graph);

        // ECMP may choose either path 0-1-3 or 0-2-3
        assert_eq!(path.len(), 5); // source_id, nodes en route, sink_id
        assert_eq!(path[0], NodeIndex::new(0));
        assert_eq!(path[path.len() - 1], NodeIndex::new(3));

        let intermediate = path[2].index();
        assert!(intermediate == 1 || intermediate == 2);
    }

    #[test]
    fn test_compute_path_from_config() {
        let flow = Flow {
            id: 3,
            starts_before: vec![],
            starts_after: vec![],
            flow_type: FlowType::PacketDistribution,
            source_host: 0,
            sink_host: 4,
            source_id: 0,
            sink_id: 4,
            routing: Routing::PathFromConfig(PathFromConfig::new(vec![1, 2, 3])),
            traffic: TrafficCharacteristics::default(),
            priority: 0,
            seed: 0,
        };

        let graph = create_graph(&[(0, 1), (1, 2), (2, 3), (3, 4)]);

        let path = flow.compute_path(&graph);

        assert_eq!(path.len(), 5);
        assert_eq!(path[0], NodeIndex::new(0));
        assert_eq!(path[1], NodeIndex::new(1));
        assert_eq!(path[2], NodeIndex::new(2));
        assert_eq!(path[3], NodeIndex::new(3));
        assert_eq!(path[4], NodeIndex::new(4));
    }

    #[test]
    fn test_flows_from_config_flow() {
        // Assuming a valid TOML configuration string
        let toml_content = r#"
            seed = 1

            [[flow]]
            starts_before = [11]
            starts_after = [12]
            flow_type = "TCP"
            graph = [[0, 1]]
            routing = "ShortestPath"
            path = [0, 1]
            [flow.traffic]
                initial_delay = 1.0
                size = 10000
                arr_dist = {type = "Exp", lambda = 1.0}
                pkt_size_dist = {type = "Uniform", low = 1000, high = 1500}

            [[flow_set]]
            first_flow_id = 20
            flow_type = "PacketDistribution"
            flow_count = 2
            [flow_set.traffic]
                initial_delay = 1.0
                size = 10000
                arr_dist = {type = "Exp", lambda = 1.0}
                pkt_size_dist = {type = "Uniform", low = 1000, high = 1500}
        "#;

        // Mock the file reading by using a temporary file or by refactoring the code to accept TOML strings.
        // Here, we'll assume the traffic characteristics can be defaulted for simplicity.

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let hosts = vec![0, 1, 2, 3, 4];
        let flows = Flow::flows_from_config(temp_file.path().to_str().unwrap(), &hosts);

        // Expecting 3 flows: 1 from [[flow]] and 2 from [[flow_set]]
        assert_eq!(flows.len(), 3);

        // First flow
        let flow1 = &flows[0];
        assert_eq!(flow1.starts_before, vec![11]);
        assert_eq!(flow1.starts_after, vec![12]);
        assert_eq!(flow1.flow_type, FlowType::TCP);
        assert_eq!(flow1.source_host, 0);
        assert_eq!(flow1.sink_host, 1);
        assert_eq!(
            flow1.routing,
            Routing::PathFromConfig(PathFromConfig::new(vec![0, 1]))
        );

        // Next two flows from flow_set
        let flow2 = &flows[1];
        let flow3 = &flows[2];
        assert_eq!(flow2.flow_type, FlowType::PacketDistribution);
        assert_eq!(flow3.flow_type, FlowType::PacketDistribution);
    }

    #[test]
    fn test_flows_from_config_empty_path_panics() {
        let toml_content = r#"
            seed = 1

            [[flow]]
            flow_type = "TCP"
            graph = [[0, 1]]
            routing = "PathFromConfig"
            path = []
            [flow.traffic]
                initial_delay = 1.0
                size = 10000
                arr_dist = {type = "Exp", lambda = 1.0}
                pkt_size_dist = {type = "Uniform", low = 1000, high = 1500}
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let hosts = vec![0, 1, 2];
        let result = std::panic::catch_unwind(|| {
            let _ = Flow::flows_from_config(temp_file.path().to_str().unwrap(), &hosts);
        });

        assert!(result.is_err(), "Empty path should trigger a panic");
    }

    #[test]
    fn test_flows_from_config_path_endpoint_mismatch_panics() {
        let toml_content = r#"
            seed = 1

            [[flow]]
            flow_type = "TCP"
            graph = [[0, 1]]
            routing = "PathFromConfig"
            path = [2, 3]
            [flow.traffic]
                initial_delay = 1.0
                size = 10000
                arr_dist = {type = "Exp", lambda = 1.0}
                pkt_size_dist = {type = "Uniform", low = 1000, high = 1500}
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let hosts = vec![0, 1, 2, 3];
        let result = std::panic::catch_unwind(|| {
            let _ = Flow::flows_from_config(temp_file.path().to_str().unwrap(), &hosts);
        });

        assert!(
            result.is_err(),
            "Path endpoints should match graph endpoints"
        );
    }

    #[test]
    fn test_flows_from_config_multiple_edges_panics() {
        let toml_content = r#"
            seed = 1

            [[flow]]
            flow_type = "TCP"
            graph = [[0, 1], [1, 2]]
            routing = "ShortestPath"
            [flow.traffic]
                initial_delay = 1.0
                size = 10000
                arr_dist = {type = "Exp", lambda = 1.0}
                pkt_size_dist = {type = "Uniform", low = 1000, high = 1500}
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let hosts = vec![0, 1, 2];
        let result = std::panic::catch_unwind(|| {
            let _ = Flow::flows_from_config(temp_file.path().to_str().unwrap(), &hosts);
        });

        assert!(result.is_err(), "Multiple edges should trigger a panic");
    }
}

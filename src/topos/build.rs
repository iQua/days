use std::fs;

use log::{debug, info};

use petgraph::graph::{NodeIndex, UnGraph};
use serde::Deserialize;
use thiserror::Error;

use crate::topos::topo::{Config, FatTreeConfig, TopoCategory, TorusConfig};

#[derive(Error, Debug)]
pub enum TopologyError {
    #[error("Failed to read configuration file: {0}")]
    ConfigReadError(#[from] std::io::Error),

    #[error("Failed to parse TOML: {0}")]
    TomlParseError(#[from] toml::de::Error),

    #[error("Invalid topology configuration: {0}")]
    InvalidConfig(String),

    #[error("Unsupported torus dimension: {0}")]
    UnsupportedDimension(u32),

    #[error("Numeric overflow in calculation: {0}")]
    NumericOverflow(String),
}

pub type Result<T> = std::result::Result<T, TopologyError>;

/// Represents a network graph configuration from TOML
#[derive(Deserialize)]
struct NetworkGraph {
    edges: Vec<(u32, u32)>,
    hosts: Vec<usize>,
}

impl NetworkGraph {
    fn validate(&self) -> Result<()> {
        if self.edges.is_empty() {
            return Err(TopologyError::InvalidConfig("Empty edge list".into()));
        }
        if self.hosts.is_empty() {
            return Err(TopologyError::InvalidConfig("Empty host list".into()));
        }
        // Additional validation could be added here at a later time
        Ok(())
    }
}

trait TopologyBuilder {
    fn build(&self) -> Result<(UnGraph<usize, ()>, Vec<usize>)>;
}

impl TopologyBuilder for FatTreeConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
        let k = u32::try_from(self.k)
            .map_err(|_| TopologyError::NumericOverflow("FatTree k parameter overflow".into()))?;

        validate_fattree_params(k)?;
        info!("Building a FatTree topology with k = {}.", k);

        let (num_layer_switches, layer_switches_per_pod, core_switches_per_agg) =
            calculate_fattree_params(k)?;

        let edges = build_fattree_edges(
            num_layer_switches,
            layer_switches_per_pod,
            core_switches_per_agg,
        );

        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts = create_host_list(num_layer_switches)?;

        Ok((graph, hosts))
    }
}

impl TopologyBuilder for TorusConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
        let dimension = self.dim as u32;
        let nodes_per_dim = self.n as u32;

        validate_torus_params(dimension, nodes_per_dim)?;

        let total_nodes = calculate_total_nodes(dimension, nodes_per_dim)?;
        info!(
            "Building {}D Torus topology with {} nodes.",
            dimension, total_nodes
        );

        let edges = build_torus_edges(dimension, nodes_per_dim)?;
        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts: Vec<usize> = (0..total_nodes).collect();

        Ok((graph, hosts))
    }
}

pub fn build_graph(file_path: &str) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
    let content = fs::read_to_string(file_path)?;

    let config: Config = match toml::from_str::<Config>(&content) {
        Ok(config) => {
            println!("Deserialized successfully.");
            config
        }
        Err(err) => {
            eprintln!("Failed to deserialize: {}", err);
            return Err(TopologyError::TomlParseError(err));
        }
    };

    match config.topology {
        Some(topo_config) => match topo_config.category {
            TopoCategory::FatTree => {
                debug!("Initializing FatTree graph");
                topo_config
                    .fat_tree
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing FatTree config".into()))?
                    .build()
            }
            TopoCategory::Torus => {
                debug!("Initializing Torus graph");
                topo_config
                    .torus
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing Torus config".into()))?
                    .build()
            }
        },
        None => build_custom_graph(&content),
    }
}

fn build_custom_graph(content: &str) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
    let graph_config: NetworkGraph = toml::from_str(content)?;
    graph_config.validate()?;
    Ok((
        UnGraph::<usize, ()>::from_edges(&graph_config.edges),
        graph_config.hosts,
    ))
}

fn validate_fattree_params(k: u32) -> Result<()> {
    if k % 2 != 0 {
        return Err(TopologyError::InvalidConfig("k must be even".into()));
    }
    if k == 0 {
        return Err(TopologyError::InvalidConfig("k must be positive".into()));
    }
    Ok(())
}

fn calculate_fattree_params(k: u32) -> Result<(u32, u32, u32)> {
    let num_layer_switches = k.pow(2) / 2;
    let layer_switches_per_pod = k / 2;
    let num_core_switches = k.pow(2) / 4;
    let core_switches_per_agg = num_core_switches / layer_switches_per_pod;

    Ok((
        num_layer_switches,
        layer_switches_per_pod,
        core_switches_per_agg,
    ))
}

fn build_fattree_edges(
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) -> Vec<(u32, u32)> {
    let mut edges = Vec::with_capacity((num_layer_switches * layer_switches_per_pod * 2) as usize);

    // Edge to aggregation layer connections
    build_edge_to_aggregation_connections(&mut edges, num_layer_switches, layer_switches_per_pod);

    // Aggregation to core layer connections
    build_aggregation_to_core_connections(
        &mut edges,
        num_layer_switches,
        layer_switches_per_pod,
        core_switches_per_agg,
    );

    edges
}

fn build_edge_to_aggregation_connections(
    edges: &mut Vec<(u32, u32)>,
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
) {
    for edge_id in 0..num_layer_switches {
        let pod_id = edge_id / layer_switches_per_pod;
        let agg_start = num_layer_switches + pod_id * layer_switches_per_pod;
        edges.extend(
            (agg_start..agg_start + layer_switches_per_pod).map(|agg_id| (edge_id, agg_id)),
        );
    }
}

fn build_aggregation_to_core_connections(
    edges: &mut Vec<(u32, u32)>,
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) {
    for agg_id in num_layer_switches..2 * num_layer_switches {
        let core_group = agg_id % layer_switches_per_pod;
        let core_start = 2 * num_layer_switches + core_group * core_switches_per_agg;
        edges.extend(
            (core_start..core_start + core_switches_per_agg).map(|core_id| (agg_id, core_id)),
        );
    }
}

fn validate_torus_params(dimension: u32, nodes_per_dim: u32) -> Result<()> {
    if !(1..=3).contains(&dimension) {
        return Err(TopologyError::UnsupportedDimension(dimension));
    }
    if nodes_per_dim == 0 {
        return Err(TopologyError::InvalidConfig(
            "nodes_per_dim must be positive".into(),
        ));
    }
    Ok(())
}

fn calculate_total_nodes(dimension: u32, nodes_per_dim: u32) -> Result<usize> {
    usize::try_from(nodes_per_dim.pow(dimension))
        .map_err(|_| TopologyError::NumericOverflow("Total node count overflow".into()))
}

fn create_host_list(count: u32) -> Result<Vec<usize>> {
    let count_usize = usize::try_from(count)
        .map_err(|_| TopologyError::NumericOverflow("Host count overflow".into()))?;
    Ok((0..count_usize).collect())
}

fn build_torus_edges(dimension: u32, nodes_per_dim: u32) -> Result<Vec<(u32, u32)>> {
    // Create empty undirected graph
    let mut graph = UnGraph::<(), ()>::default();

    // Add all nodes first
    let total_nodes = nodes_per_dim.pow(dimension);
    for _ in 0..total_nodes {
        graph.add_node(());
    }

    // Add edges based on dimension
    match dimension {
        1 => build_1d_torus_edges(&mut graph, nodes_per_dim),
        2 => build_2d_torus_edges(&mut graph, nodes_per_dim),
        3 => build_3d_torus_edges(&mut graph, nodes_per_dim),
        _ => return Err(TopologyError::UnsupportedDimension(dimension)),
    }

    // Extract edges from graph
    let edges: Vec<(u32, u32)> = graph
        .edge_indices()
        .map(|e| {
            let (a, b) = graph.edge_endpoints(e).unwrap();
            (a.index() as u32, b.index() as u32)
        })
        .collect();

    Ok(edges)
}

fn build_1d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        let next = (i + 1) % nodes_per_dim;
        let i_idx = NodeIndex::new(i as usize);
        let next_idx = NodeIndex::new(next as usize);

        // Add single edge - UnGraph handles bidirectional nature
        graph.add_edge(i_idx, next_idx, ());
    }
}

fn build_2d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        for j in 0..nodes_per_dim {
            let current = i + j * nodes_per_dim;
            let current_idx = NodeIndex::new(current as usize);

            // X dimension connection
            let next_i = (i + 1) % nodes_per_dim + j * nodes_per_dim;
            let next_i_idx = NodeIndex::new(next_i as usize);
            graph.add_edge(current_idx, next_i_idx, ());

            // Y dimension connection
            let next_j = i + ((j + 1) % nodes_per_dim) * nodes_per_dim;
            let next_j_idx = NodeIndex::new(next_j as usize);
            graph.add_edge(current_idx, next_j_idx, ());
        }
    }
}

fn build_3d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        for j in 0..nodes_per_dim {
            for k in 0..nodes_per_dim {
                let current = i + j * nodes_per_dim + k * nodes_per_dim.pow(2);
                let current_idx = NodeIndex::new(current as usize);

                // X dimension
                let next_i = (i + 1) % nodes_per_dim + j * nodes_per_dim + k * nodes_per_dim.pow(2);
                let next_i_idx = NodeIndex::new(next_i as usize);
                graph.add_edge(current_idx, next_i_idx, ());

                // Y dimension
                let next_j =
                    i + ((j + 1) % nodes_per_dim) * nodes_per_dim + k * nodes_per_dim.pow(2);
                let next_j_idx = NodeIndex::new(next_j as usize);
                graph.add_edge(current_idx, next_j_idx, ());

                // Z dimension
                let next_k =
                    i + j * nodes_per_dim + ((k + 1) % nodes_per_dim) * nodes_per_dim.pow(2);
                let next_k_idx = NodeIndex::new(next_k as usize);
                graph.add_edge(current_idx, next_k_idx, ());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::NodeIndex;
    use petgraph::visit::EdgeRef;
    use std::collections::HashSet;

    // Helper functions for tests
    fn create_fattree(k: usize) -> (UnGraph<usize, ()>, Vec<usize>) {
        let config = FatTreeConfig { k };
        config.build().unwrap()
    }

    fn create_torus(dim: usize, n: usize) -> (UnGraph<usize, ()>, Vec<usize>) {
        let config = TorusConfig { dim, n };
        config.build().unwrap()
    }

    #[test]
    fn test_validate_fattree_params() {
        assert!(validate_fattree_params(2).is_ok());
        assert!(validate_fattree_params(4).is_ok());
        assert!(validate_fattree_params(0).is_err());
        assert!(validate_fattree_params(3).is_err());
    }

    #[test]
    fn test_validate_torus_params() {
        assert!(validate_torus_params(1, 2).is_ok());
        assert!(validate_torus_params(2, 2).is_ok());
        assert!(validate_torus_params(3, 2).is_ok());
        assert!(validate_torus_params(0, 2).is_err());
        assert!(validate_torus_params(4, 2).is_err());
        assert!(validate_torus_params(1, 0).is_err());
    }

    // FatTree Tests
    mod fattree_tests {
        use super::*;

        #[test]
        fn test_fattree_node_counts() {
            let k = 4;
            let (graph, hosts) = create_fattree(k);

            let expected_edge_switches = k * k / 2;
            let expected_agg_switches = k * k / 2;
            let expected_core_switches = k * k / 4;
            let total_expected_switches =
                expected_edge_switches + expected_agg_switches + expected_core_switches;

            assert_eq!(graph.node_count(), total_expected_switches);
            assert_eq!(hosts.len(), expected_edge_switches);
        }

        #[test]
        fn test_fattree_edge_counts() {
            let k = 4;
            let (graph, _) = create_fattree(k);

            // Each edge switch connects to k/2 aggregation switches
            // Each aggregation switch connects to k/2 core switches
            // The total number of connections is:
            // (k * k / 2) * (k / 2)      // edge to aggregation connections
            // + (k * k / 2) * (k / 2)    // aggregation to core connections
            let expected_edges = (k * k / 2) * (k / 2) + (k * k / 2) * (k / 2);

            // Each connection is counted only once in our expected count
            assert_eq!(graph.edge_count(), expected_edges);
        }

        #[test]
        fn test_fattree_pod_connectivity() {
            let k = 4;
            let (graph, _) = create_fattree(k);

            for pod in 0..k / 2 {
                let pod_edge_switches: Vec<u32> =
                    (0..k / 2).map(|i| (pod * k / 2 + i) as u32).collect();

                let pod_agg_switches: Vec<u32> = (0..k / 2)
                    .map(|i| (k * k / 2 + pod * k / 2 + i) as u32)
                    .collect();

                for &edge_switch in &pod_edge_switches {
                    let neighbors: HashSet<u32> = graph
                        .edges(NodeIndex::new(edge_switch as usize))
                        .map(|e| e.target().index() as u32)
                        .collect();

                    for &agg_switch in &pod_agg_switches {
                        assert!(neighbors.contains(&agg_switch));
                    }
                }
            }
        }

        #[test]
        fn test_fattree_core_connectivity() {
            let k = 4;
            let (graph, _) = create_fattree(k);

            let agg_start = k * k / 2;
            let agg_end = k * k;

            for agg_id in agg_start..agg_end {
                let node_idx = NodeIndex::new(agg_id);
                let core_neighbors: HashSet<_> = graph
                    .edges(node_idx)
                    .map(|e| e.target().index())
                    .filter(|&n| n >= (k * k) as usize)
                    .collect();

                assert_eq!(core_neighbors.len(), k / 2);
            }
        }
    }

    // Torus Tests
    mod torus_tests {
        use super::*;

        #[test]
        fn test_torus_node_counts() {
            let test_cases = [(1, 4), (2, 3), (3, 2)];

            for &(dim, n) in &test_cases {
                let (graph, hosts) = create_torus(dim, n);
                let expected_nodes = n.pow(dim as u32);

                assert_eq!(graph.node_count(), expected_nodes);
                assert_eq!(hosts.len(), expected_nodes);
            }
        }

        #[test]
        fn test_torus_edge_counts() {
            let test_cases = [(1, 4), (2, 3), (3, 2)];

            for &(dim, n) in &test_cases {
                let (graph, _) = create_torus(dim, n);

                // In a d-dimensional torus:
                // - Each node has d connections (one in each dimension)
                // - Total number of nodes is n^d
                // - Each connection is unique in the topology
                let num_nodes = n.pow(dim as u32);
                let expected_edges = num_nodes * dim;

                assert_eq!(
                    graph.edge_count(),
                    expected_edges,
                    "Wrong edge count for {}-D torus with {} nodes per dimension",
                    dim,
                    n
                );
            }
        }

        #[test]
        fn test_torus_node_degrees() {
            // Each node in a k-dimensional torus has 2k connections (2 per dimension)
            let test_cases = [
                (1, 4, 2), // 1D: 2 connections per node
                (2, 3, 4), // 2D: 4 connections per node
                (3, 2, 6), // 3D: 6 connections per node
            ];

            for &(dim, n, expected_degree) in &test_cases {
                let (graph, _) = create_torus(dim, n);

                for node_idx in 0..n.pow(dim as u32) {
                    let node = NodeIndex::new(node_idx);
                    assert_eq!(
                        graph.edges(node).count(),
                        expected_degree,
                        "Node {} in {}-D torus has wrong degree",
                        node_idx,
                        dim
                    );
                }
            }
        }

        #[test]
        fn test_torus_wraparound_connections() {
            // Test 1D torus wraparound
            let (graph, _) = create_torus(1, 4);
            assert!(has_edge(&graph, 0, 3));

            // Test 2D torus wraparound
            let (graph, _) = create_torus(2, 3);
            // Check horizontal wraparound
            assert!(has_edge(&graph, 0, 2));
            assert!(has_edge(&graph, 3, 5));
            assert!(has_edge(&graph, 6, 8));
            // Check vertical wraparound
            assert!(has_edge(&graph, 0, 6));
            assert!(has_edge(&graph, 1, 7));
            assert!(has_edge(&graph, 2, 8));
        }

        fn has_edge(graph: &UnGraph<usize, ()>, from: usize, to: usize) -> bool {
            graph
                .edges(NodeIndex::new(from))
                .any(|e| e.target().index() == to)
        }

        #[test]
        fn test_torus_neighbor_distances() {
            let (graph, _) = create_torus(2, 4);

            let node_idx = NodeIndex::new(5);
            let neighbors: HashSet<_> = graph.edges(node_idx).map(|e| e.target().index()).collect();

            let expected: HashSet<_> = vec![4, 6, 1, 9].into_iter().collect();
            assert_eq!(neighbors, expected);
        }
    }
}

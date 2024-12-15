//! The routing protocols that are used to compute the path that each flow
//! takes. Currently, three routing protocols have been implemented:
//!
//! - Shortest path routing: Selects a random candidate from a set of shortest
//! paths, which are computed by the `petgraph` crate.
//! - Path from configuration: Uses the path that is specified in the configuration.
//! - ECMP: Implements the Equal-Cost Multi-Path algorithm (RFC 2992).
//!
use std::hash::{Hash, Hasher};

use petgraph::algo::{all_simple_paths, astar, dijkstra};
use petgraph::graph::{NodeIndex, UnGraph};
use serde::Deserialize;

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum RoutingConfig {
    ShortestPath,
    PathFromConfig,
    ECMP,
}

#[derive(Debug)]
pub enum Routing {
    ShortestPath(ShortestPath),
    PathFromConfig(PathFromConfig),
    ECMP(ECMP),
}

/// Defines the interface for all routing protocols.
pub trait RoutingProtocol {
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex>;
}

#[derive(Debug)]
pub struct ShortestPath {
    graph: UnGraph<usize, ()>,
}

impl ShortestPath {
    pub fn new(graph: UnGraph<usize, ()>) -> ShortestPath {
        ShortestPath { graph }
    }
}

impl RoutingProtocol for ShortestPath {
    /// Returns a shortest path between two nodes in the graph.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        let path = astar(&self.graph, start, |n| n == end, |_| 1, |_| 0);

        match path {
            Some((_, path)) => path,
            None => panic!("No path can be found."),
        }
    }
}

#[derive(Debug)]
pub struct PathFromConfig {
    pub path: Vec<NodeIndex>,
}

impl PathFromConfig {
    pub fn new(path_from_config: Vec<usize>) -> PathFromConfig {
        let path = path_from_config.into_iter().map(NodeIndex::new).collect();
        PathFromConfig { path }
    }
}

#[derive(Debug)]
pub struct ECMP {
    graph: UnGraph<usize, ()>,
    flow_id: usize,
    source_host: usize,
    sink_host: usize,
}

impl ECMP {
    pub fn new(
        graph: UnGraph<usize, ()>,
        flow_id: usize,
        source_host: usize,
        sink_host: usize,
    ) -> ECMP {
        ECMP {
            graph,
            flow_id,
            source_host,
            sink_host,
        }
    }

    fn compute_hash(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        // Compute a hash value based on flow attributes
        self.flow_id.hash(&mut hasher);
        self.source_host.hash(&mut hasher);
        self.sink_host.hash(&mut hasher);
        let hash_value = hasher.finish();

        hash_value
    }

    /// Selects one of the equal-cost paths using a hash of flow attributes.
    fn select_ecmp_path(&self, paths: &[Vec<NodeIndex>]) -> Vec<NodeIndex> {
        // Use the hash to select a path
        let index = (self.compute_hash() as usize) % paths.len();
        paths[index].clone()
    }
}

impl RoutingProtocol for ECMP {
    /// Returns a path based on the Equal-Cost Multi-Path (ECMP) routing protocol.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        // Compute shortest path distances from source to all nodes
        let distances = dijkstra(&self.graph, start, None, |_| 1);

        // Get the shortest distance to the target
        if let Some(&shortest_distance) = distances.get(&end) {
            // Find all simple paths from source to target within the shortest distance
            let all_paths_iter = all_simple_paths::<Vec<_>, _>(
                &self.graph,
                start,
                end,
                0,
                Some(shortest_distance + 1),
            );

            // Collect all equal-cost paths
            let equal_cost_paths: Vec<Vec<NodeIndex>> = all_paths_iter
                .filter(|path| path.len() - 1 == shortest_distance)
                .collect();

            if !equal_cost_paths.is_empty() {
                // Use a hash of flow attributes to select a path
                let selected_path = self.select_ecmp_path(&equal_cost_paths);

                selected_path
            } else {
                panic!("No equal-cost path can be found.");
            }
        } else {
            panic!("No path can be found.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::UnGraph;

    #[test]
    fn test_shortest_path_routing() {
        // Build a simple undirected graph
        // Graph structure:
        // 0 - 1
        //  \ /
        //   2
        //   |
        //   3

        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ()); // Edge 0-1
        graph.add_edge(node0, node2, ()); // Edge 0-2
        graph.add_edge(node1, node2, ()); // Edge 1-2
        graph.add_edge(node2, node3, ()); // Edge 2-3

        // Create a ShortestPath routing instance
        let mut shortest_path = ShortestPath::new(graph);

        // Compute the route from node 0 to node 3
        let start = NodeIndex::new(0);
        let end = NodeIndex::new(3);
        let path = shortest_path.compute_route(start, end);

        // The expected shortest path is [0, 2, 3]
        let expected_path = vec![start, NodeIndex::new(2), end];

        assert_eq!(path, expected_path);
    }

    #[test]
    fn test_ecmp_routing() {
        // Build a graph with multiple equal-cost paths between nodes 0 and 3
        // Graph structure:
        //     1
        //    / \
        //   0   3
        //    \ /
        //     2

        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ()); // Edge 0-1
        graph.add_edge(node1, node3, ()); // Edge 1-3
        graph.add_edge(node0, node2, ()); // Edge 0-2
        graph.add_edge(node2, node3, ()); // Edge 2-3

        // Create an ECMP routing instance
        let flow_id = 1;
        let source_host = 0;
        let sink_host = 3;
        let mut ecmp = ECMP::new(graph, flow_id, source_host, sink_host);

        // Compute the route from node 0 to node 3
        let start = NodeIndex::new(source_host);
        let end = NodeIndex::new(sink_host);
        let path = ecmp.compute_route(start, end);

        // There are two equal-cost paths: [0, 1, 3] and [0, 2, 3]
        let possible_paths = vec![
            vec![start, NodeIndex::new(1), end],
            vec![start, NodeIndex::new(2), end],
        ];

        // Check that the computed path is one of the possible equal-cost paths
        assert!(
            possible_paths.contains(&path),
            "ECMP routing did not find an equal-cost path"
        );
    }

    #[test]
    fn test_no_path() {
        // Build a disconnected graph where no path exists between nodes 0 and 3
        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let _node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ());
        graph.add_edge(node1, node2, ());
        // Note: No edge connecting to node3

        // Test ShortestPath routing for no path scenario
        let mut shortest_path = ShortestPath::new(graph.clone());
        let start = NodeIndex::new(0);
        let end = NodeIndex::new(3);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shortest_path.compute_route(start, end);
        }));

        assert!(
            result.is_err(),
            "ShortestPath should panic when no path exists"
        );

        // Test ECMP routing for no path scenario
        let flow_id = 1;
        let source_host = 0;
        let sink_host = 3;
        let mut ecmp = ECMP::new(graph, flow_id, source_host, sink_host);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ecmp.compute_route(start, end);
        }));

        assert!(result.is_err(), "ECMP should panic when no path exists");
    }
}

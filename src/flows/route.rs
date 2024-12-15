//! The routing protocols that are used to compute the path that each flow
//! takes. Currently, three routing protocols have been implemented:
//!
//! - Shortest path routing: select a random candidate from a set of shortest
//! paths, which are computed by the `petgraph` crate.
//! - Path from configuration: use the path that is specified in the configuration.
//! - ECMP: select a random candidate from a set of equal-cost multi-path routes.
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

                // Prepend source_id and append sink_id to the path
                let mut full_path = vec![start];
                full_path.extend(selected_path);
                full_path.push(end);

                full_path
            } else {
                panic!("No equal-cost path found from source to target");
            }
        } else {
            panic!("No path can be found.");
        }
    }
}

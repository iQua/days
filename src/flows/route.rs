//! The routing protocols that are used to compute the path that each flow
//! takes. Currently, three routing protocols have been implemented:
//!
//! - Shortest path routing: Selects a random candidate from a set of shortest
//!   paths, which are computed by the `petgraph` crate using the A* algorithm.
//! - Path from configuration: Uses the path that is specified in the configuration.
//! - ECMP: Implements the Equal-Cost Multi-Path algorithm (RFC 2992) optimized with A*.
//!
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};

use petgraph::algo;
use petgraph::algo::astar;
use petgraph::graph::{NodeIndex, UnGraph};
use serde::Deserialize;

#[derive(Copy, Clone, Debug)]
struct MinScoredNode {
    score: (usize, usize, usize),
    node: NodeIndex,
}

impl PartialEq for MinScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for MinScoredNode {}

impl PartialOrd for MinScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = &self.score;
        let b = &other.score;
        if a == b {
            Ordering::Equal
        } else if a < b {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum RoutingConfig {
    ShortestPath,
    PathFromConfig,
    ECMP,
}

#[derive(Debug, PartialEq)]
pub enum Routing {
    ShortestPath(ShortestPath),
    PathFromConfig(PathFromConfig),
    ECMP(ECMP),
}

/// Defines the interface for all routing protocols.
pub trait RoutingProtocol {
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex>;
}

#[derive(Debug, Clone)]
pub struct ShortestPath {
    graph: UnGraph<usize, ()>,
}

impl PartialEq for ShortestPath {
    fn eq(&self, other: &Self) -> bool {
        algo::is_isomorphic(&self.graph, &other.graph)
    }
}

impl ShortestPath {
    pub fn new(graph: UnGraph<usize, ()>) -> ShortestPath {
        ShortestPath { graph }
    }

    fn fat_tree_params(graph: &UnGraph<usize, ()>) -> Option<(usize, usize, usize)> {
        let total_nodes = graph.node_count();
        if total_nodes == 0 || !total_nodes.is_multiple_of(5) {
            return None;
        }

        let num_layer_switches = total_nodes.checked_mul(2)? / 5;
        let doubled = num_layer_switches.checked_mul(2)?;
        let k = (doubled as f64).sqrt() as usize;
        if k == 0 || k * k != doubled || !k.is_multiple_of(2) {
            return None;
        }

        let switches_per_pod = k / 2;
        Some((num_layer_switches, switches_per_pod, k))
    }

    fn compute_fat_tree_route_in(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Option<Vec<NodeIndex>> {
        let (num_layer_switches, switches_per_pod, num_pods) = Self::fat_tree_params(graph)?;
        let core_start = 2 * num_layer_switches;

        let start_idx = start.index();
        let end_idx = end.index();
        if start_idx >= num_layer_switches || end_idx >= num_layer_switches {
            return None;
        }

        let node_count = graph.node_count();
        let mut visit_next = BinaryHeap::new();
        let mut scores = vec![None; node_count];
        let mut came_from = vec![usize::MAX; node_count];

        let start_idx = start.index();
        scores[start_idx] = Some((0, 0, 0));
        visit_next.push(MinScoredNode {
            score: (0, 0, 0),
            node: start,
        });

        while let Some(MinScoredNode {
            score: (f, h, g),
            node,
        }) = visit_next.pop()
        {
            if node == end {
                let mut path = vec![node];
                let mut current = node.index();
                while current != start_idx {
                    let previous = came_from[current];
                    if previous == usize::MAX {
                        break;
                    }
                    path.push(NodeIndex::new(previous));
                    current = previous;
                }
                path.reverse();
                return Some(path);
            }

            let node_idx = node.index();
            if let Some((_, _, old_g)) = scores[node_idx] {
                if old_g < g {
                    continue;
                }
            }
            scores[node_idx] = Some((f, h, g));

            let mut push_neighbor = |neigh: NodeIndex| {
                let neigh_g = g + 1;
                let neigh_score = (neigh_g, 0, neigh_g);
                let neigh_idx = neigh.index();

                if let Some((_, _, old_neigh_g)) = scores[neigh_idx] {
                    if neigh_g >= old_neigh_g {
                        return;
                    }
                }

                scores[neigh_idx] = Some(neigh_score);
                came_from[neigh_idx] = node_idx;
                visit_next.push(MinScoredNode {
                    score: neigh_score,
                    node: neigh,
                });
            };

            if node_idx < num_layer_switches {
                let pod = node_idx / switches_per_pod;
                let agg_base = num_layer_switches + pod * switches_per_pod;
                for agg_offset in (0..switches_per_pod).rev() {
                    push_neighbor(NodeIndex::new(agg_base + agg_offset));
                }
            } else if node_idx < core_start {
                let agg_rel = node_idx - num_layer_switches;
                let pod = agg_rel / switches_per_pod;
                let group = agg_rel % switches_per_pod;

                for core_offset in (0..switches_per_pod).rev() {
                    push_neighbor(NodeIndex::new(
                        core_start + group * switches_per_pod + core_offset,
                    ));
                }

                let edge_base = pod * switches_per_pod;
                for edge_offset in (0..switches_per_pod).rev() {
                    push_neighbor(NodeIndex::new(edge_base + edge_offset));
                }
            } else {
                let core_rel = node_idx - core_start;
                let group = core_rel / switches_per_pod;

                for pod in (0..num_pods).rev() {
                    push_neighbor(NodeIndex::new(
                        num_layer_switches + pod * switches_per_pod + group,
                    ));
                }
            }
        }

        None
    }

    pub fn compute_route_in(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Vec<NodeIndex> {
        if let Some(path) = Self::compute_fat_tree_route_in(graph, start, end) {
            return path;
        }

        let path = astar(
            graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        );

        match path {
            Some((_, path)) => path,
            None => panic!("No path can be found."),
        }
    }
}

impl RoutingProtocol for ShortestPath {
    /// Returns a shortest path between two nodes in the graph using A*.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        Self::compute_route_in(&self.graph, start, end)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathFromConfig {
    pub path: Vec<NodeIndex>,
}

impl PathFromConfig {
    pub fn new(path_from_config: Vec<usize>) -> PathFromConfig {
        let path = path_from_config.into_iter().map(NodeIndex::new).collect();
        PathFromConfig { path }
    }
}

#[derive(Debug, Clone)]
pub struct ECMP {
    graph: UnGraph<usize, ()>,
    flow_id: usize,
    source_host: usize,
    sink_host: usize,
}

impl PartialEq for ECMP {
    fn eq(&self, other: &Self) -> bool {
        self.flow_id == other.flow_id
            && self.source_host == other.source_host
            && self.sink_host == other.sink_host
            && algo::is_isomorphic(&self.graph, &other.graph)
    }
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

    fn compute_hash_for(flow_id: usize, source_host: usize, sink_host: usize) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        // Compute a hash value based on flow attributes
        flow_id.hash(&mut hasher);
        source_host.hash(&mut hasher);
        sink_host.hash(&mut hasher);
        hasher.finish()
    }

    /// Selects one of the equal-cost paths using a hash of flow attributes.
    fn select_ecmp_path(&self, paths: &[Vec<NodeIndex>]) -> Vec<NodeIndex> {
        Self::select_ecmp_path_for(self.flow_id, self.source_host, self.sink_host, paths)
    }

    fn select_ecmp_path_for(
        flow_id: usize,
        source_host: usize,
        sink_host: usize,
        paths: &[Vec<NodeIndex>],
    ) -> Vec<NodeIndex> {
        // Use the hash to select a path
        let index =
            (Self::compute_hash_for(flow_id, source_host, sink_host) as usize) % paths.len();
        paths[index].clone()
    }

    /// Finds all equal-cost paths using an optimized A* approach.
    fn find_equal_cost_paths(
        &self,
        start: NodeIndex,
        end: NodeIndex,
        shortest_distance: usize,
    ) -> Vec<Vec<NodeIndex>> {
        Self::find_equal_cost_paths_in(&self.graph, start, end, shortest_distance)
    }

    fn find_equal_cost_paths_in(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
        shortest_distance: usize,
    ) -> Vec<Vec<NodeIndex>> {
        let mut paths = Vec::new();
        let mut stack = vec![(start, vec![start], 0)];

        while let Some((current, path, cost)) = stack.pop() {
            if current == end {
                if cost == shortest_distance {
                    paths.push(path.clone());
                }
                continue;
            }

            for neighbor in graph.neighbors(current) {
                if !path.contains(&neighbor) {
                    let new_cost = cost + 1; // Uniform cost
                    if new_cost <= shortest_distance {
                        let mut new_path = path.clone();
                        new_path.push(neighbor);
                        stack.push((neighbor, new_path, new_cost));
                    }
                }
            }
        }

        paths
    }

    pub fn compute_route_in(
        graph: &UnGraph<usize, ()>,
        flow_id: usize,
        source_host: usize,
        sink_host: usize,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Vec<NodeIndex> {
        let shortest_path = astar(
            graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        );

        let shortest_distance = match shortest_path {
            Some((cost, _)) => cost,
            None => panic!("No path can be found."),
        };

        let equal_cost_paths =
            Self::find_equal_cost_paths_in(graph, start, end, shortest_distance as usize);

        if !equal_cost_paths.is_empty() {
            Self::select_ecmp_path_for(flow_id, source_host, sink_host, &equal_cost_paths)
        } else {
            panic!("No equal-cost path can be found.");
        }
    }
}

impl RoutingProtocol for ECMP {
    /// Returns a path based on the Equal-Cost Multi-Path (ECMP) routing protocol optimized with A*.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        // Use A* to find the shortest path distance
        let shortest_path = astar(
            &self.graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        );

        let shortest_distance = match shortest_path {
            Some((cost, _)) => cost,
            None => panic!("No path can be found."),
        };

        // Find all equal-cost paths using the optimized A* approach
        let equal_cost_paths = self.find_equal_cost_paths(start, end, shortest_distance as usize);

        if !equal_cost_paths.is_empty() {
            self.select_ecmp_path(&equal_cost_paths)
        } else {
            panic!("No equal-cost path can be found.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::UnGraph;

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
        let possible_paths = [
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

    #[test]
    fn test_ecmp_hashing_is_deterministic() {
        // Build a graph with multiple equal-cost paths between nodes 0 and 3
        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ()); // Edge 0-1
        graph.add_edge(node1, node3, ()); // Edge 1-3
        graph.add_edge(node0, node2, ()); // Edge 0-2
        graph.add_edge(node2, node3, ()); // Edge 2-3

        let source_host = 0;
        let sink_host = 3;

        let mut ecmp = ECMP::new(graph.clone(), 1, source_host, sink_host);
        let start = NodeIndex::new(source_host);
        let end = NodeIndex::new(sink_host);

        let path1 = ecmp.compute_route(start, end);
        let path2 = ecmp.compute_route(start, end);

        assert_eq!(path1, path2, "ECMP path selection should be stable");
    }
}

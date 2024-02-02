//! The routing protocols that are used to compute the path that each flow
//! takes. Currently, the only routing protocol implemented is to select a
//! random candidate from a set of shortest paths, which are computed by the
//! `petgraph` crate.
use petgraph::algo::astar;
use petgraph::graph::{NodeIndex, UnGraph};

/// Defines the interface for all routing protocols
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

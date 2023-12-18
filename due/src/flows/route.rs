//! The routing protocols that are used to compute the path that each flow takes.
//! Currently, the only routing protocol implemented is to select a random candidate
//! from a set of simple paths, which are computed by the `petgraph` crate.

use petgraph::algo::{all_simple_paths, dijkstra};
use petgraph::graph::{NodeIndex, UnGraph};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::get_seed;

/// Defines the interface for all routing protocols
pub trait RoutingProtocol {
    /// This function returns a shortest path between two nodes in the graph
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex>;
}

#[derive(Debug)]
pub struct RandomSimplePath {
    graph: UnGraph<usize, ()>,
    rng: SmallRng,
}

impl RandomSimplePath {
    pub fn new(graph: UnGraph<usize, ()>) -> RandomSimplePath {
        let seed = get_seed();
        let rng = match seed {
            1.. => SmallRng::seed_from_u64(seed as u64),
            _ => SmallRng::from_entropy(),
        };

        RandomSimplePath { graph, rng }
    }

    fn get_all_simple_paths(
        &mut self,
        start: NodeIndex,
        end: NodeIndex,
        len: usize,
    ) -> Vec<Vec<NodeIndex>> {
        let mut paths = Vec::new();
        let result = all_simple_paths(&self.graph, start, end, 0, Some(len));
        for path in result {
            paths.push(path);
        }
        paths
    }
}

impl RoutingProtocol for RandomSimplePath {
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        let binding = dijkstra(&self.graph, start, Some(end), |_| 1);
        let len = binding.get(&end).unwrap();
        let paths = self.get_all_simple_paths(start, end, *len as usize);
        let random_index = self.rng.gen_range(0..paths.len());

        paths[random_index].clone()
    }
}

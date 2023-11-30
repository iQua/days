use petgraph::{
    algo::all_simple_paths,
    graph::{NodeIndex, UnGraph},
};
use rand::{rngs::SmallRng, Rng};

/// Defines the interface for all routing protocols
pub trait RoutingProtocol {
    /// This function returns a shortest path between two nodes in the graph
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex, rng: SmallRng) -> Vec<NodeIndex>;
}

#[derive(Debug)]
pub struct RandomSimplePath {
    graph: UnGraph<usize, ()>,
}

impl RandomSimplePath {
    pub fn new(graph: UnGraph<usize, ()>) -> RandomSimplePath {
        RandomSimplePath { graph }
    }

    fn get_all_simple_paths(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<Vec<NodeIndex>> {
        let mut paths = Vec::new();
        let result = all_simple_paths(&self.graph, start, end, 0, None);
        for path in result {
            paths.push(path);
        }
        paths
    }
}

impl RoutingProtocol for RandomSimplePath {
    fn compute_route(
        &mut self,
        start: NodeIndex,
        end: NodeIndex,
        mut rng: SmallRng,
    ) -> Vec<NodeIndex> {
        let paths = self.get_all_simple_paths(start, end);
        let rdm_idx = rng.gen_range(0..paths.len());
        paths[rdm_idx].clone()
    }
}

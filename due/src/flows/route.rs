use petgraph::{
    algo::all_simple_paths,
    graph::{NodeIndex, UnGraph},
};

/// Defines the interface for all routing protocols
pub trait Routing {
    /// This function returns a shortest path between two nodes in the graph
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<Vec<NodeIndex>>;
}

pub struct Route {
    graph: UnGraph<usize, ()>,
}

impl Route {
    pub fn new(graph: UnGraph<usize, ()>) -> Route {
        Route { graph }
    }
}

impl Routing for Route {
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<Vec<NodeIndex>> {
        let mut all_shortest_paths = Vec::new();
        let result = all_simple_paths(&self.graph, start, end, 0, None);
        for path in result {
            all_shortest_paths.push(path);
        }
        // all_shortest_paths
        all_shortest_paths
    }
}

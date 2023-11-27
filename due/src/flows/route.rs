use petgraph::graph::NodeIndex;

/// Defines the interface for all routing protocols
pub trait Routing {
    /// This function returns all shortest paths between two nodes in the graph
    fn get_shortest_paths(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<Vec<NodeIndex>>;
}

use petgraph::graph::DiGraph;

use crate::sim::Time;


#[derive(Debug)]
pub struct Flow {
    id: usize,
    graph: DiGraph<usize, ()>
}

impl Flow {
    pub fn new(id: usize, initial_delay: Time, graph: DiGraph<usize, ()>) -> Flow{
        Flow {
            id,
            graph
        }
    }
}
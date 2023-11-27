use petgraph::graph::DiGraph;

use crate::{sim::Time, DistributionInfo};

#[derive(Debug)]
pub struct Flow {
    pub id: usize,
    pub graph: DiGraph<usize, ()>,
    pub initial_delay: Time,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
}

impl Clone for Flow {
    fn clone(&self) -> Self {
        Flow {
            id: self.id,
            graph: self.graph.clone(),
            initial_delay: self.initial_delay,
            arr_dist: self.arr_dist,
            pkt_size_dist: self.pkt_size_dist,
        }
    }
}

impl Flow {
    pub fn new(
        id: usize,
        graph: DiGraph<usize, ()>,
        initial_delay: Time,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> Flow {
        Flow {
            id,
            graph,
            initial_delay,
            arr_dist,
            pkt_size_dist,
        }
    }
}

use petgraph::graph::DiGraph;

use crate::{sim::Time, DistributionInfo};

#[derive(Debug)]
pub struct Flow {
    id: usize,
    graph: DiGraph<usize, ()>,
    initial_delay: Time,
    path: Vec<usize>,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
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
            path: Vec::new(),
            arr_dist,
            pkt_size_dist,
        }
    }
}

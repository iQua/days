use petgraph::graph::DiGraph;

use crate::{sim::Time, DistributionInfo};

use super::{sink::PacketSink, source::PacketSource, EndPoint};

#[derive(Debug)]
pub struct Flow {
    pub id: usize,
    pub graph: DiGraph<usize, ()>,
    pub initial_delay: Time,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub start_id: Vec<usize>,
    pub end_id: Vec<usize>,
}

impl Clone for Flow {
    fn clone(&self) -> Self {
        Flow {
            id: self.id,
            graph: self.graph.clone(),
            initial_delay: self.initial_delay,
            arr_dist: self.arr_dist,
            pkt_size_dist: self.pkt_size_dist,
            start_id: self.start_id.clone(),
            end_id: self.end_id.clone(),
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
            start_id: Vec::new(),
            end_id: Vec::new(),
        }
    }

    pub fn init_endpoints(&mut self, endpoints: &mut Vec<EndPoint>) {
        endpoints.push(EndPoint::PacketSource(PacketSource::new(
            self.id,
            self.initial_delay,
            self.arr_dist,
            self.pkt_size_dist,
        )));
        endpoints.push(EndPoint::PacketSink(PacketSink::new(self.id)));
    }
}

use std::collections::HashMap;

use petgraph::{
    graph::{DiGraph, NodeIndex, UnGraph},
    visit::EdgeRef,
};
use rand::Rng;

use crate::{
    flows::route::Routing,
    sim::{SimContext, Time},
    switches::Element,
    DistributionInfo, Shared,
};

use super::{route::Route, sink::PacketSink, source::PacketSource, EndPoint};

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
        let source = PacketSource::new(
            self.id,
            self.initial_delay,
            self.arr_dist,
            self.pkt_size_dist,
        );
        let sink = PacketSink::new(self.id);
        self.start_id.push(source.id());
        self.end_id.push(sink.id());
        endpoints.push(EndPoint::PacketSource(source));
        endpoints.push(EndPoint::PacketSink(sink));
    }

    pub async fn run(
        mut self,
        endpoints: &mut Vec<EndPoint>,
        graph: &mut UnGraph<usize, ()>,
        indices: &HashMap<usize, NodeIndex>,
        elements: &mut Vec<Element>,
        route: &mut Route,
        sim: SimContext<'_, Shared>,
    ) {
        // waits until the initial delay
        sim.advance(self.initial_delay).await;

        // initializes endpoints of the flow
        self.init_endpoints(endpoints);

        // computes the shortest path and set fibs
        for edge in self.graph.edge_references() {
            // finds the NodeIndex of start and end nodes of the path
            let start = self.graph.node_weight(edge.source()).unwrap();
            let end = self.graph.node_weight(edge.target()).unwrap();
            let &start_node_idx = indices.get(start).unwrap();
            let &end_node_idx = indices.get(end).unwrap();

            // fetches all shortest paths and randomly select one
            let shortest_paths: Vec<Vec<NodeIndex>> =
                route.compute_route(start_node_idx, end_node_idx);
            let random_idx = (*sim.shared().rng.borrow_mut()).gen_range(0..shortest_paths.len());
            let path = &shortest_paths[random_idx];
            println!("The path of flow {}: {:?}", self.id, path);

            // get fibs for elements along the path, results: element_id -> next_id
            let mut results: HashMap<usize, usize> = HashMap::new();
            for (idx, &node_idx) in path.iter().enumerate() {
                let &element_id = graph.node_weight(node_idx).unwrap();
                let next_id = match idx < path.len() - 1 {
                    true => *graph.node_weight(path[idx + 1]).unwrap(),
                    false => self.end_id[0],
                };
                results.insert(element_id, next_id);
            }
            println!("Results: {:?}", results);

            // set fibs for elements along the path
            for element in &mut *elements {
                match element {
                    Element::PacketSwitch(switch) => {
                        let id = switch.id();
                        if let Some(&next_id) = results.get(&id) {
                            switch.set_fib(self.id, next_id);
                            println!("fib for Switch {} is: {:?}", switch.id(), switch.get_fib());
                            println!(
                                "keys of port_senders for Switch {} is: {:?}",
                                switch.id(),
                                switch.port_senders.keys()
                            );
                        }
                    }
                    _ => continue,
                }
            }
        }

        println!("Flow {} finishes setting at time {}", self.id, sim.now());
    }
}

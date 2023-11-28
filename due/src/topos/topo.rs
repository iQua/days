use std::collections::HashMap;

use petgraph::graph::{NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use rand::Rng;
use tokio::sync::mpsc::unbounded_channel;

use crate::flows::flow::Flow;
use crate::flows::route::{Route, Routing};
use crate::flows::EndPoint;
use crate::sim::SimContext;
use crate::switches::Element;
use crate::Shared;

pub struct Topology {
    /// Undirected graph of the topology
    graph: UnGraph<usize, ()>,
    /// A Vec of element ids that connects to endpoints
    hosts: Vec<usize>,
    /// A Vec of PacketSwitchs and Splitters
    elements: Vec<Element>,
    /// A Vec of PacketSources and PacketSinks
    endpoints: Vec<EndPoint>,
    /// A Vec of all flows
    flows: Vec<Flow>,
    /// Routing module
    routing: Route,
}

impl Topology {
    pub fn new(
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        elements: Vec<Element>,
        endpoints: Vec<EndPoint>,
        flows: Vec<Flow>,
    ) -> Topology {
        Topology {
            graph: graph.clone(),
            elements,
            endpoints,
            hosts,
            flows,
            routing: Route::new(graph),
        }
    }

    /// connects a vector of elements according to edges in the network topology.
    pub fn connect(&mut self) {
        for node_id in self.graph.node_indices() {
            let (sender, receiver) = unbounded_channel();
            self.elements[node_id.index()].connect_receiver(receiver);

            for neighbor in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                if neighbor.index() != node_id.index() {
                    self.elements[neighbor.index()].connect_sender(node_id.index(), sender.clone());
                }
            }
        }
    }

    /// attaches packet endpoints (sources or sinks) to hosts in the network graph.
    pub fn attach(&mut self, attach_to: Vec<usize>) {
        // the number of endpoints should be equal to the number of hosts they
        // attach to
        assert_eq!(self.endpoints.len(), attach_to.len());

        let mut endpoint_iter = self.endpoints.iter_mut();

        // attaches each endpoint's sender to its corresponding host's receiver
        for host_id in attach_to {
            assert!(self.hosts.contains(&host_id));

            // locate a neighboring element in the network graph to this host
            let mut neighbors = self.graph.neighbors(NodeIndex::new(host_id));

            let (downlink_sender, downlink_receiver) = unbounded_channel();
            let endpoint = endpoint_iter.next().unwrap();

            if let Some(next_neighbor) = neighbors.next() {
                if next_neighbor.index() != host_id {
                    self.elements[next_neighbor.index()].connect_neighbour_to_endpoint(
                        endpoint,
                        downlink_receiver,
                        host_id,
                    );
                }
                self.elements[host_id].connect_sender(endpoint.id(), downlink_sender);
            } else {
                panic!("No neighbors found for host element {}", host_id);
            }
        }
    }

    /// computes shortest paths for all flows, and sets fibs for all switches.
    pub fn set(&mut self, sim: SimContext<'_, Shared>) -> Vec<usize> {
        // element_id -> Vec<(flow_id, next_id)>
        let mut results: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
        let mut attach_to = Vec::new();
        for flow in &self.flows {
            for edge in flow.graph.edge_references() {
                // finds the index of start and end nodes of the path
                let &start = flow.graph.node_weight(edge.source()).unwrap();
                let &end = flow.graph.node_weight(edge.target()).unwrap();
                attach_to.push(start);
                attach_to.push(end);

                // fetches all shortest paths and randomly select one
                let shortest_paths: Vec<Vec<NodeIndex>> = self
                    .routing
                    .compute_route(NodeIndex::new(start), NodeIndex::new(end));
                let random_idx =
                    (*sim.shared().rng.borrow_mut()).gen_range(0..shortest_paths.len());
                let path = &shortest_paths[random_idx];
                println!("The path of flow {}: {:?}", flow.id, path);

                // gets fibs for elements along the path
                for (idx, &node_idx) in path.iter().enumerate() {
                    let element_id = self.graph.node_weight(node_idx).unwrap();
                    let mut next_id = usize::MAX;
                    let (flow_id, next_id) = match idx < path.len() - 1 {
                        true => {
                            next_id = *self.graph.node_weight(path[idx + 1]).unwrap();
                            (flow.id, next_id)
                        }
                        false => {
                            for endpoint in &self.endpoints {
                                match endpoint {
                                    EndPoint::PacketSink(sink) => {
                                        if sink.flow_id() == flow.id {
                                            next_id = sink.id();
                                            println!(
                                                "Sink {}'s flow id: {}",
                                                sink.id(),
                                                sink.flow_id()
                                            );
                                            break;
                                        }
                                    }
                                    _ => continue,
                                }
                            }
                            (flow.id, next_id)
                        }
                    };
                    results
                        .entry(*element_id)
                        .or_default()
                        .push((flow_id, next_id));
                }
            }
        }
        println!("Results: {:?}", results);

        // sets fibs for all switch elemetns
        for element in &mut self.elements {
            match element {
                Element::PacketSwitch(switch) => {
                    let id = switch.id();
                    let flow_to_next = results.get(&id).unwrap();
                    for (flow_id, next_id) in flow_to_next {
                        switch.set_fib(*flow_id, *next_id);
                    }
                    println!("fib for Switch {} is: {:?}", switch.id(), switch.get_fib());
                    println!(
                        "keys of port_senders for Switch {} is: {:?}",
                        switch.id(),
                        switch.port_senders.keys()
                    );
                }
                Element::Splitter(_) => continue,
            }
        }
        attach_to
    }

    pub fn run(mut self, sim: SimContext<'_, Shared>) {
        // constructs the network graph with network elements
        self.connect();
        // computes shortest paths for all flows, and sets fibs for all switches
        let attach_to = self.set(sim);
        // attaches sources and sinks to hosts in the network graph
        self.attach(attach_to);

        for endpoint in self.endpoints {
            match endpoint {
                EndPoint::PacketSource(source) => {
                    sim.activate(source.run(sim));
                }
                EndPoint::PacketSink(sink) => {
                    sim.activate(sink.run(sim));
                }
            }
        }

        for element in self.elements {
            element.activate(sim);
        }
    }
}

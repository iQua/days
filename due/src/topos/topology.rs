use std::collections::HashMap;

use petgraph::algo::dijkstra;
use petgraph::graph::{NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use tokio::sync::mpsc::unbounded_channel;

use crate::flow::flow::Flow;
use crate::packets::EndPoint;
use crate::sim::SimContext;
use crate::switches::Element;
use crate::Shared;

pub struct Topology {
    /// Undirected graph of the topology
    graph: UnGraph<usize, ()>,
    /// A HashMap to get the NodeIndex based on the element id
    indices: HashMap<usize, NodeIndex>,
    /// A Vec of element ids that connects to endpoints
    hosts: Vec<usize>,
    /// A Vec of PacketSwitchs and Splitters
    elements: Vec<Element>,
    /// A Vec of PacketSources and PacketSinks
    endpoints: Vec<EndPoint>,
}

impl Topology {
    pub fn new(
        graph: UnGraph<usize, ()>,
        indices: HashMap<usize, NodeIndex>,
        hosts: Vec<usize>,
        elements: Vec<Element>,
        endpoints: Vec<EndPoint>,
    ) -> Topology {
        Topology {
            graph,
            indices,
            elements,
            endpoints,
            hosts,
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
    pub fn set(&mut self, flows: Vec<Flow>) {
        for flow in flows {
            // computes the shortest paths for the flow
            let mut path = Vec::new();
            for edge in flow.graph.edge_references() {
                // start and end are element id of the host elements for the
                // flow, while start_node_idx is the NodeIndex of the start
                // host element of the flow
                let start = flow.graph.node_weight(edge.source()).unwrap();
                let end = flow.graph.node_weight(edge.target()).unwrap();
                let start_node_idx = self.indices.get(start).unwrap();
                let end_node_idx = self.indices.get(end).unwrap();

                // todo: compute the shortest path, then push it to paths
            }

            // set fibs for all elements along the path
        }
    }

    pub fn run(self, sim: SimContext<'_, Shared>) {
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

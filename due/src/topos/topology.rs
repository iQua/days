use petgraph::graph::{NodeIndex, UnGraph};
use tokio::sync::mpsc::unbounded_channel;

use crate::packets::EndPoint;
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
}

impl Topology {
    pub fn new(
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        elements: Vec<Element>,
        endpoints: Vec<EndPoint>,
    ) -> Topology {
        Topology {
            graph,
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

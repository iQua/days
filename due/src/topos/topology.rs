use petgraph::graph::{NodeIndex, UnGraph};
use tokio::sync::mpsc::unbounded_channel;

use crate::sim::SimContext;
use crate::{Element, EndPoint, Shared};

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

            match &mut self.elements[node_id.index()] {
                Element::PacketSwitch(switch) => {
                    switch.connect_receiver(receiver);
                }
                Element::Splitter(splitter) => {
                    splitter.connect_receiver(receiver);
                }
            }

            for neighbor in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                if neighbor.index() != node_id.index() {
                    match &mut self.elements[neighbor.index()] {
                        Element::PacketSwitch(switch) => {
                            switch.connect_sender(node_id.index(), sender.clone());
                        }
                        Element::Splitter(splitter) => {
                            splitter.connect_sender(node_id.index(), sender.clone());
                        }
                    }
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
            println!("Attaching endpoint to host {}.", host_id);
            assert!(self.hosts.contains(&host_id));

            // locate a neighboring element in the network graph to this host
            let mut neighbors = self.graph.neighbors(NodeIndex::new(host_id));
            println!("Host {} has neighbors.", host_id);
            println!("The neighbors are: {:?}", neighbors);

            let (downlink_sender, downlink_receiver) = unbounded_channel();
            let mut endpoint = endpoint_iter.next().unwrap();

            if let Some(next_neighbor) = neighbors.next() {
                if next_neighbor.index() != host_id {
                    println!("Processing neighbor {}.", next_neighbor.index());
                    match &mut self.elements[next_neighbor.index()] {
                        Element::PacketSwitch(switch) => {
                            println!("Processing switch {}.", switch.id());
                            let uplink_sender = switch.get_sender(host_id).unwrap();

                            match &mut endpoint {
                                EndPoint::PacketSource(source) => {
                                    // attaches each endpoint's sender to its corresponding host's receiver
                                    source.connect_sender(uplink_sender);
                                    // attaches each endpoint's receiver to its corresponding host's sender
                                    source.connect_receiver(downlink_receiver);
                                }
                                EndPoint::PacketSink(sink) => {
                                    println!("Processing sink {}.", sink.id());
                                    sink.connect_sender(uplink_sender);
                                    sink.connect_receiver(downlink_receiver);
                                }
                            }
                        }
                        Element::Splitter(splitter) => {
                            let uplink_sender = splitter.get_sender(host_id).unwrap();

                            match &mut endpoint {
                                EndPoint::PacketSource(source) => {
                                    source.connect_sender(uplink_sender);
                                    // attaches each endpoint's receiver to its corresponding host's sender
                                    source.connect_receiver(downlink_receiver);
                                }
                                EndPoint::PacketSink(sink) => {
                                    sink.connect_sender(uplink_sender);
                                    sink.connect_receiver(downlink_receiver);
                                }
                            }
                        }
                    }
                }

                match &mut self.elements[host_id] {
                    Element::PacketSwitch(switch) => {
                        switch.connect_sender(usize::MAX, downlink_sender);
                    }
                    Element::Splitter(splitter) => {
                        splitter.connect_sender(usize::MAX, downlink_sender);
                    }
                }
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
            match element {
                Element::PacketSwitch(switch) => {
                    sim.activate(switch.run(sim));
                }
                Element::Splitter(splitter) => {
                    sim.activate(splitter.run());
                }
            }
        }
    }
}

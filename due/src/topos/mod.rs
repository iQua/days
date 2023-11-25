pub mod builders;

use petgraph::graph::{NodeIndex, UnGraph};
use statrs::statistics::Distribution;
use tokio::sync::mpsc::unbounded_channel;

use crate::sim::Time;
use crate::{Element, EndPoint};

pub struct Topology<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    graph: UnGraph<i32, ()>,
    hosts: Vec<usize>,
    elements: Vec<Element>,
    endpoints: Vec<EndPoint<A, B>>,
}

impl<A, B> Topology<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    pub fn new(
        graph: UnGraph<i32, ()>,
        elements: Vec<Element>,
        endpoints: Vec<EndPoint<A, B>>,
        hosts: Vec<usize>,
    ) -> Topology<A, B> {
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

            for neighbor_index in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                match &mut self.elements[neighbor_index.index()] {
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
            let mut endpoint = endpoint_iter.next().unwrap();

            if let Some(first_neighbor) = neighbors.next() {
                match &mut self.elements[first_neighbor.index()] {
                    Element::PacketSwitch(switch) => {
                        let uplink_sender = switch.get_sender(host_id).unwrap();

                        match &mut endpoint {
                            EndPoint::PacketSource(source) => {
                                // attaches each endpoint's sender to its corresponding host's receiver
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
}

pub mod builders;

use petgraph::graph::{NodeIndex, UnGraph};
use tokio::sync::mpsc::unbounded_channel;

use crate::{Element, EndPoint};

pub struct Topology {
    graph: UnGraph<i32, ()>,
    hosts: Vec<usize>,
    endpoints: Vec<Box<dyn EndPoint>>,
}

impl Topology {
    pub fn new(graph: UnGraph<i32, ()>) -> Topology {
        Topology {
            graph,
            hosts: Vec::new(),
            endpoints: Vec::new(),
        }
    }

    pub fn construct(&mut self, elements: &mut [Box<&mut dyn Element>]) {
        let mut next_id = 0;

        for node_id in self.graph.node_indices() {
            // node indices should be assigned in order starting from 0
            assert!(node_id.index() == next_id);
            self.connect(elements);
            next_id += 1;
        }
    }

    pub fn set_hosts(&mut self, host_indices: Vec<usize>) {
        self.hosts = host_indices.clone();
    }

    /// connects a vector of elements according to edges in the network topology.
    fn connect(&mut self, elements: &mut [Box<&mut dyn Element>]) {
        for node_id in self.graph.node_indices() {
            let (sender, receiver) = unbounded_channel();

            elements[node_id.index()].connect_receiver(receiver);

            for neighbor_index in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                elements[neighbor_index.index()].connect_sender(node_id.index(), sender.clone());
            }
        }
    }

    /// attaches packet endpoints (sources or sinks) to hosts in the network graph.
    pub fn attach(
        &mut self,
        mut elements: Vec<Box<&mut dyn Element>>,
        mut endpoints: Vec<Box<dyn EndPoint>>,
        attach_to: Vec<usize>,
    ) {
        // the number of endpoints should be equal to the number of hosts they
        // attach to
        assert_eq!(endpoints.len(), attach_to.len());

        let mut endpoint_iter = endpoints.iter_mut();

        // attaches each endpoint's sender to its corresponding host's receiver
        for host_id in attach_to {
            // locate a neighboring element in the network graph to this host
            let mut neighbors = self.graph.neighbors(NodeIndex::new(host_id));

            if let Some(first_neighbor) = neighbors.next() {
                let sender = elements[first_neighbor.index()]
                    .get_sender(host_id)
                    .unwrap();
                let endpoint = endpoint_iter.next().unwrap();
                endpoint.connect_sender(sender);

                // attaches each endpoint's receiver to its corresponding host's sender
                let (sender, receiver) = unbounded_channel();
                endpoint.connect_receiver(receiver);
                elements[host_id].connect_sender(0, sender);
            } else {
                panic!("No neighbors found for host element {}", host_id);
            }
        }

        for endpoint in endpoints {
            self.endpoints.push(endpoint);
        }
    }
}

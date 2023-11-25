pub mod builders;

use petgraph::graph::UnGraph;
use tokio::sync::mpsc::unbounded_channel;

use crate::Element;

/// Connects a collection of homogeneous upstream elements to a downstream element.
pub fn connect_n_1_homo(upstream: &mut [impl Element], downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    for element in upstream {
        element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}

/// Connects a collection of heterogeneous upstream elements to a downstream element.
pub fn connect_n_1_hetero(upstream: &mut [Box<&mut dyn Element>], downstream: &mut dyn Element) {
    let (sender, receiver) = unbounded_channel();

    for upstream_element in upstream.iter_mut() {
        upstream_element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}

/// Connects an upstream element to a downstream element.
pub fn connect_pair(upstream: &mut impl Element, downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    upstream.connect_sender(sender);
    downstream.connect_receiver(receiver);
}

/// Connects an upstream element to a collection of homogeneous downstream elements.
pub fn connect_1_n(upstream: &mut impl Element, downstream: &mut [impl Element]) {
    for element in downstream {
        let (sender, receiver) = unbounded_channel();
        upstream.connect_sender(sender);
        element.connect_receiver(receiver);
    }
}

pub struct Topology {
    graph: UnGraph<i32, ()>,
}

impl Topology {
    pub fn new(graph: UnGraph<i32, ()>) -> Topology {
        Topology { graph }
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

    /// Connects a vector of elements according to edges in the network topology.
    fn connect(&mut self, elements: &mut [Box<&mut dyn Element>]) {
        for node_id in self.graph.node_indices() {
            let (sender, receiver) = unbounded_channel();

            elements[node_id.index()].connect_receiver(receiver);

            for neighbor_index in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this downstream
                // element in the provided network graph, then connect them
                elements[neighbor_index.index()].connect_sender(sender.clone());
            }
        }
    }
}

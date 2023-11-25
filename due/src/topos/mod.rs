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

/// Connects multiple heterogeneous upstream elements to multiple heterogeneous
/// downstream elements. The connections are established based on the bipartite
/// graph `edges`, where each Vec corresponds to a specific upstream element and
/// contains the indices of downstream elements it should connect to.
pub fn connect(elements: &mut [Box<&mut dyn Element>], graph: UnGraph<i32, ()>) {
    for node_id in graph.node_indices() {
        let (sender, receiver) = unbounded_channel();

        elements[node_id.index()].connect_receiver(receiver);

        for neighbor_index in graph.neighbors(node_id) {
            // if an edge exists between an upstream element and this downstream
            // element in the provided network graph, then connect them
            elements[neighbor_index.index()].connect_sender(sender.clone());
        }
    }
}

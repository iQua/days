pub mod builders;
pub mod fattree;

use crate::Element;
use tokio::sync::mpsc::unbounded_channel;

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
pub fn connect_n_m(
    upstream: &mut [Box<&mut dyn Element>],
    downstream: &mut [Box<&mut dyn Element>],
    edges: Vec<Vec<usize>>,
) {
    for downstream_element in downstream {
        let (sender, receiver) = unbounded_channel();

        downstream_element.connect_receiver(receiver);
        let id = downstream_element.id();

        for (i, upstream_element) in upstream.iter_mut().enumerate() {
            if edges[i].contains(&id) {
                upstream_element.connect_sender(sender.clone());
            }
        }
    }
}

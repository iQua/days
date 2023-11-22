use rand::rngs::SmallRng;
use std::cell::RefCell;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub mod packets;
pub mod schedulers;
pub mod sim;
pub mod switches;

use crate::packets::packet::Packet;
use crate::sim::{RandomVar, Time};

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub queueing_delay: RandomVar,
    pub duration: Time,
}

/// Element is a trait that defines the interface for all elements in the network.
pub trait Element {
    fn id(&mut self) -> usize;

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
}

/// Connects a collection of homogeneous upstream elements to a downstream element.
pub fn connect_n_1(upstream: &mut [impl Element], downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    for element in upstream {
        element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}

/// Connects a collection of upstream elements to a downstream element.
pub fn connect_n_1_new(upstream: &mut [Box<&mut dyn Element>], downstream: &mut impl Element) {
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

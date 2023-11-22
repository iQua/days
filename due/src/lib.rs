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
pub fn connect_n_1_homo(upstream: &mut [impl Element], downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    for element in upstream {
        element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}

/// Connects a collection of heterogeneous upstream elements to a downstream element.
pub fn connect_n_1_hetero(upstream: &mut [Box<&mut dyn Element>], downstream: &mut impl Element) {
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

/// This function returns the ids of aggregation layer switches and packet
/// generators that send packets to a given edge layer switch.
pub fn elements_to_edge(k: usize, edge_id: usize) -> (Vec<usize>, Vec<usize>) {
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    let hosts_per_switch = k / 2;
    assert!(
        edge_id < switches_per_layer,
        "Invalid edge layer switch id."
    );

    let pod_id = edge_id / pod_switches_per_layer;
    let agg_start = switches_per_layer + pod_id * pod_switches_per_layer;
    let host_start = edge_id * hosts_per_switch;

    let agg_ids = (agg_start..agg_start + pod_switches_per_layer).collect::<Vec<_>>();
    let generator_ids = (host_start..host_start + hosts_per_switch).collect::<Vec<_>>();

    (agg_ids, generator_ids)
}

/// This function returns the ids of core layer switches and edge layer switches
/// that send packets to a given aggregation layer switch.
pub fn elements_to_agg(k: usize, agg_id: usize) -> (Vec<usize>, Vec<usize>) {
    let core_switches = (k / 2).pow(2);
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    let core_switches_per_agg = core_switches / pod_switches_per_layer;
    assert!(
        agg_id >= switches_per_layer && agg_id < 2 * switches_per_layer,
        "Invalid aggregation layer switch id."
    );

    let pod_id = (agg_id - switches_per_layer) / pod_switches_per_layer;
    let core_start =
        2 * switches_per_layer + core_switches_per_agg * (agg_id % pod_switches_per_layer);
    let edge_start = pod_id * pod_switches_per_layer;

    let core_ids = (core_start..core_start + core_switches_per_agg).collect::<Vec<_>>();
    let edge_ids = (edge_start..edge_start + pod_switches_per_layer).collect::<Vec<_>>();

    (core_ids, edge_ids)
}

/// This function returns the ids of aggregation layer switches that send
/// packets to teh given core layer switch.
pub fn elements_to_core(k: usize, core_id: usize) -> Vec<usize> {
    let core_switches = (k / 2).pow(2);
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    assert!(core_id >= 2 * switches_per_layer && core_id < 2 * switches_per_layer + core_switches);

    let agg_start = switches_per_layer;
    let core_type = (core_id - 2 * switches_per_layer) / pod_switches_per_layer;
    let agg_ids = (agg_start + core_type..agg_start + switches_per_layer)
        .step_by(pod_switches_per_layer)
        .collect::<Vec<_>>();
    agg_ids
}

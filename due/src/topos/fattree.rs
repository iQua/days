//! This file provides standard methods to construct a fattree topology, as well
//! as connecting and activating elements inside of the fattree topology.
//! Besides, three helper functions are also provided to get the identifiers of
//! elements that will send packets to a given device.

use statrs::statistics::Distribution;

use crate::packets::dist_generator::DistPacketGenerator;
use crate::packets::sink::PacketSink;
use crate::sim::{SimContext, Time};
use crate::switches::switch::PacketSwitch;
use crate::topos::{connect_n_1_hetero, connect_pair};
use crate::{get_id, Element, Shared};

pub struct FatTree<A, B>
where
    A: Distribution<Time> + 'static,
    B: Distribution<f64> + 'static,
{
    k: usize,
    /// packet generators in hosts
    pub generators: Vec<DistPacketGenerator<A, B>>,
    /// packet sinks of in hosts
    pub sinks: Vec<PacketSink>,
    /// edge-layer switches
    edge_switches: Vec<PacketSwitch>,
    /// aggregation-layer switches
    agg_switches: Vec<PacketSwitch>,
    /// core-layer switches
    core_switches: Vec<PacketSwitch>,
}

impl<A, B> FatTree<A, B>
where
    A: Distribution<Time> + 'static,
    B: Distribution<f64> + 'static,
{
    pub fn new(k: usize, generator: DistPacketGenerator<A, B>) -> FatTree<A, B> {
        assert!(k > 0 && k % 2 == 0, "Invalid k!");

        // initializes all hosts
        let num_hosts = k.pow(3) / 4;
        let mut generators = Vec::new();
        let mut sinks = Vec::new();

        for _ in 0..num_hosts {
            let pg = generator.clone();
            let sink = PacketSink::new(get_id());
            generators.push(pg);
            sinks.push(sink);
        }
        drop(generator);

        FatTree {
            k,
            generators,
            sinks,
            edge_switches: Vec::new(),
            agg_switches: Vec::new(),
            core_switches: Vec::new(),
        }
    }

    pub fn set_switches(
        &mut self,
        edge_switches: Vec<PacketSwitch>,
        agg_switches: Vec<PacketSwitch>,
        core_switches: Vec<PacketSwitch>,
    ) {
        self.edge_switches = edge_switches;
        self.agg_switches = agg_switches;
        self.core_switches = core_switches;
    }

    fn connect(&mut self) {
        // connects edge-layer switches to sinks
        for (sink_idx, sink) in self.sinks.iter_mut().enumerate() {
            let switch_idx = sink_idx / 2;
            connect_pair(self.edge_switches.get_mut(switch_idx).unwrap(), sink);
        }

        // connects elements that send packets to edge-layer switches
        for (edge_idx, edge_switch) in self.edge_switches.iter_mut().enumerate() {
            let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
            let (agg_idxs, generator_idxs) = elements_to_edge(self.k, edge_idx);

            for (agg_idx, switch) in self.agg_switches.iter_mut().enumerate() {
                if agg_idxs.contains(&agg_idx) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            for (generator_idx, generator) in self.generators.iter_mut().enumerate() {
                if generator_idxs.contains(&generator_idx) {
                    upstreams.push(Box::new(generator as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, edge_switch);
        }

        // connects elements that send packets to aggregation-layer switches
        for (agg_idx, agg_switch) in self.agg_switches.iter_mut().enumerate() {
            let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
            let (core_idxs, edge_idxs) = elements_to_agg(self.k, agg_idx);

            for (core_idx, switch) in self.core_switches.iter_mut().enumerate() {
                if core_idxs.contains(&core_idx) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            for (edge_idx, switch) in self.edge_switches.iter_mut().enumerate() {
                if edge_idxs.contains(&edge_idx) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, agg_switch);
        }

        // connects aggregation-layer switches and core-layer switches
        for (core_idx, core_switch) in self.core_switches.iter_mut().enumerate() {
            let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
            let agg_idxs = elements_to_core(self.k, core_idx);

            for (agg_idx, switch) in self.agg_switches.iter_mut().enumerate() {
                if agg_idxs.contains(&agg_idx) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, core_switch);
        }
    }

    pub fn run(mut self, sim: SimContext<'_, Shared>) {
        // connects all elements in the fattree topology
        self.connect();

        // activates all elements in the fattree topology
        for generator in self.generators {
            sim.activate(generator.run(sim));
        }
        for sink in self.sinks {
            sim.activate(sink.run(sim));
        }
        for switch in self.core_switches {
            sim.activate(switch.run(sim));
        }
        for switch in self.agg_switches {
            sim.activate(switch.run(sim));
        }
        for switch in self.edge_switches {
            sim.activate(switch.run(sim));
        }
    }
}

/// This function returns the indexes of switches in the aggregation layer and
/// generators that send packets to a given edge layer switch.
fn elements_to_edge(k: usize, edge_idx: usize) -> (Vec<usize>, Vec<usize>) {
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    let hosts_per_switch = k / 2;
    assert!(
        edge_idx < switches_per_layer,
        "Invalid edge layer switch idx."
    );

    let pod_idx = edge_idx / pod_switches_per_layer;
    let agg_start = pod_idx * pod_switches_per_layer;
    let host_start = edge_idx * hosts_per_switch;

    let agg_idxs = (agg_start..agg_start + pod_switches_per_layer).collect::<Vec<_>>();
    let generator_idxs = (host_start..host_start + hosts_per_switch).collect::<Vec<_>>();

    (agg_idxs, generator_idxs)
}

/// This function returns the indexes of switches in the core layer and the edge
/// layer that send packets to a given aggregation layer switch.
fn elements_to_agg(k: usize, agg_idx: usize) -> (Vec<usize>, Vec<usize>) {
    let core_switches = (k / 2).pow(2);
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    let core_switches_per_agg = core_switches / pod_switches_per_layer;
    assert!(
        agg_idx < switches_per_layer,
        "Invalid aggregation layer switch idx."
    );

    let pod_idx = agg_idx / pod_switches_per_layer;
    let core_start = core_switches_per_agg * (agg_idx % pod_switches_per_layer);
    let edge_start = pod_idx * pod_switches_per_layer;

    let core_idxs = (core_start..core_start + core_switches_per_agg).collect::<Vec<_>>();
    let edge_idxs = (edge_start..edge_start + pod_switches_per_layer).collect::<Vec<_>>();

    (core_idxs, edge_idxs)
}

/// This function returns the indexes of switches in aggregation layer that send
/// packets to the given core layer switch.
fn elements_to_core(k: usize, core_idx: usize) -> Vec<usize> {
    let core_switches = (k / 2).pow(2);
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    assert!(core_idx < core_switches, "Invalid core layer switch idx.");

    let core_type = core_idx / pod_switches_per_layer;
    (core_type..switches_per_layer)
        .step_by(pod_switches_per_layer)
        .collect::<Vec<_>>()
}

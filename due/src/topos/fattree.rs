//! Constructs a FatTree topology with the given parameters.

use std::collections::HashMap;
use std::sync::Arc;

use rand::Rng;
use statrs::statistics::Distribution;

use crate::packets::sink::Sink;
use crate::packets::source::Source;
use crate::sim::{SimContext, Time};
use crate::switches::switch::PacketSwitch;
use crate::switches::SchedulingDiscipline;
use crate::topos::{connect_n_1_hetero, connect_pair};
use crate::{Element, Shared};

pub struct FatTree<A, B>
where
    A: Distribution<Time> + 'static,
    B: Distribution<f64> + 'static,
{
    k: usize,
    /// the port rate of each port in the switches
    port_rate: f64,
    /// the buffer capacity of each port in the switches
    capacity: usize,
    /// flow_id -> class_id
    flow_classes: Arc<dyn Fn(usize) -> usize>,
    /// class_id -> weight
    weights: Vec<usize>,
    /// Flow Information Base: flow_id -> destination port
    fib: Vec<usize>,
    /// scheduling discipline at each switch
    scheduling_discipline: SchedulingDiscipline,
    /// packet generators in the hosts
    pub generators: Vec<Source<A, B>>,
    /// packet sinks in the hosts
    sinks: Vec<Sink>,
    /// edge-layer switches
    edge_switches: Vec<PacketSwitch>,
    /// aggregation-layer switches
    agg_switches: Vec<PacketSwitch>,
    /// core-layer switches
    core_switches: Vec<PacketSwitch>,
}

impl<A, B> FatTree<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    pub fn new(
        k: usize,
        port_rate: f64,
        capacity: usize,
        flow_classes: Arc<dyn Fn(usize) -> usize>,
        weights: Vec<usize>,
        fib: Vec<usize>,
        scheduling_discipline: SchedulingDiscipline,
        generator: Source<A, B>,
        sink: Sink,
    ) -> FatTree<A, B> {
        assert!(k > 0 && k % 2 == 0, "The value of parameter k is invalid.");

        // initializes all hosts
        let num_hosts = k.pow(3) / 4;
        let mut generators = Vec::new();
        let mut sinks = Vec::new();

        for _ in 1..num_hosts {
            generators.push(generator.clone());
            sinks.push(sink.clone());
        }
        generators.push(generator);
        sinks.push(sink);

        FatTree {
            k,
            port_rate,
            capacity,
            flow_classes,
            weights,
            fib,
            scheduling_discipline,
            generators,
            sinks,
            edge_switches: Vec::new(),
            agg_switches: Vec::new(),
            core_switches: Vec::new(),
        }
    }

    pub fn activate(mut self, sim: SimContext<'_, Shared>) {
        // constructs and connects all elements in the FatTree topology
        self.construct();
        self.connect();

        // activates all elements in the FatTree topology
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

    fn construct(&mut self) {
        // initializes number of elements for all layers
        let num_core_switches = (self.k / 2).pow(2);
        let num_agg_switches = self.k.pow(2) / 2;
        let num_edge_switches = self.k.pow(2) / 2;

        // initializes switches in the edge layer
        for _ in 0..num_edge_switches {
            let switch = PacketSwitch::new(
                self.k,
                self.port_rate,
                self.capacity,
                self.weights.clone(),
                self.fib.clone(),
                self.scheduling_discipline.clone(),
                self.flow_classes.clone(),
            );
            self.edge_switches.push(switch);
        }

        // initializes switches in the aggregation layer
        for _ in 0..num_agg_switches {
            let switch = PacketSwitch::new(
                self.k,
                self.port_rate,
                self.capacity,
                self.weights.clone(),
                self.fib.clone(),
                self.scheduling_discipline.clone(),
                self.flow_classes.clone(),
            );
            self.agg_switches.push(switch);
        }

        // initializes switches in the core layer
        for _ in 0..num_core_switches {
            let switch = PacketSwitch::new(
                self.k,
                self.port_rate,
                self.capacity,
                self.weights.clone(),
                self.fib.clone(),
                self.scheduling_discipline.clone(),
                self.flow_classes.clone(),
            );
            self.core_switches.push(switch);
        }
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
            let (agg_idxs, generator_idxs) = FatTree::<A, B>::elements_to_edge(self.k, edge_idx);

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
            let (core_idxs, edge_idxs) = FatTree::<A, B>::elements_to_agg(self.k, agg_idx);

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
            let agg_idxs = FatTree::<A, B>::elements_to_core(self.k, core_idx);

            for (agg_idx, switch) in self.agg_switches.iter_mut().enumerate() {
                if agg_idxs.contains(&agg_idx) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, core_switch);
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
            "Invalid edge layer switch index."
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
            "Invalid aggregation layer switch index."
        );

        let pod_idx = agg_idx / pod_switches_per_layer;
        let core_start = core_switches_per_agg * (agg_idx % pod_switches_per_layer);
        let edge_start = pod_idx * pod_switches_per_layer;

        let core_idxs = (core_start..core_start + core_switches_per_agg).collect::<Vec<_>>();
        let edge_idxs = (edge_start..edge_start + pod_switches_per_layer).collect::<Vec<_>>();

        (core_idxs, edge_idxs)
    }

    /// This function returns the indexes of switches in the aggregation layer that
    /// send packets to the given core layer switch.
    fn elements_to_core(k: usize, core_idx: usize) -> Vec<usize> {
        let core_switches = (k / 2).pow(2);
        let pod_switches_per_layer = k / 2;
        let switches_per_layer = pod_switches_per_layer * k;
        assert!(core_idx < core_switches, "Invalid core layer switch index.");

        let core_type = core_idx / pod_switches_per_layer;
        (core_type..switches_per_layer)
            .step_by(pod_switches_per_layer)
            .collect::<Vec<_>>()
    }
}

/// This function is used to generate path of indexes for a given pair of
/// generator and sink index.
pub fn get_path(k: usize, generator_idx: usize, sink_idx: usize, shared: &Shared) -> Vec<usize> {
    let mut path = Vec::new();
    let pod_switches_per_layer = k / 2;
    let hosts_per_switch = k / 2;
    let hosts_per_pod = pod_switches_per_layer * hosts_per_switch;
    let core_switches = (k / 2).pow(2);
    let core_switches_per_agg = core_switches / pod_switches_per_layer;

    // adds the generator idx to the path
    path.push(generator_idx);

    // gets the indexes of edge-layer switch and the pod of both the generator
    // and the sink
    let edge_idx = generator_idx / hosts_per_switch;
    let pod_idx = generator_idx / hosts_per_pod;
    let dst_edge_idx = sink_idx / hosts_per_switch;
    let dst_pod_idx = sink_idx / hosts_per_pod;

    // adds the first edge-layer switch idx to the path
    path.push(edge_idx);

    if dst_edge_idx == edge_idx {
        // the generator and the sink connect to the same edge-layer switch
        path.push(sink_idx);
    } else if dst_pod_idx == pod_idx {
        // the generator and the sink belong to the same pod, but not same
        // edge-layer switch
        let agg_idx = shared
            .rng
            .borrow_mut()
            .gen_range(pod_idx * pod_switches_per_layer..(pod_idx + 1) * pod_switches_per_layer);
        path.push(agg_idx);
        path.push(dst_edge_idx);
        path.push(sink_idx);
    } else {
        // the generator and the sinks belong to different pod

        // 1. randomly select an aggregation-layer switch
        let agg_idx = shared
            .rng
            .borrow_mut()
            .gen_range(pod_idx * pod_switches_per_layer..(pod_idx + 1) * pod_switches_per_layer);
        path.push(agg_idx);
        // 2. based on the aggregation-layer switch, randomly select a
        //    core-layer switch
        let core_start = core_switches_per_agg * (agg_idx % pod_switches_per_layer);
        let core_idx = shared
            .rng
            .borrow_mut()
            .gen_range(core_start..core_start + core_switches_per_agg);
        path.push(core_idx);
        // 3. connects the aggregation-layer switch in the destination pod
        let dst_agg_idx = dst_pod_idx + agg_idx % pod_switches_per_layer;
        path.push(dst_agg_idx);
        // 4. connects the edge-layer switch in the destination pod, as well as
        //    the sink
        path.push(dst_edge_idx);
        path.push(sink_idx);
    };

    path
}

/// This function is used to generate demux fib for all switches in the fattree
/// topology, based on the given fattree size k and routing information.
pub fn get_fibs(k: usize, paths: Vec<Vec<usize>>) -> Vec<HashMap<usize, usize>> {
    // TODO: Do we have better ideas to replace HashMap? Like just use Vec.

    let num_switches = 5 * k.pow(2) / 4;

    let mut fibs: Vec<HashMap<usize, usize>> = (0..num_switches).map(|_| HashMap::new()).collect();
    for path in paths.iter() {
        println!("{:?} with length {}", &path, &path.len());
        let flow_id = 1; // TODO: should place this function inside of FatTree!
    }

    fibs
}

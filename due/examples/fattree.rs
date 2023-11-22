//! This example shows an examples to simulate networks of fat tree datacenter
//! topology.

use std::cell::RefCell;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::{connect_n_1_hetero, connect_pair, Element, Shared};

const SEED: u64 = 1000;

/// This function returns the ids of aggregation layer switches and packet
/// generators that send packets to a given edge layer switch.
fn elements_to_edge(k: usize, edge_id: usize) -> (Vec<usize>, Vec<usize>) {
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
fn elements_to_agg(k: usize, agg_id: usize) -> (Vec<usize>, Vec<usize>) {
    let pod_switches_per_layer = k / 2;
    let switches_per_layer = pod_switches_per_layer * k;
    let core_switches_per_agg = k.pow(2) / 2 / pod_switches_per_layer;
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

async fn network_sim(k: usize, sim: SimContext<'_, Shared>) {
    assert!(k > 0 && k % 2 == 0, "Invalid k!");

    let num_core_switches = (k / 2).pow(2);
    let num_aggregation_switches = (k.pow(2)) / 2;
    let num_edge_switches = (k.pow(2)) / 2;
    let num_hosts = num_edge_switches * k / 2;

    let port_rate = (4000 * 8) as f64;
    let capacity = 100;

    // initializes packet generators and packet sinks
    let mut generators = Vec::new();
    let mut sinks = Vec::new();
    let arr_interval_dist = Box::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Box::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // initializes all (k^3)/4 hosts
    for i in 0..num_hosts {
        let generator =
            DistPacketGenerator::new(i, 0., arr_interval_dist.clone(), packet_size_dist.clone());
        let sink = PacketSink::new(i);
        generators.push(generator);
        sinks.push(sink);
    }

    // TODO:
    // modify weights, fib, flow_to_classes, and add dst for flows!
    let weights: Vec<_> = (1..=4).cycle().take(num_hosts).collect();
    let fib: Vec<_> = (0..=3).cycle().take(num_hosts).collect();

    // initializes switches in the edge layer
    let mut edge_switches = Vec::new();
    for i in 0..num_edge_switches {
        let switch = PacketSwitch::new(
            i,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        edge_switches.push(switch);
    }

    // initializes switches in the aggregation layer
    let mut aggregation_switches = Vec::new();
    for i in 0..num_aggregation_switches {
        let switch = PacketSwitch::new(
            i + num_edge_switches,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        aggregation_switches.push(switch);
    }

    // initializes switches in the core layer
    let mut core_switches = Vec::new();
    for i in 0..num_core_switches {
        let switch = PacketSwitch::new(
            i + num_edge_switches + num_aggregation_switches,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        core_switches.push(switch);
    }

    // TODO:
    // Includes some hard-coded parts, which should be REMOVED later!

    // connects edge layer switches to sinks
    for (sink_id, sink) in sinks.iter_mut().enumerate() {
        let switch_id = sink_id / 2;
        connect_pair(edge_switches.get_mut(switch_id).unwrap(), sink);
    }

    // connects elements that send packets to edge layer switches
    for edge_switch in edge_switches.iter_mut() {
        let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
        let (agg_ids, generator_ids) = elements_to_edge(k, edge_switch.id());

        for switch in &mut aggregation_switches {
            if agg_ids.contains(&switch.id()) {
                upstreams.push(Box::new(switch as &mut dyn Element));
            }
        }

        for generator in &mut generators {
            if generator_ids.contains(&generator.id()) {
                upstreams.push(Box::new(generator as &mut dyn Element));
            }
        }

        connect_n_1_hetero(&mut upstreams, edge_switch);
    }

    // connects elements that send packets to aggregation layer switches
    for aggregation_switch in aggregation_switches.iter_mut() {
        let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
        let (core_ids, edge_ids) = elements_to_agg(k, aggregation_switch.id());

        for switch in &mut core_switches {
            if core_ids.contains(&aggregation_switch.id()) {
                upstreams.push(Box::new(switch as &mut dyn Element));
            }
        }

        for switch in &mut edge_switches {
            if edge_ids.contains(&aggregation_switch.id()) {
                upstreams.push(Box::new(switch as &mut dyn Element));
            }
        }

        connect_n_1_hetero(&mut upstreams, aggregation_switch);
    }

    // connects aggregation layer switches and core layer switches

    // activates all elements
    for generator in generators {
        sim.activate(generator.run(sim));
    }
    for sink in sinks {
        sim.activate(sink.run(sim));
    }
    for switch in core_switches {
        sim.activate(switch.run(sim));
    }
    for switch in aggregation_switches {
        sim.activate(switch.run(sim));
    }
    for switch in edge_switches {
        sim.activate(switch.run(sim));
    }

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 10.,
        },
        |sim| Process::new(sim, network_sim(4, sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}

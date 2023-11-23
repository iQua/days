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
use due::topos::fattree::FatTree;
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(k: usize, sim: SimContext<'_, Shared>) {
    assert!(k > 0 && k % 2 == 0, "Invalid k!");

    let num_core_switches = (k / 2).pow(2);
    let num_agg_switches = (k.pow(2)) / 2;
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
    let mut agg_switches = Vec::new();
    for i in 0..num_agg_switches {
        let switch = PacketSwitch::new(
            i + num_edge_switches,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        agg_switches.push(switch);
    }

    // initializes switches in the core layer
    let mut core_switches = Vec::new();
    for i in 0..num_core_switches {
        let switch = PacketSwitch::new(
            i + num_edge_switches + num_agg_switches,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        core_switches.push(switch);
    }


    let mut fattree = FatTree::new(k, generators, sinks, core_switches, agg_switches, edge_switches);

    // connect all elements in the fattree topology
    fattree.connect();

    // activates all elements and waits for the end of this simulation
    sim.activate(fattree.run(sim));

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

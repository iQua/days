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
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(k: usize, sim: SimContext<'_, Shared>) {
    // initializes packet generators and packet sinks
    let mut generators = Vec::new();
    let mut sinks = Vec::new();
    let arr_interval_dist = Box::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Box::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // initializes all (k^3)/4 hosts
    for i in 0..k.pow(3) / 4 {
        let generator =
            DistPacketGenerator::new(i, 0., arr_interval_dist.clone(), packet_size_dist.clone());
        let sink = PacketSink::new(i);
        generators.push(generator);
        sinks.push(sink);
    }

    let port_rate = (4000 * 8) as f64;
    let capacity = 100;

    let num_core_switches = (k / 2).pow(2);
    let num_aggregation_switches = (k.pow(2)) / 2;
    let num_edge_switches = (k.pow(2)) / 2;

    // TODO:
    // modify weights, fib, flow_to_classes, and add dst for flows!
    let weights: Vec<_> = (1..=4).cycle().take(num_core_switches).collect();
    let fib: Vec<_> = vec![0, 1, 2, 3];

    // initializes switches in the core layer
    let mut core_switches = Vec::new();
    for i in 0..num_core_switches {
        let switch = PacketSwitch::new(
            i,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        core_switches.push(switch);
    }

    // initializes switches in the aggregation layer
    let mut aggregation_switches = Vec::new();
    for i in 0..num_aggregation_switches {
        let switch = PacketSwitch::new(
            i,
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        aggregation_switches.push(switch);
    }

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

    // connects hosts and edge layer switches

    // connects edge layer switches and aggregation layer switches

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

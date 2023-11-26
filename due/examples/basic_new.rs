//! This file is used for fattree simulation.

use std::cell::RefCell;

use due::topos::builders::{build, build_fattree};
use due::topos::initializers::{init_elements, init_endpoints};
use due::topos::topology::Topology;
use rand::{rngs::SmallRng, SeedableRng};

use due::sim::{simulation, Process, RandomVar, SimContext};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    // reproduces examples/basic.rs through builders and initializers
    let toml_path = "configs/simple.toml";
    
    let graph = build(toml_path);
    let elements = init_elements(toml_path);
    let endpoints = init_endpoints(toml_path);
    let hosts = vec![0, 1];

    let mut topology = Topology::new(graph, hosts, elements, endpoints);

    // constructs the network graph with network elements
    topology.connect();
    // attaches sources and sinks to hosts in the network graph
    topology.attach(vec![0, 1]);
    // runs the topology
    topology.run(sim);

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
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}

//! This file is used for fattree simulation.

use std::cell::RefCell;

use rand::{rngs::SmallRng, SeedableRng};

use due::flows::flow::Flow;
use due::topos::build::build_graph;
use due::topos::topo::Topology;

use due::sim::{simulation, Process, RandomVar, SimContext};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let file_path = "configs/simple.toml";

    let graph = build_graph(file_path);
    let hosts = vec![0, 1];

    let flows = Flow::flows_from_config(file_path);

    let topology = Topology::new(file_path, graph, hosts, flows);

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

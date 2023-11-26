//! This file is used for fattree simulation.

use std::cell::RefCell;

use due::topos::builders::build_fattree;
use due::topos::initializers::init_elements;
use rand::{rngs::SmallRng, SeedableRng};

use due::sim::{simulation, Process, RandomVar, SimContext};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let (graph, hosts) = build_fattree("configs/fattree.toml");

    println!("The fattree graph is:\n{:?}", graph);
    println!("The fattree hosts is:\n{:?}", hosts);

    // tests of initializer
    let elements = init_elements("configs/simple.toml");
    println!("{}", elements.len());
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

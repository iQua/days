//! This example shows how to create a basic network where two packet sources
//! send packets to a wire that adds propagation delays according to a random
//! distribution, and then to a packet sink.

use std::sync::Mutex;

use log::{debug, info};
use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};

use due::flows::flow::Flow;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos::topo::Topology;
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let graph = UnGraph::<usize, ()>::from_edges(&[(0, 1)]);
    let hosts = vec![0, 1];
    info!("The network graph has been initialized: {:?}", graph);

    let flows = Flow::flows_from_graph(vec![vec![(0, 1)], vec![(1, 0)]]);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    // network elements are initialized from a configuration file
    let topology = Topology::new("configs/simple.toml", graph, hosts, flows);

    // runs the topology
    topology.run(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let outcome = simulation(
        Shared {
            rng: Mutex::new(SmallRng::seed_from_u64(SEED)),
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

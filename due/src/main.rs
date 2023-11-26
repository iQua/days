//! The main program for running a simulation using a specific configuration.

use std::cell::RefCell;
use std::env;

use rand::{rngs::SmallRng, SeedableRng};

use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos::build::build_graph;
use due::topos::init::{init_elements, init_endpoints};
use due::topos::topology::Topology;
use due::{set_num_elements, Shared};

const SEED: u64 = 1000;

async fn network_sim(config_path: String, sim: SimContext<'_, Shared>) {
    let file_path = config_path.as_str();

    let graph = build_graph(file_path);
    set_num_elements(graph.node_count());

    let elements = init_elements(file_path);
    let endpoints = init_endpoints(file_path);
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
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Please provide the path to the toml configuration file: cargo run -- <path>");
    }

    let path = args[1].clone();

    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 10.,
        },
        |sim| Process::new(sim, network_sim(path, sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}

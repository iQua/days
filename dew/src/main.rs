//! The main program for running a simulation using a specific configuration.

use std::cell::RefCell;
use std::env;

use log::{debug, info};
use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};

use dew::flows::flow::Flow;
use dew::sim::{simulation, Process, RandomVar, SimContext};
// use dew::topos::build::build_graph;
use dew::topos::topo::Topology;
use dew::{get_seed, Shared};

async fn network_sim(config_path: String, sim: SimContext<'_, Shared>) {
    let file_path = config_path.as_str();

    // There are three ways of building a network graph:

    // 1. build_graph(file_path) -> builds a graph from a configuration file.
    //    Example:
    //    let graph = build_graph(file_path);
    //    let hosts = vec![0, 1];

    // 2. building a graph directly using UnGraph::<usize, ()>::from_edges().
    //    Example:
    //    let graph = UnGraph::<usize, ()>::from_edges(&[(0, 1)]);
    //    let hosts = vec![0, 1];

    // 3. building a graph using a graph builder.
    //    let (graph, hosts) = build_fattree();

    let graph = UnGraph::<usize, ()>::from_edges([(0, 1)]);
    let hosts = vec![0, 1];
    info!("The network graph has been initialized: {:?}", graph);

    // There are two ways of initializing the flows:

    // 1. initializes flows directly using flows_from_graph().
    //    Example:
    //    let flows = Flow::flows_from_graph(vec![vec![(0, 1)], vec![(1, 0)]]);

    // 2. initializes flows using a configuration file.
    //    Example:
    let flows = Flow::flows_from_config(file_path);

    // let flows = Flow::flows_from_graph(vec![vec![(0, 1)], vec![(1, 0)]]);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph, hosts, flows);

    // runs the topology
    topology.run(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Please provide the path to the toml configuration file: cargo run -- <path>");
    }

    let path = args[1].clone();
    let seed = get_seed(&path);

    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(seed)),
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

//! The main program for running a simulation using a specific configuration.

use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex, RwLock};

use due::sim::Simulator;
use log::{debug, info, warn};
use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};

use due::flows::flow::Flow;
use due::topos::topo::Topology;
use due::{get_seed, Shared};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::Semaphore;

async fn network_sim(
    config_path: &str,
    sim: Simulator<Shared>,
) {
    let file_path = config_path;

    let graph = UnGraph::<usize, ()>::from_edges([(0, 1)]);
    let hosts = vec![0, 1];
    info!("The network graph has been initialized: {:?}", graph);

    let flows = Flow::flows_from_config(file_path);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph, hosts, flows);

    // // runs the topology
    topology.run(sim);

    // // waits for the end of this simulation
    // sim.advance(sim.shared().duration + 100.).await;
}

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let path = "configs/simple.toml";
    let seed = get_seed(&path);

    // initialization
    let shared = Shared {
        queueing_delay: RandomVar::new(),
        duration: 1.5,
    };
    let sim = Simulator::new(shared, seed);

    sim.activate(network_sim(config_path, sim));
}

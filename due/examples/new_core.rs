//! The main program for running a simulation using a specific configuration.

use std::sync::Arc;

use due::sim::{RandomVar, Simulator};
use log::{debug, info};
use petgraph::graph::UnGraph;

use due::flows::flow::Flow;
use due::topos::topo::Topology;
use due::{get_seed, Shared};

async fn network_sim(config_path: &str, sim: Arc<Simulator<Shared>>) {
    let graph = UnGraph::<usize, ()>::from_edges([(0, 1)]);
    let hosts = vec![0, 1];
    info!("The network graph has been initialized: {:?}", graph);

    let flows = Flow::flows_from_config(config_path);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    // initializes the topology
    let topology = Topology::new(config_path, graph, hosts, flows);

    // // runs the topology
    topology.run(Arc::clone(&sim)).await;

    // waits for the end of this simulation
    sim.add_permit().await;
    sim.advance(sim.read_shared().await.duration + 100.).await;
    sim.delete_permit().await;
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

    // tokio::spawn(network_sim(path, Arc::clone(&sim)));
    // sim.activate(network_sim(path, Arc::clone(&sim))).await;
    let handle = tokio::spawn(network_sim(path, Arc::clone(&sim)));
    handle.await.expect("Network simulation failed");
}

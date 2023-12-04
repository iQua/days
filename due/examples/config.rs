//! This file is used for fattree simulation.

use std::sync::Arc;

use log::{debug, info};

use due::flows::flow::Flow;
use due::sim::{RandomVar, Simulator};
use due::topos::build::build_graph;
use due::topos::topo::Topology;
use due::{get_seed, Shared};

async fn network_sim(config_path: &str, sim: Arc<Simulator<Shared>>) {
    let file_path = config_path;

    let graph = build_graph(file_path);
    let hosts = vec![0, 1];
    info!("The network graph has been initialized: {:?}", graph);

    let flows = Flow::flows_from_config(file_path);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    let topology = Topology::new(file_path, graph, hosts, flows);

    // runs the topology
    topology.run(Arc::clone(&sim)).await;
}

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let path = "configs/simple.toml";
    let seed = get_seed(&path);

    let shared = Shared {
        queueing_delay: RandomVar::new(),
        duration: 10.0,
    };
    let sim = Simulator::new(shared, seed);
    sim.run(Arc::clone(&sim), network_sim(path, Arc::clone(&sim)))
        .await;

    // println!(
    //     "Statistics on queueing delay in this simulation: {:#.3}",
    //     outcome.queueing_delay
    // );
}

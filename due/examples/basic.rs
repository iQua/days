//! This example shows how to create a basic network where two packet sources
//! send packets to a wire that adds propagation delays according to a random
//! distribution, and then to a packet sink.

use std::sync::Arc;

use log::{debug, info};
use petgraph::graph::UnGraph;

use due::flows::flow::Flow;
use due::sim::{RandomVar, Simulator};
use due::topos::topo::Topology;
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: Arc<Simulator<Shared>>) {
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
    topology.run(Arc::clone(&sim)).await;
}

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let shared = Shared {
        queueing_delay: RandomVar::new(),
        duration: 10.,
    };

    let sim = Simulator::new(shared, SEED);
    sim.run(Arc::clone(&sim), network_sim(Arc::clone(&sim)))
        .await;

    // println!(
    //     "Statistics on queueing delay in this simulation: {:#.3}",
    //     outcome.queueing_delay
    // );
}

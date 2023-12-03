//! The main program for running a simulation using a specific configuration.

use std::sync::Arc;

use due::sim::{RandomVar, Simulator};
use log::{debug, info, warn};
use petgraph::graph::UnGraph;

use due::flows::flow::Flow;
use due::topos::topo::Topology;
use due::{get_seed, Shared};

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

    {
        let graph = UnGraph::<usize, ()>::from_edges([(0, 1)]);
        let hosts = vec![0, 1];
        info!("The network graph has been initialized: {:?}", graph);

        let flows = Flow::flows_from_config(path);
        debug!(
            "A total of {} network flows has been initialized.",
            flows.len()
        );

        // initializes the topology
        let topology = Topology::new(path, graph, hosts, flows);

        // // runs the topology
        topology.run(Arc::clone(&sim)).await;
    }

    warn!("Finish in main at time {:.3}", sim.now().await);
}

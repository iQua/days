//! This example shows a network simulation session involving a FatTree
//! topology.

use std::cell::RefCell;

use log::{debug, info};
use rand::{rngs::SmallRng, SeedableRng};

use due::endpoints::build::build_fattree;
use due::endpoints::flow::Flow;
use due::endpoints::topo::Topology;
use due::get_seed;

async fn network_sim(config_path: &str, sim: SimContext<'_, Shared>) {
    let file_path = config_path;

    let (fattree_graph, fattree_hosts) = build_fattree(file_path);
    info!(
        "The fattree graph has been initialized: {:?}",
        fattree_graph
    );

    let flows = Flow::flows_from_config(file_path);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    let topology = Topology::new(file_path, fattree_graph, fattree_hosts, flows);

    // runs the topology
    topology.run(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let path = "configs/fattree.toml";
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

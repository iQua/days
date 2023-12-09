//! This example shows a network simulation session involving a FatTree
//! topology.

use std::cell::RefCell;

use log::{debug, info};
use rand::{rngs::SmallRng, SeedableRng};

use due::endpoints::build::build_fattree;
use due::endpoints::flow::Flow;
use due::endpoints::topo::Topology;
use due::seed_from_config;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let path = "configs/fattree.toml";
    let _ = seed_from_config(&path);

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
    topology.run();
}

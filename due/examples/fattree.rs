//! This example shows a network simulation session involving a FatTree
//! topology.

use log::info;
use std::env;

use due::flows::flow::Flow;
use due::seed_from_config;
use due::topos::build::build_fattree;
use due::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let args: Vec<String> = env::args().collect();
    let file_path = if args.len() != 2 {
        "configs/fattree.toml"
    } else {
        &args[1]
    };

    let _ = seed_from_config(&file_path);

    let (fattree_graph, fattree_hosts) = build_fattree(file_path);
    info!(
        "The fattree graph has been initialized: {:?}",
        fattree_graph
    );

    let flows = Flow::flows_from_config(file_path);
    info!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    let topology = Topology::new(file_path, fattree_graph.clone(), fattree_hosts, flows);
    topology.run(fattree_graph);
}

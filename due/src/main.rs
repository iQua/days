//! The main program for running a simulation using a specific configuration.

use std::env;

use log::info;
// use petgraph::graph::UnGraph;

use due::flows::collective::Collective;
use due::flows::flow::Flow;
use due::seed_from_config;
use due::topos::build::build_graph;
use due::topos::topo::Topology;

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Please provide the path to the toml configuration file: cargo run -- <path>");
    }

    let path = args[1].clone();
    let _ = seed_from_config(&path);
    let file_path = path.as_str();

    // There are two ways of building a network graph:

    // 1. build_graph(file_path) -> builds a graph from a configuration file.
    //    Example:
    //    let (graph, hosts) = build_graph(file_path);

    // 2. building a graph directly using UnGraph::<usize, ()>::from_edges().
    //    Example:
    //    let graph = UnGraph::<usize, ()>::from_edges(&[(0, 1)]);
    //    let hosts = vec![0, 1];

    let (graph, hosts) = build_graph(file_path);
    info!("The network graph has been initialized.");

    // There are two ways of initializing the flows:

    // 1. initializes flows directly using flows_from_graph().
    //    Example:
    //    let flows = Flow::flows_from_graph(
    //        vec![vec![(0, 1)], vec![(1, 0)]],
    //    );

    // 2. initializes flows using a configuration file.
    //    Example:
    let flows = Flow::flows_from_config(file_path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    // There are two ways of initializing the collectives:

    // 1. initializes collectives directly using collectives_from_graph().
    //    Example:
    // let collectives = Collective::collectives_from_graph(
    //     vec![vec![(0, 1), (0, 2)]],
    //     vec![vec![0]],
    //     vec![vec![1, 2]],
    // );

    // 2. initializes flows using a configuration file.
    //    Example:
    let collectives = Collective::collectives_from_config(file_path);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph.clone(), hosts, flows, collectives);

    // runs the topology
    topology.run(graph).await;
}

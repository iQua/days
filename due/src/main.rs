//! The main program for running a simulation using a specific configuration.

use std::env;

use log::info;
use petgraph::graph::UnGraph;

use due::flows::flow::Flow;
// use due::topos::build::build_graph;
use due::seed_from_config;
use due::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Please provide the path to the toml configuration file: cargo run -- <path>");
    }

    let path = args[1].clone();
    let _ = seed_from_config(&path);
    let file_path = path.as_str();

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
    //    let (graph, hosts) = build_fattree(file_path);

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
    info!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph.clone(), hosts, flows);

    // runs the topology
    topology.run(graph);
}

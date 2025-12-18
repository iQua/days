//! The main program for running a simulation using a specific configuration.

use std::env;

use log::info;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

// use petgraph::graph::UnGraph;

use days::flows::collective::Collective;
use days::flows::flow::Flow;
use days::seed_from_config;
use days::topos::build::build_graph;
use days::topos::topo::Topology;
use days::utils::tracing::{ConcurrencyTrackerLayer, is_tracing_active};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        panic!("Please provide the path to the toml configuration file: cargo run -- <path>");
    }

    let path = args[1].clone();
    let _ = seed_from_config(&path);

    // There are two ways of building a network graph:

    // 1. build_graph(file_path) -> builds a graph from a configuration file.
    //    Example:
    //    let (graph, hosts) = build_graph(file_path);

    // 2. building a graph directly using UnGraph::<usize, ()>::from_edges().
    //    Example:
    //    let graph = UnGraph::<usize, ()>::from_edges(&[(0, 1)]);
    //    let hosts = vec![0, 1];

    let Ok((graph, hosts)) = build_graph(&path) else {
        panic!("Failed to build the network graph.");
    };

    info!("The network graph has been initialized.");

    // There are two ways of initializing the flows:

    // 1. initializes flows directly using flows_from_graph().
    //    Example:
    //    let flows = Flow::flows_from_graph(
    //        vec![vec![(0, 1)], vec![(1, 0)]],
    //    );

    // 2. initializes flows using a configuration file.
    //    Example:
    let flows = Flow::flows_from_config(&path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    // There are two ways of initializing the collectives:

    // 1. initializes collectives directly using collectives_from_graph().
    //    Example:
    // let collectives = Collective::collectives_from_graph(
    //     day::flows::collective::CollectiveType::Broadcast,
    //     vec![vec![(0, 1), (0, 2)]],
    //     None,
    //     vec![vec![0, 0]],
    //     vec![vec![1, 2]],
    // );

    // 2. initializes flows using a configuration file.
    //    Example:
    let collectives = Collective::collectives_from_config(&path, &hosts);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // builds an EnvFilter that reads the RUST_LOG environment variable, defaulting to `info` if
    // not set

    if is_tracing_active(&path) {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        tracing_subscriber::registry()
            .with(fmt::layer()) // console formatting
            .with(env_filter) // env-based filtering
            .with(ConcurrencyTrackerLayer) // concurrency tracking
            .init();

        info!("Concurrency tracing is active.");
    } else {
        let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
        env_logger::init_from_env(env);
    }

    // initializes the topology
    let topology = Topology::new(&path, graph.clone(), hosts, flows, collectives);

    // runs the topology
    topology.run(graph);
}

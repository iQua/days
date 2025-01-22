#![cfg(feature = "test")]

use std::env;

use log::info;

use daytone::flows::collective::Collective;
use daytone::flows::flow::Flow;
use daytone::seed_from_config;
use daytone::topos::build::build_graph;
use daytone::topos::topo::Topology;
use daytone::utils::logger::CsvLogger;

fn test_local_time() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Initialize the logger
    if let Err(e) = CsvLogger::get_instance().init("logs/local_time_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    let path = "tests/tcp_fattree.toml";
    let _ = seed_from_config(&path);

    let Ok((graph, hosts)) = build_graph(&path) else {
        panic!("Failed to build the network graph.");
    };
    info!("The network graph has been initialized.");

    let flows = Flow::flows_from_config(&path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    let collectives = Collective::collectives_from_config(&path, &hosts);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(&path, graph.clone(), hosts, flows, collectives);

    // runs the topology
    topology.run(graph);
}

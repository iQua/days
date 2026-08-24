#![cfg(feature = "test")]

use log::info;

use days_legacy::flows::collective::Collective;
use days_legacy::flows::flow::Flow;
use days_legacy::seed_from_config;
use days_legacy::topos::build::build_graph;
use days_legacy::topos::topo::Topology;

#[test]
fn test_local_time() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Info) // explicitly set log level
        .try_init();

    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/local_time.toml");
    let _ = seed_from_config(path);

    let Ok((graph, hosts)) = build_graph(path) else {
        panic!("Failed to build the network graph.");
    };
    info!("The network graph has been initialized.");

    let flows = Flow::flows_from_config_with_attachments(path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    let collectives = Collective::collectives_from_config(path, hosts.host_ids());
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(path, graph.clone(), hosts, flows, collectives);

    // runs the topology
    topology.run(graph).unwrap();
}

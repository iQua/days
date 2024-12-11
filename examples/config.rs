//! This example shows how a network simulation session can be established by
//! using a configuration file.

use log::info;

use daytone::flows::collective::Collective;
use daytone::flows::flow::Flow;
use daytone::topos::build::build_graph;
use daytone::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let config_path = "configs/simple.toml";

    let Ok((graph, hosts)) = build_graph(&path) else {
        panic!("Failed to build the network graph.");
    };

    info!("The network graph has been initialized.");

    let flows = Flow::flows_from_config(config_path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    let collectives = Collective::collectives_from_config(config_path, &hosts);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(config_path, graph.clone(), hosts, flows, collectives);

    // runs the topology
    topology.run(graph);
}

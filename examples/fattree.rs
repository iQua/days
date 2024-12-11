//! This example shows a network simulation session involving a FatTree
//! topology.

use log::info;

use daytone::flows::collective::Collective;
use daytone::flows::flow::Flow;
use daytone::topos::build::build_graph;
use daytone::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let config_path = "configs/fattree.toml";

    let Ok((graph, hosts)) = build_graph(&config_path) else {
        panic!("Failed to build the network graph.");
    };

    info!("The FatTree graph has been initialized.");

    let flows = Flow::flows_from_config(config_path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    let collectives = Collective::collectives_from_config(config_path, &hosts);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    let topology = Topology::new(config_path, graph.clone(), hosts, flows, collectives);
    topology.run(graph);
}

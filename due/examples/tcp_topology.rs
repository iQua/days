//! An example of a network simulation session using TCP established by using a
//! configuration file.

use log::info;

use due::flows::collective::Collective;
use due::flows::flow::Flow;
use due::topos::build::build_graph;
use due::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    //let file_path = "configs/tcp_simple.toml";
    let file_path = "configs/tcp_fattree.toml";

    let (graph, hosts) = build_graph(file_path);
    info!("The FatTree graph has been initialized.");

    let flows = Flow::flows_from_config(file_path, &hosts);
    info!("A total of {} flows has been initialized.", flows.len());

    let collectives = Collective::collectives_from_config(file_path);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph.clone(), hosts, flows, collectives);

    // runs the topology
    topology.run(graph);
}

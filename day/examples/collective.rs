//! An example of running collective communication primitives.

use log::info;

use day::flows::collective::Collective;
use day::seed_from_config;
use day::topos::build::build_graph;
use day::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let file_path = "configs/collective.toml";
    let _ = seed_from_config(&file_path);

    let (graph, hosts) = build_graph(file_path);
    info!("The network graph has been initialized.");

    let collectives = Collective::collectives_from_config(file_path, &hosts);

    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph.clone(), hosts, Vec::new(), collectives);

    // runs the topology
    topology.run(graph);
}

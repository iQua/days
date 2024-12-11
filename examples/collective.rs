//! An example of running collective communication primitives.

use log::info;

use daytone::flows::collective::Collective;
use daytone::seed_from_config;
use daytone::topos::build::build_graph;
use daytone::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    let config_path = "configs/collective.toml";
    let _ = seed_from_config(&config_path);

    let Ok((graph, hosts)) = build_graph(&config_path) else {
        panic!("Failed to build the network graph.");
    };

    info!("The network graph has been initialized.");

    let collectives = Collective::collectives_from_config(config_path, &hosts);

    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    // initializes the topology
    let topology = Topology::new(config_path, graph.clone(), hosts, Vec::new(), collectives);

    // runs the topology
    topology.run(graph);
}

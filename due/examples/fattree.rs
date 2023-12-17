//! This example shows a network simulation session involving a FatTree
//! topology.

use log::info;

use due::flows::collective::Collective;
use due::flows::flow::Flow;
use due::topos::build::build_fattree;
use due::topos::topo::Topology;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let file_path = "configs/fattree.toml";

    let (fattree_graph, fattree_hosts) = build_fattree(file_path);
    info!(
        "The fattree graph has been initialized: {:?}",
        fattree_graph
    );

    let flows = Flow::flows_from_config(file_path);
    info!("A total of {} flows has been initialized.", flows.len());

    let collectives = Collective::collectives_from_config(file_path);
    info!(
        "A total of {} collective communication operations has been initialized.",
        collectives.len()
    );

    let topology = Topology::new(
        file_path,
        fattree_graph.clone(),
        fattree_hosts,
        flows,
        collectives,
    );
    topology.run(fattree_graph);
}

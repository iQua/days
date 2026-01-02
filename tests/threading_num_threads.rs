#![cfg(feature = "test")]

use days::topos::build::build_graph;
use days::topos::topo::Topology;
use days::{flows::collective::Collective, flows::flow::Flow};

#[test]
fn test_num_threads_override() {
    let _ = env_logger::builder().is_test(true).try_init();

    let path = "tests/threading_num_threads.toml";
    let Ok((graph, hosts)) = build_graph(&path) else {
        panic!("Failed to build the network graph.");
    };

    let flows: Vec<Flow> = Vec::new();
    let collectives: Vec<Collective> = Vec::new();
    let topology = Topology::new(&path, graph.clone(), hosts, flows, collectives);
    assert_eq!(topology.num_threads(), 2);
}

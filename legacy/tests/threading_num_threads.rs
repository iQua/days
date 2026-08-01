#![cfg(feature = "test")]

use days_legacy::topos::build::build_graph;
use days_legacy::topos::topo::Topology;
use days_legacy::{flows::collective::Collective, flows::flow::Flow};

#[test]
fn test_num_threads_override() {
    let _ = env_logger::builder().is_test(true).try_init();

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/threading_num_threads.toml"
    );
    let Ok((graph, hosts)) = build_graph(&path) else {
        panic!("Failed to build the network graph.");
    };

    let flows: Vec<Flow> = Vec::new();
    let collectives: Vec<Collective> = Vec::new();
    let topology = Topology::new(&path, graph.clone(), hosts, flows, collectives);
    assert_eq!(topology.num_threads(), 2);
}

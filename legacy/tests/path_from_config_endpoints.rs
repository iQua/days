#![cfg(feature = "test")]

use std::io::Write;

use petgraph::graph::NodeIndex;
use tempfile::NamedTempFile;

use days_legacy::flows::flow::Flow;
use days_legacy::topos::build::build_graph;

#[test]
fn test_path_from_config_preserves_endpoint_hosts() {
    let toml_content = r#"
seed = 1
edges = [[0, 1], [1, 2]]
hosts = [0, 2]
log_path = "logs/path_from_config_endpoints"

[switch]
port_rate = 8000
capacity = 100
weights = [1]
discipline = "FIFO"
drop = "RED"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 2]]
routing = "PathFromConfig"
path = [0, 1, 2]

[flow.traffic]
initial_delay = 0.0
duration = 0.01
arr_dist = {type = "Uniform", low = 0.001, high = 0.001}
pkt_size_dist = {type = "Uniform", low = 1000, high = 1000}
"#;

    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
    write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");
    let config_path = temp_file.path().to_str().expect("Invalid temp file path.");

    let (graph, hosts) = build_graph(config_path).expect("Failed to build graph.");
    let mut flows = Flow::flows_from_config_with_attachments(config_path, &hosts);
    assert_eq!(flows.len(), 1);

    let flow = flows.first_mut().expect("Expected one flow.");
    flow.source_id = 7;
    flow.sink_id = 9;

    let path = flow.compute_path(&graph);

    assert_eq!(
        path,
        vec![
            NodeIndex::new(7),
            NodeIndex::new(0),
            NodeIndex::new(1),
            NodeIndex::new(2),
            NodeIndex::new(9)
        ]
    );
}

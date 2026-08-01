use std::collections::HashMap;
use std::fs;

use csv::Reader;
use serde::Deserialize;

#[derive(Deserialize)]
struct SourceReportRow {
    flow_id: usize,
    packet_sizes: usize,
}

#[test]
fn mixed_tcp_broadcast_and_ring_collectives_keep_broadcast_app_sources_alive() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_path = tmp.path().join("logs");
    fs::create_dir_all(&log_path).expect("create log dir");

    let config_path = tmp.path().join("collective_tcp.toml");
    fs::write(
        &config_path,
        format!(
            r#"
seed = 1000
edges = [[0, 2], [0, 3], [1, 2], [1, 3], [0, 4], [3, 4]]
hosts = [0, 1, 2, 3, 4]
report_interval = 2.0
duration = 20.0
log_path = "{log_path}"

[app_source]
req_channel_capacity = 128
chunk_size = 512
initial_delay = 1
run_interval = 50

[switch]
port_rate = 1000000
capacity = 100
weights = [1]
discipline = "FIFO"
drop = "RED"

[[collective]]
collective_type = "Broadcast"
flow_type = "TCP"
first_flow_id = 100
flow_count = 4
sources = [4, 4, 4, 4]
sinks = [2, 3, 0, 1]

[collective.traffic]
initial_delay = 0.0
size = 3072
arr_dist = {{ type = "Uniform", low = 3, high = 4 }}
pkt_size_dist = {{ type = "Uniform", low = 2000, high = 2500 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"

[[collective]]
collective_type = "RingAllReduce"
flow_type = "TCP"
first_flow_id = 200
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 512
arr_dist = {{ type = "Uniform", low = 3, high = 4 }}
pkt_size_dist = {{ type = "Uniform", low = 2000, high = 2500 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"
"#,
            log_path = log_path.display()
        ),
    )
    .expect("write config");

    days_legacy::run_simulation_from_config(config_path.to_str().expect("utf-8 config path"))
        .expect("simulation should succeed");

    let mut sent_bytes_by_flow: HashMap<usize, usize> = HashMap::new();
    let mut reader = Reader::from_path(log_path.join("sources.csv")).expect("open sources.csv");
    for row in reader.deserialize::<SourceReportRow>() {
        let report = row.expect("deserialize source report");
        *sent_bytes_by_flow.entry(report.flow_id).or_default() += report.packet_sizes;
    }

    for flow_id in 100..104 {
        assert!(
            sent_bytes_by_flow
                .get(&flow_id)
                .copied()
                .unwrap_or_default()
                >= 3072,
            "broadcast flow {flow_id} should emit its full payload"
        );
    }
}

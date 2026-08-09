use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::sync::{Mutex, MutexGuard};

use csv::Reader;
use days_legacy::flows::collective::Collective;
use serde::Deserialize;
use tempfile::{NamedTempFile, tempdir};

static FLOW_ID_TEST_LOCK: Mutex<()> = Mutex::new(());

fn isolate_flow_ids() -> MutexGuard<'static, ()> {
    let guard = FLOW_ID_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    days_legacy::update_next_flow_id(0);
    guard
}

fn write_config(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temp config");
    write!(file, "{contents}").expect("write temp config");
    file
}

#[derive(Deserialize)]
struct SourceReportRow {
    flow_id: usize,
    packet_sizes: usize,
}

#[test]
fn ring_collective_parses_explicit_paths_from_config() {
    let _flow_id_guard = isolate_flow_ids();
    let config = r#"
seed = 1

[[collective]]
collective_type = "RingAllReduce"
first_flow_id = 1000000
flow_type = "PacketDistribution"
flow_count = 4
paths = [[0, 4, 1], [1, 4, 2], [2, 4, 3], [3, 4, 0]]

[collective.traffic]
initial_delay = 0.0
size = 4096
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
"#;

    let file = write_config(config);
    let hosts = vec![0, 1, 2, 3, 4];
    let collectives = Collective::collectives_from_config(file.path().to_str().unwrap(), &hosts);

    assert_eq!(collectives.len(), 1);
    assert_eq!(collectives[0].first_flow_id, 1_000_000);
    assert_eq!(collectives[0].sources, vec![0, 1, 2, 3]);
    assert_eq!(collectives[0].sinks, vec![1, 2, 3, 0]);
    assert_eq!(
        collectives[0].paths,
        Some(vec![
            vec![0, 4, 1],
            vec![1, 4, 2],
            vec![2, 4, 3],
            vec![3, 4, 0]
        ])
    );
}

#[test]
fn mixed_tcp_broadcast_and_ring_collectives_emit_runtime_bytes() {
    let _flow_id_guard = isolate_flow_ids();
    let tmp = tempdir().expect("tempdir");
    let log_path = tmp.path().join("logs");
    fs::create_dir_all(&log_path).expect("create log dir");

    let config_path = tmp.path().join("collective_tcp_runtime.toml");
    fs::write(
        &config_path,
        format!(
            r#"
seed = 1000
edges = [[0, 2], [0, 3], [1, 2], [1, 3], [0, 4], [3, 4]]
hosts = [0, 1, 2, 3, 4]
report_interval = 1.0
duration = 2.0
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
first_flow_id = 2000000
flow_type = "TCP"
flow_count = 4
sources = [4, 4, 4, 4]
sinks = [2, 3, 0, 1]

[collective.traffic]
initial_delay = 0.0
size = 3072
arr_dist = {{ type = "Uniform", low = 0.1, high = 0.1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 512, high = 512 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"

[[collective]]
collective_type = "RingAllReduce"
first_flow_id = 3000000
flow_type = "TCP"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 512
arr_dist = {{ type = "Uniform", low = 0.1, high = 0.1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 512, high = 512 }}

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

    for flow_id in 2_000_000..2_000_004 {
        assert!(
            sent_bytes_by_flow
                .get(&flow_id)
                .copied()
                .unwrap_or_default()
                >= 3072,
            "broadcast flow {flow_id} should emit its payload"
        );
    }

    for flow_id in 3_000_000..3_000_024 {
        assert!(
            sent_bytes_by_flow
                .get(&flow_id)
                .copied()
                .unwrap_or_default()
                > 0,
            "ring flow {flow_id} should emit non-zero bytes even when chunk < MSS"
        );
    }
}

#[test]
#[should_panic(expected = "RingAllReduce requires byte size 2 to be at least the ring size 4")]
fn ring_collective_rejects_undersized_byte_payload_from_config() {
    let _flow_id_guard = isolate_flow_ids();
    let config = r#"
seed = 1

[[collective]]
collective_type = "RingAllReduce"
first_flow_id = 4000000
flow_type = "PacketDistribution"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 2
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
"#;

    let file = write_config(config);
    let hosts = vec![0, 1, 2, 3];
    let _ = Collective::collectives_from_config(file.path().to_str().unwrap(), &hosts);
}

#[test]
#[should_panic(expected = "RingAllReduce does not support duration-based traffic")]
fn ring_collective_rejects_duration_traffic_from_config() {
    let _flow_id_guard = isolate_flow_ids();
    let config = r#"
seed = 1

[[collective]]
collective_type = "RingAllReduce"
first_flow_id = 5000000
flow_type = "PacketDistribution"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
duration = 10.0
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
"#;

    let file = write_config(config);
    let hosts = vec![0, 1, 2, 3];
    let _ = Collective::collectives_from_config(file.path().to_str().unwrap(), &hosts);
}

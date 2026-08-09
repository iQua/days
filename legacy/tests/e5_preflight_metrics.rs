#![cfg(feature = "test")]

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use days::scenario::compile_config;
use days::topos::build::build_graph;
use days_legacy::flows::flow::Flow;
use days_legacy::topos::topo::installed_host_attachment_state;
use tempfile::TempDir;

fn fixture(directory: &TempDir, name: &str, attachment: Option<bool>) -> PathBuf {
    let attachment = attachment
        .map(|enabled| format!("model_host_attachment = {enabled}\n"))
        .unwrap_or_default();
    let logs = directory.path().join(format!("{name}-logs"));
    let path = directory.path().join(format!("{name}.toml"));
    fs::write(
        &path,
        format!(
            r#"seed = 51001
duration = 0.2
threading = "single"
log_path = "{}"
legacy_e5_metrics = true
{attachment}
[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 100_000_000_000
capacity = 200
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 1000

[routing]
policy = "FatTreeEcmp"

[[flow_set]]
flow_type = "TCP"
flow_count = 16
pairing = "SwitchOffsetHalf"
traffic = {{ initial_delay = 0.0, size = 3500, arr_dist = {{ type = "Uniform", low = 1.0, high = 1.0 }}, pkt_size_dist = {{ type = "DiscreteUniform", low = 1460, high = 1460 }}, tcp = {{ cc_algorithm = "Reno" }} }}
"#,
            logs.display()
        ),
    )
    .unwrap();
    path
}

fn log_dir(config: &Path) -> PathBuf {
    let value: toml::Value = toml::from_str(&fs::read_to_string(config).unwrap()).unwrap();
    PathBuf::from(value["log_path"].as_str().unwrap())
}

#[test]
fn e5_requires_the_attachment_key_and_installs_exact_propagation_stages() {
    let directory = TempDir::new().unwrap();
    let absent = fixture(&directory, "absent", None);
    let enabled = fixture(&directory, "enabled", Some(true));

    let (_, absent_hosts) = build_graph(absent.to_str().unwrap()).unwrap();
    let absent_flows =
        Flow::try_flows_from_config_with_attachments(absent.to_str().unwrap(), &absent_hosts)
            .unwrap();
    assert!(
        installed_host_attachment_state(absent.to_str().unwrap(), &absent_hosts, &absent_flows)
            .is_none(),
        "the historical no-key mode must not silently activate propagation stages"
    );

    let (_, hosts) = build_graph(enabled.to_str().unwrap()).unwrap();
    let flows =
        Flow::try_flows_from_config_with_attachments(enabled.to_str().unwrap(), &hosts).unwrap();
    let stages =
        installed_host_attachment_state(enabled.to_str().unwrap(), &hosts, &flows).unwrap();
    assert_eq!(stages.physical.propagation_ns, 1000);
    assert!(stages.hosts.values().all(|host| {
        host.injection.propagation_ns == 1000 && host.delivery.propagation_ns == 1000
    }));

    let image = compile_config(&enabled).unwrap();
    assert!(
        image.links.iter().all(|link| link.propagation_ns == 1000),
        "the exact image and installed legacy stages must activate the same delay"
    );
}

#[test]
fn q_high_e5_analogue_exports_exact_final_metrics_and_stops_timer_work() {
    let directory = TempDir::new().unwrap();
    let config = fixture(&directory, "metrics", Some(true));

    cargo_bin_cmd!("days")
        .env("RUST_LOG", "error")
        .arg(&config)
        .assert()
        .success();

    let mut reader = csv::Reader::from_path(log_dir(&config).join("tcp_metrics.csv")).unwrap();
    let rows = reader
        .deserialize::<days::utils::logger::TcpMetricsReport>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 16);
    assert!(rows.iter().all(|row| {
        row.completed
            && row.original_bytes == 3500
            && row.final_original_segment_bytes == 580
            && row.acked_bytes == 3500
            && row.original_packets == 3
            && row.retransmissions == 0
            && row.outstanding_bytes == 0
            && row.pending_timeouts == 0
            && row.timer_ticks == 0
            && row.timer_cancelled
            && row.completion_time_ns.is_some()
    }));
}

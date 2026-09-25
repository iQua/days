#![cfg(feature = "test")]

use std::collections::BTreeSet;
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
fn scalar_propagation_installs_exact_stages_with_or_without_the_attachment_key() {
    let directory = TempDir::new().unwrap();
    let absent = fixture(&directory, "absent", None);
    let enabled = fixture(&directory, "enabled", Some(true));

    let (_, absent_hosts) = build_graph(absent.to_str().unwrap()).unwrap();
    let absent_flows =
        Flow::try_flows_from_config_with_attachments(absent.to_str().unwrap(), &absent_hosts)
            .unwrap();
    let absent_stages =
        installed_host_attachment_state(absent.to_str().unwrap(), &absent_hosts, &absent_flows)
            .expect("declared scalar propagation must activate physical stages");
    assert_eq!(absent_stages.physical.propagation_ns, 1000);
    assert!(absent_stages.hosts.values().all(|host| {
        host.injection.propagation_ns == 1000 && host.delivery.propagation_ns == 1000
    }));

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

/// Explicit correctness gate for the frozen k=32 E5 fixture. It asserts final byte ledgers only;
/// it does not collect or assert any wall-clock quantity.
#[test]
#[ignore = "full k=32 E5 non-regression gate; run explicitly for legacy publication"]
fn frozen_e5_completes_every_flow_and_acks_every_demand_byte() {
    let directory = TempDir::new().unwrap();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../configs/benchmarks/p12/e5_wide_k32_q200.toml");
    let original = fs::read_to_string(&source).unwrap();
    let logs = directory.path().join("e5-logs");
    let body = original.replacen(
        "log_path = \"logs/p12/e5_wide_k32_q200\"",
        &format!(
            "log_path = \"{}\"\nreport_interval = 1.0\nmodel_host_attachment = true\nlegacy_e5_metrics = true",
            logs.display()
        ),
        1,
    );
    assert_ne!(body, original, "E5 overlay must replace the log path");
    let config = directory.path().join("e5.toml");
    fs::write(&config, body).unwrap();

    cargo_bin_cmd!("days")
        .env("RUST_LOG", "error")
        .arg(&config)
        .assert()
        .success();

    let rows = csv::Reader::from_path(logs.join("tcp_metrics.csv"))
        .unwrap()
        .deserialize::<days::utils::logger::TcpMetricsReport>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let flows = rows.iter().map(|row| row.flow_id).collect::<BTreeSet<_>>();
    let completed = rows.iter().filter(|row| row.completed).count();
    let demand = rows
        .iter()
        .map(|row| row.original_bytes as u64)
        .sum::<u64>();
    let acked = rows.iter().map(|row| row.acked_bytes as u64).sum::<u64>();
    assert_eq!(rows.len(), 8192);
    assert_eq!(flows.len(), 8192);
    assert_eq!(completed, 8192);
    assert_eq!(demand, 8_589_934_592);
    assert_eq!(acked, demand);
    assert_eq!(
        rows.iter()
            .map(|row| row.outstanding_bytes as u64)
            .sum::<u64>(),
        0
    );
    assert_eq!(
        rows.iter()
            .map(|row| row.pending_timeouts as u64)
            .sum::<u64>(),
        0
    );
    assert!(rows.iter().all(|row| row.timer_cancelled));
    println!(
        "E5_NON_REGRESSION rows={} unique_flows={} completed={completed} demand={demand} acked={acked} outstanding=0 pending_timeouts=0 timers_cancelled={}",
        rows.len(),
        flows.len(),
        rows.iter().filter(|row| row.timer_cancelled).count(),
    );
}

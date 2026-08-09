#![cfg(feature = "test")]

use std::fs;
use std::path::PathBuf;

use assert_cmd::cargo::cargo_bin_cmd;
use days::topos::build::{PairingPolicy, build_graph};
use days_legacy::flows::flow::Flow;
use tempfile::TempDir;

#[test]
fn k4_lossy_analogue_completes_exact_demand_after_real_drops_and_retransmissions() {
    let directory = TempDir::new().unwrap();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../configs/benchmarks/p12/e5_legacy_k4_loss.toml");
    let config = directory.path().join("e5-legacy-k4-loss.toml");
    let logs = directory.path().join("logs");
    let body = fs::read_to_string(source).unwrap().replace(
        "log_path = \"/private/tmp/p12-e5-k4-loss\"",
        &format!("log_path = \"{}\"", logs.display()),
    );
    fs::write(&config, body).unwrap();

    let (_, hosts) = build_graph(config.to_str().unwrap()).unwrap();
    let flows =
        Flow::try_flows_from_config_with_attachments(config.to_str().unwrap(), &hosts).unwrap();
    assert_eq!(
        flows
            .iter()
            .map(|flow| (flow.source_host, flow.sink_host))
            .collect::<Vec<_>>(),
        hosts
            .structural_flow_pairs(PairingPolicy::SwitchOffsetHalf, 16)
            .unwrap()
    );

    cargo_bin_cmd!("days")
        .env("RUST_LOG", "error")
        .arg(&config)
        .assert()
        .success();

    let metrics = csv::Reader::from_path(logs.join("tcp_metrics.csv"))
        .unwrap()
        .deserialize::<days::utils::logger::TcpMetricsReport>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(metrics.len(), 16);
    assert!(metrics.iter().all(|row| {
        row.completed
            && row.original_packets == 3
            && row.original_bytes == 3500
            && row.final_original_segment_bytes == 580
            && row.acked_bytes == 3500
            && row.outstanding_bytes == 0
            && row.pending_timeouts == 0
            && row.timer_cancelled
            && row.completion_time_ns.unwrap() < 3_000_000_000
    }));
    assert_eq!(
        metrics.iter().map(|row| row.retransmissions).sum::<usize>(),
        4
    );

    let mut switch_reader = csv::Reader::from_path(logs.join("switches.csv")).unwrap();
    let drop_column = switch_reader
        .headers()
        .unwrap()
        .iter()
        .position(|header| header == "dropped_packets")
        .unwrap();
    let drops = switch_reader
        .records()
        .map(|row| row.unwrap()[drop_column].parse::<usize>().unwrap())
        .sum::<usize>();
    assert_eq!(drops, 4);
}

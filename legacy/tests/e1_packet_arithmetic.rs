use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use tempfile::TempDir;

fn fixture(load: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../configs/benchmarks/p12/e1_open_k32_load_{load}.toml"
    ))
}

fn materialize(source: &Path, directory: &TempDir, load: &str) -> (PathBuf, PathBuf) {
    let original = fs::read_to_string(source).expect("E1 fixture");
    let logs = directory.path().join(format!("load-{load}-logs"));
    let mut replaced_log_path = false;
    let mut body = original
        .lines()
        .map(|line| {
            if line.starts_with("log_path = ") {
                replaced_log_path = true;
                format!("log_path = \"{}\"", logs.display())
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    body.push('\n');
    assert!(replaced_log_path, "E1 overlay must replace log_path");
    body = body.replacen(
        "duration = 0.000020\n",
        "duration = 0.000020\nreport_interval = 0.000020\n",
        1,
    );
    assert_ne!(
        body, original,
        "E1 reporting overlay must change the fixture"
    );

    let config = directory.path().join(format!("load-{load}.toml"));
    fs::write(&config, body).expect("E1 reporting overlay");
    (config, logs)
}

fn source_packet_counts(path: &Path) -> Vec<u64> {
    let mut reader = csv::Reader::from_path(path).expect("sources.csv");
    let headers = reader.headers().expect("source headers").clone();
    let sent_packets = headers
        .iter()
        .position(|header| header == "sent_packets")
        .expect("sent_packets column");
    reader
        .records()
        .map(|record| {
            record.expect("source row")[sent_packets]
                .parse::<u64>()
                .expect("integer sent packet count")
        })
        .collect()
}

/// Explicit correctness gate. This runs all four 20 us E1 simulations and reads the source-side
/// report at the horizon. It does not collect or assert any wall-clock quantity.
#[test]
#[ignore = "full k=32 E1 packet-arithmetic gate; run explicitly for legacy E1 publication"]
fn e1_source_packet_arithmetic_matches_the_frozen_family() {
    let directory = TempDir::new().expect("temporary E1 gate");
    for (load, per_flow, total) in [
        ("10", 17_u64, 139_264_u64),
        ("30", 49, 401_408),
        ("60", 98, 802_816),
        ("90", 146, 1_196_032),
    ] {
        let (config, logs) = materialize(&fixture(load), &directory, load);
        cargo_bin_cmd!("days")
            .env("RUST_LOG", "error")
            .arg(&config)
            .assert()
            .success();

        let counts = source_packet_counts(&logs.join("sources.csv"));
        assert_eq!(counts.len(), 8192, "E1 load {load} source rows");
        assert!(
            counts.iter().all(|&count| count == per_flow),
            "E1 load {load} must source {per_flow} packets per flow"
        );
        assert_eq!(counts.iter().sum::<u64>(), total, "E1 load {load} total");
        println!(
            "E1_PACKET_ARITHMETIC load={load} flows={} per_flow={per_flow} sourced={total}",
            counts.len()
        );
    }
}

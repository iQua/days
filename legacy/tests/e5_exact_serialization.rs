#![cfg(feature = "test")]

use std::fs;

use assert_cmd::Command;
use days_legacy::utils::exact_time::{behavior_delay_ns, scenario_seconds_ns, serialization_ns};
use predicates::prelude::*;
use tempfile::TempDir;

#[test]
fn positive_packets_always_serialize_to_a_future_integer_tick() {
    assert_eq!(serialization_ns(1, 100_000_000_000.0).unwrap(), 1);
    assert_eq!(serialization_ns(4, 100_000_000_000.0).unwrap(), 1);
    assert_eq!(serialization_ns(40, 100_000_000_000.0).unwrap(), 4);
    assert_eq!(serialization_ns(1_460, 100_000_000_000.0).unwrap(), 117);
}

#[test]
fn scenario_time_boundaries_are_exact_integer_nanoseconds() {
    assert_eq!(scenario_seconds_ns(0.0, "zero start").unwrap(), 0);
    assert_eq!(scenario_seconds_ns(0.000_000_001, "start").unwrap(), 1);
    assert_eq!(scenario_seconds_ns(0.1, "timer").unwrap(), 100_000_000);
    assert_eq!(scenario_seconds_ns(1.0, "arrival").unwrap(), 1_000_000_000);
    assert_eq!(scenario_seconds_ns(3.0, "horizon").unwrap(), 3_000_000_000);

    for invalid in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -1.0,
        0.5e-9,
        1.5e-9,
    ] {
        assert!(scenario_seconds_ns(invalid, "test boundary").is_err());
    }
}

#[test]
fn positive_behavior_delays_round_up_to_a_future_tick() {
    assert_eq!(behavior_delay_ns(0.32e-9, "sample").unwrap(), 1);
    assert_eq!(behavior_delay_ns(1.01e-9, "sample").unwrap(), 2);
    assert!(behavior_delay_ns(0.0, "sample").is_err());
}

#[test]
fn four_byte_tcp_residue_no_longer_panics_at_100_gbps() {
    let fixture = Fixture::new("fine-arrival", 1, 516);

    Command::cargo_bin("days")
        .unwrap()
        .env("RUST_LOG", "info")
        .arg(&fixture.config_path)
        .assert()
        .success()
        .stderr(predicate::str::contains("Simulation completed"))
        .stderr(predicate::str::contains("InvalidScheduledTime").not());
}

#[test]
fn runtime_scheduling_failure_is_a_nonzero_cli_exit_without_completion_claim() {
    let fixture = Fixture::new("zero-arrival", 0, 1_032);
    let content = fs::read_to_string(&fixture.config_path)
        .unwrap()
        .replace("size = 1032", "duration = 0.0000005");
    fs::write(&fixture.config_path, content).unwrap();

    Command::cargo_bin("days")
        .unwrap()
        .env("RUST_LOG", "info")
        .arg(&fixture.config_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Simulation failed"))
        .stderr(predicate::str::contains("must be finite and positive"))
        .stderr(predicate::str::contains("Simulation completed").not());
}

#[test]
fn varying_tcp_packet_size_is_a_clean_cli_refusal() {
    let fixture = Fixture::new("varying-mss", 1, 1_032);
    let content = fs::read_to_string(&fixture.config_path)
        .unwrap()
        .replace("low = 512, high = 512", "low = 512, high = 1460");
    fs::write(&fixture.config_path, content).unwrap();

    Command::cargo_bin("days")
        .unwrap()
        .env("RUST_LOG", "info")
        .arg(&fixture.config_path)
        .assert()
        .failure()
        .stderr(predicate::str::contains("positive fixed integral MSS"))
        .stderr(predicate::str::contains("Simulation completed").not());
}

struct Fixture {
    _temp_dir: TempDir,
    config_path: std::path::PathBuf,
}

impl Fixture {
    fn new(name: &str, arrival_ns: u64, size: usize) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join(format!("{name}.toml"));
        let log_path = temp_dir.path().join("logs");
        let arrival_seconds = arrival_ns as f64 / 1_000_000_000.0;
        let initial_delay = if arrival_ns == 0 { 0.000000001 } else { 0.0 };
        fs::write(
            &config_path,
            format!(
                r#"seed = 1000
edges = [[0, 1]]
hosts = [0, 1]
duration = 0.000001
threading = "single"
log_path = "{}"

[switch]
port_rate = 100_000_000_000
capacity = 100
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "TCP"
graph = [[0, 1]]
routing = "ShortestPath"

[flow.traffic]
initial_delay = {initial_delay}
size = {size}
arr_dist = {{ type = "Uniform", low = {arrival_seconds}, high = {arrival_seconds} }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 512, high = 512 }}

[flow.traffic.tcp]
cc_algorithm = "TCPReno"
"#,
                log_path.display()
            ),
        )
        .unwrap();

        Self {
            _temp_dir: temp_dir,
            config_path,
        }
    }
}

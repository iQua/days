use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use tempfile::TempDir;

const BASE: &str = r#"seed = 51001
duration = 0.0
threading = "single"

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
flow_type = "PacketDistribution"
flow_count = 16
pairing = "SwitchOffsetHalf"
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

fn fixture(directory: &TempDir, name: &str, csv_logging: Option<&str>) -> (PathBuf, PathBuf) {
    let config_path = directory.path().join(format!("{name}.toml"));
    let log_path = directory.path().join(format!("{name}-logs"));
    let csv_logging = csv_logging
        .map(|value| format!("csv_logging = {value}\n"))
        .unwrap_or_default();
    let body = BASE.replacen(
        "duration = 0.0\n",
        &format!(
            "duration = 0.0\nlog_path = \"{}\"\n{csv_logging}",
            log_path.display()
        ),
        1,
    );
    fs::write(&config_path, body).expect("write CSV logging fixture");
    (config_path, log_path)
}

fn assert_full_csv_set(log_path: &Path) {
    let mut names = fs::read_dir(log_path)
        .expect("CSV log directory")
        .map(|entry| {
            entry
                .expect("CSV log entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    names.sort();

    let mut expected = vec!["sinks.csv", "sources.csv", "switches.csv", "traces.json"];
    #[cfg(feature = "l2_pfc")]
    expected.push("pfc.csv");
    #[cfg(all(feature = "lean", feature = "l2_pfc"))]
    expected.push("pfc_events.csv");
    #[cfg(feature = "lean")]
    expected.extend([
        "aqm_events.csv",
        "cubic_events.csv",
        "drr_events.csv",
        "wfq_events.csv",
    ]);
    #[cfg(all(feature = "lean", feature = "dcqcn"))]
    expected.push("dcqcn_events.csv");
    expected.sort();

    assert_eq!(names, expected);
}

#[test]
fn csv_logging_defaults_on_and_explicit_modes_control_all_filesystem_output() {
    let directory = TempDir::new().expect("temporary CSV logging test");
    let (default_config, default_logs) = fixture(&directory, "default", None);
    let (enabled_config, enabled_logs) = fixture(&directory, "enabled", Some("true"));
    let (disabled_config, disabled_logs) = fixture(&directory, "disabled", Some("false"));

    for config in [&default_config, &enabled_config, &disabled_config] {
        cargo_bin_cmd!("days")
            .env("RUST_LOG", "error")
            .arg(config)
            .assert()
            .success();
    }

    assert_full_csv_set(&default_logs);
    assert_full_csv_set(&enabled_logs);
    assert!(
        !disabled_logs.exists(),
        "csv_logging = false must not create the configured log path"
    );
    println!(
        "T30_IO_CONTROL default_and_true_sets=exact_for_active_features required_files=sinks.csv,sources.csv,switches.csv,traces.json explicit_false_log_path=absent"
    );
}

#[test]
fn strict_validation_requires_boolean_csv_logging() {
    let directory = TempDir::new().expect("temporary strict-validation test");
    let (config, _) = fixture(&directory, "wrong-type", Some("\"false\""));
    let error = days_legacy::validate_config(config.to_str().expect("UTF-8 config path"))
        .expect_err("a string csv_logging value must be rejected");
    assert!(error.contains("csv_logging"), "{error}");
    assert!(error.contains("invalid type"), "{error}");
}

#[cfg(feature = "test")]
#[test]
#[ignore = "full E3/E5 mode-equivalence gate; run one case per fresh test process"]
fn csv_logging_mode_preserves_e3_and_e5_outcomes() {
    let case = std::env::var("DAYS_T30_CASE").expect("set DAYS_T30_CASE to e3-st, e3-mt, or e5");
    let csv_logging = std::env::var("DAYS_T30_CSV_LOGGING")
        .expect("set DAYS_T30_CSV_LOGGING to true or false")
        .parse::<bool>()
        .expect("DAYS_T30_CSV_LOGGING must be boolean");
    if case.starts_with("e3-") {
        for tripwire in [
            "DAYS_E3_ASSERT_NO_TCP_SOURCE",
            "DAYS_E3_ASSERT_NO_FAST_RETRANSMIT",
            "DAYS_E3_ASSERT_NO_CUMULATIVE_ACK_JUMP",
            "DAYS_E3_ASSERT_NO_UNSUPPORTED_CONFIG_INPUT",
        ] {
            assert_eq!(
                std::env::var(tripwire).as_deref(),
                Ok("1"),
                "E3 equivalence requires {tripwire}=1"
            );
        }
        println!(
            "E3_TRIPWIRES_ARMED case={case} csv_logging={csv_logging} tcp_source=1 fast_retransmit=1 cumulative_ack_jump=1 unsupported_config_input=1"
        );
    }

    let directory = TempDir::new().expect("temporary T30 equivalence directory");
    let logs = directory.path().join(format!("{case}-{csv_logging}-logs"));
    let config = directory.path().join(format!("{case}-{csv_logging}.toml"));
    let source = match case.as_str() {
        "e3-st" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../configs/benchmarks/p12/e3_legacy_rack_local_st.toml"),
        "e3-mt" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../configs/benchmarks/p12/e3_legacy_rack_local_mt.toml"),
        "e5" => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../configs/benchmarks/p12/e5_wide_k32_q200.toml"),
        _ => panic!("unsupported DAYS_T30_CASE={case}"),
    };
    let original = fs::read_to_string(&source).expect("read canonical fixture");
    let original_log_path = match case.as_str() {
        "e3-st" => "log_path = \"logs/p12/e3_legacy_rack_local_st\"",
        "e3-mt" => "log_path = \"logs/p12/e3_legacy_rack_local_mt\"",
        "e5" => "log_path = \"logs/p12/e5_wide_k32_q200\"",
        _ => unreachable!(),
    };
    let mut replacement = format!(
        "log_path = \"{}\"\ncsv_logging = {csv_logging}",
        logs.display()
    );
    if case == "e5" {
        replacement.push_str(
            "\nreport_interval = 1.0\nmodel_host_attachment = true\nlegacy_e5_metrics = true",
        );
    }
    let body = original.replacen(original_log_path, &replacement, 1);
    assert_ne!(body, original, "equivalence overlay must replace log_path");
    fs::write(&config, body).expect("write equivalence overlay");

    days_legacy::run_simulation_from_config(config.to_str().expect("UTF-8 config path"))
        .expect("T30 equivalence simulation");
    let snapshot = days_legacy::utils::logger::CsvLogger::get_instance().correctness_snapshot();

    match case.as_str() {
        "e3-st" | "e3-mt" => {
            assert_eq!(snapshot.source_rows, 33_792);
            assert_eq!(snapshot.sink_rows, 16_896);
            assert_eq!(snapshot.sent_packets, 41_932_800);
            assert_eq!(snapshot.sent_bytes, 41_932_800_000);
            assert_eq!(snapshot.received_packets, snapshot.sent_packets);
            assert_eq!(snapshot.received_bytes, snapshot.sent_bytes);
            assert_eq!(snapshot.tcp_metrics_rows, 0);
            println!(
                "T30_EQUIVALENCE case={case} csv_logging={csv_logging} flows={} source_rows={} sink_rows={} sent_packets={} sent_bytes={} received_packets={} received_bytes={} derived_drops=0",
                snapshot.sink_rows,
                snapshot.source_rows,
                snapshot.sink_rows,
                snapshot.sent_packets,
                snapshot.sent_bytes,
                snapshot.received_packets,
                snapshot.received_bytes,
            );
        }
        "e5" => {
            assert_eq!(snapshot.tcp_metrics_rows, 8_192);
            assert_eq!(snapshot.completed_tcp_flows, 8_192);
            assert_eq!(snapshot.tcp_demand_bytes, 8_589_934_592);
            assert_eq!(snapshot.tcp_acked_bytes, snapshot.tcp_demand_bytes);
            assert_eq!(snapshot.tcp_outstanding_bytes, 0);
            assert_eq!(snapshot.tcp_pending_timeouts, 0);
            assert_eq!(snapshot.tcp_cancelled_timers, 8_192);
            println!(
                "T30_EQUIVALENCE case=e5 csv_logging={csv_logging} rows={} completed={} demand={} acked={} outstanding=0 pending_timeouts=0 timers_cancelled={}",
                snapshot.tcp_metrics_rows,
                snapshot.completed_tcp_flows,
                snapshot.tcp_demand_bytes,
                snapshot.tcp_acked_bytes,
                snapshot.tcp_cancelled_timers,
            );
        }
        _ => unreachable!(),
    }

    if csv_logging {
        assert!(logs.join("sources.csv").is_file());
        assert!(logs.join("switches.csv").is_file());
        assert!(logs.join("sinks.csv").is_file());
        assert!(logs.join("traces.json").is_file());
        if case == "e5" {
            assert!(logs.join("tcp_metrics.csv").is_file());
        }
    } else {
        assert!(
            !logs.exists(),
            "disabled equivalence run must leave log_path absent"
        );
    }
}

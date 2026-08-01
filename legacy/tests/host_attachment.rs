use std::fs;
use std::path::Path;

use assert_cmd::cargo::cargo_bin_cmd;
use tempfile::TempDir;

fn write_config(
    directory: &TempDir,
    name: &str,
    log_directory: &Path,
    model_host_attachment: Option<bool>,
) -> std::path::PathBuf {
    let attachment = model_host_attachment
        .map(|enabled| format!("model_host_attachment = {enabled}\n"))
        .unwrap_or_default();
    let config = format!(
        r#"
seed = 1
duration = 0.000001
log_path = "{}"
{attachment}edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 8_000_000_000
capacity = 4
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 3

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 2
arr_dist = {{ type = "Uniform", low = 1.0, high = 1.0 }}
pkt_size_dist = {{ type = "Uniform", low = 2, high = 2 }}
"#,
        log_directory.display()
    );
    let path = directory.path().join(name);
    fs::write(&path, config).expect("temporary config should be writable");
    path
}

fn run(config: &Path) {
    let mut command = cargo_bin_cmd!("days");
    command.env("RUST_LOG", "error").arg(config);
    command.assert().success().stdout("").stderr("");
}

fn sink_delay(log_directory: &Path) -> f64 {
    let csv =
        fs::read_to_string(log_directory.join("sinks.csv")).expect("sink report should exist");
    let row = csv
        .lines()
        .nth(1)
        .expect("sink report should contain one row");
    row.split(',')
        .nth(7)
        .expect("sink report should contain one-way delay")
        .parse()
        .expect("one-way delay should be numeric")
}

fn sink_delays(log_directory: &Path) -> Vec<f64> {
    let csv =
        fs::read_to_string(log_directory.join("sinks.csv")).expect("sink report should exist");
    let mut delays = csv
        .lines()
        .skip(1)
        .map(|row| {
            row.split(',')
                .nth(7)
                .expect("sink report should contain one-way delay")
                .parse()
                .expect("one-way delay should be numeric")
        })
        .collect::<Vec<_>>();
    delays.sort_by(f64::total_cmp);
    delays
}

#[test]
fn key_off_is_byte_identical_to_an_absent_key() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let historical_logs = directory.path().join("historical");
    let explicit_off_logs = directory.path().join("explicit-off");
    let historical = write_config(&directory, "historical.toml", &historical_logs, None);
    let explicit_off = write_config(
        &directory,
        "explicit-off.toml",
        &explicit_off_logs,
        Some(false),
    );

    run(&historical);
    run(&explicit_off);

    for artifact in ["sources.csv", "switches.csv", "sinks.csv", "traces.json"] {
        assert_eq!(
            fs::read(historical_logs.join(artifact)).expect("historical artifact should exist"),
            fs::read(explicit_off_logs.join(artifact)).expect("key-off artifact should exist"),
            "{artifact} changed when the opt-in key was false"
        );
    }
}

#[test]
fn key_on_serializes_both_host_attachment_directions() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let logs = directory.path().join("enabled");
    let config = write_config(&directory, "enabled.toml", &logs, Some(true));

    run(&config);

    assert_eq!(
        sink_delay(&logs),
        15e-9,
        "endpoint and physical serialization-plus-propagation stages should match exact lowering"
    );
}

#[test]
fn key_on_flows_from_one_host_share_the_injection_fifo() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let logs = directory.path().join("shared");
    let config = directory.path().join("shared.toml");
    fs::write(
        &config,
        format!(
            r#"
seed = 1
duration = 0.000001
log_path = "{}"
model_host_attachment = true
edges = [[0, 1], [0, 2]]
hosts = [0, 1, 2]

[switch]
port_rate = 8_000_000_000
capacity = 4
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_id = 0
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 2
arr_dist = {{ type = "Uniform", low = 1.0, high = 1.0 }}
pkt_size_dist = {{ type = "Uniform", low = 2, high = 2 }}

[[flow]]
flow_id = 1
flow_type = "PacketDistribution"
graph = [[0, 2]]
[flow.traffic]
initial_delay = 0.0
size = 2
arr_dist = {{ type = "Uniform", low = 1.0, high = 1.0 }}
pkt_size_dist = {{ type = "Uniform", low = 2, high = 2 }}
"#,
            logs.display()
        ),
    )
    .expect("temporary config should be writable");

    run(&config);

    assert_eq!(
        sink_delays(&logs),
        vec![6e-9, 8e-9],
        "simultaneous flows from one host must serialize through one shared injection FIFO"
    );
}

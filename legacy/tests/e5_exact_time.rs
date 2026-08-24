#![cfg(feature = "test")]

use std::fs;
use std::time::Duration;

use assert_cmd::Command;
use days_legacy::flows::packet::Packet;
use days_legacy::flows::wire::Wire;
use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::{EventSinkReader, Output, SinkState, event_queue};
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use predicates::prelude::*;
use tempfile::TempDir;

struct OnePacketSource {
    output: Output<Packet>,
}

impl Model for OnePacketSource {
    type Env = ();

    async fn init(mut self, cx: &Context<Self>, _: &mut Self::Env) -> InitializedModel<Self> {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.output.send(Packet::new(1, 0, 0, now)).await;
        self.into()
    }
}

struct ClockSink {
    output: Output<u64>,
}

impl ClockSink {
    async fn packet_received(&mut self, _: Packet, cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH);
        self.output
            .send(u64::try_from(now.as_nanos()).unwrap())
            .await;
    }
}

impl Model for ClockSink {
    type Env = ();
}

#[test]
fn fixed_propagation_is_exact_even_at_a_large_epoch() {
    let mut source = OnePacketSource {
        output: Output::default(),
    };
    let mut wire = Wire::with_propagation_ns(0, 1_000);
    let mut sink = ClockSink {
        output: Output::default(),
    };
    let (writer, mut reader) = event_queue(SinkState::Enabled);

    let wire_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    source.output.connect(Wire::packet_received, &wire_mbox);
    wire.output.connect(ClockSink::packet_received, &sink_mbox);
    sink.output.connect_sink(writer);

    let start_ns = (1_u64 << 24) * 1_000_000_000;
    let start = MonotonicTime::EPOCH + Duration::from_nanos(start_ns);
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, Mailbox::new(), "Source")
        .add_model(wire, wire_mbox, "Wire")
        .add_model(sink, sink_mbox, "Sink")
        .init(start)
        .unwrap();

    sim.step_until(Duration::from_nanos(1_000)).unwrap();
    assert_eq!(reader.try_read(), Some(start_ns + 1_000));
}

#[test]
fn zero_start_and_one_second_arrival_land_on_exact_boundaries() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let config = write_packet_config(&directory, &logs, "1.1");

    Command::cargo_bin("days")
        .unwrap()
        .env("RUST_LOG", "error")
        .arg(config)
        .assert()
        .success();

    let csv = fs::read_to_string(logs.join("sources.csv")).unwrap();
    let fields = csv.lines().nth(1).unwrap().split(',').collect::<Vec<_>>();
    assert_eq!(fields[2].parse::<f64>().unwrap(), 0.0);
    assert_eq!(fields[3].parse::<f64>().unwrap(), 1.0);
    assert_eq!(fields[4].parse::<usize>().unwrap(), 2);
    assert_eq!(fields[5].parse::<usize>().unwrap(), 2);
}

#[test]
fn fractional_nanosecond_horizon_is_a_clean_cli_failure() {
    let directory = TempDir::new().unwrap();
    let logs = directory.path().join("logs");
    let config = write_packet_config(&directory, &logs, "0.0000000005");

    Command::cargo_bin("days")
        .unwrap()
        .env("RUST_LOG", "error")
        .arg(config)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Simulation failed"))
        .stderr(predicate::str::contains("sub-nanosecond"))
        .stderr(predicate::str::contains("Simulation completed").not());
}

fn write_packet_config(
    directory: &TempDir,
    logs: &std::path::Path,
    duration: &str,
) -> std::path::PathBuf {
    let path = directory.path().join("fixture.toml");
    fs::write(
        &path,
        format!(
            r#"seed = 1
duration = {duration}
threading = "single"
log_path = "{}"
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 8_000_000_000
capacity = 4
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 2
arr_dist = {{ type = "Uniform", low = 1.0, high = 1.0 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1, high = 1 }}
"#,
            logs.display()
        ),
    )
    .unwrap();
    path
}

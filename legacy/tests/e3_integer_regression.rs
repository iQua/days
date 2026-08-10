#![cfg(feature = "test")]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use tempfile::TempDir;

#[derive(Debug, PartialEq, Eq)]
struct IntegerTotals {
    flows: usize,
    source_rows: usize,
    sink_rows: usize,
    sent_packets: u64,
    sent_bytes: u64,
    received_packets: u64,
    received_bytes: u64,
    derived_drops: u64,
}

impl IntegerTotals {
    fn stable_record(&self) -> String {
        format!(
            "flows={},source_rows={},sink_rows={},sent_packets={},sent_bytes={},received_packets={},received_bytes={},derived_drops={}\n",
            self.flows,
            self.source_rows,
            self.sink_rows,
            self.sent_packets,
            self.sent_bytes,
            self.received_packets,
            self.received_bytes,
            self.derived_drops,
        )
    }
}

fn column(headers: &csv::StringRecord, name: &str) -> usize {
    headers
        .iter()
        .position(|header| header == name)
        .unwrap_or_else(|| panic!("missing {name} column"))
}

fn summarize(
    path: &Path,
    packet_column: &str,
    byte_column: &str,
) -> (usize, u64, u64, BTreeSet<u64>) {
    let mut reader = csv::Reader::from_path(path).unwrap();
    let headers = reader.headers().unwrap().clone();
    let flow_id = column(&headers, "flow_id");
    let packets = column(&headers, packet_column);
    let bytes = column(&headers, byte_column);
    let mut row_count = 0;
    let mut packet_total = 0_u64;
    let mut byte_total = 0_u64;
    let mut flows = BTreeSet::new();

    for record in reader.records() {
        let record = record.unwrap();
        row_count += 1;
        flows.insert(record[flow_id].parse().unwrap());
        packet_total += record[packets].parse::<u64>().unwrap();
        byte_total += record[bytes].parse::<u64>().unwrap();
    }

    (row_count, packet_total, byte_total, flows)
}

/// This is an explicit release gate rather than an ordinary suite member: E3 processes
/// 41,932,800 packets. The byte record below is the integer-only contract retained from annotated
/// tag `p12-legacy-pre-e5` (`3295ad08f345c4f57c0fa6c30d0749b6a80e0a95`). Timing and delay
/// fields are deliberately excluded because integer-nanosecond scheduling supersedes their
/// tag-era f64-drifted values; see `legacy/README.md` section 6.9.
#[test]
#[ignore = "full E3 release gate; run explicitly before publishing legacy results"]
fn e3_integer_totals_match_pre_e5_tag() {
    const TAG_INTEGER_RECORD: &str = "flows=16896,source_rows=33792,sink_rows=16896,sent_packets=41932800,sent_bytes=41932800000,received_packets=41932800,received_bytes=41932800000,derived_drops=0\n";

    let directory = TempDir::new().unwrap();
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../configs/benchmarks/p12/e3_legacy_rack_local_st.toml");
    let config = directory.path().join("e3-legacy-rack-local-st.toml");
    let logs = directory.path().join("logs");
    let original = fs::read_to_string(source).unwrap();
    let body = original.replace(
        "log_path = \"logs/p12/e3_legacy_rack_local_st\"",
        &format!("log_path = \"{}\"", logs.display()),
    );
    assert_ne!(body, original, "E3 log path replacement must take effect");
    fs::write(&config, body).unwrap();

    cargo_bin_cmd!("days")
        .env("RUST_LOG", "error")
        .env("DAYS_E3_ASSERT_NO_FAST_RETRANSMIT", "1")
        .arg(&config)
        .assert()
        .success();
    assert!(
        !logs.join("tcp_metrics.csv").exists(),
        "the default E3 surface must not gain E5 metrics"
    );

    let (source_rows, sent_packets, sent_bytes, source_flows) =
        summarize(&logs.join("sources.csv"), "sent_packets", "packet_sizes");
    let (sink_rows, received_packets, received_bytes, sink_flows) = summarize(
        &logs.join("sinks.csv"),
        "received_packets",
        "received_sizes",
    );
    assert_eq!(source_flows, sink_flows, "source and sink flow IDs moved");

    let totals = IntegerTotals {
        flows: sink_flows.len(),
        source_rows,
        sink_rows,
        sent_packets,
        sent_bytes,
        received_packets,
        received_bytes,
        derived_drops: sent_packets.checked_sub(received_packets).unwrap(),
    };
    assert_eq!(
        totals.stable_record().as_bytes(),
        TAG_INTEGER_RECORD.as_bytes()
    );
}

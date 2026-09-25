use std::fs;

use days::utils::logger::{CsvLogger, PacketSourceReport, Report, ReportTiming};

#[test]
fn writes_trace_manifest_even_without_lean_traces() {
    let tmp = tempfile::tempdir().expect("tempdir");

    let logger = CsvLogger::get_instance();
    logger
        .init(tmp.path().to_str().expect("utf-8 tempdir"))
        .expect("init logger");

    CsvLogger::try_log_report(
        Report::PacketSourceReport(PacketSourceReport::default()),
        ReportTiming::Final,
    );
    logger.flush_reports();

    let manifest_path = tmp.path().join("traces.json");
    let content = fs::read_to_string(&manifest_path).expect("read traces.json");
    let v: serde_json::Value = serde_json::from_str(&content).expect("parse traces.json");

    assert_eq!(v["version"].as_u64(), Some(1));
    assert_eq!(v["traces"].as_array().map(|a| a.len()), Some(0));
}

#![cfg(feature = "migration_ledger")]

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;

fn run_explicit_fixture(root: &Path, name: &str) -> PathBuf {
    let log_path = root.join(name);
    let template = fs::read_to_string("configs/migration/explicit.toml")
        .expect("read explicit migration fixture");
    let config = template.replace(
        "log_path = \"logs/migration/explicit\"",
        &format!("log_path = \"{}\"", log_path.display()),
    );
    let config_path = root.join(format!("{name}.toml"));
    fs::write(&config_path, config).expect("write temporary migration fixture");

    let mut command = cargo_bin_cmd!("days");
    command.arg(&config_path).assert().success();
    log_path
}

#[test]
fn supported_nexosim_st_model_exposes_complete_ledger_and_terminal_digest() {
    let temporary = tempfile::tempdir().expect("create temporary directory");
    let log_path = run_explicit_fixture(temporary.path(), "complete");

    let ledger =
        fs::read_to_string(log_path.join("migration_ledger.csv")).expect("read migration ledger");
    for transition in [
        "source_emit",
        "switch_forward",
        "egress_enqueue",
        "egress_dequeue",
        "egress_departure",
        "sink_receive",
    ] {
        assert!(
            ledger.contains(transition),
            "ledger is missing {transition}"
        );
    }

    let digest = fs::read_to_string(log_path.join("migration_terminal_digest.csv"))
        .expect("read migration terminal digest");
    for model_kind in ["source", "switch", "port", "sink"] {
        assert!(
            digest.lines().any(|line| line.starts_with(model_kind)),
            "terminal digest is missing {model_kind}"
        );
    }
}

#[test]
fn migration_ledger_preserves_default_aggregate_output() {
    let temporary = tempfile::tempdir().expect("create temporary directory");
    let log_path = run_explicit_fixture(temporary.path(), "aggregate");

    assert_eq!(
        fs::read(log_path.join("sources.csv")).expect("read migration-feature sources"),
        include_bytes!("../docs/days-executor/evidence/P01/explicit-sources-default.csv"),
    );
    assert!(
        fs::read(log_path.join("switches.csv"))
            .expect("read migration-feature switches")
            .is_empty(),
        "default-build switches.csv is empty for the frozen explicit fixture"
    );
    assert_eq!(
        fs::read(log_path.join("sinks.csv")).expect("read migration-feature sinks"),
        include_bytes!("../docs/days-executor/evidence/P01/explicit-sinks-default.csv"),
    );
}

#[test]
fn repeated_fixture_runs_have_identical_ledger_output() {
    let temporary = tempfile::tempdir().expect("create temporary directory");
    let first = run_explicit_fixture(temporary.path(), "first");
    let second = run_explicit_fixture(temporary.path(), "second");

    assert_eq!(
        fs::read(first.join("migration_ledger.csv")).expect("read first ledger"),
        fs::read(second.join("migration_ledger.csv")).expect("read second ledger"),
    );
    assert_eq!(
        fs::read(first.join("migration_terminal_digest.csv")).expect("read first digest"),
        fs::read(second.join("migration_terminal_digest.csv")).expect("read second digest"),
    );
}

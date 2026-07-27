#![cfg(feature = "migration_ledger")]

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const RETIREMENT_CORPUS_PATH: &str = "evidence/P01/retirement-corpus.toml";

#[derive(Deserialize)]
struct RetirementCorpus {
    fixtures: Vec<CorpusFixture>,
}

#[derive(Deserialize)]
struct CorpusFixture {
    path: String,
    comparison_boundary: String,
    workload: String,
    ledger_path: Option<String>,
    ledger_hash: Option<String>,
}

struct ExactLedgerFixture {
    config_path: String,
    configured_log_path: String,
    golden_path: String,
    golden: Vec<u8>,
}

fn canonical_sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn config_log_path(config_path: &str) -> String {
    let input = fs::read_to_string(config_path).expect("read exact-ledger fixture config");
    let document = input
        .parse::<toml::Table>()
        .expect("parse exact-ledger fixture config as TOML document");
    document
        .get("log_path")
        .and_then(toml::Value::as_str)
        .unwrap_or_else(|| panic!("{config_path} must declare a top-level log_path"))
        .to_owned()
}

fn days_gpu_root() -> Option<PathBuf> {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = std::env::var_os("DAYS_GPU_ROOT")
        .map(PathBuf::from)
        .or_else(|| repository.parent().map(|parent| parent.join("days-gpu")))?;
    root.is_dir().then_some(root)
}

fn exact_ledger_fixtures() -> Option<Vec<ExactLedgerFixture>> {
    let Some(days_gpu) = days_gpu_root() else {
        eprintln!("skipping exact-ledger golden check: days-gpu is unavailable");
        return None;
    };
    let input = fs::read_to_string(days_gpu.join(RETIREMENT_CORPUS_PATH))
        .expect("read retirement corpus manifest");
    let manifest =
        toml::from_str::<RetirementCorpus>(&input).expect("parse retirement corpus manifest");
    let exact = manifest
        .fixtures
        .into_iter()
        .filter(|fixture| fixture.comparison_boundary == "exact-ledger")
        .collect::<Vec<_>>();
    let mut declared = exact
        .iter()
        .map(|fixture| (fixture.workload.as_str(), fixture.path.as_str()))
        .collect::<Vec<_>>();
    declared.sort_unstable();
    let mut expected = vec![
        ("migration-explicit", "configs/migration/explicit.toml"),
        ("migration-torus", "configs/migration/torus.toml"),
        ("migration-fattree-k4", "configs/migration/fattree.toml"),
    ];
    expected.sort_unstable();
    assert_eq!(
        declared, expected,
        "{RETIREMENT_CORPUS_PATH} exact-ledger fixture set changed"
    );
    Some(
        exact
            .into_iter()
            .map(|fixture| {
                let ledger_path = fixture.ledger_path.unwrap_or_else(|| {
                    panic!("{} declares exact-ledger without ledger_path", fixture.path)
                });
                let ledger_hash = fixture.ledger_hash.unwrap_or_else(|| {
                    panic!("{} declares exact-ledger without ledger_hash", fixture.path)
                });
                let golden = fs::read(days_gpu.join(&ledger_path))
                    .unwrap_or_else(|error| panic!("read frozen ledger {ledger_path}: {error}"));
                assert!(
                    !golden.is_empty(),
                    "{} declares an empty frozen ledger {ledger_path}",
                    fixture.path
                );
                assert_eq!(
                    canonical_sha256(&golden),
                    ledger_hash,
                    "{} frozen ledger hash does not match {ledger_path}",
                    fixture.path
                );
                ExactLedgerFixture {
                    configured_log_path: config_log_path(&fixture.path),
                    config_path: fixture.path,
                    golden_path: ledger_path,
                    golden,
                }
            })
            .collect(),
    )
}

fn run_fixture(root: &Path, name: &str, config_path: &str, configured_log_path: &str) -> PathBuf {
    let log_path = root.join(name);
    let template = fs::read_to_string(config_path).expect("read migration fixture");
    let config = template.replace(
        &format!("log_path = \"{configured_log_path}\""),
        &format!("log_path = \"{}\"", log_path.display()),
    );
    assert_ne!(template, config, "{config_path} log_path was not replaced");
    let config_path = root.join(format!("{name}.toml"));
    fs::write(&config_path, config).expect("write temporary migration fixture");

    let mut command = cargo_bin_cmd!("days");
    command.arg(&config_path).assert().success();
    log_path
}

fn run_explicit_fixture(root: &Path, name: &str) -> PathBuf {
    run_fixture(
        root,
        name,
        "configs/migration/explicit.toml",
        "logs/migration/explicit",
    )
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
    let Some(days_gpu) = days_gpu_root() else {
        eprintln!("skipping aggregate golden check: days-gpu is unavailable");
        return;
    };
    let temporary = tempfile::tempdir().expect("create temporary directory");
    let log_path = run_explicit_fixture(temporary.path(), "aggregate");

    assert_eq!(
        fs::read(log_path.join("sources.csv")).expect("read migration-feature sources"),
        fs::read(days_gpu.join("evidence/P01/explicit-sources-default.csv"))
            .expect("read default sources golden"),
    );
    assert!(
        fs::read(log_path.join("switches.csv"))
            .expect("read migration-feature switches")
            .is_empty(),
        "default-build switches.csv is empty for the frozen explicit fixture"
    );
    assert_eq!(
        fs::read(log_path.join("sinks.csv")).expect("read migration-feature sinks"),
        fs::read(days_gpu.join("evidence/P01/explicit-sinks-default.csv"))
            .expect("read default sinks golden"),
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

#[test]
fn exact_ledger_fixtures_match_complete_frozen_goldens() {
    let Some(fixtures) = exact_ledger_fixtures() else {
        return;
    };
    let temporary = tempfile::tempdir().expect("create temporary directory");
    for (index, fixture) in fixtures.into_iter().enumerate() {
        let name = format!("exact-golden-{index}");
        let output = run_fixture(
            temporary.path(),
            &name,
            &fixture.config_path,
            &fixture.configured_log_path,
        );
        assert_eq!(
            fs::read(output.join("migration_ledger.csv")).expect("read complete ledger"),
            fixture.golden,
            "{} diverged from its complete frozen ledger {}",
            fixture.config_path,
            fixture.golden_path
        );
    }
}

#[test]
fn exact_ledger_fixtures_declare_valid_frozen_goldens() {
    if let Some(fixtures) = exact_ledger_fixtures() {
        drop(fixtures);
    }
}

use std::fs;

use tempfile::TempDir;
use xtask::schema::{
    SchemaError, parse_budget_manifest, parse_dependency_baseline, parse_evidence_manifest,
    parse_phase_metadata,
};

const HASH: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const PHASE: &str = r#"
schema_version = 1
phase = "P90"
name = "fixture"
title = "Fixture"
design_note = "docs/days-executor/phases/P90-fixture.md"
depends_on = []
external_depends_on = []
features = ["test"]
platforms = ["any"]
semantic_changes = []
api_changes = []
trust_boundary_change = false
proof_changing = false
out_of_scope = []
selectable_backends = []

[[tasks]]
id = "T0"
title = "Fixture task"
depends_on = []
evidence = ["docs/days-executor/evidence/P90/t0.toml"]

[tasks.red_test]
path = "xtask/tests/fixture.rs"
name = "fixture_red_test"

[[test_commands]]
platform = "any"
features = ["test"]
argv = ["cargo", "--version"]
deterministic = true

[[backends]]
name = "scalar"
complete = true
selectable = true
feature = "scalar"

[[budgets]]
path = "docs/days-executor/budgets/p90.toml"
content_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
frozen_at_commit = "0123456789abcdef0123456789abcdef01234567"
"#;

const EVIDENCE: &str = r#"
schema_version = 1
id = "P90-T0-evidence"
phase = "P90"
task = "T0"
kind = "golden"
description = "Fixture evidence"
command = ["cargo", "test", "--package", "xtask"]
tool_version = "cargo 1.96.0"
schema = "days-executor/evidence/v1"
tags = []
budget = "docs/days-executor/budgets/p90.toml"
budget_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[[artifacts]]
path = "docs/days-executor/evidence/P90/golden.toml"
content_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;

const ARCHIVE_EVIDENCE: &str = r#"
schema_version = 1
id = "P90-T0-archive"
phase = "P90"
task = "T0"
kind = "archive"
description = "Fixture archive evidence"
command = ["cargo", "bench"]
tool_version = "cargo 1.96.0"
schema = "days-executor/evidence/v1"
tags = []

[[artifacts]]
path = "evidence/P90/results.tar.zst"
content_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
days_gpu_commit = "0123456789abcdef0123456789abcdef01234567"
tool_version = "cargo 1.96.0"
command = ["cargo", "bench", "--bench", "executor"]
schema = "days-executor/benchmark/v1"
"#;

const MEASUREMENT_EVIDENCE: &str = r#"
schema_version = 1
id = "P90-T0-measurement"
phase = "P90"
task = "T0"
kind = "measurement"
description = "Fixture measurement evidence"
command = ["cargo", "bench"]
tool_version = "cargo 1.96.0"
schema = "days-executor/evidence/v1"
tags = []
budget = "docs/days-executor/budgets/p90.toml"
budget_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
run_commit = "89abcdef0123456789abcdef0123456789abcdef"

[[artifacts]]
path = "docs/days-executor/evidence/P90/measurement.toml"
content_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;

const BUDGET: &str = r#"
schema_version = 2
id = "p90-fixture-budget"
phase = "P90"
frozen_at = "2026-07-26"
description = "Fixture budget"

[platform]
name = "fixture-host"
cpu = "fixture-cpu"
os_build = "fixture-os-build"
toolchain = "rustc 1.96.0; cargo 1.96.0"

[method]
warmups = 1
repetitions = 3
statistic = "median"
confidence_rule = "accept the median of three repetitions"

[[corpus]]
path = "configs/migration/fixture.toml"
content_hash = "sha256:0fef1d82942d5aad23789ecc85955e882b15de59c2ede2cb1e1cf4b920e497c9"
comparison_boundary = "exact-ledger"

[[thresholds]]
name = "runtime"
metric = "seconds"
comparison = "<="
value = 1.0
unit = "s"

[waiver]
approving_role = "executor program owner"
policy = "A waiver must be a reviewed manifest change made before the cutover decision."
"#;

const BASELINE: &str = r#"
schema_version = 1
allowed_licenses = ["MIT", "Apache-2.0"]

[[direct_dependencies]]
package = "fixture"
name = "serde"
version = "1.0"
source = "registry:crates.io"
kind = "normal"
"#;

#[test]
fn phase_schema_round_trips() {
    let value = parse_phase_metadata(PHASE).expect("valid phase");
    let encoded = toml::to_string_pretty(&value).expect("serialize phase");
    assert_eq!(parse_phase_metadata(&encoded).expect("parse phase"), value);
    assert_eq!(value.budgets[0].content_hash, HASH);
    assert_eq!(
        value.budgets[0].frozen_at_commit,
        "0123456789abcdef0123456789abcdef01234567"
    );
}

#[test]
fn evidence_schema_round_trips() {
    let value = parse_evidence_manifest(EVIDENCE).expect("valid evidence");
    let encoded = toml::to_string_pretty(&value).expect("serialize evidence");
    assert_eq!(
        parse_evidence_manifest(&encoded).expect("parse evidence"),
        value
    );

    let archive = parse_evidence_manifest(ARCHIVE_EVIDENCE).expect("valid archive evidence");
    let encoded = toml::to_string_pretty(&archive).expect("serialize archive evidence");
    assert_eq!(
        parse_evidence_manifest(&encoded).expect("parse archive evidence"),
        archive
    );

    let measurement =
        parse_evidence_manifest(MEASUREMENT_EVIDENCE).expect("valid measurement evidence");
    let encoded = toml::to_string_pretty(&measurement).expect("serialize measurement evidence");
    assert_eq!(
        parse_evidence_manifest(&encoded).expect("parse measurement evidence"),
        measurement
    );
}

#[test]
fn budget_schema_round_trips() {
    let root = budget_repo();
    let value = parse_budget_manifest(BUDGET, root.path()).expect("valid budget");
    let encoded = toml::to_string_pretty(&value).expect("serialize budget");
    assert_eq!(
        parse_budget_manifest(&encoded, root.path()).expect("parse budget"),
        value
    );
}

#[test]
fn baseline_schema_round_trips() {
    let value = parse_dependency_baseline(BASELINE).expect("valid baseline");
    let encoded = toml::to_string_pretty(&value).expect("serialize baseline");
    assert_eq!(
        parse_dependency_baseline(&encoded).expect("parse baseline"),
        value
    );
}

#[test]
fn unknown_fields_are_rejected() {
    let root = budget_repo();
    assert_malformed(parse_phase_metadata(&format!("unknown = true\n{PHASE}")));
    assert_malformed(parse_evidence_manifest(&format!(
        "unknown = true\n{EVIDENCE}"
    )));
    assert_malformed(parse_budget_manifest(
        &format!("unknown = true\n{BUDGET}"),
        root.path(),
    ));
    assert_malformed(parse_dependency_baseline(&format!(
        "unknown = true\n{BASELINE}"
    )));
}

#[test]
fn missing_required_fields_are_rejected() {
    let root = budget_repo();
    assert_malformed(parse_phase_metadata(
        &PHASE.replace("title = \"Fixture\"\n", ""),
    ));
    assert_malformed(parse_evidence_manifest(
        &EVIDENCE.replace("description = \"Fixture evidence\"\n", ""),
    ));
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("description = \"Fixture budget\"\n", ""),
        root.path(),
    ));
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("cpu = \"fixture-cpu\"\n", ""),
        root.path(),
    ));
    assert_malformed(parse_dependency_baseline(
        &BASELINE.replace("allowed_licenses = [\"MIT\", \"Apache-2.0\"]\n", ""),
    ));
    assert_malformed(parse_phase_metadata(&PHASE.replace(
        "frozen_at_commit = \"0123456789abcdef0123456789abcdef01234567\"\n",
        "",
    )));
}

#[test]
fn unsupported_schema_versions_are_distinct() {
    for version in [0, 2] {
        assert_unsupported(parse_phase_metadata(&with_version(PHASE, version)), version);
        assert_unsupported(
            parse_evidence_manifest(&with_version(EVIDENCE, version)),
            version,
        );
        assert_unsupported(
            parse_dependency_baseline(&with_version(BASELINE, version)),
            version,
        );
    }
    let root = budget_repo();
    for version in [0, 1, 3] {
        assert_unsupported(
            parse_budget_manifest(&with_version(BUDGET, version), root.path()),
            version,
        );
    }
}

#[test]
fn repository_paths_cannot_escape_the_root() {
    assert_malformed(parse_phase_metadata(&PHASE.replace(
        "design_note = \"docs/days-executor/phases/P90-fixture.md\"",
        "design_note = \"../outside.md\"",
    )));
    assert_malformed(parse_evidence_manifest(&EVIDENCE.replace(
        "path = \"docs/days-executor/evidence/P90/golden.toml\"",
        "path = \"/outside/golden.toml\"",
    )));
}

#[test]
fn empty_phase_tasks_are_rejected() {
    let empty_tasks = format!(
        "{}tasks = []\n\n[[test_commands]]{}",
        PHASE.split("[[tasks]]").next().expect("phase prefix"),
        PHASE
            .split("[[test_commands]]")
            .nth(1)
            .expect("phase commands")
    );
    assert_malformed(parse_phase_metadata(&empty_tasks));
}

#[test]
fn task_without_evidence_is_rejected() {
    assert_malformed(parse_phase_metadata(&PHASE.replace(
        "evidence = [\"docs/days-executor/evidence/P90/t0.toml\"]",
        "evidence = []",
    )));
}

#[test]
fn task_without_red_test_is_left_for_the_red_test_audit() {
    let without_red_test = PHASE.replace(
        r#"
[tasks.red_test]
path = "xtask/tests/fixture.rs"
name = "fixture_red_test"
"#,
        "",
    );
    let phase = parse_phase_metadata(&without_red_test).expect("schema-valid phase");
    assert!(phase.tasks[0].red_test.is_none());
}

#[test]
fn evidence_without_artifacts_is_rejected_for_every_kind() {
    let golden_without_artifacts = format!(
        "{}artifacts = []\n",
        EVIDENCE
            .split("[[artifacts]]")
            .next()
            .expect("golden prefix")
    );
    assert_malformed(parse_evidence_manifest(&golden_without_artifacts));

    let archive_without_artifacts = format!(
        "{}artifacts = []\n",
        ARCHIVE_EVIDENCE
            .split("[[artifacts]]")
            .next()
            .expect("archive prefix")
    );
    assert_malformed(parse_evidence_manifest(&archive_without_artifacts));

    let measurement_without_artifacts = format!(
        "{}artifacts = []\n",
        MEASUREMENT_EVIDENCE
            .split("[[artifacts]]")
            .next()
            .expect("measurement prefix")
    );
    assert_malformed(parse_evidence_manifest(&measurement_without_artifacts));
}

#[test]
fn budget_without_thresholds_is_rejected() {
    let root = budget_repo();
    let without_thresholds = BUDGET.replace(
        r#"[[thresholds]]
name = "runtime"
metric = "seconds"
comparison = "<="
value = 1.0
unit = "s"

"#,
        "",
    );
    assert_malformed(parse_budget_manifest(&without_thresholds, root.path()));
}

#[test]
fn empty_platform_fields_are_rejected() {
    let root = budget_repo();
    for (valid, empty) in [
        ("name = \"fixture-host\"", "name = \"\""),
        ("cpu = \"fixture-cpu\"", "cpu = \"\""),
        ("os_build = \"fixture-os-build\"", "os_build = \"\""),
        (
            "toolchain = \"rustc 1.96.0; cargo 1.96.0\"",
            "toolchain = \"\"",
        ),
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, empty),
            root.path(),
        ));
    }
}

#[test]
fn zero_repetitions_are_rejected() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("repetitions = 3", "repetitions = 0"),
        root.path(),
    ));
}

#[test]
fn empty_corpus_is_rejected() {
    let root = budget_repo();
    let empty = BUDGET
        .replace(
            "description = \"Fixture budget\"\n",
            "description = \"Fixture budget\"\ncorpus = []\n",
        )
        .replace(
            r#"[[corpus]]
path = "configs/migration/fixture.toml"
content_hash = "sha256:0fef1d82942d5aad23789ecc85955e882b15de59c2ede2cb1e1cf4b920e497c9"
comparison_boundary = "exact-ledger"

"#,
            "",
        );
    assert_malformed(parse_budget_manifest(&empty, root.path()));
}

#[test]
fn malformed_corpus_hash_is_rejected() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "sha256:0fef1d82942d5aad23789ecc85955e882b15de59c2ede2cb1e1cf4b920e497c9",
            "sha256:not-a-hash",
        ),
        root.path(),
    ));
}

#[test]
fn missing_corpus_path_is_rejected() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "configs/migration/fixture.toml",
            "configs/migration/missing.toml",
        ),
        root.path(),
    ));
}

#[test]
fn mismatched_corpus_content_is_rejected() {
    let root = budget_repo();
    fs::write(
        root.path().join("configs/migration/fixture.toml"),
        "changed corpus\n",
    )
    .expect("mutate corpus");
    assert_malformed(parse_budget_manifest(BUDGET, root.path()));
}

#[test]
fn invalid_comparison_boundary_is_rejected() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "comparison_boundary = \"exact-ledger\"",
            "comparison_boundary = \"full\"",
        ),
        root.path(),
    ));
}

#[test]
fn empty_waiver_role_is_rejected() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "approving_role = \"executor program owner\"",
            "approving_role = \"\"",
        ),
        root.path(),
    ));
}

#[test]
fn weakened_waiver_policy_is_rejected() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "A waiver must be a reviewed manifest change made before the cutover decision.",
            "Waivers can be approved later.",
        ),
        root.path(),
    ));
}

#[test]
fn empty_method_descriptions_are_rejected() {
    let root = budget_repo();
    for (valid, empty) in [
        ("statistic = \"median\"", "statistic = \"\""),
        (
            "confidence_rule = \"accept the median of three repetitions\"",
            "confidence_rule = \"\"",
        ),
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, empty),
            root.path(),
        ));
    }
}

#[test]
fn git_commit_fields_require_full_lowercase_hashes() {
    for invalid in [
        "",
        "0123456789abcdef0123456789abcdef0123456",
        "0123456789abcdef0123456789abcdef012345678",
        "0123456789ABCDEF0123456789abcdef01234567",
        "g123456789abcdef0123456789abcdef01234567",
    ] {
        assert_malformed(parse_phase_metadata(&PHASE.replace(
            "frozen_at_commit = \"0123456789abcdef0123456789abcdef01234567\"",
            &format!("frozen_at_commit = \"{invalid}\""),
        )));
        assert_malformed(parse_evidence_manifest(&MEASUREMENT_EVIDENCE.replace(
            "run_commit = \"89abcdef0123456789abcdef0123456789abcdef\"",
            &format!("run_commit = \"{invalid}\""),
        )));
    }
}

#[test]
fn legacy_archive_url_field_is_rejected() {
    let with_url = ARCHIVE_EVIDENCE.replace(
        "path = \"evidence/P90/results.tar.zst\"",
        "path = \"evidence/P90/results.tar.zst\"\nurl = \"https://example.com/results.tar.zst\"",
    );
    assert_malformed(parse_evidence_manifest(&with_url));
}

fn with_version(document: &str, version: i64) -> String {
    let declared = document
        .lines()
        .find(|line| line.starts_with("schema_version = "))
        .expect("schema version");
    document.replacen(declared, &format!("schema_version = {version}"), 1)
}

fn assert_malformed<T>(result: Result<T, SchemaError>) {
    assert!(
        matches!(
            result,
            Err(SchemaError::Parse(_) | SchemaError::Validation { .. })
        ),
        "expected a malformed-schema error"
    );
}

fn assert_unsupported<T>(result: Result<T, SchemaError>, expected: i64) {
    assert!(
        matches!(
            result,
            Err(SchemaError::UnsupportedVersion { found, .. }) if found == expected
        ),
        "expected unsupported schema_version {expected}"
    );
}

fn budget_repo() -> TempDir {
    let root = tempfile::tempdir().expect("create budget repository");
    fs::create_dir_all(root.path().join("configs/migration")).expect("create corpus directory");
    fs::write(
        root.path().join("configs/migration/fixture.toml"),
        "fixture corpus\n",
    )
    .expect("write corpus");
    root
}

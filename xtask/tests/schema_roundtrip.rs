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
id = "p90-fixture-budget"
owner_phase = "P90"
required_consumers = ["P23"]
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
schema_version = 4
id = "p90-fixture-budget"
phase = "P90"
frozen_at = "2026-07-26"
description = "Fixture budget"

[platform]
name = "fixture-host"
cpu = "fixture-cpu"
os_build = "fixture-os-build"
toolchain = "rustc 1.96.0; cargo 1.96.0"
expected_num_cpus = 8
mt_thread_count_source = "std::thread::available_parallelism"

[method]
warmups = 1
repetitions = 3
build_profile = "release"
cargo_flags = ["--release", "--locked"]
sim_execution_boundary = "std::time::Instant around sim.step_until only"
end_to_end_boundary = "process invocation through process exit"
minimum_sample_wall_time_seconds = 1.0
effective_simulation_duration_seconds = 0.002
effective_simulation_duration_source = "top-level duration or topology duration"
sample_simulated_end_time_rule = "must equal the effective simulation duration"
sample_effective_thread_count_rule = "ST uses one; MT uses expected_num_cpus"
run_order = "corpus order, then ST followed by MT"
pairing_order = "pair repetition i within each workload and mode"
resampling_algorithm = "paired bootstrap with 10000 resamples"
resampling_prng = "ChaCha8Rng"
resampling_seed = 1776
st_mode = "nexosim-st"
mt_mode = "nexosim-mt"
best_exact_mode_rule = "lowest median sim_execution among exact Nexosim CPU modes"

[[method.resolved_defaults]]
name = "mailbox-capacity"
value = "16 entries"
source = "src/topos/topo.rs"

[admission]
evaluated_at = "P23"
statistic = "geometric mean of paired throughput ratios"
confidence_rule = "two-sided 95 percent paired-bootstrap interval"

[[admission.thresholds]]
name = "runtime"
metric = "seconds"
metric_kind = "wall-time"
applies_to = "fixture"
timing_boundary = "sim_execution"
comparison = "<="
value = 1.0
unit = "s"

[[corpus]]
path = "configs/migration/fixture.toml"
content_hash = "sha256:0fef1d82942d5aad23789ecc85955e882b15de59c2ede2cb1e1cf4b920e497c9"
comparison_boundary = "exact-ledger"
workload = "fixture"
mode = "nexosim-st"
role = "correctness"

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
    assert_eq!(value.budgets[0].id, "p90-fixture-budget");
    assert_eq!(value.budgets[0].owner_phase, "P90");
    assert_eq!(value.budgets[0].required_consumers, ["P23"]);
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
    for required in [
        "id = \"p90-fixture-budget\"\n",
        "owner_phase = \"P90\"\n",
        "required_consumers = [\"P23\"]\n",
    ] {
        assert_malformed(parse_phase_metadata(&PHASE.replace(required, "")));
    }
}

#[test]
fn budget_reference_identity_fields_are_validated() {
    assert_malformed(parse_phase_metadata(
        &PHASE.replace("id = \"p90-fixture-budget\"", "id = \"\""),
    ));
    assert_malformed(parse_phase_metadata(
        &PHASE.replace("owner_phase = \"P90\"", "owner_phase = \"p90\""),
    ));
    assert_malformed(parse_phase_metadata(&PHASE.replace(
        "required_consumers = [\"P23\"]",
        "required_consumers = [\"phase-23\"]",
    )));
    let budget = r#"[[budgets]]
id = "p90-fixture-budget"
owner_phase = "P90"
required_consumers = ["P23"]
path = "docs/days-executor/budgets/p90.toml"
content_hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
frozen_at_commit = "0123456789abcdef0123456789abcdef01234567"
"#;
    assert_malformed(parse_phase_metadata(
        &PHASE.replace(budget, &format!("{budget}\n{budget}")),
    ));
}

#[test]
fn unsupported_schema_versions_are_distinct() {
    for version in [0, 2, 3] {
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
    for version in [0, 1, 2, 3, 5] {
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
    let threshold = r#"[[admission.thresholds]]
name = "runtime"
metric = "seconds"
metric_kind = "wall-time"
applies_to = "fixture"
timing_boundary = "sim_execution"
comparison = "<="
value = 1.0
unit = "s"

"#;
    let without_thresholds = BUDGET.replace(threshold, "");
    let empty_thresholds = BUDGET.replace(threshold, "thresholds = []\n\n");
    assert_malformed(parse_budget_manifest(&without_thresholds, root.path()));
    assert_malformed(parse_budget_manifest(&empty_thresholds, root.path()));
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
        (
            "mt_thread_count_source = \"std::thread::available_parallelism\"",
            "mt_thread_count_source = \"\"",
        ),
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, empty),
            root.path(),
        ));
    }
}

#[test]
fn platform_cpu_count_is_required_and_positive() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("expected_num_cpus = 8\n", ""),
        root.path(),
    ));
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("expected_num_cpus = 8", "expected_num_cpus = 0"),
        root.path(),
    ));
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
fn required_method_fields_cannot_be_removed() {
    let root = budget_repo();
    for required in [
        "build_profile = \"release\"\n",
        "cargo_flags = [\"--release\", \"--locked\"]\n",
        "sim_execution_boundary = \"std::time::Instant around sim.step_until only\"\n",
        "end_to_end_boundary = \"process invocation through process exit\"\n",
        "minimum_sample_wall_time_seconds = 1.0\n",
        "effective_simulation_duration_seconds = 0.002\n",
        "effective_simulation_duration_source = \"top-level duration or topology duration\"\n",
        "sample_simulated_end_time_rule = \"must equal the effective simulation duration\"\n",
        "sample_effective_thread_count_rule = \"ST uses one; MT uses expected_num_cpus\"\n",
        "run_order = \"corpus order, then ST followed by MT\"\n",
        "pairing_order = \"pair repetition i within each workload and mode\"\n",
        "resampling_algorithm = \"paired bootstrap with 10000 resamples\"\n",
        "resampling_prng = \"ChaCha8Rng\"\n",
        "resampling_seed = 1776\n",
        "st_mode = \"nexosim-st\"\n",
        "mt_mode = \"nexosim-mt\"\n",
        "best_exact_mode_rule = \"lowest median sim_execution among exact Nexosim CPU modes\"\n",
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(required, ""),
            root.path(),
        ));
    }
}

#[test]
fn method_strings_and_cargo_flags_cannot_be_empty() {
    let root = budget_repo();
    for valid in [
        "build_profile = \"release\"",
        "sim_execution_boundary = \"std::time::Instant around sim.step_until only\"",
        "end_to_end_boundary = \"process invocation through process exit\"",
        "effective_simulation_duration_source = \"top-level duration or topology duration\"",
        "sample_simulated_end_time_rule = \"must equal the effective simulation duration\"",
        "sample_effective_thread_count_rule = \"ST uses one; MT uses expected_num_cpus\"",
        "run_order = \"corpus order, then ST followed by MT\"",
        "pairing_order = \"pair repetition i within each workload and mode\"",
        "resampling_algorithm = \"paired bootstrap with 10000 resamples\"",
        "resampling_prng = \"ChaCha8Rng\"",
        "st_mode = \"nexosim-st\"",
        "mt_mode = \"nexosim-mt\"",
        "best_exact_mode_rule = \"lowest median sim_execution among exact Nexosim CPU modes\"",
    ] {
        let field = valid.split(" = ").next().expect("field name");
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, &format!("{field} = \"\"")),
            root.path(),
        ));
    }
    for invalid in [
        "cargo_flags = []",
        "cargo_flags = [\"\"]",
        "cargo_flags = [\"--release\", \"\"]",
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace("cargo_flags = [\"--release\", \"--locked\"]", invalid),
            root.path(),
        ));
    }
}

#[test]
fn resolved_defaults_are_required_and_validated() {
    let root = budget_repo();
    let table = r#"[[method.resolved_defaults]]
name = "mailbox-capacity"
value = "16 entries"
source = "src/topos/topo.rs"

"#;
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(table, ""),
        root.path(),
    ));
    for (valid, empty) in [
        ("name = \"mailbox-capacity\"", "name = \"\""),
        ("value = \"16 entries\"", "value = \"\""),
        ("source = \"src/topos/topo.rs\"", "source = \"\""),
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, empty),
            root.path(),
        ));
    }

    let duplicate = format!("{BUDGET}\n{table}");
    assert_malformed(parse_budget_manifest(&duplicate, root.path()));
}

#[test]
fn declared_time_values_must_be_positive_and_finite() {
    let root = budget_repo();
    for field in [
        "minimum_sample_wall_time_seconds",
        "effective_simulation_duration_seconds",
    ] {
        let valid = if field == "minimum_sample_wall_time_seconds" {
            "1.0"
        } else {
            "0.002"
        };
        for invalid in ["0.0", "-1.0", "nan", "inf"] {
            assert_malformed(parse_budget_manifest(
                &BUDGET.replace(
                    &format!("{field} = {valid}"),
                    &format!("{field} = {invalid}"),
                ),
                root.path(),
            ));
        }
    }
}

#[test]
fn admission_fields_are_required_and_validated() {
    let root = budget_repo();
    for required in [
        "evaluated_at = \"P23\"\n",
        "statistic = \"geometric mean of paired throughput ratios\"\n",
        "confidence_rule = \"two-sided 95 percent paired-bootstrap interval\"\n",
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(required, ""),
            root.path(),
        ));
    }
    for (valid, invalid) in [
        ("evaluated_at = \"P23\"", "evaluated_at = \"P01\""),
        (
            "statistic = \"geometric mean of paired throughput ratios\"",
            "statistic = \"\"",
        ),
        (
            "confidence_rule = \"two-sided 95 percent paired-bootstrap interval\"",
            "confidence_rule = \"\"",
        ),
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, invalid),
            root.path(),
        ));
    }
}

#[test]
fn threshold_metric_kind_is_required_and_typed() {
    let root = budget_repo();
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("metric_kind = \"wall-time\"\n", ""),
        root.path(),
    ));
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "metric_kind = \"wall-time\"",
            "metric_kind = \"elapsed-ish\"",
        ),
        root.path(),
    ));
}

#[test]
fn timing_metrics_require_a_valid_timing_boundary() {
    let root = budget_repo();
    for metric_kind in ["wall-time", "throughput"] {
        let input = BUDGET
            .replace(
                "metric_kind = \"wall-time\"",
                &format!("metric_kind = \"{metric_kind}\""),
            )
            .replace("timing_boundary = \"sim_execution\"\n", "");
        assert_malformed(parse_budget_manifest(&input, root.path()));
    }
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace(
            "timing_boundary = \"sim_execution\"",
            "timing_boundary = \"statistics_and_flush\"",
        ),
        root.path(),
    ));
}

#[test]
fn non_timing_metrics_forbid_a_timing_boundary() {
    let root = budget_repo();
    for metric_kind in ["bytes", "count"] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(
                "metric_kind = \"wall-time\"",
                &format!("metric_kind = \"{metric_kind}\""),
            ),
            root.path(),
        ));
    }
}

#[test]
fn non_timing_metrics_are_valid_without_a_timing_boundary() {
    let root = budget_repo();
    for metric_kind in ["bytes", "count"] {
        let input = BUDGET
            .replace(
                "metric_kind = \"wall-time\"",
                &format!("metric_kind = \"{metric_kind}\""),
            )
            .replace("timing_boundary = \"sim_execution\"\n", "");
        parse_budget_manifest(&input, root.path()).expect("valid non-timing threshold");
    }
}

#[test]
fn threshold_scope_must_be_corpus_or_a_declared_workload() {
    let root = budget_repo();
    parse_budget_manifest(
        &BUDGET.replace("applies_to = \"fixture\"", "applies_to = \"corpus\""),
        root.path(),
    )
    .expect("valid corpus-level threshold");

    for invalid in ["", "missing-workload"] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(
                "applies_to = \"fixture\"",
                &format!("applies_to = \"{invalid}\""),
            ),
            root.path(),
        ));
    }
    assert_malformed(parse_budget_manifest(
        &BUDGET.replace("applies_to = \"fixture\"\n", ""),
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
workload = "fixture"
mode = "nexosim-st"
role = "correctness"

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
fn corpus_identity_and_role_are_required_and_validated_independently() {
    let root = budget_repo();
    for required in [
        "workload = \"fixture\"\n",
        "mode = \"nexosim-st\"\n",
        "role = \"correctness\"\n",
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(required, ""),
            root.path(),
        ));
    }
    for (valid, invalid) in [
        ("workload = \"fixture\"", "workload = \"\""),
        ("mode = \"nexosim-st\"", "mode = \"\""),
        ("role = \"correctness\"", "role = \"ledger-equality\""),
    ] {
        assert_malformed(parse_budget_manifest(
            &BUDGET.replace(valid, invalid),
            root.path(),
        ));
    }
    parse_budget_manifest(
        &BUDGET.replace("role = \"correctness\"", "role = \"performance\""),
        root.path(),
    )
    .expect("independently valid performance role");
}

#[test]
fn nested_unknown_fields_are_rejected() {
    let root = budget_repo();
    for budget in [
        BUDGET.replace(
            "expected_num_cpus = 8",
            "expected_num_cpus = 8\nunknown_platform = true",
        ),
        BUDGET.replace(
            "build_profile = \"release\"",
            "build_profile = \"release\"\nunknown_method = true",
        ),
        BUDGET.replace(
            "evaluated_at = \"P23\"",
            "evaluated_at = \"P23\"\nunknown_admission = true",
        ),
        BUDGET.replace(
            "workload = \"fixture\"",
            "workload = \"fixture\"\nunknown_corpus = true",
        ),
    ] {
        assert_malformed(parse_budget_manifest(&budget, root.path()));
    }
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
fn empty_admission_descriptions_are_rejected() {
    let root = budget_repo();
    for (valid, empty) in [
        (
            "statistic = \"geometric mean of paired throughput ratios\"",
            "statistic = \"\"",
        ),
        (
            "confidence_rule = \"two-sided 95 percent paired-bootstrap interval\"",
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

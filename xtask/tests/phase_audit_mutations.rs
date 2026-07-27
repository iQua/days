use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;
use xtask::audit::{AuditReport, phase_audit, phase_ids};
use xtask::dependency_baseline;
use xtask::hash::sha256_bytes;
use xtask::reproduce::reproduce;

struct Fixture {
    directory: TempDir,
    root: PathBuf,
    host: String,
    pre_budget_commit: String,
    frozen_commit: String,
    measurement_commit: String,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("create fixture directory");
        let root = directory.path().join("days");
        fs::create_dir(&root).expect("create fixture repository");
        run(&root, &["init", "-q"]);
        run(&root, &["config", "user.email", "days@example.invalid"]);
        run(&root, &["config", "user.name", "Days Audit"]);

        write(&root, ".gitignore", "target/\nCargo.lock\n");
        write(
            &root,
            "Cargo.toml",
            r#"[workspace]
members = ["xtask"]
resolver = "2"

[package]
name = "fixture-root"
version = "0.1.0"
edition = "2021"
license = "AGPL-3.0-only"

[lib]
path = "root.rs"
"#,
        );
        write(&root, "root.rs", "pub fn fixture_root() {}\n");
        write(
            &root,
            "xtask/Cargo.toml",
            r#"[package]
name = "fixture-xtask"
version = "0.1.0"
edition = "2021"
license = "AGPL-3.0-only"
"#,
        );
        write(&root, "xtask/src/lib.rs", "pub fn fixture_xtask() {}\n");
        let baseline = dependency_baseline::render(&root).expect("render fixture baseline");
        dependency_baseline::write(&root, &baseline).expect("write fixture baseline");
        run(&root, &["add", "-A"]);
        run(&root, &["commit", "-q", "-m", "Initialize fixture"]);
        let pre_budget_commit = run_output(&root, &["rev-parse", "HEAD"]);

        write(
            &root,
            "docs/days-executor/phases/P90-fixture.md",
            "# P90 fixture\n",
        );
        write(&root, "audit/red.rs", "fn red_fixture() {}\n");

        let corpus = "fixture corpus\n";
        write(&root, "configs/migration/p90-fixture.toml", corpus);
        let budget = budget_manifest(&sha256_bytes(corpus.as_bytes()));
        write(
            &root,
            "docs/days-executor/budgets/p90-fixture.toml",
            &budget,
        );
        let budget_hash = sha256_bytes(budget.as_bytes());

        let golden = "fixture golden\n";
        write(&root, "docs/days-executor/evidence/P90/golden.txt", golden);
        let golden_hash = sha256_bytes(golden.as_bytes());
        run(&root, &["add", "-A"]);
        run(&root, &["commit", "-q", "-m", "Freeze fixture budget"]);
        let frozen_commit = run_output(&root, &["rev-parse", "HEAD"]);

        let measurement = "fixture measurement\n";
        write(
            &root,
            "docs/days-executor/evidence/P90/measurement.txt",
            measurement,
        );
        let measurement_hash = sha256_bytes(measurement.as_bytes());
        run(
            &root,
            &["add", "docs/days-executor/evidence/P90/measurement.txt"],
        );
        run(&root, &["commit", "-q", "-m", "Record fixture measurement"]);
        let measurement_commit = run_output(&root, &["rev-parse", "HEAD"]);

        write(
            &root,
            "docs/days-executor/evidence/P90/t0-evidence.toml",
            &golden_evidence(&golden_hash),
        );
        write(
            &root,
            "docs/days-executor/evidence/P90/measurement.toml",
            &measurement_evidence(&budget_hash, &measurement_commit, &measurement_hash),
        );

        let host = host_platform();
        write(
            &root,
            "docs/days-executor/phases/P90-fixture.toml",
            &phase_metadata(&host, &budget_hash, &frozen_commit),
        );
        run(&root, &["add", "-A"]);
        run(&root, &["commit", "-q", "-m", "Add fixture audit metadata"]);
        Self {
            directory,
            root,
            host,
            pre_budget_commit,
            frozen_commit,
            measurement_commit,
        }
    }

    fn root(&self) -> &Path {
        &self.root
    }

    fn phase_path(&self) -> PathBuf {
        self.root()
            .join("docs/days-executor/phases/P90-fixture.toml")
    }

    fn evidence_path(&self) -> PathBuf {
        self.root()
            .join("docs/days-executor/evidence/P90/t0-evidence.toml")
    }

    fn measurement_path(&self) -> PathBuf {
        self.root()
            .join("docs/days-executor/evidence/P90/measurement.toml")
    }

    fn mutate_phase(&self, mutation: impl FnOnce(String) -> String) {
        mutate(&self.phase_path(), mutation);
    }

    fn set_evidence(&self, contents: &str) {
        fs::write(self.evidence_path(), contents).expect("write evidence mutation");
    }

    fn set_measurement(&self, contents: &str) {
        fs::write(self.measurement_path(), contents).expect("write measurement mutation");
    }

    fn audit(&self) -> AuditReport {
        phase_audit(self.root(), "P90", false)
    }

    fn days_gpu_root(&self) -> PathBuf {
        self.directory.path().join("days-gpu")
    }
}

fn phase_metadata(host: &str, budget_hash: &str, frozen_commit: &str) -> String {
    format!(
        r#"schema_version = 1
phase = "P90"
name = "fixture"
title = "Synthetic fixture"
design_note = "docs/days-executor/phases/P90-fixture.md"
depends_on = []
external_depends_on = []
features = []
platforms = ["{host}"]
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
evidence = [
    "docs/days-executor/evidence/P90/t0-evidence.toml",
    "docs/days-executor/evidence/P90/measurement.toml",
]

[tasks.red_test]
path = "audit/red.rs"
name = "red_fixture"

[[test_commands]]
platform = "{host}"
features = []
argv = ["cargo", "--version"]
deterministic = true

[[budgets]]
path = "docs/days-executor/budgets/p90-fixture.toml"
content_hash = "{budget_hash}"
frozen_at_commit = "{frozen_commit}"
"#
    )
}

fn budget_manifest(corpus_hash: &str) -> String {
    format!(
        r#"schema_version = 3
id = "p90-fixture-budget"
phase = "P90"
frozen_at = "2026-07-26"
description = "Synthetic audit budget"

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
sim_execution_boundary = "std::time::Instant around simulator execution only"
end_to_end_boundary = "process invocation through process exit"
minimum_sample_wall_time_seconds = 1.0
effective_simulation_duration_seconds = 0.002
effective_simulation_duration_source = "top-level duration"
sample_simulated_end_time_rule = "must equal effective duration"
sample_effective_thread_count_rule = "must equal the mode thread count"
run_order = "corpus order, then ST followed by MT"
pairing_order = "pair repetition i within each workload and mode"
resampling_algorithm = "paired bootstrap with 10000 resamples"
resampling_prng = "ChaCha8Rng"
resampling_seed = 1776
st_mode = "nexosim-st"
mt_mode = "nexosim-mt"
best_exact_mode_rule = "lowest median sim_execution among exact Nexosim CPU modes"

[admission]
evaluated_at = "P23"
statistic = "geometric mean of paired throughput ratios"
confidence_rule = "two-sided 95 percent paired-bootstrap interval"

[[admission.thresholds]]
name = "fixture"
metric = "wall-time"
comparison = "<="
value = 1.0
unit = "second"

[[corpus]]
path = "configs/migration/p90-fixture.toml"
content_hash = "{corpus_hash}"
comparison_boundary = "exact-ledger"
workload = "p90-fixture"
mode = "nexosim-st"
role = "correctness"

[waiver]
approving_role = "executor program owner"
policy = "A waiver must be a reviewed manifest change made before the cutover decision."
"#
    )
}

fn golden_evidence(golden_hash: &str) -> String {
    format!(
        r#"schema_version = 1
id = "P90-T0-evidence"
phase = "P90"
task = "T0"
kind = "golden"
description = "Synthetic golden evidence"
command = ["cargo", "--version"]
tool_version = "cargo fixture"
schema = "days-executor/evidence/v1"
tags = []

[[artifacts]]
path = "docs/days-executor/evidence/P90/golden.txt"
content_hash = "{golden_hash}"
"#
    )
}

fn measurement_evidence(budget_hash: &str, run_commit: &str, artifact_hash: &str) -> String {
    format!(
        r#"schema_version = 1
id = "P90-T0-measurement"
phase = "P90"
task = "T0"
kind = "measurement"
description = "Synthetic measurement evidence"
command = ["cargo", "--version"]
tool_version = "cargo fixture"
schema = "days-executor/measurement/v1"
tags = []
budget = "docs/days-executor/budgets/p90-fixture.toml"
budget_hash = "{budget_hash}"
run_commit = "{run_commit}"

[[artifacts]]
path = "docs/days-executor/evidence/P90/measurement.txt"
content_hash = "{artifact_hash}"
"#
    )
}

fn archive_evidence(path: &str, commit: &str, content_hash: Option<&str>) -> String {
    let hash = content_hash
        .map(|value| format!("content_hash = \"{value}\"\n"))
        .unwrap_or_default();
    format!(
        r#"schema_version = 1
id = "P90-T0-evidence"
phase = "P90"
task = "T0"
kind = "archive"
description = "Synthetic archive evidence"
command = ["cargo", "--version"]
tool_version = "cargo fixture"
schema = "days-executor/evidence/v1"
tags = []

[[artifacts]]
path = "{path}"
days_gpu_commit = "{commit}"
{hash}tool_version = "cargo fixture"
command = ["cargo", "--version"]
schema = "days-executor/archive/v1"
"#
    )
}

fn empty_evidence(kind: &str) -> String {
    format!(
        r#"schema_version = 1
id = "P90-T0-evidence"
phase = "P90"
task = "T0"
kind = "{kind}"
description = "Synthetic empty evidence"
command = ["cargo", "--version"]
tool_version = "cargo fixture"
schema = "days-executor/evidence/v1"
tags = []
artifacts = []
"#
    )
}

fn secondary_phase_metadata(
    phase: &str,
    name: &str,
    host: &str,
    task_dependencies: &[&str],
    include_red_test: bool,
) -> String {
    let task_dependencies = task_dependencies
        .iter()
        .map(|dependency| format!("\"{dependency}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let red_test = if include_red_test {
        format!(
            r#"
[tasks.red_test]
path = "audit/{phase}.rs"
name = "red_{phase}"
"#
        )
    } else {
        String::new()
    };
    format!(
        r#"schema_version = 1
phase = "{phase}"
name = "{name}"
title = "Synthetic secondary phase"
design_note = "docs/days-executor/phases/{phase}-{name}.md"
depends_on = []
external_depends_on = []
features = []
platforms = ["{host}"]
semantic_changes = []
api_changes = []
trust_boundary_change = false
proof_changing = false
out_of_scope = []
selectable_backends = []

[[tasks]]
id = "T0"
title = "Secondary fixture task"
depends_on = [{task_dependencies}]
evidence = ["docs/days-executor/evidence/P90/t0-evidence.toml"]
{red_test}
[[test_commands]]
platform = "{host}"
features = []
argv = ["cargo", "--version"]
deterministic = true
"#
    )
}

fn add_secondary_phase(
    fixture: &Fixture,
    phase: &str,
    name: &str,
    task_dependencies: &[&str],
    include_red_test: bool,
) {
    write(
        fixture.root(),
        &format!("docs/days-executor/phases/{phase}-{name}.md"),
        &format!("# {phase} fixture\n"),
    );
    if include_red_test {
        write(
            fixture.root(),
            &format!("audit/{phase}.rs"),
            &format!("fn red_{phase}() {{}}\n"),
        );
    }
    write(
        fixture.root(),
        &format!("docs/days-executor/phases/{phase}-{name}.toml"),
        &secondary_phase_metadata(
            phase,
            name,
            &fixture.host,
            task_dependencies,
            include_red_test,
        ),
    );
}

fn init_days_gpu(fixture: &Fixture, artifact: &str, commit_artifact: bool) -> (String, String) {
    init_days_gpu_at(&fixture.days_gpu_root(), artifact, commit_artifact)
}

fn init_days_gpu_at(root: &Path, artifact: &str, commit_artifact: bool) -> (String, String) {
    fs::create_dir(root).expect("create days-gpu fixture");
    run(root, &["init", "-q"]);
    run(root, &["config", "user.email", "days@example.invalid"]);
    run(root, &["config", "user.name", "Days Audit"]);
    write(root, "README.md", "# days-gpu fixture\n");
    run(root, &["add", "README.md"]);
    run(root, &["commit", "-q", "-m", "Initialize fixture"]);

    write(root, "evidence/P90/archive.bin", artifact);
    if commit_artifact {
        run(root, &["add", "evidence/P90/archive.bin"]);
        run(root, &["commit", "-q", "-m", "Add evidence"]);
    }
    let commit = run_output(root, &["rev-parse", "HEAD"]);
    (commit, sha256_bytes(artifact.as_bytes()))
}

fn host_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn write(root: &Path, relative: &str, contents: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create fixture parent");
    }
    fs::write(path, contents).expect("write fixture file");
}

fn mutate(path: &Path, mutation: impl FnOnce(String) -> String) {
    let contents = fs::read_to_string(path).expect("read mutation target");
    fs::write(path, mutation(contents)).expect("write mutation target");
}

fn file_hash(path: &Path) -> String {
    sha256_bytes(&fs::read(path).expect("read file for hash"))
}

fn run(root: &Path, arguments: &[&str]) {
    let status = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .status()
        .expect("run git");
    assert!(status.success(), "git {arguments:?} failed");
}

fn run_output(root: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .expect("run git for output");
    assert!(output.status.success(), "git {arguments:?} failed");
    String::from_utf8(output.stdout)
        .expect("git output is UTF-8")
        .trim()
        .to_owned()
}

fn run_xtask(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(arguments)
        .arg("--repo-root")
        .arg(root)
        .output()
        .expect("run fixture xtask")
}

fn run_xtask_with_days_gpu(root: &Path, arguments: &[&str], days_gpu_root: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(arguments)
        .arg("--repo-root")
        .arg(root)
        .env("DAYS_GPU_ROOT", days_gpu_root)
        .output()
        .expect("run fixture xtask with days-gpu override")
}

fn assert_code(report: &AuditReport, code: &str) {
    assert!(
        report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code() == code),
        "expected {code}, got: {:#?}",
        report.diagnostics
    );
}

fn assert_code_message(report: &AuditReport, code: &str, message: &str) {
    assert!(
        report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code() == code && diagnostic.message().contains(message)
        }),
        "expected {code} containing {message:?}, got: {:#?}",
        report.diagnostics
    );
}

#[test]
fn p01_metadata_audits_itself() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask has a workspace parent");
    let report = phase_audit(root, "P01", false);
    assert!(!report.has_errors(), "{:#?}", report.diagnostics);
}

#[test]
fn valid_fixture_passes() {
    let fixture = Fixture::new();
    let report = fixture.audit();
    assert!(!report.has_errors(), "{:#?}", report.diagnostics);
}

#[test]
fn malformed_phase_metadata_is_rejected() {
    let fixture = Fixture::new();
    fs::write(fixture.phase_path(), "schema_version = [").expect("truncate metadata");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn phase_metadata_missing_required_field_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replace("title = \"Synthetic fixture\"\n", ""));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn phase_metadata_unknown_field_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|mut value| {
        value.push_str("unknown_field = true\n");
        value
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn phase_metadata_wrong_schema_version_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replacen("schema_version = 1", "schema_version = 2", 1));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0003");
}

#[test]
fn requested_phase_metadata_missing_is_rejected() {
    let fixture = Fixture::new();
    fs::remove_file(fixture.phase_path()).expect("remove requested phase metadata");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0001");
}

#[test]
fn all_phases_empty_directory_is_rejected() {
    let fixture = Fixture::new();
    fs::remove_file(fixture.phase_path()).expect("remove fixture phase metadata");
    let output = run_xtask(fixture.root(), &["all-phases"]);
    assert!(!output.status.success(), "all-phases passed with no phases");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("DAYS-AUDIT-0001"),
        "expected DAYS-AUDIT-0001, got stdout={:?}, stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn all_phases_missing_directory_is_rejected() {
    let fixture = Fixture::new();
    fs::remove_dir_all(fixture.root().join("docs/days-executor/phases"))
        .expect("remove fixture phase directory");
    let output = run_xtask(fixture.root(), &["all-phases"]);
    assert!(
        !output.status.success(),
        "all-phases passed without phase directory"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("DAYS-AUDIT-0001"),
        "expected DAYS-AUDIT-0001, got stdout={:?}, stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn phase_metadata_identity_mismatch_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replacen("name = \"fixture\"", "name = \"wrong\"", 1));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0004");
}

#[test]
fn lowercase_task_suffix_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replace("id = \"T0\"", "id = \"T3r\""));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn forbidden_python_source_is_rejected() {
    let fixture = Fixture::new();
    write(fixture.root(), "executor/gen.py", "print('fixture')\n");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0008");
}

#[test]
fn python_sources_are_forbidden_in_allowlisted_subtrees() {
    let fixture = Fixture::new();
    write(fixture.root(), "docs/generate.py", "print('fixture')\n");
    write(
        fixture.root(),
        "src/executor_codegen.py",
        "print('fixture')\n",
    );
    let report = fixture.audit();
    for path in ["docs/generate.py", "src/executor_codegen.py"] {
        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.code() == "DAYS-AUDIT-0008" && diagnostic.subject() == path
            }),
            "expected DAYS-AUDIT-0008 for {path}, got {:#?}",
            report.diagnostics
        );
    }
}

#[test]
fn python_source_exact_allowlist_preserves_default_deny() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "utils/count_loc.py",
        "print('allowed fixture')\n",
    );
    write(fixture.root(), "docs/generate.py", "print('rejected')\n");
    write(
        fixture.root(),
        "src/executor_codegen.py",
        "print('rejected')\n",
    );
    write(
        fixture.root(),
        "utils/count_loc_copy.py",
        "print('exact-match rejection')\n",
    );
    let report = fixture.audit();
    assert!(
        !report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code() == "DAYS-AUDIT-0008" && diagnostic.subject() == "utils/count_loc.py"
        }),
        "exactly allowlisted utility was rejected: {:#?}",
        report.diagnostics
    );
    for path in [
        "docs/generate.py",
        "src/executor_codegen.py",
        "utils/count_loc_copy.py",
    ] {
        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.code() == "DAYS-AUDIT-0008" && diagnostic.subject() == path
            }),
            "expected DAYS-AUDIT-0008 for {path}, got {:#?}",
            report.diagnostics
        );
    }
}

#[test]
fn foreign_sources_are_forbidden_in_allowlisted_subtrees() {
    let fixture = Fixture::new();
    for (path, contents) in [
        ("src/kernel.cu", "cuda fixture\n"),
        ("docs/gen.pyx", "python extension fixture\n"),
        ("utils/x.metal", "metal fixture\n"),
        ("docs/CMakeLists.txt", "cmake fixture\n"),
    ] {
        write(fixture.root(), path, contents);
    }

    let report = fixture.audit();
    for path in [
        "src/kernel.cu",
        "docs/gen.pyx",
        "utils/x.metal",
        "docs/CMakeLists.txt",
    ] {
        assert!(
            report.diagnostics.iter().any(|diagnostic| {
                diagnostic.code() == "DAYS-AUDIT-0008" && diagnostic.subject() == path
            }),
            "expected DAYS-AUDIT-0008 for {path}, got {:#?}",
            report.diagnostics
        );
    }
}

#[test]
fn forbidden_cuda_source_is_rejected() {
    let fixture = Fixture::new();
    write(fixture.root(), "executor/kernel.cu", "fixture\n");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0008");
}

#[test]
fn forbidden_metal_source_is_rejected() {
    let fixture = Fixture::new();
    write(fixture.root(), "executor/kernel.metal", "fixture\n");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0008");
}

#[test]
fn forbidden_wgsl_source_is_rejected() {
    let fixture = Fixture::new();
    write(fixture.root(), "executor/kernel.wgsl", "fixture\n");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0008");
}

#[test]
fn forbidden_cmake_source_is_rejected() {
    let fixture = Fixture::new();
    write(fixture.root(), "executor/CMakeLists.txt", "fixture\n");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0008");
}

#[test]
fn executor_shebang_interpreter_is_rejected() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "executor/gen",
        "#!/usr/bin/env python3\nprint('fixture')\n",
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0008");
}

#[test]
fn unrecorded_dependency_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replacen("depends_on = []", "depends_on = [\"P89\"]", 1));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0005");
}

#[test]
fn dependency_cycle_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replacen("depends_on = []", "depends_on = [\"P90\"]", 1));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0006");
}

#[test]
fn unknown_external_dependency_gate_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            "external_depends_on = []",
            "external_depends_on = [\"G23_RELESAE\"]",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0005");
}

#[test]
fn cross_phase_task_dependency_requires_phase_closure() {
    let fixture = Fixture::new();
    add_secondary_phase(&fixture, "P89", "prerequisite", &[], true);
    fixture.mutate_phase(|value| {
        value.replace(
            "title = \"Fixture task\"\ndepends_on = []",
            "title = \"Fixture task\"\ndepends_on = [\"P89/T0\"]",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0005");
}

#[test]
fn dirty_generated_tree_is_rejected() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "target/gpu-inspect/tracked.txt",
        "tracked\n",
    );
    run(
        fixture.root(),
        &["add", "-f", "target/gpu-inspect/tracked.txt"],
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0010");
}

#[test]
fn mismatched_budget_hash_is_rejected() {
    let fixture = Fixture::new();
    mutate(
        &fixture
            .root()
            .join("docs/days-executor/budgets/p90-fixture.toml"),
        |value| value.replace("value = 1.0", "value = 2.0"),
    );
    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0011", "declared hash");
}

#[test]
fn malformed_budget_manifest_is_rejected() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "docs/days-executor/budgets/p90-fixture.toml",
        "schema_version = 3\n",
    );
    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0011", "TOML");
}

#[test]
fn post_measurement_budget_change_is_rejected() {
    let fixture = Fixture::new();
    mutate(&fixture.measurement_path(), |value| {
        value.replacen(
            "budget_hash = \"sha256:",
            "budget_hash = \"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\"\n# ",
            1,
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0012");
}

#[test]
fn budget_recommitted_after_measurement_is_rejected() {
    let fixture = Fixture::new();
    let budget_path = fixture
        .root()
        .join("docs/days-executor/budgets/p90-fixture.toml");
    let old_hash = file_hash(&budget_path);
    mutate(&budget_path, |value| {
        value.replace("value = 1.0", "value = 2.0")
    });
    run(
        fixture.root(),
        &["add", "docs/days-executor/budgets/p90-fixture.toml"],
    );
    run(
        fixture.root(),
        &["commit", "-q", "-m", "Move budget threshold"],
    );
    let new_hash = file_hash(&budget_path);
    let new_commit = run_output(fixture.root(), &["rev-parse", "HEAD"]);
    fixture.mutate_phase(|value| {
        value
            .replace(&old_hash, &new_hash)
            .replace(&fixture.frozen_commit, &new_commit)
    });
    mutate(&fixture.measurement_path(), |value| {
        value.replace(&old_hash, &new_hash)
    });

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0026", "is not an ancestor");
}

#[test]
fn retirement_budget_edited_after_measurement_is_rejected() {
    let fixture = Fixture::new();
    let budget_path = fixture
        .root()
        .join("docs/days-executor/budgets/p90-fixture.toml");
    let old_hash = file_hash(&budget_path);
    mutate(&budget_path, |value| {
        value.replace("value = 1.0", "value = 1.1")
    });
    run(
        fixture.root(),
        &["add", "docs/days-executor/budgets/p90-fixture.toml"],
    );
    run(
        fixture.root(),
        &["commit", "-q", "-m", "Edit retirement threshold after run"],
    );
    let edited_hash = file_hash(&budget_path);
    let edited_commit = run_output(fixture.root(), &["rev-parse", "HEAD"]);
    fixture.mutate_phase(|value| {
        value
            .replace(&old_hash, &edited_hash)
            .replace(&fixture.frozen_commit, &edited_commit)
    });
    mutate(&fixture.measurement_path(), |value| {
        value.replace(&old_hash, &edited_hash)
    });

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0026", "is not an ancestor");
}

#[test]
fn budget_and_measurement_in_same_commit_is_rejected() {
    let fixture = Fixture::new();
    fixture
        .mutate_phase(|value| value.replace(&fixture.frozen_commit, &fixture.measurement_commit));
    let expected_message = format!(
        "measurement run_commit {} equals frozen_at_commit {}; the measurement was committed together with the budget instead of after the freeze",
        fixture.measurement_commit, fixture.measurement_commit
    );

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0026", &expected_message);
}

#[test]
fn budget_frozen_before_measurement_passes() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "audit/measurement_marker.rs",
        "fn measurement_marker() {}\n",
    );
    run(fixture.root(), &["add", "audit/measurement_marker.rs"]);
    run(
        fixture.root(),
        &["commit", "-q", "-m", "Prepare measurement run"],
    );
    let run_commit = run_output(fixture.root(), &["rev-parse", "HEAD"]);
    mutate(&fixture.measurement_path(), |value| {
        value.replace(&fixture.measurement_commit, &run_commit)
    });

    let report = fixture.audit();
    assert!(!report.has_errors(), "{:#?}", report.diagnostics);
}

#[test]
fn uncommitted_budget_is_rejected() {
    let fixture = Fixture::new();
    let budget_path = fixture
        .root()
        .join("docs/days-executor/budgets/p90-fixture.toml");
    let old_hash = file_hash(&budget_path);
    mutate(&budget_path, |value| {
        value.replace("value = 1.0", "value = 2.0")
    });
    let new_hash = file_hash(&budget_path);
    fixture.mutate_phase(|value| value.replace(&old_hash, &new_hash));
    mutate(&fixture.measurement_path(), |value| {
        value.replace(&old_hash, &new_hash)
    });

    assert_code(&fixture.audit(), "DAYS-AUDIT-0026");
}

#[test]
fn frozen_budget_path_missing_at_commit_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replace(&fixture.frozen_commit, &fixture.pre_budget_commit));

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0026", "budget path is absent");
}

#[test]
fn frozen_budget_content_mismatch_is_rejected() {
    let fixture = Fixture::new();
    let budget_path = fixture
        .root()
        .join("docs/days-executor/budgets/p90-fixture.toml");
    let original = fs::read_to_string(&budget_path).expect("read original budget");
    mutate(&budget_path, |value| {
        value.replace("value = 1.0", "value = 2.0")
    });
    run(
        fixture.root(),
        &["add", "docs/days-executor/budgets/p90-fixture.toml"],
    );
    run(
        fixture.root(),
        &["commit", "-q", "-m", "Commit different budget"],
    );
    let different_commit = run_output(fixture.root(), &["rev-parse", "HEAD"]);
    fs::write(&budget_path, original).expect("restore working budget bytes");
    fixture.mutate_phase(|value| value.replace(&fixture.frozen_commit, &different_commit));

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0026", "hashes to");
}

#[test]
fn missing_freeze_commit_reports_shallow_history_hint() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            &fixture.frozen_commit,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
    });

    assert_code_message(
        &fixture.audit(),
        "DAYS-AUDIT-0026",
        "not present in this clone (history may be shallow)",
    );
}

#[test]
fn non_commit_freeze_object_is_distinguished() {
    let fixture = Fixture::new();
    let blob = run_output(fixture.root(), &["hash-object", "-w", "root.rs"]);
    fixture.mutate_phase(|value| value.replace(&fixture.frozen_commit, &blob));

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0026", "not a commit");
}

#[test]
fn measurement_missing_required_binding_is_rejected() {
    let fixture = Fixture::new();
    mutate(&fixture.measurement_path(), |value| {
        value
            .lines()
            .filter(|line| {
                !line.starts_with("budget = ")
                    && !line.starts_with("budget_hash = ")
                    && !line.starts_with("run_commit = ")
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    });

    assert_code(&fixture.audit(), "DAYS-AUDIT-0027");
}

#[test]
fn declared_budget_without_measurement_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            r#"evidence = [
    "docs/days-executor/evidence/P90/t0-evidence.toml",
    "docs/days-executor/evidence/P90/measurement.toml",
]"#,
            r#"evidence = ["docs/days-executor/evidence/P90/t0-evidence.toml"]"#,
        )
    });

    assert_code(&fixture.audit(), "DAYS-AUDIT-0027");
}

#[test]
fn measurement_evidence_without_artifacts_is_rejected() {
    let fixture = Fixture::new();
    fixture.set_measurement(&empty_evidence("measurement"));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0013");
}

#[test]
fn measurement_artifact_absent_at_run_commit_is_rejected() {
    let fixture = Fixture::new();
    let original_path = fixture
        .root()
        .join("docs/days-executor/evidence/P90/measurement.txt");
    let original_hash = file_hash(&original_path);
    let late = "late measurement artifact\n";
    write(
        fixture.root(),
        "docs/days-executor/evidence/P90/late-measurement.txt",
        late,
    );
    let late_hash = sha256_bytes(late.as_bytes());
    mutate(&fixture.measurement_path(), |value| {
        value
            .replace(
                "docs/days-executor/evidence/P90/measurement.txt",
                "docs/days-executor/evidence/P90/late-measurement.txt",
            )
            .replace(&original_hash, &late_hash)
    });

    assert_code_message(&fixture.audit(), "DAYS-AUDIT-0014", "absent at run_commit");
}

#[test]
fn measurement_artifact_content_at_run_commit_must_match() {
    let fixture = Fixture::new();
    let artifact_path = fixture
        .root()
        .join("docs/days-executor/evidence/P90/measurement.txt");
    let original_hash = file_hash(&artifact_path);
    fs::write(&artifact_path, "fabricated after run\n").expect("change measurement artifact");
    let changed_hash = file_hash(&artifact_path);
    mutate(&fixture.measurement_path(), |value| {
        value.replace(&original_hash, &changed_hash)
    });

    assert_code_message(
        &fixture.audit(),
        "DAYS-AUDIT-0014",
        "measurement artifact hash",
    );
}

#[test]
fn incomplete_selectable_backend_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|mut value| {
        value.push_str(
            r#"
[[backends]]
name = "cuda"
complete = false
selectable = true
feature = "cuda"
"#,
        );
        value
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0016");
}

#[test]
fn missing_red_test_is_rejected() {
    let fixture = Fixture::new();
    fs::remove_file(fixture.root().join("audit/red.rs")).expect("remove red test");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0007");
}

#[test]
fn task_without_red_test_declaration_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            r#"
[tasks.red_test]
path = "audit/red.rs"
name = "red_fixture"
"#,
            "",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0007");
}

#[test]
fn all_phase_discovery_audits_later_phase_red_tests() {
    let fixture = Fixture::new();
    add_secondary_phase(&fixture, "P91", "later", &[], false);
    let phases = phase_ids(fixture.root()).expect("discover fixture phases");
    assert!(phases.contains(&"P91".to_owned()));
    let report = phase_audit(fixture.root(), "P91", false);
    assert_code(&report, "DAYS-AUDIT-0007");
}

#[test]
fn red_test_function_missing_is_rejected() {
    let fixture = Fixture::new();
    write(fixture.root(), "audit/red.rs", "fn another_test() {}\n");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0007");
}

#[test]
fn phase_without_tasks_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            r#"
[[tasks]]
id = "T0"
title = "Fixture task"
depends_on = []
evidence = [
    "docs/days-executor/evidence/P90/t0-evidence.toml",
    "docs/days-executor/evidence/P90/measurement.toml",
]

[tasks.red_test]
path = "audit/red.rs"
name = "red_fixture"
"#,
            "",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn task_without_evidence_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            r#"evidence = [
    "docs/days-executor/evidence/P90/t0-evidence.toml",
    "docs/days-executor/evidence/P90/measurement.toml",
]"#,
            "evidence = []",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn budget_without_thresholds_is_rejected() {
    let fixture = Fixture::new();
    let corpus_hash = sha256_bytes(b"fixture corpus\n");
    write(
        fixture.root(),
        "docs/days-executor/budgets/p90-fixture.toml",
        &format!(
            r#"schema_version = 3
id = "p90-fixture-budget"
phase = "P90"
frozen_at = "2026-07-26"
description = "Synthetic audit budget"

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
sim_execution_boundary = "std::time::Instant around simulator execution only"
end_to_end_boundary = "process invocation through process exit"
minimum_sample_wall_time_seconds = 1.0
effective_simulation_duration_seconds = 0.002
effective_simulation_duration_source = "top-level duration"
sample_simulated_end_time_rule = "must equal effective duration"
sample_effective_thread_count_rule = "must equal the mode thread count"
run_order = "corpus order, then ST followed by MT"
pairing_order = "pair repetition i within each workload and mode"
resampling_algorithm = "paired bootstrap with 10000 resamples"
resampling_prng = "ChaCha8Rng"
resampling_seed = 1776
st_mode = "nexosim-st"
mt_mode = "nexosim-mt"
best_exact_mode_rule = "lowest median sim_execution among exact Nexosim CPU modes"

[admission]
evaluated_at = "P23"
statistic = "geometric mean of paired throughput ratios"
confidence_rule = "two-sided 95 percent paired-bootstrap interval"
thresholds = []

[[corpus]]
path = "configs/migration/p90-fixture.toml"
content_hash = "{corpus_hash}"
comparison_boundary = "exact-ledger"
workload = "p90-fixture"
mode = "nexosim-st"
role = "correctness"

[waiver]
approving_role = "executor program owner"
policy = "A waiver must be a reviewed manifest change made before the cutover decision."
"#
        ),
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0011");
}

#[test]
fn golden_evidence_without_artifacts_is_rejected() {
    let fixture = Fixture::new();
    fixture.set_evidence(&empty_evidence("golden"));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0013");
}

#[test]
fn archive_evidence_without_artifacts_is_rejected() {
    let fixture = Fixture::new();
    fixture.set_evidence(&empty_evidence("archive"));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0013");
}

#[test]
fn evidence_missing_content_hash_is_rejected() {
    let fixture = Fixture::new();
    fixture.set_evidence(&archive_evidence(
        "evidence/P90/archive.tar",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        None,
    ));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0015");
}

#[test]
fn days_gpu_archive_hash_mismatch_is_rejected() {
    let fixture = Fixture::new();
    let (commit, _) = init_days_gpu(&fixture, "committed evidence\n", true);
    fixture.set_evidence(&archive_evidence(
        "evidence/P90/archive.bin",
        &commit,
        Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    ));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0015");
}

#[test]
fn uncommitted_days_gpu_archive_is_rejected() {
    let fixture = Fixture::new();
    let (commit, hash) = init_days_gpu(&fixture, "uncommitted evidence\n", false);
    fixture.set_evidence(&archive_evidence(
        "evidence/P90/archive.bin",
        &commit,
        Some(&hash),
    ));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0015");
}

#[test]
fn unavailable_days_gpu_archive_check_is_explicitly_skipped() {
    let fixture = Fixture::new();
    fixture.set_evidence(&archive_evidence(
        "evidence/P90/archive.bin",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    ));

    assert_code(&fixture.audit(), "DAYS-AUDIT-0028");
}

#[test]
fn days_gpu_root_override_verifies_archive() {
    let fixture = Fixture::new();
    let archive_root = fixture.directory.path().join("archive-store");
    let (commit, hash) = init_days_gpu_at(&archive_root, "override evidence\n", true);
    fixture.set_evidence(&archive_evidence(
        "evidence/P90/archive.bin",
        &commit,
        Some(&hash),
    ));

    let output = run_xtask_with_days_gpu(fixture.root(), &["phase-audit", "P90"], &archive_root);
    assert!(
        output.status.success(),
        "override audit failed: stdout={:?}, stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn golden_checksum_mismatch_is_rejected() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "docs/days-executor/evidence/P90/golden.txt",
        "changed\n",
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0014");
}

#[cfg(unix)]
#[test]
fn symlinked_golden_cannot_escape_repository() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let outside = fixture.directory.path().join("outside.txt");
    fs::write(&outside, "fixture golden\n").expect("write outside golden");
    let link = fixture
        .root()
        .join("docs/days-executor/evidence/P90/escape.txt");
    symlink(&outside, &link).expect("create escaping symlink");
    mutate(&fixture.evidence_path(), |value| {
        value.replace(
            "docs/days-executor/evidence/P90/golden.txt",
            "docs/days-executor/evidence/P90/escape.txt",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0014");
}

#[cfg(unix)]
#[test]
fn symlinked_evidence_manifest_cannot_escape_repository() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let contents = fs::read_to_string(fixture.evidence_path()).expect("read evidence manifest");
    let outside = fixture.directory.path().join("outside-evidence.toml");
    fs::write(&outside, contents).expect("write outside evidence manifest");
    fs::remove_file(fixture.evidence_path()).expect("remove in-repository evidence manifest");
    symlink(&outside, fixture.evidence_path()).expect("create escaping evidence symlink");

    assert_code(&fixture.audit(), "DAYS-AUDIT-0013");
}

#[cfg(unix)]
#[test]
fn symlinked_phase_manifest_cannot_escape_repository() {
    use std::os::unix::fs::symlink;

    let fixture = Fixture::new();
    let contents = fs::read_to_string(fixture.phase_path()).expect("read phase manifest");
    let outside = fixture.directory.path().join("outside-phase.toml");
    fs::write(&outside, contents).expect("write outside phase manifest");
    fs::remove_file(fixture.phase_path()).expect("remove in-repository phase manifest");
    symlink(&outside, fixture.phase_path()).expect("create escaping phase symlink");

    assert_code(&fixture.audit(), "DAYS-AUDIT-0002");
}

#[test]
fn missing_design_note_is_rejected() {
    let fixture = Fixture::new();
    fs::remove_file(
        fixture
            .root()
            .join("docs/days-executor/phases/P90-fixture.md"),
    )
    .expect("remove design note");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0018");
}

#[test]
fn feature_without_test_command_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replacen("features = []", "features = [\"cuda\"]", 1));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0017");
}

#[test]
fn any_command_does_not_cover_declared_platform() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            &format!("platform = \"{}\"", fixture.host),
            "platform = \"any\"",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0017");
}

#[test]
fn dependency_baseline_drift_is_rejected() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "docs/days-executor/dependency-baseline.toml",
        r#"schema_version = 1
allowed_licenses = ["AGPL-3.0-only"]

[[direct_dependencies]]
package = "fixture-root"
name = "missing"
version = "1"
source = "crates.io"
kind = "normal"
"#,
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0019");
}

#[test]
fn dependency_source_drift_is_rejected() {
    let fixture = Fixture::new();
    mutate(&fixture.root().join("Cargo.toml"), |mut value| {
        value.push_str(
            r#"
[dependencies]
fixture_dep = { package = "fixture-dep", version = "0.1.0", path = "vendor/original" }
"#,
        );
        value
    });
    for directory in ["vendor/original", "vendor/fork"] {
        write(
            fixture.root(),
            &format!("{directory}/Cargo.toml"),
            r#"[package]
name = "fixture-dep"
version = "0.1.0"
edition = "2021"
license = "MIT"
"#,
        );
        write(
            fixture.root(),
            &format!("{directory}/src/lib.rs"),
            "pub fn fixture_dependency() {}\n",
        );
    }
    let baseline = dependency_baseline::render(fixture.root()).expect("render dependency baseline");
    dependency_baseline::write(fixture.root(), &baseline).expect("write dependency baseline");
    mutate(&fixture.root().join("Cargo.toml"), |value| {
        value.replace("path = \"vendor/original\"", "path = \"vendor/fork\"")
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0019");
}

#[test]
fn dependency_baseline_write_does_not_allow_new_license() {
    let fixture = Fixture::new();
    mutate(&fixture.root().join("Cargo.toml"), |value| {
        value.replace(
            "license = \"AGPL-3.0-only\"",
            "license = \"LicenseRef-Proprietary\"",
        )
    });
    let baseline = dependency_baseline::render(fixture.root()).expect("render dependency baseline");
    dependency_baseline::write(fixture.root(), &baseline).expect("write dependency baseline");
    assert_code(&fixture.audit(), "DAYS-AUDIT-0020");
}

#[test]
fn security_advisory_scan_skip_is_explicit() {
    let fixture = Fixture::new();
    let report = fixture.audit();
    assert!(
        report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code() == "DAYS-AUDIT-0022"
                && diagnostic.subject() == "security-advisory-scan"
        }),
        "expected explicit security advisory skip, got {:#?}",
        report.diagnostics
    );
}

#[test]
fn proof_changing_without_evidence_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| value.replace("proof_changing = false", "proof_changing = true"));
    assert_code(&fixture.audit(), "DAYS-AUDIT-0021");
}

#[test]
fn forbidden_toolchain_in_test_command_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|mut value| {
        value.push_str(&format!(
            r#"
[[test_commands]]
platform = "{}"
features = []
argv = ["python3", "--version"]
deterministic = true
"#,
            fixture.host
        ));
        value
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0009");
}

#[test]
fn shell_in_test_command_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            "argv = [\"cargo\", \"--version\"]",
            "argv = [\"bash\", \"docs/run.sh\"]",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0009");
}

#[test]
fn forbidden_toolchain_in_evidence_command_is_rejected() {
    let fixture = Fixture::new();
    mutate(&fixture.evidence_path(), |value| {
        value.replace(
            "command = [\"cargo\", \"--version\"]",
            "command = [\"python3\", \"bench.py\"]",
        )
    });
    assert_code(&fixture.audit(), "DAYS-AUDIT-0009");
}

#[test]
fn dynamically_named_executor_workflow_is_audited() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        ".github/workflows/days-executor-later.yml",
        "steps:\n  - run: python3 --version\n",
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0009");
}

#[test]
fn all_workflows_reject_foreign_toolchains_but_allow_existing_packaging() {
    let fixture = Fixture::new();
    for (path, contents) in [
        (
            ".github/workflows/examples.yml",
            include_str!("../../.github/workflows/examples.yml"),
        ),
        (
            ".github/workflows/leanguard.yml",
            include_str!("../../.github/workflows/leanguard.yml"),
        ),
        (
            ".github/workflows/publish-pypi.yml",
            include_str!("../../.github/workflows/publish-pypi.yml"),
        ),
    ] {
        write(fixture.root(), path, contents);
    }
    let allowed = fixture.audit();
    for path in [
        ".github/workflows/examples.yml",
        ".github/workflows/leanguard.yml",
        ".github/workflows/publish-pypi.yml",
    ] {
        assert!(
            !allowed.diagnostics.iter().any(|diagnostic| {
                diagnostic.code() == "DAYS-AUDIT-0009" && diagnostic.subject().starts_with(path)
            }),
            "pre-existing workflow {path} was rejected: {:#?}",
            allowed.diagnostics
        );
    }

    write(
        fixture.root(),
        ".github/workflows/other.yml",
        "steps:\n  - run: nvcc --version\n",
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0009");
}

#[test]
fn metal_tool_matching_uses_word_boundaries() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        ".github/workflows/other.yml",
        "name: metallic fixture\n",
    );
    let allowed = fixture.audit();
    assert!(
        !allowed.diagnostics.iter().any(|diagnostic| {
            diagnostic.code() == "DAYS-AUDIT-0009"
                && diagnostic
                    .subject()
                    .starts_with(".github/workflows/other.yml")
        }),
        "metal matched inside another identifier: {:#?}",
        allowed.diagnostics
    );

    write(
        fixture.root(),
        ".github/workflows/other.yml",
        "steps:\n  - run: metal --version\n",
    );
    assert_code(&fixture.audit(), "DAYS-AUDIT-0009");
}

#[test]
fn reproduce_succeeds_for_synthetic_command() {
    let fixture = Fixture::new();
    let report = reproduce(fixture.root(), "P90");
    assert!(!report.has_errors(), "{:#?}", report.diagnostics);
}

#[test]
fn reproduce_failing_command_is_rejected() {
    let fixture = Fixture::new();
    fixture.mutate_phase(|value| {
        value.replace(
            "argv = [\"cargo\", \"--version\"]",
            "argv = [\"cargo\", \"--this-flag-does-not-exist\"]",
        )
    });
    assert_code(&reproduce(fixture.root(), "P90"), "DAYS-AUDIT-0023");
}

#[test]
fn reproduce_postflight_detects_corrupted_golden() {
    let fixture = Fixture::new();
    write(
        fixture.root(),
        "src/bin/corrupt.rs",
        r#"fn main() {
    std::fs::write(
        "docs/days-executor/evidence/P90/golden.txt",
        "corrupted by reproduce\n",
    )
    .unwrap();
}
"#,
    );
    fixture.mutate_phase(|value| {
        value.replace(
            "argv = [\"cargo\", \"--version\"]",
            "argv = [\"cargo\", \"run\", \"--quiet\", \"--bin\", \"corrupt\"]",
        )
    });
    assert_code(&reproduce(fixture.root(), "P90"), "DAYS-AUDIT-0014");
}

#[test]
fn reproduce_without_host_command_is_rejected() {
    let fixture = Fixture::new();
    let other = if fixture.host == "linux-x86_64" {
        "macos-aarch64"
    } else {
        "linux-x86_64"
    };
    fixture.mutate_phase(|value| value.replace(&fixture.host, other));
    assert_code(&reproduce(fixture.root(), "P90"), "DAYS-AUDIT-0024");
}

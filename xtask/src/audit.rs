//! Phase audit implementation.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde::Deserialize;

use crate::dependency_baseline::{CURATED_ALLOWED_LICENSES, collect_direct_dependencies};
use crate::diagnostics::{self, Diagnostic};
use crate::hash::{sha256_bytes, sha256_file};
use crate::schema::{
    BudgetReference, DependencyBaseline, DependencyKind, EvidenceKind, EvidenceManifest,
    PhaseMetadata, SchemaError, parse_budget_manifest, parse_dependency_baseline,
    parse_evidence_manifest, parse_phase_metadata,
};

const PHASE_DIRECTORY: &str = "docs/days-executor/phases";
const BASELINE_PATH: &str = "docs/days-executor/dependency-baseline.toml";

/// A repository path prefix exempt from the foreign-source check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceAllowlistEntry {
    /// Repository-relative path prefix.
    pub path: &'static str,
    /// Why the pre-existing tree is outside the executor audit boundary.
    pub reason: &'static str,
}

/// Pre-existing paths outside the executor audit boundary.
pub const SOURCE_ALLOWLIST: &[SourceAllowlistEntry] = &[
    SourceAllowlistEntry {
        path: "lean/",
        reason: "LeanGuard owns its existing fixture runners",
    },
    SourceAllowlistEntry {
        path: "utils/",
        reason: "legacy repository utilities predate the executor",
    },
    SourceAllowlistEntry {
        path: "docs/",
        reason: "the existing documentation site is outside the executor boundary",
    },
    SourceAllowlistEntry {
        path: "crates/nexosim/",
        reason: "the vendored simulator dependency is unchanged by the executor",
    },
    SourceAllowlistEntry {
        path: "src/",
        reason: "the existing Days implementation is outside the executor boundary",
    },
    SourceAllowlistEntry {
        path: "tests/",
        reason: "the existing Days integration tests are outside the executor boundary",
    },
    SourceAllowlistEntry {
        path: "configs/",
        reason: "the existing simulator configurations are outside the executor boundary",
    },
    SourceAllowlistEntry {
        path: "examples/",
        reason: "the existing simulator examples are outside the executor boundary",
    },
    SourceAllowlistEntry {
        path: "ideas/",
        reason: "design scratch material is outside the executor boundary",
    },
    SourceAllowlistEntry {
        path: ".github/workflows/",
        reason: "pre-existing workflows are not executor-owned",
    },
];

/// Exact pre-existing files outside the executor audit boundary.
pub const SOURCE_FILE_ALLOWLIST: &[SourceAllowlistEntry] = &[
    SourceAllowlistEntry {
        path: "pyproject.toml",
        reason: "the existing maturin packaging manifest is not executor-owned",
    },
    SourceAllowlistEntry {
        path: "autoresearch.sh",
        reason: "the existing research helper is not executor-owned",
    },
];

/// Path prefixes whose contents are owned by the executor program.
pub const EXECUTOR_OWNED_ROOTS: &[&str] = &["executor/", "xtask/", "docs/days-executor/"];

/// Prefix and suffix identifying workflows owned by the executor program.
pub const EXECUTOR_OWNED_WORKFLOW_PATTERN: (&str, &str) =
    (".github/workflows/days-executor-", ".yml");

/// Generated trees that must remain ignored, untracked, and clean.
pub const GENERATED_DIRECTORIES: &[&str] =
    &["target/gpu-inspect/", "target/days-executor-generated/"];

/// Evidence tags required whenever a phase changes proofs.
pub const PROOF_EVIDENCE_TAGS: &[&str] = &[
    "lean-toolchain",
    "lake-build-command",
    "theorem-inventory",
    "axiom-report",
    "schema-roundtrip-vectors",
    "proof-mutation-fixtures",
    "trust-boundary-diff",
];

const FORBIDDEN_EXTENSIONS: &[&str] = &[
    "py", "pyi", "pyx", "cu", "cuh", "metal", "msl", "wgsl", "cpp", "cc", "cxx", "hpp", "hh",
    "hxx", "c", "h", "cmake", "sh", "bash", "zsh", "ps1", "bat",
];
const FORBIDDEN_NAMES: &[&str] = &[
    "cmakelists.txt",
    "setup.py",
    "conanfile.txt",
    "makefile",
    "meson.build",
    "requirements.txt",
    "environment.yml",
];
const REPOSITORY_WIDE_FOREIGN_EXTENSIONS: &[&str] = &[
    "py", "cu", "cuh", "metal", "msl", "wgsl", "pyx", "cpp", "cc", "cxx", "hpp",
];
const REPOSITORY_WIDE_FOREIGN_NAMES: &[&str] = &["cmakelists.txt"];
const FORBIDDEN_TOOLS: &[&str] = &[
    "sh",
    "bash",
    "zsh",
    "python",
    "python2",
    "python3",
    "pip",
    "pip3",
    "nvcc",
    "metal",
    "msl",
    "wgsl",
    "cuda",
    "xcrun",
    "cmake",
    "ninja",
    "make",
    "curl",
    "conda",
    "nvidia",
    "nvidia_smi",
];
const FOREIGN_WORKFLOW_TOOLS: &[&str] = &[
    "nvcc", "metal", "msl", "wgsl", "cuda", "nvidia", "cmake", "ninja", "xcrun", "conda",
];
const FORBIDDEN_INTERPRETERS: &[&str] = &["sh", "bash", "zsh", "python", "python2", "python3"];

/// Existing files explicitly exempt from the repository-wide foreign-source prohibition.
pub const FOREIGN_SOURCE_ALLOWLIST: &[SourceAllowlistEntry] = &[SourceAllowlistEntry {
    path: "utils/count_loc.py",
    reason: "pre-existing developer utility outside the Days Executor source-purity boundary",
}];

fn is_foreign_source_allowlisted(path: &str) -> bool {
    FOREIGN_SOURCE_ALLOWLIST
        .iter()
        .any(|entry| entry.path == path)
}

/// External gates recognized by the phase dependency contract.
pub const KNOWN_EXTERNAL_GATES: &[&str] = &["G23_RELEASE"];

/// A validated phase and the metadata file that declared it.
#[derive(Clone, Debug)]
pub struct LoadedPhase {
    /// Parsed phase metadata.
    pub metadata: PhaseMetadata,
    /// Repository-relative metadata path.
    pub path: PathBuf,
}

/// Complete result of a phase audit.
#[derive(Clone, Debug, Default)]
pub struct AuditReport {
    /// Stable diagnostics collected by all checks.
    pub diagnostics: Vec<Diagnostic>,
    /// The requested phase, when exactly one valid matching file was loaded.
    pub phase: Option<LoadedPhase>,
}

impl AuditReport {
    /// Returns whether any collected diagnostic is an error.
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }
}

/// Audits one phase against the repository audit and evidence contract.
pub fn phase_audit(repo_root: &Path, requested_phase: &str, allow_network: bool) -> AuditReport {
    let mut diagnostics = Vec::new();
    let phases = load_phase_set(repo_root, requested_phase, &mut diagnostics);
    check_metadata_identity(&phases, &mut diagnostics);
    check_dependencies(&phases, &mut diagnostics);
    check_budget_consumer_bindings(&phases, &mut diagnostics);

    let matching: Vec<&LoadedPhase> = phases
        .iter()
        .filter(|loaded| {
            metadata_stem_parts(&loaded.path).is_some_and(|(phase, _)| phase == requested_phase)
        })
        .collect();
    let phase = if matching.len() == 1 {
        Some((*matching[0]).clone())
    } else {
        None
    };

    if let Some(loaded) = &phase {
        check_red_tests(repo_root, &loaded.metadata, &mut diagnostics);
        check_test_command_tools(&loaded.metadata, &mut diagnostics);
        check_design_note(repo_root, &loaded.metadata, &mut diagnostics);
        check_backends_and_matrix(&loaded.metadata, &mut diagnostics);
        let evidence_tags =
            check_budgets_and_evidence(repo_root, &loaded.metadata, &mut diagnostics);
        check_proof_gate(&loaded.metadata, &evidence_tags, &mut diagnostics);
    }

    check_source_purity(repo_root, &mut diagnostics);
    check_workflow_tools(repo_root, &mut diagnostics);
    check_generated_trees(repo_root, &mut diagnostics);
    check_dependency_baseline(repo_root, allow_network, &mut diagnostics);
    emit(
        &mut diagnostics,
        "DAYS-AUDIT-0022",
        "security-advisory-scan",
        "security advisory check skipped because T0 has no advisory scanner",
    );

    sort_diagnostics(&mut diagnostics);
    AuditReport { diagnostics, phase }
}

/// Returns every phase identifier declared by a metadata file, sorted and deduplicated.
pub fn phase_ids(repo_root: &Path) -> Result<Vec<String>, String> {
    let directory = repo_root.join(PHASE_DIRECTORY);
    let entries = fs::read_dir(&directory).map_err(|error| {
        phase_discovery_error(format!("cannot read {PHASE_DIRECTORY}: {error}"))
    })?;
    let mut phases = BTreeSet::new();
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("cannot read an entry in {PHASE_DIRECTORY}: {error}"))?;
        let path = entry.path();
        if path.extension() != Some(OsStr::new("toml")) || !path.is_file() {
            continue;
        }
        if let Some((phase, _)) = metadata_stem_parts(&path) {
            phases.insert(phase.to_owned());
        }
    }
    if phases.is_empty() {
        return Err(phase_discovery_error(format!(
            "{PHASE_DIRECTORY} contains no phase metadata files"
        )));
    }
    Ok(phases.into_iter().collect())
}

fn phase_discovery_error(message: String) -> String {
    match Diagnostic::new("DAYS-AUDIT-0001", "all-phases", message) {
        Ok(diagnostic) => diagnostic.to_string(),
        Err(error) => error.to_string(),
    }
}

/// Loads a phase and verifies the metadata, toolchain, budget, and checked-in
/// evidence needed before `reproduce` executes any declared command.
pub fn reproduce_preflight(repo_root: &Path, requested_phase: &str) -> AuditReport {
    let mut diagnostics = Vec::new();
    let phases = load_phase_set(repo_root, requested_phase, &mut diagnostics);
    check_metadata_identity(&phases, &mut diagnostics);
    check_budget_consumer_bindings(&phases, &mut diagnostics);

    let matching: Vec<&LoadedPhase> = phases
        .iter()
        .filter(|loaded| {
            metadata_stem_parts(&loaded.path).is_some_and(|(phase, _)| phase == requested_phase)
        })
        .collect();
    let phase = if matching.len() == 1 {
        Some((*matching[0]).clone())
    } else {
        None
    };
    if let Some(loaded) = &phase {
        check_test_command_tools(&loaded.metadata, &mut diagnostics);
        check_declared_budgets(repo_root, &loaded.metadata, &mut diagnostics);
        check_reproduce_evidence(repo_root, &loaded.metadata, &mut diagnostics);
    }
    sort_diagnostics(&mut diagnostics);
    AuditReport { diagnostics, phase }
}

pub(crate) fn sort_diagnostics(diagnostics: &mut Vec<Diagnostic>) {
    diagnostics.sort_by(|left, right| {
        (left.code(), left.subject(), left.message()).cmp(&(
            right.code(),
            right.subject(),
            right.message(),
        ))
    });
    diagnostics.dedup();
}

pub(crate) fn reproduce_postflight(repo_root: &Path, phase: &PhaseMetadata) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    check_declared_budgets(repo_root, phase, &mut diagnostics);
    check_reproduce_evidence(repo_root, phase, &mut diagnostics);
    check_generated_trees(repo_root, &mut diagnostics);
    check_source_purity(repo_root, &mut diagnostics);
    sort_diagnostics(&mut diagnostics);
    diagnostics
}

pub(crate) fn emit(
    diagnostics: &mut Vec<Diagnostic>,
    code: &'static str,
    subject: impl Into<String>,
    message: impl Into<String>,
) {
    let subject = subject.into();
    let message = message.into();
    if let Some(definition) = diagnostics::definition(code) {
        diagnostics.push(Diagnostic::from_definition(definition, subject, message));
    } else if let Some(definition) = diagnostics::definition("DAYS-AUDIT-0025") {
        diagnostics.push(Diagnostic::from_definition(
            definition,
            code,
            format!("unknown diagnostic requested for {subject}: {message}"),
        ));
    }
}

fn load_phase_set(
    repo_root: &Path,
    requested_phase: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Vec<LoadedPhase> {
    let directory = repo_root.join(PHASE_DIRECTORY);
    let mut paths = Vec::new();
    match fs::read_dir(&directory) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension() == Some(OsStr::new("toml")) && path.is_file() {
                    paths.push(path);
                }
            }
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0001",
                requested_phase,
                format!("cannot read {PHASE_DIRECTORY}: {error}"),
            );
            return Vec::new();
        }
    }
    paths.sort();

    let requested_matches = paths
        .iter()
        .filter(|path| metadata_stem_parts(path).is_some_and(|(phase, _)| phase == requested_phase))
        .count();
    if requested_matches != 1 {
        emit(
            diagnostics,
            "DAYS-AUDIT-0001",
            requested_phase,
            format!(
                "expected exactly one {requested_phase}-*.toml file, found {requested_matches}"
            ),
        );
    }

    let mut loaded = Vec::new();
    for absolute_path in paths {
        let relative_path = relative_display(repo_root, &absolute_path);
        let resolved_path = match resolve_repo_path(repo_root, &relative_path) {
            Ok(path) => path,
            Err(error) => {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0002",
                    &relative_path,
                    format!("phase metadata path is invalid: {error}"),
                );
                continue;
            }
        };
        let contents = match fs::read_to_string(&resolved_path) {
            Ok(contents) => contents,
            Err(error) => {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0002",
                    &relative_path,
                    format!("cannot read phase metadata: {error}"),
                );
                continue;
            }
        };
        match parse_phase_metadata(&contents) {
            Ok(metadata) => loaded.push(LoadedPhase {
                metadata,
                path: PathBuf::from(relative_path),
            }),
            Err(error) => emit_schema_error(diagnostics, &relative_path, error, "DAYS-AUDIT-0002"),
        }
    }
    loaded
}

fn emit_schema_error(
    diagnostics: &mut Vec<Diagnostic>,
    subject: &str,
    error: SchemaError,
    malformed_code: &'static str,
) {
    let code = if matches!(error, SchemaError::UnsupportedVersion { .. }) {
        "DAYS-AUDIT-0003"
    } else {
        malformed_code
    };
    emit(diagnostics, code, subject, error.to_string());
}

fn metadata_stem_parts(path: &Path) -> Option<(&str, &str)> {
    path.file_stem()?.to_str()?.split_once('-')
}

fn relative_display(repo_root: &Path, path: &Path) -> String {
    path.strip_prefix(repo_root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn resolve_repo_path(repo_root: &Path, relative_path: &str) -> Result<PathBuf, String> {
    let canonical_root = fs::canonicalize(repo_root)
        .map_err(|error| format!("cannot canonicalize repository root: {error}"))?;
    let path = repo_root.join(relative_path);
    let canonical_path = fs::canonicalize(&path)
        .map_err(|error| format!("cannot resolve repository path: {error}"))?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(format!(
            "resolved path escapes repository root: {}",
            canonical_path.display()
        ));
    }
    Ok(canonical_path)
}

fn check_metadata_identity(phases: &[LoadedPhase], diagnostics: &mut Vec<Diagnostic>) {
    let mut phase_paths: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    let mut task_paths: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for loaded in phases {
        let path = loaded.path.to_string_lossy().replace('\\', "/");
        match metadata_stem_parts(&loaded.path) {
            Some((phase, name))
                if phase == loaded.metadata.phase && name == loaded.metadata.name => {}
            Some((phase, name)) => emit(
                diagnostics,
                "DAYS-AUDIT-0004",
                &path,
                format!(
                    "file identity {phase}-{name} disagrees with metadata {}-{}",
                    loaded.metadata.phase, loaded.metadata.name
                ),
            ),
            None => emit(
                diagnostics,
                "DAYS-AUDIT-0004",
                &path,
                "file stem must be <PHASE>-<name>",
            ),
        }
        phase_paths
            .entry(&loaded.metadata.phase)
            .or_default()
            .push(path.clone());
        for task in &loaded.metadata.tasks {
            task_paths
                .entry(format!("{}/{}", loaded.metadata.phase, task.id))
                .or_default()
                .push(path.clone());
        }
    }
    for (phase, paths) in phase_paths {
        if paths.len() > 1 {
            emit(
                diagnostics,
                "DAYS-AUDIT-0004",
                phase,
                format!("duplicate phase id in {}", paths.join(", ")),
            );
        }
    }
    for (task, paths) in task_paths {
        if paths.len() > 1 {
            emit(
                diagnostics,
                "DAYS-AUDIT-0004",
                task,
                format!("duplicate task id in {}", paths.join(", ")),
            );
        }
    }
}

fn check_dependencies(phases: &[LoadedPhase], diagnostics: &mut Vec<Diagnostic>) {
    let mut nodes = BTreeSet::new();
    for loaded in phases {
        nodes.insert(loaded.metadata.phase.clone());
        for task in &loaded.metadata.tasks {
            nodes.insert(format!("{}/{}", loaded.metadata.phase, task.id));
        }
    }
    let mut graph: BTreeMap<String, BTreeSet<String>> = nodes
        .iter()
        .cloned()
        .map(|node| (node, BTreeSet::new()))
        .collect();
    let phase_dependencies: BTreeMap<String, BTreeSet<String>> = phases
        .iter()
        .map(|loaded| {
            (
                loaded.metadata.phase.clone(),
                loaded.metadata.depends_on.iter().cloned().collect(),
            )
        })
        .collect();

    for loaded in phases {
        let phase = &loaded.metadata.phase;
        for external in &loaded.metadata.external_depends_on {
            if !KNOWN_EXTERNAL_GATES.contains(&external.as_str()) {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0005",
                    phase,
                    format!("external dependency {external} is not a known gate"),
                );
            }
        }
        for dependency in &loaded.metadata.depends_on {
            add_dependency(&mut graph, &nodes, phase, dependency, diagnostics);
        }
        for task in &loaded.metadata.tasks {
            let node = format!("{phase}/{}", task.id);
            for dependency in &task.depends_on {
                let target = if dependency.starts_with('T') && !dependency.contains('/') {
                    format!("{phase}/{dependency}")
                } else {
                    dependency.clone()
                };
                add_dependency(&mut graph, &nodes, &node, &target, diagnostics);
                if let Some((target_phase, _)) = target.split_once('/') {
                    if target_phase != phase
                        && !phase_dependency_closure(&phase_dependencies, phase)
                            .contains(target_phase)
                    {
                        emit(
                            diagnostics,
                            "DAYS-AUDIT-0005",
                            &node,
                            format!(
                                "cross-phase task dependency {target} lacks phase dependency closure through {target_phase}"
                            ),
                        );
                    }
                }
            }
        }
    }

    let members = dependency_cycle_members(&graph);
    if !members.is_empty() {
        emit(
            diagnostics,
            "DAYS-AUDIT-0006",
            "dependency-graph",
            format!(
                "cycle contains {}",
                members.into_iter().collect::<Vec<_>>().join(", ")
            ),
        );
    }
}

fn phase_dependency_closure(
    graph: &BTreeMap<String, BTreeSet<String>>,
    start: &str,
) -> BTreeSet<String> {
    let mut closure = BTreeSet::new();
    let mut pending: Vec<String> = graph
        .get(start)
        .into_iter()
        .flat_map(|dependencies| dependencies.iter().cloned())
        .collect();
    while let Some(phase) = pending.pop() {
        if !closure.insert(phase.clone()) {
            continue;
        }
        if let Some(dependencies) = graph.get(&phase) {
            pending.extend(dependencies.iter().cloned());
        }
    }
    closure
}

fn add_dependency(
    graph: &mut BTreeMap<String, BTreeSet<String>>,
    nodes: &BTreeSet<String>,
    source: &str,
    target: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if nodes.contains(target) {
        if let Some(edges) = graph.get_mut(source) {
            edges.insert(target.to_owned());
        }
    } else {
        emit(
            diagnostics,
            "DAYS-AUDIT-0005",
            source,
            format!("dependency {target} is not recorded"),
        );
    }
}

fn dependency_cycle_members(graph: &BTreeMap<String, BTreeSet<String>>) -> BTreeSet<String> {
    let mut state: BTreeMap<String, u8> = graph.keys().cloned().map(|node| (node, 0)).collect();
    let mut cycle_members = BTreeSet::new();

    for start in graph.keys() {
        if state.get(start).copied().unwrap_or(0) != 0 {
            continue;
        }
        state.insert(start.clone(), 1);
        let mut active = vec![start.clone()];
        let mut positions = BTreeMap::from([(start.clone(), 0_usize)]);
        let mut frames = vec![(start.clone(), 0_usize)];

        while let Some((node, next_index)) = frames.last_mut() {
            let neighbors: Vec<String> = graph
                .get(node)
                .map(|edges| edges.iter().cloned().collect())
                .unwrap_or_default();
            if *next_index >= neighbors.len() {
                let completed = node.clone();
                frames.pop();
                positions.remove(&completed);
                active.pop();
                state.insert(completed, 2);
                continue;
            }
            let neighbor = neighbors[*next_index].clone();
            *next_index += 1;
            match state.get(&neighbor).copied().unwrap_or(0) {
                0 => {
                    state.insert(neighbor.clone(), 1);
                    positions.insert(neighbor.clone(), active.len());
                    active.push(neighbor.clone());
                    frames.push((neighbor, 0));
                }
                1 => {
                    if let Some(position) = positions.get(&neighbor).copied() {
                        cycle_members.extend(active[position..].iter().cloned());
                    }
                }
                _ => {}
            }
        }
    }
    cycle_members
}

fn check_red_tests(repo_root: &Path, phase: &PhaseMetadata, diagnostics: &mut Vec<Diagnostic>) {
    for task in &phase.tasks {
        let subject = format!("{}/{}", phase.phase, task.id);
        let Some(red_test) = &task.red_test else {
            emit(
                diagnostics,
                "DAYS-AUDIT-0007",
                subject,
                "task declares no red test",
            );
            continue;
        };
        let path = match resolve_repo_path(repo_root, &red_test.path) {
            Ok(path) => path,
            Err(error) => {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0007",
                    red_test.path.clone(),
                    format!("red-test path is invalid: {error}"),
                );
                continue;
            }
        };
        match fs::read_to_string(path) {
            Ok(contents) => {
                let needle = format!("fn {}", red_test.name);
                if !contents.contains(&needle) {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0007",
                        red_test.path.clone(),
                        format!("file does not contain `{needle}`"),
                    );
                }
            }
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0007",
                red_test.path.clone(),
                format!("red-test file cannot be read: {error}"),
            ),
        }
    }
}

fn check_source_purity(repo_root: &Path, diagnostics: &mut Vec<Diagnostic>) {
    let output = match git(
        repo_root,
        &[
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
    ) {
        Ok(output) if output.status.success() => output,
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0008",
                "repository",
                format!(
                    "git could not enumerate repository files: {}",
                    output_text(&output)
                ),
            );
            return;
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0008",
                "repository",
                format!("git could not enumerate repository files: {error}"),
            );
            return;
        }
    };

    let mut files: Vec<String> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| String::from_utf8_lossy(entry).replace('\\', "/"))
        .collect();
    files.sort();
    files.dedup();

    for path in files {
        if fs::symlink_metadata(repo_root.join(&path)).is_err() {
            continue;
        }
        let forbidden_repository_source =
            is_repository_wide_foreign_source(&path) && !is_foreign_source_allowlisted(&path);
        if forbidden_repository_source
            || (is_audited_source_path(&path) && is_forbidden_source(&path))
        {
            let description = Path::new(&path)
                .extension()
                .and_then(OsStr::to_str)
                .map(|extension| format!("hand-authored `.{extension}` source"))
                .unwrap_or_else(|| "hand-authored foreign build source".to_owned());
            emit(
                diagnostics,
                "DAYS-AUDIT-0008",
                &path,
                format!("{description} in the audited repository"),
            );
        }
        check_executor_shebang(repo_root, &path, diagnostics);
    }
}

fn is_audited_source_path(path: &str) -> bool {
    if is_executor_owned_path(path) {
        return true;
    }
    !SOURCE_ALLOWLIST
        .iter()
        .any(|entry| path.starts_with(entry.path))
        && !SOURCE_FILE_ALLOWLIST.iter().any(|entry| path == entry.path)
}

fn is_executor_owned_path(path: &str) -> bool {
    EXECUTOR_OWNED_ROOTS
        .iter()
        .any(|prefix| path.starts_with(prefix))
        || is_executor_owned_workflow(path)
}

fn is_executor_owned_workflow(path: &str) -> bool {
    path.starts_with(EXECUTOR_OWNED_WORKFLOW_PATTERN.0)
        && path.ends_with(EXECUTOR_OWNED_WORKFLOW_PATTERN.1)
}

fn check_executor_shebang(
    repo_root: &Path,
    relative_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !is_executor_owned_path(relative_path) {
        return;
    }
    let Ok(file) = fs::File::open(repo_root.join(relative_path)) else {
        return;
    };
    let mut first_line = String::new();
    if std::io::BufReader::new(file)
        .read_line(&mut first_line)
        .is_err()
    {
        return;
    }
    if !first_line.starts_with("#!") {
        return;
    }
    let interpreters: BTreeSet<String> = first_line
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .filter(|token| FORBIDDEN_INTERPRETERS.contains(&token.as_str()))
        .collect();
    for interpreter in interpreters {
        emit(
            diagnostics,
            "DAYS-AUDIT-0008",
            relative_path,
            format!("executor-owned file has forbidden {interpreter} interpreter shebang"),
        );
    }
}

fn is_forbidden_source(path: &str) -> bool {
    let file_name = Path::new(path)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if FORBIDDEN_NAMES.contains(&file_name.as_str()) {
        return true;
    }
    Path::new(path)
        .extension()
        .and_then(OsStr::to_str)
        .map(|extension| {
            let extension = extension.to_ascii_lowercase();
            FORBIDDEN_EXTENSIONS.contains(&extension.as_str())
        })
        .unwrap_or(false)
}

fn is_repository_wide_foreign_source(path: &str) -> bool {
    let path = Path::new(path);
    let file_name = path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if REPOSITORY_WIDE_FOREIGN_NAMES.contains(&file_name.as_str()) {
        return true;
    }
    path.extension()
        .and_then(OsStr::to_str)
        .map(|extension| extension.to_ascii_lowercase())
        .is_some_and(|extension| REPOSITORY_WIDE_FOREIGN_EXTENSIONS.contains(&extension.as_str()))
}

fn check_workflow_tools(repo_root: &Path, diagnostics: &mut Vec<Diagnostic>) {
    let workflow_root = repo_root.join(".github/workflows");
    let mut relative_paths = Vec::new();
    if let Ok(entries) = fs::read_dir(&workflow_root) {
        for entry in entries.flatten() {
            let relative_path = relative_display(repo_root, &entry.path());
            if relative_path.ends_with(".yml") {
                relative_paths.push(relative_path);
            }
        }
    }
    relative_paths.sort();
    for relative_path in relative_paths {
        let path = repo_root.join(&relative_path);
        if !path.exists() {
            continue;
        }
        match fs::read_to_string(&path) {
            Ok(contents) => {
                for (index, line) in contents.lines().enumerate() {
                    if line.trim_start().starts_with('#') {
                        continue;
                    }
                    let tools = if is_executor_owned_workflow(&relative_path) {
                        FORBIDDEN_TOOLS
                    } else {
                        FOREIGN_WORKFLOW_TOOLS
                    };
                    for tool in matching_forbidden_tokens(line, tools) {
                        emit(
                            diagnostics,
                            "DAYS-AUDIT-0009",
                            format!("{relative_path}:{}", index + 1),
                            format!("workflow invokes {tool}"),
                        );
                    }
                }
            }
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0009",
                relative_path,
                format!("cannot inspect executor-owned workflow: {error}"),
            ),
        }
    }
}

fn check_test_command_tools(phase: &PhaseMetadata, diagnostics: &mut Vec<Diagnostic>) {
    for (index, command) in phase.test_commands.iter().enumerate() {
        for argument in &command.argv {
            for tool in forbidden_tokens(argument) {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0009",
                    format!("{} test_commands[{index}]", phase.phase),
                    format!("declared argv invokes {tool}"),
                );
            }
        }
    }
}

fn forbidden_tokens(text: &str) -> BTreeSet<String> {
    matching_forbidden_tokens(text, FORBIDDEN_TOOLS)
}

fn matching_forbidden_tokens(text: &str, tools: &[&str]) -> BTreeSet<String> {
    text.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .filter(|token| tools.contains(&token.as_str()))
        .collect()
}

fn check_generated_trees(repo_root: &Path, diagnostics: &mut Vec<Diagnostic>) {
    for directory in GENERATED_DIRECTORIES {
        match git(repo_root, &["check-ignore", "-q", directory]) {
            Ok(output) if output.status.success() => {}
            Ok(output) => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                format!("directory is not gitignored: {}", output_text(&output)),
            ),
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                format!("cannot check ignore status: {error}"),
            ),
        }
        match git(repo_root, &["ls-files", "-z", "--", directory]) {
            Ok(output) if output.status.success() && output.stdout.is_empty() => {}
            Ok(output) if output.status.success() => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                "generated directory contains tracked files",
            ),
            Ok(output) => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                format!("cannot inspect tracked files: {}", output_text(&output)),
            ),
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                format!("cannot inspect tracked files: {error}"),
            ),
        }
        match git(repo_root, &["status", "--porcelain", "--", directory]) {
            Ok(output) if output.status.success() && output.stdout.is_empty() => {}
            Ok(output) if output.status.success() => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                "generated directory has repository changes",
            ),
            Ok(output) => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                format!("cannot inspect tree status: {}", output_text(&output)),
            ),
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0010",
                *directory,
                format!("cannot inspect tree status: {error}"),
            ),
        }
    }
}

fn check_design_note(repo_root: &Path, phase: &PhaseMetadata, diagnostics: &mut Vec<Diagnostic>) {
    match resolve_repo_path(repo_root, &phase.design_note) {
        Ok(path) if path.is_file() => {}
        Ok(_) => emit(
            diagnostics,
            "DAYS-AUDIT-0018",
            &phase.design_note,
            "declared design note is not a file",
        ),
        Err(error) => emit(
            diagnostics,
            "DAYS-AUDIT-0018",
            &phase.design_note,
            format!("declared design note cannot be resolved: {error}"),
        ),
    }
}

fn check_backends_and_matrix(phase: &PhaseMetadata, diagnostics: &mut Vec<Diagnostic>) {
    let backends: BTreeMap<&str, _> = phase
        .backends
        .iter()
        .map(|backend| (backend.name.as_str(), backend))
        .collect();
    for backend in &phase.backends {
        if !backend.complete
            && (backend.selectable
                || backend.feature.as_deref().is_none_or(str::is_empty)
                || phase.selectable_backends.contains(&backend.name))
        {
            emit(
                diagnostics,
                "DAYS-AUDIT-0016",
                &backend.name,
                "incomplete backend is selectable or lacks a non-empty feature gate",
            );
        }
    }
    for selected in &phase.selectable_backends {
        if !backends
            .get(selected.as_str())
            .is_some_and(|backend| backend.complete)
        {
            emit(
                diagnostics,
                "DAYS-AUDIT-0016",
                selected,
                "selectable backend is not declared complete",
            );
        }
    }

    for platform in &phase.platforms {
        if !phase
            .test_commands
            .iter()
            .any(|command| command.platform == *platform)
        {
            emit(
                diagnostics,
                "DAYS-AUDIT-0017",
                platform,
                "declared platform has no test command",
            );
        }
    }
    for feature in &phase.features {
        if !phase
            .test_commands
            .iter()
            .any(|command| command.features.contains(feature))
        {
            emit(
                diagnostics,
                "DAYS-AUDIT-0017",
                feature,
                "declared feature has no test command",
            );
        }
    }
}

fn check_budgets_and_evidence(
    repo_root: &Path,
    phase: &PhaseMetadata,
    diagnostics: &mut Vec<Diagnostic>,
) -> BTreeSet<String> {
    check_declared_budgets(repo_root, phase, diagnostics);
    let mut tags = BTreeSet::new();
    let mut cited_budgets = BTreeSet::new();
    for task in &phase.tasks {
        for evidence_path in &task.evidence {
            if let Some(evidence) = load_evidence(repo_root, evidence_path, diagnostics) {
                if evidence.phase != phase.phase || evidence.task != task.id {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0013",
                        evidence_path,
                        format!(
                            "manifest identity {}-{} does not match {}/{}",
                            evidence.phase, evidence.task, phase.phase, task.id
                        ),
                    );
                }
                tags.extend(evidence.tags.iter().cloned());
                check_evidence_artifacts(repo_root, evidence_path, &evidence, diagnostics);
                check_evidence_budget(repo_root, evidence_path, &evidence, diagnostics);
                check_measurement_contract(
                    repo_root,
                    phase,
                    evidence_path,
                    &evidence,
                    &mut cited_budgets,
                    diagnostics,
                );
            }
        }
    }
    check_measurement_coverage(phase, &cited_budgets, diagnostics);
    tags
}

fn check_declared_budgets(
    repo_root: &Path,
    phase: &PhaseMetadata,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for budget in &phase.budgets {
        let path = match resolve_repo_path(repo_root, &budget.path) {
            Ok(path) => path,
            Err(error) => {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0011",
                    &budget.path,
                    format!("declared budget path is invalid: {error}"),
                );
                continue;
            }
        };
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) => {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0011",
                    &budget.path,
                    format!("declared budget cannot be read: {error}"),
                );
                continue;
            }
        };
        match parse_budget_manifest(&contents, repo_root) {
            Ok(manifest) => {
                if manifest.id != budget.id || manifest.phase != budget.owner_phase {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0011",
                        &budget.path,
                        format!(
                            "declared budget identity {} owned by {} does not match manifest identity {} owned by {}",
                            budget.id, budget.owner_phase, manifest.id, manifest.phase
                        ),
                    );
                }
            }
            Err(error) => {
                emit_schema_error(diagnostics, &budget.path, error, "DAYS-AUDIT-0011");
                continue;
            }
        }
        match sha256_file(&path) {
            Ok(actual) if actual == budget.content_hash => {}
            Ok(actual) => emit(
                diagnostics,
                "DAYS-AUDIT-0011",
                &budget.path,
                format!(
                    "declared hash {} does not match {actual}",
                    budget.content_hash
                ),
            ),
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0011",
                &budget.path,
                format!("cannot hash declared budget: {error}"),
            ),
        }
        check_frozen_budget_blob(repo_root, budget, diagnostics);
    }
}

fn check_budget_consumer_bindings(phases: &[LoadedPhase], diagnostics: &mut Vec<Diagnostic>) {
    for owner in phases {
        for budget in &owner.metadata.budgets {
            for consumer_id in &budget.required_consumers {
                let Some(consumer) = phases
                    .iter()
                    .find(|phase| phase.metadata.phase == *consumer_id)
                else {
                    continue;
                };
                let matching = consumer.metadata.budgets.iter().any(|candidate| {
                    candidate.id == budget.id
                        && candidate.owner_phase == budget.owner_phase
                        && candidate.path == budget.path
                        && candidate.content_hash == budget.content_hash
                        && candidate.frozen_at_commit == budget.frozen_at_commit
                });
                if !matching {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0027",
                        consumer.path.display().to_string(),
                        format!(
                            "phase {consumer_id} must cite frozen budget {} owned by {} at {} with hash {} and frozen_at_commit {}",
                            budget.id,
                            budget.owner_phase,
                            budget.path,
                            budget.content_hash,
                            budget.frozen_at_commit
                        ),
                    );
                }
            }
        }
    }
}

fn check_reproduce_evidence(
    repo_root: &Path,
    phase: &PhaseMetadata,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut cited_budgets = BTreeSet::new();
    for task in &phase.tasks {
        for evidence_path in &task.evidence {
            if let Some(evidence) = load_evidence(repo_root, evidence_path, diagnostics) {
                if evidence.kind != EvidenceKind::Archive {
                    check_evidence_artifacts(repo_root, evidence_path, &evidence, diagnostics);
                }
                check_evidence_budget(repo_root, evidence_path, &evidence, diagnostics);
                check_measurement_contract(
                    repo_root,
                    phase,
                    evidence_path,
                    &evidence,
                    &mut cited_budgets,
                    diagnostics,
                );
            }
        }
    }
    check_measurement_coverage(phase, &cited_budgets, diagnostics);
}

fn check_frozen_budget_blob(
    repo_root: &Path,
    budget: &BudgetReference,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !check_reachable_commit(
        repo_root,
        &budget.frozen_at_commit,
        &budget.path,
        "frozen_at_commit",
        diagnostics,
    ) {
        return;
    }

    let object = format!("{}:{}", budget.frozen_at_commit, budget.path);
    match git(repo_root, &["cat-file", "blob", &object]) {
        Ok(output) if output.status.success() => {
            let actual = sha256_bytes(&output.stdout);
            if actual != budget.content_hash {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0026",
                    &budget.path,
                    format!(
                        "budget at frozen_at_commit {} hashes to {actual}, not {}",
                        budget.frozen_at_commit, budget.content_hash
                    ),
                );
            }
        }
        Ok(output) => emit(
            diagnostics,
            "DAYS-AUDIT-0026",
            &budget.path,
            format!(
                "budget path is absent at frozen_at_commit {}: {}",
                budget.frozen_at_commit,
                output_text(&output)
            ),
        ),
        Err(error) => emit(
            diagnostics,
            "DAYS-AUDIT-0026",
            &budget.path,
            format!(
                "cannot read budget at frozen_at_commit {}: {error}",
                budget.frozen_at_commit
            ),
        ),
    }
}

fn check_measurement_contract(
    repo_root: &Path,
    phase: &PhaseMetadata,
    evidence_path: &str,
    evidence: &EvidenceManifest,
    cited_budgets: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if evidence.kind != EvidenceKind::Measurement {
        return;
    }

    let mut missing = Vec::new();
    if evidence.budget.is_none() {
        missing.push("budget");
    }
    if evidence.budget_hash.is_none() {
        missing.push("budget_hash");
    }
    if evidence.run_commit.is_none() {
        missing.push("run_commit");
    }
    if !missing.is_empty() {
        emit(
            diagnostics,
            "DAYS-AUDIT-0027",
            evidence_path,
            format!(
                "measurement evidence is missing required field(s): {}",
                missing.join(", ")
            ),
        );
        return;
    }

    let Some(budget_path) = evidence.budget.as_deref() else {
        return;
    };
    let Some(budget) = phase
        .budgets
        .iter()
        .find(|budget| budget.path == budget_path)
    else {
        emit(
            diagnostics,
            "DAYS-AUDIT-0027",
            evidence_path,
            format!("measurement cites undeclared budget {budget_path}"),
        );
        return;
    };
    cited_budgets.insert(budget_path.to_owned());

    let Some(run_commit) = evidence.run_commit.as_deref() else {
        return;
    };
    if !check_reachable_commit(
        repo_root,
        run_commit,
        evidence_path,
        "run_commit",
        diagnostics,
    ) {
        return;
    }
    if run_commit == budget.frozen_at_commit {
        emit(
            diagnostics,
            "DAYS-AUDIT-0026",
            budget_path,
            format!(
                "measurement run_commit {run_commit} equals frozen_at_commit {}; the measurement was committed together with the budget instead of after the freeze",
                budget.frozen_at_commit
            ),
        );
        return;
    }
    let strict_ancestor = match git(
        repo_root,
        &[
            "merge-base",
            "--is-ancestor",
            &budget.frozen_at_commit,
            run_commit,
        ],
    ) {
        Ok(output) if output.status.success() => true,
        Ok(output) if output.status.code() == Some(1) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "frozen_at_commit {} is not an ancestor of measurement run_commit {run_commit}",
                    budget.frozen_at_commit
                ),
            );
            false
        }
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "cannot determine ancestry from frozen_at_commit {} to measurement run_commit {run_commit}: {}",
                    budget.frozen_at_commit,
                    output_text(&output)
                ),
            );
            false
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "cannot determine ancestry from frozen_at_commit {} to measurement run_commit {run_commit}: {error}",
                    budget.frozen_at_commit
                ),
            );
            false
        }
    };
    if !strict_ancestor {
        return;
    }

    if !check_linear_measurement_history(
        repo_root,
        &budget.frozen_at_commit,
        run_commit,
        budget_path,
        diagnostics,
    ) {
        return;
    }

    check_measurement_artifact_introduction(
        repo_root,
        &budget.frozen_at_commit,
        run_commit,
        evidence_path,
        evidence,
        diagnostics,
    );
}

fn check_linear_measurement_history(
    repo_root: &Path,
    frozen_at_commit: &str,
    run_commit: &str,
    budget_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> bool {
    let range = format!("{frozen_at_commit}..{run_commit}");
    match git(repo_root, &["rev-list", "--merges", &range]) {
        Ok(output) if output.status.success() => {
            let merges = String::from_utf8_lossy(&output.stdout);
            let merges = merges.trim();
            if !merges.is_empty() {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0026",
                    budget_path,
                    format!(
                        "measurement history from frozen_at_commit {frozen_at_commit} to run_commit {run_commit} contains merge commit(s): {}",
                        merges.lines().collect::<Vec<_>>().join(", ")
                    ),
                );
                return false;
            }
        }
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "cannot inspect measurement history from frozen_at_commit {frozen_at_commit} to run_commit {run_commit}: {}",
                    output_text(&output)
                ),
            );
            return false;
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "cannot inspect measurement history from frozen_at_commit {frozen_at_commit} to run_commit {run_commit}: {error}"
                ),
            );
            return false;
        }
    }

    match git(repo_root, &["rev-list", "--first-parent", run_commit]) {
        Ok(output) if output.status.success() => {
            if !String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|commit| commit == frozen_at_commit)
            {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0026",
                    budget_path,
                    format!(
                        "measurement run_commit {run_commit} does not reach frozen_at_commit {frozen_at_commit} through first-parent history"
                    ),
                );
                return false;
            }
        }
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "cannot inspect first-parent history from measurement run_commit {run_commit}: {}",
                    output_text(&output)
                ),
            );
            return false;
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                budget_path,
                format!(
                    "cannot inspect first-parent history from measurement run_commit {run_commit}: {error}"
                ),
            );
            return false;
        }
    }

    true
}

fn check_measurement_artifact_introduction(
    repo_root: &Path,
    frozen_at_commit: &str,
    run_commit: &str,
    evidence_path: &str,
    evidence: &EvidenceManifest,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let range = format!("{frozen_at_commit}..{run_commit}");
    for (index, artifact) in evidence.artifacts.iter().enumerate() {
        let Some(path) = artifact.path.as_deref() else {
            continue;
        };
        let subject = format!("{evidence_path} artifacts[{index}]");
        match git(
            repo_root,
            &[
                "--literal-pathspecs",
                "log",
                "--diff-filter=A",
                "--format=%H",
                &range,
                "--",
                path,
            ],
        ) {
            Ok(output) if output.status.success() => {
                if output.stdout.is_empty() {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0026",
                        &subject,
                        format!(
                            "measurement artifact path {path} has no adding commit after frozen_at_commit {frozen_at_commit} and by run_commit {run_commit}"
                        ),
                    );
                }
            }
            Ok(output) => emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                &subject,
                format!(
                    "cannot inspect adding commits for measurement artifact path {path}: {}",
                    output_text(&output)
                ),
            ),
            Err(error) => emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                &subject,
                format!(
                    "cannot inspect adding commits for measurement artifact path {path}: {error}"
                ),
            ),
        }

        let frozen_object = format!("{frozen_at_commit}:{path}");
        let run_object = format!("{run_commit}:{path}");
        match (
            git(repo_root, &["cat-file", "blob", &frozen_object]),
            git(repo_root, &["cat-file", "blob", &run_object]),
        ) {
            (Ok(frozen), Ok(run))
                if frozen.status.success()
                    && run.status.success()
                    && frozen.stdout == run.stdout =>
            {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0026",
                    &subject,
                    format!(
                        "measurement artifact {path} already had identical content at frozen_at_commit {frozen_at_commit}"
                    ),
                );
            }
            (_, Ok(run)) if !run.status.success() => {}
            (Ok(_), Ok(_)) => {}
            (Err(error), _) | (_, Err(error)) => emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                &subject,
                format!(
                    "cannot compare measurement artifact {path} at frozen_at_commit and run_commit: {error}"
                ),
            ),
        }
    }
}

fn check_measurement_coverage(
    phase: &PhaseMetadata,
    cited_budgets: &BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for budget in &phase.budgets {
        if !cited_budgets.contains(&budget.path) {
            emit(
                diagnostics,
                "DAYS-AUDIT-0027",
                &budget.path,
                format!(
                    "phase {} declares a budget with no citing measurement evidence",
                    phase.phase
                ),
            );
        }
    }
}

fn check_reachable_commit(
    repo_root: &Path,
    commit: &str,
    subject: &str,
    field: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> bool {
    match git(repo_root, &["cat-file", "-t", commit]) {
        Ok(output) if output.status.success() => {
            let object_type = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if object_type != "commit" {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0026",
                    subject,
                    format!("{field} {commit} is a {object_type}, not a commit"),
                );
                return false;
            }
        }
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                subject,
                format!(
                    "{field} {commit} is not present in this clone (history may be shallow): {}",
                    output_text(&output)
                ),
            );
            return false;
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                subject,
                format!("cannot verify {field} {commit}: {error}"),
            );
            return false;
        }
    }

    match git(repo_root, &["merge-base", "--is-ancestor", commit, "HEAD"]) {
        Ok(output) if output.status.success() => true,
        Ok(output) if output.status.code() == Some(1) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                subject,
                format!("{field} {commit} is not reachable from HEAD"),
            );
            false
        }
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                subject,
                format!(
                    "cannot determine whether {field} {commit} is reachable from HEAD: {}",
                    output_text(&output)
                ),
            );
            false
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0026",
                subject,
                format!(
                    "cannot determine whether {field} {commit} is reachable from HEAD: {error}"
                ),
            );
            false
        }
    }
}

fn load_evidence(
    repo_root: &Path,
    relative_path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<EvidenceManifest> {
    let path = match resolve_repo_path(repo_root, relative_path) {
        Ok(path) => path,
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0013",
                relative_path,
                format!("evidence manifest path is invalid: {error}"),
            );
            return None;
        }
    };
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0013",
                relative_path,
                format!("evidence manifest cannot be read: {error}"),
            );
            return None;
        }
    };

    if let Ok(table) = contents.parse::<toml::Table>() {
        let has_budget = table.get("budget").is_some();
        let has_budget_hash = table.get("budget_hash").is_some();
        let is_measurement = table
            .get("kind")
            .and_then(toml::Value::as_str)
            .is_some_and(|kind| kind == "measurement");
        if has_budget_hash && !has_budget && !is_measurement {
            emit(
                diagnostics,
                "DAYS-AUDIT-0002",
                relative_path,
                "budget_hash is forbidden without budget",
            );
            return None;
        }
    }

    match parse_evidence_manifest(&contents) {
        Ok(evidence) => {
            check_evidence_command_tools(relative_path, &evidence, diagnostics);
            Some(evidence)
        }
        Err(error) => {
            emit_schema_error(diagnostics, relative_path, error, "DAYS-AUDIT-0013");
            None
        }
    }
}

fn check_evidence_command_tools(
    evidence_path: &str,
    evidence: &EvidenceManifest,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for argument in &evidence.command {
        for tool in forbidden_tokens(argument) {
            emit(
                diagnostics,
                "DAYS-AUDIT-0009",
                evidence_path,
                format!("evidence command invokes {tool}"),
            );
        }
    }
    for (index, artifact) in evidence.artifacts.iter().enumerate() {
        for argument in artifact.command.iter().flatten() {
            for tool in forbidden_tokens(argument) {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0009",
                    format!("{evidence_path} artifacts[{index}]"),
                    format!("artifact command invokes {tool}"),
                );
            }
        }
    }
}

fn check_evidence_artifacts(
    repo_root: &Path,
    evidence_path: &str,
    evidence: &EvidenceManifest,
    diagnostics: &mut Vec<Diagnostic>,
) {
    match evidence.kind {
        EvidenceKind::Golden | EvidenceKind::Measurement => {
            for (index, artifact) in evidence.artifacts.iter().enumerate() {
                let subject = artifact.path.as_deref().unwrap_or(evidence_path).to_owned();
                let Some(path) = artifact.path.as_deref() else {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0014",
                        format!("{evidence_path} artifacts[{index}]"),
                        "golden artifact has no repository path",
                    );
                    continue;
                };
                if artifact.days_gpu_commit.is_some()
                    || artifact.tool_version.is_some()
                    || artifact.command.is_some()
                    || artifact.schema.is_some()
                {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0014",
                        &subject,
                        "golden artifact must not declare days-gpu archive provenance",
                    );
                }
                let Some(expected_hash) = artifact.content_hash.as_deref() else {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0014",
                        &subject,
                        "golden artifact has no content_hash",
                    );
                    continue;
                };
                match evidence.kind {
                    EvidenceKind::Golden => {
                        let resolved = match resolve_repo_path(repo_root, path) {
                            Ok(resolved) => resolved,
                            Err(error) => {
                                emit(
                                    diagnostics,
                                    "DAYS-AUDIT-0014",
                                    &subject,
                                    format!("golden artifact path is invalid: {error}"),
                                );
                                continue;
                            }
                        };
                        match sha256_file(&resolved) {
                            Ok(actual) if actual == expected_hash => {}
                            Ok(actual) => emit(
                                diagnostics,
                                "DAYS-AUDIT-0014",
                                &subject,
                                format!("declared hash {expected_hash} does not match {actual}"),
                            ),
                            Err(error) => emit(
                                diagnostics,
                                "DAYS-AUDIT-0014",
                                &subject,
                                format!("golden artifact cannot be hashed: {error}"),
                            ),
                        }
                    }
                    EvidenceKind::Measurement => {
                        let Some(run_commit) = evidence.run_commit.as_deref() else {
                            continue;
                        };
                        let object = format!("{run_commit}:{path}");
                        match git(repo_root, &["cat-file", "blob", &object]) {
                            Ok(output) if output.status.success() => {
                                let actual = sha256_bytes(&output.stdout);
                                if actual != expected_hash {
                                    emit(
                                        diagnostics,
                                        "DAYS-AUDIT-0014",
                                        &subject,
                                        format!(
                                            "measurement artifact hash {expected_hash} does not match {actual} at {object}"
                                        ),
                                    );
                                }
                            }
                            Ok(output) => emit(
                                diagnostics,
                                "DAYS-AUDIT-0014",
                                &subject,
                                format!(
                                    "measurement artifact is absent at run_commit {object}: {}",
                                    output_text(&output)
                                ),
                            ),
                            Err(error) => emit(
                                diagnostics,
                                "DAYS-AUDIT-0014",
                                &subject,
                                format!(
                                    "cannot read measurement artifact at run_commit {object}: {error}"
                                ),
                            ),
                        }
                    }
                    EvidenceKind::Archive => {}
                }
            }
        }
        EvidenceKind::Archive => {
            for (index, artifact) in evidence.artifacts.iter().enumerate() {
                let subject = format!("{evidence_path} artifacts[{index}]");
                let Some(path) = artifact.path.as_deref() else {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0015",
                        &subject,
                        "archive artifact has no days-gpu repository path",
                    );
                    continue;
                };
                let expected_prefix = format!("evidence/{}/", evidence.phase);
                if !path.starts_with(&expected_prefix) {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0015",
                        &subject,
                        format!("archive artifact path must be under days-gpu/{expected_prefix}"),
                    );
                }
                let Some(expected_hash) = artifact.content_hash.as_deref() else {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0015",
                        &subject,
                        "archive artifact has no content_hash",
                    );
                    continue;
                };
                let Some(commit) = artifact.days_gpu_commit.as_deref() else {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0015",
                        &subject,
                        "archive artifact has no days_gpu_commit",
                    );
                    continue;
                };
                if !is_git_commit_sha(commit) {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0015",
                        &subject,
                        "days_gpu_commit must be 40 lowercase hexadecimal characters",
                    );
                    continue;
                }
                if artifact.tool_version.as_deref().is_none_or(str::is_empty)
                    || artifact.command.as_ref().is_none_or(Vec::is_empty)
                    || artifact.schema.as_deref().is_none_or(str::is_empty)
                {
                    emit(
                        diagnostics,
                        "DAYS-AUDIT-0015",
                        &subject,
                        "archive artifact needs tool_version, command, and schema",
                    );
                    continue;
                }
                check_days_gpu_artifact(
                    repo_root,
                    &subject,
                    path,
                    expected_hash,
                    commit,
                    diagnostics,
                );
            }
        }
    }
}

fn is_git_commit_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn check_days_gpu_artifact(
    repo_root: &Path,
    subject: &str,
    path: &str,
    expected_hash: &str,
    commit: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let days_gpu = std::env::var_os("DAYS_GPU_ROOT")
        .map(PathBuf::from)
        .or_else(|| repo_root.parent().map(|parent| parent.join("days-gpu")));
    let Some(days_gpu) = days_gpu else {
        emit(
            diagnostics,
            "DAYS-AUDIT-0028",
            subject,
            format!("archive verification skipped; cannot locate days-gpu for {commit}:{path}"),
        );
        return;
    };
    match git(&days_gpu, &["rev-parse", "--is-inside-work-tree"]) {
        Ok(output)
            if output.status.success()
                && String::from_utf8_lossy(&output.stdout).trim() == "true" => {}
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0028",
                subject,
                format!(
                    "archive verification skipped; days-gpu repository {} is unavailable: {}",
                    days_gpu.display(),
                    output_text(&output)
                ),
            );
            return;
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0028",
                subject,
                format!(
                    "archive verification skipped; days-gpu repository {} is unavailable: {error}",
                    days_gpu.display()
                ),
            );
            return;
        }
    }
    let commit_object = format!("{commit}^{{commit}}");
    match git(&days_gpu, &["cat-file", "-e", &commit_object]) {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0015",
                subject,
                format!(
                    "days-gpu commit {commit} cannot be verified: {}",
                    output_text(&output)
                ),
            );
            return;
        }
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0015",
                subject,
                format!("cannot execute git for days-gpu commit {commit}: {error}"),
            );
            return;
        }
    }

    let object = format!("{commit}:{path}");
    match git(&days_gpu, &["show", &object]) {
        Ok(output) if output.status.success() => {
            let actual = sha256_bytes(&output.stdout);
            if actual != expected_hash {
                emit(
                    diagnostics,
                    "DAYS-AUDIT-0015",
                    subject,
                    format!(
                        "days-gpu artifact hash {expected_hash} does not match {actual} at {commit}:{path}"
                    ),
                );
            }
        }
        Ok(output) => emit(
            diagnostics,
            "DAYS-AUDIT-0015",
            subject,
            format!(
                "days-gpu path is absent from recorded commit {commit}:{path}: {}",
                output_text(&output)
            ),
        ),
        Err(error) => emit(
            diagnostics,
            "DAYS-AUDIT-0015",
            subject,
            format!("cannot read days-gpu artifact at {commit}:{path}: {error}"),
        ),
    }
}

fn check_evidence_budget(
    repo_root: &Path,
    evidence_path: &str,
    evidence: &EvidenceManifest,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let Some(budget_path) = evidence.budget.as_deref() else {
        return;
    };
    let Some(expected_hash) = evidence.budget_hash.as_deref() else {
        if evidence.kind != EvidenceKind::Measurement {
            emit(
                diagnostics,
                "DAYS-AUDIT-0012",
                evidence_path,
                "evidence declares budget without budget_hash",
            );
        }
        return;
    };
    let path = match resolve_repo_path(repo_root, budget_path) {
        Ok(path) => path,
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0012",
                budget_path,
                format!("measurement budget path is invalid: {error}"),
            );
            return;
        }
    };
    match sha256_file(&path) {
        Ok(actual) if actual == expected_hash => {}
        Ok(actual) => emit(
            diagnostics,
            "DAYS-AUDIT-0012",
            budget_path,
            format!("measurement hash {expected_hash} does not match {actual}"),
        ),
        Err(error) => emit(
            diagnostics,
            "DAYS-AUDIT-0012",
            budget_path,
            format!("measurement budget cannot be hashed: {error}"),
        ),
    }
}

fn check_proof_gate(
    phase: &PhaseMetadata,
    evidence_tags: &BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !phase.proof_changing {
        return;
    }
    for required in PROOF_EVIDENCE_TAGS {
        if !evidence_tags.contains(*required) {
            emit(
                diagnostics,
                "DAYS-AUDIT-0021",
                *required,
                format!("proof-changing phase {} lacks required tag", phase.phase),
            );
        }
    }
}

fn check_dependency_baseline(
    repo_root: &Path,
    allow_network: bool,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let baseline_path = repo_root.join(BASELINE_PATH);
    let contents = match fs::read_to_string(&baseline_path) {
        Ok(contents) => contents,
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0019",
                BASELINE_PATH,
                format!("dependency baseline cannot be read: {error}"),
            );
            return;
        }
    };
    let baseline = match parse_dependency_baseline(&contents) {
        Ok(baseline) => baseline,
        Err(error) => {
            emit_schema_error(diagnostics, BASELINE_PATH, error, "DAYS-AUDIT-0019");
            return;
        }
    };
    match collect_direct_dependencies(repo_root) {
        Ok(actual) => compare_direct_dependencies(&baseline, &actual, diagnostics),
        Err(error) => emit(
            diagnostics,
            "DAYS-AUDIT-0019",
            BASELINE_PATH,
            error.to_string(),
        ),
    }
    check_license_allowlist(&baseline, diagnostics);
    check_resolved_licenses(repo_root, allow_network, diagnostics);
}

fn compare_direct_dependencies(
    baseline: &DependencyBaseline,
    actual: &[crate::dependency_baseline::GeneratedDependency],
    diagnostics: &mut Vec<Diagnostic>,
) {
    let expected: BTreeSet<(String, String, String, String, String)> = baseline
        .direct_dependencies
        .iter()
        .map(|dependency| {
            (
                dependency.package.clone(),
                dependency.name.clone(),
                dependency.version.clone(),
                dependency.source.clone(),
                dependency_kind_name(dependency.kind).to_owned(),
            )
        })
        .collect();
    let actual: BTreeSet<(String, String, String, String, String)> = actual
        .iter()
        .map(|dependency| {
            (
                dependency.package.clone(),
                dependency.name.clone(),
                dependency.version.clone(),
                dependency.source.clone(),
                dependency.kind.clone(),
            )
        })
        .collect();
    let added: Vec<String> = actual
        .difference(&expected)
        .map(format_dependency)
        .collect();
    let removed: Vec<String> = expected
        .difference(&actual)
        .map(format_dependency)
        .collect();
    if !added.is_empty() || !removed.is_empty() {
        emit(
            diagnostics,
            "DAYS-AUDIT-0019",
            BASELINE_PATH,
            format!(
                "added [{}]; removed [{}]",
                added.join(", "),
                removed.join(", ")
            ),
        );
    }
}

fn check_license_allowlist(baseline: &DependencyBaseline, diagnostics: &mut Vec<Diagnostic>) {
    let declared: BTreeSet<&str> = baseline
        .allowed_licenses
        .iter()
        .map(String::as_str)
        .collect();
    let curated: BTreeSet<&str> = CURATED_ALLOWED_LICENSES.iter().copied().collect();
    if declared != curated {
        let added = declared
            .difference(&curated)
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        let removed = curated
            .difference(&declared)
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        emit(
            diagnostics,
            "DAYS-AUDIT-0020",
            "allowed_licenses",
            format!("baseline differs from curated policy: added [{added}]; removed [{removed}]"),
        );
    }
}

fn dependency_kind_name(kind: DependencyKind) -> &'static str {
    match kind {
        DependencyKind::Normal => "normal",
        DependencyKind::Dev => "dev",
        DependencyKind::Build => "build",
    }
}

fn format_dependency(dependency: &(String, String, String, String, String)) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        dependency.0, dependency.1, dependency.2, dependency.3, dependency.4
    )
}

#[derive(Debug, Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
}

#[derive(Debug, Deserialize)]
struct CargoPackage {
    name: String,
    version: String,
    license: Option<String>,
}

fn check_resolved_licenses(
    repo_root: &Path,
    allow_network: bool,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let offline = cargo_metadata(repo_root, true);
    let output = match offline {
        Ok(output) if output.status.success() => Some(output),
        _ if allow_network => match cargo_metadata(repo_root, false) {
            Ok(output) if output.status.success() => Some(output),
            _ => None,
        },
        _ => None,
    };
    let Some(output) = output else {
        emit(
            diagnostics,
            "DAYS-AUDIT-0022",
            "resolved-set-licenses",
            if allow_network {
                "resolved-set license check skipped because cargo metadata failed offline and after the network retry"
            } else {
                "resolved-set license check skipped because cargo metadata --offline failed"
            },
        );
        return;
    };
    let mut metadata: CargoMetadata = match serde_json::from_slice(&output.stdout) {
        Ok(metadata) => metadata,
        Err(error) => {
            emit(
                diagnostics,
                "DAYS-AUDIT-0022",
                "resolved-set-licenses",
                format!(
                    "resolved-set license check skipped because cargo metadata emitted invalid JSON: {error}"
                ),
            );
            return;
        }
    };
    metadata
        .packages
        .sort_by(|left, right| (&left.name, &left.version).cmp(&(&right.name, &right.version)));
    let allowed: BTreeSet<&str> = CURATED_ALLOWED_LICENSES.iter().copied().collect();
    for package in metadata.packages {
        let license = package.license.as_deref().unwrap_or("<missing>");
        if !allowed.contains(license) {
            emit(
                diagnostics,
                "DAYS-AUDIT-0020",
                format!("{}@{}", package.name, package.version),
                format!("license {license} is not allowed"),
            );
        }
    }
}

fn cargo_metadata(repo_root: &Path, offline: bool) -> std::io::Result<Output> {
    let mut command = Command::new("cargo");
    command.args(["metadata", "--format-version", "1"]);
    if offline {
        command.arg("--offline");
    }
    command.current_dir(repo_root).output()
}

fn git(repo_root: &Path, arguments: &[&str]) -> std::io::Result<Output> {
    Command::new("git")
        .args(arguments)
        .current_dir(repo_root)
        .output()
}

fn output_text(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("exit status {}", output.status)
    } else {
        stderr
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FOREIGN_SOURCE_ALLOWLIST, SOURCE_ALLOWLIST, SOURCE_FILE_ALLOWLIST, emit,
        is_foreign_source_allowlisted,
    };

    #[test]
    fn unknown_diagnostic_code_fails_closed() {
        let mut diagnostics = Vec::new();
        emit(
            &mut diagnostics,
            "DAYS-AUDIT-9999",
            "fixture",
            "unknown code",
        );
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code(), "DAYS-AUDIT-0025");
        assert!(diagnostics[0].is_error());
    }

    #[test]
    fn source_allowlists_have_exact_reviewed_contents() {
        assert_eq!(
            SOURCE_ALLOWLIST
                .iter()
                .map(|entry| (entry.path, entry.reason))
                .collect::<Vec<_>>(),
            vec![
                ("lean/", "LeanGuard owns its existing fixture runners"),
                ("utils/", "legacy repository utilities predate the executor"),
                (
                    "docs/",
                    "the existing documentation site is outside the executor boundary"
                ),
                (
                    "crates/nexosim/",
                    "the vendored simulator dependency is unchanged by the executor"
                ),
                (
                    "src/",
                    "the existing Days implementation is outside the executor boundary"
                ),
                (
                    "tests/",
                    "the existing Days integration tests are outside the executor boundary"
                ),
                (
                    "configs/",
                    "the existing simulator configurations are outside the executor boundary"
                ),
                (
                    "examples/",
                    "the existing simulator examples are outside the executor boundary"
                ),
                (
                    "ideas/",
                    "design scratch material is outside the executor boundary"
                ),
                (
                    ".github/workflows/",
                    "pre-existing workflows are not executor-owned"
                ),
            ]
        );
        assert_eq!(
            SOURCE_FILE_ALLOWLIST
                .iter()
                .map(|entry| (entry.path, entry.reason))
                .collect::<Vec<_>>(),
            vec![
                (
                    "pyproject.toml",
                    "the existing maturin packaging manifest is not executor-owned"
                ),
                (
                    "autoresearch.sh",
                    "the existing research helper is not executor-owned"
                ),
            ]
        );
        assert_eq!(
            FOREIGN_SOURCE_ALLOWLIST
                .iter()
                .map(|entry| (entry.path, entry.reason))
                .collect::<Vec<_>>(),
            vec![(
                "utils/count_loc.py",
                "pre-existing developer utility outside the Days Executor source-purity boundary"
            )]
        );
        assert!(is_foreign_source_allowlisted("utils/count_loc.py"));
        assert!(!is_foreign_source_allowlisted("utils/COUNT_LOC.PY"));
        assert!(!is_foreign_source_allowlisted("utils/count_loc_copy.py"));
    }
}

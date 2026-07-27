//! Reproducible Nexosim baseline collection for P01.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::hash::{is_sha256, sha256_file};
use crate::schema::{BudgetManifest, ComparisonBoundary, CorpusRole, parse_budget_manifest};

const DEFAULT_BUDGET: &str = "docs/days-executor/budgets/retirement-budget.toml";
const RETIREMENT_CORPUS: &str = "docs/days-executor/evidence/P01/retirement-corpus.toml";
const PERF_STATS_FEATURE: &str = "perf_stats";
const MIGRATION_LEDGER_FEATURE: &str = "migration_ledger";

/// Runs corpus, parser, and arithmetic checks without building or executing Days.
pub fn self_test(repo_root: &Path) -> Result<(), String> {
    let budget_input = fs::read_to_string(repo_root.join(DEFAULT_BUDGET)).map_err(display_io)?;
    let budget =
        parse_budget_manifest(&budget_input, repo_root).map_err(|error| error.to_string())?;
    let fixtures = resolve_fixtures(repo_root, &budget)?;
    if fixtures.len() != budget.corpus.len() {
        return Err(format!(
            "resolved {} of {} corpus configs",
            fixtures.len(),
            budget.corpus.len()
        ));
    }

    let output = "\
[INFO] Starting simulation with single threading (1 thread(s)).
[INFO] Simulation execution wall-clock time: 0.021000001 seconds.
[INFO] Simulation completed at time 1500.000 seconds in simulation time.
[perf_stats] steps=7 actions=123 groups=9
";
    let parsed = parse_run_output(output, "st")?;
    if parsed.sim_execution_ns != 21_000_001
        || parsed.simulated_end_ns != 1_500_000_000_000
        || parsed.thread_count != 1
    {
        return Err("baseline output parser self-test mismatch".to_owned());
    }
    if parse_action_count(output)? != 123 {
        return Err("event-count parser self-test mismatch".to_owned());
    }
    if events_per_second(1_000, 2_000_000_000) != 500.0 {
        return Err("events-per-second self-test mismatch".to_owned());
    }
    Ok(())
}

/// Collects correctness artifacts and timed Nexosim baselines under the frozen method.
pub fn collect(repo_root: &Path, output_dir: &Path) -> Result<(), String> {
    let budget_path = repo_root.join(DEFAULT_BUDGET);
    let budget_input = fs::read_to_string(&budget_path).map_err(display_io)?;
    let budget =
        parse_budget_manifest(&budget_input, repo_root).map_err(|error| error.to_string())?;
    validate_output_path(repo_root, output_dir)?;
    let preflight = preflight(repo_root, &budget)?;
    prepare_output_dir(output_dir, &preflight.fixtures)?;

    collect_correctness(repo_root, output_dir, &budget, &preflight.fixtures)?;
    let build = build_timed_binary(repo_root, output_dir, &budget, preflight.feature_probe)?;
    let event_counts = collect_event_counts(repo_root, &budget, &preflight.fixtures)?;
    let samples = collect_timing(
        repo_root,
        &budget,
        &preflight.fixtures,
        &event_counts,
        &build.binary_hash,
    )?;
    let selections = select_modes(&samples, budget.method.repetitions as usize)?;

    write_samples(
        &output_dir.join("nexosim-baseline-samples.csv"),
        &samples,
        &selections,
    )?;
    write_summary(
        &output_dir.join("nexosim-baseline-summary.csv"),
        &samples,
        &selections,
        budget.method.repetitions as usize,
    )?;
    write_selections(
        &output_dir.join("nexosim-baseline-selected-modes.csv"),
        &samples,
        &selections,
    )?;
    write_build_record(&output_dir.join("nexosim-baseline-build.toml"), &build)?;
    Ok(())
}

#[derive(Clone, Debug)]
struct Fixture {
    workload: String,
    mode: String,
    config: String,
    log_path: String,
    required_features: Vec<String>,
    role: CorpusRole,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementCorpusManifest {
    schema_version: u32,
    id: String,
    phase: String,
    task: String,
    description: String,
    fixtures: Vec<RetirementCorpusFixture>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementCorpusFixture {
    path: String,
    content_hash: String,
    comparison_boundary: ComparisonBoundary,
    workload: String,
    mode: String,
    role: CorpusRole,
    model_scope: String,
    required_features: Vec<String>,
    command: Vec<String>,
    ledger_path: Option<String>,
    ledger_hash: Option<String>,
}

fn resolve_fixtures(repo_root: &Path, budget: &BudgetManifest) -> Result<Vec<Fixture>, String> {
    let input = fs::read_to_string(repo_root.join(RETIREMENT_CORPUS)).map_err(display_io)?;
    let declared: RetirementCorpusManifest =
        toml::from_str(&input).map_err(|error| error.to_string())?;
    validate_corpus_header(&declared)?;
    if declared.fixtures.len() != budget.corpus.len() {
        return Err(format!(
            "retirement corpus declares {} fixtures; budget declares {}",
            declared.fixtures.len(),
            budget.corpus.len()
        ));
    }

    let cargo_features = declared_cargo_features(repo_root)?;
    budget
        .corpus
        .iter()
        .zip(&declared.fixtures)
        .map(|(budget_entry, declared_entry)| {
            validate_declared_fixture(budget_entry, declared_entry, &cargo_features)?;
            Ok(Fixture {
                workload: budget_entry.workload.clone(),
                mode: budget_entry.mode.clone(),
                config: budget_entry.path.clone(),
                log_path: config_log_path(repo_root, &budget_entry.path)?,
                required_features: declared_entry.required_features.clone(),
                role: budget_entry.role,
            })
        })
        .collect()
}

fn validate_corpus_header(manifest: &RetirementCorpusManifest) -> Result<(), String> {
    if manifest.schema_version != 1
        || manifest.id != "P01-retirement-corpus"
        || manifest.phase != "P01"
        || manifest.task != "T1"
        || manifest.description.trim().is_empty()
    {
        return Err("retirement corpus header does not match P01 T1".to_owned());
    }
    Ok(())
}

fn declared_cargo_features(repo_root: &Path) -> Result<BTreeSet<String>, String> {
    let input = fs::read_to_string(repo_root.join("Cargo.toml")).map_err(display_io)?;
    let table = input
        .parse::<toml::Table>()
        .map_err(|error| error.to_string())?;
    let features = table
        .get("features")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "workspace Cargo.toml has no [features] table".to_owned())?;
    Ok(features.keys().cloned().collect())
}

fn validate_declared_fixture(
    budget: &crate::schema::BudgetCorpusEntry,
    declared: &RetirementCorpusFixture,
    cargo_features: &BTreeSet<String>,
) -> Result<(), String> {
    if declared.path != budget.path
        || declared.content_hash != budget.content_hash
        || declared.comparison_boundary != budget.comparison_boundary
        || declared.workload != budget.workload
        || declared.mode != budget.mode
        || declared.role != budget.role
    {
        return Err(format!(
            "retirement corpus fixture {} does not match the frozen budget entry",
            declared.path
        ));
    }
    if declared.model_scope.trim().is_empty() {
        return Err(format!("{} has an empty model_scope", declared.path));
    }
    validate_fixture_features(
        &declared.path,
        &declared.mode,
        declared.role,
        &declared.required_features,
        cargo_features,
    )?;

    let expected_command = expected_fixture_command(declared);
    if declared.command != expected_command {
        return Err(format!(
            "{} command does not match its role, path, and required_features",
            declared.path
        ));
    }
    match (&declared.ledger_path, &declared.ledger_hash) {
        (Some(path), Some(hash)) if !path.is_empty() && is_sha256(hash) => {}
        (None, None) => {}
        _ => {
            return Err(format!(
                "{} ledger_path and ledger_hash must form a valid pair",
                declared.path
            ));
        }
    }
    Ok(())
}

fn validate_fixture_features(
    path: &str,
    mode: &str,
    role: CorpusRole,
    required_features: &[String],
    cargo_features: &BTreeSet<String>,
) -> Result<(), String> {
    let feature_set: BTreeSet<&str> = required_features.iter().map(String::as_str).collect();
    if feature_set.len() != required_features.len()
        || required_features.iter().any(String::is_empty)
    {
        return Err(format!("{path} has empty or duplicate required_features"));
    }
    for feature in required_features {
        if !cargo_features.contains(feature) {
            return Err(format!(
                "{path} requires undeclared Cargo feature {feature}"
            ));
        }
    }
    match role {
        CorpusRole::Correctness => {
            if mode != "st" || !feature_set.contains(MIGRATION_LEDGER_FEATURE) {
                return Err(format!(
                    "{path} correctness evidence requires mode st and migration_ledger"
                ));
            }
        }
        CorpusRole::Performance => {
            if !required_features.is_empty() {
                return Err(format!(
                    "{} performance entry declares features {:?}; timed builds must remain feature-free",
                    path, required_features
                ));
            }
        }
    }
    Ok(())
}

fn expected_fixture_command(fixture: &RetirementCorpusFixture) -> Vec<String> {
    match fixture.role {
        CorpusRole::Correctness => vec![
            "cargo".to_owned(),
            "run".to_owned(),
            "--quiet".to_owned(),
            "--features".to_owned(),
            fixture.required_features.join(","),
            "--bin".to_owned(),
            "days".to_owned(),
            "--".to_owned(),
            fixture.path.clone(),
        ],
        CorpusRole::Performance => vec!["target/release/days".to_owned(), fixture.path.clone()],
    }
}

fn prepare_output_dir(output_dir: &Path, fixtures: &[Fixture]) -> Result<(), String> {
    if !output_dir.exists() {
        fs::create_dir_all(output_dir).map_err(display_io)?;
        return Ok(());
    }
    let metadata = fs::symlink_metadata(output_dir).map_err(display_io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "baseline output path is not a regular directory: {}",
            output_dir.display()
        ));
    }

    let expected: Vec<String> = fixtures
        .iter()
        .filter(|fixture| fixture.role == CorpusRole::Correctness)
        .map(digest_name)
        .collect();
    let mut present = BTreeSet::new();
    for entry in fs::read_dir(output_dir).map_err(display_io)? {
        let entry = entry.map_err(display_io)?;
        let entry_metadata = fs::symlink_metadata(entry.path()).map_err(display_io)?;
        if entry_metadata.file_type().is_symlink()
            || !entry_metadata.is_file()
            || entry_metadata.len() == 0
        {
            return Err(format!(
                "partial baseline output is not a nonempty regular file: {}",
                entry.path().display()
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "partial baseline output name is not UTF-8".to_owned())?;
        present.insert(name);
    }
    if present.len() >= expected.len() {
        return Err(format!(
            "existing baseline output does not prove a pre-timing strict prefix: {}",
            output_dir.display()
        ));
    }
    let prefix: BTreeSet<String> = expected.into_iter().take(present.len()).collect();
    if present != prefix {
        return Err(format!(
            "existing baseline output is not the ordered correctness prefix: {}",
            output_dir.display()
        ));
    }
    Ok(())
}

fn digest_name(fixture: &Fixture) -> String {
    format!("{}-terminal-digest.csv", fixture.workload)
}

#[derive(Debug)]
struct Preflight {
    fixtures: Vec<Fixture>,
    feature_probe: FeatureProbe,
}

fn preflight(repo_root: &Path, budget: &BudgetManifest) -> Result<Preflight, String> {
    let fixtures = resolve_fixtures(repo_root, budget)?;
    let feature_probe = probe_timed_features(repo_root, budget)?;
    Ok(Preflight {
        fixtures,
        feature_probe,
    })
}

#[derive(Debug)]
struct ParsedRun {
    sim_execution_ns: u64,
    simulated_end_ns: u64,
    thread_count: u32,
}

#[derive(Clone, Debug)]
struct Sample {
    fixture: Fixture,
    repetition: u32,
    event_count: u64,
    sim_execution_ns: u64,
    end_to_end_ns: u128,
    simulated_end_ns: u64,
    thread_count: u32,
    binary_hash: String,
}

#[derive(Debug, Serialize)]
struct BuildRecord {
    schema_version: u32,
    collector_command: Vec<String>,
    timed_build_command: Vec<String>,
    feature_probe_command: Vec<String>,
    feature_probe_output: Vec<String>,
    verified_features: Vec<String>,
    migration_ledger_enabled: bool,
    timed_binary: String,
    timed_binary_hash: String,
    rustc_version: String,
    cargo_version: String,
    event_count_definition: String,
    event_counter_feature: String,
}

fn collect_correctness(
    repo_root: &Path,
    output_dir: &Path,
    budget: &BudgetManifest,
    fixtures: &[Fixture],
) -> Result<(), String> {
    for fixture in fixtures
        .iter()
        .filter(|fixture| fixture.role == CorpusRole::Correctness)
    {
        if fixture.mode != "st" {
            return Err(format!(
                "correctness fixture {} must use mode st, found {}",
                fixture.config, fixture.mode
            ));
        }
        let args = correctness_build_args(budget, fixture);
        run_checked(repo_root, "cargo", &prepend("build", &args))?;
        let output = run_checked(
            repo_root,
            &timed_binary_path(repo_root, &budget.method.build_profile)
                .display()
                .to_string(),
            std::slice::from_ref(&fixture.config),
        )?;
        let text = combined_output(&output);
        let parsed = parse_run_output(&text, &fixture.mode)?;
        if parsed.thread_count != 1 {
            return Err(format!(
                "{} observed {} threads; correctness evidence requires 1",
                fixture.config, parsed.thread_count
            ));
        }

        let source = repo_root.join(&fixture.log_path);
        copy_nonempty(
            &source.join("migration_terminal_digest.csv"),
            &output_dir.join(digest_name(fixture)),
        )?;
    }
    Ok(())
}

fn correctness_build_args(budget: &BudgetManifest, fixture: &Fixture) -> Vec<String> {
    let mut args = budget.method.cargo_flags.clone();
    if !fixture.required_features.is_empty() {
        args.extend(["--features".to_owned(), fixture.required_features.join(",")]);
    }
    args
}

#[derive(Debug)]
struct TimedBuild {
    record: BuildRecord,
    binary_hash: String,
}

#[derive(Debug)]
struct FeatureProbe {
    command: Vec<String>,
    output: Vec<String>,
    features: BTreeSet<String>,
}

fn probe_timed_features(repo_root: &Path, budget: &BudgetManifest) -> Result<FeatureProbe, String> {
    reject_feature_flags(&budget.method.cargo_flags)?;

    let mut probe_args = prepend("rustc", &budget.method.cargo_flags);
    probe_args.extend(["--".to_owned(), "--print".to_owned(), "cfg".to_owned()]);
    let probe = run_checked(repo_root, "cargo", &probe_args)?;
    let output: Vec<String> = String::from_utf8_lossy(&probe.stdout)
        .lines()
        .map(str::to_owned)
        .collect();
    let features = parse_cfg_features(&output.join("\n"));
    verify_timed_features(&features)?;
    Ok(FeatureProbe {
        command: prepend("cargo", &probe_args),
        output,
        features,
    })
}

fn build_timed_binary(
    repo_root: &Path,
    output_dir: &Path,
    budget: &BudgetManifest,
    feature_probe: FeatureProbe,
) -> Result<TimedBuild, String> {
    let build_args = prepend("build", &budget.method.cargo_flags);
    run_checked(repo_root, "cargo", &build_args)?;
    let binary = timed_binary_path(repo_root, &budget.method.build_profile);
    let binary_hash = sha256_file(&binary).map_err(display_io)?;
    let rustc_version = command_version(repo_root, "rustc")?;
    let cargo_version = command_version(repo_root, "cargo")?;

    let record = BuildRecord {
        schema_version: 1,
        collector_command: vec![
            "cargo".to_owned(),
            "xtask".to_owned(),
            "nexosim-baseline".to_owned(),
            "collect".to_owned(),
            "--output-dir".to_owned(),
            relative_path(repo_root, output_dir)?,
        ],
        timed_build_command: prepend("cargo", &build_args),
        feature_probe_command: feature_probe.command,
        feature_probe_output: feature_probe.output,
        verified_features: feature_probe.features.into_iter().collect(),
        migration_ledger_enabled: false,
        timed_binary: relative_path(repo_root, &binary)?,
        timed_binary_hash: binary_hash.clone(),
        rustc_version,
        cargo_version,
        event_count_definition:
            "Nexosim perf_stats `actions`: scheduled events, scheduled queries, and injected events processed by step_until"
                .to_owned(),
        event_counter_feature: PERF_STATS_FEATURE.to_owned(),
    };
    Ok(TimedBuild {
        record,
        binary_hash,
    })
}

fn collect_event_counts(
    repo_root: &Path,
    budget: &BudgetManifest,
    fixtures: &[Fixture],
) -> Result<BTreeMap<String, u64>, String> {
    let target_dir = TemporaryTargetDir::new()?;
    let mut args = budget.method.cargo_flags.clone();
    args.extend([
        "--features".to_owned(),
        PERF_STATS_FEATURE.to_owned(),
        "--target-dir".to_owned(),
        target_dir.path.display().to_string(),
    ]);
    run_checked(repo_root, "cargo", &prepend("build", &args))?;
    let binary = target_dir
        .path
        .join(&budget.method.build_profile)
        .join("days");

    let mut counts = BTreeMap::new();
    for fixture in fixtures
        .iter()
        .filter(|fixture| fixture.role == CorpusRole::Performance)
    {
        let output = run_checked(
            repo_root,
            &binary.display().to_string(),
            std::slice::from_ref(&fixture.config),
        )?;
        let text = combined_output(&output);
        let parsed = parse_run_output(&text, &fixture.mode)?;
        validate_resolved_inputs(budget, fixture, &parsed)?;
        let count = parse_action_count(&text)?;
        if count == 0 {
            return Err(format!("{} produced a zero event count", fixture.config));
        }
        counts.insert(fixture.config.clone(), count);
    }
    Ok(counts)
}

fn collect_timing(
    repo_root: &Path,
    budget: &BudgetManifest,
    fixtures: &[Fixture],
    event_counts: &BTreeMap<String, u64>,
    binary_hash: &str,
) -> Result<Vec<Sample>, String> {
    let performance: Vec<&Fixture> = fixtures
        .iter()
        .filter(|fixture| fixture.role == CorpusRole::Performance)
        .collect();
    let binary = timed_binary_path(repo_root, &budget.method.build_profile);

    for _ in 0..budget.method.warmups {
        for fixture in &performance {
            let (_, parsed, elapsed) = invoke(repo_root, &binary, fixture)?;
            validate_sample(budget, fixture, &parsed, elapsed)?;
        }
    }

    let mut samples = Vec::with_capacity(
        performance.len() * usize::try_from(budget.method.repetitions).unwrap_or(0),
    );
    for repetition in 1..=budget.method.repetitions {
        for fixture in &performance {
            let (_, parsed, elapsed) = invoke(repo_root, &binary, fixture)?;
            validate_sample(budget, fixture, &parsed, elapsed)?;
            let event_count = *event_counts
                .get(&fixture.config)
                .ok_or_else(|| format!("missing event count for {}", fixture.config))?;
            samples.push(Sample {
                fixture: (*fixture).clone(),
                repetition,
                event_count,
                sim_execution_ns: parsed.sim_execution_ns,
                end_to_end_ns: elapsed,
                simulated_end_ns: parsed.simulated_end_ns,
                thread_count: parsed.thread_count,
                binary_hash: binary_hash.to_owned(),
            });
        }
    }
    Ok(samples)
}

fn invoke(
    repo_root: &Path,
    binary: &Path,
    fixture: &Fixture,
) -> Result<(String, ParsedRun, u128), String> {
    let start = Instant::now();
    let output = run_checked(
        repo_root,
        &binary.display().to_string(),
        std::slice::from_ref(&fixture.config),
    )?;
    let elapsed = start.elapsed().as_nanos();
    let text = combined_output(&output);
    let parsed = parse_run_output(&text, &fixture.mode)?;
    Ok((text, parsed, elapsed))
}

fn validate_sample(
    budget: &BudgetManifest,
    fixture: &Fixture,
    parsed: &ParsedRun,
    end_to_end_ns: u128,
) -> Result<(), String> {
    validate_resolved_inputs(budget, fixture, parsed)?;
    let floor_ns = seconds_to_ns(budget.method.minimum_sample_wall_time_seconds)?;
    if parsed.sim_execution_ns < floor_ns {
        return Err(format!(
            "{} sim_execution {}ns is below frozen floor {}ns",
            fixture.config, parsed.sim_execution_ns, floor_ns
        ));
    }
    if end_to_end_ns < u128::from(parsed.sim_execution_ns) {
        return Err(format!(
            "{} end_to_end {}ns is below sim_execution {}ns",
            fixture.config, end_to_end_ns, parsed.sim_execution_ns
        ));
    }
    Ok(())
}

fn validate_resolved_inputs(
    budget: &BudgetManifest,
    fixture: &Fixture,
    parsed: &ParsedRun,
) -> Result<(), String> {
    let expected_end = seconds_to_ns(budget.method.effective_simulation_duration_seconds)?;
    if parsed.simulated_end_ns != expected_end {
        return Err(format!(
            "{} simulated end {}ns does not equal frozen {}ns",
            fixture.config, parsed.simulated_end_ns, expected_end
        ));
    }
    let expected_threads = match fixture.mode.as_str() {
        "st" => 1,
        "mt" => budget.platform.expected_num_cpus,
        mode => return Err(format!("unsupported performance mode {mode}")),
    };
    if parsed.thread_count != expected_threads {
        return Err(format!(
            "{} observed {} threads; expected {}",
            fixture.config, parsed.thread_count, expected_threads
        ));
    }
    Ok(())
}

fn select_modes(
    samples: &[Sample],
    repetitions: usize,
) -> Result<BTreeMap<String, String>, String> {
    let workloads: BTreeSet<&str> = samples
        .iter()
        .map(|sample| sample.fixture.workload.as_str())
        .collect();
    let mut selections = BTreeMap::new();
    for workload in workloads {
        let st = median_sim(samples, workload, "st", repetitions)?;
        let mt = median_sim(samples, workload, "mt", repetitions)?;
        let selected = if st <= mt { "st" } else { "mt" };
        selections.insert(workload.to_owned(), selected.to_owned());
    }
    Ok(selections)
}

fn median_sim(
    samples: &[Sample],
    workload: &str,
    mode: &str,
    repetitions: usize,
) -> Result<u128, String> {
    let mut values: Vec<u128> = samples
        .iter()
        .filter(|sample| sample.fixture.workload == workload && sample.fixture.mode == mode)
        .map(|sample| u128::from(sample.sim_execution_ns))
        .collect();
    if values.len() != repetitions || repetitions.is_multiple_of(2) {
        return Err(format!(
            "{workload} {mode} requires an odd set of {repetitions} samples, found {}",
            values.len()
        ));
    }
    values.sort_unstable();
    Ok(values[repetitions / 2])
}

fn write_samples(
    path: &Path,
    samples: &[Sample],
    selections: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut file = create_new_file(path)?;
    writeln!(
        file,
        "schema_version,workload,mode,selected_mode,repetition,config_path,event_count,sim_execution_ns,sim_execution_events_per_second,end_to_end_ns,end_to_end_events_per_second,simulated_end_time_ns,effective_thread_count,binary_sha256"
    )
    .map_err(display_io)?;
    for sample in samples {
        let selected = selections
            .get(&sample.fixture.workload)
            .ok_or_else(|| format!("missing selection for {}", sample.fixture.workload))?;
        writeln!(
            file,
            "2,{},{},{},{},{},{},{},{:.6},{},{:.6},{},{},{}",
            sample.fixture.workload,
            sample.fixture.mode,
            selected,
            sample.repetition,
            sample.fixture.config,
            sample.event_count,
            sample.sim_execution_ns,
            events_per_second(sample.event_count, u128::from(sample.sim_execution_ns)),
            sample.end_to_end_ns,
            events_per_second(sample.event_count, sample.end_to_end_ns),
            sample.simulated_end_ns,
            sample.thread_count,
            sample.binary_hash,
        )
        .map_err(display_io)?;
    }
    Ok(())
}

fn write_summary(
    path: &Path,
    samples: &[Sample],
    selections: &BTreeMap<String, String>,
    repetitions: usize,
) -> Result<(), String> {
    let mut file = create_new_file(path)?;
    writeln!(
        file,
        "schema_version,workload,mode,selected_mode,sample_count,event_count,sim_execution_median_ns,sim_execution_min_ns,sim_execution_max_ns,sim_execution_median_events_per_second,end_to_end_median_ns,end_to_end_min_ns,end_to_end_max_ns,end_to_end_median_events_per_second"
    )
    .map_err(display_io)?;
    let mut keys = BTreeSet::new();
    for sample in samples {
        keys.insert((sample.fixture.workload.clone(), sample.fixture.mode.clone()));
    }
    for (workload, mode) in keys {
        let selected_mode = selections
            .get(&workload)
            .ok_or_else(|| format!("missing selection for {workload}"))?;
        let selected: Vec<&Sample> = samples
            .iter()
            .filter(|sample| sample.fixture.workload == workload && sample.fixture.mode == mode)
            .collect();
        if selected.len() != repetitions {
            return Err(format!(
                "{workload} {mode} expected {repetitions} samples, found {}",
                selected.len()
            ));
        }
        let event_count = selected[0].event_count;
        if selected
            .iter()
            .any(|sample| sample.event_count != event_count)
        {
            return Err(format!(
                "{workload} {mode} event count changed across samples"
            ));
        }
        let mut sim: Vec<u128> = selected
            .iter()
            .map(|sample| u128::from(sample.sim_execution_ns))
            .collect();
        let mut end: Vec<u128> = selected.iter().map(|sample| sample.end_to_end_ns).collect();
        sim.sort_unstable();
        end.sort_unstable();
        let middle = repetitions / 2;
        writeln!(
            file,
            "2,{workload},{mode},{selected_mode},{repetitions},{event_count},{},{},{},{:.6},{},{},{},{:.6}",
            sim[middle],
            sim[0],
            sim[repetitions - 1],
            events_per_second(event_count, sim[middle]),
            end[middle],
            end[0],
            end[repetitions - 1],
            events_per_second(event_count, end[middle]),
        )
        .map_err(display_io)?;
    }
    Ok(())
}

fn write_selections(
    path: &Path,
    samples: &[Sample],
    selections: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut file = create_new_file(path)?;
    writeln!(
        file,
        "schema_version,workload,selected_mode,selection_timing_boundary,event_count,median_wall_time_ns,median_events_per_second"
    )
    .map_err(display_io)?;
    for (workload, mode) in selections {
        let mut selected: Vec<&Sample> = samples
            .iter()
            .filter(|sample| sample.fixture.workload == *workload && sample.fixture.mode == *mode)
            .collect();
        selected.sort_by_key(|sample| sample.sim_execution_ns);
        let middle = selected.len() / 2;
        let sample = selected
            .get(middle)
            .ok_or_else(|| format!("no selected samples for {workload}"))?;
        writeln!(
            file,
            "1,{workload},{mode},sim_execution,{},{},{:.6}",
            sample.event_count,
            sample.sim_execution_ns,
            events_per_second(sample.event_count, u128::from(sample.sim_execution_ns)),
        )
        .map_err(display_io)?;
    }
    Ok(())
}

fn write_build_record(path: &Path, build: &TimedBuild) -> Result<(), String> {
    let contents = toml::to_string_pretty(&build.record).map_err(|error| error.to_string())?;
    let mut file = create_new_file(path)?;
    file.write_all(contents.as_bytes()).map_err(display_io)
}

fn parse_run_output(text: &str, expected_mode: &str) -> Result<ParsedRun, String> {
    if text.contains("Simulation stopped early:") {
        return Err("simulator reported an early stop".to_owned());
    }
    let prefix = match expected_mode {
        "st" => "Starting simulation with single threading (",
        "mt" => "Starting simulation with multiple threading (",
        mode => return Err(format!("unsupported mode {mode}")),
    };
    let thread_count = parse_between(text, prefix, " thread(s)).")?
        .parse::<u32>()
        .map_err(|error| format!("invalid thread count: {error}"))?;
    let sim_execution_ns = decimal_seconds_to_ns(parse_between(
        text,
        "Simulation execution wall-clock time: ",
        " seconds.",
    )?)?;
    let simulated_end_ns = decimal_seconds_to_ns(parse_between(
        text,
        "Simulation completed at time ",
        " seconds in simulation time.",
    )?)?;
    Ok(ParsedRun {
        sim_execution_ns,
        simulated_end_ns,
        thread_count,
    })
}

fn parse_action_count(text: &str) -> Result<u64, String> {
    let marker = "[perf_stats] ";
    let line = text
        .lines()
        .find(|line| line.contains(marker))
        .ok_or_else(|| "missing Nexosim perf_stats line".to_owned())?;
    let actions = line
        .split_whitespace()
        .find_map(|field| field.strip_prefix("actions="))
        .ok_or_else(|| "perf_stats line has no actions field".to_owned())?;
    actions
        .parse::<u64>()
        .map_err(|error| format!("invalid perf_stats action count: {error}"))
}

fn parse_between<'a>(text: &'a str, prefix: &str, suffix: &str) -> Result<&'a str, String> {
    let start = text
        .find(prefix)
        .ok_or_else(|| format!("missing output prefix {prefix:?}"))?
        + prefix.len();
    let rest = &text[start..];
    let end = rest
        .find(suffix)
        .ok_or_else(|| format!("missing output suffix {suffix:?}"))?;
    Ok(&rest[..end])
}

fn decimal_seconds_to_ns(value: &str) -> Result<u64, String> {
    let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("unsupported decimal seconds {value:?}"));
    }
    let seconds = seconds
        .parse::<u64>()
        .map_err(|error| format!("invalid seconds {value:?}: {error}"))?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<u64>()
            .map_err(|error| format!("invalid fraction {value:?}: {error}"))?
            * 10_u64.pow(u32::try_from(9 - fraction.len()).unwrap_or(0))
    };
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|base| base.checked_add(fraction))
        .ok_or_else(|| format!("decimal seconds overflow {value:?}"))
}

fn seconds_to_ns(seconds: f64) -> Result<u64, String> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(format!("invalid seconds value {seconds}"));
    }
    let ns = seconds * 1_000_000_000.0;
    if ns > u64::MAX as f64 {
        return Err(format!("seconds value {seconds} overflows nanoseconds"));
    }
    Ok(ns.round() as u64)
}

fn events_per_second(events: u64, elapsed_ns: u128) -> f64 {
    events as f64 * 1_000_000_000.0 / elapsed_ns as f64
}

fn parse_cfg_features(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("feature=\"")
                .and_then(|value| value.strip_suffix('"'))
                .map(str::to_owned)
        })
        .collect()
}

fn verify_timed_features(features: &BTreeSet<String>) -> Result<(), String> {
    if features.contains(MIGRATION_LEDGER_FEATURE) {
        Err("timed feature probe enabled migration_ledger".to_owned())
    } else {
        Ok(())
    }
}

fn reject_feature_flags(flags: &[String]) -> Result<(), String> {
    if flags.iter().any(|flag| {
        flag == "--features"
            || flag.starts_with("--features=")
            || flag == "--all-features"
            || flag == "-F"
            || flag.starts_with("-F")
    }) {
        return Err("timed cargo_flags must not enable Cargo features".to_owned());
    }
    Ok(())
}

fn config_log_path(repo_root: &Path, config: &str) -> Result<String, String> {
    let input = fs::read_to_string(repo_root.join(config)).map_err(display_io)?;
    let table = input
        .parse::<toml::Table>()
        .map_err(|error| error.to_string())?;
    table
        .get("log_path")
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("{config} has no top-level log_path"))
}

fn run_checked(repo_root: &Path, program: &str, args: &[String]) -> Result<Output, String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(repo_root)
        .output()
        .map_err(display_io)?;
    if !output.status.success() {
        return Err(format!(
            "{} exited with {}:\n{}",
            prepend(program, args).join(" "),
            output.status,
            combined_output(&output)
        ));
    }
    Ok(output)
}

fn command_version(repo_root: &Path, program: &str) -> Result<String, String> {
    let output = run_checked(repo_root, program, &["--version".to_owned()])?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn combined_output(output: &Output) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

fn timed_binary_path(root: &Path, profile: &str) -> PathBuf {
    root.join("target").join(profile).join("days")
}

fn validate_output_path(repo_root: &Path, output_dir: &Path) -> Result<(), String> {
    if !output_dir.is_absolute() {
        return Err("baseline output directory must be an absolute repository path".to_owned());
    }
    if !output_dir.starts_with(repo_root) {
        return Err(format!(
            "baseline output directory must stay under {}",
            repo_root.display()
        ));
    }
    Ok(())
}

fn relative_path(repo_root: &Path, path: &Path) -> Result<String, String> {
    path.strip_prefix(repo_root)
        .map(|relative| relative.display().to_string())
        .map_err(|_| format!("{} is outside {}", path.display(), repo_root.display()))
}

fn prepend(first: &str, rest: &[String]) -> Vec<String> {
    std::iter::once(first.to_owned())
        .chain(rest.iter().cloned())
        .collect()
}

fn copy_nonempty(source: &Path, destination: &Path) -> Result<(), String> {
    let source_bytes = fs::read(source).map_err(display_io)?;
    if source_bytes.is_empty() {
        return Err(format!("{} is empty", source.display()));
    }
    if destination.exists() {
        let destination_bytes = fs::read(destination).map_err(display_io)?;
        return if source_bytes == destination_bytes {
            Ok(())
        } else {
            Err(format!(
                "{} differs from regenerated {}",
                destination.display(),
                source.display()
            ))
        };
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(display_io)?;
    file.write_all(&source_bytes).map_err(display_io)
}

fn create_new_file(path: &Path) -> Result<File, String> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(display_io)
}

fn display_io(error: io::Error) -> String {
    error.to_string()
}

struct TemporaryTargetDir {
    path: PathBuf,
}

impl TemporaryTargetDir {
    fn new() -> Result<Self, String> {
        let path =
            std::env::temp_dir().join(format!("days-t1-event-counter-{}", std::process::id()));
        if path.exists() {
            return Err(format!(
                "temporary target directory already exists: {}",
                path.display()
            ));
        }
        fs::create_dir(&path).map_err(display_io)?;
        Ok(Self { path })
    }
}

impl Drop for TemporaryTargetDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_BUDGET, config_log_path, copy_nonempty, correctness_build_args,
        decimal_seconds_to_ns, digest_name, parse_action_count, parse_cfg_features,
        prepare_output_dir, resolve_fixtures, self_test, validate_fixture_features,
        verify_timed_features,
    };
    use crate::schema::{BudgetManifest, CorpusRole, parse_budget_manifest};
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask has a workspace parent")
            .to_path_buf()
    }

    fn frozen_budget() -> BudgetManifest {
        let root = repository_root();
        let input = fs::read_to_string(root.join(DEFAULT_BUDGET)).expect("read frozen budget");
        parse_budget_manifest(&input, &root).expect("parse frozen budget")
    }

    #[test]
    fn config_log_path_parses_a_document_with_a_leading_comment() {
        let repo = TempDir::new().expect("temporary repository");
        fs::write(
            repo.path().join("fixture.toml"),
            "# corpus fixture\nlog_path = \"logs/test\"\n",
        )
        .expect("write fixture");

        assert_eq!(
            config_log_path(repo.path(), "fixture.toml").expect("parse TOML document"),
            "logs/test"
        );
    }

    #[test]
    fn correctness_resume_does_not_overwrite_an_existing_digest() {
        let repo = TempDir::new().expect("temporary repository");
        let source = repo.path().join("source.csv");
        let destination = repo.path().join("destination.csv");
        fs::write(&source, "new digest\n").expect("write source");
        fs::write(&destination, "existing digest\n").expect("write destination");

        assert!(copy_nonempty(&source, &destination).is_err());
        assert_eq!(
            fs::read_to_string(destination).expect("read destination"),
            "existing digest\n"
        );
    }

    #[test]
    fn frozen_corpus_features_are_satisfiable_and_timed_entries_are_empty() {
        let root = repository_root();
        let budget = frozen_budget();
        let fixtures = resolve_fixtures(&root, &budget).expect("resolve frozen corpus");
        assert_eq!(fixtures.len(), 13);
        assert!(
            fixtures
                .iter()
                .filter(|fixture| fixture.role == CorpusRole::Performance)
                .all(|fixture| fixture.required_features.is_empty())
        );

        let dcqcn = fixtures
            .iter()
            .find(|fixture| fixture.workload == "leanguard-dcqcn")
            .expect("DCQCN fixture");
        assert_eq!(
            dcqcn.required_features,
            ["migration_ledger", "l2_pfc", "dcqcn"]
        );
        assert_eq!(
            correctness_build_args(&budget, dcqcn),
            [
                "--locked",
                "--release",
                "--bin",
                "days",
                "--features",
                "migration_ledger,l2_pfc,dcqcn",
            ]
        );
    }

    #[test]
    fn preflight_rejects_unknown_and_performance_features() {
        let available: BTreeSet<String> = ["migration_ledger", "dcqcn"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert!(
            validate_fixture_features(
                "correctness.toml",
                "st",
                CorpusRole::Correctness,
                &["migration_ledger".to_owned(), "missing".to_owned()],
                &available,
            )
            .is_err()
        );
        assert!(
            validate_fixture_features(
                "performance.toml",
                "st",
                CorpusRole::Performance,
                &["migration_ledger".to_owned()],
                &available,
            )
            .is_err()
        );
    }

    #[test]
    fn partial_output_requires_an_ordered_correctness_prefix() {
        let root = repository_root();
        let budget = frozen_budget();
        let fixtures = resolve_fixtures(&root, &budget).expect("resolve frozen corpus");
        let output = TempDir::new().expect("temporary output");
        let correctness: Vec<_> = fixtures
            .iter()
            .filter(|fixture| fixture.role == CorpusRole::Correctness)
            .collect();

        for fixture in correctness.iter().take(4) {
            fs::write(output.path().join(digest_name(fixture)), "digest\n")
                .expect("write prefix digest");
        }
        prepare_output_dir(output.path(), &fixtures).expect("accept strict prefix");

        fs::write(output.path().join(digest_name(correctness[4])), "digest\n")
            .expect("write complete correctness set");
        assert!(prepare_output_dir(output.path(), &fixtures).is_err());
    }

    #[test]
    fn baseline_runner_self_test_passes() {
        self_test(&repository_root()).expect("baseline runner self-test");
    }

    #[test]
    fn cfg_feature_probe_is_explicit() {
        let features = parse_cfg_features(
            "debug_assertions\nfeature=\"l2\"\nfeature=\"migration_ledger\"\nunix\n",
        );
        assert!(features.contains("l2"));
        assert!(features.contains("migration_ledger"));
        assert!(verify_timed_features(&features).is_err());
        assert!(verify_timed_features(&parse_cfg_features("unix\n")).is_ok());
    }

    #[test]
    fn event_count_requires_perf_stats_actions() {
        assert_eq!(
            parse_action_count("[perf_stats] steps=1 actions=42 groups=2").unwrap(),
            42
        );
        assert!(parse_action_count("actions=42").is_err());
    }

    #[test]
    fn decimal_seconds_are_parsed_without_float_rounding() {
        assert_eq!(decimal_seconds_to_ns("0.000000001").unwrap(), 1);
        assert_eq!(
            decimal_seconds_to_ns("1500.000").unwrap(),
            1_500_000_000_000
        );
    }
}

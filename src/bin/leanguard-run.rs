use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use days::utils::trace_manifest::{self, TraceManifestV1};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    SimulateAndCheck,
    CheckOnly,
}

#[derive(Parser, Debug)]
#[command(name = "leanguard-run")]
struct Cli {
    #[arg(long)]
    config: PathBuf,

    #[arg(long, value_enum, default_value_t = Mode::SimulateAndCheck)]
    mode: Mode,

    #[arg(long, default_value = "lean/.lake/build/bin")]
    checker_dir: PathBuf,

    #[arg(long, default_value_t = false)]
    allow_nondeterministic: bool,

    /// Path to the separately built legacy engine used for trace generation.
    #[arg(long)]
    legacy_runner: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    coverage: bool,

    #[arg(long)]
    coverage_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckerStatus {
    Accept,
    Reject,
    Error,
    MissingChecker,
}

#[derive(Debug, Clone, Serialize)]
struct CheckerResult {
    checker: String,
    argv: Vec<String>,
    status: CheckerStatus,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DaysStatus {
    Ok,
    Panic,
    Error,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum TraceDiscoveryMode {
    Manifest,
    ScanFallback,
}

#[derive(Debug, Clone, Serialize)]
struct TraceDiscoverySummary {
    mode: TraceDiscoveryMode,
    traces: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeterminismStatus {
    Ok,
    RefusedMultipleThreading,
    UnknownMissingThreading,
}

#[derive(Debug, Clone, Serialize)]
struct RunSummaryV1 {
    version: u32,
    mode: String,
    config_path: String,
    log_path: String,
    determinism: DeterminismStatus,
    days: DaysStatus,
    days_error: Option<String>,
    trace_discovery: TraceDiscoverySummary,
    checker_results: Vec<CheckerResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coverage: Option<CoverageSummary>,
    accept: bool,
}

#[derive(Debug, Clone, Serialize)]
struct CoverageSummary {
    union: Vec<String>,
    per_checker: BTreeMap<String, Vec<String>>,
}

fn main() {
    let cli = Cli::parse();

    let mut summary = RunSummaryV1 {
        version: 1,
        mode: match cli.mode {
            Mode::SimulateAndCheck => "simulate-and-check".to_string(),
            Mode::CheckOnly => "check-only".to_string(),
        },
        config_path: cli.config.display().to_string(),
        log_path: String::new(),
        determinism: DeterminismStatus::Ok,
        days: DaysStatus::Skipped,
        days_error: None,
        trace_discovery: TraceDiscoverySummary {
            mode: TraceDiscoveryMode::ScanFallback,
            traces: Vec::new(),
        },
        checker_results: Vec::new(),
        coverage: None,
        accept: false,
    };

    let config_content = match fs::read_to_string(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            summary.days = DaysStatus::Error;
            summary.days_error = Some(format!("Failed to read config: {e}"));
            emit_and_exit(summary, 2);
        }
    };

    let config_toml: toml::Value = match toml::from_str(&config_content) {
        Ok(v) => v,
        Err(e) => {
            summary.days = DaysStatus::Error;
            summary.days_error = Some(format!("Failed to parse config TOML: {e}"));
            emit_and_exit(summary, 2);
        }
    };

    let threading = config_toml
        .get("threading")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    if matches!(cli.mode, Mode::SimulateAndCheck) {
        match threading.as_deref() {
            Some("multiple") if !cli.allow_nondeterministic => {
                summary.determinism = DeterminismStatus::RefusedMultipleThreading;
                summary.days = DaysStatus::Error;
                summary.days_error = Some(
                    "Refused to run with threading=\"multiple\" without --allow-nondeterministic"
                        .to_string(),
                );
                emit_and_exit(summary, 2);
            }
            None => {
                summary.determinism = DeterminismStatus::UnknownMissingThreading;
            }
            _ => {}
        }
    } else if threading.is_none() {
        summary.determinism = DeterminismStatus::UnknownMissingThreading;
    }

    let log_path = config_toml
        .get("log_path")
        .and_then(|v| v.as_str())
        .unwrap_or("./output");
    summary.log_path = log_path.to_string();
    let log_path = PathBuf::from(log_path);
    let coverage_enabled = cli.coverage || cli.coverage_dir.is_some();
    let coverage_dir = if coverage_enabled {
        let dir = cli
            .coverage_dir
            .clone()
            .unwrap_or_else(|| log_path.join("coverage"));
        if let Err(e) = fs::create_dir_all(&dir) {
            summary.days = DaysStatus::Error;
            summary.days_error = Some(format!("Failed to create coverage dir: {e}"));
            emit_and_exit(summary, 2);
        }
        Some(dir)
    } else {
        None
    };

    let required_features_hint = required_features_hint(&config_toml);

    if matches!(cli.mode, Mode::SimulateAndCheck) {
        let legacy_runner = resolve_legacy_runner(cli.legacy_runner.as_deref());
        match Command::new(&legacy_runner)
            .arg(&summary.config_path)
            .output()
        {
            Ok(output) if output.status.success() => {
                summary.days = DaysStatus::Ok;
            }
            Ok(output) if output.status.code() == Some(101) => {
                summary.days = DaysStatus::Panic;
                summary.days_error = Some("Days panicked".to_string());
            }
            Ok(output) => {
                summary.days = DaysStatus::Error;
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
                summary.days_error = Some(if stderr.is_empty() {
                    format!("legacy runner exited with status {}", output.status)
                } else {
                    stderr
                });
            }
            Err(error) => {
                summary.days = DaysStatus::Error;
                summary.days_error = Some(format!(
                    "failed to execute legacy runner `{}`: {error}",
                    legacy_runner.display()
                ));
            }
        }
    } else {
        summary.days = DaysStatus::Skipped;
    }

    if matches!(cli.mode, Mode::SimulateAndCheck) && summary.days != DaysStatus::Ok {
        if summary.days_error.is_some() && !required_features_hint.is_empty() {
            summary.days_error = Some(format!(
                "{}\nHint: you may need to build Days with {}",
                summary.days_error.take().unwrap(),
                required_features_hint
            ));
        }

        summary.trace_discovery = TraceDiscoverySummary {
            mode: TraceDiscoveryMode::ScanFallback,
            traces: Vec::new(),
        };
        summary.checker_results = Vec::new();
        summary.accept = false;
        emit_and_exit(summary, 1);
    }

    summary.trace_discovery = discover_traces(&log_path);
    if matches!(cli.mode, Mode::CheckOnly) && summary.trace_discovery.traces.is_empty() {
        summary.days = DaysStatus::Error;
        summary.days_error = Some(format!("No trace CSVs found under {}", log_path.display()));
        summary.checker_results = Vec::new();
        summary.accept = false;
        emit_and_exit(summary, 2);
    }

    let invocations = select_checkers(&log_path, &summary.trace_discovery.traces);
    for inv in invocations {
        summary
            .checker_results
            .push(run_checker(&cli.checker_dir, inv, coverage_dir.as_deref()));
    }

    summary.coverage = aggregate_coverage(&summary.checker_results);

    summary.accept = summary.days == DaysStatus::Ok || summary.days == DaysStatus::Skipped;
    if summary.accept {
        summary.accept = summary
            .checker_results
            .iter()
            .all(|r| matches!(r.status, CheckerStatus::Accept));
    }

    let exit_code = if summary.accept { 0 } else { 1 };
    emit_and_exit(summary, exit_code);
}

fn resolve_legacy_runner(configured: Option<&Path>) -> PathBuf {
    if let Some(path) = configured {
        return path.to_owned();
    }
    if let Some(path) = std::env::var_os("DAYS_LEGACY_RUNNER") {
        return PathBuf::from(path);
    }

    let executable_name = if cfg!(windows) { "days.exe" } else { "days" };
    std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|parent| parent.join(executable_name)))
        .unwrap_or_else(|| PathBuf::from(executable_name))
}

fn emit_and_exit(summary: RunSummaryV1, exit_code: i32) -> ! {
    let content = serde_json::to_string_pretty(&summary).unwrap_or_else(|e| {
        format!(
            "{{\"version\":1,\"accept\":false,\"days\":\"error\",\"days_error\":\"failed to serialize JSON: {e}\"}}"
        )
    });
    println!("{content}");
    std::process::exit(exit_code);
}

fn discover_traces(log_path: &Path) -> TraceDiscoverySummary {
    let manifest_path = trace_manifest::manifest_path(log_path);
    if let Ok(content) = fs::read_to_string(&manifest_path) {
        if let Ok(manifest) = serde_json::from_str::<TraceManifestV1>(&content) {
            if manifest.version == 1 {
                return TraceDiscoverySummary {
                    mode: TraceDiscoveryMode::Manifest,
                    traces: manifest.traces,
                };
            }
        }
    }

    TraceDiscoverySummary {
        mode: TraceDiscoveryMode::ScanFallback,
        traces: scan_for_traces(log_path),
    }
}

fn scan_for_traces(log_path: &Path) -> Vec<String> {
    let candidates = [
        "pfc_events.csv",
        "aqm_events.csv",
        "dcqcn_events.csv",
        "wfq_events.csv",
        "drr_events.csv",
        "cubic_events.csv",
        "tcp_events.csv",
    ];

    let mut traces = Vec::new();
    for filename in candidates {
        let path = log_path.join(filename);
        if let Ok(meta) = fs::metadata(&path) {
            if meta.len() > 0 {
                traces.push(filename.to_string());
            }
        }
    }
    traces
}

#[derive(Debug, Clone)]
enum CheckerInvocation {
    One {
        exe: &'static str,
        args: Vec<PathBuf>,
    },
}

fn select_checkers(log_path: &Path, traces: &[String]) -> Vec<CheckerInvocation> {
    let has = |name: &str| traces.iter().any(|t| t == name);

    let mut invocations = Vec::new();

    if has("pfc_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "pfc_check",
            args: vec![log_path.join("pfc_events.csv")],
        });
    }
    if has("aqm_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "aqm_check",
            args: vec![log_path.join("aqm_events.csv")],
        });
    }
    if has("dcqcn_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "dcqcn_check",
            args: vec![log_path.join("dcqcn_events.csv")],
        });
    }
    if has("aqm_events.csv") && has("dcqcn_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "aqm_dcqcn_check",
            args: vec![
                log_path.join("aqm_events.csv"),
                log_path.join("dcqcn_events.csv"),
            ],
        });
    }
    if has("wfq_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "wfq_check",
            args: vec![log_path.join("wfq_events.csv")],
        });
    }
    if has("drr_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "drr_check",
            args: vec![log_path.join("drr_events.csv")],
        });
    }
    if has("cubic_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "cubic_check",
            args: vec![log_path.join("cubic_events.csv")],
        });
    }
    if has("tcp_events.csv") {
        invocations.push(CheckerInvocation::One {
            exe: "tcp_check",
            args: vec![log_path.join("tcp_events.csv")],
        });
    }

    invocations
}

fn run_checker(
    checker_dir: &Path,
    inv: CheckerInvocation,
    coverage_dir: Option<&Path>,
) -> CheckerResult {
    let (exe, args) = match inv {
        CheckerInvocation::One { exe, args } => (exe, args),
    };

    let exe_path = checker_dir.join(exe);
    let mut argv = std::iter::once(exe_path.display().to_string())
        .chain(args.iter().map(|p| p.display().to_string()))
        .collect::<Vec<_>>();
    let coverage_path = coverage_dir.map(|dir| dir.join(format!("{exe}_coverage.json")));
    if let Some(path) = &coverage_path {
        argv.push("--coverage-out".to_string());
        argv.push(path.display().to_string());
    }
    let coverage_path_str = coverage_path.as_ref().map(|p| p.display().to_string());

    if !exe_path.exists() {
        return CheckerResult {
            checker: exe.to_string(),
            argv,
            status: CheckerStatus::MissingChecker,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Missing checker binary: {}", exe_path.display()),
            coverage_path: coverage_path_str,
            coverage: None,
        };
    }

    let mut cmd = Command::new(&exe_path);
    cmd.args(&args);
    if let Some(path) = &coverage_path {
        cmd.arg("--coverage-out").arg(path);
    }
    let output = cmd.output();
    match output {
        Ok(output) => {
            let exit_code = output.status.code();
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let status = match exit_code {
                Some(0) => CheckerStatus::Accept,
                Some(1) => CheckerStatus::Reject,
                _ => CheckerStatus::Error,
            };

            CheckerResult {
                checker: exe.to_string(),
                argv,
                status,
                exit_code,
                stdout,
                stderr,
                coverage_path: coverage_path_str,
                coverage: coverage_path
                    .as_ref()
                    .and_then(|path| read_coverage_points(path.as_path())),
            }
        }
        Err(e) => CheckerResult {
            checker: exe.to_string(),
            argv,
            status: CheckerStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to execute checker: {e}"),
            coverage_path: coverage_path_str,
            coverage: None,
        },
    }
}

fn read_coverage_points(path: &Path) -> Option<Vec<String>> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum CoverageFile {
        Points(Vec<String>),
        Report { cover: Vec<String> },
    }

    let content = fs::read_to_string(path).ok()?;
    if let Ok(parsed) = serde_json::from_str::<CoverageFile>(&content) {
        let mut points = match parsed {
            CoverageFile::Points(points) | CoverageFile::Report { cover: points } => points,
        };
        points.sort();
        points.dedup();
        return Some(points);
    }
    let mut points = content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    if points.is_empty() {
        return None;
    }
    points.sort();
    points.dedup();
    Some(points)
}

#[cfg(test)]
mod tests {
    use super::read_coverage_points;

    #[test]
    fn coverage_report_object_extracts_sorted_unique_coverpoints() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("coverage.json");
        std::fs::write(
            &path,
            r#"{"checker":"tcp_check","accept":true,"cover":["reno_timeout","reno_new_ack","reno_timeout"],"stats":{"rows":3,"processed_rows":3}}"#,
        )
        .expect("write coverage report");

        assert_eq!(
            read_coverage_points(&path),
            Some(vec!["reno_new_ack".to_string(), "reno_timeout".to_string()])
        );
    }
}

fn aggregate_coverage(results: &[CheckerResult]) -> Option<CoverageSummary> {
    let mut union = BTreeSet::new();
    let mut per_checker = BTreeMap::new();

    for result in results {
        if let Some(coverage) = &result.coverage {
            if coverage.is_empty() {
                continue;
            }
            let mut points = coverage.clone();
            points.sort();
            points.dedup();
            for point in &points {
                union.insert(point.clone());
            }
            per_checker.insert(result.checker.clone(), points);
        }
    }

    if union.is_empty() {
        return None;
    }

    Some(CoverageSummary {
        union: union.into_iter().collect(),
        per_checker,
    })
}

fn required_features_hint(config: &toml::Value) -> String {
    let mut features = Vec::new();

    if config
        .get("flow")
        .and_then(|v| v.as_array())
        .is_some_and(|flows| {
            flows.iter().any(|flow| {
                flow.get("flow_type")
                    .and_then(|v| v.as_str())
                    .is_some_and(|t| t.eq_ignore_ascii_case("dcqcn"))
            })
        })
    {
        features.push("dcqcn");
    }

    if config
        .get("flow_set")
        .and_then(|v| v.as_array())
        .is_some_and(|flows| {
            flows.iter().any(|flow| {
                flow.get("flow_type")
                    .and_then(|v| v.as_str())
                    .is_some_and(|t| t.eq_ignore_ascii_case("dcqcn"))
            })
        })
    {
        features.push("dcqcn");
    }

    if config
        .get("link")
        .and_then(|v| v.get("mode"))
        .and_then(|v| v.as_str())
        .is_some_and(|m| m.eq_ignore_ascii_case("pfc"))
    {
        features.push("l2_pfc");
    }

    if features.is_empty() {
        return String::new();
    }

    features.sort();
    features.dedup();

    format!("`--features {}`", features.join(","))
}

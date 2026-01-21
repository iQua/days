use clap::{Parser, ValueEnum};
use serde::Serialize;
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
    accept: bool,
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

    let required_features_hint = required_features_hint(&config_toml);

    if matches!(cli.mode, Mode::SimulateAndCheck) {
        let run_result =
            std::panic::catch_unwind(|| days::run_simulation_from_config(&summary.config_path));
        match run_result {
            Ok(Ok(())) => {
                summary.days = DaysStatus::Ok;
            }
            Ok(Err(e)) => {
                summary.days = DaysStatus::Error;
                summary.days_error = Some(e);
            }
            Err(_) => {
                summary.days = DaysStatus::Panic;
                summary.days_error = Some("Days panicked".to_string());
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
            .push(run_checker(&cli.checker_dir, inv));
    }

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

    invocations
}

fn run_checker(checker_dir: &Path, inv: CheckerInvocation) -> CheckerResult {
    let (exe, args) = match inv {
        CheckerInvocation::One { exe, args } => (exe, args),
    };

    let exe_path = checker_dir.join(exe);
    let argv = std::iter::once(exe_path.display().to_string())
        .chain(args.iter().map(|p| p.display().to_string()))
        .collect::<Vec<_>>();

    if !exe_path.exists() {
        return CheckerResult {
            checker: exe.to_string(),
            argv,
            status: CheckerStatus::MissingChecker,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Missing checker binary: {}", exe_path.display()),
        };
    }

    let output = Command::new(&exe_path).args(&args).output();
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
            }
        }
        Err(e) => CheckerResult {
            checker: exe.to_string(),
            argv,
            status: CheckerStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to execute checker: {e}"),
        },
    }
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

use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::{self, File};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use days::utils::trace_export;
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

    #[arg(long, default_value_t = false)]
    coverage: bool,

    #[arg(long)]
    coverage_dir: Option<PathBuf>,

    /// Run the TLA+/TLC trace-validation baseline (in addition to LeanGuard checkers).
    #[arg(long, default_value_t = false)]
    tlc_check: bool,

    /// Directory containing baseline `.tla` modules and `.cfg` model configs.
    #[arg(long, default_value = "tla")]
    tlc_spec_dir: PathBuf,

    /// Optional TLC runner executable. If provided, this binary is executed directly.
    ///
    /// If omitted, `leanguard-run` runs TLC via `java -cp <tlc_jar> tlc2.TLC ...`.
    #[arg(long)]
    tlc_bin: Option<PathBuf>,

    /// Path to `tla2tools.jar` (required unless `--tlc-bin` is provided).
    #[arg(long)]
    tlc_jar: Option<PathBuf>,

    /// Disable the DFS state queue optimization recommended for trace validation.
    #[arg(long, default_value_t = false)]
    tlc_no_dfs: bool,
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
#[serde(rename_all = "snake_case")]
enum TlcStatus {
    Accept,
    Reject,
    Error,
    MissingRunner,
}

#[derive(Debug, Clone, Serialize)]
struct TlcFirstFailure {
    trace_file: String,
    index: u64,
    time_ns: Option<u64>,
    event_id: Option<u64>,
    kind: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TlcResult {
    module: String,
    cfg: String,
    trace_csv: String,
    trace_ndjson: String,
    trace_tla: String,
    argv: Vec<String>,
    status: TlcStatus,
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_runtime_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diameter: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_prefix: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_failure: Option<TlcFirstFailure>,
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
    runtime_ms: Option<u128>,
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
    tlc_results: Option<Vec<TlcResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tlc_accept: Option<bool>,
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
        tlc_results: None,
        tlc_accept: None,
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
            .push(run_checker(&cli.checker_dir, inv, coverage_dir.as_deref()));
    }

    if cli.tlc_check {
        let tlc_invocations = select_tlc_specs(
            &log_path,
            &summary.trace_discovery.traces,
            &cli.tlc_spec_dir,
        );
        let mut results = Vec::new();
        for inv in tlc_invocations {
            results.push(run_tlc(&cli, &log_path, inv));
        }
        summary.tlc_accept = Some(
            results
                .iter()
                .all(|r| matches!(r.status, TlcStatus::Accept)),
        );
        summary.tlc_results = Some(results);
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

#[derive(Debug, Clone)]
struct TlcInvocation {
    module: PathBuf,
    cfg: PathBuf,
    trace_csv: PathBuf,
}

fn select_tlc_specs(log_path: &Path, traces: &[String], spec_dir: &Path) -> Vec<TlcInvocation> {
    let has = |name: &str| traces.iter().any(|t| t == name);
    let mut invocations = Vec::new();

    if has("aqm_events.csv") {
        invocations.push(TlcInvocation {
            module: spec_dir.join("AqmTrace.tla"),
            cfg: spec_dir.join("AqmTrace.cfg"),
            trace_csv: log_path.join("aqm_events.csv"),
        });
    }
    if has("pfc_events.csv") {
        invocations.push(TlcInvocation {
            module: spec_dir.join("PfcTrace.tla"),
            cfg: spec_dir.join("PfcTrace.cfg"),
            trace_csv: log_path.join("pfc_events.csv"),
        });
    }
    if has("dcqcn_events.csv") {
        invocations.push(TlcInvocation {
            module: spec_dir.join("DcqcnTrace.tla"),
            cfg: spec_dir.join("DcqcnTrace.cfg"),
            trace_csv: log_path.join("dcqcn_events.csv"),
        });
    }
    if has("wfq_events.csv") {
        invocations.push(TlcInvocation {
            module: spec_dir.join("WfqTrace.tla"),
            cfg: spec_dir.join("WfqTrace.cfg"),
            trace_csv: log_path.join("wfq_events.csv"),
        });
    }
    if has("drr_events.csv") {
        invocations.push(TlcInvocation {
            module: spec_dir.join("DrrTrace.tla"),
            cfg: spec_dir.join("DrrTrace.cfg"),
            trace_csv: log_path.join("drr_events.csv"),
        });
    }
    if has("cubic_events.csv") {
        invocations.push(TlcInvocation {
            module: spec_dir.join("CubicTrace.tla"),
            cfg: spec_dir.join("CubicTrace.cfg"),
            trace_csv: log_path.join("cubic_events.csv"),
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
            runtime_ms: None,
            coverage_path: coverage_path_str,
            coverage: None,
        };
    }

    let mut cmd = Command::new(&exe_path);
    cmd.args(&args);
    if let Some(path) = &coverage_path {
        cmd.arg("--coverage-out").arg(path);
    }
    let start = Instant::now();
    let output = cmd.output();
    let runtime_ms = start.elapsed().as_millis();
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
                runtime_ms: Some(runtime_ms),
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
            runtime_ms: Some(runtime_ms),
            coverage_path: coverage_path_str,
            coverage: None,
        },
    }
}

fn run_tlc(cli: &Cli, log_path: &Path, inv: TlcInvocation) -> TlcResult {
    let start_total = Instant::now();
    let module_str = inv.module.display().to_string();
    let cfg_str = inv.cfg.display().to_string();
    let trace_csv_str = inv.trace_csv.display().to_string();

    let trace_ndjson = trace_export::default_ndjson_output_path(&inv.trace_csv);
    let trace_ndjson_str = trace_ndjson.display().to_string();

    if let Err(e) = trace_export::export_csv_to_ndjson(&inv.trace_csv, &trace_ndjson, true) {
        return TlcResult {
            module: module_str,
            cfg: cfg_str,
            trace_csv: trace_csv_str,
            trace_ndjson: trace_ndjson_str,
            trace_tla: String::new(),
            argv: Vec::new(),
            status: TlcStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to export CSV to NDJSON: {e}"),
            runtime_ms: None,
            total_runtime_ms: Some(start_total.elapsed().as_millis()),
            diameter: None,
            matched_prefix: None,
            first_failure: None,
        };
    }

    let meta_root = log_path.join("tlc");
    let _ = fs::create_dir_all(&meta_root);
    let meta_dir = meta_root.join(
        inv.module
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("tlc"),
    );
    let _ = fs::create_dir_all(&meta_dir);

    let spec_dir = meta_dir.join("spec");
    let _ = fs::create_dir_all(&spec_dir);

    let module_file = spec_dir.join(Path::new(
        inv.module
            .file_name()
            .expect("TLC module path has no filename"),
    ));
    let cfg_file = spec_dir.join(Path::new(
        inv.cfg.file_name().expect("TLC cfg path has no filename"),
    ));
    let trace_tla = spec_dir.join("TraceData.tla");
    let trace_tla_str = trace_tla.display().to_string();

    if let Err(e) = fs::copy(&inv.module, &module_file) {
        return TlcResult {
            module: module_str,
            cfg: cfg_str,
            trace_csv: trace_csv_str,
            trace_ndjson: trace_ndjson_str,
            trace_tla: trace_tla_str,
            argv: Vec::new(),
            status: TlcStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to copy TLA module into TLC workspace: {e}"),
            runtime_ms: None,
            total_runtime_ms: Some(start_total.elapsed().as_millis()),
            diameter: None,
            matched_prefix: None,
            first_failure: None,
        };
    }
    if let Err(e) = fs::copy(&inv.cfg, &cfg_file) {
        return TlcResult {
            module: module_str,
            cfg: cfg_str,
            trace_csv: trace_csv_str,
            trace_ndjson: trace_ndjson_str,
            trace_tla: trace_tla_str,
            argv: Vec::new(),
            status: TlcStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to copy TLC config into workspace: {e}"),
            runtime_ms: None,
            total_runtime_ms: Some(start_total.elapsed().as_millis()),
            diameter: None,
            matched_prefix: None,
            first_failure: None,
        };
    }
    if let Err(e) =
        trace_export::export_csv_to_tla_trace_module(&inv.trace_csv, &trace_tla, true, "TraceData")
    {
        return TlcResult {
            module: module_str,
            cfg: cfg_str,
            trace_csv: trace_csv_str,
            trace_ndjson: trace_ndjson_str,
            trace_tla: trace_tla_str,
            argv: Vec::new(),
            status: TlcStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to export CSV to TLA trace module: {e}"),
            runtime_ms: None,
            total_runtime_ms: Some(start_total.elapsed().as_millis()),
            diameter: None,
            matched_prefix: None,
            first_failure: None,
        };
    }

    let mut cmd = if let Some(bin) = &cli.tlc_bin {
        Command::new(bin)
    } else {
        let Some(jar) = &cli.tlc_jar else {
            return TlcResult {
                module: module_str,
                cfg: cfg_str,
                trace_csv: trace_csv_str,
                trace_ndjson: trace_ndjson_str,
                trace_tla: String::new(),
                argv: Vec::new(),
                status: TlcStatus::MissingRunner,
                exit_code: None,
                stdout: String::new(),
                stderr: "Missing TLC runner: pass --tlc-bin (wrapper) or --tlc-jar (tla2tools.jar)"
                    .to_string(),
                runtime_ms: None,
                total_runtime_ms: Some(start_total.elapsed().as_millis()),
                diameter: None,
                matched_prefix: None,
                first_failure: None,
            };
        };

        let java = find_java_binary().unwrap_or_else(|| PathBuf::from("java"));
        let mut cmd = Command::new(&java);
        if !cli.tlc_no_dfs {
            cmd.arg("-Dtlc2.tool.queue.IStateQueue=StateDeque");
        }
        cmd.arg("-cp").arg(jar).arg("tlc2.TLC");
        cmd
    };

    cmd.arg("-workers").arg("1");
    cmd.arg("-metadir").arg(&meta_dir);
    cmd.arg("-config").arg(&cfg_file);
    cmd.arg(&module_file);

    let argv = std::iter::once(cmd.get_program().to_string_lossy().to_string())
        .chain(
            cmd.get_args()
                .map(|a| a.to_string_lossy().to_string())
                .collect::<Vec<_>>(),
        )
        .collect::<Vec<_>>();

    let start = Instant::now();
    let output = cmd.output();
    let runtime_ms = start.elapsed().as_millis();

    match output {
        Ok(output) => {
            let exit_code = output.status.code();
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();

            let combined = format!("{stdout}\n{stderr}");
            let diameter = parse_tlc_diameter(&combined);
            let trace_len = count_nonempty_lines(&trace_ndjson).ok();
            let matched_prefix = diameter.map(|d| d.saturating_sub(1));

            let status = match exit_code {
                Some(0) => match (trace_len, matched_prefix) {
                    (Some(len), Some(prefix)) if prefix == len => TlcStatus::Accept,
                    (Some(_), Some(_)) => TlcStatus::Reject,
                    _ => TlcStatus::Error,
                },
                _ => TlcStatus::Error,
            };

            let first_failure = match (status.clone(), trace_len, matched_prefix) {
                (TlcStatus::Reject, Some(len), Some(prefix)) if prefix < len => {
                    let idx = prefix + 1;
                    let (time_ns, event_id, kind) =
                        read_trace_fields_at_index(&trace_ndjson, idx).unwrap_or_default();
                    Some(TlcFirstFailure {
                        trace_file: inv
                            .trace_csv
                            .file_name()
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_else(|| inv.trace_csv.display().to_string()),
                        index: idx,
                        time_ns,
                        event_id,
                        kind,
                    })
                }
                _ => None,
            };

            TlcResult {
                module: module_str,
                cfg: cfg_str,
                trace_csv: trace_csv_str,
                trace_ndjson: trace_ndjson_str,
                trace_tla: trace_tla_str,
                argv,
                status,
                exit_code,
                stdout,
                stderr,
                runtime_ms: Some(runtime_ms),
                total_runtime_ms: Some(start_total.elapsed().as_millis()),
                diameter,
                matched_prefix,
                first_failure,
            }
        }
        Err(e) => TlcResult {
            module: module_str,
            cfg: cfg_str,
            trace_csv: trace_csv_str,
            trace_ndjson: trace_ndjson_str,
            trace_tla: trace_tla_str,
            argv,
            status: TlcStatus::Error,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("Failed to execute TLC runner: {e}"),
            runtime_ms: Some(runtime_ms),
            total_runtime_ms: Some(start_total.elapsed().as_millis()),
            diameter: None,
            matched_prefix: None,
            first_failure: None,
        },
    }
}

fn find_java_binary() -> Option<PathBuf> {
    if let Ok(home) = env::var("JAVA_HOME") {
        let candidate = PathBuf::from(home).join("bin").join("java");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    for candidate in [
        "/opt/homebrew/opt/openjdk/bin/java",
        "/usr/local/opt/openjdk/bin/java",
    ] {
        let path = PathBuf::from(candidate);
        if path.is_file() {
            return Some(path);
        }
    }

    find_in_path("java")
}

fn find_in_path(exe: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    for dir in env::split_paths(&path) {
        let candidate = dir.join(exe);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn parse_tlc_diameter(text: &str) -> Option<u64> {
    let mut last = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some((_, rest)) = line.split_once("Diameter:") {
            let n = rest.trim().split_whitespace().next()?;
            if let Ok(v) = n.parse::<u64>() {
                last = Some(v);
            }
            continue;
        }

        let Some(rest) = line.strip_prefix("The depth of the complete state graph search is")
        else {
            continue;
        };

        let n = rest.trim().split_whitespace().next()?.trim_end_matches('.');
        if let Ok(v) = n.parse::<u64>() {
            last = Some(v);
        }
    }
    last
}

fn count_nonempty_lines(path: &Path) -> Result<u64, String> {
    let f =
        File::open(path).map_err(|e| format!("Failed to open trace {}: {e}", path.display()))?;
    let reader = std::io::BufReader::new(f);
    let mut count = 0u64;
    for line in reader.lines() {
        let line = line.map_err(|e| format!("Failed to read trace {}: {e}", path.display()))?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

fn read_trace_fields_at_index(
    path: &Path,
    index_1_based: u64,
) -> Result<(Option<u64>, Option<u64>, Option<String>), String> {
    let f =
        File::open(path).map_err(|e| format!("Failed to open trace {}: {e}", path.display()))?;
    let reader = std::io::BufReader::new(f);
    for (i, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("Failed to read trace {}: {e}", path.display()))?;
        let i1 = (i as u64) + 1;
        if i1 != index_1_based {
            continue;
        }

        let v: serde_json::Value =
            serde_json::from_str(&line).map_err(|e| format!("Invalid NDJSON at line {i1}: {e}"))?;
        let obj = v
            .as_object()
            .ok_or_else(|| format!("NDJSON line {i1} is not an object"))?;

        let time_ns = obj.get("time_ns").and_then(|v| v.as_u64());
        let event_id = obj.get("event_id").and_then(|v| v.as_u64());
        let kind = obj
            .get("kind")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        return Ok((time_ns, event_id, kind));
    }

    Err(format!(
        "Trace {} has no line {}",
        path.display(),
        index_1_based
    ))
}

fn read_coverage_points(path: &Path) -> Option<Vec<String>> {
    let content = fs::read_to_string(path).ok()?;
    if let Ok(mut points) = serde_json::from_str::<Vec<String>>(&content) {
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

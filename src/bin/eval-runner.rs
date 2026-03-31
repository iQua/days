use std::collections::HashSet;
use std::fs::{self, File};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use clap::{Parser, ValueEnum};
use serde::Deserialize;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    #[value(name = "days_off")]
    DaysOff,
    #[value(name = "days_on")]
    DaysOn,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Self::DaysOff => "days_off",
            Self::DaysOn => "days_on",
        }
    }

    fn from_str(value: &str) -> Result<Self, String> {
        match value {
            "days_off" => Ok(Self::DaysOff),
            "days_on" => Ok(Self::DaysOn),
            _ => Err(format!(
                "unknown mode: {value} (expected one of: days_off, days_on)"
            )),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WorkloadMode {
    File,
    Generate,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum WorkloadGeneratePolicy {
    IfMissing,
    Always,
}

#[derive(Debug, Deserialize)]
struct ScenarioToml {
    name: String,
    network: String,
    workload: String,
    workload_mode: Option<String>,
    workload_generate_policy: Option<String>,
    workload_generate_cmd: Option<String>,
}

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    network: PathBuf,
    workload: PathBuf,
    workload_mode: WorkloadMode,
    workload_generate_policy: WorkloadGeneratePolicy,
    workload_generate_cmd: String,
}

#[derive(Parser, Debug)]
#[command(name = "eval-runner")]
#[command(about = "Run days evaluation scenarios from TOML")]
struct Cli {
    #[arg(long = "native-exec-mode", default_value = "nccl_compat")]
    native_exec_mode: String,

    #[arg(long = "repeats", default_value_t = 1)]
    repeats: u32,

    #[arg(long = "scenario-dir", default_value = "configs/workload/collective")]
    scenario_dir: PathBuf,

    #[arg(long = "scenario", value_delimiter = ',')]
    scenarios: Vec<String>,

    #[arg(long = "mode", value_delimiter = ',', value_enum)]
    modes: Vec<Mode>,

    #[arg(long = "target")]
    targets: Vec<String>,

    #[arg(long = "continue-on-error", default_value_t = false)]
    continue_on_error: bool,

    #[arg(long = "run-prefix", default_value = "_days")]
    run_prefix: String,

    #[arg(long = "output-csv")]
    output_csv: Option<PathBuf>,

    #[arg(long = "days-bin")]
    days_bin: Option<PathBuf>,
}

fn parse_workload_mode(value: Option<&str>, source: &Path) -> Result<WorkloadMode, String> {
    match value.unwrap_or("file") {
        "file" => Ok(WorkloadMode::File),
        "generate" => Ok(WorkloadMode::Generate),
        other => Err(format!(
            "invalid workload_mode={other} in {}",
            source.display()
        )),
    }
}

fn parse_workload_policy(
    value: Option<&str>,
    source: &Path,
) -> Result<WorkloadGeneratePolicy, String> {
    match value.unwrap_or("if_missing") {
        "if_missing" => Ok(WorkloadGeneratePolicy::IfMissing),
        "always" => Ok(WorkloadGeneratePolicy::Always),
        other => Err(format!(
            "invalid workload_generate_policy={other} in {}",
            source.display()
        )),
    }
}

fn resolve_path(root: &Path, value: &str) -> PathBuf {
    let p = PathBuf::from(value);
    if p.is_absolute() { p } else { root.join(p) }
}

fn collect_toml_files_recursively(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut stack = vec![dir.to_path_buf()];
    let mut files = Vec::new();

    while let Some(current) = stack.pop() {
        let mut entries = fs::read_dir(&current)
            .map_err(|e| format!("failed to read {}: {e}", current.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .collect::<Vec<_>>();
        entries.sort();

        for path in entries {
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().map(|ext| ext == "toml").unwrap_or(false) {
                files.push(path);
            }
        }
    }

    files.sort();
    Ok(files)
}

fn load_scenarios(root: &Path, scenario_dir: &Path) -> Result<Vec<Scenario>, String> {
    let dir = if scenario_dir.is_absolute() {
        scenario_dir.to_path_buf()
    } else {
        root.join(scenario_dir)
    };
    if !dir.is_dir() {
        return Err(format!("scenario directory not found: {}", dir.display()));
    }

    let files = collect_toml_files_recursively(&dir)?;
    if files.is_empty() {
        return Err(format!("no scenario toml files found in {}", dir.display()));
    }

    let mut scenarios = Vec::with_capacity(files.len());
    for file in files {
        let content = fs::read_to_string(&file)
            .map_err(|e| format!("failed to read {}: {e}", file.display()))?;
        let parsed: ScenarioToml = toml::from_str(&content)
            .map_err(|e| format!("failed to parse {}: {e}", file.display()))?;

        if parsed.name.trim().is_empty() {
            return Err(format!("invalid or missing name in {}", file.display()));
        }
        if parsed.network.trim().is_empty() {
            return Err(format!("invalid or missing network in {}", file.display()));
        }
        if parsed.workload.trim().is_empty() {
            return Err(format!("invalid or missing workload in {}", file.display()));
        }

        let workload_mode = parse_workload_mode(parsed.workload_mode.as_deref(), &file)?;
        let workload_generate_policy =
            parse_workload_policy(parsed.workload_generate_policy.as_deref(), &file)?;
        let workload_generate_cmd = parsed.workload_generate_cmd.unwrap_or_default();
        if workload_mode == WorkloadMode::Generate && workload_generate_cmd.trim().is_empty() {
            return Err(format!(
                "workload_generate_cmd is required when workload_mode=generate in {}",
                file.display()
            ));
        }

        scenarios.push(Scenario {
            name: parsed.name,
            network: resolve_path(root, &parsed.network),
            workload: resolve_path(root, &parsed.workload),
            workload_mode,
            workload_generate_policy,
            workload_generate_cmd,
        });
    }

    Ok(scenarios)
}

fn now_timestamp() -> String {
    let output = Command::new("date").arg("+%Y%m%d_%H%M%S").output();
    match output {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => format!("ts_{}", std::process::id()),
    }
}

fn require_file(path: &Path) -> Result<(), String> {
    if path.is_file() {
        Ok(())
    } else {
        Err(format!("required file missing: {}", path.display()))
    }
}

fn require_executable(path: &Path) -> Result<(), String> {
    if !path.is_file() {
        return Err(format!("required executable missing: {}", path.display()));
    }
    #[cfg(unix)]
    {
        let mode = fs::metadata(path)
            .map_err(|e| format!("failed to stat {}: {e}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o111 == 0 {
            return Err(format!(
                "file is not executable: {} (set DAYS_SIM_BIN or chmod +x)",
                path.display()
            ));
        }
    }
    Ok(())
}

fn run_shell_command(root: &Path, cmd: &str) -> Result<(), String> {
    let status = Command::new("bash")
        .arg("-lc")
        .arg(cmd)
        .current_dir(root)
        .status()
        .map_err(|e| format!("failed to run shell command `{cmd}`: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("shell command failed with status {status}: {cmd}"))
    }
}

fn run_training_generator(root: &Path, args: &[&str]) -> Result<(), String> {
    let manifest_path = root.join("Cargo.toml");
    let status = Command::new("cargo")
        .arg("run")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("--quiet")
        .arg("--bin")
        .arg("training-workload-generator")
        .arg("--")
        .args(args)
        .current_dir(root)
        .status()
        .map_err(|e| format!("failed to run training-workload-generator: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "training-workload-generator failed with status {status}"
        ))
    }
}

fn auto_generate_days_workload(root: &Path, scenario: &Scenario) -> Result<bool, String> {
    let generated = match scenario.name.as_str() {
        "gpt3_13b_128_a100" => {
            run_training_generator(
                root,
                &[
                    "--frame",
                    "Megatron",
                    "--gpu_type",
                    "A100",
                    "--model_name",
                    "gpt_13B",
                    "--world_size",
                    "128",
                    "--tensor_model_parallel_size",
                    "8",
                    "--pipeline_model_parallel",
                    "2",
                    "--expert_model_parallel_size",
                    "1",
                    "--global_batch",
                    "128",
                    "--micro_batch",
                    "1",
                    "--num_layers",
                    "40",
                    "--seq_length",
                    "1024",
                    "--hidden_size",
                    "2048",
                    "--vocab_size",
                    "32000",
                    "--ffn_hidden_size",
                    "8192",
                    "--num_attention_heads",
                    "16",
                    "--enable_sequence_parallel",
                    "--use_flash_attn",
                    "--result_dir",
                    "./workload/collective/training",
                ],
            )?;
            root.join("workload/collective/training/A100-gpt_13B-world_size128-tp8-pp2-ep1-gbs128-mbs1-seq1024-MOE-False-GEMM-False-flash_attn-True.txt")
        }
        "llama_65b_512_h100" => {
            run_training_generator(
                root,
                &[
                    "--frame",
                    "Megatron",
                    "--gpu_type",
                    "H100",
                    "--model_name",
                    "llama_65B",
                    "--world_size",
                    "512",
                    "--tensor_model_parallel_size",
                    "8",
                    "--pipeline_model_parallel",
                    "2",
                    "--expert_model_parallel_size",
                    "1",
                    "--global_batch",
                    "64",
                    "--micro_batch",
                    "1",
                    "--num_layers",
                    "80",
                    "--seq_length",
                    "1024",
                    "--hidden_size",
                    "3072",
                    "--vocab_size",
                    "32000",
                    "--ffn_hidden_size",
                    "12288",
                    "--num_attention_heads",
                    "24",
                    "--enable_sequence_parallel",
                    "--use_flash_attn",
                    "--result_dir",
                    "./workload/collective/training",
                ],
            )?;
            root.join("workload/collective/training/H100-llama_65B-world_size512-tp8-pp2-ep1-gbs64-mbs1-seq1024-MOE-False-GEMM-False-flash_attn-True.txt")
        }
        "gpt3_175b_1024_h100" => {
            run_training_generator(
                root,
                &[
                    "--frame",
                    "Megatron",
                    "--gpu_type",
                    "H100",
                    "--model_name",
                    "gpt_175B",
                    "--world_size",
                    "1024",
                    "--tensor_model_parallel_size",
                    "8",
                    "--pipeline_model_parallel",
                    "2",
                    "--expert_model_parallel_size",
                    "1",
                    "--global_batch",
                    "1024",
                    "--micro_batch",
                    "1",
                    "--num_layers",
                    "96",
                    "--seq_length",
                    "4096",
                    "--hidden_size",
                    "12288",
                    "--vocab_size",
                    "32000",
                    "--ffn_hidden_size",
                    "49152",
                    "--num_attention_heads",
                    "96",
                    "--enable_sequence_parallel",
                    "--use_flash_attn",
                    "--result_dir",
                    "./workload/collective/training",
                ],
            )?;
            root.join("workload/collective/training/H100-gpt_175B-world_size1024-tp8-pp2-ep1-gbs1024-mbs1-seq4096-MOE-False-GEMM-False-flash_attn-True.txt")
        }
        _ => return Ok(false),
    };

    require_file(&generated)?;
    if let Some(parent) = scenario.workload.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }
    if scenario.workload.exists() {
        fs::remove_file(&scenario.workload)
            .map_err(|e| format!("failed to remove {}: {e}", scenario.workload.display()))?;
    }
    fs::copy(&generated, &scenario.workload).map_err(|e| {
        format!(
            "failed to copy generated workload {} -> {}: {e}",
            generated.display(),
            scenario.workload.display()
        )
    })?;
    println!(
        "[gen:days] scenario={} -> {}",
        scenario.name,
        scenario.workload.display()
    );
    Ok(true)
}

fn ensure_workload_ready(root: &Path, scenario: &Scenario) -> Result<(), String> {
    match scenario.workload_mode {
        WorkloadMode::File => {
            if scenario.workload.is_file() {
                return Ok(());
            }
            if !scenario.workload_generate_cmd.trim().is_empty() {
                println!(
                    "[gen:file-missing] scenario={} workload={}",
                    scenario.name,
                    scenario.workload.display()
                );
                run_shell_command(root, &scenario.workload_generate_cmd)?;
                return require_file(&scenario.workload);
            }
            if auto_generate_days_workload(root, scenario)? {
                return require_file(&scenario.workload);
            }
            Err(format!(
                "missing workload for scenario={}: {}",
                scenario.name,
                scenario.workload.display()
            ))
        }
        WorkloadMode::Generate => {
            if scenario.workload_generate_policy == WorkloadGeneratePolicy::IfMissing
                && scenario.workload.is_file()
            {
                println!(
                    "[skip] workload exists for {}: {}",
                    scenario.name,
                    scenario.workload.display()
                );
                return Ok(());
            }
            if scenario.workload_generate_cmd.trim().is_empty() {
                return Err(format!(
                    "workload_generate_cmd is required for scenario={}",
                    scenario.name
                ));
            }
            println!(
                "[gen] scenario={} workload={}",
                scenario.name,
                scenario.workload.display()
            );
            run_shell_command(root, &scenario.workload_generate_cmd)?;
            require_file(&scenario.workload)
        }
    }
}

fn sanitize_scope_component(raw: &str) -> String {
    let normalized = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if normalized.is_empty() {
        "x".to_string()
    } else {
        normalized
    }
}

fn parse_target_spec(spec: &str) -> Result<(String, Vec<Mode>), String> {
    let (scenario, modes_part) = if let Some((s, m)) = spec.split_once(':') {
        (s, m)
    } else if let Some((s, m)) = spec.split_once('-') {
        (s, m)
    } else {
        return Err(format!(
            "invalid target spec: {spec} (expected <scenario>:<mode>/<mode>)"
        ));
    };
    if scenario.is_empty() || modes_part.is_empty() {
        return Err(format!("invalid target spec: {spec}"));
    }
    let modes = modes_part
        .replace(',', "/")
        .split('/')
        .filter(|s| !s.is_empty())
        .map(Mode::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    if modes.is_empty() {
        return Err(format!("target has no modes: {spec}"));
    }
    Ok((scenario.to_string(), modes))
}

fn extract_sim_all_passes_tick(log_path: &Path) -> String {
    let content = match fs::read_to_string(log_path) {
        Ok(v) => v,
        Err(_) => return String::new(),
    };
    let mut result = String::new();
    let marker = "all passes finished at time:";
    for line in content.lines() {
        if let Some(idx) = line.find(marker) {
            let suffix = &line[idx + marker.len()..];
            let digits = suffix
                .chars()
                .skip_while(|c| !c.is_ascii_digit())
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>();
            if !digits.is_empty() {
                result = digits;
            }
        }
    }
    result
}

fn extract_endtoend_total_time(csv_path: &Path) -> String {
    let content = match fs::read_to_string(csv_path) {
        Ok(v) => v,
        Err(_) => return String::new(),
    };
    let marker = "total time,";
    let mut result = String::new();
    for line in content.lines() {
        if let Some(idx) = line.find(marker) {
            let suffix = &line[idx + marker.len()..];
            let value = suffix
                .split(|c: char| c == ',' || c.is_ascii_whitespace())
                .find(|token| !token.is_empty())
                .unwrap_or("");
            if !value.is_empty() {
                result = value.to_string();
            }
        }
    }
    result
}

fn run_days_mode(
    days_bin: &Path,
    native_exec_mode: &str,
    mode: Mode,
    scenario: &Scenario,
    repeat: u32,
    run_prefix: &str,
    run_ts: &str,
    results_dir: &Path,
    writer: &mut csv::Writer<File>,
) -> Result<bool, String> {
    let run_name = format!(
        "{}_{}_{}_r{}_{}",
        run_prefix,
        scenario.name,
        mode.as_str(),
        repeat,
        run_ts
    );
    let log_path = results_dir.join(format!("{run_name}.profile.log"));
    let out_csv = results_dir.join(format!("{run_name}EndToEnd.csv"));

    let log_file = File::create(&log_path)
        .map_err(|e| format!("failed to create {}: {e}", log_path.display()))?;
    let log_file_err = log_file
        .try_clone()
        .map_err(|e| format!("failed to clone log handle: {e}"))?;

    let mut cmd = Command::new(days_bin);
    cmd.arg("-n")
        .arg(&scenario.network)
        .arg("-w")
        .arg(&scenario.workload)
        .arg("-r")
        .arg(&run_name)
        .env("AS_SEND_LAT", "0")
        .env("AS_NVLS_ENABLE", "1")
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err));

    let command_desc = match mode {
        Mode::DaysOff => {
            cmd.env_remove("DAYS_COLLECTIVE_MODE");
            cmd.env_remove("DAYS_ALLREDUCE_EXEC_MODE");
            format!(
                "AS_SEND_LAT=0 AS_NVLS_ENABLE=1 env -u DAYS_COLLECTIVE_MODE -u DAYS_ALLREDUCE_EXEC_MODE {} -n {} -w {} -r {}",
                days_bin.display(),
                scenario.network.display(),
                scenario.workload.display(),
                run_name
            )
        }
        Mode::DaysOn => {
            cmd.env("DAYS_COLLECTIVE_MODE", "native_allreduce");
            cmd.env("DAYS_ALLREDUCE_EXEC_MODE", native_exec_mode);
            format!(
                "AS_SEND_LAT=0 AS_NVLS_ENABLE=1 DAYS_COLLECTIVE_MODE=native_allreduce DAYS_ALLREDUCE_EXEC_MODE={} {} -n {} -w {} -r {}",
                native_exec_mode,
                days_bin.display(),
                scenario.network.display(),
                scenario.workload.display(),
                run_name
            )
        }
    };

    let start = Instant::now();
    let status = cmd
        .status()
        .map_err(|e| format!("failed to execute {}: {e}", days_bin.display()))?;
    let wall = format!("{:.6}", start.elapsed().as_secs_f64());

    if !out_csv.exists() {
        File::create(&out_csv)
            .map_err(|e| format!("failed to create {}: {e}", out_csv.display()))?;
    }
    let sim_all_passes_tick = extract_sim_all_passes_tick(&log_path);
    let endtoend_total_time = extract_endtoend_total_time(&out_csv);
    let mut command_cell = command_desc;
    if !status.success() {
        command_cell = format!("{command_cell} [exit={}]", status.code().unwrap_or(-1));
    }

    writer
        .write_record([
            scenario.name.as_str(),
            &repeat.to_string(),
            mode.as_str(),
            run_ts,
            &run_name,
            &wall,
            "NA",
            &sim_all_passes_tick,
            &endtoend_total_time,
            &log_path.display().to_string(),
            &out_csv.display().to_string(),
            &command_cell,
        ])
        .map_err(|e| format!("failed to write summary csv: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("failed to flush summary csv: {e}"))?;

    if status.success() {
        println!("[done] {run_name}");
        Ok(true)
    } else {
        println!(
            "[fail] {run_name} exit={} log={}",
            status.code().unwrap_or(-1),
            log_path.display()
        );
        Ok(false)
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let run_ts = now_timestamp();
    let results_dir = root.join("results/evaluation");
    fs::create_dir_all(&results_dir)
        .map_err(|e| format!("failed to create {}: {e}", results_dir.display()))?;

    let scenarios = load_scenarios(&root, &cli.scenario_dir)?;
    let all_modes = [Mode::DaysOff, Mode::DaysOn];

    if !cli.targets.is_empty() && !cli.scenarios.is_empty() {
        return Err("--target and --scenario cannot be used together".to_string());
    }
    if !cli.targets.is_empty() && !cli.modes.is_empty() {
        return Err("--target and --mode cannot be used together".to_string());
    }

    let selected_modes = if cli.modes.is_empty() {
        all_modes.to_vec()
    } else {
        cli.modes.clone()
    };
    let selected_mode_set = selected_modes
        .iter()
        .map(|m| m.as_str())
        .collect::<Vec<_>>();

    let mut tasks = Vec::<(usize, Mode)>::new();
    let mut selected_scenario_names = Vec::<String>::new();

    if !cli.targets.is_empty() {
        for target in &cli.targets {
            let (scenario_name, modes) = parse_target_spec(target)?;
            let idx = scenarios
                .iter()
                .position(|s| s.name == scenario_name)
                .ok_or_else(|| {
                    format!(
                        "unknown scenario in target: {} (available: {})",
                        scenario_name,
                        scenarios
                            .iter()
                            .map(|s| s.name.as_str())
                            .collect::<Vec<_>>()
                            .join(" ")
                    )
                })?;
            for mode in modes {
                tasks.push((idx, mode));
            }
        }
    } else {
        let mut selected_indexes = Vec::<usize>::new();
        if cli.scenarios.is_empty() {
            selected_indexes.extend(0..scenarios.len());
            selected_scenario_names.extend(scenarios.iter().map(|s| s.name.clone()));
        } else {
            for wanted in &cli.scenarios {
                let idx = scenarios
                    .iter()
                    .position(|s| s.name == *wanted)
                    .ok_or_else(|| {
                        format!(
                            "unknown scenario: {} (available: {})",
                            wanted,
                            scenarios
                                .iter()
                                .map(|s| s.name.as_str())
                                .collect::<Vec<_>>()
                                .join(" ")
                        )
                    })?;
                selected_indexes.push(idx);
                selected_scenario_names.push(wanted.clone());
            }
        }
        for idx in selected_indexes {
            for mode in &selected_modes {
                tasks.push((idx, *mode));
            }
        }
    }

    let csv_scope_suffix = if !cli.targets.is_empty() {
        let mut scope = String::from("target");
        for t in &cli.targets {
            scope.push('_');
            scope.push_str(&sanitize_scope_component(t));
        }
        scope
    } else if !cli.scenarios.is_empty() {
        let mut scope = String::from("scenario");
        for s in &selected_scenario_names {
            scope.push('_');
            scope.push_str(&sanitize_scope_component(s));
        }
        scope
    } else if !cli.modes.is_empty() {
        let mut scope = String::from("mode");
        for m in &selected_mode_set {
            scope.push('_');
            scope.push_str(m);
        }
        scope
    } else {
        "all_days".to_string()
    };
    let csv_scope_suffix = if csv_scope_suffix.len() > 120 {
        csv_scope_suffix[..120].to_string()
    } else {
        csv_scope_suffix
    };
    let output_csv = cli.output_csv.unwrap_or_else(|| {
        results_dir.join(format!("profile_days_{csv_scope_suffix}_{run_ts}.csv"))
    });

    let days_bin = cli
        .days_bin
        .or_else(|| std::env::var("DAYS_SIM_BIN").ok().map(PathBuf::from))
        .unwrap_or_else(|| root.join("bin/SimAI_days"));
    require_executable(&days_bin)?;

    let mut check_indexes = HashSet::<usize>::new();
    for (idx, _) in &tasks {
        check_indexes.insert(*idx);
    }
    for idx in check_indexes {
        let scenario = &scenarios[idx];
        require_file(&scenario.network)?;
        ensure_workload_ready(&root, scenario)?;
    }

    let output_file = File::create(&output_csv)
        .map_err(|e| format!("failed to create {}: {e}", output_csv.display()))?;
    let mut writer = csv::Writer::from_writer(output_file);
    writer
        .write_record([
            "scenario",
            "repeat",
            "mode",
            "timestamp",
            "run_name",
            "wall_time_sec",
            "peak_rss_kb",
            "sim_all_passes_tick",
            "endtoend_total_time",
            "log_path",
            "endtoend_csv",
            "command",
        ])
        .map_err(|e| format!("failed to write csv header: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("failed to flush csv header: {e}"))?;

    if !cli.targets.is_empty() {
        println!("running targets: {}", cli.targets.join(" "));
    } else {
        println!(
            "running scenarios: {}",
            if selected_scenario_names.is_empty() {
                scenarios
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            } else {
                selected_scenario_names.join(" ")
            }
        );
        println!(
            "modes: {}",
            selected_modes
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        );
    }
    println!(
        "repeats={}, native_exec_mode={}, timestamp={}",
        cli.repeats, cli.native_exec_mode, run_ts
    );

    let mut failed_runs = 0usize;
    for repeat in 1..=cli.repeats {
        for (idx, mode) in &tasks {
            let scenario = &scenarios[*idx];
            println!(
                "[run] scenario={} mode={} repeat={}",
                scenario.name,
                mode.as_str(),
                repeat
            );
            let ok = run_days_mode(
                &days_bin,
                &cli.native_exec_mode,
                *mode,
                scenario,
                repeat,
                &cli.run_prefix,
                &run_ts,
                &results_dir,
                &mut writer,
            )?;
            if !ok {
                failed_runs += 1;
                if !cli.continue_on_error {
                    return Err(
                        "stopping due to failure (use --continue-on-error to keep going)"
                            .to_string(),
                    );
                }
            }
        }
    }

    println!();
    println!("Done.");
    println!("summary csv: {}", output_csv.display());
    println!("logs/results: {}", results_dir.display());
    let scenario_dir_display = if cli.scenario_dir.is_absolute() {
        cli.scenario_dir
    } else {
        root.join(cli.scenario_dir)
    };
    println!("scenario_dir: {}", scenario_dir_display.display());
    if failed_runs != 0 {
        return Err(format!("failed tasks: {failed_runs}"));
    }
    Ok(())
}

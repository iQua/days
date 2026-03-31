use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Parser, ValueEnum};
use days::p2p_workload::{ExportOptions, parse_flow_dump_tsv, render_days_config_toml};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum WorkloadClass {
    Training,
    Inference,
}

#[derive(Parser, Debug)]
#[command(name = "fct-to-days")]
#[command(about = "Convert FCT records into days TSV/TOML inputs")]
struct Cli {
    #[arg(long = "input-fct")]
    input_fct: PathBuf,

    #[arg(long = "workload-name")]
    workload_name: String,

    #[arg(long = "class", value_enum)]
    class: Option<WorkloadClass>,

    #[arg(long = "output-tsv")]
    output_tsv: Option<PathBuf>,

    #[arg(long = "output-toml")]
    output_toml: Option<PathBuf>,

    #[arg(long = "output-fct")]
    output_fct: Option<PathBuf>,

    #[arg(long = "seed", default_value_t = 1000)]
    seed: usize,

    #[arg(long = "duration", default_value_t = 1500.0)]
    duration: f64,

    #[arg(long = "log-path", default_value = "logs/p2p_from_fct")]
    log_path: String,

    #[arg(long = "port-rate", default_value_t = 8000.0)]
    port_rate: f64,

    #[arg(long = "capacity", default_value_t = 100)]
    capacity: usize,

    #[arg(long = "packet-size", default_value_t = 4096)]
    packet_size: i64,

    #[arg(long = "run-days", default_value_t = false)]
    run_days: bool,
}

fn class_dir(class: WorkloadClass) -> &'static str {
    match class {
        WorkloadClass::Training => "training",
        WorkloadClass::Inference => "inference",
    }
}

fn sanitize_workload_name(raw: &str) -> String {
    let name = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if name.is_empty() {
        "workload".to_string()
    } else {
        name
    }
}

fn infer_class_from_workload_name(name: &str) -> WorkloadClass {
    if name.starts_with("infer_") {
        WorkloadClass::Inference
    } else {
        WorkloadClass::Training
    }
}

fn default_output_tsv(class: WorkloadClass, workload_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("workload/p2p")
        .join(class_dir(class))
        .join(format!("{workload_name}.tsv"))
}

fn default_output_toml(class: WorkloadClass, workload_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/workload/p2p")
        .join(class_dir(class))
        .join(format!("{workload_name}.toml"))
}

fn default_output_fct(class: WorkloadClass, workload_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("workload/p2p")
        .join(class_dir(class))
        .join(format!("{workload_name}_fct.txt"))
}

fn parse_hex_token(token: &str) -> Option<u64> {
    let t = token.trim();
    if t.is_empty() {
        return None;
    }
    let hex = t
        .strip_prefix("0x")
        .or_else(|| t.strip_prefix("0X"))
        .unwrap_or(t);
    u64::from_str_radix(hex, 16).ok()
}

fn decode_host(token: &str) -> Option<usize> {
    let raw = parse_hex_token(token)?;
    Some(((raw >> 8) & 0xffff) as usize)
}

fn build_tsv_from_fct(content: &str) -> String {
    let mut out = String::from(
        "flow_key_channel\tflow_key_idx\tflow_id\tsrc\tdst\tflow_size\tchannel_id\tchunk_id\tchunk_count\tconn_type\tprev\tparent\tchild\n",
    );
    let mut flow_id = 0usize;

    for line in content.lines() {
        let row = line.split_whitespace().collect::<Vec<_>>();
        if row.len() < 5 {
            continue;
        }
        let Some(src) = decode_host(row[0]) else {
            continue;
        };
        let Some(dst) = decode_host(row[1]) else {
            continue;
        };
        let Ok(bytes) = row[4].parse::<usize>() else {
            continue;
        };
        if bytes == 0 {
            continue;
        }

        out.push_str(&format!(
            "0\t0\t{flow_id}\t{src}\t{dst}\t{bytes}\t0\t0\t1\tx\t\t-1\t\n"
        ));
        flow_id += 1;
    }
    out
}

fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
    }
    Ok(())
}

fn run_days_config(root: &Path, config: &Path) -> Result<(), String> {
    let status = Command::new("cargo")
        .arg("run")
        .arg("--manifest-path")
        .arg(root.join("Cargo.toml"))
        .arg("--bin")
        .arg("days")
        .arg("--")
        .arg(config)
        .status()
        .map_err(|e| format!("failed to run days with {}: {e}", config.display()))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "days execution failed for {} with status {}",
            config.display(),
            status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string())
        ))
    }
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(&cli) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<(), String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let raw_workload_name = cli.workload_name.clone();
    let workload_name = sanitize_workload_name(&raw_workload_name);
    let class = cli
        .class
        .unwrap_or_else(|| infer_class_from_workload_name(&workload_name));
    let output_tsv = cli
        .output_tsv
        .clone()
        .unwrap_or_else(|| default_output_tsv(class, &workload_name));
    let output_toml = cli
        .output_toml
        .clone()
        .unwrap_or_else(|| default_output_toml(class, &workload_name));
    let output_fct = cli
        .output_fct
        .clone()
        .unwrap_or_else(|| default_output_fct(class, &workload_name));

    if !cli.input_fct.is_file() {
        return Err(format!("input fct not found: {}", cli.input_fct.display()));
    }

    let content = fs::read_to_string(&cli.input_fct)
        .map_err(|e| format!("failed to read {}: {e}", cli.input_fct.display()))?;
    let tsv = build_tsv_from_fct(&content);
    let flows = parse_flow_dump_tsv(&tsv)?;
    let toml = render_days_config_toml(
        &flows,
        &ExportOptions {
            seed: cli.seed,
            duration: cli.duration,
            log_path: cli.log_path.clone(),
            port_rate: cli.port_rate,
            capacity: cli.capacity,
            packet_size: cli.packet_size,
        },
    )?;

    ensure_parent_dir(&output_tsv)?;
    ensure_parent_dir(&output_toml)?;
    ensure_parent_dir(&output_fct)?;

    fs::write(&output_tsv, tsv)
        .map_err(|e| format!("failed to write {}: {e}", output_tsv.display()))?;
    fs::write(&output_toml, toml)
        .map_err(|e| format!("failed to write {}: {e}", output_toml.display()))?;
    fs::copy(&cli.input_fct, &output_fct).map_err(|e| {
        format!(
            "failed to copy fct {} -> {}: {e}",
            cli.input_fct.display(),
            output_fct.display()
        )
    })?;

    println!("workload_name: {workload_name}");
    println!("class: {}", class_dir(class));
    println!("copied fct: {}", output_fct.display());
    println!("converted fct -> tsv: {}", output_tsv.display());
    println!("converted tsv -> toml: {}", output_toml.display());

    if cli.run_days {
        run_days_config(&root, &output_toml)?;
        println!("days run completed with config: {}", output_toml.display());
    }
    Ok(())
}

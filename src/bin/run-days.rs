use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Parser, ValueEnum};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum BuildProfile {
    Release,
    Debug,
}

#[derive(Parser, Debug)]
#[command(name = "run-days")]
#[command(about = "Run days evaluation groups with native TOML configs")]
struct Cli {
    #[arg(long = "config")]
    configs: Vec<PathBuf>,

    #[arg(long = "profile", value_enum, default_value_t = BuildProfile::Release)]
    profile: BuildProfile,

    #[arg(long = "continue-on-error", default_value_t = false)]
    continue_on_error: bool,
}

fn default_configs(root: &Path) -> Vec<PathBuf> {
    vec![
        root.join("configs/workload/collective/training/gpt3_13b_128_a100.toml"),
        root.join("configs/workload/collective/training/llama_65b_512_h100.toml"),
        root.join("configs/workload/collective/training/gpt3_175b_1024_h100.toml"),
        root.join("configs/workload/collective/inference/infer_prefill_13b_128_a100.toml"),
        root.join("configs/workload/collective/inference/infer_decode_13b_128_a100.toml"),
        root.join("configs/workload/collective/inference/infer_longctx_prefill_13b_128_a100.toml"),
    ]
}

fn resolve_configs(root: &Path, cli: &Cli) -> Vec<PathBuf> {
    if cli.configs.is_empty() {
        return default_configs(root);
    }
    cli.configs
        .iter()
        .map(|p| {
            if p.is_absolute() {
                p.clone()
            } else {
                root.join(p)
            }
        })
        .collect()
}

fn run_one(root: &Path, config: &Path, profile: BuildProfile) -> Result<(), String> {
    if !config.is_file() {
        return Err(format!("missing config: {}", config.display()));
    }

    let manifest_path = root.join("Cargo.toml");
    let mut cmd = Command::new("cargo");
    cmd.arg("run").arg("--manifest-path").arg(&manifest_path);
    if profile == BuildProfile::Release {
        cmd.arg("--release");
    }
    cmd.arg("--bin").arg("days").arg("--").arg(config);

    let status = cmd
        .status()
        .map_err(|e| format!("failed to execute cargo for {}: {e}", config.display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "days run failed for {} with status {}",
            config.display(),
            status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string())
        ))
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
    let configs = resolve_configs(&root, &cli);
    let total = configs.len();
    let mut failed = 0usize;

    for (idx, config) in configs.iter().enumerate() {
        println!("[{}/{}] running {}", idx + 1, total, config.display());
        if let Err(err) = run_one(&root, config, cli.profile) {
            eprintln!("[fail] {err}");
            failed += 1;
            if !cli.continue_on_error {
                return Err("stopping on first failure (use --continue-on-error)".to_string());
            }
        }
    }

    if failed == 0 {
        println!("all runs completed.");
        Ok(())
    } else {
        Err(format!("completed with failures: {failed}"))
    }
}

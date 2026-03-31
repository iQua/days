use clap::{Parser, ValueEnum};
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CollectiveModeArg {
    Off,
    On,
}

#[derive(Parser, Debug)]
#[command(name = "days-txt-runner")]
#[command(about = "Run legacy -n/-w txt inputs via SimAI_days from a cargo entrypoint")]
struct Cli {
    #[arg(long = "network")]
    network: PathBuf,

    #[arg(long = "workload")]
    workload: PathBuf,

    #[arg(long = "run-name")]
    run_name: String,

    #[arg(long = "collective-mode", value_enum, default_value_t = CollectiveModeArg::Off)]
    collective_mode: CollectiveModeArg,

    #[arg(long = "native-exec-mode", default_value = "nccl_compat")]
    native_exec_mode: String,

    #[arg(long = "simai-days-bin")]
    simai_days_bin: Option<PathBuf>,
}

fn default_simai_days_bin() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.join("..")
        .join("daytone-experiments")
        .join("bin")
        .join("SimAI_days")
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), String> {
    if !cli.network.is_file() {
        return Err(format!(
            "network file not found: {}",
            cli.network.to_string_lossy()
        ));
    }
    if !cli.workload.is_file() {
        return Err(format!(
            "workload file not found: {}",
            cli.workload.to_string_lossy()
        ));
    }

    let simai_bin = cli
        .simai_days_bin
        .or_else(|| std::env::var("SIMAI_DAYS_BIN").ok().map(PathBuf::from))
        .unwrap_or_else(default_simai_days_bin);
    if !simai_bin.is_file() {
        return Err(format!(
            "SimAI_days not found: {} (set --simai-days-bin or SIMAI_DAYS_BIN)",
            simai_bin.to_string_lossy()
        ));
    }

    let mut cmd = Command::new(&simai_bin);
    cmd.arg("-n")
        .arg(&cli.network)
        .arg("-w")
        .arg(&cli.workload)
        .arg("-r")
        .arg(&cli.run_name)
        .env("AS_SEND_LAT", "0")
        .env("AS_NVLS_ENABLE", "1");

    match cli.collective_mode {
        CollectiveModeArg::Off => {
            cmd.env_remove("DAYS_COLLECTIVE_MODE");
            cmd.env_remove("DAYS_ALLREDUCE_EXEC_MODE");
        }
        CollectiveModeArg::On => {
            cmd.env("DAYS_COLLECTIVE_MODE", "native_allreduce");
            cmd.env("DAYS_ALLREDUCE_EXEC_MODE", &cli.native_exec_mode);
        }
    }

    let status = cmd
        .status()
        .map_err(|e| format!("failed to start {}: {e}", simai_bin.to_string_lossy()))?;

    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "SimAI_days exited with status {}",
            status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string())
        ))
    }
}

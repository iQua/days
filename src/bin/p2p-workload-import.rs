use std::fs;
use std::path::PathBuf;

use clap::Parser;
use days::p2p_workload::{ExportOptions, parse_flow_dump_tsv, render_days_config_toml};

#[derive(Parser, Debug)]
#[command(name = "p2p-workload-import")]
#[command(about = "Convert p2p flow dump files into days TOML configs")]
struct Cli {
    #[arg(long = "input")]
    input: PathBuf,

    #[arg(long = "output")]
    output: PathBuf,

    #[arg(long = "seed", default_value_t = 1000)]
    seed: usize,

    #[arg(long = "duration", default_value_t = 1500.0)]
    duration: f64,

    #[arg(long = "log_path", default_value = "logs/p2p_import")]
    log_path: String,

    #[arg(long = "port_rate", default_value_t = 8000.0)]
    port_rate: f64,

    #[arg(long = "capacity", default_value_t = 100)]
    capacity: usize,

    #[arg(long = "packet_size", default_value_t = 4096)]
    packet_size: i64,
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(&cli) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run(cli: &Cli) -> Result<(), String> {
    let content = fs::read_to_string(&cli.input)
        .map_err(|e| format!("failed to read {}: {e}", cli.input.display()))?;
    let flows = parse_flow_dump_tsv(&content)?;
    let output = render_days_config_toml(
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

    if let Some(parent) = cli.output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
        }
    }
    fs::write(&cli.output, output)
        .map_err(|e| format!("failed to write {}: {e}", cli.output.display()))?;

    println!("config written to: {}", cli.output.display());
    Ok(())
}

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(about = "Repository tooling for the Days Executor program")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Audit and reproduce every declared phase.
    AllPhases {
        /// Repository root; discovered by walking upward when omitted.
        #[arg(long)]
        repo_root: Option<PathBuf>,
        /// Permit one network-enabled cargo metadata retry per phase audit.
        #[arg(long)]
        allow_network: bool,
    },
    /// Audit one phase's metadata, evidence, and repository policy.
    PhaseAudit {
        /// Phase identifier such as P01.
        phase: String,
        /// Repository root; discovered by walking upward when omitted.
        #[arg(long)]
        repo_root: Option<PathBuf>,
        /// Permit one network-enabled cargo metadata retry.
        #[arg(long)]
        allow_network: bool,
    },
    /// Reproduce the commands declared for the host platform.
    Reproduce {
        /// Phase identifier such as P01.
        #[arg(long)]
        phase: String,
        /// Repository root; discovered by walking upward when omitted.
        #[arg(long)]
        repo_root: Option<PathBuf>,
    },
    /// Print or rewrite the direct-dependency and license baseline.
    DependencyBaseline {
        /// Rewrite docs/days-executor/dependency-baseline.toml.
        #[arg(long)]
        write: bool,
        /// Repository root; discovered by walking upward when omitted.
        #[arg(long)]
        repo_root: Option<PathBuf>,
    },
    /// Collect the frozen P01 Nexosim correctness and performance baseline.
    NexosimBaseline {
        #[command(subcommand)]
        command: NexosimBaselineCommand,
    },
}

#[derive(Debug, Subcommand)]
enum NexosimBaselineCommand {
    /// Exercise the runner's parsers without building or running Days.
    SelfTest,
    /// Run the frozen method and write new versioned artifacts.
    Collect {
        /// New output directory below the repository root.
        #[arg(long)]
        output_dir: PathBuf,
        /// Repository root; discovered by walking upward when omitted.
        #[arg(long)]
        repo_root: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("xtask: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    let current = std::env::current_dir()?;
    match cli.command {
        Command::AllPhases {
            repo_root,
            allow_network,
        } => {
            let root = repo_root.map_or_else(|| xtask::discover_repo_root(&current), Ok)?;
            let mut passed = true;
            for phase in xtask::audit::phase_ids(&root)? {
                let audit = xtask::audit::phase_audit(&root, &phase, allow_network);
                for diagnostic in &audit.diagnostics {
                    println!("{diagnostic}");
                }
                passed &= !audit.has_errors();

                let reproduction = xtask::reproduce::reproduce(&root, &phase);
                for diagnostic in &reproduction.diagnostics {
                    println!("{diagnostic}");
                }
                passed &= !reproduction.has_errors();
            }
            Ok(passed)
        }
        Command::PhaseAudit {
            phase,
            repo_root,
            allow_network,
        } => {
            let root = repo_root.map_or_else(|| xtask::discover_repo_root(&current), Ok)?;
            let report = xtask::audit::phase_audit(&root, &phase, allow_network);
            for diagnostic in &report.diagnostics {
                println!("{diagnostic}");
            }
            Ok(!report.has_errors())
        }
        Command::Reproduce { phase, repo_root } => {
            let root = repo_root.map_or_else(|| xtask::discover_repo_root(&current), Ok)?;
            let report = xtask::reproduce::reproduce(&root, &phase);
            for diagnostic in &report.diagnostics {
                println!("{diagnostic}");
            }
            Ok(!report.has_errors())
        }
        Command::DependencyBaseline { write, repo_root } => {
            let root = repo_root.map_or_else(|| xtask::discover_repo_root(&current), Ok)?;
            let rendered = xtask::dependency_baseline::render(&root)?;
            if write {
                xtask::dependency_baseline::write(&root, &rendered)?;
            } else {
                print!("{rendered}");
            }
            Ok(true)
        }
        Command::NexosimBaseline { command } => match command {
            NexosimBaselineCommand::SelfTest => {
                let root = xtask::discover_repo_root(&current)?;
                xtask::baseline::self_test(&root)?;
                Ok(true)
            }
            NexosimBaselineCommand::Collect {
                output_dir,
                repo_root,
            } => {
                let root = repo_root.map_or_else(|| xtask::discover_repo_root(&current), Ok)?;
                let output = if output_dir.is_absolute() {
                    output_dir
                } else {
                    root.join(output_dir)
                };
                xtask::baseline::collect(&root, &output)?;
                Ok(true)
            }
        },
    }
}

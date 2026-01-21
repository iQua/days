use clap::{Parser, Subcommand};
use std::path::PathBuf;

use days::utils::testgen::{TestGenOptions, fuzz, minimize, replay, seed_index};

#[derive(Parser, Debug)]
#[command(name = "leanguard-testgen")]
struct Cli {
    #[arg(long, default_value = "leanguard_corpus")]
    corpus_root: PathBuf,

    #[arg(long, default_value = "lean/.lake/build/bin")]
    checker_dir: PathBuf,

    #[arg(long)]
    leanguard_run: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    allow_nondeterministic: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    SeedIndex {
        seeds_src: PathBuf,
    },
    Fuzz {
        #[arg(long)]
        budget: usize,
        #[arg(long)]
        rng_seed: Option<u64>,
    },
    Replay {
        case_dir: PathBuf,
    },
    Minimize {
        case_dir: PathBuf,
        #[arg(long, default_value_t = 25)]
        max_iters: usize,
    },
}

fn main() {
    let cli = Cli::parse();
    let opts = TestGenOptions {
        corpus_root: cli.corpus_root,
        checker_dir: cli.checker_dir,
        leanguard_run: cli.leanguard_run,
        allow_nondeterministic: cli.allow_nondeterministic,
    };

    let result = match cli.command {
        Command::SeedIndex { seeds_src } => {
            seed_index(&opts, &seeds_src).map(|summary| serde_json::to_string_pretty(&summary))
        }
        Command::Fuzz { budget, rng_seed } => {
            fuzz(&opts, budget, rng_seed).map(|summary| serde_json::to_string_pretty(&summary))
        }
        Command::Replay { case_dir } => {
            replay(&opts, &case_dir).map(|summary| serde_json::to_string_pretty(&summary))
        }
        Command::Minimize {
            case_dir,
            max_iters,
        } => minimize(&opts, &case_dir, max_iters)
            .map(|summary| serde_json::to_string_pretty(&summary)),
    };

    match result {
        Ok(Ok(json)) => {
            println!("{json}");
        }
        Ok(Err(e)) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to serialize JSON: {e}");
            std::process::exit(1);
        }
    }
}

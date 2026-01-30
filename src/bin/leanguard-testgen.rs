use clap::{Parser, Subcommand};
use std::path::PathBuf;

use days::utils::testgen::{
    CampaignArgs, TestGenOptions, campaign, fuzz, minimize, replay, seed_index,
};

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

    /// Also run a TLC-based trace-validation baseline via leanguard-run.
    #[arg(long, default_value_t = false)]
    tlc_check: bool,

    /// If set, require TLC baseline acceptance in addition to LeanGuard checkers.
    #[arg(long, default_value_t = false)]
    require_tlc_accept: bool,

    /// Directory containing baseline `.tla` modules and `.cfg` model configs.
    #[arg(long, default_value = "tla")]
    tlc_spec_dir: PathBuf,

    /// Optional TLC runner executable. If provided, this binary is executed directly.
    ///
    /// If omitted, leanguard-run runs TLC via `java -cp <tlc_jar> tlc2.TLC ...`.
    #[arg(long)]
    tlc_bin: Option<PathBuf>,

    /// Path to `tla2tools.jar` (required unless `--tlc-bin` is provided).
    #[arg(long)]
    tlc_jar: Option<PathBuf>,

    /// Disable the DFS state queue optimization recommended for trace validation.
    #[arg(long, default_value_t = false)]
    tlc_no_dfs: bool,

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
    Campaign {
        #[arg(long)]
        protocol: String,
        #[arg(long)]
        budget: usize,
        #[arg(long)]
        rng_seed: Option<u64>,
        #[arg(long)]
        goal: Option<String>,
        #[arg(long)]
        max_calibration_iters: Option<usize>,
        #[arg(long)]
        seed_filter: Option<String>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        use_trace_signature: bool,
    },
}

fn main() {
    let cli = Cli::parse();
    let opts = TestGenOptions {
        corpus_root: cli.corpus_root,
        checker_dir: cli.checker_dir,
        leanguard_run: cli.leanguard_run,
        allow_nondeterministic: cli.allow_nondeterministic,
        tlc_check: cli.tlc_check,
        require_tlc_accept: cli.require_tlc_accept,
        tlc_spec_dir: cli.tlc_spec_dir,
        tlc_bin: cli.tlc_bin,
        tlc_jar: cli.tlc_jar,
        tlc_no_dfs: cli.tlc_no_dfs,
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
        Command::Campaign {
            protocol,
            budget,
            rng_seed,
            goal,
            max_calibration_iters,
            seed_filter,
            dry_run,
            use_trace_signature,
        } => campaign(
            &opts,
            CampaignArgs {
                protocol,
                budget,
                rng_seed,
                goal,
                max_calibration_iters,
                seed_filter,
                dry_run,
                use_trace_signature,
            },
        )
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

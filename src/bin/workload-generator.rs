use clap::{Parser, ValueEnum};
use days::workload_generator::{GeneratorArgs, InferencePhase, generate_workload_file};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum PhaseArg {
    Decode,
    Prefill,
}

impl From<PhaseArg> for InferencePhase {
    fn from(value: PhaseArg) -> Self {
        match value {
            PhaseArg::Decode => InferencePhase::Decode,
            PhaseArg::Prefill => InferencePhase::Prefill,
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "workload-generator")]
#[command(about = "Generate inference workload files")]
struct Cli {
    model_name: String,

    config_file: PathBuf,

    #[arg(long = "aiob_enable", default_value_t = false)]
    aiob_enable: bool,

    #[arg(long = "aiob_forward_loops", default_value_t = 1)]
    aiob_forward_loops: u64,

    #[arg(long = "moe_routing_strategy", default_value = "RoundRobin")]
    moe_routing_strategy: String,

    #[arg(long = "seq_length", default_value_t = 1)]
    seq_length: u64,

    #[arg(long = "micro_batch", default_value_t = 1)]
    micro_batch: u64,

    #[arg(long = "world_size", default_value_t = 1)]
    world_size: u64,

    #[arg(long = "tensor_model_parallel_size", default_value_t = 1)]
    tensor_model_parallel_size: u64,

    #[arg(long = "expert_model_parallel_size", default_value_t = 1)]
    expert_model_parallel_size: u64,

    #[arg(long = "pipeline_model_parallel", default_value_t = 1)]
    pipeline_model_parallel: u64,

    #[arg(long = "moe_enable", default_value_t = true)]
    moe_enable: bool,

    #[arg(long = "result_dir", default_value = "results/workload/")]
    result_dir: PathBuf,

    #[arg(long, value_enum, default_value_t = PhaseArg::Decode)]
    phase: PhaseArg,
}

fn main() {
    let cli = Cli::parse();

    let args = GeneratorArgs {
        model_name: cli.model_name.clone(),
        world_size: cli.world_size,
        tensor_model_parallel_size: cli.tensor_model_parallel_size,
        expert_model_parallel_size: cli.expert_model_parallel_size,
        pipeline_model_parallel: cli.pipeline_model_parallel,
        seq_length: cli.seq_length,
        micro_batch: cli.micro_batch,
        phase: cli.phase.into(),
        aiob_enable: cli.aiob_enable,
    };

    match generate_workload_file(&cli.config_file, &cli.result_dir, &args) {
        Ok(path) => {
            println!("workload save in : {}", path.display());
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

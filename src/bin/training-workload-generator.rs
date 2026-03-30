use clap::Parser;
use days::training_workload_generator::{TrainingGeneratorArgs, generate_training_workload_file};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "training-workload-generator")]
#[command(about = "Generate training workload files (Rust rewrite subset)")]
struct Cli {
    #[arg(long = "gpu_type", default_value = "A100")]
    gpu_type: String,

    #[arg(long = "model_name")]
    model_name: String,

    #[arg(long = "world_size")]
    world_size: u64,

    #[arg(long = "tensor_model_parallel_size")]
    tensor_model_parallel_size: u64,

    #[arg(long = "pipeline_model_parallel", default_value_t = 1)]
    pipeline_model_parallel: u64,

    #[arg(long = "expert_model_parallel_size", default_value_t = 1)]
    expert_model_parallel_size: u64,

    #[arg(long = "global_batch")]
    global_batch: u64,

    #[arg(long = "micro_batch")]
    micro_batch: u64,

    #[arg(long = "num_layers")]
    num_layers: u64,

    #[arg(long = "seq_length")]
    seq_length: u64,

    #[arg(long = "hidden_size")]
    hidden_size: u64,

    #[arg(long = "vocab_size")]
    vocab_size: u64,

    #[arg(long = "ffn_hidden_size")]
    ffn_hidden_size: Option<u64>,

    #[arg(long = "make_vocab_size_divisible_by", default_value_t = 128)]
    make_vocab_size_divisible_by: u64,

    #[arg(long = "enable_sequence_parallel", default_value_t = false)]
    enable_sequence_parallel: bool,

    #[arg(long = "recompute_activations", default_value_t = false)]
    recompute_activations: bool,

    #[arg(long = "use_flash_attn", default_value_t = false)]
    use_flash_attn: bool,

    #[arg(long = "moe_enable", default_value_t = false)]
    moe_enable: bool,

    #[arg(long = "moe_grouped_gemm", default_value_t = false)]
    moe_grouped_gemm: bool,

    #[arg(long = "aiob_enable", default_value_t = false)]
    aiob_enable: bool,

    #[arg(long = "result_dir", default_value = "results/workload/")]
    result_dir: PathBuf,
}

fn main() {
    let cli = Cli::parse();
    let args = TrainingGeneratorArgs {
        gpu_type: cli.gpu_type,
        model_name: cli.model_name,
        world_size: cli.world_size,
        tensor_model_parallel_size: cli.tensor_model_parallel_size,
        pipeline_model_parallel: cli.pipeline_model_parallel,
        expert_model_parallel_size: cli.expert_model_parallel_size,
        global_batch: cli.global_batch,
        micro_batch: cli.micro_batch,
        num_layers: cli.num_layers,
        seq_length: cli.seq_length,
        hidden_size: cli.hidden_size,
        vocab_size: cli.vocab_size,
        ffn_hidden_size: cli.ffn_hidden_size.unwrap_or(cli.hidden_size * 4),
        make_vocab_size_divisible_by: cli.make_vocab_size_divisible_by,
        enable_sequence_parallel: cli.enable_sequence_parallel,
        recompute_activations: cli.recompute_activations,
        use_flash_attn: cli.use_flash_attn,
        moe_enable: cli.moe_enable,
        moe_grouped_gemm: cli.moe_grouped_gemm,
        aiob_enable: cli.aiob_enable,
    };

    match generate_training_workload_file(&cli.result_dir, &args) {
        Ok(path) => println!("workload save in : {}", path.display()),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

use clap::Parser;
use days::training_workload_generator::{TrainingGeneratorArgs, generate_training_workload_file};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser, Debug)]
#[command(name = "training-workload-generator")]
#[command(about = "Generate training workload files (Rust rewrite subset)")]
struct Cli {
    #[arg(long = "config")]
    config: Option<PathBuf>,

    #[arg(long = "gpu_type")]
    gpu_type: Option<String>,

    #[arg(long = "frame")]
    frame: Option<String>,

    #[arg(long = "model_name")]
    model_name: Option<String>,

    #[arg(long = "world_size")]
    world_size: Option<u64>,

    #[arg(long = "tensor_model_parallel_size")]
    tensor_model_parallel_size: Option<u64>,

    #[arg(long = "pipeline_model_parallel")]
    pipeline_model_parallel: Option<u64>,

    #[arg(long = "expert_model_parallel_size")]
    expert_model_parallel_size: Option<u64>,

    #[arg(long = "global_batch")]
    global_batch: Option<u64>,

    #[arg(long = "micro_batch")]
    micro_batch: Option<u64>,

    #[arg(long = "num_layers")]
    num_layers: Option<u64>,

    #[arg(long = "seq_length")]
    seq_length: Option<u64>,

    #[arg(long = "hidden_size")]
    hidden_size: Option<u64>,

    #[arg(long = "vocab_size")]
    vocab_size: Option<u64>,

    #[arg(long = "ffn_hidden_size")]
    ffn_hidden_size: Option<u64>,

    #[arg(long = "num_attention_heads")]
    num_attention_heads: Option<u64>,

    #[arg(long = "num_experts")]
    num_experts: Option<u64>,

    #[arg(long = "moe_router_topk")]
    moe_router_topk: Option<u64>,

    #[arg(long = "make_vocab_size_divisible_by")]
    make_vocab_size_divisible_by: Option<u64>,

    #[arg(long = "n_dense_layers")]
    n_dense_layers: Option<u64>,

    #[arg(long = "n_shared_expert")]
    n_shared_expert: Option<u64>,

    #[arg(long = "qk_rope_dim")]
    qk_rope_dim: Option<u64>,

    #[arg(long = "qk_nope_dim")]
    qk_nope_dim: Option<u64>,

    #[arg(long = "q_lora_rank")]
    q_lora_rank: Option<u64>,

    #[arg(long = "kv_lora_rank")]
    kv_lora_rank: Option<u64>,

    #[arg(long = "v_head_dim")]
    v_head_dim: Option<u64>,

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

    #[arg(long = "aiob_profile")]
    aiob_profile: Option<PathBuf>,

    #[arg(long = "auto_aiob_profile", default_value_t = false)]
    auto_aiob_profile: bool,

    #[arg(long = "python_bin")]
    python_bin: Option<String>,

    #[arg(long = "aicb_dir")]
    aicb_dir: Option<PathBuf>,

    #[arg(long = "result_dir")]
    result_dir: Option<PathBuf>,
}

#[derive(Debug, Default, Deserialize)]
struct GeneratorConfig {
    gpu_type: Option<String>,
    frame: Option<String>,
    model_name: Option<String>,
    world_size: Option<u64>,
    tensor_model_parallel_size: Option<u64>,
    pipeline_model_parallel: Option<u64>,
    expert_model_parallel_size: Option<u64>,
    global_batch: Option<u64>,
    micro_batch: Option<u64>,
    num_layers: Option<u64>,
    seq_length: Option<u64>,
    hidden_size: Option<u64>,
    vocab_size: Option<u64>,
    ffn_hidden_size: Option<u64>,
    num_attention_heads: Option<u64>,
    num_experts: Option<u64>,
    moe_router_topk: Option<u64>,
    make_vocab_size_divisible_by: Option<u64>,
    n_dense_layers: Option<u64>,
    n_shared_expert: Option<u64>,
    qk_rope_dim: Option<u64>,
    qk_nope_dim: Option<u64>,
    q_lora_rank: Option<u64>,
    kv_lora_rank: Option<u64>,
    v_head_dim: Option<u64>,
    enable_sequence_parallel: Option<bool>,
    recompute_activations: Option<bool>,
    use_flash_attn: Option<bool>,
    moe_enable: Option<bool>,
    moe_grouped_gemm: Option<bool>,
    aiob_enable: Option<bool>,
    aiob_profile: Option<PathBuf>,
    auto_aiob_profile: Option<bool>,
    python_bin: Option<String>,
    aicb_dir: Option<PathBuf>,
    result_dir: Option<PathBuf>,
}

fn load_config(path: &Path) -> Result<GeneratorConfig, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("failed to read config {}: {e}", path.display()))?;
    toml::from_str::<GeneratorConfig>(&content)
        .map_err(|e| format!("failed to parse config {}: {e}", path.display()))
}

fn merge_opt<T>(cli: Option<T>, cfg: Option<T>, default: T) -> T {
    cli.or(cfg).unwrap_or(default)
}

fn merge_required<T>(name: &str, cli: Option<T>, cfg: Option<T>) -> Result<T, String> {
    cli.or(cfg)
        .ok_or_else(|| format!("missing required field `{name}` (pass CLI arg or set in --config)"))
}

fn merge_flag(cli: bool, cfg: Option<bool>) -> bool {
    if cli { true } else { cfg.unwrap_or(false) }
}

fn bool_python_style(v: bool) -> &'static str {
    if v { "True" } else { "False" }
}

fn default_training_aiob_profile_path(args: &TrainingGeneratorArgs) -> PathBuf {
    let filename = format!(
        "{}-world_size{}-tp{}-pp{}-ep{}-gbs{}-mbs{}-seq{}-flash_attn-{}.txt",
        args.model_name,
        args.world_size,
        args.tensor_model_parallel_size,
        args.pipeline_model_parallel,
        args.expert_model_parallel_size,
        args.global_batch,
        args.micro_batch,
        args.seq_length,
        bool_python_style(args.use_flash_attn)
    );
    PathBuf::from("results").join("aiob_outputs").join(filename)
}

fn resolve_aicb_dir(aicb_dir: Option<&PathBuf>) -> PathBuf {
    if let Some(dir) = aicb_dir {
        return dir.clone();
    }
    if let Ok(dir) = std::env::var("DAYTONE_AICB_DIR") {
        return PathBuf::from(dir);
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let vendored = manifest_dir.join("vendor").join("aicb");
    if vendored.is_dir() {
        return vendored;
    }
    match manifest_dir.parent() {
        Some(parent) => parent.join("daytone-experiments").join("aicb"),
        None => PathBuf::from("daytone-experiments").join("aicb"),
    }
}

fn ensure_training_aiob_profile(
    auto_aiob_profile: bool,
    python_bin: &str,
    aicb_dir: Option<&PathBuf>,
    args: &mut TrainingGeneratorArgs,
) -> Result<(), String> {
    if !auto_aiob_profile || !args.aiob_enable {
        return Ok(());
    }

    let default_profile = default_training_aiob_profile_path(args);
    let target_profile = args
        .aiob_profile
        .clone()
        .unwrap_or_else(|| default_profile.clone());
    if target_profile.is_file() {
        args.aiob_profile = Some(target_profile);
        return Ok(());
    }

    let aicb_dir = resolve_aicb_dir(aicb_dir);
    let tmp_dir = tempfile::tempdir().map_err(|e| format!("tempdir error: {e}"))?;
    let mut cmd = Command::new(python_bin);
    cmd.current_dir(&aicb_dir)
        .arg("-m")
        .arg("workload_generator.SimAI_training_workload_generator")
        .arg("--frame")
        .arg(&args.frame)
        .arg("--gpu_type")
        .arg(&args.gpu_type)
        .arg("--model_name")
        .arg(&args.model_name)
        .arg("--world_size")
        .arg(args.world_size.to_string())
        .arg("--tensor_model_parallel_size")
        .arg(args.tensor_model_parallel_size.to_string())
        .arg("--pipeline_model_parallel")
        .arg(args.pipeline_model_parallel.to_string())
        .arg("--expert_model_parallel_size")
        .arg(args.expert_model_parallel_size.to_string())
        .arg("--global_batch")
        .arg(args.global_batch.to_string())
        .arg("--micro_batch")
        .arg(args.micro_batch.to_string())
        .arg("--num_layers")
        .arg(args.num_layers.to_string())
        .arg("--seq_length")
        .arg(args.seq_length.to_string())
        .arg("--hidden_size")
        .arg(args.hidden_size.to_string())
        .arg("--vocab_size")
        .arg(args.vocab_size.to_string())
        .arg("--ffn_hidden_size")
        .arg(args.ffn_hidden_size.to_string())
        .arg("--num_attention_heads")
        .arg(args.num_attention_heads.to_string())
        .arg("--num_experts")
        .arg(args.num_experts.to_string())
        .arg("--moe_router_topk")
        .arg(args.moe_router_topk.to_string())
        .arg("--make_vocab_size_divisible_by")
        .arg(args.make_vocab_size_divisible_by.to_string())
        .arg("--n_dense_layers")
        .arg(args.n_dense_layers.to_string())
        .arg("--n_shared_expert")
        .arg(args.n_shared_expert.to_string())
        .arg("--qk_rope_dim")
        .arg(args.qk_rope_dim.to_string())
        .arg("--qk_nope_dim")
        .arg(args.qk_nope_dim.to_string())
        .arg("--q_lora_rank")
        .arg(args.q_lora_rank.to_string())
        .arg("--kv_lora_rank")
        .arg(args.kv_lora_rank.to_string())
        .arg("--v_head_dim")
        .arg(args.v_head_dim.to_string())
        .arg("--aiob_enable")
        .arg("--result_dir")
        .arg(tmp_dir.path());
    if args.enable_sequence_parallel {
        cmd.arg("--enable_sequence_parallel");
    }
    if args.recompute_activations {
        cmd.arg("--recompute_activations");
    }
    if args.use_flash_attn {
        cmd.arg("--use_flash_attn");
    }
    if args.moe_enable {
        cmd.arg("--moe_enable");
    }
    if args.moe_grouped_gemm {
        cmd.arg("--moe_grouped_gemm");
    }

    let output = cmd
        .output()
        .map_err(|e| format!("failed to run python profiler: {e}"))?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "python profiler failed in {} with status {}.\nstdout:\n{}\nstderr:\n{}",
            aicb_dir.display(),
            output.status,
            stdout.trim(),
            stderr.trim()
        ));
    }

    if !target_profile.is_file() && default_profile.is_file() && target_profile != default_profile {
        if let Some(parent) = target_profile.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "failed to create aiob profile parent directory {}: {e}",
                    parent.display()
                )
            })?;
        }
        fs::copy(&default_profile, &target_profile).map_err(|e| {
            format!(
                "failed to copy generated profile {} -> {}: {e}",
                default_profile.display(),
                target_profile.display()
            )
        })?;
    }

    if !target_profile.is_file() {
        return Err(format!(
            "auto aiob profiling finished but profile not found at {}",
            target_profile.display()
        ));
    }
    args.aiob_profile = Some(target_profile);
    Ok(())
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let cfg = match cli.config.as_ref() {
        Some(path) => load_config(path)?,
        None => GeneratorConfig::default(),
    };

    let hidden_size = merge_required("hidden_size", cli.hidden_size, cfg.hidden_size)?;
    let ffn_hidden_size = cli
        .ffn_hidden_size
        .or(cfg.ffn_hidden_size)
        .unwrap_or(hidden_size * 4);
    let auto_aiob_profile = merge_flag(cli.auto_aiob_profile, cfg.auto_aiob_profile);
    let python_bin = merge_opt(cli.python_bin, cfg.python_bin, "python".to_string());
    let aicb_dir = cli.aicb_dir.or(cfg.aicb_dir);
    let result_dir = merge_opt(
        cli.result_dir,
        cfg.result_dir,
        PathBuf::from("workload/collective/training/"),
    );

    let mut args = TrainingGeneratorArgs {
        gpu_type: merge_opt(cli.gpu_type, cfg.gpu_type, "A100".to_string()),
        frame: merge_opt(cli.frame, cfg.frame, "Megatron".to_string()),
        model_name: merge_required("model_name", cli.model_name, cfg.model_name)?,
        world_size: merge_required("world_size", cli.world_size, cfg.world_size)?,
        tensor_model_parallel_size: merge_required(
            "tensor_model_parallel_size",
            cli.tensor_model_parallel_size,
            cfg.tensor_model_parallel_size,
        )?,
        pipeline_model_parallel: merge_opt(
            cli.pipeline_model_parallel,
            cfg.pipeline_model_parallel,
            1,
        ),
        expert_model_parallel_size: merge_opt(
            cli.expert_model_parallel_size,
            cfg.expert_model_parallel_size,
            1,
        ),
        global_batch: merge_required("global_batch", cli.global_batch, cfg.global_batch)?,
        micro_batch: merge_required("micro_batch", cli.micro_batch, cfg.micro_batch)?,
        num_layers: merge_required("num_layers", cli.num_layers, cfg.num_layers)?,
        seq_length: merge_required("seq_length", cli.seq_length, cfg.seq_length)?,
        hidden_size,
        vocab_size: merge_required("vocab_size", cli.vocab_size, cfg.vocab_size)?,
        ffn_hidden_size,
        num_attention_heads: merge_opt(cli.num_attention_heads, cfg.num_attention_heads, 1),
        num_experts: merge_opt(cli.num_experts, cfg.num_experts, 1),
        moe_router_topk: merge_opt(cli.moe_router_topk, cfg.moe_router_topk, 1),
        make_vocab_size_divisible_by: merge_opt(
            cli.make_vocab_size_divisible_by,
            cfg.make_vocab_size_divisible_by,
            128,
        ),
        n_dense_layers: merge_opt(cli.n_dense_layers, cfg.n_dense_layers, 3),
        n_shared_expert: merge_opt(cli.n_shared_expert, cfg.n_shared_expert, 2),
        qk_rope_dim: merge_opt(cli.qk_rope_dim, cfg.qk_rope_dim, 64),
        qk_nope_dim: merge_opt(cli.qk_nope_dim, cfg.qk_nope_dim, 128),
        q_lora_rank: merge_opt(cli.q_lora_rank, cfg.q_lora_rank, 1536),
        kv_lora_rank: merge_opt(cli.kv_lora_rank, cfg.kv_lora_rank, 512),
        v_head_dim: merge_opt(cli.v_head_dim, cfg.v_head_dim, 128),
        enable_sequence_parallel: merge_flag(cli.enable_sequence_parallel, cfg.enable_sequence_parallel),
        recompute_activations: merge_flag(cli.recompute_activations, cfg.recompute_activations),
        use_flash_attn: merge_flag(cli.use_flash_attn, cfg.use_flash_attn),
        moe_enable: merge_flag(cli.moe_enable, cfg.moe_enable),
        moe_grouped_gemm: merge_flag(cli.moe_grouped_gemm, cfg.moe_grouped_gemm),
        aiob_enable: merge_flag(cli.aiob_enable, cfg.aiob_enable),
        aiob_profile: cli.aiob_profile.or(cfg.aiob_profile),
    };

    ensure_training_aiob_profile(
        auto_aiob_profile,
        &python_bin,
        aicb_dir.as_ref(),
        &mut args,
    )?;

    let path = generate_training_workload_file(&result_dir, &args)
        .map_err(|e| e.to_string())?;
    println!("workload save in : {}", path.display());
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

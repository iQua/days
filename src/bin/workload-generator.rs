use clap::{Parser, ValueEnum};
use days::workload_generator::{GeneratorArgs, InferencePhase, generate_workload_file};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

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

    #[arg(long = "aiob_profile")]
    aiob_profile: Option<PathBuf>,

    #[arg(long = "auto_aiob_profile", default_value_t = false)]
    auto_aiob_profile: bool,

    #[arg(long = "python_bin", default_value = "python")]
    python_bin: String,

    #[arg(long = "aicb_dir")]
    aicb_dir: Option<PathBuf>,

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

    #[arg(long = "result_dir", default_value = "workload/collective/inference/")]
    result_dir: PathBuf,

    #[arg(long, value_enum, default_value_t = PhaseArg::Decode)]
    phase: PhaseArg,
}

fn default_inference_aiob_profile_path(args: &GeneratorArgs) -> PathBuf {
    let filename = format!(
        "{}-world_size{}-tp{}-pp{}-ep{}-bpg{}-seq{}-{}.txt",
        args.model_name,
        args.world_size,
        args.tensor_model_parallel_size,
        args.pipeline_model_parallel,
        args.expert_model_parallel_size,
        args.micro_batch,
        args.seq_length,
        args.phase.as_str()
    );
    PathBuf::from("results").join("aiob_outputs").join(filename)
}

fn resolve_aicb_dir(cli: &Cli) -> PathBuf {
    if let Some(dir) = cli.aicb_dir.as_ref() {
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

fn ensure_inference_aiob_profile(cli: &Cli, args: &mut GeneratorArgs) -> Result<(), String> {
    if !cli.auto_aiob_profile || !args.aiob_enable {
        return Ok(());
    }

    let default_profile = default_inference_aiob_profile_path(args);
    let target_profile = args
        .aiob_profile
        .clone()
        .unwrap_or_else(|| default_profile.clone());
    if target_profile.is_file() {
        args.aiob_profile = Some(target_profile);
        return Ok(());
    }

    let aicb_dir = resolve_aicb_dir(cli);
    let tmp_dir = tempfile::tempdir().map_err(|e| format!("tempdir error: {e}"))?;
    let mut cmd = Command::new(&cli.python_bin);
    cmd.current_dir(&aicb_dir)
        .arg("-m")
        .arg("workload_generator.SimAI_inference_workload_generator")
        .arg(&args.model_name)
        .arg(&cli.config_file)
        .arg("--aiob_enable")
        .arg("--seq_length")
        .arg(args.seq_length.to_string())
        .arg("--micro_batch")
        .arg(args.micro_batch.to_string())
        .arg("--world_size")
        .arg(args.world_size.to_string())
        .arg("--tensor_model_parallel_size")
        .arg(args.tensor_model_parallel_size.to_string())
        .arg("--expert_model_parallel_size")
        .arg(args.expert_model_parallel_size.to_string())
        .arg("--pipeline_model_parallel")
        .arg(args.pipeline_model_parallel.to_string())
        .arg("--phase")
        .arg(args.phase.as_str())
        .arg("--result_dir")
        .arg(tmp_dir.path());

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

fn main() {
    let cli = Cli::parse();

    let mut args = GeneratorArgs {
        model_name: cli.model_name.clone(),
        world_size: cli.world_size,
        tensor_model_parallel_size: cli.tensor_model_parallel_size,
        expert_model_parallel_size: cli.expert_model_parallel_size,
        pipeline_model_parallel: cli.pipeline_model_parallel,
        seq_length: cli.seq_length,
        micro_batch: cli.micro_batch,
        phase: cli.phase.into(),
        aiob_enable: cli.aiob_enable,
        aiob_profile: cli.aiob_profile.clone(),
    };

    if let Err(err) = ensure_inference_aiob_profile(&cli, &mut args) {
        eprintln!("{err}");
        std::process::exit(1);
    }

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

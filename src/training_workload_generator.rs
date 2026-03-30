use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct TrainingGeneratorArgs {
    pub gpu_type: String,
    pub frame: String,
    pub model_name: String,
    pub world_size: u64,
    pub tensor_model_parallel_size: u64,
    pub pipeline_model_parallel: u64,
    pub expert_model_parallel_size: u64,
    pub global_batch: u64,
    pub micro_batch: u64,
    pub num_layers: u64,
    pub seq_length: u64,
    pub hidden_size: u64,
    pub vocab_size: u64,
    pub ffn_hidden_size: u64,
    pub num_attention_heads: u64,
    pub num_experts: u64,
    pub moe_router_topk: u64,
    pub make_vocab_size_divisible_by: u64,
    pub n_dense_layers: u64,
    pub n_shared_expert: u64,
    pub qk_rope_dim: u64,
    pub qk_nope_dim: u64,
    pub q_lora_rank: u64,
    pub kv_lora_rank: u64,
    pub v_head_dim: u64,
    pub enable_sequence_parallel: bool,
    pub recompute_activations: bool,
    pub use_flash_attn: bool,
    pub moe_enable: bool,
    pub moe_grouped_gemm: bool,
    pub aiob_enable: bool,
    pub aiob_profile: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrainingWorkItem {
    pub name: String,
    pub placeholder: i64,
    pub forward_compute_time: u64,
    pub forward_comm: String,
    pub forward_comm_size: u64,
    pub backward_compute_time: u64,
    pub backward_comm: String,
    pub backward_comm_size: u64,
    pub dp_compute_time: u64,
    pub dp_comm: String,
    pub dp_comm_size: u64,
    pub process_time: u64,
}

impl TrainingWorkItem {
    fn to_tsv_line(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.name,
            self.placeholder,
            self.forward_compute_time,
            self.forward_comm,
            self.forward_comm_size,
            self.backward_compute_time,
            self.backward_comm,
            self.backward_comm_size,
            self.dp_compute_time,
            self.dp_comm,
            self.dp_comm_size,
            self.process_time
        )
    }
}

#[derive(Debug)]
pub enum TrainingWorkloadError {
    Io(std::io::Error),
    MissingAiobProfile(PathBuf),
    InvalidConfig(&'static str),
    Unsupported(&'static str),
}

impl std::fmt::Display for TrainingWorkloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::MissingAiobProfile(path) => write!(
                f,
                "missing aiob profile: {} (use --aiob_profile or place default file under results/aiob_outputs/)",
                path.display()
            ),
            Self::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported option: {msg}"),
        }
    }
}

impl std::error::Error for TrainingWorkloadError {}

impl From<std::io::Error> for TrainingWorkloadError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone, Copy)]
struct Derived {
    dp_num: u64,
    ga_num: u64,
    num_layers_per_stage: u64,
    #[allow(dead_code)]
    padded_vocab_size: u64,
    tp_comm_size: u64,
    total_params: u64,
    moe_param_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrainingFrame {
    Megatron,
    DeepSeek,
}

const DEEPSEEK_FP8_FACTOR: f64 = (1.0 + 4.0 / 128.0) / 2.0;

impl TrainingFrame {
    fn parse(value: &str) -> Result<Self, TrainingWorkloadError> {
        match value {
            v if v.eq_ignore_ascii_case("megatron") => Ok(Self::Megatron),
            v if v.eq_ignore_ascii_case("deepseek") => Ok(Self::DeepSeek),
            _ => Err(TrainingWorkloadError::InvalidConfig(
                "frame must be Megatron or DeepSeek",
            )),
        }
    }
}

pub fn generate_training_workload_file(
    result_dir: &Path,
    args: &TrainingGeneratorArgs,
) -> Result<PathBuf, TrainingWorkloadError> {
    validate_training_args(args)?;
    let derived = derive(args)?;
    let compute_cache = load_training_aiob_compute_cache(args)?;
    let payload = generate_training_workload_payload(args, derived, &compute_cache);

    fs::create_dir_all(result_dir)?;
    let output_path = result_dir.join(training_output_stem(args) + ".txt");
    fs::write(&output_path, payload)?;
    Ok(output_path)
}

pub fn training_output_stem(args: &TrainingGeneratorArgs) -> String {
    format!(
        "{}-{}-world_size{}-tp{}-pp{}-ep{}-gbs{}-mbs{}-seq{}-MOE-{}-GEMM-{}-flash_attn-{}",
        args.gpu_type,
        args.model_name,
        args.world_size,
        args.tensor_model_parallel_size,
        args.pipeline_model_parallel,
        args.expert_model_parallel_size,
        args.global_batch,
        args.micro_batch,
        args.seq_length,
        bool_python_style(args.moe_enable),
        bool_python_style(args.moe_grouped_gemm),
        bool_python_style(args.use_flash_attn),
    )
}

fn generate_training_workload_payload(
    args: &TrainingGeneratorArgs,
    derived: Derived,
    compute_cache: &HashMap<String, u64>,
) -> String {
    let mut items = Vec::new();
    let default_compute_time = 1_u64;
    let per_step_compute_time = 0_u64;

    if args.aiob_enable {
        let grad_fwd = get_training_compute_time(compute_cache, "forward", "grad", true);
        let grad_bwd = get_training_compute_time(compute_cache, "backward", "grad", true);
        items.push(item(
            "grad_gather",
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "ALLGATHER",
            2 * (derived.total_params - derived.moe_param_count),
        ));
        items.push(item(
            "grad_param_comm",
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "REDUCESCATTER",
            4 * (derived.total_params - derived.moe_param_count),
        ));
        items.push(item(
            "grad_param_compute",
            default_compute_time,
            "NONE",
            0,
            grad_fwd + grad_bwd,
            "NONE",
            0,
            default_compute_time,
            "NONE",
            0,
        ));
    } else {
        items.push(item(
            "grad_norm",
            default_compute_time,
            "ALLGATHER",
            2 * derived.total_params,
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "REDUCESCATTER",
            4 * derived.total_params,
        ));
    }

    if !args.enable_sequence_parallel {
        items.push(item(
            "layernorm",
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "ALLREDUCE",
            2 * derived.total_params,
            default_compute_time,
            "NONE",
            0,
        ));
    }

    if args.aiob_enable {
        let embedding_backward_comm = if args.tensor_model_parallel_size == 1 {
            "NONE"
        } else {
            "ALLREDUCE"
        };
        items.push(item(
            "embedding_grads",
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            embedding_backward_comm,
            derived.tp_comm_size,
            default_compute_time,
            "NONE",
            0,
        ));
    }

    if args.expert_model_parallel_size != derived.dp_num {
        items.push(item(
            "moe_grad_norm1",
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "ALLGATHER_DP_EP",
            2 * derived.moe_param_count,
        ));
        items.push(item(
            "moe_grad_norm2",
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "NONE",
            0,
            default_compute_time,
            "REDUCESCATTER_DP_EP",
            4 * derived.moe_param_count,
        ));
    }

    for _ in 0..derived.ga_num {
        if args.enable_sequence_parallel {
            if args.aiob_enable {
                append_sp_aiob_items(
                    &mut items,
                    args,
                    derived,
                    compute_cache,
                    default_compute_time,
                );
            } else {
                append_sp_items(&mut items, args, derived, default_compute_time);
            }
        } else if args.aiob_enable {
            append_non_sp_aiob_items(
                &mut items,
                args,
                derived,
                compute_cache,
                default_compute_time,
            );
        } else {
            append_non_sp_items(&mut items, args, derived, default_compute_time);
        }

        if !args.aiob_enable {
            items.push(item(
                "embedding_norm",
                default_compute_time,
                "ALLREDUCE",
                args.vocab_size * args.hidden_size * 2,
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
            ));
        }
    }

    for idx in 1..=3 {
        items.push(item(
            &format!("cross_entropy{idx}"),
            per_step_compute_time,
            "ALLREDUCE",
            args.seq_length * args.micro_batch * 4,
            per_step_compute_time,
            "NONE",
            0,
            per_step_compute_time,
            "NONE",
            0,
        ));
    }

    for idx in 1..=4 {
        items.push(item(
            &format!("optimizer{idx}"),
            per_step_compute_time,
            "ALLREDUCE",
            4,
            per_step_compute_time,
            "NONE",
            0,
            per_step_compute_time,
            "NONE",
            0,
        ));
    }

    let pp_comm = if args.pipeline_model_parallel != 1 {
        let base = 2_u64
            .checked_mul(args.micro_batch)
            .and_then(|v| v.checked_mul(args.seq_length))
            .and_then(|v| v.checked_mul(args.hidden_size))
            .expect("pp comm overflow");
        if args.enable_sequence_parallel {
            format!(
                "{:.1}",
                (base as f64) / (args.tensor_model_parallel_size as f64)
            )
        } else {
            format!("{base}")
        }
    } else {
        "0".to_string()
    };

    let header = format!(
        "HYBRID_TRANSFORMER_FWD_IN_BCKWD model_parallel_NPU_group: {} ep: {} pp: {} vpp: {} ga: {} all_gpus: {} checkpoints: 0 checkpoint_initiates: 0 pp_comm: {}",
        args.tensor_model_parallel_size,
        args.expert_model_parallel_size,
        args.pipeline_model_parallel,
        derived.num_layers_per_stage,
        derived.ga_num,
        args.world_size,
        pp_comm
    );

    let mut out = String::new();
    out.push_str(&header);
    out.push('\n');
    out.push_str(&items.len().to_string());
    out.push('\n');
    for entry in items {
        out.push_str(&entry.to_tsv_line());
        out.push('\n');
    }
    out
}

fn append_sp_items(
    items: &mut Vec<TrainingWorkItem>,
    args: &TrainingGeneratorArgs,
    derived: Derived,
    default_compute_time: u64,
) {
    let frame = TrainingFrame::parse(&args.frame).expect("validated frame");
    items.push(item(
        "embedding_layer",
        default_compute_time,
        "NONE",
        derived.tp_comm_size,
        default_compute_time,
        "NONE",
        derived.tp_comm_size,
        default_compute_time,
        "NONE",
        0,
    ));

    for layer_idx in 0..derived.num_layers_per_stage {
        let attention_forward_comm_size = if args.recompute_activations {
            derived
                .tp_comm_size
                .checked_mul(2)
                .expect("attention comm size overflow")
        } else {
            derived.tp_comm_size
        };

        if frame == TrainingFrame::DeepSeek {
            items.push(item(
                "attention_linear_q_lora",
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
            ));
            items.push(item(
                "attention_q_column",
                default_compute_time,
                "ALLGATHER",
                attention_forward_comm_size,
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
            ));
            items.push(item(
                "attention_linear_kv_lora",
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
            ));
            items.push(item(
                "attention_kv_column",
                default_compute_time,
                "ALLGATHER",
                attention_forward_comm_size,
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
            ));
            items.push(item(
                "attention_o_row",
                default_compute_time,
                "REDUCESCATTER",
                attention_forward_comm_size,
                default_compute_time,
                "NONE",
                derived.tp_comm_size,
                default_compute_time,
                "NONE",
                0,
            ));

            if layer_idx < args.n_dense_layers {
                items.push(item(
                    "mlp_column",
                    default_compute_time,
                    "ALLGATHER",
                    derived.tp_comm_size,
                    default_compute_time,
                    "NONE",
                    0,
                    default_compute_time,
                    "NONE",
                    0,
                ));
                items.push(item(
                    "mlp_row",
                    default_compute_time,
                    "REDUCESCATTER",
                    derived.tp_comm_size,
                    default_compute_time,
                    "NONE",
                    derived.tp_comm_size,
                    default_compute_time,
                    "NONE",
                    0,
                ));
            } else {
                append_moe_sequence(
                    items,
                    "mlp_moelayer",
                    default_compute_time,
                    default_compute_time,
                    default_compute_time,
                    derived.tp_comm_size,
                    args,
                    frame,
                );
                if args.n_shared_expert > 0 {
                    items.push(item(
                        "mlp_column",
                        default_compute_time,
                        "ALLGATHER",
                        derived.tp_comm_size,
                        default_compute_time,
                        "NONE",
                        0,
                        default_compute_time,
                        "NONE",
                        0,
                    ));
                    items.push(item(
                        "mlp_row",
                        default_compute_time,
                        "REDUCESCATTER",
                        derived.tp_comm_size,
                        default_compute_time,
                        "NONE",
                        derived.tp_comm_size,
                        default_compute_time,
                        "NONE",
                        0,
                    ));
                }
            }
        } else {
            items.push(item(
                "attention_column",
                default_compute_time,
                "ALLGATHER",
                attention_forward_comm_size,
                default_compute_time,
                "NONE",
                0,
                default_compute_time,
                "NONE",
                0,
            ));
            items.push(item(
                "attention_row",
                default_compute_time,
                "REDUCESCATTER",
                attention_forward_comm_size,
                default_compute_time,
                "NONE",
                derived.tp_comm_size,
                default_compute_time,
                "NONE",
                0,
            ));

            if args.moe_enable {
                append_moe_sequence(
                    items,
                    "mlp_moelayer",
                    default_compute_time,
                    default_compute_time,
                    default_compute_time,
                    derived.tp_comm_size,
                    args,
                    frame,
                );
            } else {
                items.push(item(
                    "mlp_column",
                    default_compute_time,
                    "ALLGATHER",
                    derived.tp_comm_size,
                    default_compute_time,
                    "NONE",
                    0,
                    default_compute_time,
                    "NONE",
                    0,
                ));
                items.push(item(
                    "mlp_row",
                    default_compute_time,
                    "REDUCESCATTER",
                    derived.tp_comm_size,
                    default_compute_time,
                    "NONE",
                    derived.tp_comm_size,
                    default_compute_time,
                    "NONE",
                    0,
                ));
            }
        }
    }

    items.push(item(
        "final_column",
        default_compute_time,
        "ALLGATHER",
        derived.tp_comm_size,
        default_compute_time,
        "NONE",
        0,
        default_compute_time,
        "NONE",
        0,
    ));
}

fn append_moe_sequence(
    items: &mut Vec<TrainingWorkItem>,
    name: &str,
    forward_compute_time: u64,
    backward_compute_time: u64,
    default_compute_time: u64,
    tp_comm_size: u64,
    args: &TrainingGeneratorArgs,
    frame: TrainingFrame,
) {
    let mut forward_comm1 = "ALLGATHER";
    let mut forward_comm2 = "ALLTOALL_EP";
    let mut forward_comm3 = "ALLGATHER";
    let mut forward_comm4 = "REDUCESCATTER";
    let mut forward_comm5 = "ALLTOALL_EP";

    if args.expert_model_parallel_size == 1 {
        forward_comm2 = "NONE";
        forward_comm5 = "NONE";
    }
    if args.tensor_model_parallel_size == 1 {
        if args.expert_model_parallel_size == 1 {
            forward_comm1 = "NONE";
        }
        forward_comm3 = "NONE";
        forward_comm4 = "NONE";
    }

    let ep_allgather_size = 2_u64
        .checked_mul(args.expert_model_parallel_size)
        .and_then(|v| v.checked_mul(args.num_experts))
        .and_then(|v| v.checked_mul(args.tensor_model_parallel_size))
        .expect("moe ep allgather overflow");
    let dispatch_base = tp_comm_size
        .checked_mul(args.moe_router_topk)
        .and_then(|v| v.checked_div(args.tensor_model_parallel_size))
        .expect("moe dispatch overflow");
    let fwd_ep_dispatch_size = if frame == TrainingFrame::DeepSeek {
        (dispatch_base as f64 * DEEPSEEK_FP8_FACTOR) as u64
    } else {
        dispatch_base
    };
    let bwd_ep_dispatch_size = dispatch_base;
    let tp_allgather_size = tp_comm_size
        .checked_mul(args.moe_router_topk)
        .expect("moe tp allgather overflow");
    let ep_combine_size = dispatch_base;

    items.push(item(
        name,
        forward_compute_time,
        forward_comm1,
        ep_allgather_size,
        backward_compute_time,
        "NONE",
        0,
        default_compute_time,
        "NONE",
        0,
    ));
    items.push(item(
        name,
        default_compute_time,
        forward_comm2,
        fwd_ep_dispatch_size,
        default_compute_time,
        forward_comm2,
        bwd_ep_dispatch_size,
        default_compute_time,
        "NONE",
        0,
    ));
    items.push(item(
        name,
        default_compute_time,
        forward_comm3,
        tp_allgather_size,
        default_compute_time,
        forward_comm4,
        tp_allgather_size,
        default_compute_time,
        "NONE",
        0,
    ));
    items.push(item(
        name,
        default_compute_time,
        forward_comm4,
        tp_allgather_size,
        default_compute_time,
        forward_comm3,
        tp_allgather_size,
        default_compute_time,
        "NONE",
        0,
    ));
    items.push(item(
        name,
        default_compute_time,
        forward_comm5,
        ep_combine_size,
        default_compute_time,
        forward_comm5,
        ep_combine_size,
        default_compute_time,
        "NONE",
        0,
    ));
}

fn append_non_sp_items(
    items: &mut Vec<TrainingWorkItem>,
    args: &TrainingGeneratorArgs,
    derived: Derived,
    default_compute_time: u64,
) {
    items.push(item(
        "embedding_layer",
        default_compute_time,
        "ALLREDUCE",
        derived.tp_comm_size,
        default_compute_time,
        "ALLREDUCE",
        derived.tp_comm_size,
        default_compute_time,
        "NONE",
        0,
    ));

    for _ in 0..derived.num_layers_per_stage {
        let attention_forward_comm_size = if args.recompute_activations {
            derived
                .tp_comm_size
                .checked_mul(2)
                .expect("attention comm size overflow")
        } else {
            derived.tp_comm_size
        };
        items.push(item(
            "attention_layer",
            default_compute_time,
            "ALLREDUCE",
            attention_forward_comm_size,
            default_compute_time,
            "ALLREDUCE",
            derived.tp_comm_size,
            default_compute_time,
            "NONE",
            0,
        ));
        items.push(item(
            "mlp_layer",
            default_compute_time,
            "ALLREDUCE",
            derived.tp_comm_size,
            default_compute_time,
            "ALLREDUCE",
            derived.tp_comm_size,
            default_compute_time,
            "NONE",
            0,
        ));
    }
}

fn append_sp_aiob_items(
    items: &mut Vec<TrainingWorkItem>,
    args: &TrainingGeneratorArgs,
    derived: Derived,
    compute_cache: &HashMap<String, u64>,
    default_compute_time: u64,
) {
    let frame = TrainingFrame::parse(&args.frame).expect("validated frame");

    let layer_compute = |layer_name: &str, fallback_stage: &str| {
        let mut forward = get_training_compute_time(compute_cache, "", layer_name, false);
        let mut backward = forward;
        if forward == 1 {
            forward = get_training_compute_time(compute_cache, "forward", fallback_stage, true);
            backward = get_training_compute_time(compute_cache, "backward", fallback_stage, true);
        }
        if args.recompute_activations && layer_name.contains("attention") {
            forward = forward.saturating_mul(2);
        }
        (forward, backward)
    };

    let (embedding_forward_comm, embedding_backward_comm) = if args.tensor_model_parallel_size == 1
    {
        ("NONE", "NONE")
    } else {
        ("ALLREDUCE", "NONE")
    };
    let emb_compute = get_training_compute_time(compute_cache, "", "embedding", true);
    let grad_backward = get_training_compute_time(compute_cache, "backward", "grad", true);
    items.push(item(
        "embedding_layer",
        emb_compute,
        embedding_forward_comm,
        derived.tp_comm_size,
        default_compute_time,
        embedding_backward_comm,
        0,
        grad_backward,
        "NONE",
        0,
    ));

    let (row_forward_comm, row_backward_comm) = if args.tensor_model_parallel_size == 1 {
        ("NONE", "NONE")
    } else {
        ("REDUCESCATTER", "ALLGATHER")
    };
    let (col_forward_comm, col_backward_comm) = if args.tensor_model_parallel_size == 1 {
        ("NONE", "NONE")
    } else {
        ("ALLGATHER", "REDUCESCATTER")
    };

    for layer_idx in 0..derived.num_layers_per_stage {
        if frame == TrainingFrame::DeepSeek {
            let (q_lora_fwd, q_lora_bwd) = layer_compute("attention_linear_q_lora", "attention");
            items.push(item(
                "attention_linear_q_lora",
                q_lora_fwd,
                "NONE",
                0,
                q_lora_bwd,
                "NONE",
                0,
                q_lora_bwd,
                "NONE",
                0,
            ));

            let (q_col_fwd, q_col_bwd) = layer_compute("attention_q_column", "attention");
            items.push(item(
                "attention_q_column",
                q_col_fwd / 2,
                col_forward_comm,
                derived.tp_comm_size,
                q_col_bwd / 2,
                col_backward_comm,
                0,
                q_col_bwd / 2,
                "NONE",
                0,
            ));

            let (kv_lora_fwd, kv_lora_bwd) = layer_compute("attention_linear_kv_lora", "attention");
            items.push(item(
                "attention_linear_kv_lora",
                kv_lora_fwd,
                "NONE",
                0,
                kv_lora_bwd,
                "NONE",
                0,
                kv_lora_bwd,
                "NONE",
                0,
            ));

            let (kv_col_fwd, kv_col_bwd) = layer_compute("attention_kv_column", "attention");
            items.push(item(
                "attention_kv_column",
                kv_col_fwd / 2,
                col_forward_comm,
                derived.tp_comm_size,
                kv_col_bwd / 2,
                col_backward_comm,
                0,
                kv_col_bwd / 2,
                "NONE",
                0,
            ));

            let (o_row_fwd, o_row_bwd) = layer_compute("attention_o_row", "attention");
            items.push(item(
                "attention_o_row",
                o_row_fwd / 2,
                row_forward_comm,
                derived.tp_comm_size,
                o_row_bwd / 2,
                row_backward_comm,
                derived.tp_comm_size,
                o_row_bwd / 2,
                "NONE",
                0,
            ));

            if layer_idx < args.n_dense_layers {
                let (mlp_fwd, mlp_bwd) = layer_compute("mlp_column", "mlp");
                items.push(item(
                    "mlp_column",
                    mlp_fwd / 2,
                    col_forward_comm,
                    derived.tp_comm_size,
                    mlp_bwd / 2,
                    col_backward_comm,
                    0,
                    mlp_bwd / 2,
                    "NONE",
                    0,
                ));
                items.push(item(
                    "mlp_row",
                    mlp_fwd / 2,
                    row_forward_comm,
                    derived.tp_comm_size,
                    mlp_bwd / 2,
                    row_backward_comm,
                    derived.tp_comm_size,
                    mlp_bwd / 2,
                    "NONE",
                    0,
                ));
            } else {
                let (moe_fwd, moe_bwd) = layer_compute("mlp_moelayer", "mlp");
                append_moe_sequence(
                    items,
                    "mlp_moelayer",
                    moe_fwd,
                    moe_bwd,
                    default_compute_time,
                    derived.tp_comm_size,
                    args,
                    frame,
                );
                if args.n_shared_expert > 0 {
                    let (mlp_fwd, mlp_bwd) = layer_compute("mlp_column", "mlp");
                    items.push(item(
                        "mlp_column",
                        mlp_fwd / 2,
                        col_forward_comm,
                        derived.tp_comm_size,
                        mlp_bwd / 2,
                        col_backward_comm,
                        0,
                        mlp_bwd / 2,
                        "NONE",
                        0,
                    ));
                    items.push(item(
                        "mlp_row",
                        mlp_fwd / 2,
                        row_forward_comm,
                        derived.tp_comm_size,
                        mlp_bwd / 2,
                        row_backward_comm,
                        derived.tp_comm_size,
                        mlp_bwd / 2,
                        "NONE",
                        0,
                    ));
                }
            }
            continue;
        }

        let (att_fwd, att_bwd) = layer_compute("attention_row", "attention");
        let att_fwd_half = att_fwd / 2;
        let att_bwd_half = att_bwd / 2;
        items.push(item(
            "attention_row",
            att_fwd_half,
            row_forward_comm,
            derived.tp_comm_size,
            att_bwd_half,
            row_backward_comm,
            derived.tp_comm_size,
            att_bwd_half,
            "NONE",
            0,
        ));
        items.push(item(
            "attention_column",
            att_fwd_half,
            col_forward_comm,
            derived.tp_comm_size,
            att_bwd_half,
            col_backward_comm,
            0,
            att_bwd_half,
            "NONE",
            0,
        ));

        if args.moe_enable {
            let (moe_fwd, moe_bwd) = layer_compute("mlp_moelayer", "mlp");
            append_moe_sequence(
                items,
                "mlp_moelayer",
                moe_fwd,
                moe_bwd,
                default_compute_time,
                derived.tp_comm_size,
                args,
                frame,
            );
        } else {
            let (mlp_fwd, mlp_bwd) = layer_compute("mlp_row", "mlp");
            let mlp_fwd_half = mlp_fwd / 2;
            let mlp_bwd_half = mlp_bwd / 2;
            items.push(item(
                "mlp_row",
                mlp_fwd_half,
                row_forward_comm,
                derived.tp_comm_size,
                mlp_bwd_half,
                row_backward_comm,
                derived.tp_comm_size,
                mlp_bwd_half,
                "NONE",
                0,
            ));
            items.push(item(
                "mlp_column",
                mlp_fwd_half,
                col_forward_comm,
                derived.tp_comm_size,
                mlp_bwd_half,
                col_backward_comm,
                0,
                mlp_bwd_half,
                "NONE",
                0,
            ));
        }
    }

    let mut final_fwd = get_training_compute_time(compute_cache, "forward", "final", true);
    let final_bwd = get_training_compute_time(compute_cache, "backward", "final", true);
    let (col_forward_comm, col_backward_comm) = if args.tensor_model_parallel_size == 1 {
        ("NONE", "NONE")
    } else {
        ("ALLGATHER", "REDUCESCATTER")
    };
    if args.recompute_activations {
        final_fwd = final_fwd.saturating_mul(2);
    }
    items.push(item(
        "final_column",
        final_fwd / 2,
        col_forward_comm,
        derived.tp_comm_size,
        final_bwd / 2,
        col_backward_comm,
        0,
        final_bwd / 2,
        "NONE",
        0,
    ));
}

fn append_non_sp_aiob_items(
    items: &mut Vec<TrainingWorkItem>,
    args: &TrainingGeneratorArgs,
    derived: Derived,
    compute_cache: &HashMap<String, u64>,
    default_compute_time: u64,
) {
    let (forward_comm, backward_comm) = if args.tensor_model_parallel_size == 1 {
        ("NONE", "NONE")
    } else {
        ("ALLREDUCE", "NONE")
    };
    let emb_compute = get_training_compute_time(compute_cache, "", "embedding", true);
    let grad_backward = get_training_compute_time(compute_cache, "backward", "grad", true);
    items.push(item(
        "embedding_layer",
        emb_compute,
        forward_comm,
        derived.tp_comm_size,
        default_compute_time,
        backward_comm,
        0,
        grad_backward,
        "NONE",
        0,
    ));

    for _ in 0..derived.num_layers_per_stage {
        let mut att_fwd = get_training_compute_time(compute_cache, "forward", "attention", true);
        let att_bwd = get_training_compute_time(compute_cache, "backward", "attention", true);
        if args.recompute_activations {
            att_fwd = att_fwd.saturating_mul(2);
        }
        items.push(item(
            "attention_layer",
            att_fwd,
            forward_comm,
            derived.tp_comm_size,
            att_bwd,
            backward_comm,
            0,
            att_bwd,
            "NONE",
            0,
        ));

        let mlp_fwd = get_training_compute_time(compute_cache, "forward", "mlp", true);
        let mlp_bwd = get_training_compute_time(compute_cache, "backward", "mlp", true);
        items.push(item(
            "mlp_layer",
            mlp_fwd,
            forward_comm,
            derived.tp_comm_size,
            mlp_bwd,
            backward_comm,
            0,
            mlp_bwd,
            "NONE",
            0,
        ));
    }
}

fn item(
    name: &str,
    forward_compute_time: u64,
    forward_comm: &str,
    forward_comm_size: u64,
    backward_compute_time: u64,
    backward_comm: &str,
    backward_comm_size: u64,
    dp_compute_time: u64,
    dp_comm: &str,
    dp_comm_size: u64,
) -> TrainingWorkItem {
    TrainingWorkItem {
        name: name.to_string(),
        placeholder: -1,
        forward_compute_time,
        forward_comm: forward_comm.to_string(),
        forward_comm_size,
        backward_compute_time,
        backward_comm: backward_comm.to_string(),
        backward_comm_size,
        dp_compute_time,
        dp_comm: dp_comm.to_string(),
        dp_comm_size,
        process_time: 100,
    }
}

fn validate_training_args(args: &TrainingGeneratorArgs) -> Result<(), TrainingWorkloadError> {
    let frame = TrainingFrame::parse(&args.frame)?;
    if args.world_size == 0
        || args.tensor_model_parallel_size == 0
        || args.pipeline_model_parallel == 0
        || args.micro_batch == 0
        || args.hidden_size == 0
        || args.num_layers == 0
        || args.vocab_size == 0
    {
        return Err(TrainingWorkloadError::InvalidConfig(
            "world/tp/pp/micro/hidden/layers/vocab must be positive",
        ));
    }
    if args.world_size % (args.tensor_model_parallel_size * args.pipeline_model_parallel) != 0 {
        return Err(TrainingWorkloadError::InvalidConfig(
            "world_size must be divisible by tp * pp",
        ));
    }
    if args.global_batch
        % (args.micro_batch
            * (args.world_size / (args.tensor_model_parallel_size * args.pipeline_model_parallel)))
        != 0
    {
        return Err(TrainingWorkloadError::InvalidConfig(
            "global_batch must be divisible by micro_batch * dp_num",
        ));
    }
    if args.num_layers % args.pipeline_model_parallel != 0 {
        return Err(TrainingWorkloadError::InvalidConfig(
            "num_layers must be divisible by pipeline_model_parallel",
        ));
    }
    if args.moe_enable && !args.enable_sequence_parallel {
        return Err(TrainingWorkloadError::InvalidConfig(
            "moe_enable requires enable_sequence_parallel=true",
        ));
    }
    if args.num_experts == 0 || args.moe_router_topk == 0 {
        return Err(TrainingWorkloadError::InvalidConfig(
            "num_experts and moe_router_topk must be positive",
        ));
    }
    if args.expert_model_parallel_size == 0 {
        return Err(TrainingWorkloadError::InvalidConfig(
            "expert_model_parallel_size must be positive",
        ));
    }
    if args.num_experts % args.expert_model_parallel_size != 0 {
        return Err(TrainingWorkloadError::InvalidConfig(
            "num_experts must be divisible by expert_model_parallel_size",
        ));
    }
    if frame == TrainingFrame::DeepSeek {
        if args.num_attention_heads == 0 {
            return Err(TrainingWorkloadError::InvalidConfig(
                "num_attention_heads must be positive for DeepSeek",
            ));
        }
        if args.tensor_model_parallel_size == 0 {
            return Err(TrainingWorkloadError::InvalidConfig(
                "tensor_model_parallel_size must be positive",
            ));
        }
        if !args.moe_enable {
            return Err(TrainingWorkloadError::InvalidConfig(
                "DeepSeek training path expects moe_enable=true",
            ));
        }
        if args.n_dense_layers > (args.num_layers / args.pipeline_model_parallel) {
            return Err(TrainingWorkloadError::InvalidConfig(
                "n_dense_layers cannot exceed per-stage num_layers",
            ));
        }
    }
    Ok(())
}

fn derive(args: &TrainingGeneratorArgs) -> Result<Derived, TrainingWorkloadError> {
    let frame = TrainingFrame::parse(&args.frame)?;
    let dp_num = args.world_size / (args.tensor_model_parallel_size * args.pipeline_model_parallel);
    let ga_num = args.global_batch / (args.micro_batch * dp_num);
    let num_layers_per_stage = args.num_layers / args.pipeline_model_parallel;

    let multiple = args
        .make_vocab_size_divisible_by
        .checked_mul(args.tensor_model_parallel_size)
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "make_vocab_size_divisible_by overflow",
        ))?;
    let mut padded_vocab_size = args.vocab_size;
    while padded_vocab_size % multiple != 0 {
        padded_vocab_size =
            padded_vocab_size
                .checked_add(1)
                .ok_or(TrainingWorkloadError::InvalidConfig(
                    "padded vocab overflow",
                ))?;
    }

    let tp_comm_size = 2_u64
        .checked_mul(args.micro_batch)
        .and_then(|v| v.checked_mul(args.seq_length))
        .and_then(|v| v.checked_mul(args.hidden_size))
        .ok_or(TrainingWorkloadError::InvalidConfig("tp comm overflow"))?;

    let h = args.hidden_size;
    let tp = args.tensor_model_parallel_size;
    let ffn = args.ffn_hidden_size;
    let seq = args.seq_length;
    let padded_per_tp = padded_vocab_size / tp;

    let embedding_params = 4_u64
        .checked_mul(padded_per_tp)
        .and_then(|v| v.checked_mul(h))
        .and_then(|v| v.checked_add(seq * h))
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "embedding params overflow",
        ))?;
    let qkv_params = h
        .checked_mul(3 * h / tp)
        .ok_or(TrainingWorkloadError::InvalidConfig("qkv params overflow"))?;
    let att_row_params = h
        .checked_mul(h / tp)
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "attention row params overflow",
        ))?;
    let mlp_dense_params = h
        .checked_mul(ffn / tp)
        .and_then(|v| v.checked_mul(2))
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "mlp dense params overflow",
        ))?;

    let (layer_total_params, moe_param_count, final_params) =
        match frame {
            TrainingFrame::Megatron => {
                if args.moe_enable {
                    let num_local_experts = args.num_experts / args.expert_model_parallel_size;
                    let fc_part = ffn
                        .checked_mul(num_local_experts)
                        .and_then(|v| v.checked_div(tp))
                        .ok_or(TrainingWorkloadError::InvalidConfig(
                            "moe fc partition overflow",
                        ))?;
                    let moe_layer_params = h
                        .checked_mul(fc_part)
                        .and_then(|v| v.checked_mul(2))
                        .ok_or(TrainingWorkloadError::InvalidConfig(
                            "moe layer params overflow",
                        ))?;
                    let per_layer = qkv_params
                        .checked_add(att_row_params)
                        .and_then(|v| v.checked_add(2 * h))
                        .and_then(|v| v.checked_add(moe_layer_params))
                        .ok_or(TrainingWorkloadError::InvalidConfig(
                            "per layer params overflow",
                        ))?;
                    (
                        per_layer.checked_mul(num_layers_per_stage).ok_or(
                            TrainingWorkloadError::InvalidConfig("total layer params overflow"),
                        )?,
                        moe_layer_params.checked_mul(num_layers_per_stage).ok_or(
                            TrainingWorkloadError::InvalidConfig("moe param count overflow"),
                        )?,
                        h.checked_mul(padded_per_tp).ok_or(
                            TrainingWorkloadError::InvalidConfig("final params overflow"),
                        )?,
                    )
                } else if args.enable_sequence_parallel {
                    let per_layer = qkv_params
                        .checked_add(att_row_params)
                        .and_then(|v| v.checked_add(mlp_dense_params))
                        .and_then(|v| v.checked_add(2 * h))
                        .ok_or(TrainingWorkloadError::InvalidConfig(
                            "per layer params overflow",
                        ))?;
                    (
                        per_layer.checked_mul(num_layers_per_stage).ok_or(
                            TrainingWorkloadError::InvalidConfig("total layer params overflow"),
                        )?,
                        0,
                        h.checked_mul(padded_per_tp).ok_or(
                            TrainingWorkloadError::InvalidConfig("final params overflow"),
                        )?,
                    )
                } else {
                    let per_layer = qkv_params
                        .checked_add(att_row_params)
                        .and_then(|v| v.checked_add(mlp_dense_params))
                        .ok_or(TrainingWorkloadError::InvalidConfig(
                            "per layer params overflow",
                        ))?;
                    (
                        per_layer.checked_mul(num_layers_per_stage).ok_or(
                            TrainingWorkloadError::InvalidConfig("total layer params overflow"),
                        )?,
                        0,
                        0,
                    )
                }
            }
            TrainingFrame::DeepSeek => {
                let qk_dim = args
                    .qk_nope_dim
                    .checked_add(args.qk_rope_dim)
                    .ok_or(TrainingWorkloadError::InvalidConfig("qk dim overflow"))?;
                let q_proj = args.num_attention_heads.checked_mul(qk_dim).ok_or(
                    TrainingWorkloadError::InvalidConfig("q projection overflow"),
                )?;
                let kv_proj = args
                    .num_attention_heads
                    .checked_mul(args.qk_nope_dim + args.v_head_dim)
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "kv projection overflow",
                    ))?;
                let o_in = args
                    .num_attention_heads
                    .checked_mul(args.v_head_dim)
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "o projection overflow",
                    ))?;
                let attention_params = h
                    .checked_mul(args.q_lora_rank)
                    .and_then(|v| v.checked_add(args.q_lora_rank * (q_proj / tp)))
                    .and_then(|v| v.checked_add(h * (args.kv_lora_rank + args.qk_rope_dim)))
                    .and_then(|v| v.checked_add(args.kv_lora_rank * (kv_proj / tp)))
                    .and_then(|v| v.checked_add(h * (o_in / tp)))
                    .and_then(|v| v.checked_add(2 * args.q_lora_rank))
                    .and_then(|v| v.checked_add(2 * args.kv_lora_rank))
                    .and_then(|v| v.checked_add(2 * h))
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "deepseek attention params overflow",
                    ))?;

                let num_local_experts = args.num_experts / args.expert_model_parallel_size;
                let fc_part = ffn
                    .checked_mul(num_local_experts)
                    .and_then(|v| v.checked_div(tp))
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "deepseek moe fc partition overflow",
                    ))?;
                let moe_layer_params = h
                    .checked_mul(fc_part)
                    .and_then(|v| v.checked_mul(3))
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "deepseek moe layer params overflow",
                    ))?;
                let shared_expert_params = if args.n_shared_expert > 0 {
                    mlp_dense_params
                } else {
                    0
                };
                let dense_layers = args.n_dense_layers.min(num_layers_per_stage);
                let moe_layers = num_layers_per_stage - dense_layers;
                let dense_stack = attention_params
                    .checked_add(mlp_dense_params)
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "deepseek dense stack overflow",
                    ))?
                    .checked_mul(dense_layers)
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "deepseek dense stack overflow",
                    ))?;
                let moe_stack_per = attention_params
                    .checked_add(moe_layer_params)
                    .and_then(|v| v.checked_add(shared_expert_params))
                    .ok_or(TrainingWorkloadError::InvalidConfig(
                        "deepseek moe stack overflow",
                    ))?;
                let moe_stack = moe_stack_per.checked_mul(moe_layers).ok_or(
                    TrainingWorkloadError::InvalidConfig("deepseek moe stack overflow"),
                )?;
                (
                    dense_stack.checked_add(moe_stack).ok_or(
                        TrainingWorkloadError::InvalidConfig("deepseek layer total overflow"),
                    )?,
                    moe_layer_params.checked_mul(moe_layers).ok_or(
                        TrainingWorkloadError::InvalidConfig("deepseek moe param count overflow"),
                    )?,
                    h.checked_mul(padded_per_tp)
                        .ok_or(TrainingWorkloadError::InvalidConfig(
                            "final params overflow",
                        ))?,
                )
            }
        };

    let total_params = embedding_params
        .checked_add(layer_total_params)
        .and_then(|v| v.checked_add(final_params))
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "total params overflow",
        ))?;

    Ok(Derived {
        dp_num,
        ga_num,
        num_layers_per_stage,
        padded_vocab_size,
        tp_comm_size,
        total_params,
        moe_param_count,
    })
}

fn load_training_aiob_compute_cache(
    args: &TrainingGeneratorArgs,
) -> Result<HashMap<String, u64>, TrainingWorkloadError> {
    if let Some(path) = args.aiob_profile.as_ref() {
        let text = fs::read_to_string(path)?;
        return Ok(parse_training_aiob_compute_cache(
            &text,
            args.recompute_activations,
        ));
    }

    let default_path = default_training_aiob_profile_path(args);
    if default_path.is_file() {
        let text = fs::read_to_string(&default_path)?;
        return Ok(parse_training_aiob_compute_cache(
            &text,
            args.recompute_activations,
        ));
    }

    if args.aiob_enable {
        return Err(TrainingWorkloadError::MissingAiobProfile(default_path));
    }

    Ok(HashMap::new())
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

fn parse_training_aiob_compute_cache(
    content: &str,
    recompute_activations: bool,
) -> HashMap<String, u64> {
    let mut attention_avg_sum = 0.0_f64;
    let mut mlp_avg_sum = 0.0_f64;
    let mut grad_forward = 0.0_f64;
    let mut grad_backward = 0.0_f64;
    let mut other_avgs = HashMap::<String, f64>::new();

    let mut per_layer_time_map = HashMap::<&str, u64>::from([
        ("attention_linear_q_lora", 0),
        ("attention_q_column", 0),
        ("attention_linear_kv_lora", 0),
        ("attention_kv_column", 0),
        ("attention_o_row", 0),
    ]);

    let mut current_section = String::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(header) = trimmed.strip_suffix(':') {
            if !header.is_empty()
                && header
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                current_section.clear();
                current_section.push_str(header);
            }
        }

        let avg_match = parse_time_value(trimmed, "time_gpu_avg:");
        let min_match = parse_time_value(trimmed, "time_gpu_min:");

        if current_section == "param_time" {
            if let Some(min_ms) = min_match {
                grad_forward = min_ms * 1000.0;
            }
            if let Some(avg_ms) = avg_match {
                grad_backward = avg_ms * 1000.0;
            }
            continue;
        }

        if let Some(avg_ms) = avg_match {
            let avg_value = avg_ms * 1000.0;
            if current_section.contains("atten") || current_section == "layernorm" {
                if recompute_activations && current_section.contains("flash") {
                    attention_avg_sum += avg_value * 2.0;
                } else {
                    attention_avg_sum += avg_value;
                }
            } else if current_section.contains("mlp") || current_section == "layernorm2" {
                mlp_avg_sum += avg_value;
            } else if !current_section.is_empty() {
                other_avgs.insert(current_section.clone(), avg_value);
            }

            if let Some(slot) = per_layer_time_map.get_mut(current_section.as_str()) {
                *slot = avg_value.round() as u64;
            }
        }
    }

    let attention_forward = attention_avg_sum.round() as u64;
    let attention_backward = attention_forward;
    let mlp_forward = mlp_avg_sum.round() as u64;
    let mlp_backward = mlp_forward;

    let mut cache = HashMap::new();
    cache.insert("attention_forward".to_string(), attention_forward);
    cache.insert("attention_backward".to_string(), attention_backward);
    cache.insert("mlp_forward".to_string(), mlp_forward);
    cache.insert("mlp_backward".to_string(), mlp_backward);
    cache.insert("grad_forward".to_string(), grad_forward.round() as u64);
    cache.insert("grad_backward".to_string(), grad_backward.round() as u64);

    for (key, value) in other_avgs {
        if key != "param_time" {
            cache.insert(key, value.round() as u64);
        }
    }
    for (key, value) in per_layer_time_map {
        cache.insert(key.to_string(), value);
    }
    cache
}

fn parse_time_value(line: &str, prefix: &str) -> Option<f64> {
    let (_, rest) = line.split_once(prefix)?;
    let token = rest.trim().split_whitespace().next()?.trim_end_matches(',');
    token.parse::<f64>().ok()
}

fn get_training_compute_time(
    cache: &HashMap<String, u64>,
    forward_or_backward: &str,
    stage: &str,
    warn_default: bool,
) -> u64 {
    if let Some(v) = cache.get(stage) {
        return *v;
    }

    let prefix = if stage == "grad" {
        format!("{stage}_{forward_or_backward}")
    } else if stage == "embedding" {
        "Emb".to_string()
    } else if stage == "final" {
        format!("attention_{forward_or_backward}")
    } else if forward_or_backward.is_empty() {
        stage.to_string()
    } else {
        format!("{stage}_{forward_or_backward}")
    };

    if let Some(v) = cache.get(&prefix) {
        return *v;
    }
    if warn_default {
        // Keep behavior silent in Rust output; this flag mirrors Python callsites.
    }
    1
}

fn bool_python_style(v: bool) -> &'static str {
    if v { "True" } else { "False" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> TrainingGeneratorArgs {
        TrainingGeneratorArgs {
            gpu_type: "A100".to_string(),
            frame: "Megatron".to_string(),
            model_name: "gpt_13B".to_string(),
            world_size: 128,
            tensor_model_parallel_size: 8,
            pipeline_model_parallel: 2,
            expert_model_parallel_size: 1,
            global_batch: 128,
            micro_batch: 1,
            num_layers: 40,
            seq_length: 1024,
            hidden_size: 2048,
            vocab_size: 32000,
            ffn_hidden_size: 8192,
            num_attention_heads: 32,
            num_experts: 1,
            moe_router_topk: 1,
            make_vocab_size_divisible_by: 128,
            n_dense_layers: 3,
            n_shared_expert: 2,
            qk_rope_dim: 64,
            qk_nope_dim: 128,
            q_lora_rank: 1536,
            kv_lora_rank: 512,
            v_head_dim: 128,
            enable_sequence_parallel: true,
            recompute_activations: false,
            use_flash_attn: true,
            moe_enable: false,
            moe_grouped_gemm: false,
            aiob_enable: false,
            aiob_profile: None,
        }
    }

    #[test]
    fn derive_matches_characterized_counts_for_gpt13b_sp_case() {
        let derived = derive(&base_args()).expect("derive");
        assert_eq!(derived.dp_num, 8);
        assert_eq!(derived.ga_num, 16);
        assert_eq!(derived.num_layers_per_stage, 20);
        assert_eq!(derived.padded_vocab_size, 32768);
        assert_eq!(derived.tp_comm_size, 4_194_304);
        assert_eq!(derived.total_params, 169_951_232);
    }

    #[test]
    fn derive_non_sp_has_smaller_total_params() {
        let mut args = base_args();
        args.enable_sequence_parallel = false;
        let derived = derive(&args).expect("derive");
        assert_eq!(derived.total_params, 161_480_704);
    }

    #[test]
    fn derive_megatron_moe_matches_characterized_counts() {
        let mut args = base_args();
        args.model_name = "gpt_moe_test".to_string();
        args.world_size = 8;
        args.tensor_model_parallel_size = 4;
        args.pipeline_model_parallel = 1;
        args.expert_model_parallel_size = 2;
        args.global_batch = 2;
        args.micro_batch = 1;
        args.num_layers = 4;
        args.seq_length = 16;
        args.hidden_size = 1024;
        args.ffn_hidden_size = 4096;
        args.num_attention_heads = 4;
        args.vocab_size = 32000;
        args.use_flash_attn = false;
        args.moe_enable = true;
        args.num_experts = 8;
        args.moe_router_topk = 2;

        let derived = derive(&args).expect("derive");
        assert_eq!(derived.total_params, 79_060_992);
        assert_eq!(derived.moe_param_count, 33_554_432);
    }

    #[test]
    fn output_stem_matches_legacy_format() {
        let stem = training_output_stem(&base_args());
        assert_eq!(
            stem,
            "A100-gpt_13B-world_size128-tp8-pp2-ep1-gbs128-mbs1-seq1024-MOE-False-GEMM-False-flash_attn-True"
        );
    }

    #[test]
    fn parse_training_aiob_cache_maps_expected_keys() {
        let cache = parse_training_aiob_compute_cache(
            r#"
param_time:
time_gpu_min: 0.3
time_gpu_avg: 0.4
flash_atten:
time_gpu_avg: 1.0
mlp:
time_gpu_avg: 2.0
Emb:
time_gpu_avg: 3.0
"#,
            true,
        );
        assert_eq!(cache.get("grad_forward"), Some(&300));
        assert_eq!(cache.get("grad_backward"), Some(&400));
        assert_eq!(cache.get("attention_forward"), Some(&2000));
        assert_eq!(cache.get("attention_backward"), Some(&2000));
        assert_eq!(cache.get("mlp_forward"), Some(&2000));
        assert_eq!(cache.get("mlp_backward"), Some(&2000));
        assert_eq!(cache.get("Emb"), Some(&3000));
    }
}

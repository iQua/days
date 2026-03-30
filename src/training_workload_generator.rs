use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct TrainingGeneratorArgs {
    pub gpu_type: String,
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
    pub make_vocab_size_divisible_by: u64,
    pub enable_sequence_parallel: bool,
    pub recompute_activations: bool,
    pub use_flash_attn: bool,
    pub moe_enable: bool,
    pub moe_grouped_gemm: bool,
    pub aiob_enable: bool,
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
    InvalidConfig(&'static str),
    Unsupported(&'static str),
}

impl std::fmt::Display for TrainingWorkloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
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
}

pub fn generate_training_workload_file(
    result_dir: &Path,
    args: &TrainingGeneratorArgs,
) -> Result<PathBuf, TrainingWorkloadError> {
    validate_training_args(args)?;
    let derived = derive(args)?;
    let payload = generate_training_workload_payload(args, derived);

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

fn generate_training_workload_payload(args: &TrainingGeneratorArgs, derived: Derived) -> String {
    let mut items = Vec::new();

    let total_params = derived.total_params;
    items.push(item(
        "grad_norm",
        1,
        "ALLGATHER",
        2 * total_params,
        1,
        "NONE",
        0,
        1,
        "REDUCESCATTER",
        4 * total_params,
    ));

    if args.expert_model_parallel_size != derived.dp_num {
        items.push(item(
            "moe_grad_norm1",
            1,
            "NONE",
            0,
            1,
            "NONE",
            0,
            1,
            "ALLGATHER_DP_EP",
            0,
        ));
        items.push(item(
            "moe_grad_norm2",
            1,
            "NONE",
            0,
            1,
            "NONE",
            0,
            1,
            "REDUCESCATTER_DP_EP",
            0,
        ));
    }

    for _ in 0..derived.ga_num {
        items.push(item(
            "embedding_layer",
            1,
            "NONE",
            derived.tp_comm_size,
            1,
            "NONE",
            derived.tp_comm_size,
            1,
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
                "attention_column",
                1,
                "ALLGATHER",
                attention_forward_comm_size,
                1,
                "NONE",
                0,
                1,
                "NONE",
                0,
            ));
            items.push(item(
                "attention_row",
                1,
                "REDUCESCATTER",
                attention_forward_comm_size,
                1,
                "NONE",
                derived.tp_comm_size,
                1,
                "NONE",
                0,
            ));
            items.push(item(
                "mlp_column",
                1,
                "ALLGATHER",
                derived.tp_comm_size,
                1,
                "NONE",
                0,
                1,
                "NONE",
                0,
            ));
            items.push(item(
                "mlp_row",
                1,
                "REDUCESCATTER",
                derived.tp_comm_size,
                1,
                "NONE",
                derived.tp_comm_size,
                1,
                "NONE",
                0,
            ));
        }

        items.push(item(
            "final_column",
            1,
            "ALLGATHER",
            derived.tp_comm_size,
            1,
            "NONE",
            0,
            1,
            "NONE",
            0,
        ));
        items.push(item(
            "embedding_norm",
            1,
            "ALLREDUCE",
            args.vocab_size * args.hidden_size * 2,
            1,
            "NONE",
            0,
            1,
            "NONE",
            0,
        ));
    }

    for idx in 1..=3 {
        items.push(item(
            &format!("cross_entropy{idx}"),
            0,
            "ALLREDUCE",
            args.seq_length * args.micro_batch * 4,
            0,
            "NONE",
            0,
            0,
            "NONE",
            0,
        ));
    }

    for idx in 1..=4 {
        items.push(item(
            &format!("optimizer{idx}"),
            0,
            "ALLREDUCE",
            4,
            0,
            "NONE",
            0,
            0,
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
        pp_comm,
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
    if args.moe_enable {
        return Err(TrainingWorkloadError::Unsupported(
            "moe_enable=true path is not implemented in Rust training generator yet",
        ));
    }
    if args.aiob_enable {
        return Err(TrainingWorkloadError::Unsupported(
            "aiob_enable=true path is not implemented in Rust training generator yet",
        ));
    }
    if !args.enable_sequence_parallel {
        return Err(TrainingWorkloadError::Unsupported(
            "enable_sequence_parallel=false path is not implemented in Rust training generator yet",
        ));
    }
    Ok(())
}

fn derive(args: &TrainingGeneratorArgs) -> Result<Derived, TrainingWorkloadError> {
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
    let mlp_col_params = h
        .checked_mul(ffn / tp)
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "mlp column params overflow",
        ))?;
    let mlp_row_params = h
        .checked_mul(ffn / tp)
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "mlp row params overflow",
        ))?;
    let fused_ln_params = 2 * h;
    let per_layer_params = qkv_params
        .checked_add(att_row_params)
        .and_then(|v| v.checked_add(mlp_col_params))
        .and_then(|v| v.checked_add(mlp_row_params))
        .and_then(|v| v.checked_add(fused_ln_params))
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "per layer params overflow",
        ))?;
    let final_params = h
        .checked_mul(padded_per_tp)
        .ok_or(TrainingWorkloadError::InvalidConfig(
            "final params overflow",
        ))?;

    let total_params = embedding_params
        .checked_add(per_layer_params.checked_mul(num_layers_per_stage).ok_or(
            TrainingWorkloadError::InvalidConfig("total layer params overflow"),
        )?)
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
    })
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
            make_vocab_size_divisible_by: 128,
            enable_sequence_parallel: true,
            recompute_activations: false,
            use_flash_attn: true,
            moe_enable: false,
            moe_grouped_gemm: false,
            aiob_enable: false,
        }
    }

    #[test]
    fn derive_matches_characterized_counts_for_gpt13b_case() {
        let derived = derive(&base_args()).expect("derive");
        assert_eq!(derived.dp_num, 8);
        assert_eq!(derived.ga_num, 16);
        assert_eq!(derived.num_layers_per_stage, 20);
        assert_eq!(derived.padded_vocab_size, 32768);
        assert_eq!(derived.tp_comm_size, 4_194_304);
        assert_eq!(derived.total_params, 169_951_232);
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
    fn rejects_non_sp_path_for_minimal_subset() {
        let mut args = base_args();
        args.enable_sequence_parallel = false;
        let err = generate_training_workload_file(Path::new("."), &args).expect_err("must fail");
        assert!(format!("{err}").contains("enable_sequence_parallel=false"));
    }
}

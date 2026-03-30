use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use toml::Value;

pub const FP8_FACTOR: f64 = (1.0 + 4.0 / 128.0) / 2.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InferencePhase {
    Decode,
    Prefill,
}

impl InferencePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Decode => "decode",
            Self::Prefill => "prefill",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelFamily {
    DeepSeek,
    Qwen3Moe,
    Qwen3Next,
}

impl ModelFamily {
    pub fn from_model_name(model_name: &str) -> Result<Self, WorkloadGenError> {
        if model_name.contains("DeepSeek") {
            return Ok(Self::DeepSeek);
        }
        if model_name.contains("Qwen3-Moe") {
            return Ok(Self::Qwen3Moe);
        }
        if model_name.contains("Qwen3-Next") {
            return Ok(Self::Qwen3Next);
        }
        Err(WorkloadGenError::UnknownModel(model_name.to_string()))
    }

    fn frame_name(self) -> &'static str {
        match self {
            Self::DeepSeek => "DeepSeek",
            Self::Qwen3Moe => "Qwen3-Moe",
            Self::Qwen3Next => "Qwen3-Next",
        }
    }
}

#[derive(Debug, Clone)]
pub struct GeneratorArgs {
    pub model_name: String,
    pub world_size: u64,
    pub tensor_model_parallel_size: u64,
    pub expert_model_parallel_size: u64,
    pub pipeline_model_parallel: u64,
    pub seq_length: u64,
    pub micro_batch: u64,
    pub phase: InferencePhase,
    pub aiob_enable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
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

impl WorkItem {
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
pub enum WorkloadGenError {
    Io(std::io::Error),
    Toml(toml::de::Error),
    UnknownModel(String),
    MissingConfigField(&'static str),
    InvalidConfig(&'static str),
    Unsupported(&'static str),
}

impl std::fmt::Display for WorkloadGenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Toml(err) => write!(f, "toml error: {err}"),
            Self::UnknownModel(name) => write!(f, "invalid model name: {name}"),
            Self::MissingConfigField(key) => write!(f, "missing config field: {key}"),
            Self::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported option: {msg}"),
        }
    }
}

impl std::error::Error for WorkloadGenError {}

impl From<std::io::Error> for WorkloadGenError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<toml::de::Error> for WorkloadGenError {
    fn from(value: toml::de::Error) -> Self {
        Self::Toml(value)
    }
}

pub fn generate_workload_file(
    config_path: &Path,
    result_dir: &Path,
    args: &GeneratorArgs,
) -> Result<PathBuf, WorkloadGenError> {
    if args.aiob_enable {
        return Err(WorkloadGenError::Unsupported(
            "--aiob-enable path is not implemented in Rust generator yet",
        ));
    }

    let config_text = fs::read_to_string(config_path)?;
    let config_toml: Value = toml::from_str(&config_text)?;
    let payload = generate_workload_payload(args, &config_toml, &HashMap::new())?;

    fs::create_dir_all(result_dir)?;
    let filename = format!(
        "{}-world_size{}-tp{}-pp{}-ep{}-bs{}-seq{}-{}.txt",
        args.model_name,
        args.world_size,
        args.tensor_model_parallel_size,
        args.pipeline_model_parallel,
        args.expert_model_parallel_size,
        args.micro_batch,
        args.seq_length,
        args.phase.as_str()
    );
    let output_path = result_dir.join(filename);
    fs::write(&output_path, payload)?;
    Ok(output_path)
}

pub fn generate_workload_payload(
    args: &GeneratorArgs,
    config_toml: &Value,
    compute_cache: &HashMap<String, u64>,
) -> Result<String, WorkloadGenError> {
    let family = ModelFamily::from_model_name(&args.model_name)?;
    validate_parallel_args(args)?;

    let hidden_size = config_u64(config_toml, "hidden_size")?;
    let topk = match family {
        ModelFamily::DeepSeek => config_u64(config_toml, "moe_router_topk")?,
        ModelFamily::Qwen3Moe | ModelFamily::Qwen3Next => {
            config_u64(config_toml, "num_experts_per_tok")?
        }
    };

    let m = match args.phase {
        InferencePhase::Decode => args.micro_batch,
        InferencePhase::Prefill => args.seq_length,
    };

    let tp_comm_size = 2 * m * hidden_size;
    let ep_combine_size = tp_comm_size
        .checked_mul(topk)
        .ok_or(WorkloadGenError::InvalidConfig("ep combine overflow"))?
        / args.tensor_model_parallel_size;

    let ep_dispatch_size =
        if family.frame_name().contains("DeepSeek") || family.frame_name().contains("Qwen3") {
            (ep_combine_size as f64 * FP8_FACTOR) as u64
        } else {
            ep_combine_size
        };

    let layer_names = build_layer_sequence(family, config_toml)?;
    let mut items = Vec::new();
    for name in layer_names {
        append_layer_items(
            &mut items,
            family,
            &name,
            tp_comm_size,
            ep_dispatch_size,
            ep_combine_size,
            compute_cache,
        );
    }

    let pp_comm_value = 2_u64
        .checked_mul(args.micro_batch)
        .and_then(|v| v.checked_mul(args.seq_length))
        .and_then(|v| v.checked_mul(hidden_size))
        .ok_or(WorkloadGenError::InvalidConfig("pp comm overflow"))?
        * if args.pipeline_model_parallel > 1 {
            1
        } else {
            0
        };

    let header = format!(
        "HYBRID_TRANSFORMER_FWD_IN_BCKWD model_parallel_NPU_group: {} ep: {} pp: {} all_gpus: {} mode: 1 vpp: 1 ga: 1 checkpoints: 0 checkpoint_initiates: 0 pp_comm: {}",
        args.tensor_model_parallel_size,
        args.expert_model_parallel_size,
        args.pipeline_model_parallel,
        args.world_size,
        pp_comm_value
    );

    let mut out = String::new();
    out.push_str(&header);
    out.push('\n');
    out.push_str(&items.len().to_string());
    out.push('\n');
    for item in &items {
        out.push_str(&item.to_tsv_line());
        out.push('\n');
    }

    Ok(out)
}

fn validate_parallel_args(args: &GeneratorArgs) -> Result<(), WorkloadGenError> {
    if args.tensor_model_parallel_size == 0
        || args.expert_model_parallel_size == 0
        || args.world_size == 0
    {
        return Err(WorkloadGenError::InvalidConfig(
            "world/tp/ep parallel sizes must be positive",
        ));
    }
    if args.world_size < args.tensor_model_parallel_size
        || args.world_size < args.expert_model_parallel_size
    {
        return Err(WorkloadGenError::InvalidConfig(
            "world_size must be >= tp and >= ep",
        ));
    }
    Ok(())
}

fn build_layer_sequence(
    family: ModelFamily,
    config_toml: &Value,
) -> Result<Vec<String>, WorkloadGenError> {
    match family {
        ModelFamily::DeepSeek => {
            let num_layers = config_u64(config_toml, "num_layers")?;
            let dense_layer = config_u64(config_toml, "dense_layer")?;
            let shared_experts = config_u64(config_toml, "shared_experts")?;

            let mut names = Vec::new();
            for i in 0..num_layers {
                names.push("attention_layer".to_string());
                if i < dense_layer {
                    names.push("dense_mlp".to_string());
                } else {
                    for _ in 0..shared_experts {
                        names.push("shared_experts".to_string());
                    }
                    names.push("moe_expert".to_string());
                }
            }
            Ok(names)
        }
        ModelFamily::Qwen3Moe => {
            let num_layers = config_u64(config_toml, "num_hidden_layers")?;
            let mut names = Vec::new();
            for _ in 0..num_layers {
                names.push("attention_norm".to_string());
                names.push("attention_layer".to_string());
                names.push("moe_norm".to_string());
                names.push("moe_route".to_string());
                names.push("moe_expert".to_string());
            }
            Ok(names)
        }
        ModelFamily::Qwen3Next => {
            let num_layers = config_u64(config_toml, "num_hidden_layers")?;
            let interval = config_u64(config_toml, "full_attention_interval")?;
            if interval == 0 {
                return Err(WorkloadGenError::InvalidConfig(
                    "full_attention_interval must be positive",
                ));
            }
            let mut names = Vec::new();
            for i in 0..num_layers {
                names.push("attention_norm".to_string());
                if (i + 1) % interval == 0 {
                    names.push("attention_layer".to_string());
                } else {
                    names.push("attention_gdn".to_string());
                }
                names.push("moe_norm".to_string());
                names.push("moe_route".to_string());
                names.push("moe_expert".to_string());
            }
            Ok(names)
        }
    }
}

fn append_layer_items(
    items: &mut Vec<WorkItem>,
    family: ModelFamily,
    name: &str,
    tp_comm_size: u64,
    ep_dispatch_size: u64,
    ep_combine_size: u64,
    compute_cache: &HashMap<String, u64>,
) {
    let default_compute_time = 1;

    if name.contains("norm") || name.contains("shared_expert") {
        let compute_time = get_compute_time(compute_cache, name);
        items.push(WorkItem {
            name: name.to_string(),
            placeholder: -1,
            forward_compute_time: compute_time,
            forward_comm: "NONE".to_string(),
            forward_comm_size: 0,
            backward_compute_time: 0,
            backward_comm: "NONE".to_string(),
            backward_comm_size: 0,
            dp_compute_time: 0,
            dp_comm: "NONE".to_string(),
            dp_comm_size: 0,
            process_time: 100,
        });
        return;
    }

    if name.contains("attention") || name.contains("dense_mlp") {
        let compute_time = get_compute_time(compute_cache, name);
        items.push(WorkItem {
            name: name.to_string(),
            placeholder: -1,
            forward_compute_time: compute_time,
            forward_comm: "ALLREDUCE".to_string(),
            forward_comm_size: tp_comm_size,
            backward_compute_time: 0,
            backward_comm: "NONE".to_string(),
            backward_comm_size: 0,
            dp_compute_time: 0,
            dp_comm: "NONE".to_string(),
            dp_comm_size: 0,
            process_time: 100,
        });
        return;
    }

    if name.contains("moe_route") {
        let compute_time = get_compute_time(compute_cache, name);
        items.push(WorkItem {
            name: "moe_route".to_string(),
            placeholder: -1,
            forward_compute_time: compute_time,
            forward_comm: "ALLTOALL_EP".to_string(),
            forward_comm_size: ep_dispatch_size,
            backward_compute_time: default_compute_time,
            backward_comm: "NONE".to_string(),
            backward_comm_size: 0,
            dp_compute_time: default_compute_time,
            dp_comm: "NONE".to_string(),
            dp_comm_size: 0,
            process_time: 100,
        });
        return;
    }

    if name.contains("moe_expert") {
        let compute_time = get_compute_time(compute_cache, name);
        if family == ModelFamily::DeepSeek {
            items.push(WorkItem {
                name: "moe_route".to_string(),
                placeholder: -1,
                forward_compute_time: default_compute_time,
                forward_comm: "ALLTOALL_EP".to_string(),
                forward_comm_size: ep_dispatch_size,
                backward_compute_time: default_compute_time,
                backward_comm: "NONE".to_string(),
                backward_comm_size: 0,
                dp_compute_time: default_compute_time,
                dp_comm: "NONE".to_string(),
                dp_comm_size: 0,
                process_time: 100,
            });
        }

        items.push(WorkItem {
            name: "moe_expert".to_string(),
            placeholder: -1,
            forward_compute_time: compute_time,
            forward_comm: "ALLTOALL_EP".to_string(),
            forward_comm_size: ep_combine_size,
            backward_compute_time: default_compute_time,
            backward_comm: "NONE".to_string(),
            backward_comm_size: 0,
            dp_compute_time: default_compute_time,
            dp_comm: "NONE".to_string(),
            dp_comm_size: 0,
            process_time: 100,
        });
    }
}

fn get_compute_time(cache: &HashMap<String, u64>, stage: &str) -> u64 {
    *cache.get(stage).unwrap_or(&1)
}

fn config_u64(config: &Value, key: &'static str) -> Result<u64, WorkloadGenError> {
    config
        .get(key)
        .and_then(|v| v.as_integer())
        .and_then(|v| u64::try_from(v).ok())
        .ok_or(WorkloadGenError::MissingConfigField(key))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args(model_name: &str, phase: InferencePhase) -> GeneratorArgs {
        GeneratorArgs {
            model_name: model_name.to_string(),
            world_size: 32,
            tensor_model_parallel_size: 8,
            expert_model_parallel_size: 32,
            pipeline_model_parallel: 1,
            seq_length: 16,
            micro_batch: 2,
            phase,
            aiob_enable: false,
        }
    }

    #[test]
    fn deepseek_layer_count_matches_formula() {
        let cfg: Value = toml::from_str(
            r#"
num_layers = 61
dense_layer = 3
shared_experts = 1
hidden_size = 7168
moe_router_topk = 8
"#,
        )
        .expect("toml");
        let payload = generate_workload_payload(
            &base_args("DeepSeek-671B", InferencePhase::Decode),
            &cfg,
            &HashMap::new(),
        )
        .expect("payload");
        let mut lines = payload.lines();
        let _header = lines.next().expect("header");
        let count = lines
            .next()
            .expect("count")
            .parse::<usize>()
            .expect("count parse");
        assert_eq!(count, 238);
    }

    #[test]
    fn qwen3_next_attention_schedule_follows_interval() {
        let cfg: Value = toml::from_str(
            r#"
num_hidden_layers = 4
full_attention_interval = 4
hidden_size = 2048
num_experts_per_tok = 10
"#,
        )
        .expect("toml");
        let payload = generate_workload_payload(
            &base_args("Qwen3-Next-80B", InferencePhase::Prefill),
            &cfg,
            &HashMap::new(),
        )
        .expect("payload");
        let lines: Vec<&str> = payload.lines().collect();
        // Header + count + 20 rows => row 2 starts layer 0.
        assert!(lines[3].contains("attention_gdn"));
        assert!(lines[8].contains("attention_gdn"));
        assert!(lines[13].contains("attention_gdn"));
        assert!(lines[18].contains("attention_layer"));
    }

    #[test]
    fn aiob_flag_is_rejected_in_file_generation() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cfg_path = tmp.path().join("cfg.toml");
        fs::write(
            &cfg_path,
            r#"
num_hidden_layers = 1
full_attention_interval = 4
hidden_size = 2048
num_experts_per_tok = 10
"#,
        )
        .expect("write");
        let mut args = base_args("Qwen3-Next-80B", InferencePhase::Decode);
        args.aiob_enable = true;
        let err = generate_workload_file(&cfg_path, tmp.path(), &args).expect_err("must fail");
        assert!(format!("{err}").contains("--aiob-enable"));
    }
}

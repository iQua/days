use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use std::path::{Path, PathBuf};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("training_workload")
        .join(name)
}

fn read_expected(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("expected file should be readable")
}

#[test]
fn gpt13b_gbs128_seq1024_matches_characterization_fixture() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--gpu_type",
        "A100",
        "--model_name",
        "gpt_13B",
        "--world_size",
        "128",
        "--tensor_model_parallel_size",
        "8",
        "--pipeline_model_parallel",
        "2",
        "--expert_model_parallel_size",
        "1",
        "--global_batch",
        "128",
        "--micro_batch",
        "1",
        "--num_layers",
        "40",
        "--seq_length",
        "1024",
        "--hidden_size",
        "2048",
        "--vocab_size",
        "32000",
        "--enable_sequence_parallel",
        "--use_flash_attn",
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir
        .join("A100-gpt_13B-world_size128-tp8-pp2-ep1-gbs128-mbs1-seq1024-MOE-False-GEMM-False-flash_attn-True.txt");
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    let expected = read_expected(fixture_path("expected_a100_gpt13b_gbs128_seq1024.txt"));
    assert_eq!(generated, expected);
}

#[test]
fn gpt13b_gbs1024_seq4096_matches_characterization_fixture() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--gpu_type",
        "A100",
        "--model_name",
        "gpt_13B",
        "--world_size",
        "128",
        "--tensor_model_parallel_size",
        "8",
        "--pipeline_model_parallel",
        "2",
        "--expert_model_parallel_size",
        "1",
        "--global_batch",
        "1024",
        "--micro_batch",
        "1",
        "--num_layers",
        "40",
        "--seq_length",
        "4096",
        "--hidden_size",
        "5120",
        "--vocab_size",
        "32000",
        "--enable_sequence_parallel",
        "--use_flash_attn",
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir
        .join("A100-gpt_13B-world_size128-tp8-pp2-ep1-gbs1024-mbs1-seq4096-MOE-False-GEMM-False-flash_attn-True.txt");
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    let expected = read_expected(fixture_path("expected_a100_gpt13b_gbs1024_seq4096.txt"));
    assert_eq!(generated, expected);
}

#[test]
fn megatron_moe_sp_matches_characterization_fixture() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--frame",
        "Megatron",
        "--gpu_type",
        "A100",
        "--model_name",
        "gpt_moe_test",
        "--world_size",
        "8",
        "--tensor_model_parallel_size",
        "4",
        "--pipeline_model_parallel",
        "1",
        "--expert_model_parallel_size",
        "2",
        "--global_batch",
        "2",
        "--micro_batch",
        "1",
        "--num_layers",
        "4",
        "--seq_length",
        "16",
        "--hidden_size",
        "1024",
        "--ffn_hidden_size",
        "4096",
        "--vocab_size",
        "32000",
        "--enable_sequence_parallel",
        "--moe_enable",
        "--num_experts",
        "8",
        "--moe_router_topk",
        "2",
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join(
        "A100-gpt_moe_test-world_size8-tp4-pp1-ep2-gbs2-mbs1-seq16-MOE-True-GEMM-False-flash_attn-False.txt",
    );
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    let expected = read_expected(fixture_path("expected_a100_gpt_moe_test_sp.txt"));
    assert_eq!(generated, expected);
}

#[test]
fn deepseek_aiob_matches_characterization_fixture() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");
    let aiob_profile = tmp.path().join("deepseek_aiob_profile.txt");
    fs::copy(
        fixture_path("deepseek_train_test_aiob_profile.txt"),
        &aiob_profile,
    )
    .expect("copy aiob profile");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--frame",
        "DeepSeek",
        "--gpu_type",
        "A100",
        "--model_name",
        "deepseek_train_test",
        "--world_size",
        "8",
        "--tensor_model_parallel_size",
        "4",
        "--pipeline_model_parallel",
        "1",
        "--expert_model_parallel_size",
        "2",
        "--global_batch",
        "2",
        "--micro_batch",
        "1",
        "--num_layers",
        "4",
        "--seq_length",
        "16",
        "--hidden_size",
        "2048",
        "--ffn_hidden_size",
        "4096",
        "--num_attention_heads",
        "16",
        "--vocab_size",
        "32000",
        "--enable_sequence_parallel",
        "--moe_enable",
        "--num_experts",
        "8",
        "--moe_router_topk",
        "2",
        "--n_dense_layers",
        "1",
        "--n_shared_expert",
        "1",
        "--aiob_enable",
        "--aiob_profile",
        aiob_profile.to_str().expect("profile path utf8"),
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join(
        "A100-deepseek_train_test-world_size8-tp4-pp1-ep2-gbs2-mbs1-seq16-MOE-True-GEMM-False-flash_attn-False.txt",
    );
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    let expected = read_expected(fixture_path("expected_a100_deepseek_train_test_aiob.txt"));
    assert_eq!(generated, expected);
}

#[test]
fn aiob_enable_requires_profile_when_default_missing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--gpu_type",
        "A100",
        "--model_name",
        "gpt_13B",
        "--world_size",
        "128",
        "--tensor_model_parallel_size",
        "8",
        "--pipeline_model_parallel",
        "2",
        "--expert_model_parallel_size",
        "1",
        "--global_batch",
        "128",
        "--micro_batch",
        "1",
        "--num_layers",
        "40",
        "--seq_length",
        "1024",
        "--hidden_size",
        "2048",
        "--vocab_size",
        "32000",
        "--enable_sequence_parallel",
        "--aiob_enable",
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    let output = cmd.assert().failure().get_output().stderr.clone();
    let stderr = String::from_utf8_lossy(&output);
    assert!(stderr.contains("missing aiob profile"));
}

#[test]
fn non_sp_path_generates_attention_and_mlp_layers() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--gpu_type",
        "A100",
        "--model_name",
        "gpt_non_sp",
        "--world_size",
        "8",
        "--tensor_model_parallel_size",
        "4",
        "--pipeline_model_parallel",
        "1",
        "--expert_model_parallel_size",
        "1",
        "--global_batch",
        "2",
        "--micro_batch",
        "1",
        "--num_layers",
        "4",
        "--seq_length",
        "16",
        "--hidden_size",
        "1024",
        "--ffn_hidden_size",
        "4096",
        "--vocab_size",
        "32000",
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join(
        "A100-gpt_non_sp-world_size8-tp4-pp1-ep1-gbs2-mbs1-seq16-MOE-False-GEMM-False-flash_attn-False.txt",
    );
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    assert!(generated.contains("\nlayernorm\t-1\t1\tNONE\t0\t1\tALLREDUCE\t"));
    assert!(generated.contains("\nattention_layer\t-1\t1\tALLREDUCE\t"));
    assert!(generated.contains("\nmlp_layer\t-1\t1\tALLREDUCE\t"));
    assert!(!generated.contains("\nfinal_column\t"));
    assert!(!generated.contains("\nattention_column\t"));
}

#[test]
fn aiob_profile_applies_training_compute_times() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");
    let aiob_profile = tmp.path().join("aiob_profile.txt");
    fs::write(
        &aiob_profile,
        r#"
param_time:
time_gpu_min: 0.3
time_gpu_avg: 0.4
Emb:
time_gpu_avg: 1.2
atten_kernel:
time_gpu_avg: 2.0
mlp_kernel:
time_gpu_avg: 3.0
"#,
    )
    .expect("write aiob profile");

    let mut cmd = cargo_bin_cmd!("training-workload-generator");
    cmd.args([
        "--gpu_type",
        "A100",
        "--model_name",
        "gpt_aiob",
        "--world_size",
        "4",
        "--tensor_model_parallel_size",
        "4",
        "--pipeline_model_parallel",
        "1",
        "--expert_model_parallel_size",
        "1",
        "--global_batch",
        "1",
        "--micro_batch",
        "1",
        "--num_layers",
        "2",
        "--seq_length",
        "16",
        "--hidden_size",
        "1024",
        "--ffn_hidden_size",
        "4096",
        "--vocab_size",
        "32000",
        "--enable_sequence_parallel",
        "--aiob_enable",
        "--aiob_profile",
        aiob_profile.to_str().expect("profile path utf8"),
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join(
        "A100-gpt_aiob-world_size4-tp4-pp1-ep1-gbs1-mbs1-seq16-MOE-False-GEMM-False-flash_attn-False.txt",
    );
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    assert!(
        generated.contains("\ngrad_param_compute\t-1\t1\tNONE\t0\t700\tNONE\t0\t1\tNONE\t0\t100")
    );
    assert!(
        generated.contains(
            "\nembedding_layer\t-1\t1200\tALLREDUCE\t32768\t1\tNONE\t0\t400\tNONE\t0\t100"
        )
    );
    assert!(generated.contains("\nattention_row\t-1\t1000\tREDUCESCATTER\t32768\t1000\tALLGATHER\t32768\t1000\tNONE\t0\t100"));
    assert!(generated.contains(
        "\nmlp_row\t-1\t1500\tREDUCESCATTER\t32768\t1500\tALLGATHER\t32768\t1500\tNONE\t0\t100"
    ));
    assert!(!generated.contains("\nembedding_norm\t"));
}

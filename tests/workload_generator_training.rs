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
fn aiob_enable_is_rejected_for_minimal_subset() {
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
    cmd.assert().failure();
}

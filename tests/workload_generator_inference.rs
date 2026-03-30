use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use std::path::{Path, PathBuf};

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("workload_generator")
        .join(name)
}

fn run_and_read(
    model_name: &str,
    config_fixture: &str,
    phase: &str,
    expected_filename: &str,
) -> String {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let config = fixture_path(config_fixture);
    assert!(config.exists(), "fixture missing: {}", config.display());

    let mut cmd = cargo_bin_cmd!("workload-generator");
    cmd.args([
        model_name,
        config.to_str().expect("config path utf8"),
        "--seq_length",
        "16",
        "--micro_batch",
        "2",
        "--world_size",
        "32",
        "--tensor_model_parallel_size",
        "8",
        "--expert_model_parallel_size",
        "32",
        "--pipeline_model_parallel",
        "1",
        "--phase",
        phase,
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join(expected_filename);
    fs::read_to_string(out_path).expect("generated output should be readable")
}

fn read_expected(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("expected file should be readable")
}

#[test]
fn deepseek_decode_matches_characterization_fixture() {
    let generated = run_and_read(
        "DeepSeek-671B",
        "deepseek_default.toml",
        "decode",
        "DeepSeek-671B-world_size32-tp8-pp1-ep32-bs2-seq16-decode.txt",
    );
    let expected = read_expected(fixture_path("expected_deepseek_decode.txt"));
    assert_eq!(generated, expected);
}

#[test]
fn qwen3_moe_decode_matches_characterization_fixture() {
    let generated = run_and_read(
        "Qwen3-Moe-235B",
        "qwen3_moe_default.toml",
        "decode",
        "Qwen3-Moe-235B-world_size32-tp8-pp1-ep32-bs2-seq16-decode.txt",
    );
    let expected = read_expected(fixture_path("expected_qwen3_moe_decode.txt"));
    assert_eq!(generated, expected);
}

#[test]
fn qwen3_next_prefill_matches_characterization_fixture() {
    let generated = run_and_read(
        "Qwen3-Next-80B",
        "qwen3_next_default.toml",
        "prefill",
        "Qwen3-Next-80B-world_size32-tp8-pp1-ep32-bs2-seq16-prefill.txt",
    );
    let expected = read_expected(fixture_path("expected_qwen3_next_prefill.txt"));
    assert_eq!(generated, expected);
}

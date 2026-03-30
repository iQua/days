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

#[test]
fn aiob_profile_applies_non_default_compute_times() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let config = tmp.path().join("qwen3_moe_min.toml");
    fs::write(
        &config,
        r#"
num_hidden_layers = 1
hidden_size = 4096
num_experts_per_tok = 8
"#,
    )
    .expect("write config");

    let aiob_profile = tmp.path().join("aiob_profile.txt");
    fs::write(
        &aiob_profile,
        r#"
atten_norm_kernel:
time_gpu_avg: 0.5
atten_kernel:
time_gpu_avg: 1.5
moe_norm_kernel:
time_gpu_avg: 2.0
moe_route_kernel:
time_gpu_avg: 2.5
moe_expert_kernel:
time_gpu_avg: 3.0
"#,
    )
    .expect("write aiob profile");

    let mut cmd = cargo_bin_cmd!("workload-generator");
    cmd.args([
        "Qwen3-Moe-235B",
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
        "decode",
        "--aiob_enable",
        "--aiob_profile",
        aiob_profile.to_str().expect("aiob profile path utf8"),
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join("Qwen3-Moe-235B-world_size32-tp8-pp1-ep32-bs2-seq16-decode.txt");
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");

    let mut lines = generated.lines();
    let _header = lines.next().expect("header line");
    let _count = lines.next().expect("count line");
    assert_eq!(
        lines.next().expect("attention_norm row"),
        "attention_norm\t-1\t500\tNONE\t0\t0\tNONE\t0\t0\tNONE\t0\t100"
    );
    assert_eq!(
        lines.next().expect("attention_layer row"),
        "attention_layer\t-1\t1500\tALLREDUCE\t16384\t0\tNONE\t0\t0\tNONE\t0\t100"
    );
    assert_eq!(
        lines.next().expect("moe_norm row"),
        "moe_norm\t-1\t2000\tNONE\t0\t0\tNONE\t0\t0\tNONE\t0\t100"
    );
    assert_eq!(
        lines.next().expect("moe_route row"),
        "moe_route\t-1\t2500\tALLTOALL_EP\t8448\t1\tNONE\t0\t1\tNONE\t0\t100"
    );
    assert_eq!(
        lines.next().expect("moe_expert row"),
        "moe_expert\t-1\t3000\tALLTOALL_EP\t16384\t1\tNONE\t0\t1\tNONE\t0\t100"
    );
}

#[test]
fn aiob_profile_is_applied_even_without_aiob_enable_flag() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let config = tmp.path().join("qwen3_moe_min.toml");
    fs::write(
        &config,
        r#"
num_hidden_layers = 1
hidden_size = 4096
num_experts_per_tok = 8
"#,
    )
    .expect("write config");

    let aiob_profile = tmp.path().join("aiob_profile.txt");
    fs::write(
        &aiob_profile,
        r#"
atten_kernel:
time_gpu_avg: 1.5
"#,
    )
    .expect("write aiob profile");

    let mut cmd = cargo_bin_cmd!("workload-generator");
    cmd.args([
        "Qwen3-Moe-235B",
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
        "decode",
        "--aiob_profile",
        aiob_profile.to_str().expect("aiob profile path utf8"),
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join("Qwen3-Moe-235B-world_size32-tp8-pp1-ep32-bs2-seq16-decode.txt");
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    let mut lines = generated.lines();
    let _header = lines.next().expect("header line");
    let _count = lines.next().expect("count line");
    assert_eq!(
        lines.next().expect("attention_norm row"),
        "attention_norm\t-1\t1\tNONE\t0\t0\tNONE\t0\t0\tNONE\t0\t100"
    );
    assert_eq!(
        lines.next().expect("attention_layer row"),
        "attention_layer\t-1\t1500\tALLREDUCE\t16384\t0\tNONE\t0\t0\tNONE\t0\t100"
    );
}

#[test]
fn aiob_enable_uses_default_profile_path_when_present() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).expect("create output dir");

    let config = tmp.path().join("qwen3_moe_min.toml");
    fs::write(
        &config,
        r#"
num_hidden_layers = 1
hidden_size = 4096
num_experts_per_tok = 8
"#,
    )
    .expect("write config");

    let default_profile_dir = tmp.path().join("results").join("aiob_outputs");
    fs::create_dir_all(&default_profile_dir).expect("create default profile dir");
    let default_profile_path =
        default_profile_dir.join("Qwen3-Moe-235B-world_size32-tp8-pp1-ep32-bpg2-seq16-decode.txt");
    fs::write(
        &default_profile_path,
        r#"
atten_kernel:
time_gpu_avg: 1.5
"#,
    )
    .expect("write default aiob profile");

    let mut cmd = cargo_bin_cmd!("workload-generator");
    cmd.current_dir(tmp.path());
    cmd.args([
        "Qwen3-Moe-235B",
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
        "decode",
        "--aiob_enable",
        "--result_dir",
        out_dir.to_str().expect("out dir utf8"),
    ]);
    cmd.assert().success();

    let out_path = out_dir.join("Qwen3-Moe-235B-world_size32-tp8-pp1-ep32-bs2-seq16-decode.txt");
    let generated = fs::read_to_string(out_path).expect("generated output should be readable");
    let mut lines = generated.lines();
    let _header = lines.next().expect("header line");
    let _count = lines.next().expect("count line");
    let _attention_norm = lines.next().expect("attention_norm row");
    assert_eq!(
        lines.next().expect("attention_layer row"),
        "attention_layer\t-1\t1500\tALLREDUCE\t16384\t0\tNONE\t0\t0\tNONE\t0\t100"
    );
}

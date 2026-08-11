//! T32 profile-only prepare decomposition.
//!
//! The production CUDA graph remains the existing 13-dispatch DAG. The direct profiling path may
//! replace its one prepare dispatch with four ordered diagnostic dispatches so count, prefix,
//! ordered write, and fixed publication have honest event boundaries.

const CUDA_KERNELS: &str = include_str!("../src/cuda_kernels.cu");
const CUDA_BACKEND: &str = include_str!("../src/cuda.rs");

#[test]
fn production_dag_stays_thirteen_dispatches_and_uses_the_original_prepare() {
    let production_names = source_array(CUDA_BACKEND, "KERNEL_NAMES");
    assert_eq!(production_names.len(), 13);
    assert_eq!(production_names[2], "days_round_reset");
    assert_eq!(production_names[3], "days_round_prepare");
    assert!(!production_names.iter().any(|name| name.contains("profile")));
}

#[test]
fn profiled_prepare_has_four_ordered_dispatch_boundaries() {
    let profile_names = source_array(CUDA_BACKEND, "PROFILE_KERNEL_NAMES");
    let count = position(&profile_names, "days_round_prepare_count_profile");
    let prefix = position(&profile_names, "days_round_prepare_prefix_profile");
    let write = position(&profile_names, "days_round_prepare_write_profile");
    let combine = position(&profile_names, "days_round_prepare_combine_profile");
    assert_eq!([prefix, write, combine], [count + 1, count + 2, count + 3]);
    assert!(count > position(&profile_names, "days_round_reset"));
    assert!(combine < position(&profile_names, "days_round"));

    for name in [
        "days_round_prepare_count_profile",
        "days_round_prepare_prefix_profile",
        "days_round_prepare_write_profile",
        "days_round_prepare_combine_profile",
    ] {
        assert!(
            CUDA_KERNELS.contains(&format!("void {name}(")),
            "missing profile-only kernel {name}"
        );
    }
}

#[test]
fn prepare_profile_reports_every_true_subphase_once() {
    for field in [
        "round_reset_ns",
        "prepare_count_ns",
        "prepare_prefix_ns",
        "prepare_write_ns",
        "prepare_combine_ns",
    ] {
        assert!(
            CUDA_BACKEND.contains(&format!("pub {field}: u64")),
            "missing retained profile field {field}"
        );
    }
}

fn position(values: &[String], needle: &str) -> usize {
    values
        .iter()
        .position(|value| value == needle)
        .unwrap_or_else(|| panic!("missing {needle} in {values:?}"))
}

fn source_array(source: &str, name: &str) -> Vec<String> {
    let declaration = format!("const {name}: [");
    let start = source
        .find(&declaration)
        .unwrap_or_else(|| panic!("missing {name}"));
    let body = &source[start..];
    let body = &body[body.find("= [").expect("array opener") + 3..];
    let body = &body[..body.find("];").expect("array closer")];
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(open) = rest.find('"') {
        rest = &rest[open + 1..];
        let close = rest.find('"').expect("string closer");
        values.push(rest[..close].to_owned());
        rest = &rest[close + 1..];
    }
    values
}

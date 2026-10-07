//! P16 H3 (aicb): an AICB scenario lowers from SimAI's three inputs (design note §5; ruling A3).
//!
//! `tests/fixtures/aicb/b4-simai.toml` names SimAI's b4 trace, its `SimAI.conf` and its 128g
//! topology file; the adapter checks their sha256, plans the trace (SimAI fidelity), derives the
//! fabric from `SimAI.conf` and lowers the workload IR: one fused compute segment per DP16 group
//! (the forward and backward TP collectives are delay-only), then the group's reduce-scatter ring
//! on 16 servers, every message on the network.

use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{CollectiveAlgorithm, SimulationImage, StageRole};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aicb")
        .join(name)
}

fn lower(path: &Path) -> Result<SimulationImage, String> {
    compile_config(path).map_err(|error| error.to_string())
}

/// `(collective stages by algorithm, compute stages)`.
fn stages(image: &SimulationImage) -> (Vec<(CollectiveAlgorithm, usize)>, usize) {
    let mut collectives = std::collections::BTreeMap::<u8, (CollectiveAlgorithm, usize)>::new();
    let mut computes = 0;
    for host in &image.host_states {
        for stage in host.stages.iter().flatten() {
            match stage.role {
                StageRole::Collective(identity) => {
                    collectives
                        .entry(identity.algorithm as u8)
                        .or_insert((identity.algorithm, 0))
                        .1 += 1;
                }
                StageRole::Compute(_) => computes += 1,
            }
        }
    }
    (collectives.into_values().collect(), computes)
}

#[test]
fn b4_lowers_from_its_three_simai_inputs() {
    let image = lower(&fixture("b4-simai.toml")).unwrap_or_else(|error| panic!("{error}"));
    let (collectives, computes) = stages(&image);
    // 8 DP16 rings x 16 ranks x 15 steps; one fused segment per rank.
    assert_eq!(collectives, [(CollectiveAlgorithm::ReduceScatter, 1_920)]);
    assert_eq!(computes, 128);
    // Every RS message is in the lossless class and stays on its rail's ASW (rail-aligned DP).
    for flow in image.flows.iter().filter(|flow| !flow.route.is_empty()) {
        assert_eq!(flow.priority, 3);
        assert_eq!(flow.route.len(), 2, "same-ASW route");
    }
}

#[test]
fn the_reduced_dense_trace_crosses_the_spine() {
    let image =
        lower(&fixture("reduced-dense-simai.toml")).unwrap_or_else(|error| panic!("{error}"));
    let (collectives, computes) = stages(&image);
    // 8 DP4 rings (stride 8, one rank per server, two segments) x 4 ranks x 3 steps.
    assert_eq!(collectives, [(CollectiveAlgorithm::ReduceScatter, 96)]);
    assert_eq!(computes, 32);
    let spine = image
        .flows
        .iter()
        .filter(|flow| flow.route.len() == 4)
        .count();
    assert!(spine > 0, "DP rings that cross segments cross a PSW");
}

fn variant(name: &str, edit: impl Fn(String) -> String) -> Result<SimulationImage, String> {
    let text = edit(std::fs::read_to_string(fixture(name)).unwrap());
    let directory = tempfile::TempDir::new().expect("temp dir");
    for file in [
        "b4-gpt13b-w128-tp8-pp2-gbs8.txt",
        "smoke-moe-w128-tp2-ep32.txt",
        "SimAI.conf",
    ] {
        std::fs::copy(fixture(file), directory.path().join(file)).unwrap();
    }
    let path = directory.path().join(name);
    std::fs::write(&path, text).unwrap();
    lower(&path)
}

#[test]
fn an_aicb_scenario_is_refused_when_it_is_not_what_it_names() {
    for (edit, expected) in [
        (
            Box::new(|text: String| format!("{text}\n[switch]\ncapacity = 100\n"))
                as Box<dyn Fn(String) -> String>,
            "takes its `[switch]`",
        ),
        (
            Box::new(|text: String| text.replace("8268ee83", "0268ee83")),
            "not the declared",
        ),
        (
            Box::new(|text: String| text.replace("fidelity = \"simai\"", "fidelity = \"exact\"")),
            "unknown fidelity",
        ),
        (
            Box::new(|text: String| text.replace("send_lat_us = 3\n", "")),
            "required together",
        ),
        (
            Box::new(|text: String| text.replace("psws = 64", "psws = 32")),
            "psws = 32, but SimAI's",
        ),
        (
            Box::new(|text: String| {
                text.replace("gpus = 128", "gpus = 256")
                    .replace("topology = \"Spectrum-X_128g_8gps_100Gbps_A100\"\n", "")
            }),
            "all_gpus = 128 is not the fabric's 256 GPUs",
        ),
        (
            Box::new(|text: String| {
                text.replace(
                    "expert_routing = \"uniform\"",
                    "expert_routing = \"imbalanced\"",
                )
            }),
            "needs [workload.aicb.imbalanced]",
        ),
        (
            Box::new(|text: String| text.replace("pxn_enable = false", "pxn_enable = true")),
            "AS_PXN_ENABLE",
        ),
    ] {
        let error = variant("b4-simai.toml", edit).expect_err(expected);
        assert!(error.contains(expected), "`{error}` lacks `{expected}`");
    }
}

#[test]
fn chains_across_group_families_wait_for_host_matched_after() {
    // The Megatron arm's pipeline transfers, and the smoke's all-to-all to DP_EP fork.
    let error = variant("b4-simai.toml", |text| {
        text.replace("fidelity = \"simai\"", "fidelity = \"megatron\"")
    })
    .expect_err("megatron b4");
    assert!(error.contains("ruling C1"), "{error}");
    let error = lower(&fixture("smoke-simai.toml")).expect_err("smoke");
    assert!(error.contains("ruling C1"), "{error}");
}

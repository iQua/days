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

#[test]
fn the_manifest_records_what_the_run_was_made_from() {
    let (image, manifest) = days::scenario::compile_config_with_manifest(
        fixture("b4-simai.toml"),
        days::topos::route::RouteWorkers::serial(),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let manifest = manifest.expect("an AICB scenario has a manifest");
    assert_eq!(image, lower(&fixture("b4-simai.toml")).unwrap());
    let line = manifest.to_string();
    for field in [
        "record=days_workload adapter=aicb",
        "trace_sha256=8268ee8380f9105452428a283713a1e3451fe054635da212aaf9131c09c8a68f",
        "records=93 tp=8 ep=1 pp=2 ga=1 all_gpus=128 fidelity=simai expert_routing=uniform",
        "simai_conf_sha256=1fe56bee9c2a0e0f27fdbe816c2c51c9254b5d5a6c5e05adaac347a758e77fcf",
        "simai_topology=Spectrum-X_128g_8gps_100Gbps_A100",
        "simai_topology_sha256=db2114fe21ffb5092432407eac13bf91ba7cefcc7cfcd7471fd483b0bf7e2705",
        "send_lat_us=3 nvls_enable=true pxn_enable=false",
        "mtu_bytes=9000 window_bytes=72500 queue_capacity_packets=3729",
        "ecn_by_rate=100000000000:112,400000000000:223",
        "pfc_asw_xoff=3515844 pfc_asw_xon=3512772 pfc_psw_xoff=4115208 pfc_psw_xon=4112136",
        "headroom_by_rate=100000000000:30574,400000000000:75000",
        "collectives=91 operations=16 fused_segments=1 fused_single_server_ops=90",
        "fp_clamps=4 elided=2 hang_window_recorded=0 data_queue=fifo data_queue_order=grad_norm",
        "ecmp_ordinals=exact",
        "as-send-lat",
    ] {
        assert!(line.contains(field), "the manifest lacks `{field}`: {line}");
    }
    // An ordinary scenario has no manifest.
    let (_, none) = days::scenario::compile_config_with_manifest(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("configs/p16/rail_mini_roce.toml"),
        days::topos::route::RouteWorkers::serial(),
    )
    .unwrap();
    assert!(none.is_none());
}

//! P16 H3 (aicb): helpers shared by the AICB test files (`#[path]`-included; not a test target).
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{CollectiveAlgorithm, GeneratorStatus, RunResult, SimulationImage, StageRole};

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/aicb")
        .join(name)
}

/// Lowers a scenario under `tests/fixtures/aicb`.
pub fn lower(name: &str) -> SimulationImage {
    compile_config(fixture(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
}

/// The flagship scenario, lowered from a directory holding `flagship-simai.toml`, `SimAI.conf`
/// and the trace named by `DAYS_AICB_FLAGSHIP` (not committed; ruling A5).
pub fn lower_flagship() -> SimulationImage {
    let trace = std::env::var_os("DAYS_AICB_FLAGSHIP").expect(
        "DAYS_AICB_FLAGSHIP names the flagship trace \
         (days-gpu evidence/P16/collops-design/traces/flagship-tp2-ep32-w1024.txt)",
    );
    let directory = tempfile::TempDir::new().expect("temp dir");
    for file in ["flagship-simai.toml", "SimAI.conf"] {
        std::fs::copy(fixture(file), directory.path().join(file)).expect("copy the scenario");
    }
    std::fs::copy(
        Path::new(&trace),
        directory.path().join("flagship-tp2-ep32-w1024.txt"),
    )
    .expect("copy the flagship trace");
    compile_config(directory.path().join("flagship-simai.toml"))
        .unwrap_or_else(|error| panic!("flagship: {error}"))
}

/// `(network stages by algorithm, NVLink notify stages, compute stages)`. A notify stage is a
/// collective stage with an empty route (its message stays on the server's NVLink).
pub fn stage_kinds(image: &SimulationImage) -> (Vec<(CollectiveAlgorithm, usize)>, usize, usize) {
    let mut network = BTreeMap::<u8, (CollectiveAlgorithm, usize)>::new();
    let (mut notify, mut computes) = (0, 0);
    for host in &image.host_states {
        for (generator, stage) in host.generators_with_stages() {
            match stage.map(|stage| stage.role) {
                Some(StageRole::Collective(identity)) => {
                    if image.flows[generator.flow.0 as usize].route.is_empty() {
                        notify += 1;
                    } else {
                        network
                            .entry(identity.algorithm as u8)
                            .or_insert((identity.algorithm, 0))
                            .1 += 1;
                    }
                }
                Some(StageRole::Compute(_)) => computes += 1,
                None => {}
            }
        }
    }
    (network.into_values().collect(), notify, computes)
}

/// Network messages per `(source, target)` host pair: the count behind SimAI's per-pair `sport`
/// (design note §3.7).
pub fn ecmp_pairs(image: &SimulationImage) -> BTreeMap<(u64, u64), usize> {
    let mut pairs = BTreeMap::new();
    for host in &image.host_states {
        for (generator, stage) in host.generators_with_stages() {
            if let Some(StageRole::Collective(_)) = stage.map(|stage| stage.role) {
                let flow = &image.flows[generator.flow.0 as usize];
                if !flow.route.is_empty() {
                    *pairs.entry((flow.source.0, flow.target.0)).or_insert(0) += 1;
                }
            }
        }
    }
    pairs
}

/// Exactness condition (b) of design note §0.1 item 5: the network pairs of each ring collective
/// that appear on more than one of its channels. Empty when the per-pair static order (op, then
/// channel, then step) is SimAI's run-time order.
pub fn ring_pairs_across_channels(image: &SimulationImage) -> Vec<(u64, u64, u64)> {
    let mut channels = BTreeMap::<(u64, u64, u64), BTreeSet<u32>>::new();
    for host in &image.host_states {
        for (generator, stage) in host.generators_with_stages() {
            let Some(StageRole::Collective(identity)) = stage.map(|stage| stage.role) else {
                continue;
            };
            if matches!(
                identity.algorithm,
                CollectiveAlgorithm::AllToAll | CollectiveAlgorithm::SendRecv
            ) {
                continue;
            }
            let flow = &image.flows[generator.flow.0 as usize];
            if !flow.route.is_empty() {
                channels
                    .entry((identity.collective_id, flow.source.0, flow.target.0))
                    .or_default()
                    .insert(identity.channel);
            }
        }
    }
    channels
        .into_iter()
        .filter(|(_, channels)| channels.len() > 1)
        .map(|(key, _)| key)
        .collect()
}

/// The most channels any ring collective's network stages use (so a test of
/// [`ring_pairs_across_channels`] can show it is not vacuous).
pub fn max_ring_channels(image: &SimulationImage) -> usize {
    let mut channels = BTreeMap::<u64, BTreeSet<u32>>::new();
    for host in &image.host_states {
        for (generator, stage) in host.generators_with_stages() {
            let Some(StageRole::Collective(identity)) = stage.map(|stage| stage.role) else {
                continue;
            };
            let ring = !matches!(
                identity.algorithm,
                CollectiveAlgorithm::AllToAll | CollectiveAlgorithm::SendRecv
            );
            if ring && !image.flows[generator.flow.0 as usize].route.is_empty() {
                channels
                    .entry(identity.collective_id)
                    .or_default()
                    .insert(identity.channel);
            }
        }
    }
    channels.values().map(BTreeSet::len).max().unwrap_or(0)
}

/// Stages that have not finished at the end of a run.
pub fn unfinished(result: &RunResult) -> usize {
    result
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter(|(generator, stage)| {
            stage.is_some() && generator.next_emission.status != GeneratorStatus::Finished
        })
        .count()
}

/// The end of the first fused compute segment (the compute stages without predecessors), which
/// every rank runs before its first cross-host point.
pub fn first_segment_end(image: &SimulationImage) -> u64 {
    image
        .host_states
        .iter()
        .flat_map(|state| state.stages.iter().flatten())
        .filter(|stage| {
            stage.dependencies.local.count() == 0 && stage.dependencies.inbound.count() == 0
        })
        .filter_map(|stage| match stage.role {
            StageRole::Compute(compute) => Some(compute.duration_ns),
            StageRole::Collective(_) => None,
        })
        .max()
        .expect("an AICB image starts with a compute segment")
}

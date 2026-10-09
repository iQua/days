//! P16 H1 fix round 1: the collective operations on the rail fabric, with H2's stage notify.
//!
//! On a rail topology a collective message between two GPUs of one server crosses NVLink, which
//! Days models delay-only: it lowers to a stage notify whose delay is
//! `ServerLocality::nvlink_message_delay_ns(bytes, k, mtu)`, with the sender port's concurrency `k`
//! per hop type (orchestrator ruling, from the N1 SimAI comparison):
//! - an all-to-all's same-server sends, all released together: `k` = their number;
//! - a same-server hop of a ring that spans servers: `k = 1`;
//! - a collective whose ranks all share one server: one delay stage per rank (ruling H2-2),
//!   `ServerLocality::single_server_collective_delay_ns` with `k` = its channels.
//!
//! Every fixture lowers with those delays, runs to completion on Scalar, and equals Scalar on CPU
//! (1, 2 and 4 workers), Metal and CUDA, under Full and Summary observation, at the stop, at stop/2
//! and at checkpoints; its progress certificate is pinned under `lean/fixtures/p10c/`, where the
//! LeanGuard collective campaign accepts it.

use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days::topos::config::SpectrumXConfig;
use days::topos::rail::{RailTopology, ServerLocality};
use days_executor::{
    CpuConfig, FlowGeneratorKind, ObservationMode, RunResult, SimulationImage, StageOperation,
    StageRole, run_cpu_with_observations, run_scalar_with_observations,
};

/// 8 GPUs, 4 per server (servers {0..3} and {4..7}), on SimAI's rail fabric.
const FABRIC: &str = r#"
[topology]
category = "SpectrumX"

[topology.spectrum_x]
gpus = 8
gpus_per_server = 4
nics_per_asw = 2
psws = 2
gpu_type = "H100"
nic_rate_bps = 100000000000
uplink_rate_bps = 400000000000
nvlink_rate_bps = 2400000000000
link_delay_ns = 500
nvlink_delay_ns = 25

[routing]
policy = "SimAiEcmp"

[switch]
capacity = 400
discipline = "FIFO"
drop = "TailDrop"
"#;

const MTU: u64 = 1_000;

fn locality() -> ServerLocality {
    let rail = RailTopology::new(&SpectrumXConfig {
        gpus: 8,
        gpus_per_server: 4,
        nics_per_asw: 2,
        psws: 2,
        gpu_type: "H100".to_owned(),
        nic_rate_bps: 100_000_000_000,
        uplink_rate_bps: 400_000_000_000,
        nvlink_rate_bps: 2_400_000_000_000,
        link_delay_ns: 500,
        nvlink_delay_ns: 25,
    })
    .expect("fixture fabric");
    ServerLocality::new(rail.profile())
}

fn hosts(range: std::ops::Range<u64>) -> String {
    range
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn compute(name: &str, ranks: &str, after: &str) -> String {
    format!("\n[[compute]]\nname = \"{name}\"\nhosts = [{ranks}]\nduration_ns = 1000\n{after}\n")
}

fn transport(flow: &str) -> &'static str {
    if flow == "TCP" {
        "\n[collective.traffic.tcp]\ncc_algorithm = \"TCPReno\"\n"
    } else {
        "\n[collective.traffic.dcqcn]\nmax_rate_gbps = 100.0\npacing_interval_ns = 80\n\n[collective.traffic.roce]\nretransmit_timeout_ns = 1000000\n"
    }
}

/// A collective over `ranks` (its count), `extra` keys between the type and the traffic.
fn collective(name: &str, kind: &str, flow: &str, ranks: &str, extra: &str, size: u64) -> String {
    let count = ranks.split(',').count();
    format!(
        "\n[[collective]]\nname = \"{name}\"\ncollective_type = \"{kind}\"\nflow_type = \"{flow}\"\nflow_count = {count}\nsources = [{ranks}]\n{extra}\n[collective.traffic]\ninitial_delay = 0.0\nsize = {size}\narr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\npkt_size_dist = {{ type = \"DiscreteUniform\", low = {MTU}, high = {MTU} }}\n{}",
        transport(flow)
    )
}

fn scenario(body: &str) -> String {
    format!("seed = 26\nduration = 0.01\n{FABRIC}{body}")
}

/// Every fixture, by label.
fn fixtures() -> Vec<(&'static str, String)> {
    let all = hosts(0..8);
    let ring_sinks = "sinks = [1, 2, 3, 4, 5, 6, 7, 0]\n";
    vec![
        (
            // Each rank sends three same-server messages (k = 3) and four across the fabric; the
            // compute after it joins notifies and queue pairs.
            "rail-a2a-roce",
            scenario(
                &(compute("fwd", &all, "")
                    + &collective(
                        "dispatch",
                        "AllToAll",
                        "RoCE",
                        &all,
                        "after = \"fwd\"\n",
                        64_000,
                    )
                    + &compute("expert", &all, "after = \"dispatch\"")),
            ),
        ),
        (
            // Ranks 3 -> 4 and 7 -> 0 cross servers; the other six hops are notifies (k = 1).
            "rail-ring-roce",
            scenario(
                &(compute("fwd", &all, "")
                    + &collective(
                        "ring",
                        "RingAllReduce",
                        "RoCE",
                        &all,
                        &format!("{ring_sinks}after = \"fwd\"\n"),
                        32_000,
                    )
                    + &compute("bwd", &all, "after = \"ring\"")),
            ),
        ),
        (
            "rail-ring-tcp",
            scenario(
                &(compute("fwd", &all, "")
                    + &collective(
                        "ring",
                        "AllGather",
                        "TCP",
                        &all,
                        &format!("{ring_sinks}after = \"fwd\"\n"),
                        16_000,
                    )
                    + &compute("bwd", &all, "after = \"ring\"")),
            ),
        ),
        (
            // A TP AllGather inside each server (one delay stage per rank, k = 4 channels), then
            // a compute that also follows a cross-server all-to-all.
            "rail-tp-and-a2a",
            scenario(
                &(compute("fwd", &all, "")
                    + &collective(
                        "tp0",
                        "AllGather",
                        "RoCE",
                        &hosts(0..4),
                        "sinks = [1, 2, 3, 0]\nafter = \"fwd0\"\n",
                        40_000,
                    )
                    + &compute("fwd0", &hosts(0..4), "")
                    + &collective(
                        "tp1",
                        "AllGather",
                        "RoCE",
                        &hosts(4..8),
                        "sinks = [5, 6, 7, 4]\nafter = \"fwd1\"\n",
                        40_000,
                    )
                    + &compute("fwd1", &hosts(4..8), "")
                    + &collective("a2a", "AllToAll", "RoCE", &all, "after = \"fwd\"\n", 64_000)
                    + &compute("after0", &hosts(0..4), "after = \"tp0\"")
                    + &compute("join", &all, "after = \"a2a\"")),
            ),
        ),
        (
            // Each all-to-all send is released by a counted join of two computes (`fwd` at
            // 1,000 ns, `aux` after it at 2,000 ns): its notifies leave at the join's completion.
            "rail-a2a-after-join",
            scenario(
                &(compute("fwd", &all, "")
                    + &compute("aux", &all, "after = \"fwd\"")
                    + &collective(
                        "dispatch",
                        "AllToAll",
                        "RoCE",
                        &all,
                        "after = [\"fwd\", \"aux\"]\n",
                        64_000,
                    )
                    + &compute("expert", &all, "after = \"dispatch\"")),
            ),
        ),
        (
            // An ungated all-to-all: its sends, notifies included, start with it at its initial
            // delay (2 us) and are not logged; the compute after it certifies them, each notify by
            // its sender's timer completion and its delivery.
            "rail-a2a-ungated",
            scenario(
                &(collective("dispatch", "AllToAll", "RoCE", &all, "", 64_000)
                    .replace("initial_delay = 0.0", "initial_delay = 0.000002")
                    + &compute("expert", &all, "after = \"dispatch\"")),
            ),
        ),
    ]
}

fn lower(label: &str, config: &str) -> SimulationImage {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p16-collops-rail-{label}-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, config).expect("write fixture");
    let image = compile_config(&path);
    std::fs::remove_file(&path).expect("remove fixture");
    image.unwrap_or_else(|error| panic!("{label} must lower: {error}"))
}

fn images() -> Vec<(&'static str, SimulationImage)> {
    fixtures()
        .into_iter()
        .map(|(label, config)| (label, lower(label, &config)))
        .collect()
}

/// Every collective stage on a constant timer (a stage notify): `(algorithm, rank, step, chunk,
/// delay)`, the delay being the lead plus the lane.
fn notifies(image: &SimulationImage) -> Vec<(String, u32, u32, u64, u64)> {
    image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter_map(|(generator, stage)| match (generator.kind, stage?.role) {
            (FlowGeneratorKind::Constant(constant), StageRole::Collective(identity)) => Some((
                format!("{:?}", identity.algorithm),
                identity.rank,
                identity.step,
                identity.chunk_bytes,
                constant.interval_ns + constant.first_departure_ns,
            )),
            _ => None,
        })
        .collect()
}

/// The ruled concurrency per hop type: an all-to-all's same-server sends share the port (3 per
/// rank here); a ring's same-server hops run alone.
#[test]
fn notifies_take_the_concurrency_of_their_hop_type() {
    let locality = locality();
    for (label, image) in images() {
        let notifies = notifies(&image);
        assert!(!notifies.is_empty(), "{label}: same-server messages notify");
        for (algorithm, rank, step, chunk, delay) in notifies {
            let k = if algorithm == "AllToAll" { 3 } else { 1 };
            assert_eq!(
                Some(delay),
                locality.nvlink_message_delay_ns(chunk, k, MTU),
                "{label}: {algorithm} rank {rank} step {step}"
            );
        }
    }
}

/// A collective inside one server is one delay stage per rank, of
/// `single_server_collective_delay_ns(n - 1, floor(floor(S / n) / n), n, mtu)` (SimAI's `n`
/// channels inside a server), and no collective stage.
#[test]
fn a_single_server_collective_is_one_delay_stage_per_rank() {
    let (label, config) = &fixtures()[3];
    let image = lower(label, config);
    let delay = locality()
        .single_server_collective_delay_ns(3, 40_000 / 4 / 4, 4, MTU)
        .expect("a delay");
    let stages = image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter_map(|(_, stage)| stage)
        .collect::<Vec<_>>();
    let collectives = stages
        .iter()
        .filter_map(|stage| match stage.role {
            StageRole::Collective(identity) => Some(identity.algorithm),
            StageRole::Compute(_) => None,
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        collectives,
        [days_executor::CollectiveAlgorithm::AllToAll]
            .into_iter()
            .collect(),
        "only the all-to-all keeps collective stages"
    );
    let delays = stages
        .iter()
        .filter_map(|stage| match stage.role {
            StageRole::Compute(compute) if compute.duration_ns == delay => Some(compute.rank),
            _ => None,
        })
        .count();
    assert_eq!(delays, 8, "two TP groups of four ranks");
}

/// A single-server collective keeps its issue stream (review F9's `stream`) as a delay group: TP on
/// stream 1 inside the first server and the cross-server all-to-all on stream 1 each name one
/// `stage_streams` entry, the TP one its delay stages' compute group.
#[test]
fn a_single_server_collective_keeps_its_stream() {
    let (label, config) = &fixtures()[3];
    let tagged = config
        .replace(
            "sinks = [1, 2, 3, 0]\nafter = \"fwd0\"\n",
            "sinks = [1, 2, 3, 0]\nafter = \"fwd0\"\nstream = 1\n",
        )
        .replace(
            "after = \"fwd\"\n\n[collective.traffic]\ninitial_delay = 0.0\nsize = 64000",
            "after = \"fwd\"\nstream = 1\n\n[collective.traffic]\ninitial_delay = 0.0\nsize = 64000",
        );
    assert_eq!(tagged.matches("stream = 1").count(), 2, "both tags placed");
    let image = lower(label, &tagged);
    let delay = locality()
        .single_server_collective_delay_ns(3, 40_000 / 4 / 4, 4, MTU)
        .expect("a delay");
    let stages = image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter_map(|(_, stage)| stage)
        .collect::<Vec<_>>();
    assert_eq!(image.stage_streams.len(), 2, "{:?}", image.stage_streams);
    for entry in &image.stage_streams {
        assert_eq!(entry.stream, 1);
        match entry.operation {
            StageOperation::Compute(id) => {
                let ranks = stages
                    .iter()
                    .filter_map(|stage| match stage.role {
                        StageRole::Compute(compute) if compute.compute_id == id => {
                            Some(compute.duration_ns)
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(ranks, vec![delay; 4], "the TP group's four delay stages");
            }
            StageOperation::Collective(id) => {
                assert!(stages.iter().any(|stage| matches!(
                    stage.role,
                    StageRole::Collective(identity) if identity.collective_id == id
                        && identity.algorithm == days_executor::CollectiveAlgorithm::AllToAll
                )));
            }
        }
    }
}

fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    run_scalar_with_observations(image, horizon, mode).expect("the Scalar oracle runs")
}

#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn without_diagnostics(mut result: RunResult) -> RunResult {
    result.diagnostics = None;
    result
}

fn checkpoint_image(original: &SimulationImage, checkpoint: &RunResult) -> SimulationImage {
    let mut image = original.clone();
    image.host_states.clone_from(&checkpoint.host_states);
    image.switch_states.clone_from(&checkpoint.switch_states);
    image
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    image.initial_events.clone_from(&checkpoint.pending_events);
    image
}

/// The fixtures, then checkpoints of each at four horizons over its departures and notifies.
fn suite_images() -> Vec<(String, SimulationImage)> {
    let mut all = Vec::new();
    for (label, image) in images() {
        let full = scalar(&image, None, ObservationMode::Full);
        let end = full
            .departures
            .iter()
            .map(|departure| departure.time_ns)
            .max()
            .unwrap_or(1);
        for step in 1..=4 {
            let horizon = end * step / 5 + 1;
            let prefix = scalar(&image, Some(horizon), ObservationMode::Full);
            all.push((
                format!("{label}@{horizon}"),
                checkpoint_image(&image, &prefix),
            ));
        }
        all.push((label.to_owned(), image));
    }
    all
}

#[test]
fn every_fixture_runs_to_completion_on_scalar() {
    for (label, image) in images() {
        let result = scalar(&image, None, ObservationMode::Summary);
        for state in &result.host_states {
            for (generator, stage) in state.generators_with_stages() {
                if stage.is_some() {
                    assert_eq!(
                        generator.next_emission.status,
                        days_executor::GeneratorStatus::Finished,
                        "{label}: flow {:?}",
                        generator.flow
                    );
                }
            }
        }
    }
}

#[test]
fn cpu_matches_scalar_on_every_fixture_and_checkpoint() {
    for (label, image) in suite_images() {
        days_executor::validate(&image, days_executor::Backend::Scalar)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        for mode in [ObservationMode::Full, ObservationMode::Summary] {
            let expected = scalar(&image, None, mode);
            for workers in [1, 2, 4] {
                let actual = run_cpu_with_observations(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    mode,
                )
                .unwrap_or_else(|error| panic!("{label}: {error}"))
                .result;
                assert_eq!(actual, expected, "{label} {mode:?} workers={workers}");
            }
        }
    }
}

/// The Scalar progress certificate of every fixture is pinned under `lean/fixtures/p10c/`
/// (`collective_collops_<label>_executor_accept.csv`). Set
/// `DAYS_UPDATE_COLLECTIVE_TRACE_FIXTURES=1` to regenerate.
#[test]
fn the_certificates_are_scalar_generated() {
    for (label, image) in images() {
        let result = scalar(&image, None, ObservationMode::Full);
        let csv = days_executor::collective_transitions_csv(
            &result.diagnostics.as_ref().unwrap().mechanism_transitions,
            &image,
        )
        .unwrap_or_else(|error| panic!("{label}: {error}"));
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("lean/fixtures/p10c")
            .join(format!(
                "collective_collops_{}_executor_accept.csv",
                label.replace('-', "_")
            ));
        if std::env::var_os("DAYS_UPDATE_COLLECTIVE_TRACE_FIXTURES").is_some() {
            std::fs::write(&path, &csv).unwrap();
        }
        assert_eq!(
            csv,
            std::fs::read_to_string(&path).unwrap_or_default(),
            "{label}"
        );
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{scalar, suite_images, without_diagnostics};

    #[test]
    fn cuda_matches_scalar_on_every_fixture_and_checkpoint() {
        for (label, image) in suite_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual =
                        run_cuda_with_observations(&image, horizon, CudaConfig::default(), mode)
                            .unwrap_or_else(|error| panic!("{label}: {error}"))
                            .result;
                    assert_eq!(actual, expected, "{label} horizon={horizon:?} {mode:?}");
                }
            }
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{scalar, suite_images, without_diagnostics};

    #[test]
    fn metal_matches_scalar_on_every_fixture_and_checkpoint() {
        for (label, image) in suite_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual =
                        run_metal_with_observations(&image, horizon, MetalConfig::default(), mode)
                            .unwrap_or_else(|error| panic!("{label}: {error}"))
                            .result;
                    assert_eq!(actual, expected, "{label} horizon={horizon:?} {mode:?}");
                }
            }
        }
    }
}

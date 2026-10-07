//! P16 host-matched `after` (orchestrator ruling on H3's C1; `days-gpu/evidence/P16/
//! collops-design.md` §4.2): rank `r` of an operation waits, at its host, for each operation its
//! `after` lists that runs on that host, through that operation's rank there. The predecessor need
//! not run on the same hosts in the same order; equal host lists are the special case.
//!
//! The fixtures are the shapes that equality blocked: a data-parallel (DP-like) collective after
//! the compute groups of two expert-parallel (EP-like) instances, in the instances' order and
//! permuted; a data-queue chain across collective families (ruling R9); a DP ring after two EP
//! all-to-alls on the rail fabric (stage notifies); and a pipeline Send/Recv across two compute
//! groups. Every fixture lowers, runs to completion on Scalar, and equals Scalar on CPU (1, 2 and
//! 4 workers), Metal and CUDA under Full and Summary observation, at the stop, at stop/2 and at
//! four checkpoints; its progress certificate is pinned under `lean/fixtures/p10c/`, where the
//! LeanGuard collective campaign accepts it.

use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, ObservationMode, RunResult, SimulationImage, StageRole, run_cpu_with_observations,
    run_scalar_with_observations,
};

fn lower_text(label: &str, config: &str) -> Result<SimulationImage, String> {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p16-hostafter-{label}-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, config).expect("write fixture");
    let image = compile_config(&path).map_err(|error| error.to_string());
    std::fs::remove_file(&path).expect("remove fixture");
    image
}

fn lower(label: &str, config: &str) -> SimulationImage {
    lower_text(label, config).unwrap_or_else(|error| panic!("{label} must lower: {error}"))
}

/// `hosts` hosts on one switch; `body` holds the stage groups.
fn star(hosts: u64, body: &str) -> String {
    let edges = (0..hosts)
        .map(|host| format!("[{host}, {hosts}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let list = (0..hosts)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
seed = 26
edges = [{edges}]
hosts = [{list}]
duration = 0.05

[switch]
port_rate = 8000000000
capacity = 200
discipline = "FIFO"
drop = "TailDrop"
{body}"#
    )
}

/// 8 GPUs, 4 per server (servers {0..3} and {4..7}), on SimAI's rail fabric.
fn rail(body: &str) -> String {
    format!(
        r#"
seed = 26
duration = 0.01

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
{body}"#
    )
}

const TCP: &str = r#"
[collective.traffic.tcp]
cc_algorithm = "TCPReno"
"#;

const ROCE: &str = r#"
[collective.traffic.dcqcn]
max_rate_gbps = 8.0
pacing_interval_ns = 500

[collective.traffic.roce]
retransmit_timeout_ns = 1000000
"#;

fn compute(name: &str, hosts: &str, duration_ns: u64, after: &str) -> String {
    format!(
        "\n[[compute]]\nname = \"{name}\"\nhosts = [{hosts}]\nduration_ns = {duration_ns}\n{after}\n"
    )
}

/// A collective block over `hosts` (its ranks, in order); `extra` holds keys between the type and
/// the traffic table. A ring's sinks are its next ranks.
fn collective(name: &str, kind: &str, flow: &str, hosts: &str, extra: &str, size: u64) -> String {
    let transport = if flow == "TCP" { TCP } else { ROCE };
    let ranks = hosts.split(", ").collect::<Vec<_>>();
    let sinks = if matches!(kind, "RingAllReduce" | "AllGather" | "ReduceScatter") {
        let next = (0..ranks.len())
            .map(|rank| ranks[(rank + 1) % ranks.len()])
            .collect::<Vec<_>>()
            .join(", ");
        format!("sinks = [{next}]\n")
    } else {
        String::new()
    };
    format!(
        "\n[[collective]]\nname = \"{name}\"\ncollective_type = \"{kind}\"\nflow_type = \"{flow}\"\nflow_count = {}\nsources = [{hosts}]\n{sinks}{extra}\n[collective.traffic]\ninitial_delay = 0.0\nsize = {size}\narr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\npkt_size_dist = {{ type = \"DiscreteUniform\", low = 500, high = 500 }}\n{transport}",
        ranks.len()
    )
}

const EP0: &str = "0, 1, 2, 3";
const EP1: &str = "4, 5, 6, 7";

/// Every fixture, by label.
fn fixtures() -> Vec<(&'static str, String)> {
    vec![
        (
            // Two DP rings, {0, 4} (TCP) and {1, 5} (RoCE), each after both EP instances' compute
            // groups (each rank waits for the instance it is in), then a compute over their four
            // hosts after both rings (each host waits for the ring it is in).
            "dp-after-ep",
            star(
                8,
                &(compute("ep0", EP0, 2_000, "")
                    + &compute("ep1", EP1, 3_000, "")
                    + &collective(
                        "dp0",
                        "RingAllReduce",
                        "TCP",
                        "0, 4",
                        "after = [\"ep0\", \"ep1\"]\n",
                        6_000,
                    )
                    + &collective(
                        "dp1",
                        "RingAllReduce",
                        "RoCE",
                        "1, 5",
                        "after = [\"ep0\", \"ep1\"]\n",
                        6_000,
                    )
                    + &compute("post", "0, 1, 4, 5", 1_000, "after = [\"dp0\", \"dp1\"]")),
            ),
        ),
        (
            // A DP AllGather over all eight hosts in interleaved order after both EP instances,
            // and an EP-ordered compute after it: every rank maps through a different position.
            "dp-permuted",
            star(
                8,
                &(compute("ep0", EP0, 2_000, "")
                    + &compute("ep1", EP1, 3_000, "")
                    + &collective(
                        "dp",
                        "AllGather",
                        "RoCE",
                        "0, 4, 1, 5, 2, 6, 3, 7",
                        "after = [\"ep0\", \"ep1\"]\n",
                        16_000,
                    )
                    + &compute("post", "0, 1, 2, 3, 4, 5, 6, 7", 1_000, "after = \"dp\"")),
            ),
        ),
        (
            // Ruling R9's data-queue chain across families (stream 1): a DP ring {0, 4} after its
            // weight gradient, then a DP-EP ring {0, 1, 4, 5} after its own weight gradient and
            // the previous data collective, which only hosts 0 and 4 run.
            "data-queue-across-families",
            star(
                8,
                &(compute("wg0", "0, 4", 2_000, "stream = 1")
                    + &collective(
                        "dpa",
                        "RingAllReduce",
                        "TCP",
                        "0, 4",
                        "after = \"wg0\"\nstream = 1\n",
                        8_000,
                    )
                    + &compute("wg1", "0, 1, 4, 5", 1_000, "")
                    + &collective(
                        "dpb",
                        "ReduceScatter",
                        "TCP",
                        "0, 1, 4, 5",
                        "after = [\"wg1\", \"dpa\"]\nstream = 1\n",
                        8_000,
                    )
                    + &compute("end", "0, 1, 4, 5", 1_000, "after = \"dpb\"")),
            ),
        ),
        (
            // On the rail fabric: two EP all-to-alls (all stage notifies, inside each server),
            // then a DP ring {0, 4} across the servers after both all-to-alls and its weight
            // gradient: rank 0 waits for instance 0's completion at host 0, rank 1 for instance
            // 1's at host 4.
            "rail-dp-after-ep-a2a",
            rail(
                &(compute("fwd0", EP0, 1_000, "")
                    + &compute("fwd1", EP1, 1_000, "")
                    + &collective(
                        "a2a0",
                        "AllToAll",
                        "RoCE",
                        EP0,
                        "after = \"fwd0\"\n",
                        16_000,
                    )
                    + &collective(
                        "a2a1",
                        "AllToAll",
                        "RoCE",
                        EP1,
                        "after = \"fwd1\"\n",
                        16_000,
                    )
                    + &compute("wg", "0, 4", 500, "")
                    + &collective(
                        "dp",
                        "RingAllReduce",
                        "RoCE",
                        "0, 4",
                        "after = [\"a2a0\", \"a2a1\", \"wg\"]\n",
                        8_000,
                    )
                    + &compute("end", "0, 4", 1_000, "after = \"dp\"")),
            ),
        ),
        (
            // A pipeline: stage 0 on {0, 1}, stage 1 on {2, 3}; the activations go from host 1 to
            // host 2 after stage 0. Stage 1's next compute waits at host 2 for the message and
            // stage 1, at host 3 for stage 1 alone; stage 0's next compute at host 1 for the send.
            "sendrecv-across-stages",
            star(
                4,
                &(compute("s0", "0, 1", 2_000, "")
                    + &compute("s1", "2, 3", 1_000, "")
                    + &collective("pp", "SendRecv", "RoCE", "1, 2", "after = \"s0\"\n", 9_000)
                    + &compute("s1b", "2, 3", 1_000, "after = [\"s1\", \"pp\"]")
                    + &compute("s0b", "0, 1", 1_000, "after = [\"s0\", \"pp\"]")),
            ),
        ),
    ]
}

fn images() -> Vec<(&'static str, SimulationImage)> {
    fixtures()
        .into_iter()
        .map(|(label, config)| (label, lower(label, &config)))
        .collect()
}

/// The flow ids of a group's stages at a host: the compute stage of `compute_id`, or every stage
/// of collective `collective_id` sourced there.
fn stages_at(image: &SimulationImage, host: u64) -> Vec<(StageRole, u64, Vec<u64>, Vec<u64>)> {
    let node = image
        .nodes
        .iter()
        .find(|node| node.id.0 == host && node.kind == days_executor::NodeKind::Host)
        .expect("a host");
    image.host_states[node.state_slot as usize]
        .generators_with_stages()
        .filter_map(|(generator, stage)| {
            let stage = stage?;
            Some((
                stage.role,
                generator.flow.0,
                stage
                    .dependencies
                    .local
                    .iter(&image.stage_joins)
                    .map(|flow| flow.0)
                    .collect(),
                stage
                    .dependencies
                    .inbound
                    .iter(&image.stage_joins)
                    .map(|flow| flow.0)
                    .collect(),
            ))
        })
        .collect()
}

fn compute_flow(image: &SimulationImage, host: u64, compute_id: u64) -> u64 {
    stages_at(image, host)
        .into_iter()
        .find_map(|(role, flow, ..)| match role {
            StageRole::Compute(compute) if compute.compute_id == compute_id => Some(flow),
            _ => None,
        })
        .expect("the compute stage")
}

/// Each DP ring's roots wait, at their host, for the compute stage there of the EP instance that
/// contains the host, and for nothing else.
#[test]
fn a_dp_ring_waits_at_each_host_for_the_ep_instance_there() {
    let image = lower("dp-after-ep", &fixtures()[0].1);
    for (host, instance) in [(0, 0), (1, 0), (4, 1), (5, 1)] {
        let gate = compute_flow(&image, host, instance);
        let roots = stages_at(&image, host)
            .into_iter()
            .filter(|(role, ..)| {
                matches!(role, StageRole::Collective(identity)
                    if identity.step == 1 && identity.phase == days_executor::CollectivePhase::ReduceScatter)
            })
            .collect::<Vec<_>>();
        assert_eq!(roots.len(), 1, "host {host}: one ring root");
        assert_eq!(roots[0].2, vec![gate], "host {host}: local gate");
        assert!(roots[0].3.is_empty(), "host {host}: no inbound gate");
    }
}

/// The Send/Recv's message waits for stage 0 at the sender; stage 1's next compute waits at host
/// 2 for stage 1 and the message (inbound) and at host 3 for stage 1 alone.
#[test]
fn a_pipeline_send_joins_only_the_hosts_it_runs_on() {
    let image = lower("sendrecv-across-stages", &fixtures()[4].1);
    let message = stages_at(&image, 1)
        .into_iter()
        .find(|(role, ..)| matches!(role, StageRole::Collective(_)))
        .expect("the message");
    assert_eq!(message.2, vec![compute_flow(&image, 1, 0)]);
    let s1 = |host| compute_flow(&image, host, 1);
    let s1b_at = |host| {
        stages_at(&image, host)
            .into_iter()
            .find(
                |(role, ..)| matches!(role, StageRole::Compute(compute) if compute.compute_id == 2),
            )
            .expect("s1b")
    };
    let at2 = s1b_at(2);
    assert_eq!((at2.2, at2.3), (vec![s1(2)], vec![message.1]));
    let at3 = s1b_at(3);
    assert_eq!((at3.2, at3.3), (vec![s1(3)], vec![]));
}

/// Rejected host matches, each with a precise message.
#[test]
fn unmatched_after_lists_are_rejected() {
    let cases = [
        (
            // Host 4 of the ring runs no listed operation.
            star(
                8,
                &(compute("ep0", EP0, 2_000, "")
                    + &collective(
                        "dp",
                        "RingAllReduce",
                        "TCP",
                        "0, 4",
                        "after = \"ep0\"\n",
                        6_000,
                    )),
            ),
            "invalid scenario: collective `dp` rank 1 (host 4) starts after no operation its `after` lists",
        ),
        (
            // `ep1` shares no host with the compute group.
            star(
                8,
                &(compute("ep1", EP1, 2_000, "")
                    + &compute("post", "0, 1", 1_000, "after = \"ep1\"")),
            ),
            "invalid scenario: compute `post` names stage group `ep1`, which runs on none of its hosts",
        ),
        (
            // The message starts at host 1 alone; `s1` runs on the receiver only.
            star(
                4,
                &(compute("s0", "0, 1", 2_000, "")
                    + &compute("s1", "2, 3", 1_000, "")
                    + &collective(
                        "pp",
                        "SendRecv",
                        "RoCE",
                        "1, 2",
                        "after = [\"s0\", \"s1\"]\n",
                        9_000,
                    )),
            ),
            "invalid scenario: collective `pp` names stage group `s1`, which runs on none of the hosts it starts at",
        ),
    ];
    for (config, expected) in cases {
        assert_eq!(lower_text("reject", &config).unwrap_err(), expected);
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

/// The fixtures, then checkpoints of each at four horizons over its departures.
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
/// (`collective_hostafter_<label>_executor_accept.csv`). Set
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
                "collective_hostafter_{}_executor_accept.csv",
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

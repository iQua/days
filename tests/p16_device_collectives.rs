//! P16 lane G1 (colldev): collective and compute stages on the device backends, byte-identical to
//! the Scalar oracle (`days-gpu/evidence/P16/colldev-design.md`).
//!
//! The images are every collective fixture that lowers: the seven `configs/p15` RoCE collective
//! and compute fixtures and the generated TCP fixtures of the P14 suites (rings and AllGathers at
//! 2 to 4 ranks, a lossy ring, compute chains, a compute-only scenario whose late stage passes the
//! stop), each with gated and ungated stages. Checkpoints taken from them resume on the devices
//! with gated, active and finished stages, pending compute timers, and an unreleased stage queue
//! pair at a host whose data class is paused. Identity is the complete state under full
//! observation with Scalar's diagnostics stripped (P15 ruling D1, design note G5), the summary-mode
//! result, and the six frozen Summary anchors of the RoCE collective fixtures.

#![cfg(any(
    feature = "cuda",
    feature = "cuda-planner-test",
    all(feature = "metal", target_vendor = "apple")
))]

#[path = "collective_tcp.rs"]
#[allow(dead_code)]
mod tcp;

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    EventKind, FlowGeneratorKind, GeneratorStatus, ObservationMode, PacketKind, RunResult,
    SimulationImage, StageRole, run_scalar_with_observations,
};

/// The RoCE collective and compute fixtures of P15 lane R3, every one of which lowers
/// (`days-gpu/plans/briefs/p16/collective-device-facts.md` §7).
const ROCE_FIXTURES: &[&str] = &[
    "roce_ring_allreduce_lossless.toml",
    "roce_allgather_lossless.toml",
    "roce_ring_lossy.toml",
    "roce_compute_dag.toml",
    "roce_ring_release_paused.toml",
    "roce_tcp_mixed_collectives.toml",
    "roce_allgather_compute_lossy.toml",
];

/// Frozen Scalar summary-mode anchors (`tests/p15_roce_collectives.rs`, re-frozen at P16 D1): the
/// devices must reproduce the same pretty-`Debug` bytes and FNV-1a64.
const ANCHORS: [(&str, u64, u64); 6] = [
    (
        "roce_ring_allreduce_lossless.toml",
        193_075,
        0x02fc_5b57_38bf_9639,
    ),
    (
        "roce_allgather_lossless.toml",
        123_170,
        0xc32f_fe08_1e7b_440d,
    ),
    ("roce_ring_lossy.toml", 182_347, 0xb718_c505_7453_0f5a),
    ("roce_compute_dag.toml", 212_197, 0xe94b_963f_a5fd_954a),
    (
        "roce_tcp_mixed_collectives.toml",
        138_792,
        0x88ff_2fe4_10cc_0df8,
    ),
    (
        "roce_ring_release_paused.toml",
        197_029,
        0xf5e6_4cf6_d7ca_985e,
    ),
];

/// Capacity retries per fixture on the default plan (`*_stage_fixtures_pin_their_capacity_retries`),
/// MEASURED at P16 G1 commit 5 on Metal (M5 Max) and CUDA (sim, RTX A4500): none.
#[allow(dead_code)]
const PINNED_RETRIES_METAL: &[(&str, usize)] = &[
    ("roce_ring_allreduce_lossless", 0),
    ("roce_allgather_lossless", 0),
    ("roce_ring_lossy", 0),
    ("roce_compute_dag", 0),
    ("roce_ring_release_paused", 0),
    ("roce_tcp_mixed_collectives", 0),
    ("roce_allgather_compute_lossy", 0),
    ("tcp-RingAllReduce-2", 0),
    ("tcp-RingAllReduce-3", 0),
    ("tcp-RingAllReduce-4", 0),
    ("tcp-AllGather-2", 0),
    ("tcp-AllGather-3", 0),
    ("tcp-AllGather-4", 0),
    ("tcp-lossy-ring", 0),
    ("tcp-compute-chain", 0),
    ("tcp-ring-allgather-compute-4", 0),
    ("compute-only", 0),
    ("fanout-after-compute", 0),
    ("one-ns-compute", 0),
    ("compute-at-stop", 0),
];
#[allow(dead_code)]
const PINNED_RETRIES_CUDA: &[(&str, usize)] = PINNED_RETRIES_METAL;

pub fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p15")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

/// Hosts 0 and 1 hang off switch 4, hosts 2 and 3 off switch 5; the ring order sends two ring hops
/// across each direction of the 4-5 link into a four-packet TailDrop queue
/// (`tests/collective_tcp_loss.rs`).
fn lossy_ring_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 4, 20_000, 4)
        .replace("duration = 0.05", "duration = 5.0")
        .replace(
            "edges = [[0, 4], [1, 4], [2, 4], [3, 4]]",
            "edges = [[0, 4], [1, 4], [2, 5], [3, 5], [4, 5]]",
        )
        .replace("sources = [0, 1, 2, 3]", "sources = [0, 2, 1, 3]")
        .replace("sinks = [1, 2, 3, 0]", "sinks = [2, 1, 3, 0]")
}

/// compute -> TCP ring -> compute (`tests/scalar_stage_index.rs`).
fn compute_chain_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 3, 9_001, 100).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + r#"
[[compute]]
name = "forward"
hosts = [0, 1, 2]
duration_ns = 5000

[[compute]]
name = "backward"
hosts = [0, 1, 2]
duration_ns = 7000
after = "grad"
"#
}

/// compute -> ring -> compute -> AllGather -> compute (`tests/scalar_stage_index.rs`).
fn ring_allgather_compute_config(ranks: u64) -> String {
    let hosts = (0..ranks)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let gather = tcp::tcp_collective_config("AllGather", ranks, ranks * 1_500, 100);
    let gather = &gather[gather.find("[[collective]]").expect("collective block")..];
    tcp::tcp_collective_config("RingAllReduce", ranks, ranks * 2_500, 100).replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"grad\"\nafter = \"forward\"\n",
    ) + &gather.replace(
        "[[collective]]\n",
        "[[collective]]\nname = \"gather\"\nafter = \"backward\"\n",
    ) + &format!(
        r#"
[[compute]]
name = "forward"
hosts = [{hosts}]
duration_ns = 5000

[[compute]]
name = "backward"
hosts = [{hosts}]
duration_ns = 7000
after = "grad"

[[compute]]
name = "optimizer"
hosts = [{hosts}]
duration_ns = 3000
after = "gather"
"#
    )
}

/// Compute only; 4 us + 7 us passes the 10 us stop, so the late stage stops without a timer
/// (`tests/collective_compute.rs`).
const COMPUTE_ONLY: &str = r#"
seed = 26
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "a"
hosts = [0, 1]
duration_ns = 4000

[[compute]]
name = "late"
hosts = [0, 1]
duration_ns = 7000
after = "a"
"#;

/// One compute group released into three collectives on the same ranks, a TCP ring, a TCP AllGather
/// and a RoCE AllGather: each compute timer releases three stages on its host in one event. The
/// release order (ascending `FlowId`, Scalar's table order) decides the payload sequence and the
/// host-queue order of the two TCP stages' first windows, so a reversed order changes bytes
/// (design note §1.2).
const FANOUT_AFTER_COMPUTE: &str = r#"
seed = 26
edges = [[0, 4], [1, 4], [2, 4], [3, 4]]
hosts = [0, 1, 2, 3]
duration = 0.01

[switch]
port_rate = 1000000000
capacity = 300
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "forward"
hosts = [0, 1, 2, 3]
duration_ns = 3000

[[collective]]
name = "ring"
after = "forward"
collective_type = "RingAllReduce"
flow_type = "TCP"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 8000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }

[collective.traffic.tcp]
cc_algorithm = "TCPReno"

[[collective]]
name = "gather"
after = "forward"
collective_type = "AllGather"
flow_type = "TCP"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 6000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }

[collective.traffic.tcp]
cc_algorithm = "TCPReno"

[[collective]]
name = "rgather"
after = "forward"
collective_type = "AllGather"
flow_type = "RoCE"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 4000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }

[collective.traffic.dcqcn]
max_rate_gbps = 1.0
pacing_interval_ns = 1000

[collective.traffic.roce]
retransmit_timeout_ns = 1000000
"#;

/// The shortest compute interval, 1 ns, between collectives: a release never completes a stage in
/// its own event (design note §1.2, fact B), so each 1 ns stage finishes one nanosecond after its
/// release, in a later event.
const ONE_NS_COMPUTE: &str = r#"
seed = 26
edges = [[0, 3], [1, 3], [2, 3]]
hosts = [0, 1, 2]
duration = 0.01

[switch]
port_rate = 1000000000
capacity = 300
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "a"
hosts = [0, 1, 2]
duration_ns = 1

[[compute]]
name = "b"
hosts = [0, 1, 2]
duration_ns = 1
after = "a"

[[collective]]
name = "ring"
after = "b"
collective_type = "RingAllReduce"
flow_type = "TCP"
flow_count = 3
sources = [0, 1, 2]
sinks = [1, 2, 0]

[collective.traffic]
initial_delay = 0.0
size = 3000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }

[collective.traffic.tcp]
cc_algorithm = "TCPReno"

[[compute]]
name = "c"
hosts = [0, 1, 2]
duration_ns = 1
after = "ring"
"#;

/// A compute deadline exactly at the stop time (review F9): `a` ends at 4 us and `edge` 6 us later,
/// at the 10 us stop, so Scalar schedules its timer (`deadline <= stop`) and finishes it at the stop.
const COMPUTE_AT_STOP: &str = r#"
seed = 26
edges = [[0, 2], [1, 2]]
hosts = [0, 1]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[compute]]
name = "a"
hosts = [0, 1]
duration_ns = 4000

[[compute]]
name = "edge"
hosts = [0, 1]
duration_ns = 6000
after = "a"
"#;

/// Every fixture that lowers, by name.
pub fn fixtures() -> Vec<(String, SimulationImage)> {
    let mut images = ROCE_FIXTURES
        .iter()
        .map(|name| (name.trim_end_matches(".toml").to_owned(), lower(name)))
        .collect::<Vec<_>>();
    for algorithm in ["RingAllReduce", "AllGather"] {
        for ranks in [2, 3, 4] {
            let label = format!("tcp-{algorithm}-{ranks}");
            let config = tcp::tcp_collective_config(algorithm, ranks, 10_001, 100);
            images.push((label.clone(), tcp::compile_text(&label, &config)));
        }
    }
    for (label, config) in [
        ("tcp-lossy-ring", lossy_ring_config()),
        ("tcp-compute-chain", compute_chain_config()),
        (
            "tcp-ring-allgather-compute-4",
            ring_allgather_compute_config(4),
        ),
        ("compute-only", COMPUTE_ONLY.to_owned()),
        ("fanout-after-compute", FANOUT_AFTER_COMPUTE.to_owned()),
        ("one-ns-compute", ONE_NS_COMPUTE.to_owned()),
        ("compute-at-stop", COMPUTE_AT_STOP.to_owned()),
    ] {
        images.push((label.to_owned(), tcp::compile_text(label, &config)));
    }
    images
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

/// `count` horizons spread evenly over the span of `image`'s departures, so they fall while stages
/// are gated, running and finished; empty for an image that sends nothing.
pub fn checkpoint_horizons(image: &SimulationImage, count: u64) -> Vec<u64> {
    let full = run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar oracle must run");
    let first = full
        .departures
        .iter()
        .map(|departure| departure.time_ns)
        .min();
    let last = full
        .departures
        .iter()
        .map(|departure| departure.time_ns)
        .max();
    let (Some(first), Some(last)) = (first, last) else {
        return Vec::new();
    };
    (1..=count)
        .map(|step| first + (last - first) * step / (count + 1) + 1)
        .collect()
}

/// Checkpoints of `image` at its [`checkpoint_horizons`].
fn checkpoints(label: &str, image: &SimulationImage, count: u64) -> Vec<(String, SimulationImage)> {
    checkpoint_horizons(image, count)
        .into_iter()
        .map(|horizon| {
            let prefix = run_scalar_with_observations(image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            (
                format!("{label}@{horizon}"),
                checkpoint_image(image, &prefix),
            )
        })
        .collect()
}

/// The fixtures, then checkpoints of the fixtures that carry every stage state between them.
pub fn stage_images() -> Vec<(String, SimulationImage)> {
    let fixtures = fixtures();
    let mut images = fixtures.clone();
    for (label, image) in &fixtures {
        if [
            "roce_ring_allreduce_lossless",
            "roce_compute_dag",
            "roce_ring_release_paused",
            "roce_tcp_mixed_collectives",
            "tcp-lossy-ring",
            "tcp-compute-chain",
            "tcp-ring-allgather-compute-4",
            "fanout-after-compute",
        ]
        .contains(&label.as_str())
        {
            images.extend(checkpoints(label, image, 5));
        }
    }
    // A checkpoint of the compute-only scenario between the two compute deadlines: the late stage
    // is released with its timer pending.
    let compute_only = &fixtures
        .iter()
        .find(|(label, _)| label == "compute-only")
        .expect("compute-only fixture")
        .1;
    let prefix = run_scalar_with_observations(compute_only, Some(5_000), ObservationMode::Full)
        .expect("compute-only prefix must run");
    images.push((
        "compute-only@5000".to_owned(),
        checkpoint_image(compute_only, &prefix),
    ));
    images
}

pub fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    let mut expected =
        run_scalar_with_observations(image, horizon, mode).expect("scalar oracle must run");
    expected.diagnostics = None;
    expected
}

/// FNV-1a64 over the pretty `Debug` rendering, the `result_fnv1a64` the `days` CLI prints.
fn fingerprint(value: &impl std::fmt::Debug) -> (u64, u64) {
    let text = format!("{value:#?}");
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (text.len() as u64, hash)
}

/// Facts A and B of the release rule (design note §1.2), on Scalar: the fan-out fixture releases two
/// stages on one host in one event, so the identity suite exercises the release order; and a 1 ns
/// compute stage finishes exactly 1 ns after its release, in a later event than the release.
#[test]
fn release_order_and_one_ns_fixtures_exercise_the_release_rule() {
    let all = fixtures();
    let image = |label: &str| {
        &all.iter()
            .find(|(name, _)| name == label)
            .unwrap_or_else(|| panic!("{label}"))
            .1
    };
    let fanout =
        run_scalar_with_observations(image("fanout-after-compute"), None, ObservationMode::Full)
            .expect("scalar oracle must run");
    let mut releases = std::collections::BTreeMap::<_, usize>::new();
    for record in &fanout
        .diagnostics
        .as_ref()
        .expect("full")
        .mechanism_transitions
    {
        if let days_executor::MechanismTransitionRecord::Collective(record) = record {
            if record.activated {
                *releases.entry((record.key, record.node)).or_default() += 1;
            }
        }
    }
    assert!(
        releases.values().any(|&count| count >= 3),
        "some event releases three stages on one host"
    );

    let one_ns =
        run_scalar_with_observations(image("one-ns-compute"), None, ObservationMode::Summary)
            .expect("scalar oracle must run");
    for host in &one_ns.host_states {
        let deadlines = host
            .generators
            .iter()
            .enumerate()
            .filter(|(position, _)| {
                matches!(
                    host.stage(*position).map(|stage| stage.role),
                    Some(StageRole::Compute(_))
                )
            })
            .map(|(_, generator)| {
                assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
                generator.next_emission.departure_time_ns
            })
            .collect::<Vec<_>>();
        if deadlines.is_empty() {
            continue;
        }
        assert_eq!(
            &deadlines[..2],
            &[1, 2],
            "a at 1 ns, b released at 1 ns finishes at 2 ns"
        );
    }
}

/// Review F9: the `compute-at-stop` fixture schedules a compute timer exactly at the stop, and Scalar
/// finishes the stage there, so the device's `deadline <= stop` boundary is pinned by identity.
#[test]
fn a_compute_deadline_at_the_stop_finishes_there() {
    let image = fixtures()
        .into_iter()
        .find(|(name, _)| name == "compute-at-stop")
        .expect("compute-at-stop fixture")
        .1;
    let result = run_scalar_with_observations(&image, None, ObservationMode::Summary)
        .expect("scalar oracle must run");
    let edges = result
        .host_states
        .iter()
        .flat_map(|host| {
            host.generators
                .iter()
                .enumerate()
                .filter(|(position, _)| {
                    host.stage(*position)
                        .is_some_and(|stage| stage.dependencies.local_predecessor.is_some())
                })
                .map(|(_, generator)| generator.next_emission)
        })
        .collect::<Vec<_>>();
    assert_eq!(edges.len(), 2);
    for emission in edges {
        assert_eq!(emission.status, GeneratorStatus::Finished);
        assert_eq!(emission.departure_time_ns, image.stop_time_ns);
    }
}

/// Every stage image selects the mechanisms build (design note G4): the TCP-only and compute-only
/// images carry no other mechanism, so without the stage bit they would select the plain build.
#[test]
fn stage_images_select_the_mechanisms_round_kernel() {
    for (name, image) in stage_images() {
        assert_eq!(
            days_executor::RoundKernel::for_image(&image),
            days_executor::RoundKernel::Mechanisms,
            "{name}"
        );
    }
}

/// The images exercise what the identity tests rely on: unreleased, released and finished stages
/// of both transports and of compute at checkpoints, a pending compute timer, and an unreleased
/// stage queue pair at a host whose data class is paused (the RESUME skip, design note §3.3).
#[test]
fn stage_images_cover_every_stage_state() {
    let images = stage_images();
    let mut gated = [false; 3];
    let mut active = [false; 3];
    let mut finished = [false; 3];
    let mut pending_compute_timer = false;
    let mut unreleased_pair_at_paused_host = false;
    for (label, image) in images.iter().filter(|(label, _)| label.contains('@')) {
        for host in &image.host_states {
            for (position, generator) in host.generators.iter().enumerate() {
                let Some(stage) = host.stage(position) else {
                    continue;
                };
                let kind = match (generator.kind, stage.role) {
                    (FlowGeneratorKind::Tcp(_), _) => 0,
                    (FlowGeneratorKind::Roce(_), _) => 1,
                    (_, StageRole::Compute(_)) => 2,
                    _ => panic!("{label}: unexpected stage generator"),
                };
                let status = generator.next_emission.status;
                if !stage.activated {
                    gated[kind] = true;
                } else if status == GeneratorStatus::Finished {
                    finished[kind] = true;
                } else {
                    active[kind] = true;
                }
                if kind == 2 && status == GeneratorStatus::Scheduled {
                    pending_compute_timer |= image.initial_events.iter().any(|event| {
                        event.kind == EventKind::PacingTimer
                            && event.payload == generator.next_emission.payload
                            && image.initial_packets.iter().any(|packet| {
                                packet.id == event.payload && packet.kind == PacketKind::Data
                            })
                    });
                }
                if kind == 1 && !stage.activated {
                    let class = image.flows[generator.flow.0 as usize].priority;
                    unreleased_pair_at_paused_host |= host
                        .pfc
                        .as_deref()
                        .is_some_and(|pfc| pfc.is_paused(usize::from(class)));
                }
            }
        }
    }
    assert_eq!(gated, [true; 3], "gated TCP, RoCE and compute stages");
    assert_eq!(active, [true; 3], "running TCP, RoCE and compute stages");
    assert_eq!(finished, [true; 3], "finished TCP, RoCE and compute stages");
    assert!(
        pending_compute_timer,
        "a checkpoint with a pending compute timer"
    );
    assert!(
        unreleased_pair_at_paused_host,
        "a checkpoint with an unreleased stage queue pair at a host whose class is paused"
    );
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{ANCHORS, fingerprint, fixtures, lower, scalar, stage_images};

    fn run(
        image: &days_executor::SimulationImage,
        horizon: Option<u64>,
        mode: ObservationMode,
        (streams_enabled, round_threads_per_block): (bool, usize),
    ) -> Result<days_executor::RunResult, String> {
        run_cuda_with_observations(
            image,
            horizon,
            CudaConfig {
                streams_enabled,
                round_threads_per_block,
                ..CudaConfig::default()
            },
            mode,
        )
        .map(|run| run.result)
        .map_err(|error| error.to_string())
    }

    #[test]
    fn cuda_stage_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in stage_images() {
            let configs: &[(bool, usize)] = if name.contains('@') {
                &[(true, 256), (false, 32)]
            } else {
                &[(true, 256), (true, 32), (false, 32)]
            };
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                let expected = scalar(&image, horizon, ObservationMode::Full);
                for &config in configs {
                    let actual = run(&image, horizon, ObservationMode::Full, config)
                        .unwrap_or_else(|error| panic!("{name} {horizon:?} {config:?}: {error}"));
                    assert_eq!(actual, expected, "{name} horizon={horizon:?} {config:?}");
                }
            }
        }
    }

    /// Review F3: the device's own readback while stages are in flight. Each fixture runs on the
    /// device to its five checkpoint horizons (gated, mid-delivery and running stages, pending
    /// compute timers, parked stage pairs) and the complete state must equal Scalar's prefix there.
    #[test]
    fn cuda_stage_fixtures_match_scalar_at_their_checkpoint_horizons() {
        let mut differing = Vec::new();
        for (name, image) in fixtures() {
            for horizon in super::checkpoint_horizons(&image, 5) {
                let expected = scalar(&image, Some(horizon), ObservationMode::Full);
                match run_cuda_with_observations(
                    &image,
                    Some(horizon),
                    CudaConfig::default(),
                    ObservationMode::Full,
                ) {
                    Ok(run) if run.result == expected => {}
                    Ok(_) => differing.push(format!("{name}@{horizon}")),
                    Err(error) => differing.push(format!("{name}@{horizon}: {error}")),
                }
            }
        }
        assert!(
            differing.is_empty(),
            "differing at checkpoint horizons: {differing:?}"
        );
    }

    /// Deterministic capacity retries of the default plan per fixture (design note §5.2): pinned,
    /// so a sizing change that adds a retry (each one replays the whole run) fails here.
    #[test]
    fn cuda_stage_fixtures_pin_their_capacity_retries() {
        let mut retries = Vec::new();
        for (name, image) in fixtures() {
            let run = run_cuda_with_observations(
                &image,
                None,
                CudaConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            retries.push((name, run.capacity_retry_trace.len()));
        }
        eprintln!("record=stage_retries backend=cuda {retries:?}");
        assert_eq!(
            retries,
            super::PINNED_RETRIES_CUDA
                .iter()
                .map(|(n, r)| (n.to_string(), *r))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cuda_stage_fixtures_match_scalar_in_summary_mode_and_the_frozen_anchors() {
        for (name, image) in fixtures() {
            let expected = scalar(&image, None, ObservationMode::Summary);
            let actual = run(&image, None, ObservationMode::Summary, (true, 256))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(actual, expected, "{name} summary");
        }
        for (name, bytes, fnv1a64) in ANCHORS {
            let actual = run(&lower(name), None, ObservationMode::Summary, (true, 256))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(fingerprint(&actual), (bytes, fnv1a64), "{name} anchor");
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{ANCHORS, fingerprint, fixtures, lower, scalar, stage_images};

    fn run(
        image: &days_executor::SimulationImage,
        horizon: Option<u64>,
        mode: ObservationMode,
        (streams_enabled, round_threads_per_threadgroup): (bool, usize),
    ) -> Result<days_executor::RunResult, String> {
        run_metal_with_observations(
            image,
            horizon,
            MetalConfig {
                streams_enabled,
                round_threads_per_threadgroup,
                ..MetalConfig::default()
            },
            mode,
        )
        .map(|run| run.result)
        .map_err(|error| error.to_string())
    }

    #[test]
    fn metal_stage_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in stage_images() {
            let configs: &[(bool, usize)] = if name.contains('@') {
                &[(true, 256), (false, 32)]
            } else {
                &[(true, 256), (true, 32), (false, 32)]
            };
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                let expected = scalar(&image, horizon, ObservationMode::Full);
                for &config in configs {
                    let actual = run(&image, horizon, ObservationMode::Full, config)
                        .unwrap_or_else(|error| panic!("{name} {horizon:?} {config:?}: {error}"));
                    assert_eq!(actual, expected, "{name} horizon={horizon:?} {config:?}");
                }
            }
        }
    }

    /// Review F3: the device's own readback while stages are in flight. Each fixture runs on the
    /// device to its five checkpoint horizons (gated, mid-delivery and running stages, pending
    /// compute timers, parked stage pairs) and the complete state must equal Scalar's prefix there.
    #[test]
    fn metal_stage_fixtures_match_scalar_at_their_checkpoint_horizons() {
        let mut differing = Vec::new();
        for (name, image) in fixtures() {
            for horizon in super::checkpoint_horizons(&image, 5) {
                let expected = scalar(&image, Some(horizon), ObservationMode::Full);
                match run_metal_with_observations(
                    &image,
                    Some(horizon),
                    MetalConfig::default(),
                    ObservationMode::Full,
                ) {
                    Ok(run) if run.result == expected => {}
                    Ok(_) => differing.push(format!("{name}@{horizon}")),
                    Err(error) => differing.push(format!("{name}@{horizon}: {error}")),
                }
            }
        }
        assert!(
            differing.is_empty(),
            "differing at checkpoint horizons: {differing:?}"
        );
    }

    /// Deterministic capacity retries of the default plan per fixture (design note §5.2): pinned,
    /// so a sizing change that adds a retry (each one replays the whole run) fails here.
    #[test]
    fn metal_stage_fixtures_pin_their_capacity_retries() {
        let mut retries = Vec::new();
        for (name, image) in fixtures() {
            let run = run_metal_with_observations(
                &image,
                None,
                MetalConfig::default(),
                ObservationMode::Summary,
            )
            .unwrap_or_else(|error| panic!("{name}: {error}"));
            retries.push((name, run.capacity_retry_trace.len()));
        }
        eprintln!("record=stage_retries backend=metal {retries:?}");
        assert_eq!(
            retries,
            super::PINNED_RETRIES_METAL
                .iter()
                .map(|(n, r)| (n.to_string(), *r))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn metal_stage_fixtures_match_scalar_in_summary_mode_and_the_frozen_anchors() {
        for (name, image) in fixtures() {
            let expected = scalar(&image, None, ObservationMode::Summary);
            let actual = run(&image, None, ObservationMode::Summary, (true, 256))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(actual, expected, "{name} summary");
        }
        for (name, bytes, fnv1a64) in ANCHORS {
            let actual = run(&lower(name), None, ObservationMode::Summary, (true, 256))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(fingerprint(&actual), (bytes, fnv1a64), "{name} anchor");
        }
    }
}

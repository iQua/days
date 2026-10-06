//! P16 lane H1 (collops): the flagship's collective operations (`days-gpu/evidence/P16/
//! collops-design.md`): joins (a stage after several stage groups), ReduceScatter, multi-channel
//! rings with the `UniformFloor` chunk, all-to-all (uniform and seeded per-pair sizes), Send/Recv,
//! and a collective that waits for a compute group and the previous collective of its stream.
//!
//! Every fixture lowers, runs to completion on Scalar, and equals Scalar on CPU (1, 2 and 4
//! workers), under Full and Summary observation, at the stop and at checkpoints resumed on CPU.
//! The device suites (`metal`, `cuda`) hold Metal and CUDA to the same complete state. Each
//! operation's shape is pinned: stage counts, per-message bytes, and the join release rule.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    CollectiveActivationCause, CpuConfig, FlowGeneratorKind, GeneratorStatus,
    MechanismTransitionRecord, ObservationMode, RunResult, SimulationImage, StageRole,
    run_cpu_with_observations, run_scalar_with_observations,
};

/// Lowers `config`, written to a scratch file.
fn lower_text(label: &str, config: &str) -> Result<SimulationImage, String> {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p16-collops-{label}-{}-{}.toml",
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

/// Four hosts on one switch; `body` holds the stage groups.
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

fn traffic(size: u64, transport: &str) -> String {
    format!(
        r#"
[collective.traffic]
initial_delay = 0.0
size = {size}
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 500, high = 500 }}
{transport}"#
    )
}

fn compute(name: &str, hosts: &str, duration_ns: u64, after: &str) -> String {
    format!(
        "\n[[compute]]\nname = \"{name}\"\nhosts = [{hosts}]\nduration_ns = {duration_ns}\n{after}\n"
    )
}

/// A collective block; `extra` holds keys between the type and the traffic table.
fn collective(name: &str, kind: &str, flow: &str, hosts: &str, extra: &str, size: u64) -> String {
    let transport = if flow == "TCP" { TCP } else { ROCE };
    let count = hosts.split(',').count();
    format!(
        "\n[[collective]]\nname = \"{name}\"\ncollective_type = \"{kind}\"\nflow_type = \"{flow}\"\nflow_count = {count}\nsources = [{hosts}]\n{extra}{}",
        traffic(size, transport)
    )
}

const H4: &str = "0, 1, 2, 3";

/// Every fixture of the suite, by label.
fn fixtures() -> Vec<(&'static str, String)> {
    vec![
        (
            "join-two-computes",
            star(
                4,
                &(compute("a", H4, 3_000, "")
                    + &compute("b", H4, 5_000, "")
                    + &compute("c", H4, 1_000, "after = [\"a\", \"b\"]")),
            ),
        ),
        (
            "join-ring-and-compute",
            star(
                4,
                &(compute("pre", H4, 2_000, "")
                    + &collective(
                        "ring",
                        "RingAllReduce",
                        "TCP",
                        H4,
                        "sinks = [1, 2, 3, 0]\nafter = \"pre\"\n",
                        8_000,
                    )
                    + &compute("side", H4, 40_000, "")
                    + &compute("post", H4, 1_000, "after = [\"ring\", \"side\"]")),
            ),
        ),
        (
            "rs-tcp-4",
            star(
                4,
                &collective(
                    "rs",
                    "ReduceScatter",
                    "TCP",
                    H4,
                    "sinks = [1, 2, 3, 0]\n",
                    10_001,
                ),
            ),
        ),
        (
            "rs-roce-compute-4",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "rs",
                        "ReduceScatter",
                        "RoCE",
                        H4,
                        "sinks = [1, 2, 3, 0]\nafter = \"fwd\"\n",
                        12_000,
                    )
                    + &compute("bwd", H4, 1_000, "after = \"rs\"")),
            ),
        ),
        (
            "ring-channels-2",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "ag",
                        "AllGather",
                        "TCP",
                        H4,
                        "channels = [[0, 1, 2, 3], [0, 2, 1, 3]]\nchunk = \"UniformFloor\"\nafter = \"fwd\"\n",
                        16_003,
                    )
                    + &compute("bwd", H4, 1_000, "after = \"ag\"")),
            ),
        ),
        (
            "allreduce-channels-2-roce",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "ar",
                        "RingAllReduce",
                        "RoCE",
                        H4,
                        "channels = [[0, 1, 2, 3], [1, 0, 3, 2]]\nchunk = \"UniformFloor\"\nafter = \"fwd\"\n",
                        16_000,
                    )
                    + &compute("bwd", H4, 1_000, "after = \"ar\"")),
            ),
        ),
        (
            "a2a-uniform-tcp",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "dispatch",
                        "AllToAll",
                        "TCP",
                        H4,
                        "after = \"fwd\"\n",
                        8_002,
                    )
                    + &compute("expert", H4, 1_000, "after = \"dispatch\"")),
            ),
        ),
        (
            "a2a-uniform-roce",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "dispatch",
                        "AllToAll",
                        "RoCE",
                        H4,
                        "after = \"fwd\"\n",
                        8_000,
                    )
                    + &compute("expert", H4, 1_000, "after = \"dispatch\"")
                    + &collective(
                        "combine",
                        "AllToAll",
                        "RoCE",
                        H4,
                        "after = \"expert\"\n",
                        8_000,
                    )
                    + &compute("bwd", H4, 1_000, "after = \"combine\"")),
            ),
        ),
        (
            "a2a-seeded-roce",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "dispatch",
                        "AllToAll",
                        "RoCE",
                        H4,
                        "after = \"fwd\"\n[collective.alltoall]\nseed = 7\nmatrix = 0\ntranspose = false\nexperts = 8\ntopk = 2\ntokens = 16\nbytes_per_copy = 100\nskew = \"Zipf1\"\n",
                        3_200,
                    )
                    + &compute("expert", H4, 1_000, "after = \"dispatch\"")
                    + &collective(
                        "combine",
                        "AllToAll",
                        "RoCE",
                        H4,
                        "after = \"expert\"\n[collective.alltoall]\nseed = 7\nmatrix = 0\ntranspose = true\nexperts = 8\ntopk = 2\ntokens = 16\nbytes_per_copy = 100\nskew = \"Zipf1\"\n",
                        3_200,
                    )
                    + &compute("bwd", H4, 1_000, "after = \"combine\"")),
            ),
        ),
        (
            "data-stream",
            star(
                4,
                &(compute("wg1", H4, 1_000, "")
                    + &collective(
                        "rs1",
                        "ReduceScatter",
                        "TCP",
                        H4,
                        "sinks = [1, 2, 3, 0]\nafter = \"wg1\"\n",
                        12_000,
                    )
                    + &compute("wg2", H4, 1_000, "after = \"wg1\"")
                    + &collective(
                        "rs2",
                        "ReduceScatter",
                        "TCP",
                        H4,
                        "sinks = [1, 2, 3, 0]\nafter = [\"wg2\", \"rs1\"]\n",
                        12_000,
                    )
                    + &compute("end", H4, 1_000, "after = \"rs2\"")),
            ),
        ),
        (
            "sendrecv",
            star(
                2,
                &(compute("stage0", "0, 1", 2_000, "")
                    + &collective(
                        "pp",
                        "SendRecv",
                        "RoCE",
                        "0, 1",
                        "after = \"stage0\"\n",
                        9_000,
                    )
                    + &compute("stage1", "0, 1", 1_000, "after = \"pp\"")),
            ),
        ),
        (
            "rs-uniform-floor-tcp",
            star(
                4,
                &(compute("fwd", H4, 2_000, "")
                    + &collective(
                        "rs",
                        "ReduceScatter",
                        "TCP",
                        H4,
                        "sinks = [1, 2, 3, 0]\nchunk = \"UniformFloor\"\nafter = \"fwd\"\n",
                        10_003,
                    )),
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

fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    run_scalar_with_observations(image, horizon, mode).expect("the Scalar oracle runs")
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

/// `count` horizons spread over the span of `image`'s departures.
fn checkpoint_horizons(image: &SimulationImage, count: u64) -> Vec<u64> {
    let full = scalar(image, None, ObservationMode::Full);
    let times = full.departures.iter().map(|departure| departure.time_ns);
    let (Some(first), Some(last)) = (times.clone().min(), times.max()) else {
        return Vec::new();
    };
    (1..=count)
        .map(|step| first + (last - first) * step / (count + 1) + 1)
        .collect()
}

/// The fixtures, then checkpoints of every fixture at five horizons.
fn suite_images() -> Vec<(String, SimulationImage)> {
    let mut all = Vec::new();
    for (label, image) in images() {
        for horizon in checkpoint_horizons(&image, 5) {
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

fn without_diagnostics(mut result: RunResult) -> RunResult {
    result.diagnostics = None;
    result
}

/// Every stage generator, with its stage record.
fn stage_generators(
    image: &SimulationImage,
) -> Vec<(
    days_executor::FlowGeneratorState,
    days_executor::CollectiveStage,
)> {
    image
        .host_states
        .iter()
        .flat_map(|state| state.generators_with_stages())
        .filter_map(|(generator, stage)| stage.map(|stage| (*generator, stage)))
        .collect()
}

/// The transport stages of the collective whose algorithm renders as `algorithm`, with their
/// bytes, by collective id.
fn transport_stages(image: &SimulationImage, algorithm: &str) -> BTreeMap<u64, Vec<u64>> {
    let mut stages = BTreeMap::<u64, Vec<u64>>::new();
    for (generator, stage) in stage_generators(image) {
        let StageRole::Collective(identity) = stage.role else {
            continue;
        };
        if format!("{:?}", identity.algorithm) != algorithm {
            continue;
        }
        let bytes = match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => tcp.total_bytes,
            FlowGeneratorKind::Roce(roce) => roce.pacer.total_bytes,
            _ => panic!("a collective stage is carried by TCP or RoCE"),
        };
        assert_eq!(bytes, identity.chunk_bytes);
        stages
            .entry(identity.collective_id)
            .or_default()
            .push(bytes);
    }
    stages
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
                        GeneratorStatus::Finished,
                        "{label}: {:?} did not finish",
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
        for horizon in [None, Some(image.stop_time_ns / 2)] {
            for mode in [ObservationMode::Full, ObservationMode::Summary] {
                let expected = without_diagnostics(scalar(&image, horizon, mode));
                for workers in [1, 2, 4] {
                    let actual = run_cpu_with_observations(
                        &image,
                        horizon,
                        CpuConfig {
                            workers,
                            ..CpuConfig::default()
                        },
                        mode,
                    )
                    .unwrap_or_else(|error| panic!("{label} cpu {workers}: {error}"))
                    .result;
                    assert_eq!(
                        without_diagnostics(actual),
                        expected,
                        "{label} horizon={horizon:?} {mode:?} workers={workers}"
                    );
                }
            }
        }
    }
}

/// ReduceScatter is one ring phase: `n(n-1)` stages, owner chunks under `EqualRemainderLast`.
#[test]
fn reduce_scatter_is_one_ring_phase() {
    let image = lower("rs-tcp-4", &fixtures()[2].1);
    let stages = transport_stages(&image, "ReduceScatter");
    assert_eq!(stages.len(), 1);
    let bytes = stages.values().next().unwrap();
    assert_eq!(bytes.len(), 4 * 3);
    // 10,001 over 4 owners: 2,500 three times and 2,501 for the last owner, each sent 3 times.
    assert_eq!(bytes.iter().sum::<u64>(), 3 * 10_001);
}

/// A two-channel ring under `UniformFloor` sends `floor(floor(S/n)/c)` bytes per message on each
/// channel: `c n (n-1)` stages for AllGather, twice that for AllReduce.
#[test]
fn multi_channel_rings_send_uniform_floor_messages() {
    let all = fixtures();
    let gather = lower("ring-channels-2", &all[4].1);
    let bytes = transport_stages(&gather, "AllGather");
    let bytes = bytes.values().next().unwrap();
    assert_eq!(bytes.len(), 2 * 4 * 3);
    assert!(bytes.iter().all(|&b| b == 16_003 / 4 / 2));
    let reduce = lower("allreduce-channels-2-roce", &all[5].1);
    let bytes = transport_stages(&reduce, "RingAllReduce");
    let bytes = bytes.values().next().unwrap();
    assert_eq!(bytes.len(), 2 * 2 * 4 * 3);
    assert!(bytes.iter().all(|&b| b == 16_000 / 4 / 2));
}

/// A uniform all-to-all is `n(n-1)` ordered-pair messages of `floor(S/n)` bytes.
#[test]
fn uniform_all_to_all_sends_every_ordered_pair_its_floor_share() {
    let image = lower("a2a-uniform-tcp", &fixtures()[6].1);
    let stages = transport_stages(&image, "AllToAll");
    let bytes = stages.values().next().unwrap();
    assert_eq!(bytes.len(), 4 * 3);
    assert!(bytes.iter().all(|&b| b == 8_002 / 4));
}

/// The seeded matrix is a pure function of its parameters: lowering twice gives the same image, a
/// new seed gives other sizes, each source sends at most its `tokens x topk x bytes_per_copy`, and
/// combine (`transpose = true`) returns exactly what dispatch sent.
#[test]
fn seeded_all_to_all_sizes_are_deterministic_and_conserve_bytes() {
    let config = &fixtures()[8].1;
    let first = lower("a2a-seeded-roce", config);
    let again = lower("a2a-seeded-roce", config);
    assert_eq!(first, again);
    let reseeded = lower("a2a-seeded-roce", &config.replace("seed = 7", "seed = 8"));
    assert_ne!(
        transport_stages(&first, "AllToAll"),
        transport_stages(&reseeded, "AllToAll")
    );
    let mut sent = BTreeMap::<(u64, u64, u64), u64>::new();
    for (generator, stage) in stage_generators(&first) {
        let StageRole::Collective(identity) = stage.role else {
            continue;
        };
        let flow = &first.flows[generator.flow.0 as usize];
        sent.insert(
            (identity.collective_id, flow.source.0, flow.target.0),
            identity.chunk_bytes,
        );
    }
    let ids = sent
        .keys()
        .map(|key| key.0)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 2, "dispatch and combine");
    let (dispatch, combine) = (*ids.first().unwrap(), *ids.last().unwrap());
    let mut per_source = BTreeMap::<(u64, u64), u64>::new();
    for (&(id, source, target), &bytes) in &sent {
        *per_source.entry((id, source)).or_default() += bytes;
        let other = if id == dispatch { combine } else { dispatch };
        assert_eq!(sent.get(&(other, target, source)), Some(&bytes));
    }
    // Dispatch's rows are a source's routed copies (its column sums, combine's rows, may exceed).
    assert!([dispatch, combine].iter().any(|&id| {
        per_source
            .iter()
            .filter(|((collective, _), _)| *collective == id)
            .all(|(_, &bytes)| bytes <= 16 * 2 * 100)
    }));
}

/// A join is released at the event that completes its last predecessor: the compute stage after
/// an all-to-all waits for every own send to be acknowledged and every inbound message to arrive.
#[test]
fn a_join_is_released_by_its_last_predecessor() {
    for index in [6, 7, 8] {
        let (label, config) = &fixtures()[index];
        let image = lower(label, config);
        let full = scalar(&image, None, ObservationMode::Full);
        let rows = full
            .diagnostics
            .as_ref()
            .expect("Full observation keeps the diagnostic planes")
            .mechanism_transitions
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Collective(row) => Some(*row),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut joins = 0;
        for (generator, stage) in stage_generators(&image) {
            if !matches!(stage.role, StageRole::Compute(_)) {
                continue;
            }
            let own = rows
                .iter()
                .filter(|row| row.flow == generator.flow)
                .collect::<Vec<_>>();
            let locals = own
                .iter()
                .filter(|row| row.cause == CollectiveActivationCause::LocalCompletion)
                .count();
            if locals < 2 {
                continue;
            }
            joins += 1;
            let release = own
                .iter()
                .find(|row| row.activated)
                .unwrap_or_else(|| panic!("{label}: {:?} was released", generator.flow));
            let last = own.iter().map(|row| row.key).max().unwrap();
            assert_eq!(
                release.key, last,
                "{label}: released by its last predecessor"
            );
        }
        if index == 8 {
            assert!(joins >= 4, "{label}");
        } else {
            assert_eq!(joins, 4 * (1 + usize::from(index > 6)), "{label}");
        }
    }
}

/// A Send/Recv is one message; the receiver's next stage waits for its delivery.
#[test]
fn send_recv_is_one_message() {
    let image = lower("sendrecv", &fixtures()[10].1);
    let stages = transport_stages(&image, "SendRecv");
    assert_eq!(stages.values().next().unwrap(), &vec![9_000]);
}

/// A collective may wait for its stream's previous collective as well as its compute stage.
#[test]
fn a_collective_after_its_stream_predecessor_waits_for_both() {
    let image = lower("data-stream", &fixtures()[9].1);
    let rs = transport_stages(&image, "ReduceScatter");
    assert_eq!(rs.len(), 2);
    let full = scalar(&image, None, ObservationMode::Full);
    assert!(full.diagnostics.is_some());
}

#[test]
fn a_join_of_unknown_or_repeated_groups_is_refused() {
    let unknown = star(
        4,
        &(compute("a", H4, 1_000, "") + &compute("c", H4, 1_000, "after = [\"a\", \"z\"]")),
    );
    assert!(
        lower_text("unknown", &unknown)
            .unwrap_err()
            .contains("depends on unknown stage group `z`")
    );
    let repeated = star(
        4,
        &(compute("a", H4, 1_000, "") + &compute("c", H4, 1_000, "after = [\"a\", \"a\"]")),
    );
    assert!(
        lower_text("repeated", &repeated)
            .unwrap_err()
            .contains("names stage group `a` more than once")
    );
    let cycle = star(
        4,
        &(compute("a", H4, 1_000, "after = [\"b\"]")
            + &compute("b", H4, 1_000, "after = [\"c\"]")
            + &compute("c", H4, 1_000, "after = [\"a\"]")),
    );
    assert!(
        lower_text("cycle", &cycle)
            .unwrap_err()
            .contains("form a cycle")
    );
}

/// A join's predecessor run is validated: strictly ascending, at least two flows, inside
/// `stage_joins`.
#[test]
fn corrupt_joins_are_refused() {
    let image = lower("join-two-computes", &fixtures()[0].1);
    days_executor::validate(&image, days_executor::Backend::Scalar).expect("the join image");
    assert!(!image.stage_joins.is_empty());
    let refused = |mutate: &dyn Fn(&mut SimulationImage)| {
        let mut broken = image.clone();
        mutate(&mut broken);
        days_executor::validate(&broken, days_executor::Backend::Scalar)
            .expect_err("a corrupt join must be refused")
            .to_string()
    };
    assert!(refused(&|image| image.stage_joins.swap(0, 1)).contains("strictly ascending run"));
    assert!(refused(&|image| image.stage_joins.truncate(1)).contains("strictly ascending run"));
    let shrink = |image: &mut SimulationImage| {
        for state in &mut image.host_states {
            for stage in state.stages.iter_mut().flatten() {
                if let days_executor::StagePredecessors::Join { first, .. } =
                    stage.dependencies.local
                {
                    stage.dependencies.local =
                        days_executor::StagePredecessors::Join { first, count: 1 };
                }
            }
        }
    };
    assert!(refused(&shrink).contains("strictly ascending run"));
}

/// The Scalar stage index answers every join query as the retained scans do, on the image and
/// after every event (`tests/scalar_stage_index.rs`).
#[cfg(feature = "test")]
#[test]
fn the_stage_index_agrees_with_the_scans_on_joins() {
    for (label, image) in images() {
        days_executor::scalar::assert_scalar_stage_index_equivalent_for_testing(&image, None)
            .unwrap_or_else(|mismatch| panic!("{label}: {mismatch}"))
            .unwrap_or_else(|error| panic!("{label}: {error}"));
    }
}

/// A typed workload lowers exactly as its TOML rendering (operation `i` is the group `@i`): a
/// compute, a RoCE all-to-all and a TCP ring with two afters.
#[test]
fn a_workload_lowers_as_its_toml_rendering() {
    use days::scenario::workload::{
        Algorithm, Collective, Operation, OperationKind, Transport, Workload,
    };
    let roce_traffic = "initial_delay = 0.0\narr_dist = { type = \"Uniform\", low = 1, high = 1 }\npkt_size_dist = { type = \"DiscreteUniform\", low = 500, high = 500 }\n\n[dcqcn]\nmax_rate_gbps = 8.0\npacing_interval_ns = 500\n\n[roce]\nretransmit_timeout_ns = 1000000\n";
    let tcp_traffic = "initial_delay = 0.0\narr_dist = { type = \"Uniform\", low = 1, high = 1 }\npkt_size_dist = { type = \"DiscreteUniform\", low = 500, high = 500 }\n\n[tcp]\ncc_algorithm = \"TCPReno\"\n";
    let workload = Workload {
        groups: vec![vec![0, 1, 2, 3]],
        transports: vec![
            Transport {
                flow_type: "RoCE".to_owned(),
                priority: 0,
                traffic: roce_traffic.to_owned(),
            },
            Transport {
                flow_type: "TCP".to_owned(),
                priority: 0,
                traffic: tcp_traffic.to_owned(),
            },
        ],
        operations: vec![
            Operation {
                group: 0,
                after: vec![],
                kind: OperationKind::Compute { duration_ns: 2_000 },
            },
            Operation {
                group: 0,
                after: vec![0],
                kind: OperationKind::Collective(Collective {
                    algorithm: Algorithm::AllToAll,
                    bytes: 8_000,
                    transport: 0,
                    channels: None,
                    uniform_floor: true,
                    seeded: None,
                }),
            },
            Operation {
                group: 0,
                after: vec![1],
                kind: OperationKind::Compute { duration_ns: 1_000 },
            },
            Operation {
                group: 0,
                after: vec![2, 1],
                kind: OperationKind::Collective(Collective {
                    algorithm: Algorithm::ReduceScatter,
                    bytes: 12_000,
                    transport: 1,
                    channels: None,
                    uniform_floor: false,
                    seeded: None,
                }),
            },
        ],
    };
    let base = star(4, "");
    let path = std::env::temp_dir().join(format!(
        "days-p16-collops-workload-{}.toml",
        std::process::id()
    ));
    std::fs::write(&path, &base).unwrap();
    let lowered = days::scenario::compile_config_with_workload(
        &path,
        &workload,
        days::topos::route::RouteWorkers::serial(),
    );
    std::fs::remove_file(&path).unwrap();
    let lowered = lowered.unwrap_or_else(|error| panic!("the workload lowers: {error}"));
    let rendering = star(
        4,
        &(compute("@0", H4, 2_000, "")
            + &collective("@1", "AllToAll", "RoCE", H4, "after = \"@0\"\n", 8_000)
            + &compute("@2", H4, 1_000, "after = \"@1\"")
            + &collective(
                "@3",
                "ReduceScatter",
                "TCP",
                H4,
                "sinks = [1, 2, 3, 0]\nafter = [\"@2\", \"@1\"]\n",
                12_000,
            )),
    );
    assert_eq!(lowered, lower("workload-rendering", &rendering));
}

/// The Scalar progress certificate of every fixture is pinned under `lean/fixtures/p10c/`
/// (`collective_collops_<label>_executor_accept.csv`), where the LeanGuard collective campaign
/// accepts it and rejects its mutations. Set `DAYS_UPDATE_COLLECTIVE_TRACE_FIXTURES=1` to
/// regenerate.
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

/// A compute stage after a TCP ring and a RoCE all-to-all has inbound predecessors of two
/// transports, which its one pair of Amendment 5 columns cannot name: the run is exact, but its
/// certificate cannot be written.
#[test]
fn a_join_of_two_transports_has_no_certificate() {
    let image = lower(
        "mixed-join",
        &star(
            4,
            &(compute("fwd", H4, 2_000, "")
                + &collective(
                    "ring",
                    "AllGather",
                    "TCP",
                    H4,
                    "sinks = [1, 2, 3, 0]\nafter = \"fwd\"\n",
                    8_000,
                )
                + &collective("a2a", "AllToAll", "RoCE", H4, "after = \"fwd\"\n", 8_000)
                + &compute("post", H4, 1_000, "after = [\"ring\", \"a2a\"]")),
        ),
    );
    let result = scalar(&image, None, ObservationMode::Full);
    let error = days_executor::collective_transitions_csv(
        &result.diagnostics.as_ref().unwrap().mechanism_transitions,
        &image,
    )
    .unwrap_err();
    assert!(
        matches!(
            error,
            days_executor::CollectiveTraceError::MixedInboundTransports { .. }
        ),
        "{error}"
    );
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

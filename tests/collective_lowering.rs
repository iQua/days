use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    Backend, CollectiveAlgorithm, CollectivePhase, CollectiveStage, CollectiveStageIdentity,
    CpuConfig, FlowGeneratorKind, FlowGeneratorState, GeneratorStatus, HostState,
    MechanismTransitionRecord, ObservationMode, SimulationImage, StageRole,
    run_cpu_with_observations, run_scalar_with_observations, validate,
};

fn collective_config(algorithm: &str) -> String {
    format!(
        r#"
seed = 26
edges = [[0, 4], [1, 4], [2, 4], [3, 4]]
hosts = [0, 1, 2, 3]
duration = 0.0001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[collective]]
collective_type = "{algorithm}"
flow_type = "TCP"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 10
arr_dist = {{ type = "Uniform", low = 0.000000001, high = 0.000000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 3, high = 3 }}

[collective.traffic.tcp]
cc_algorithm = "TCPReno"
"#
    )
}

fn compile_text(label: &str, config: &str) -> Result<SimulationImage, String> {
    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-collective-{label}-{}-{}.toml",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, config).expect("write collective fixture");
    let image = compile_config(&path).map_err(|error| error.to_string());
    fs::remove_file(path).expect("remove collective fixture");
    image
}

fn compile_collective_with_total(algorithm: &str, total_bytes: u64) -> SimulationImage {
    let config =
        collective_config(algorithm).replace("size = 10", &format!("size = {total_bytes}"));
    compile_text(algorithm, &config).expect("collective fixture must lower")
}

fn compile_collective(algorithm: &str) -> SimulationImage {
    compile_collective_with_total(algorithm, 10)
}

fn identity(stage: &CollectiveStage) -> CollectiveStageIdentity {
    let StageRole::Collective(identity) = stage.role else {
        panic!("every expanded flow is a collective stage")
    };
    identity
}

fn set_identity(stage: &mut CollectiveStage, identity: CollectiveStageIdentity) {
    stage.role = StageRole::Collective(identity);
}

/// Every generator of `image` with its stage record; every expanded flow is a stage.
fn stages(image: &SimulationImage) -> impl Iterator<Item = (&FlowGeneratorState, CollectiveStage)> {
    image
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
        .map(|(generator, stage)| (generator, stage.expect("every expanded flow is a stage")))
}

fn owner(identity: CollectiveStageIdentity) -> u64 {
    let offset = match (identity.algorithm, identity.phase) {
        (CollectiveAlgorithm::RingAllReduce, CollectivePhase::AllGather) => 2,
        _ => 1,
    };
    (u64::from(identity.rank) + u64::from(identity.group_size) - u64::from(identity.step) + offset)
        % u64::from(identity.group_size)
}

/// [`stages`], writable.
fn stages_mut(
    image: &mut SimulationImage,
) -> impl Iterator<Item = (&mut FlowGeneratorState, &mut CollectiveStage)> {
    image
        .host_states
        .iter_mut()
        .flat_map(HostState::generators_with_stages_mut)
        .map(|(generator, stage)| (generator, stage.expect("every expanded flow is a stage")))
}

/// Keeps a scheduled root's first TCP segment equal to `min(mss, total_bytes)` after a mutation.
fn resize_initial_segment(
    image: &mut SimulationImage,
    payload: days_executor::PayloadId,
    size: u64,
) {
    image
        .initial_packets
        .iter_mut()
        .find(|packet| packet.id == payload)
        .expect("root packet exists")
        .size_bytes = size;
}

fn run_everywhere(image: &SimulationImage, label: &str) -> days_executor::RunResult {
    validate(image, Backend::Scalar).unwrap();
    let scalar = run_scalar_with_observations(image, None, ObservationMode::Full).unwrap();
    for workers in [1, 2, 4] {
        validate(image, Backend::Cpu { workers }).unwrap();
        let cpu = run_cpu_with_observations(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, scalar, "{label}, workers={workers}");
    }
    scalar
}

fn assert_complete(result: &days_executor::RunResult, label: &str) {
    assert!(result.pending_events.is_empty(), "{label}");
    for (generator, stage) in result
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
    {
        assert_eq!(
            generator.next_emission.status,
            GeneratorStatus::Finished,
            "{label}"
        );
        let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
            panic!("{label}: collective stages are TCP generators")
        };
        assert_eq!(tcp.highest_ack, tcp.total_bytes, "{label}");
        let stage = stage.unwrap();
        assert!(stage.activated && stage.dependencies.prerequisites_complete());
    }
}

#[test]
fn ring_allreduce_and_allgather_lower_to_parametric_tcp_stages() {
    for (algorithm, expected_flows, expected_phase_count) in
        [("RingAllReduce", 24, 2), ("AllGather", 12, 1)]
    {
        let image = compile_collective(algorithm);
        assert_eq!(image.flows.len(), expected_flows);

        let generators = stages(&image).collect::<Vec<_>>();
        assert_eq!(generators.len(), expected_flows);
        assert_eq!(
            generators
                .iter()
                .filter(|(generator, _)| {
                    generator.next_emission.status == GeneratorStatus::Scheduled
                })
                .count(),
            4
        );
        assert_eq!(
            generators
                .iter()
                .filter(|(generator, _)| generator.next_emission.status == GeneratorStatus::Blocked)
                .count(),
            expected_flows - 4
        );

        let mut phases = std::collections::BTreeSet::new();
        let mut chunk_lengths = std::collections::BTreeSet::new();
        for (generator, stage) in generators {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                panic!("every expanded flow uses the ordinary TCP generator")
            };
            let stage = identity(&stage);
            assert_eq!(stage.topology_level, 0);
            assert_eq!(stage.topology_group, 0);
            assert_eq!(stage.group_size, 4);
            assert!(stage.rank < 4);
            assert!((1..4).contains(&stage.step));
            assert_eq!(tcp.mss_bytes, 3);
            assert_eq!(tcp.total_bytes, stage.chunk_bytes);
            phases.insert(stage.phase);
            chunk_lengths.insert(stage.chunk_bytes);
            match algorithm {
                "RingAllReduce" => assert_eq!(stage.algorithm, CollectiveAlgorithm::RingAllReduce),
                "AllGather" => assert_eq!(stage.algorithm, CollectiveAlgorithm::AllGather),
                _ => unreachable!(),
            }
        }
        assert_eq!(phases.len(), expected_phase_count);
        assert!(phases.contains(&CollectivePhase::AllGather));
        assert_eq!(chunk_lengths, [2, 4].into_iter().collect());
    }
}

#[test]
fn collectives_are_scalar_cpu_byte_identical() {
    for algorithm in ["RingAllReduce", "AllGather"] {
        let image = compile_collective(algorithm);
        let result = run_everywhere(&image, algorithm);
        assert_complete(&result, algorithm);
    }
}

#[test]
fn collective_progress_certificates_are_scalar_generated() {
    for (algorithm, expected_activations) in [("AllGather", 8), ("RingAllReduce", 20)] {
        let image = compile_collective(algorithm);
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        let progress = scalar
            .diagnostics
            .as_ref()
            .expect("full scalar observation retains diagnostics")
            .mechanism_transitions
            .iter()
            .filter_map(|record| match record {
                MechanismTransitionRecord::Collective(record) => Some(record),
                _ => None,
            })
            .collect::<Vec<_>>();
        let activation_count = progress.iter().filter(|record| record.activated).count();
        assert!(
            progress.len() > activation_count,
            "{algorithm} must log prerequisite progress before activation"
        );
        assert_eq!(activation_count, expected_activations, "{algorithm}");
        for cause in [
            days_executor::CollectiveActivationCause::LocalCompletion,
            days_executor::CollectiveActivationCause::InboundArrival,
        ] {
            assert!(
                progress.iter().any(|record| record.cause == cause),
                "{algorithm} logs {cause:?} progress"
            );
        }
    }
}

#[test]
fn collective_transport_rejections_are_precise_and_flow_dependencies_stay_rejected() {
    let tcp_options = "\n[collective.traffic.tcp]\ncc_algorithm = \"TCPReno\"\n";
    for (replacement, expected) in [
        (
            "flow_type = \"PacketDistribution\"",
            "unsupported collective flow type `PacketDistribution`; collectives require flow_type = \"TCP\" (RoCE queue pairs arrive in P15)",
        ),
        (
            "flow_type = \"DCQCN\"",
            "unsupported collective flow type `DCQCN`; collectives require flow_type = \"TCP\" (RoCE queue pairs arrive in P15)",
        ),
        (
            "flow_type = \"Rate\"",
            "unsupported collective flow type `Rate`; collectives require flow_type = \"TCP\" (RoCE queue pairs arrive in P15)",
        ),
        (
            "",
            "collective flow_type is missing; collectives require flow_type = \"TCP\" (RoCE queue pairs arrive in P15)",
        ),
    ] {
        let config = collective_config("AllGather")
            .replace("flow_type = \"TCP\"", replacement)
            .replace(tcp_options, "\n");
        assert_eq!(compile_text("reject", &config).unwrap_err(), expected);
    }

    let config = collective_config("AllGather").replace(
        "[[collective]]",
        r#"[[flow]]
flow_type = "PacketDistribution"
starts_after = [0]
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 1
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "DiscreteUniform", low = 1, high = 1 }

[[collective]]"#,
    );
    assert_eq!(
        compile_text("flow-dependency", &config).unwrap_err(),
        "unsupported inter-flow start dependencies; executor generators must be independently scheduled in the lowered image"
    );
}

#[test]
fn collective_validator_rejects_inconsistent_dependency_state() {
    let image = compile_collective("AllGather");
    validate(&image, Backend::Scalar)
        .expect("a pristine dependency-blocked collective must remain legal");
    let blocked = |(generator, _): &(&mut FlowGeneratorState, &mut CollectiveStage)| {
        generator.next_emission.status == GeneratorStatus::Blocked
    };

    let mut reblocked_partial = image.clone();
    let (generator, _) = stages_mut(&mut reblocked_partial)
        .find(blocked)
        .expect("all-gather has blocked descendants");
    generator.packets_emitted = 1;
    let error = validate(&reblocked_partial, Backend::Scalar)
        .expect_err("dependency-blocked collective state cannot contain an emitted prefix")
        .to_string();
    assert!(
        error.contains("dependency-blocked after sending state changed"),
        "{error}"
    );

    let mut inbound_mismatch = image.clone();
    let (_, stage) = stages_mut(&mut inbound_mismatch)
        .find(blocked)
        .expect("all-gather has blocked descendants");
    stage.dependencies.inbound_predecessor_complete = true;
    assert!(
        validate(&inbound_mismatch, Backend::Scalar)
            .expect_err("completion without inbound bytes must reject")
            .to_string()
            .contains("inbound completion flag disagrees with received bytes")
    );

    let mut frontier_mismatch = image.clone();
    let (_, stage) = stages_mut(&mut frontier_mismatch)
        .find(blocked)
        .expect("all-gather has blocked descendants");
    stage.dependencies.inbound_bytes_received = 1;
    assert!(
        validate(&frontier_mismatch, Backend::Scalar)
            .expect_err("inbound bytes must equal the in-order TCP frontier")
            .to_string()
            .contains("disagree with the in-order TCP frontier")
    );

    let mut local_mismatch = image.clone();
    let (_, stage) = stages_mut(&mut local_mismatch)
        .find(blocked)
        .expect("all-gather has blocked descendants");
    stage.dependencies.local_predecessor_complete = true;
    assert!(
        validate(&local_mismatch, Backend::Scalar)
            .expect_err("local completion before the predecessor is acknowledged must reject")
            .to_string()
            .contains("local completion flag disagrees with predecessor state")
    );

    let mut early_release = image;
    let (_, stage) = stages_mut(&mut early_release)
        .find(blocked)
        .expect("all-gather has blocked descendants");
    stage.activated = true;
    assert!(
        validate(&early_release, Backend::Scalar)
            .expect_err("a stage cannot be released before its prerequisites")
            .to_string()
            .contains("release flag disagrees with its prerequisites")
    );
}

#[test]
fn collective_validator_rejects_duplicate_stage_positions() {
    let image = compile_collective("AllGather");
    validate(&image, Backend::Scalar)
        .expect("the complete unique collective stage table must remain legal");

    let mut duplicate = image;
    let duplicate_stage = stages(&duplicate)
        .map(|(_, stage)| identity(&stage))
        .find(|stage| stage.rank == 2 && stage.step == 2)
        .expect("all-gather rank 2 has step 2");
    let (_, stage) = stages_mut(&mut duplicate)
        .find(|(_, stage)| {
            let stage = identity(stage);
            stage.rank == 2 && stage.step == 3
        })
        .expect("all-gather rank 2 has a terminal step 3");
    set_identity(stage, duplicate_stage);

    assert!(
        validate(&duplicate, Backend::Scalar)
            .expect_err("ambiguous collective stage positions must reject")
            .to_string()
            .contains("duplicate collective stage position")
    );
}

#[test]
fn collective_validator_rejects_overlapping_equal_remainder_last_partition() {
    let image = compile_collective("AllGather");
    validate(&image, Backend::Scalar).expect("the canonical [2,2,2,4] partition must validate");

    let mut overlapping = image;
    let mut changed_stages = 0;
    let mut root_payload = None;
    for (generator, record) in stages_mut(&mut overlapping) {
        let mut stage = identity(record);
        if owner(stage) == 0 {
            stage.chunk_bytes = 3;
            set_identity(record, stage);
            record.dependencies.inbound_predecessor_bytes = 3;
            let FlowGeneratorKind::Tcp(mut tcp) = generator.kind else {
                unreachable!()
            };
            tcp.total_bytes = 3;
            generator.kind = FlowGeneratorKind::Tcp(tcp);
            changed_stages += 1;
            if generator.next_emission.status == GeneratorStatus::Scheduled {
                root_payload = Some(generator.next_emission.payload);
            }
        }
    }
    assert_eq!(changed_stages, 3);
    resize_initial_segment(
        &mut overlapping,
        root_payload.expect("owner zero has a root"),
        3,
    );

    let error = validate(&overlapping, Backend::Scalar)
        .expect_err("[0,3), [2,4), [4,6), [6,10) is not a partition")
        .to_string();
    assert!(error.contains("collective partition"), "{error}");
}

#[test]
fn collective_validator_binds_partition_to_the_declared_total() {
    let image = compile_collective("AllGather");
    let mut changed_total = image;
    let mut root_payloads = Vec::new();
    for (generator, record) in stages_mut(&mut changed_total) {
        let mut stage = identity(record);
        stage.chunk_offset_bytes = owner(stage) * 3;
        stage.chunk_bytes = 3;
        set_identity(record, stage);
        record.dependencies.inbound_predecessor_bytes = 3;
        let FlowGeneratorKind::Tcp(mut tcp) = generator.kind else {
            unreachable!()
        };
        tcp.total_bytes = 3;
        generator.kind = FlowGeneratorKind::Tcp(tcp);
        if generator.next_emission.status == GeneratorStatus::Scheduled {
            root_payloads.push(generator.next_emission.payload);
        }
    }
    for payload in root_payloads {
        resize_initial_segment(&mut changed_total, payload, 3);
    }

    let error = validate(&changed_total, Backend::Scalar)
        .expect_err("declared total 10 cannot be changed to a self-consistent total 12")
        .to_string();
    assert!(error.contains("collective partition"), "{error}");

    let canonical_twelve = compile_collective_with_total("AllGather", 12);
    assert!(
        stages(&canonical_twelve)
            .map(|(_, stage)| identity(&stage))
            .all(|stage| stage.declared_total_bytes == 12 && stage.chunk_bytes == 3)
    );
    let result = run_everywhere(&canonical_twelve, "declared twelve");
    assert_complete(&result, "declared twelve");

    let mut inconsistent_total = canonical_twelve;
    let (_, record) = stages_mut(&mut inconsistent_total)
        .next()
        .expect("collective stage");
    let mut stage = identity(record);
    stage.declared_total_bytes = 11;
    set_identity(record, stage);
    let error = validate(&inconsistent_total, Backend::Scalar)
        .expect_err("declared total must agree across all stages")
        .to_string();
    assert!(error.contains("metadata is inconsistent"), "{error}");
}

#[test]
fn collective_algorithm_specific_boundaries_are_canonical() {
    let single = collective_config("AllGather")
        .replace("flow_count = 4", "flow_count = 1")
        .replace(
            "sources = [0, 1, 2, 3]\nsinks = [1, 2, 3, 0]\n",
            "sources = [0]\nsinks = [0]\n",
        );
    let image = compile_text("allgather-single", &single).expect("AllGather n=1 must lower");
    assert!(image.flows.is_empty());
    assert!(image.initial_packets.is_empty());
    assert!(image.initial_events.is_empty());
    let scalar = run_everywhere(&image, "allgather n=1");
    assert_eq!(scalar.summary.sourced_bytes, 0);

    for (algorithm, flow_count, total_bytes, expected) in [
        (
            "RingAllReduce",
            1,
            10,
            "invalid scenario: RingAllReduce flow_count must be at least 2",
        ),
        (
            "RingAllReduce",
            4,
            3,
            "invalid scenario: RingAllReduce byte size 3 must be at least flow_count 4",
        ),
        (
            "AllGather",
            4,
            3,
            "invalid scenario: TCP collective byte size 3 must be at least flow_count 4",
        ),
        (
            "AllGather",
            4,
            0,
            "invalid scenario: TCP traffic `size` must be positive",
        ),
    ] {
        let config = collective_config(algorithm)
            .replace("flow_count = 4", &format!("flow_count = {flow_count}"))
            .replace("size = 10", &format!("size = {total_bytes}"))
            .replace(
                "sources = [0, 1, 2, 3]\nsinks = [1, 2, 3, 0]\n",
                if flow_count == 1 {
                    "sources = [0]\nsinks = [0]\n"
                } else {
                    "sources = [0, 1, 2, 3]\nsinks = [1, 2, 3, 0]\n"
                },
            );
        assert_eq!(compile_text("boundary", &config).unwrap_err(), expected);
    }

    let minimum_ring = collective_config("RingAllReduce")
        .replace("flow_count = 4", "flow_count = 2")
        .replace("sources = [0, 1, 2, 3]", "sources = [0, 1]")
        .replace("sinks = [1, 2, 3, 0]", "sinks = [1, 0]")
        .replace("size = 10", "size = 2");
    let image = compile_text("ring-minimum", &minimum_ring).expect("minimum ring must lower");
    let result = run_everywhere(&image, "minimum ring");
    assert_complete(&result, "minimum ring");
    let data_bytes = stages(&image)
        .map(|(_, stage)| identity(&stage).chunk_bytes)
        .sum::<u64>();
    assert_eq!(data_bytes, 4);
}

#[test]
fn collective_roots_beyond_the_stop_leave_every_descendant_unreleased() {
    let config =
        collective_config("RingAllReduce").replace("initial_delay = 0.0", "initial_delay = 0.001");
    let image = compile_text("terminal", &config).expect("late roots must lower");
    assert_eq!(image.initial_events.len(), 4);
    assert!(
        image
            .initial_events
            .iter()
            .all(|event| event.key.time_ns > image.stop_time_ns)
    );
    let result = run_everywhere(&image, "late roots");
    assert_eq!(result.summary.sourced_packets, 0);
    for (generator, stage) in result
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
    {
        let stage = stage.expect("every expanded flow is a stage");
        let root = stage.dependencies.local_predecessor.is_none()
            && stage.dependencies.inbound_predecessor.is_none();
        assert_eq!(stage.activated, root);
        assert_eq!(generator.packets_emitted, 0);
        assert_eq!(
            generator.next_emission.status,
            if root {
                GeneratorStatus::Scheduled
            } else {
                GeneratorStatus::Blocked
            }
        );
    }
}

#[test]
fn collective_maximum_group_width_rejects_without_validator_panic() {
    let mut image = compile_collective("AllGather");
    let (_, record) = stages_mut(&mut image)
        .find(|(_, stage)| identity(stage).rank > 0)
        .expect("all-gather has a nonzero-rank stage");
    let mut stage = identity(record);
    stage.group_size = u32::MAX;
    stage.rank = u32::MAX - 1;
    set_identity(record, stage);

    let result = std::panic::catch_unwind(|| validate(&image, Backend::Scalar));
    assert!(
        result.is_ok(),
        "maximum-width metadata must not panic validation"
    );
    assert!(
        result
            .unwrap()
            .expect_err("incomplete maximum-width topology metadata must reject")
            .to_string()
            .contains("predecessor identities")
    );
}

#[test]
fn collective_device_capability_rejection_is_precise() {
    let image = compile_collective("AllGather");
    for backend in [Backend::Metal, Backend::Cuda] {
        assert_eq!(
            validate(&image, backend).unwrap_err().to_string(),
            format!("backend {backend} does not support collective generators; use Scalar or Cpu")
        );
    }
}

#[test]
fn blocked_collective_stages_reserve_their_payload_identities() {
    let image = compile_collective("RingAllReduce");
    validate(&image, Backend::Scalar).expect("the lowered image reserves within capacity");
    let node_count = image.nodes.len() as u64;
    let mut exhausted = image;
    for node in exhausted
        .nodes
        .iter()
        .filter(|node| node.kind == days_executor::NodeKind::Host)
    {
        // The last representable identity is already allocated: no future payload fits.
        let maximum_sequence = (u64::MAX - node.id.0) / node_count;
        exhausted.host_states[node.state_slot as usize].next_payload_seq = maximum_sequence + 1;
    }
    let validation = validate(&exhausted, Backend::Scalar)
        .expect_err("dependency-blocked stages must reserve future payload identities")
        .to_string();
    assert!(
        validation.contains("payload identity sequence"),
        "{validation}"
    );
}

/// P14 slim round 2: a host's stage table is empty or parallel to its generators with at least one
/// stage. Those are the only shapes the `Debug` rendering, which prints each record inline in its
/// generator, determines, so the validator rejects every other shape.
#[test]
fn validator_accepts_only_canonical_stage_tables() {
    let image = compile_collective("AllGather");
    validate(&image, Backend::Scalar).expect("the lowered stage tables are canonical");
    let slot = image
        .host_states
        .iter()
        .position(|state| !state.stages.is_empty())
        .expect("a host with stages");

    let mut long = image.clone();
    long.host_states[slot].stages.push(None);
    let mut short = image.clone();
    short.host_states[slot].stages.pop();
    for (label, mutated) in [("one entry too many", long), ("one entry too few", short)] {
        let error = validate(&mutated, Backend::Scalar)
            .expect_err(label)
            .to_string();
        assert!(
            error.contains(&format!("host state slot {slot} has"))
                && error.contains("stage table entries for"),
            "{label}: {error}"
        );
    }

    let mut without_stage = image;
    without_stage.host_states[slot].stages.fill(None);
    let error = validate(&without_stage, Backend::Scalar)
        .expect_err("a table without a stage")
        .to_string();
    assert!(
        error.contains(&format!(
            "host state slot {slot} has a stage table without a stage"
        )),
        "{error}"
    );
}

/// P14 slim round 2: a host that carries plain flows and stages keeps one stage-table entry per
/// generator. Plain flows sort before stages, so lowering back-fills `None` for them when the host's
/// first stage arrives; the image validates and runs identically on Scalar and CPU.
#[test]
fn mixed_hosts_keep_a_parallel_stage_table() {
    let config = collective_config("RingAllReduce")
        + r#"
[[flow_set]]
flow_type = "TCP"
flow_count = 4

[flow_set.traffic]
initial_delay = 0.0
size = 10
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "DiscreteUniform", low = 3, high = 3 }

[flow_set.traffic.tcp]
cc_algorithm = "TCPReno"
"#;
    let image =
        compile_text("mixed-hosts", &config).expect("a collective beside plain flows lowers");
    validate(&image, Backend::Scalar).expect("mixed hosts validate");
    let mut mixed_hosts = 0;
    for state in &image.host_states {
        let stages = state.stages.iter().flatten().count();
        let plain = state.generators.len() - stages;
        if stages == 0 {
            assert!(
                state.stages.is_empty(),
                "a host without stages has no table"
            );
            continue;
        }
        assert_eq!(state.stages.len(), state.generators.len());
        if plain > 0 {
            mixed_hosts += 1;
            // The plain flows come first in canonical flow order and carry no record.
            assert!(state.stages[..plain].iter().all(Option::is_none));
            assert!(state.stages[plain..].iter().all(Option::is_some));
        }
    }
    assert!(
        mixed_hosts > 0,
        "the scenario must put plain flows beside stages"
    );
    run_everywhere(&image, "mixed hosts");
}

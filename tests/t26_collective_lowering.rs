use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    Backend, CollectiveAlgorithm, CollectivePhase, CpuConfig, FlowGeneratorKind, GeneratorStatus,
    ObservationMode, run_cpu_with_observations, run_scalar_with_observations, validate,
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
flow_type = "PacketDistribution"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]

[collective.traffic]
initial_delay = 0.0
size = 10
arr_dist = {{ type = "Uniform", low = 0.000000001, high = 0.000000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 3, high = 3 }}
"#
    )
}

fn compile_collective(algorithm: &str) -> days_executor::SimulationImage {
    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-t26-collective-{}-{}-{algorithm}.toml",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, collective_config(algorithm)).expect("write collective fixture");
    let image = compile_config(&path).expect("collective fixture must lower");
    fs::remove_file(path).expect("remove collective fixture");
    image
}

#[test]
fn ring_allreduce_and_allgather_lower_to_one_parametric_generator_kind() {
    for (algorithm, expected_flows, expected_phase_count) in
        [("RingAllReduce", 24, 2), ("AllGather", 12, 1)]
    {
        let image = compile_collective(algorithm);
        assert_eq!(image.flows.len(), expected_flows);

        let generators = image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .collect::<Vec<_>>();
        assert_eq!(generators.len(), expected_flows);
        assert_eq!(
            generators
                .iter()
                .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
                .count(),
            4
        );
        assert_eq!(
            generators
                .iter()
                .filter(|generator| generator.next_emission.status == GeneratorStatus::Blocked)
                .count(),
            expected_flows - 4
        );

        let mut phases = std::collections::BTreeSet::new();
        let mut chunk_lengths = std::collections::BTreeSet::new();
        for generator in generators {
            let FlowGeneratorKind::Collective(stage) = generator.kind else {
                panic!("every expanded flow must use the collective generator")
            };
            assert_eq!(stage.topology_level, 0);
            assert_eq!(stage.topology_group, 0);
            assert_eq!(stage.group_size, 4);
            assert!(stage.rank < 4);
            assert!((1..4).contains(&stage.step));
            assert_eq!(stage.packet_size_bytes, 3);
            assert_eq!(stage.interval_ns, 1);
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
        validate(&image, Backend::Scalar).unwrap();
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        assert!(scalar.pending_events.is_empty());
        let (expected_packets, expected_bytes) = match algorithm {
            "RingAllReduce" => (30, 60),
            "AllGather" => (15, 30),
            _ => unreachable!(),
        };
        assert_eq!(scalar.summary.sourced_packets, expected_packets);
        assert_eq!(scalar.summary.sourced_bytes, expected_bytes);
        assert!(
            scalar
                .host_states
                .iter()
                .flat_map(|state| &state.generators)
                .all(|generator| generator.next_emission.status == GeneratorStatus::Finished)
        );

        for workers in [1, 2, 4] {
            validate(&image, Backend::Cpu { workers }).unwrap();
            let cpu = run_cpu_with_observations(
                &image,
                None,
                CpuConfig {
                    workers,
                    ..CpuConfig::default()
                },
                ObservationMode::Full,
            )
            .unwrap();
            assert_eq!(cpu.result, scalar, "{algorithm}, workers={workers}");
        }
    }
}

#[test]
fn collective_transport_rejections_are_precise_and_flow_dependencies_stay_rejected() {
    for flow_type in ["TCP", "DCQCN"] {
        let config = collective_config("AllGather").replace(
            "flow_type = \"PacketDistribution\"",
            &format!("flow_type = \"{flow_type}\""),
        );
        let path = std::env::temp_dir().join(format!(
            "days-t26-collective-reject-{}-{flow_type}.toml",
            std::process::id()
        ));
        fs::write(&path, config).unwrap();
        let error = compile_config(&path).expect_err("transport must be rejected");
        fs::remove_file(path).unwrap();
        assert_eq!(
            error.to_string(),
            format!(
                "unsupported collective flow type `{flow_type}`; T26 collectives require deterministic byte-terminated PacketDistribution traffic"
            )
        );
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
    let path = std::env::temp_dir().join(format!(
        "days-t26-flow-dependency-reject-{}.toml",
        std::process::id()
    ));
    fs::write(&path, config).unwrap();
    let error = compile_config(&path).expect_err("ordinary dependency must stay rejected");
    fs::remove_file(path).unwrap();
    assert_eq!(
        error.to_string(),
        "unsupported inter-flow start dependencies; executor generators must be independently scheduled in the lowered image"
    );
}

#[test]
fn collective_validator_rejects_inconsistent_dependency_state() {
    let image = compile_collective("AllGather");
    validate(&image, Backend::Scalar)
        .expect("a pristine dependency-blocked collective must remain legal");

    let mut reblocked_partial = image.clone();
    let state = reblocked_partial
        .host_states
        .iter_mut()
        .find(|state| {
            state.generators.iter().any(|generator| {
                generator.next_emission.status == GeneratorStatus::Blocked
                    && matches!(generator.kind, FlowGeneratorKind::Collective(stage)
                        if stage.chunk_bytes > stage.packet_size_bytes)
            })
        })
        .expect("all-gather has a multi-packet blocked descendant");
    let generator = state
        .generators
        .iter_mut()
        .find(|generator| {
            generator.next_emission.status == GeneratorStatus::Blocked
                && matches!(generator.kind, FlowGeneratorKind::Collective(stage)
                    if stage.chunk_bytes > stage.packet_size_bytes)
        })
        .expect("selected host owns the blocked descendant");
    let FlowGeneratorKind::Collective(stage) = generator.kind else {
        unreachable!()
    };
    generator.packets_emitted = 1;
    generator.bytes_emitted = stage.packet_size_bytes;
    state.next_payload_seq += 1;
    assert!(
        validate(&reblocked_partial, Backend::Scalar)
            .expect_err("dependency-blocked collective state cannot contain an emitted prefix")
            .to_string()
            .contains("dependency-blocked after emitting")
    );

    let mut inbound_mismatch = image.clone();
    let generator = inbound_mismatch
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
        .find(|generator| generator.next_emission.status == GeneratorStatus::Blocked)
        .expect("all-gather has blocked descendants");
    let FlowGeneratorKind::Collective(mut stage) = generator.kind else {
        unreachable!()
    };
    stage.inbound_predecessor_complete = true;
    generator.kind = FlowGeneratorKind::Collective(stage);
    assert!(
        validate(&inbound_mismatch, Backend::Scalar)
            .expect_err("completion without inbound bytes must reject")
            .to_string()
            .contains("inbound completion flag disagrees with received bytes")
    );

    let mut local_mismatch = image;
    let generator = local_mismatch
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
        .find(|generator| generator.next_emission.status == GeneratorStatus::Blocked)
        .expect("all-gather has blocked descendants");
    let FlowGeneratorKind::Collective(mut stage) = generator.kind else {
        unreachable!()
    };
    stage.local_predecessor_complete = true;
    generator.kind = FlowGeneratorKind::Collective(stage);
    assert!(
        validate(&local_mismatch, Backend::Scalar)
            .expect_err("local completion before predecessor finish must reject")
            .to_string()
            .contains("local completion flag disagrees with predecessor state")
    );
}

#[test]
fn collective_validator_rejects_duplicate_stage_positions() {
    let image = compile_collective("AllGather");
    validate(&image, Backend::Scalar)
        .expect("the complete unique collective stage table must remain legal");

    let mut duplicate = image;
    let duplicate_stage = duplicate
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .find_map(|generator| match generator.kind {
            FlowGeneratorKind::Collective(stage) if stage.rank == 2 && stage.step == 2 => {
                Some(stage)
            }
            _ => None,
        })
        .expect("all-gather rank 2 has step 2");
    let generator = duplicate
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
        .find(|generator| {
            matches!(generator.kind, FlowGeneratorKind::Collective(stage)
                if stage.rank == 2 && stage.step == 3)
        })
        .expect("all-gather rank 2 has a terminal step 3");
    generator.kind = FlowGeneratorKind::Collective(duplicate_stage);

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
    for generator in overlapping
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
    {
        let FlowGeneratorKind::Collective(mut stage) = generator.kind else {
            continue;
        };
        let owner = (u64::from(stage.rank) + u64::from(stage.group_size) - u64::from(stage.step)
            + 1)
            % u64::from(stage.group_size);
        if owner == 0 {
            stage.chunk_bytes = 3;
            stage.inbound_predecessor_bytes = 3;
            generator.kind = FlowGeneratorKind::Collective(stage);
            changed_stages += 1;
            if generator.next_emission.status == GeneratorStatus::Scheduled {
                root_payload = Some(generator.next_emission.payload);
            }
        }
    }
    assert_eq!(changed_stages, 3);
    let root_payload = root_payload.expect("owner zero has one scheduled root stage");
    overlapping
        .initial_packets
        .iter_mut()
        .find(|packet| packet.id == root_payload)
        .expect("root packet exists")
        .size_bytes = 3;

    let error = validate(&overlapping, Backend::Scalar)
        .expect_err("[0,3), [2,4), [4,6), [6,10) is not a partition")
        .to_string();
    assert!(error.contains("collective partition"), "{error}");
}

#[test]
fn collective_validator_binds_partition_to_the_declared_total() {
    let image = compile_collective("AllGather");
    let mut changed_total = image;
    for generator in changed_total
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
    {
        let FlowGeneratorKind::Collective(mut stage) = generator.kind else {
            continue;
        };
        let owner = (u64::from(stage.rank) + u64::from(stage.group_size) - u64::from(stage.step)
            + 1)
            % u64::from(stage.group_size);
        stage.chunk_offset_bytes = owner * 3;
        stage.chunk_bytes = 3;
        stage.inbound_predecessor_bytes = 3;
        generator.kind = FlowGeneratorKind::Collective(stage);
        if generator.next_emission.status == GeneratorStatus::Scheduled {
            changed_total
                .initial_packets
                .iter_mut()
                .find(|packet| packet.id == generator.next_emission.payload)
                .expect("scheduled collective packet")
                .size_bytes = 3;
        }
    }

    let error = validate(&changed_total, Backend::Scalar)
        .expect_err("declared total 10 cannot be changed to a self-consistent total 12")
        .to_string();
    assert!(error.contains("collective partition"), "{error}");

    let config = collective_config("AllGather").replace("size = 10", "size = 12");
    let path = std::env::temp_dir().join(format!(
        "days-t26-allgather-declared-twelve-{}.toml",
        std::process::id()
    ));
    fs::write(&path, config).unwrap();
    let canonical_twelve = compile_config(&path).expect("declared total 12 must lower canonically");
    fs::remove_file(path).unwrap();
    assert!(
        canonical_twelve
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .all(
                |generator| matches!(generator.kind, FlowGeneratorKind::Collective(stage)
            if stage.declared_total_bytes == 12 && stage.chunk_bytes == 3)
            )
    );
    let scalar = run_scalar_with_observations(&canonical_twelve, None, ObservationMode::Full)
        .expect("declared total 12 must execute");
    assert_eq!(scalar.summary.sourced_bytes, 36);

    let mut inconsistent_total = canonical_twelve;
    let generator = inconsistent_total
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
        .next()
        .expect("collective stage");
    let FlowGeneratorKind::Collective(mut stage) = generator.kind else {
        unreachable!()
    };
    stage.declared_total_bytes = 11;
    generator.kind = FlowGeneratorKind::Collective(stage);
    let error = validate(&inconsistent_total, Backend::Scalar)
        .expect_err("declared total must agree across all stages")
        .to_string();
    assert!(error.contains("metadata is inconsistent"), "{error}");
}

#[test]
fn collective_algorithm_specific_no_op_boundaries_are_canonical() {
    for (flow_count, total_bytes) in [(1, 10), (4, 0)] {
        let config = collective_config("AllGather")
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
        let path = std::env::temp_dir().join(format!(
            "days-t26-allgather-noop-{}-{flow_count}-{total_bytes}.toml",
            std::process::id()
        ));
        fs::write(&path, config).unwrap();
        let image = compile_config(&path).expect("AllGather no-op must lower");
        fs::remove_file(path).unwrap();
        assert!(image.flows.is_empty());
        assert!(image.initial_packets.is_empty());
        assert!(image.initial_events.is_empty());
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("AllGather no-op must execute");
        assert_eq!(scalar.summary.sourced_bytes, 0);
        for workers in [1, 2, 4] {
            let cpu = run_cpu_with_observations(
                &image,
                None,
                CpuConfig {
                    workers,
                    ..CpuConfig::default()
                },
                ObservationMode::Full,
            )
            .expect("AllGather no-op must execute on CPU");
            assert_eq!(cpu.result, scalar);
        }
    }

    for (flow_count, total_bytes) in [(1, 10), (4, 0), (4, 3)] {
        let config = collective_config("RingAllReduce")
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
        let path = std::env::temp_dir().join(format!(
            "days-t26-ring-reject-{}-{flow_count}-{total_bytes}.toml",
            std::process::id()
        ));
        fs::write(&path, config).unwrap();
        let error = compile_config(&path).expect_err("RingAllReduce traffic boundary must reject");
        fs::remove_file(path).unwrap();
        assert!(error.to_string().contains("RingAllReduce"), "{error}");
    }

    let minimum_ring = collective_config("RingAllReduce")
        .replace("flow_count = 4", "flow_count = 2")
        .replace("sources = [0, 1, 2, 3]", "sources = [0, 1]")
        .replace("sinks = [1, 2, 3, 0]", "sinks = [1, 0]")
        .replace("size = 10", "size = 2");
    let path = std::env::temp_dir().join(format!(
        "days-t26-ring-minimum-active-{}.toml",
        std::process::id()
    ));
    fs::write(&path, minimum_ring).unwrap();
    let image = compile_config(&path).expect("minimum active RingAllReduce must lower");
    fs::remove_file(path).unwrap();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("minimum active RingAllReduce must execute");
    assert_eq!(scalar.summary.sourced_bytes, 4);
}

#[test]
fn allgather_partial_zero_partition_executes_only_nonzero_chunks() {
    let config = collective_config("AllGather").replace("size = 10", "size = 1");
    let path = std::env::temp_dir().join(format!(
        "days-t26-allgather-zero-chunks-{}.toml",
        std::process::id()
    ));
    fs::write(&path, config).unwrap();
    let image = compile_config(&path).expect("[0,0,0,1] AllGather partition must lower");
    fs::remove_file(path).unwrap();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("partial-zero AllGather must execute");
    assert_eq!(scalar.summary.sourced_bytes, 3);
    assert!(scalar.pending_events.is_empty());
    for workers in [1, 2, 4] {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("partial-zero AllGather must execute on CPU");
        assert_eq!(cpu.result, scalar);
    }
}

#[test]
fn collective_terminal_closure_is_zero_and_maximum_state_remains_representable() {
    let config = collective_config("RingAllReduce")
        .replace("initial_delay = 0.0", "initial_delay = 0.001")
        .replace("size = 10", "size = 9223372036854775807")
        .replace(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 3, high = 3 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 9223372036854775807, high = 9223372036854775807 }",
        );
    let path = std::env::temp_dir().join(format!(
        "days-t26-collective-terminal-{}.toml",
        std::process::id()
    ));
    fs::write(&path, config).unwrap();
    let mut image = compile_config(&path).expect("maximum legal TOML integers must lower");
    fs::remove_file(path).unwrap();

    assert_eq!(image.initial_packets.len(), 0);
    assert_eq!(image.initial_events.len(), 0);
    for state in &mut image.host_states {
        state.next_payload_seq = u64::MAX;
        state.next_origin_seq = u64::MAX;
        state.sourced_packets = u64::MAX;
        state.departed_packets = u64::MAX;
        state.received_packets = u64::MAX;
    }
    for state in &mut image.switch_states {
        state.next_origin_seq = u64::MAX;
        state.arrived_packets = u64::MAX;
        state.dropped_packets = u64::MAX;
        state.departed_packets = u64::MAX;
    }
    validate(&image, Backend::Scalar)
        .expect("terminal roots and their blocked descendants reserve exactly zero work");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("zero-work maximum state must not fault");
    assert_eq!(result.summary.sourced_packets, 0);
    assert!(result.pending_events.is_empty());
}

#[test]
fn collective_maximum_group_width_rejects_without_validator_panic() {
    let mut image = compile_collective("AllGather");
    let generator = image
        .host_states
        .iter_mut()
        .flat_map(|state| &mut state.generators)
        .find(|generator| {
            matches!(generator.kind, FlowGeneratorKind::Collective(stage) if stage.rank > 0)
        })
        .expect("all-gather has a nonzero-rank stage");
    let FlowGeneratorKind::Collective(mut stage) = generator.kind else {
        unreachable!()
    };
    stage.group_size = u32::MAX;
    stage.rank = u32::MAX - 1;
    generator.kind = FlowGeneratorKind::Collective(stage);

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
fn collective_full_u64_stop_horizon_is_representable() {
    let mut image = compile_collective("AllGather");
    image.stop_time_ns = u64::MAX;

    let result = std::panic::catch_unwind(|| validate(&image, Backend::Scalar));
    assert!(
        result.is_ok(),
        "full-width stop horizon must not panic validation"
    );
    result
        .unwrap()
        .expect("finite collective work remains representable at the full u64 stop horizon");
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
fn collective_capacity_closure_accepts_exact_boundary_and_rejects_one_past() {
    let image = compile_collective("RingAllReduce");
    let node_count = image.nodes.len() as u64;
    let future_by_host = image
        .host_states
        .iter()
        .map(|state| {
            state
                .generators
                .iter()
                .map(|generator| {
                    let FlowGeneratorKind::Collective(stage) = generator.kind else {
                        unreachable!()
                    };
                    stage.chunk_bytes.div_ceil(stage.packet_size_bytes)
                })
                .sum::<u64>()
        })
        .collect::<Vec<_>>();

    let mut payload_boundary = image.clone();
    for node in payload_boundary
        .nodes
        .iter()
        .filter(|node| node.kind == days_executor::NodeKind::Host)
    {
        let state = &mut payload_boundary.host_states[node.state_slot as usize];
        let scheduled = state
            .generators
            .iter()
            .filter(|generator| generator.next_emission.status == GeneratorStatus::Scheduled)
            .count() as u64;
        let allocations = future_by_host[node.state_slot as usize] - scheduled;
        let maximum_sequence = (u64::MAX - node.id.0) / node_count;
        state.next_payload_seq = maximum_sequence + 1 - allocations;
    }
    validate(&payload_boundary, Backend::Scalar)
        .expect("the last representable payload identity must validate");
    run_scalar_with_observations(&payload_boundary, None, ObservationMode::Full)
        .expect("an accepted payload boundary must execute without a capacity fault");

    let mut payload_overflow = payload_boundary;
    payload_overflow.host_states[0].next_payload_seq += 1;
    let validation = validate(&payload_overflow, Backend::Scalar)
        .expect_err("one identity past the exact payload boundary must reject")
        .to_string();
    assert!(validation.contains("payload identity sequence"));
    let execution = run_scalar_with_observations(&payload_overflow, None, ObservationMode::Full)
        .expect_err("the rejected image must demonstrate the protected payload fault")
        .to_string();
    assert!(execution.contains("payload identity sequence exhausted"));

    let mut counter_boundary = image.clone();
    for (state, future) in counter_boundary.host_states.iter_mut().zip(&future_by_host) {
        state.sourced_packets = u64::MAX - future;
    }
    validate(&counter_boundary, Backend::Scalar)
        .expect("the exact sourced-counter boundary must validate");
    run_scalar_with_observations(&counter_boundary, None, ObservationMode::Full)
        .expect("an accepted sourced-counter boundary must execute without overflow");

    let mut counter_overflow = counter_boundary;
    counter_overflow.host_states[0].sourced_packets += 1;
    let validation = validate(&counter_overflow, Backend::Scalar)
        .expect_err("one packet past the sourced-counter boundary must reject")
        .to_string();
    assert!(validation.contains("sourced_packets") && validation.contains("remaining upper bound"));
    let execution = run_scalar_with_observations(&counter_overflow, None, ObservationMode::Full)
        .expect_err("the rejected image must demonstrate the protected counter fault")
        .to_string();
    assert!(execution.contains("counter overflow"));
}

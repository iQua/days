//! P14 T1: a collective stage is a dependency record attached to an ordinary generator.
//!
//! The record lives in the host's stage table, parallel to its generators, is read and written
//! through one accessor pair on the host, and renders inline in its generator: absent, it is
//! invisible in the complete-state rendering, and the table itself never renders.

use std::collections::VecDeque;

use days_executor::{
    CollectiveAlgorithm, CollectiveChannelPolicy, CollectiveChunkPolicy, CollectivePhase,
    CollectiveStage, CollectiveStageIdentity, ConstantGenerator, FlowGeneratorKind,
    FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination,
    HostState, LinkId, PayloadId, ScheduledEmission, StageDependencies, StagePredecessors,
    StageRole, TcpCongestionControl, TcpGenerator,
};

fn generator(kind: FlowGeneratorKind) -> FlowGeneratorState {
    FlowGeneratorState {
        flow: FlowId(7),
        packets_emitted: 0,
        bytes_emitted: 0,
        next_emission: ScheduledEmission {
            status: GeneratorStatus::Blocked,
            departure_time_ns: 0,
            payload: PayloadId(0),
        },
        rng_state: 3,
        feedback: GeneratorFeedbackState {
            arrivals: 0,
            outstanding_bytes: 0,
            unacknowledged_bytes: 0,
        },
        kind,
    }
}

/// A host owning `generator`, with `stage` as its stage record: a host without stages has an empty
/// stage table.
fn host(generator: FlowGeneratorState, stage: Option<CollectiveStage>) -> HostState {
    HostState {
        egress_link: LinkId(0),
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: vec![generator],
        stages: stage.map_or_else(Vec::new, |stage| vec![Some(stage)]),
        tcp_receivers: Vec::new(),
        dcqcn_receivers: Vec::new(),
        roce_receivers: None,
        pfc: None,
        next_origin_seq: 0,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    }
}

fn constant() -> FlowGeneratorKind {
    FlowGeneratorKind::Constant(ConstantGenerator {
        first_departure_ns: 0,
        interval_ns: 1,
        packet_size_bytes: 1,
        termination: GeneratorTermination::Bytes(1),
    })
}

fn tcp() -> FlowGeneratorKind {
    FlowGeneratorKind::Tcp(TcpGenerator::new(3, 1, 1, TcpCongestionControl::reno(1)))
}

fn collective_stage() -> CollectiveStage {
    CollectiveStage {
        role: StageRole::Collective(identity()),
        dependencies: dependencies(),
        activated: false,
    }
}

fn identity() -> CollectiveStageIdentity {
    CollectiveStageIdentity {
        collective_id: 0,
        algorithm: CollectiveAlgorithm::RingAllReduce,
        channel: 0,
        group_size: 3,
        declared_total_bytes: 9,
        rank: 1,
        phase: CollectivePhase::ReduceScatter,
        step: 2,
        chunk_policy: CollectiveChunkPolicy::EqualRemainderLast,
        channel_policy: CollectiveChannelPolicy::RingNext,
        chunk_offset_bytes: 0,
        chunk_bytes: 3,
    }
}

fn dependencies() -> StageDependencies {
    StageDependencies {
        local: StagePredecessors::One(FlowId(4)),
        inbound: StagePredecessors::One(FlowId(2)),
        inbound_predecessor_bytes: 3,
        inbound_bytes_received: 0,
        local_completed: 0,
    }
}

#[test]
fn absent_sidecar_is_invisible_in_the_complete_state_rendering() {
    let plain = host(generator(constant()), None);
    let rendered = format!("{plain:#?}");
    assert!(!rendered.contains("stage"), "{rendered}");
    // A generator renders alone exactly as it renders in a host without stages.
    let alone = format!("{:#?}", plain.generators[0]);
    assert!(!alone.contains("stage"), "{alone}");
    assert!(alone.starts_with("FlowGeneratorState {\n    flow: FlowId(\n"));
    assert!(alone.ends_with("    ),\n}"), "{alone}");

    let wrapped = host(generator(tcp()), Some(collective_stage()));
    let rendered = format!("{wrapped:#?}");
    // The record renders inline in its generator, where the generator field used to hold it; the
    // table does not render.
    assert!(
        rendered.contains("            stage: Some(\n"),
        "{rendered}"
    );
    assert!(!rendered.contains("stages"), "{rendered}");
}

#[test]
fn stage_dependencies_round_trip_through_the_host_table() {
    let mut wrapped = host(generator(tcp()), Some(collective_stage()));
    assert_eq!(wrapped.stage_dependencies(0), Some(dependencies()));
    let mut updated = dependencies();
    updated.local_completed = 1;
    updated.inbound_bytes_received = 3;
    assert!(!dependencies().prerequisites_complete());
    assert!(updated.prerequisites_complete());
    wrapped.set_stage_dependencies(0, updated);
    assert_eq!(wrapped.stage_dependencies(0), Some(updated));
    assert_eq!(
        wrapped.stage(0).unwrap().role,
        StageRole::Collective(identity())
    );
    assert!(!wrapped.stage(0).unwrap().activated);
    // Positions past the table answer `None`, as every position of an empty table does.
    assert_eq!(wrapped.stage(1), None);

    let plain = host(generator(constant()), None);
    assert_eq!(plain.stage_dependencies(0), None);
    assert!(plain.stages.is_empty());
}

#[test]
fn only_an_empty_or_parallel_stage_table_is_canonical() {
    assert!(host(generator(constant()), None).stages_are_canonical());
    assert!(host(generator(tcp()), Some(collective_stage())).stages_are_canonical());

    // A table without a stage must be empty.
    let mut empty_entries = host(generator(constant()), None);
    empty_entries.stages = vec![None];
    assert!(!empty_entries.stages_are_canonical());
    // A non-empty table has exactly one entry per generator.
    let mut short = host(generator(tcp()), Some(collective_stage()));
    short.generators.push(generator(constant()));
    assert!(!short.stages_are_canonical());
    let mut long = host(generator(tcp()), Some(collective_stage()));
    long.stages.push(None);
    assert!(!long.stages_are_canonical());
}

//! P14 T1: a collective stage is a dependency record attached to an ordinary generator.
//!
//! The paced `Collective` generator keeps its embedded dependency fields; wrapped transports carry
//! the same dependency state in the `stage` sidecar. Both representations answer the same
//! accessor, and the sidecar is invisible in the complete-state rendering when absent.

use days_executor::{
    CollectiveAlgorithm, CollectiveChannelPolicy, CollectiveChunkPolicy, CollectiveGenerator,
    CollectivePhase, CollectiveStage, CollectiveStageIdentity, ConstantGenerator,
    FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus,
    GeneratorTermination, PayloadId, ScheduledEmission, StageDependencies, StageRole,
    TcpCongestionControl, TcpGenerator,
};

fn generator(kind: FlowGeneratorKind, stage: Option<CollectiveStage>) -> FlowGeneratorState {
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
        stage,
    }
}

fn identity() -> CollectiveStageIdentity {
    CollectiveStageIdentity {
        collective_id: 0,
        algorithm: CollectiveAlgorithm::RingAllReduce,
        topology_level: 0,
        topology_group: 0,
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
        local_predecessor: Some(FlowId(4)),
        inbound_predecessor: Some(FlowId(2)),
        inbound_predecessor_bytes: 3,
        local_predecessor_complete: false,
        inbound_predecessor_complete: false,
        inbound_bytes_received: 0,
    }
}

#[test]
fn absent_sidecar_is_invisible_in_the_complete_state_rendering() {
    let constant = generator(
        FlowGeneratorKind::Constant(ConstantGenerator {
            first_departure_ns: 0,
            interval_ns: 1,
            packet_size_bytes: 1,
            termination: GeneratorTermination::Bytes(1),
        }),
        None,
    );
    let rendered = format!("{constant:#?}");
    assert!(!rendered.contains("stage"), "{rendered}");
    assert!(rendered.starts_with("FlowGeneratorState {\n    flow: FlowId(\n"));
    assert!(rendered.ends_with("    ),\n}"), "{rendered}");

    let wrapped = generator(
        FlowGeneratorKind::Tcp(TcpGenerator::new(3, 1, 1, TcpCongestionControl::reno(1))),
        Some(CollectiveStage {
            role: StageRole::Collective(identity()),
            dependencies: dependencies(),
            activated: false,
        }),
    );
    let rendered = format!("{wrapped:#?}");
    assert!(rendered.contains("    stage: Some(\n"), "{rendered}");
}

#[test]
fn both_stage_representations_answer_one_dependency_accessor() {
    let paced = generator(
        FlowGeneratorKind::Collective(CollectiveGenerator {
            collective_id: 0,
            algorithm: CollectiveAlgorithm::RingAllReduce,
            topology_level: 0,
            topology_group: 0,
            group_size: 3,
            declared_total_bytes: 9,
            rank: 1,
            phase: CollectivePhase::ReduceScatter,
            step: 2,
            chunk_policy: CollectiveChunkPolicy::EqualRemainderLast,
            channel_policy: CollectiveChannelPolicy::RingNext,
            chunk_offset_bytes: 0,
            chunk_bytes: 3,
            packet_size_bytes: 1,
            interval_ns: 1,
            local_predecessor: Some(FlowId(4)),
            inbound_predecessor: Some(FlowId(2)),
            inbound_predecessor_bytes: 3,
            local_predecessor_complete: false,
            inbound_predecessor_complete: false,
            inbound_bytes_received: 0,
        }),
        None,
    );
    let wrapped = generator(
        FlowGeneratorKind::Tcp(TcpGenerator::new(3, 1, 1, TcpCongestionControl::reno(1))),
        Some(CollectiveStage {
            role: StageRole::Collective(identity()),
            dependencies: dependencies(),
            activated: false,
        }),
    );
    assert_eq!(paced.stage_dependencies(), Some(dependencies()));
    assert_eq!(wrapped.stage_dependencies(), Some(dependencies()));

    let mut updated = dependencies();
    updated.local_predecessor_complete = true;
    updated.inbound_bytes_received = 3;
    updated.inbound_predecessor_complete = true;
    for mut stage in [paced, wrapped] {
        stage.set_stage_dependencies(updated);
        assert_eq!(stage.stage_dependencies(), Some(updated));
        assert!(updated.prerequisites_complete());
    }
    let FlowGeneratorKind::Collective(after) = ({
        let mut stage = paced;
        stage.set_stage_dependencies(updated);
        stage
    })
    .kind
    else {
        unreachable!()
    };
    assert!(after.local_predecessor_complete && after.inbound_predecessor_complete);
    assert_eq!(after.inbound_bytes_received, 3);

    let plain = generator(
        FlowGeneratorKind::Constant(ConstantGenerator {
            first_departure_ns: 0,
            interval_ns: 1,
            packet_size_bytes: 1,
            termination: GeneratorTermination::Bytes(1),
        }),
        None,
    );
    assert_eq!(plain.stage_dependencies(), None);
}

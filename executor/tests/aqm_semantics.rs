use std::collections::VecDeque;

use days_executor::ecn_ramp::{EcnRampAction, ecn_draw, ecn_queue_key, ecn_ramp_decision};
use days_executor::{
    AqmTraceError, AqmTransitionAction, ArrivalDisposition, Backend, CpuConfig, DiagnosticPlanes,
    DropMarkPolicy, EcnRampPolicy, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor,
    PacketKind, PayloadId, RemoteChannel, RunResult, SchedulerKind, SimulationImage,
    SwitchQueueState, SwitchState, aqm_transitions_csv, event_phase, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, run_cuda_with_observations};
#[cfg(all(feature = "metal", target_vendor = "apple"))]
use days_executor::{MetalConfig, run_metal_with_observations};

const SOURCE: NodeId = NodeId(0);
const SWITCH: NodeId = NodeId(1);
const SINK: NodeId = NodeId(2);
const SOURCE_LINK: LinkId = LinkId(0);
const SWITCH_LINK: LinkId = LinkId(1);
const SINK_EGRESS: LinkId = LinkId(2);

fn payload(sequence: u64) -> PayloadId {
    PayloadId(sequence * 3)
}

fn packet(sequence: u64, size_bytes: u64) -> PacketDescriptor {
    PacketDescriptor {
        id: payload(sequence),
        flow: FlowId(sequence),
        size_bytes,
        ecn_marked: false,
        kind: PacketKind::Data,
    }
}

fn aqm_image(policy: DropMarkPolicy, packet_sizes: &[u64]) -> SimulationImage {
    let initial_packets = packet_sizes
        .iter()
        .copied()
        .enumerate()
        .map(|(sequence, size)| packet(sequence as u64, size))
        .collect::<Vec<_>>();
    let initial_events = initial_packets
        .iter()
        .enumerate()
        .map(|(origin_seq, packet)| Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: origin_seq as u64,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: packet.id,
        })
        .collect();
    let minimum_packet_size = packet_sizes.iter().copied().min().unwrap_or(1);

    SimulationImage {
        stop_time_ns: 1_000,
        nodes: vec![
            NodeDescriptor {
                id: SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: SWITCH,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: SINK,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: SOURCE_LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                stages: Vec::new(),
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                roce_receivers: None,
                pfc: None,
                next_origin_seq: packet_sizes.len() as u64,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: SINK_EGRESS,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                stages: Vec::new(),
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                roce_receivers: None,
                pfc: None,
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![SwitchState {
            physical_switch: 0,
            queues: vec![SwitchQueueState {
                egress_link: Some(SWITCH_LINK),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 64,
                drop_mark: policy,
                pfc: None,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        }],
        flows: packet_sizes
            .iter()
            .enumerate()
            .map(|(sequence, _)| FlowDescriptor {
                id: FlowId(sequence as u64),
                source: SOURCE,
                target: SINK,
                priority: 0,
                feedback_priority: 0,
                route: vec![SOURCE_LINK, SWITCH_LINK],
                reverse_route: vec![],
            })
            .collect(),
        initial_packets,
        links: vec![
            LinkDescriptor {
                id: SOURCE_LINK,
                source: SOURCE,
                target: SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
            LinkDescriptor {
                id: SWITCH_LINK,
                source: SWITCH,
                target: SINK,
                rate_bps: 80_000_000,
                propagation_ns: 0,
            },
            LinkDescriptor {
                id: SINK_EGRESS,
                source: SINK,
                target: SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(
                LinkDescriptor {
                    id: SOURCE_LINK,
                    source: SOURCE,
                    target: SWITCH,
                    rate_bps: 8_000_000_000,
                    propagation_ns: 0,
                },
                minimum_packet_size,
            )
            .expect("source delay must fit"),
            RemoteChannel::for_packet_link(
                LinkDescriptor {
                    id: SWITCH_LINK,
                    source: SWITCH,
                    target: SINK,
                    rate_bps: 80_000_000,
                    propagation_ns: 0,
                },
                minimum_packet_size,
            )
            .expect("switch delay must fit"),
        ],
        initial_events,
        seed: 25,
        stage_joins: Vec::new(),
        seeded_all_to_alls: Vec::new(),
        stage_streams: Vec::new(),
    }
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

fn hidden_byte_overflow_image(policy: DropMarkPolicy) -> SimulationImage {
    let mut image = aqm_image(policy, &[u64::MAX, 1]);
    image.links[0].rate_bps = u64::MAX;
    image.links[1].rate_bps = u64::MAX;
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 1).unwrap();
    image.channels[1] = RemoteChannel::for_packet_link(image.links[1], 1).unwrap();

    let queue = &mut image.switch_states[0].queues[0];
    queue.queue.push_back(payload(0));
    queue.tx_ready_pending = true;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SWITCH,
            kind: EventKind::RemoteArrival,
            payload: payload(1),
        },
        Event {
            key: EventKey {
                time_ns: 1,
                phase: event_phase(EventKind::TxReady),
                origin_node: SWITCH,
                origin_seq: 0,
            },
            target: SWITCH,
            kind: EventKind::TxReady,
            payload: payload(0),
        },
    ];
    image.switch_states[0].next_origin_seq = 1;
    image
}

fn hidden_byte_overflow_result(image: &SimulationImage) -> RunResult {
    validate(image, Backend::Scalar).expect("the representable pre-arrival state must validate");
    let result = run_scalar_with_observations(image, Some(1), ObservationMode::Full)
        .expect("an unrepresentable post-enqueue byte total must be a capacity drop");
    assert_eq!(result.summary.dropped_packets, 1);
    assert_eq!(
        result.arrivals.last().map(|arrival| arrival.disposition),
        Some(ArrivalDisposition::Dropped)
    );
    assert!(
        result
            .resident_packets
            .iter()
            .all(|packet| packet.id != payload(1))
    );
    validate(&checkpoint_image(image, &result), Backend::Scalar)
        .expect("the forced drop must leave a representable checkpoint");

    for workers in [1, 2, 4] {
        validate(image, Backend::Cpu { workers }).unwrap();
        let cpu = run_cpu_with_observations(
            image,
            Some(1),
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, result, "worker count {workers}");
    }
    result
}

fn marked(result: &RunResult, id: PayloadId) -> bool {
    result
        .resident_packets
        .iter()
        .find(|packet| packet.id == id)
        .is_some_and(|packet| packet.ecn_marked)
}

fn diagnostics(result: &RunResult) -> &DiagnosticPlanes {
    result
        .diagnostics
        .as_ref()
        .expect("full reference observation retains diagnostics")
}

#[test]
fn taildrop_forces_drop_when_post_enqueue_byte_sum_is_unrepresentable() {
    let image = hidden_byte_overflow_image(DropMarkPolicy::TailDrop);
    let result = hidden_byte_overflow_result(&image);

    assert!(diagnostics(&result).aqm_transitions.is_empty());
}

#[test]
fn queue_byte_total_is_rederived_exactly_on_checkpoint_resume() {
    let image = hidden_byte_overflow_image(DropMarkPolicy::TailDrop);
    let prefix = hidden_byte_overflow_result(&image);
    let checkpoint = checkpoint_image(&image, &prefix);
    let scalar = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
        .expect("checkpoint restore must rederive the executor-local queue byte total");

    assert!(scalar.switch_states[0].queues[0].queue.is_empty());
    for workers in [1, 2, 4] {
        let cpu = run_cpu_with_observations(
            &checkpoint,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, scalar, "worker count {workers}");
    }
}

#[test]
fn aqm_certificate_records_enqueue_mark_and_drop_with_exact_state() {
    let image = aqm_image(ramp(2, 2, 2, 1, 1), &[1, 1, 1, 1]);
    let result = run_scalar_with_observations(&image, Some(5), ObservationMode::Full).unwrap();
    let csv = aqm_transitions_csv(
        &diagnostics(&result).aqm_transitions,
        &result.observed_packets,
        image.seed,
    )
    .unwrap();
    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/aqm_executor_accept.csv"),
        "the committed LeanGuard certificate must be generated byte-for-byte by the scalar oracle"
    );
    let rows = csv.lines().collect::<Vec<_>>();

    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|row| row.split(',').count() == 19));
    assert!(rows[1].ends_with(",enqueue"));
    assert!(rows[3].contains(",0,1,25,2,2,2,1,1,"), "{csv}");
    assert!(rows[3].ends_with(",mark"));
    assert!(rows[4].ends_with(",drop"));

    let duplicate = diagnostics(&result).aqm_transitions[0];
    assert_eq!(
        aqm_transitions_csv(
            &[duplicate, duplicate],
            &result.observed_packets,
            image.seed
        )
        .expect_err("duplicate canonical transition keys must be rejected"),
        AqmTraceError::DuplicateKey(diagnostics(&result).aqm_transitions[0].key)
    );
}

#[test]
fn ramp_certificate_is_generated_byte_for_byte_by_the_scalar_oracle() {
    let image = ramp_image(ramp(100_000, 300, 3_000, 1, 2), 40, 100);
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let csv = aqm_transitions_csv(
        &diagnostics(&result).aqm_transitions,
        &result.observed_packets,
        image.seed,
    )
    .unwrap();
    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/aqm_ramp_executor_accept.csv")
    );
}
#[test]
fn device_backends_accept_the_ecn_step() {
    let step = aqm_image(ramp(4, 2, 2, 1, 1), &[1]);
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&step, backend)
            .unwrap_or_else(|error| panic!("{backend} must accept the ECN step: {error}"));
    }

    let mut marked_packet = aqm_image(DropMarkPolicy::TailDrop, &[1]);
    marked_packet.initial_packets[0].ecn_marked = true;
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&marked_packet, backend)
            .unwrap_or_else(|error| panic!("{backend} must retain ECN packet marks: {error}"));
    }
}
#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn adversarial_device_ecn_images() -> Vec<SimulationImage> {
    let packet_threshold = aqm_image(ramp(2, 2, 2, 1, 1), &[1, 1, 1, 1]);
    let mut byte_threshold = aqm_image(ramp(8, 5, 5, 1, 1), &[1, 2, 3]);
    byte_threshold.switch_states[0].queues[0].queue_capacity_packets = 1;
    let byte_threshold_overflow =
        hidden_byte_overflow_image(ramp(u64::MAX, u64::MAX, u64::MAX, 1, 1));
    let packet_threshold_overflow = hidden_byte_overflow_image(ramp(u64::MAX, 2, 2, 1, 1));
    let taildrop_overflow = hidden_byte_overflow_image(DropMarkPolicy::TailDrop);
    let mut retained_mark = aqm_image(DropMarkPolicy::TailDrop, &[1, 1]);
    retained_mark.initial_packets[0].ecn_marked = true;
    let prefix = run_scalar_with_observations(&byte_threshold, Some(7), ObservationMode::Full)
        .expect("ECN checkpoint prefix must execute");
    let checkpoint = checkpoint_image(&byte_threshold, &prefix);
    vec![
        packet_threshold,
        byte_threshold,
        byte_threshold_overflow,
        packet_threshold_overflow,
        taildrop_overflow,
        retained_mark,
        checkpoint,
    ]
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
#[test]
fn metal_ecn_step_and_persistent_marks_match_scalar() {
    for (image_index, image) in adversarial_device_ecn_images().into_iter().enumerate() {
        for horizon in [Some(7), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
            assert!(expected.diagnostics.is_some());
            for streams_enabled in [true, false] {
                for round_threads_per_threadgroup in [32, 256] {
                    let actual = run_metal_with_observations(
                        &image,
                        horizon,
                        MetalConfig {
                            streams_enabled,
                            round_threads_per_threadgroup,
                            ..MetalConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "image={image_index} horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_threadgroup}: {error}"
                        )
                    });
                    assert!(actual.result.diagnostics.is_none());
                    let mut expected_without_diagnostics = expected.clone();
                    expected_without_diagnostics.diagnostics = None;
                    assert_eq!(actual.result, expected_without_diagnostics);
                }
            }
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_ecn_step_and_persistent_marks_match_scalar() {
    for image in adversarial_device_ecn_images() {
        for horizon in [Some(7), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
            assert!(expected.diagnostics.is_some());
            for streams_enabled in [true, false] {
                for round_threads_per_block in [32, 256] {
                    let actual = run_cuda_with_observations(
                        &image,
                        horizon,
                        CudaConfig {
                            streams_enabled,
                            round_threads_per_block,
                            ..CudaConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_block}: {error}"
                        )
                    });
                    assert!(actual.result.diagnostics.is_none());
                    let mut expected_without_diagnostics = expected.clone();
                    expected_without_diagnostics.diagnostics = None;
                    assert_eq!(actual.result, expected_without_diagnostics);
                }
            }
        }
    }
}

fn ramp(
    capacity_bytes: u64,
    kmin_bytes: u64,
    kmax_bytes: u64,
    num: u64,
    den: u64,
) -> DropMarkPolicy {
    DropMarkPolicy::EcnRamp(EcnRampPolicy {
        capacity_bytes,
        kmin_bytes,
        kmax_bytes,
        pmax_numerator: num,
        pmax_denominator: den,
    })
}

/// A queue that builds: `count` packets of `size` bytes leave the source back to back at 8 Gb/s
/// and drain at 80 Mb/s, so the post-admission depth climbs by `size` per arrival.
fn ramp_image(policy: DropMarkPolicy, count: usize, size: u64) -> SimulationImage {
    let mut image = aqm_image(policy, &vec![size; count]);
    image.stop_time_ns = 1_000_000_000;
    image
}

#[test]
fn ramp_step_marks_at_its_byte_boundary_and_capacity_drop_takes_precedence() {
    let image = aqm_image(ramp(2, 2, 2, 1, 1), &[1, 1, 1, 1]);
    validate(&image, Backend::Scalar).expect("byte-step image must validate");
    let result = run_scalar_with_observations(&image, Some(5), ObservationMode::Full)
        .expect("byte-step prefix must execute");
    let switch_arrivals = result
        .arrivals
        .iter()
        .filter(|arrival| arrival.time_ns <= 4)
        .map(|arrival| (arrival.payload, arrival.disposition))
        .collect::<Vec<_>>();
    assert_eq!(
        switch_arrivals,
        vec![
            (payload(0), ArrivalDisposition::Admitted),
            (payload(1), ArrivalDisposition::Admitted),
            (payload(2), ArrivalDisposition::Admitted),
            (payload(3), ArrivalDisposition::Dropped),
        ]
    );
    assert!(
        !marked(&result, payload(1)),
        "post-depth one byte is below kmin"
    );
    assert!(
        marked(&result, payload(2)),
        "post-depth two bytes is the inclusive kmax"
    );
    let actions = diagnostics(&result)
        .aqm_transitions
        .iter()
        .map(|transition| transition.action)
        .collect::<Vec<_>>();
    assert_eq!(
        actions,
        vec![
            AqmTransitionAction::Enqueue,
            AqmTransitionAction::Enqueue,
            AqmTransitionAction::Mark,
            AqmTransitionAction::Drop,
        ]
    );
}

#[test]
fn ramp_step_uses_exact_post_enqueue_bytes() {
    let mut image = aqm_image(ramp(8, 5, 5, 1, 1), &[1, 2, 3]);
    image.switch_states[0].queues[0].queue_capacity_packets = 1;
    validate(&image, Backend::Scalar).expect("byte-step image must validate");
    let result = run_scalar_with_observations(&image, Some(7), ObservationMode::Full)
        .expect("byte-step prefix must execute");
    assert!(
        !marked(&result, payload(1)),
        "two queued bytes remain below five"
    );
    assert!(
        marked(&result, payload(2)),
        "two plus three bytes marks at five"
    );
    assert_eq!(result.summary.dropped_packets, 0);
    validate(&checkpoint_image(&image, &result), Backend::Scalar)
        .expect("the ramp's byte capacity, not the TailDrop packet field, closes checkpoints");
}

#[test]
fn ramp_marks_follow_the_stateless_draw_of_each_arrival() {
    let policy = EcnRampPolicy {
        capacity_bytes: 100_000,
        kmin_bytes: 300,
        kmax_bytes: 3_000,
        pmax_numerator: 1,
        pmax_denominator: 2,
    };
    let image = ramp_image(DropMarkPolicy::EcnRamp(policy), 40, 100);
    validate(&image, Backend::Scalar).expect("ramp image must validate");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("ramp run must execute");
    let transitions = &diagnostics(&result).aqm_transitions;
    assert_eq!(
        transitions.len(),
        40,
        "every arrival at the ramp queue is recorded"
    );
    let key = ecn_queue_key(image.seed, SWITCH.0, 0);
    let (mut ramp_marks, mut ramp_enqueues) = (0, 0);
    for transition in transitions {
        let expected = match ecn_ramp_decision(
            &policy,
            transition.queued_bytes_before,
            transition.packet_size_bytes,
            true,
            || ecn_draw(key, transition.payload.0),
        ) {
            EcnRampAction::Enqueue => AqmTransitionAction::Enqueue,
            EcnRampAction::Mark => AqmTransitionAction::Mark,
            EcnRampAction::Drop => AqmTransitionAction::Drop,
        };
        assert_eq!(transition.action, expected, "{transition:?}");
        assert_eq!(transition.policy, policy);
        let depth = transition.queued_bytes_before + transition.packet_size_bytes;
        if depth > policy.kmin_bytes && depth < policy.kmax_bytes {
            match transition.action {
                AqmTransitionAction::Mark => ramp_marks += 1,
                _ => ramp_enqueues += 1,
            }
        }
        assert_eq!(
            transition.ecn_after,
            transition.action == AqmTransitionAction::Mark
        );
    }
    assert!(
        ramp_marks > 0 && ramp_enqueues > 0,
        "the ramp both marks and admits unmarked inside (kmin, kmax): {ramp_marks} {ramp_enqueues}"
    );
    assert!(
        transitions
            .iter()
            .any(
                |transition| transition.queued_bytes_before + transition.packet_size_bytes
                    >= policy.kmax_bytes
                    && transition.action == AqmTransitionAction::Mark
            ),
        "the queue reaches kmax"
    );
}

#[test]
fn ramp_marking_changes_neither_departure_times_nor_pending_event_keys() {
    let marked_image = ramp_image(ramp(100_000, 300, 3_000, 1, 2), 40, 100);
    let taildrop_image = ramp_image(DropMarkPolicy::TailDrop, 40, 100);
    let marked = run_scalar_with_observations(&marked_image, None, ObservationMode::Full)
        .expect("marking run must execute");
    let taildrop = run_scalar_with_observations(&taildrop_image, None, ObservationMode::Full)
        .expect("TailDrop control run must execute");
    assert_eq!(marked.departures, taildrop.departures);
    assert_eq!(marked.arrivals, taildrop.arrivals);
    assert_eq!(marked.pending_events, taildrop.pending_events);
    assert!(
        marked
            .resident_packets
            .iter()
            .any(|packet| packet.ecn_marked)
            || {
                marked
                    .observed_packets
                    .iter()
                    .any(|packet| packet.ecn_marked)
            }
    );
}

#[test]
fn ramp_checkpoint_and_cpu_worker_matrix_match_scalar() {
    let image = ramp_image(ramp(3_200, 300, 3_000, 1, 2), 40, 100);
    let uninterrupted = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("uninterrupted ramp run must execute");
    assert!(
        uninterrupted.summary.dropped_packets > 0,
        "the byte capacity drops"
    );
    for horizon in [5_000, 50_000, 200_000] {
        let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
            .expect("ramp prefix must execute");
        let checkpoint = checkpoint_image(&image, &prefix);
        validate(&checkpoint, Backend::Scalar).expect("ramp checkpoint must validate");
        let resumed = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
            .expect("ramp checkpoint must resume");
        assert_eq!(resumed.host_states, uninterrupted.host_states);
        assert_eq!(resumed.switch_states, uninterrupted.switch_states);
        assert_eq!(resumed.resident_packets, uninterrupted.resident_packets);
        assert_eq!(resumed.pending_events, uninterrupted.pending_events);
    }
    for workers in [1, 2, 4] {
        validate(&image, Backend::Cpu { workers }).expect("CPU ramp image must validate");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CPU ramp run must execute");
        assert_eq!(cpu.result, uninterrupted, "worker count {workers}");
    }
}

#[test]
fn ramp_forces_traced_drop_when_post_enqueue_byte_sum_is_unrepresentable() {
    let policy = ramp(u64::MAX, u64::MAX, u64::MAX, 1, 1);
    let image = hidden_byte_overflow_image(policy);
    let result = hidden_byte_overflow_result(&image);
    assert_eq!(diagnostics(&result).aqm_transitions.len(), 1);
    let transition = &diagnostics(&result).aqm_transitions[0];
    assert_eq!(transition.queued_bytes_before, u64::MAX);
    assert_eq!(transition.packet_size_bytes, 1);
    assert_eq!(transition.action, AqmTransitionAction::Drop);

    let csv = aqm_transitions_csv(
        &diagnostics(&result).aqm_transitions,
        &result.observed_packets,
        image.seed,
    )
    .unwrap();
    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/aqm_ramp_byte_overflow_executor_accept.csv"),
        "the hidden-byte certificate must be generated byte-for-byte by the scalar oracle"
    );
}

#[test]
fn validator_rejects_malformed_ramp_policies() {
    for (policy, reason) in [
        (ramp(0, 0, 0, 1, 1), "zero capacity"),
        (ramp(10, 0, 5, 1, 2), "zero kmin"),
        (ramp(10, 6, 5, 1, 2), "kmin above kmax"),
        (ramp(10, 5, 11, 1, 2), "kmax above capacity"),
        (ramp(10, 5, 8, 0, 2), "zero pmax"),
        (ramp(10, 5, 8, 3, 2), "pmax above one"),
        (ramp(10, 5, 8, 1, 0), "zero denominator"),
        (ramp(10, 5, 8, 2, 4), "pmax not in lowest terms"),
        (ramp(10, 5, 5, 1, 2), "a step with pmax below one"),
        (ramp(u64::MAX, 1, u64::MAX, 1, 3), "span beyond u64"),
    ] {
        let image = aqm_image(policy, &[1]);
        let error = validate(&image, Backend::Scalar)
            .expect_err(reason)
            .to_string();
        assert!(error.contains("ECN ramp"), "{reason}: {error}");
    }
    let image = aqm_image(ramp(u64::MAX, 1, u64::MAX / 3, 1, 3), &[1]);
    validate(&image, Backend::Scalar).expect("the widest representable span validates");
}

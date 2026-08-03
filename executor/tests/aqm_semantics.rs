use std::collections::VecDeque;

use days_executor::{
    AqmTransitionAction, ArrivalDisposition, Backend, CpuConfig, DropMarkPolicy,
    EcnThresholdPolicy, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor,
    PacketKind, PayloadId, QueueDepthUnit, RedPolicyState, RemoteChannel, RunResult, SchedulerKind,
    SimulationImage, SwitchQueueState, SwitchState, aqm_transitions_csv, event_phase,
    run_cpu_with_observations, run_scalar_with_observations, validate,
};
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, run_cuda_with_observations};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
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
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
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
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
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

#[test]
fn taildrop_forces_drop_when_post_enqueue_byte_sum_is_unrepresentable() {
    let image = hidden_byte_overflow_image(DropMarkPolicy::TailDrop);
    let result = hidden_byte_overflow_result(&image);

    assert!(result.aqm_transitions.is_empty());
}

#[test]
fn packet_ecn_forces_traced_drop_when_post_enqueue_byte_sum_is_unrepresentable() {
    let policy = DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
        unit: QueueDepthUnit::Packets,
        capacity: 2,
        threshold: 2,
    });
    let image = hidden_byte_overflow_image(policy);
    let result = hidden_byte_overflow_result(&image);

    assert_eq!(result.aqm_transitions.len(), 1);
    let transition = &result.aqm_transitions[0];
    assert_eq!(transition.queued_packets_before, 1);
    assert_eq!(transition.queued_bytes_before, u64::MAX);
    assert_eq!(transition.packet_size_bytes, 1);
    assert_eq!(transition.before, policy);
    assert_eq!(transition.after, policy);
    assert_eq!(transition.action, AqmTransitionAction::Drop);

    let csv = aqm_transitions_csv(&result.aqm_transitions).unwrap();
    assert_eq!(
        csv,
        include_str!(
            "../../lean/fixtures/p10c/aqm_threshold_packet_byte_overflow_executor_accept.csv"
        ),
        "the hidden-byte threshold certificate must be generated byte-for-byte by the scalar oracle"
    );
}

#[test]
fn packet_red_updates_ewma_then_traces_hidden_byte_domain_drop() {
    const SCALE: u128 = 1_u128 << 32;
    let policy = DropMarkPolicy::Red(RedPolicyState {
        unit: QueueDepthUnit::Packets,
        capacity: 2,
        min_threshold: 1,
        max_threshold: 2,
        max_probability_numerator: 1,
        max_probability_denominator: 1,
        average_scaled: 0,
        counter: 7,
        mark_ecn: true,
    });
    let image = hidden_byte_overflow_image(policy);
    let result = hidden_byte_overflow_result(&image);

    assert_eq!(result.aqm_transitions.len(), 1);
    let transition = &result.aqm_transitions[0];
    assert_eq!(transition.queued_packets_before, 1);
    assert_eq!(transition.queued_bytes_before, u64::MAX);
    assert_eq!(transition.packet_size_bytes, 1);
    assert_eq!(transition.before, policy);
    assert_eq!(
        transition.after,
        DropMarkPolicy::Red(RedPolicyState {
            unit: QueueDepthUnit::Packets,
            capacity: 2,
            min_threshold: 1,
            max_threshold: 2,
            max_probability_numerator: 1,
            max_probability_denominator: 1,
            average_scaled: SCALE / 512,
            counter: 7,
            mark_ecn: true,
        })
    );
    assert_eq!(transition.action, AqmTransitionAction::Drop);

    let csv = aqm_transitions_csv(&result.aqm_transitions).unwrap();
    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/aqm_red_packet_byte_overflow_executor_accept.csv"),
        "the hidden-byte RED certificate must be generated byte-for-byte by the scalar oracle"
    );
}

#[test]
fn aqm_certificate_records_enqueue_mark_and_drop_with_exact_state() {
    let image = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Packets,
            capacity: 2,
            threshold: 2,
        }),
        &[1, 1, 1, 1],
    );
    let result = run_scalar_with_observations(&image, Some(5), ObservationMode::Full).unwrap();
    let csv = aqm_transitions_csv(&result.aqm_transitions).unwrap();
    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/aqm_executor_accept.csv"),
        "the committed LeanGuard certificate must be generated byte-for-byte by the scalar oracle"
    );
    let rows = csv.lines().collect::<Vec<_>>();

    assert_eq!(rows.len(), 5);
    assert!(rows.iter().all(|row| row.split(',').count() == 26));
    assert!(rows[1].ends_with(",enqueue"));
    assert!(rows[3].contains(",0,1,threshold,packets,2,2,"), "{csv}");
    assert!(rows[3].ends_with(",mark"));
    assert!(rows[4].ends_with(",drop"));

    let duplicate = result.aqm_transitions[0];
    assert_eq!(
        aqm_transitions_csv(&[duplicate, duplicate])
            .expect_err("duplicate canonical transition keys must be rejected")
            .duplicate_key,
        result.aqm_transitions[0].key
    );
}

#[test]
fn red_certificate_is_generated_byte_for_byte_by_the_scalar_oracle() {
    let image = aqm_image(
        DropMarkPolicy::Red(RedPolicyState {
            unit: QueueDepthUnit::Packets,
            capacity: 10,
            min_threshold: 1,
            max_threshold: 2,
            max_probability_numerator: 1,
            max_probability_denominator: 1,
            average_scaled: 2_u128 << 32,
            counter: 9,
            mark_ecn: true,
        }),
        &[1],
    );
    let result = run_scalar_with_observations(&image, Some(2), ObservationMode::Full).unwrap();
    let csv = aqm_transitions_csv(&result.aqm_transitions).unwrap();

    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/aqm_red_executor_accept.csv")
    );
}

#[test]
fn validator_rejects_red_counter_spacing_beyond_u64_state() {
    let image = aqm_image(
        DropMarkPolicy::Red(RedPolicyState {
            unit: QueueDepthUnit::Packets,
            capacity: u64::MAX,
            min_threshold: 0,
            max_threshold: u64::MAX,
            max_probability_numerator: 1,
            max_probability_denominator: u64::MAX,
            average_scaled: 0,
            counter: 0,
            mark_ecn: false,
        }),
        &[1],
    );

    let error = validate(&image, Backend::Scalar)
        .expect_err("the deterministic RED spacing counter must remain representable")
        .to_string();
    assert!(
        error.contains("RED worst-case signal spacing exceeds u64"),
        "expected a RED counter-bound diagnostic, got: {error}"
    );
}

#[test]
fn ecn_packet_threshold_marks_at_boundary_and_capacity_drop_takes_precedence() {
    let image = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Packets,
            capacity: 2,
            threshold: 2,
        }),
        &[1, 1, 1, 1],
    );
    validate(&image, Backend::Scalar).expect("packet-threshold image must validate");

    let result = run_scalar_with_observations(&image, Some(5), ObservationMode::Full)
        .expect("packet-threshold prefix must execute");
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
        "post-depth one is below threshold"
    );
    assert!(
        marked(&result, payload(2)),
        "post-depth two is the inclusive boundary"
    );
    assert!(
        result
            .resident_packets
            .iter()
            .all(|packet| packet.id != payload(3)),
        "capacity overflow must drop rather than mark and retain the packet"
    );
}

#[test]
fn ecn_byte_threshold_uses_exact_post_enqueue_bytes() {
    let mut image = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Bytes,
            capacity: 8,
            threshold: 5,
        }),
        &[1, 2, 3],
    );
    image.switch_states[0].queues[0].queue_capacity_packets = 1;
    validate(&image, Backend::Scalar).expect("byte-threshold image must validate");

    let result = run_scalar_with_observations(&image, Some(7), ObservationMode::Full)
        .expect("byte-threshold prefix must execute");

    assert!(
        !marked(&result, payload(1)),
        "two queued bytes remain below five"
    );
    assert!(
        marked(&result, payload(2)),
        "two plus three bytes marks at five"
    );
    assert_eq!(result.summary.dropped_packets, 0);
    validate(&checkpoint_image(&image, &result), Backend::Scalar).expect(
        "AQM policy capacity, not the dormant TailDrop packet field, must close checkpoints",
    );
}

#[test]
fn deterministic_red_counter_has_an_exact_replayable_sequence() {
    const SCALE: u128 = 1_u128 << 32;
    let policy = DropMarkPolicy::Red(RedPolicyState {
        unit: QueueDepthUnit::Packets,
        capacity: 16,
        min_threshold: 0,
        max_threshold: 4,
        max_probability_numerator: 1,
        max_probability_denominator: 1,
        average_scaled: 2 * SCALE,
        counter: 1,
        mark_ecn: false,
    });
    let image = aqm_image(policy, &[1, 1, 1, 1, 1, 1]);
    validate(&image, Backend::Scalar).expect("RED image must validate");

    let first = run_scalar_with_observations(&image, Some(7), ObservationMode::Full)
        .expect("first RED replay must execute");
    let second = run_scalar_with_observations(&image, Some(7), ObservationMode::Full)
        .expect("second RED replay must execute");

    assert_eq!(first, second, "RED contains no random state or random draw");
    assert_eq!(
        first
            .arrivals
            .iter()
            .map(|arrival| arrival.disposition)
            .collect::<Vec<_>>(),
        vec![
            ArrivalDisposition::Admitted,
            ArrivalDisposition::Dropped,
            ArrivalDisposition::Admitted,
            ArrivalDisposition::Admitted,
            ArrivalDisposition::Dropped,
            ArrivalDisposition::Admitted,
        ],
        "the fixed-point EWMA and counter must retain their exact discrete sequence"
    );
}

#[test]
fn marking_changes_neither_departure_times_nor_pending_event_keys() {
    let marked_image = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Packets,
            capacity: 16,
            threshold: 2,
        }),
        &[1, 1, 1],
    );
    let taildrop_image = aqm_image(DropMarkPolicy::TailDrop, &[1, 1, 1]);

    let marked = run_scalar_with_observations(&marked_image, Some(7), ObservationMode::Full)
        .expect("marking run must execute");
    let taildrop = run_scalar_with_observations(&taildrop_image, Some(7), ObservationMode::Full)
        .expect("TailDrop control run must execute");

    assert_eq!(marked.departures, taildrop.departures);
    assert_eq!(marked.arrivals, taildrop.arrivals);
    assert_eq!(marked.pending_events, taildrop.pending_events);
}

#[test]
fn aqm_checkpoint_and_cpu_worker_matrix_match_scalar() {
    let image = aqm_image(
        DropMarkPolicy::Red(RedPolicyState {
            unit: QueueDepthUnit::Bytes,
            capacity: 32,
            min_threshold: 1,
            max_threshold: 8,
            max_probability_numerator: 1,
            max_probability_denominator: 2,
            average_scaled: 3 * (1_u128 << 32),
            counter: 2,
            mark_ecn: true,
        }),
        &[1, 2, 3, 4, 5],
    );
    let uninterrupted = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("uninterrupted AQM run must execute");
    let prefix = run_scalar_with_observations(&image, Some(8), ObservationMode::Full)
        .expect("AQM prefix must execute");
    let checkpoint = checkpoint_image(&image, &prefix);
    validate(&checkpoint, Backend::Scalar).expect("AQM checkpoint must validate");
    let resumed = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
        .expect("AQM checkpoint must resume");

    assert_eq!(resumed.host_states, uninterrupted.host_states);
    assert_eq!(resumed.switch_states, uninterrupted.switch_states);
    assert_eq!(resumed.resident_packets, uninterrupted.resident_packets);
    assert_eq!(resumed.pending_events, uninterrupted.pending_events);

    for workers in [1, 2, 4] {
        validate(&image, Backend::Cpu { workers }).expect("CPU AQM image must validate");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CPU AQM run must execute");
        assert_eq!(cpu.result, uninterrupted, "worker count {workers}");
    }
}

#[test]
fn device_backends_accept_ecn_and_reject_red_exactly() {
    let ecn = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Packets,
            capacity: 4,
            threshold: 2,
        }),
        &[1],
    );
    let red = aqm_image(
        DropMarkPolicy::Red(RedPolicyState {
            unit: QueueDepthUnit::Packets,
            capacity: 4,
            min_threshold: 1,
            max_threshold: 3,
            max_probability_numerator: 1,
            max_probability_denominator: 2,
            average_scaled: 0,
            counter: 0,
            mark_ecn: false,
        }),
        &[1],
    );
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&ecn, backend)
            .unwrap_or_else(|error| panic!("{backend} must accept deterministic ECN: {error}"));
        assert_eq!(
            validate(&red, backend)
                .expect_err("device backend must retain the RED capability rejection")
                .to_string(),
            format!("backend {backend} does not support RED admission; use Scalar or Cpu")
        );
    }

    let mut marked_packet = aqm_image(DropMarkPolicy::TailDrop, &[1]);
    marked_packet.initial_packets[0].ecn_marked = true;
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&marked_packet, backend)
            .unwrap_or_else(|error| panic!("{backend} must retain ECN packet marks: {error}"));
    }
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn adversarial_device_ecn_images() -> Vec<SimulationImage> {
    let packet_threshold = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Packets,
            capacity: 2,
            threshold: 2,
        }),
        &[1, 1, 1, 1],
    );
    let mut byte_threshold = aqm_image(
        DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
            unit: QueueDepthUnit::Bytes,
            capacity: 8,
            threshold: 5,
        }),
        &[1, 2, 3],
    );
    byte_threshold.switch_states[0].queues[0].queue_capacity_packets = 1;
    let overflow = hidden_byte_overflow_image(DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
        unit: QueueDepthUnit::Bytes,
        capacity: u64::MAX,
        threshold: u64::MAX,
    }));
    let mut retained_mark = aqm_image(DropMarkPolicy::TailDrop, &[1, 1]);
    retained_mark.initial_packets[0].ecn_marked = true;
    let prefix = run_scalar_with_observations(&byte_threshold, Some(7), ObservationMode::Full)
        .expect("ECN checkpoint prefix must execute");
    let checkpoint = checkpoint_image(&byte_threshold, &prefix);
    vec![
        packet_threshold,
        byte_threshold,
        overflow,
        retained_mark,
        checkpoint,
    ]
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn without_mechanism_transitions(mut result: RunResult) -> RunResult {
    result.aqm_transitions.clear();
    result.mechanism_transitions.clear();
    result
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_ecn_threshold_and_persistent_marks_match_scalar() {
    for (image_index, image) in adversarial_device_ecn_images().into_iter().enumerate() {
        for horizon in [Some(7), None] {
            let expected = without_mechanism_transitions(
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap(),
            );
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
                    assert_eq!(actual.result, expected);
                }
            }
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_ecn_threshold_and_persistent_marks_match_scalar() {
    for image in adversarial_device_ecn_images() {
        for horizon in [Some(7), None] {
            let expected = without_mechanism_transitions(
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap(),
            );
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
                    assert_eq!(actual.result, expected);
                }
            }
        }
    }
}

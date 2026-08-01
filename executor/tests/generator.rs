use std::collections::VecDeque;

use days_executor::{
    ArrivalDisposition, Backend, ConstantGenerator, Event, EventKey, EventKind, FlowDescriptor,
    FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus,
    GeneratorTermination, HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind,
    ObservationMode, PacketDescriptor, PayloadId, RemoteChannel, ScheduledEmission,
    SimulationImage, event_phase, run_scalar, run_scalar_with_observations, validate,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const FORWARD: LinkId = LinkId(0);
const REVERSE: LinkId = LinkId(1);
const FLOW: FlowId = FlowId(0);
const FIRST_PACKET: PayloadId = PayloadId(0);

fn image(status: GeneratorStatus, bytes: u64, next_payload_seq: u64) -> SimulationImage {
    let scheduled = status == GeneratorStatus::Scheduled;
    let first_packet = PacketDescriptor {
        id: FIRST_PACKET,
        flow: FLOW,
        size_bytes: 1,
        ecn_marked: false,
        kind: days_executor::PacketKind::Data,
    };
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    SimulationImage {
        stop_time_ns: 20,
        nodes: vec![
            NodeDescriptor {
                id: SOURCE,
                kind: NodeKind::Host,
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
                egress_link: FORWARD,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![FlowGeneratorState {
                    flow: FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status,
                        departure_time_ns: 0,
                        payload: FIRST_PACKET,
                    },
                    rng_state: 7,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: 0,
                        interval_ns: 2,
                        packet_size_bytes: 1,
                        termination: GeneratorTermination::Bytes(bytes),
                    }),
                }],
                tcp_receivers: vec![],
                next_origin_seq: u64::from(scheduled),
                next_payload_seq,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: REVERSE,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        }],
        initial_packets: scheduled.then_some(first_packet).into_iter().collect(),
        links: vec![
            forward,
            LinkDescriptor {
                id: REVERSE,
                source: SINK,
                target: SOURCE,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![RemoteChannel::for_packet_link(forward, 1).expect("delay must fit")],
        initial_events: scheduled
            .then_some(Event {
                key: EventKey {
                    time_ns: 0,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: SOURCE,
                    origin_seq: 0,
                },
                target: SOURCE,
                kind: EventKind::PacketArrival,
                payload: FIRST_PACKET,
            })
            .into_iter()
            .collect(),
        seed: 1,
    }
}

fn feedback_image() -> SimulationImage {
    let mut image = image(GeneratorStatus::Blocked, 2, 0);
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: 1,
        ecn_marked: false,
        kind: days_executor::PacketKind::Feedback,
    });
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: SINK,
            origin_seq: 0,
        },
        target: SOURCE,
        kind: EventKind::RemoteArrival,
        payload: PayloadId(1),
    });
    image
        .channels
        .push(RemoteChannel::for_packet_link(image.links[1], 1).expect("reverse delay must fit"));
    image.host_states[1].next_origin_seq = 1;
    image
}

#[test]
fn constant_generator_allocates_unique_node_local_payloads() {
    assert_ne!(
        PayloadId::from_node_sequence(NodeId(0), 2, 0),
        PayloadId::from_node_sequence(NodeId(1), 2, 0)
    );
    assert_ne!(
        PayloadId::from_node_sequence(NodeId(0), 2, 0),
        PayloadId::from_node_sequence(NodeId(0), 2, 1),
        "retransmission consumes a fresh identity even when transport sequence is unchanged"
    );
    let image = image(GeneratorStatus::Scheduled, 3, 1);
    validate(&image, Backend::Scalar).expect("generator image should validate");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("generator image should run");

    assert_eq!(result.summary.sourced_packets, 3);
    assert_eq!(result.summary.received_packets, 3);
    assert_eq!(
        result
            .arrivals
            .iter()
            .map(|arrival| arrival.payload)
            .collect::<Vec<_>>(),
        vec![PayloadId(0), PayloadId(2), PayloadId(4)]
    );
    assert_eq!(result.host_states[0].generators[0].packets_emitted, 3);
    assert_eq!(result.host_states[0].generators[0].bytes_emitted, 3);
    assert_eq!(
        result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Finished
    );
}

#[test]
fn constant_generator_preserves_bytes_overshoot_and_duration_half_open_end() {
    let mut bytes = image(GeneratorStatus::Scheduled, 5, 1);
    bytes.initial_packets[0].size_bytes = 2;
    let FlowGeneratorKind::Constant(mut constant) = bytes.host_states[0].generators[0].kind else {
        panic!("fixture must remain constant")
    };
    constant.packet_size_bytes = 2;
    bytes.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(constant);
    bytes.channels[0] = RemoteChannel::for_packet_link(bytes.links[0], 2).expect("delay must fit");
    let bytes_result = run_scalar(&bytes, None).expect("byte-terminated flow should run");
    assert_eq!(bytes_result.summary.sourced_packets, 3);
    assert_eq!(bytes_result.summary.sourced_bytes, 6);

    let mut duration = image(GeneratorStatus::Scheduled, 0, 1);
    let FlowGeneratorKind::Constant(mut constant) = duration.host_states[0].generators[0].kind
    else {
        panic!("fixture must remain constant")
    };
    constant.termination = GeneratorTermination::DurationNs(5);
    duration.host_states[0].generators[0].kind = FlowGeneratorKind::Constant(constant);
    let duration_result = run_scalar(&duration, None).expect("duration-terminated flow should run");
    assert_eq!(duration_result.summary.sourced_packets, 3);
    assert_eq!(duration_result.summary.sourced_bytes, 3);
}

#[test]
fn blocked_generator_is_valid_without_a_scheduled_event() {
    let image = image(GeneratorStatus::Blocked, 2, 0);
    validate(&image, Backend::Scalar).expect("blocked flow should be a valid idle LP");
    validate(&image, Backend::Cpu { workers: 2 })
        .expect("parallel validation should accept a feedback-waiting LP");

    let result = run_scalar(&image, None).expect("blocked flow should return without work");
    assert!(result.pending_events.is_empty());
    assert_eq!(
        result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked
    );
    assert_eq!(result.summary.sourced_packets, 0);
}

#[test]
fn ordinary_remote_arrival_routes_feedback_to_the_source_generator() {
    let image = feedback_image();

    validate(&image, Backend::Scalar).expect("feedback must be representable in an accepted image");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("ordinary packet feedback should reach the generator hook");
    assert_eq!(result.host_states[0].generators[0].feedback.arrivals, 1);
    assert_eq!(
        result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked,
        "the constant generator records but otherwise ignores feedback"
    );
    assert_eq!(result.summary.feedback_packets, 1);
    assert_eq!(result.summary.sourced_packets, 0);
    assert_eq!(result.arrivals[0].disposition, ArrivalDisposition::Feedback);
}

#[test]
fn scheduled_generator_payload_reuse_is_rejected() {
    let mut image = image(GeneratorStatus::Scheduled, 3, 2);
    let generator = &mut image.host_states[0].generators[0];
    generator.packets_emitted = 1;
    generator.bytes_emitted = 1;
    generator.next_emission.departure_time_ns = 2;
    image.initial_events[0].key.time_ns = 2;

    let error = validate(&image, Backend::Scalar)
        .expect_err("a consumed payload identity must not be scheduled again");
    assert_eq!(
        error.to_string(),
        "flow FlowId(0) scheduled payload PayloadId(0) sequence 0 was already consumed; generator has emitted 1 packets"
    );
}

#[test]
fn feedback_without_a_source_generator_is_rejected() {
    let mut image = feedback_image();
    image.host_states[0].generators.clear();
    image.channels.remove(0);

    let error = validate(&image, Backend::Scalar)
        .expect_err("feedback without source-owned generator state must reject");
    assert_eq!(
        error.to_string(),
        "feedback packet PayloadId(1) for flow FlowId(0) has no generator at source node NodeId(0)"
    );
}

#[test]
fn pending_feedback_arrivals_are_reserved_against_generator_state() {
    let mut image = feedback_image();
    image.host_states[0].generators[0].feedback.arrivals = u64::MAX;

    let error = validate(&image, Backend::Scalar)
        .expect_err("pending feedback must not overflow generator state");
    assert_eq!(
        error.to_string(),
        "node NodeId(0) flow FlowId(0) generator feedback arrivals 18446744073709551615 overflows with 1 pending feedback arrivals"
    );
}

#[test]
fn preloaded_data_payload_must_have_source_provenance() {
    let mut image = image(GeneratorStatus::Scheduled, 1, 1);
    image.host_states[0].generators.clear();
    image.initial_packets[0].id = PayloadId(1);
    image.initial_events[0].payload = PayloadId(1);

    let error = validate(&image, Backend::Scalar)
        .expect_err("preloaded data must use its source node payload residue");
    assert_eq!(
        error.to_string(),
        "data packet PayloadId(1) for flow FlowId(0) is not allocated by source node NodeId(0)"
    );
}

#[test]
fn payload_identity_exhaustion_has_a_specific_runtime_and_validation_diagnostic() {
    let image = image(GeneratorStatus::Scheduled, 2, u64::MAX);
    let validation = validate(&image, Backend::Scalar).expect_err("capacity must reject");
    assert_eq!(
        validation.to_string(),
        "node NodeId(0) payload identity sequence 18446744073709551615 overflows while reserving 1 generated packets"
    );

    let runtime = run_scalar(&image, None).expect_err("checked allocation must reject");
    assert_eq!(
        runtime.to_string(),
        "payload identity sequence exhausted at node NodeId(0)"
    );
}

#[test]
fn summary_is_default_and_full_records_are_opt_in_without_semantic_changes() {
    let image = image(GeneratorStatus::Scheduled, 3, 1);
    let summary = run_scalar(&image, None).expect("summary run should succeed");
    let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("full oracle run should succeed");

    assert!(summary.departures.is_empty());
    assert!(summary.arrivals.is_empty());
    assert!(summary.observed_packets.is_empty());
    assert_eq!(summary.summary, full.summary);
    assert_eq!(summary.host_states, full.host_states);
    assert_eq!(summary.switch_states, full.switch_states);
    assert_eq!(summary.resident_packets, full.resident_packets);
    assert_eq!(summary.pending_events, full.pending_events);
    assert_eq!(full.departures.len(), 3);
    assert_eq!(full.arrivals.len(), 3);
    assert_eq!(full.observed_packets.len(), 3);
    assert_eq!(
        full.observed_packets
            .iter()
            .find(|packet| packet.id == PayloadId(4))
            .expect("generated terminal packet must remain resolvable")
            .size_bytes,
        1
    );
}

#[test]
fn stop_before_termination_preserves_the_blocked_next_candidate() {
    let mut image = image(GeneratorStatus::Scheduled, 2, 1);
    image.stop_time_ns = 0;

    let result = run_scalar(&image, None).expect("first emission should run at the stop boundary");
    let next = result.host_states[0].generators[0].next_emission;
    assert_eq!(result.summary.sourced_packets, 1);
    assert_eq!(next.status, GeneratorStatus::Stopped);
    assert_eq!(next.departure_time_ns, 2);
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.kind != EventKind::PacketArrival),
        "Stopped must not retain a generator emission event"
    );
}

#[test]
fn payload_identity_stride_boundary_is_checked_for_every_source() {
    let last_sequence = u64::MAX / 2;
    assert_eq!(
        PayloadId::from_node_sequence(NodeId(0), 2, last_sequence),
        Some(PayloadId(u64::MAX - 1))
    );
    assert_eq!(
        PayloadId::from_node_sequence(NodeId(1), 2, last_sequence),
        Some(PayloadId(u64::MAX))
    );
    assert_eq!(
        PayloadId::from_node_sequence(NodeId(0), 2, last_sequence + 1),
        None
    );
    assert_eq!(
        PayloadId::from_node_sequence(NodeId(1), 2, last_sequence + 1),
        None
    );
}

#[test]
fn generator_recurrence_and_payload_cursor_mutations_are_rejected() {
    let mut bad_time = image(GeneratorStatus::Scheduled, 2, 1);
    bad_time.host_states[0].generators[0]
        .next_emission
        .departure_time_ns = 1;
    bad_time.initial_events[0].key.time_ns = 1;
    let error = validate(&bad_time, Backend::Scalar).expect_err("recurrence mutation must reject");
    assert_eq!(
        error.to_string(),
        "flow FlowId(0) scheduled departure time 1 does not match recurrence value 0"
    );

    let mut bad_cursor = image(GeneratorStatus::Scheduled, 2, 0);
    let error = validate(&bad_cursor, Backend::Scalar).expect_err("cursor reset must reject");
    assert_eq!(
        error.to_string(),
        "flow FlowId(0) scheduled payload PayloadId(0) sequence 0 is not below node NodeId(0) next payload sequence 0"
    );
    bad_cursor.host_states[0].generators[0].next_emission.status = GeneratorStatus::Blocked;
}

#[test]
fn converging_emissions_keep_canonical_flow_order_at_the_source() {
    let mut image = image(GeneratorStatus::Scheduled, 2, 2);
    image.flows = vec![
        FlowDescriptor {
            id: FlowId(0),
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        },
        FlowDescriptor {
            id: FlowId(1),
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        },
    ];
    let flow_zero = FlowGeneratorState {
        flow: FlowId(0),
        packets_emitted: 0,
        bytes_emitted: 0,
        next_emission: ScheduledEmission {
            status: GeneratorStatus::Scheduled,
            departure_time_ns: 9,
            payload: PayloadId(0),
        },
        rng_state: 11,
        feedback: GeneratorFeedbackState {
            arrivals: 0,
            outstanding_bytes: 0,
            unacknowledged_bytes: 0,
        },
        kind: FlowGeneratorKind::Constant(ConstantGenerator {
            first_departure_ns: 9,
            interval_ns: 1,
            packet_size_bytes: 1,
            termination: GeneratorTermination::Bytes(2),
        }),
    };
    let flow_one = FlowGeneratorState {
        flow: FlowId(1),
        packets_emitted: 0,
        bytes_emitted: 0,
        next_emission: ScheduledEmission {
            status: GeneratorStatus::Scheduled,
            departure_time_ns: 0,
            payload: PayloadId(2),
        },
        rng_state: 13,
        feedback: GeneratorFeedbackState {
            arrivals: 0,
            outstanding_bytes: 0,
            unacknowledged_bytes: 0,
        },
        kind: FlowGeneratorKind::Constant(ConstantGenerator {
            first_departure_ns: 0,
            interval_ns: 10,
            packet_size_bytes: 1,
            termination: GeneratorTermination::Bytes(2),
        }),
    };
    image.host_states[0].generators = vec![flow_zero, flow_one];
    image.host_states[0].next_origin_seq = 2;
    image.initial_packets = vec![
        PacketDescriptor {
            id: PayloadId(0),
            flow: FlowId(0),
            size_bytes: 1,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        },
        PacketDescriptor {
            id: PayloadId(2),
            flow: FlowId(1),
            size_bytes: 1,
            ecn_marked: false,
            kind: days_executor::PacketKind::Data,
        },
    ];
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: PayloadId(2),
        },
        Event {
            key: EventKey {
                time_ns: 9,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: PayloadId(0),
        },
    ];

    validate(&image, Backend::Scalar).expect("converging generator image should validate");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("converging emissions should run");
    assert_eq!(
        result
            .departures
            .iter()
            .map(|departure| (departure.payload, departure.time_ns))
            .collect::<Vec<_>>(),
        vec![
            (PayloadId(2), 1),
            (PayloadId(0), 10),
            (PayloadId(6), 11),
            (PayloadId(4), 12),
        ]
    );
}

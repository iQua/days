use std::collections::{BTreeMap, VecDeque};

use days_executor::{
    Backend, CUBIC_WINDOW_SCALE, ChunkGranularity, ConstantGenerator, CpuConfig, Event,
    EventFelClass, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState,
    FlowId, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor,
    PacketKind, PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind, SimulationImage,
    StaticPartitionPolicy, SwitchQueueState, SwitchState, TcpAckHeader, TcpCongestionControl,
    TcpDataHeader, TcpGenerator, TcpPhase, TcpReceiverState, TcpTimerState, TcpTransitionInput,
    event_fel_class, event_phase, run_cpu_with_observations, run_scalar_rounds_with_observations,
    run_scalar_with_observations, size_default_device_plan, validate,
};
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, run_cuda_with_observations};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{MetalConfig, run_metal_with_observations};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const FORWARD: LinkId = LinkId(0);
const REVERSE: LinkId = LinkId(1);
const FLOW: FlowId = FlowId(0);
const FIRST: PayloadId = PayloadId(0);
const MSS: u64 = 512;
const ACK_BYTES: u64 = 40;

fn tcp_image(control: TcpCongestionControl, total_bytes: u64) -> SimulationImage {
    let first_size = MSS.min(total_bytes);
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let reverse = LinkDescriptor {
        id: REVERSE,
        source: SINK,
        target: SOURCE,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let first = PacketDescriptor {
        id: FIRST,
        flow: FLOW,
        size_bytes: first_size,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    };
    SimulationImage {
        stop_time_ns: 1_000_000_001,
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
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: 0,
                        payload: FIRST,
                    },
                    rng_state: 7,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Tcp(TcpGenerator::new(
                        total_bytes,
                        MSS,
                        ACK_BYTES,
                        control,
                    )),
                }],
                tcp_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 1,
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
                tcp_receivers: vec![TcpReceiverState::new(FLOW, ACK_BYTES)],
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
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        }],
        initial_packets: vec![first],
        links: vec![forward, reverse],
        channels: vec![
            RemoteChannel::for_packet_link(forward, 1).unwrap(),
            RemoteChannel::for_packet_link(reverse, ACK_BYTES).unwrap(),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: FIRST,
        }],
        seed: 1,
    }
}

fn switched_tcp_image(
    control: TcpCongestionControl,
    scheduler: SchedulerKind,
    queue_capacity_packets: u64,
) -> SimulationImage {
    let source = NodeId(0);
    let forward_switch = NodeId(1);
    let sink = NodeId(2);
    let reverse_switch = NodeId(3);
    let links = [
        LinkDescriptor {
            id: LinkId(0),
            source,
            target: forward_switch,
            rate_bps: 100_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(1),
            source: forward_switch,
            target: sink,
            rate_bps: 10_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(2),
            source: sink,
            target: reverse_switch,
            rate_bps: 100_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: LinkId(3),
            source: reverse_switch,
            target: source,
            rate_bps: 100_000_000_000,
            propagation_ns: 0,
        },
    ];
    let first = PacketDescriptor {
        id: FIRST,
        flow: FLOW,
        size_bytes: MSS,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    };
    SimulationImage {
        stop_time_ns: 5_000_000_000,
        nodes: vec![
            NodeDescriptor {
                id: source,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: forward_switch,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: sink,
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: reverse_switch,
                kind: NodeKind::Switch,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LinkId(0),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![FlowGeneratorState {
                    flow: FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Scheduled,
                        departure_time_ns: 0,
                        payload: FIRST,
                    },
                    rng_state: 11,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Tcp(TcpGenerator::new(
                        160 * MSS,
                        MSS,
                        ACK_BYTES,
                        control,
                    )),
                }],
                tcp_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 1,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(2),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                tcp_receivers: vec![TcpReceiverState::new(FLOW, ACK_BYTES)],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![
            SwitchState {
                physical_switch: 0,
                queues: vec![SwitchQueueState {
                    egress_link: Some(LinkId(1)),
                    scheduler,
                    queue_capacity_packets,
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                }],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            },
            SwitchState {
                physical_switch: 1,
                queues: vec![SwitchQueueState {
                    egress_link: Some(LinkId(3)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 0,
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                }],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            },
        ],
        flows: vec![FlowDescriptor {
            id: FLOW,
            source,
            target: sink,
            route: vec![LinkId(0), LinkId(1)],
            reverse_route: vec![LinkId(2), LinkId(3)],
        }],
        initial_packets: vec![first],
        links: links.to_vec(),
        channels: vec![
            RemoteChannel::for_packet_link(links[0], 1).unwrap(),
            RemoteChannel::for_packet_link(links[1], 1).unwrap(),
            RemoteChannel::for_packet_link(links[2], ACK_BYTES).unwrap(),
            RemoteChannel::for_packet_link(links[3], ACK_BYTES).unwrap(),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: source,
                origin_seq: 0,
            },
            target: source,
            kind: EventKind::PacketArrival,
            payload: FIRST,
        }],
        seed: 11,
    }
}

fn recovery_campaign_image(control: TcpCongestionControl) -> SimulationImage {
    let mut image = switched_tcp_image(control, SchedulerKind::Fifo, 1);
    let acknowledgments = [MSS, MSS, MSS, MSS, MSS, 2 * MSS, 4 * MSS];
    let node_count = image.nodes.len() as u64;
    let target = image.flows[0].target;
    for (sequence, acknowledgment) in acknowledgments.into_iter().enumerate() {
        let payload = PayloadId(target.0 + node_count * sequence as u64);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: FLOW,
            size_bytes: ACK_BYTES,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment,
                acknowledged_bytes: 0,
                echoed_sent_time_ns: 0,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: sequence as u64 + 1,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: NodeId(3),
                origin_seq: sequence as u64,
            },
            target: SOURCE,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    image.host_states[1].next_payload_seq = acknowledgments.len() as u64;
    image.switch_states[1].next_origin_seq = acknowledgments.len() as u64;
    image.initial_events.sort_by_key(|event| event.key);
    image
}

fn tcp_ack_burst_image(
    control: TcpCongestionControl,
    total_bytes: u64,
    acknowledgments: &[u64],
) -> SimulationImage {
    let mut image = tcp_image(control, total_bytes);
    image.stop_time_ns = 0;
    let node_count = image.nodes.len() as u64;
    for (origin_seq, acknowledgment) in acknowledgments.iter().copied().enumerate() {
        let origin_seq = origin_seq as u64;
        let payload = PayloadId(SINK.0 + node_count * origin_seq);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: FLOW,
            size_bytes: ACK_BYTES,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment,
                acknowledged_bytes: acknowledgment,
                echoed_sent_time_ns: 0,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SINK,
                origin_seq,
            },
            target: SOURCE,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    image.host_states[1].next_origin_seq = acknowledgments.len() as u64;
    image.host_states[1].next_payload_seq = acknowledgments.len() as u64;
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.sort_by_key(|event| event.key);
    image
}

fn cubic_wide_magnitude_checkpoint_image() -> SimulationImage {
    const ACK_TIME_NS: u64 = 82_858;
    const ECHOED_TIME_NS: u64 = 68_164;
    const TIMER_DEADLINE_NS: u64 = 1_000_000_000;

    let mut control = TcpCongestionControl::cubic(MSS);
    let TcpCongestionControl::Cubic(ref mut cubic) = control else {
        unreachable!()
    };
    cubic.cwnd_scaled = 5_600_000_000;
    cubic.ssthresh_scaled = 5_600_000_000;
    cubic.phase = TcpPhase::CongestionAvoidance;
    cubic.w_max_scaled = 8_000_000_000;
    cubic.w_last_max_scaled = 8_000_000_000;
    cubic.epoch_start_ns = 68_140;
    cubic.srtt_ns = 15_898;
    cubic.k_ns = 1_817_120_592;
    let mut image = tcp_image(control, MSS);
    image.stop_time_ns = ACK_TIME_NS;

    let generator = &mut image.host_states[0].generators[0];
    generator.packets_emitted = 1;
    generator.bytes_emitted = MSS;
    generator.next_emission.status = GeneratorStatus::Blocked;
    generator.feedback.outstanding_bytes = MSS;
    generator.feedback.unacknowledged_bytes = MSS;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.next_sequence = MSS;
    tcp.bytes_in_flight = MSS;
    tcp.last_attempt = FIRST;
    tcp.timer_generation = 1;
    tcp.active_timer = Some(TcpTimerState {
        attempt: FIRST,
        sequence: 0,
        deadline_ns: TIMER_DEADLINE_NS,
        generation: 1,
        rto_ns: TIMER_DEADLINE_NS,
    });
    tcp.srtt_ns = 15_898;
    tcp.rtt_var_ns = 1;
    tcp.rto_ns = TIMER_DEADLINE_NS;

    image.host_states[0].next_origin_seq = 2;
    image.host_states[0].sourced_packets = 1;
    image.host_states[0].departed_packets = 1;
    image.host_states[1].next_origin_seq = 1;
    image.host_states[1].next_payload_seq = 1;

    let ack = PacketDescriptor {
        id: PayloadId(1),
        flow: FLOW,
        size_bytes: ACK_BYTES,
        kind: PacketKind::TcpAck(TcpAckHeader {
            acknowledgment: MSS,
            acknowledged_bytes: MSS,
            echoed_sent_time_ns: ECHOED_TIME_NS,
        }),
    };
    image.initial_packets.push(ack);
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: ACK_TIME_NS,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SINK,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::RemoteArrival,
            payload: ack.id,
        },
        Event {
            key: EventKey {
                time_ns: TIMER_DEADLINE_NS,
                phase: event_phase(EventKind::RetransmissionTimeout),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SOURCE,
            kind: EventKind::RetransmissionTimeout,
            payload: FIRST,
        },
    ];
    image
}

fn scheduled_tcp_checkpoint_image(in_flight_segments: u64, total_segments: u64) -> SimulationImage {
    assert!(0 < in_flight_segments && in_flight_segments < total_segments);
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), total_segments * MSS);
    image.stop_time_ns = 0;
    image.initial_packets.clear();
    image.initial_events.clear();

    let node_count = image.nodes.len() as u64;
    for sequence in 0..in_flight_segments {
        let payload = PayloadId(SOURCE.0 + node_count * sequence);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: FLOW,
            size_bytes: MSS,
            kind: PacketKind::TcpData(TcpDataHeader {
                sequence: sequence * MSS,
                sent_time_ns: 0,
                retransmission: false,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: sequence + 1,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }

    let scheduled_payload = PayloadId(SOURCE.0 + node_count * in_flight_segments);
    image.initial_packets.push(PacketDescriptor {
        id: scheduled_payload,
        flow: FLOW,
        size_bytes: MSS,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: in_flight_segments * MSS,
            sent_time_ns: 0,
            retransmission: false,
        }),
    });
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::PacketArrival),
            origin_node: SOURCE,
            origin_seq: 0,
        },
        target: SOURCE,
        kind: EventKind::PacketArrival,
        payload: scheduled_payload,
    });

    let generator = &mut image.host_states[0].generators[0];
    generator.packets_emitted = in_flight_segments;
    generator.bytes_emitted = in_flight_segments * MSS;
    generator.next_emission.payload = scheduled_payload;
    generator.feedback.outstanding_bytes = in_flight_segments * MSS;
    generator.feedback.unacknowledged_bytes = in_flight_segments * MSS;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.next_sequence = in_flight_segments * MSS;
    tcp.bytes_in_flight = in_flight_segments * MSS;
    tcp.last_attempt = PayloadId(SOURCE.0 + node_count * (in_flight_segments - 1));
    image.host_states[0].next_origin_seq = in_flight_segments + 1;
    image.host_states[0].next_payload_seq = in_flight_segments + 1;
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.sort_by_key(|event| event.key);
    image
}

fn checkpoint_image(
    source: &SimulationImage,
    result: &days_executor::RunResult,
) -> SimulationImage {
    let mut checkpoint = source.clone();
    checkpoint.host_states = result.host_states.clone();
    checkpoint.switch_states = result.switch_states.clone();
    checkpoint.initial_packets = result.resident_packets.clone();
    checkpoint.initial_events = result.pending_events.clone();
    checkpoint
}

fn stitch_checkpoint_run(
    prefix: &days_executor::RunResult,
    suffix: &days_executor::RunResult,
) -> days_executor::RunResult {
    let mut summary = prefix.summary;
    macro_rules! add_summary {
        ($field:ident) => {
            summary.$field += suffix.summary.$field;
        };
    }
    add_summary!(sourced_packets);
    add_summary!(sourced_bytes);
    add_summary!(departed_packets);
    add_summary!(departed_bytes);
    add_summary!(admitted_packets);
    add_summary!(admitted_bytes);
    add_summary!(received_packets);
    add_summary!(received_bytes);
    add_summary!(dropped_packets);
    add_summary!(dropped_bytes);
    add_summary!(feedback_packets);
    add_summary!(feedback_bytes);

    let mut observed_packets = prefix
        .observed_packets
        .iter()
        .chain(&suffix.observed_packets)
        .map(|packet| (packet.id, *packet))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    observed_packets.sort_by_key(|packet| packet.id);
    let mut departures = prefix.departures.clone();
    departures.extend_from_slice(&suffix.departures);
    let mut arrivals = prefix.arrivals.clone();
    arrivals.extend_from_slice(&suffix.arrivals);
    let mut tcp_transitions = prefix.tcp_transitions.clone();
    tcp_transitions.extend_from_slice(&suffix.tcp_transitions);

    days_executor::RunResult {
        host_states: suffix.host_states.clone(),
        switch_states: suffix.switch_states.clone(),
        summary,
        resident_packets: suffix.resident_packets.clone(),
        observed_packets,
        departures,
        arrivals,
        tcp_transitions,
        pending_events: suffix.pending_events.clone(),
    }
}

#[test]
fn reno_slow_start_doubles_exactly_over_one_window_of_acks() {
    let mut reno = TcpCongestionControl::reno(MSS);
    assert_eq!(reno.cwnd_bytes(MSS), 2 * MSS);

    reno.on_new_ack(MSS, 100, 100, 2 * MSS, 2 * MSS);
    reno.on_new_ack(MSS, 100, 100, MSS, 2 * MSS);

    assert_eq!(reno.phase(), TcpPhase::SlowStart);
    assert_eq!(reno.cwnd_bytes(MSS), 4 * MSS);
}

#[test]
fn reno_single_flow_known_buffer_sawtooth_matches_closed_form() {
    // One saturated flow with a 24-segment BDP and an 8-segment TailDrop buffer has the
    // deterministic loss threshold W = BDP + B = 32 segments. Reno beta=1/2 gives W/2 after
    // recovery, additive increase gives W/2 RTTs per cycle, and the linear time mean is 3W/4.
    const BDP_SEGMENTS: u64 = 24;
    const BUFFER_SEGMENTS: u64 = 8;
    const PEAK_SEGMENTS: u64 = BDP_SEGMENTS + BUFFER_SEGMENTS;
    const LOW_SEGMENTS: u64 = PEAK_SEGMENTS / 2;
    const PERIOD_RTTS: u64 = PEAK_SEGMENTS - LOW_SEGMENTS;
    const MEAN_SEGMENTS: u64 = (PEAK_SEGMENTS + LOW_SEGMENTS) / 2;

    let mut reno = TcpCongestionControl::reno(MSS);
    let TcpCongestionControl::Reno(ref mut state) = reno else {
        unreachable!()
    };
    state.phase = TcpPhase::CongestionAvoidance;
    state.cwnd_bytes = LOW_SEGMENTS * MSS;
    state.ssthresh_bytes = LOW_SEGMENTS * MSS;
    state.ca_credit = 0;

    let mut twice_trapezoid_area = 0_u64;
    let mut acknowledgment = 0_u64;
    for rtt in 0..PERIOD_RTTS {
        let before = reno.cwnd_bytes(MSS);
        for _ in 0..before / MSS {
            acknowledgment += MSS;
            reno.on_new_ack(MSS, rtt, 1, before, acknowledgment);
        }
        let after = reno.cwnd_bytes(MSS);
        assert_eq!(after, before + MSS, "Reno additive increase at RTT {rtt}");
        twice_trapezoid_area += before / MSS + after / MSS;
    }

    assert_eq!(reno.cwnd_bytes(MSS) / MSS, PEAK_SEGMENTS);
    assert_eq!(PERIOD_RTTS, PEAK_SEGMENTS / 2);
    assert_eq!(twice_trapezoid_area / (2 * PERIOD_RTTS), MEAN_SEGMENTS);
    reno.on_fast_retransmit(PEAK_SEGMENTS * MSS, PERIOD_RTTS);
    reno.on_recovery_exit();
    assert_eq!(reno.cwnd_bytes(MSS) / MSS, LOW_SEGMENTS);
    println!(
        "reno_sawtooth bdp={BDP_SEGMENTS} buffer={BUFFER_SEGMENTS} peak={PEAK_SEGMENTS} period_rtts={PERIOD_RTTS} mean={MEAN_SEGMENTS}"
    );
}

#[test]
fn cubic_window_uses_the_documented_integer_lattice() {
    let mut cubic = TcpCongestionControl::cubic(MSS);
    cubic.force_cubic_epoch_for_test(100 * CUBIC_WINDOW_SCALE, 0);

    let first = cubic.cubic_target_for_test(1_000_000_000, 100_000_000);
    let second = cubic.cubic_target_for_test(2_000_000_000, 100_000_000);

    assert!(first > 0);
    assert!(second > first);
    assert!(second < 100 * CUBIC_WINDOW_SCALE);
}

#[test]
fn cubic_wide_magnitude_transition_is_byte_identical_on_all_available_backends() {
    let image = cubic_wide_magnitude_checkpoint_image();
    validate(&image, Backend::Scalar).expect("wide CUBIC checkpoint should validate");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("wide CUBIC scalar checkpoint should run");
    let TcpCongestionControl::Cubic(after) = scalar.tcp_transitions[0].after else {
        unreachable!()
    };
    assert_eq!(after.cwnd_scaled, 6_094_816_939);

    let cpu = run_cpu_with_observations(&image, None, CpuConfig::default(), ObservationMode::Full)
        .expect("wide CUBIC CPU checkpoint should run");
    assert_eq!(cpu.result, scalar);

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    {
        let metal = run_metal_with_observations(
            &image,
            None,
            MetalConfig::default(),
            ObservationMode::Full,
        )
        .expect("wide CUBIC Metal checkpoint should run");
        assert_eq!(metal.result, scalar);
    }

    #[cfg(feature = "cuda")]
    {
        let cuda =
            run_cuda_with_observations(&image, None, CudaConfig::default(), ObservationMode::Full)
                .expect("wide CUBIC CUDA checkpoint should run");
        assert_eq!(cuda.result, scalar);
    }
}

#[test]
fn retransmission_timeout_is_a_phase_one_fallback_classified_event() {
    assert_eq!(event_phase(EventKind::RetransmissionTimeout), 1);
    assert_eq!(
        event_fel_class(EventKind::RetransmissionTimeout),
        EventFelClass::FallbackHeap
    );

    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("scalar rounds should execute the stale timer");
    assert!(
        scalar
            .rounds
            .iter()
            .flat_map(|round| &round.lp_work)
            .any(|work| work.fallback_classified_pushes > 0),
        "scalar must classify generic-FEL TCP timer insertions as fallback events"
    );

    let cpu = run_cpu_with_observations(
        &image,
        None,
        days_executor::CpuConfig::default(),
        ObservationMode::Full,
    )
    .expect("CPU should execute the stale timer");
    assert!(
        cpu.rounds
            .iter()
            .flat_map(|round| &round.semantic.lp_work)
            .any(|work| work.fallback_classified_pushes > 0),
        "CPU must classify generic-FEL TCP timer insertions as fallback events"
    );
}

#[test]
fn closed_loop_reno_emits_acks_blocks_and_finishes_with_fresh_payloads() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 4 * MSS);
    validate(&image, Backend::Scalar).expect("TCP scalar image should validate");
    validate(&image, Backend::Cpu { workers: 2 }).expect("TCP CPU image should validate");

    let blocked = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("the source should reach its congestion-window gate");
    assert_eq!(
        blocked.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked
    );

    let run = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("closed-loop Reno should run");
    let generator = run.host_states[0].generators[0];

    assert_eq!(run.summary.received_bytes, 4 * u128::from(MSS));
    assert_eq!(run.summary.feedback_packets, 4);
    assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
    assert_eq!(generator.feedback.outstanding_bytes, 0);
    assert_eq!(generator.feedback.unacknowledged_bytes, 0);
    assert_eq!(generator.feedback.arrivals, 4);
    assert_eq!(
        run.observed_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::TcpData(_)))
            .map(|packet| packet.id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4,
        "every transmission attempt consumes a distinct PayloadId"
    );
}

#[test]
fn scalar_rounds_and_cpu_are_byte_identical_for_reno_and_cubic() {
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        let image = tcp_image(control, 8 * MSS);
        let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
            .expect("scalar rounds should run");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            days_executor::CpuConfig {
                workers: 2,
                ..days_executor::CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CPU should run");
        assert_eq!(cpu.result, scalar.result);
    }
}

#[test]
fn tcp_cartesian_matrix_is_byte_identical_across_all_available_backends() {
    let disciplines = [
        ("FIFO", SchedulerKind::Fifo, 0),
        ("TailDrop", SchedulerKind::Fifo, 1),
        ("SP", SchedulerKind::static_priority(vec![1]), 0),
        ("WFQ", SchedulerKind::weighted_fair_queue(vec![1]), 0),
    ];
    let mut comparisons = 0;
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    let mut metal_comparisons = 0;
    #[cfg(feature = "cuda")]
    let mut cuda_comparisons = 0;
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        for (discipline, scheduler, capacity) in &disciplines {
            let image = switched_tcp_image(control, scheduler.clone(), *capacity);
            let partial_horizon = Some(2_000);
            let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
                .unwrap_or_else(|error| panic!("{discipline} scalar full failed: {error}"));
            if matches!(control, TcpCongestionControl::Cubic(_)) {
                assert!(
                    full.tcp_transitions.iter().any(|record| {
                        matches!(record.after, TcpCongestionControl::Cubic(_))
                            && record.after.phase() == TcpPhase::CongestionAvoidance
                    }),
                    "the shared CUBIC fixture must exercise congestion avoidance"
                );
            }
            let partial =
                run_scalar_with_observations(&image, partial_horizon, ObservationMode::Full)
                    .unwrap_or_else(|error| panic!("{discipline} scalar partial failed: {error}"));
            for partition in [
                StaticPartitionPolicy::Modulo,
                StaticPartitionPolicy::RouteLoad,
            ] {
                for workers in [1, 2, 4] {
                    for granularity in [ChunkGranularity::Static, ChunkGranularity::Fixed(1)] {
                        for (horizon, expected) in [(None, &full), (partial_horizon, &partial)] {
                            let actual = run_cpu_with_observations(
                                &image,
                                horizon,
                                CpuConfig {
                                    workers,
                                    granularity,
                                    static_partition: partition,
                                    ..CpuConfig::default()
                                },
                                ObservationMode::Full,
                            )
                            .unwrap_or_else(|error| {
                                panic!(
                                    "{} {discipline} {partition:?}/{workers}/{granularity:?}/{horizon:?} failed: {error}",
                                    control.label()
                                )
                            });
                            assert_eq!(
                                &actual.result,
                                expected,
                                "{} {discipline} {partition:?}/{workers}/{granularity:?}/{horizon:?}",
                                control.label()
                            );
                            comparisons += 1;
                        }
                    }
                }
            }
            #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
            for streams_enabled in [false, true] {
                for round_threads_per_threadgroup in [32, 256] {
                    for (horizon, expected) in [(None, &full), (partial_horizon, &partial)] {
                        validate(&image, Backend::Metal).unwrap_or_else(|error| {
                            panic!(
                                "{} {discipline} Metal validation failed: {error}",
                                control.label()
                            )
                        });
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
                                "{} {discipline} Metal streams={streams_enabled}/geometry={round_threads_per_threadgroup}/{horizon:?} failed: {error}",
                                control.label()
                            )
                        });
                        assert_eq!(
                            &actual.result,
                            expected,
                            "{} {discipline} Metal streams={streams_enabled}/geometry={round_threads_per_threadgroup}/{horizon:?}",
                            control.label()
                        );
                        metal_comparisons += 1;
                    }
                }
            }
            #[cfg(feature = "cuda")]
            for streams_enabled in [false, true] {
                for round_threads_per_block in [32, 256] {
                    for (horizon, expected) in [(None, &full), (partial_horizon, &partial)] {
                        validate(&image, Backend::Cuda).unwrap_or_else(|error| {
                            panic!(
                                "{} {discipline} CUDA validation failed: {error}",
                                control.label()
                            )
                        });
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
                                "{} {discipline} CUDA streams={streams_enabled}/geometry={round_threads_per_block}/{horizon:?} failed: {error}",
                                control.label()
                            )
                        });
                        assert_eq!(
                            &actual.result,
                            expected,
                            "{} {discipline} CUDA streams={streams_enabled}/geometry={round_threads_per_block}/{horizon:?}",
                            control.label()
                        );
                        cuda_comparisons += 1;
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 2 * 4 * 2 * 3 * 2 * 2);
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    assert_eq!(metal_comparisons, 2 * 4 * 2 * 2 * 2);
    #[cfg(feature = "cuda")]
    assert_eq!(cuda_comparisons, 2 * 4 * 2 * 2 * 2);
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_tcp_reno_and_cubic_literal_smoke_is_byte_identical() {
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        let image = tcp_image(control, 2 * MSS);
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("scalar TCP smoke must run");
        let metal = run_metal_with_observations(
            &image,
            None,
            MetalConfig::default(),
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("Metal {} TCP smoke failed: {error}", control.label()));
        assert_eq!(metal.result, scalar, "Metal {} TCP smoke", control.label());
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_tcp_timer_checkpoint_is_byte_identical() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    let prefix = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("scalar TCP prefix must run");
    let checkpoint = checkpoint_image(&image, &prefix);
    let scalar = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
        .expect("scalar timer checkpoint must resume");
    let metal = run_metal_with_observations(
        &checkpoint,
        None,
        MetalConfig::default(),
        ObservationMode::Full,
    )
    .expect("Metal timer checkpoint must resume");
    assert_eq!(metal.result, scalar);
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_tcp_timer_checkpoint_is_byte_identical() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    let prefix = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("scalar TCP prefix must run");
    let checkpoint = checkpoint_image(&image, &prefix);
    let scalar = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
        .expect("scalar timer checkpoint must resume");
    let cuda = run_cuda_with_observations(
        &checkpoint,
        None,
        CudaConfig::default(),
        ObservationMode::Full,
    )
    .expect("CUDA timer checkpoint must resume");
    assert_eq!(cuda.result, scalar);
}

#[test]
fn tcp_lookahead_admits_one_byte_data_segments_and_forty_byte_acks() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    assert_eq!(image.channels[0].min_delay_ns, 1);
    assert_eq!(image.channels[1].min_delay_ns, 4);

    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("round execution should run");
    assert_eq!(run.rounds[0].horizon_advance_ns, 1);
}

#[test]
fn sub_mss_final_segment_respects_safe_horizon_and_cpu_byte_identity() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), MSS + 1);
    validate(&image, Backend::Scalar).expect("TCP scalar image should validate");

    let serial = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("serial execution should deliver the one-byte final segment");
    let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("safe-horizon scalar execution must admit the one-byte final segment");
    assert_eq!(scalar.result, serial);

    for workers in [1, 2, 4] {
        validate(&image, Backend::Cpu { workers }).expect("TCP CPU image should validate");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| {
            panic!("safe-horizon CPU execution with {workers} workers failed: {error}")
        });
        assert_eq!(
            cpu.result, serial,
            "CPU byte identity with {workers} workers"
        );
    }

    assert_eq!(serial.summary.received_bytes, u128::from(MSS + 1));
}

#[test]
fn tcp_images_validate_for_device_backends() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 8 * MSS);
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend)
            .unwrap_or_else(|error| panic!("T24 {backend} TCP image must validate: {error}"));
    }
}

#[test]
fn device_validators_reject_tcp_packets_without_a_tcp_generator() {
    let packet_kinds = [
        PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
        PacketKind::TcpAck(TcpAckHeader {
            acknowledgment: 0,
            acknowledged_bytes: 0,
            echoed_sent_time_ns: 0,
        }),
    ];
    for packet_kind in packet_kinds {
        let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
        image.host_states[0].generators.clear();
        image.host_states[1].tcp_receivers.clear();
        image.initial_packets[0].kind = packet_kind;

        for backend in [Backend::Metal, Backend::Cuda] {
            let error = validate(&image, backend)
                .expect_err("TCP packet metadata requires a TCP generator");
            assert_eq!(
                error.to_string(),
                "TCP packet PayloadId(0) for flow FlowId(0) requires a TCP generator"
            );
        }
    }
}

#[test]
fn device_validators_reject_orphan_tcp_receiver_state() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    image.host_states[0].generators.clear();
    image.initial_packets[0].kind = PacketKind::Data;

    for backend in [Backend::Metal, Backend::Cuda] {
        let error = validate(&image, backend).expect_err("receiver state requires a TCP generator");
        assert_eq!(
            error.to_string(),
            "host node NodeId(1) owns TCP receiver state for flow FlowId(0), but the flow has no generator"
        );
    }
}

#[test]
fn device_validators_accept_tcp_timer_checkpoints() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    let prefix = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("initial TCP send should produce a blocked checkpoint");
    let checkpoint = checkpoint_image(&image, &prefix);
    assert!(checkpoint.initial_events.iter().any(|event| {
        event.kind == EventKind::RetransmissionTimeout
            && event.key.phase == event_phase(EventKind::RetransmissionTimeout)
    }));

    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&checkpoint, backend).unwrap_or_else(|error| {
            panic!("{backend} must accept fallback-heap TCP timers: {error}")
        });
    }
}

#[test]
fn device_sizing_derives_a_tcp_plan() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    let report = size_default_device_plan(&image)
        .expect("generic GPU sizing must account for TCP state and exact fallback timers");
    assert!(report.total_device_bytes > 0);
    assert!(report.event_arenas.fallback_heap_event_slots >= image.nodes.len() + 1);
}

#[test]
fn validator_rejects_tcp_generator_and_controller_mss_disagreement() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.control = TcpCongestionControl::cubic(1460);

    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("transport and congestion control must share one MSS");
        assert_eq!(
            error.to_string(),
            "flow FlowId(0) TCP generator MSS 512 does not match CUBIC controller MSS 1460"
        );
    }
}

#[test]
fn validator_rejects_inconsistent_tcp_transport_state_without_panicking() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.next_sequence = 2 * MSS;
    let PacketKind::TcpData(ref mut header) = image.initial_packets[0].kind else {
        unreachable!()
    };
    header.sequence = 2 * MSS;

    let error = validate(&image, Backend::Scalar).expect_err("invalid TCP state must be rejected");
    assert!(
        error
            .to_string()
            .contains("TCP emitted byte state exceeds total bytes")
    );
}

#[test]
fn validator_rejects_duplicate_tcp_receiver_ownership() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    image.host_states[1]
        .tcp_receivers
        .push(TcpReceiverState::new(FLOW, ACK_BYTES));

    let error = validate(&image, Backend::Scalar).expect_err("duplicate receiver must be rejected");
    assert!(error.to_string().contains("duplicate receiver state"));
}

#[test]
fn validator_rejects_nonprogressing_tcp_blocked_fresh_flow_without_an_event() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    image.host_states[0].generators[0].next_emission.status = GeneratorStatus::Blocked;
    image.initial_events.clear();

    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("a fresh blocked TCP flow without live work must be rejected");
        assert_eq!(
            error.to_string(),
            "flow FlowId(0) TCP generator is Blocked without an active retransmission timer"
        );
    }
}

#[test]
fn validator_rejects_blocked_tcp_with_only_unscheduled_timer_state() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    image.initial_events.clear();
    let generator = &mut image.host_states[0].generators[0];
    generator.next_emission.status = GeneratorStatus::Blocked;
    generator.packets_emitted = 1;
    generator.bytes_emitted = MSS;
    generator.feedback.outstanding_bytes = MSS;
    generator.feedback.unacknowledged_bytes = MSS;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.next_sequence = MSS;
    tcp.bytes_in_flight = MSS;
    tcp.last_attempt = FIRST;
    tcp.timer_generation = 1;
    tcp.active_timer = Some(TcpTimerState {
        attempt: FIRST,
        sequence: 0,
        deadline_ns: TcpGenerator::INITIAL_RTO_NS,
        generation: 1,
        rto_ns: TcpGenerator::INITIAL_RTO_NS,
    });

    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("timer state without a live timer event cannot make progress");
        assert_eq!(
            error.to_string(),
            "flow FlowId(0) TCP active retransmission timer has 0 matching events; expected 1"
        );
    }
}

#[test]
fn validator_rejects_nonprogressing_tcp_zero_rto_before_execution() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    image.stop_time_ns = 0;
    image.host_states[0].next_payload_seq = u64::MAX / 2 - 4;
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.rto_ns = 0;

    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("a zero-time retransmission cycle must be rejected before execution");
        assert_eq!(
            error.to_string(),
            "flow FlowId(0) TCP retransmission timeout must be positive"
        );
    }
}

#[test]
fn validator_reserves_tcp_timer_generation_capacity_before_execution() {
    let mut rejected = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    rejected.stop_time_ns = 0;
    let FlowGeneratorKind::Tcp(ref mut tcp) = rejected.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.timer_generation = u64::MAX;

    let expected = "flow FlowId(0) TCP timer generation 18446744073709551615 overflows with remaining upper bound 1";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&rejected, backend)
            .expect_err("an exhausted timer generation must reject before execution");
        assert_eq!(error.to_string(), expected);
    }

    let mut boundary = rejected;
    let FlowGeneratorKind::Tcp(ref mut tcp) = boundary.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.timer_generation = u64::MAX - 1;
    validate(&boundary, Backend::Scalar).expect("maximum safe Scalar generation must validate");
    validate(&boundary, Backend::Cpu { workers: 2 })
        .expect("maximum safe CPU generation must validate");
    let scalar = run_scalar_with_observations(&boundary, None, ObservationMode::Full)
        .expect("maximum safe Scalar generation must execute");
    let cpu = run_cpu_with_observations(
        &boundary,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("maximum safe CPU generation must execute")
    .result;
    assert_eq!(cpu, scalar, "timer-generation boundary byte identity");
    let checkpoint = checkpoint_image(&boundary, &scalar);
    validate(&checkpoint, Backend::Scalar)
        .expect("maximum safe generation checkpoint must validate for Scalar");
    validate(&checkpoint, Backend::Cpu { workers: 2 })
        .expect("maximum safe generation checkpoint must validate for CPU");
}

#[test]
fn ack_service_siblings_do_not_double_count_timer_installations() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), 1);
    image.stop_time_ns = 2;
    for link in &mut image.links {
        link.rate_bps = u64::MAX;
    }
    image.channels = vec![
        RemoteChannel::for_packet_link(image.links[0], 1).unwrap(),
        RemoteChannel::for_packet_link(image.links[1], ACK_BYTES).unwrap(),
    ];
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.timer_generation = u64::MAX - 3;

    validate(&image, Backend::Scalar).expect("reviewer ACK-sibling probe must validate for Scalar");
    validate(&image, Backend::Cpu { workers: 2 })
        .expect("reviewer ACK-sibling probe must validate for CPU");
    let scalar = run_scalar_with_observations(&image, Some(2), ObservationMode::Full)
        .expect("reviewer ACK-sibling Scalar prefix must execute");
    let cpu = run_cpu_with_observations(
        &image,
        Some(2),
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("reviewer ACK-sibling CPU prefix must execute")
    .result;
    assert_eq!(cpu, scalar, "reviewer ACK-sibling byte identity");
    let FlowGeneratorKind::Tcp(tcp) = scalar.host_states[0].generators[0].kind else {
        unreachable!()
    };
    assert_eq!(tcp.timer_generation, u64::MAX - 2);

    let checkpoint = checkpoint_image(&image, &scalar);
    validate(&checkpoint, Backend::Scalar)
        .expect("reviewer ACK-sibling checkpoint must validate for Scalar");
    validate(&checkpoint, Backend::Cpu { workers: 2 })
        .expect("reviewer ACK-sibling checkpoint must validate for CPU");
}

#[test]
fn validator_reserves_tcp_rto_deadline_headroom_before_execution() {
    let mut rejected = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    rejected.stop_time_ns = 1;
    let generator = &mut rejected.host_states[0].generators[0];
    generator.next_emission.departure_time_ns = 1;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.rto_ns = u64::MAX;
    let PacketKind::TcpData(ref mut header) = rejected.initial_packets[0].kind else {
        unreachable!()
    };
    header.sent_time_ns = 1;
    rejected.initial_events[0].key.time_ns = 1;

    let expected = "flow FlowId(0) TCP retransmission timeout 18446744073709551615 exceeds deadline headroom 18446744073709551614 for stop time 1";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&rejected, backend)
            .expect_err("an unrepresentable first timer deadline must reject before execution");
        assert_eq!(error.to_string(), expected);
    }

    let mut boundary = rejected;
    let FlowGeneratorKind::Tcp(ref mut tcp) = boundary.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.rto_ns = u64::MAX - boundary.stop_time_ns;
    validate(&boundary, Backend::Scalar).expect("maximum safe Scalar RTO must validate");
    validate(&boundary, Backend::Cpu { workers: 2 }).expect("maximum safe CPU RTO must validate");
    let scalar = run_scalar_with_observations(&boundary, None, ObservationMode::Full)
        .expect("maximum safe Scalar RTO must execute");
    let cpu = run_cpu_with_observations(
        &boundary,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("maximum safe CPU RTO must execute")
    .result;
    assert_eq!(cpu, scalar, "RTO deadline boundary byte identity");
    let checkpoint = checkpoint_image(&boundary, &scalar);
    validate(&checkpoint, Backend::Scalar)
        .expect("maximum safe RTO checkpoint must validate for Scalar");
    validate(&checkpoint, Backend::Cpu { workers: 2 })
        .expect("maximum safe RTO checkpoint must validate for CPU");
}

#[test]
fn stale_same_attempt_timers_preserve_checkpoint_closure() {
    let acknowledgments = [0, 0, 0, MSS, 2 * MSS, 2 * MSS, 3 * MSS];
    let mut image = tcp_ack_burst_image(TcpCongestionControl::reno(MSS), 4 * MSS, &acknowledgments);
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.timer_generation = u64::MAX - 8;

    validate(&image, Backend::Scalar).expect("reviewer stale-timer probe must validate for Scalar");
    validate(&image, Backend::Cpu { workers: 2 })
        .expect("reviewer stale-timer probe must validate for CPU");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("reviewer stale-timer Scalar run must execute");
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("reviewer stale-timer CPU run must execute")
    .result;
    assert_eq!(cpu, scalar, "reviewer stale-timer byte identity");

    let checkpoint = checkpoint_image(&image, &scalar);
    let FlowGeneratorKind::Tcp(tcp) = checkpoint.host_states[0].generators[0].kind else {
        unreachable!()
    };
    let timer = tcp
        .active_timer
        .expect("reviewer checkpoint has a live timer");
    assert_eq!(
        checkpoint
            .initial_events
            .iter()
            .filter(|event| {
                event.kind == EventKind::RetransmissionTimeout
                    && event.target == SOURCE
                    && event.payload == timer.attempt
                    && event.key.time_ns == timer.deadline_ns
            })
            .count(),
        3,
        "reviewer checkpoint must retain the live timer and two stale generations"
    );
    validate(&checkpoint, Backend::Scalar)
        .expect("reviewer stale-timer checkpoint must validate for Scalar");
    validate(&checkpoint, Backend::Cpu { workers: 2 })
        .expect("reviewer stale-timer checkpoint must validate for CPU");
}

#[test]
fn scalar_adversarial_trace_covers_tcp_leanguard_transition_classes() {
    let mut traces = Vec::new();
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        let image = recovery_campaign_image(control);
        validate(&image, Backend::Scalar).expect("recovery campaign image should validate");
        let run = run_scalar_with_observations(&image, Some(8), ObservationMode::Full)
            .expect("adversarial recovery TailDrop trace should run");
        let retransmissions = run
            .observed_packets
            .iter()
            .filter_map(|packet| match packet.kind {
                PacketKind::TcpData(header) if header.retransmission => Some((packet.id, header)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(!retransmissions.is_empty());
        for (attempt, header) in retransmissions {
            assert!(run.observed_packets.iter().any(|packet| {
                packet.id != attempt
                    && matches!(
                        packet.kind,
                        PacketKind::TcpData(original)
                            if !original.retransmission && original.sequence == header.sequence
                    )
            }));
        }
        let records = run.tcp_transitions;
        assert!(records.iter().any(|record| {
            matches!(record.input, TcpTransitionInput::DuplicateAck { .. })
                && record.before.duplicate_acks() == 2
                && record.after.phase() == TcpPhase::FastRecovery
        }));
        assert!(records.iter().any(|record| {
            matches!(record.input, TcpTransitionInput::DuplicateAck { .. })
                && record.before.phase() == TcpPhase::FastRecovery
                && record.before.duplicate_acks() >= 3
                && record.after.cwnd_scaled() > record.before.cwnd_scaled()
        }));
        assert!(records.iter().any(|record| {
            matches!(
                record.input,
                TcpTransitionInput::NewAck { acknowledgment, .. }
                    if record.before.phase() == TcpPhase::FastRecovery
                        && acknowledgment < record.before.recovery_high_sequence()
            )
        }));
        assert!(records.iter().any(|record| {
            matches!(
                record.input,
                TcpTransitionInput::NewAck { acknowledgment, .. }
                    if record.before.phase() == TcpPhase::FastRecovery
                        && acknowledgment >= record.before.recovery_high_sequence()
            )
        }));
        if control.label() == "CUBIC" {
            assert!(records.iter().any(|record| record.after.cubic_k_ns() > 0));
        }
        traces.push((format!("{}-recovery", control.label()), records));

        let timeout_run = run_scalar_with_observations(
            &switched_tcp_image(control, SchedulerKind::Fifo, 1),
            None,
            ObservationMode::Full,
        )
        .expect("adversarial timeout TailDrop trace should run");
        assert!(
            timeout_run
                .tcp_transitions
                .iter()
                .any(|record| matches!(record.input, TcpTransitionInput::Timeout { .. }))
        );
        traces.push((
            format!("{}-timeout", control.label()),
            timeout_run.tcp_transitions,
        ));
    }

    assert!(traces.iter().all(|(_, records)| {
        records
            .iter()
            .any(|record| matches!(record.input, TcpTransitionInput::NewAck { .. }))
    }));
    assert!(traces.iter().all(|(_, records)| {
        records
            .iter()
            .any(|record| matches!(record.input, TcpTransitionInput::DuplicateAck { .. }))
    }));
    assert!(traces.iter().any(|(_, records)| {
        records
            .iter()
            .any(|record| matches!(record.input, TcpTransitionInput::Timeout { .. }))
    }));
    let before = TcpCongestionControl::reno(u64::MAX);
    let mut after = before;
    after.on_new_ack(1, 8, 1, u64::MAX, 1);
    traces.push((
        "reno-saturation".to_string(),
        vec![days_executor::TcpTransitionRecord {
            key: EventKey {
                time_ns: 8,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: NodeId(3),
                origin_seq: 99,
            },
            node: SOURCE,
            flow: FlowId(99),
            mss_bytes: u64::MAX,
            input: TcpTransitionInput::NewAck {
                acknowledged_bytes: 1,
                rtt_sample_ns: 1,
                flight_size_bytes: u64::MAX,
                acknowledgment: 1,
            },
            before,
            after,
        }],
    ));
    let output_dir = std::env::var_os("DAYS_TCP_TRACE_DIR").map(std::path::PathBuf::from);
    for (algorithm, records) in traces {
        let csv = days_executor::tcp_transitions_csv(&records)
            .expect("one scalar run must have unique canonical event keys");
        assert_eq!(csv.lines().count(), records.len() + 1);
        if let Some(directory) = &output_dir {
            std::fs::create_dir_all(directory).expect("create requested TCP trace directory");
            let name = format!("{}-tcp-events.csv", algorithm.to_ascii_lowercase());
            std::fs::write(directory.join(name), csv).expect("write requested TCP LeanGuard trace");
        }
    }
}

#[test]
fn retransmission_preserves_cwnd_limited_segment_length() {
    let mut control = TcpCongestionControl::reno(MSS);
    let TcpCongestionControl::Reno(ref mut reno) = control else {
        unreachable!()
    };
    reno.cwnd_bytes = 3 * MSS - 1;

    let image = switched_tcp_image(control, SchedulerKind::Fifo, 1);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the cwnd-limited TailDrop trace should run");
    let original = scalar
        .observed_packets
        .iter()
        .find(|packet| {
            matches!(
                packet.kind,
                PacketKind::TcpData(header)
                    if !header.retransmission && header.sequence == 2 * MSS
            )
        })
        .expect("sequence 1024 should be emitted before its retransmission");
    let retransmission = scalar
        .observed_packets
        .iter()
        .find(|packet| {
            matches!(
                packet.kind,
                PacketKind::TcpData(header)
                    if header.retransmission && header.sequence == 2 * MSS
            )
        })
        .expect("TailDrop should force sequence 1024 to be retransmitted");

    assert_eq!(original.size_bytes, MSS - 1);
    assert_eq!(retransmission.size_bytes, original.size_bytes);

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
        .unwrap_or_else(|error| panic!("CPU execution with {workers} workers failed: {error}"));
        assert_eq!(
            cpu.result, scalar,
            "CPU byte identity with {workers} workers"
        );
    }
}

#[test]
fn partial_cumulative_ack_retransmits_only_unacknowledged_remainder() {
    let image = tcp_ack_burst_image(TcpCongestionControl::reno(MSS), 4 * MSS, &[1; 7]);
    validate(&image, Backend::Scalar).expect("partial-ACK scalar image should validate");
    validate(&image, Backend::Cpu { workers: 2 }).expect("partial-ACK CPU image should validate");

    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full);
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    );
    assert!(
        scalar.is_ok() && cpu.is_ok(),
        "partial ACK outcomes: Scalar={:?}, Cpu={:?}",
        scalar.as_ref().err(),
        cpu.as_ref().err()
    );
    let scalar = scalar.expect("checked scalar success");
    let cpu = cpu.expect("checked CPU success");
    assert_eq!(cpu.result, scalar, "partial-ACK Scalar/CPU byte identity");

    let retransmission = scalar
        .observed_packets
        .iter()
        .find(|packet| {
            matches!(
                packet.kind,
                PacketKind::TcpData(header)
                    if header.retransmission && header.sequence == 1
            )
        })
        .expect("the fourth ACK should retransmit the remainder beginning at sequence 1");
    assert_eq!(retransmission.size_bytes, MSS - 1);
}

#[test]
fn same_timestamp_ack_burst_cannot_escape_tcp_payload_reservation() {
    const ACK_COUNT: u64 = 18;
    let mut control = TcpCongestionControl::reno(MSS);
    let TcpCongestionControl::Reno(ref mut reno) = control else {
        unreachable!()
    };
    reno.cwnd_bytes = ACK_COUNT * MSS;
    let acknowledgments = std::iter::repeat_n(0, 3)
        .chain((1..=ACK_COUNT - 3).map(|segments| segments * MSS))
        .collect::<Vec<_>>();
    assert_eq!(acknowledgments.len(), ACK_COUNT as usize);

    let mut image = tcp_ack_burst_image(control, ACK_COUNT * MSS, &acknowledgments);
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.rto_ns = 1;
    let maximum_sequence = u64::MAX / image.nodes.len() as u64;
    image.host_states[0].next_payload_seq = maximum_sequence - (ACK_COUNT - 1);
    let expected = format!(
        "node NodeId(0) payload identity sequence {} overflows while reserving 55 generated packets",
        image.host_states[0].next_payload_seq
    );

    let outcomes = [
        ("Scalar", Backend::Scalar),
        ("Cpu", Backend::Cpu { workers: 2 }),
    ]
    .map(|(label, backend)| match validate(&image, backend) {
        Err(error) => format!("{label}: rejected: {error}"),
        Ok(()) => {
            let execution = match backend {
                Backend::Scalar => {
                    run_scalar_with_observations(&image, None, ObservationMode::Full).map(|_| ())
                }
                Backend::Cpu { workers } => run_cpu_with_observations(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    ObservationMode::Full,
                )
                .map(|_| ()),
                Backend::Metal | Backend::Cuda => unreachable!(),
            };
            match execution {
                Ok(()) => format!("{label}: validated and completed"),
                Err(error) => format!("{label}: validated then faulted: {error}"),
            }
        }
    });

    assert_eq!(
        outcomes,
        [
            format!("Scalar: rejected: {expected}"),
            format!("Cpu: rejected: {expected}"),
        ]
    );
}

#[test]
fn cpu_checkpoint_retransmission_uses_source_owned_segment_ledger() {
    let mut image = scheduled_tcp_checkpoint_image(2, 4);
    let node_count = image.nodes.len() as u64;
    for origin_seq in 0..3 {
        let payload = PayloadId(SINK.0 + node_count * origin_seq);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: FLOW,
            size_bytes: ACK_BYTES,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment: 0,
                acknowledged_bytes: 0,
                echoed_sent_time_ns: 0,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SINK,
                origin_seq,
            },
            target: SOURCE,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    image.host_states[1].next_origin_seq = 3;
    image.host_states[1].next_payload_seq = 3;
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.sort_by_key(|event| event.key);

    validate(&image, Backend::Scalar).expect("checkpoint scalar image should validate");
    validate(&image, Backend::Cpu { workers: 2 }).expect("checkpoint CPU image should validate");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("checkpoint scalar execution should retransmit sequence zero");
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("checkpoint CPU execution should retransmit sequence zero");
    assert_eq!(cpu.result, scalar, "checkpoint Scalar/CPU byte identity");
    assert!(scalar.observed_packets.iter().any(|packet| {
        matches!(
            packet.kind,
            PacketKind::TcpData(header) if header.retransmission && header.sequence == 0
        )
    }));
}

#[test]
fn ack_normalized_checkpoint_reconstructs_partial_segment_for_both_backends() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    let generator = &mut image.host_states[0].generators[0];
    generator.feedback.outstanding_bytes = MSS - 1;
    generator.feedback.unacknowledged_bytes = MSS - 1;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.highest_ack = 1;
    tcp.bytes_in_flight = MSS - 1;

    let node_count = image.nodes.len() as u64;
    for origin_seq in 0..3 {
        let payload = PayloadId(SINK.0 + node_count * origin_seq);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: FLOW,
            size_bytes: ACK_BYTES,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment: 1,
                acknowledged_bytes: 1,
                echoed_sent_time_ns: 0,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SINK,
                origin_seq,
            },
            target: SOURCE,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    image.host_states[1].next_origin_seq = 3;
    image.host_states[1].next_payload_seq = 3;
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.sort_by_key(|event| event.key);

    validate(&image, Backend::Scalar).expect("partial checkpoint Scalar validation");
    validate(&image, Backend::Cpu { workers: 2 }).expect("partial checkpoint CPU validation");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("Scalar must reconstruct and retransmit the normalized remainder");
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU must reconstruct and retransmit the normalized remainder");

    assert_eq!(cpu.result, scalar, "partial checkpoint Scalar/CPU identity");
    assert!(scalar.observed_packets.iter().any(|packet| {
        packet.size_bytes == MSS - 1
            && matches!(
                packet.kind,
                PacketKind::TcpData(TcpDataHeader {
                    sequence: 1,
                    retransmission: true,
                    ..
                })
            )
    }));
}

#[test]
fn inconsistent_initial_tcp_segment_sizes_are_rejected_by_both_validators() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(4),
        flow: FLOW,
        size_bytes: MSS - 1,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: true,
        }),
    });
    image.host_states[0].next_payload_seq = 3;
    image.initial_packets.sort_by_key(|packet| packet.id);

    let expected = "TCP flow FlowId(0) sequence 0 changed segment size from 512 to 511 bytes";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("same-sequence TCP segments with distinct sizes must reject");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn ack_normalization_conflicts_are_rejected_by_both_validators() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    let generator = &mut image.host_states[0].generators[0];
    generator.feedback.outstanding_bytes = MSS - 1;
    generator.feedback.unacknowledged_bytes = MSS - 1;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.highest_ack = 1;
    tcp.bytes_in_flight = MSS - 1;
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(4),
        flow: FLOW,
        size_bytes: MSS - 2,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 1,
            sent_time_ns: 0,
            retransmission: true,
        }),
    });
    image.host_states[0].next_payload_seq = 3;
    image.initial_packets.sort_by_key(|packet| packet.id);

    let expected = "TCP flow FlowId(0) sequence 1 changed segment size from 510 to 511 bytes";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("ACK-boundary segment conflicts must reject before construction");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn incomplete_initial_tcp_segment_ledger_is_rejected_by_both_validators() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    image.initial_packets.retain(|packet| {
        !matches!(
            packet.kind,
            PacketKind::TcpData(TcpDataHeader { sequence: 0, .. })
        )
    });
    image
        .initial_events
        .retain(|event| event.payload != PayloadId(0));

    let expected = "flow FlowId(0) TCP segment ledger does not cover unacknowledged byte range 0..512; expected segment at sequence 0";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("an in-flight TCP range requires an exact segment ledger");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn future_initial_tcp_segment_is_rejected_before_fresh_send_collision() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(2),
        flow: FLOW,
        size_bytes: MSS - 1,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: MSS,
            sent_time_ns: 0,
            retransmission: false,
        }),
    });
    image.host_states[0].next_payload_seq = 2;
    image.initial_packets.sort_by_key(|packet| packet.id);

    let expected = "flow FlowId(0) TCP segment ledger has unexpected initial segment at sequence 512 at or beyond next sequence 0";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("future TCP ledger entries must reject before construction");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn live_raw_segment_precedes_normalized_ledger_seed_at_partial_horizon() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    let generator = &mut image.host_states[0].generators[0];
    generator.feedback.outstanding_bytes = MSS - 1;
    generator.feedback.unacknowledged_bytes = MSS - 1;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.highest_ack = 1;
    tcp.bytes_in_flight = MSS - 1;

    validate(&image, Backend::Scalar).expect("live/ledger overlap Scalar validation");
    validate(&image, Backend::Cpu { workers: 2 }).expect("live/ledger overlap CPU validation");
    let scalar = run_scalar_with_observations(&image, Some(0), ObservationMode::Full)
        .expect("Scalar partial horizon with live raw descriptor");
    let cpu = run_cpu_with_observations(
        &image,
        Some(0),
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU partial horizon with live raw descriptor");

    assert_eq!(cpu.result, scalar);
    assert!(scalar.resident_packets.iter().any(|packet| {
        packet.id == PayloadId(0)
            && packet.size_bytes == MSS
            && matches!(
                packet.kind,
                PacketKind::TcpData(TcpDataHeader { sequence: 0, .. })
            )
    }));
}

#[test]
fn ledger_only_tcp_seeds_do_not_consume_live_counter_capacity() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    image
        .initial_events
        .retain(|event| event.payload != PayloadId(0));
    let generator = &mut image.host_states[0].generators[0];
    generator.feedback.outstanding_bytes = MSS - 1;
    generator.feedback.unacknowledged_bytes = MSS - 1;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.highest_ack = 1;
    tcp.bytes_in_flight = MSS - 1;
    image.host_states[1].received_packets = u64::MAX - 3;

    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        validate(&image, backend)
            .expect("ledger-only TCP seeds must not consume live packet counter capacity");
    }
}

#[test]
fn partial_ack_and_retransmit_checkpoints_round_trip_byte_identically() {
    let mut image = tcp_ack_burst_image(TcpCongestionControl::reno(MSS), 2 * MSS, &[1; 4]);
    image.stop_time_ns = 1_000_000_001;
    image.links[1].propagation_ns = 10_000;
    for (offset, event) in image
        .initial_events
        .iter_mut()
        .filter(|event| event.key.origin_node == SINK)
        .enumerate()
    {
        event.key.time_ns = 100 + offset as u64;
    }
    image.initial_events.sort_by_key(|event| event.key);

    let uninterrupted = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("uninterrupted partial-ACK campaign");
    let partial_prefix = run_scalar_with_observations(&image, Some(103), ObservationMode::Full)
        .expect("checkpoint after partial ACK and two duplicate ACKs");
    assert!(partial_prefix.resident_packets.iter().any(|packet| {
        packet.size_bytes == MSS - 1
            && matches!(
                packet.kind,
                PacketKind::TcpData(TcpDataHeader { sequence: 1, .. })
            )
    }));
    let after_partial_ack = checkpoint_image(&image, &partial_prefix);
    validate(&after_partial_ack, Backend::Scalar)
        .expect("partial-ACK checkpoint Scalar validation");
    validate(&after_partial_ack, Backend::Cpu { workers: 2 })
        .expect("partial-ACK checkpoint CPU validation");

    let scalar_after_partial =
        run_scalar_with_observations(&after_partial_ack, None, ObservationMode::Full)
            .expect("Scalar continuation after partial ACK");
    let cpu_after_partial = run_cpu_with_observations(
        &after_partial_ack,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU continuation after partial ACK");
    assert_eq!(cpu_after_partial.result, scalar_after_partial);
    assert_eq!(
        stitch_checkpoint_run(&partial_prefix, &scalar_after_partial),
        uninterrupted,
        "partial-ACK checkpoint must reproduce the entire uninterrupted result"
    );
    assert!(scalar_after_partial.observed_packets.iter().any(|packet| {
        packet.size_bytes == MSS - 1
            && matches!(
                packet.kind,
                PacketKind::TcpData(TcpDataHeader {
                    sequence: 1,
                    retransmission: true,
                    ..
                })
            )
    }));

    let retransmit_prefix =
        run_scalar_with_observations(&after_partial_ack, Some(200), ObservationMode::Full)
            .expect("checkpoint after retransmitted remainder crosses the forward path");
    assert!(retransmit_prefix.resident_packets.iter().any(|packet| {
        packet.size_bytes == MSS - 1
            && matches!(
                packet.kind,
                PacketKind::TcpData(TcpDataHeader {
                    sequence: 1,
                    retransmission: true,
                    ..
                })
            )
    }));
    let after_retransmit = checkpoint_image(&after_partial_ack, &retransmit_prefix);
    validate(&after_retransmit, Backend::Scalar)
        .expect("post-retransmit checkpoint Scalar validation");
    validate(&after_retransmit, Backend::Cpu { workers: 2 })
        .expect("post-retransmit checkpoint CPU validation");
    let scalar_after_retransmit =
        run_scalar_with_observations(&after_retransmit, None, ObservationMode::Full)
            .expect("Scalar continuation after retransmit");
    let cpu_after_retransmit = run_cpu_with_observations(
        &after_retransmit,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU continuation after retransmit");
    assert_eq!(cpu_after_retransmit.result, scalar_after_retransmit);
    let through_retransmit = stitch_checkpoint_run(&partial_prefix, &retransmit_prefix);
    assert_eq!(
        stitch_checkpoint_run(&through_retransmit, &scalar_after_retransmit),
        uninterrupted,
        "post-retransmit checkpoint must reproduce the entire uninterrupted result"
    );
}

#[test]
fn receiver_reserves_ack_payloads_for_preloaded_in_flight_data() {
    let mut constant_receiver = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    constant_receiver.initial_packets[0].kind = PacketKind::Data;
    constant_receiver.host_states[0].generators[0].kind =
        FlowGeneratorKind::Constant(ConstantGenerator {
            first_departure_ns: 0,
            interval_ns: 1,
            packet_size_bytes: MSS,
            termination: GeneratorTermination::Bytes(MSS),
        });
    let error = validate(&constant_receiver, Backend::Scalar)
        .expect_err("TCP receiver state must require a TCP generator");
    assert_eq!(
        error.to_string(),
        "host node NodeId(1) owns TCP receiver state for flow FlowId(0), but the flow generator is not TCP"
    );

    let mut constant_tcp_packet = constant_receiver;
    constant_tcp_packet.host_states[1].tcp_receivers.clear();
    constant_tcp_packet.initial_packets.push(PacketDescriptor {
        id: PayloadId(2),
        flow: FLOW,
        size_bytes: MSS,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    });
    constant_tcp_packet.initial_events.push(Event {
        key: EventKey {
            time_ns: 0,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: SOURCE,
            origin_seq: 1,
        },
        target: SINK,
        kind: EventKind::RemoteArrival,
        payload: PayloadId(2),
    });
    constant_tcp_packet.host_states[0].next_origin_seq = 2;
    constant_tcp_packet.host_states[0].next_payload_seq = 2;
    constant_tcp_packet
        .initial_packets
        .sort_by_key(|packet| packet.id);
    constant_tcp_packet
        .initial_events
        .sort_by_key(|event| event.key);
    let error = validate(&constant_tcp_packet, Backend::Scalar)
        .expect_err("TCP packets must require a TCP generator");
    assert_eq!(
        error.to_string(),
        "TCP packet PayloadId(2) for flow FlowId(0) requires a TCP generator"
    );

    let image = scheduled_tcp_checkpoint_image(4, 5);

    let mut counter_image = image.clone();
    counter_image.host_states[1].sourced_packets = u64::MAX - 3;
    let counter_expected = format!(
        "node NodeId(1) counter sourced_packets value {} overflows with remaining upper bound 7",
        counter_image.host_states[1].sourced_packets
    );
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&counter_image, backend)
            .expect_err("preloaded data ACKs must be included in receiver counter bounds");
        assert_eq!(error.to_string(), counter_expected);
    }

    let mut image = image;
    let maximum_sequence = u64::MAX / image.nodes.len() as u64;
    image.host_states[1].next_payload_seq = maximum_sequence - 2;
    let expected = format!(
        "node NodeId(1) payload identity sequence {} overflows while reserving 7 generated packets",
        image.host_states[1].next_payload_seq
    );

    let outcomes = [
        ("Scalar", Backend::Scalar),
        ("Cpu", Backend::Cpu { workers: 2 }),
    ]
    .map(|(label, backend)| match validate(&image, backend) {
        Err(error) => format!("{label}: rejected: {error}"),
        Ok(()) => {
            let execution = match backend {
                Backend::Scalar => {
                    run_scalar_with_observations(&image, None, ObservationMode::Full).map(|_| ())
                }
                Backend::Cpu { workers } => run_cpu_with_observations(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    ObservationMode::Full,
                )
                .map(|_| ()),
                Backend::Metal | Backend::Cuda => unreachable!(),
            };
            match execution {
                Ok(()) => format!("{label}: validated and completed"),
                Err(error) => format!("{label}: validated then faulted: {error}"),
            }
        }
    });

    assert_eq!(
        outcomes,
        [
            format!("Scalar: rejected: {expected}"),
            format!("Cpu: rejected: {expected}"),
        ]
    );
}

#[test]
fn fully_sent_unacknowledged_tcp_requires_its_reverse_ack_channel() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    image.stop_time_ns = 3_000_000_000;
    image.links[0].propagation_ns = 1_500_000_000;
    image.channels = vec![
        RemoteChannel::for_packet_link(image.links[0], 1).unwrap(),
        RemoteChannel::for_packet_link(image.links[1], ACK_BYTES).unwrap(),
    ];

    let uninterrupted = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("uninterrupted final-window timer campaign");
    let prefix = run_scalar_with_observations(
        &image,
        Some(TcpGenerator::INITIAL_RTO_NS),
        ObservationMode::Full,
    )
    .expect("reachable final-window timer boundary");
    let FlowGeneratorKind::Tcp(tcp) = prefix.host_states[0].generators[0].kind else {
        unreachable!()
    };
    assert_eq!(tcp.next_sequence, tcp.total_bytes);
    assert!(tcp.highest_ack < tcp.next_sequence);

    let checkpoint = checkpoint_image(&image, &prefix);
    validate(&checkpoint, Backend::Scalar)
        .expect("the reachable reverse ACK channel must validate for Scalar");
    validate(&checkpoint, Backend::Cpu { workers: 2 })
        .expect("the reachable reverse ACK channel must validate for CPU");
    let scalar = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
        .expect("Scalar continuation with the reverse channel");
    let cpu = run_cpu_with_observations(
        &checkpoint,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU continuation with the reverse channel");
    assert_eq!(cpu.result, scalar);
    assert_eq!(stitch_checkpoint_run(&prefix, &scalar), uninterrupted);

    let completed_checkpoint = checkpoint_image(&image, &uninterrupted);
    validate(&completed_checkpoint, Backend::Scalar)
        .expect("a completed runtime checkpoint must remain valid for Scalar");
    validate(&completed_checkpoint, Backend::Cpu { workers: 2 })
        .expect("a completed runtime checkpoint must remain valid for CPU");
    let mut quiescent_without_channels = completed_checkpoint;
    quiescent_without_channels.channels.clear();
    validate(&quiescent_without_channels, Backend::Scalar)
        .expect("quiescent completed TCP does not require dormant channels for Scalar");
    validate(&quiescent_without_channels, Backend::Cpu { workers: 2 })
        .expect("quiescent completed TCP does not require dormant channels for CPU");

    let mut missing_reverse = checkpoint;
    missing_reverse
        .channels
        .retain(|channel| channel.link != REVERSE);
    let expected = "link LinkId(1) can emit RemoteArrival from node NodeId(1) to node NodeId(0), but no channel is declared";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&missing_reverse, backend)
            .expect_err("an in-flight TCP segment requires its reverse ACK channel");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn scheduled_active_timer_preserves_validate_execute_checkpoint_closure() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
    image.stop_time_ns = 0;
    let FlowGeneratorKind::Tcp(ref mut tcp) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    tcp.last_attempt = PayloadId(2);
    tcp.timer_generation = 1;
    tcp.active_timer = Some(TcpTimerState {
        attempt: PayloadId(2),
        sequence: 0,
        deadline_ns: 0,
        generation: 1,
        rto_ns: TcpGenerator::INITIAL_RTO_NS,
    });

    let outcomes = [Backend::Scalar, Backend::Cpu { workers: 2 }].map(|backend| {
        if let Err(error) = validate(&image, backend) {
            return format!("{backend}: rejected input: {error}");
        }
        let result = match backend {
            Backend::Scalar => run_scalar_with_observations(&image, None, ObservationMode::Full)
                .expect("accepted Scalar image must execute"),
            Backend::Cpu { workers } => {
                run_cpu_with_observations(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    ObservationMode::Full,
                )
                .expect("accepted CPU image must execute")
                .result
            }
            Backend::Metal | Backend::Cuda => unreachable!(),
        };
        let checkpoint = checkpoint_image(&image, &result);
        match validate(&checkpoint, backend) {
            Ok(()) => format!("{backend}: accepted input and continuation"),
            Err(error) => format!("{backend}: accepted input but rejected continuation: {error}"),
        }
    });

    let expected = "flow FlowId(0) TCP generator is Scheduled with an active retransmission timer";
    assert_eq!(
        outcomes,
        [
            format!("Scalar: rejected input: {expected}"),
            format!("Cpu: rejected input: {expected}"),
        ]
    );
}

#[test]
fn reachable_recovery_timer_may_precede_the_latest_attempt() {
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        let image = recovery_campaign_image(control);
        let prefix = run_scalar_with_observations(&image, Some(6), ObservationMode::Full)
            .unwrap_or_else(|error| panic!("{} recovery prefix: {error}", control.label()));
        let FlowGeneratorKind::Tcp(tcp) = prefix.host_states[0].generators[0].kind else {
            unreachable!()
        };
        let timer = tcp.active_timer.expect("recovery keeps the oldest timer");
        assert_eq!(tcp.control.phase(), TcpPhase::FastRecovery);
        assert_ne!(timer.attempt, tcp.last_attempt);

        let checkpoint = checkpoint_image(&image, &prefix);
        validate(&checkpoint, Backend::Scalar)
            .unwrap_or_else(|error| panic!("{} Scalar checkpoint: {error}", control.label()));
        validate(&checkpoint, Backend::Cpu { workers: 2 })
            .unwrap_or_else(|error| panic!("{} CPU checkpoint: {error}", control.label()));
        let scalar = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
            .unwrap_or_else(|error| panic!("{} Scalar continuation: {error}", control.label()));
        let cpu = run_cpu_with_observations(
            &checkpoint,
            None,
            CpuConfig {
                workers: 2,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("{} CPU continuation: {error}", control.label()));
        assert_eq!(cpu.result, scalar, "{} continuation", control.label());
    }
}

#[test]
fn retransmission_timer_requires_unacknowledged_ledger_coverage() {
    for status in [GeneratorStatus::Finished, GeneratorStatus::Blocked] {
        let mut image = tcp_image(TcpCongestionControl::reno(MSS), MSS);
        image.stop_time_ns = 0;
        image.initial_events.clear();
        let generator = &mut image.host_states[0].generators[0];
        generator.packets_emitted = 1;
        generator.bytes_emitted = MSS;
        generator.next_emission.status = status;
        generator.feedback.outstanding_bytes = 0;
        generator.feedback.unacknowledged_bytes = 0;
        let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
            unreachable!()
        };
        tcp.next_sequence = MSS;
        tcp.highest_ack = MSS;
        tcp.last_attempt = FIRST;
        tcp.timer_generation = 1;
        tcp.active_timer = Some(TcpTimerState {
            attempt: FIRST,
            sequence: MSS,
            deadline_ns: 0,
            generation: 1,
            rto_ns: TcpGenerator::INITIAL_RTO_NS,
        });
        image.host_states[0].sourced_packets = 1;
        image.host_states[0].departed_packets = 1;
        image.host_states[1].sourced_packets = 1;
        image.host_states[1].departed_packets = 1;
        image.host_states[1].received_packets = 1;
        image.host_states[1].tcp_receivers[0].next_expected_sequence = MSS;
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RetransmissionTimeout),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::RetransmissionTimeout,
            payload: FIRST,
        });

        let expected = match status {
            GeneratorStatus::Finished => {
                "flow FlowId(0) TCP generator is Finished with an active retransmission timer"
            }
            GeneratorStatus::Blocked => {
                "flow FlowId(0) TCP generator is Blocked without unacknowledged data"
            }
            GeneratorStatus::Scheduled | GeneratorStatus::Stopped => unreachable!(),
        };
        for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
            let error = validate(&image, backend)
                .expect_err("a timer cannot target the empty acknowledged ledger range");
            assert_eq!(error.to_string(), expected);
        }
    }
}

#[test]
fn active_timer_attempt_must_be_source_allocated() {
    let image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    let prefix = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("initial TCP send should produce a blocked checkpoint");
    let mut checkpoint = checkpoint_image(&image, &prefix);
    let next_payload_seq = checkpoint.host_states[0].next_payload_seq;
    let unallocated =
        PayloadId::from_node_sequence(SOURCE, checkpoint.nodes.len() as u64, next_payload_seq)
            .expect("small fixture payload identity");
    let FlowGeneratorKind::Tcp(ref mut tcp) = checkpoint.host_states[0].generators[0].kind else {
        unreachable!()
    };
    let timer = tcp
        .active_timer
        .as_mut()
        .expect("blocked sender has an active timer");
    let old_attempt = timer.attempt;
    timer.attempt = unallocated;
    checkpoint
        .initial_events
        .iter_mut()
        .find(|event| {
            event.kind == EventKind::RetransmissionTimeout && event.payload == old_attempt
        })
        .expect("checkpoint contains the timer event")
        .payload = unallocated;

    let expected = format!(
        "flow FlowId(0) TCP active retransmission timer attempt {unallocated:?} was not allocated by source node NodeId(0)"
    );
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&checkpoint, backend)
            .expect_err("timer identity must name an already allocated source attempt");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn fast_recovery_cannot_retransmit_beyond_the_sent_ledger() {
    for control in [
        TcpCongestionControl::reno(MSS),
        TcpCongestionControl::cubic(MSS),
    ] {
        let image = recovery_campaign_image(control);
        let prefix = run_scalar_with_observations(&image, Some(6), ObservationMode::Full)
            .unwrap_or_else(|error| panic!("{} recovery prefix: {error}", control.label()));
        for corrupt_generator_bound in [false, true] {
            let mut checkpoint = checkpoint_image(&image, &prefix);
            let FlowGeneratorKind::Tcp(ref mut tcp) = checkpoint.host_states[0].generators[0].kind
            else {
                unreachable!()
            };
            assert_eq!(tcp.control.phase(), TcpPhase::FastRecovery);
            let unsent_sequence = tcp.next_sequence + 1;
            if corrupt_generator_bound {
                tcp.recovery_high_sequence = unsent_sequence;
            } else {
                match &mut tcp.control {
                    TcpCongestionControl::Reno(state) => {
                        state.recovery_high_sequence = unsent_sequence;
                    }
                    TcpCongestionControl::Cubic(state) => {
                        state.recovery_high_sequence = unsent_sequence;
                    }
                }
            }

            let expected = format!(
                "flow FlowId(0) TCP recovery can retransmit beyond next sequence {}",
                tcp.next_sequence
            );
            for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
                let error = validate(&checkpoint, backend).expect_err(
                    "every recovery retransmission target must stay inside the sent ledger",
                );
                assert_eq!(error.to_string(), expected);
            }
        }
    }
}

#[test]
fn one_timeout_event_cannot_drive_two_blocked_tcp_flows() {
    let mut image = tcp_image(TcpCongestionControl::reno(MSS), 2 * MSS);
    image.stop_time_ns = TcpGenerator::INITIAL_RTO_NS;
    image.initial_events.clear();
    image.initial_packets[0].size_bytes = MSS;
    let generator = &mut image.host_states[0].generators[0];
    generator.packets_emitted = 1;
    generator.bytes_emitted = MSS;
    generator.next_emission.status = GeneratorStatus::Blocked;
    generator.feedback.outstanding_bytes = MSS;
    generator.feedback.unacknowledged_bytes = MSS;
    let FlowGeneratorKind::Tcp(ref mut tcp) = generator.kind else {
        unreachable!()
    };
    tcp.next_sequence = MSS;
    tcp.bytes_in_flight = MSS;
    tcp.last_attempt = FIRST;
    tcp.timer_generation = 1;
    tcp.active_timer = Some(TcpTimerState {
        attempt: FIRST,
        sequence: 0,
        deadline_ns: TcpGenerator::INITIAL_RTO_NS,
        generation: 1,
        rto_ns: TcpGenerator::INITIAL_RTO_NS,
    });
    image.host_states[0].sourced_packets = 1;
    image.host_states[0].departed_packets = 1;

    let mut second_flow = image.flows[0].clone();
    second_flow.id = FlowId(1);
    image.flows.push(second_flow);
    let mut second_generator = image.host_states[0].generators[0];
    second_generator.flow = FlowId(1);
    second_generator.next_emission.payload = PayloadId(2);
    let FlowGeneratorKind::Tcp(ref mut second_tcp) = second_generator.kind else {
        unreachable!()
    };
    second_tcp.last_attempt = PayloadId(2);
    image.host_states[0].generators.push(second_generator);
    image.host_states[0].next_payload_seq = 2;
    image.host_states[0].sourced_packets = 2;
    image.host_states[0].departed_packets = 2;
    let mut second_receiver = image.host_states[1].tcp_receivers[0].clone();
    second_receiver.flow = FlowId(1);
    image.host_states[1].tcp_receivers.push(second_receiver);
    image.initial_packets.push(PacketDescriptor {
        id: PayloadId(2),
        flow: FlowId(1),
        size_bytes: MSS,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    });
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: TcpGenerator::INITIAL_RTO_NS,
            phase: event_phase(EventKind::RetransmissionTimeout),
            origin_node: SOURCE,
            origin_seq: 0,
        },
        target: SOURCE,
        kind: EventKind::RetransmissionTimeout,
        payload: FIRST,
    });

    let expected = "flows FlowId(0) and FlowId(1) TCP active retransmission timers share event identity at node NodeId(0), payload PayloadId(0), deadline 1000000000";
    for backend in [Backend::Scalar, Backend::Cpu { workers: 2 }] {
        let error = validate(&image, backend)
            .expect_err("one timeout event cannot be claimed by two blocked generators");
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn zero_flight_duplicate_acks_do_not_preempt_a_scheduled_send() {
    let mut image = tcp_ack_burst_image(TcpCongestionControl::reno(MSS), MSS, &[0, 0, 0]);
    image.stop_time_ns = 100;
    image.host_states[0].generators[0]
        .next_emission
        .departure_time_ns = 100;
    let PacketKind::TcpData(ref mut header) = image.initial_packets[0].kind else {
        unreachable!()
    };
    header.sent_time_ns = 100;
    image
        .initial_events
        .iter_mut()
        .find(|event| event.kind == EventKind::PacketArrival)
        .expect("scheduled source event")
        .key
        .time_ns = 100;
    image.initial_events.sort_by_key(|event| event.key);

    validate(&image, Backend::Scalar).expect("stale zero-flight ACKs are safe for Scalar");
    validate(&image, Backend::Cpu { workers: 2 }).expect("stale zero-flight ACKs are safe for CPU");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("zero-flight ACKs must not invalidate the scheduled send");
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU zero-flight ACK handling")
    .result;
    assert_eq!(cpu, scalar);
    assert_eq!(
        scalar.host_states[0].generators[0].packets_emitted, 1,
        "the original scheduled segment must be emitted exactly once"
    );
}

#[test]
fn advancing_ack_does_not_preempt_a_scheduled_send() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    image.stop_time_ns = 200;
    let scheduled_payload = image.host_states[0].generators[0].next_emission.payload;
    image.host_states[0].generators[0]
        .next_emission
        .departure_time_ns = 100;
    let scheduled_event = image
        .initial_events
        .iter_mut()
        .find(|event| event.kind == EventKind::PacketArrival)
        .expect("scheduled source event");
    scheduled_event.key.time_ns = 100;
    let scheduled_packet = image
        .initial_packets
        .iter_mut()
        .find(|packet| packet.id == scheduled_payload)
        .expect("scheduled source descriptor");
    let PacketKind::TcpData(ref mut header) = scheduled_packet.kind else {
        unreachable!()
    };
    header.sent_time_ns = 100;
    image.initial_events.sort_by_key(|event| event.key);

    validate(&image, Backend::Scalar).expect("advancing-ACK image is valid for Scalar");
    validate(&image, Backend::Cpu { workers: 2 }).expect("advancing-ACK image is valid for CPU");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the advancing ACK must preserve the reserved scheduled send");
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU advancing-ACK handling")
    .result;

    assert_eq!(cpu, scalar);
    let prefix = run_scalar_with_observations(&image, Some(100), ObservationMode::Full)
        .expect("Scalar prefix after ACK and before the scheduled send");
    let cpu_prefix = run_cpu_with_observations(
        &image,
        Some(100),
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU prefix after ACK and before the scheduled send")
    .result;
    assert_eq!(cpu_prefix, prefix);
    let FlowGeneratorKind::Tcp(prefix_tcp) = prefix.host_states[0].generators[0].kind else {
        unreachable!()
    };
    assert_eq!(prefix_tcp.highest_ack, MSS);
    assert_eq!(
        prefix.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Scheduled
    );
    let prefix_checkpoint = checkpoint_image(&image, &prefix);
    validate(&prefix_checkpoint, Backend::Scalar).expect("Scalar prefix checkpoint must validate");
    validate(&prefix_checkpoint, Backend::Cpu { workers: 2 })
        .expect("CPU prefix checkpoint must validate");
    let scalar_continuation =
        run_scalar_with_observations(&prefix_checkpoint, None, ObservationMode::Full)
            .expect("Scalar continuation must preserve the scheduled send");
    let cpu_continuation = run_cpu_with_observations(
        &prefix_checkpoint,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU continuation must preserve the scheduled send")
    .result;
    assert_eq!(cpu_continuation, scalar_continuation);
    assert_eq!(stitch_checkpoint_run(&prefix, &scalar_continuation), scalar);
    assert_eq!(scalar.host_states[0].generators[0].packets_emitted, 2);
    assert_eq!(
        scalar
            .observed_packets
            .iter()
            .filter(|packet| packet.id == scheduled_payload)
            .count(),
        1,
        "the reserved scheduled descriptor must be emitted exactly once"
    );
    let checkpoint = checkpoint_image(&image, &scalar);
    validate(&checkpoint, Backend::Scalar).expect("Scalar continuation must validate");
    validate(&checkpoint, Backend::Cpu { workers: 2 }).expect("CPU continuation must validate");
}

#[test]
fn loss_acks_before_a_scheduled_send_still_retransmit() {
    let mut image = scheduled_tcp_checkpoint_image(1, 2);
    image.stop_time_ns = 200;
    let scheduled_payload = image.host_states[0].generators[0].next_emission.payload;
    image.host_states[0].generators[0]
        .next_emission
        .departure_time_ns = 100;
    image.initial_events.retain(|event| event.payload != FIRST);
    let scheduled_event = image
        .initial_events
        .iter_mut()
        .find(|event| event.kind == EventKind::PacketArrival)
        .expect("scheduled source event");
    scheduled_event.key.time_ns = 100;
    let scheduled_packet = image
        .initial_packets
        .iter_mut()
        .find(|packet| packet.id == scheduled_payload)
        .expect("scheduled source descriptor");
    let PacketKind::TcpData(ref mut header) = scheduled_packet.kind else {
        unreachable!()
    };
    header.sent_time_ns = 100;

    let node_count = image.nodes.len() as u64;
    for origin_seq in 0..3 {
        let payload = PayloadId(SINK.0 + node_count * origin_seq);
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow: FLOW,
            size_bytes: ACK_BYTES,
            kind: PacketKind::TcpAck(TcpAckHeader {
                acknowledgment: 0,
                acknowledged_bytes: 0,
                echoed_sent_time_ns: 0,
            }),
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: origin_seq + 1,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SINK,
                origin_seq,
            },
            target: SOURCE,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    image.host_states[1].next_origin_seq = 3;
    image.host_states[1].next_payload_seq = 3;
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.sort_by_key(|event| event.key);

    validate(&image, Backend::Scalar).expect("loss-ACK image is valid for Scalar");
    validate(&image, Backend::Cpu { workers: 2 }).expect("loss-ACK image is valid for CPU");
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("Scalar must retain fast retransmit before the scheduled send");
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU must retain fast retransmit before the scheduled send")
    .result;

    assert_eq!(cpu, scalar);
    let prefix = run_scalar_with_observations(&image, Some(100), ObservationMode::Full)
        .expect("Scalar loss-recovery prefix before the scheduled send");
    let cpu_prefix = run_cpu_with_observations(
        &image,
        Some(100),
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU loss-recovery prefix before the scheduled send")
    .result;
    assert_eq!(cpu_prefix, prefix);
    assert_eq!(
        prefix.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Scheduled
    );
    let prefix_checkpoint = checkpoint_image(&image, &prefix);
    validate(&prefix_checkpoint, Backend::Scalar).expect("Scalar prefix checkpoint must validate");
    validate(&prefix_checkpoint, Backend::Cpu { workers: 2 })
        .expect("CPU prefix checkpoint must validate");
    let scalar_continuation =
        run_scalar_with_observations(&prefix_checkpoint, None, ObservationMode::Full)
            .expect("Scalar loss-recovery continuation");
    let cpu_continuation = run_cpu_with_observations(
        &prefix_checkpoint,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .expect("CPU loss-recovery continuation")
    .result;
    assert_eq!(cpu_continuation, scalar_continuation);
    assert_eq!(stitch_checkpoint_run(&prefix, &scalar_continuation), scalar);
    assert!(scalar.observed_packets.iter().any(|packet| {
        matches!(
            packet.kind,
            PacketKind::TcpData(TcpDataHeader {
                sequence: 0,
                retransmission: true,
                ..
            })
        )
    }));
    let checkpoint = checkpoint_image(&image, &scalar);
    validate(&checkpoint, Backend::Scalar).expect("Scalar continuation must validate");
    validate(&checkpoint, Backend::Cpu { workers: 2 }).expect("CPU continuation must validate");
}

use std::collections::{BTreeMap, VecDeque};

use days_executor::{
    Backend, ChunkGranularity, ConstantGenerator, CpuConfig, CpuFaultInjection, CpuFaultKind,
    Event, EventKey, EventKind, ExecutionError, FlowDescriptor, FlowGeneratorKind,
    FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination,
    HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode,
    PacketDescriptor, PacketKind, PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind,
    SimulationImage, SwitchQueueState, SwitchState, WorkClass, event_phase,
    run_cpu_with_observations, run_scalar_rounds_with_observations, run_scalar_with_observations,
    validate,
};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const LINK: LinkId = LinkId(0);
const FLOW: FlowId = FlowId(0);
const PACKET: PayloadId = PayloadId(0);

fn image(stop_time_ns: u64, event_time_ns: u64, channel_delay_ns: u64) -> SimulationImage {
    SimulationImage {
        stop_time_ns,
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
                egress_link: LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LINK,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
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
            route: vec![LINK],
            reverse_route: vec![],
        }],
        initial_packets: vec![PacketDescriptor {
            id: PACKET,
            flow: FLOW,
            size_bytes: 1,
            kind: PacketKind::Data,
        }],
        links: vec![LinkDescriptor {
            id: LINK,
            source: SOURCE,
            target: SINK,
            rate_bps: 8_000_000_000,
            propagation_ns: channel_delay_ns - 1,
        }],
        channels: vec![RemoteChannel {
            source: SOURCE,
            target: SINK,
            link: LINK,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: channel_delay_ns,
        }],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: event_time_ns,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: PACKET,
        }],
        seed: 1,
    }
}

fn add_sink_owned_egress(image: &mut SimulationImage) {
    let sink_egress = LinkDescriptor {
        id: LinkId(1),
        source: SINK,
        target: SOURCE,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    image.host_states[1].egress_link = sink_egress.id;
    image.links.push(sink_egress);
}

fn causally_incompatible_positions_image() -> SimulationImage {
    let mut image = image(0, 0, 1);
    add_sink_owned_egress(&mut image);
    image.host_states[0].next_origin_seq = 2;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload: PACKET,
        },
        Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload: PACKET,
        },
    ];
    image
}

fn in_flight_image(propagation_ns: u64) -> SimulationImage {
    let transmission_complete_ns = 1;
    let remote_arrival_ns = transmission_complete_ns + propagation_ns;
    let mut image = image(remote_arrival_ns, 0, propagation_ns + 1);
    add_sink_owned_egress(&mut image);
    image.host_states[0].in_service = Some(PACKET);
    image.host_states[0].next_origin_seq = 2;
    image.host_states[0].sourced_packets = 1;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: transmission_complete_ns,
                phase: event_phase(EventKind::TxComplete),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::TxComplete,
            payload: PACKET,
        },
        Event {
            key: EventKey {
                time_ns: remote_arrival_ns,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload: PACKET,
        },
    ];
    image.initial_events.sort_unstable_by_key(|event| event.key);
    image
}

fn ready_token_and_remote_arrival_image() -> SimulationImage {
    let queued = PayloadId::from_node_sequence(SOURCE, 2, 1).unwrap();
    let mut image = image(5, 0, 4);
    add_sink_owned_egress(&mut image);
    image.initial_packets.push(PacketDescriptor {
        id: queued,
        flow: FLOW,
        size_bytes: 1,
        kind: PacketKind::Data,
    });
    image.host_states[0].queue = VecDeque::from([queued]);
    image.host_states[0].tx_ready_pending = true;
    image.host_states[0].next_origin_seq = 3;
    image.host_states[0].sourced_packets = 2;
    image.host_states[0].departed_packets = 1;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 1,
                phase: event_phase(EventKind::TxReady),
                origin_node: SOURCE,
                origin_seq: 2,
            },
            target: SOURCE,
            kind: EventKind::TxReady,
            payload: PACKET,
        },
        Event {
            key: EventKey {
                time_ns: 4,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SINK,
            kind: EventKind::RemoteArrival,
            payload: PACKET,
        },
    ];
    image
}

fn assert_equivalent(image: &SimulationImage, exclusive_horizon_ns: Option<u64>) {
    let global =
        run_scalar_with_observations(image, exclusive_horizon_ns, ObservationMode::Full).unwrap();
    let rounds =
        run_scalar_rounds_with_observations(image, exclusive_horizon_ns, ObservationMode::Full)
            .unwrap();

    assert_eq!(rounds.result, global);
    for round in &rounds.rounds {
        assert_eq!(
            round.events_processed,
            round.lp_work.iter().map(|work| work.events_processed).sum()
        );
        assert_eq!(round.active_lp_count, round.lp_work.len());
        assert!(round.lp_work.iter().all(|work| work.events_processed > 0));
    }
}

#[test]
fn causally_incompatible_initial_payload_positions_are_rejected() {
    let image = causally_incompatible_positions_image();
    let expected = "payload PayloadId(0) has causally incompatible initial positions: PacketArrival event 1 at node NodeId(0) and RemoteArrival event 0 on link LinkId(0) from NodeId(0) to NodeId(1)";

    for backend in [Backend::Scalar, Backend::Cpu { workers: 1 }] {
        let diagnostic = validate(&image, backend)
            .expect_err("causally incompatible positions must reject")
            .to_string();
        println!("{backend} validation: Err({diagnostic})");
        assert_eq!(diagnostic, expected);
    }
}

#[test]
fn in_flight_pair_must_be_siblings_from_one_transmission() {
    let mut image = in_flight_image(0);
    image.initial_events[0].key.origin_seq = 2;
    image.host_states[0].next_origin_seq = 3;

    assert_eq!(
        validate(&image, Backend::Scalar)
            .expect_err("independently emitted positions must reject")
            .to_string(),
        "payload PayloadId(0) has causally incompatible initial positions: TxComplete event 1 at node NodeId(0) and RemoteArrival event 0 on link LinkId(0) from NodeId(0) to NodeId(1)"
    );
}

#[test]
fn in_flight_completion_and_remote_arrival_remain_valid_and_equivalent() {
    for propagation_ns in [0, 3] {
        let image = in_flight_image(propagation_ns);
        validate(&image, Backend::Scalar).unwrap();
        validate(&image, Backend::Cpu { workers: 1 }).unwrap();
        assert_equivalent(&image, None);

        let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 4,
                granularity: ChunkGranularity::Fixed(1),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, result);
        assert_eq!(result.summary.departed_packets, 1);
        assert_eq!(result.summary.received_packets, 1);
        assert!(result.resident_packets.is_empty());
        assert!(result.pending_events.is_empty());
    }
}

#[test]
fn ready_control_token_can_coexist_with_an_in_flight_payload() {
    let image = ready_token_and_remote_arrival_image();
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 1 }).unwrap();
    assert_equivalent(&image, None);

    let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();
    assert_eq!(cpu.result, result);
    assert_eq!(result.summary.departed_packets, 1);
    assert_eq!(result.summary.received_packets, 2);
    assert!(result.resident_packets.is_empty());
    assert!(result.pending_events.is_empty());
}

#[test]
fn cpu_preserves_accepted_orphan_packet_snapshots() {
    let mut lost_remote = in_flight_image(4);
    lost_remote
        .initial_events
        .retain(|event| event.kind == EventKind::TxComplete);
    validate(&lost_remote, Backend::Scalar).unwrap();
    validate(&lost_remote, Backend::Cpu { workers: 2 }).unwrap();

    let mut orphan_ready = ready_token_and_remote_arrival_image();
    orphan_ready
        .initial_events
        .retain(|event| event.kind == EventKind::TxReady);
    validate(&orphan_ready, Backend::Scalar).unwrap();
    validate(&orphan_ready, Backend::Cpu { workers: 2 }).unwrap();

    for image in [lost_remote, orphan_ready] {
        let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        let actual = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity: ChunkGranularity::Fixed(1),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(actual.result, expected);
    }
}

#[test]
fn scalar_rounds_match_the_global_queue_and_record_sparse_round_work() {
    let image = image(100, 0, 10);
    assert_equivalent(&image, None);

    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();
    assert_eq!(run.rounds[0].events_processed, 3);
    assert_eq!(run.rounds[0].messages_exchanged, 1);
    assert_eq!(run.rounds[0].lp_work[0].node, SOURCE);
    assert_eq!(run.rounds[1].events_processed, 1);
    assert_eq!(run.rounds[1].lp_work[0].node, SINK);
    assert_eq!(run.rounds[0].parallel_efficiency, 1.0);
    assert_eq!(run.rounds[1].parallel_efficiency, 1.0);
}

#[test]
fn cpu_handles_no_active_lps_and_more_workers_than_nodes() {
    let mut image = image(100, 0, 10);
    image.initial_events.clear();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 8,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert!(actual.rounds.is_empty());
    assert_eq!(actual.result, expected);
}

#[test]
fn inclusive_stop_and_half_open_external_horizon_remain_distinct() {
    let image = image(10, 10, 1);
    assert_equivalent(&image, None);
    assert_equivalent(&image, Some(10));

    let through_stop = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("the inclusive stop event should execute");
    let before_horizon =
        run_scalar_rounds_with_observations(&image, Some(10), ObservationMode::Full)
            .expect("the half-open horizon should return without executing its boundary");
    assert_eq!(through_stop.result.summary.sourced_packets, 1);
    assert_eq!(before_horizon.result.summary.sourced_packets, 0);
}

fn direct_packets(channel_delay_ns: u64, packet_count: u64) -> SimulationImage {
    let mut image = image(1_000, 0, channel_delay_ns);
    image.flows.clear();
    image.initial_packets.clear();
    image.initial_events.clear();
    image.host_states[0].next_origin_seq = packet_count;
    for sequence in 0..packet_count {
        let flow = FlowId(sequence);
        let payload = PayloadId::from_node_sequence(SOURCE, 2, sequence).unwrap();
        image.flows.push(FlowDescriptor {
            id: flow,
            source: SOURCE,
            target: SINK,
            route: vec![LINK],
            reverse_route: vec![],
        });
        image.initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes: 1,
            kind: PacketKind::Data,
        });
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: sequence,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: SOURCE,
                origin_seq: sequence,
            },
            target: SOURCE,
            kind: EventKind::PacketArrival,
            payload,
        });
    }
    image
}

#[test]
fn wide_and_narrow_lookahead_pin_events_per_round_separately_from_efficiency() {
    let wide = direct_packets(100, 8);
    let narrow = direct_packets(1, 2);
    assert_equivalent(&wide, None);
    assert_equivalent(&narrow, None);

    let wide = run_scalar_rounds_with_observations(&wide, None, ObservationMode::Full).unwrap();
    let narrow = run_scalar_rounds_with_observations(&narrow, None, ObservationMode::Full).unwrap();
    assert_eq!(wide.rounds[0].events_per_round(), 24);
    assert_eq!(wide.rounds[0].messages_exchanged, 8);
    assert_eq!(narrow.rounds[0].events_per_round(), 2);
    assert_eq!(narrow.rounds[0].messages_exchanged, 1);
    assert_eq!(wide.rounds[0].parallel_efficiency, 1.0);
    assert_eq!(narrow.rounds[0].parallel_efficiency, 1.0);
}

#[test]
fn same_time_tx_ready_continuations_skip_round_queue_churn_without_changing_state() {
    let image = direct_packets(100, 8);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let scalar = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Static,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(scalar.result, expected);
    assert_eq!(cpu.result, expected);
    assert_eq!(
        scalar
            .rounds
            .iter()
            .flat_map(|round| &round.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>(),
        7
    );
    assert_eq!(
        cpu.rounds
            .iter()
            .flat_map(|round| &round.semantic.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>(),
        7
    );
}

#[test]
fn cross_transport_round_semantics_match_for_remote_and_finish_merges() {
    let image = image(100, 0, 10);
    for exclusive_horizon_ns in [None, Some(10)] {
        let expected = run_scalar_rounds_with_observations(
            &image,
            exclusive_horizon_ns,
            ObservationMode::Full,
        )
        .unwrap();
        let semantics = |round: &days_executor::RoundMetrics| {
            (
                round.frontier_ns,
                round.exclusive_horizon_ns,
                round.horizon_advance_ns,
                round.events_processed,
                round.active_lp_count,
                round.lp_work.clone(),
                round.parallel_efficiency,
                round.messages_exchanged,
                round.frontier_updates,
                round.frontier_heap_pops,
            )
        };
        let expected_semantics = expected.rounds.iter().map(&semantics).collect::<Vec<_>>();
        assert_eq!(
            expected
                .rounds
                .iter()
                .map(|round| round.frontier_updates)
                .collect::<Vec<_>>(),
            if exclusive_horizon_ns.is_some() {
                vec![2]
            } else {
                vec![2, 1]
            }
        );

        for workers in [1, 2, 4] {
            for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
                let actual = run_cpu_with_observations(
                    &image,
                    exclusive_horizon_ns,
                    CpuConfig {
                        workers,
                        granularity,
                        ..CpuConfig::default()
                    },
                    ObservationMode::Full,
                )
                .unwrap();
                let actual_semantics = actual
                    .rounds
                    .iter()
                    .map(|round| semantics(&round.semantic))
                    .collect::<Vec<_>>();

                assert_eq!(
                    actual.result, expected.result,
                    "workers={workers}, granularity={granularity:?}, \
                     exclusive_horizon_ns={exclusive_horizon_ns:?}"
                );
                assert_eq!(
                    actual_semantics, expected_semantics,
                    "workers={workers}, granularity={granularity:?}, \
                     exclusive_horizon_ns={exclusive_horizon_ns:?}"
                );
            }
        }
    }
}

#[test]
fn cpu_batches_remote_events_once_per_source_and_target_owner_per_round() {
    let mut image = incast_image(8);
    image.initial_events.clear();
    for sender in 0..8 {
        let sender = NodeId(sender);
        let state = &mut image.host_states[sender.0 as usize];
        state.in_service = None;
        state.next_origin_seq = 1;
        state.sourced_packets = 0;
        let payload = image.initial_packets[sender.0 as usize].id;
        image.initial_events.push(Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: sender,
                origin_seq: 0,
            },
            target: sender,
            kind: EventKind::PacketArrival,
            payload,
        });
    }
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 1,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    assert_eq!(actual.rounds[0].semantic.active_lp_count, 8);
    assert_eq!(actual.rounds[0].semantic.messages_exchanged, 8);
    assert_eq!(actual.rounds[0].partition.bulk_chunks.len(), 8);
    assert_eq!(actual.rounds[0].owner_batch_messages, 1);
    assert!(
        actual
            .rounds
            .iter()
            .all(|round| round.owner_batch_messages <= 1)
    );
}

#[test]
fn static_cpu_pool_uses_one_assignment_and_one_completion_per_worker_per_round() {
    let image = direct_packets(100, 8);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let workers = 4;
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers,
            granularity: ChunkGranularity::Static,
            straggler_threshold_events: None,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    for round in &actual.rounds {
        assert_eq!(round.worker_wake_messages, workers as u64);
        assert_eq!(round.worker_completion_messages, workers as u64);
        assert_eq!(round.chunk_request_messages, 0);
        assert_eq!(round.pool_messages(), 2 * workers as u64);
    }
}

#[test]
fn bounded_spin_configuration_preserves_static_and_dynamic_results() {
    let image = direct_packets(100, 8);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for spin_before_park in [0, 4_096] {
        for granularity in [ChunkGranularity::Static, ChunkGranularity::Fixed(1)] {
            let actual = run_cpu_with_observations(
                &image,
                None,
                CpuConfig {
                    workers: 4,
                    granularity,
                    spin_before_park,
                    ..CpuConfig::default()
                },
                ObservationMode::Full,
            )
            .unwrap();
            assert_eq!(
                actual.result, expected,
                "spin_before_park={spin_before_park}, granularity={granularity:?}"
            );
        }
    }
}

fn blocked_feedback_image() -> SimulationImage {
    let forward = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(1),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let reverse = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let feedback = PayloadId(1);
    SimulationImage {
        stop_time_ns: 10,
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Host,
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
                    flow: FlowId(0),
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status: GeneratorStatus::Blocked,
                        departure_time_ns: 0,
                        payload: PayloadId(0),
                    },
                    rng_state: 1,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: 0,
                        interval_ns: 1,
                        packet_size_bytes: 1,
                        termination: GeneratorTermination::Bytes(1),
                    }),
                }],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(1),
                queue: VecDeque::from([feedback]),
                in_service: None,
                tx_ready_pending: true,
                generators: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
        ],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: FlowId(0),
            source: NodeId(0),
            target: NodeId(1),
            route: vec![LinkId(0)],
            reverse_route: vec![LinkId(1)],
        }],
        initial_packets: vec![PacketDescriptor {
            id: feedback,
            flow: FlowId(0),
            size_bytes: 1,
            kind: PacketKind::Feedback,
        }],
        links: vec![forward, reverse],
        channels: vec![
            RemoteChannel::for_packet_link(forward, 1).unwrap(),
            RemoteChannel::for_packet_link(reverse, 1).unwrap(),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 5,
                phase: event_phase(EventKind::TxReady),
                origin_node: NodeId(1),
                origin_seq: 0,
            },
            target: NodeId(1),
            kind: EventKind::TxReady,
            payload: feedback,
        }],
        seed: 9,
    }
}

#[test]
fn blocked_lp_is_absent_from_the_frontier_until_feedback_arrives() {
    let image = blocked_feedback_image();
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 1 }).unwrap();
    assert_equivalent(&image, None);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();

    assert_eq!(run.rounds[0].frontier_ns, 5);
    assert_eq!(run.rounds[0].exclusive_horizon_ns, 6);
    assert_eq!(
        run.rounds[0]
            .lp_work
            .iter()
            .map(|work| work.node)
            .collect::<Vec<_>>(),
        vec![NodeId(1)]
    );
    assert!(
        run.rounds[1]
            .lp_work
            .iter()
            .any(|work| work.node == NodeId(0))
    );
    assert_eq!(
        run.result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Blocked
    );
    assert_eq!(run.result.host_states[0].generators[0].feedback.arrivals, 1);
}

fn canonical_exchange_image() -> SimulationImage {
    let source_zero_link = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(2),
        rate_bps: 8_000_000_000,
        propagation_ns: 9,
    };
    let source_one_link = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(2),
        rate_bps: 8_000_000_000,
        propagation_ns: 10,
    };
    let switch_link = LinkDescriptor {
        id: LinkId(2),
        source: NodeId(2),
        target: NodeId(3),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let sink_egress = LinkDescriptor {
        id: LinkId(3),
        source: NodeId(3),
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let packet_zero = PayloadId(0);
    let packet_one = PayloadId(1);
    SimulationImage {
        stop_time_ns: 100,
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(2),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Host,
                state_slot: 2,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LinkId(0),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(1),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                next_origin_seq: 1,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(3),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
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
                egress_link: Some(LinkId(2)),
                scheduler: SchedulerKind::Fifo,
                queue_capacity_packets: 1,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
            }],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        }],
        flows: vec![
            FlowDescriptor {
                id: FlowId(0),
                source: NodeId(0),
                target: NodeId(3),
                route: vec![LinkId(0), LinkId(2)],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(1),
                target: NodeId(3),
                route: vec![LinkId(1), LinkId(2)],
                reverse_route: vec![],
            },
        ],
        initial_packets: vec![
            PacketDescriptor {
                id: packet_zero,
                flow: FlowId(0),
                size_bytes: 1,
                kind: PacketKind::Data,
            },
            PacketDescriptor {
                id: packet_one,
                flow: FlowId(1),
                size_bytes: 1,
                kind: PacketKind::Data,
            },
        ],
        links: vec![source_zero_link, source_one_link, switch_link, sink_egress],
        channels: vec![
            RemoteChannel::for_packet_link(source_zero_link, 1).unwrap(),
            RemoteChannel::for_packet_link(source_one_link, 1).unwrap(),
            RemoteChannel::for_packet_link(switch_link, 1).unwrap(),
        ],
        initial_events: vec![
            Event {
                key: EventKey {
                    time_ns: 0,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(1),
                    origin_seq: 0,
                },
                target: NodeId(1),
                kind: EventKind::PacketArrival,
                payload: packet_one,
            },
            Event {
                key: EventKey {
                    time_ns: 1,
                    phase: event_phase(EventKind::PacketArrival),
                    origin_node: NodeId(0),
                    origin_seq: 0,
                },
                target: NodeId(0),
                kind: EventKind::PacketArrival,
                payload: packet_zero,
            },
        ],
        seed: 10,
    }
}

#[test]
fn canonical_exchange_reorders_outboxes_before_finite_queue_admission() {
    let image = canonical_exchange_image();
    validate(&image, Backend::Cpu { workers: 1 }).unwrap();
    assert_equivalent(&image, None);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full).unwrap();
    let at_switch = run
        .result
        .arrivals
        .iter()
        .filter(|arrival| arrival.time_ns == 11)
        .collect::<Vec<_>>();

    assert_eq!(at_switch[0].payload, PayloadId(0));
    assert_eq!(
        at_switch[0].disposition,
        days_executor::ArrivalDisposition::Admitted
    );
    assert_eq!(at_switch[1].payload, PayloadId(1));
    assert_eq!(
        at_switch[1].disposition,
        days_executor::ArrivalDisposition::Dropped
    );
    let cpu = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();
    assert_eq!(cpu.result, run.result);
}

fn add_idle_switches(image: &mut SimulationImage, count: usize) {
    for _ in 0..count {
        let id = NodeId(image.nodes.len() as u64);
        let state_slot = image.switch_states.len() as u32;
        image.nodes.push(NodeDescriptor {
            id,
            kind: NodeKind::Switch,
            state_slot,
        });
        image.switch_states.push(SwitchState {
            physical_switch: u64::from(state_slot),
            queues: vec![],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        });
    }
}

#[test]
fn idle_lp_count_does_not_change_round_loop_operations() {
    let mut small = image(100, 0, 10);
    let mut large = small.clone();
    add_idle_switches(&mut small, 100);
    add_idle_switches(&mut large, 10_000);

    let scalar_small =
        run_scalar_rounds_with_observations(&small, None, ObservationMode::Full).unwrap();
    let scalar_large =
        run_scalar_rounds_with_observations(&large, None, ObservationMode::Full).unwrap();
    let operations = |run: &days_executor::ScalarRoundRun| {
        run.rounds
            .iter()
            .map(|round| {
                (
                    round.frontier_ns,
                    round.exclusive_horizon_ns,
                    round.events_processed,
                    round.active_lp_count,
                    round.lp_work.clone(),
                    round.messages_exchanged,
                    round.frontier_updates,
                    round.frontier_heap_pops,
                )
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(operations(&scalar_small), operations(&scalar_large));

    let config = CpuConfig {
        workers: 4,
        granularity: ChunkGranularity::Fixed(1),
        ..CpuConfig::default()
    };
    let cpu_small = run_cpu_with_observations(&small, None, config, ObservationMode::Full).unwrap();
    let cpu_large = run_cpu_with_observations(&large, None, config, ObservationMode::Full).unwrap();
    let cpu_operations = |run: &days_executor::CpuRun| {
        run.rounds
            .iter()
            .map(|round| {
                (
                    round.semantic.frontier_ns,
                    round.semantic.exclusive_horizon_ns,
                    round.semantic.events_processed,
                    round.semantic.active_lp_count,
                    round.semantic.lp_work.clone(),
                    round.semantic.messages_exchanged,
                    round.semantic.frontier_updates,
                    round.semantic.frontier_heap_pops,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(cpu_operations(&cpu_small), cpu_operations(&cpu_large));
    let physical_probes = |run: &days_executor::ScalarRoundRun| {
        run.rounds
            .iter()
            .map(|round| round.physical_lp_probes)
            .collect::<Vec<_>>()
    };
    let cpu_physical_probes = |run: &days_executor::CpuRun| {
        run.rounds
            .iter()
            .map(|round| round.semantic.physical_lp_probes)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        (
            physical_probes(&scalar_small),
            cpu_physical_probes(&cpu_small)
        ),
        (
            physical_probes(&scalar_large),
            cpu_physical_probes(&cpu_large)
        )
    );
    assert_eq!(cpu_small.result, scalar_small.result);
    assert_eq!(cpu_large.result, scalar_large.result);
}

#[derive(Clone, Copy)]
struct StableRng(u64);

impl StableRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn range(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn heterogeneous_image(seed: u64) -> SimulationImage {
    let mut rng = StableRng(seed);
    let idle_switches = rng.range(5) as usize;
    let node_count = 5 + idle_switches;
    let source_link = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(1),
        rate_bps: [2_000_000_000, 4_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(5),
    };
    let switch_link = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(3),
        rate_bps: [1_000_000_000, 2_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(7),
    };
    let alternate_switch_link = LinkDescriptor {
        id: LinkId(2),
        source: NodeId(2),
        target: NodeId(4),
        rate_bps: [1_000_000_000, 4_000_000_000, 8_000_000_000][rng.range(3) as usize],
        propagation_ns: rng.range(7),
    };
    let return_link = LinkDescriptor {
        id: LinkId(3),
        source: NodeId(3),
        target: NodeId(1),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let alternate_return_link = LinkDescriptor {
        id: LinkId(4),
        source: NodeId(4),
        target: NodeId(2),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let flow_count = 2 + rng.range(10);
    let packet_count = flow_count;
    let mut flows = Vec::new();
    for flow in 0..flow_count {
        let alternate_sink = flow % 2 == 1;
        flows.push(FlowDescriptor {
            id: FlowId(flow),
            source: NodeId(0),
            target: if alternate_sink { NodeId(4) } else { NodeId(3) },
            route: vec![LinkId(0), LinkId(if alternate_sink { 2 } else { 1 })],
            reverse_route: vec![],
        });
    }

    let mut initial_packets = Vec::new();
    let mut initial_events = Vec::new();
    let mut minimum_source_size = u64::MAX;
    let mut minimum_switch_size = u64::MAX;
    let mut minimum_alternate_switch_size = u64::MAX;
    for sequence in 0..packet_count {
        let flow = FlowId(sequence);
        let size_bytes = 1 + rng.range(4);
        let payload =
            PayloadId::from_node_sequence(NodeId(0), node_count as u64, sequence).unwrap();
        initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes,
            kind: PacketKind::Data,
        });
        initial_events.push(Event {
            key: EventKey {
                time_ns: rng.range(18),
                phase: event_phase(EventKind::PacketArrival),
                origin_node: NodeId(0),
                origin_seq: sequence,
            },
            target: NodeId(0),
            kind: EventKind::PacketArrival,
            payload,
        });
        minimum_source_size = minimum_source_size.min(size_bytes);
        if flows[flow.0 as usize].target == NodeId(3) {
            minimum_switch_size = minimum_switch_size.min(size_bytes);
        } else {
            minimum_alternate_switch_size = minimum_alternate_switch_size.min(size_bytes);
        }
    }
    initial_events.sort_unstable_by_key(|event| event.key);
    if minimum_switch_size == u64::MAX {
        minimum_switch_size = 1;
    }
    if minimum_alternate_switch_size == u64::MAX {
        minimum_alternate_switch_size = 1;
    }

    let mut image = SimulationImage {
        stop_time_ns: 20 + rng.range(120),
        nodes: vec![
            NodeDescriptor {
                id: NodeId(0),
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(1),
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: NodeId(2),
                kind: NodeKind::Switch,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(3),
                kind: NodeKind::Host,
                state_slot: 1,
            },
            NodeDescriptor {
                id: NodeId(4),
                kind: NodeKind::Host,
                state_slot: 2,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: LinkId(0),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                next_origin_seq: packet_count,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(3),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
                next_origin_seq: 0,
                next_payload_seq: 0,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: LinkId(4),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![],
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
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1 + rng.range(5),
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
                physical_switch: 0,
                queues: vec![SwitchQueueState {
                    egress_link: Some(LinkId(2)),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: 1 + rng.range(5),
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
        flows,
        initial_packets,
        links: vec![
            source_link,
            switch_link,
            alternate_switch_link,
            return_link,
            alternate_return_link,
        ],
        channels: vec![
            RemoteChannel::for_packet_link_to(source_link, NodeId(1), minimum_source_size).unwrap(),
            RemoteChannel::for_packet_link_to(source_link, NodeId(2), minimum_source_size).unwrap(),
            RemoteChannel::for_packet_link(switch_link, minimum_switch_size).unwrap(),
            RemoteChannel::for_packet_link(alternate_switch_link, minimum_alternate_switch_size)
                .unwrap(),
        ],
        initial_events,
        seed,
    };
    add_idle_switches(&mut image, idle_switches);
    image
}

fn pre_split_heterogeneous_image(seed: u64) -> SimulationImage {
    let mut image = heterogeneous_image(seed);
    let post_node_count = image.nodes.len() as u64;
    let pre_node_count = post_node_count - 1;

    let second_port = image.switch_states.remove(1);
    image.switch_states[0].queues.extend(second_port.queues);
    image.nodes = image
        .nodes
        .into_iter()
        .filter_map(|mut node| {
            if node.id == NodeId(2) {
                return None;
            }
            if node.id.0 > 2 {
                node.id.0 -= 1;
            }
            if node.kind == NodeKind::Switch && node.state_slot > 1 {
                node.state_slot -= 1;
            }
            Some(node)
        })
        .collect();

    for link in &mut image.links {
        match link.id {
            LinkId(0) => {
                link.source = NodeId(0);
                link.target = NodeId(1);
            }
            LinkId(1) => {
                link.source = NodeId(1);
                link.target = NodeId(2);
            }
            LinkId(2) => {
                link.source = NodeId(1);
                link.target = NodeId(3);
            }
            LinkId(3) => {
                link.source = NodeId(2);
                link.target = NodeId(0);
            }
            LinkId(4) => {
                link.source = NodeId(3);
                link.target = NodeId(0);
            }
            other => panic!("unexpected heterogeneous-image link {other:?}"),
        }
    }
    for flow in &mut image.flows {
        if flow.target.0 > 2 {
            flow.target.0 -= 1;
        }
    }

    let remap_payload = |payload: PayloadId| {
        let sequence = payload.0 / post_node_count;
        PayloadId(sequence * pre_node_count)
    };
    for packet in &mut image.initial_packets {
        packet.id = remap_payload(packet.id);
    }
    for event in &mut image.initial_events {
        event.payload = remap_payload(event.payload);
    }

    let minimum_by_link =
        image
            .channels
            .iter()
            .fold(BTreeMap::<LinkId, u64>::new(), |mut minimums, channel| {
                minimums
                    .entry(channel.link)
                    .and_modify(|minimum| *minimum = (*minimum).min(channel.min_delay_ns))
                    .or_insert(channel.min_delay_ns);
                minimums
            });
    image.channels = [LinkId(0), LinkId(1), LinkId(2)]
        .into_iter()
        .map(|link_id| {
            let link = image.links[link_id.0 as usize];
            RemoteChannel::for_packet_link(link, minimum_by_link[&link_id]).unwrap()
        })
        .collect();
    image
}

type SemanticPacket = (u64, u8, u64);
type NormalizedQueue = (
    LinkId,
    u64,
    Vec<SemanticPacket>,
    Option<SemanticPacket>,
    bool,
);
type NormalizedPendingEvent = (SemanticPacket, u64, u16, u64);

#[derive(Debug, Eq, PartialEq)]
struct NormalizedPhysicalResult {
    summary: days_executor::RunSummary,
    switch_counters: (u64, u64, u64),
    queues: Vec<NormalizedQueue>,
    departures: Vec<(SemanticPacket, u64)>,
    arrivals: Vec<(SemanticPacket, u64, u8)>,
    pending: Vec<NormalizedPendingEvent>,
}

fn normalized_physical_result(
    result: &days_executor::RunResult,
    post_split: bool,
) -> NormalizedPhysicalResult {
    let packets = result
        .observed_packets
        .iter()
        .chain(&result.resident_packets)
        .map(|packet| (packet.id, *packet))
        .collect::<BTreeMap<_, _>>();
    let semantic_packet = |payload: PayloadId| {
        let packet = packets[&payload];
        (packet.flow.0, packet.kind as u8, packet.size_bytes)
    };
    let physical_target = |target: NodeId| {
        if post_split {
            match target {
                NodeId(0) => 0,
                NodeId(1) | NodeId(2) => 1,
                NodeId(id) => id - 1,
            }
        } else {
            target.0
        }
    };

    let mut queues = result
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .filter_map(|queue| {
            queue.egress_link.map(|link| {
                (
                    link,
                    queue.queue_capacity_packets,
                    queue.queue.iter().copied().map(semantic_packet).collect(),
                    queue.in_service.map(semantic_packet),
                    queue.tx_ready_pending,
                )
            })
        })
        .collect::<Vec<_>>();
    queues.sort_unstable_by_key(|queue| queue.0);

    let mut departures = result
        .departures
        .iter()
        .map(|departure| (semantic_packet(departure.payload), departure.time_ns))
        .collect::<Vec<_>>();
    departures.sort_unstable();
    let mut arrivals = result
        .arrivals
        .iter()
        .map(|arrival| {
            let disposition = match arrival.disposition {
                days_executor::ArrivalDisposition::Admitted => 0,
                days_executor::ArrivalDisposition::Dropped => 1,
                days_executor::ArrivalDisposition::Delivered => 2,
                days_executor::ArrivalDisposition::Feedback => 3,
            };
            (
                semantic_packet(arrival.payload),
                arrival.time_ns,
                disposition,
            )
        })
        .collect::<Vec<_>>();
    arrivals.sort_unstable();
    let mut pending = result
        .pending_events
        .iter()
        .map(|event| {
            (
                semantic_packet(event.payload),
                event.key.time_ns,
                event.kind as u16,
                physical_target(event.target),
            )
        })
        .collect::<Vec<_>>();
    pending.sort_unstable();

    NormalizedPhysicalResult {
        summary: result.summary,
        switch_counters: result.switch_states.iter().fold(
            (0_u64, 0_u64, 0_u64),
            |(arrived, dropped, departed), state| {
                (
                    arrived + state.arrived_packets,
                    dropped + state.dropped_packets,
                    departed + state.departed_packets,
                )
            },
        ),
        queues,
        departures,
        arrivals,
        pending,
    }
}

#[test]
fn port_lps_take_independent_same_time_tx_ready_continuations() {
    let mut image = heterogeneous_image(1);
    image.flows.truncate(4);
    image.initial_packets.truncate(4);
    image.initial_events.clear();

    let queue_a_in_service = image.initial_packets[0].id;
    let queue_b_in_service = image.initial_packets[1].id;
    let queue_a_waiting = image.initial_packets[2].id;
    let queue_b_waiting = image.initial_packets[3].id;
    image.switch_states[0].queues[0].queue = VecDeque::from([queue_a_waiting]);
    image.switch_states[0].queues[0].in_service = Some(queue_a_in_service);
    image.switch_states[0].queues[0].tx_ready_pending = false;
    image.switch_states[0].queues[0].queue_capacity_packets = 1;
    image.switch_states[1].queues[0].queue = VecDeque::from([queue_b_waiting]);
    image.switch_states[1].queues[0].in_service = Some(queue_b_in_service);
    image.switch_states[1].queues[0].tx_ready_pending = false;
    image.switch_states[1].queues[0].queue_capacity_packets = 1;
    image.switch_states[0].next_origin_seq = 1;
    image.switch_states[1].next_origin_seq = 1;
    image.initial_events = vec![
        Event {
            key: EventKey {
                time_ns: 10,
                phase: event_phase(EventKind::TxComplete),
                origin_node: NodeId(1),
                origin_seq: 0,
            },
            target: NodeId(1),
            kind: EventKind::TxComplete,
            payload: queue_a_in_service,
        },
        Event {
            key: EventKey {
                time_ns: 10,
                phase: event_phase(EventKind::TxComplete),
                origin_node: NodeId(2),
                origin_seq: 0,
            },
            target: NodeId(2),
            kind: EventKind::TxComplete,
            payload: queue_b_in_service,
        },
    ];

    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 4 }).unwrap();
    let expected = run_scalar_with_observations(&image, Some(11), ObservationMode::Full).unwrap();
    let scalar =
        run_scalar_rounds_with_observations(&image, Some(11), ObservationMode::Full).unwrap();
    let cpu = [ChunkGranularity::Static, ChunkGranularity::Fixed(1)].map(|granularity| {
        run_cpu_with_observations(
            &image,
            Some(11),
            CpuConfig {
                workers: 4,
                granularity,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap()
    });

    assert_eq!(scalar.result, expected);
    assert!(cpu.iter().all(|run| run.result == expected));
    assert_eq!(
        scalar
            .rounds
            .iter()
            .flat_map(|round| &round.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>(),
        2
    );
    assert!(cpu.iter().all(|run| {
        run.rounds
            .iter()
            .flat_map(|round| &round.semantic.lp_work)
            .map(|work| work.same_time_continuations)
            .sum::<u64>()
            == 2
    }));

    let origin_sequence = |payload, kind| {
        expected
            .pending_events
            .iter()
            .find(|event| event.payload == payload && event.kind == kind)
            .map(|event| event.key.origin_seq)
            .expect("each waiting packet must have one pending transmission event")
    };
    assert_eq!(
        (
            origin_sequence(queue_a_waiting, EventKind::TxComplete),
            origin_sequence(queue_a_waiting, EventKind::RemoteArrival),
            origin_sequence(queue_b_waiting, EventKind::TxComplete),
            origin_sequence(queue_b_waiting, EventKind::RemoteArrival),
        ),
        (2, 3, 2, 3)
    );
}

#[test]
fn one_port_outbox_need_not_be_sorted_by_target_then_event_key() {
    let image = heterogeneous_image(1);
    let mut route_targets = image
        .channels
        .iter()
        .filter(|channel| channel.link == LinkId(0))
        .map(|channel| channel.target)
        .collect::<Vec<_>>();
    route_targets.sort_unstable();
    route_targets.dedup();
    assert_eq!(route_targets, vec![NodeId(1), NodeId(2)]);

    // One source-port LP may serialize a packet for the higher target before a packet for the
    // lower target. Its EventKeys advance, but the exchange's target-first composite key falls.
    let first_key = EventKey {
        time_ns: 100,
        phase: event_phase(EventKind::RemoteArrival),
        origin_node: NodeId(0),
        origin_seq: 10,
    };
    let second_key = EventKey {
        time_ns: 200,
        phase: event_phase(EventKind::RemoteArrival),
        origin_node: NodeId(0),
        origin_seq: 12,
    };
    assert!(first_key < second_key);
    assert!((NodeId(2), first_key) > (NodeId(1), second_key));
}

#[test]
fn port_decomposition_preserves_pre_split_physical_outcomes() {
    for seed in 0..128 {
        let post_split = heterogeneous_image(seed);
        let pre_split = pre_split_heterogeneous_image(seed);
        for horizon in [None, Some(1 + (seed * 17) % post_split.stop_time_ns)] {
            let before =
                run_scalar_with_observations(&pre_split, horizon, ObservationMode::Full).unwrap();
            let after =
                run_scalar_with_observations(&post_split, horizon, ObservationMode::Full).unwrap();
            assert_eq!(
                normalized_physical_result(&after, true),
                normalized_physical_result(&before, false),
                "seed {seed}, horizon {horizon:?}"
            );
        }
    }
}

#[test]
fn randomized_small_heterogeneous_images_match_complete_global_state() {
    for seed in 0..128 {
        let image = heterogeneous_image(seed);
        validate(&image, Backend::Scalar)
            .unwrap_or_else(|error| panic!("seed {seed} scalar validation failed: {error}"));
        validate(&image, Backend::Cpu { workers: 1 })
            .unwrap_or_else(|error| panic!("seed {seed} CPU validation failed: {error}"));
        assert_equivalent(&image, None);
        let cut = 1 + (seed * 17) % image.stop_time_ns;
        assert_equivalent(&image, Some(cut));
    }
}

#[test]
fn cpu_worker_count_granularity_and_straggler_classification_preserve_complete_state() {
    for seed in 0..128 {
        let image = heterogeneous_image(seed);
        let expected = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .unwrap_or_else(|error| panic!("seed {seed} scalar execution failed: {error}"));

        for workers in 1..=4 {
            for granularity in [
                ChunkGranularity::Static,
                ChunkGranularity::Fixed(1),
                ChunkGranularity::Fixed(3),
            ] {
                for straggler_threshold_events in [None, Some(0), Some(3)] {
                    let config = CpuConfig {
                        workers,
                        granularity,
                        straggler_threshold_events,
                        ..CpuConfig::default()
                    };
                    let actual =
                        run_cpu_with_observations(&image, None, config, ObservationMode::Full)
                            .unwrap_or_else(|error| {
                                panic!(
                                    "seed {seed}, workers {workers}, granularity {granularity:?}, \
                                    threshold {straggler_threshold_events:?} failed: {error}"
                                )
                            });
                    let maximum_owner_batches =
                        u64::try_from(workers * workers).expect("worker bound must fit u64");
                    assert!(actual.rounds.iter().all(|round| {
                        round.owner_batch_messages <= maximum_owner_batches
                            && round.owner_batch_messages <= round.semantic.messages_exchanged
                    }));
                    assert_eq!(
                        actual.result, expected,
                        "seed {seed}, workers {workers}, granularity {granularity:?}, \
                         threshold {straggler_threshold_events:?}"
                    );
                }
            }
        }

        let cut = 1 + (seed * 17) % image.stop_time_ns;
        let partial_expected =
            run_scalar_with_observations(&image, Some(cut), ObservationMode::Full).unwrap();
        let partial_actual = run_cpu_with_observations(
            &image,
            Some(cut),
            CpuConfig {
                workers: 4,
                granularity: ChunkGranularity::Fixed(1),
                straggler_threshold_events: Some(3),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(
            partial_actual.result, partial_expected,
            "partial-horizon seed {seed}"
        );
    }
}

fn incast_image(sender_count: usize) -> SimulationImage {
    let sink = NodeId(sender_count as u64);
    let sink_egress = LinkId(sender_count as u64);
    let mut nodes = Vec::new();
    let mut host_states = Vec::new();
    let mut flows = Vec::new();
    let mut initial_packets = Vec::new();
    let mut links = Vec::new();
    let mut channels = Vec::new();
    let mut initial_events = Vec::new();

    for sender in 0..sender_count {
        let sender = NodeId(sender as u64);
        let link = LinkDescriptor {
            id: LinkId(sender.0),
            source: sender,
            target: sink,
            rate_bps: 8_000_000_000,
            propagation_ns: 9,
        };
        let flow = FlowId(sender.0);
        let payload = PayloadId::from_node_sequence(sender, (sender_count + 1) as u64, 0).unwrap();
        nodes.push(NodeDescriptor {
            id: sender,
            kind: NodeKind::Host,
            state_slot: sender.0 as u32,
        });
        host_states.push(HostState {
            egress_link: link.id,
            queue: VecDeque::new(),
            in_service: Some(payload),
            tx_ready_pending: false,
            generators: vec![],
            next_origin_seq: 2,
            next_payload_seq: 0,
            sourced_packets: 1,
            departed_packets: 0,
            received_packets: 0,
        });
        flows.push(FlowDescriptor {
            id: flow,
            source: sender,
            target: sink,
            route: vec![link.id],
            reverse_route: vec![],
        });
        initial_packets.push(PacketDescriptor {
            id: payload,
            flow,
            size_bytes: 1,
            kind: PacketKind::Data,
        });
        links.push(link);
        channels.push(RemoteChannel::for_packet_link(link, 1).unwrap());
        initial_events.push(Event {
            key: EventKey {
                time_ns: 10,
                phase: event_phase(EventKind::TxComplete),
                origin_node: sender,
                origin_seq: 0,
            },
            target: sender,
            kind: EventKind::TxComplete,
            payload,
        });
        initial_events.push(Event {
            key: EventKey {
                time_ns: 19,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: sender,
                origin_seq: 1,
            },
            target: sink,
            kind: EventKind::RemoteArrival,
            payload,
        });
    }
    nodes.push(NodeDescriptor {
        id: sink,
        kind: NodeKind::Host,
        state_slot: sender_count as u32,
    });
    host_states.push(HostState {
        egress_link: sink_egress,
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: vec![],
        next_origin_seq: 0,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    });
    links.push(LinkDescriptor {
        id: sink_egress,
        source: sink,
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    });
    initial_events.sort_unstable_by_key(|event| event.key);

    SimulationImage {
        stop_time_ns: 19,
        nodes,
        host_states,
        switch_states: vec![],
        flows,
        initial_packets,
        links,
        channels,
        initial_events,
        seed: 1,
    }
}

#[test]
fn incast_dominating_lp_is_classified_first_and_routed_to_a_dedicated_worker() {
    let image = incast_image(16);
    validate(&image, Backend::Scalar).unwrap();
    validate(&image, Backend::Cpu { workers: 4 }).unwrap();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let actual = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            straggler_threshold_events: Some(4),
            dedicated_straggler_workers: 1,
            ..CpuConfig::default()
        },
        ObservationMode::Full,
    )
    .unwrap();

    assert_eq!(actual.result, expected);
    assert_eq!(
        actual.rounds[0].partition.stragglers,
        vec![days_executor::LpWorkEstimate {
            node: NodeId(16),
            estimated_events: 16,
        }]
    );
    assert!(!actual.rounds[0].partition.bulk_chunks.is_empty());
    assert_eq!(
        actual.rounds[0].partition.reserved_straggler_workers,
        vec![0]
    );
    let sink = actual.rounds[0]
        .lp_timings
        .iter()
        .find(|timing| timing.node == NodeId(16))
        .unwrap();
    assert_eq!(sink.class, WorkClass::Straggler);
    assert_eq!(sink.worker, 0);
    assert_eq!(sink.dispatch_order, 0);
    assert!(
        actual.rounds[0]
            .partition
            .reserved_straggler_workers
            .contains(&sink.worker)
    );
    assert!(
        actual.rounds[0]
            .lp_timings
            .iter()
            .any(|timing| timing.class == WorkClass::Bulk)
    );
    assert!(
        actual.rounds[0]
            .lp_timings
            .iter()
            .filter(|timing| timing.class == WorkClass::Bulk)
            .all(|timing| !actual.rounds[0]
                .partition
                .reserved_straggler_workers
                .contains(&timing.worker))
    );
    assert!(
        actual.rounds[0]
            .lp_timings
            .iter()
            .filter(|timing| timing.class == WorkClass::Bulk)
            .all(|timing| timing.worker != sink.worker
                && timing.started_after_ns >= sink.started_after_ns)
    );
}

#[test]
fn cpu_worker_faults_and_capacity_errors_abort_without_a_partial_result() {
    let image = image(100, 0, 10);
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
        let error = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                fault_injection: Some(CpuFaultInjection {
                    worker: 0,
                    round: 0,
                    after_events: 1,
                    kind: CpuFaultKind::Failure,
                }),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("an injected worker fault must abort the run");
        assert_eq!(
            error,
            ExecutionError::WorkerFailed {
                worker: 0,
                round: 0,
            },
            "granularity={granularity:?}"
        );

        let capacity_error = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                max_outbox_events_per_lp: Some(0),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("an exhausted LP outbox must abort the run");
        assert_eq!(
            capacity_error,
            ExecutionError::OutboxCapacityExceeded {
                node: SOURCE,
                capacity: 0,
            },
            "granularity={granularity:?}"
        );

        let clean = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("a clean rerun after injected failures must succeed");
        assert_eq!(
            clean.result, expected,
            "clean rerun granularity={granularity:?}"
        );
    }
}

#[test]
fn cpu_panic_root_cause_beats_a_forced_earlier_disconnect() {
    let image = image(100, 0, 10);
    for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
        let error = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                fault_injection: Some(CpuFaultInjection {
                    worker: 0,
                    round: 0,
                    after_events: 1,
                    kind: CpuFaultKind::Panic,
                }),
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("an injected worker panic must abort the run");

        // The injected-panic wrapper queues a disconnect report first to force the reviewed race.
        assert_eq!(
            error,
            ExecutionError::WorkerPanicked { worker: 0 },
            "granularity={granularity:?}"
        );
    }
}

#[test]
fn cpu_arithmetic_overflow_aborts_with_the_scalar_error() {
    let mut image = image(100, 0, 10);
    image.host_states[0].next_origin_seq = u64::MAX;
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect_err("the scalar oracle must reject exhausted origin sequences");

    assert_eq!(scalar, ExecutionError::OriginSequenceOverflow(SOURCE));
    for granularity in [ChunkGranularity::Fixed(1), ChunkGranularity::Static] {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 2,
                granularity,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect_err("the CPU executor must reject exhausted origin sequences");
        assert_eq!(cpu, scalar, "granularity={granularity:?}");
    }
}

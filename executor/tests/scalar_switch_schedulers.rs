use std::collections::VecDeque;

use days_executor::{
    Backend, CpuConfig, DropMarkPolicy, DrrTransitionRecord, EcnThresholdPolicy, Event, EventKey,
    EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId,
    GeneratorFeedbackState, GeneratorStatus, HostState, LinkDescriptor, LinkId,
    MechanismTransitionRecord, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor,
    PacketKind, PayloadId, QueueDepthUnit, RemoteChannel, ScheduledEmission, SchedulerKind,
    SchedulerPacket, SimulationImage, SwitchQueueState, SwitchState, TcpAckHeader,
    TcpCongestionControl, TcpDataHeader, TcpGenerator, TcpReceiverState, TcpTimerState,
    drr_transitions_csv, event_phase, run_cpu_with_observations, run_scalar_with_observations,
    validate, wrr_transitions_csv,
};
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, CudaError, run_cuda_with_observations};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{MetalConfig, MetalError, run_metal_with_observations};
#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
use num_bigint::BigUint;
#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
use num_rational::Ratio;

const SOURCE: NodeId = NodeId(0);
const SWITCH: NodeId = NodeId(1);
const SINK: NodeId = NodeId(2);
const SOURCE_LINK: LinkId = LinkId(0);
const SWITCH_LINK: LinkId = LinkId(1);
const SINK_EGRESS: LinkId = LinkId(2);

fn image(
    scheduler: SchedulerKind,
    packets: &[(u64, u64, u64, u64)],
    stop_time_ns: u64,
) -> SimulationImage {
    let flow_count = packets
        .iter()
        .map(|(_, flow, _, _)| *flow)
        .max()
        .map_or(0, |maximum| maximum + 1);
    let initial_packets = packets
        .iter()
        .map(|(payload, flow, size_bytes, _)| PacketDescriptor {
            id: PayloadId(*payload * 3),
            flow: FlowId(*flow),
            size_bytes: *size_bytes,
            ecn_marked: false,
            kind: PacketKind::Data,
        })
        .collect::<Vec<_>>();
    let mut initial_events = packets
        .iter()
        .enumerate()
        .map(|(origin_seq, (payload, _, _, arrival_ns))| Event {
            key: EventKey {
                time_ns: *arrival_ns,
                phase: event_phase(EventKind::RemoteArrival),
                origin_node: SOURCE,
                origin_seq: origin_seq as u64,
            },
            target: SWITCH,
            kind: EventKind::RemoteArrival,
            payload: PayloadId(*payload * 3),
        })
        .collect::<Vec<_>>();
    initial_events.sort_unstable_by_key(|event| event.key);

    let source_link = LinkDescriptor {
        id: SOURCE_LINK,
        source: SOURCE,
        target: SWITCH,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let switch_link = LinkDescriptor {
        id: SWITCH_LINK,
        source: SWITCH,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };

    SimulationImage {
        stop_time_ns,
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
                next_origin_seq: packets.len() as u64,
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
                scheduler,
                queue_capacity_packets: 0,
                drop_mark: Default::default(),
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
        flows: (0..flow_count)
            .map(|flow| FlowDescriptor {
                id: FlowId(flow),
                source: SOURCE,
                target: SINK,
                priority: 0,
                route: vec![SOURCE_LINK, SWITCH_LINK],
                reverse_route: vec![],
            })
            .collect(),
        initial_packets,
        links: vec![
            source_link,
            switch_link,
            LinkDescriptor {
                id: SINK_EGRESS,
                source: SINK,
                target: SWITCH,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(source_link, 1).expect("source delay must fit"),
            RemoteChannel::for_packet_link(switch_link, 1).expect("switch delay must fit"),
        ],
        initial_events,
        seed: 18,
    }
}

fn switch_departures(image: &SimulationImage) -> Vec<PayloadId> {
    validate(image, Backend::Scalar).expect("scheduler fixture must validate");
    run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scheduler fixture must run")
        .departures
        .into_iter()
        .map(|departure| departure.payload)
        .collect()
}

#[test]
fn sp_arrival_after_ready_is_scheduled_but_before_service_start_is_visible() {
    let image = image(
        SchedulerKind::static_priority(vec![1, 9]),
        &[
            (0, 0, 1, 0), // emits the same-time TxReady after admitting low priority
            (1, 1, 1, 0), // phase-0 arrival interposes before phase-2 service start
        ],
        4,
    );

    assert_eq!(switch_departures(&image), vec![PayloadId(3), PayloadId(0)]);
}

#[test]
fn wfq_arrival_after_ready_is_scheduled_but_before_service_start_is_visible() {
    let image = image(
        SchedulerKind::weighted_fair_queue(vec![1, 8]),
        &[
            (0, 0, 8, 0), // F=64 in rate-normalized virtual-work units
            (1, 1, 1, 0), // F=1 and must be visible to the pending TxReady
        ],
        16,
    );

    assert_eq!(switch_departures(&image), vec![PayloadId(3), PayloadId(0)]);
}

#[test]
fn sp_priority_inversion_edge_is_nonpreemptive_and_fifo_within_equal_priority() {
    let image = image(
        SchedulerKind::static_priority(vec![1, 9, 9]),
        &[(0, 0, 8, 0), (1, 1, 1, 1), (2, 2, 1, 1)],
        16,
    );

    assert_eq!(
        switch_departures(&image),
        vec![PayloadId(0), PayloadId(3), PayloadId(6)],
        "higher-priority arrivals cannot preempt service, and equal priorities remain FIFO"
    );
}

#[test]
fn wfq_positive_weight_class_is_not_starved_by_a_heavier_backlog() {
    let mut packets = (0..16)
        .map(|payload| (payload, 1, 1, 0))
        .collect::<Vec<_>>();
    packets.push((16, 0, 1, 0));
    let image = image(SchedulerKind::weighted_fair_queue(vec![1, 8]), &packets, 32);
    let departures = switch_departures(&image);

    assert_eq!(departures.len(), 17);
    assert_eq!(departures[8], PayloadId(48));
}

#[test]
fn exact_equal_wfq_tags_use_canonical_arrival_order() {
    let image = image(
        SchedulerKind::weighted_fair_queue(vec![1, 2]),
        &[
            (0, 0, 1, 0), // 8 / 1
            (1, 1, 2, 0), // 16 / 2
        ],
        8,
    );

    validate(&image, Backend::Scalar).expect("equal-tag image must validate");
    let partial = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("equal-tag prefix must run");
    let SchedulerKind::WeightedFairQueue(wfq) = &partial.switch_states[0].queues[0].scheduler
    else {
        panic!("fixture must retain WFQ state");
    };
    assert_eq!(wfq.virtual_time.to_string(), "0");
    assert_eq!(
        wfq.finish_times
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["8", "8"]
    );
    assert_eq!(wfq.active_packets, [1, 1]);
    assert_eq!(wfq.packet_finish_times[&PayloadId(0)].to_string(), "8");
    assert_eq!(wfq.packet_finish_times[&PayloadId(3)].to_string(), "8");
    assert_eq!(wfq.packet_finish_times.len(), 2);

    assert_eq!(switch_departures(&image), vec![PayloadId(0), PayloadId(3)]);
}

#[test]
fn wfq_virtual_time_is_fractional_while_active_and_resets_when_idle() {
    let image = image(
        SchedulerKind::weighted_fair_queue(vec![3, 2]),
        &[
            (0, 0, 3, 0), // F = 8; in service from t=0 through t=3
            (1, 1, 1, 1), // V advances by 8 / 3 before this admission
        ],
        8,
    );

    validate(&image, Backend::Scalar).expect("fractional WFQ fixture must validate");
    let partial = run_scalar_with_observations(&image, Some(3), ObservationMode::Full)
        .expect("fractional WFQ prefix must run");
    let SchedulerKind::WeightedFairQueue(wfq) = &partial.switch_states[0].queues[0].scheduler
    else {
        panic!("fixture must retain WFQ state");
    };
    assert_eq!(wfq.virtual_time.to_string(), "8/3");
    assert_eq!(wfq.last_updated_ns, 1);
    assert_eq!(
        wfq.finish_times
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["8", "20/3"]
    );
    assert_eq!(wfq.active_packets, [1, 1]);

    let complete = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("fractional WFQ fixture must complete");
    let SchedulerKind::WeightedFairQueue(wfq) = &complete.switch_states[0].queues[0].scheduler
    else {
        panic!("fixture must retain WFQ state");
    };
    assert_eq!(wfq.virtual_time.to_string(), "0");
    assert_eq!(wfq.active_packets, [0, 0]);
    assert!(wfq.packet_finish_times.is_empty());
}

#[test]
fn drr_accumulates_exact_byte_quanta_across_wraps() {
    let image = image(
        SchedulerKind::deficit_round_robin(vec![2, 1]),
        &[(0, 0, 3, 0), (1, 1, 1, 0)],
        8,
    );

    assert_eq!(switch_departures(&image), vec![PayloadId(3), PayloadId(0)]);
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let SchedulerKind::DeficitRoundRobin(state) = &result.switch_states[0].queues[0].scheduler
    else {
        panic!("fixture must retain DRR state");
    };
    assert_eq!(state.deficits_bytes, [1, 0]);
    assert_eq!(state.current_class, 0);
}

#[test]
fn wrr_uses_exact_packet_weights_and_fifo_within_class() {
    let image = image(
        SchedulerKind::weighted_round_robin(vec![2, 1]),
        &[(0, 0, 1, 0), (1, 1, 1, 0), (2, 0, 1, 0), (3, 1, 1, 0)],
        8,
    );

    assert_eq!(
        switch_departures(&image),
        vec![PayloadId(0), PayloadId(6), PayloadId(3), PayloadId(9)]
    );
}

#[test]
fn drr_and_wrr_certificates_are_generated_by_the_scalar_oracle() {
    let drr = image(
        SchedulerKind::deficit_round_robin(vec![2, 1]),
        &[(0, 0, 3, 0), (1, 1, 1, 0)],
        8,
    );
    let drr_result = run_scalar_with_observations(&drr, None, ObservationMode::Full).unwrap();
    assert_eq!(
        drr_transitions_csv(
            &drr_result
                .diagnostics
                .as_ref()
                .unwrap()
                .mechanism_transitions,
        )
        .unwrap(),
        include_str!("../../lean/fixtures/p10c/drr_executor_accept.csv")
    );

    let wrr = image(
        SchedulerKind::weighted_round_robin(vec![2, 1]),
        &[(0, 0, 1, 0), (1, 1, 1, 0), (2, 0, 1, 0), (3, 1, 1, 0)],
        8,
    );
    let wrr_result = run_scalar_with_observations(&wrr, None, ObservationMode::Full).unwrap();
    assert_eq!(
        wrr_transitions_csv(
            &wrr_result
                .diagnostics
                .as_ref()
                .unwrap()
                .mechanism_transitions,
        )
        .unwrap(),
        include_str!("../../lean/fixtures/p10c/wrr_executor_accept.csv")
    );
}

#[test]
fn drr_and_wrr_are_byte_identical_on_scalar_and_cpu() {
    for scheduler in [
        SchedulerKind::deficit_round_robin(vec![2, 1]),
        SchedulerKind::weighted_round_robin(vec![2, 1]),
    ] {
        let image = image(
            scheduler,
            &[(0, 0, 3, 0), (1, 1, 1, 0), (2, 0, 1, 0), (3, 1, 2, 0)],
            16,
        );
        let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
        for workers in [1, 2, 4] {
            validate(&image, Backend::Cpu { workers }).unwrap();
            let actual = run_cpu_with_observations(
                &image,
                None,
                CpuConfig {
                    workers,
                    ..CpuConfig::default()
                },
                ObservationMode::Full,
            )
            .unwrap();
            assert_eq!(actual.result, expected);
        }
    }
}

#[test]
fn devices_accept_valid_drr_and_wrr_state() {
    for scheduler in [
        SchedulerKind::deficit_round_robin(vec![1]),
        SchedulerKind::weighted_round_robin(vec![1]),
    ] {
        let image = image(scheduler, &[(0, 0, 1, 0)], 4);
        for backend in [Backend::Metal, Backend::Cuda] {
            validate(&image, backend).unwrap_or_else(|error| {
                panic!("{backend} must accept the device scheduler state: {error}")
            });
        }
    }
}

#[test]
fn validator_rejects_drr_state_whose_next_quantum_addition_overflows() {
    let mut image = image(
        SchedulerKind::deficit_round_robin(vec![u64::MAX]),
        &[(0, 0, 2, 0)],
        8,
    );
    let SchedulerKind::DeficitRoundRobin(state) = &mut image.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.deficits_bytes[0] = 1;
    image.initial_events.clear();
    image.switch_states[0].queues[0]
        .queue
        .push_back(PayloadId(0));

    let error = validate(&image, Backend::Scalar)
        .expect_err("the next DRR quantum addition would overflow")
        .to_string();
    assert!(
        error.contains("DRR class 0 cannot accumulate enough deficit"),
        "expected a DRR closure diagnostic, got: {error}"
    );
}

#[test]
fn validator_reserves_drr_deficit_for_future_arrivals() {
    let mut image = image(
        SchedulerKind::deficit_round_robin(vec![u64::MAX]),
        &[(0, 0, 2, 0)],
        8,
    );
    let SchedulerKind::DeficitRoundRobin(state) = &mut image.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.deficits_bytes[0] = 1;

    let error = validate(&image, Backend::Scalar)
        .expect_err("the future arrival's DRR quantum addition would overflow")
        .to_string();
    assert!(
        error.contains("DRR class 0 cannot accumulate enough deficit"),
        "expected a future-arrival DRR closure diagnostic, got: {error}"
    );
}

fn drr_future_tcp_image() -> SimulationImage {
    let mut image = image(
        SchedulerKind::deficit_round_robin(vec![u64::MAX]),
        &[(0, 0, 1, 0)],
        10,
    );
    let reverse = &mut image.links[SINK_EGRESS.0 as usize];
    reverse.target = SOURCE;
    image
        .channels
        .push(RemoteChannel::for_packet_link(*reverse, 1).expect("one-byte ACK delay must fit"));
    image.flows.push(FlowDescriptor {
        id: FlowId(1),
        source: SOURCE,
        target: SINK,
        priority: 0,
        route: vec![SOURCE_LINK, SWITCH_LINK],
        reverse_route: vec![],
    });
    image.flows.push(FlowDescriptor {
        id: FlowId(2),
        source: SOURCE,
        target: SINK,
        priority: 0,
        route: vec![SOURCE_LINK, SWITCH_LINK],
        reverse_route: vec![SINK_EGRESS],
    });

    let prior_attempt = PacketDescriptor {
        id: PayloadId(3),
        flow: FlowId(2),
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::TcpData(TcpDataHeader {
            sequence: 0,
            sent_time_ns: 0,
            retransmission: false,
        }),
    };
    let ack = PacketDescriptor {
        id: PayloadId(2),
        flow: FlowId(2),
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::TcpAck(TcpAckHeader {
            acknowledgment: 1,
            acknowledged_bytes: 1,
            echoed_sent_time_ns: 0,
        }),
    };
    image.initial_packets.extend([ack, prior_attempt]);
    image
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    image.initial_events.extend([
        Event {
            key: EventKey {
                time_ns: 1,
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
                time_ns: 100,
                phase: event_phase(EventKind::RetransmissionTimeout),
                origin_node: SOURCE,
                origin_seq: 1,
            },
            target: SOURCE,
            kind: EventKind::RetransmissionTimeout,
            payload: prior_attempt.id,
        },
    ]);
    image.initial_events.sort_unstable_by_key(|event| event.key);

    let mut tcp = TcpGenerator::new(4, 2, 1, TcpCongestionControl::reno(2));
    tcp.next_sequence = 1;
    tcp.bytes_in_flight = 1;
    tcp.last_attempt = prior_attempt.id;
    tcp.timer_generation = 1;
    tcp.active_timer = Some(TcpTimerState {
        attempt: prior_attempt.id,
        sequence: 0,
        deadline_ns: 100,
        generation: 1,
        rto_ns: 100,
    });
    tcp.rto_ns = 100;
    image.host_states[0].generators.push(FlowGeneratorState {
        flow: FlowId(2),
        packets_emitted: 1,
        bytes_emitted: 1,
        next_emission: ScheduledEmission {
            status: GeneratorStatus::Blocked,
            departure_time_ns: 0,
            payload: prior_attempt.id,
        },
        rng_state: 19,
        feedback: GeneratorFeedbackState {
            arrivals: 0,
            outstanding_bytes: 1,
            unacknowledged_bytes: 1,
        },
        kind: FlowGeneratorKind::Tcp(tcp),
    });
    image.host_states[0].next_origin_seq = 2;
    image.host_states[0].next_payload_seq = 2;
    image.host_states[0].sourced_packets = 1;
    image.host_states[0].departed_packets = 1;
    image.host_states[1].tcp_receivers = vec![TcpReceiverState {
        flow: FlowId(2),
        ack_size_bytes: 1,
        next_expected_sequence: 1,
        out_of_order: vec![],
    }];
    image.host_states[1].next_origin_seq = 1;
    image.host_states[1].next_payload_seq = 1;
    image.host_states[1].sourced_packets = 1;
    image.host_states[1].departed_packets = 1;
    image.host_states[1].received_packets = 1;
    let SchedulerKind::DeficitRoundRobin(state) = &mut image.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.deficits_bytes[0] = 2;
    image
}

#[test]
fn validator_reserves_drr_deficit_for_larger_future_tcp_segments() {
    let image = drr_future_tcp_image();
    match validate(&image, Backend::Scalar) {
        Ok(()) => {
            run_scalar_with_observations(&image, None, ObservationMode::Full)
                .expect("an accepted future TCP segment must not fault DRR accumulation");
        }
        Err(error) => assert!(
            error
                .to_string()
                .contains("DRR class 0 cannot accumulate enough deficit"),
            "expected a DRR future-TCP closure diagnostic, got: {error}"
        ),
    }
}

#[test]
fn validator_does_not_reserve_drr_for_a_packet_already_past_the_egress() {
    let mut image = image(
        SchedulerKind::deficit_round_robin(vec![u64::MAX]),
        &[(0, 0, 2, 0)],
        4,
    );
    let event = &mut image.initial_events[0];
    event.key.origin_node = SWITCH;
    event.target = SINK;
    image.switch_states[0].next_origin_seq = 1;

    validate(&image, Backend::Scalar)
        .expect("a terminal arrival cannot return to the scheduler egress it has crossed");
}

#[test]
fn drr_trace_scan_steps_are_wider_than_u64() {
    let beyond_u64 = u128::from(u64::MAX) + 1;
    let csv = drr_transitions_csv(&[MechanismTransitionRecord::Drr(DrrTransitionRecord {
        key: EventKey {
            time_ns: 0,
            phase: 2,
            origin_node: SWITCH,
            origin_seq: 0,
        },
        node: SWITCH,
        queue_id: 0,
        class_count: 1,
        quanta_bytes: vec![1],
        before_deficits_bytes: vec![0],
        before_current_class: 0,
        scan_steps: beyond_u64,
        eligible_packets: vec![SchedulerPacket {
            payload: PayloadId(0),
            flow: FlowId(0),
            size_bytes: 1,
        }],
        selected_payload: PayloadId(0),
        after_deficits_bytes: vec![0],
        after_current_class: 0,
    })])
    .expect("one DRR transition has a unique key");

    assert_eq!(
        csv.lines().nth(1).and_then(|row| row.split(',').nth(10)),
        Some("18446744073709551616")
    );
}

#[test]
fn byte_capacity_overflow_drops_before_unneeded_post_sum() {
    let mut image = image(SchedulerKind::Fifo, &[(0, 0, u64::MAX, 0), (1, 1, 1, 0)], 4);
    image.links[0].rate_bps = u64::MAX;
    image.links[1].rate_bps = u64::MAX;
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 1).unwrap();
    image.channels[1] = RemoteChannel::for_packet_link(image.links[1], 1).unwrap();
    let queue = &mut image.switch_states[0].queues[0];
    queue.drop_mark = DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
        unit: QueueDepthUnit::Bytes,
        capacity: u64::MAX,
        threshold: u64::MAX,
    });
    queue.queue.push_back(PayloadId(0));
    queue.tx_ready_pending = true;

    image
        .initial_events
        .retain(|event| event.payload == PayloadId(3));
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::TxReady),
            origin_node: SWITCH,
            origin_seq: 0,
        },
        target: SWITCH,
        kind: EventKind::TxReady,
        payload: PayloadId(0),
    });
    image.initial_events.sort_unstable_by_key(|event| event.key);
    image.switch_states[0].next_origin_seq = 1;

    validate(&image, Backend::Scalar).expect("the exact byte-capacity boundary must validate");
    let result = run_scalar_with_observations(&image, Some(1), ObservationMode::Full)
        .expect("the overflowing post-byte sum is a capacity drop, not an execution fault");
    assert_eq!(result.summary.dropped_packets, 1);
    assert!(
        result
            .resident_packets
            .iter()
            .all(|packet| packet.id != PayloadId(3))
    );
}

#[test]
fn cpu_is_byte_identical_to_scalar_for_sp_and_wfq() {
    for scheduler in [
        SchedulerKind::static_priority(vec![1, 3, 2]),
        SchedulerKind::weighted_fair_queue(vec![1, 3, 2]),
    ] {
        let image = image(scheduler, &[(0, 0, 3, 0), (1, 1, 2, 0), (2, 2, 1, 0)], 16);
        validate(&image, Backend::Cpu { workers: 3 }).expect("CPU must accept the discipline");
        let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("scalar oracle must run");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers: 3,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CPU executor must run");
        assert_eq!(cpu.result, scalar);
    }
}

fn adversarial_scheduler_images() -> [SimulationImage; 3] {
    [
        image(
            SchedulerKind::static_priority(vec![1, 9]),
            &[(0, 0, 1, 0), (1, 1, 1, 0)],
            4,
        ),
        image(
            SchedulerKind::weighted_fair_queue(vec![1, 8]),
            &[(0, 0, 8, 0), (1, 1, 1, 0)],
            16,
        ),
        image(
            SchedulerKind::weighted_fair_queue(vec![1, 2]),
            &[(0, 0, 1, 0), (1, 1, 2, 0)],
            8,
        ),
    ]
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn adversarial_drr_wrr_images() -> [SimulationImage; 3] {
    let mut drr_wrap = image(
        SchedulerKind::deficit_round_robin(vec![2, 7, 2]),
        &[(0, 2, 5, 0), (1, 0, 2, 0)],
        16,
    );
    let SchedulerKind::DeficitRoundRobin(state) =
        &mut drr_wrap.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.deficits_bytes = vec![0, 6, 0];
    state.current_class = 2;

    let mut drr_skip = image(
        SchedulerKind::deficit_round_robin(vec![1, 3]),
        &[(0, 0, 1_000_000, 0), (1, 1, 5, 0)],
        1_000_016,
    );
    let SchedulerKind::DeficitRoundRobin(state) =
        &mut drr_skip.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.current_class = 1;

    let mut wrr = image(
        SchedulerKind::weighted_round_robin(vec![2, 1, 3]),
        &[(0, 2, 1, 0), (1, 1, 1, 0), (2, 2, 1, 0)],
        16,
    );
    let SchedulerKind::WeightedRoundRobin(state) = &mut wrr.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.packets_sent_in_round = vec![2, 0, 2];
    state.current_class = 0;

    [drr_wrap, drr_skip, wrr]
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn drr_legal_maximum_image() -> SimulationImage {
    let mut image = image(
        SchedulerKind::deficit_round_robin(vec![1]),
        &[(0, 0, u64::MAX, 0)],
        1,
    );
    image.links[0].rate_bps = u64::MAX;
    image.links[1].rate_bps = u64::MAX;
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 1).unwrap();
    image.channels[1] = RemoteChannel::for_packet_link(image.links[1], 1).unwrap();
    image
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn wrr_legal_maximum_image() -> SimulationImage {
    let mut image = image(
        SchedulerKind::weighted_round_robin(vec![u64::MAX]),
        &[(0, 0, 1, 0)],
        1,
    );
    let SchedulerKind::WeightedRoundRobin(state) = &mut image.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    state.packets_sent_in_round[0] = u64::MAX - 1;
    image
}

#[test]
fn cpu_is_byte_identical_for_adversarial_sp_wfq_and_in_service_checkpoints() {
    for image in adversarial_scheduler_images() {
        for horizon in [Some(1), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
            for workers in [1, 2, 4] {
                let actual = run_cpu_with_observations(
                    &image,
                    horizon,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    ObservationMode::Full,
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "{} horizon={horizon:?} workers={workers}: {error}",
                        image.switch_states[0].queues[0].scheduler.label()
                    )
                });
                assert_eq!(actual.result, expected);
            }
        }
    }
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn wfq_multilimb_checkpoint() -> (SimulationImage, Ratio<BigUint>) {
    let mut checkpoint = image(
        SchedulerKind::weighted_fair_queue(vec![1_000_000_000, 2_000_000_000]),
        &[(0, 1, 1, 0), (1, 0, 1, 0), (2, 0, 1, 1)],
        32,
    );
    let prefix = run_scalar_with_observations(&checkpoint, Some(1), ObservationMode::Full)
        .expect("WFQ multi-limb checkpoint prefix must run");
    checkpoint.host_states = prefix.host_states;
    checkpoint.switch_states = prefix.switch_states;
    checkpoint.initial_packets = prefix.resident_packets;
    checkpoint.initial_events = prefix.pending_events;

    let denominator = BigUint::from(125_000_000_u64) << 288_usize;
    let numerator = &denominator * BigUint::from(15_u8) + BigUint::from(1_u8);
    let high_finish = Ratio::new(numerator, denominator);
    let expected_finish =
        high_finish.clone() + Ratio::new(BigUint::from(8_u8), BigUint::from(1_000_000_000_u64));
    let queue = &mut checkpoint.switch_states[0].queues[0];
    let waiting = *queue
        .queue
        .front()
        .expect("prefix must retain the high-weight waiting packet");
    let SchedulerKind::WeightedFairQueue(state) = &mut queue.scheduler else {
        unreachable!()
    };
    state.finish_times[0] = high_finish.clone();
    state.packet_finish_times.insert(waiting, high_finish);
    (checkpoint, expected_finish)
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn wfq_overflow_checkpoint() -> SimulationImage {
    let mut checkpoint = image(
        SchedulerKind::weighted_fair_queue(vec![1]),
        &[(0, 0, 1, 0)],
        4,
    );
    let prefix = run_scalar_with_observations(&checkpoint, Some(1), ObservationMode::Full)
        .expect("WFQ overflow checkpoint prefix must run");
    checkpoint.host_states = prefix.host_states;
    checkpoint.switch_states = prefix.switch_states;
    checkpoint.initial_packets = prefix.resident_packets;
    checkpoint.initial_events = prefix.pending_events;

    let payload = checkpoint.switch_states[0].queues[0]
        .in_service
        .expect("prefix must retain one in-service packet");
    let SchedulerKind::WeightedFairQueue(state) =
        &mut checkpoint.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    let maximum = Ratio::from_integer((BigUint::from(1_u8) << 320_usize) - 1_u8);
    state.virtual_time = maximum.clone();
    state.finish_times[0] = maximum.clone();
    state.packet_finish_times.insert(payload, maximum);
    checkpoint
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_is_byte_identical_for_adversarial_sp_wfq_and_in_service_checkpoints() {
    for image in adversarial_scheduler_images() {
        for horizon in [Some(1), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
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
                            "{} horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_threadgroup}: {error}",
                            image.switch_states[0].queues[0].scheduler.label()
                        )
                    });
                    assert_eq!(actual.result, expected);
                }
            }
        }
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_drr_wrr_service_start_and_checkpoint_state_match_scalar() {
    for image in adversarial_drr_wrr_images() {
        for horizon in [Some(1), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Summary).unwrap();
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
                        ObservationMode::Summary,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "{} horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_threadgroup}: {error}",
                            image.switch_states[0].queues[0].scheduler.label()
                        )
                    });
                    assert_eq!(actual.result, expected);
                }
            }
        }
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_drr_skips_the_exact_legal_maximum_round_count() {
    let image = drr_legal_maximum_image();
    validate(&image, Backend::Metal).expect("the maximum representable DRR round count is legal");
    let result = run_metal_with_observations(
        &image,
        Some(1),
        MetalConfig::default(),
        ObservationMode::Summary,
    )
    .expect("the exact analytical skip must not iterate once per deficit round")
    .result;
    assert_eq!(
        result.switch_states[0].queues[0].in_service,
        Some(PayloadId(0))
    );
    let SchedulerKind::DeficitRoundRobin(state) = &result.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    assert_eq!(state.deficits_bytes, [0]);
    assert_eq!(state.current_class, 0);
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_wrr_restores_the_maximum_legal_counter() {
    let image = wrr_legal_maximum_image();
    validate(&image, Backend::Metal).expect("the maximum representable WRR counter is legal");
    let result = run_metal_with_observations(
        &image,
        Some(1),
        MetalConfig::default(),
        ObservationMode::Summary,
    )
    .expect("the maximum legal WRR counter must not wrap")
    .result;
    let SchedulerKind::WeightedRoundRobin(state) = &result.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    assert_eq!(state.packets_sent_in_round, [u64::MAX]);
    assert_eq!(state.current_class, 0);
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_wfq_overflow_faults_instead_of_wrapping() {
    let image = wfq_overflow_checkpoint();
    validate(&image, Backend::Metal).expect("320-bit checkpoint must pass Metal pre-screening");
    for streams_enabled in [true, false] {
        assert_eq!(
            run_metal_with_observations(
                &image,
                None,
                MetalConfig {
                    streams_enabled,
                    ..MetalConfig::default()
                },
                ObservationMode::Full,
            )
            .expect_err("the exact reduced virtual time needs 321 bits"),
            MetalError::WfqArithmeticOverflow { node: SWITCH }
        );
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_wfq_multilimb_cross_cancel_and_comparison_match_scalar() {
    let (image, expected_finish) = wfq_multilimb_checkpoint();
    validate(&image, Backend::Metal).expect("320-bit multi-limb checkpoint must validate");
    let scalar_prefix =
        run_scalar_with_observations(&image, Some(2), ObservationMode::Full).unwrap();
    let SchedulerKind::WeightedFairQueue(state) =
        &scalar_prefix.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    assert_eq!(state.finish_times[0], expected_finish);
    assert!(state.finish_times[0].numer().bits() >= 319);
    assert!(state.finish_times[0].denom().bits() >= 315);
    for horizon in [Some(2), None] {
        let expected =
            run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
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
                .unwrap();
                assert_eq!(actual.result, expected);
            }
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_is_byte_identical_for_adversarial_sp_wfq_and_in_service_checkpoints() {
    for image in adversarial_scheduler_images() {
        for horizon in [Some(1), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
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
                            "{} horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_block}: {error}",
                            image.switch_states[0].queues[0].scheduler.label()
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
fn cuda_drr_wrr_service_start_and_checkpoint_state_match_scalar() {
    for image in adversarial_drr_wrr_images() {
        for horizon in [Some(1), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Summary).unwrap();
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
                        ObservationMode::Summary,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "{} horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_block}: {error}",
                            image.switch_states[0].queues[0].scheduler.label()
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
fn cuda_drr_skips_the_exact_legal_maximum_round_count() {
    let image = drr_legal_maximum_image();
    validate(&image, Backend::Cuda).expect("the maximum representable DRR round count is legal");
    let result = run_cuda_with_observations(
        &image,
        Some(1),
        CudaConfig::default(),
        ObservationMode::Summary,
    )
    .expect("the exact analytical skip must not iterate once per deficit round")
    .result;
    assert_eq!(
        result.switch_states[0].queues[0].in_service,
        Some(PayloadId(0))
    );
    let SchedulerKind::DeficitRoundRobin(state) = &result.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    assert_eq!(state.deficits_bytes, [0]);
    assert_eq!(state.current_class, 0);
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_wrr_restores_the_maximum_legal_counter() {
    let image = wrr_legal_maximum_image();
    validate(&image, Backend::Cuda).expect("the maximum representable WRR counter is legal");
    let result = run_cuda_with_observations(
        &image,
        Some(1),
        CudaConfig::default(),
        ObservationMode::Summary,
    )
    .expect("the maximum legal WRR counter must not wrap")
    .result;
    let SchedulerKind::WeightedRoundRobin(state) = &result.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    assert_eq!(state.packets_sent_in_round, [u64::MAX]);
    assert_eq!(state.current_class, 0);
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_wfq_overflow_faults_instead_of_wrapping() {
    let image = wfq_overflow_checkpoint();
    validate(&image, Backend::Cuda).expect("320-bit checkpoint must pass CUDA pre-screening");
    for streams_enabled in [true, false] {
        assert_eq!(
            run_cuda_with_observations(
                &image,
                None,
                CudaConfig {
                    streams_enabled,
                    ..CudaConfig::default()
                },
                ObservationMode::Full,
            )
            .expect_err("the exact reduced virtual time needs 321 bits"),
            CudaError::WfqArithmeticOverflow { node: SWITCH }
        );
    }
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_wfq_multilimb_cross_cancel_and_comparison_match_scalar() {
    let (image, expected_finish) = wfq_multilimb_checkpoint();
    validate(&image, Backend::Cuda).expect("320-bit multi-limb checkpoint must validate");
    let scalar_prefix =
        run_scalar_with_observations(&image, Some(2), ObservationMode::Full).unwrap();
    let SchedulerKind::WeightedFairQueue(state) =
        &scalar_prefix.switch_states[0].queues[0].scheduler
    else {
        unreachable!()
    };
    assert_eq!(state.finish_times[0], expected_finish);
    assert!(state.finish_times[0].numer().bits() >= 319);
    assert!(state.finish_times[0].denom().bits() >= 315);
    for horizon in [Some(2), None] {
        let expected =
            run_scalar_with_observations(&image, horizon, ObservationMode::Full).unwrap();
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
                .unwrap();
                assert_eq!(actual.result, expected);
            }
        }
    }
}

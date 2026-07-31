use std::collections::VecDeque;

use days_executor::{
    Backend, CpuConfig, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor,
    PacketKind, PayloadId, RemoteChannel, SchedulerKind, SimulationImage, SwitchQueueState,
    SwitchState, event_phase, run_cpu_with_observations, run_scalar_with_observations, validate,
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

use std::collections::{BTreeSet, VecDeque};

use days_executor::{
    Backend, CpuConfig, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor,
    PacketKind, PayloadId, PfcHeader, PfcIngressState, PfcQueueState, RemoteChannel, RunResult,
    SchedulerKind, SimulationImage, SwitchQueueState, SwitchState, event_phase,
    run_cpu_with_observations, run_scalar_with_observations, validate,
};

const SOURCE: NodeId = NodeId(0);
const UPSTREAM: NodeId = NodeId(1);
const DOWNSTREAM: NodeId = NodeId(2);
const SINK: NodeId = NodeId(3);

const SOURCE_LINK: LinkId = LinkId(0);
const CONTROLLED_LINK: LinkId = LinkId(1);
const EGRESS_LINK: LinkId = LinkId(2);
const CONTROL_LINK: LinkId = LinkId(3);
const SINK_EGRESS: LinkId = LinkId(4);

const FLOW: FlowId = FlowId(0);
const PRIORITY: u8 = 3;
const DATA_0: PayloadId = PayloadId(0);
const DATA_1: PayloadId = PayloadId(4);

fn thresholds(xoff: u64, xon: u64) -> ([u64; 8], [u64; 8]) {
    let mut xoff_threshold_bytes = [0; 8];
    let mut xon_threshold_bytes = [0; 8];
    xoff_threshold_bytes[usize::from(PRIORITY)] = xoff;
    xon_threshold_bytes[usize::from(PRIORITY)] = xon;
    (xoff_threshold_bytes, xon_threshold_bytes)
}

fn host_state(egress_link: LinkId) -> HostState {
    HostState {
        egress_link,
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
    }
}

fn pfc_queue(ingress: Option<PfcIngressState>) -> PfcQueueState {
    PfcQueueState {
        paused_priorities: [false; 8],
        ingresses: ingress.into_iter().collect(),
    }
}

fn switch_state(
    physical_switch: u64,
    egress_link: LinkId,
    pfc: Option<PfcQueueState>,
) -> SwitchState {
    SwitchState {
        physical_switch,
        queues: vec![SwitchQueueState {
            egress_link: Some(egress_link),
            scheduler: SchedulerKind::Fifo,
            queue_capacity_packets: 0,
            drop_mark: Default::default(),
            pfc,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
        }],
        next_origin_seq: 0,
        arrived_packets: 0,
        dropped_packets: 0,
        departed_packets: 0,
    }
}

fn data_packet(id: PayloadId, size_bytes: u64) -> PacketDescriptor {
    PacketDescriptor {
        id,
        flow: FLOW,
        size_bytes,
        ecn_marked: false,
        kind: PacketKind::Data,
    }
}

fn control_packet(id: PayloadId, pause: bool) -> PacketDescriptor {
    PacketDescriptor {
        id,
        flow: FLOW,
        size_bytes: 64,
        ecn_marked: false,
        kind: PacketKind::Pfc(PfcHeader {
            controlled_link: CONTROLLED_LINK,
            priority: PRIORITY,
            pause,
        }),
    }
}

fn path_image() -> SimulationImage {
    let source_link = LinkDescriptor {
        id: SOURCE_LINK,
        source: SOURCE,
        target: UPSTREAM,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let controlled_link = LinkDescriptor {
        id: CONTROLLED_LINK,
        source: UPSTREAM,
        target: DOWNSTREAM,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let egress_link = LinkDescriptor {
        id: EGRESS_LINK,
        source: DOWNSTREAM,
        target: SINK,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let control_link = LinkDescriptor {
        id: CONTROL_LINK,
        source: DOWNSTREAM,
        target: UPSTREAM,
        rate_bps: 100_000_000_000,
        propagation_ns: 0,
    };
    let sink_egress = LinkDescriptor {
        id: SINK_EGRESS,
        source: SINK,
        target: DOWNSTREAM,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let (xoff_threshold_bytes, xon_threshold_bytes) = thresholds(1_000, 500);

    SimulationImage {
        stop_time_ns: 100,
        nodes: vec![
            NodeDescriptor {
                id: SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: UPSTREAM,
                kind: NodeKind::Switch,
                state_slot: 0,
            },
            NodeDescriptor {
                id: DOWNSTREAM,
                kind: NodeKind::Switch,
                state_slot: 1,
            },
            NodeDescriptor {
                id: SINK,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![host_state(SOURCE_LINK), host_state(SINK_EGRESS)],
        switch_states: vec![
            switch_state(10, CONTROLLED_LINK, Some(pfc_queue(None))),
            switch_state(
                20,
                EGRESS_LINK,
                Some(pfc_queue(Some(PfcIngressState {
                    controlled_link: CONTROLLED_LINK,
                    control_channel_index: 3,
                    buffer_capacity_bytes: {
                        let mut capacity = [0; 8];
                        capacity[usize::from(PRIORITY)] = 20_000;
                        capacity
                    },
                    max_frame_bytes: 1_000,
                    xoff_threshold_bytes,
                    xon_threshold_bytes,
                    occupancy_bytes: [0; 8],
                    pause_asserted: [false; 8],
                }))),
            ),
        ],
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            priority: PRIORITY,
            route: vec![SOURCE_LINK, CONTROLLED_LINK, EGRESS_LINK],
            reverse_route: vec![],
        }],
        initial_packets: vec![data_packet(DATA_0, 1)],
        links: vec![
            source_link,
            controlled_link,
            egress_link,
            control_link,
            sink_egress,
        ],
        channels: vec![
            RemoteChannel::for_packet_link(source_link, 1).expect("source delay must fit"),
            RemoteChannel::for_packet_link(controlled_link, 1)
                .expect("controlled-link delay must fit"),
            RemoteChannel::for_packet_link(egress_link, 1).expect("egress delay must fit"),
            RemoteChannel {
                source: DOWNSTREAM,
                target: UPSTREAM,
                link: CONTROL_LINK,
                event_kind: EventKind::RemoteArrival,
                min_delay_ns: control_link.delay_ns(64).expect("PFC delay must fit"),
            },
        ],
        initial_events: vec![],
        seed: 25,
    }
}

fn add_control(image: &mut SimulationImage, time_ns: u64, origin_seq: u64, pause: bool) {
    let payload = PayloadId::from_node_sequence(DOWNSTREAM, 4, origin_seq)
        .expect("small PFC fixture identity must fit");
    image.initial_packets.push(control_packet(payload, pause));
    image
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: DOWNSTREAM,
            origin_seq,
        },
        target: UPSTREAM,
        kind: EventKind::RemoteArrival,
        payload,
    });
    image.switch_states[1].next_origin_seq = origin_seq + 1;
    image.initial_events.sort_unstable_by_key(|event| event.key);
}

fn run_scalar_cpu(image: &SimulationImage) -> RunResult {
    validate(image, Backend::Scalar).expect("scalar PFC fixture must validate");
    let scalar = run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar PFC fixture must execute");
    for workers in [1, 2, 4] {
        validate(image, Backend::Cpu { workers }).expect("CPU PFC fixture must validate");
        let cpu = run_cpu_with_observations(
            image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("CPU PFC fixture failed with {workers} workers: {error}"));
        assert_eq!(cpu.result, scalar, "PFC worker count {workers}");
    }
    scalar
}

#[test]
fn devices_reject_pfc_before_packing() {
    let image = path_image();
    for backend in [Backend::Metal, Backend::Cuda] {
        let error = validate(&image, backend).unwrap_err().to_string();
        assert!(
            error.contains("does not support PFC per-priority link pause"),
            "{backend}: {error}"
        );
    }
}

fn upstream_pfc(result: &RunResult) -> &PfcQueueState {
    result.switch_states[0].queues[0]
        .pfc
        .as_ref()
        .expect("upstream queue must retain PFC state")
}

#[test]
fn pause_during_service_completes_in_service_and_blocks_the_next_priority() {
    let mut image = path_image();
    image.stop_time_ns = 10;
    image.initial_packets = vec![data_packet(DATA_0, 100), data_packet(DATA_1, 100)];
    image.switch_states[0].queues[0].in_service = Some(DATA_0);
    image.switch_states[0].queues[0].queue.push_back(DATA_1);
    image.switch_states[0].next_origin_seq = 1;
    add_control(&mut image, 5, 0, true);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 10,
            phase: event_phase(EventKind::TxComplete),
            origin_node: UPSTREAM,
            origin_seq: 0,
        },
        target: UPSTREAM,
        kind: EventKind::TxComplete,
        payload: DATA_0,
    });
    image.initial_events.sort_unstable_by_key(|event| event.key);

    let result = run_scalar_cpu(&image);
    let queue = &result.switch_states[0].queues[0];

    assert_eq!(queue.in_service, None, "the committed packet must complete");
    assert_eq!(queue.queue, VecDeque::from([DATA_1]));
    assert!(!queue.tx_ready_pending);
    assert!(upstream_pfc(&result).paused_priorities[usize::from(PRIORITY)]);
    assert_eq!(result.departures.len(), 1);
    assert_eq!(result.departures[0].payload, DATA_0);
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.kind != EventKind::TxReady),
        "a paused waiting packet must not own TxReady"
    );
}

#[test]
fn duplicate_pause_is_an_idempotent_state_assignment() {
    let mut one = path_image();
    one.stop_time_ns = 1;
    add_control(&mut one, 1, 0, true);

    let mut duplicate = path_image();
    duplicate.stop_time_ns = 2;
    add_control(&mut duplicate, 1, 0, true);
    add_control(&mut duplicate, 2, 1, true);

    let one_result = run_scalar_cpu(&one);
    let duplicate_result = run_scalar_cpu(&duplicate);

    assert_eq!(upstream_pfc(&one_result), upstream_pfc(&duplicate_result));
    assert!(
        duplicate_result.pending_events.is_empty(),
        "duplicate pause must not schedule service or a timer"
    );
}

#[test]
fn resume_before_pause_is_an_idempotent_no_op() {
    let mut pause_only = path_image();
    pause_only.stop_time_ns = 2;
    add_control(&mut pause_only, 2, 0, true);

    let mut early_resume = path_image();
    early_resume.stop_time_ns = 2;
    add_control(&mut early_resume, 1, 0, false);
    add_control(&mut early_resume, 2, 1, true);

    let pause_result = run_scalar_cpu(&pause_only);
    let early_result = run_scalar_cpu(&early_resume);

    assert_eq!(upstream_pfc(&pause_result), upstream_pfc(&early_result));
    assert!(upstream_pfc(&early_result).paused_priorities[usize::from(PRIORITY)]);
    assert!(early_result.pending_events.is_empty());
}

#[test]
fn resume_emits_one_same_time_ready_and_enables_service() {
    let mut image = path_image();
    image.stop_time_ns = 5;
    image.initial_packets[0] = data_packet(DATA_0, 100);
    let queue = &mut image.switch_states[0].queues[0];
    queue.queue.push_back(DATA_0);
    queue
        .pfc
        .as_mut()
        .expect("fixture has PFC")
        .paused_priorities[usize::from(PRIORITY)] = true;
    add_control(&mut image, 5, 0, false);

    let result = run_scalar_cpu(&image);
    let queue = &result.switch_states[0].queues[0];

    assert!(!upstream_pfc(&result).paused_priorities[usize::from(PRIORITY)]);
    assert_eq!(queue.queue, VecDeque::new());
    assert_eq!(queue.in_service, Some(DATA_0));
    assert!(!queue.tx_ready_pending);
    assert_eq!(
        result.switch_states[0].next_origin_seq, 3,
        "resume emits one TxReady, whose service start emits completion and arrival"
    );
    assert_eq!(result.pending_events.len(), 2);
    assert!(result.pending_events.iter().all(|event| {
        event.payload == DATA_0
            && event.key.time_ns == 105
            && matches!(event.kind, EventKind::TxComplete | EventKind::RemoteArrival)
    }));
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.kind != EventKind::TxReady),
        "the single same-time TxReady must be consumed"
    );
}

#[test]
fn xoff_boundary_emits_a_64_byte_pause_on_the_referenced_control_channel() {
    let mut image = path_image();
    image.stop_time_ns = 1;
    image.initial_packets[0] = data_packet(DATA_0, 1_000);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: UPSTREAM,
            origin_seq: 0,
        },
        target: DOWNSTREAM,
        kind: EventKind::RemoteArrival,
        payload: DATA_0,
    });
    image.switch_states[0].next_origin_seq = 1;
    let control_channel_index = image.switch_states[1].queues[0]
        .pfc
        .as_ref()
        .and_then(|pfc| pfc.ingresses.first())
        .expect("fixture has an ingress monitor")
        .control_channel_index as usize;
    let control_channel = image.channels[control_channel_index];

    let result = run_scalar_cpu(&image);
    let pauses = result
        .pending_events
        .iter()
        .filter_map(|event| {
            let packet = result
                .resident_packets
                .iter()
                .find(|packet| packet.id == event.payload)?;
            let PacketKind::Pfc(header) = packet.kind else {
                return None;
            };
            header.pause.then_some((event, packet, header))
        })
        .collect::<Vec<_>>();

    assert_eq!(pauses.len(), 1, "XOFF equality must emit one pause edge");
    let (event, packet, header) = pauses[0];
    assert_eq!(packet.size_bytes, 64);
    assert_eq!(header.controlled_link, CONTROLLED_LINK);
    assert_eq!(header.priority, PRIORITY);
    assert_eq!(event.kind, EventKind::RemoteArrival);
    assert_eq!(event.key.phase, event_phase(EventKind::RemoteArrival));
    assert_eq!(event.key.origin_node, control_channel.source);
    assert_eq!(event.target, control_channel.target);
    assert_eq!(event.key.time_ns, 1 + control_channel.min_delay_ns);

    let mut checkpoint = image.clone();
    checkpoint.host_states = result.host_states.clone();
    checkpoint.switch_states = result.switch_states.clone();
    checkpoint.initial_packets = result.resident_packets.clone();
    checkpoint
        .initial_packets
        .sort_unstable_by_key(|packet| packet.id);
    checkpoint.initial_events = result.pending_events.clone();
    checkpoint
        .initial_events
        .sort_unstable_by_key(|event| event.key);
    validate(&checkpoint, Backend::Scalar).expect("PFC output checkpoint must validate");
    validate(&checkpoint, Backend::Cpu { workers: 4 })
        .expect("PFC output checkpoint must validate for CPU");
}

#[test]
fn disabled_pfc_priority_uses_the_ordinary_queue_capacity_path() {
    let mut image = path_image();
    image.stop_time_ns = 1;
    image.flows[0].priority = 2;
    image.initial_packets[0] = data_packet(DATA_0, 1_000);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: UPSTREAM,
            origin_seq: 0,
        },
        target: DOWNSTREAM,
        kind: EventKind::RemoteArrival,
        payload: DATA_0,
    });
    image.switch_states[0].next_origin_seq = 1;

    let result = run_scalar_cpu(&image);
    assert_eq!(result.summary.dropped_packets, 0);
    assert_eq!(result.summary.admitted_packets, 1);
    assert!(
        result
            .resident_packets
            .iter()
            .any(|packet| packet.id == DATA_0),
        "a priority with XOFF=0 is not governed by the zero PFC capacity slot"
    );
    let mut checkpoint = image;
    checkpoint.host_states = result.host_states;
    checkpoint.switch_states = result.switch_states;
    checkpoint.initial_packets = result.resident_packets;
    checkpoint.initial_events = result.pending_events;
    validate(&checkpoint, Backend::Scalar)
        .expect("a disabled-priority output checkpoint must remain valid");
}

#[test]
fn validator_rejects_one_byte_less_than_required_pfc_headroom() {
    let mut image = path_image();
    image.stop_time_ns = 1;
    image.initial_packets[0] = data_packet(DATA_0, 1_000);
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::RemoteArrival),
            origin_node: UPSTREAM,
            origin_seq: 0,
        },
        target: DOWNSTREAM,
        kind: EventKind::RemoteArrival,
        payload: DATA_0,
    });
    image.switch_states[0].next_origin_seq = 1;
    let ingress = image.switch_states[1].queues[0]
        .pfc
        .as_mut()
        .and_then(|pfc| pfc.ingresses.first_mut())
        .expect("fixture has an ingress monitor");
    // M=1,000, reverse control delay=6 ns, and R_fwd=8 Gb/s derive 2,005 bytes.
    ingress.buffer_capacity_bytes[usize::from(PRIORITY)] =
        ingress.xoff_threshold_bytes[usize::from(PRIORITY)] + 2_004;

    let error = validate(&image, Backend::Scalar)
        .expect_err("zero headroom must not admit a 1,000-byte controlled frame")
        .to_string();
    assert!(
        error.contains("PFC priority 3 has 2004 bytes of headroom")
            && error.contains("derived requirement 2005"),
        "expected a PFC headroom diagnostic, got: {error}"
    );
}

#[test]
fn validator_rejects_a_configured_frame_bound_below_reachable_traffic() {
    let mut image = path_image();
    image.switch_states[1].queues[0]
        .pfc
        .as_mut()
        .and_then(|pfc| pfc.ingresses.first_mut())
        .expect("fixture has an ingress monitor")
        .max_frame_bytes = 999;
    image.initial_packets[0] = data_packet(DATA_0, 1_000);

    let error = validate(&image, Backend::Scalar)
        .expect_err("the configured PFC frame bound must cover reachable traffic")
        .to_string();
    assert!(
        error.contains("maximum frame bound 999") && error.contains("reachable frame size 1000"),
        "expected a derived frame-bound diagnostic, got: {error}"
    );
}

#[test]
fn validator_rejects_unasserted_pfc_state_at_or_above_xoff() {
    let mut image = path_image();
    image.stop_time_ns = 1;
    image.initial_packets[0] = data_packet(DATA_0, 1_000);
    image.switch_states[1].queues[0].queue.push_back(DATA_0);
    image.switch_states[1].queues[0].tx_ready_pending = true;
    let ingress = image.switch_states[1].queues[0]
        .pfc
        .as_mut()
        .and_then(|pfc| pfc.ingresses.first_mut())
        .expect("fixture has an ingress monitor");
    ingress.occupancy_bytes[usize::from(PRIORITY)] = 1_000;
    image.initial_events.push(Event {
        key: EventKey {
            time_ns: 1,
            phase: event_phase(EventKind::TxReady),
            origin_node: DOWNSTREAM,
            origin_seq: 0,
        },
        target: DOWNSTREAM,
        kind: EventKind::TxReady,
        payload: DATA_0,
    });
    image.switch_states[1].next_origin_seq = 1;

    let error = validate(&image, Backend::Scalar)
        .expect_err("an XOFF crossing must already have asserted local state")
        .to_string();
    assert!(
        error.contains("is unasserted at occupancy 1000, at or above XOFF 1000"),
        "expected an XOFF state-consistency diagnostic, got: {error}"
    );
}

fn ring_pfc_ingress(controlled_link: LinkId, control_channel_index: u32) -> PfcIngressState {
    let (xoff_threshold_bytes, xon_threshold_bytes) = thresholds(1_000, 500);
    PfcIngressState {
        controlled_link,
        control_channel_index,
        buffer_capacity_bytes: {
            let mut capacity = [0; 8];
            capacity[usize::from(PRIORITY)] = 10_000;
            capacity
        },
        max_frame_bytes: 1,
        xoff_threshold_bytes,
        xon_threshold_bytes,
        occupancy_bytes: [0; 8],
        pause_asserted: [false; 8],
    }
}

fn circular_dependency_image() -> SimulationImage {
    const HOST_A: NodeId = NodeId(0);
    const HOST_B: NodeId = NodeId(1);
    const HOST_C: NodeId = NodeId(2);
    const A_AB: NodeId = NodeId(3);
    const A_OUT: NodeId = NodeId(4);
    const B_BC: NodeId = NodeId(5);
    const B_OUT: NodeId = NodeId(6);
    const C_CA: NodeId = NodeId(7);
    const C_OUT: NodeId = NodeId(8);

    const HOST_A_LINK: LinkId = LinkId(0);
    const HOST_B_LINK: LinkId = LinkId(1);
    const HOST_C_LINK: LinkId = LinkId(2);
    const AB: LinkId = LinkId(3);
    const BC: LinkId = LinkId(4);
    const CA: LinkId = LinkId(5);
    const A_TERMINAL: LinkId = LinkId(6);
    const B_TERMINAL: LinkId = LinkId(7);
    const C_TERMINAL: LinkId = LinkId(8);
    const BA_CONTROL: LinkId = LinkId(9);
    const CB_CONTROL: LinkId = LinkId(10);
    const AC_CONTROL: LinkId = LinkId(11);

    let nodes = vec![
        NodeDescriptor {
            id: HOST_A,
            kind: NodeKind::Host,
            state_slot: 0,
        },
        NodeDescriptor {
            id: HOST_B,
            kind: NodeKind::Host,
            state_slot: 1,
        },
        NodeDescriptor {
            id: HOST_C,
            kind: NodeKind::Host,
            state_slot: 2,
        },
        NodeDescriptor {
            id: A_AB,
            kind: NodeKind::Switch,
            state_slot: 0,
        },
        NodeDescriptor {
            id: A_OUT,
            kind: NodeKind::Switch,
            state_slot: 1,
        },
        NodeDescriptor {
            id: B_BC,
            kind: NodeKind::Switch,
            state_slot: 2,
        },
        NodeDescriptor {
            id: B_OUT,
            kind: NodeKind::Switch,
            state_slot: 3,
        },
        NodeDescriptor {
            id: C_CA,
            kind: NodeKind::Switch,
            state_slot: 4,
        },
        NodeDescriptor {
            id: C_OUT,
            kind: NodeKind::Switch,
            state_slot: 5,
        },
    ];
    let links = vec![
        LinkDescriptor {
            id: HOST_A_LINK,
            source: HOST_A,
            target: A_AB,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: HOST_B_LINK,
            source: HOST_B,
            target: B_BC,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: HOST_C_LINK,
            source: HOST_C,
            target: C_CA,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: AB,
            source: A_AB,
            target: B_BC,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: BC,
            source: B_BC,
            target: C_CA,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: CA,
            source: C_CA,
            target: A_AB,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: A_TERMINAL,
            source: A_OUT,
            target: HOST_A,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: B_TERMINAL,
            source: B_OUT,
            target: HOST_B,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: C_TERMINAL,
            source: C_OUT,
            target: HOST_C,
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: BA_CONTROL,
            source: B_BC,
            target: A_AB,
            rate_bps: 512_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: CB_CONTROL,
            source: C_CA,
            target: B_BC,
            rate_bps: 512_000_000_000,
            propagation_ns: 0,
        },
        LinkDescriptor {
            id: AC_CONTROL,
            source: A_AB,
            target: C_CA,
            rate_bps: 512_000_000_000,
            propagation_ns: 0,
        },
    ];
    let flows = vec![
        FlowDescriptor {
            id: FlowId(0),
            source: HOST_A,
            target: HOST_C,
            priority: PRIORITY,
            route: vec![HOST_A_LINK, AB, BC, C_TERMINAL],
            reverse_route: vec![],
        },
        FlowDescriptor {
            id: FlowId(1),
            source: HOST_B,
            target: HOST_A,
            priority: PRIORITY,
            route: vec![HOST_B_LINK, BC, CA, A_TERMINAL],
            reverse_route: vec![],
        },
        FlowDescriptor {
            id: FlowId(2),
            source: HOST_C,
            target: HOST_B,
            priority: PRIORITY,
            route: vec![HOST_C_LINK, CA, AB, B_TERMINAL],
            reverse_route: vec![],
        },
    ];
    let initial_packets = flows
        .iter()
        .map(|flow| PacketDescriptor {
            id: PayloadId(flow.id.0),
            flow: flow.id,
            size_bytes: 1,
            ecn_marked: false,
            kind: PacketKind::Data,
        })
        .collect::<Vec<_>>();
    let initial_events = flows
        .iter()
        .map(|flow| Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacketArrival),
                origin_node: flow.source,
                origin_seq: 0,
            },
            target: flow.source,
            kind: EventKind::PacketArrival,
            payload: PayloadId(flow.id.0),
        })
        .collect::<Vec<_>>();

    let mut channels = Vec::new();
    let mut seen = BTreeSet::new();
    for flow in &flows {
        for (index, link_id) in flow.route.iter().copied().enumerate() {
            let target = flow
                .route
                .get(index + 1)
                .map(|next| links[next.0 as usize].source)
                .unwrap_or(flow.target);
            if seen.insert((link_id, target)) {
                channels.push(
                    RemoteChannel::for_packet_link_to(links[link_id.0 as usize], target, 1)
                        .expect("ring data-channel delay must fit"),
                );
            }
        }
    }
    let control_base = channels.len() as u32;
    for (link_id, source, target) in [
        (BA_CONTROL, B_BC, A_AB),
        (CB_CONTROL, C_CA, B_BC),
        (AC_CONTROL, A_AB, C_CA),
    ] {
        let link = links[link_id.0 as usize];
        channels.push(RemoteChannel {
            source,
            target,
            link: link_id,
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: link.delay_ns(64).expect("ring PFC delay must fit"),
        });
    }

    SimulationImage {
        stop_time_ns: 100,
        nodes,
        host_states: vec![
            HostState {
                next_origin_seq: 1,
                ..host_state(HOST_A_LINK)
            },
            HostState {
                next_origin_seq: 1,
                ..host_state(HOST_B_LINK)
            },
            HostState {
                next_origin_seq: 1,
                ..host_state(HOST_C_LINK)
            },
        ],
        switch_states: vec![
            switch_state(
                100,
                AB,
                Some(pfc_queue(Some(ring_pfc_ingress(CA, control_base + 2)))),
            ),
            switch_state(100, A_TERMINAL, None),
            switch_state(
                200,
                BC,
                Some(pfc_queue(Some(ring_pfc_ingress(AB, control_base)))),
            ),
            switch_state(200, B_TERMINAL, None),
            switch_state(
                300,
                CA,
                Some(pfc_queue(Some(ring_pfc_ingress(BC, control_base + 1)))),
            ),
            switch_state(300, C_TERMINAL, None),
        ],
        flows,
        initial_packets,
        links,
        channels,
        initial_events,
        seed: 25,
    }
}

#[test]
fn validator_rejects_a_circular_controlled_link_dependency() {
    let image = circular_dependency_image();
    let error = validate(&image, Backend::Scalar)
        .expect_err("AB -> BC -> CA -> AB must be rejected")
        .to_string();

    assert!(
        error.contains("PFC circular pause dependency")
            && error.contains("(LinkId(3), 3)")
            && error.contains("(LinkId(4), 3)")
            && error.contains("(LinkId(5), 3)"),
        "expected the canonical PFC cycle diagnostic, got: {error}"
    );
}

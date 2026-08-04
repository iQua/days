#![cfg(all(
    feature = "lane-packing-counters",
    any(
        feature = "cuda",
        all(feature = "metal-spike", target_vendor = "apple")
    )
))]

use std::collections::VecDeque;

use days_executor::{
    DeviceLanePacking, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState,
    LanePackingGroupCounters, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind,
    ObservationMode, PacketDescriptor, PacketKind, PayloadId, RemoteChannel, RunResult,
    SimulationImage, event_phase, run_scalar_with_observations,
};

const ACTIVE_LPS: usize = 100;
const NODE_COUNT: usize = ACTIVE_LPS + 1;

fn stale_timeout_image() -> SimulationImage {
    let mut nodes = Vec::with_capacity(NODE_COUNT);
    let mut host_states = Vec::with_capacity(NODE_COUNT);
    let mut initial_events = Vec::new();

    for index in 0..ACTIVE_LPS {
        let source = NodeId(index as u64);
        nodes.push(NodeDescriptor {
            id: source,
            kind: NodeKind::Host,
            state_slot: index as u32,
        });
        host_states.push(HostState {
            egress_link: LinkId(index as u64),
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators: Vec::new(),
            tcp_receivers: Vec::new(),
            dcqcn_receivers: Vec::new(),
            next_origin_seq: (2 * index + 3) as u64,
            next_payload_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        });
        let mut origin_seq = 0_u64;
        for (time_ns, count) in [(0, 1), (10, index + 1), (20, index + 1)] {
            for _ in 0..count {
                initial_events.push(Event {
                    key: EventKey {
                        time_ns,
                        phase: event_phase(EventKind::RetransmissionTimeout),
                        origin_node: source,
                        origin_seq,
                    },
                    target: source,
                    kind: EventKind::RetransmissionTimeout,
                    payload: PayloadId::from_node_sequence(source, NODE_COUNT as u64, origin_seq)
                        .expect("test payload identity must fit"),
                });
                origin_seq += 1;
            }
        }
    }
    let sink = NodeId(ACTIVE_LPS as u64);
    nodes.push(NodeDescriptor {
        id: sink,
        kind: NodeKind::Host,
        state_slot: ACTIVE_LPS as u32,
    });
    host_states.push(HostState {
        egress_link: LinkId(ACTIVE_LPS as u64),
        queue: VecDeque::new(),
        in_service: None,
        tx_ready_pending: false,
        generators: Vec::new(),
        tcp_receivers: Vec::new(),
        dcqcn_receivers: Vec::new(),
        next_origin_seq: 0,
        next_payload_seq: 0,
        sourced_packets: 0,
        departed_packets: 0,
        received_packets: 0,
    });

    let link = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: sink,
        rate_bps: 8_000_000_000,
        propagation_ns: 9,
    };
    let mut links = vec![link];
    links.extend((1..ACTIVE_LPS).map(|index| LinkDescriptor {
        id: LinkId(index as u64),
        source: NodeId(index as u64),
        target: sink,
        rate_bps: 8_000_000_000,
        propagation_ns: 9,
    }));
    links.push(LinkDescriptor {
        id: LinkId(ACTIVE_LPS as u64),
        source: sink,
        target: NodeId(0),
        rate_bps: 8_000_000_000,
        propagation_ns: 9,
    });
    let queued = PacketDescriptor {
        id: PayloadId::from_node_sequence(NodeId(0), NODE_COUNT as u64, 3)
            .expect("queued packet identity must fit"),
        flow: FlowId(0),
        size_bytes: 1,
        ecn_marked: false,
        kind: PacketKind::Data,
    };
    host_states[0].queue.push_back(queued.id);
    host_states[0].tx_ready_pending = true;
    host_states[0].next_origin_seq += 1;
    host_states[0].next_payload_seq = 4;
    initial_events.push(Event {
        key: EventKey {
            time_ns: 100,
            phase: event_phase(EventKind::TxReady),
            origin_node: NodeId(0),
            origin_seq: 3,
        },
        target: NodeId(0),
        kind: EventKind::TxReady,
        payload: queued.id,
    });
    initial_events.sort_unstable_by_key(|event| event.key);

    SimulationImage {
        stop_time_ns: 21,
        nodes,
        host_states,
        switch_states: Vec::new(),
        flows: vec![FlowDescriptor {
            id: FlowId(0),
            source: NodeId(0),
            target: sink,
            priority: 0,
            route: vec![LinkId(0)],
            reverse_route: Vec::new(),
        }],
        initial_packets: vec![queued],
        links,
        channels: vec![
            RemoteChannel::for_packet_link(link, 1).expect("one-byte link delay must fit"),
        ],
        initial_events,
        seed: 1,
    }
}

fn group(lanes: u64, work: u64, maximum_lane_work: u64) -> LanePackingGroupCounters {
    LanePackingGroupCounters {
        lanes,
        work,
        maximum_lane_work,
    }
}

fn assert_structural_counters(
    packing: DeviceLanePacking,
    result: &RunResult,
    scalar: &RunResult,
    counters: &days_executor::LanePackingCounters,
) {
    let mut expected = scalar.clone();
    expected.diagnostics = None;
    assert_eq!(result, &expected, "{} result identity", packing.label());
    assert_eq!(counters.rounds.len(), 3);
    assert_eq!(
        counters.rounds[0],
        [
            group(32, 32, 1),
            group(32, 32, 1),
            group(32, 32, 1),
            group(4, 4, 1),
        ]
    );
    let ascending = [
        group(32, 528, 32),
        group(32, 1_552, 64),
        group(32, 2_576, 96),
        group(4, 394, 100),
    ];
    let descending = [
        group(32, 2_704, 100),
        group(32, 1_680, 68),
        group(32, 656, 36),
        group(4, 10, 4),
    ];
    match packing {
        DeviceLanePacking::Unpacked => {
            assert_eq!(counters.rounds[1], ascending);
            assert_eq!(counters.rounds[2], ascending);
        }
        DeviceLanePacking::Descending => {
            assert_eq!(counters.rounds[1], ascending);
            assert_eq!(counters.rounds[2], descending);
        }
        DeviceLanePacking::Ascending => {
            assert_eq!(
                counters.rounds[1],
                [ascending[2], ascending[1], ascending[0], ascending[3]]
            );
            assert_eq!(
                counters.rounds[2],
                [descending[2], descending[1], descending[0], descending[3]]
            );
        }
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_lane_packing_orders_more_than_two_full_groups_deterministically() {
    use days_executor::{MetalConfig, run_metal_with_observations};

    let image = stale_timeout_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for packing in [
        DeviceLanePacking::Unpacked,
        DeviceLanePacking::Descending,
        DeviceLanePacking::Ascending,
    ] {
        let run = run_metal_with_observations(
            &image,
            None,
            MetalConfig {
                lane_packing: packing,
                ..MetalConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_structural_counters(
            packing,
            &run.result,
            &scalar,
            run.lane_packing_counters.as_ref().unwrap(),
        );
    }
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_lane_packing_orders_more_than_two_full_groups_deterministically() {
    use days_executor::{CudaConfig, run_cuda_with_observations};

    let image = stale_timeout_image();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for packing in [
        DeviceLanePacking::Unpacked,
        DeviceLanePacking::Descending,
        DeviceLanePacking::Ascending,
    ] {
        let run = run_cuda_with_observations(
            &image,
            None,
            CudaConfig {
                lane_packing: packing,
                ..CudaConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_structural_counters(
            packing,
            &run.result,
            &scalar,
            run.lane_packing_counters.as_ref().unwrap(),
        );
    }
}

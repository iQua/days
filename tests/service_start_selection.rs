#![cfg(feature = "test")]

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::{EventSinkReader, Output, SinkState, event_queue};
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days::flows::packet::Packet;
use days::schedulers::drop::{CapacityUnit, DropStrategy};
use days::schedulers::drr::DRRServer;
use days::schedulers::port::Port;
use days::schedulers::sp::SPServer;
use days::schedulers::wfq::WFQServer;
use days::schedulers::wrr::WRRServer;
use days_executor::{
    Backend, Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState, LinkDescriptor, LinkId,
    NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketDescriptor, PacketKind, PayloadId,
    RemoteChannel, SchedulerKind, SimulationImage, SwitchQueueState, SwitchState, event_phase,
    run_scalar_with_observations, validate,
};

struct ScriptedPacketSource {
    packets: Vec<Packet>,
    output: Output<Packet>,
}

impl ScriptedPacketSource {
    const EMIT_SID: SchedulableId<Self, Packet> = SchedulableId::__from_decorated(0);

    fn new(packets: Vec<Packet>) -> Self {
        Self {
            packets,
            output: Output::default(),
        }
    }

    async fn emit(&mut self, mut packet: Packet, cx: &Context<Self>) {
        packet.time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.output.send(packet).await;
    }
}

impl Model for ScriptedPacketSource {
    type Env = ();

    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::emit));
        registry
    }

    async fn init(mut self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        for packet in std::mem::take(&mut self.packets) {
            if packet.time == 0.0 {
                self.emit(packet, cx).await;
            } else {
                cx.schedule_event(
                    Duration::from_secs_f64(packet.time),
                    &Self::EMIT_SID,
                    packet,
                )
                .unwrap();
            }
        }
        self.into()
    }
}

struct OneShotLoopback {
    output: Output<Packet>,
}

impl OneShotLoopback {
    async fn packet_received(&mut self, packet: Packet, cx: &Context<Self>) {
        if packet.packet_id == 0 {
            let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            self.output
                .send(Packet::new(1, 1, packet.flow_id, now))
                .await;
        }
    }
}

impl Model for OneShotLoopback {
    type Env = ();
}

fn packets_with_intervening_arrival() -> Vec<Packet> {
    vec![
        Packet::new(1, 0, 0, 0.01),
        Packet::new(1, 1, 1, 0.10),
        Packet::new(1, 2, 1, 0.10),
        Packet::new(2, 3, 0, 1.50),
    ]
}

fn assert_only_capacity_valid_packets_departed(mut reader: impl EventSinkReader<Packet>) {
    let mut observed = Vec::new();
    while let Some(packet) = reader.try_read() {
        observed.push((packet.packet_id, packet.time));
    }

    let packet_ids: Vec<_> = observed.iter().map(|(packet_id, _)| *packet_id).collect();
    assert_eq!(
        packet_ids,
        vec![0, 1, 2],
        "the intervening two-byte packet must be dropped while packet 2 still occupies capacity"
    );

    for ((packet_id, actual), expected) in observed.iter().zip([1.01, 2.01, 3.01]) {
        assert!(
            (actual - expected).abs() <= 1e-12,
            "packet {packet_id} departed at {actual}, expected {expected}"
        );
    }
}

#[test]
fn fifo_rounding_does_not_delay_future_service_starts() {
    let mut packets: Vec<_> = (0..20)
        .map(|packet_id| Packet::new(64, packet_id, 0, 0.0))
        .collect();
    packets.push(Packet::new(1_280, 20, 0, 101e-9));

    let mut source = ScriptedPacketSource::new(packets);
    let mut scheduler = Port::new(
        100e9,
        1_280,
        CapacityUnit::Bytes,
        DropStrategy::TailDrop,
        0.0,
    );
    let source_mbox = Mailbox::new();
    let scheduler_mbox = Mailbox::new();
    let (writer, mut reader) = event_queue(SinkState::Enabled);

    source
        .output
        .connect(Port::packet_received, &scheduler_mbox);
    scheduler.output.connect_sink(writer);

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, source_mbox, "Source")
        .add_model(scheduler, scheduler_mbox, "FIFO")
        .init(t0)
        .unwrap();

    sim.step_until(t0 + Duration::from_nanos(300)).unwrap();

    let mut observed = Vec::new();
    while let Some(packet) = reader.try_read() {
        observed.push((
            packet.packet_id,
            Duration::from_secs_f64(packet.time).as_nanos(),
        ));
    }

    let mut expected: Vec<_> = (0..20)
        .map(|packet_id| (packet_id, (packet_id as u128 + 1) * 5))
        .collect();
    expected.push((20, 203));

    assert_eq!(
        observed, expected,
        "the backlog must depart by 100 ns so the 101 ns arrival is admitted"
    );
}

#[test]
fn fifo_idle_port_starts_service_when_busy_until_is_one_ulp_ahead() {
    let rate = f64::from_bits(800.0_f64.to_bits() - 1);
    let first_size = 149_935;
    let busy_until = first_size as f64 * 8.0 / rate;
    let event_time = Duration::from_secs_f64(busy_until).as_nanos() as f64 / 1_000_000_000.0;
    assert_eq!(busy_until - event_time, f64::EPSILON * 1024.0);

    let mut source = ScriptedPacketSource::new(vec![Packet::new(first_size, 0, 0, 0.0)]);
    let mut scheduler = Port::new(rate, 2, CapacityUnit::Packets, DropStrategy::TailDrop, 0.0);
    let mut loopback = OneShotLoopback {
        output: Output::default(),
    };
    let source_mbox = Mailbox::new();
    let scheduler_mbox = Mailbox::new();
    let loopback_mbox = Mailbox::new();
    let (writer, mut reader) = event_queue(SinkState::Enabled);

    source
        .output
        .connect(Port::packet_received, &scheduler_mbox);
    scheduler
        .output
        .connect(OneShotLoopback::packet_received, &loopback_mbox);
    scheduler.output.connect_sink(writer);
    loopback
        .output
        .connect(Port::packet_received, &scheduler_mbox);

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, source_mbox, "Source")
        .add_model(scheduler, scheduler_mbox, "FIFO")
        .add_model(loopback, loopback_mbox, "Loopback")
        .init(t0)
        .unwrap();

    sim.step_until(t0 + Duration::from_secs(1_501)).unwrap();

    let mut observed = Vec::new();
    while let Some(packet) = reader.try_read() {
        observed.push(packet.packet_id);
    }
    assert_eq!(
        observed,
        vec![0, 1],
        "an idle port must not strand a queued packet when busy_until is one ULP ahead"
    );
}

#[test]
fn drr_does_not_free_future_service_capacity_early() {
    let mut source = ScriptedPacketSource::new(packets_with_intervening_arrival());
    let mut scheduler = DRRServer::new(
        8.0,
        2,
        CapacityUnit::Bytes,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        0.0,
        vec![1, 1],
    );
    let source_mbox = Mailbox::new();
    let scheduler_mbox = Mailbox::new();
    let (writer, reader) = event_queue(SinkState::Enabled);

    source
        .output
        .connect(DRRServer::packet_received, &scheduler_mbox);
    scheduler.output.connect_sink(writer);

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, source_mbox, "Source")
        .add_model(scheduler, scheduler_mbox, "DRR")
        .init(t0)
        .unwrap();

    sim.step_until(t0 + Duration::from_secs(6)).unwrap();
    assert_only_capacity_valid_packets_departed(reader);
}

#[test]
fn wrr_does_not_free_future_service_capacity_early() {
    let mut source = ScriptedPacketSource::new(packets_with_intervening_arrival());
    let mut scheduler = WRRServer::new(
        8.0,
        2,
        CapacityUnit::Bytes,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        0.0,
        vec![1, 2],
    );
    let source_mbox = Mailbox::new();
    let scheduler_mbox = Mailbox::new();
    let (writer, reader) = event_queue(SinkState::Enabled);

    source
        .output
        .connect(WRRServer::packet_received, &scheduler_mbox);
    scheduler.output.connect_sink(writer);

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, source_mbox, "Source")
        .add_model(scheduler, scheduler_mbox, "WRR")
        .init(t0)
        .unwrap();

    sim.step_until(t0 + Duration::from_secs(6)).unwrap();
    assert_only_capacity_valid_packets_departed(reader);
}

fn executor_scheduler_image(scheduler: SchedulerKind) -> SimulationImage {
    let source_link = LinkDescriptor {
        id: LinkId(0),
        source: NodeId(0),
        target: NodeId(1),
        rate_bps: 8,
        propagation_ns: 0,
    };
    let switch_link = LinkDescriptor {
        id: LinkId(1),
        source: NodeId(1),
        target: NodeId(2),
        rate_bps: 8,
        propagation_ns: 0,
    };
    SimulationImage {
        stop_time_ns: 4_000_000_000,
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
                generators: vec![],
                next_origin_seq: 3,
                next_payload_seq: 0,
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
                egress_link: Some(LinkId(1)),
                scheduler,
                queue_capacity_packets: 8,
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
                target: NodeId(2),
                route: vec![LinkId(0), LinkId(1)],
                reverse_route: vec![],
            },
            FlowDescriptor {
                id: FlowId(1),
                source: NodeId(0),
                target: NodeId(2),
                route: vec![LinkId(0), LinkId(1)],
                reverse_route: vec![],
            },
        ],
        initial_packets: vec![
            PacketDescriptor {
                id: PayloadId(0),
                flow: FlowId(0),
                size_bytes: 1,
                kind: PacketKind::Data,
            },
            PacketDescriptor {
                id: PayloadId(3),
                flow: FlowId(0),
                size_bytes: 1,
                kind: PacketKind::Data,
            },
            PacketDescriptor {
                id: PayloadId(6),
                flow: FlowId(1),
                size_bytes: 1,
                kind: PacketKind::Data,
            },
        ],
        links: vec![
            source_link,
            switch_link,
            LinkDescriptor {
                id: LinkId(2),
                source: NodeId(2),
                target: NodeId(1),
                rate_bps: 8,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(source_link, 1).unwrap(),
            RemoteChannel::for_packet_link(switch_link, 1).unwrap(),
        ],
        initial_events: [0_u64, 100_000_000, 200_000_000]
            .into_iter()
            .enumerate()
            .map(|(sequence, time_ns)| Event {
                key: EventKey {
                    time_ns,
                    phase: event_phase(EventKind::RemoteArrival),
                    origin_node: NodeId(0),
                    origin_seq: sequence as u64,
                },
                target: NodeId(1),
                kind: EventKind::RemoteArrival,
                payload: PayloadId(sequence as u64 * 3),
            })
            .collect(),
        seed: 18,
    }
}

fn executor_trajectory(scheduler: SchedulerKind) -> Vec<(usize, u128)> {
    let image = executor_scheduler_image(scheduler);
    validate(&image, Backend::Scalar).expect("trajectory image must validate");
    run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("trajectory image must run")
        .departures
        .into_iter()
        .map(|departure| {
            (
                (departure.payload.0 / 3) as usize,
                u128::from(departure.time_ns),
            )
        })
        .collect()
}

fn scripted_trajectory_packets() -> Vec<Packet> {
    vec![
        Packet::new(1, 0, 0, 0.0),
        Packet::new(1, 1, 0, 0.1),
        Packet::new(1, 2, 1, 0.2),
    ]
}

#[test]
fn sp_scalar_matches_legacy_on_a_nontied_integer_time_trajectory() {
    let mut source = ScriptedPacketSource::new(scripted_trajectory_packets());
    let mut scheduler = SPServer::new(
        8.0,
        8,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        0.0,
        vec![1, 9],
    );
    let source_mbox = Mailbox::new();
    let scheduler_mbox = Mailbox::new();
    let (writer, mut reader) = event_queue(SinkState::Enabled);
    source
        .output
        .connect(SPServer::packet_received, &scheduler_mbox);
    scheduler.output.connect_sink(writer);
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, source_mbox, "Source")
        .add_model(scheduler, scheduler_mbox, "SP")
        .init(t0)
        .unwrap();
    sim.step_until(t0 + Duration::from_secs(4)).unwrap();
    let mut legacy = Vec::new();
    while let Some(packet) = reader.try_read() {
        legacy.push((
            packet.packet_id,
            Duration::from_secs_f64(packet.time).as_nanos(),
        ));
    }

    assert_eq!(
        executor_trajectory(SchedulerKind::static_priority(vec![1, 9])),
        legacy
    );
}

#[test]
fn wfq_scalar_matches_legacy_on_a_nontied_integer_time_trajectory() {
    let mut source = ScriptedPacketSource::new(scripted_trajectory_packets());
    let mut scheduler = WFQServer::new(
        8.0,
        8,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        0.0,
        vec![1, 4],
    );
    let source_mbox = Mailbox::new();
    let scheduler_mbox = Mailbox::new();
    let (writer, mut reader) = event_queue(SinkState::Enabled);
    source
        .output
        .connect(WFQServer::packet_received, &scheduler_mbox);
    scheduler.output.connect_sink(writer);
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(source, source_mbox, "Source")
        .add_model(scheduler, scheduler_mbox, "WFQ")
        .init(t0)
        .unwrap();
    sim.step_until(t0 + Duration::from_secs(4)).unwrap();
    let mut legacy = Vec::new();
    while let Some(packet) = reader.try_read() {
        legacy.push((
            packet.packet_id,
            Duration::from_secs_f64(packet.time).as_nanos(),
        ));
    }

    assert_eq!(
        executor_trajectory(SchedulerKind::weighted_fair_queue(vec![1, 4])),
        legacy
    );
}

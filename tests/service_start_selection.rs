#![cfg(feature = "test")]

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
use days::schedulers::wrr::WRRServer;

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

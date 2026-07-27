#![cfg(feature = "test")]

use std::time::Duration;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::{EventSlot, Output};
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use tempfile::NamedTempFile;

use days::flows::packet::{EcnField, Packet};
use days::schedulers::drop::{CapacityUnit, DropStrategy};
use days::schedulers::port::Port;
use days::seed_from_config;

struct EcnBurstSource {
    time: f64,
    next_id: usize,
    remaining: usize,
    size: usize,
    flow_id: usize,
    output: Output<Packet>,
}

impl EcnBurstSource {
    const RUN_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    fn new(flow_id: usize, size: usize, count: usize) -> Self {
        Self {
            time: 0.0,
            next_id: 0,
            remaining: count,
            size,
            flow_id,
            output: Output::default(),
        }
    }

    async fn run(&mut self, _: (), cx: &Context<Self>) {
        if self.remaining == 0 {
            return;
        }

        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.time = now;

        let mut packet = Packet::new(self.size, self.next_id, self.flow_id, now);
        packet.ecn = EcnField::Ect0;
        self.next_id += 1;
        self.remaining -= 1;

        self.output.send(packet).await;

        if self.remaining > 0 {
            cx.schedule_event(Duration::from_secs_f64(0.0005), &Self::RUN_SID, ())
                .unwrap();
        }
    }
}

impl Model for EcnBurstSource {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::run));
        registry
    }

    async fn init(mut self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        self.run((), cx).await;
        self.into()
    }
}

#[test]
fn test_red_ecn_marks_ce_in_simulation() {
    let seed_file = NamedTempFile::new().expect("Failed to create temp seed file");
    std::fs::write(seed_file.path(), "seed = 1\n").expect("Failed to write seed file");
    let _ = seed_from_config(seed_file.path().to_str().unwrap());

    let mut source = EcnBurstSource::new(0, 512, 2_000);
    let mut port = Port::new(
        100_000.0, // 100 kbps to build queue
        5,         // small queue
        CapacityUnit::Packets,
        DropStrategy::RedEcn,
        0.0,
        None,
    );

    let source_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let mut sink_slot = EventSlot::new();

    source.output.connect(Port::packet_received, &port_mbox);
    port.output.connect_sink(sink_slot.writer());

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox, "ECNSource")
        .add_model(port, port_mbox, "ECNPort")
        .init(t0)
        .expect("Failed to initialize ECN integration simulation");

    let _ = sim.step_until(Duration::from_secs_f64(2.0));

    let mut saw_ce = false;
    while let Some(pkt) = sink_slot.next() {
        if pkt.ecn == EcnField::Ce {
            saw_ce = true;
            break;
        }
    }

    assert!(saw_ce, "expected at least one CE-marked packet");
}

#![cfg(feature = "test")]

use std::collections::HashMap;
use std::time::Duration;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::{EventQueueReader, EventSinkReader, Output, SinkState, event_queue};
use nexosim::simulation::{Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;

use days::flows::packet::{ControlPacket, Packet, TCPAck};
use days::switches::switch::PacketSwitch;

struct PacketEmitter {
    packet: Packet,
    delay: Duration,
    output: Output<Packet>,
}

impl PacketEmitter {
    const EMIT_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    fn new(packet: Packet, delay: Duration) -> Self {
        Self {
            packet,
            delay,
            output: Output::default(),
        }
    }

    async fn emit(&mut self, _: (), cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        let mut packet = self.packet.clone();
        packet.time = now;
        self.output.send(packet).await;
    }
}

impl Model for PacketEmitter {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::emit));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        cx.schedule_event(self.delay, &Self::EMIT_SID, ()).unwrap();
        self.into()
    }
}

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn build_sim(mut emitter: PacketEmitter, switch: PacketSwitch) -> (Simulation, MonotonicTime) {
    let emitter_mbox = Mailbox::new();
    let switch_mbox = Mailbox::new();
    emitter
        .output
        .connect(PacketSwitch::packet_received, &switch_mbox);

    let t0 = MonotonicTime::EPOCH;
    let sim = SimInit::with_num_threads(1)
        .add_model(emitter, emitter_mbox, "Emitter")
        .add_model(switch, switch_mbox, "Switch")
        .init(t0)
        .unwrap();

    (sim, t0)
}

fn build_switch_with_outputs(
    fib: HashMap<usize, usize>,
    r_fib: HashMap<usize, usize>,
    output_ids: &[usize],
) -> (PacketSwitch, Vec<EventQueueReader<Packet>>) {
    let mut switch = PacketSwitch::new(fib, r_fib);
    let mut readers = Vec::new();

    for output_id in output_ids {
        let (writer, reader) = event_queue(SinkState::Enabled);
        switch
            .outputs
            .get_mut(output_id)
            .expect("missing output for fib entry")
            .connect_sink(writer);
        readers.push(reader);
    }

    (switch, readers)
}

fn data_packet(flow_id: usize) -> Packet {
    Packet::new(1200, 1, flow_id, 0.0)
}

fn control_packet(flow_id: usize) -> Packet {
    let mut packet = Packet::new(64, 2, flow_id, 0.0);
    packet.control = Some(ControlPacket::DcqcnCnp);
    packet
}

fn ack_packet(flow_id: usize) -> Packet {
    let mut packet = Packet::new(64, 3, flow_id, 0.0);
    packet.ack = Some(TCPAck {
        sequence_num: 1,
        acknowledged_size: 64,
        ece: false,
    });
    packet
}

#[test]
fn data_packets_forward_to_fib_output() {
    init_logger();

    let mut fib = HashMap::new();
    fib.insert(1, 10);
    let r_fib = HashMap::new();

    let (switch, mut readers) = build_switch_with_outputs(fib, r_fib, &[10]);
    let mut reader = readers.swap_remove(0);

    let emitter = PacketEmitter::new(data_packet(1), Duration::from_millis(1));
    let (mut sim, t0) = build_sim(emitter, switch);

    sim.step_until(t0 + Duration::from_millis(5)).unwrap();

    let received = reader.try_read().expect("no packet forwarded");
    assert_eq!(received.flow_id, 1);
    assert_eq!(received.packet_id, 1);
    assert!(reader.try_read().is_none(), "unexpected extra packet");
}

#[test]
fn control_packets_forward_to_r_fib_output() {
    init_logger();

    let mut fib = HashMap::new();
    fib.insert(1, 10);
    fib.insert(2, 20);
    let mut r_fib = HashMap::new();
    r_fib.insert(1, 20);

    let (switch, mut readers) = build_switch_with_outputs(fib, r_fib, &[10, 20]);
    let mut reader_b = readers.pop().expect("missing output");
    let mut reader_a = readers.pop().expect("missing output");

    let emitter = PacketEmitter::new(control_packet(1), Duration::from_millis(1));
    let (mut sim, t0) = build_sim(emitter, switch);

    sim.step_until(t0 + Duration::from_millis(5)).unwrap();

    assert!(
        reader_a.try_read().is_none(),
        "control packet sent to fib output"
    );
    let received = reader_b.try_read().expect("no control packet forwarded");
    assert_eq!(received.flow_id, 1);
    assert_eq!(received.packet_id, 2);
    assert!(reader_b.try_read().is_none(), "unexpected extra packet");
}

#[test]
fn ack_packets_forward_to_r_fib_output() {
    init_logger();

    let mut fib = HashMap::new();
    fib.insert(1, 10);
    fib.insert(2, 20);
    let mut r_fib = HashMap::new();
    r_fib.insert(1, 20);

    let (switch, mut readers) = build_switch_with_outputs(fib, r_fib, &[10, 20]);
    let mut reader_b = readers.pop().expect("missing output");
    let mut reader_a = readers.pop().expect("missing output");

    let emitter = PacketEmitter::new(ack_packet(1), Duration::from_millis(1));
    let (mut sim, t0) = build_sim(emitter, switch);

    sim.step_until(t0 + Duration::from_millis(5)).unwrap();

    assert!(
        reader_a.try_read().is_none(),
        "ack packet sent to fib output"
    );
    let received = reader_b.try_read().expect("no ack packet forwarded");
    assert_eq!(received.flow_id, 1);
    assert_eq!(received.packet_id, 3);
    assert!(reader_b.try_read().is_none(), "unexpected extra packet");
}

#[test]
fn data_packet_requires_fib_entry() {
    init_logger();

    let switch = PacketSwitch::new(HashMap::new(), HashMap::new());
    let emitter = PacketEmitter::new(data_packet(1), Duration::from_millis(1));
    let (mut sim, t0) = build_sim(emitter, switch);

    assert!(
        sim.step_until(t0 + Duration::from_millis(5)).is_err(),
        "missing fib entry should error the simulation"
    );
}

#[test]
fn control_packet_requires_r_fib_entry() {
    init_logger();

    let mut fib = HashMap::new();
    fib.insert(1, 10);

    let switch = PacketSwitch::new(fib, HashMap::new());
    let emitter = PacketEmitter::new(control_packet(1), Duration::from_millis(1));
    let (mut sim, t0) = build_sim(emitter, switch);

    assert!(
        sim.step_until(t0 + Duration::from_millis(5)).is_err(),
        "missing r_fib entry should error the simulation"
    );
}

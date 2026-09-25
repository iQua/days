#![cfg(feature = "test")]

use std::time::Duration;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::{EventSinkReader, Output, SinkState, event_queue};
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

#[derive(Default)]
struct BatchEmitter {
    output: Output<(u32, f64)>,
}

impl BatchEmitter {
    const EMIT_SID: SchedulableId<Self, u32> = SchedulableId::__from_decorated(0);

    async fn emit(&mut self, value: u32, cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.output.send((value, now)).await;
    }
}

impl Model for BatchEmitter {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::emit));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        cx.schedule_event(Duration::from_millis(1), &Self::EMIT_SID, 0u32)
            .unwrap();

        cx.schedule_event_batch(
            vec![
                (Duration::from_millis(1), 1u32),
                (Duration::from_millis(1), 2u32),
                (Duration::from_millis(3), 3u32),
            ],
            &Self::EMIT_SID,
        )
        .unwrap();

        self.into()
    }
}

#[test]
fn schedule_event_batch_preserves_order() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (writer, mut reader) = event_queue(SinkState::Enabled);

    let mut emitter = BatchEmitter::default();
    emitter.output.connect_sink(writer);

    let emitter_mbox = Mailbox::new();
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(emitter, emitter_mbox, "Emitter")
        .init(t0)
        .unwrap();

    sim.step_until(t0 + Duration::from_millis(5)).unwrap();

    let mut got = Vec::new();
    while let Some(evt) = reader.try_read() {
        got.push(evt);
    }

    assert_eq!(got.len(), 4);
    assert_eq!(got[0].0, 0);
    assert_eq!(got[1].0, 1);
    assert_eq!(got[2].0, 2);
    assert_eq!(got[3].0, 3);

    let t1 = 0.001;
    let t3 = 0.003;
    for (value, time) in got {
        let expected = match value {
            0..=2 => t1,
            3 => t3,
            _ => unreachable!(),
        };
        assert!(
            (time - expected).abs() <= 1e-12,
            "Unexpected time for value {value}: got {time}, expected {expected}",
        );
    }
}

#[derive(Default)]
struct AtomicityEmitter {
    output: Output<(u32, f64)>,
}

impl AtomicityEmitter {
    const EMIT_SID: SchedulableId<Self, u32> = SchedulableId::__from_decorated(0);

    async fn emit(&mut self, value: u32, cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        self.output.send((value, now)).await;
    }
}

impl Model for AtomicityEmitter {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::emit));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        assert!(
            cx.schedule_event_batch(
                vec![(Duration::ZERO, 1u32), (Duration::from_millis(1), 2u32)],
                &Self::EMIT_SID,
            )
            .is_err(),
            "batch scheduling a non-future event should fail",
        );

        cx.schedule_event(Duration::from_millis(2), &Self::EMIT_SID, 99u32)
            .unwrap();

        self.into()
    }
}

#[test]
fn schedule_event_batch_is_atomic_on_error() {
    let _ = env_logger::builder().is_test(true).try_init();

    let (writer, mut reader) = event_queue(SinkState::Enabled);

    let mut emitter = AtomicityEmitter::default();
    emitter.output.connect_sink(writer);

    let emitter_mbox = Mailbox::new();
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::with_num_threads(1)
        .add_model(emitter, emitter_mbox, "Emitter")
        .init(t0)
        .unwrap();

    sim.step_until(t0 + Duration::from_millis(5)).unwrap();

    let mut got = Vec::new();
    while let Some(evt) = reader.try_read() {
        got.push(evt);
    }

    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, 99);
    assert!(
        (got[0].1 - 0.002).abs() <= 1e-12,
        "Unexpected time: got {}, expected 0.002",
        got[0].1
    );
}

#![cfg(all(feature = "test", feature = "l2_pfc"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use nexosim::model::{
    BuildContext, Context, InitializedModel, Model, ModelRegistry, ProtoModel, SchedulableId,
};
use nexosim::ports::Output;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days_legacy::flows::packet::Packet;
use days_legacy::l2::frame::LinkFrame;
use days_legacy::l2::pfc::{PfcConfig, PfcFrame, PfcIngressPort};

struct FrameSource {
    count: usize,
    size: usize,
    output: Output<LinkFrame>,
}

impl FrameSource {
    const SEND_BURST_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    fn new(count: usize, size: usize) -> Self {
        Self {
            count,
            size,
            output: Output::default(),
        }
    }

    async fn send_burst(&mut self, _: (), cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        for i in 0..self.count {
            let mut packet = Packet::new(self.size, i, 0, now);
            packet.set_priority(0);
            self.output.send(LinkFrame::Data(packet)).await;
        }
    }
}

impl Model for FrameSource {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::send_burst));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        cx.schedule_event(Duration::from_secs_f64(1e-9), &Self::SEND_BURST_SID, ())
            .unwrap();
        self.into()
    }
}

struct PfcSink {
    pause: Arc<AtomicUsize>,
    resume: Arc<AtomicUsize>,
}

impl PfcSink {
    fn new(pause: Arc<AtomicUsize>, resume: Arc<AtomicUsize>) -> Self {
        Self { pause, resume }
    }

    async fn frame_received(&mut self, frame: PfcFrame, _: &Context<Self>) {
        let has_pause = frame.pause_quanta.iter().any(|&q| q > 0);
        if has_pause {
            self.pause.fetch_add(1, Ordering::Relaxed);
        } else {
            self.resume.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Model for PfcSink {
    type Env = ();
}

struct DelayedFrameSource {
    delay: Duration,
    size: usize,
    output: Output<LinkFrame>,
}

impl DelayedFrameSource {
    const SEND_ONCE_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    fn new(delay: Duration, size: usize) -> Self {
        Self {
            delay,
            size,
            output: Output::default(),
        }
    }

    async fn send_once(&mut self, _: (), cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        let mut packet = Packet::new(self.size, 0, 0, now);
        packet.set_priority(0);
        self.output.send(LinkFrame::Data(packet)).await;
    }
}

impl Model for DelayedFrameSource {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::send_once));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        cx.schedule_event(self.delay, &Self::SEND_ONCE_SID, ())
            .unwrap();
        self.into()
    }
}

struct GateToggle {
    open: Arc<AtomicBool>,
    delay: Duration,
}

impl GateToggle {
    const OPEN_GATE_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    fn new(open: Arc<AtomicBool>, delay: Duration) -> Self {
        Self { open, delay }
    }

    async fn open_gate(&mut self, _: (), _: &Context<Self>) {
        self.open.store(true, Ordering::Relaxed);
    }
}

impl Model for GateToggle {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::open_gate));
        registry
    }

    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        cx.schedule_event(self.delay, &Self::OPEN_GATE_SID, ())
            .unwrap();
        self.into()
    }
}

#[test]
fn test_pfc_pause_frames_emitted() {
    let mut pfc_config = PfcConfig {
        xoff: [1000; 8],
        xon: [500; 8],
        pause_quanta: [10; 8],
        buffer_capacity: [0; 8],
        refresh_interval: None,
        drain_interval: Some(0.001),
    };
    pfc_config.pause_quanta[0] = 10;

    let can_forward = Arc::new(|_packet: &Packet| false);
    let ingress = PfcIngressPort::new(0, 0, pfc_config, can_forward);

    let source = FrameSource::new(4, 600);
    let pause = Arc::new(AtomicUsize::new(0));
    let resume = Arc::new(AtomicUsize::new(0));
    let sink = PfcSink::new(pause.clone(), resume.clone());

    let source_mbox = Mailbox::new();
    let ingress_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let mut ingress = ingress;
    let mut source = source;
    let sink = sink;

    source
        .output
        .connect(PfcIngressPort::frame_received, &ingress_mbox);
    ingress
        .pfc_output
        .connect(PfcSink::frame_received, &sink_mbox);

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox, "FrameSource")
        .add_model(ingress, ingress_mbox, "PfcIngress")
        .add_model(sink, sink_mbox, "PfcSink")
        .init(t0)
        .expect("failed to init simulation");

    let _ = sim.step_until(Duration::from_secs_f64(0.01));
    assert!(
        pause.load(Ordering::Relaxed) > 0,
        "expected pause frames to be emitted"
    );
}

#[test]
fn test_pfc_resume_frames_emitted_after_drain() {
    let mut pfc_config = PfcConfig {
        xoff: [500; 8],
        xon: [100; 8],
        pause_quanta: [10; 8],
        buffer_capacity: [0; 8],
        refresh_interval: None,
        drain_interval: Some(0.0005),
    };
    pfc_config.pause_quanta[0] = 10;

    let open = Arc::new(AtomicBool::new(false));
    let can_forward = {
        let open = open.clone();
        Arc::new(move |_packet: &Packet| open.load(Ordering::Relaxed))
    };
    let ingress = PfcIngressPort::new(0, 0, pfc_config, can_forward);

    let source = FrameSource::new(2, 600);
    let late_source = DelayedFrameSource::new(Duration::from_secs_f64(0.003), 600);
    let pause = Arc::new(AtomicUsize::new(0));
    let resume = Arc::new(AtomicUsize::new(0));
    let sink = PfcSink::new(pause.clone(), resume.clone());
    let toggle = GateToggle::new(open.clone(), Duration::from_secs_f64(0.002));

    let source_mbox = Mailbox::new();
    let late_source_mbox = Mailbox::new();
    let ingress_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let toggle_mbox = Mailbox::new();

    let mut ingress = ingress;
    let mut source = source;
    let mut late_source = late_source;
    let sink = sink;
    let toggle = toggle;

    source
        .output
        .connect(PfcIngressPort::frame_received, &ingress_mbox);
    late_source
        .output
        .connect(PfcIngressPort::frame_received, &ingress_mbox);
    ingress
        .pfc_output
        .connect(PfcSink::frame_received, &sink_mbox);

    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox, "FrameSource")
        .add_model(late_source, late_source_mbox, "LateFrameSource")
        .add_model(ingress, ingress_mbox, "PfcIngress")
        .add_model(toggle, toggle_mbox, "GateToggle")
        .add_model(sink, sink_mbox, "PfcSink")
        .init(t0)
        .expect("failed to init simulation");

    let _ = sim.step_until(Duration::from_secs_f64(0.01));

    assert!(
        pause.load(Ordering::Relaxed) > 0,
        "expected pause frames to be emitted"
    );
    assert!(
        resume.load(Ordering::Relaxed) > 0,
        "expected resume frames to be emitted"
    );
}

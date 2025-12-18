#![cfg(all(feature = "test", feature = "l2_pfc"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days::flows::packet::Packet;
use days::l2::frame::LinkFrame;
use days::l2::pfc::{PfcConfig, PfcFrame, PfcIngressPort};

struct FrameSource {
    count: usize,
    size: usize,
    output: Output<LinkFrame>,
}

impl FrameSource {
    fn new(count: usize, size: usize) -> Self {
        Self {
            count,
            size,
            output: Output::default(),
        }
    }

    async fn send_burst(&mut self, _: (), cx: &mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        for i in 0..self.count {
            let mut packet = Packet::new(self.size, i, 0, now);
            packet.set_priority(0);
            self.output.send(LinkFrame::Data(packet)).await;
        }
    }
}

impl Model for FrameSource {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        cx.schedule_event(Duration::from_secs_f64(0.0), Self::send_burst, ())
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

    async fn frame_received(&mut self, frame: PfcFrame, _: &mut Context<Self>) {
        let has_pause = frame.pause_quanta.iter().any(|&q| q > 0);
        if has_pause {
            self.pause.fetch_add(1, Ordering::Relaxed);
        } else {
            self.resume.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Model for PfcSink {}

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
    let ingress = PfcIngressPort::new(0, pfc_config, can_forward);

    let source = FrameSource::new(4, 600);
    let pause = Arc::new(AtomicUsize::new(0));
    let resume = Arc::new(AtomicUsize::new(0));
    let sink = PfcSink::new(pause.clone(), resume.clone());

    let source_mbox = Mailbox::new();
    let ingress_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let mut ingress = ingress;
    let mut source = source;
    let mut sink = sink;

    source.output.connect(PfcIngressPort::frame_received, &ingress_mbox);
    ingress
        .pfc_output
        .connect(PfcSink::frame_received, &sink_mbox);

    let t0 = MonotonicTime::EPOCH;
    let (mut sim, _) = SimInit::new()
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

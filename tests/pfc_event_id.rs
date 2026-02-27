#![cfg(all(feature = "test", feature = "l2_pfc", feature = "lean"))]

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use csv::ReaderBuilder;
use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;
use tempfile::tempdir;

use days::flows::packet::Packet;
use days::l2::frame::LinkFrame;
use days::l2::pfc::{PfcConfig, PfcEgressGate, PfcIngressPort};
use days::utils::logger::CsvLogger;

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
    async fn init(self, cx: &Context<Self>, _env: &mut Self::Env) -> InitializedModel<Self> {
        cx.schedule_event(Duration::from_secs_f64(1e-9), Self::send_burst, ())
            .unwrap();
        self.into()
    }
}

#[test]
fn pfc_events_include_event_id_and_match_sent_recv() {
    let tmp = tempdir().expect("create temp dir");
    let logger = CsvLogger::get_instance();
    logger
        .init(tmp.path().to_str().expect("temp dir path"))
        .expect("init logger");

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
    let ingress = PfcIngressPort::new(0, 1, pfc_config, can_forward);
    let gate = PfcEgressGate::new(1, 1e9);

    let source = FrameSource::new(4, 600);

    let source_mbox = Mailbox::new();
    let ingress_mbox = Mailbox::new();
    let gate_mbox = Mailbox::new();

    let mut ingress = ingress;
    let mut source = source;
    let gate = gate;

    source
        .output
        .connect(PfcIngressPort::frame_received, &ingress_mbox);
    ingress
        .pfc_output
        .connect(PfcEgressGate::pfc_received, &gate_mbox);

    let t0 = MonotonicTime::EPOCH;
    let (mut sim, _) = SimInit::new()
        .add_model(source, source_mbox, "FrameSource")
        .add_model(ingress, ingress_mbox, "PfcIngress")
        .add_model(gate, gate_mbox, "PfcGate")
        .init(t0)
        .expect("failed to init simulation");

    let _ = sim.step_until(Duration::from_secs_f64(0.01));
    logger.flush_reports();

    let path = tmp.path().join("pfc_events.csv");
    let content = fs::read_to_string(&path).expect("read pfc_events.csv");

    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(content.as_bytes());

    let headers = reader.headers().expect("read headers").clone();
    assert_eq!(headers.get(0), Some("time_ns"));
    assert_eq!(headers.get(1), Some("event_id"));

    let idx_event_id = headers
        .iter()
        .position(|h| h == "event_id")
        .expect("event_id column");
    let idx_kind = headers.iter().position(|h| h == "kind").expect("kind");
    let idx_frame_id = headers
        .iter()
        .position(|h| h == "pfc_frame_id")
        .expect("pfc_frame_id");
    let idx_prio = headers
        .iter()
        .position(|h| h == "priority")
        .expect("priority");

    let mut event_ids: Vec<u64> = Vec::new();
    let mut kinds: Vec<String> = Vec::new();
    let mut by_frame: HashMap<(u64, u8), Vec<String>> = HashMap::new();

    for row in reader.records() {
        let row = row.expect("read record");
        let event_id: u64 = row
            .get(idx_event_id)
            .expect("event_id column")
            .parse()
            .expect("parse event_id");
        event_ids.push(event_id);

        let kind = row.get(idx_kind).expect("kind").to_string();
        kinds.push(kind.clone());

        let frame_id: u64 = row
            .get(idx_frame_id)
            .expect("pfc_frame_id")
            .parse()
            .expect("parse pfc_frame_id");
        let prio: u8 = row
            .get(idx_prio)
            .expect("priority")
            .parse()
            .expect("parse priority");
        by_frame.entry((frame_id, prio)).or_default().push(kind);
    }

    assert!(!event_ids.is_empty(), "expected pfc_events rows");

    let mut unique_ids = event_ids.clone();
    unique_ids.sort_unstable();
    unique_ids.dedup();
    assert_eq!(unique_ids.len(), event_ids.len(), "event_id must be unique");

    assert!(
        kinds.iter().any(|k| k == "pfc_sent"),
        "expected at least one pfc_sent row"
    );
    assert!(
        kinds.iter().any(|k| k == "pfc_recv"),
        "expected at least one pfc_recv row"
    );

    for ((frame_id, prio), ks) in by_frame {
        assert!(
            ks.iter().any(|k| k == "pfc_sent"),
            "missing pfc_sent for frame_id={frame_id} prio={prio}"
        );
        assert!(
            ks.iter().any(|k| k == "pfc_recv"),
            "missing pfc_recv for frame_id={frame_id} prio={prio}"
        );
    }
}

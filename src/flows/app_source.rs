//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource using actor model compatible with `nexosim`.

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use crate::get_seed;
use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::future::Future;
use std::time::Duration;
use tachyonix::{channel, Receiver, Sender};

#[derive(Debug)]
pub struct AppSourceRequest {
    pub size: usize,
    pub respond_to: Sender<Vec<Packet>>,
}

#[derive(Clone)]
pub struct AppSourceHandle {
    tx: Sender<AppSourceRequest>,
}

impl AppSourceHandle {
    pub fn new(tx: Sender<AppSourceRequest>) -> Self {
        Self { tx }
    }

    pub async fn pull(&self, size: usize) -> Vec<Packet> {
        let (resp_tx, mut resp_rx) = channel(1);
        let _ = self
            .tx
            .send(AppSourceRequest {
                size,
                respond_to: resp_tx,
            })
            .await;
        resp_rx.recv().await.unwrap_or_default()
    }

    pub async fn shutdown(&self) {
        let (resp_tx, _resp_rx) = channel(1);
        let _ = self
            .tx
            .send(AppSourceRequest {
                size: 0,
                respond_to: resp_tx,
            })
            .await;
    }
}

pub struct AppActor {
    rx: Receiver<AppSourceRequest>,
    buffer: Vec<Packet>,
    traffic: Option<TrafficCharacteristics>,
    rng: Option<SmallRng>,
    pub out: Output<Packet>,
}

impl AppActor {
    pub fn buffered(packets: Vec<Packet>) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(128);
        let actor = AppActor {
            rx,
            buffer: packets,
            traffic: None,
            rng: None,
            out: Output::default(),
        };
        (actor, tx)
    }

    pub fn dist(
        flow_id: usize,
        tr: TrafficCharacteristics,
        rng: SmallRng,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(128);
        let mut src = DistPacketSource::new(flow_id, Vec::new(), tr.clone(), rng.clone());
        let mut packets = Vec::new();
        for _ in 0..512 {
            let (p, _) = src.produce_packet(0.0);
            packets.push(p);
        }
        let actor = AppActor {
            rx,
            buffer: packets,
            traffic: Some(tr),
            rng: Some(rng),
            out: Output::default(),
        };
        (actor, tx)
    }
}

impl Model for AppActor {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        cx.schedule_event(Duration::ZERO, Self::run_once, ())
            .unwrap();
        self.into()
    }
}

impl AppActor {
    fn run_once<'a>(
        &'a mut self,
        _: (),
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            while let Ok(req) = self.rx.try_recv() {
                let mut out = Vec::new();
                let mut sent = 0;
                while sent < req.size && !self.buffer.is_empty() {
                    let pkt = self.buffer.remove(0);
                    sent += pkt.size;
                    out.push(pkt);
                }
                let _ = req.respond_to.try_send(out);
            }
            cx.schedule_event(Duration::from_micros(50), Self::run_once, ())
                .unwrap();
        }
    }
}

pub enum AppDataSource {
    Buffered(AppSourceHandle),
    Dist(AppSourceHandle),
}

impl AppDataSource {
    pub fn buffered(packets: Vec<Packet>) -> (Self, AppActor) {
        let (actor, tx) = AppActor::buffered(packets);
        (Self::Buffered(AppSourceHandle::new(tx)), actor)
    }

    pub fn dist(flow_id: usize, tr: TrafficCharacteristics) -> (Self, AppActor) {
        let seed = get_seed();
        let rng = SmallRng::seed_from_u64(seed as u64 + flow_id as u64);
        let (actor, tx) = AppActor::dist(flow_id, tr, rng);
        (Self::Dist(AppSourceHandle::new(tx)), actor)
    }

    pub fn handle(&self) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => h.clone(),
        }
    }
}

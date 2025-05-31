//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource.

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use futures_executor::ThreadPool;
use rand::rngs::SmallRng;
use tachyonix::{channel, Sender};

#[derive(Debug)]
pub(crate) enum AppSourceRequest {
    Pull {
        size: usize,
        respond_to: Sender<Vec<Packet>>,
    },
    Shutdown,
}

#[derive(Clone)]
pub struct AppSourceHandle {
    tx: Sender<AppSourceRequest>,
}

impl AppSourceHandle {
    pub(crate) fn new(tx: Sender<AppSourceRequest>) -> Self {
        Self { tx }
    }

    pub async fn pull(&self, size: usize) -> Vec<Packet> {
        let (resp_tx, mut resp_rx) = channel(128);
        self.tx
            .send(AppSourceRequest::Pull {
                size,
                respond_to: resp_tx,
            })
            .await
            .unwrap();
        resp_rx.recv().await.unwrap()
    }

    pub async fn shutdown(&self) {
        let _ = self.tx.send(AppSourceRequest::Shutdown).await;
    }
}

#[derive(Debug, Clone)]
pub struct BufferedAppDataSource {
    total_size: usize,
    packets: Vec<Packet>,
}

impl BufferedAppDataSource {
    pub fn new(packets: Vec<Packet>) -> Self {
        let total_size = packets.iter().map(|p| p.size).sum();
        Self {
            total_size,
            packets,
        }
    }

    pub fn clone_packets(&self) -> Vec<Packet> {
        self.packets.clone()
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }
}

pub fn spawn_buffered_appsource(packets: Vec<Packet>) -> AppSourceHandle {
    let (tx, mut rx) = channel(128);
    let mut buffer = packets;
    let pool = ThreadPool::new().unwrap();

    pool.spawn_ok(async move {
        while let Ok(req) = rx.recv().await {
            match req {
                AppSourceRequest::Pull { size, respond_to } => {
                    let mut out = Vec::new();
                    let mut sent = 0;
                    while sent < size && !buffer.is_empty() {
                        let pkt = buffer.remove(0);
                        sent += pkt.size;
                        out.push(pkt);
                    }
                    let _ = respond_to.send(out).await;
                }
                AppSourceRequest::Shutdown => break,
            }
        }
    });

    AppSourceHandle::new(tx)
}

pub fn spawn_dist_appsource(
    flow_id: usize,
    traffic: TrafficCharacteristics,
    rng: SmallRng,
) -> AppSourceHandle {
    let (tx, mut rx) = channel(128);
    let mut source = DistPacketSource::new(flow_id, Vec::new(), traffic, rng);
    let pool = ThreadPool::new().unwrap();

    pool.spawn_ok(async move {
        while let Ok(req) = rx.recv().await {
            match req {
                AppSourceRequest::Pull { size, respond_to } => {
                    let mut out = Vec::new();
                    let mut sent = 0;
                    while sent < size {
                        let (pkt, _) = source.produce_packet(0.0);
                        source.packet_sent(&pkt, 0.0);
                        sent += pkt.size;
                        out.push(pkt);
                    }
                    let _ = respond_to.send(out).await;
                }
                AppSourceRequest::Shutdown => break,
            }
        }
    });

    AppSourceHandle::new(tx)
}

pub fn spawn_dummy_appsource() -> AppSourceHandle {
    let (tx, mut rx) = channel(128);
    let pool = ThreadPool::new().unwrap();

    pool.spawn_ok(async move {
        while let Ok(req) = rx.recv().await {
            match req {
                AppSourceRequest::Pull { respond_to, .. } => {
                    let _ = respond_to.send(Vec::new()).await;
                }
                AppSourceRequest::Shutdown => break,
            }
        }
    });

    AppSourceHandle::new(tx)
}

pub enum AppDataSource {
    Buffered(AppSourceHandle),
    Dist(AppSourceHandle),
    Dummy(AppSourceHandle),
}

impl AppDataSource {
    pub fn buffered(packets: Vec<Packet>) -> Self {
        Self::Buffered(spawn_buffered_appsource(packets))
    }

    pub fn dist(flow_id: usize, tr: TrafficCharacteristics, rng: SmallRng) -> Self {
        Self::Dist(spawn_dist_appsource(flow_id, tr, rng))
    }

    pub fn dummy() -> Self {
        Self::Dummy(spawn_dummy_appsource())
    }

    pub fn handle(&self) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) | Self::Dummy(h) => h.clone(),
        }
    }
}

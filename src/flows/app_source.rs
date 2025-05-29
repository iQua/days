//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource.

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use futures::future::join_all;
use futures_executor::ThreadPool;
use rand::rngs::SmallRng;
use tachyonix::{channel, Receiver, Sender};

#[derive(Debug)]
enum AppSourceRequest {
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
    pub fn new(tx: Sender<AppSourceRequest>) -> Self {
        Self { tx }
    }

    pub async fn pull(&self, size: usize) -> Vec<Packet> {
        let (resp_tx, resp_rx) = bounded(1);
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
    let (tx, mut rx) = unbounded();
    let mut buffer = packets;

    tachyonix::spawn(async move {
        while let Some(req) = rx.recv().await {
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
    let (tx, mut rx) = unbounded();
    let mut source = DistPacketSource::new(flow_id, Vec::new(), traffic, rng);

    tachyonix::spawn(async move {
        while let Some(req) = rx.recv().await {
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

pub enum AppDataSource {
    DistDataSource(DistPacketSource),
    Dummy,
}

pub enum AppDataType {
    DistData,
}

impl AppDataSource {
    pub fn dummy() -> Self {
        AppDataSource::Dummy
    }

    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        AppDataSource::DistDataSource(DistPacketSource::new(flow_id, Vec::new(), traffic, rng))
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            AppDataSource::DistDataSource(source) => source.traffic_exceeded(now),
            AppDataSource::Dummy => true,
        }
    }
}

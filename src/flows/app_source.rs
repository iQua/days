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

// A request sent to the AppActor asking for `size` bytes worth of packets.
// The `respond_to` channel is used to send back the result asynchronously.
#[derive(Debug)]
pub struct AppSourceRequest {
    pub start: usize,
    pub size: usize,
    pub respond_to: Sender<Vec<Packet>>,
}

// A handle to an application source actor. Allows TCPPacketSource to `pull()` packets asynchronously.
#[derive(Clone)]
pub struct AppSourceHandle {
    tx: Sender<AppSourceRequest>,
    offset: usize, // chunk start in the shared buffer
    cursor: usize,
}

impl AppSourceHandle {
    /// handle that starts at byte‐offset 0 (broadcast case)
    pub fn new(tx: Sender<AppSourceRequest>) -> Self {
        Self {
            tx,
            offset: 0,
            cursor: 0,
        }
    }

    /// NEW: create a handle that starts at `offset` (Ring-AllReduce chunk)
    pub fn with_offset(tx: Sender<AppSourceRequest>, offset: usize) -> Self {
        Self {
            tx,
            offset,
            cursor: 0,
        }
    }
    // Send a pull request to the actor, and await the returned packets.
    pub async fn pull(&mut self, size: usize) -> Vec<Packet> {
        let (resp_tx, mut resp_rx) = channel(1);
        // send the current cursor to the actor
        let _ = self
            .tx
            .send(AppSourceRequest {
                start: self.offset + self.cursor,
                size,
                respond_to: resp_tx,
            })
            .await;
        let pkts = resp_rx.recv().await.unwrap_or_default();
        // move the cursor
        self.cursor += pkts.iter().map(|p| p.size).sum::<usize>();
        pkts
    }
    // A shutdown signal by sending a request with size=0 (not actually handled yet).
    pub async fn shutdown(&self) {
        let (resp_tx, _resp_rx) = channel(1);
        let _ = self
            .tx
            .send(AppSourceRequest {
                start: 0,
                size: 0,
                respond_to: resp_tx,
            })
            .await;
    }
}

// The actor that holds a buffer of packets and services pull requests.
pub struct AppActor {
    rx: Receiver<AppSourceRequest>,
    buffer: Vec<Packet>,
    traffic: Option<TrafficCharacteristics>,
    rng: Option<SmallRng>,
    pub out: Output<Packet>,
}

// Construct a buffered actor with pre-generated packets.
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

    // Construct a dist actor that dynamically generates packets using traffic profile.
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
        println!(
            "[AppActor] Initialized buffer with {} packets",
            packets.len()
        );
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
        // Schedule the actor's run_once function every 1µs after simulation start
        cx.schedule_event(Duration::from_micros(1), Self::run_once, ())
            .expect("schedule_event failed");
        self.into()
    }
}

impl AppActor {
    /// 定时事件：处理所有 Pull 请求并立即返回所需数据（只 clone，不 pop）
    fn run_once<'a>(
        &'a mut self,
        _: (),
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            // 逐条处理 channel 中积压的请求
            while let Ok(req) = self.rx.try_recv() {
                let mut out = Vec::new();
                let mut sent = 0usize; // 已返回的总字节
                let mut idx = 0usize; // packet 下标
                let mut byte_pos = 0usize; // 当前 packet 起始字节位置

                // ① 找到 start 所在的 packet 下标
                while idx < self.buffer.len() && byte_pos + self.buffer[idx].size <= req.start {
                    byte_pos += self.buffer[idx].size;
                    idx += 1;
                }

                // ② 从 idx 开始 clone，直到满足 size
                while idx < self.buffer.len() && sent < req.size {
                    let pkt = self.buffer[idx].clone(); // 只 clone，不删除
                    sent += pkt.size;
                    out.push(pkt);
                    idx += 1;
                }

                // ③ 将结果异步返回
                let _ = req.respond_to.try_send(out);
            }

            // 重新调度下一轮（50 µs 后）
            cx.schedule_event(Duration::from_micros(50), Self::run_once, ())
                .expect("reschedule run_once failed");
        }
    }
}

// Enum wrapper around different types of app sources.
#[derive(Clone)]
pub enum AppDataSource {
    Buffered(AppSourceHandle),
    Dist(AppSourceHandle),
}

impl AppDataSource {
    // Create a buffered source with pre-generated packets
    pub fn buffered(packets: Vec<Packet>) -> (Self, AppActor) {
        let (actor, tx) = AppActor::buffered(packets);
        (Self::Buffered(AppSourceHandle::new(tx)), actor)
    }
    // Create a dist source from traffic profile and flow ID
    pub fn dist(flow_id: usize, tr: TrafficCharacteristics) -> (Self, AppActor) {
        let seed = get_seed();
        let rng = SmallRng::seed_from_u64(seed as u64 + flow_id as u64);
        let (actor, tx) = AppActor::dist(flow_id, tr, rng);
        (Self::Dist(AppSourceHandle::new(tx)), actor)
    }
    // Get the underlying handle to use in TCPPacketSource
    pub fn handle(&self) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => h.clone(),
        }
    }
    pub fn handle_with_offset(&self, offset: usize) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => AppSourceHandle::with_offset(h.clone().tx, offset),
        }
    }
}

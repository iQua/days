//! Implements a unified interface for application-level sources with channel-based delivery to TCPPacketSource using actor model compatible with `nexosim`.

use crate::flows::TrafficCharacteristics;
use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::get_seed;
use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::future::Future;
use std::time::Duration;
use tachyonix::{Receiver, Sender, channel};

// a request sent to the AppActor asking for `size` bytes of packets. The `respond_to` channel is used to send back the result asynchronously.
#[derive(Debug)]
pub struct AppSourceRequest {
    pub start: usize,
    pub size: usize,
    pub respond_to: Sender<Vec<Packet>>,
}

// a handle to an application source actor. Allows TCPPacketSource to `pull()` packets asynchronously.
#[derive(Clone)]
pub struct AppSourceHandle {
    tx: Sender<AppSourceRequest>,
    // chunk start in the shared buffer
    offset: usize,
    cursor: usize,
    total_size: Option<usize>,
}

impl AppSourceHandle {
    /// handle that starts at byte‐offset 0 (broadcast case)
    pub fn new(tx: Sender<AppSourceRequest>, total_size: Option<usize>) -> Self {
        Self {
            tx,
            offset: 0,
            cursor: 0,
            total_size,
        }
    }

    /// create a handle that starts at `offset` (Ring-AllReduce chunk)
    pub fn with_offset(
        tx: Sender<AppSourceRequest>,
        offset: usize,
        total_size: Option<usize>,
    ) -> Self {
        Self {
            tx,
            offset,
            cursor: 0,
            total_size,
        }
    }

    pub fn get_total_size(&self) -> Option<usize> {
        self.total_size
    }

    // send a pull request to the actor, and await the returned packets.
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
        self.cursor += pkts.iter().map(|p| p.size).sum::<usize>();
        pkts
    }
    // a shutdown signal by sending a request with size=0 (not actually handled yet).
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

// the actor that holds a buffer of packets and services pull requests.
pub struct AppActor {
    rx: Receiver<AppSourceRequest>,
    buffer: Vec<Packet>,
    pub out: Output<Packet>,
}

// construct a buffered actor with pre-generated packets.
impl AppActor {
    pub fn buffered(packets: Vec<Packet>) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(128);

        let actor = AppActor {
            rx,
            buffer: packets,
            out: Output::default(),
        };

        (actor, tx)
    }

    // construct a distributed actor that dynamically generates packets using traffic profile.
    pub fn distributed_actor(
        flow_id: usize,
        traffic: TrafficCharacteristics,
        rng: SmallRng,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(128);
        let mut src = DistPacketSource::new(flow_id, Vec::new(), traffic, rng.clone());
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
            out: Output::default(),
        };
        (actor, tx)
    }
}

impl Model for AppActor {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        // schedule the actor's run_once function every 1µs after simulation start
        cx.schedule_event(Duration::from_micros(1), Self::run_once, ())
            .expect("schedule_event failed");
        self.into()
    }
}

impl AppActor {
    #[allow(clippy::manual_async_fn)]
    fn run_once<'a>(
        &'a mut self,
        _: (),
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            while let Ok(req) = self.rx.try_recv() {
                let mut out = Vec::new();
                let mut sent = 0usize; // the total bytes of returned data
                let mut idx = 0usize; // index of packet
                let mut byte_pos = 0usize; // the current packet start position

                // find the index of current packet start
                while idx < self.buffer.len() && byte_pos + self.buffer[idx].size <= req.start {
                    byte_pos += self.buffer[idx].size;
                    idx += 1;
                }

                // clone from idx, until sent < req.size
                while idx < self.buffer.len() && sent < req.size {
                    let pkt = self.buffer[idx].clone();
                    sent += pkt.size;
                    out.push(pkt);
                    idx += 1;
                }

                let _ = req.respond_to.try_send(out);
            }

            cx.schedule_event(Duration::from_micros(50), Self::run_once, ())
                .expect("reschedule run_once failed");
        }
    }
}

#[derive(Clone)]
pub enum AppDataSource {
    Buffered(AppSourceHandle),
    Dist(AppSourceHandle),
}

impl AppDataSource {
    // create a buffered source with pre-generated packets
    fn buffered_from_packets(packets: Vec<Packet>) -> (Self, AppActor) {
        let total_size = packets.iter().map(|p| p.size).sum::<usize>();
        let (actor, tx) = AppActor::buffered(packets);
        (
            Self::Buffered(AppSourceHandle::new(tx, Some(total_size))),
            actor,
        )
    }

    pub fn buffered(total_size: usize, mss: usize) -> (Self, AppActor) {
        let mut packets = Vec::new();
        let mut remaining = total_size;
        let mut seq = 0;
        while remaining > 0 {
            let sz = mss.min(remaining);
            packets.push(Packet::new(sz, seq, 0, 0.0));
            seq += sz;
            remaining -= sz;
        }
        Self::buffered_from_packets(packets)
    }

    // create a dist source from traffic profile and flow ID
    pub fn distributed_source(flow_id: usize, tr: TrafficCharacteristics) -> (Self, AppActor) {
        let seed = get_seed();
        let rng = SmallRng::seed_from_u64(seed as u64 + flow_id as u64);
        let (actor, tx) = AppActor::distributed_actor(flow_id, tr, rng);
        (Self::Dist(AppSourceHandle::new(tx, None)), actor)
    }

    // get the underlying handle to use in TCPPacketSource
    pub fn handle(&self) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => h.clone(),
        }
    }
    pub fn handle_with_offset(&self, offset: usize) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => {
                AppSourceHandle::with_offset(h.tx.clone(), offset, h.total_size)
            }
        }
    }
}

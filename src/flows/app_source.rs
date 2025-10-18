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

#[derive(Clone, Copy, Debug)]
pub struct AppSourceRuntimeConfig {
    pub request_channel_capacity: usize,
    pub dist_initial_buffer_packets: usize,
    pub init_interval_micros: u64,
    pub run_interval_micros: u64,
}

impl Default for AppSourceRuntimeConfig {
    fn default() -> Self {
        Self {
            request_channel_capacity: 128,
            dist_initial_buffer_packets: 512,
            init_interval_micros: 1,
            run_interval_micros: 50,
        }
    }
}
// a request sent to the AppActor asking for `size` bytes of packets. The `respond_to` channel is used to send back the result asynchronously.
#[derive(Debug)]
pub struct AppSourceRequest {
    /// Start reading at this byte position inside the stream.
    pub start: usize,
    /// Total number of bytes the requester wants to receive
    pub size: usize,
    /// One-shot channel used by the actor to send back the packets.
    pub respond_to: Sender<Vec<Packet>>,
}

// a handle to an application source actor. Allows TCPPacketSource to `pull()` packets asynchronously.
#[derive(Clone)]
pub struct AppSourceHandle {
    /// Channel for sending requests to the app actor.
    tx: Sender<AppSourceRequest>,
    // First byte in the shared buffer assigned to this handle.
    offset: usize,
    /// Number of bytes already consumed using this handle.
    cursor: usize,
    /// Total length of the slice exposed through this handle.
    length: Option<usize>,
}

impl AppSourceHandle {
    /// handle that starts at byte‐offset 0 (broadcast case)
    pub fn new(tx: Sender<AppSourceRequest>, total_size: Option<usize>) -> Self {
        Self {
            tx,
            offset: 0,
            cursor: 0,
            length: total_size,
        }
    }

    /// create a handle that starts at `offset` (Ring-AllReduce chunk)
    pub fn with_offset(tx: Sender<AppSourceRequest>, offset: usize, length: Option<usize>) -> Self {
        Self {
            tx,
            offset,
            cursor: 0,
            length,
        }
    }

    pub fn get_total_size(&self) -> Option<usize> {
        self.length
    }

    // send a pull request to the actor, and await the returned packets.
    pub async fn pull(&mut self, size: usize) -> Vec<Packet> {
        // Clamp the requested size to the remaining bytes exposed by this handle.
        let allowed = self
            .length
            .map(|len| len.saturating_sub(self.cursor))
            .unwrap_or(size);
        let req_size = size.min(allowed);

        if req_size == 0 {
            return Vec::new();
        }

        let (resp_tx, mut resp_rx) = channel(1);
        // send the current cursor to the actor
        let _ = self
            .tx
            .send(AppSourceRequest {
                start: self.offset + self.cursor,
                size: req_size,
                respond_to: resp_tx,
            })
            .await;
        let pkts = resp_rx.recv().await.unwrap_or_default();
        let consumed = pkts.iter().map(|p| p.size).sum::<usize>();
        let consumed = if let Some(len) = self.length {
            consumed.min(len.saturating_sub(self.cursor))
        } else {
            consumed
        };
        self.cursor += consumed;
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
    /// Receives pull requests from every handle.
    rx: Receiver<AppSourceRequest>,
    /// Packet buffer that backs all responses.
    buffer: Vec<Packet>,
    /// Simulation output port exposed to other actors.
    pub out: Output<Packet>,
    /// Interval before first scheduling (µs)
    init_interval_micros: u64,
    /// Interval between ticks (µs)
    run_interval_micros: u64,
}

impl AppActor {
    pub fn buffered(
        packets: Vec<Packet>,
        config: &AppSourceRuntimeConfig,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(config.request_channel_capacity);

        let actor = AppActor {
            rx,
            buffer: packets,
            out: Output::default(),
            init_interval_micros: config.init_interval_micros,
            run_interval_micros: config.run_interval_micros,
        };

        (actor, tx)
    }

    // construct a distributed actor that generates packets following the probability and size patterns defined by the PacketDistribution.
    pub fn dist_actor(
        flow_id: usize,
        traffic: TrafficCharacteristics,
        rng: SmallRng,
        config: &AppSourceRuntimeConfig,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(config.request_channel_capacity);
        let mut src = DistPacketSource::new(flow_id, Vec::new(), traffic, rng.clone());
        let mut packets = Vec::new();

        for _ in 0..config.dist_initial_buffer_packets {
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
            init_interval_micros: config.init_interval_micros,
            run_interval_micros: config.run_interval_micros,
        };
        (actor, tx)
    }
}

impl Model for AppActor {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        // schedule the actor's run_once function after the configured initial interval
        cx.schedule_event(
            Duration::from_micros(self.init_interval_micros),
            Self::run_once,
            (),
        )
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

                if req.size == 0 {
                    let _ = req.respond_to.try_send(out);
                    continue;
                }

                let req_end = req.start.saturating_add(req.size);

                // find the index of current packet start
                while idx < self.buffer.len() && byte_pos + self.buffer[idx].size <= req.start {
                    byte_pos += self.buffer[idx].size;
                    idx += 1;
                }

                // clone from idx, until sent < req.size
                while idx < self.buffer.len() && sent < req.size && byte_pos < req_end {
                    let base_pkt = &self.buffer[idx];
                    let pkt_start = byte_pos;
                    let pkt_end = pkt_start.saturating_add(base_pkt.size);

                    if pkt_end <= req.start {
                        byte_pos = pkt_end;
                        idx += 1;
                        continue;
                    }

                    if pkt_start >= req_end {
                        break;
                    }

                    let slice_start = req.start.max(pkt_start);
                    let max_take = req.size - sent;
                    let slice_end = slice_start
                        .saturating_add(max_take)
                        .min(req_end)
                        .min(pkt_end);
                    let slice_len = slice_end.saturating_sub(slice_start);

                    if slice_len == 0 {
                        byte_pos = pkt_end;
                        idx += 1;
                        continue;
                    }

                    let mut pkt = base_pkt.clone();
                    pkt.size = slice_len;
                    sent += slice_len;
                    out.push(pkt);

                    // advance to next packet
                    byte_pos = pkt_end;
                    idx += 1;
                }

                let _ = req.respond_to.try_send(out);
            }

            cx.schedule_event(
                Duration::from_micros(self.run_interval_micros),
                Self::run_once,
                (),
            )
            .expect("reschedule run_once failed");
        }
    }
}

// Application-level sources that can be used to feed data to a TCPPacketSource.
#[derive(Clone)]
pub enum AppDataSource {
    // Application-level DataSource for pre-buffered packets.
    Buffered(AppSourceHandle),
    /// Application-level DataSource for packets generated from traffic distributions.
    Dist(AppSourceHandle),
}

impl AppDataSource {
    // Build a data source backed by the supplied packet list.
    fn buffered_actor_from_packets(
        packets: Vec<Packet>,
        config: AppSourceRuntimeConfig,
    ) -> (Self, AppActor) {
        let total_size = packets.iter().map(|p| p.size).sum::<usize>();
        let (actor, tx) = AppActor::buffered(packets, &config);
        (
            Self::Buffered(AppSourceHandle::new(tx, Some(total_size))),
            actor,
        )
    }

    pub fn buffered_actor(
        total_size: usize,
        mss: usize,
        config: AppSourceRuntimeConfig,
    ) -> (Self, AppActor) {
        let mut packets = Vec::new();
        let mut remaining = total_size;
        let mut seq = 0;
        while remaining > 0 {
            let sz = mss.min(remaining);
            packets.push(Packet::new(sz, seq, 0, 0.0));
            seq += sz;
            remaining -= sz;
        }
        Self::buffered_actor_from_packets(packets, config)
    }

    // create a dist source from traffic distributions and flow ID
    pub fn distributed_source(
        flow_id: usize,
        tr: TrafficCharacteristics,
        config: AppSourceRuntimeConfig,
    ) -> (Self, AppActor) {
        let seed = get_seed();
        let rng = SmallRng::seed_from_u64(seed as u64 + flow_id as u64);

        let (actor, tx) = AppActor::dist_actor(flow_id, tr, rng, &config);

        (Self::Dist(AppSourceHandle::new(tx, None)), actor)
    }

    // get the underlying handle to use in TCPPacketSource
    pub fn handle(&self) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => h.clone(),
        }
    }
    pub fn handle_with_offset(&self, offset: usize, length: Option<usize>) -> AppSourceHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => {
                let effective_length =
                    length.or_else(|| h.length.map(|len| len.saturating_sub(offset)));
                let absolute_offset = h.offset.saturating_add(offset);
                AppSourceHandle::with_offset(h.tx.clone(), absolute_offset, effective_length)
            }
        }
    }
}

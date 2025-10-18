//! Implements application-level data sources using an actor-based model.
//!
//! This module provides `AppActor` which manages byte buffers and serves
//! byte-range requests from TCP sources via async channels. The actor is
//! responsible ONLY for data management, not packetization - that responsibility
//! belongs to the TCP layer (`TCPPacketSource`).

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
// a request sent to the AppActor asking for `size` bytes of data. The `respond_to` channel is used to send back the result asynchronously.
#[derive(Debug)]
pub struct AppSourceRequest {
    /// Start reading at this byte position inside the stream.
    pub start: usize,
    /// Total number of bytes the requester wants to receive
    pub size: usize,
    /// One-shot channel used by the actor to send back the raw bytes.
    pub respond_to: Sender<Vec<u8>>,
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

    // send a pull request to the actor, and await the returned bytes.
    pub async fn pull(&mut self, size: usize) -> Vec<u8> {
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
        let data = resp_rx.recv().await.unwrap_or_default();
        let consumed = data.len();
        let consumed = if let Some(len) = self.length {
            consumed.min(len.saturating_sub(self.cursor))
        } else {
            consumed
        };
        self.cursor += consumed;
        data
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

// the actor that holds a buffer of bytes and services pull requests.
pub struct AppActor {
    /// Receives pull requests from every handle.
    rx: Receiver<AppSourceRequest>,
    /// Byte buffer that backs all responses.
    buffer: Vec<u8>,
    /// Simulation output port exposed to other actors.
    pub out: Output<Packet>,
    /// Interval before first scheduling (µs)
    init_interval_micros: u64,
    /// Interval between ticks (µs)
    run_interval_micros: u64,
}

impl AppActor {
    pub fn buffered(
        buffer: Vec<u8>,
        config: &AppSourceRuntimeConfig,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(config.request_channel_capacity);

        let actor = AppActor {
            rx,
            buffer,
            out: Output::default(),
            init_interval_micros: config.init_interval_micros,
            run_interval_micros: config.run_interval_micros,
        };

        (actor, tx)
    }

    // construct a distributed actor that generates bytes following the size patterns defined by the PacketDistribution.
    // Note: This pre-generates a buffer of bytes. The distribution characteristics are approximated.
    pub fn dist_actor(
        flow_id: usize,
        traffic: TrafficCharacteristics,
        rng: SmallRng,
        config: &AppSourceRuntimeConfig,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(config.request_channel_capacity);
        let mut src = DistPacketSource::new(flow_id, Vec::new(), traffic, rng.clone());
        let mut buffer = Vec::new();

        // Pre-generate bytes by creating packets and extracting their sizes
        for _ in 0..config.dist_initial_buffer_packets {
            let (p, _) = src.produce_packet(0.0);
            // Extend buffer with 'size' bytes (filled with zeros for now)
            buffer.extend(vec![0u8; p.size]);
        }
        log::debug!(
            "[AppActor] Initialized distributed buffer with {} bytes from {} packet sizes",
            buffer.len(),
            config.dist_initial_buffer_packets
        );
        let actor = AppActor {
            rx,
            buffer,
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
                // Handle shutdown signal
                if req.size == 0 {
                    log::debug!("[AppActor] Received shutdown signal, terminating actor");
                    // Don't reschedule - actor terminates
                    return;
                }

                // Simple byte-range extraction from the buffer
                let start = req.start.min(self.buffer.len());
                let end = (req.start + req.size).min(self.buffer.len());
                let data = self.buffer[start..end].to_vec();

                if let Err(e) = req.respond_to.try_send(data) {
                    log::warn!("[AppActor] Failed to send response: {:?}", e);
                }
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
    // Build a data source backed by a byte buffer of the specified size.
    pub fn buffered_actor(
        total_size: usize,
        config: AppSourceRuntimeConfig,
    ) -> (Self, AppActor) {
        // Create a buffer filled with zeros (or could be filled with meaningful data)
        let buffer = vec![0u8; total_size];
        let (actor, tx) = AppActor::buffered(buffer, &config);
        (
            Self::Buffered(AppSourceHandle::new(tx, Some(total_size))),
            actor,
        )
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

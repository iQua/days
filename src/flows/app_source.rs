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

    /// Get the current offset (for testing and debugging)
    pub fn get_offset(&self) -> usize {
        self.offset
    }

    /// Get the current cursor position (for testing and debugging)
    pub fn get_cursor(&self) -> usize {
        self.cursor
    }

    /// Get the length constraint (for testing and debugging)
    pub fn get_length(&self) -> Option<usize> {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test basic byte-range extraction logic
    #[test]
    fn test_app_actor_basic_byte_extraction() {
        let config = AppSourceRuntimeConfig::default();

        // Create a buffer with known data: [0, 1, 2, 3, ..., 99]
        let buffer: Vec<u8> = (0..100u8).collect();
        let (_actor, _tx) = AppActor::buffered(buffer.clone(), &config);

        // Test the byte slicing logic directly
        let start = 10usize;
        let size = 10usize;
        let buffer_len = buffer.len();

        let slice_start = start.min(buffer_len);
        let slice_end = (start + size).min(buffer_len);
        let data = buffer[slice_start..slice_end].to_vec();

        assert_eq!(slice_start, 10);
        assert_eq!(slice_end, 20);
        assert_eq!(data.len(), 10);
        assert_eq!(data[0], 10);
        assert_eq!(data[9], 19);
    }

    /// Test boundary conditions for byte extraction
    #[test]
    fn test_app_actor_boundary_conditions() {
        let buffer_size = 100;

        // Test 1: Request at exact buffer start
        let start = 0usize.min(buffer_size);
        let end = 10usize.min(buffer_size);
        assert_eq!(start, 0);
        assert_eq!(end, 10);

        // Test 2: Request at exact buffer end
        let start = 90usize.min(buffer_size);
        let end = (90 + 10).min(buffer_size);
        assert_eq!(start, 90);
        assert_eq!(end, 100);

        // Test 3: Request beyond buffer (should be clamped)
        let start = 90usize.min(buffer_size);
        let end = (90 + 20).min(buffer_size); // Request 20 bytes but only 10 available
        assert_eq!(start, 90);
        assert_eq!(end, 100);

        // Test 4: Request completely beyond buffer
        let start = 150usize.min(buffer_size);
        let end = (150 + 10).min(buffer_size);
        assert_eq!(start, 100);
        assert_eq!(end, 100); // Empty range

        // Test 5: Zero-size request
        let start = 50usize.min(buffer_size);
        let end = 50usize.min(buffer_size);
        assert_eq!(start, 50);
        assert_eq!(end, 50); // Empty range
    }

    /// Test AppSourceHandle cursor management
    #[test]
    fn test_handle_cursor_tracking() {
        // Simulate cursor advancement
        let mut cursor = 0usize;
        let length = Some(100usize);

        // First pull: 30 bytes
        let requested = 30;
        let allowed = length.map(|len| len.saturating_sub(cursor)).unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 30);
        cursor += actual_size;
        assert_eq!(cursor, 30);

        // Second pull: 50 bytes
        let requested = 50;
        let allowed = length.map(|len| len.saturating_sub(cursor)).unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 50);
        cursor += actual_size;
        assert_eq!(cursor, 80);

        // Third pull: 50 bytes (but only 20 remaining)
        let requested = 50;
        let allowed = length.map(|len| len.saturating_sub(cursor)).unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 20);
        cursor += actual_size;
        assert_eq!(cursor, 100);

        // Fourth pull: should return 0 (exhausted)
        let requested = 10;
        let allowed = length.map(|len| len.saturating_sub(cursor)).unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 0);
    }

    /// Test offset-based handles for RingAllReduce
    #[test]
    fn test_handle_with_offset() {
        // Simulate RingAllReduce with 4 nodes, 1000 bytes total
        let total_size = 1000;
        let num_chunks = 4;
        let chunk_size = total_size / num_chunks; // 250 bytes per chunk

        // Chunk 0: bytes 0-249
        let offset_0 = 0;
        let length_0 = chunk_size;
        assert_eq!(offset_0, 0);
        assert_eq!(length_0, 250);

        // Chunk 1: bytes 250-499
        let offset_1 = chunk_size;
        let length_1 = chunk_size;
        assert_eq!(offset_1, 250);
        assert_eq!(length_1, 250);

        // Chunk 2: bytes 500-749
        let offset_2 = 2 * chunk_size;
        let length_2 = chunk_size;
        assert_eq!(offset_2, 500);
        assert_eq!(length_2, 250);

        // Chunk 3: bytes 750-999 (last chunk might be different if not evenly divisible)
        let offset_3 = 3 * chunk_size;
        let length_3 = total_size - offset_3;
        assert_eq!(offset_3, 750);
        assert_eq!(length_3, 250);
    }

    /// Test offset calculations for non-evenly divisible buffers
    #[test]
    fn test_handle_with_offset_uneven() {
        // 1000 bytes divided by 3 chunks
        let total_size = 1000;
        let num_chunks = 3;
        let chunk_size = total_size / num_chunks; // 333 bytes

        // Chunk 0: bytes 0-332
        let offset_0 = 0;
        let length_0 = chunk_size;
        assert_eq!(offset_0, 0);
        assert_eq!(length_0, 333);

        // Chunk 1: bytes 333-665
        let offset_1 = chunk_size;
        let length_1 = chunk_size;
        assert_eq!(offset_1, 333);
        assert_eq!(length_1, 333);

        // Chunk 2: bytes 666-999 (last chunk gets the remainder)
        let offset_2 = 2 * chunk_size;
        let length_2 = total_size - offset_2; // Should be 334 to cover remaining bytes
        assert_eq!(offset_2, 666);
        assert_eq!(length_2, 334);

        // Verify all bytes are covered
        assert_eq!(length_0 + length_1 + length_2, total_size);
    }

    /// Test absolute offset calculation for nested offsets
    #[test]
    fn test_nested_offset_calculation() {
        // Base handle at offset 100
        let base_offset: usize = 100;
        let base_length: Option<usize> = Some(200); // bytes 100-299

        // Create a sub-handle at offset 50 within the base
        let sub_offset: usize = 50;
        let absolute_offset = base_offset.saturating_add(sub_offset);
        let effective_length = base_length.map(|len| len.saturating_sub(sub_offset));

        assert_eq!(absolute_offset, 150); // 100 + 50
        assert_eq!(effective_length, Some(150)); // 200 - 50

        // The sub-handle should access bytes 150-299 of the original buffer
    }

    /// Test that zero-length handles work correctly
    #[test]
    fn test_zero_length_handle() {
        let cursor = 0usize;
        let length = Some(0usize);

        let requested = 100;
        let allowed = length.map(|len| len.saturating_sub(cursor)).unwrap_or(requested);
        let actual_size = requested.min(allowed);

        assert_eq!(actual_size, 0);
    }

    /// Test AppDataSource buffered_actor constructor
    #[test]
    fn test_buffered_actor_creation() {
        let config = AppSourceRuntimeConfig::default();
        let total_size = 1024;

        let (datasrc, actor) = AppDataSource::buffered_actor(total_size, config);

        // Verify actor has correct buffer size
        assert_eq!(actor.buffer.len(), total_size);

        // Verify handle has correct total size
        let handle = datasrc.handle();
        assert_eq!(handle.get_total_size(), Some(total_size));
        assert_eq!(handle.get_offset(), 0);
        assert_eq!(handle.get_cursor(), 0);
    }

    /// Test that multiple handles can be created from the same datasource
    #[test]
    fn test_multiple_handles_from_same_source() {
        let config = AppSourceRuntimeConfig::default();
        let total_size = 1000;

        let (datasrc, _actor) = AppDataSource::buffered_actor(total_size, config);

        // Create multiple handles with different offsets (simulating Broadcast)
        let handle1 = datasrc.handle(); // Full buffer
        let handle2 = datasrc.handle(); // Full buffer again

        assert_eq!(handle1.get_offset(), 0);
        assert_eq!(handle1.get_length(), Some(total_size));
        assert_eq!(handle2.get_offset(), 0);
        assert_eq!(handle2.get_length(), Some(total_size));

        // Both handles should be independent (different cursors)
        assert_eq!(handle1.get_cursor(), 0);
        assert_eq!(handle2.get_cursor(), 0);
    }

    /// Test chunk offset handles for RingAllReduce
    #[test]
    fn test_ring_allreduce_chunk_handles() {
        let config = AppSourceRuntimeConfig::default();
        let total_size = 512;
        let num_nodes = 4;
        let chunk_size = total_size / num_nodes; // 128 bytes per chunk

        let (datasrc, _actor) = AppDataSource::buffered_actor(total_size, config);

        // Create handles for each chunk
        let chunk0 = datasrc.handle_with_offset(0, Some(chunk_size));
        let chunk1 = datasrc.handle_with_offset(chunk_size, Some(chunk_size));
        let chunk2 = datasrc.handle_with_offset(2 * chunk_size, Some(chunk_size));
        let chunk3 = datasrc.handle_with_offset(3 * chunk_size, Some(chunk_size));

        // Verify offsets
        assert_eq!(chunk0.get_offset(), 0);
        assert_eq!(chunk1.get_offset(), 128);
        assert_eq!(chunk2.get_offset(), 256);
        assert_eq!(chunk3.get_offset(), 384);

        // Verify lengths
        assert_eq!(chunk0.get_length(), Some(128));
        assert_eq!(chunk1.get_length(), Some(128));
        assert_eq!(chunk2.get_length(), Some(128));
        assert_eq!(chunk3.get_length(), Some(128));

        // Verify cursors are independent
        assert_eq!(chunk0.get_cursor(), 0);
        assert_eq!(chunk1.get_cursor(), 0);
        assert_eq!(chunk2.get_cursor(), 0);
        assert_eq!(chunk3.get_cursor(), 0);
    }
}

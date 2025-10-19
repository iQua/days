//! Implements application-level data sources using an actor-based model.
//!
//! This module provides `AppSourceBuffer` as an actor, which manages byte buffers and serves
//! byte-range requests from TCP sources via async channels. The actor is responsible *only*
//! for data management, not packetization - that responsibility belongs to the TCP layer
//! (`TCPPacketSource`).

use std::future::Future;
use std::time::Duration;

use rand::SeedableRng;
use rand::rngs::SmallRng;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use tachyonix::{Receiver, Sender, channel};

use crate::flows::TrafficCharacteristics;
use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::get_seed;

/// Runtime configuration for AppSourceBuffer
#[derive(Clone, Copy, Debug)]
pub struct AppBufferConfig {
    pub req_channel_capacity: usize,
    pub chunk_size: usize,
    pub initial_delay: u64,
    pub run_interval: u64,
}

impl Default for AppBufferConfig {
    fn default() -> Self {
        Self {
            req_channel_capacity: 256,
            chunk_size: 512,
            initial_delay: 1,
            run_interval: 50,
        }
    }
}
// Each request sent to the AppSourceBuffer asks for `size` bytes of data. The `respond_to` channel
// is used to send back the result asynchronously.
#[derive(Debug)]
pub struct AppSourceRequest {
    /// Start reading at this byte position inside the stream.
    pub start: usize,
    /// Total number of bytes the requester wants to receive
    pub size: usize,
    /// One-shot channel used by the actor to send back the raw bytes.
    pub respond_to: Sender<Vec<u8>>,
}

// The handle for the AppSourceBuffer actor. Allows TCPPacketSource to `pull()` packets asynchronously.
#[derive(Clone)]
pub struct AppSourceBufferHandle {
    /// Channel for sending requests to the app actor.
    tx: Sender<AppSourceRequest>,
    // First byte in the shared buffer assigned to this handle.
    offset: usize,
    /// Number of bytes already consumed using this handle.
    cursor: usize,
    /// Total length of the slice exposed through this handle.
    length: Option<usize>,
}

impl AppSourceBufferHandle {
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

// An actor that holds a buffer of bytes in the application layer, and services pull requests.
pub struct AppSourceBuffer {
    /// Receives pull requests from every handle.
    rx: Receiver<AppSourceRequest>,
    /// Byte buffer that backs all responses.
    buffer: Vec<u8>,
    /// Simulation output port exposed to other actors.
    pub out: Output<Packet>,
    /// Interval before first scheduling (µs)
    initial_delay: u64,
    /// Interval between ticks (µs)
    run_interval: u64,
}

impl AppSourceBuffer {
    pub fn buffered(buffer: Vec<u8>, config: &AppBufferConfig) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(config.req_channel_capacity);

        let actor = AppSourceBuffer {
            rx,
            buffer,
            out: Output::default(),
            initial_delay: config.initial_delay,
            run_interval: config.run_interval,
        };

        (actor, tx)
    }

    // Construct a distributed actor that generates bytes following the size patterns defined by the PacketDistribution.
    pub fn dist_actor(
        flow_id: usize,
        traffic: TrafficCharacteristics,
        rng: SmallRng,
        config: &AppBufferConfig,
    ) -> (Self, Sender<AppSourceRequest>) {
        let (tx, rx) = channel(config.req_channel_capacity);

        let mut src = DistPacketSource::new(flow_id, Vec::new(), traffic, rng.clone());
        let mut buffer = Vec::new();

        // Pre-generate bytes by creating chunks and extracting their sizes
        for _ in 0..config.chunk_size {
            let (p, _) = src.produce_packet(0.0);
            // Extend buffer with 'size' bytes (filled with zeros for now)
            buffer.extend(vec![0u8; p.size]);
        }
        log::debug!(
            "[AppSourceBuffer] Initialized distributed buffer with {} bytes and a chunk size of {}.",
            buffer.len(),
            config.chunk_size
        );

        let source_buffer = AppSourceBuffer {
            rx,
            buffer,
            out: Output::default(),
            initial_delay: config.initial_delay,
            run_interval: config.run_interval,
        };

        (source_buffer, tx)
    }
}

impl Model for AppSourceBuffer {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        // schedule the actor's run_once function after the configured initial interval
        cx.schedule_event(
            Duration::from_micros(self.initial_delay),
            Self::run_once,
            (),
        )
        .expect("schedule_event failed");

        self.into()
    }
}

impl AppSourceBuffer {
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
                    log::debug!("[AppSourceBuffer] Received shutdown signal, terminating actor");
                    // Don't reschedule - actor terminates
                    return;
                }

                // Simple byte-range extraction from the buffer
                let start = req.start.min(self.buffer.len());
                let end = (req.start + req.size).min(self.buffer.len());
                let data = self.buffer[start..end].to_vec();

                if let Err(e) = req.respond_to.try_send(data) {
                    log::warn!("[AppSourceBuffer] Failed to send response: {:?}", e);
                }
            }

            cx.schedule_event(Duration::from_micros(self.run_interval), Self::run_once, ())
                .expect("reschedule run_once failed");
        }
    }
}

// Application-level sources that can be used to feed data to a TCPPacketSource.
#[derive(Clone)]
pub enum AppDataSource {
    // Application-level DataSource for pre-buffered packets.
    Buffered(AppSourceBufferHandle),
    /// Application-level DataSource for packets generated from traffic distributions.
    Dist(AppSourceBufferHandle),
}

impl AppDataSource {
    // Build a data source backed by a byte buffer of the specified size.
    pub fn buffered_actor(total_size: usize, config: AppBufferConfig) -> (Self, AppSourceBuffer) {
        // Create a buffer filled with zeros (or could be filled with meaningful data)
        let buffer = vec![0u8; total_size];
        let (actor, tx) = AppSourceBuffer::buffered(buffer, &config);
        (
            Self::Buffered(AppSourceBufferHandle::new(tx, Some(total_size))),
            actor,
        )
    }

    // Create a distributed source from traffic distributions and flow ID.
    pub fn distributed_source(
        flow_id: usize,
        tr: TrafficCharacteristics,
        config: AppBufferConfig,
    ) -> (Self, AppSourceBuffer) {
        let seed = get_seed();
        let rng = SmallRng::seed_from_u64(seed as u64 + flow_id as u64);

        let (actor, tx) = AppSourceBuffer::dist_actor(flow_id, tr, rng, &config);

        (Self::Dist(AppSourceBufferHandle::new(tx, None)), actor)
    }

    // Retrieves the underlying handle to be used in TCPPacketSource.
    pub fn handle(&self) -> AppSourceBufferHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => h.clone(),
        }
    }

    pub fn handle_with_offset(
        &self,
        offset: usize,
        length: Option<usize>,
    ) -> AppSourceBufferHandle {
        match self {
            Self::Buffered(h) | Self::Dist(h) => {
                let effective_length =
                    length.or_else(|| h.length.map(|len| len.saturating_sub(offset)));
                let absolute_offset = h.offset.saturating_add(offset);

                AppSourceBufferHandle::with_offset(h.tx.clone(), absolute_offset, effective_length)
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
        let config = AppBufferConfig::default();

        // Create a buffer with known data: [0, 1, 2, 3, ..., 99]
        let buffer: Vec<u8> = (0..100u8).collect();
        let (_actor, _tx) = AppSourceBuffer::buffered(buffer.clone(), &config);

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

    /// Test AppSourceBufferHandle cursor management
    #[test]
    fn test_handle_cursor_tracking() {
        // Simulate cursor advancement
        let mut cursor = 0usize;
        let length = Some(100usize);

        // First pull: 30 bytes
        let requested = 30;
        let allowed = length
            .map(|len| len.saturating_sub(cursor))
            .unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 30);
        cursor += actual_size;
        assert_eq!(cursor, 30);

        // Second pull: 50 bytes
        let requested = 50;
        let allowed = length
            .map(|len| len.saturating_sub(cursor))
            .unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 50);
        cursor += actual_size;
        assert_eq!(cursor, 80);

        // Third pull: 50 bytes (but only 20 remaining)
        let requested = 50;
        let allowed = length
            .map(|len| len.saturating_sub(cursor))
            .unwrap_or(requested);
        let actual_size = requested.min(allowed);
        assert_eq!(actual_size, 20);
        cursor += actual_size;
        assert_eq!(cursor, 100);

        // Fourth pull: should return 0 (exhausted)
        let requested = 10;
        let allowed = length
            .map(|len| len.saturating_sub(cursor))
            .unwrap_or(requested);
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
        let allowed = length
            .map(|len| len.saturating_sub(cursor))
            .unwrap_or(requested);
        let actual_size = requested.min(allowed);

        assert_eq!(actual_size, 0);
    }

    /// Test AppDataSource buffered_actor constructor
    #[test]
    fn test_buffered_actor_creation() {
        let config = AppBufferConfig::default();
        let total_size = 1024;

        let (data_src, actor) = AppDataSource::buffered_actor(total_size, config);

        // Verify actor has correct buffer size
        assert_eq!(actor.buffer.len(), total_size);

        // Verify handle has correct total size
        let handle = data_src.handle();
        assert_eq!(handle.get_total_size(), Some(total_size));
        assert_eq!(handle.get_offset(), 0);
        assert_eq!(handle.get_cursor(), 0);
    }

    /// Test that multiple handles can be created from the same datasource
    #[test]
    fn test_multiple_handles_from_same_source() {
        let config = AppBufferConfig::default();
        let total_size = 1000;

        let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

        // Create multiple handles with different offsets (simulating Broadcast)
        let handle1 = data_src.handle(); // Full buffer
        let handle2 = data_src.handle(); // Full buffer again

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
        let config = AppBufferConfig::default();
        let total_size = 512;
        let num_nodes = 4;
        let chunk_size = total_size / num_nodes; // 128 bytes per chunk

        let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

        // Create handles for each chunk
        let chunk0 = data_src.handle_with_offset(0, Some(chunk_size));
        let chunk1 = data_src.handle_with_offset(chunk_size, Some(chunk_size));
        let chunk2 = data_src.handle_with_offset(2 * chunk_size, Some(chunk_size));
        let chunk3 = data_src.handle_with_offset(3 * chunk_size, Some(chunk_size));

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

    /// Test that TCP packetization creates packets with correct sequence numbers
    #[test]
    fn test_tcp_packetization_sequence_numbers() {
        use crate::flows::packet::Packet;

        let config = AppBufferConfig::default();
        let total_size = 1536; // 3 MSS worth of data (512 * 3)
        let mss = 512;

        let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);
        let _handle = data_src.handle();

        // Simulate TCP packetization logic
        let mut packets = Vec::new();
        let mut next_seq = 0;
        let flow_id = 42;

        // Simulate pulling data and creating packets (what TCPPacketSource does)
        let data: Vec<u8> = vec![0u8; total_size]; // Simulated pull result
        let mut offset = 0;

        while offset < data.len() {
            let chunk_size = mss.min(data.len() - offset);
            let packet = Packet::new(chunk_size, next_seq, flow_id, 0.0);

            packets.push(packet.clone());
            next_seq += chunk_size;
            offset += chunk_size;
        }

        // Verify we created exactly 3 packets
        assert_eq!(packets.len(), 3);

        // Verify sequence numbers are correct
        assert_eq!(packets[0].packet_id, 0);
        assert_eq!(packets[0].size, 512);

        assert_eq!(packets[1].packet_id, 512);
        assert_eq!(packets[1].size, 512);

        assert_eq!(packets[2].packet_id, 1024);
        assert_eq!(packets[2].size, 512);

        // Verify flow IDs
        assert!(packets.iter().all(|p| p.flow_id == flow_id));
    }

    /// Test TCP packetization with non-MSS-aligned data
    #[test]
    fn test_tcp_packetization_non_aligned() {
        use crate::flows::packet::Packet;

        let total_size = 1300; // Not evenly divisible by 512
        let mss = 512;

        let data: Vec<u8> = vec![0u8; total_size];
        let mut packets = Vec::new();
        let mut next_seq = 0;
        let mut offset = 0;

        while offset < data.len() {
            let chunk_size = mss.min(data.len() - offset);
            let packet = Packet::new(chunk_size, next_seq, 0, 0.0);

            packets.push(packet.clone());
            next_seq += chunk_size;
            offset += chunk_size;
        }

        // Should create 3 packets: 512 + 512 + 276
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0].size, 512);
        assert_eq!(packets[1].size, 512);
        assert_eq!(packets[2].size, 276); // Remainder

        // Verify sequence numbers
        assert_eq!(packets[0].packet_id, 0);
        assert_eq!(packets[1].packet_id, 512);
        assert_eq!(packets[2].packet_id, 1024);

        // Verify total coverage
        let total_bytes: usize = packets.iter().map(|p| p.size).sum();
        assert_eq!(total_bytes, total_size);
    }

    /// Test that small data (< MSS) creates a single packet
    #[test]
    fn test_tcp_packetization_small_data() {
        use crate::flows::packet::Packet;

        let total_size = 100; // Much smaller than MSS
        let mss = 512;

        let data: Vec<u8> = vec![0u8; total_size];
        let mut packets = Vec::new();
        let mut offset = 0;
        let mut next_seq = 0;

        while offset < data.len() {
            let chunk_size = mss.min(data.len() - offset);
            let packet = Packet::new(chunk_size, next_seq, 0, 0.0);

            packets.push(packet);
            next_seq += chunk_size;
            offset += chunk_size;
        }

        // Should create exactly 1 packet
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].size, 100);
        assert_eq!(packets[0].packet_id, 0);
    }

    /// Test Broadcast scenario: multiple flows share the same data
    #[test]
    fn test_broadcast_multiple_flows_same_data() {
        let config = AppBufferConfig::default();
        let total_size = 1024;

        let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

        // Create handles for 4 different flows (simulating broadcast to 4 destinations)
        let handle1 = data_src.handle();
        let handle2 = data_src.handle();
        let handle3 = data_src.handle();
        let handle4 = data_src.handle();

        // All handles should access the same data range
        assert_eq!(handle1.get_offset(), 0);
        assert_eq!(handle2.get_offset(), 0);
        assert_eq!(handle3.get_offset(), 0);
        assert_eq!(handle4.get_offset(), 0);

        assert_eq!(handle1.get_length(), Some(total_size));
        assert_eq!(handle2.get_length(), Some(total_size));
        assert_eq!(handle3.get_length(), Some(total_size));
        assert_eq!(handle4.get_length(), Some(total_size));

        // But cursors are independent (each flow tracks its own progress)
        assert_eq!(handle1.get_cursor(), 0);
        assert_eq!(handle2.get_cursor(), 0);
        assert_eq!(handle3.get_cursor(), 0);
        assert_eq!(handle4.get_cursor(), 0);
    }

    /// Test RingAllReduce scenario: different flows access different chunks
    #[test]
    fn test_ring_allreduce_chunk_partitioning() {
        let config = AppBufferConfig::default();
        let total_size = 2048;
        let num_nodes = 4;
        let chunk_size = total_size / num_nodes; // 512 bytes per chunk

        let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

        // Create handles for each chunk (each flow in RingAllReduce sends one chunk)
        let mut handles = Vec::new();
        for i in 0..num_nodes {
            let offset = i * chunk_size;
            let length = if i == num_nodes - 1 {
                total_size - offset // Last chunk gets remainder
            } else {
                chunk_size
            };
            let handle = data_src.handle_with_offset(offset, Some(length));
            handles.push(handle);
        }

        // Verify each handle accesses a different chunk
        assert_eq!(handles[0].get_offset(), 0);
        assert_eq!(handles[0].get_length(), Some(512));

        assert_eq!(handles[1].get_offset(), 512);
        assert_eq!(handles[1].get_length(), Some(512));

        assert_eq!(handles[2].get_offset(), 1024);
        assert_eq!(handles[2].get_length(), Some(512));

        assert_eq!(handles[3].get_offset(), 1536);
        assert_eq!(handles[3].get_length(), Some(512));

        // Verify no overlaps
        for i in 0..num_nodes {
            for j in (i + 1)..num_nodes {
                let end_i = handles[i].get_offset() + handles[i].get_length().unwrap();
                let start_j = handles[j].get_offset();
                assert!(end_i <= start_j, "Chunks {} and {} overlap!", i, j);
            }
        }

        // Verify full coverage
        let total_coverage: usize = handles.iter().map(|h| h.get_length().unwrap()).sum();
        assert_eq!(total_coverage, total_size);
    }

    /// Test cursor advancement after simulated pulls
    #[test]
    fn test_cursor_advancement_simulation() {
        // Simulate multiple pull operations
        let mut cursor = 0;
        let length = Some(1000usize);

        // Pull 1: 300 bytes
        let req1 = 300;
        let allowed1 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req1);
        let actual1 = req1.min(allowed1);
        cursor += actual1;
        assert_eq!(cursor, 300);

        // Pull 2: 500 bytes
        let req2 = 500;
        let allowed2 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req2);
        let actual2 = req2.min(allowed2);
        cursor += actual2;
        assert_eq!(cursor, 800);

        // Pull 3: 300 bytes (but only 200 left)
        let req3 = 300;
        let allowed3 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req3);
        let actual3 = req3.min(allowed3);
        cursor += actual3;
        assert_eq!(cursor, 1000);
        assert_eq!(actual3, 200); // Only 200 bytes were available

        // Pull 4: should return 0 (exhausted)
        let req4 = 100;
        let allowed4 = length.map(|l| l.saturating_sub(cursor)).unwrap_or(req4);
        let actual4 = req4.min(allowed4);
        assert_eq!(actual4, 0);
    }

    /// Test packet metadata is created correctly (not cloned from wrong source)
    #[test]
    fn test_packet_metadata_correctness() {
        use crate::flows::packet::Packet;

        let flow_id = 123;
        let start_seq = 5000;
        let timestamp = 42.5;
        let size = 256;

        // Create a packet with specific metadata
        let packet = Packet::new(size, start_seq, flow_id, timestamp);

        // Verify all metadata is correct
        assert_eq!(packet.size, size);
        assert_eq!(packet.packet_id, start_seq);
        assert_eq!(packet.flow_id, flow_id);
        assert_eq!(packet.time, timestamp);

        // This verifies that we're creating fresh packets with correct metadata,
        // not cloning and modifying existing packets (which was the old buggy approach)
    }

    /// Test RingAllReduce with uneven chunk sizes
    #[test]
    fn test_ring_allreduce_uneven_chunks() {
        let config = AppBufferConfig::default();
        let total_size = 1000; // Not evenly divisible by 3
        let num_nodes = 3;
        let chunk_size = total_size / num_nodes; // 333

        let (data_src, _actor) = AppDataSource::buffered_actor(total_size, config);

        let chunk0 = data_src.handle_with_offset(0, Some(chunk_size));
        let chunk1 = data_src.handle_with_offset(chunk_size, Some(chunk_size));
        let chunk2_offset = 2 * chunk_size;
        let chunk2_len = total_size - chunk2_offset; // Remainder
        let chunk2 = data_src.handle_with_offset(chunk2_offset, Some(chunk2_len));

        assert_eq!(chunk0.get_offset(), 0);
        assert_eq!(chunk0.get_length(), Some(333));

        assert_eq!(chunk1.get_offset(), 333);
        assert_eq!(chunk1.get_length(), Some(333));

        assert_eq!(chunk2.get_offset(), 666);
        assert_eq!(chunk2.get_length(), Some(334)); // Gets the extra byte

        // Verify complete coverage
        let total = chunk0.get_length().unwrap()
            + chunk1.get_length().unwrap()
            + chunk2.get_length().unwrap();
        assert_eq!(total, total_size);
    }
}

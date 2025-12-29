//! Implements application-level data sources using an actor-based model.
//!
//! This module provides `AppSourceBuffer` as an actor, which manages byte buffers and serves
//! byte-range requests from TCP sources via async channels. The actor is responsible *only*
//! for data management, not packetization - that responsibility belongs to the TCP layer
//! (`TCPPacketSource`).

use std::future::Future;
use std::time::Duration;

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::ports::Output;
use tachyonix::{Receiver, Sender, channel};

use crate::flows::packet::Packet;

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
// Each request sent to the AppSourceBuffer asks for `size` bytes of data. The `respond_to`
// channel is used to send back the result asynchronously.
#[derive(Debug)]
pub struct AppSourceRequest {
    /// Start reading at this byte position inside the stream.
    pub start: usize,
    /// Total number of bytes the requester wants to receive
    pub size: usize,
    /// One-shot channel used by the actor to send back the raw bytes.
    pub respond_to: Sender<Vec<u8>>,
}

pub struct AppSourceBufferHandle {
    /// Channel for sending requests to the app actor.
    tx: Sender<AppSourceRequest>,
    /// First byte in the shared buffer assigned to this handle.
    offset: usize,
    /// Number of bytes already consumed using this handle.
    cursor: usize,
    /// Total length of the slice exposed through this handle.
    length: Option<usize>,
    /// Underlying actor, present only on the primary handle.
    actor: Option<AppSourceBuffer>,
}

impl Clone for AppSourceBufferHandle {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            offset: self.offset,
            cursor: 0,
            length: self.length,
            actor: None,
        }
    }
}

impl AppSourceBufferHandle {
    /// Builds a handle (and actor) around the provided buffer.
    pub fn from_buffer(buffer: Vec<u8>, config: &AppBufferConfig) -> Self {
        let total_size = buffer.len();
        let (actor, tx) = AppSourceBuffer::new(buffer, config);

        Self {
            tx,
            offset: 0,
            cursor: 0,
            length: Some(total_size),
            actor: Some(actor),
        }
    }

    /// Creates a handle that starts at `offset` (ring all-reduce chunk) sharing
    /// the same actor.
    pub fn with_offset(&self, offset: usize, length: Option<usize>) -> Self {
        Self {
            tx: self.tx.clone(),
            offset,
            cursor: 0,
            length,
            actor: None,
        }
    }

    pub fn get_total_size(&self) -> Option<usize> {
        self.length
    }

    /// Gets the current offset (for testing and debugging)
    pub fn get_offset(&self) -> usize {
        self.offset
    }

    /// Gets the current cursor position (for testing and debugging)
    pub fn get_cursor(&self) -> usize {
        self.cursor
    }

    /// Gets the length constraint (for testing and debugging)
    pub fn get_length(&self) -> Option<usize> {
        self.length
    }

    // Used for the topology to register this actor as a nexosim task
    pub fn take_actor(&mut self) -> Option<AppSourceBuffer> {
        self.actor.take()
    }

    // Sends a pull request to the actor, and await the returned bytes.
    pub async fn pull(&mut self, size: usize) -> Vec<u8> {
        // clamps the requested size to the remaining bytes exposed by this handle
        let allowed = self
            .length
            .map(|len| len.saturating_sub(self.cursor))
            .unwrap_or(size);
        let req_size = size.min(allowed);

        if req_size == 0 {
            return Vec::new();
        }

        let (resp_tx, mut resp_rx) = channel(1);
        // sends the current cursor to the actor
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

    // sends a shutdown signal by sending a request with size = 0 (not actually handled yet)
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
    pub fn new(buffer: Vec<u8>, config: &AppBufferConfig) -> (Self, Sender<AppSourceRequest>) {
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
}

impl Model for AppSourceBuffer {
    async fn init(self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        // schedules the actor's run_once function after the configured initial interval
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
                // handles the shutdown signal
                if req.size == 0 {
                    log::debug!("[AppSourceBuffer] Received shutdown signal, terminating actor");

                    // Don't reschedule - actor terminates
                    return;
                }

                // simple byte-range extraction from the buffer
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

pub struct AppDataSource {
    handle: AppSourceBufferHandle,
}

impl AppDataSource {
    /// Builds a data source backed by a byte buffer of the specified size.
    pub fn create_source_buffer(total_size: usize, config: AppBufferConfig) -> Self {
        let buffer = vec![0u8; total_size];
        let handle = AppSourceBufferHandle::from_buffer(buffer, &config);
        Self { handle }
    }

    /// Retrieves the underlying handle to be used in TCPPacketSource.
    pub fn handle(&self) -> AppSourceBufferHandle {
        self.handle.clone()
    }

    pub fn handle_with_offset(
        &self,
        offset: usize,
        length: Option<usize>,
    ) -> AppSourceBufferHandle {
        let effective_length =
            length.or_else(|| self.handle.length.map(|len| len.saturating_sub(offset)));
        let absolute_offset = self.handle.offset.saturating_add(offset);

        self.handle.with_offset(absolute_offset, effective_length)
    }

    pub fn take_actor(&mut self) -> Option<AppSourceBuffer> {
        self.handle.take_actor()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::join;
    use tachyonix::TryRecvError;

    async fn respond_once(actor: &mut AppSourceBuffer) {
        let req = actor.rx.recv().await.expect("expected request");
        let start = req.start.min(actor.buffer.len());
        let end = (req.start + req.size).min(actor.buffer.len());
        let data = actor.buffer[start..end].to_vec();
        let _ = req.respond_to.send(data).await;
    }

    #[test]
    fn test_handle_pull_respects_offset_and_length() {
        let config = AppBufferConfig::default();
        let buffer: Vec<u8> = (0..20u8).collect();
        let mut handle = AppSourceBufferHandle::from_buffer(buffer, &config);
        let mut actor = handle.take_actor().expect("missing actor");

        let mut handle = handle.with_offset(5, Some(10));
        let data = block_on(async {
            let pull = handle.pull(6);
            let respond = respond_once(&mut actor);
            let (data, _) = join!(pull, respond);
            data
        });
        assert_eq!(data, vec![5u8, 6, 7, 8, 9, 10]);
        assert_eq!(handle.get_cursor(), 6);

        let data = block_on(async {
            let pull = handle.pull(10);
            let respond = respond_once(&mut actor);
            let (data, _) = join!(pull, respond);
            data
        });
        assert_eq!(data, vec![11u8, 12, 13, 14]);
        assert_eq!(handle.get_cursor(), 10);
    }

    #[test]
    fn test_handle_pull_zero_size_no_request() {
        let config = AppBufferConfig::default();
        let buffer: Vec<u8> = (0..10u8).collect();
        let mut handle = AppSourceBufferHandle::from_buffer(buffer, &config);
        let mut actor = handle.take_actor().expect("missing actor");

        let data = block_on(handle.pull(0));
        assert!(data.is_empty());
        assert_eq!(handle.get_cursor(), 0);
        assert!(matches!(actor.rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_handle_with_offset_infers_length() {
        let config = AppBufferConfig::default();
        let data_src = AppDataSource::create_source_buffer(1000, config);
        let handle = data_src.handle_with_offset(250, None);

        assert_eq!(handle.get_offset(), 250);
        assert_eq!(handle.get_length(), Some(750));
        assert_eq!(handle.get_cursor(), 0);
    }

    #[test]
    fn test_handle_clone_resets_cursor() {
        let config = AppBufferConfig::default();
        let buffer: Vec<u8> = (0..10u8).collect();
        let mut handle = AppSourceBufferHandle::from_buffer(buffer, &config);
        let mut actor = handle.take_actor().expect("missing actor");

        let _ = block_on(async {
            let pull = handle.pull(4);
            let respond = respond_once(&mut actor);
            let (data, _) = join!(pull, respond);
            data
        });
        assert_eq!(handle.get_cursor(), 4);

        let clone = handle.clone();
        assert_eq!(clone.get_cursor(), 0);
        assert_eq!(clone.get_offset(), handle.get_offset());
        assert_eq!(clone.get_length(), handle.get_length());
    }
}

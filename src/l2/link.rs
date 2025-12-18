//! A simple link serializer that transmits LinkFrames at a configured rate.

use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

use log::debug;
use tracing::instrument;

use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::l2::frame::LinkFrame;

pub struct Link {
    link_id: usize,
    /// locally maintained simulation time
    pub time: f64,
    /// bit rate in bits per second (0 for unlimited)
    rate: f64,
    /// pending frames
    queue: VecDeque<LinkFrame>,
    /// time until which the link is busy transmitting
    busy_until: f64,

    pub output: Output<LinkFrame>,
}

impl Link {
    pub fn new(link_id: usize, rate: f64) -> Link {
        Link {
            link_id,
            time: 0.0,
            rate,
            queue: VecDeque::new(),
            busy_until: 0.0,
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.link_id
    }

    #[instrument(skip(self, cx))]
    pub async fn frame_received(&mut self, frame: LinkFrame, cx: &mut Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
            assert!(
                (frame.time() - global_time).abs() <= 1e-7,
                "Timing mismatch: frame.time = {}, global_time = {}",
                frame.time(),
                global_time
            );
        }

        let frame_time = frame.time();
        self.queue.push_back(frame);

        if frame_time >= self.busy_until {
            self.run(frame_time, cx).await;
        }
    }

    #[instrument(skip(self))]
    pub async fn send(&mut self, frame: LinkFrame) {
        self.time = frame.time();
        self.output.send(frame).await;
    }

    #[instrument(skip(self, cx))]
    pub fn run<'a>(
        &'a mut self,
        now: f64,
        cx: &'a mut Context<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            #[cfg(feature = "test")]
            {
                let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
                assert!(
                    (now - global_time).abs() <= 1e-7,
                    "Timing mismatch: now = {}, global_time = {}",
                    now,
                    global_time
                );
            }

            self.time = now;

            if self.time == 0.0 {
                let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
                self.time = global_time;
            }

            if let Some(mut frame) = self.queue.pop_front() {
                let bytes = frame.size_bytes();
                let timeout = if self.rate > 0.0 {
                    bytes as f64 * 8.0 / self.rate
                } else {
                    0.0
                };

                frame.set_time(self.time + timeout);

                cx.schedule_event(Duration::from_secs_f64(timeout), Self::send, frame.clone())
                    .unwrap();
                cx.schedule_event(Duration::from_secs_f64(timeout), Self::run, now + timeout)
                    .unwrap();

                self.busy_until = self.time + timeout;

                debug!(
                    "Link {} will send frame ({} bytes) at time {:.3}. {} frames in queue.",
                    self.link_id,
                    bytes,
                    self.time + timeout,
                    self.queue.len()
                );
            }
        }
    }
}

impl Model for Link {}

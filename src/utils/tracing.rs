//! Implements lightweight concurrency tracing utilities.
//!
//! When enabled, Days installs a tracing layer (`ConcurrencyTrackerLayer`) that counts how many
//! Nexosim model tasks are currently being polled. A separate wall-clock sampler can be used to
//! compute an average concurrency during a simulation run.
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing_subscriber::{Layer, registry::LookupSpan};

use crate::ACTIVE_TASKS;
use crate::PEAK_ACTIVE_TASKS;
use crate::topos::topo::TracingConfig;

pub struct ConcurrencyTrackerLayer;

impl<S> Layer<S> for ConcurrencyTrackerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_enter(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        // Only count Nexosim's per-model execution span (entered once per task poll).
        // Counting every span would measure nesting rather than task concurrency.
        let metadata = span.metadata();
        if metadata.name() != "model" || metadata.target() != "nexosim" {
            return;
        }

        let active = ACTIVE_TASKS.fetch_add(1, Ordering::Relaxed) + 1;
        PEAK_ACTIVE_TASKS.fetch_max(active, Ordering::Relaxed);
    }

    fn on_exit(&self, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        let metadata = span.metadata();
        if metadata.name() != "model" || metadata.target() != "nexosim" {
            return;
        }

        ACTIVE_TASKS.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WallClockConcurrencyStats {
    pub average: f64,
    pub elapsed: Duration,
}

pub struct WallClockConcurrencySampler {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<WallClockConcurrencyStats>>,
}

impl WallClockConcurrencySampler {
    pub fn start(interval: Duration) -> Self {
        let interval = interval.max(Duration::from_micros(100));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let mut last_t = start;
            let mut last_v = ACTIVE_TASKS.load(Ordering::Relaxed);
            let mut area = 0.0f64;

            while !stop_clone.load(Ordering::Relaxed) {
                thread::park_timeout(interval);

                let now = Instant::now();
                let dt = now.duration_since(last_t).as_secs_f64();
                area += last_v as f64 * dt;
                last_t = now;
                last_v = ACTIVE_TASKS.load(Ordering::Relaxed);
            }

            let elapsed = start.elapsed();
            let average = if elapsed.is_zero() {
                0.0
            } else {
                area / elapsed.as_secs_f64()
            };

            WallClockConcurrencyStats { average, elapsed }
        });

        Self {
            stop,
            handle: Some(handle),
        }
    }

    pub fn stop(&mut self) -> Option<WallClockConcurrencyStats> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            return Some(handle.join().expect("wall-clock sampler thread panicked"));
        }
        None
    }
}

impl Drop for WallClockConcurrencySampler {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub fn is_tracing_active(config_path: &str) -> bool {
    let content = fs::read_to_string(config_path).expect("The configuration is not valid");

    // Obtain the concurrency tracing interval from the configuration file.
    let tracing_config: TracingConfig = toml::from_str(&content)
        .expect("Failed to deserialize the configuration of concurrency tracing");

    tracing_config.tracing_active.unwrap_or(false)
}

pub fn tracing_interval(config_path: &str) -> Option<Duration> {
    let content = fs::read_to_string(config_path).expect("The configuration is not valid");

    // Obtain the concurrency tracing interval from the configuration file.
    let tracing_config: TracingConfig = toml::from_str(&content)
        .expect("Failed to deserialize the configuration of concurrency tracing");

    if !tracing_config.tracing_active.unwrap_or(false) {
        return None;
    }

    let duration = tracing_config.duration.unwrap_or(1500.0);
    let interval_s = tracing_config.tracing_interval.unwrap_or(duration / 100.0);
    Some(Duration::from_secs_f64(interval_s))
}

pub fn start_wall_clock_concurrency_sampler(
    config_path: &str,
) -> Option<WallClockConcurrencySampler> {
    tracing_interval(config_path).map(WallClockConcurrencySampler::start)
}

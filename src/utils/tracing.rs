//! Implements a concurrency tracer struct that periodically reports the number of coroutines (tasks)
//! that are concurrently running.
use std::fs;
use std::sync::atomic::Ordering;
use std::time::Duration;

use log::info;
use tracing::Subscriber;
use tracing_subscriber::{registry::LookupSpan, Layer};

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::time::MonotonicTime;

use crate::topos::topo::TracingConfig;
use crate::ACTIVE_TASKS;

pub struct ConcurrencyTrackerLayer;

impl<S> Layer<S> for ConcurrencyTrackerLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_enter(&self, _id: &tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        ACTIVE_TASKS.fetch_add(1, Ordering::Relaxed);
    }

    fn on_exit(&self, _id: &tracing::span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        ACTIVE_TASKS.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct ConcurrencyTracer {
    active: bool,
    duration: f64,
    interval: f64,
    concurrency_stats: Vec<usize>,
}

pub fn is_tracing_active(config_path: &str) -> bool {
    let content = fs::read_to_string(config_path).expect("The configuration is not valid");

    // Obtain the concurrency tracing interval from the configuration file
    let tracing_config: TracingConfig = toml::from_str(&content)
        .expect("Failed to deserialize the configuration of concurrency tracing");

    tracing_config.tracing_active.unwrap_or(false)
}

impl ConcurrencyTracer {
    pub fn new(config_path: &str) -> ConcurrencyTracer {
        let content = fs::read_to_string(config_path).expect("The configuration is not valid");

        // Obtain the concurrency tracing interval from the configuration file
        let tracing_config: TracingConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of concurrency tracing");
        let active = tracing_config.tracing_active.unwrap_or(false);
        let duration = tracing_config.duration.unwrap_or(1500.);
        let interval = tracing_config.tracing_interval.unwrap_or(duration / 100.);

        ConcurrencyTracer {
            active,
            duration,
            interval,
            concurrency_stats: Vec::new(),
        }
    }

    fn current_concurrency(&self) -> usize {
        ACTIVE_TASKS.load(Ordering::Relaxed)
    }

    fn run(&mut self, _: (), cx: &mut Context<Self>) {
        if !self.active {
            return;
        }

        let concurrency = self.current_concurrency();
        self.concurrency_stats.push(concurrency);

        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        if now < self.duration {
            cx.schedule_event(Duration::from_secs_f64(self.interval), Self::run, ())
                .unwrap();
        } else {
            let max = self.concurrency_stats.iter().max().cloned().unwrap_or(0);

            // Calculate the sum
            let sum: usize = self.concurrency_stats.iter().sum();
            // Calculate the length of the vector
            let count = self.concurrency_stats.len() as f64;

            // Calculate average, avoid integer division by casting to f64
            let average = if count > 0.0 { sum as f64 / count } else { 0.0 };

            info!("Concurrency: max {}, average {}.", max, average)
        }
    }
}

impl Model for ConcurrencyTracer {
    async fn init(mut self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        self.run((), cx);
        self.into()
    }
}

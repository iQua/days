//! Implements a concurrency tracer struct that periodically reports the number of coroutines (tasks)
//! that are concurrently running.
use std::sync::atomic::Ordering;
use std::time::Duration;

use log::info;
use tracing::Subscriber;
use tracing_subscriber::{registry::LookupSpan, Layer};

use nexosim::model::{Context, InitializedModel, Model};
use nexosim::time::MonotonicTime;

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

pub fn current_concurrency() -> usize {
    ACTIVE_TASKS.load(Ordering::Relaxed)
}

pub struct ConcurrencyTracer {
    concurrency_stats: Vec<usize>,
}

impl ConcurrencyTracer {
    pub fn new() -> ConcurrencyTracer {
        ConcurrencyTracer {
            concurrency_stats: Vec::new(),
        }
    }

    async fn collect_stats<'a>(&'a mut self, _: (), cx: &'a mut Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();
        let concurrency = current_concurrency();

        // saves the history for future analysis
        self.concurrency_stats.push(concurrency);

        info!("Concurrency: {} at time {:.2}", concurrency, now);
    }

    fn run(&mut self, _: (), cx: &mut Context<Self>) {
        cx.schedule_periodic_event(
            Duration::from_secs_f64(0.01),
            Duration::from_secs_f64(0.01),
            Self::collect_stats,
            (),
        )
        .unwrap();
    }
}

impl Model for ConcurrencyTracer {
    async fn init(mut self, cx: &mut Context<Self>) -> InitializedModel<Self> {
        self.run((), cx);
        self.into()
    }
}

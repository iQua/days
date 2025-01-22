use std::sync::atomic::Ordering;

use tracing::Subscriber;
use tracing_subscriber::{registry::LookupSpan, Layer};

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

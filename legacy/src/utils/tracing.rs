//! Legacy-safe facade over shared concurrency tracing helpers.

use std::time::Duration;

pub use days::utils::tracing::{
    ConcurrencyTrackerLayer, WallClockConcurrencySampler, WallClockConcurrencyStats,
    current_concurrency, peak_concurrency, reset_peak_concurrency,
};

pub fn is_tracing_active(config_path: &str) -> bool {
    crate::validate_config(config_path).unwrap_or_else(|error| panic!("{error}"));
    days::utils::tracing::is_tracing_active(config_path)
}

pub fn tracing_interval(config_path: &str) -> Option<Duration> {
    crate::validate_config(config_path).unwrap_or_else(|error| panic!("{error}"));
    days::utils::tracing::tracing_interval(config_path)
}

pub fn start_wall_clock_concurrency_sampler(
    config_path: &str,
) -> Option<WallClockConcurrencySampler> {
    crate::validate_config(config_path).unwrap_or_else(|error| panic!("{error}"));
    days::utils::tracing::start_wall_clock_concurrency_sampler(config_path)
}

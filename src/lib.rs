//! Shared scenario, topology, and validation infrastructure for Days.

use std::fs;
pub mod scenario;
pub mod topos;
pub mod utils;
pub mod workload;

/// The `days` CLI's stock device capacity caps (`--channel-events-per-stream` overrides one lane).
/// Tests that run what the CLI runs share them.
pub const STOCK_CAPACITY_CAPS: days_executor::DeviceCapacityCaps =
    days_executor::DeviceCapacityCaps {
        fallback_fel_events_per_lp: Some(16_384),
        queue_packets_per_lp: Some(2_048),
        channel_events_per_stream: Some(2_048),
        remote_staging_events_per_lp: Some(2_048),
        outbox_events_total: Some(2_000_000),
        tcp_receiver_ranges_per_flow: Some(64),
        tcp_ledger_segments_per_flow: Some(4_096),
        observation_events_per_lp: Some(512),
    };

pub fn validate_config(config_path: &str) -> Result<(), String> {
    let content = fs::read_to_string(config_path)
        .map_err(|error| format!("Failed to read configuration file: {error}"))?;
    validate_config_text(&content)
}

/// [`validate_config`] of a scenario's text.
pub fn validate_config_text(content: &str) -> Result<(), String> {
    let config: toml::Value = toml::from_str(content)
        .map_err(|error| format!("Failed to parse configuration file: {error}"))?;
    let legacy_key = concat!("run_batch", "_size");

    let has_legacy_key = config
        .get("switch")
        .and_then(toml::Value::as_table)
        .is_some_and(|switch| switch.contains_key(legacy_key));

    if has_legacy_key {
        return Err(format!(
            "Configuration key `switch.{legacy_key}` was removed; \
             schedulers now select one packet per service start."
        ));
    }

    Ok(())
}

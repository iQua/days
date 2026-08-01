//! Shared scenario, topology, and validation infrastructure for Days.

use std::fs;
pub mod scenario;
pub mod topos;
pub mod utils;

pub fn validate_config(config_path: &str) -> Result<(), String> {
    let content = fs::read_to_string(config_path)
        .map_err(|error| format!("Failed to read configuration file: {error}"))?;
    let config: toml::Value = toml::from_str(&content)
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

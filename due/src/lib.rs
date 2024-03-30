use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Deserialize;

pub mod flows;
pub mod schedulers;
pub mod switches;
pub mod topos;
pub mod utils;

#[derive(Deserialize)]
pub struct SeedConfig {
    seed: usize,
}

static SEED: AtomicUsize = AtomicUsize::new(0);
static NUM_SWITCHES: AtomicUsize = AtomicUsize::new(0);
static SWITCH_ID: AtomicUsize = AtomicUsize::new(0);
static ENDPOINT_ID: AtomicUsize = AtomicUsize::new(0);
static SCHEDULER_ID: AtomicUsize = AtomicUsize::new(0);
static FLOW_ID: AtomicUsize = AtomicUsize::new(0);
static COLLECTIVE_ID: AtomicUsize = AtomicUsize::new(0);

pub fn seed_from_config(file_path: &str) -> usize {
    // reads the configuration
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: SeedConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    SEED.store(config.seed, Ordering::Relaxed);
    config.seed
}

pub fn get_seed() -> usize {
    SEED.load(Ordering::Relaxed)
}

pub fn num_switches() -> usize {
    NUM_SWITCHES.load(Ordering::Relaxed)
}

pub fn set_num_switches(num_switches: usize) {
    NUM_SWITCHES.store(num_switches, Ordering::Relaxed);
    ENDPOINT_ID.store(num_switches, Ordering::Relaxed);
}

pub fn next_switch_id() -> usize {
    SWITCH_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_endpoint_id() -> usize {
    ENDPOINT_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_scheduler_id() -> usize {
    SCHEDULER_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_flow_id() -> usize {
    FLOW_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_collective_id() -> usize {
    COLLECTIVE_ID.fetch_add(1, Ordering::Relaxed)
}

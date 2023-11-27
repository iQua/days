use std::cell::RefCell;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::rngs::SmallRng;
use serde::Deserialize;

use crate::sim::{RandomVar, Time};

pub mod flow;
pub mod flows;
pub mod schedulers;
pub mod sim;
pub mod switches;
pub mod topos;

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub queueing_delay: RandomVar,
    pub duration: Time,
}

#[derive(Deserialize)]
pub struct SeedConfig {
    seed: u64,
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type")]
pub enum DistributionInfo {
    Exp { lambda: f64 },
    Uniform { low: i64, high: i64 },
}

pub fn get_seed(file_path: &str) -> u64 {
    // reads the configuration
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: SeedConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    config.seed
}

static NUM_ELEMENTS: AtomicUsize = AtomicUsize::new(0);
static ELEMENT_ID: AtomicUsize = AtomicUsize::new(0);
static ENDPOINT_ID: AtomicUsize = AtomicUsize::new(0);
static SCHEDULER_ID: AtomicUsize = AtomicUsize::new(0);

pub fn num_elements() -> usize {
    NUM_ELEMENTS.load(Ordering::Relaxed)
}

pub fn set_num_elements(num_elements: usize) {
    NUM_ELEMENTS.store(num_elements, Ordering::Relaxed);
    ENDPOINT_ID.store(num_elements, Ordering::Relaxed);
}

pub fn next_element_id() -> usize {
    ELEMENT_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_endpoint_id() -> usize {
    ENDPOINT_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_scheduler_id() -> usize {
    SCHEDULER_ID.fetch_add(1, Ordering::Relaxed)
}

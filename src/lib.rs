//! Core library APIs and global state for the Days simulator.

use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::Deserialize;

pub mod flows;
#[cfg(feature = "l2")]
pub mod l2;
pub mod schedulers;
pub mod switches;
pub mod topos;
pub mod utils;

#[derive(Deserialize)]
pub struct SeedConfig {
    seed: usize,
}

// for tracking the number of active async tasks (coroutines)
static ACTIVE_TASKS: AtomicUsize = AtomicUsize::new(0);
// tracks the peak number of concurrently active tasks
static PEAK_ACTIVE_TASKS: AtomicUsize = AtomicUsize::new(0);

static SEED: AtomicUsize = AtomicUsize::new(0);
static NUM_SWITCHES: AtomicUsize = AtomicUsize::new(0);
static SWITCH_ID: AtomicUsize = AtomicUsize::new(0);
static ENDPOINT_ID: AtomicUsize = AtomicUsize::new(0);
static SCHEDULER_ID: AtomicUsize = AtomicUsize::new(0);
static FLOW_ID: AtomicUsize = AtomicUsize::new(0);
static COLLECTIVE_ID: AtomicUsize = AtomicUsize::new(0);
static LINK_ID: AtomicUsize = AtomicUsize::new(0);

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

pub fn update_next_flow_id(next_flow_id: usize) {
    FLOW_ID.store(next_flow_id, Ordering::Relaxed)
}

pub fn next_collective_id() -> usize {
    COLLECTIVE_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_link_id() -> usize {
    LINK_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn current_concurrency() -> usize {
    ACTIVE_TASKS.load(Ordering::Relaxed)
}

pub fn peak_concurrency() -> usize {
    PEAK_ACTIVE_TASKS.load(Ordering::Relaxed)
}

pub fn reset_peak_concurrency() {
    PEAK_ACTIVE_TASKS.store(0, Ordering::Relaxed);
}

pub fn run_simulation_from_config(config_path: &str) -> Result<(), String> {
    use crate::flows::collective::Collective;
    use crate::flows::flow::Flow;
    use crate::topos::build::build_graph;
    use crate::topos::topo::Topology;

    let _ = seed_from_config(config_path);

    let (graph, hosts) = build_graph(config_path).map_err(|e| e.to_string())?;
    let flows = Flow::flows_from_config(config_path, &hosts);
    let collectives = Collective::collectives_from_config(config_path, &hosts);

    let topology = Topology::new(config_path, graph.clone(), hosts, flows, collectives);
    topology.run(graph);
    Ok(())
}

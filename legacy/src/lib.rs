//! Nexosim-based legacy Days simulation engine.

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

pub use days::utils::tracing::{current_concurrency, peak_concurrency, reset_peak_concurrency};
pub use days::validate_config;

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
static LINK_ID: AtomicUsize = AtomicUsize::new(0);

pub fn seed_from_config(file_path: &str) -> usize {
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");
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
    FLOW_ID.store(next_flow_id, Ordering::Relaxed);
}

pub fn next_collective_id() -> usize {
    COLLECTIVE_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_link_id() -> usize {
    LINK_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn run_simulation_from_config(config_path: &str) -> Result<(), String> {
    use crate::flows::FlowSize;
    use crate::flows::collective::Collective;
    use crate::flows::flow::Flow;
    use crate::topos::build::build_graph;
    use crate::topos::topo::{Topology, validate_flow_routing};
    use crate::utils::exact_time::scenario_seconds_ns;
    use days::topos::config::UIConfig;

    validate_config(config_path)?;
    let content = fs::read_to_string(config_path)
        .map_err(|error| format!("failed to read simulation configuration: {error}"))?;
    let ui_config: UIConfig = toml::from_str(&content)
        .map_err(|error| format!("failed to parse simulation time configuration: {error}"))?;
    scenario_seconds_ns(ui_config.duration.unwrap_or(1500.0), "simulation duration")?;
    let _ = seed_from_config(config_path);

    let (graph, hosts) = build_graph(config_path).map_err(|error| error.to_string())?;
    let flows = Flow::try_flows_from_config_with_attachments(config_path, &hosts)?;
    validate_flow_routing(&graph, &flows)?;
    let collectives = Collective::collectives_from_config(config_path, hosts.host_ids());
    for (kind, id, traffic) in flows
        .iter()
        .map(|flow| ("flow", flow.id, &flow.traffic))
        .chain(
            collectives
                .iter()
                .map(|collective| ("collective", collective.id, &collective.traffic)),
        )
    {
        scenario_seconds_ns(traffic.initial_delay, &format!("{kind} {id} initial delay"))?;
        if let FlowSize::Duration(duration) = traffic.size {
            scenario_seconds_ns(duration, &format!("{kind} {id} duration"))?;
        }
    }

    let topology = Topology::new(config_path, graph.clone(), hosts, flows, collectives);
    topology.run(graph)
}

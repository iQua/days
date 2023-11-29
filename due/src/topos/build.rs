//! Provides builders for building specific types of topologies, or building
//! topologies based on the information given in a TOML configuration file.

use log::info;
use petgraph::graph::UnGraph;
use serde::Deserialize;
use std::fs;

use crate::switches::SchedulingDiscipline;

#[derive(Deserialize)]
struct NetworkGraph {
    edges: Vec<(u32, u32)>,
}

#[derive(Deserialize)]
pub struct FatTreeConfig {
    pub k: usize,
    pub port_rate: f64,
    pub capacity: usize,
    pub weights: Vec<usize>,
    pub discipline: SchedulingDiscipline,
}

/// This function is used to build a topology from a toml configuration file
pub fn build_graph(file_path: &str) -> UnGraph<usize, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the toml configuration file
    let graph: NetworkGraph =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    UnGraph::<usize, ()>::from_edges(graph.edges)
}

/// This function is used to build a fattree topology and its hosts.
pub fn build_fattree(file_path: &str) -> (UnGraph<usize, ()>, Vec<usize>) {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: FatTreeConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    info!("The k of fattree is {}.", config.k);

    let num_layer_switches = config.k.pow(2) / 2;
    let num_core_switches = config.k.pow(2) / 4;
    let layer_switches_per_pod = config.k / 2;
    let core_switches_per_agg = num_core_switches / layer_switches_per_pod;

    let mut edges: Vec<(u32, u32)> = Vec::new();

    // sets edges between edge-layer switches and aggregation-layer switches
    for edge_id in 0..num_layer_switches {
        let pod_id = edge_id / layer_switches_per_pod;
        let agg_start = num_layer_switches + pod_id * layer_switches_per_pod;
        for agg_id in agg_start..agg_start + layer_switches_per_pod {
            edges.push((edge_id as u32, agg_id as u32))
        }
    }

    // sets edges between aggregation-layer switches and core-layer switches
    for agg_id in num_layer_switches..2 * num_layer_switches {
        let core_group = agg_id % layer_switches_per_pod;
        let core_start = 2 * num_layer_switches + core_group * core_switches_per_agg;
        for core_id in core_start..core_start + core_switches_per_agg {
            edges.push((agg_id as u32, core_id as u32));
        }
    }

    // initializes the graph from edges
    let graph: UnGraph<usize, ()> = UnGraph::<usize, ()>::from_edges(edges);

    // distinguishes all hosts (edge switches)
    let hosts: Vec<usize> = (0..num_layer_switches).collect();

    (graph, hosts)
}

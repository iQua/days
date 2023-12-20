//! Provides builders for building specific types of topologies, or building
//! topologies based on the information given in a TOML configuration file.

use core::panic;
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

#[derive(Deserialize)]
pub struct TorusConfig {
    pub dim: usize,
    pub n: usize,
    pub port_rate: f64,
    pub capacity: usize,
    pub weights: Vec<usize>,
    pub discipline: SchedulingDiscipline,
}

/// Builds a topology from a configuration file.
pub fn build_graph(file_path: &str) -> UnGraph<usize, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the toml configuration file
    let graph: NetworkGraph =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    UnGraph::<usize, ()>::from_edges(graph.edges)
}

/// Builds a FatTree topology and its hosts.
pub fn build_fattree(file_path: &str) -> (UnGraph<usize, ()>, Vec<usize>) {
    // reads the configuration file
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

/// Builds a Torus topology and its hosts.
pub fn build_torus(file_path: &str) -> (UnGraph<usize, ()>, Vec<usize>) {
    // reads the configuration file
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: TorusConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    let dimension = config.dim as u32;
    let node_per_dim = config.n as u32;
    let total_node = node_per_dim.pow(dimension) as usize;

    info!(
        "The total nodes of {}D Torus is {}.",
        dimension,
        node_per_dim.pow(dimension)
    );

    let mut edges: Vec<(u32, u32)> = Vec::new();

    match dimension {
        1 => {
            for i in 0..node_per_dim {
                let start = i;
                let end = (i + 1) % node_per_dim;
                edges.push((start, end));
                edges.push((end, start));
            }
        }
        2 => {
            for i in 0..node_per_dim {
                for j in 0..node_per_dim {
                    let start = i + j * node_per_dim;
                    let end = (i + 1) % node_per_dim + j * node_per_dim;
                    edges.push((start, end));
                    edges.push((end, start));

                    let end = i + ((j + 1) % node_per_dim) * node_per_dim;
                    edges.push((start, end));
                    edges.push((end, start));
                }
            }
        }
        3 => {
            for i in 0..node_per_dim {
                for j in 0..node_per_dim {
                    for k in 0..node_per_dim {
                        let start = i + j * node_per_dim + k * node_per_dim.pow(2);
                        let end =
                            (i + 1) % node_per_dim + j * node_per_dim + k * node_per_dim.pow(2);
                        edges.push((start, end));
                        edges.push((end, start));

                        let end =
                            i + ((j + 1) % node_per_dim) * node_per_dim + k * node_per_dim.pow(2);
                        edges.push((start, end));
                        edges.push((end, start));

                        let end =
                            i + j * node_per_dim + ((k + 1) % node_per_dim) * node_per_dim.pow(2);
                        edges.push((start, end));
                        edges.push((end, start));
                    }
                }
            }
        }
        _ => {
            panic!("Only support 1D, 2D, and 3D Torus.")
        }
    }

    // initializes the graph from edges
    let graph: UnGraph<usize, ()> = UnGraph::<usize, ()>::from_edges(edges);

    // distinguishes all hosts
    let hosts: Vec<usize> = (0..total_node).collect();

    (graph, hosts)
}

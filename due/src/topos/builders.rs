//! This file provides builders for building the topology based on the
//! information given in a toml file.

use petgraph::graph::UnGraph;
use serde::Deserialize;
use std::{collections::HashMap, fs};

#[derive(Deserialize)]
struct Config {
    num_nodes: usize,
    edges: Vec<(usize, usize)>,
}

#[derive(Deserialize)]
struct FatTreeConfig {
    k: usize,
    port_rate: f64,
    capacity: usize,
    n_classes_per_port: usize,
}

/// This function is used to build a topology from a toml file
pub fn build(file_path: &str) -> UnGraph<usize, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: Config = toml::from_str(&content).expect("Failed to deserialize the toml file.");

    let mut graph = UnGraph::<usize, ()>::new_undirected();
    let mut indices = HashMap::new();

    // addes nodes for the graph
    for id in 0..config.num_nodes {
        let node_index = graph.add_node(id);
        indices.insert(id, node_index);
    }

    // adds edges for the graph
    for edge in config.edges {
        graph.add_edge(indices[&edge.0], indices[&edge.1], ());
    }

    graph
}

/// This function is used to build a fattree topology
pub fn build_fattree(file_path: &str) -> UnGraph<usize, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: FatTreeConfig =
        toml::from_str(&content).expect("Failed to deserialize the toml file.");

    println!(
        "k: {}, port_rate: {:.1}, capacity: {}, n_classes_per_port: {}",
        config.k, config.port_rate, config.capacity, config.n_classes_per_port
    );

    let num_edge_switches = config.k.pow(2) / 2;
    let num_regular_switches = config.k.pow(2) * 3 / 4;
    let num_switches = num_edge_switches + num_regular_switches;
    let mut graph = UnGraph::<usize, ()>::new_undirected();
    let mut indices = HashMap::new();

    // initializes nodes for all elements
    for id in 0..num_switches {
        let node_index = graph.add_node(id);
        indices.insert(id, node_index);
    }

    // TODO: connects nodes

    graph
}

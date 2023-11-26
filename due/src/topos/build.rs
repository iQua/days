//! This file provides builders for building the topology based on the
//! information given in a toml file.

use petgraph::graph::UnGraph;
use serde::Deserialize;
use std::{collections::HashMap, fs};

#[derive(Deserialize)]
struct Config {
    num_elements: usize,
    edges: Vec<(usize, usize)>,
}

#[derive(Deserialize)]
struct FatTreeConfig {
    k: usize,
}

/// This function is used to build a topology from a toml configuration file
pub fn build_graph(file_path: &str) -> UnGraph<usize, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("The configuration is not valid.");

    // deserializes the content of the toml configuration file
    let config: Config =
        toml::from_str(&content).expect("Failed to deserialize the configuration.");

    let mut graph = UnGraph::<usize, ()>::new_undirected();
    let mut indices = HashMap::new();

    // addes nodes to the graph
    for id in 0..config.num_elements {
        let node_index = graph.add_node(id);
        indices.insert(id, node_index);
    }

    // connects edges for the graph
    for edge in config.edges {
        graph.add_edge(indices[&edge.0], indices[&edge.1], ());
    }

    graph
}

/// This function is used to build a fattree topology and its hosts.
pub fn build_fattree(file_path: &str) -> (UnGraph<usize, ()>, Vec<usize>) {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("The configuration is not valid.");

    // deserializes the content of the toml file
    let config: FatTreeConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration.");

    println!("In build_fattree, k: {}", config.k);

    let num_layer_switches = config.k.pow(2) / 2;
    let num_core_switches = config.k.pow(2) / 4;
    let num_switches = 2 * num_layer_switches + num_core_switches;

    let layer_switches_per_pod = config.k / 2;
    let core_switches_per_agg = num_core_switches / layer_switches_per_pod;

    let mut graph = UnGraph::<usize, ()>::new_undirected();
    let mut indices = HashMap::new();
    let mut edges = Vec::new();

    // initializes nodes for all elements
    for id in 0..num_switches {
        let node_index = graph.add_node(id);
        indices.insert(id, node_index);
    }

    // sets edges between edge-layer switches and aggregation-layer switches
    for edge_id in 0..num_layer_switches {
        let pod_id = edge_id / layer_switches_per_pod;
        let agg_start = num_layer_switches + pod_id * layer_switches_per_pod;
        for agg_id in agg_start..agg_start + layer_switches_per_pod {
            edges.push((edge_id, agg_id))
        }
    }

    // sets edges between aggregation-layer switches and core-layer switches
    for agg_id in num_layer_switches..2 * num_layer_switches {
        let core_group = agg_id % layer_switches_per_pod;
        let core_start = 2 * num_layer_switches + core_group * core_switches_per_agg;
        for core_id in core_start..core_start + core_switches_per_agg {
            edges.push((agg_id, core_id));
        }
    }

    // connects all nodes based on edges
    for edge in edges {
        graph.add_edge(indices[&edge.0], indices[&edge.1], ());
    }

    // distinguishes all hosts (edge switches)
    let hosts: Vec<usize> = (0..num_layer_switches).collect();

    (graph, hosts)
}

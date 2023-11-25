use petgraph::graph::UnGraph;
use serde::Deserialize;
use statrs::distribution::{DiscreteUniform, Uniform};
use std::sync::Arc;
use std::{collections::HashMap, fs};

/// Types of elements in the topology
#[derive(Debug, Deserialize)]
pub enum NodeType {
    Edge,
    Regular,
}

#[derive(Debug)]
pub struct Node {
    pub id: usize,
    pub node_type: NodeType,
}

#[derive(Deserialize)]
struct TomlNode {
    id: usize,
    node_type: NodeType,
}

#[derive(Deserialize)]
struct Edges {
    pairs: Vec<(usize, usize)>,
}

#[derive(Deserialize)]
struct Config {
    nodes: Vec<TomlNode>,
    edges: Edges,
}

#[derive(Deserialize)]
struct FatTreeConfig {
    k: usize,
    port_rate: f64,
    capacity: usize,
    n_classes_per_port: usize,
}

/// This function is used to build a topology from a toml file
pub fn build(file_path: &str) -> UnGraph<Node, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: Config = toml::from_str(&content).expect("Failed to deserialize the toml file.");

    let mut graph = UnGraph::<Node, ()>::new_undirected();
    let mut indices = HashMap::new();

    // addes nodes for the graph
    for toml_node in config.nodes {
        let node = Node {
            id: toml_node.id,
            node_type: toml_node.node_type,
        };

        let index = graph.add_node(node);
        indices.insert(toml_node.id, index);
    }

    // adds edges for the graph
    for edge in config.edges.pairs {
        graph.add_edge(indices[&edge.0], indices[&edge.1], ());
    }

    graph
}

/// This function is used to build a fattree topology
pub fn build_fattree(file_path: &str) -> UnGraph<Node, ()> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: FatTreeConfig =
        toml::from_str(&content).expect("Failed to deserialize the toml file.");

    println!(
        "k: {}, port_rate: {:.1}, capacity: {}, n_classes_per_port: {}",
        config.k, config.port_rate, config.capacity, config.n_classes_per_port
    );

    let mut graph = UnGraph::<Node, ()>::new_undirected();

    // TODO: Put these in toml
    let arr_interval_dist = Arc::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // TODO: construct the fattree
    graph
}

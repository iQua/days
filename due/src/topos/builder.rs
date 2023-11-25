use petgraph::graph::UnGraph;
use serde::Deserialize;
use std::fs;

/// Types of elements in the topology
#[derive(Debug, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeType {
    Edge,
    Regular,
    Source,
    Sink,
}

#[derive(Debug)]
pub struct Node {
    pub id: usize,
    pub node_type: NodeType,
}

#[derive(Deserialize)]
struct Config {
    nodes: Vec<TomlNode>,
    edges: Edges,
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

/// This function is used to build a topology from a toml file
pub fn build(file_path: &str) -> UnGraph<Node, ()> {
    // read the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserialize the content of the toml file
    let config: Config = toml::from_str(&content).expect("Failed to deserialize the toml file.");

    let mut graph = UnGraph::<Node, ()>::new_undirected();
    let mut indices = Vec::new();

    for toml_node in config.nodes {
        let node = Node {
            id: toml_node.id,
            node_type: toml_node.node_type
        };

        // as indices is a Vec, need to make sure consecutive indices and start
        // from 0
        let index = graph.add_node(node);
        indices.push(index);
    }

    for edge in config.edges.pairs {
        println!("{:?}", edge);
        graph.add_edge(indices[edge.0], indices[edge.1], ());
    }

    println!("{:?}", graph);
    graph
}
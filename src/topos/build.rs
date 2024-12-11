use std::fs;

use log::{debug, info};

use petgraph::graph::UnGraph;
use serde::Deserialize;
use thiserror::Error;

use crate::topos::topo::{Config, FatTreeConfig, TopoCategory, TorusConfig};

#[derive(Error, Debug)]
pub enum TopologyError {
    #[error("Failed to read configuration file: {0}")]
    ConfigReadError(#[from] std::io::Error),

    #[error("Failed to parse TOML: {0}")]
    TomlParseError(#[from] toml::de::Error),

    #[error("Invalid topology configuration: {0}")]
    InvalidConfig(String),

    #[error("Unsupported torus dimension: {0}")]
    UnsupportedDimension(u32),
}

pub type Result<T> = std::result::Result<T, TopologyError>;

#[derive(Deserialize)]
struct NetworkGraph {
    edges: Vec<(u32, u32)>,
    hosts: Vec<usize>,
}

impl NetworkGraph {
    fn validate(&self) -> Result<()> {
        if self.edges.is_empty() {
            return Err(TopologyError::InvalidConfig("Empty edge list".into()));
        }
        if self.hosts.is_empty() {
            return Err(TopologyError::InvalidConfig("Empty host list".into()));
        }
        Ok(())
    }
}

trait TopologyBuilder {
    fn build(&self) -> Result<(UnGraph<usize, ()>, Vec<usize>)>;
}

impl TopologyBuilder for FatTreeConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
        let k = self.k;
        info!("Building FatTree topology with k = {}", k);

        let num_layer_switches = (k.pow(2) / 2) as u32;
        let num_core_switches = (k.pow(2) / 4) as u32;
        let layer_switches_per_pod = (k / 2) as u32;
        let core_switches_per_agg = num_core_switches / layer_switches_per_pod;

        let edges = build_fattree_edges(
            num_layer_switches,
            layer_switches_per_pod,
            core_switches_per_agg,
        );

        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts: Vec<usize> = (0..usize::try_from(num_layer_switches)
            .map_err(|_| TopologyError::InvalidConfig("Switch count overflow".into()))?)
            .collect();

        Ok((graph, hosts))
    }
}

impl TopologyBuilder for TorusConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
        let dimension = self.dim as u32;
        let nodes_per_dim = self.n as u32;

        if !(1..=3).contains(&dimension) {
            return Err(TopologyError::UnsupportedDimension(dimension));
        }

        let total_nodes = usize::try_from(nodes_per_dim.pow(dimension))
            .map_err(|_| TopologyError::InvalidConfig("Node count overflow".into()))?;

        info!(
            "Building {}D Torus topology with {} total nodes",
            dimension, total_nodes
        );

        let edges = build_torus_edges(dimension, nodes_per_dim)?;
        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts: Vec<usize> = (0..total_nodes).collect();

        Ok((graph, hosts))
    }
}

pub fn build_graph(file_path: &str) -> Result<(UnGraph<usize, ()>, Vec<usize>)> {
    let content = fs::read_to_string(file_path)?;
    let config: Config = toml::from_str(&content)?;

    match config.topology {
        Some(topo_config) => match topo_config.category {
            TopoCategory::FatTree => {
                debug!("Initializing FatTree graph");
                topo_config
                    .fat_tree
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing FatTree config".into()))?
                    .build()
            }
            TopoCategory::Torus => {
                debug!("Initializing Torus graph");
                topo_config
                    .torus
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing Torus config".into()))?
                    .build()
            }
        },
        None => {
            let graph_config: NetworkGraph = toml::from_str(&content)?;
            graph_config.validate()?;
            Ok((
                UnGraph::<usize, ()>::from_edges(&graph_config.edges),
                graph_config.hosts,
            ))
        }
    }
}

fn build_fattree_edges(
    num_layer_switches: u32, // Changed from usize to u32
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) -> Vec<(u32, u32)> {
    let mut edges = Vec::new();

    // Edge to aggregation layer connections
    for edge_id in 0..num_layer_switches {
        let pod_id = edge_id / layer_switches_per_pod;
        let agg_start = num_layer_switches + pod_id * layer_switches_per_pod;

        edges.extend(
            (agg_start..agg_start + layer_switches_per_pod).map(|agg_id| (edge_id, agg_id)),
        );
    }

    // Aggregation to core layer connections
    for agg_id in num_layer_switches..2 * num_layer_switches {
        let core_group = agg_id % layer_switches_per_pod;
        let core_start = 2 * num_layer_switches + core_group * core_switches_per_agg;

        edges.extend(
            (core_start..core_start + core_switches_per_agg).map(|core_id| (agg_id, core_id)),
        );
    }

    edges
}

fn build_torus_edges(dimension: u32, nodes_per_dim: u32) -> Result<Vec<(u32, u32)>> {
    let mut edges = Vec::new();

    match dimension {
        1 => {
            for i in 0..nodes_per_dim {
                add_torus_edge(&mut edges, i, (i + 1) % nodes_per_dim);
            }
        }
        2 => {
            for i in 0..nodes_per_dim {
                for j in 0..nodes_per_dim {
                    let current = i + j * nodes_per_dim;

                    // Horizontal connections
                    add_torus_edge(
                        &mut edges,
                        current,
                        (i + 1) % nodes_per_dim + j * nodes_per_dim,
                    );

                    // Vertical connections
                    add_torus_edge(
                        &mut edges,
                        current,
                        i + ((j + 1) % nodes_per_dim) * nodes_per_dim,
                    );
                }
            }
        }
        3 => {
            for i in 0..nodes_per_dim {
                for j in 0..nodes_per_dim {
                    for k in 0..nodes_per_dim {
                        let current = i + j * nodes_per_dim + k * nodes_per_dim.pow(2);

                        // X-axis connections
                        add_torus_edge(
                            &mut edges,
                            current,
                            (i + 1) % nodes_per_dim + j * nodes_per_dim + k * nodes_per_dim.pow(2),
                        );

                        // Y-axis connections
                        add_torus_edge(
                            &mut edges,
                            current,
                            i + ((j + 1) % nodes_per_dim) * nodes_per_dim
                                + k * nodes_per_dim.pow(2),
                        );

                        // Z-axis connections
                        add_torus_edge(
                            &mut edges,
                            current,
                            i + j * nodes_per_dim
                                + ((k + 1) % nodes_per_dim) * nodes_per_dim.pow(2),
                        );
                    }
                }
            }
        }
        _ => return Err(TopologyError::UnsupportedDimension(dimension)),
    }

    Ok(edges)
}

#[inline]
fn add_torus_edge(edges: &mut Vec<(u32, u32)>, from: u32, to: u32) {
    // Changed from usize to u32
    edges.push((from, to));
    edges.push((to, from));
}

//! Initializers for creating elements and endpoints based on the information
//! given in a configuration.

use std::collections::HashMap;
use std::{fs, sync::Arc};

use petgraph::graph::DiGraph;
use serde::Deserialize;

use crate::flows::flow::{DistributionInfo, Flow, FlowType};
use crate::next_flow_id;
use crate::sim::Time;
use crate::switches::splitter::Splitter;
use crate::switches::switch::PacketSwitch;
use crate::switches::{Element, SchedulingDiscipline};

#[derive(Deserialize)]
struct TomlSwitch {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
}

#[derive(Deserialize, Debug)]
struct TomlFlow {
    flow_type: FlowType,
    graph: Vec<(u32, u32)>,
    initial_delay: Time,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
}

#[derive(Deserialize)]
struct ElementConfig {
    num_splitters: usize,
    switch: Vec<TomlSwitch>,
}

#[derive(Deserialize, Debug)]
struct FlowConfig {
    flows: Vec<TomlFlow>,
}

pub fn init_elements(file_path: &str) -> Vec<Element> {
    // reads the configuration
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: ElementConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    let mut elements: Vec<Element> = Vec::new();

    for e in config.switch {
        println!(
            "{}, {}, {:?}, {:?}",
            e.port_rate, e.capacity, e.weights, e.discipline
        );

        let switch = PacketSwitch::new(
            e.port_rate,
            e.capacity,
            e.weights,
            HashMap::new(),
            e.discipline,
            Arc::new(|flow_id| flow_id),
        );
        elements.push(Element::PacketSwitch(switch));
    }

    for _ in 0..config.num_splitters {
        elements.push(Element::Splitter(Splitter::new()));
    }

    elements
}

// Initializes flows from a vector of directed graphs.
pub fn flows_from_graph(graphs: Vec<Vec<(u32, u32)>>) -> Vec<Flow> {
    let mut flows = Vec::new();

    for graph in graphs {
        let flow_graph = DiGraph::<usize, ()>::from_edges(&graph);

        flows.push(Flow::new(
            next_flow_id(),
            FlowType::PacketDistribution,
            flow_graph,
            0.,
            DistributionInfo::Exp { lambda: 1. },
            DistributionInfo::Uniform {
                low: 1000,
                high: 1000,
            },
        ));
    }

    flows
}

// Initializes flows from a configuration file.
pub fn flows_from_config(file_path: &str) -> Vec<Flow> {
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    let config: FlowConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    let mut flows = Vec::new();

    for flow in config.flows {
        let graph = DiGraph::<usize, ()>::from_edges(flow.graph);

        flows.push(Flow::new(
            next_flow_id(),
            flow.flow_type,
            graph,
            flow.initial_delay,
            flow.arr_dist,
            flow.pkt_size_dist,
        ));
    }

    flows
}

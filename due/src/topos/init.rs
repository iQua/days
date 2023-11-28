//! Initializers for creating elements and endpoints based on the information
//! given in a configuration.

use std::collections::HashMap;
use std::{fs, sync::Arc};

use petgraph::graph::DiGraph;
use serde::Deserialize;

use crate::flows::flow::Flow;
use crate::flows::sink::PacketSink;
use crate::flows::source::PacketSource;
use crate::flows::EndPoint;
use crate::sim::Time;
use crate::switches::splitter::Splitter;
use crate::switches::switch::PacketSwitch;
use crate::switches::{Element, SchedulingDiscipline};
use crate::DistributionInfo;

#[derive(Deserialize)]
struct TomlSwitch {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
}

#[derive(Deserialize, Debug)]
struct TomlFlow {
    id: usize,
    flow: Vec<Vec<usize>>,
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

pub fn init_endpoints(flows: Vec<Flow>) -> Vec<EndPoint> {
    let mut endpoints: Vec<EndPoint> = Vec::new();

    for flow in flows {
        // Need to set params for the pkt source!
        endpoints.push(EndPoint::PacketSource(PacketSource::new(flow.clone())));
        endpoints.push(EndPoint::PacketSink(PacketSink::new(flow.id)));
    }

    endpoints
}

// This function is used to initialize flows by a Vec of directed graphs.
pub fn init_flows(file_path: &str) -> Vec<Flow> {
    // reads the configuration
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: FlowConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    let mut flows = Vec::new();
    for e in config.flows {
        let mut graph = DiGraph::<usize, ()>::new();
        for pair in e.flow {
            // for the case that pair.len() > 1, there are more than one
            // generator for this flow, then the flow_id of that packet can
            // not be 'self.endpoint_id - num_elements()'
            let start = graph.add_node(pair[0]);
            let end = graph.add_node(pair[1]);
            graph.add_edge(start, end, ());
        }
        println!("Graph: {:?}", graph);

        flows.push(Flow::new(
            e.id,
            graph,
            e.initial_delay,
            e.arr_dist,
            e.pkt_size_dist,
        ));
    }
    flows
}

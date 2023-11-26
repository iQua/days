//! Initializers for creating elements and endpoints based on the information
//! given in a configuration.

use crate::{
    packets::{sink::PacketSink, source::PacketSource, splitter::Splitter},
    switches::{switch::PacketSwitch, SchedulingDiscipline},
    Element, EndPoint,
};
use serde::Deserialize;
use std::{fs, sync::Arc};

#[derive(Deserialize)]
struct TomlSwitch {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
    fib: Vec<usize>,
}

#[derive(Deserialize)]
struct TomlSource {
    initial_delay: f64,
}

#[derive(Deserialize)]
struct ElementConfig {
    num_splitters: usize,
    switch: Vec<TomlSwitch>,
}

#[derive(Deserialize)]
struct EndPointConfig {
    num_sinks: usize,
    source: Vec<TomlSource>,
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
            "{}, {}, {:?}, {:?}, {:?}",
            e.port_rate, e.capacity, e.weights, e.discipline, e.fib
        );

        let switch = PacketSwitch::new(
            e.port_rate,
            e.capacity,
            e.weights,
            e.fib,
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

pub fn init_endpoints(file_path: &str) -> Vec<EndPoint> {
    // reads the configuration
    let content = fs::read_to_string(file_path).expect("The configuration is not valid");

    // deserializes the content of the configuration
    let config: EndPointConfig =
        toml::from_str(&content).expect("Failed to deserialize the configuration");

    let mut endpoints: Vec<EndPoint> = Vec::new();

    for e in config.source {
        let source = PacketSource::new(e.initial_delay);
        endpoints.push(EndPoint::PacketSource(source));
    }

    for _ in 0..config.num_sinks {
        endpoints.push(EndPoint::PacketSink(PacketSink::new()));
    }

    endpoints
}

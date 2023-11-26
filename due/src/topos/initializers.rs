//! This file provides initializers for creating elements and endpoints based on
//! the information given in a toml file.

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
    discipline: String,
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
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: ElementConfig =
        toml::from_str(&content).expect("Failed to deserialize the toml file.");

    let mut elements: Vec<Element> = Vec::new();

    for e in config.switch {
        println!(
            "{}, {}, {:?}, {}, {:?}",
            e.port_rate, e.capacity, e.weights, e.discipline, e.fib
        );

        let switch = PacketSwitch::new(
            e.port_rate,
            e.capacity,
            e.weights,
            e.fib,
            get_discipline(&e.discipline),
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
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: EndPointConfig =
        toml::from_str(&content).expect("Failed to deserialize the toml file.");

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

/// This function uses scheduling discipline information in the toml file to get
/// the discipline. May be removed later by implementing Deserialize for SchedulingDiscipline.
fn get_discipline(discipline: &str) -> SchedulingDiscipline {
    match discipline {
        "FIFO" => SchedulingDiscipline::FIFO,
        "DRR" => SchedulingDiscipline::DRR,
        _ => {
            println!("Invalid discipline.");
            SchedulingDiscipline::FIFO
        }
    }
}

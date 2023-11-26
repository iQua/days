//! This file provides initializers for creating elements and endpoints based on
//! the information given in a toml file.

use crate::{
    packets::splitter::Splitter,
    switches::{switch::PacketSwitch, SchedulingDiscipline},
    Element,
};
use serde::Deserialize;
use std::{fs, sync::Arc};

#[derive(Deserialize)]
struct TomlElement {
    element_type: String,
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: String,
}

#[derive(Deserialize)]
struct ElementConfig {
    element: Vec<TomlElement>,
}

pub fn init_elements(file_path: &str) -> Vec<Element> {
    // reads the toml file
    let content = fs::read_to_string(file_path).expect("No valid TOML file.");

    // deserializes the content of the toml file
    let config: ElementConfig =
        toml::from_str(&content).expect("Failed to deserialize the toml file.");

    let mut elements: Vec<Element> = Vec::new();

    for e in config.element {
        println!(
            "{}, {}, {}, {:?}, {}",
            e.element_type, e.port_rate, e.capacity, e.weights, e.discipline
        );
        // todo: fib
        let fib = vec![0];
        match e.element_type.as_str() {
            "PacketSwitch" => {
                let switch = PacketSwitch::new(
                    e.port_rate,
                    e.capacity,
                    e.weights,
                    fib,
                    get_discipline(&e.discipline),
                    Arc::new(|flow_id| flow_id),
                );
                elements.push(Element::PacketSwitch(switch));
            }
            "Splitter" => {
                // todo: if the element is a splitter, then no parameters there!
                let splitter = Splitter::new();
                elements.push(Element::Splitter(splitter));
            }
            _ => {
                println!("Invalid element type.")
            }
        }
    }

    elements
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

//! Implements all the necessary utilities for initializing, constructing, and
//! running a network topology. These utilities include connecting network elements
//! according to a network graph, attaching packet endpoints to hosts, computing
//! feasible paths for all the flows, and installing Flow Information Base tables
//! to all the switches to route these flows accordingly.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use log::warn;
use petgraph::graph::UnGraph;
use serde::Deserialize;
use tokio::sync::mpsc::unbounded_channel;

use crate::flows::flow::Flow;
use crate::sim::SimContext;
use crate::switches::splitter::Splitter;
use crate::switches::switch::PacketSwitch;
use crate::switches::{Element, SchedulingDiscipline};
use crate::topos::build::FatTreeConfig;
use crate::{set_num_elements, Shared};

#[derive(Deserialize)]
struct TomlSwitch {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
}

#[derive(Deserialize)]
struct ElementConfig {
    num_splitters: usize,
    switch: Vec<TomlSwitch>,
}
pub struct Topology {
    /// Undirected graph of the topology
    graph: UnGraph<usize, ()>,
    /// A Vec of element ids that connects to endpoints
    hosts: Vec<usize>,
    /// A Vec of PacketSwitchs and Splitters
    elements: Vec<Element>,
    /// A Vec of all flows
    flows: Vec<Flow>,
}

impl Topology {
    pub fn new(
        file_path: &str,
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        flows: Vec<Flow>,
    ) -> Topology {
        set_num_elements(graph.node_count());

        // reads the configuration
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let elements: Vec<Element> = if let Ok(config) = toml::from_str::<FatTreeConfig>(&content) {
            Topology::init_fattree_elements(config)
        } else {
            let config: ElementConfig =
                toml::from_str(&content).expect("Failed to deserialize the configuration");
            Topology::init_elements(config)
        };

        Topology {
            graph: graph.clone(),
            hosts,
            flows,
            elements,
        }
    }

    fn init_elements(config: ElementConfig) -> Vec<Element> {
        let mut elements: Vec<Element> = Vec::new();

        for e in config.switch {
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

    fn init_fattree_elements(config: FatTreeConfig) -> Vec<Element> {
        let mut elements: Vec<Element> = Vec::new();
        let num_switches = config.k.pow(2) * 5 / 4;

        for _ in 0..num_switches {
            let weight_len = config.weights.len();
            let switch = PacketSwitch::new(
                config.port_rate,
                config.capacity,
                config.weights.clone(),
                HashMap::new(),
                config.discipline.clone(),
                Arc::new(move |flow_id| flow_id % weight_len),
            );
            elements.push(Element::PacketSwitch(switch));
        }

        elements
    }

    /// Connects a vector of elements according to edges in the network topology.
    fn connect(&mut self) {
        for node_id in self.graph.node_indices() {
            let (sender, receiver) = unbounded_channel();
            self.elements[node_id.index()].connect_receiver(receiver);

            for neighbor in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                if neighbor.index() != node_id.index() {
                    self.elements[neighbor.index()].connect_sender(node_id.index(), sender.clone());
                }
            }
        }
    }

    /// Attaches packet endpoints (sources or sinks) to hosts in the network graph.
    fn attach(&mut self) {
        // obtains the element_id of all end hosts (where endpoints can be
        // attached to), and initializes endpoints for all flows
        let mut attach_to = Vec::new();
        for flow in self.flows.iter_mut() {
            attach_to.extend(flow.get_hosts());
            flow.init_endpoints();
        }

        let mut endpoint_iter = self
            .flows
            .iter_mut()
            .flat_map(|flow| flow.endpoints.iter_mut());

        // attaches each endpoint's sender to its corresponding host's receiver
        for host_id in attach_to {
            // an element in the network must be a host, as specified by the
            // network graph
            assert!(self.hosts.contains(&host_id.index()));

            // locate a neighboring element in the network graph to this host
            let mut neighbors = self.graph.neighbors(host_id);

            let (downlink_sender, downlink_receiver) = unbounded_channel();
            let endpoint = endpoint_iter.next().unwrap();

            if let Some(next_neighbor) = neighbors.next() {
                if next_neighbor != host_id {
                    self.elements[next_neighbor.index()].connect_neighbour_to_endpoint(
                        endpoint,
                        downlink_receiver,
                        host_id.index(),
                    );
                }

                self.elements[host_id.index()].connect_sender(endpoint.id(), downlink_sender);
            } else {
                panic!("No neighbors found for host element {}", host_id.index());
            }
        }
    }

    /// Computes routing decisions for all the flows, and installs Flow
    /// Information Base tables (FIBs) of these routing decisions into all the
    /// switches.
    fn route(&mut self, sim: SimContext<'_, Shared>) {
        for flow in self.flows.iter_mut() {
            let paths = flow.compute_paths(self.graph.clone(), sim);

            for path in paths {
                for window in path.windows(2) {
                    let node_id = window.get(0).unwrap().index();
                    let next_id = window.get(1).unwrap().index();

                    match &mut self.elements[node_id] {
                        Element::PacketSwitch(switch) => {
                            switch.set_fib(flow.id, next_id);
                        }
                        _ => {
                            warn!(
                                "Element {} is not a packet switch when setting up the
                                Flow Information Base table along the path in flow {}.",
                                node_id, flow.id
                            );
                        }
                    }
                }
            }
        }
    }

    pub fn run(mut self, sim: SimContext<'_, Shared>) {
        // constructs the network graph with network elements
        self.connect();
        // attaches sources and sinks to hosts in the network graph
        self.attach();
        // computes feasible paths for all flows, and sets FIBs for all switches
        self.route(sim);

        for flow in self.flows {
            sim.activate(flow.run(sim));
        }

        for element in self.elements {
            element.activate(sim);
        }
    }
}

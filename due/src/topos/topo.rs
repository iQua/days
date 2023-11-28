use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use log::debug;
use petgraph::graph::UnGraph;
use serde::Deserialize;
use tokio::sync::mpsc::unbounded_channel;

use crate::flows::flow::Flow;
use crate::sim::SimContext;
use crate::switches::splitter::Splitter;
use crate::switches::switch::PacketSwitch;
use crate::switches::{Element, SchedulingDiscipline};
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

        Topology {
            graph: graph.clone(),
            hosts,
            flows,
            elements: Topology::init_elements(file_path),
        }
    }

    fn init_elements(file_path: &str) -> Vec<Element> {
        // reads the configuration
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        // deserializes the content of the configuration
        let config: ElementConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut elements: Vec<Element> = Vec::new();

        for e in config.switch {
            debug!(
                "Initialized a switch with port_rate: {}, capacity: {},\n weights: {:?}, discipline: {:?}",
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

    /// connects a vector of elements according to edges in the network topology.
    pub fn connect(&mut self) {
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

    /// attaches packet endpoints (sources or sinks) to hosts in the network graph.
    pub fn attach(&mut self) {
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

    /// computes routing decisions for all the flows, and installs Flow
    /// Information Base tables (FIBs) of these routing decisions into all the
    /// switches.
    pub fn set(&mut self, sim: SimContext<'_, Shared>) {
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
                            panic!(
                                "element {} will be skipped when setting fib in flow {}",
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
        // computes shortest paths for all flows, and sets fibs for all switches
        self.set(sim);

        for flow in self.flows {
            sim.activate(flow.run(sim));
        }

        for element in self.elements {
            element.activate(sim);
        }
    }
}

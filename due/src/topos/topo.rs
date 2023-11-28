use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use petgraph::graph::{NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use serde::Deserialize;
use tokio::sync::mpsc::unbounded_channel;

use crate::flows::flow::Flow;
use crate::flows::route::{RandomSimplePath, RoutingProtocol};
use crate::flows::sink::PacketSink;
use crate::flows::source::PacketSource;
use crate::flows::EndPoint;
use crate::sim::SimContext;
use crate::switches::splitter::Splitter;
use crate::switches::switch::PacketSwitch;
use crate::switches::{Element, SchedulingDiscipline};
use crate::Shared;

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
    /// A Vec of PacketSources and PacketSinks
    endpoints: Vec<EndPoint>,
    /// A Vec of all flows
    flows: Vec<Flow>,
    /// Routing module
    routing: RandomSimplePath,
}

impl Topology {
    pub fn new(
        file_path: &str,
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        flows: Vec<Flow>,
    ) -> Topology {
        // initializes endpoints based on flows
        let mut endpoints: Vec<EndPoint> = Vec::new();
        for flow in &flows {
            // TODO: this only works for flows with one pair of source and sink
            endpoints.push(EndPoint::PacketSource(PacketSource::new(flow.clone())));
            endpoints.push(EndPoint::PacketSink(PacketSink::new(flow.id)));
        }

        Topology {
            graph: graph.clone(),
            endpoints,
            hosts,
            flows,
            elements: Topology::init_elements(file_path),
            routing: RandomSimplePath::new(graph),
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
        // fetches NodeIndex of hosts for all paths
        let mut attach_to = Vec::new();
        for flow in &self.flows {
            for edge in flow.graph.edge_references() {
                attach_to.push(edge.source());
                attach_to.push(edge.target());
            }
        }

        // the number of endpoints should be equal to the number of hosts they
        // attach to
        assert_eq!(self.endpoints.len(), attach_to.len());

        let mut endpoint_iter = self.endpoints.iter_mut();

        // attaches each endpoint's sender to its corresponding host's receiver
        for host_id in attach_to {
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

    /// computes shortest paths for all flows, and sets fibs for all switches.
    pub fn set(&mut self, sim: SimContext<'_, Shared>) {
        for flow in &self.flows {
            for edge in flow.graph.edge_references() {
                // finds the index of start and end nodes of the path
                let start = edge.source().index();
                let end = edge.target().index();

                // get the simple path
                let path =
                    self.routing
                        .compute_route(NodeIndex::new(start), NodeIndex::new(end), sim);
                println!("The path of flow {}: {:?}", flow.id, path);

                for (idx, &node_idx) in path.iter().enumerate() {
                    // gets fibs for elements along the path
                    let mut next_id = usize::MAX;
                    let next_id = match idx < path.len() - 1 {
                        true => path[idx + 1].index(),
                        false => {
                            for endpoint in &self.endpoints {
                                if let EndPoint::PacketSink(sink) = endpoint {
                                    if sink.flow_id() == flow.id {
                                        next_id = sink.id()
                                    }
                                }
                            }
                            next_id
                        }
                    };

                    // sets fibs
                    if let Element::PacketSwitch(switch) = &mut self.elements[node_idx.index()] {
                        switch.set_fib(flow.id, next_id)
                    }
                }
            }
        }
    }

    pub fn run(mut self, sim: SimContext<'_, Shared>) {
        // constructs the network graph with network elements
        self.connect();
        // computes shortest paths for all flows, and sets fibs for all switches
        self.set(sim);
        // attaches sources and sinks to hosts in the network graph
        self.attach();

        for endpoint in self.endpoints {
            match endpoint {
                EndPoint::PacketSource(source) => {
                    sim.activate(source.run(sim));
                }
                EndPoint::PacketSink(sink) => {
                    sim.activate(sink.run(sim));
                }
            }
        }

        for element in self.elements {
            element.activate(sim);
        }
    }
}

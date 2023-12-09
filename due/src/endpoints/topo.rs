//! Implements all the necessary utilities for initializing, constructing, and
//! running a network topology. These utilities include connecting network switches
//! according to a network graph, attaching packet endpoints to hosts, computing
//! feasible paths for all the flows, and installing Flow Information Base tables
//! to all the switches to route these flows accordingly.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use log::info;
use petgraph::graph::UnGraph;
use serde::Deserialize;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use crate::endpoints::build::FatTreeConfig;
use crate::endpoints::flow::Flow;
use crate::endpoints::switch::PacketSwitch;
use crate::endpoints::SchedulingDiscipline;
use crate::set_num_switches;

#[derive(Deserialize)]
struct TomlSwitch {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
}

#[derive(Deserialize)]
struct SwitchConfig {
    switch: Vec<TomlSwitch>,
}
pub struct Topology {
    /// The simulation engine
    sim_init: SimInit,
    /// Undirected graph of the topology
    graph: UnGraph<usize, ()>,
    /// A vector of element ids that connects to endpoints
    hosts: Vec<usize>,
    /// A vector of PacketSwitches
    switches: Vec<PacketSwitch>,
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
        set_num_switches(graph.node_count());

        // reads the configuration
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let switches: Vec<PacketSwitch> =
            if let Ok(config) = toml::from_str::<FatTreeConfig>(&content) {
                Topology::init_fattree_switches(config)
            } else {
                let config: SwitchConfig =
                    toml::from_str(&content).expect("Failed to deserialize the configuration");
                Topology::init_switches(config)
            };

        Topology {
            sim_init: SimInit::new(),
            graph: graph.clone(),
            hosts,
            flows,
            switches,
        }
    }

    fn init_switches(config: SwitchConfig) -> Vec<PacketSwitch> {
        let mut switches: Vec<PacketSwitch> = Vec::new();

        for e in config.switch {
            let switch = PacketSwitch::new(HashMap::new(), Arc::new(|flow_id| flow_id));
            switches.push(switch);
        }

        switches
    }

    fn init_fattree_switches(config: FatTreeConfig) -> Vec<PacketSwitch> {
        let mut switches: Vec<PacketSwitch> = Vec::new();
        let num_switches = config.k.pow(2) * 5 / 4;

        for _ in 0..num_switches {
            let switch = PacketSwitch::new(HashMap::new(), Arc::new(|flow_id| flow_id));
            switches.push(switch);
        }

        switches
    }

    /// Connects a vector of packet switches according to edges in the network topology.
    fn connect(&mut self) {
        let switch_mailboxes = HashMap::new();

        for node_id in self.graph.node_indices() {
            let switch_mbox = Mailbox::new();
            switch_mailboxes.insert(node_id.index(), switch_mbox);

            for neighbor in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                if neighbor.index() != node_id.index() {
                    self.switches[neighbor.index()].connect_sender(node_id.index(), sender.clone());
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
                    self.switches[next_neighbor.index()].connect_neighbour_to_endpoint(
                        endpoint,
                        downlink_receiver,
                        host_id.index(),
                    );
                }

                self.switches[host_id.index()].connect_sender(endpoint.id(), downlink_sender);
            } else {
                panic!("No neighbors found for host element {}", host_id.index());
            }
        }
    }

    /// Computes routing decisions for all the flows, and installs Flow
    /// Information Base tables (FIBs) of these routing decisions into all the
    /// switches.
    fn route(&mut self) {
        for flow in self.flows.iter_mut() {
            let paths = flow.compute_paths(self.graph.clone());

            for path in paths {
                for window in path.windows(2) {
                    let node_id = window.get(0).unwrap().index();
                    let next_id = window.get(1).unwrap().index();
                    self.switches[node_id].set_fib(flow.id, next_id);
                }
            }
        }
    }

    pub fn run(mut self) {
        // constructs the network graph with network switches
        self.connect();
        // attaches sources and sinks to hosts in the network graph
        self.attach();
        // computes feasible paths for all flows, and sets FIBs for all switches
        self.route();

        // starts the simulation
        let t0 = MonotonicTime::EPOCH;
        let mut sim = self.sim_init.init(t0);
        sim.step_by(Duration::from_secs(100));

        info!(
            "Simulation completed at time {:.3}.",
            sim.time().duration_since(t0).as_secs_f64()
        );
    }
}

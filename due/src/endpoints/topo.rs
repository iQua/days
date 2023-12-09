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

use asynchronix::model::Output;
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use crate::endpoints::build::FatTreeConfig;
use crate::endpoints::drop::{CapacityUnit, DropStrategy};
use crate::endpoints::drr::DRRServer;
use crate::endpoints::flow::Flow;
use crate::endpoints::port::Port;
use crate::endpoints::sink::PacketSink;
use crate::endpoints::source::PacketSource;
use crate::endpoints::switch::PacketSwitch;
use crate::endpoints::{EndPoint, Scheduler, SchedulingDiscipline};
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
pub enum Config {
    SwitchConfig(SwitchConfig),
    FatTreeConfig(FatTreeConfig),
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
    /// A vector of all flows
    flows: Vec<Flow>,
    /// A hash map of switch mailboxes
    switch_mailboxes: HashMap<usize, Mailbox<PacketSwitch>>,
    /// Configuration of the topology
    config: Config,
}

impl Topology {
    pub fn new(
        file_path: &str,
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        flows: Vec<Flow>,
    ) -> Topology {
        set_num_switches(graph.node_count());
        let switch_mailboxes = HashMap::new();

        // reads the configuration
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");
        if let Ok(config) = toml::from_str::<FatTreeConfig>(&content) {
            let switches = Topology::init_fattree_switches(&config);

            Topology {
                sim_init: SimInit::new(),
                graph: graph.clone(),
                hosts,
                switches,
                flows,
                switch_mailboxes,
                config: Config::FatTreeConfig(config),
            }
        } else {
            let config: SwitchConfig =
                toml::from_str(&content).expect("Failed to deserialize the configuration");
            let switches = Topology::init_switches(&config);

            Topology {
                sim_init: SimInit::new(),
                graph: graph.clone(),
                hosts,
                switches,
                flows,
                switch_mailboxes,
                config: Config::SwitchConfig(config),
            }
        }
    }

    fn init_switches(config: &SwitchConfig) -> Vec<PacketSwitch> {
        let mut switches: Vec<PacketSwitch> = Vec::new();

        for _ in config.switch.iter() {
            let switch = PacketSwitch::new(HashMap::new(), Arc::new(|flow_id| flow_id));
            switches.push(switch);
        }

        switches
    }

    fn init_fattree_switches(config: &FatTreeConfig) -> Vec<PacketSwitch> {
        let mut switches: Vec<PacketSwitch> = Vec::new();
        let num_switches = config.k.pow(2) * 5 / 4;

        for _ in 0..num_switches {
            let switch = PacketSwitch::new(HashMap::new(), Arc::new(|flow_id| flow_id));
            switches.push(switch);
        }

        switches
    }

    fn connect_neighbours(
        &mut self,
        upstream_id: usize,
        downstream_id: usize,
        downstream_mbox: &Mailbox<PacketSwitch>,
    ) {
        let upstream_switch = &mut self.switches[upstream_id];
        let scheduler: Scheduler = match &self.config {
            Config::SwitchConfig(config) => match config.switch[upstream_id].discipline {
                SchedulingDiscipline::DRR => {
                    let server = DRRServer::new(
                        config.switch[upstream_id].port_rate,
                        config.switch[upstream_id].capacity,
                        CapacityUnit::Packets,
                        Arc::new(|flow_id| flow_id),
                        DropStrategy::TailDrop,
                        config.switch[upstream_id].weights.clone(),
                    );

                    Scheduler::DRRServer(server)
                }
                SchedulingDiscipline::FIFO => {
                    let server = Port::new(
                        config.switch[upstream_id].port_rate,
                        config.switch[upstream_id].capacity,
                        CapacityUnit::Packets,
                        DropStrategy::TailDrop,
                    );

                    Scheduler::Port(server)
                }
            },
            Config::FatTreeConfig(config) => match config.discipline {
                SchedulingDiscipline::DRR => {
                    let server = DRRServer::new(
                        config.port_rate,
                        config.capacity,
                        CapacityUnit::Packets,
                        Arc::new(|flow_id| flow_id),
                        DropStrategy::TailDrop,
                        config.weights.clone(),
                    );

                    Scheduler::DRRServer(server)
                }
                SchedulingDiscipline::FIFO => {
                    let server = Port::new(
                        config.port_rate,
                        config.capacity,
                        CapacityUnit::Packets,
                        DropStrategy::TailDrop,
                    );

                    Scheduler::Port(server)
                }
            },
        };

        let mut output = Output::default();

        match scheduler {
            Scheduler::DRRServer(mut drr_server) => {
                let scheduler_mbox: Mailbox<DRRServer> = Mailbox::new();
                output.connect(DRRServer::packet_received, &scheduler_mbox);
                upstream_switch.outputs.insert(downstream_id, output);
                drr_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
            }

            Scheduler::Port(mut port) => {
                let scheduler_mbox: Mailbox<Port> = Mailbox::new();
                output.connect(Port::packet_received, &scheduler_mbox);
                upstream_switch.outputs.insert(downstream_id, output);
                port.output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
            }
        }
    }

    /// Connects a vector of packet switches according to edges in the network topology.
    fn connect(&mut self) {
        self.switch_mailboxes = HashMap::new();

        for node_id in self.graph.node_indices() {
            let switch_mbox = Mailbox::new();

            for neighbor in self.graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them
                if neighbor.index() != node_id.index() {
                    self.connect_neighbours(neighbor.index(), node_id.index(), &switch_mbox);
                }
            }

            self.switch_mailboxes.insert(node_id.index(), switch_mbox);
        }
    }

    fn activate_switches(mut self) {
        for node_id in self.graph.node_indices() {
            let switch = self.switches.remove(node_id.index());
            let switch_mbox = self.switch_mailboxes.remove(&node_id.index()).unwrap();

            self.sim_init = self.sim_init.add_model(switch, switch_mbox);
        }
    }

    /// Attaches packet endpoints (sources or sinks) to hosts in the network graph.
    fn attach(&mut self) {
        // obtains the upstream_id of all end hosts (where endpoints can be
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
            // obtains the next endpoint
            if let Some(endpoint) = endpoint_iter.next() {
                // obtains the host's mailbox
                let host_mbox = self.switch_mailboxes.get(&host_id.index()).unwrap();

                // establishes a bi-directional connection between the endpoint and the host
                match endpoint {
                    EndPoint::PacketSource(source) => {
                        let source_mbox: Mailbox<PacketSource> = Mailbox::new();

                        source
                            .output
                            .connect(PacketSwitch::packet_received, host_mbox);

                        let mut output = Output::default();
                        let host = &mut self.switches[host_id.index()];
                        output.connect(PacketSource::packet_received, &source_mbox);
                        host.outputs.insert(source.id(), output);
                    }
                    EndPoint::PacketSink(sink) => {
                        let sink_mbox: Mailbox<PacketSink> = Mailbox::new();

                        sink.output
                            .connect(PacketSwitch::packet_received, host_mbox);

                        let mut output = Output::default();
                        let host = &mut self.switches[host_id.index()];
                        output.connect(PacketSink::packet_received, &sink_mbox);
                        host.outputs.insert(sink.id(), output);
                    }
                }
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

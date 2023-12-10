//! Implements all the necessary utilities for initializing, constructing, and
//! running a network topology. These utilities include connecting network switches
//! according to a network graph, attaching packet endpoints to hosts, computing
//! feasible paths for all the flows, and installing Flow Information Base tables
//! to all the switches to route these flows accordingly.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info};
use petgraph::graph::UnGraph;
use serde::Deserialize;

use asynchronix::model::Output;
use asynchronix::simulation::{Mailbox, SimInit, Simulation};
use asynchronix::time::MonotonicTime;

use crate::endpoints::build::FatTreeConfig;
use crate::endpoints::drop::{CapacityUnit, DropStrategy};
use crate::endpoints::drr::DRRServer;
use crate::endpoints::flow::Flow;
use crate::endpoints::port::Port;
use crate::endpoints::sink::PacketSink;
use crate::endpoints::source::PacketSource;
use crate::endpoints::switch::PacketSwitch;
use crate::endpoints::{Scheduler, SchedulingDiscipline};
use crate::set_num_switches;

#[derive(Deserialize)]
struct TomlSwitch {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
}

#[derive(Deserialize)]
pub struct SwitchConfig {
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
    /// A hash map of element ids that connects to endpoints
    hosts: Vec<usize>,
    /// A hash map of packet switches
    switches: HashMap<usize, PacketSwitch>,
    /// A hash map of packet sources and sinks
    sources: HashMap<usize, PacketSource>,
    sinks: HashMap<usize, PacketSink>,
    /// A hash map of all flows
    flows: Vec<Flow>,
    /// Hash maps of switch and endpoint mailboxes
    switch_mailboxes: HashMap<usize, Mailbox<PacketSwitch>>,
    source_mailboxes: HashMap<usize, Mailbox<PacketSource>>,
    sink_mailboxes: HashMap<usize, Mailbox<PacketSink>>,
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

        // reads the configuration
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");
        if let Ok(config) = toml::from_str::<FatTreeConfig>(&content) {
            let switches = Topology::init_fattree_switches(&config);

            Topology {
                sim_init: SimInit::new(),
                graph: graph.clone(),
                hosts,
                switches,
                sources: HashMap::new(),
                sinks: HashMap::new(),
                flows,
                switch_mailboxes: HashMap::new(),
                source_mailboxes: HashMap::new(),
                sink_mailboxes: HashMap::new(),
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
                sources: HashMap::new(),
                sinks: HashMap::new(),
                flows,
                switch_mailboxes: HashMap::new(),
                source_mailboxes: HashMap::new(),
                sink_mailboxes: HashMap::new(),
                config: Config::SwitchConfig(config),
            }
        }
    }

    fn init_switches(config: &SwitchConfig) -> HashMap<usize, PacketSwitch> {
        debug!("Initializing {} switches.", config.switch.len());
        let mut switches: HashMap<usize, PacketSwitch> = HashMap::new();

        for _ in config.switch.iter() {
            let switch = PacketSwitch::new(HashMap::new(), Arc::new(|flow_id| flow_id));
            println!("Creating a new switch with id = {}", switch.id());
            switches.insert(switch.id(), switch);
        }

        switches
    }

    fn init_fattree_switches(config: &FatTreeConfig) -> HashMap<usize, PacketSwitch> {
        let mut switches: HashMap<usize, PacketSwitch> = HashMap::new();
        let num_switches = config.k.pow(2) * 5 / 4;
        debug!("Initializing {} FatTree switches.", num_switches);

        for _ in 0..num_switches {
            let switch = PacketSwitch::new(HashMap::new(), Arc::new(|flow_id| flow_id));
            switches.insert(switch.id(), switch);
        }

        switches
    }

    // Initializes endpoints for the flow
    pub fn init_endpoints(&mut self) {
        debug!("Initializing endpoints for {} flows.", self.flows.len());

        for flow in self.flows.iter_mut() {
            for (edge_index, _) in flow.graph.edge_references().enumerate() {
                let source = PacketSource::new(
                    flow.id,
                    flow.initial_delay,
                    flow.duration,
                    flow.arr_dist,
                    flow.pkt_size_dist,
                );
                self.sources.insert(source.id(), source);

                let sink = PacketSink::new(flow.id);
                // record the packet sink ids for later construction of paths
                // in Flow::compute_paths()
                flow.sink_ids.insert(edge_index, sink.id());
                self.sinks.insert(sink.id(), sink);
            }
        }
    }

    fn connect_neighbours(
        &mut self,
        upstream_id: usize,
        downstream_id: usize,
        downstream_mbox: &Mailbox<PacketSwitch>,
    ) {
        debug!(
            "Connecting switch {} with switch {}.",
            upstream_id, downstream_id
        );
        let upstream_switch = self.switches.get_mut(&upstream_id).unwrap();
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

    /// Connects a hash map of packet switches according to edges in the network topology.
    fn connect(&mut self, graph: UnGraph<usize, ()>) {
        debug!(
            "Connecting {} switches according to the network topology.",
            self.switches.len()
        );
        self.switch_mailboxes = HashMap::new();

        for node_id in self.graph.node_indices() {
            let switch_mbox = Mailbox::new();

            for neighbor in graph.neighbors(node_id) {
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

    /// Activates all the switches and endpoints in the topology.
    fn activate(mut self) -> Simulation {
        debug!(
            "Activating all {} switches, {} packet sources, and {} packet sinks.",
            self.switches.len(),
            self.sources.len(),
            self.sinks.len()
        );

        for (_, switch) in self.switches {
            let switch_mbox = self.switch_mailboxes.remove(&switch.id()).unwrap();
            println!("Activating switch {}", switch.id());
            self.sim_init = self.sim_init.add_model(switch, switch_mbox);
        }

        for (_, source) in self.sources {
            let source_mbox = self.source_mailboxes.remove(&source.id()).unwrap();
            println!("Activating source {}", source.id());
            self.sim_init = self.sim_init.add_model(source, source_mbox);
        }

        for (_, sink) in self.sinks {
            let sink_mbox = self.sink_mailboxes.remove(&sink.id()).unwrap();
            println!("Activating sink {}", sink.id());
            self.sim_init = self.sim_init.add_model(sink, sink_mbox);
        }

        self.sim_init.init(MonotonicTime::EPOCH)
    }

    /// Attaches packet endpoints (sources or sinks) to hosts in the network graph.
    fn attach(&mut self) {
        // obtains the upstream_id of all end hosts (where endpoints can be
        // attached to), and initializes endpoints for all flows
        let mut attach_to = Vec::new();
        for flow in self.flows.iter_mut() {
            attach_to.extend(flow.get_hosts());
        }

        // creates and initializes all packet sources and sinks
        self.init_endpoints();

        // attaches each endpoint's output to its corresponding host's mailbox
        let mut host_iter = attach_to.iter();
        let mut source_iter = self.sources.iter_mut();
        let mut sink_iter = self.sinks.iter_mut();

        while let Some(host_id) = host_iter.next() {
            // an element in the network must be a host, as specified by the
            // network graph
            assert!(self.hosts.contains(&host_id.index()));
            // obtains the next packet source
            if let Some((_, source)) = source_iter.next() {
                // obtains the host's mailbox
                let host_mbox = self.switch_mailboxes.get(&host_id.index()).unwrap();

                // establishes a bi-directional connection between the packet source and the host
                let source_mbox: Mailbox<PacketSource> = Mailbox::new();

                source
                    .output
                    .connect(PacketSwitch::packet_received, host_mbox);

                let mut output = Output::default();
                let host = self.switches.get_mut(&host_id.index()).unwrap();
                println!("Connecting source {} to host {}.", source.id(), host.id());
                output.connect(PacketSource::packet_received, &source_mbox);
                host.outputs.insert(source.id(), output);

                self.source_mailboxes.insert(source.id(), source_mbox);
            }

            let host_id = host_iter.next().unwrap();

            // obtains the next packet sink
            if let Some((_, sink)) = sink_iter.next() {
                // obtains the host's mailbox
                let host_mbox = self.switch_mailboxes.get(&host_id.index()).unwrap();

                // establishes a bi-directional connection between the packet source and the host
                let sink_mbox: Mailbox<PacketSink> = Mailbox::new();

                sink.output
                    .connect(PacketSwitch::packet_received, host_mbox);

                let mut output = Output::default();
                let host = self.switches.get_mut(&host_id.index()).unwrap();
                println!("Connecting sink {} to host {}.", sink.id(), host.id());
                output.connect(PacketSink::packet_received, &sink_mbox);
                host.outputs.insert(sink.id(), output);

                self.sink_mailboxes.insert(sink.id(), sink_mbox);
            }
        }
    }

    /// Computes routing decisions for all the flows, and installs Flow
    /// Information Base tables (FIBs) of these routing decisions into all the
    /// switches.
    fn route(&mut self) {
        debug!("Computing routing decisions for all flows.");
        for flow in self.flows.iter_mut() {
            let paths = flow.compute_paths(self.graph.clone());

            for path in paths {
                println!("path");
                for window in path.windows(2) {
                    let node_id = window.get(0).unwrap().index();
                    let next_id = window.get(1).unwrap().index();
                    println!("upstream_id = {}, downstream_id = {}", node_id, next_id);
                    let switch = self.switches.get_mut(&node_id).unwrap();
                    switch.set_fib(flow.id, next_id);
                }
            }
        }
    }

    pub fn run(mut self, graph: UnGraph<usize, ()>) {
        // constructs the network graph with network switches
        self.connect(graph);
        // attaches sources and sinks to hosts in the network graph
        self.attach();
        // computes feasible paths for all flows, and sets FIBs for all switches
        self.route();
        // activates the switches and endpoints
        let mut sim = self.activate();

        // starts the simulation
        sim.step_by(Duration::from_secs(100));

        info!(
            "Simulation completed at time {:.3}.",
            sim.time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64()
        );
    }
}

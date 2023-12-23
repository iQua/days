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
use asynchronix::simulation::{Address, EventSlot, Mailbox, SimInit, Simulation};
use asynchronix::time::MonotonicTime;

use crate::flows::collective::{Collective, CollectiveType};
use crate::flows::flow::Flow;
use crate::flows::sink::{PacketSink, PacketStatistics};
use crate::flows::source::PacketSource;
use crate::schedulers::drop::{CapacityUnit, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::schedulers::wfq::WFQServer;
use crate::switches::switch::PacketSwitch;
use crate::switches::SchedulingDiscipline;
use crate::{next_flow_id, num_switches, set_num_switches};

#[derive(Deserialize)]
pub struct SwitchConfig {
    port_rate: f64,
    capacity: usize,
    weights: Vec<usize>,
    discipline: SchedulingDiscipline,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum TopoCategory {
    FatTree,
    Torus,
}

#[derive(Deserialize)]
pub struct FatTreeConfig {
    pub k: usize,
}

#[derive(Deserialize)]
pub struct TorusConfig {
    pub dim: usize,
    pub n: usize,
}

#[derive(Deserialize)]
pub struct TopoConfig {
    pub category: TopoCategory,
    pub fat_tree: Option<FatTreeConfig>,
    pub torus: Option<TorusConfig>,
}

#[derive(Deserialize)]
pub struct Config {
    pub switch: SwitchConfig,
    pub topology: Option<TopoConfig>,
}

#[derive(Default)]
struct SinkStatistics {
    // A vector of sink ids
    sink_ids: Vec<usize>,
    // sink id -> sink mailbox address
    sink_addresses: HashMap<usize, Address<PacketSink>>,
    // sink id -> sink statistics
    sink_statistics: HashMap<usize, EventSlot<PacketStatistics>>,
}

impl SinkStatistics {
    /// Collects and outputs the packet statistics at all sinks after the
    /// simulation finishes.
    pub fn collect_statistics(&mut self, mut sim: Simulation) -> Simulation {
        for sink_id in self.sink_ids.iter() {
            let sink_addr = self.sink_addresses.get(sink_id).unwrap();
            sim.send_event(PacketSink::report, *sink_id, sink_addr);

            let mut sink_statistics = self.sink_statistics.remove(sink_id).unwrap();
            if let Some(statistics) = sink_statistics.take() {
                info!("{:#.3}", statistics);
            }
        }

        sim
    }
}

pub struct Topology {
    /// The simulation engine
    sim_init: SimInit,
    /// Undirected graph of the topology
    graph: UnGraph<usize, ()>,
    /// A hash map of element ids that connects to endpoints
    hosts: Vec<usize>,
    /// A hash map of packet switches and their mailboxes
    switches: HashMap<usize, PacketSwitch>,
    switch_mailboxes: HashMap<usize, Mailbox<PacketSwitch>>,
    /// A vector of all flows
    flows: Vec<Flow>,
    /// A vector of all collectives
    collectives: Vec<Collective>,
    /// Configuration of packet switches in the topology
    switch_config: SwitchConfig,
}

impl Topology {
    pub fn new(
        file_path: &str,
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        flows: Vec<Flow>,
        collectives: Vec<Collective>,
    ) -> Topology {
        // reads the configuration
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: Config =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        set_num_switches(graph.node_count());
        let switches = Topology::init_switches();

        Topology {
            sim_init: SimInit::new(),
            graph: graph.clone(),
            hosts,
            switches,
            flows,
            collectives,
            switch_mailboxes: HashMap::new(),
            switch_config: config.switch,
        }
    }

    // Initializes mailboxes for switches.
    fn init_mailboxes(&mut self) {
        for (_, switch) in self.switches.iter() {
            let switch_mbox: Mailbox<PacketSwitch> = Mailbox::new();
            self.switch_mailboxes.insert(switch.id(), switch_mbox);
        }
    }

    fn init_switches() -> HashMap<usize, PacketSwitch> {
        let mut switches: HashMap<usize, PacketSwitch> = HashMap::new();

        for _ in 0..num_switches() {
            let switch = PacketSwitch::new(HashMap::new());
            switches.insert(switch.id(), switch);
        }

        switches
    }

    /// Connects a hash map of packet switches according to edges in a network
    /// topology.
    fn connect(mut self, graph: UnGraph<usize, ()>) -> Self {
        for node_id in graph.node_indices() {
            for neighbor in graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them and activate all schedulers in between
                if neighbor.index() != node_id.index() {
                    self = self.connect_neighbours(neighbor.index(), node_id.index());
                }
            }
        }

        self
    }

    /// Produces flows within all collectives in the network graph.
    fn process_collectives(&mut self) {
        info!(
            "Producing flows in all {} collective communication operations.",
            self.collectives.len()
        );

        // constructs, attaches, and routes flows in each collective
        for collective in self.collectives.iter_mut() {
            for &source in collective.sources.iter() {
                for &sink in collective.sinks.iter() {
                    let flow_id = next_flow_id();
                    match collective.collective_type {
                        CollectiveType::Broadcast => {
                            self.flows.push(Flow::new(
                                flow_id,
                                collective.flow_type,
                                source,
                                sink,
                                collective.traffic,
                                // uses collective_id as the random seed for the
                                // flow, which ensures that all flows in the
                                // broadcast have the same arrival and size
                                // distribution
                                collective.id,
                            ));
                            debug!(
                                "Produced Flow {} of Broadcast collective communication operation {}.",
                                flow_id, collective.id
                            );
                        }
                        CollectiveType::Gather => {
                            self.flows.push(Flow::new(
                                flow_id,
                                collective.flow_type,
                                source,
                                sink,
                                collective.traffic,
                                // uses flow_id as the random seed for the flow,
                                // which ensures that different flows have
                                // different arrival and size distributions
                                flow_id,
                            ));
                            debug!(
                                "Produced Flow {} of Gather collective communication operation {}.",
                                flow_id, collective.id
                            );
                        }
                        CollectiveType::AllReduce => {
                            self.flows.push(Flow::new(
                                flow_id,
                                collective.flow_type,
                                source,
                                sink,
                                collective.traffic,
                                // uses the source host's id as the random seed
                                // for the flow, which ensures that different
                                // hosts have different arrival and size
                                // distributions, but packet sources attached to
                                // the same host have the same distribution
                                source,
                            ));
                            debug!(
                                "Produced Flow {} of AllReduce collective communication operation {}.",
                                flow_id, collective.id
                            );
                        }
                    }
                }
            }
        }
    }

    /// Connects two adjacent switches in the network graph.
    fn connect_neighbours(mut self, upstream_id: usize, downstream_id: usize) -> Self {
        let upstream_switch = self.switches.get_mut(&upstream_id).unwrap();
        let weight_len = self.switch_config.weights.len();
        match self.switch_config.discipline {
            SchedulingDiscipline::DRR => {
                let mut drr_server = DRRServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weight_len),
                    DropStrategy::TailDrop,
                    self.switch_config.weights.clone(),
                );
                let mut output = Output::default();
                let drr_mbox: Mailbox<DRRServer> = Mailbox::new();
                output.connect(DRRServer::packet_received, &drr_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                drr_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);

                self.sim_init = self.sim_init.add_model(drr_server, drr_mbox);
            }

            SchedulingDiscipline::FIFO => {
                let mut port = Port::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    DropStrategy::TailDrop,
                );
                let mut output = Output::default();
                let port_mbox: Mailbox<Port> = Mailbox::new();
                output.connect(Port::packet_received, &port_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                port.output
                    .connect(PacketSwitch::packet_received, downstream_mbox);

                self.sim_init = self.sim_init.add_model(port, port_mbox);
            }

            SchedulingDiscipline::WFQ => {
                let mut wfq_server = WFQServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weight_len),
                    DropStrategy::TailDrop,
                    self.switch_config.weights.clone(),
                );

                let mut output = Output::default();
                let wfq_mbox: Mailbox<WFQServer> = Mailbox::new();
                output.connect(WFQServer::packet_received, &wfq_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                wfq_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);

                self.sim_init = self.sim_init.add_model(wfq_server, wfq_mbox);
            }
        }

        self
    }

    /// Attaches packet sources and sinks from the flows to hosts in the network
    /// graph.
    fn attach_flows(mut self, stats: &mut SinkStatistics) -> Self {
        info!(
            "Attaching packet sources and sinks to their hosts in all {} flows.",
            self.flows.len()
        );

        for flow in self.flows.iter_mut() {
            // creates and attaches a packet source and sink for each flow

            // packet sources and sinks must be attached to hosts
            assert!(self.hosts.contains(&flow.source_host));
            assert!(self.hosts.contains(&flow.sink_host));

            // creates a new packet source
            let mut source = PacketSource::new(flow.id, flow.traffic, flow.seed);

            // obtains the host switch and its mailbox for the packet source
            let source_host = self.switches.get_mut(&flow.source_host).unwrap();
            let host_mbox = self.switch_mailboxes.get(&flow.source_host).unwrap();

            // establishes a bi-directional connection between the packet source and the host
            let source_mbox: Mailbox<PacketSource> = Mailbox::new();
            source
                .output
                .connect(PacketSwitch::packet_received, host_mbox);
            let mut output = Output::default();
            output.connect(PacketSource::packet_received, &source_mbox);
            source_host.outputs.insert(source.id(), output);

            // activates the packet source
            self.sim_init = self.sim_init.add_model(source, source_mbox);

            // creates a new packet sink
            let mut sink = PacketSink::new(flow.id);

            // obtains the host switch and its mailbox for the packet sink
            let sink_host = self.switches.get_mut(&flow.sink_host).unwrap();
            let host_mbox = self.switch_mailboxes.get(&flow.sink_host).unwrap();

            // establishes a bi-directional connection between the packet sink and the host
            let sink_mbox: Mailbox<PacketSink> = Mailbox::new();

            // record the packet sink ids for later construction of paths in
            // Flow::compute_paths()
            flow.sink_id = sink.id();

            // records the sink ids, sink mailbox's address and sink
            // statistics event slot for the retrieval of packet statistics
            // after the simulation finishes
            stats.sink_ids.push(sink.id());
            stats.sink_addresses.insert(sink.id(), sink_mbox.address());
            stats
                .sink_statistics
                .insert(sink.id(), sink.statistics.connect_slot().0);

            sink.output
                .connect(PacketSwitch::packet_received, host_mbox);
            let mut output = Output::default();
            output.connect(PacketSink::packet_received, &sink_mbox);
            sink_host.outputs.insert(sink.id(), output);

            // activates the packet sink
            self.sim_init = self.sim_init.add_model(sink, sink_mbox);
        }

        self
    }

    /// Computes routing decisions for all the flows, and installs Flow
    /// Information Base tables (FIBs) of these routing decisions into all the
    /// switches.
    fn route_flows(&mut self) {
        info!(
            "Computing routing decisions for all {} flows.",
            self.flows.len()
        );

        for flow in self.flows.iter_mut() {
            let path = flow.compute_path(self.graph.clone());

            debug!("The path for flow {} is: {:?}", flow.id, path);
            for window in path.windows(2) {
                let node_id = window.get(0).unwrap().index();
                let next_id = window.get(1).unwrap().index();
                let switch = self.switches.get_mut(&node_id).unwrap();
                switch.set_fib(flow.id, next_id);
            }
        }
    }

    /// Activates all the switches and initializes the simulation.
    fn init_sim(mut self) -> Simulation {
        info!(
            "Activating all {} switches and initializing the simulation.",
            self.switches.len(),
        );

        for (_, switch) in self.switches {
            let switch_mbox = self.switch_mailboxes.remove(&switch.id()).unwrap();
            self.sim_init = self.sim_init.add_model(switch, switch_mbox);
        }

        self.sim_init.init(MonotonicTime::EPOCH)
    }

    pub fn run(mut self, graph: UnGraph<usize, ()>) {
        let mut statistics = SinkStatistics::default();

        // initializes mailboxes for the packet switches
        self.init_mailboxes();

        // produces flows within all collectives in the network graph
        self.process_collectives();

        // constructs the network graph by connecting the packet switches
        self = self.connect(graph);

        // attaches packet sources and sinks from flows to hosts in the network graph
        self = self.attach_flows(&mut statistics);

        // computes feasible paths for all flows, and sets FIBs for all switches
        self.route_flows();

        // activates all the switches and initializes the simulation
        let mut sim = self.init_sim();

        // starts the simulation
        sim.step_by(Duration::from_secs(1500));
        sim = statistics.collect_statistics(sim);

        info!(
            "Simulation completed at time {:.3}.",
            sim.time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64()
        );
    }
}

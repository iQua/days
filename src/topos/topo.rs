//! Implements all the necessary utilities for initializing, constructing, and
//! running a network topology. These utilities include connecting network
//! switches according to a network graph, attaching packet endpoints to hosts,
//! computing feasible paths for all the flows, and installing Flow Information
//! Base tables to all the switches to route these flows accordingly.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info};
use petgraph::graph::UnGraph;
use serde::Deserialize;

use nexosim::ports::{EventSlot, Output};
use nexosim::simulation::{Address, Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;

use crate::flows::collective::{Collective, CollectiveType};
use crate::flows::flow::{Flow, FlowType};
use crate::flows::sink::{PacketSink, PacketStatistics};
use crate::flows::source::PacketSource;
use crate::schedulers::drop::{CapacityUnit, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::schedulers::sp::SPServer;
use crate::schedulers::vc::VirtualClockServer;
use crate::schedulers::wfq::WFQServer;
use crate::switches::switch::PacketSwitch;
use crate::switches::SchedulingDiscipline;
use crate::utils::ui::UserInterface;
use crate::{num_switches, set_num_switches, set_update_interval};

#[derive(Deserialize)]
struct UIConfig {
    update_interval: Option<f64>,
    duration: Option<f64>,
}

#[derive(Deserialize)]
struct ConcurrencyConfig {
    num_threads: Option<usize>,
}

#[derive(Deserialize)]
pub struct SwitchConfig {
    port_rate: f64,
    capacity: usize,
    discipline: SchedulingDiscipline,
    drop: DropStrategy,
    weights: Option<Vec<usize>>,
    priorities: Option<HashMap<usize, usize>>,
    vticks: Option<HashMap<usize, usize>>,
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

#[derive(Deserialize)]
struct MailboxConfig {
    mailbox_capacity: Option<usize>,
}

#[derive(Default)]
struct SinkStatistics {
    /// a vector of sink ids
    sink_ids: Vec<usize>,
    /// sink id -> sink mailbox address
    sink_addresses: HashMap<usize, Address<PacketSink>>,
    /// sink id -> sink statistics
    sink_statistics: HashMap<usize, EventSlot<PacketStatistics>>,
}

impl SinkStatistics {
    /// Collects and outputs the packet statistics at all sinks after the
    /// simulation finishes.
    pub fn collect_statistics(&mut self, mut sim: Simulation) -> Simulation {
        for sink_id in self.sink_ids.iter() {
            let sink_addr = self.sink_addresses.get(sink_id).unwrap();
            let _ = sim.process_event(PacketSink::report, *sink_id, sink_addr);

            let mut sink_statistics = self.sink_statistics.remove(sink_id).unwrap();
            if let Some(statistics) = sink_statistics.next() {
                debug!("{:#.3}", statistics);
            }
        }

        sim
    }
}

pub struct Topology {
    /// the simulation engine
    sim_init: SimInit,
    /// undirected graph of the topology
    graph: UnGraph<usize, ()>,
    /// a hash map of switch ids that connects to endpoints
    hosts: Vec<usize>,
    /// switch id -> switch
    switches: HashMap<usize, PacketSwitch>,
    /// switch id -> switch mailbox
    switch_mailboxes: HashMap<usize, Mailbox<PacketSwitch>>,
    /// a vector of all flows
    flows: Vec<Flow>,
    /// a vector of all collectives
    collectives: Vec<Collective>,
    /// configuration of packet switches in the topology
    switch_config: SwitchConfig,
    /// the capacity of every mailbox
    mailbox_capacity: usize,
    /// the duration of the simulation
    duration: f64,
    /// the path to the configuration file
    file_path: String,
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

        let mailbox_config: MailboxConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of mailbox capacity");
        // uses 16 as the default value and limits the maximial capacity to
        // usize::MAX/2 + 1 as it is designed in nexosim
        let mailbox_capacity = mailbox_config
            .mailbox_capacity
            .unwrap_or(16)
            .min(usize::MAX / 2 + 1);

        let ui_config: UIConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of the user interface");
        let duration = ui_config.duration.unwrap_or(1500.);
        set_update_interval(ui_config.update_interval.unwrap_or(f64::MAX));

        let concurrency_config: ConcurrencyConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of concurrency");

        let sim_init;

        if let Some(num_threads) = concurrency_config.num_threads {
            sim_init = SimInit::with_num_threads(num_threads);
            info!("Starting simulation with {num_threads} thread(s).",);
        } else {
            sim_init = SimInit::new();
            info!("Starting simulation with the default number of thread(s).",);
        }

        set_num_switches(graph.node_count());
        let switches = Topology::init_switches();

        Topology {
            sim_init,
            graph: graph.clone(),
            hosts,
            switches,
            flows,
            collectives,
            switch_mailboxes: HashMap::new(),
            switch_config: config.switch,
            mailbox_capacity,
            duration,
            file_path: file_path.to_string(),
        }
    }

    // Initializes mailboxes for switches.
    fn init_mailboxes(&mut self) {
        for (_, switch) in self.switches.iter() {
            let switch_mbox: Mailbox<PacketSwitch> = Mailbox::with_capacity(self.mailbox_capacity);
            self.switch_mailboxes.insert(switch.id(), switch_mbox);
        }
    }

    fn init_switches() -> HashMap<usize, PacketSwitch> {
        let mut switches: HashMap<usize, PacketSwitch> = HashMap::new();

        for _ in 0..num_switches() {
            let switch = PacketSwitch::new(HashMap::new(), HashMap::new());
            switches.insert(switch.id(), switch);
        }

        switches
    }

    /// Connects a hash map of packet switches according to edges in a network
    /// topology.
    fn connect(
        mut self,
        graph: UnGraph<usize, ()>,
        ui_mbox: Mailbox<UserInterface>,
    ) -> (Self, Mailbox<UserInterface>) {
        for node_id in graph.node_indices() {
            for neighbor in graph.neighbors(node_id) {
                // if an edge exists between an upstream element and this
                // downstream element in the provided network graph, then
                // connect them and activate all schedulers in between
                if neighbor.index() != node_id.index() {
                    self = self.connect_neighbours(neighbor.index(), node_id.index(), &ui_mbox);
                }
            }
        }

        (self, ui_mbox)
    }

    /// Produces flows within all collectives in the network graph.
    fn process_collectives(&mut self) {
        info!(
            "Producing flows in all {} collective communication operations.",
            self.collectives.len()
        );

        // constructs, attaches, and routes flows in each collective
        for collective in self.collectives.iter_mut() {
            for (index, &source) in collective.sources.iter().enumerate() {
                let sink = collective.sinks[index];
                let flow_id = collective.first_flow_id + index;
                let path = collective.paths.as_ref().map(|paths| paths[index].clone());

                match collective.collective_type {
                    CollectiveType::Broadcast => {
                        self.flows.push(Flow::new(
                            flow_id,
                            path,
                            Vec::new(),
                            Vec::new(),
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
                            path,
                            Vec::new(),
                            Vec::new(),
                            collective.flow_type,
                            source,
                            sink,
                            collective.traffic,
                            // uses flow_id as the random seed for the flow,
                            // which ensures that different flows have different
                            // arrival and size distributions
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
                            path,
                            Vec::new(),
                            Vec::new(),
                            collective.flow_type,
                            source,
                            sink,
                            collective.traffic,
                            // uses the source host's id as the random seed for
                            // the flow, which ensures that different hosts have
                            // different arrival and size distributions, but
                            // packet sources attached to the same host have the
                            // same distribution
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

    /// Connects two adjacent switches in the network graph.
    fn connect_neighbours(
        mut self,
        upstream_id: usize,
        downstream_id: usize,
        ui_mbox: &Mailbox<UserInterface>,
    ) -> Self {
        let upstream_switch = self.switches.get_mut(&upstream_id).unwrap();

        match self.switch_config.discipline {
            SchedulingDiscipline::DRR => {
                let weights = self.switch_config.weights.as_ref().unwrap();
                let weights_len = weights.len();
                let mut drr_server = DRRServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weights_len),
                    self.switch_config.drop,
                    weights.clone(),
                );

                let mut output = Output::default();
                let drr_mbox: Mailbox<DRRServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(DRRServer::packet_received, &drr_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                drr_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
                drr_server
                    .report_output
                    .connect(UserInterface::report_arrived, ui_mbox);

                self.sim_init = self.sim_init.add_model(drr_server, drr_mbox, "DRR");
            }

            SchedulingDiscipline::FIFO => {
                let mut port = Port::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    self.switch_config.drop,
                );

                let mut output = Output::default();
                let port_mbox: Mailbox<Port> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(Port::packet_received, &port_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                port.output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
                port.report_output
                    .connect(UserInterface::report_arrived, ui_mbox);

                self.sim_init = self.sim_init.add_model(port, port_mbox, "Port");
            }

            SchedulingDiscipline::SP => {
                let priorities = self.switch_config.priorities.as_ref().unwrap();
                let priorities_len = priorities.len();
                let mut sp_server = SPServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % priorities_len),
                    self.switch_config.drop,
                    priorities.clone(),
                );

                let mut output = Output::default();
                let sp_mbox: Mailbox<SPServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(SPServer::packet_received, &sp_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                sp_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
                sp_server
                    .report_output
                    .connect(UserInterface::report_arrived, ui_mbox);

                self.sim_init = self.sim_init.add_model(sp_server, sp_mbox, "SP");
            }

            SchedulingDiscipline::VirtualClock => {
                let vticks = self.switch_config.vticks.as_ref().unwrap();
                let vticks_len = vticks.len();
                let mut virtual_clock_server = VirtualClockServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % vticks_len),
                    self.switch_config.drop,
                    vticks.clone(),
                );

                let mut output = Output::default();
                let virtual_clock_mbox: Mailbox<VirtualClockServer> =
                    Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(VirtualClockServer::packet_received, &virtual_clock_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                virtual_clock_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
                virtual_clock_server
                    .report_output
                    .connect(UserInterface::report_arrived, ui_mbox);

                self.sim_init = self.sim_init.add_model(
                    virtual_clock_server,
                    virtual_clock_mbox,
                    "VirtualClock",
                );
            }

            SchedulingDiscipline::WFQ => {
                let weights = self.switch_config.weights.as_ref().unwrap();
                let weights_len = weights.len();
                let mut wfq_server = WFQServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weights_len),
                    self.switch_config.drop,
                    weights.clone(),
                );

                let mut output = Output::default();
                let wfq_mbox: Mailbox<WFQServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(WFQServer::packet_received, &wfq_mbox);
                upstream_switch.outputs.insert(downstream_id, output);

                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                wfq_server
                    .output
                    .connect(PacketSwitch::packet_received, downstream_mbox);
                wfq_server
                    .report_output
                    .connect(UserInterface::report_arrived, ui_mbox);

                self.sim_init = self.sim_init.add_model(wfq_server, wfq_mbox, "WFQ");
            }
        }

        self
    }

    /// Attaches packet sources and sinks from the flows to hosts in the network
    /// graph.
    fn attach_flows(
        mut self,
        stats: &mut SinkStatistics,
        ui_mbox: Mailbox<UserInterface>,
    ) -> (Self, Mailbox<UserInterface>) {
        info!(
            "Attaching packet sources and sinks to their hosts in all {} flows.",
            self.flows.len()
        );

        let mut sources = HashMap::new();
        let mut source_mboxes = HashMap::new();
        for flow in self.flows.iter() {
            let source_mbox: Mailbox<PacketSource> = Mailbox::with_capacity(self.mailbox_capacity);
            source_mboxes.insert(flow.id, source_mbox);
        }

        for flow in self.flows.iter_mut() {
            // creates and attaches a packet source and sink for each flow

            // packet sources and sinks must be attached to hosts
            assert!(self.hosts.contains(&flow.source_host));
            assert!(self.hosts.contains(&flow.sink_host));

            // creates a new packet source
            let mut source = PacketSource::new(
                flow.id,
                flow.starts_after.clone(),
                flow.flow_type,
                flow.traffic,
                flow.seed,
            );
            // records the PacketSource id for adding it as the start of the
            // flow's path in later construction of the path in
            // Flow::compute_path()
            flow.source_id = source.id();

            // creates a new packet sink
            let mut sink = PacketSink::new(&source);
            // records the PacketSink id for adding it as the end of the flow's
            // path in later construction of the path in Flow::compute_path()
            flow.sink_id = sink.id();

            // obtains the host switch and its mailbox for the packet source
            let source_host = self.switches.get_mut(&flow.source_host).unwrap();
            let host_mbox = self.switch_mailboxes.get(&flow.source_host).unwrap();

            // establishes a bi-directional connection between the packet source
            // and the host
            let source_mbox = &source_mboxes[&flow.id];
            source
                .output()
                .connect(PacketSwitch::packet_received, host_mbox);
            source
                .report_output()
                .connect(UserInterface::report_arrived, &ui_mbox);

            let mut output = Output::default();
            output.connect(PacketSource::packet_received, source_mbox);
            source_host.outputs.insert(source.id(), output);

            // obtains the host switch and its mailbox for the packet sink
            let sink_host = self.switches.get_mut(&flow.sink_host).unwrap();
            let host_mbox = self.switch_mailboxes.get(&flow.sink_host).unwrap();

            // establishes a bi-directional connection between the packet sink
            // and the host
            let sink_mbox: Mailbox<PacketSink> = Mailbox::with_capacity(self.mailbox_capacity);

            // records the sink ids, sink mailbox's address and sink statistics
            // event slot for the retrieval of packet statistics after the
            // simulation finishes
            stats.sink_ids.push(sink.id());
            stats.sink_addresses.insert(sink.id(), sink_mbox.address());
            let sink_stats = EventSlot::new();
            sink.statistics().connect_sink(&sink_stats);
            stats.sink_statistics.insert(sink.id(), sink_stats);

            sink.output()
                .connect(PacketSwitch::packet_received, host_mbox);
            sink.report_output()
                .connect(UserInterface::report_arrived, &ui_mbox);

            let mut output = Output::default();
            output.connect(PacketSink::packet_received, &sink_mbox);
            sink_host.outputs.insert(sink.id(), output);

            // establishes connections between the packet sink (or source for
            // TCP) and the packet sources that will not start until this sink
            // receives (or source for TCP) its last packet
            for flow_id in flow.starts_before.iter() {
                let mut flow_finish_output = Output::default();
                flow_finish_output.connect(PacketSource::flow_finished, &source_mboxes[&flow_id]);
                match flow.flow_type {
                    FlowType::PacketDistribution => {
                        sink.connect_flow_finish_output(flow_finish_output);
                    }
                    FlowType::TCP => {
                        source.connect_flow_finish_output(flow_finish_output);
                    }
                }
            }

            sources.insert(flow.id, source);

            // activates the packet sink
            self.sim_init = self.sim_init.add_model(sink, sink_mbox, "Sink");
        }

        // activates all packet sources
        for (flow_id, source) in sources.into_iter() {
            let source_mbox = source_mboxes.remove(&flow_id).unwrap_or_default();
            self.sim_init = self.sim_init.add_model(source, source_mbox, "Source");
        }

        (self, ui_mbox)
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

            for window in path.windows(2) {
                let node_id = window.get(0).unwrap().index();
                let next_id = window.get(1).unwrap().index();

                // FIBs do not include PacketSource
                if node_id != path.first().unwrap().index() {
                    let switch = self.switches.get_mut(&node_id).unwrap();
                    switch.set_fib(flow.id, next_id);
                }

                // reverse FIBs do not include PacketSink
                if next_id != path.last().unwrap().index() {
                    let switch = self.switches.get_mut(&next_id).unwrap();
                    switch.set_r_fib(flow.id, node_id);
                }
            }
        }

        info!(
            "Routing decisions for all {} flows have been finalized.",
            self.flows.len()
        );
    }

    /// Creates and activates a UserInterface coroutine, which contains a progress bar and
    /// collect reports from all the network elements.
    fn activate_ui(mut self, ui_mbox: Mailbox<UserInterface>, file_path: String) -> Self {
        let ui = UserInterface::new(self.duration, self.flows.len(), file_path);
        self.sim_init = self.sim_init.add_model(ui, ui_mbox, "UserInterface");

        self
    }

    /// Activates all the switches and initializes the simulation.
    fn init_sim(mut self) -> Simulation {
        info!(
            "Activating all {} switches and initializing the simulation.",
            self.switches.len(),
        );

        for (_, switch) in self.switches {
            let switch_mbox = self.switch_mailboxes.remove(&switch.id()).unwrap();
            self.sim_init = self.sim_init.add_model(switch, switch_mbox, "Switch");
        }

        match self.sim_init.init(MonotonicTime::EPOCH) {
            Ok((simulation, _)) => simulation,
            Err(error) => panic!("Problem when initializing the simulation: {error:?}"),
        }
    }

    pub fn run(mut self, graph: UnGraph<usize, ()>) {
        let mut statistics = SinkStatistics::default();

        // initializes mailboxes for the packet switches
        self.init_mailboxes();

        // produces flows within all collectives in the network graph
        self.process_collectives();

        let mut ui_mbox: Mailbox<UserInterface> = Mailbox::with_capacity(self.mailbox_capacity);

        // constructs the network graph by connecting the packet switches
        (self, ui_mbox) = self.connect(graph, ui_mbox);

        // attaches packet sources and sinks from flows to hosts in the network graph
        (self, ui_mbox) = self.attach_flows(&mut statistics, ui_mbox);

        // computes feasible paths for all flows, and sets FIBs for all switches
        self.route_flows();

        // creates and activates a UserInterface coroutine
        let config_path = self.file_path.clone();
        self = self.activate_ui(ui_mbox, config_path);
        let duration = self.duration;

        // activates all the switches and initializes the simulation
        let mut sim = self.init_sim();

        // starts the performance measurement clock
        let timer = std::time::Instant::now();

        // starts the simulation
        let _ = sim.step_until(Duration::from_secs_f64(duration));
        sim = statistics.collect_statistics(sim);

        let elapsed = timer.elapsed();
        info!(
            "Simulation completed at time {:.3} seconds in simulation time.",
            sim.time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64()
        );
        info!(
            "Elapsed wall-clock time: {:.3} seconds.",
            elapsed.as_secs_f64()
        );
    }
}

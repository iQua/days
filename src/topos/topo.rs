//! Implements all the necessary utilities for initializing, constructing, and
//! running a network topology. These utilities include connecting network
//! switches according to a network graph, attaching packet endpoints to hosts,
//! computing feasible paths for all the flows, and installing Flow Information
//! Base tables to all the switches to route these flows accordingly.

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
#[cfg(feature = "l2_pfc")]
use std::sync::RwLock;
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use indicatif_log_bridge::LogWrapper;
use log::{debug, error, info};
use petgraph::graph::UnGraph;
use serde::Deserialize;

use crate::flows::FlowSize;
use crate::flows::app_source::{AppBufferConfig, AppSourceBufferHandle};
use crate::flows::collective::{Collective, CollectiveType};
use crate::flows::flow::{Flow, FlowParams, FlowType};
use crate::flows::packet::Packet;
use crate::flows::sink::{PacketSink, PacketStatistics};
use crate::flows::source::PacketSource;
#[cfg(feature = "l2_pfc")]
use crate::l2::link::Link;
#[cfg(feature = "l2_pfc")]
use crate::l2::pfc::{PfcEgressGate, PfcIngressPort};
#[cfg(feature = "l2_pfc")]
use crate::next_link_id;
use crate::schedulers::drop::{CapacityUnit, DEFAULT_ECN_THRESHOLD, DropStrategy};
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::schedulers::sp::SPServer;
#[cfg(feature = "l2_pfc")]
use crate::schedulers::state::QueueState;
use crate::schedulers::vc::VirtualClockServer;
use crate::schedulers::wfq::WFQServer;
use crate::schedulers::wrr::WRRServer;
use crate::switches::SchedulingDiscipline;
use crate::switches::switch::PacketSwitch;
use crate::utils::logger::CsvLogger;
use crate::utils::time::set_time_quantum_ns;
use crate::utils::tracing::start_wall_clock_concurrency_sampler;
use crate::utils::ui::UserInterface;
use crate::{num_switches, peak_concurrency, reset_peak_concurrency, set_num_switches};
use nexosim::ports::{EventSlot, Output};
use nexosim::simulation::{Address, Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;

#[cfg(feature = "l2_pfc")]
type OutputStateMap = HashMap<usize, HashMap<usize, Arc<QueueState>>>;
#[cfg(feature = "l2_pfc")]
type OutputStates = Arc<RwLock<OutputStateMap>>;

use crate::flows::app_source::AppDataSource;

#[derive(Deserialize)]
pub struct UIConfig {
    pub ui_interval: Option<f64>,
    pub duration: Option<f64>,
}

#[derive(Deserialize)]
pub struct TracingConfig {
    pub tracing_active: Option<bool>,
    pub tracing_interval: Option<f64>,
    pub duration: Option<f64>,
}

#[derive(Deserialize)]
struct ConcurrencyConfig {
    threading: Option<ThreadingModel>,
    num_threads: Option<usize>,
    hot_workers: Option<usize>,
    concurrency_level: Option<ConcurrencyLevel>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ThreadingModel {
    Single,
    Multiple,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ConcurrencyLevel {
    Default,
    Accelerated,
}

#[derive(Deserialize)]
pub struct SwitchConfig {
    port_rate: f64,
    capacity: usize,
    discipline: SchedulingDiscipline,
    drop: DropStrategy,
    ecn_threshold: Option<f64>,
    run_batch_size: Option<usize>,
    weights: Option<Vec<usize>>,
    priorities: Option<Vec<usize>>,
    vticks: Option<Vec<f64>>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum LinkMode {
    None,
    Pfc,
}

#[cfg_attr(not(feature = "l2_pfc"), allow(dead_code))]
#[derive(Clone, Debug, Deserialize, Default)]
pub struct PfcLinkConfig {
    xoff: Option<Vec<usize>>,
    xon: Option<Vec<usize>>,
    pause_quanta: Option<Vec<u16>>,
    buffer_capacity: Option<Vec<usize>>,
    refresh_interval: Option<f64>,
    drain_interval: Option<f64>,
}

#[cfg_attr(not(feature = "l2_pfc"), allow(dead_code))]
#[derive(Clone, Debug, Deserialize, Default)]
pub struct LinkConfig {
    mode: Option<LinkMode>,
    pfc: Option<PfcLinkConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Scatter,
    Gather,
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
    pub app_source: Option<AppSourceConfig>,
    pub link: Option<LinkConfig>,
    pub time_quantum_ns: Option<u64>,
}

#[derive(Deserialize)]
pub struct AppSourceConfig {
    pub req_channel_capacity: Option<usize>,
    pub chunk_size: Option<usize>,
    pub initial_delay: Option<u64>,
    pub run_interval: Option<u64>,
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
            let _ = sim.process_event_fn(PacketSink::report, *sink_id, sink_addr);

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
    runtime_num_threads: usize,
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
    /// configuration of link-layer behavior
    link_config: LinkConfig,
    #[cfg(feature = "l2_pfc")]
    fib_views: HashMap<usize, Arc<RwLock<HashMap<usize, usize>>>>,
    #[cfg(feature = "l2_pfc")]
    output_states: OutputStates,
    /// the capacity of every mailbox
    mailbox_capacity: usize,
    /// the path to the configuration file
    config_path: String,
    /// the duration of the simulation
    duration: f64,
    /// app source runtime config
    app_source_cfg: AppBufferConfig,
}

impl Topology {
    pub fn new(
        config_path: &str,
        graph: UnGraph<usize, ()>,
        hosts: Vec<usize>,
        flows: Vec<Flow>,
        collectives: Vec<Collective>,
    ) -> Topology {
        // reads the configuration
        let content = fs::read_to_string(config_path).expect("The configuration is not valid");

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

        let concurrency_config: ConcurrencyConfig = toml::from_str(&content)
            .expect("Failed to deserialize the configuration of concurrency");

        let threading = concurrency_config.threading;
        let config_num_threads = concurrency_config.num_threads;
        if threading.is_none() && config_num_threads.is_some() {
            panic!("num_threads requires threading to be set (e.g., threading = \"multiple\").");
        }

        fn normalize_num_threads(num_threads: usize) -> usize {
            if cfg!(target_family = "wasm") {
                1
            } else {
                num_threads.clamp(1, usize::BITS as usize)
            }
        }

        let num_threads = threading.map(|model| match model {
            ThreadingModel::Single => {
                if let Some(n) = config_num_threads {
                    if n != 1 {
                        panic!(
                            "num_threads={n} is incompatible with threading=\"single\" (expected 1)."
                        );
                    }
                }
                1
            }
            ThreadingModel::Multiple => {
                let n = config_num_threads.unwrap_or_else(num_cpus::get);
                if n == 0 {
                    panic!("num_threads must be >= 1.");
                }
                normalize_num_threads(n)
            }
        });

        let mut sim_init = if let Some(model) = threading {
            let num_threads = num_threads.expect("threading implies a thread count");
            let mode = match model {
                ThreadingModel::Single => "single",
                ThreadingModel::Multiple => "multiple",
            };
            info!("Starting simulation with {mode} threading ({num_threads} thread(s)).");
            SimInit::with_num_threads(num_threads)
        } else {
            info!("Starting simulation with the default threading model.");
            SimInit::new()
        };
        let runtime_num_threads =
            num_threads.unwrap_or_else(|| normalize_num_threads(num_cpus::get()));

        if let Some(hot_workers) = concurrency_config.hot_workers {
            sim_init = sim_init.set_hot_worker_count(hot_workers);
            info!("Using {hot_workers} hot standby worker(s).",);
        }

        if let Some(level) = concurrency_config.concurrency_level {
            match level {
                ConcurrencyLevel::Default => {
                    sim_init = sim_init.set_max_groups_per_step_task(1);
                    info!("Using default concurrency level.");
                }
                ConcurrencyLevel::Accelerated => {
                    let groups_per_task = runtime_num_threads.saturating_mul(10).max(1);
                    sim_init = sim_init.set_max_groups_per_step_task(groups_per_task);
                    info!("Using accelerated concurrency level.");
                }
            }
        }

        let time_quantum_ns = config.time_quantum_ns;
        set_time_quantum_ns(time_quantum_ns);
        if let Some(quantum_ns) = time_quantum_ns {
            sim_init = sim_init.set_time_quantum_ns(quantum_ns);
        }

        set_num_switches(graph.node_count());
        let switches = Topology::init_switches();
        #[cfg(feature = "l2_pfc")]
        let fib_views: HashMap<usize, Arc<RwLock<HashMap<usize, usize>>>> = switches
            .keys()
            .map(|id| (*id, Arc::new(RwLock::new(HashMap::new()))))
            .collect();
        #[cfg(feature = "l2_pfc")]
        let output_states: OutputStates = Arc::new(RwLock::new(HashMap::new()));

        let app_source_cfg = if let Some(app_src) = &config.app_source {
            AppBufferConfig {
                req_channel_capacity: app_src.req_channel_capacity.unwrap_or(128),
                chunk_size: app_src.chunk_size.unwrap_or(512),
                initial_delay: app_src.initial_delay.unwrap_or(1),
                run_interval: app_src.run_interval.unwrap_or(50),
            }
        } else {
            AppBufferConfig::default()
        };
        let link_config = config.link.clone().unwrap_or_default();
        #[cfg(not(feature = "l2_pfc"))]
        {
            if matches!(link_config.mode.unwrap_or(LinkMode::None), LinkMode::Pfc) {
                panic!("link.mode = \"Pfc\" requires building with --features l2_pfc");
            }
        }
        Topology {
            sim_init,
            runtime_num_threads,
            graph: graph.clone(),
            hosts,
            switches,
            flows,
            collectives,
            switch_mailboxes: HashMap::new(),
            switch_config: config.switch,
            link_config,
            #[cfg(feature = "l2_pfc")]
            fib_views,
            #[cfg(feature = "l2_pfc")]
            output_states,
            mailbox_capacity,
            config_path: config_path.to_string(),
            duration,
            app_source_cfg,
        }
    }

    pub fn num_threads(&self) -> usize {
        self.runtime_num_threads
    }

    fn init_logger(config_path: &str) {
        CsvLogger::get_instance()
            .init_from_config(config_path)
            .expect("Failed to initialize the logger.")
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

    fn link_mode(&self) -> LinkMode {
        self.link_config.mode.unwrap_or(LinkMode::None)
    }

    fn attach_link(
        &mut self,
        _upstream_id: usize,
        downstream_id: usize,
        scheduler_output: &mut Output<Packet>,
    ) {
        match self.link_mode() {
            LinkMode::None => {
                let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
                scheduler_output.connect(PacketSwitch::packet_received, downstream_mbox);
            }
            LinkMode::Pfc => {
                #[cfg(feature = "l2_pfc")]
                {
                    self.attach_pfc_link(_upstream_id, downstream_id, scheduler_output);
                }
                #[cfg(not(feature = "l2_pfc"))]
                {
                    panic!("link.mode = \"Pfc\" requires building with --features l2_pfc");
                }
            }
        }
    }

    #[cfg(feature = "l2_pfc")]
    fn attach_pfc_link(
        &mut self,
        _upstream_id: usize,
        downstream_id: usize,
        scheduler_output: &mut Output<Packet>,
    ) {
        let gate_id = next_link_id();
        let link_id = next_link_id();
        let ingress_id = next_link_id();
        let pfc_config = self.build_pfc_config();
        let fib_view = self
            .fib_views
            .get(&downstream_id)
            .expect("Missing FIB view for switch")
            .clone();
        let output_states = self.output_states.clone();
        let can_forward = Arc::new(move |packet: &Packet| {
            let next_id = {
                let fib_guard = fib_view.read().expect("FIB view lock poisoned");
                fib_guard.get(&packet.flow_id).copied()
            };
            let Some(next_id) = next_id else {
                return true;
            };
            let state = {
                let states_guard = output_states.read().expect("Output state lock poisoned");
                states_guard
                    .get(&downstream_id)
                    .and_then(|outputs| outputs.get(&next_id).cloned())
            };
            match state {
                Some(state) => state.can_accept(packet.size),
                None => true,
            }
        });

        let mut gate = PfcEgressGate::new(gate_id, self.switch_config.port_rate);
        let mut link = Link::new(link_id, self.switch_config.port_rate);
        let mut ingress = PfcIngressPort::new(ingress_id, gate_id, pfc_config, can_forward);

        let gate_mbox: Mailbox<PfcEgressGate> = Mailbox::with_capacity(self.mailbox_capacity);
        let link_mbox: Mailbox<Link> = Mailbox::with_capacity(self.mailbox_capacity);
        let ingress_mbox: Mailbox<PfcIngressPort> = Mailbox::with_capacity(self.mailbox_capacity);

        scheduler_output.connect(PfcEgressGate::packet_received, &gate_mbox);
        gate.output.connect(Link::frame_received, &link_mbox);
        link.output
            .connect(PfcIngressPort::frame_received, &ingress_mbox);

        let downstream_mbox = self.switch_mailboxes.get(&downstream_id).unwrap();
        ingress
            .output
            .connect(PacketSwitch::packet_received, downstream_mbox);
        ingress
            .pfc_output
            .connect(PfcEgressGate::pfc_received, &gate_mbox);

        let sim_init = std::mem::replace(&mut self.sim_init, SimInit::new());
        self.sim_init = sim_init
            .add_model(gate, gate_mbox, "PfcEgressGate")
            .add_model(link, link_mbox, "Link")
            .add_model(ingress, ingress_mbox, "PfcIngressPort");
    }

    #[cfg(feature = "l2_pfc")]
    fn build_pfc_config(&self) -> crate::l2::pfc::PfcConfig {
        use crate::l2::pfc::PfcConfig;

        fn vec_to_array<T: Copy, const N: usize>(vec: Option<Vec<T>>, default: [T; N]) -> [T; N] {
            if let Some(values) = vec {
                assert!(
                    values.len() == N,
                    "Expected {} entries but got {}.",
                    N,
                    values.len()
                );
                let mut output = default;
                for (idx, value) in values.into_iter().enumerate() {
                    output[idx] = value;
                }
                output
            } else {
                default
            }
        }

        let default_xoff = [64 * 1024; 8];
        let default_xon = [48 * 1024; 8];
        let default_pause_quanta = [65535; 8];
        let default_capacity = [0; 8];

        let pfc = self.link_config.pfc.clone().unwrap_or_default();
        PfcConfig {
            xoff: vec_to_array(pfc.xoff, default_xoff),
            xon: vec_to_array(pfc.xon, default_xon),
            pause_quanta: vec_to_array(pfc.pause_quanta, default_pause_quanta),
            buffer_capacity: vec_to_array(pfc.buffer_capacity, default_capacity),
            refresh_interval: pfc.refresh_interval,
            drain_interval: pfc.drain_interval,
        }
    }

    #[cfg(feature = "l2_pfc")]
    fn register_queue_state(
        &mut self,
        upstream_id: usize,
        downstream_id: usize,
        state: Arc<QueueState>,
    ) {
        let mut guard = self.output_states.write().unwrap();
        guard
            .entry(upstream_id)
            .or_default()
            .insert(downstream_id, state);
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

        for collective in self.collectives.iter_mut() {
            let collective_type = collective.collective_type.clone();
            match collective_type {
                CollectiveType::RingAllReduce => {
                    let n = collective.sources.len();
                    let mut flow_id = collective.first_flow_id;

                    let mut last_scatter: Vec<Option<usize>> = vec![None; n];
                    let mut last_gather: Vec<Option<usize>> = vec![None; n];

                    for (rank, &src) in collective.sources.iter().enumerate() {
                        let dst = collective.sources[(rank + 1) % n];

                        for _step in 1..n {
                            let mut starts_after = Vec::new();
                            if let Some(prev_flow_id) = last_scatter[rank] {
                                starts_after.push(prev_flow_id);
                            }

                            let flow = Flow::new(FlowParams {
                                id: flow_id,
                                path: None,
                                starts_before: Vec::new(),
                                starts_after,
                                flow_type: collective.flow_type.clone(),
                                source_host: src,
                                sink_host: dst,
                                routing: collective.routing.clone(),
                                traffic: collective.traffic.clone(),
                                priority: 0,
                                seed: collective.id,
                            });

                            self.flows.push(flow);

                            last_scatter[rank] = Some(flow_id);

                            flow_id += 1;
                        }
                    }

                    for (rank, &src) in collective.sources.iter().enumerate() {
                        let dst = collective.sources[(rank + 1) % n];

                        for step in 1..n {
                            let mut starts_after = Vec::new();
                            if let Some(prev_flow_id) = last_gather[rank] {
                                starts_after.push(prev_flow_id);
                            }

                            if step == 1 {
                                if let Some(scatter_flow_id) = last_scatter[rank] {
                                    starts_after.push(scatter_flow_id);
                                }
                            }

                            let flow = Flow::new(FlowParams {
                                id: flow_id,
                                path: None,
                                starts_before: Vec::new(),
                                starts_after,
                                flow_type: collective.flow_type.clone(),
                                source_host: src,
                                sink_host: dst,
                                routing: collective.routing.clone(),
                                traffic: collective.traffic.clone(),
                                priority: 0,
                                seed: collective.id,
                            });

                            self.flows.push(flow);

                            last_gather[rank] = Some(flow_id);

                            flow_id += 1;
                        }
                    }

                    collective.flow_count = flow_id - collective.first_flow_id;
                }

                _ => {
                    for (index, &source) in collective.sources.iter().enumerate() {
                        let sink = collective.sinks[index];
                        let flow_id = collective.first_flow_id + index;
                        let path = collective.paths.as_ref().map(|paths| paths[index].clone());

                        self.flows.push(Flow::new(FlowParams {
                            id: flow_id,
                            path,
                            starts_before: Vec::new(),
                            starts_after: Vec::new(),
                            flow_type: collective.flow_type.clone(),
                            source_host: source,
                            sink_host: sink,
                            routing: collective.routing.clone(),
                            traffic: collective.traffic.clone(),
                            priority: 0,
                            seed: match collective_type.clone() {
                                CollectiveType::Broadcast => collective.id,
                                CollectiveType::Gather => flow_id,
                                CollectiveType::AllReduce => source,
                                _ => 0, // fallback
                            },
                        }));

                        debug!(
                            "Produced Flow {} of {:?} collective communication operation {}.",
                            flow_id, collective_type, collective.id
                        );
                    }
                }
            }
        }
    }

    /// Connects two adjacent switches in the network graph.
    fn connect_neighbours(mut self, upstream_id: usize, downstream_id: usize) -> Self {
        let drop_strategy = self.switch_config.drop.clone();
        let ecn_threshold = self
            .switch_config
            .ecn_threshold
            .unwrap_or(DEFAULT_ECN_THRESHOLD);

        match self.switch_config.discipline {
            SchedulingDiscipline::DRR => {
                let weights = self.switch_config.weights.as_ref().unwrap_or_else(|| {
                    panic!(
                        "`weights` must be provided for Deficit Round Robin scheduling discipline."
                    )
                });
                let weights_len = weights.len();

                let mut drr_server = DRRServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weights_len),
                    drop_strategy.clone(),
                    ecn_threshold,
                    weights.clone(),
                );
                drr_server.set_run_batch_size(self.switch_config.run_batch_size);
                #[cfg(feature = "l2_pfc")]
                {
                    let state = QueueState::new(self.switch_config.capacity, CapacityUnit::Packets);
                    drr_server.set_queue_state(state.clone());
                    self.register_queue_state(upstream_id, downstream_id, state);
                }

                let mut output = Output::default();
                let drr_mbox: Mailbox<DRRServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(DRRServer::packet_received, &drr_mbox);
                self.switches
                    .get_mut(&upstream_id)
                    .unwrap()
                    .outputs
                    .insert(downstream_id, output);

                self.attach_link(upstream_id, downstream_id, &mut drr_server.output);

                self.sim_init = self.sim_init.add_model(drr_server, drr_mbox, "DRR");
            }

            SchedulingDiscipline::FIFO => {
                let mut port = Port::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    drop_strategy.clone(),
                    ecn_threshold,
                    self.switch_config.run_batch_size,
                );
                #[cfg(feature = "l2_pfc")]
                {
                    let state = QueueState::new(self.switch_config.capacity, CapacityUnit::Packets);
                    port.set_queue_state(state.clone());
                    self.register_queue_state(upstream_id, downstream_id, state);
                }

                let mut output = Output::default();
                let port_mbox: Mailbox<Port> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(Port::packet_received, &port_mbox);
                self.switches
                    .get_mut(&upstream_id)
                    .unwrap()
                    .outputs
                    .insert(downstream_id, output);

                self.attach_link(upstream_id, downstream_id, &mut port.output);

                self.sim_init = self.sim_init.add_model(port, port_mbox, "Port");
            }

            SchedulingDiscipline::SP => {
                let mut priorities = self
                    .switch_config
                    .priorities
                    .clone()
                    .unwrap_or_else(|| vec![1]);
                if priorities.is_empty() {
                    priorities.push(1);
                }
                let priorities_len = priorities.len();

                let mut sp_server = SPServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % priorities_len),
                    drop_strategy.clone(),
                    ecn_threshold,
                    priorities,
                );
                #[cfg(feature = "l2_pfc")]
                {
                    let state = QueueState::new(self.switch_config.capacity, CapacityUnit::Packets);
                    sp_server.set_queue_state(state.clone());
                    self.register_queue_state(upstream_id, downstream_id, state);
                }

                let mut output = Output::default();
                let sp_mbox: Mailbox<SPServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(SPServer::packet_received, &sp_mbox);
                self.switches
                    .get_mut(&upstream_id)
                    .unwrap()
                    .outputs
                    .insert(downstream_id, output);

                self.attach_link(upstream_id, downstream_id, &mut sp_server.output);

                self.sim_init = self.sim_init.add_model(sp_server, sp_mbox, "SP");
            }

            SchedulingDiscipline::VirtualClock => {
                let mut vticks = self
                    .switch_config
                    .vticks
                    .clone()
                    .unwrap_or_else(|| vec![1.0]);
                if vticks.is_empty() {
                    vticks.push(1.0);
                }
                let vticks_len = vticks.len();

                let mut virtual_clock_server = VirtualClockServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % vticks_len),
                    drop_strategy.clone(),
                    ecn_threshold,
                    vticks,
                );
                #[cfg(feature = "l2_pfc")]
                {
                    let state = QueueState::new(self.switch_config.capacity, CapacityUnit::Packets);
                    virtual_clock_server.set_queue_state(state.clone());
                    self.register_queue_state(upstream_id, downstream_id, state);
                }

                let mut output = Output::default();
                let virtual_clock_mbox: Mailbox<VirtualClockServer> =
                    Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(VirtualClockServer::packet_received, &virtual_clock_mbox);
                self.switches
                    .get_mut(&upstream_id)
                    .unwrap()
                    .outputs
                    .insert(downstream_id, output);

                self.attach_link(upstream_id, downstream_id, &mut virtual_clock_server.output);

                self.sim_init = self.sim_init.add_model(
                    virtual_clock_server,
                    virtual_clock_mbox,
                    "VirtualClock",
                );
            }

            SchedulingDiscipline::WFQ => {
                let weights = self.switch_config.weights.as_ref().unwrap_or_else(|| {
                    panic!("`weights` must be provided for Weighted Fair Queuing scheduling discipline.")
                });
                let weights_len = weights.len();

                let mut wfq_server = WFQServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weights_len),
                    drop_strategy.clone(),
                    ecn_threshold,
                    weights.clone(),
                );
                #[cfg(feature = "l2_pfc")]
                {
                    let state = QueueState::new(self.switch_config.capacity, CapacityUnit::Packets);
                    wfq_server.set_queue_state(state.clone());
                    self.register_queue_state(upstream_id, downstream_id, state);
                }

                let mut output = Output::default();
                let wfq_mbox: Mailbox<WFQServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(WFQServer::packet_received, &wfq_mbox);
                self.switches
                    .get_mut(&upstream_id)
                    .unwrap()
                    .outputs
                    .insert(downstream_id, output);

                self.attach_link(upstream_id, downstream_id, &mut wfq_server.output);

                self.sim_init = self.sim_init.add_model(wfq_server, wfq_mbox, "WFQ");
            }

            SchedulingDiscipline::WRR => {
                let weights = self.switch_config.weights.as_ref().unwrap_or_else(|| {
                    panic!(
                        "`weights` must be provided for Weighted Round Robin scheduling discipline."
                    )
                });

                let weights_len = weights.len();
                let mut wrr_server = WRRServer::new(
                    self.switch_config.port_rate,
                    self.switch_config.capacity,
                    CapacityUnit::Packets,
                    Arc::new(move |flow_id| flow_id % weights_len),
                    drop_strategy,
                    ecn_threshold,
                    weights.clone(),
                );
                wrr_server.set_run_batch_size(self.switch_config.run_batch_size);
                #[cfg(feature = "l2_pfc")]
                {
                    let state = QueueState::new(self.switch_config.capacity, CapacityUnit::Packets);
                    wrr_server.set_queue_state(state.clone());
                    self.register_queue_state(upstream_id, downstream_id, state);
                }

                let mut output = Output::default();
                let wrr_mbox: Mailbox<WRRServer> = Mailbox::with_capacity(self.mailbox_capacity);
                output.connect(WRRServer::packet_received, &wrr_mbox);
                self.switches
                    .get_mut(&upstream_id)
                    .unwrap()
                    .outputs
                    .insert(downstream_id, output);

                self.attach_link(upstream_id, downstream_id, &mut wrr_server.output);

                self.sim_init = self.sim_init.add_model(wrr_server, wrr_mbox, "WRR");
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
        flow_id_to_source_handle: Option<HashMap<usize, AppSourceBufferHandle>>,
    ) -> (Self, Mailbox<UserInterface>) {
        info!(
            "Attaching packet sources and sinks to their hosts in all {} flows.",
            self.flows.len()
        );

        let mut successor_map: HashMap<usize, Vec<usize>> = HashMap::new();
        for flow in self.flows.iter() {
            if !flow.starts_before.is_empty() {
                successor_map
                    .entry(flow.id)
                    .or_default()
                    .extend(flow.starts_before.iter().copied());
            }
        }

        for flow in self.flows.iter() {
            for dependency_id in flow.starts_after.iter() {
                successor_map
                    .entry(*dependency_id)
                    .or_default()
                    .push(flow.id);
            }
        }

        for successors in successor_map.values_mut() {
            successors.sort_unstable();
            successors.dedup();
        }

        for flow in self.flows.iter_mut() {
            if let Some(dependents) = successor_map.get(&flow.id) {
                flow.starts_before = dependents.clone();
            } else {
                flow.starts_before.clear();
            }
        }

        let mut sources = HashMap::new();
        let mut source_mboxes = HashMap::new();
        for flow in self.flows.iter() {
            let source_mbox: Mailbox<PacketSource> = Mailbox::with_capacity(self.mailbox_capacity);
            source_mboxes.insert(flow.id, source_mbox);
        }

        // creates and attaches a packet source and sink for each flow
        for flow in self.flows.iter_mut() {
            // packet sources and sinks must be attached to hosts
            assert!(self.hosts.contains(&flow.source_host));
            assert!(self.hosts.contains(&flow.sink_host));

            let handle = flow_id_to_source_handle
                .as_ref()
                .and_then(|m| m.get(&flow.id).cloned());
            // let handle = flow_id_to_source_handle
            //     .as_ref()
            //     .and_then(|m| m.get(&flow.id).cloned());
            let mut source = PacketSource::new(
                flow.id,
                flow.starts_after.clone(),
                flow.flow_type.clone(),
                flow.traffic.clone(),
                flow.priority,
                flow.seed,
                handle,
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
                .ui_output()
                .connect(UserInterface::flow_finished, &ui_mbox);

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
            sink.statistics().connect_sink(sink_stats.writer());
            stats.sink_statistics.insert(sink.id(), sink_stats);

            sink.output()
                .connect(PacketSwitch::packet_received, host_mbox);

            let mut output = Output::default();
            output.connect(PacketSink::packet_received, &sink_mbox);
            sink_host.outputs.insert(sink.id(), output);

            // establishes connections between the packet sink (or source for
            // TCP) and the packet sources that will not start until this sink
            // receives (or source for TCP) its last packet
            for flow_id in flow.starts_before.iter() {
                let mut flow_finish_output = Output::default();
                flow_finish_output.connect(PacketSource::flow_finished, &source_mboxes[flow_id]);
                match flow.flow_type {
                    FlowType::PacketDistribution => {
                        sink.connect_flow_finish_output(flow_finish_output);
                    }
                    FlowType::TCP => {
                        source.connect_flow_finish_output(flow_finish_output);
                    }
                    #[cfg(feature = "dcqcn")]
                    FlowType::DCQCN => {
                        source.connect_flow_finish_output(flow_finish_output);
                    }
                }
            }

            // if flow.flow_type == FlowType::TCP {
            //     sink.output()
            //         .connect(PacketSource::packet_received, &source_mboxes[&flow.id]);
            // }

            sources.insert(flow.id, source);

            // activates the packet sink
            self.sim_init = self.sim_init.add_model(sink, sink_mbox, "Sink");
        }

        // activates all packet sources in deterministic flow_id order
        let mut sources_vec: Vec<(usize, PacketSource)> = sources.into_iter().collect();
        sources_vec.sort_by_key(|(flow_id, _)| *flow_id);
        for (flow_id, source) in sources_vec {
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

        let num_flows = self.flows.len();

        // initializes a multi-progress bar for the routing process
        let multi = MultiProgress::new();
        let env_logger = env_logger::Builder::from_default_env().build();
        LogWrapper::new(multi.clone(), env_logger);
        let progress_bar = ProgressBar::new(num_flows as u64);
        progress_bar.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:90.magenta/blue/cyan} {pos:>7}/{len:7} {msg}",
            )
            .unwrap(),
        );
        let pg = multi.add(progress_bar);
        let mut flow_count = 0;

        for flow in self.flows.iter_mut() {
            let path = flow.compute_path(self.graph.clone());

            for window in path.windows(2) {
                let node_id = window.first().unwrap().index();
                let next_id = window.get(1).unwrap().index();

                // FIBs do not include PacketSource
                if node_id != path.first().unwrap().index() {
                    let switch = self.switches.get_mut(&node_id).unwrap();
                    switch.set_fib(flow.id, next_id);
                    #[cfg(feature = "l2_pfc")]
                    if let Some(fib_view) = self.fib_views.get(&node_id) {
                        let mut guard = fib_view.write().expect("FIB view lock poisoned");
                        guard.insert(flow.id, next_id);
                    }
                }

                // reverse FIBs do not include PacketSink
                if next_id != path.last().unwrap().index() {
                    let switch = self.switches.get_mut(&next_id).unwrap();
                    switch.set_r_fib(flow.id, node_id);
                }
            }

            // increment the progress bar
            flow_count += 1;
            pg.inc(flow_count as u64 - pg.position());
        }

        pg.inc(num_flows as u64 - pg.position());
        pg.finish_with_message("Done.");
    }

    /// Creates and activates a UserInterface coroutine, which contains a progress bar.
    fn activate_ui(mut self, ui_mbox: Mailbox<UserInterface>) -> Self {
        let ui = UserInterface::new(self.flows.len(), self.config_path.as_str());
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
            Ok(simulation) => simulation,
            Err(error) => panic!("Problem when initializing the simulation: {error:?}"),
        }
    }

    pub fn run(mut self, graph: UnGraph<usize, ()>) {
        let mut statistics = SinkStatistics::default();

        // initializes the logger
        Topology::init_logger(&self.config_path);

        // initializes mailboxes for the packet switches
        self.init_mailboxes();

        // produces flows within all collectives in the network graph
        self.process_collectives();

        // Prepares application-level packet sources and their actors (only for TCP Broadcast).
        let mut app_sources: HashMap<usize, AppDataSource> = HashMap::new();
        let mut flow_id_to_source_handle: HashMap<usize, AppSourceBufferHandle> = HashMap::new();
        let mut conn_map: HashMap<(usize, usize), AppDataSource> = HashMap::new();

        for collective in &self.collectives {
            if matches!(
                (&collective.collective_type, &collective.flow_type),
                (CollectiveType::Broadcast, FlowType::TCP)
            ) {
                let total_size = match collective.traffic.size {
                    FlowSize::Bytes(s) => s,
                    _ => panic!("Only byte-based broadcast is supported."),
                };

                // create unique AppDataSource and actor (no MSS needed - TCP handles packetization)
                let data_src = AppDataSource::create_source_buffer(total_size, self.app_source_cfg);
                // assign handle to each flow：all flows obtain data from the same data_src
                for flow_id in
                    collective.first_flow_id..collective.first_flow_id + collective.flow_count
                {
                    flow_id_to_source_handle.insert(flow_id, data_src.handle());
                }
                app_sources.insert(collective.id, data_src);
            }
            // Ring-AllReduce (TCP)
            if matches!(
                (&collective.collective_type, &collective.flow_type),
                (CollectiveType::RingAllReduce, FlowType::TCP)
            ) {
                let total_size = match collective.traffic.size {
                    FlowSize::Bytes(b) => b,
                    _ => panic!("RingAllReduce only supports byte-based flows."),
                };
                let n = collective.sources.len();
                let chunk_size = total_size / n;

                let mut flow_id = collective.first_flow_id;

                for phase in [Phase::Scatter, Phase::Gather] {
                    for rank in 0..n {
                        for step in 1..n {
                            let src_host = collective.sources[rank]; // flow's sender
                            let dst_host = collective.sources[(rank + 1) % n];

                            // which chunk travels in this hop
                            let chunk_owner = match phase {
                                Phase::Scatter => (rank + n - step + 1) % n,
                                Phase::Gather => (rank + n - step) % n,
                            };
                            let chunk_offset = chunk_owner * chunk_size;
                            let chunk_len = if chunk_owner == n - 1 {
                                total_size - chunk_offset
                            } else {
                                chunk_size
                            };

                            // ensure we have one AppSourceBuffer for this src_host (no MSS - TCP handles packetization)
                            let data_src =
                                conn_map.entry((src_host, dst_host)).or_insert_with(|| {
                                    AppDataSource::create_source_buffer(
                                        total_size,
                                        self.app_source_cfg,
                                    )
                                });

                            let handle = data_src.handle_with_offset(chunk_offset, Some(chunk_len));
                            flow_id_to_source_handle.insert(flow_id, handle);

                            flow_id += 1;
                        }
                    }
                }
            }
        }

        let mut actor_count = 0usize;

        for ((src, _dst), mut ds) in conn_map.into_iter() {
            if let Some(actor) = ds.take_actor() {
                let mbox = Mailbox::new();
                self.sim_init = self.sim_init.add_model(actor, mbox, "AppSourceBuffer");
                actor_count += 1;
            }
            app_sources.insert(src, ds);
        }

        for ds in app_sources.values_mut() {
            if let Some(actor) = ds.take_actor() {
                let mbox = Mailbox::new();
                self.sim_init = self.sim_init.add_model(actor, mbox, "AppSourceBuffer");
                actor_count += 1;
            }
        }

        debug!(
            "Initialized {} AppSource actors for {} TCP flows with {} unique source handles",
            actor_count,
            flow_id_to_source_handle.len(),
            app_sources.len()
        );

        let mut ui_mbox: Mailbox<UserInterface> = Mailbox::with_capacity(self.mailbox_capacity);

        // constructs the network graph by connecting the packet switches
        self = self.connect(graph);

        // attaches packet sources and sinks from flows to hosts in the network graph
        (self, ui_mbox) =
            self.attach_flows(&mut statistics, ui_mbox, Some(flow_id_to_source_handle));

        // computes feasible paths for all flows, and sets FIBs for all switches
        self.route_flows();

        // creates and activates a UserInterface coroutine
        self = self.activate_ui(ui_mbox);
        let duration = self.duration;
        let config_path = self.config_path.clone();

        // activates all the switches and initializes the simulation
        let mut sim = self.init_sim();

        // starts the performance measurement clock
        let mut wall_sampler = start_wall_clock_concurrency_sampler(&config_path);
        if wall_sampler.is_some() {
            reset_peak_concurrency();
        }
        let timer = std::time::Instant::now();

        // starts the simulation
        match sim.step_until(Duration::from_secs_f64(duration)) {
            Ok(()) => {}
            Err(err) => {
                error!("Simulation stopped early: {err}");
            }
        }

        if let Some(stats) = wall_sampler.as_mut().and_then(|s| s.stop()) {
            info!(
                "Concurrency: peak {}, average {:.3} (wall-clock, {:.3}s).",
                peak_concurrency(),
                stats.average,
                stats.elapsed.as_secs_f64()
            );
        }
        sim = statistics.collect_statistics(sim);

        // logs the remaining reports
        CsvLogger::get_instance().flush_reports();

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_switches() {
        // Set number of switches
        set_num_switches(3);

        // Initialize switches
        let switches = Topology::init_switches();

        // Assertions to verify switches are initialized correctly
        assert_eq!(switches.len(), 3);
        for (id, switch) in switches.iter() {
            assert_eq!(*id, switch.id());
        }
    }
}

#[cfg(test)]
mod ring_allreduce_serialization_tests {
    use super::*;
    use crate::flows::collective::{Collective, CollectiveType};
    use crate::flows::flow::FlowType;
    use crate::flows::{DistributionInfo, TrafficCharacteristics};
    use std::collections::HashMap;

    // Helper: minimal, unused-in-test switch config. Adjust DropStrategy variant if needed.
    fn dummy_switch_cfg() -> SwitchConfig {
        use crate::schedulers::drop::DropStrategy;
        use crate::switches::SchedulingDiscipline;
        SwitchConfig {
            port_rate: 1.0,
            capacity: 64,
            discipline: SchedulingDiscipline::FIFO,
            // If your DropStrategy variant name differs, change it here
            drop: DropStrategy::TailDrop,
            ecn_threshold: None,
            run_batch_size: None,
            weights: None,
            priorities: None,
            vticks: None,
        }
    }

    #[test]
    fn ring_allreduce_n4_serialization() {
        let n = 4usize;
        let sources: Vec<usize> = (0..n).collect();
        let sinks: Vec<usize> = (0..n).map(|i| (i + 1) % n).collect();

        // Minimal traffic; only size matters (byte-based).
        let traffic = TrafficCharacteristics::new(
            0.0,        // initial_delay
            None,       // duration
            Some(4096), // size (bytes)
            DistributionInfo::Exp { lambda: 1.0 },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            None, // tcp opts not needed for this test
        );

        // Construct a collective directly (no file parsing).
        let collective = Collective {
            id: 77,
            collective_type: CollectiveType::RingAllReduce,
            first_flow_id: 1000,
            flow_type: FlowType::TCP,
            flow_count: n, // ignored in ring branch; recomputed in process_collectives()
            graph: None,
            paths: None,
            sources: sources.clone(),
            sinks: sinks.clone(),
            routing: None,
            traffic: traffic.clone(),
        };

        // Build a minimal Topology that lets us call process_collectives() only.
        let mut topo = Topology {
            sim_init: SimInit::new(),
            runtime_num_threads: 1,
            graph: UnGraph::<usize, ()>::default(),
            hosts: sources.clone(),
            switches: HashMap::new(),
            switch_mailboxes: HashMap::new(),
            flows: Vec::new(),
            collectives: vec![collective],
            switch_config: dummy_switch_cfg(),
            link_config: LinkConfig::default(),
            #[cfg(feature = "l2_pfc")]
            fib_views: HashMap::new(),
            #[cfg(feature = "l2_pfc")]
            output_states: Arc::new(RwLock::new(HashMap::new())),
            mailbox_capacity: 16,
            config_path: String::new(),
            duration: 1.0,
            app_source_cfg: crate::flows::app_source::AppBufferConfig::default(),
        };

        // Produce flows for the collective (no scheduling, no routing).
        topo.process_collectives();

        // 2 * n * (n - 1) flows: scatter-reduce and allgather
        assert_eq!(
            topo.flows.len(),
            2 * n * (n - 1),
            "expected 24 flows for n=4"
        );

        // id -> &Flow lookup
        let mut by_id: HashMap<usize, &crate::flows::flow::Flow> = HashMap::new();
        for f in &topo.flows {
            by_id.insert(f.id, f);
        }

        // By construction order: first n*(n-1) are scatter; remaining are gather.
        let scatter = &topo.flows[..(n * (n - 1))];
        let gather = &topo.flows[(n * (n - 1))..];

        for rank in 0..n {
            let src = sources[rank];
            let dst = sources[(rank + 1) % n];

            // There are exactly (n-1) flows on this link in each phase.
            let mut s_ids: Vec<usize> = scatter
                .iter()
                .filter(|f| f.source_host == src && f.sink_host == dst)
                .map(|f| f.id)
                .collect();
            s_ids.sort_unstable();

            let mut g_ids: Vec<usize> = gather
                .iter()
                .filter(|f| f.source_host == src && f.sink_host == dst)
                .map(|f| f.id)
                .collect();
            g_ids.sort_unstable();

            assert_eq!(s_ids.len(), n - 1, "rank {} scatter count", rank);
            assert_eq!(g_ids.len(), n - 1, "rank {} gather count", rank);

            // --- These asserts require the fix that populates `starts_after` on successors. ---

            // Scatter should be serialized: S1 -> S2 -> S3 via starts_after on successors.
            assert!(
                by_id[&s_ids[1]].starts_after.contains(&s_ids[0]),
                "scatter[1] should wait for scatter[0] for rank {}",
                rank
            );
            assert!(
                by_id[&s_ids[2]].starts_after.contains(&s_ids[1]),
                "scatter[2] should wait for scatter[1] for rank {}",
                rank
            );

            // The first gather step should wait for the last scatter step (cross-phase gating).
            assert!(
                by_id[&g_ids[0]].starts_after.contains(&s_ids[n - 2]),
                "gather[0] should wait for last scatter for rank {}",
                rank
            );

            // Gather should be serialized: G1 -> G2 -> G3 via starts_after on successors.
            assert!(
                by_id[&g_ids[1]].starts_after.contains(&g_ids[0]),
                "gather[1] should wait for gather[0] for rank {}",
                rank
            );
            assert!(
                by_id[&g_ids[2]].starts_after.contains(&g_ids[1]),
                "gather[2] should wait for gather[1] for rank {}",
                rank
            );
        }
    }
}

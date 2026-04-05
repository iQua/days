//! C ABI exports for embedding days native simulation runtime from external simulators.

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::ffi::{CStr, c_char, c_void};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::simulation::{Address, Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;

use crate::flows::DistributionInfo;
use crate::flows::collective::build_ring_allreduce_flow_templates;
use crate::flows::packet::Packet;
use crate::flows::wire::Wire;
use crate::schedulers::drop::{CapacityUnit, DEFAULT_ECN_THRESHOLD, DropStrategy};
use crate::schedulers::port::Port;
use crate::switches::switch::PacketSwitch;
use crate::utils::time::set_time_quantum_ns;

/// Callback type consumed by the C ABI.
type DaysCallback = unsafe extern "C" fn(*mut c_void);

#[derive(Clone, Copy)]
struct RuntimeCallback {
    callback: DaysCallback,
    callback_arg: usize,
}

unsafe impl Send for RuntimeCallback {}
unsafe impl Sync for RuntimeCallback {}

#[derive(Clone, Copy)]
struct ReadyCallback {
    when_ns: u64,
    sequence: u64,
    callback: RuntimeCallback,
}

impl PartialEq for ReadyCallback {
    fn eq(&self, other: &Self) -> bool {
        self.when_ns == other.when_ns && self.sequence == other.sequence
    }
}

impl Eq for ReadyCallback {}

impl PartialOrd for ReadyCallback {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadyCallback {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior via BinaryHeap.
        other
            .when_ns
            .cmp(&self.when_ns)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

struct ReadyCallbackQueue {
    next_sequence: AtomicU64,
    heap: Mutex<BinaryHeap<ReadyCallback>>,
}

impl ReadyCallbackQueue {
    fn new() -> Self {
        Self {
            next_sequence: AtomicU64::new(0),
            heap: Mutex::new(BinaryHeap::new()),
        }
    }

    fn clear(&self) {
        if let Ok(mut heap) = self.heap.lock() {
            heap.clear();
        }
        self.next_sequence.store(0, AtomicOrdering::Relaxed);
    }

    fn push(&self, when_ns: u64, callback: RuntimeCallback) {
        let sequence = self.next_sequence.fetch_add(1, AtomicOrdering::Relaxed);
        let event = ReadyCallback {
            when_ns,
            sequence,
            callback,
        };
        if let Ok(mut heap) = self.heap.lock() {
            heap.push(event);
        }
    }

    fn pop(&self) -> Option<ReadyCallback> {
        self.heap.lock().ok()?.pop()
    }

    fn is_empty(&self) -> bool {
        self.heap.lock().map(|heap| heap.is_empty()).unwrap_or(true)
    }
}

#[derive(Clone, Copy)]
struct ScheduleRequest {
    delay_ns: u64,
    callback: RuntimeCallback,
}

unsafe impl Send for ScheduleRequest {}
unsafe impl Sync for ScheduleRequest {}

struct RuntimeCallbackScheduler {
    ready_callbacks: Arc<ReadyCallbackQueue>,
    pending_events: Arc<AtomicUsize>,
    next_token: u64,
    scheduled_callbacks: HashMap<u64, RuntimeCallback>,
}

#[Model]
impl RuntimeCallbackScheduler {
    fn new(ready_callbacks: Arc<ReadyCallbackQueue>, pending_events: Arc<AtomicUsize>) -> Self {
        Self {
            ready_callbacks,
            pending_events,
            next_token: 0,
            scheduled_callbacks: HashMap::new(),
        }
    }

    async fn schedule_callback(&mut self, request: ScheduleRequest, cx: &Context<Self>) {
        if request.delay_ns == 0 {
            self.ready_callbacks
                .push(sim_time_to_ns(cx.time()), request.callback);
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
            return;
        }

        // schedule_event always runs from current simulation time.
        let token = self.next_token;
        self.next_token = self.next_token.saturating_add(1);
        self.scheduled_callbacks.insert(token, request.callback);
        cx.schedule_event(
            Duration::from_nanos(request.delay_ns),
            schedulable!(Self::fire_callback),
            token,
        )
        .unwrap();
    }

    #[nexosim(schedulable)]
    async fn fire_callback(&mut self, token: u64, cx: &Context<Self>) {
        if let Some(callback) = self.scheduled_callbacks.remove(&token) {
            self.ready_callbacks.push(sim_time_to_ns(cx.time()), callback);
        }
        self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

type InflightCallbacks = Arc<Mutex<HashMap<usize, RuntimeCallback>>>;

#[derive(Clone, Copy)]
struct AllReduceParticipant {
    rank: usize,
    callback: RuntimeCallback,
}

struct PendingAllReduceRegistration {
    collective_kind: RingCollectiveKind,
    group_size: usize,
    ranks: Vec<usize>,
    bytes: u64,
    ring_channels: usize,
    participants: Vec<AllReduceParticipant>,
}

struct RuntimeCollectiveFlow {
    channel_id: usize,
    src: i32,
    dst: i32,
    bytes: u64,
    remaining_prereqs: usize,
    dependents: Vec<usize>,
    submitted: bool,
    completed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AllReduceExecMode {
    Dag,
    NcclCompat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RingCollectiveKind {
    AllReduce,
    AllGather,
    ReduceScatter,
}

impl RingCollectiveKind {
    fn as_str(self) -> &'static str {
        match self {
            RingCollectiveKind::AllReduce => "allreduce",
            RingCollectiveKind::AllGather => "allgather",
            RingCollectiveKind::ReduceScatter => "reducescatter",
        }
    }

    fn nccl_compat_stage_count(self, group_size: usize) -> usize {
        let ring_steps = group_size.saturating_sub(1);
        match self {
            RingCollectiveKind::AllReduce => 2usize.saturating_mul(ring_steps),
            RingCollectiveKind::AllGather | RingCollectiveKind::ReduceScatter => ring_steps,
        }
    }

    fn uses_module_dag_templates(self) -> bool {
        self == RingCollectiveKind::AllReduce
    }
}

struct ActiveAllReduceCollective {
    collective_kind: RingCollectiveKind,
    mode: AllReduceExecMode,
    group_size: usize,
    ranks: Vec<usize>,
    bytes: u64,
    ring_channels: usize,
    flows: Vec<RuntimeCollectiveFlow>,
    qp_busy: HashSet<(usize, i32, i32)>,
    completed_flows: usize,
    participant_callbacks: Vec<RuntimeCallback>,
    participant_callbacks_by_rank: HashMap<usize, RuntimeCallback>,
    registered_ranks: HashSet<usize>,
    remaining_local_flows: HashMap<usize, usize>,
    notified_ranks: HashSet<usize>,
    collective_complete: bool,
}

#[derive(Clone, Copy)]
struct AllReduceFlowContext {
    runtime: usize,
    op_id: u64,
    flow_index: usize,
}

#[derive(Clone, Copy)]
struct AllReduceFlowSubmitContext {
    runtime: usize,
    op_id: u64,
    flow_index: usize,
    src: i32,
    dst: i32,
    bytes: u64,
}

struct RuntimeHostSink {
    node_id: usize,
    inflight_callbacks: InflightCallbacks,
    ready_callbacks: Arc<ReadyCallbackQueue>,
    pending_events: Arc<AtomicUsize>,
}

impl RuntimeHostSink {
    fn new(
        node_id: usize,
        inflight_callbacks: InflightCallbacks,
        ready_callbacks: Arc<ReadyCallbackQueue>,
        pending_events: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            node_id,
            inflight_callbacks,
            ready_callbacks,
            pending_events,
        }
    }

    async fn packet_received(&mut self, packet: Packet, cx: &Context<Self>) {
        if packet.ack.is_some() || packet.control.is_some() {
            return;
        }

        if packet.flow_id == 0 || packet.flow_id == usize::MAX {
            return;
        }

        if packet.flow_id == self.node_id {
            // This is allowed only if someone intentionally uses host-id flow-id.
            // We still route by registered in-flight flow IDs below.
        }

        let callback = {
            let mut inflight = match self.inflight_callbacks.lock() {
                Ok(map) => map,
                Err(_) => return,
            };
            inflight.remove(&packet.flow_id)
        };

        if let Some(callback) = callback {
            self.ready_callbacks
                .push(sim_time_to_ns(cx.time()), callback);
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }
}

impl Model for RuntimeHostSink {
    type Env = ();
}

#[derive(Clone)]
struct TopologyEdge {
    to: usize,
    bandwidth_bps: f64,
    latency_s: f64,
    latency_ns: f64,
}

#[derive(Clone)]
struct TopologyLink {
    src: usize,
    dst: usize,
    bandwidth_bps: f64,
    latency_s: f64,
    latency_ns: f64,
}

#[derive(Clone)]
struct RuntimeTopology {
    node_num: usize,
    links: Vec<TopologyLink>,
    adjacency: Vec<Vec<TopologyEdge>>,
}

impl RuntimeTopology {
    fn load(path: &str) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("failed to open topology file: {e}"))?;
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        let header_line =
            next_non_empty_line(&mut lines)?.ok_or_else(|| "topology file is empty".to_string())?;

        let header_tokens: Vec<&str> = header_line.split_whitespace().collect();
        if header_tokens.len() < 6 {
            return Err(format!("invalid topology header: {header_line}"));
        }

        let node_num = header_tokens[0]
            .parse::<usize>()
            .map_err(|_| format!("invalid node count in header: {}", header_tokens[0]))?;
        let link_count = header_tokens[4]
            .parse::<usize>()
            .map_err(|_| format!("invalid link count in header: {}", header_tokens[4]))?;

        if node_num == 0 {
            return Err("node count must be > 0".to_string());
        }

        // The second non-empty line is the switch-id list in daytone topology files.
        let _switch_line = next_non_empty_line(&mut lines)?
            .ok_or_else(|| "missing switch-id line in topology file".to_string())?;

        let mut links = Vec::with_capacity(link_count);
        let mut adjacency = vec![Vec::<TopologyEdge>::new(); node_num];

        for edge_idx in 0..link_count {
            let line = next_non_empty_line(&mut lines)?.ok_or_else(|| {
                format!("topology file ended early, missing link line at index {edge_idx}")
            })?;

            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.len() < 5 {
                return Err(format!("invalid link line at index {edge_idx}: {line}"));
            }

            let src = tokens[0]
                .parse::<usize>()
                .map_err(|_| format!("invalid src node id at link index {edge_idx}: {line}"))?;
            let dst = tokens[1]
                .parse::<usize>()
                .map_err(|_| format!("invalid dst node id at link index {edge_idx}: {line}"))?;

            if src >= node_num || dst >= node_num {
                return Err(format!(
                    "out-of-range node id at link index {edge_idx}: src={src} dst={dst}"
                ));
            }

            let bandwidth_bps = parse_bandwidth_to_bps(tokens[2]).ok_or_else(|| {
                format!(
                    "invalid bandwidth token at link index {edge_idx}: {}",
                    tokens[2]
                )
            })?;
            if bandwidth_bps <= 0.0 {
                return Err(format!(
                    "non-positive bandwidth at link index {edge_idx}: {bandwidth_bps}"
                ));
            }

            let latency_ns = parse_delay_to_ns(tokens[3]).ok_or_else(|| {
                format!(
                    "invalid delay token at link index {edge_idx}: {}",
                    tokens[3]
                )
            })?;
            let latency_s = (latency_ns / 1e9).max(0.0);

            links.push(TopologyLink {
                src,
                dst,
                bandwidth_bps,
                latency_s,
                latency_ns,
            });

            adjacency[src].push(TopologyEdge {
                to: dst,
                bandwidth_bps,
                latency_s,
                latency_ns,
            });
            adjacency[dst].push(TopologyEdge {
                to: src,
                bandwidth_bps,
                latency_s,
                latency_ns,
            });
        }

        Ok(Self {
            node_num,
            links,
            adjacency,
        })
    }

    fn shortest_path(&self, src: usize, dst: usize) -> Option<Vec<usize>> {
        if src >= self.node_num || dst >= self.node_num {
            return None;
        }
        if src == dst {
            return Some(vec![src]);
        }

        let mut dist = vec![f64::INFINITY; self.node_num];
        let mut prev = vec![None::<usize>; self.node_num];

        #[derive(Copy, Clone, PartialEq)]
        struct Entry {
            cost: f64,
            node: usize,
        }

        impl Eq for Entry {}

        impl Ord for Entry {
            fn cmp(&self, other: &Self) -> Ordering {
                other
                    .cost
                    .partial_cmp(&self.cost)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| other.node.cmp(&self.node))
            }
        }

        impl PartialOrd for Entry {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut heap = BinaryHeap::<Entry>::new();
        dist[src] = 0.0;
        heap.push(Entry {
            cost: 0.0,
            node: src,
        });

        while let Some(entry) = heap.pop() {
            if entry.cost > dist[entry.node] + 1e-12 {
                continue;
            }
            if entry.node == dst {
                break;
            }

            for edge in &self.adjacency[entry.node] {
                let candidate = entry.cost + edge.latency_ns.max(0.0);
                let better = candidate + 1e-12 < dist[edge.to];
                let same_but_stabler =
                    (candidate - dist[edge.to]).abs() <= 1e-12 && prev[edge.to].is_some();

                if better || same_but_stabler {
                    dist[edge.to] = candidate;
                    prev[edge.to] = Some(entry.node);
                    heap.push(Entry {
                        cost: candidate,
                        node: edge.to,
                    });
                }
            }
        }

        if !dist[dst].is_finite() {
            return None;
        }

        let mut path = Vec::<usize>::new();
        let mut current = dst;
        path.push(current);

        while current != src {
            let p = prev[current]?;
            current = p;
            path.push(current);
        }

        path.reverse();
        Some(path)
    }
}

struct RuntimeCore {
    stop_flag: bool,
    topology: Option<RuntimeTopology>,
    simulation: Option<Simulation>,
    switch_addresses: Vec<Address<PacketSwitch>>,
    callback_scheduler_addr: Option<Address<RuntimeCallbackScheduler>>,
    ready_callbacks: Arc<ReadyCallbackQueue>,
    inflight_callbacks: InflightCallbacks,
    pending_events: Arc<AtomicUsize>,
    next_flow_id: usize,
    next_packet_id: usize,
    flow_meta: HashMap<usize, (usize, usize, u64)>,
    pending_allreduce: HashMap<u64, PendingAllReduceRegistration>,
    active_allreduce: HashMap<u64, ActiveAllReduceCollective>,
    trace_submit_send_count: usize,
}

impl RuntimeCore {
    fn new() -> Self {
        Self {
            stop_flag: false,
            topology: None,
            simulation: None,
            switch_addresses: Vec::new(),
            callback_scheduler_addr: None,
            ready_callbacks: Arc::new(ReadyCallbackQueue::new()),
            inflight_callbacks: Arc::new(Mutex::new(HashMap::new())),
            pending_events: Arc::new(AtomicUsize::new(0)),
            next_flow_id: 1,
            next_packet_id: 1,
            flow_meta: HashMap::new(),
            pending_allreduce: HashMap::new(),
            active_allreduce: HashMap::new(),
            trace_submit_send_count: 0,
        }
    }

    fn current_time_ns(&self) -> u64 {
        self.simulation
            .as_ref()
            .map(|sim| sim_time_to_ns(sim.time()))
            .unwrap_or(0)
    }

    fn current_time_s(&self) -> f64 {
        self.simulation
            .as_ref()
            .map(|sim| sim_time_to_s(sim.time()))
            .unwrap_or(0.0)
    }

    fn set_topology(&mut self, topology: RuntimeTopology) -> Result<(), String> {
        self.topology = Some(topology);
        self.rebuild_simulation()
    }

    fn rebuild_simulation(&mut self) -> Result<(), String> {
        let topology = self
            .topology
            .clone()
            .ok_or_else(|| "topology is not loaded".to_string())?;

        self.stop_flag = false;
        self.next_flow_id = 1;
        self.next_packet_id = 1;
        self.pending_events.store(0, AtomicOrdering::Relaxed);
        self.ready_callbacks.clear();
        self.flow_meta.clear();
        self.pending_allreduce.clear();
        self.active_allreduce.clear();

        if let Ok(mut inflight) = self.inflight_callbacks.lock() {
            inflight.clear();
        }

        let node_num = topology.node_num;
        let mut sim_init = SimInit::new();
        let time_quantum_ns = ffi_time_quantum_ns();
        if time_quantum_ns == 0 {
            set_time_quantum_ns(None);
        } else {
            set_time_quantum_ns(Some(time_quantum_ns));
            sim_init = sim_init.set_time_quantum_ns(time_quantum_ns);
        }

        let mut switches: Vec<PacketSwitch> = (0..node_num)
            .map(|_| PacketSwitch::new(HashMap::new(), HashMap::new()))
            .collect();

        let switch_mailboxes: Vec<Mailbox<PacketSwitch>> = (0..node_num)
            .map(|_| Mailbox::with_capacity(1024))
            .collect();

        let switch_addresses: Vec<Address<PacketSwitch>> =
            switch_mailboxes.iter().map(|mbox| mbox.address()).collect();

        let callback_scheduler = RuntimeCallbackScheduler::new(
            self.ready_callbacks.clone(),
            self.pending_events.clone(),
        );
        let callback_scheduler_mbox: Mailbox<RuntimeCallbackScheduler> =
            Mailbox::with_capacity(1024);
        let callback_scheduler_addr = callback_scheduler_mbox.address();
        sim_init = sim_init.add_model(
            callback_scheduler,
            callback_scheduler_mbox,
            "RuntimeCallbackScheduler",
        );

        for node_id in 0..node_num {
            let sink = RuntimeHostSink::new(
                node_id,
                self.inflight_callbacks.clone(),
                self.ready_callbacks.clone(),
                self.pending_events.clone(),
            );
            let sink_mbox: Mailbox<RuntimeHostSink> = Mailbox::with_capacity(1024);

            let mut output = Output::default();
            output.connect(RuntimeHostSink::packet_received, &sink_mbox);
            switches[node_id]
                .outputs
                .insert(endpoint_id(node_num, node_id), output);

            sim_init = sim_init.add_model(sink, sink_mbox, "RuntimeHostSink");
        }

        let mut wire_id: usize = 0;
        for link in &topology.links {
            for (src, dst) in [(link.src, link.dst), (link.dst, link.src)] {
                let mut port = Port::new(
                    link.bandwidth_bps,
                    1_000_000,
                    CapacityUnit::Packets,
                    DropStrategy::TailDrop,
                    DEFAULT_ECN_THRESHOLD,
                    None,
                );
                let port_mbox: Mailbox<Port> = Mailbox::with_capacity(1024);

                let mut to_port = Output::default();
                to_port.connect(Port::packet_received, &port_mbox);
                switches[src].outputs.insert(dst, to_port);

                let mut wire = Wire::new(
                    wire_id,
                    DistributionInfo::Uniform {
                        low: link.latency_s,
                        high: link.latency_s,
                    },
                );
                wire_id = wire_id.saturating_add(1);

                let wire_mbox: Mailbox<Wire> = Mailbox::with_capacity(1024);
                port.output.connect(Wire::packet_received, &wire_mbox);
                wire.output
                    .connect(PacketSwitch::packet_received, &switch_addresses[dst]);

                sim_init = sim_init.add_model(port, port_mbox, "Port");
                sim_init = sim_init.add_model(wire, wire_mbox, "Wire");
            }
        }

        for (switch, switch_mbox) in switches.into_iter().zip(switch_mailboxes.into_iter()) {
            sim_init = sim_init.add_model(switch, switch_mbox, "Switch");
        }

        let simulation = sim_init
            .init(MonotonicTime::EPOCH)
            .map_err(|e| format!("failed to initialize simulation: {e:?}"))?;

        self.simulation = Some(simulation);
        self.switch_addresses = switch_addresses;
        self.callback_scheduler_addr = Some(callback_scheduler_addr);

        Ok(())
    }

    fn schedule_event(&mut self, delay_ns: u64, callback: RuntimeCallback) -> Result<(), String> {
        let sim = self
            .simulation
            .as_mut()
            .ok_or_else(|| "simulation is not initialized".to_string())?;

        let callback_addr = self
            .callback_scheduler_addr
            .clone()
            .ok_or_else(|| "callback scheduler is not initialized".to_string())?;

        self.pending_events.fetch_add(1, AtomicOrdering::Relaxed);

        let request = ScheduleRequest { delay_ns, callback };
        if let Err(e) = sim.process_event_fn(
            RuntimeCallbackScheduler::schedule_callback,
            request,
            callback_addr,
        ) {
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
            return Err(format!("failed to schedule callback event: {e}"));
        }

        Ok(())
    }

    fn submit_send(
        &mut self,
        src: i32,
        dst: i32,
        bytes: u64,
        callback: RuntimeCallback,
    ) -> Result<(), String> {
        let topology = self
            .topology
            .as_ref()
            .ok_or_else(|| "topology is not loaded".to_string())?;

        if src < 0 || dst < 0 {
            return Err("src/dst must be non-negative".to_string());
        }

        let src = src as usize;
        let dst = dst as usize;

        if src >= topology.node_num || dst >= topology.node_num {
            return Err(format!(
                "src/dst out of range: src={} dst={} node_num={}",
                src, dst, topology.node_num
            ));
        }

        if bytes == 0 {
            return self.schedule_event(0, callback);
        }

        if bytes > usize::MAX as u64 {
            return Err(format!("message bytes exceed usize::MAX: {bytes}"));
        }

        let path = topology
            .shortest_path(src, dst)
            .ok_or_else(|| format!("no path from src={} to dst={}", src, dst))?;

        if path.is_empty() {
            return Err(format!("invalid path from src={} to dst={}", src, dst));
        }
        let flow_id = self.next_flow_id;
        self.next_flow_id = self.next_flow_id.saturating_add(1);
        self.flow_meta.insert(flow_id, (src, dst, bytes));
        if trace_submit_send_enabled() {
            let trace_idx = self.trace_submit_send_count;
            self.trace_submit_send_count = self.trace_submit_send_count.saturating_add(1);
            if trace_idx < trace_submit_send_max() {
                let callback_ptr = callback.callback as usize;
                let native_ptr = days_allreduce_flow_done_callback as *const () as usize;
                if callback_ptr == native_ptr {
                    let mut op_id = 0u64;
                    let mut op_flow_index = usize::MAX;
                    let ctx_ptr = callback.callback_arg as *const AllReduceFlowContext;
                    if !ctx_ptr.is_null() {
                        unsafe {
                            op_id = (*ctx_ptr).op_id;
                            op_flow_index = (*ctx_ptr).flow_index;
                        }
                    }
                    eprintln!(
                        "[TRACE][submit-send] idx={} t_ns={} kind=native_allreduce op_id={} op_flow={} src={} dst={} bytes={} flow_id={}",
                        trace_idx,
                        self.current_time_ns(),
                        op_id,
                        op_flow_index,
                        src,
                        dst,
                        bytes,
                        flow_id
                    );
                } else {
                    eprintln!(
                        "[TRACE][submit-send] idx={} t_ns={} kind=ffi_send src={} dst={} bytes={} flow_id={}",
                        trace_idx,
                        self.current_time_ns(),
                        src,
                        dst,
                        bytes,
                        flow_id
                    );
                }
            }
        }

        self.pending_events.fetch_add(1, AtomicOrdering::Relaxed);

        {
            let mut inflight = self
                .inflight_callbacks
                .lock()
                .map_err(|_| "failed to lock inflight callback map".to_string())?;
            inflight.insert(flow_id, callback);
        }

        let sim = self
            .simulation
            .as_mut()
            .ok_or_else(|| "simulation is not initialized".to_string())?;

        for hop in path.windows(2) {
            let current = hop[0];
            let next = hop[1];
            if let Err(e) = sim.process_event_fn(
                PacketSwitch::install_fib,
                (flow_id, next),
                self.switch_addresses[current].clone(),
            ) {
                let mut inflight = self
                    .inflight_callbacks
                    .lock()
                    .map_err(|_| "failed to lock inflight callback map".to_string())?;
                inflight.remove(&flow_id);
                self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
                return Err(format!(
                    "failed to install fib on node {} for flow {}: {}",
                    current, flow_id, e
                ));
            }
        }

        if let Err(e) = sim.process_event_fn(
            PacketSwitch::install_fib,
            (flow_id, endpoint_id(topology.node_num, dst)),
            self.switch_addresses[dst].clone(),
        ) {
            let mut inflight = self
                .inflight_callbacks
                .lock()
                .map_err(|_| "failed to lock inflight callback map".to_string())?;
            inflight.remove(&flow_id);
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
            return Err(format!(
                "failed to install sink fib on dst {} for flow {}: {}",
                dst, flow_id, e
            ));
        }

        let packet_id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.saturating_add(1);

        let now_s = sim_time_to_s(sim.time());
        let packet = Packet::new(bytes as usize, packet_id, flow_id, now_s);

        if let Err(e) = sim.process_event_fn(
            PacketSwitch::packet_received,
            packet,
            self.switch_addresses[src].clone(),
        ) {
            let mut inflight = self
                .inflight_callbacks
                .lock()
                .map_err(|_| "failed to lock inflight callback map".to_string())?;
            inflight.remove(&flow_id);
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
            return Err(format!("failed to inject packet from src {}: {}", src, e));
        }

        Ok(())
    }

    fn queue_callbacks_now(&self, callbacks: Vec<RuntimeCallback>) {
        if callbacks.is_empty() {
            return;
        }
        let now_ns = self.current_time_ns();
        for callback in callbacks {
            self.ready_callbacks.push(now_ns, callback);
        }
    }

    fn pending_callbacks_from_active(active: ActiveAllReduceCollective) -> Vec<RuntimeCallback> {
        let mut callbacks = active.participant_callbacks;
        for (rank, callback) in active.participant_callbacks_by_rank {
            if !active.notified_ranks.contains(&rank) {
                callbacks.push(callback);
            }
        }
        callbacks
    }

    fn build_allreduce_flows_from_module(
        &self,
        op_id: u64,
        ranks: &[usize],
        bytes: u64,
        ring_channels: usize,
    ) -> Result<Vec<RuntimeCollectiveFlow>, String> {
        let templates = build_ring_allreduce_flow_templates(ranks);
        if templates.is_empty() {
            return Ok(Vec::new());
        }

        let group_size = ranks.len();
        if group_size == 0 {
            return Ok(Vec::new());
        }

        let effective_channels = ring_channels.max(1);
        let per_flow_bytes = bytes / group_size as u64 / effective_channels as u64;
        let mut flows: Vec<RuntimeCollectiveFlow> =
            Vec::with_capacity(templates.len().saturating_mul(effective_channels));

        for channel_id in 0..effective_channels {
            let channel_base = flows.len();
            for template in templates.iter() {
                flows.push(RuntimeCollectiveFlow {
                    channel_id,
                    src: template.source_host as i32,
                    dst: template.sink_host as i32,
                    bytes: per_flow_bytes,
                    remaining_prereqs: template.starts_after.len(),
                    dependents: Vec::new(),
                    submitted: false,
                    completed: false,
                });
            }

            for (local_flow_index, template) in templates.iter().enumerate() {
                let flow_index = channel_base + local_flow_index;
                for &dep in &template.starts_after {
                    let dep_index = channel_base + dep;
                    if dep_index >= flows.len() {
                        return Err(format!(
                            "invalid allreduce dependency {} for flow {}",
                            dep, flow_index
                        ));
                    }
                    flows[dep_index].dependents.push(flow_index);
                }
            }
        }

        if trace_allreduce_split_enabled() {
            let ring_edges = ranks
                .iter()
                .enumerate()
                .map(|(idx, &src)| format!("{src}->{}", ranks[(idx + 1) % group_size]))
                .collect::<Vec<_>>()
                .join(",");
            let mut flow_bytes = BTreeSet::new();
            for flow in &flows {
                flow_bytes.insert(flow.bytes);
            }
            let flow_bytes_text = flow_bytes
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("|");
            let flow_count_est = group_size
                .saturating_mul(group_size.saturating_sub(1))
                .saturating_mul(2)
                .saturating_mul(effective_channels);
            eprintln!(
                "[TRACE][native-allreduce] op_id={} group_size={} ring_channels={} input_bytes={} \
                 per_ring_chunk_bytes={} per_flow_bytes={} chunk_count={} \
                 flow_count_est={} flow_count_actual={} ring_edges={{{}}}",
                op_id,
                group_size,
                effective_channels,
                bytes,
                per_flow_bytes,
                flow_bytes_text,
                2 * (group_size.saturating_sub(1)),
                flow_count_est,
                flows.len(),
                ring_edges
            );

            let max_flows = trace_allreduce_split_max_flows();
            for (idx, flow) in flows.iter().take(max_flows).enumerate() {
                eprintln!(
                    "[TRACE][native-allreduce-flow] op_id={} idx={} {}->{} bytes={} prereqs={} deps={}",
                    op_id,
                    idx,
                    flow.src,
                    flow.dst,
                    flow.bytes,
                    flow.remaining_prereqs,
                    flow.dependents.len()
                );
            }
        }

        Ok(flows)
    }

    fn build_ring_collective_flows_nccl_compat(
        &self,
        collective_kind: RingCollectiveKind,
        op_id: u64,
        ranks: &[usize],
        bytes: u64,
        ring_channels: usize,
    ) -> Result<Vec<RuntimeCollectiveFlow>, String> {
        let group_size = ranks.len();
        if group_size <= 1 {
            return Ok(Vec::new());
        }

        let effective_channels = ring_channels.max(1);
        let per_flow_bytes = bytes / group_size as u64 / effective_channels as u64;
        let stage_count = collective_kind.nccl_compat_stage_count(group_size);
        let per_channel_flow_count = group_size.saturating_mul(stage_count);

        let mut flows: Vec<RuntimeCollectiveFlow> =
            Vec::with_capacity(per_channel_flow_count.saturating_mul(effective_channels));

        for channel_id in 0..effective_channels {
            let channel_base = flows.len();
            for _stage in 0..stage_count {
                for rank in 0..group_size {
                    flows.push(RuntimeCollectiveFlow {
                        channel_id,
                        src: ranks[rank] as i32,
                        dst: ranks[(rank + 1) % group_size] as i32,
                        bytes: per_flow_bytes,
                        remaining_prereqs: 0,
                        dependents: Vec::new(),
                        submitted: false,
                        completed: false,
                    });
                }
            }

            // Match MockNccl ring-flow model: each stage t flow on rank r depends on
            // stage t-1 flow on rank prev(r), forming the ring wavefront.
            for stage in 1..stage_count {
                for rank in 0..group_size {
                    let prev_rank = (rank + group_size - 1) % group_size;
                    let dep_index = channel_base + (stage - 1) * group_size + prev_rank;
                    let cur_index = channel_base + stage * group_size + rank;
                    if dep_index >= flows.len() || cur_index >= flows.len() {
                        return Err(format!(
                            "invalid {} nccl_compat dependency dep={} cur={}",
                            collective_kind.as_str(),
                            dep_index, cur_index
                        ));
                    }
                    flows[cur_index].remaining_prereqs = 1;
                    flows[dep_index].dependents.push(cur_index);
                }
            }
        }

        if trace_allreduce_split_enabled() {
            let ring_edges = ranks
                .iter()
                .enumerate()
                .map(|(idx, &src)| format!("{src}->{}", ranks[(idx + 1) % group_size]))
                .collect::<Vec<_>>()
                .join(",");
            eprintln!(
                "[TRACE][native-{}-compat] op_id={} group_size={} ring_channels={} input_bytes={} \
                 per_flow_bytes={} stage_count={} flow_count_actual={} ring_edges={{{}}}",
                collective_kind.as_str(),
                op_id,
                group_size,
                effective_channels,
                bytes,
                per_flow_bytes,
                stage_count,
                flows.len(),
                ring_edges
            );
        }

        Ok(flows)
    }

    fn submit_ready_allreduce_flows(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
    ) -> Result<(), String> {
        if runtime_ptr.is_null() {
            return Err("runtime pointer is null".to_string());
        }

        let mut ready_flows: Vec<(
            usize,
            usize,
            i32,
            i32,
            u64,
            AllReduceExecMode,
            RingCollectiveKind,
            usize,
        )> = Vec::new();
        {
            let active = self
                .active_allreduce
                .get_mut(&op_id)
                .ok_or_else(|| format!("collective op {} is not active", op_id))?;
            match active.mode {
                AllReduceExecMode::Dag => {
                    for (flow_index, flow) in active.flows.iter_mut().enumerate() {
                        if !flow.submitted && !flow.completed && flow.remaining_prereqs == 0 {
                            flow.submitted = true;
                            ready_flows.push((
                                flow_index,
                                flow.channel_id,
                                flow.src,
                                flow.dst,
                                flow.bytes,
                                AllReduceExecMode::Dag,
                                active.collective_kind,
                                active.ring_channels,
                            ));
                        }
                    }
                }
                AllReduceExecMode::NcclCompat => {
                    for (flow_index, flow) in active.flows.iter_mut().enumerate() {
                        if flow.submitted || flow.completed || flow.remaining_prereqs != 0 {
                            continue;
                        }
                        if !active.registered_ranks.contains(&(flow.src as usize)) {
                            continue;
                        }
                        let qp_key = (flow.channel_id, flow.src, flow.dst);
                        if active.qp_busy.contains(&qp_key) {
                            continue;
                        }
                        flow.submitted = true;
                        active.qp_busy.insert(qp_key);
                        ready_flows.push((
                            flow_index,
                            flow.channel_id,
                            flow.src,
                            flow.dst,
                            flow.bytes,
                            AllReduceExecMode::NcclCompat,
                            active.collective_kind,
                            active.ring_channels,
                        ));
                    }
                }
            }
        }

        for (flow_index, _channel_id, src, dst, bytes, mode, collective_kind, ring_channels) in ready_flows {
            let submit_delay_ns = if mode == AllReduceExecMode::NcclCompat {
                nccl_compat_submit_delay_ns_for(collective_kind, ring_channels)
            } else {
                0
            };
            let submit_result = if submit_delay_ns > 0 {
                self.schedule_allreduce_flow_submission(
                    runtime_ptr,
                    op_id,
                    flow_index,
                    src,
                    dst,
                    bytes,
                    submit_delay_ns,
                )
            } else {
                self.submit_allreduce_flow_transport(runtime_ptr, op_id, flow_index, src, dst, bytes)
            };
            if let Err(err) = submit_result {
                let active = self.active_allreduce.remove(&op_id);
                if let Some(active) = active {
                    self.queue_callbacks_now(Self::pending_callbacks_from_active(active));
                }
                return Err(format!("failed to submit allreduce flow: {err}"));
            }
        }

        Ok(())
    }

    fn submit_allreduce_flow_transport(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
        flow_index: usize,
        src: i32,
        dst: i32,
        bytes: u64,
    ) -> Result<(), String> {
        let Some(active) = self.active_allreduce.get(&op_id) else {
            return Ok(());
        };
        if flow_index >= active.flows.len() {
            return Ok(());
        }
        let flow = &active.flows[flow_index];
        if flow.completed || !flow.submitted {
            return Ok(());
        }

        let flow_ctx = AllReduceFlowContext {
            runtime: runtime_ptr as usize,
            op_id,
            flow_index,
        };
        let callback = RuntimeCallback {
            callback: days_allreduce_flow_done_callback,
            callback_arg: Box::into_raw(Box::new(flow_ctx)) as usize,
        };
        self.submit_send(src, dst, bytes, callback)
    }

    fn schedule_allreduce_flow_submission(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
        flow_index: usize,
        src: i32,
        dst: i32,
        bytes: u64,
        delay_ns: u64,
    ) -> Result<(), String> {
        let submit_ctx = AllReduceFlowSubmitContext {
            runtime: runtime_ptr as usize,
            op_id,
            flow_index,
            src,
            dst,
            bytes,
        };
        let callback = RuntimeCallback {
            callback: days_allreduce_flow_submit_callback,
            callback_arg: Box::into_raw(Box::new(submit_ctx)) as usize,
        };
        self.schedule_event(delay_ns, callback)
    }

    fn dispatch_allreduce_flow_submission(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        submit_ctx: AllReduceFlowSubmitContext,
    ) {
        let submit_result = self.submit_allreduce_flow_transport(
            runtime_ptr,
            submit_ctx.op_id,
            submit_ctx.flow_index,
            submit_ctx.src,
            submit_ctx.dst,
            submit_ctx.bytes,
        );
        if let Err(err) = submit_result {
            let active = self.active_allreduce.remove(&submit_ctx.op_id);
            if let Some(active) = active {
                self.queue_callbacks_now(Self::pending_callbacks_from_active(active));
            }
            eprintln!(
                "[ERROR][submit-allreduce] op_id={} flow_index={} err={}",
                submit_ctx.op_id, submit_ctx.flow_index, err
            );
        }
    }

    fn allreduce_completion_delay_ns(&self, op_id: u64) -> u64 {
        let Some(active) = self.active_allreduce.get(&op_id) else {
            return 0;
        };
        match active.mode {
            AllReduceExecMode::NcclCompat => {
                if active.collective_kind == RingCollectiveKind::AllReduce {
                    nccl_compat_completion_delay_ns()
                } else {
                    0
                }
            }
            AllReduceExecMode::Dag => 0,
        }
    }

    fn complete_allreduce_flow(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
        flow_index: usize,
    ) {
        let mut completion_callbacks: Vec<RuntimeCallback> = Vec::new();
        let mut collective_complete = false;
        let mut should_remove = false;
        {
            let active = match self.active_allreduce.get_mut(&op_id) {
                Some(active) => active,
                None => return,
            };

            if flow_index >= active.flows.len() || active.flows[flow_index].completed {
                return;
            }

            active.flows[flow_index].completed = true;
            active.completed_flows = active.completed_flows.saturating_add(1);

            let dependents = active.flows[flow_index].dependents.clone();
            for dependent in dependents {
                if let Some(flow) = active.flows.get_mut(dependent) {
                    if flow.remaining_prereqs > 0 {
                        flow.remaining_prereqs -= 1;
                    }
                }
            }

            let flow = &active.flows[flow_index];
            let src_rank = flow.src as usize;
            let dst_rank = flow.dst as usize;
            if active.mode == AllReduceExecMode::NcclCompat {
                active.qp_busy.remove(&(flow.channel_id, flow.src, flow.dst));

                for rank in [src_rank, dst_rank] {
                    if let Some(remaining) = active.remaining_local_flows.get_mut(&rank) {
                        if *remaining > 0 {
                            *remaining -= 1;
                        }
                        if *remaining == 0 && active.notified_ranks.insert(rank) {
                            if let Some(callback) = active.participant_callbacks_by_rank.get(&rank)
                            {
                                completion_callbacks.push(*callback);
                            }
                        }
                    }
                }
            }

            if active.completed_flows == active.flows.len() {
                active.collective_complete = true;
                if active.mode == AllReduceExecMode::Dag {
                    completion_callbacks.extend(active.participant_callbacks.clone());
                }
            }
            collective_complete = active.collective_complete;
            should_remove = match active.mode {
                AllReduceExecMode::Dag => active.collective_complete,
                AllReduceExecMode::NcclCompat => {
                    active.collective_complete
                        && active.registered_ranks.len() == active.group_size
                        && active.notified_ranks.len() == active.group_size
                }
            };
        }

        if !completion_callbacks.is_empty() {
            self.queue_callbacks_now(completion_callbacks);
        }
        if should_remove {
            self.active_allreduce.remove(&op_id);
            return;
        }
        if collective_complete {
            return;
        }

        if self
            .submit_ready_allreduce_flows(runtime_ptr, op_id)
            .is_err()
        {
            if let Some(active) = self.active_allreduce.remove(&op_id) {
                self.queue_callbacks_now(Self::pending_callbacks_from_active(active));
            }
        }
    }

    fn submit_allreduce_collective(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
        rank: usize,
        ranks: Vec<usize>,
        bytes: u64,
        ring_channels: usize,
        callback: RuntimeCallback,
    ) -> Result<(), String> {
        self.submit_ring_collective(
            runtime_ptr,
            RingCollectiveKind::AllReduce,
            op_id,
            rank,
            ranks,
            bytes,
            ring_channels,
            callback,
        )
    }

    fn submit_allgather_collective(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
        rank: usize,
        ranks: Vec<usize>,
        bytes: u64,
        ring_channels: usize,
        callback: RuntimeCallback,
    ) -> Result<(), String> {
        self.submit_ring_collective(
            runtime_ptr,
            RingCollectiveKind::AllGather,
            op_id,
            rank,
            ranks,
            bytes,
            ring_channels,
            callback,
        )
    }

    fn submit_reducescatter_collective(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        op_id: u64,
        rank: usize,
        ranks: Vec<usize>,
        bytes: u64,
        ring_channels: usize,
        callback: RuntimeCallback,
    ) -> Result<(), String> {
        self.submit_ring_collective(
            runtime_ptr,
            RingCollectiveKind::ReduceScatter,
            op_id,
            rank,
            ranks,
            bytes,
            ring_channels,
            callback,
        )
    }

    fn submit_ring_collective(
        &mut self,
        runtime_ptr: *mut DaysRuntime,
        collective_kind: RingCollectiveKind,
        op_id: u64,
        rank: usize,
        ranks: Vec<usize>,
        bytes: u64,
        ring_channels: usize,
        callback: RuntimeCallback,
    ) -> Result<(), String> {
        let collective_name = collective_kind.as_str();
        if runtime_ptr.is_null() {
            return Err("runtime pointer is null".to_string());
        }

        if ranks.is_empty() {
            return Err(format!("{collective_name} group ranks must be non-empty"));
        }
        if !ranks.contains(&rank) {
            return Err(format!(
                "{collective_name} registration rank {} is not in group {:?}",
                rank, ranks
            ));
        }

        let mut dedup = HashSet::with_capacity(ranks.len());
        for &node in &ranks {
            if !dedup.insert(node) {
                return Err(format!("{collective_name} group has duplicate node {}", node));
            }
        }

        let topology = self
            .topology
            .as_ref()
            .ok_or_else(|| "topology is not loaded".to_string())?;
        for &node in &ranks {
            if node >= topology.node_num {
                return Err(format!(
                    "{collective_name} group node {} out of range (node_num={})",
                    node, topology.node_num
                ));
            }
        }

        let group_size = ranks.len();
        let ring_channels = ring_channels.max(1);
        let exec_mode = ring_collective_exec_mode(collective_kind);
        match exec_mode {
            AllReduceExecMode::Dag => {
                if !collective_kind.uses_module_dag_templates() {
                    return Err(format!(
                        "{collective_name} does not support dag mode; set DAYS_ALLREDUCE_EXEC_MODE=nccl_compat",
                    ));
                }
                match self.pending_allreduce.entry(op_id) {
                    std::collections::hash_map::Entry::Occupied(mut occupied) => {
                        let entry = occupied.get_mut();
                        if entry.collective_kind != collective_kind
                            || entry.group_size != group_size
                            || entry.ranks != ranks
                            || entry.bytes != bytes
                            || entry.ring_channels != ring_channels
                        {
                            return Err(format!(
                                "{} op {} has inconsistent registrations",
                                collective_name, op_id
                            ));
                        }
                        if entry.participants.iter().any(|p| p.rank == rank) {
                            return Err(format!(
                                "{} op {} duplicate registration from rank {}",
                                collective_name, op_id, rank
                            ));
                        }
                        entry
                            .participants
                            .push(AllReduceParticipant { rank, callback });
                    }
                    std::collections::hash_map::Entry::Vacant(vacant) => {
                        vacant.insert(PendingAllReduceRegistration {
                            collective_kind,
                            group_size,
                            ranks: ranks.clone(),
                            bytes,
                            ring_channels,
                            participants: vec![AllReduceParticipant { rank, callback }],
                        });
                    }
                }

                let ready_to_start = self
                    .pending_allreduce
                    .get(&op_id)
                    .map(|entry| entry.participants.len() == entry.group_size)
                    .unwrap_or(false);
                if !ready_to_start {
                    return Ok(());
                }

                let registration = self
                    .pending_allreduce
                    .remove(&op_id)
                    .ok_or_else(|| format!("missing {collective_name} registration for op {}", op_id))?;
                let participant_callbacks: Vec<RuntimeCallback> = registration
                    .participants
                    .iter()
                    .map(|p| p.callback)
                    .collect();

                if registration.group_size <= 1 || registration.bytes == 0 {
                    self.queue_callbacks_now(participant_callbacks);
                    return Ok(());
                }

                let flows = self.build_allreduce_flows_from_module(
                    op_id,
                    &registration.ranks,
                    registration.bytes,
                    registration.ring_channels,
                )?;
                if flows.is_empty() {
                    self.queue_callbacks_now(participant_callbacks);
                    return Ok(());
                }

                if trace_allreduce_split_enabled() {
                    eprintln!("[TRACE][native-allreduce-mode] op_id={} mode=dag", op_id);
                }

                let mut registered_ranks = HashSet::with_capacity(registration.group_size);
                for participant in &registration.participants {
                    registered_ranks.insert(participant.rank);
                }

                self.active_allreduce.insert(
                    op_id,
                    ActiveAllReduceCollective {
                        collective_kind,
                        mode: AllReduceExecMode::Dag,
                        group_size: registration.group_size,
                        ranks: registration.ranks,
                        bytes: registration.bytes,
                        ring_channels: registration.ring_channels,
                        flows,
                        qp_busy: HashSet::new(),
                        completed_flows: 0,
                        participant_callbacks,
                        participant_callbacks_by_rank: HashMap::new(),
                        registered_ranks,
                        remaining_local_flows: HashMap::new(),
                        notified_ranks: HashSet::new(),
                        collective_complete: false,
                    },
                );

                self.submit_ready_allreduce_flows(runtime_ptr, op_id)
            }
            AllReduceExecMode::NcclCompat => {
                if group_size <= 1 || bytes == 0 {
                    self.queue_callbacks_now(vec![callback]);
                    return Ok(());
                }

                if let Some(active) = self.active_allreduce.get_mut(&op_id) {
                    let mut callback_now: Option<RuntimeCallback> = None;
                    let should_submit_more;
                    let should_remove;
                    if active.collective_kind != collective_kind
                        || active.mode != AllReduceExecMode::NcclCompat
                        || active.group_size != group_size
                        || active.ranks != ranks
                        || active.bytes != bytes
                        || active.ring_channels != ring_channels
                    {
                        return Err(format!(
                            "{} op {} has inconsistent registrations",
                            collective_name, op_id
                        ));
                    }
                    if !active.registered_ranks.insert(rank) {
                        return Err(format!(
                            "{} op {} duplicate registration from rank {}",
                            collective_name, op_id, rank
                        ));
                    }
                    active.participant_callbacks_by_rank.insert(rank, callback);
                    if active.remaining_local_flows.get(&rank).copied().unwrap_or(0) == 0
                        && active.notified_ranks.insert(rank)
                    {
                        callback_now = Some(callback);
                    }

                    should_remove = active.collective_complete
                        && active.registered_ranks.len() == active.group_size
                        && active.notified_ranks.len() == active.group_size;
                    should_submit_more = !active.collective_complete;

                    if let Some(cb) = callback_now {
                        self.queue_callbacks_now(vec![cb]);
                    }
                    if should_remove {
                        self.active_allreduce.remove(&op_id);
                        return Ok(());
                    }
                    if !should_submit_more {
                        return Ok(());
                    }
                    return self.submit_ready_allreduce_flows(runtime_ptr, op_id);
                }

                let flows = self.build_ring_collective_flows_nccl_compat(
                    collective_kind,
                    op_id,
                    &ranks,
                    bytes,
                    ring_channels,
                )?;
                if flows.is_empty() {
                    self.queue_callbacks_now(vec![callback]);
                    return Ok(());
                }

                if trace_allreduce_split_enabled() {
                    eprintln!(
                        "[TRACE][native-{}-mode] op_id={} mode=nccl_compat",
                        collective_name, op_id
                    );
                }

                let mut registered_ranks = HashSet::with_capacity(group_size);
                registered_ranks.insert(rank);
                let mut callbacks_by_rank = HashMap::with_capacity(group_size);
                callbacks_by_rank.insert(rank, callback);
                let mut remaining_local_flows = HashMap::with_capacity(group_size);
                for flow in &flows {
                    *remaining_local_flows.entry(flow.src as usize).or_insert(0) += 1;
                    *remaining_local_flows.entry(flow.dst as usize).or_insert(0) += 1;
                }

                self.active_allreduce.insert(
                    op_id,
                    ActiveAllReduceCollective {
                        collective_kind,
                        mode: AllReduceExecMode::NcclCompat,
                        group_size,
                        ranks,
                        bytes,
                        ring_channels,
                        flows,
                        qp_busy: HashSet::new(),
                        completed_flows: 0,
                        participant_callbacks: Vec::new(),
                        participant_callbacks_by_rank: callbacks_by_rank,
                        registered_ranks,
                        remaining_local_flows,
                        notified_ranks: HashSet::new(),
                        collective_complete: false,
                    },
                );

                self.submit_ready_allreduce_flows(runtime_ptr, op_id)
            }
        }
    }
}

pub struct DaysRuntime {
    core: Mutex<RuntimeCore>,
}

impl DaysRuntime {
    fn new() -> Self {
        Self {
            core: Mutex::new(RuntimeCore::new()),
        }
    }
}

fn complete_allreduce_flow_from_ctx(runtime_ptr: *mut DaysRuntime, flow_ctx: AllReduceFlowContext) {
    if runtime_ptr.is_null() {
        return;
    }
    let runtime = unsafe { &*runtime_ptr };
    let mut core = match runtime.core.lock() {
        Ok(core) => core,
        Err(_) => return,
    };
    core.complete_allreduce_flow(runtime_ptr, flow_ctx.op_id, flow_ctx.flow_index);
}

unsafe extern "C" fn days_allreduce_flow_submit_callback(arg: *mut c_void) {
    if arg.is_null() {
        return;
    }

    let submit_ctx = Box::from_raw(arg as *mut AllReduceFlowSubmitContext);
    let runtime_ptr = submit_ctx.runtime as *mut DaysRuntime;
    if runtime_ptr.is_null() {
        return;
    }

    let runtime = &*runtime_ptr;
    let mut core = match runtime.core.lock() {
        Ok(core) => core,
        Err(_) => return,
    };
    core.dispatch_allreduce_flow_submission(runtime_ptr, *submit_ctx);
}

unsafe extern "C" fn days_allreduce_flow_complete_callback(arg: *mut c_void) {
    if arg.is_null() {
        return;
    }
    let flow_ctx = Box::from_raw(arg as *mut AllReduceFlowContext);
    complete_allreduce_flow_from_ctx(flow_ctx.runtime as *mut DaysRuntime, *flow_ctx);
}

unsafe extern "C" fn days_allreduce_flow_done_callback(arg: *mut c_void) {
    if arg.is_null() {
        return;
    }

    let flow_ctx = Box::from_raw(arg as *mut AllReduceFlowContext);
    let runtime_ptr = flow_ctx.runtime as *mut DaysRuntime;
    if runtime_ptr.is_null() {
        return;
    }

    let delay_ns = {
        let runtime = &*runtime_ptr;
        let mut core = match runtime.core.lock() {
            Ok(core) => core,
            Err(_) => return,
        };
        let delay_ns = core.allreduce_completion_delay_ns(flow_ctx.op_id);
        if delay_ns > 0 {
            let callback = RuntimeCallback {
                callback: days_allreduce_flow_complete_callback,
                callback_arg: Box::into_raw(Box::new(*flow_ctx)) as usize,
            };
            if core.schedule_event(delay_ns, callback).is_ok() {
                return;
            }
        }
        delay_ns
    };
    if delay_ns > 0 {
        eprintln!(
            "[WARN][native-allreduce] failed to schedule delayed completion for op_id={} flow_index={}; completing immediately",
            flow_ctx.op_id, flow_ctx.flow_index
        );
    }
    complete_allreduce_flow_from_ctx(runtime_ptr, *flow_ctx);
}

fn endpoint_id(node_num: usize, node_id: usize) -> usize {
    node_num.saturating_add(node_id)
}

fn sim_time_to_ns(time: MonotonicTime) -> u64 {
    let nanos = time.duration_since(MonotonicTime::EPOCH).as_nanos();
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

fn sim_time_to_s(time: MonotonicTime) -> f64 {
    time.duration_since(MonotonicTime::EPOCH).as_secs_f64()
}

fn ffi_time_quantum_ns() -> u64 {
    // Use a quantized clock by default for FFI runtime to avoid diverging
    // f64-time comparisons between different event paths.
    std::env::var("DAYS_TIME_QUANTUM_NS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(1)
}

fn trace_allreduce_split_enabled() -> bool {
    std::env::var("DAYS_TRACE_ALLREDUCE_SPLIT")
        .ok()
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

fn trace_allreduce_split_max_flows() -> usize {
    std::env::var("DAYS_TRACE_ALLREDUCE_SPLIT_MAX")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(12)
}

fn trace_submit_send_enabled() -> bool {
    std::env::var("DAYS_TRACE_SUBMIT_SEND")
        .ok()
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

fn trace_submit_send_max() -> usize {
    std::env::var("DAYS_TRACE_SUBMIT_SEND_MAX")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(200)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn nccl_compat_submit_delay_ns_for(kind: RingCollectiveKind, ring_channels: usize) -> u64 {
    let global = env_u64("DAYS_NCCL_COMPAT_SUBMIT_DELAY_NS");
    match kind {
        RingCollectiveKind::AllReduce => global.unwrap_or(10),
        RingCollectiveKind::AllGather => env_u64("DAYS_ALLGATHER_NCCL_COMPAT_SUBMIT_DELAY_NS")
            .or(global)
            .unwrap_or(10),
        RingCollectiveKind::ReduceScatter => {
            if let Some(v) = env_u64("DAYS_REDUCESCATTER_NCCL_COMPAT_SUBMIT_DELAY_NS").or(global)
            {
                v
            } else if ring_channels <= 1 {
                // Single-channel RS needs a larger submit skew to align with legacy
                // SimAI NcclFlowModel timing.
                38
            } else {
                // Multi-channel RS matches legacy timing with the baseline skew.
                10
            }
        }
    }
}

fn nccl_compat_completion_delay_ns() -> u64 {
    std::env::var("DAYS_NCCL_COMPAT_COMPLETION_DELAY_NS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

fn allreduce_exec_mode() -> AllReduceExecMode {
    let raw = std::env::var("DAYS_ALLREDUCE_EXEC_MODE")
        .or_else(|_| std::env::var("DAYS_ALLREDUCE_MODEL"))
        .unwrap_or_else(|_| "dag".to_string());
    match raw.trim().to_ascii_lowercase().as_str() {
        "nccl_compat" | "nccl-treeflow" | "nccltreeflow" | "treeflow" | "compat" => {
            AllReduceExecMode::NcclCompat
        }
        _ => AllReduceExecMode::Dag,
    }
}

fn ring_collective_exec_mode(kind: RingCollectiveKind) -> AllReduceExecMode {
    match kind {
        RingCollectiveKind::AllReduce => allreduce_exec_mode(),
        RingCollectiveKind::AllGather | RingCollectiveKind::ReduceScatter => {
            // Align with SimAI MockNccl ring-flow model for AG/RS by default.
            AllReduceExecMode::NcclCompat
        }
    }
}

fn next_non_empty_line<I>(lines: &mut I) -> Result<Option<String>, String>
where
    I: Iterator<Item = Result<String, std::io::Error>>,
{
    for line_result in lines {
        let line = line_result.map_err(|e| format!("failed to read topology file: {e}"))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        return Ok(Some(trimmed.to_string()));
    }
    Ok(None)
}

fn parse_number_and_unit(token: &str) -> Option<(f64, String)> {
    let trimmed: String = token.chars().filter(|c| !c.is_whitespace()).collect();
    if trimmed.is_empty() {
        return None;
    }

    let mut idx = 0usize;
    for (i, ch) in trimmed.char_indices() {
        if !(ch.is_ascii_digit() || ch == '.' || ch == '-' || ch == '+') {
            idx = i;
            break;
        }
        idx = i + ch.len_utf8();
    }

    let number_part = &trimmed[..idx];
    if number_part.is_empty() {
        return None;
    }

    let value = number_part.parse::<f64>().ok()?;
    let unit = trimmed[idx..].to_lowercase();
    Some((value, unit))
}

fn parse_bandwidth_to_bps(token: &str) -> Option<f64> {
    let (numeric, unit) = parse_number_and_unit(token)?;

    if unit.is_empty() {
        return Some(numeric);
    }

    if unit.contains("tb") {
        Some(numeric * 1e12)
    } else if unit.contains("gb") {
        Some(numeric * 1e9)
    } else if unit.contains("mb") {
        Some(numeric * 1e6)
    } else if unit.contains("kb") {
        Some(numeric * 1e3)
    } else if unit.contains('b') {
        Some(numeric)
    } else {
        None
    }
}

fn parse_delay_to_ns(token: &str) -> Option<f64> {
    let (numeric, unit) = parse_number_and_unit(token)?;
    match unit.as_str() {
        "" | "ns" => Some(numeric),
        "s" => Some(numeric * 1e9),
        "ms" => Some(numeric * 1e6),
        "us" => Some(numeric * 1e3),
        "ps" => Some(numeric / 1e3),
        "fs" => Some(numeric / 1e6),
        _ => None,
    }
}

#[no_mangle]
pub extern "C" fn days_net_runtime_create() -> *mut DaysRuntime {
    Box::into_raw(Box::new(DaysRuntime::new()))
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_destroy(runtime: *mut DaysRuntime) {
    if runtime.is_null() {
        return;
    }
    drop(Box::from_raw(runtime));
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_now_ns(runtime: *const DaysRuntime) -> u64 {
    if runtime.is_null() {
        return 0;
    }

    let runtime = &*runtime;
    let core = match runtime.core.lock() {
        Ok(core) => core,
        Err(_) => return 0,
    };

    core.current_time_ns()
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_load_topology(
    runtime: *mut DaysRuntime,
    topology_path: *const c_char,
) -> i32 {
    if runtime.is_null() || topology_path.is_null() {
        return -1;
    }

    let path = match CStr::from_ptr(topology_path).to_str() {
        Ok(path) => path,
        Err(_) => return -2,
    };

    let topology = match RuntimeTopology::load(path) {
        Ok(topo) => topo,
        Err(_) => return -3,
    };

    let runtime = &*runtime;
    let mut core = match runtime.core.lock() {
        Ok(core) => core,
        Err(_) => return -3,
    };

    match core.set_topology(topology) {
        Ok(()) => 0,
        Err(_) => -3,
    }
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_schedule_event(
    runtime: *mut DaysRuntime,
    delay_ns: u64,
    callback: Option<DaysCallback>,
    callback_arg: *mut c_void,
) -> i32 {
    if runtime.is_null() {
        return -1;
    }

    let callback = match callback {
        Some(callback) => callback,
        None => return -2,
    };

    let runtime = &*runtime;
    let mut core = match runtime.core.lock() {
        Ok(core) => core,
        Err(_) => return -3,
    };

    let callback = RuntimeCallback {
        callback,
        callback_arg: callback_arg as usize,
    };

    match core.schedule_event(delay_ns, callback) {
        Ok(()) => 0,
        Err(_) => -3,
    }
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_submit_send(
    runtime: *mut DaysRuntime,
    src: i32,
    dst: i32,
    _tag: i32,
    bytes: u64,
    callback: Option<DaysCallback>,
    callback_arg: *mut c_void,
) -> i32 {
    if runtime.is_null() {
        return -1;
    }

    let callback = match callback {
        Some(callback) => callback,
        None => return -2,
    };

    let runtime = &*runtime;
    let mut core = match runtime.core.lock() {
        Ok(core) => core,
        Err(_) => return -3,
    };

    let callback = RuntimeCallback {
        callback,
        callback_arg: callback_arg as usize,
    };

    match core.submit_send(src, dst, bytes, callback) {
        Ok(()) => 0,
        Err(_) => -3,
    }
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_submit_allreduce_collective(
    runtime: *mut DaysRuntime,
    op_id: u64,
    rank: i32,
    group_size: i32,
    group_ranks: *const i32,
    bytes: u64,
    allreduce_channels: i32,
    callback: Option<DaysCallback>,
    callback_arg: *mut c_void,
) -> i32 {
    submit_ring_collective_ffi(
        runtime,
        RingCollectiveKind::AllReduce,
        op_id,
        rank,
        group_size,
        group_ranks,
        bytes,
        allreduce_channels,
        callback,
        callback_arg,
    )
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_submit_allgather_collective(
    runtime: *mut DaysRuntime,
    op_id: u64,
    rank: i32,
    group_size: i32,
    group_ranks: *const i32,
    bytes: u64,
    allgather_channels: i32,
    callback: Option<DaysCallback>,
    callback_arg: *mut c_void,
) -> i32 {
    submit_ring_collective_ffi(
        runtime,
        RingCollectiveKind::AllGather,
        op_id,
        rank,
        group_size,
        group_ranks,
        bytes,
        allgather_channels,
        callback,
        callback_arg,
    )
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_submit_reducescatter_collective(
    runtime: *mut DaysRuntime,
    op_id: u64,
    rank: i32,
    group_size: i32,
    group_ranks: *const i32,
    bytes: u64,
    reducescatter_channels: i32,
    callback: Option<DaysCallback>,
    callback_arg: *mut c_void,
) -> i32 {
    submit_ring_collective_ffi(
        runtime,
        RingCollectiveKind::ReduceScatter,
        op_id,
        rank,
        group_size,
        group_ranks,
        bytes,
        reducescatter_channels,
        callback,
        callback_arg,
    )
}

unsafe fn submit_ring_collective_ffi(
    runtime: *mut DaysRuntime,
    collective_kind: RingCollectiveKind,
    op_id: u64,
    rank: i32,
    group_size: i32,
    group_ranks: *const i32,
    bytes: u64,
    ring_channels_raw: i32,
    callback: Option<DaysCallback>,
    callback_arg: *mut c_void,
) -> i32 {
    if runtime.is_null() {
        return -1;
    }
    if rank < 0 || group_size <= 0 || group_ranks.is_null() {
        return -4;
    }

    let callback = match callback {
        Some(callback) => callback,
        None => return -2,
    };

    let group_size = group_size as usize;
    let group_rank_slice = std::slice::from_raw_parts(group_ranks, group_size);
    let mut ranks = Vec::with_capacity(group_size);
    for &group_rank in group_rank_slice {
        if group_rank < 0 {
            return -4;
        }
        ranks.push(group_rank as usize);
    }

    let callback = RuntimeCallback {
        callback,
        callback_arg: callback_arg as usize,
    };
    let ring_channels = if ring_channels_raw > 0 {
        ring_channels_raw as usize
    } else {
        1
    };

    let runtime_ref = &*runtime;
    let mut core = match runtime_ref.core.lock() {
        Ok(core) => core,
        Err(_) => return -3,
    };

    let submit_result = match collective_kind {
        RingCollectiveKind::AllReduce => core.submit_allreduce_collective(
            runtime,
            op_id,
            rank as usize,
            ranks,
            bytes,
            ring_channels,
            callback,
        ),
        RingCollectiveKind::AllGather => core.submit_allgather_collective(
            runtime,
            op_id,
            rank as usize,
            ranks,
            bytes,
            ring_channels,
            callback,
        ),
        RingCollectiveKind::ReduceScatter => core.submit_reducescatter_collective(
            runtime,
            op_id,
            rank as usize,
            ranks,
            bytes,
            ring_channels,
            callback,
        ),
    };

    match submit_result {
        Ok(()) => 0,
        Err(err) => {
            eprintln!(
                "[ERROR][submit-{}] op_id={} rank={} group_size={} bytes={} ring_channels={} err={}",
                collective_kind.as_str(),
                op_id,
                rank,
                group_size,
                bytes,
                ring_channels,
                err
            );
            -3
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_run(runtime: *mut DaysRuntime) -> i32 {
    if runtime.is_null() {
        return -1;
    }

    let runtime = &*runtime;
    let mut stagnant_steps: u64 = 0;
    let mut observed_activity = false;

    {
        let mut core = match runtime.core.lock() {
            Ok(core) => core,
            Err(_) => return -3,
        };
        core.stop_flag = false;
    }

    loop {
        let maybe_callback = {
            let core = match runtime.core.lock() {
                Ok(core) => core,
                Err(_) => return -3,
            };
            core.ready_callbacks.pop().map(|event| event.callback)
        };

        if let Some(callback) = maybe_callback {
            observed_activity = true;
            let callback_result = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
                (callback.callback)(callback.callback_arg as *mut c_void);
            }));
            if callback_result.is_err() {
                return -2;
            }
            continue;
        }

        let (stop_flag, pending_events, before_time_ns, queue_empty) = {
            let core = match runtime.core.lock() {
                Ok(core) => core,
                Err(_) => return -3,
            };
            (
                core.stop_flag,
                core.pending_events.load(AtomicOrdering::Relaxed),
                core.current_time_ns(),
                core.ready_callbacks.is_empty(),
            )
        };

        if stop_flag {
            break;
        }
        if pending_events == 0 {
            if observed_activity && std::env::var("DAYS_STRICT_IDLE").is_ok() {
                // Optional strict mode for debugging synchronization issues.
                return -6;
            }
            break;
        }

        observed_activity = true;

        let step_result = {
            let mut core = match runtime.core.lock() {
                Ok(core) => core,
                Err(_) => return -3,
            };
            match core.simulation.as_mut() {
                Some(sim) => sim.step(),
                None => return -3,
            }
        };

        if step_result.is_err() {
            return -4;
        }

        let (after_time_ns, queue_still_empty, pending_after_step) = {
            let core = match runtime.core.lock() {
                Ok(core) => core,
                Err(_) => return -3,
            };
            (
                core.current_time_ns(),
                core.ready_callbacks.is_empty(),
                core.pending_events.load(AtomicOrdering::Relaxed),
            )
        };

        if after_time_ns == before_time_ns
            && queue_empty
            && queue_still_empty
            && pending_after_step > 0
            && pending_after_step >= pending_events
        {
            stagnant_steps = stagnant_steps.saturating_add(1);
            if stagnant_steps > 100_000_000 {
                if std::env::var("DAYS_DEBUG").is_ok() {
                    let (now_ns, pending, inflight_len, inflight_desc) = {
                        let core = match runtime.core.lock() {
                            Ok(core) => core,
                            Err(_) => return -3,
                        };
                        let (inflight_len, inflight_desc) = core
                            .inflight_callbacks
                            .lock()
                            .map(|inflight| {
                                let mut desc = Vec::new();
                                for flow_id in inflight.keys().take(16) {
                                    if let Some((src, dst, bytes)) = core.flow_meta.get(flow_id) {
                                        desc.push(format!(
                                            "flow_id={} src={} dst={} bytes={}",
                                            flow_id, src, dst, bytes
                                        ));
                                    } else {
                                        desc.push(format!(
                                            "flow_id={} src=? dst=? bytes=?",
                                            flow_id
                                        ));
                                    }
                                }
                                (inflight.len(), desc.join("; "))
                            })
                            .unwrap_or((0, String::new()));
                        (
                            core.current_time_ns(),
                            core.pending_events.load(AtomicOrdering::Relaxed),
                            inflight_len,
                            inflight_desc,
                        )
                    };
                    eprintln!(
                        "days runtime stagnant: now_ns={} pending_events={} inflight_callbacks={} [{}]",
                        now_ns, pending, inflight_len, inflight_desc
                    );
                }
                return -5;
            }
        } else {
            stagnant_steps = 0;
        }
    }

    0
}

#[no_mangle]
pub unsafe extern "C" fn days_net_runtime_stop(runtime: *mut DaysRuntime) {
    if runtime.is_null() {
        return;
    }

    let runtime = &*runtime;
    if let Ok(mut core) = runtime.core.lock() {
        core.stop_flag = true;
    }
}

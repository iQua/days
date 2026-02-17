//! C ABI exports for embedding days native simulation runtime from external simulators.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::ffi::{CStr, c_char, c_void};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use nexosim::simulation::{Address, Mailbox, SimInit, Simulation};
use nexosim::time::MonotonicTime;

use crate::flows::DistributionInfo;
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
}

impl RuntimeCallbackScheduler {
    fn new(ready_callbacks: Arc<ReadyCallbackQueue>, pending_events: Arc<AtomicUsize>) -> Self {
        Self {
            ready_callbacks,
            pending_events,
        }
    }

    async fn schedule_callback(&mut self, request: ScheduleRequest, cx: &mut Context<Self>) {
        if request.delay_ns == 0 {
            self.ready_callbacks
                .push(sim_time_to_ns(cx.time()), request.callback);
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
            return;
        }

        // schedule_event always runs from current simulation time.
        cx.schedule_event(
            Duration::from_nanos(request.delay_ns),
            Self::fire_callback,
            request.callback,
        )
        .unwrap();
    }

    async fn fire_callback(&mut self, callback: RuntimeCallback, cx: &mut Context<Self>) {
        self.ready_callbacks.push(sim_time_to_ns(cx.time()), callback);
        self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

impl Model for RuntimeCallbackScheduler {}

type InflightCallbacks = Arc<Mutex<HashMap<usize, RuntimeCallback>>>;

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

    async fn packet_received(&mut self, packet: Packet, cx: &mut Context<Self>) {
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
            self.ready_callbacks.push(sim_time_to_ns(cx.time()), callback);
            self.pending_events.fetch_sub(1, AtomicOrdering::Relaxed);
        }
    }
}

impl Model for RuntimeHostSink {}

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
        let header_line = next_non_empty_line(&mut lines)?
            .ok_or_else(|| "topology file is empty".to_string())?;

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
                format!("invalid delay token at link index {edge_idx}: {}", tokens[3])
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

        let (simulation, _) = sim_init
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
        if let Err(e) = sim.process_event(
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

        let sim = self
            .simulation
            .as_mut()
            .ok_or_else(|| "simulation is not initialized".to_string())?;

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

        self.pending_events.fetch_add(1, AtomicOrdering::Relaxed);

        {
            let mut inflight = self
                .inflight_callbacks
                .lock()
                .map_err(|_| "failed to lock inflight callback map".to_string())?;
            inflight.insert(flow_id, callback);
        }

        for hop in path.windows(2) {
            let current = hop[0];
            let next = hop[1];
            if let Err(e) = sim.process_event(
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

        if let Err(e) = sim.process_event(
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

        if let Err(e) = sim.process_event(
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
            if observed_activity {
                // The runtime became idle without an explicit stop request from
                // the embedding simulator. Treat this as a synchronization bug
                // instead of silently returning success.
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
                                        desc.push(format!("flow_id={} src=? dst=? bytes=?", flow_id));
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

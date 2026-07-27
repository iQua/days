use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::Path;

use days_executor::{
    Event, EventKey, EventKind, FlowDescriptor, FlowId, HostState, LinkDescriptor, LinkId,
    NodeDescriptor, NodeKind, PacketDescriptor, PayloadId, RemoteChannel, SchedulerKind,
    SimulationImage, SwitchQueueState, SwitchState, event_phase,
};
use petgraph::visit::EdgeRef;
use rand::SeedableRng;
use rand::prelude::IndexedRandom;
use rand::rngs::SmallRng;
use serde::Deserialize;
use thiserror::Error;

use super::ids::{IdError, LinkKey, NodeKey, StableIds, dense_ids};
use crate::flows::DistributionInfo;
use crate::topos::build::{TopologyError, build_graph};

/// Failure while lowering supported Days source configuration.
#[derive(Debug, Error)]
pub enum CompileError {
    #[error("failed to read scenario configuration `{path}`: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse scenario configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error(transparent)]
    Topology(#[from] TopologyError),
    #[error("{0}")]
    Unsupported(String),
    #[error("invalid scenario: {0}")]
    Invalid(String),
    #[error("scenario contains more than u64::MAX semantic identities")]
    Id,
}

impl From<IdError> for CompileError {
    fn from(_: IdError) -> Self {
        Self::Id
    }
}

#[derive(Debug, Deserialize)]
struct SourceConfig {
    seed: Option<u64>,
    switch: SourceSwitch,
    link: Option<SourceLink>,
    time_quantum_ns: Option<u64>,
    flow: Option<Vec<SourceFlow>>,
    flow_set: Option<Vec<SourceFlowSet>>,
    collective: Option<Vec<toml::Value>>,
    collective_set: Option<Vec<toml::Value>>,
}

#[derive(Debug, Deserialize)]
struct SourceSwitch {
    port_rate: Option<toml::Value>,
    capacity: u64,
    discipline: Option<String>,
    drop: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct SourceLink {
    mode: Option<String>,
    propagation_ns: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SourceFlow {
    flow_id: Option<u64>,
    starts_before: Option<Vec<u64>>,
    starts_after: Option<Vec<u64>>,
    flow_type: String,
    priority: Option<u8>,
    graph: Vec<(u64, u64)>,
    routing: Option<toml::Value>,
    path: Option<Vec<u64>>,
    traffic: SourceTraffic,
}

#[derive(Debug, Deserialize)]
struct SourceFlowSet {
    first_flow_id: Option<u64>,
    starts_before: Option<Vec<u64>>,
    starts_after: Option<Vec<u64>>,
    flow_type: String,
    flow_count: u64,
    priority: Option<u8>,
    routing: Option<toml::Value>,
    traffic: SourceTraffic,
}

#[derive(Debug, Deserialize)]
struct SourceTraffic {
    initial_delay: Option<f64>,
    duration: Option<f64>,
    size: Option<u64>,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
    tcp: Option<toml::Value>,
    dcqcn: Option<toml::Value>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Termination {
    Bytes(u64),
    DurationNs(u64),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TrafficKey {
    initial_delay_ns: u64,
    interval_ns: u64,
    packet_size_bytes: u64,
    termination: Termination,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ExplicitFlowKey {
    source: u64,
    target: u64,
    traffic: TrafficKey,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FlowSetKey {
    flow_count: u64,
    traffic: TrafficKey,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FlowKey {
    Explicit {
        semantic: ExplicitFlowKey,
        duplicate_ordinal: u64,
    },
    SetMember {
        semantic: FlowSetKey,
        duplicate_ordinal: u64,
        member_ordinal: u64,
        source: u64,
        target: u64,
    },
}

#[derive(Clone, Debug)]
struct FlowInput {
    key: FlowKey,
    source: u64,
    target: u64,
    traffic: TrafficKey,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PacketKey {
    flow: FlowKey,
    packet_ordinal: u64,
}

#[derive(Clone, Debug)]
struct PacketInput {
    key: PacketKey,
    source: NodeKey,
    time_ns: u64,
    size_bytes: u64,
}

/// Lowers one Days configuration file into one heterogeneous semantic image.
///
/// This is a separate construction path from Nexosim. It never instantiates legacy actors and
/// therefore never observes or mutates their process-global ID counters.
pub fn compile_config(path: impl AsRef<Path>) -> Result<SimulationImage, CompileError> {
    let path = path.as_ref();
    let content = fs::read_to_string(path).map_err(|source| CompileError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let path_str = path
        .to_str()
        .ok_or_else(|| CompileError::Invalid("configuration path is not valid UTF-8".to_owned()))?;
    crate::validate_config(path_str).map_err(CompileError::Unsupported)?;

    let source: SourceConfig = toml::from_str(&content)?;
    let model = SupportedModel::from_source(source)?;
    let (graph, hosts) = build_graph(path_str)?;

    lower(model, &graph, hosts)
}

struct SupportedModel {
    seed: u64,
    rate_bps: u64,
    queue_capacity_packets: u64,
    propagation_ns: u64,
    explicit_flows: Vec<ExplicitFlowKey>,
    flow_sets: Vec<FlowSetKey>,
}

impl SupportedModel {
    fn from_source(source: SourceConfig) -> Result<Self, CompileError> {
        let seed = source
            .seed
            .ok_or_else(|| CompileError::Invalid("`seed` is missing".to_owned()))?;
        let rate_bps = parse_rate(source.switch.port_rate.as_ref())?;

        let discipline = source
            .switch
            .discipline
            .as_deref()
            .ok_or_else(|| CompileError::Unsupported(
                "unsupported scheduler: `switch.discipline` is missing; Days executor v1 supports only FIFO"
                    .to_owned(),
            ))?;
        if discipline != "FIFO" {
            return Err(CompileError::Unsupported(format!(
                "unsupported scheduler `{discipline}`; Days executor v1 supports only FIFO"
            )));
        }

        let drop = source.switch.drop.as_deref().ok_or_else(|| {
            CompileError::Unsupported(
                "unsupported drop policy: `switch.drop` is missing; Days executor v1 supports only TailDrop"
                    .to_owned(),
            )
        })?;
        if drop != "TailDrop" {
            return Err(CompileError::Unsupported(format!(
                "unsupported drop policy `{drop}`; Days executor v1 supports only TailDrop"
            )));
        }

        let link = source.link.unwrap_or_default();
        if let Some(mode) = link.mode.as_deref() {
            if mode != "None" {
                let suffix = if mode == "Pfc" {
                    "Days executor v1 does not support PFC".to_owned()
                } else {
                    "Days executor v1 supports only direct constant links".to_owned()
                };
                return Err(CompileError::Unsupported(format!(
                    "unsupported link mode `{mode}`; {suffix}"
                )));
            }
        }

        if source.time_quantum_ns.is_some_and(|quantum| quantum != 0) {
            return Err(CompileError::Unsupported(
                "unsupported `time_quantum_ns`; remove it or set it to zero for the exact-time Days executor"
                    .to_owned(),
            ));
        }
        if source
            .collective
            .as_ref()
            .is_some_and(|collectives| !collectives.is_empty())
            || source
                .collective_set
                .as_ref()
                .is_some_and(|collectives| !collectives.is_empty())
        {
            return Err(CompileError::Unsupported(
                "unsupported collective traffic; Days executor v1 lowering supports only independent open-loop flows"
                    .to_owned(),
            ));
        }

        let explicit_flows = source
            .flow
            .unwrap_or_default()
            .into_iter()
            .map(validate_explicit_flow)
            .collect::<Result<Vec<_>, _>>()?;
        let flow_sets = source
            .flow_set
            .unwrap_or_default()
            .into_iter()
            .map(validate_flow_set)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            seed,
            rate_bps,
            queue_capacity_packets: source.switch.capacity,
            propagation_ns: link.propagation_ns.unwrap_or(0),
            explicit_flows,
            flow_sets,
        })
    }
}

fn parse_rate(value: Option<&toml::Value>) -> Result<u64, CompileError> {
    let Some(value) = value else {
        return Err(CompileError::Unsupported(
            "unsupported link rate: `switch.port_rate` is missing; Days executor v1 requires a positive constant rate"
                .to_owned(),
        ));
    };

    let rate = match value {
        toml::Value::Integer(rate) => {
            if *rate == 0 {
                return Err(CompileError::Unsupported(
                    "unsupported link rate: `switch.port_rate` is zero; Days executor v1 requires a positive constant rate"
                        .to_owned(),
                ));
            }
            u64::try_from(*rate).map_err(|_| {
                CompileError::Unsupported(format!(
                    "unsupported link rate `{rate}`; Days executor v1 requires a positive constant rate"
                ))
            })?
        }
        toml::Value::Float(rate) => {
            if *rate == 0.0 {
                return Err(CompileError::Unsupported(
                    "unsupported link rate: `switch.port_rate` is zero; Days executor v1 requires a positive constant rate"
                        .to_owned(),
                ));
            }
            if !rate.is_finite() || *rate < 0.0 || rate.fract() != 0.0 || *rate >= 2_f64.powi(64) {
                return Err(CompileError::Unsupported(format!(
                    "unsupported link rate `{rate}`; Days executor v1 requires a positive integer constant rate"
                )));
            }
            *rate as u64
        }
        other => {
            return Err(CompileError::Unsupported(format!(
                "unsupported link rate `{other}`; Days executor v1 requires a positive integer constant rate"
            )));
        }
    };

    Ok(rate)
}

fn validate_flow_type(flow_type: &str) -> Result<(), CompileError> {
    if flow_type != "PacketDistribution" {
        return Err(CompileError::Unsupported(format!(
            "unsupported flow type `{flow_type}`; Days executor v1 supports only open-loop PacketDistribution traffic"
        )));
    }
    Ok(())
}

fn reject_flow_options(
    id: Option<u64>,
    starts_before: Option<&[u64]>,
    starts_after: Option<&[u64]>,
    priority: Option<u8>,
    routing: Option<&toml::Value>,
    path: Option<&[u64]>,
) -> Result<(), CompileError> {
    if id.is_some() {
        return Err(CompileError::Unsupported(
            "unsupported explicit flow IDs; scenario-local flow identity is derived from semantic flow keys"
                .to_owned(),
        ));
    }
    if starts_before.is_some_and(|dependencies| !dependencies.is_empty())
        || starts_after.is_some_and(|dependencies| !dependencies.is_empty())
    {
        return Err(CompileError::Unsupported(
            "unsupported closed-loop flow dependencies; Days executor v1 requires precomputed open-loop inputs"
                .to_owned(),
        ));
    }
    if priority.is_some_and(|priority| priority != 0) {
        return Err(CompileError::Unsupported(
            "unsupported packet priority; Days executor v1 FIFO lowering supports only priority 0"
                .to_owned(),
        ));
    }
    if routing.is_some() || path.is_some() {
        return Err(CompileError::Unsupported(
            "unsupported source routing selection; T7 does not approximate unencoded forwarding behavior"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_explicit_flow(flow: SourceFlow) -> Result<ExplicitFlowKey, CompileError> {
    validate_flow_type(&flow.flow_type)?;
    reject_flow_options(
        flow.flow_id,
        flow.starts_before.as_deref(),
        flow.starts_after.as_deref(),
        flow.priority,
        flow.routing.as_ref(),
        flow.path.as_deref(),
    )?;
    if flow.graph.len() != 1 {
        return Err(CompileError::Invalid(format!(
            "open-loop flow graph must contain exactly one source/target edge, got {}",
            flow.graph.len()
        )));
    }
    let (source, target) = flow.graph[0];
    if source == target {
        return Err(CompileError::Invalid(format!(
            "open-loop flow source and target must differ, got {source}"
        )));
    }
    Ok(ExplicitFlowKey {
        source,
        target,
        traffic: validate_traffic(flow.traffic)?,
    })
}

fn validate_flow_set(flow_set: SourceFlowSet) -> Result<FlowSetKey, CompileError> {
    validate_flow_type(&flow_set.flow_type)?;
    reject_flow_options(
        flow_set.first_flow_id,
        flow_set.starts_before.as_deref(),
        flow_set.starts_after.as_deref(),
        flow_set.priority,
        flow_set.routing.as_ref(),
        None,
    )?;
    Ok(FlowSetKey {
        flow_count: flow_set.flow_count,
        traffic: validate_traffic(flow_set.traffic)?,
    })
}

fn validate_traffic(traffic: SourceTraffic) -> Result<TrafficKey, CompileError> {
    if traffic.tcp.is_some() {
        return Err(CompileError::Unsupported(
            "unsupported closed-loop TCP source behavior; Days executor v1 requires precomputed open-loop inputs"
                .to_owned(),
        ));
    }
    if traffic.dcqcn.is_some() {
        return Err(CompileError::Unsupported(
            "unsupported DCQCN source behavior; Days executor v1 requires precomputed open-loop inputs"
                .to_owned(),
        ));
    }

    let interval_seconds = constant_distribution(&traffic.arr_dist, "packet arrival distribution")?;
    let interval_ns = seconds_to_ns(interval_seconds, "packet arrival interval")?;
    if interval_ns == 0 {
        return Err(CompileError::Unsupported(
            "unsupported zero packet arrival interval; deterministic precomputation would not advance time"
                .to_owned(),
        ));
    }

    let packet_size_bytes = constant_packet_size_bytes(&traffic.pkt_size_dist)?;

    let termination = match (traffic.size, traffic.duration) {
        (Some(size), _) => Termination::Bytes(size),
        (None, Some(duration)) => {
            Termination::DurationNs(seconds_to_ns(duration, "flow duration")?)
        }
        (None, None) => {
            return Err(CompileError::Invalid(
                "open-loop traffic must specify `size` or `duration`".to_owned(),
            ));
        }
    };

    Ok(TrafficKey {
        initial_delay_ns: seconds_to_ns(
            traffic.initial_delay.unwrap_or_default(),
            "initial flow delay",
        )?,
        interval_ns,
        packet_size_bytes,
        termination,
    })
}

fn constant_packet_size_bytes(distribution: &DistributionInfo) -> Result<u64, CompileError> {
    match distribution {
        DistributionInfo::DiscreteUniform { low, high } if low == high => {
            u64::try_from(*low).map_err(|_| {
                CompileError::Unsupported(format!(
                    "unsupported constant packet size `{low}`; Days executor v1 requires a positive integer byte size"
                ))
            }).and_then(|size| {
                if size == 0 {
                    Err(CompileError::Unsupported(
                        "unsupported constant packet size `0`; Days executor v1 requires a positive integer byte size"
                            .to_owned(),
                    ))
                } else {
                    Ok(size)
                }
            })
        }
        DistributionInfo::Uniform { low, high } if low == high => {
            if !low.is_finite() || *low <= 0.0 || low.fract() != 0.0 {
                return Err(CompileError::Unsupported(format!(
                    "unsupported constant packet size `{low}`; Days executor v1 requires a positive integer byte size"
                )));
            }
            if *low >= 2_f64.powi(64) {
                return Err(CompileError::Invalid(
                    "constant packet size exceeds the u64 byte domain".to_owned(),
                ));
            }
            Ok(*low as u64)
        }
        DistributionInfo::DiscreteUniform { .. } => Err(CompileError::Unsupported(
            "unsupported nonconstant packet size distribution `DiscreteUniform`; Days executor v1 requires deterministic constant precomputed inputs"
                .to_owned(),
        )),
        DistributionInfo::Uniform { .. } => Err(CompileError::Unsupported(
            "unsupported nonconstant packet size distribution `Uniform`; Days executor v1 requires deterministic constant precomputed inputs"
                .to_owned(),
        )),
        DistributionInfo::Exp { .. } => Err(CompileError::Unsupported(
            "unsupported packet size distribution `Exp`; Days executor v1 requires deterministic constant precomputed inputs"
                .to_owned(),
        )),
    }
}

fn constant_distribution(
    distribution: &DistributionInfo,
    label: &str,
) -> Result<f64, CompileError> {
    match distribution {
        DistributionInfo::DiscreteUniform { low, high } if low == high => Ok(*low as f64),
        DistributionInfo::Uniform { low, high } if low == high => Ok(*low),
        DistributionInfo::DiscreteUniform { .. } => Err(CompileError::Unsupported(format!(
            "unsupported nonconstant {label} `DiscreteUniform`; Days executor v1 requires deterministic constant precomputed inputs"
        ))),
        DistributionInfo::Uniform { .. } => Err(CompileError::Unsupported(format!(
            "unsupported nonconstant {label} `Uniform`; Days executor v1 requires deterministic constant precomputed inputs"
        ))),
        DistributionInfo::Exp { .. } => Err(CompileError::Unsupported(format!(
            "unsupported {label} `Exp`; Days executor v1 requires deterministic constant precomputed inputs"
        ))),
    }
}

fn seconds_to_ns(seconds: f64, label: &str) -> Result<u64, CompileError> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(CompileError::Invalid(format!(
            "{label} must be a finite nonnegative duration, got {seconds}"
        )));
    }
    let nanoseconds = seconds * 1_000_000_000.0;
    if !nanoseconds.is_finite() || nanoseconds >= 2_f64.powi(64) {
        return Err(CompileError::Invalid(format!(
            "{label} `{seconds}` exceeds the u64 nanosecond domain"
        )));
    }
    if nanoseconds.fract() != 0.0 {
        return Err(CompileError::Unsupported(format!(
            "unsupported {label} `{seconds}`; Days executor v1 requires an integer number of nanoseconds"
        )));
    }
    Ok(nanoseconds as u64)
}

fn canonical_adjacency(edges: &BTreeSet<(u64, u64)>) -> BTreeMap<u64, BTreeSet<u64>> {
    let mut adjacency = BTreeMap::<u64, BTreeSet<u64>>::new();
    for &(left, right) in edges {
        adjacency.entry(left).or_default().insert(right);
        adjacency.entry(right).or_default().insert(left);
    }
    adjacency
}

fn canonical_route(
    source: u64,
    target: u64,
    adjacency: &BTreeMap<u64, BTreeSet<u64>>,
    ids: &StableIds,
) -> Result<Vec<LinkId>, CompileError> {
    let mut parents = BTreeMap::from([(source, source)]);
    let mut frontier = VecDeque::from([source]);

    while let Some(node) = frontier.pop_front() {
        if node == target {
            break;
        }
        for &neighbor in adjacency.get(&node).into_iter().flatten() {
            if let std::collections::btree_map::Entry::Vacant(entry) = parents.entry(neighbor) {
                entry.insert(node);
                frontier.push_back(neighbor);
            }
        }
    }

    if !parents.contains_key(&target) {
        return Err(CompileError::Unsupported(format!(
            "unsupported unreachable flow {source} -> {target}; no static topology route exists"
        )));
    }

    let mut switch_path = vec![target];
    let mut current = target;
    while current != source {
        current = parents[&current];
        switch_path.push(current);
    }
    switch_path.reverse();

    let mut route = Vec::with_capacity(switch_path.len() + 1);
    route.push(ids.link(LinkKey {
        source: NodeKey::Host(source),
        target: NodeKey::Switch(source),
    }));
    for pair in switch_path.windows(2) {
        route.push(ids.link(LinkKey {
            source: NodeKey::Switch(pair[0]),
            target: NodeKey::Switch(pair[1]),
        }));
    }
    route.push(ids.link(LinkKey {
        source: NodeKey::Switch(target),
        target: NodeKey::Host(target),
    }));
    Ok(route)
}

fn lower(
    model: SupportedModel,
    graph: &petgraph::graph::UnGraph<usize, ()>,
    hosts: Vec<usize>,
) -> Result<SimulationImage, CompileError> {
    let switch_topology_ids = graph
        .node_indices()
        .map(|node| u64::try_from(node.index()))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| CompileError::Invalid("topology node identity exceeds u64".to_owned()))?;
    let host_count = hosts.len();
    let host_topology_ids = hosts
        .into_iter()
        .map(u64::try_from)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| CompileError::Invalid("host topology identity exceeds u64".to_owned()))?;
    if host_topology_ids.len() != host_count {
        return Err(CompileError::Unsupported(
            "unsupported duplicate host topology identity".to_owned(),
        ));
    }

    for host in &host_topology_ids {
        if !switch_topology_ids.contains(host) {
            return Err(CompileError::Invalid(format!(
                "host topology identity {host} does not name a switch attachment"
            )));
        }
    }

    let node_keys = host_topology_ids
        .iter()
        .copied()
        .map(NodeKey::Host)
        .chain(switch_topology_ids.iter().copied().map(NodeKey::Switch))
        .collect::<Vec<_>>();

    let mut link_keys = BTreeSet::new();
    let mut undirected_edges = BTreeSet::new();
    for edge in graph.edge_references() {
        let left = u64::try_from(edge.source().index())
            .map_err(|_| CompileError::Invalid("topology edge source exceeds u64".to_owned()))?;
        let right = u64::try_from(edge.target().index())
            .map_err(|_| CompileError::Invalid("topology edge target exceeds u64".to_owned()))?;
        if left == right {
            return Err(CompileError::Unsupported(format!(
                "unsupported self-loop at switch topology identity {left}"
            )));
        }
        let undirected = (left.min(right), left.max(right));
        if !undirected_edges.insert(undirected) {
            return Err(CompileError::Unsupported(format!(
                "unsupported parallel physical links between switch topology identities {} and {}",
                undirected.0, undirected.1
            )));
        }
        link_keys.insert(LinkKey {
            source: NodeKey::Switch(left),
            target: NodeKey::Switch(right),
        });
        link_keys.insert(LinkKey {
            source: NodeKey::Switch(right),
            target: NodeKey::Switch(left),
        });
    }
    for host in &host_topology_ids {
        link_keys.insert(LinkKey {
            source: NodeKey::Host(*host),
            target: NodeKey::Switch(*host),
        });
        link_keys.insert(LinkKey {
            source: NodeKey::Switch(*host),
            target: NodeKey::Host(*host),
        });
    }

    let ids = StableIds::new(node_keys, link_keys.iter().copied())?;
    let flows = canonical_flows(
        model.explicit_flows,
        model.flow_sets,
        &host_topology_ids,
        model.seed,
    )?;
    let flow_ids = dense_ids(flows.iter().map(|flow| flow.key.clone()))?;
    let adjacency = canonical_adjacency(&undirected_edges);
    let flow_descriptors = flows
        .iter()
        .map(|flow| {
            Ok(FlowDescriptor {
                id: FlowId(flow_ids[&flow.key]),
                source: ids.node(NodeKey::Host(flow.source)),
                target: ids.node(NodeKey::Host(flow.target)),
                route: canonical_route(flow.source, flow.target, &adjacency, &ids)?,
            })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    let inputs = precompute_inputs(&flows)?;
    let payload_ids = dense_ids(inputs.iter().map(|input| input.key.clone()))?;

    let host_slots = dense_ids(host_topology_ids.iter().copied().map(NodeKey::Host))?;
    let switch_slots = dense_ids(switch_topology_ids.iter().copied().map(NodeKey::Switch))?;
    let nodes = ids
        .nodes()
        .map(|(key, id)| {
            let (kind, slot) = match key {
                NodeKey::Host(_) => (NodeKind::Host, host_slots[&key]),
                NodeKey::Switch(_) => (NodeKind::Switch, switch_slots[&key]),
            };
            let state_slot = u32::try_from(slot)
                .map_err(|_| CompileError::Invalid(format!("{kind:?} state slot exceeds u32")))?;
            Ok(NodeDescriptor {
                id,
                kind,
                state_slot,
            })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;

    let mut origin_sequences = BTreeMap::<NodeKey, u64>::new();
    let mut event_inputs = inputs.iter().collect::<Vec<_>>();
    event_inputs.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.time_ns.cmp(&right.time_ns))
            .then_with(|| left.key.cmp(&right.key))
    });
    let mut initial_events = Vec::with_capacity(event_inputs.len());
    for input in event_inputs {
        let sequence = origin_sequences.entry(input.source).or_default();
        let origin_seq = *sequence;
        *sequence = sequence.checked_add(1).ok_or_else(|| {
            CompileError::Invalid(format!(
                "initial origin sequence overflow at {:?}",
                input.source
            ))
        })?;
        let origin_node = ids.node(input.source);
        initial_events.push(Event {
            key: EventKey {
                time_ns: input.time_ns,
                phase: event_phase(EventKind::PacketArrival),
                origin_node,
                origin_seq,
            },
            target: origin_node,
            kind: EventKind::PacketArrival,
            payload: PayloadId(payload_ids[&input.key]),
        });
    }
    initial_events.sort_by_key(|event| event.key);

    let host_states = host_topology_ids
        .iter()
        .map(|host| {
            let node_key = NodeKey::Host(*host);
            let egress_key = LinkKey {
                source: node_key,
                target: NodeKey::Switch(*host),
            };
            Ok(HostState {
                egress_link: ids.link(egress_key),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                next_origin_seq: origin_sequences.get(&node_key).copied().unwrap_or(0),
                sourced_packets: 0,
                departed_packets: 0,
            })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    let switch_states = switch_topology_ids
        .iter()
        .map(|switch| SwitchState {
            queues: ids
                .links()
                .filter(|(key, _)| key.source == NodeKey::Switch(*switch))
                .map(|(_, link)| SwitchQueueState {
                    egress_link: Some(link),
                    scheduler: SchedulerKind::Fifo,
                    queue_capacity_packets: model.queue_capacity_packets,
                    queue: VecDeque::new(),
                })
                .collect(),
            arrived_packets: 0,
            dropped_packets: 0,
        })
        .collect();
    let packets = inputs
        .iter()
        .map(|input| PacketDescriptor {
            id: PayloadId(payload_ids[&input.key]),
            flow: FlowId(flow_ids[&input.key.flow]),
            size_bytes: input.size_bytes,
        })
        .collect();
    let links = ids
        .links()
        .map(|(key, id)| LinkDescriptor {
            id,
            source: ids.node(key.source),
            target: ids.node(key.target),
            rate_bps: model.rate_bps,
            propagation_ns: model.propagation_ns,
        })
        .collect::<Vec<_>>();
    let channels = ids
        .links()
        .map(|(key, link)| RemoteChannel {
            source: ids.node(key.source),
            target: ids.node(key.target),
            link,
            event_kind: EventKind::RemoteArrival,
            // T8 derives and validates serialization-plus-propagation bounds.
            min_delay_ns: 0,
        })
        .collect();

    Ok(SimulationImage {
        nodes,
        host_states,
        switch_states,
        flows: flow_descriptors,
        packets,
        links,
        channels,
        initial_events,
        seed: model.seed,
    })
}

fn canonical_flows(
    mut explicit: Vec<ExplicitFlowKey>,
    mut flow_sets: Vec<FlowSetKey>,
    hosts: &BTreeSet<u64>,
    seed: u64,
) -> Result<Vec<FlowInput>, CompileError> {
    explicit.sort();
    flow_sets.sort();

    let mut flows = Vec::new();
    flows
        .try_reserve_exact(explicit.len())
        .map_err(|error| CompileError::Invalid(format!("flow table is too large: {error}")))?;
    let mut explicit_duplicates = BTreeMap::<ExplicitFlowKey, u64>::new();
    for semantic in explicit {
        if !hosts.contains(&semantic.source) || !hosts.contains(&semantic.target) {
            return Err(CompileError::Invalid(format!(
                "flow {} -> {} must use configured host attachments",
                semantic.source, semantic.target
            )));
        }
        let duplicate_ordinal = explicit_duplicates.entry(semantic.clone()).or_default();
        let key = FlowKey::Explicit {
            semantic: semantic.clone(),
            duplicate_ordinal: *duplicate_ordinal,
        };
        *duplicate_ordinal = duplicate_ordinal.checked_add(1).ok_or_else(|| {
            CompileError::Invalid("duplicate explicit flow ordinal overflow".to_owned())
        })?;
        flows.push(FlowInput {
            key,
            source: semantic.source,
            target: semantic.target,
            traffic: semantic.traffic,
        });
    }

    if !flow_sets.is_empty() && hosts.len() < 2 {
        return Err(CompileError::Invalid(
            "flow sets require at least two configured host attachments".to_owned(),
        ));
    }
    let host_candidates = hosts.iter().copied().collect::<Vec<_>>();
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut set_duplicates = BTreeMap::<FlowSetKey, u64>::new();
    for semantic in flow_sets {
        let duplicate_ordinal = set_duplicates.entry(semantic.clone()).or_default();
        let set_ordinal = *duplicate_ordinal;
        *duplicate_ordinal = duplicate_ordinal.checked_add(1).ok_or_else(|| {
            CompileError::Invalid("duplicate flow-set ordinal overflow".to_owned())
        })?;

        let member_count = usize::try_from(semantic.flow_count).map_err(|_| {
            CompileError::Invalid("flow-set count exceeds the platform index domain".to_owned())
        })?;
        flows.try_reserve_exact(member_count).map_err(|error| {
            CompileError::Invalid(format!("flow-set expansion is too large: {error}"))
        })?;
        for member_ordinal in 0..semantic.flow_count {
            let pair = host_candidates
                .sample(&mut rng, 2)
                .copied()
                .collect::<Vec<_>>();
            let source = pair[0];
            let target = pair[1];
            flows.push(FlowInput {
                key: FlowKey::SetMember {
                    semantic: semantic.clone(),
                    duplicate_ordinal: set_ordinal,
                    member_ordinal,
                    source,
                    target,
                },
                source,
                target,
                traffic: semantic.traffic.clone(),
            });
        }
    }

    flows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(flows)
}

fn precompute_inputs(flows: &[FlowInput]) -> Result<Vec<PacketInput>, CompileError> {
    let mut inputs = Vec::new();
    let input_count = flows.iter().try_fold(0_usize, |total, flow| {
        let count = packet_count(&flow.traffic);
        let count = usize::try_from(count).map_err(|_| {
            CompileError::Invalid("packet input count exceeds the platform index domain".to_owned())
        })?;
        total
            .checked_add(count)
            .ok_or_else(|| CompileError::Invalid("total packet input count overflow".to_owned()))
    })?;
    inputs.try_reserve_exact(input_count).map_err(|error| {
        CompileError::Invalid(format!("packet input expansion is too large: {error}"))
    })?;

    for flow in flows {
        let mut packet_ordinal = 0_u64;
        let mut time_ns = flow.traffic.initial_delay_ns;
        let mut sent_bytes = 0_u64;
        let end_time_ns = match flow.traffic.termination {
            Termination::DurationNs(duration_ns) => {
                Some(time_ns.checked_add(duration_ns).ok_or_else(|| {
                    CompileError::Invalid("flow duration end time exceeds u64".to_owned())
                })?)
            }
            Termination::Bytes(_) => None,
        };

        loop {
            let finished = match flow.traffic.termination {
                Termination::Bytes(size) => sent_bytes >= size,
                Termination::DurationNs(_) => time_ns >= end_time_ns.expect("duration has an end"),
            };
            if finished {
                break;
            }

            inputs.push(PacketInput {
                key: PacketKey {
                    flow: flow.key.clone(),
                    packet_ordinal,
                },
                source: NodeKey::Host(flow.source),
                time_ns,
                size_bytes: flow.traffic.packet_size_bytes,
            });
            packet_ordinal = packet_ordinal
                .checked_add(1)
                .ok_or_else(|| CompileError::Invalid("packet ordinal exceeds u64".to_owned()))?;
            sent_bytes = sent_bytes
                .checked_add(flow.traffic.packet_size_bytes)
                .ok_or_else(|| CompileError::Invalid("flow byte count exceeds u64".to_owned()))?;

            let has_next = match flow.traffic.termination {
                Termination::Bytes(size) => sent_bytes < size,
                Termination::DurationNs(_) => true,
            };
            if has_next {
                time_ns = time_ns
                    .checked_add(flow.traffic.interval_ns)
                    .ok_or_else(|| {
                        CompileError::Invalid("packet input time exceeds u64".to_owned())
                    })?;
            }
        }
    }
    inputs.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(inputs)
}

fn packet_count(traffic: &TrafficKey) -> u64 {
    let (extent, step) = match traffic.termination {
        Termination::Bytes(bytes) => (bytes, traffic.packet_size_bytes),
        Termination::DurationNs(duration_ns) => (duration_ns, traffic.interval_ns),
    };
    if extent == 0 {
        return 0;
    }
    1 + (extent - 1) / step
}

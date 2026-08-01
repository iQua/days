use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::Path;

use days_executor::{
    Backend, ConstantGenerator, Event, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind,
    FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination,
    HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, PacketDescriptor,
    PacketKind, PayloadId, RemoteChannel, ScheduledEmission, SchedulerKind, SimulationImage,
    SwitchQueueState, SwitchState, TcpCongestionControl, TcpDataHeader, TcpGenerator,
    TcpReceiverState, event_phase, validate,
};
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::Deserialize;
use thiserror::Error;

use super::ids::{IdError, LinkKey, LpKey, PhysicalNodeKey, StableIds, dense_ids};
use crate::flows::DistributionInfo;
use crate::flows::route::{RouteTableError, compute_shortest_path_route_table};
use crate::topos::build::{HostAttachments, TopologyError, build_graph};

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
    duration: Option<f64>,
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
    weights: Option<Vec<u64>>,
    priorities: Option<Vec<u64>>,
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
    tcp: Option<SourceTcp>,
    dcqcn: Option<toml::Value>,
}

#[derive(Debug, Deserialize)]
struct SourceTcp {
    cc_algorithm: String,
    #[serde(default)]
    ecn: bool,
    cubic: Option<SourceCubic>,
}

#[derive(Debug, Deserialize)]
struct SourceCubic {
    beta: Option<f64>,
    c: Option<f64>,
    fast_convergence: Option<bool>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Termination {
    Bytes(u64),
    DurationNs(u64),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum TrafficKind {
    Constant,
    Tcp(TcpAlgorithm),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum TcpAlgorithm {
    Reno,
    Cubic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceFlowKind {
    PacketDistribution,
    Tcp,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TrafficKey {
    initial_delay_ns: u64,
    interval_ns: u64,
    packet_size_bytes: u64,
    termination: Termination,
    kind: TrafficKind,
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

    let image = lower(model, &graph, hosts)?;
    validate(&image, Backend::Scalar).map_err(|error| {
        CompileError::Invalid(format!("lowered image failed validation: {error}"))
    })?;
    Ok(image)
}

struct SupportedModel {
    seed: u64,
    stop_time_ns: u64,
    rate_bps: u64,
    queue_capacity_packets: u64,
    scheduler: SchedulerKind,
    propagation_ns: u64,
    explicit_flows: Vec<ExplicitFlowKey>,
    flow_sets: Vec<FlowSetKey>,
}

impl SupportedModel {
    fn from_source(source: SourceConfig) -> Result<Self, CompileError> {
        let seed = source
            .seed
            .ok_or_else(|| CompileError::Invalid("`seed` is missing".to_owned()))?;
        let stop_time_ns = seconds_to_ns(source.duration.unwrap_or(1500.0), "simulation duration")?;
        let rate_bps = parse_rate(source.switch.port_rate.as_ref())?;

        let discipline = source
            .switch
            .discipline
            .as_deref()
            .ok_or_else(|| CompileError::Unsupported(
                "unsupported scheduler: `switch.discipline` is missing; Days executor supports FIFO, SP, and WFQ"
                    .to_owned(),
            ))?;
        let scheduler = match discipline {
            "FIFO" => SchedulerKind::Fifo,
            "SP" => {
                let priorities = source
                    .switch
                    .priorities
                    .clone()
                    .filter(|priorities| !priorities.is_empty())
                    .unwrap_or_else(|| vec![1]);
                SchedulerKind::static_priority(priorities)
            }
            "WFQ" => {
                let weights = source.switch.weights.clone().ok_or_else(|| {
                    CompileError::Invalid(
                        "`switch.weights` must be provided for WFQ scheduling".to_owned(),
                    )
                })?;
                if weights.is_empty() {
                    return Err(CompileError::Invalid(
                        "`switch.weights` must contain at least one class for WFQ scheduling"
                            .to_owned(),
                    ));
                }
                if let Some(class) = weights.iter().position(|weight| *weight == 0) {
                    return Err(CompileError::Invalid(format!(
                        "`switch.weights[{class}]` must be positive for WFQ scheduling"
                    )));
                }
                SchedulerKind::weighted_fair_queue(weights)
            }
            unsupported => {
                return Err(CompileError::Unsupported(format!(
                    "unsupported scheduler `{unsupported}`; Days executor supports FIFO, SP, and WFQ"
                )));
            }
        };

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
            stop_time_ns,
            rate_bps,
            queue_capacity_packets: source.switch.capacity,
            scheduler,
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

fn validate_flow_type(flow_type: &str) -> Result<SourceFlowKind, CompileError> {
    match flow_type {
        "PacketDistribution" => Ok(SourceFlowKind::PacketDistribution),
        "TCP" => Ok(SourceFlowKind::Tcp),
        "DCQCN" => Err(CompileError::Unsupported(
            "unsupported flow type `DCQCN`; Days executor supports PacketDistribution and exact TCP Reno/CUBIC traffic"
                .to_owned(),
        )),
        _ => Err(CompileError::Unsupported(format!(
            "unsupported flow type `{flow_type}`; Days executor supports PacketDistribution and exact TCP Reno/CUBIC traffic"
        ))),
    }
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
            "unsupported inter-flow start dependencies; executor generators must be independently scheduled in the lowered image"
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
            "unsupported source routing or explicit path selection; executor scenario lowering derives deterministic topology routes"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_explicit_flow(flow: SourceFlow) -> Result<ExplicitFlowKey, CompileError> {
    let flow_kind = validate_flow_type(&flow.flow_type)?;
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
            "flow graph must contain exactly one source/target edge, got {}",
            flow.graph.len()
        )));
    }
    let (source, target) = flow.graph[0];
    if source == target {
        return Err(CompileError::Invalid(format!(
            "flow source and target must differ, got {source}"
        )));
    }
    Ok(ExplicitFlowKey {
        source,
        target,
        traffic: validate_traffic(flow.traffic, flow_kind)?,
    })
}

fn validate_flow_set(flow_set: SourceFlowSet) -> Result<FlowSetKey, CompileError> {
    let flow_kind = validate_flow_type(&flow_set.flow_type)?;
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
        traffic: validate_traffic(flow_set.traffic, flow_kind)?,
    })
}

fn validate_traffic(
    traffic: SourceTraffic,
    flow_kind: SourceFlowKind,
) -> Result<TrafficKey, CompileError> {
    if traffic.dcqcn.is_some() {
        return Err(CompileError::Unsupported(
            "unsupported DCQCN source behavior; the executor transport lattice implements exact loss-only TCP Reno/CUBIC"
                .to_owned(),
        ));
    }

    let packet_size_bytes = constant_packet_size_bytes(&traffic.pkt_size_dist)?;
    let (kind, interval_ns, termination) = match flow_kind {
        SourceFlowKind::PacketDistribution => {
            if traffic.tcp.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported TCP options on PacketDistribution traffic; use `flow_type = \"TCP\"`"
                        .to_owned(),
                ));
            }
            let interval_seconds =
                constant_distribution(&traffic.arr_dist, "packet arrival distribution")?;
            let interval_ns = seconds_to_ns(interval_seconds, "packet arrival interval")?;
            if interval_ns == 0 {
                return Err(CompileError::Unsupported(
                    "unsupported zero packet arrival interval; deterministic precomputation would not advance time"
                        .to_owned(),
                ));
            }
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
            (TrafficKind::Constant, interval_ns, termination)
        }
        SourceFlowKind::Tcp => {
            let tcp = traffic.tcp.ok_or_else(|| {
                CompileError::Invalid(
                    "TCP traffic must provide `[flow.traffic.tcp]` or `[flow_set.traffic.tcp]`"
                        .to_owned(),
                )
            })?;
            if tcp.ecn {
                return Err(CompileError::Unsupported(
                    "unsupported TCP ECN; the T23 executor TCP lattice models loss feedback only"
                        .to_owned(),
                ));
            }
            let algorithm = match tcp.cc_algorithm.as_str() {
                "TCPReno" | "RENO" | "Reno" | "reno" => {
                    if tcp.cubic.is_some() {
                        return Err(CompileError::Unsupported(
                            "unsupported CUBIC parameters on a Reno flow".to_owned(),
                        ));
                    }
                    TcpAlgorithm::Reno
                }
                "TCPCubic" | "CUBIC" | "Cubic" | "cubic" => {
                    validate_cubic_profile(tcp.cubic.as_ref())?;
                    TcpAlgorithm::Cubic
                }
                unsupported => {
                    return Err(CompileError::Unsupported(format!(
                        "unsupported TCP congestion control `{unsupported}`; Days executor supports exact Reno and CUBIC"
                    )));
                }
            };
            let size = traffic.size.ok_or_else(|| {
                CompileError::Unsupported(
                    "unsupported duration-terminated TCP traffic; the executor requires an exact byte `size`"
                        .to_owned(),
                )
            })?;
            if size == 0 {
                return Err(CompileError::Invalid(
                    "TCP traffic `size` must be positive".to_owned(),
                ));
            }
            // Closed-loop TCP owns all post-start send timing. The legacy `arr_dist` field is
            // accepted for source-file compatibility but has no executor semantic effect.
            (TrafficKind::Tcp(algorithm), 0, Termination::Bytes(size))
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
        kind,
    })
}

fn validate_cubic_profile(cubic: Option<&SourceCubic>) -> Result<(), CompileError> {
    let supported = cubic.is_none_or(|cubic| {
        cubic.beta.is_none_or(|beta| beta == 0.7)
            && cubic.c.is_none_or(|c| c == 0.4)
            && cubic
                .fast_convergence
                .is_none_or(|fast_convergence| fast_convergence)
    });
    if supported {
        Ok(())
    } else {
        Err(CompileError::Unsupported(
            "unsupported TCP CUBIC parameters; the exact T23 lattice requires beta=0.7, c=0.4, fast_convergence=true"
                .to_owned(),
        ))
    }
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

fn image_route(
    source: u64,
    target: u64,
    switch_path: &[NodeIndex],
    ids: &StableIds,
) -> Vec<LinkId> {
    let mut route = Vec::with_capacity(switch_path.len() + 1);
    let source_switch = u64::try_from(
        switch_path
            .first()
            .expect("a topology route must include the source attachment switch")
            .index(),
    )
    .expect("topology switch identities were checked before route construction");
    let target_switch = u64::try_from(
        switch_path
            .last()
            .expect("a topology route must include the target attachment switch")
            .index(),
    )
    .expect("topology switch identities were checked before route construction");
    route.push(ids.link(LinkKey {
        source: PhysicalNodeKey::Host(source),
        target: PhysicalNodeKey::Switch(source_switch),
    }));
    for pair in switch_path.windows(2) {
        let source = u64::try_from(pair[0].index())
            .expect("topology node identities were checked before route selection");
        let target = u64::try_from(pair[1].index())
            .expect("topology node identities were checked before route selection");
        route.push(ids.link(LinkKey {
            source: PhysicalNodeKey::Switch(source),
            target: PhysicalNodeKey::Switch(target),
        }));
    }
    route.push(ids.link(LinkKey {
        source: PhysicalNodeKey::Switch(target_switch),
        target: PhysicalNodeKey::Host(target),
    }));
    route
}

fn lower(
    model: SupportedModel,
    graph: &petgraph::graph::UnGraph<usize, ()>,
    hosts: HostAttachments,
) -> Result<SimulationImage, CompileError> {
    let switch_topology_ids = graph
        .node_indices()
        .map(|node| u64::try_from(node.index()))
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| CompileError::Invalid("topology node identity exceeds u64".to_owned()))?;
    let host_count = hosts.len();
    let host_topology_ids = hosts
        .host_ids()
        .iter()
        .copied()
        .map(u64::try_from)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(|_| CompileError::Invalid("host topology identity exceeds u64".to_owned()))?;
    if host_topology_ids.len() != host_count {
        return Err(CompileError::Unsupported(
            "unsupported duplicate host topology identity".to_owned(),
        ));
    }

    let host_attachment_switches = hosts
        .iter()
        .map(|host| {
            Ok((
                u64::try_from(host.host_id).map_err(|_| {
                    CompileError::Invalid("host topology identity exceeds u64".to_owned())
                })?,
                u64::try_from(host.switch_id).map_err(|_| {
                    CompileError::Invalid("host attachment switch identity exceeds u64".to_owned())
                })?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, CompileError>>()?;
    for (&host, &switch) in &host_attachment_switches {
        if !switch_topology_ids.contains(&switch) {
            return Err(CompileError::Invalid(format!(
                "host topology identity {host} attaches to missing switch {switch}"
            )));
        }
    }

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
            source: PhysicalNodeKey::Switch(left),
            target: PhysicalNodeKey::Switch(right),
        });
        link_keys.insert(LinkKey {
            source: PhysicalNodeKey::Switch(right),
            target: PhysicalNodeKey::Switch(left),
        });
    }
    for (&host, &switch) in &host_attachment_switches {
        link_keys.insert(LinkKey {
            source: PhysicalNodeKey::Host(host),
            target: PhysicalNodeKey::Switch(switch),
        });
        link_keys.insert(LinkKey {
            source: PhysicalNodeKey::Switch(switch),
            target: PhysicalNodeKey::Host(host),
        });
    }

    let switch_port_keys = link_keys
        .iter()
        .copied()
        .filter_map(|egress| match egress.source {
            PhysicalNodeKey::Switch(switch) => Some(LpKey::SwitchPort { switch, egress }),
            PhysicalNodeKey::Host(_) => None,
        })
        .collect::<BTreeSet<_>>();
    let node_keys = host_topology_ids
        .iter()
        .copied()
        .map(LpKey::Host)
        .chain(switch_port_keys.iter().copied())
        .collect::<Vec<_>>();
    let ids = StableIds::new(node_keys, link_keys.iter().copied())?;
    let flows = canonical_flows(
        model.explicit_flows,
        model.flow_sets,
        &host_topology_ids,
        &hosts,
        model.seed,
    )?;
    let flow_ids = dense_ids(flows.iter().map(|flow| flow.key.clone()))?;
    let route_table = compute_shortest_path_route_table(
        graph,
        flows.iter().enumerate().map(|(index, flow)| {
            (
                index,
                NodeIndex::new(host_attachment_switches[&flow.source] as usize),
                NodeIndex::new(host_attachment_switches[&flow.target] as usize),
            )
        }),
    )
    .map_err(|error| match error {
        RouteTableError::Unreachable(index) => {
            let flow = &flows[index];
            CompileError::Unsupported(format!(
                "unsupported unreachable flow {} -> {}; no static topology route exists",
                flow.source, flow.target
            ))
        }
        RouteTableError::DuplicateKey(_) => {
            CompileError::Invalid("duplicate internal flow route key".to_string())
        }
    })?;
    let flow_descriptors = flows
        .iter()
        .enumerate()
        .map(|(index, flow)| {
            let switch_path = &route_table[&index];
            let reverse_switch_path = switch_path.iter().rev().copied().collect::<Vec<_>>();
            FlowDescriptor {
                id: FlowId(flow_ids[&flow.key]),
                source: ids.node(LpKey::Host(flow.source)),
                target: ids.node(LpKey::Host(flow.target)),
                route: image_route(flow.source, flow.target, switch_path, &ids),
                reverse_route: image_route(flow.target, flow.source, &reverse_switch_path, &ids),
            }
        })
        .collect::<Vec<_>>();
    validate_input_bounds(&flows)?;

    let host_slots = dense_ids(host_topology_ids.iter().copied().map(LpKey::Host))?;
    let switch_slots = dense_ids(switch_port_keys.iter().copied())?;
    let nodes = ids
        .nodes()
        .map(|(key, id)| {
            let (kind, slot) = match key {
                LpKey::Host(_) => (NodeKind::Host, host_slots[&key]),
                LpKey::SwitchPort { .. } => (NodeKind::Switch, switch_slots[&key]),
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
    let node_count = u64::try_from(nodes.len())
        .map_err(|_| CompileError::Invalid("node count exceeds u64".to_owned()))?;

    let mut generators_by_source = BTreeMap::<LpKey, Vec<FlowGeneratorState>>::new();
    let mut payload_sequences = BTreeMap::<LpKey, u64>::new();
    let mut initial_packets = Vec::with_capacity(flows.len());
    let mut initial_event_inputs = Vec::<(LpKey, u64, FlowId, PayloadId)>::new();
    for (flow, descriptor) in flows.iter().zip(&flow_descriptors) {
        let source = LpKey::Host(flow.source);
        let emission_count = packet_count(&flow.traffic);
        let next_emission = if emission_count == 0 {
            ScheduledEmission {
                status: GeneratorStatus::Finished,
                departure_time_ns: 0,
                payload: PayloadId(0),
            }
        } else {
            let sequence = payload_sequences.entry(source).or_default();
            let payload = allocate_payload_id(
                ids.node(source),
                node_count,
                *sequence,
                "initial payload sequence",
            )?;
            *sequence = sequence.checked_add(1).ok_or_else(|| {
                CompileError::Invalid(format!("initial payload sequence overflow at {source:?}"))
            })?;
            let initial_size_bytes = match (flow.traffic.kind, &flow.traffic.termination) {
                (TrafficKind::Tcp(_), Termination::Bytes(total_bytes)) => {
                    flow.traffic.packet_size_bytes.min(*total_bytes)
                }
                _ => flow.traffic.packet_size_bytes,
            };
            let packet_kind = match flow.traffic.kind {
                TrafficKind::Constant => PacketKind::Data,
                TrafficKind::Tcp(_) => PacketKind::TcpData(TcpDataHeader {
                    sequence: 0,
                    sent_time_ns: flow.traffic.initial_delay_ns,
                    retransmission: false,
                }),
            };
            initial_packets.push(PacketDescriptor {
                id: payload,
                flow: descriptor.id,
                size_bytes: initial_size_bytes,
                kind: packet_kind,
            });
            initial_event_inputs.push((
                source,
                flow.traffic.initial_delay_ns,
                descriptor.id,
                payload,
            ));
            ScheduledEmission {
                status: GeneratorStatus::Scheduled,
                departure_time_ns: flow.traffic.initial_delay_ns,
                payload,
            }
        };
        generators_by_source
            .entry(source)
            .or_default()
            .push(FlowGeneratorState {
                flow: descriptor.id,
                packets_emitted: 0,
                bytes_emitted: 0,
                next_emission,
                rng_state: generator_seed(model.seed, &flow.key),
                feedback: GeneratorFeedbackState {
                    arrivals: 0,
                    outstanding_bytes: 0,
                    unacknowledged_bytes: 0,
                },
                kind: match flow.traffic.kind {
                    TrafficKind::Constant => FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: flow.traffic.initial_delay_ns,
                        interval_ns: flow.traffic.interval_ns,
                        packet_size_bytes: flow.traffic.packet_size_bytes,
                        termination: match flow.traffic.termination {
                            Termination::Bytes(bytes) => GeneratorTermination::Bytes(bytes),
                            Termination::DurationNs(duration_ns) => {
                                GeneratorTermination::DurationNs(duration_ns)
                            }
                        },
                    }),
                    TrafficKind::Tcp(algorithm) => {
                        let Termination::Bytes(total_bytes) = flow.traffic.termination else {
                            unreachable!("TCP validation requires byte termination")
                        };
                        let control = match algorithm {
                            TcpAlgorithm::Reno => {
                                TcpCongestionControl::reno(flow.traffic.packet_size_bytes)
                            }
                            TcpAlgorithm::Cubic => {
                                TcpCongestionControl::cubic(flow.traffic.packet_size_bytes)
                            }
                        };
                        FlowGeneratorKind::Tcp(TcpGenerator::new(
                            total_bytes,
                            flow.traffic.packet_size_bytes,
                            40,
                            control,
                        ))
                    }
                },
            });
    }
    initial_packets.sort_by_key(|packet| packet.id);

    let mut origin_sequences = BTreeMap::<LpKey, u64>::new();
    initial_event_inputs.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut initial_events = Vec::with_capacity(initial_event_inputs.len());
    for (source, time_ns, _, payload) in initial_event_inputs {
        let sequence = origin_sequences.entry(source).or_default();
        let origin_seq = *sequence;
        *sequence = sequence.checked_add(1).ok_or_else(|| {
            CompileError::Invalid(format!("initial origin sequence overflow at {source:?}"))
        })?;
        let origin_node = ids.node(source);
        initial_events.push(Event {
            key: EventKey {
                time_ns,
                phase: event_phase(EventKind::PacketArrival),
                origin_node,
                origin_seq,
            },
            target: origin_node,
            kind: EventKind::PacketArrival,
            payload,
        });
    }
    initial_events.sort_by_key(|event| event.key);

    let mut tcp_receivers_by_target = BTreeMap::<LpKey, Vec<TcpReceiverState>>::new();
    for (flow, descriptor) in flows.iter().zip(&flow_descriptors) {
        if matches!(flow.traffic.kind, TrafficKind::Tcp(_)) {
            tcp_receivers_by_target
                .entry(LpKey::Host(flow.target))
                .or_default()
                .push(TcpReceiverState::new(descriptor.id, 40));
        }
    }

    let host_states = host_topology_ids
        .iter()
        .map(|host| {
            let node_key = LpKey::Host(*host);
            let switch = host_attachment_switches[host];
            let egress_key = LinkKey {
                source: PhysicalNodeKey::Host(*host),
                target: PhysicalNodeKey::Switch(switch),
            };
            Ok(HostState {
                egress_link: ids.link(egress_key),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: generators_by_source.remove(&node_key).unwrap_or_default(),
                tcp_receivers: tcp_receivers_by_target
                    .remove(&node_key)
                    .unwrap_or_default(),
                next_origin_seq: origin_sequences.get(&node_key).copied().unwrap_or(0),
                next_payload_seq: payload_sequences.get(&node_key).copied().unwrap_or(0),
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    let switch_states = switch_port_keys
        .iter()
        .map(|port| {
            let LpKey::SwitchPort { switch, egress } = *port else {
                unreachable!("switch-port key set contains only switch ports")
            };
            SwitchState {
                physical_switch: switch,
                queues: vec![SwitchQueueState {
                    egress_link: Some(ids.link(egress)),
                    scheduler: model.scheduler.clone(),
                    queue_capacity_packets: model.queue_capacity_packets,
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                }],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            }
        })
        .collect();
    let links = ids
        .links()
        .map(|(key, id)| LinkDescriptor {
            id,
            source: ids.node(LpKey::for_link_source(key)),
            target: ids.node(LpKey::for_link_target(key)),
            rate_bps: model.rate_bps,
            propagation_ns: model.propagation_ns,
        })
        .collect::<Vec<_>>();
    let mut channel_keys = BTreeSet::<(LinkId, NodeId)>::new();
    let mut min_packet_size_by_link = BTreeMap::<LinkId, u64>::new();
    let initial_payload_by_flow = initial_packets
        .iter()
        .map(|packet| (packet.flow, packet.id))
        .collect::<BTreeMap<_, _>>();
    for (flow, input) in flow_descriptors.iter().zip(&flows) {
        if packet_count(&input.traffic) == 0 {
            continue;
        }
        let payload = initial_payload_by_flow[&flow.id];
        let data_min_size = if matches!(input.traffic.kind, TrafficKind::Tcp(_)) {
            // Both controllers may fill the exact remaining congestion-window bytes, so even a
            // byte-aligned total/MSS pair can legally produce a one-byte intermediate segment.
            1
        } else {
            input.traffic.packet_size_bytes
        };
        let mut routed_packets = vec![(flow.route.as_slice(), flow.target, data_min_size, "data")];
        if matches!(input.traffic.kind, TrafficKind::Tcp(_)) {
            routed_packets.push((flow.reverse_route.as_slice(), flow.source, 40, "ACK"));
        }
        for (route, terminal, packet_size_bytes, direction) in routed_packets {
            for (index, link_id) in route.iter().enumerate() {
                let link = links.get(link_id.0 as usize).ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "flow {:?} references missing link {link_id:?}",
                        flow.id
                    ))
                })?;
                link.delay_ns(packet_size_bytes).map_err(|error| {
                    CompileError::Invalid(format!(
                        "link {link_id:?} delay overflows for flow {:?} TCP {direction} packet {:?}: {error}",
                        flow.id, payload
                    ))
                })?;
                let target = route
                    .get(index + 1)
                    .map_or(terminal, |next| links[next.0 as usize].source);
                channel_keys.insert((*link_id, target));
                min_packet_size_by_link
                    .entry(*link_id)
                    .and_modify(|size| *size = (*size).min(packet_size_bytes))
                    .or_insert(packet_size_bytes);
            }
        }
    }
    let channels = channel_keys
        .into_iter()
        .map(|(link_id, target)| {
            let link = links[link_id.0 as usize];
            RemoteChannel::for_packet_link_to(link, target, min_packet_size_by_link[&link_id])
                .map_err(|error| {
                    CompileError::Invalid(format!(
                        "failed to derive channel for link {link_id:?} to node {target:?}: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;

    Ok(SimulationImage {
        stop_time_ns: model.stop_time_ns,
        nodes,
        host_states,
        switch_states,
        flows: flow_descriptors,
        initial_packets,
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
    host_attachments: &HostAttachments,
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
        let pairs = host_attachments
            .sample_canonical_flow_pairs(&mut rng, member_count)
            .map_err(CompileError::Invalid)?;
        for (member_ordinal, (source, target)) in (0..semantic.flow_count).zip(pairs) {
            let source = u64::try_from(source).map_err(|_| {
                CompileError::Invalid("source host identity exceeds u64".to_owned())
            })?;
            let target = u64::try_from(target).map_err(|_| {
                CompileError::Invalid("target host identity exceeds u64".to_owned())
            })?;
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

fn validate_input_bounds(flows: &[FlowInput]) -> Result<(), CompileError> {
    let _input_count = flows.iter().try_fold(0_usize, |total, flow| {
        let count = packet_count(&flow.traffic);
        let count = usize::try_from(count).map_err(|_| {
            CompileError::Invalid("packet input count exceeds the platform index domain".to_owned())
        })?;
        total
            .checked_add(count)
            .ok_or_else(|| CompileError::Invalid("total packet input count overflow".to_owned()))
    })?;

    for flow in flows {
        let count = packet_count(&flow.traffic);
        if matches!(flow.traffic.kind, TrafficKind::Tcp(_)) {
            let Termination::Bytes(total_bytes) = flow.traffic.termination else {
                unreachable!("TCP validation requires byte termination")
            };
            flow.traffic
                .packet_size_bytes
                .checked_mul(count)
                .ok_or_else(|| {
                    CompileError::Invalid("TCP segment input count overflow".to_owned())
                })?;
            if total_bytes == 0 {
                return Err(CompileError::Invalid(
                    "TCP traffic `size` must be positive".to_owned(),
                ));
            }
            continue;
        }
        match flow.traffic.termination {
            Termination::DurationNs(duration_ns) => {
                flow.traffic
                    .initial_delay_ns
                    .checked_add(duration_ns)
                    .ok_or_else(|| {
                        CompileError::Invalid("flow duration end time exceeds u64".to_owned())
                    })?;
            }
            Termination::Bytes(_) => {}
        }
        flow.traffic
            .packet_size_bytes
            .checked_mul(count)
            .ok_or_else(|| CompileError::Invalid("flow byte count exceeds u64".to_owned()))?;
        let interval_steps = match flow.traffic.termination {
            Termination::Bytes(_) => count.saturating_sub(1),
            Termination::DurationNs(_) => count,
        };
        let interval_extent = flow
            .traffic
            .interval_ns
            .checked_mul(interval_steps)
            .ok_or_else(|| CompileError::Invalid("packet input time exceeds u64".to_owned()))?;
        flow.traffic
            .initial_delay_ns
            .checked_add(interval_extent)
            .ok_or_else(|| CompileError::Invalid("packet input time exceeds u64".to_owned()))?;
    }
    Ok(())
}

fn packet_count(traffic: &TrafficKey) -> u64 {
    if matches!(traffic.kind, TrafficKind::Tcp(_)) {
        let Termination::Bytes(bytes) = traffic.termination else {
            unreachable!("TCP validation requires byte termination")
        };
        return bytes.div_ceil(traffic.packet_size_bytes);
    }
    let (extent, step) = match traffic.termination {
        Termination::Bytes(bytes) => (bytes, traffic.packet_size_bytes),
        Termination::DurationNs(duration_ns) => (duration_ns, traffic.interval_ns),
    };
    if extent == 0 {
        return 0;
    }
    1 + (extent - 1) / step
}

fn allocate_payload_id(
    source: NodeId,
    node_count: u64,
    local_sequence: u64,
    label: &str,
) -> Result<PayloadId, CompileError> {
    PayloadId::from_node_sequence(source, node_count, local_sequence).ok_or_else(|| {
        CompileError::Invalid(format!(
            "{label} overflow at node {source:?} sequence {local_sequence}"
        ))
    })
}

fn generator_seed(image_seed: u64, key: &FlowKey) -> u64 {
    let mut state = mix_seed(image_seed ^ 0x6a09_e667_f3bc_c909);
    match key {
        FlowKey::Explicit {
            semantic,
            duplicate_ordinal,
        } => {
            state = mix_seed(state ^ 0x4558_504c_4943_4954);
            state = mix_seed(state ^ semantic.source);
            state = mix_seed(state ^ semantic.target);
            state = mix_traffic_seed(state, &semantic.traffic);
            state = mix_seed(state ^ duplicate_ordinal);
        }
        FlowKey::SetMember {
            semantic,
            duplicate_ordinal,
            member_ordinal,
            source,
            target,
        } => {
            state = mix_seed(state ^ 0x5345_545f_4d45_4d42);
            state = mix_seed(state ^ semantic.flow_count);
            state = mix_traffic_seed(state, &semantic.traffic);
            state = mix_seed(state ^ duplicate_ordinal);
            state = mix_seed(state ^ member_ordinal);
            state = mix_seed(state ^ source);
            state = mix_seed(state ^ target);
        }
    }
    state
}

fn mix_traffic_seed(mut state: u64, traffic: &TrafficKey) -> u64 {
    state = mix_seed(state ^ traffic.initial_delay_ns);
    state = mix_seed(state ^ traffic.interval_ns);
    state = mix_seed(state ^ traffic.packet_size_bytes);
    state = match traffic.termination {
        Termination::Bytes(bytes) => {
            state = mix_seed(state ^ 0x4259_5445_5300_0000);
            mix_seed(state ^ bytes)
        }
        Termination::DurationNs(duration_ns) => {
            state = mix_seed(state ^ 0x4455_5241_5449_4f4e);
            mix_seed(state ^ duration_ns)
        }
    };
    match traffic.kind {
        TrafficKind::Constant => state,
        TrafficKind::Tcp(TcpAlgorithm::Reno) => mix_seed(state ^ 0x5450_435f_5245_4e4f),
        TrafficKind::Tcp(TcpAlgorithm::Cubic) => mix_seed(state ^ 0x5450_435f_4355_4249),
    }
}

fn mix_seed(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

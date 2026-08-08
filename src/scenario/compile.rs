use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::Path;

use days_executor::{
    Backend, CollectiveAlgorithm, CollectiveChannelPolicy, CollectiveChunkPolicy,
    CollectiveGenerator, CollectivePhase, ConstantGenerator, DcqcnController,
    DcqcnControllerConfig, DcqcnGenerator, DcqcnReceiverState, DropMarkPolicy, EcnThresholdPolicy,
    Event, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId,
    GeneratorFeedbackState, GeneratorStatus, GeneratorTermination, HostState, LinkDescriptor,
    LinkId, NodeDescriptor, NodeId, NodeKind, PacketDescriptor, PacketKind, PayloadId,
    PfcIngressState, PfcQueueState, QueueDepthUnit, RateGenerator, RedPolicyState, RemoteChannel,
    ScheduledEmission, SchedulerKind, SimulationImage, SwitchQueueState, SwitchState,
    TcpCongestionControl, TcpDataHeader, TcpGenerator, TcpReceiverState, event_phase, validate,
};
use num_bigint::BigUint;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::Deserialize;
use thiserror::Error;

use super::ids::{IdError, LinkKey, LpKey, PhysicalNodeKey, StableIds, dense_ids};
use crate::topos::build::{
    HostAttachments, PairingPolicy, TopologyError, TopologyProfile, build_graph_with_profile,
};
use crate::topos::route::{
    EcmpFlow, RouteTableError, RouteWorkers, compute_fat_tree_ecmp_route_table,
    compute_shortest_path_route_table_with,
};

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
    duration: Option<ExactDecimal>,
    switch: SourceSwitch,
    link: Option<SourceLink>,
    time_quantum_ns: Option<u64>,
    routing: Option<SourceRouting>,
    flow: Option<Vec<SourceFlow>>,
    flow_set: Option<Vec<SourceFlowSet>>,
    collective: Option<Vec<SourceCollective>>,
    collective_set: Option<Vec<SourceCollectiveSet>>,
}

#[derive(Debug, Deserialize)]
struct SourceSwitch {
    port_rate: Option<ExactDecimal>,
    capacity: u64,
    discipline: Option<String>,
    drop: Option<String>,
    ecn_threshold: Option<ExactDecimal>,
    weights: Option<Vec<u64>>,
    priorities: Option<Vec<u64>>,
}

#[derive(Debug, Deserialize)]
struct SourceRouting {
    policy: String,
}

/// Which equal-cost path a flow takes through the fabric.
///
/// `ShortestPath` is the standing single-path policy and stays the default, so every scenario
/// authored before T21 lowers to the bytes it was measured with.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum RoutingPolicy {
    #[default]
    ShortestPath,
    FatTreeEcmp,
}

#[derive(Debug, Default, Deserialize)]
struct SourceLink {
    mode: Option<String>,
    pfc: Option<SourcePfc>,
    propagation_ns: Option<u64>,
    propagation_tiers: Option<SourcePropagationTiers>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct SourcePropagationTiers {
    host_to_edge_ns: u64,
    edge_to_aggregation_ns: u64,
    aggregation_to_core_ns: u64,
}

#[derive(Debug, Default, Deserialize)]
struct SourcePfc {
    xoff: Option<Vec<u64>>,
    xon: Option<Vec<u64>>,
    buffer_capacity: Option<Vec<u64>>,
    pause_quanta: Option<Vec<u16>>,
    refresh_interval: Option<ExactDecimal>,
    drain_interval: Option<ExactDecimal>,
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
    pairing: Option<String>,
    traffic: SourceTraffic,
}

#[derive(Debug, Deserialize)]
struct SourceCollective {
    collective_type: String,
    first_flow_id: Option<u64>,
    flow_type: String,
    flow_count: u64,
    graph: Option<Vec<(u64, u64)>>,
    paths: Option<Vec<Vec<u64>>>,
    sources: Option<Vec<u64>>,
    sinks: Option<Vec<u64>>,
    priority: Option<u8>,
    routing: Option<toml::Value>,
    traffic: SourceTraffic,
}

#[derive(Debug, Deserialize)]
struct SourceCollectiveSet {
    collective_type: String,
    collective_count: u64,
    first_flow_id: Option<u64>,
    flow_type: String,
    flow_count: u64,
    sources: Option<Vec<Vec<u64>>>,
    sinks: Option<Vec<Vec<u64>>>,
    priority: Option<u8>,
    routing: Option<toml::Value>,
    traffic: SourceTraffic,
}

#[derive(Clone, Debug, Deserialize)]
struct SourceTraffic {
    initial_delay: Option<ExactDecimal>,
    duration: Option<ExactDecimal>,
    size: Option<u64>,
    arr_dist: SourceDistributionInfo,
    pkt_size_dist: SourceDistributionInfo,
    tcp: Option<SourceTcp>,
    dcqcn: Option<SourceDcqcn>,
}

#[derive(Clone, Debug)]
struct ExactDecimal {
    span: std::ops::Range<usize>,
}

impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = toml::Spanned::<serde::de::IgnoredAny>::deserialize(deserializer)?;
        Ok(Self { span: value.span() })
    }
}

#[derive(Clone, Debug)]
struct SourceDistributionInfo {
    span: std::ops::Range<usize>,
}

impl<'de> Deserialize<'de> for SourceDistributionInfo {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = toml::Spanned::<serde::de::IgnoredAny>::deserialize(deserializer)?;
        Ok(Self { span: value.span() })
    }
}

#[derive(Clone, Debug, Deserialize)]
struct SourceDcqcn {
    rate_gbps: ExactDecimal,
    min_rate_gbps: ExactDecimal,
    max_rate_gbps: ExactDecimal,
    g: ExactDecimal,
    ai_rate_gbps: ExactDecimal,
    hai_rate_gbps: ExactDecimal,
    mi_factor: ExactDecimal,
    rtt_ns: Option<ExactDecimal>,
    cnp_interval_ns: Option<ExactDecimal>,
    pacing_interval_ns: Option<ExactDecimal>,
    cnp_priority: Option<u8>,
    increase_byte_threshold: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct SourceTcp {
    cc_algorithm: String,
    #[serde(default)]
    ecn: bool,
    cubic: Option<SourceCubic>,
}

#[derive(Clone, Debug, Deserialize)]
struct SourceCubic {
    beta: Option<ExactDecimal>,
    c: Option<ExactDecimal>,
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
    Dcqcn(DcqcnTrafficKey),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DcqcnTrafficKey {
    initial_rate_bps: u64,
    minimum_rate_bps: u64,
    maximum_rate_bps: u64,
    additive_rate_bps: u64,
    hyper_rate_bps: u64,
    g_ppb: u64,
    decrease_ppb: u64,
    cnp_interval_ns: u64,
    control_interval_ns: u64,
    pacing_interval_ns: u64,
    cnp_priority: u8,
    increase_byte_threshold: u64,
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
    Dcqcn,
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
    priority: u8,
    traffic: TrafficKey,
}

/// Semantic identity of one flow set.
///
/// `pairing` is declared last so that ordering, and therefore lowered flow identity, is unchanged
/// for every scenario that does not name a structural policy: two `Random` keys compare on the
/// earlier fields exactly as they did before T21.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FlowSetKey {
    flow_count: u64,
    priority: u8,
    traffic: TrafficKey,
    pairing: PairingPolicy,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CollectiveKey {
    algorithm: CollectiveAlgorithm,
    flow_count: u64,
    sources: Vec<u64>,
    sinks: Vec<u64>,
    priority: u8,
    traffic: TrafficKey,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CollectiveStagePosition {
    phase: CollectivePhase,
    rank: u32,
    step: u32,
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
    CollectiveStage {
        semantic: CollectiveKey,
        duplicate_ordinal: u64,
        stage: CollectiveStagePosition,
    },
}

#[derive(Clone, Debug)]
struct CollectiveStageInput {
    collective_id: u64,
    declared_total_bytes: u64,
    position: CollectiveStagePosition,
    chunk_offset_bytes: u64,
    chunk_bytes: u64,
    local_predecessor: Option<CollectiveStagePosition>,
    inbound_predecessor: Option<CollectiveStagePosition>,
    inbound_predecessor_bytes: u64,
    local_predecessor_complete: bool,
    inbound_predecessor_complete: bool,
}

#[derive(Clone, Debug)]
struct FlowInput {
    key: FlowKey,
    source: u64,
    target: u64,
    priority: u8,
    traffic: TrafficKey,
    collective: Option<CollectiveStageInput>,
}

/// Lowers one Days configuration file into one heterogeneous semantic image.
///
/// This is a separate construction path from Nexosim. It never instantiates legacy actors and
/// therefore never observes or mutates their process-global ID counters.
pub fn compile_config(path: impl AsRef<Path>) -> Result<SimulationImage, CompileError> {
    compile_config_with_route_workers(path, RouteWorkers::available())
}

/// [`compile_config`] with an explicit host-thread budget for per-flow route computation.
///
/// Routes are pure functions of the canonical topology graph and the flow endpoints and scatter
/// into index-addressed slots, so every budget lowers the same image bytes. Equality gates use
/// `RouteWorkers::serial()` as the single-threaded reference.
pub fn compile_config_with_route_workers(
    path: impl AsRef<Path>,
    route_workers: RouteWorkers,
) -> Result<SimulationImage, CompileError> {
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
    let model = SupportedModel::from_source(source, &content)?;
    let (graph, hosts, profile) = build_graph_with_profile(path_str)?;

    let image = lower(model, &graph, hosts, profile, route_workers)?;
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
    drop_mark: DropMarkPolicy,
    pfc: Option<PfcLowering>,
    routing: RoutingPolicy,
    propagation: PropagationModel,
    explicit_flows: Vec<ExplicitFlowKey>,
    flow_sets: Vec<FlowSetKey>,
    collectives: Vec<CollectiveKey>,
}

/// How lowering stamps `LinkDescriptor::propagation_ns`.
///
/// `Uniform` is the standing model. `FatTreeTiers` (T21/P12 F-HET) keys the delay on the fabric
/// layer a link belongs to: host attachment, edge-to-aggregation, aggregation-to-core.
#[derive(Clone, Copy, Debug)]
enum PropagationModel {
    Uniform(u64),
    FatTreeTiers(SourcePropagationTiers),
}

#[derive(Clone)]
struct PfcLowering {
    xoff: [u64; 8],
    xon: [u64; 8],
    buffer_capacity: [u64; 8],
}

impl SupportedModel {
    fn from_source(source: SourceConfig, scenario_text: &str) -> Result<Self, CompileError> {
        let seed = source
            .seed
            .ok_or_else(|| CompileError::Invalid("`seed` is missing".to_owned()))?;
        let stop_time_ns = optional_scaled_decimal(
            scenario_text,
            source.duration.as_ref(),
            "1500",
            1_000_000_000,
            "simulation duration",
        )?;
        let rate_bps = parse_rate(scenario_text, source.switch.port_rate.as_ref())?;

        let discipline = source
            .switch
            .discipline
            .as_deref()
            .ok_or_else(|| CompileError::Unsupported(
                "unsupported scheduler: `switch.discipline` is missing; Days executor supports FIFO, SP, WFQ, DRR, and WRR"
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
            "DRR" => {
                let weights = validated_weights(source.switch.weights.clone(), "DRR")?;
                let minimum = *weights.iter().min().expect("weights are nonempty");
                let quanta = weights
                    .into_iter()
                    .map(|weight| {
                        1_500_u64
                            .checked_mul(weight)
                            .map(|scaled| scaled / minimum)
                            .ok_or_else(|| {
                                CompileError::Invalid(
                                    "DRR quantum normalization exceeds u64".to_owned(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                SchedulerKind::deficit_round_robin(quanta)
            }
            "WRR" => SchedulerKind::weighted_round_robin(validated_weights(
                source.switch.weights.clone(),
                "WRR",
            )?),
            unsupported => {
                return Err(CompileError::Unsupported(format!(
                    "unsupported scheduler `{unsupported}`; Days executor supports FIFO, SP, WFQ, DRR, and WRR"
                )));
            }
        };

        let drop = source.switch.drop.as_deref().ok_or_else(|| {
            CompileError::Unsupported(
                "unsupported drop policy: `switch.drop` is missing; Days executor supports TailDrop, RED, RED_ECN, and ECN_THRESHOLD"
                    .to_owned(),
            )
        })?;
        let drop_mark = match drop {
            "TailDrop" => DropMarkPolicy::TailDrop,
            "RED" | "RED_ECN" => {
                if source.switch.capacity < 10 {
                    return Err(CompileError::Invalid(
                        "RED packet capacity must be at least 10 to represent 70%/90% thresholds"
                            .to_owned(),
                    ));
                }
                let min_threshold = u64::try_from(u128::from(source.switch.capacity) * 7 / 10)
                    .map_err(|_| {
                        CompileError::Invalid(
                            "RED 70% packet threshold exceeds the u64 state domain".to_owned(),
                        )
                    })?;
                let max_threshold = u64::try_from(u128::from(source.switch.capacity) * 9 / 10)
                    .map_err(|_| {
                        CompileError::Invalid(
                            "RED 90% packet threshold exceeds the u64 state domain".to_owned(),
                        )
                    })?;
                DropMarkPolicy::Red(RedPolicyState {
                    unit: QueueDepthUnit::Packets,
                    capacity: source.switch.capacity,
                    min_threshold,
                    max_threshold,
                    max_probability_numerator: 4,
                    max_probability_denominator: 5,
                    average_scaled: 0,
                    counter: 0,
                    mark_ecn: drop == "RED_ECN",
                })
            }
            "ECN_THRESHOLD" => {
                let threshold = exact_decimal_product_ceil(
                    scenario_text,
                    source.switch.ecn_threshold.as_ref(),
                    "0.8",
                    source.switch.capacity,
                    "switch.ecn_threshold",
                )?;
                DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
                    unit: QueueDepthUnit::Packets,
                    capacity: source.switch.capacity,
                    threshold,
                })
            }
            unsupported => {
                return Err(CompileError::Unsupported(format!(
                    "unsupported drop policy `{unsupported}`; Days executor supports TailDrop, RED, RED_ECN, and ECN_THRESHOLD"
                )));
            }
        };

        let link = source.link.unwrap_or_default();
        let mut pfc = None;
        if let Some(mode) = link.mode.as_deref() {
            if mode == "Pfc" {
                let config = link.pfc.as_ref().ok_or_else(|| {
                    CompileError::Invalid("`link.pfc` is required when link mode is Pfc".to_owned())
                })?;
                let refresh_nonzero = config
                    .refresh_interval
                    .as_ref()
                    .map(|interval| {
                        exact_decimal_is_zero(scenario_text, interval, "PFC refresh interval")
                            .map(|zero| !zero)
                    })
                    .transpose()?
                    .unwrap_or(false);
                let drain_nonzero = config
                    .drain_interval
                    .as_ref()
                    .map(|interval| {
                        exact_decimal_is_zero(scenario_text, interval, "PFC drain interval")
                            .map(|zero| !zero)
                    })
                    .transpose()?
                    .unwrap_or(false);
                if refresh_nonzero || drain_nonzero {
                    return Err(CompileError::Unsupported(
                        "PFC refresh/drain timers are outside the T25 executor mechanism; use edge-triggered XOFF/XON"
                            .to_owned(),
                    ));
                }
                let mut xoff = exact_pfc_array(config.xoff.as_deref(), "xoff")?;
                let mut xon = exact_pfc_array(config.xon.as_deref(), "xon")?;
                let mut buffer_capacity =
                    exact_pfc_array(config.buffer_capacity.as_deref(), "buffer_capacity")?;
                if let Some(quanta) = config.pause_quanta.as_deref() {
                    let quanta: [u16; 8] = quanta.try_into().map_err(|_| {
                        CompileError::Invalid(
                            "`link.pfc.pause_quanta` must contain exactly eight entries".to_owned(),
                        )
                    })?;
                    for priority in 0..8 {
                        if quanta[priority] == 0 {
                            xoff[priority] = 0;
                            xon[priority] = 0;
                            buffer_capacity[priority] = 0;
                        }
                    }
                }
                for priority in 0..8 {
                    if xoff[priority] == 0 {
                        xon[priority] = 0;
                        buffer_capacity[priority] = 0;
                    }
                }
                pfc = Some(PfcLowering {
                    xoff,
                    xon,
                    buffer_capacity,
                });
            } else if mode != "None" {
                return Err(CompileError::Unsupported(format!(
                    "unsupported link mode `{mode}`; Days executor supports None and Pfc"
                )));
            }
        }

        if source.time_quantum_ns.is_some_and(|quantum| quantum != 0) {
            return Err(CompileError::Unsupported(
                "unsupported `time_quantum_ns`; remove it or set it to zero for the exact-time Days executor"
                    .to_owned(),
            ));
        }
        let explicit_flows = source
            .flow
            .unwrap_or_default()
            .into_iter()
            .map(|flow| validate_explicit_flow(flow, scenario_text))
            .collect::<Result<Vec<_>, _>>()?;
        let flow_sets = source
            .flow_set
            .unwrap_or_default()
            .into_iter()
            .map(|flow_set| validate_flow_set(flow_set, scenario_text))
            .collect::<Result<Vec<_>, _>>()?;
        let mut collectives = source
            .collective
            .unwrap_or_default()
            .into_iter()
            .map(|collective| validate_collective(collective, scenario_text))
            .collect::<Result<Vec<_>, _>>()?;
        for collective_set in source.collective_set.unwrap_or_default() {
            collectives.extend(validate_collective_set(collective_set, scenario_text)?);
        }

        let routing = match source
            .routing
            .as_ref()
            .map(|routing| routing.policy.as_str())
        {
            None | Some("ShortestPath") => RoutingPolicy::ShortestPath,
            Some("FatTreeEcmp") => RoutingPolicy::FatTreeEcmp,
            Some(unsupported) => {
                return Err(CompileError::Unsupported(format!(
                    "unsupported `routing.policy` `{unsupported}`; Days lowering supports \
                     ShortestPath and FatTreeEcmp"
                )));
            }
        };

        let propagation = match (link.propagation_ns, link.propagation_tiers) {
            (Some(_), Some(_)) => {
                return Err(CompileError::Invalid(
                    "`link.propagation_ns` and `link.propagation_tiers` are mutually exclusive; \
                     a link carries exactly one delay"
                        .to_owned(),
                ));
            }
            (_, Some(tiers)) => PropagationModel::FatTreeTiers(tiers),
            (propagation_ns, None) => PropagationModel::Uniform(propagation_ns.unwrap_or(0)),
        };

        Ok(Self {
            seed,
            stop_time_ns,
            rate_bps,
            queue_capacity_packets: source.switch.capacity,
            scheduler,
            drop_mark,
            pfc,
            routing,
            propagation,
            explicit_flows,
            flow_sets,
            collectives,
        })
    }
}

fn exact_pfc_array(values: Option<&[u64]>, field: &str) -> Result<[u64; 8], CompileError> {
    let values = values.ok_or_else(|| {
        CompileError::Invalid(format!(
            "`link.pfc.{field}` must contain exactly eight entries"
        ))
    })?;
    values.try_into().map_err(|_| {
        CompileError::Invalid(format!(
            "`link.pfc.{field}` must contain exactly eight entries"
        ))
    })
}

fn validated_weights(
    weights: Option<Vec<u64>>,
    discipline: &str,
) -> Result<Vec<u64>, CompileError> {
    let weights = weights.ok_or_else(|| {
        CompileError::Invalid(format!(
            "`switch.weights` must be provided for {discipline} scheduling"
        ))
    })?;
    if weights.is_empty() {
        return Err(CompileError::Invalid(format!(
            "`switch.weights` must contain at least one class for {discipline} scheduling"
        )));
    }
    if let Some(class) = weights.iter().position(|weight| *weight == 0) {
        return Err(CompileError::Invalid(format!(
            "`switch.weights[{class}]` must be positive for {discipline} scheduling"
        )));
    }
    Ok(weights)
}

fn parse_rate(scenario_text: &str, value: Option<&ExactDecimal>) -> Result<u64, CompileError> {
    let Some(value) = value else {
        return Err(CompileError::Unsupported(
            "unsupported link rate: `switch.port_rate` is missing; Days executor v1 requires a positive constant rate"
                .to_owned(),
        ));
    };

    let literal = exact_decimal_literal(scenario_text, value, "link rate")?;
    let rate = scaled_decimal_literal(literal, 1, "link rate")?;
    if rate == 0 {
        return Err(CompileError::Unsupported(
            "unsupported link rate: `switch.port_rate` is zero; Days executor v1 requires a positive constant rate"
                .to_owned(),
        ));
    }
    Ok(rate)
}

fn validate_flow_type(flow_type: &str) -> Result<SourceFlowKind, CompileError> {
    match flow_type {
        "PacketDistribution" => Ok(SourceFlowKind::PacketDistribution),
        "TCP" => Ok(SourceFlowKind::Tcp),
        "DCQCN" => Ok(SourceFlowKind::Dcqcn),
        _ => Err(CompileError::Unsupported(format!(
            "unsupported flow type `{flow_type}`; Days executor supports PacketDistribution, exact TCP Reno/CUBIC, and exact DCQCN traffic"
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
    if priority.is_some_and(|priority| priority > 7) {
        return Err(CompileError::Invalid(
            "packet priority must be in IEEE 802.1Q range 0..=7".to_owned(),
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

fn validate_explicit_flow(
    flow: SourceFlow,
    scenario_text: &str,
) -> Result<ExplicitFlowKey, CompileError> {
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
    let priority = flow.priority.unwrap_or(0);
    let traffic = validate_traffic(flow.traffic, flow_kind, scenario_text)?;
    if let TrafficKind::Dcqcn(dcqcn) = traffic.kind {
        if dcqcn.cnp_priority != priority {
            return Err(CompileError::Unsupported(format!(
                "DCQCN CNP priority {} must equal flow priority {priority}; the v1 packet record has one priority per flow",
                dcqcn.cnp_priority
            )));
        }
    }
    Ok(ExplicitFlowKey {
        source,
        target,
        priority,
        traffic,
    })
}

fn validate_flow_set(
    flow_set: SourceFlowSet,
    scenario_text: &str,
) -> Result<FlowSetKey, CompileError> {
    let flow_kind = validate_flow_type(&flow_set.flow_type)?;
    reject_flow_options(
        flow_set.first_flow_id,
        flow_set.starts_before.as_deref(),
        flow_set.starts_after.as_deref(),
        flow_set.priority,
        flow_set.routing.as_ref(),
        None,
    )?;
    let priority = flow_set.priority.unwrap_or(0);
    let traffic = validate_traffic(flow_set.traffic, flow_kind, scenario_text)?;
    if let TrafficKind::Dcqcn(dcqcn) = traffic.kind {
        if dcqcn.cnp_priority != priority {
            return Err(CompileError::Unsupported(format!(
                "DCQCN CNP priority {} must equal flow priority {priority}; the v1 packet record has one priority per flow",
                dcqcn.cnp_priority
            )));
        }
    }
    Ok(FlowSetKey {
        flow_count: flow_set.flow_count,
        priority,
        traffic,
        pairing: validate_pairing(flow_set.pairing.as_deref())?,
    })
}

fn validate_pairing(pairing: Option<&str>) -> Result<PairingPolicy, CompileError> {
    match pairing {
        None | Some("Random") => Ok(PairingPolicy::Random),
        Some("SwitchOffsetHalf") => Ok(PairingPolicy::SwitchOffsetHalf),
        Some("SameSwitchNext") => Ok(PairingPolicy::SameSwitchNext),
        Some(unsupported) => Err(CompileError::Unsupported(format!(
            "unsupported flow-set pairing `{unsupported}`; Days lowering supports Random, \
             SwitchOffsetHalf, and SameSwitchNext"
        ))),
    }
}

fn collective_algorithm(name: &str) -> Result<CollectiveAlgorithm, CompileError> {
    match name {
        "RingAllReduce" => Ok(CollectiveAlgorithm::RingAllReduce),
        "AllGather" => Ok(CollectiveAlgorithm::AllGather),
        unsupported => Err(CompileError::Unsupported(format!(
            "unsupported collective algorithm `{unsupported}`; T26 supports RingAllReduce and AllGather"
        ))),
    }
}

fn validate_collective_transport(flow_type: &str) -> Result<(), CompileError> {
    if flow_type == "PacketDistribution" {
        Ok(())
    } else {
        Err(CompileError::Unsupported(format!(
            "unsupported collective flow type `{flow_type}`; T26 collectives require deterministic byte-terminated PacketDistribution traffic"
        )))
    }
}

#[allow(clippy::too_many_arguments)]
fn collective_key(
    collective_type: &str,
    flow_type: &str,
    flow_count: u64,
    sources: Vec<u64>,
    sinks: Vec<u64>,
    priority: Option<u8>,
    first_flow_id: Option<u64>,
    routing: Option<&toml::Value>,
    paths: Option<&[Vec<u64>]>,
    graph: Option<&[(u64, u64)]>,
    traffic: SourceTraffic,
    scenario_text: &str,
) -> Result<CollectiveKey, CompileError> {
    validate_collective_transport(flow_type)?;
    let algorithm = collective_algorithm(collective_type)?;
    if first_flow_id.is_some() {
        return Err(CompileError::Unsupported(
            "unsupported explicit collective flow IDs; scenario-local flow identity is derived from the collective stage key"
                .to_owned(),
        ));
    }
    if routing.is_some() || paths.is_some_and(|paths| !paths.is_empty()) {
        return Err(CompileError::Unsupported(
            "unsupported collective source routing or explicit paths; executor lowering derives deterministic topology routes"
                .to_owned(),
        ));
    }
    if graph.is_some_and(|graph| !graph.is_empty()) {
        return Err(CompileError::Unsupported(
            "unsupported collective graph; use ordered sources/sinks for the flat T26 topology hierarchy"
                .to_owned(),
        ));
    }
    if priority.is_some_and(|priority| priority > 7) {
        return Err(CompileError::Invalid(
            "collective priority must be in IEEE 802.1Q range 0..=7".to_owned(),
        ));
    }
    if flow_count == 0 || algorithm == CollectiveAlgorithm::RingAllReduce && flow_count < 2 {
        return Err(CompileError::Invalid(match algorithm {
            CollectiveAlgorithm::RingAllReduce => {
                "RingAllReduce flow_count must be at least 2".to_owned()
            }
            CollectiveAlgorithm::AllGather => "AllGather flow_count must be at least 1".to_owned(),
        }));
    }
    if sources.is_empty() != sinks.is_empty() {
        return Err(CompileError::Invalid(
            "collective sources and sinks must either both be provided or both be omitted"
                .to_owned(),
        ));
    }
    if !sources.is_empty()
        && (sources.len() != flow_count as usize || sinks.len() != flow_count as usize)
    {
        return Err(CompileError::Invalid(format!(
            "collective sources and sinks must each contain flow_count={flow_count} entries"
        )));
    }
    if !sources.is_empty() {
        for rank in 0..sources.len() {
            if sinks[rank] != sources[(rank + 1) % sources.len()] {
                return Err(CompileError::Invalid(format!(
                    "collective ring-next mismatch at rank {rank}: sink {} must equal next source {}",
                    sinks[rank],
                    sources[(rank + 1) % sources.len()]
                )));
            }
        }
    }
    let traffic = validate_traffic(traffic, SourceFlowKind::PacketDistribution, scenario_text)?;
    let Termination::Bytes(total_bytes) = traffic.termination else {
        return Err(CompileError::Unsupported(
            "unsupported duration-terminated collective traffic; T26 collectives require an exact byte size"
                .to_owned(),
        ));
    };
    if algorithm == CollectiveAlgorithm::RingAllReduce && total_bytes < flow_count {
        return Err(CompileError::Invalid(format!(
            "RingAllReduce byte size {total_bytes} must be at least flow_count {flow_count}"
        )));
    }
    Ok(CollectiveKey {
        algorithm,
        flow_count,
        sources,
        sinks,
        priority: priority.unwrap_or(0),
        traffic,
    })
}

fn validate_collective(
    source: SourceCollective,
    scenario_text: &str,
) -> Result<CollectiveKey, CompileError> {
    collective_key(
        &source.collective_type,
        &source.flow_type,
        source.flow_count,
        source.sources.unwrap_or_default(),
        source.sinks.unwrap_or_default(),
        source.priority,
        source.first_flow_id,
        source.routing.as_ref(),
        source.paths.as_deref(),
        source.graph.as_deref(),
        source.traffic,
        scenario_text,
    )
}

fn validate_collective_set(
    source: SourceCollectiveSet,
    scenario_text: &str,
) -> Result<Vec<CollectiveKey>, CompileError> {
    validate_collective_transport(&source.flow_type)?;
    let count = usize::try_from(source.collective_count).map_err(|_| {
        CompileError::Invalid("collective_count exceeds the platform index domain".to_owned())
    })?;
    let sources = source.sources.unwrap_or_else(|| vec![Vec::new(); count]);
    let sinks = source.sinks.unwrap_or_else(|| vec![Vec::new(); count]);
    if sources.len() != count || sinks.len() != count {
        return Err(CompileError::Invalid(format!(
            "collective_set sources and sinks must each contain collective_count={} entries",
            source.collective_count
        )));
    }
    let mut result = Vec::with_capacity(count);
    for (collective_sources, collective_sinks) in sources.into_iter().zip(sinks) {
        result.push(collective_key(
            &source.collective_type,
            &source.flow_type,
            source.flow_count,
            collective_sources,
            collective_sinks,
            source.priority,
            source.first_flow_id,
            source.routing.as_ref(),
            None,
            None,
            source.traffic.clone(),
            scenario_text,
        )?);
    }
    Ok(result)
}

fn validate_traffic(
    traffic: SourceTraffic,
    flow_kind: SourceFlowKind,
    scenario_text: &str,
) -> Result<TrafficKey, CompileError> {
    let packet_size_bytes = constant_packet_size_bytes(&traffic.pkt_size_dist, scenario_text)?;
    let (kind, interval_ns, termination) = match flow_kind {
        SourceFlowKind::PacketDistribution => {
            if traffic.dcqcn.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported DCQCN options on PacketDistribution traffic; use `flow_type = \"DCQCN\"`"
                        .to_owned(),
                ));
            }
            if traffic.tcp.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported TCP options on PacketDistribution traffic; use `flow_type = \"TCP\"`"
                        .to_owned(),
                ));
            }
            let interval_ns = constant_distribution_scaled(
                &traffic.arr_dist,
                scenario_text,
                1_000_000_000,
                "packet arrival distribution",
            )?;
            if interval_ns == 0 {
                return Err(CompileError::Unsupported(
                    "unsupported zero packet arrival interval; deterministic precomputation would not advance time"
                        .to_owned(),
                ));
            }
            let termination = match (traffic.size, traffic.duration) {
                (Some(size), _) => Termination::Bytes(size),
                (None, Some(duration)) => Termination::DurationNs(scaled_decimal(
                    scenario_text,
                    &duration,
                    1_000_000_000,
                    "flow duration",
                )?),
                (None, None) => {
                    return Err(CompileError::Invalid(
                        "open-loop traffic must specify `size` or `duration`".to_owned(),
                    ));
                }
            };
            (TrafficKind::Constant, interval_ns, termination)
        }
        SourceFlowKind::Tcp => {
            if traffic.dcqcn.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported DCQCN options on TCP traffic; use `flow_type = \"DCQCN\"`"
                        .to_owned(),
                ));
            }
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
                    validate_cubic_profile(tcp.cubic.as_ref(), scenario_text)?;
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
        SourceFlowKind::Dcqcn => {
            if traffic.tcp.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported TCP options on DCQCN traffic".to_owned(),
                ));
            }
            let dcqcn = traffic.dcqcn.ok_or_else(|| {
                CompileError::Invalid(
                    "DCQCN traffic must provide `[flow.traffic.dcqcn]` or `[flow_set.traffic.dcqcn]`"
                        .to_owned(),
                )
            })?;
            let size = traffic.size.ok_or_else(|| {
                CompileError::Unsupported(
                    "unsupported duration-terminated DCQCN traffic; the executor requires an exact byte `size`"
                        .to_owned(),
                )
            })?;
            if size == 0 {
                return Err(CompileError::Invalid(
                    "DCQCN traffic `size` must be positive".to_owned(),
                ));
            }
            let key = DcqcnTrafficKey {
                initial_rate_bps: scaled_decimal(
                    scenario_text,
                    &dcqcn.rate_gbps,
                    1_000_000_000,
                    "DCQCN rate",
                )?,
                minimum_rate_bps: scaled_decimal(
                    scenario_text,
                    &dcqcn.min_rate_gbps,
                    1_000_000_000,
                    "DCQCN minimum rate",
                )?,
                maximum_rate_bps: scaled_decimal(
                    scenario_text,
                    &dcqcn.max_rate_gbps,
                    1_000_000_000,
                    "DCQCN maximum rate",
                )?,
                additive_rate_bps: scaled_decimal(
                    scenario_text,
                    &dcqcn.ai_rate_gbps,
                    1_000_000_000,
                    "DCQCN additive rate",
                )?,
                hyper_rate_bps: scaled_decimal(
                    scenario_text,
                    &dcqcn.hai_rate_gbps,
                    1_000_000_000,
                    "DCQCN hyper rate",
                )?,
                g_ppb: scaled_decimal(scenario_text, &dcqcn.g, 1_000_000_000, "DCQCN g ppb")?,
                decrease_ppb: scaled_decimal(
                    scenario_text,
                    &dcqcn.mi_factor,
                    1_000_000_000,
                    "DCQCN decrease ppb",
                )?,
                cnp_interval_ns: optional_scaled_decimal(
                    scenario_text,
                    dcqcn.cnp_interval_ns.as_ref(),
                    "50000",
                    1,
                    "DCQCN CNP interval ns",
                )?,
                control_interval_ns: optional_scaled_decimal(
                    scenario_text,
                    dcqcn.rtt_ns.as_ref(),
                    "100000",
                    1,
                    "DCQCN control interval ns",
                )?,
                pacing_interval_ns: optional_scaled_decimal(
                    scenario_text,
                    dcqcn.pacing_interval_ns.as_ref(),
                    "1000",
                    1,
                    "DCQCN pacing interval ns",
                )?,
                cnp_priority: dcqcn.cnp_priority.unwrap_or(0),
                increase_byte_threshold: dcqcn.increase_byte_threshold.unwrap_or(10_000_000),
            };
            if key.pacing_interval_ns == 0 {
                return Err(CompileError::Invalid(
                    "DCQCN pacing interval must be positive".to_owned(),
                ));
            }
            if key.cnp_priority > 7 {
                return Err(CompileError::Invalid(
                    "DCQCN CNP priority must be in IEEE 802.1Q range 0..=7".to_owned(),
                ));
            }
            DcqcnControllerConfig {
                initial_rate_bps: key.initial_rate_bps,
                minimum_rate_bps: key.minimum_rate_bps,
                maximum_rate_bps: key.maximum_rate_bps,
                additive_rate_bps: key.additive_rate_bps,
                hyper_rate_bps: key.hyper_rate_bps,
                g_ppb: key.g_ppb,
                decrease_ppb: key.decrease_ppb,
                cnp_interval_ns: key.cnp_interval_ns,
                control_interval_ns: key.control_interval_ns,
                increase_byte_threshold: key.increase_byte_threshold,
            }
            .validate()
            .map_err(|error| CompileError::Invalid(error.to_string()))?;
            (
                TrafficKind::Dcqcn(key),
                key.pacing_interval_ns,
                Termination::Bytes(size),
            )
        }
    };

    Ok(TrafficKey {
        initial_delay_ns: optional_scaled_decimal(
            scenario_text,
            traffic.initial_delay.as_ref(),
            "0",
            1_000_000_000,
            "initial flow delay",
        )?,
        interval_ns,
        packet_size_bytes,
        termination,
        kind,
    })
}

fn validate_cubic_profile(
    cubic: Option<&SourceCubic>,
    scenario_text: &str,
) -> Result<(), CompileError> {
    let supported = cubic.is_none_or(|cubic| {
        cubic.beta.as_ref().is_none_or(|beta| {
            scaled_decimal(scenario_text, beta, 10, "TCP CUBIC beta").is_ok_and(|value| value == 7)
        }) && cubic.c.as_ref().is_none_or(|c| {
            scaled_decimal(scenario_text, c, 10, "TCP CUBIC c").is_ok_and(|value| value == 4)
        }) && cubic
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

enum ParsedSourceDistribution<'a> {
    DiscreteUniform { low: i64, high: i64 },
    Exp,
    Uniform { low: &'a str, high: &'a str },
}

fn source_distribution<'a>(
    distribution: &SourceDistributionInfo,
    scenario_text: &'a str,
) -> Result<ParsedSourceDistribution<'a>, CompileError> {
    let literal = scenario_text
        .get(distribution.span.clone())
        .ok_or_else(|| {
            CompileError::Invalid(
                "distribution source span is outside the scenario text".to_owned(),
            )
        })?;
    let body = literal
        .trim()
        .strip_prefix('{')
        .and_then(|body| body.strip_suffix('}'))
        .ok_or_else(|| {
            CompileError::Invalid("executor distributions must use an inline TOML table".to_owned())
        })?;
    let mut fields = BTreeMap::new();
    for field in body.split(',') {
        let (key, value) = field.split_once('=').ok_or_else(|| {
            CompileError::Invalid(format!("invalid distribution field `{field}`"))
        })?;
        fields.insert(key.trim(), value.trim());
    }
    let kind = fields
        .get("type")
        .and_then(|value| {
            value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    value
                        .strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
        })
        .ok_or_else(|| CompileError::Invalid("distribution `type` must be a string".to_owned()))?;
    let value = |field: &str| {
        fields.get(field).copied().ok_or_else(|| {
            CompileError::Invalid(format!("distribution `{kind}` is missing `{field}`"))
        })
    };
    match kind {
        "DiscreteUniform" => Ok(ParsedSourceDistribution::DiscreteUniform {
            low: exact_i64_literal(value("low")?, "DiscreteUniform low")?,
            high: exact_i64_literal(value("high")?, "DiscreteUniform high")?,
        }),
        "Uniform" => {
            let low = value("low")?;
            let high = value("high")?;
            parsed_decimal(low, "Uniform low")?;
            parsed_decimal(high, "Uniform high")?;
            Ok(ParsedSourceDistribution::Uniform { low, high })
        }
        "Exp" => {
            parsed_decimal(value("lambda")?, "Exp lambda")?;
            Ok(ParsedSourceDistribution::Exp)
        }
        unsupported => Err(CompileError::Unsupported(format!(
            "unsupported distribution type `{unsupported}`"
        ))),
    }
}

fn exact_i64_literal(literal: &str, label: &str) -> Result<i64, CompileError> {
    let normalized = literal.trim().replace('_', "");
    let (negative, unsigned) = if let Some(unsigned) = normalized.strip_prefix('-') {
        (true, unsigned)
    } else {
        (false, normalized.strip_prefix('+').unwrap_or(&normalized))
    };
    let (radix, digits) = integer_radix(unsigned).unwrap_or((10, unsigned));
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(CompileError::Invalid(format!(
            "{label} `{literal}` must be an integer"
        )));
    }
    let magnitude = i128::from_str_radix(digits, radix)
        .map_err(|_| CompileError::Invalid(format!("{label} `{literal}` exceeds i64")))?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed)
        .map_err(|_| CompileError::Invalid(format!("{label} `{literal}` exceeds i64")))
}

fn constant_packet_size_bytes(
    distribution: &SourceDistributionInfo,
    scenario_text: &str,
) -> Result<u64, CompileError> {
    match source_distribution(distribution, scenario_text)? {
        ParsedSourceDistribution::DiscreteUniform { low, high } if low == high => {
            u64::try_from(low).map_err(|_| {
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
        ParsedSourceDistribution::Uniform { low, high }
            if parsed_decimal(low, "packet size distribution")?
                == parsed_decimal(high, "packet size distribution")? =>
        {
            let size = scaled_decimal_literal(low, 1, "constant packet size")?;
            if size == 0 {
                return Err(CompileError::Unsupported(
                    "unsupported constant packet size `0`; Days executor v1 requires a positive integer byte size"
                        .to_owned(),
                ));
            }
            Ok(size)
        }
        ParsedSourceDistribution::DiscreteUniform { .. } => Err(CompileError::Unsupported(
            "unsupported nonconstant packet size distribution `DiscreteUniform`; Days executor v1 requires deterministic constant precomputed inputs"
                .to_owned(),
        )),
        ParsedSourceDistribution::Uniform { .. } => Err(CompileError::Unsupported(
            "unsupported nonconstant packet size distribution `Uniform`; Days executor v1 requires deterministic constant precomputed inputs"
                .to_owned(),
        )),
        ParsedSourceDistribution::Exp => Err(CompileError::Unsupported(
            "unsupported packet size distribution `Exp`; Days executor v1 requires deterministic constant precomputed inputs"
                .to_owned(),
        )),
    }
}

fn constant_distribution_scaled(
    distribution: &SourceDistributionInfo,
    scenario_text: &str,
    scale: u64,
    label: &str,
) -> Result<u64, CompileError> {
    match source_distribution(distribution, scenario_text)? {
        ParsedSourceDistribution::DiscreteUniform { low, high } if low == high => {
            let value = u64::try_from(low).map_err(|_| {
                CompileError::Invalid(format!("{label} must be finite and nonnegative, got {low}"))
            })?;
            u128::from(value)
                .checked_mul(u128::from(scale))
                .and_then(|scaled| u64::try_from(scaled).ok())
                .ok_or_else(|| {
                    CompileError::Invalid(format!("{label} `{low}` exceeds the u64 representation"))
                })
        }
        ParsedSourceDistribution::Uniform { low, high }
            if parsed_decimal(low, label)? == parsed_decimal(high, label)? =>
        {
            scaled_decimal_literal(low, scale, label)
        }
        ParsedSourceDistribution::DiscreteUniform { .. } => {
            Err(CompileError::Unsupported(format!(
                "unsupported nonconstant {label} `DiscreteUniform`; Days executor v1 requires deterministic constant precomputed inputs"
            )))
        }
        ParsedSourceDistribution::Uniform { .. } => Err(CompileError::Unsupported(format!(
            "unsupported nonconstant {label} `Uniform`; Days executor v1 requires deterministic constant precomputed inputs"
        ))),
        ParsedSourceDistribution::Exp => Err(CompileError::Unsupported(format!(
            "unsupported {label} `Exp`; Days executor v1 requires deterministic constant precomputed inputs"
        ))),
    }
}

fn optional_scaled_decimal(
    scenario_text: &str,
    value: Option<&ExactDecimal>,
    default: &str,
    scale: u64,
    label: &str,
) -> Result<u64, CompileError> {
    match value {
        Some(value) => scaled_decimal(scenario_text, value, scale, label),
        None => scaled_decimal_literal(default, scale, label),
    }
}

fn scaled_decimal(
    scenario_text: &str,
    value: &ExactDecimal,
    scale: u64,
    label: &str,
) -> Result<u64, CompileError> {
    let literal = exact_decimal_literal(scenario_text, value, label)?;
    scaled_decimal_literal(literal, scale, label)
}

fn exact_decimal_literal<'a>(
    scenario_text: &'a str,
    value: &ExactDecimal,
    label: &str,
) -> Result<&'a str, CompileError> {
    scenario_text.get(value.span.clone()).ok_or_else(|| {
        CompileError::Invalid(format!("{label} source span is outside the scenario text"))
    })
}

#[derive(Debug, Eq, PartialEq)]
struct ParsedDecimal {
    negative: bool,
    digits: String,
    power: i64,
}

fn parsed_decimal(literal: &str, label: &str) -> Result<ParsedDecimal, CompileError> {
    let literal = literal.trim();
    let normalized = literal.replace('_', "");
    let (negative, unsigned) = if let Some(unsigned) = normalized.strip_prefix('-') {
        (true, unsigned)
    } else {
        (false, normalized.strip_prefix('+').unwrap_or(&normalized))
    };
    if matches!(unsigned, "inf" | "nan") {
        return Err(CompileError::Invalid(format!(
            "{label} must be finite and nonnegative, got {literal}"
        )));
    }

    if let Some((radix, digits)) = integer_radix(unsigned) {
        let value = u128::from_str_radix(digits, radix).map_err(|_| {
            CompileError::Invalid(format!(
                "{label} `{literal}` exceeds the exact integer representation"
            ))
        })?;
        return Ok(ParsedDecimal {
            negative: negative && value != 0,
            digits: value.to_string(),
            power: 0,
        });
    }

    let (mantissa, exponent) = split_decimal_exponent(unsigned, label, literal)?;
    let mut parts = mantissa.split('.');
    let whole = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(CompileError::Invalid(format!(
            "{label} `{literal}` is not a decimal number"
        )));
    }
    let mut digits = format!("{whole}{fraction}");
    let significant_start = digits
        .bytes()
        .position(|byte| byte != b'0')
        .unwrap_or(digits.len());
    digits.drain(..significant_start);
    if digits.is_empty() {
        return Ok(ParsedDecimal {
            negative: false,
            digits: "0".to_owned(),
            power: 0,
        });
    }
    let exponent = exponent.map_or(Ok(0), |value| {
        value.parse::<i64>().map_err(|_| {
            let positive = !value.starts_with('-');
            decimal_range_error(label, literal, positive)
        })
    })?;
    let fraction_len = i64::try_from(fraction.len())
        .map_err(|_| decimal_range_error(label, literal, exponent.is_positive()))?;
    let mut power = exponent
        .checked_sub(fraction_len)
        .ok_or_else(|| decimal_range_error(label, literal, exponent.is_positive()))?;
    let trailing_zeros = digits
        .bytes()
        .rev()
        .take_while(|byte| *byte == b'0')
        .count();
    if trailing_zeros != 0 {
        digits.truncate(digits.len() - trailing_zeros);
        power = power
            .checked_add(i64::try_from(trailing_zeros).unwrap_or(i64::MAX))
            .ok_or_else(|| decimal_range_error(label, literal, true))?;
    }
    Ok(ParsedDecimal {
        negative,
        digits,
        power,
    })
}

fn scaled_decimal_literal(literal: &str, scale: u64, label: &str) -> Result<u64, CompileError> {
    let literal = literal.trim();
    let parsed = parsed_decimal(literal, label)?;
    if parsed.negative {
        return Err(CompileError::Invalid(format!(
            "{label} must be finite and nonnegative, got {literal}"
        )));
    }
    if parsed.digits == "0" {
        return Ok(0);
    }
    let scale_power = decimal_scale_power(scale).ok_or_else(|| {
        CompileError::Invalid(format!("{label} uses a non-decimal semantic scale {scale}"))
    })?;
    let decimal_shift = parsed
        .power
        .checked_add(scale_power)
        .ok_or_else(|| decimal_range_error(label, literal, parsed.power.is_positive()))?;
    let scaled_digits = if decimal_shift >= 0 {
        let zero_count = usize::try_from(decimal_shift)
            .map_err(|_| decimal_range_error(label, literal, true))?;
        if parsed.digits.len().saturating_add(zero_count) > 20 {
            return Err(decimal_range_error(label, literal, true));
        }
        let mut scaled = String::with_capacity(parsed.digits.len() + zero_count);
        scaled.push_str(&parsed.digits);
        scaled.extend(std::iter::repeat_n('0', zero_count));
        scaled
    } else {
        return Err(CompileError::Unsupported(format!(
            "unsupported {label} `{literal}`; exact representation requires an integer scaled value"
        )));
    };
    scaled_digits
        .parse::<u64>()
        .map_err(|_| decimal_range_error(label, literal, true))
}

fn exact_decimal_is_zero(
    scenario_text: &str,
    value: &ExactDecimal,
    label: &str,
) -> Result<bool, CompileError> {
    let value = parsed_decimal(exact_decimal_literal(scenario_text, value, label)?, label)?;
    Ok(value.digits == "0")
}

fn exact_decimal_product_ceil(
    scenario_text: &str,
    value: Option<&ExactDecimal>,
    default: &str,
    capacity: u64,
    label: &str,
) -> Result<u64, CompileError> {
    let literal = match value {
        Some(value) => exact_decimal_literal(scenario_text, value, label)?,
        None => default,
    };
    let parsed = parsed_decimal(literal, label)?;
    let invalid = || {
        CompileError::Invalid(
            "ECN threshold requires finite 0 < switch.ecn_threshold <= 1 and positive capacity"
                .to_owned(),
        )
    };
    if capacity == 0 || parsed.negative || parsed.digits == "0" {
        return Err(invalid());
    }
    if parsed.power >= 0 {
        if parsed.digits == "1" && parsed.power == 0 {
            return Ok(capacity);
        }
        return Err(invalid());
    }
    let denominator_power = usize::try_from(parsed.power.unsigned_abs()).map_err(|_| invalid())?;
    if parsed.digits.len() > denominator_power {
        return Err(invalid());
    }
    if denominator_power > parsed.digits.len().saturating_add(20) {
        return Ok(1);
    }
    let denominator_power = u32::try_from(denominator_power).map_err(|_| invalid())?;
    let numerator = parsed.digits.parse::<BigUint>().map_err(|_| invalid())?;
    let denominator = BigUint::from(10_u8).pow(denominator_power);
    let product = numerator * BigUint::from(capacity);
    let threshold = (&product + &denominator - BigUint::from(1_u8)) / denominator;
    u64::try_from(threshold).map_err(|_| invalid())
}

fn integer_radix(literal: &str) -> Option<(u32, &str)> {
    literal
        .strip_prefix("0x")
        .map(|digits| (16, digits))
        .or_else(|| literal.strip_prefix("0o").map(|digits| (8, digits)))
        .or_else(|| literal.strip_prefix("0b").map(|digits| (2, digits)))
}

fn decimal_scale_power(mut scale: u64) -> Option<i64> {
    let mut power = 0_i64;
    while scale > 1 && scale.is_multiple_of(10) {
        scale /= 10;
        power += 1;
    }
    (scale == 1).then_some(power)
}

fn split_decimal_exponent<'a>(
    literal: &'a str,
    label: &str,
    original: &str,
) -> Result<(&'a str, Option<&'a str>), CompileError> {
    let mut parts = literal.split(['e', 'E']);
    let mantissa = parts.next().unwrap_or_default();
    let exponent = match parts.next() {
        None => None,
        Some(value)
            if !value.is_empty()
                && !value.strip_prefix(['+', '-']).unwrap_or(value).is_empty()
                && value
                    .strip_prefix(['+', '-'])
                    .unwrap_or(value)
                    .bytes()
                    .all(|byte| byte.is_ascii_digit()) =>
        {
            Some(value)
        }
        Some(_) => {
            return Err(CompileError::Invalid(format!(
                "{label} `{original}` has an invalid decimal exponent"
            )));
        }
    };
    if parts.next().is_some() {
        return Err(CompileError::Invalid(format!(
            "{label} `{original}` has an invalid decimal exponent"
        )));
    }
    Ok((mantissa, exponent))
}

fn decimal_range_error(label: &str, literal: &str, positive: bool) -> CompileError {
    if positive {
        CompileError::Invalid(format!(
            "{label} `{literal}` exceeds the u64 representation"
        ))
    } else {
        CompileError::Unsupported(format!(
            "unsupported {label} `{literal}`; exact representation requires an integer scaled value"
        ))
    }
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

/// Resolves the configured propagation model against the built topology into a per-link delay.
///
/// Tiering is fat-tree-only on purpose: the tier names *are* fat-tree layer names, and inventing a
/// mapping for a topology whose layers are not those layers would be improvisation, not lowering.
fn link_delay_model(
    propagation: PropagationModel,
    profile: TopologyProfile,
) -> Result<LinkDelay, CompileError> {
    let tiers = match propagation {
        PropagationModel::Uniform(propagation_ns) => {
            return Ok(LinkDelay::Uniform(propagation_ns));
        }
        PropagationModel::FatTreeTiers(tiers) => tiers,
    };
    let TopologyProfile::FatTree {
        edge_switches,
        aggregation_switches,
        ..
    } = profile
    else {
        return Err(CompileError::Unsupported(format!(
            "unsupported `link.propagation_tiers` on a {profile:?} topology; per-tier link delay \
             is defined only for the FatTree profile"
        )));
    };
    let core_boundary = edge_switches
        .checked_add(aggregation_switches)
        .ok_or_else(|| CompileError::Invalid("fat-tree layer boundary exceeds u64".to_owned()))?;
    Ok(LinkDelay::FatTreeTiers {
        tiers,
        core_boundary,
    })
}

/// Propagation model resolved against one built topology.
#[derive(Clone, Copy, Debug)]
enum LinkDelay {
    Uniform(u64),
    FatTreeTiers {
        tiers: SourcePropagationTiers,
        /// First aggregation-to-core switch identity: `edge_switches + aggregation_switches`.
        core_boundary: u64,
    },
}

impl LinkDelay {
    fn of(self, key: LinkKey) -> u64 {
        match self {
            Self::Uniform(propagation_ns) => propagation_ns,
            Self::FatTreeTiers {
                tiers,
                core_boundary,
            } => match (key.source, key.target) {
                (PhysicalNodeKey::Host(_), _) | (_, PhysicalNodeKey::Host(_)) => {
                    tiers.host_to_edge_ns
                }
                (PhysicalNodeKey::Switch(left), PhysicalNodeKey::Switch(right)) => {
                    if left.max(right) < core_boundary {
                        tiers.edge_to_aggregation_ns
                    } else {
                        tiers.aggregation_to_core_ns
                    }
                }
            },
        }
    }
}

fn lower(
    model: SupportedModel,
    graph: &petgraph::graph::UnGraph<usize, ()>,
    hosts: HostAttachments,
    profile: TopologyProfile,
    route_workers: RouteWorkers,
) -> Result<SimulationImage, CompileError> {
    let link_delay = link_delay_model(model.propagation, profile)?;
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
        model.collectives,
        &host_topology_ids,
        &hosts,
        model.seed,
    )?;
    let flow_ids = dense_ids(flows.iter().map(|flow| flow.key.clone()))?;
    let route_table = match model.routing {
        RoutingPolicy::ShortestPath => compute_shortest_path_route_table_with(
            graph,
            flows.iter().enumerate().map(|(index, flow)| {
                (
                    index,
                    NodeIndex::new(host_attachment_switches[&flow.source] as usize),
                    NodeIndex::new(host_attachment_switches[&flow.target] as usize),
                )
            }),
            route_workers,
        ),
        RoutingPolicy::FatTreeEcmp => compute_fat_tree_ecmp_route_table(
            graph,
            flows.iter().enumerate().map(|(index, flow)| EcmpFlow {
                key: index,
                source_switch: NodeIndex::new(host_attachment_switches[&flow.source] as usize),
                target_switch: NodeIndex::new(host_attachment_switches[&flow.target] as usize),
                // The hash stands in for a switch's header hash. It is taken from the flow's own
                // semantic key, so it is a pure function of the scenario text.
                flow_hash: generator_seed(model.seed ^ 0x4543_4d50_5f48_4153, &flow.key),
            }),
        ),
    }
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
        RouteTableError::UnsupportedTopology => CompileError::Unsupported(
            "unsupported `routing.policy = \"FatTreeEcmp\"` on a topology that is not a canonical \
             fat tree; equal-cost multipath selection is defined only for the FatTree profile"
                .to_owned(),
        ),
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
                priority: flow.priority,
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
    let mut initial_event_inputs = Vec::<(LpKey, u64, FlowId, PayloadId, EventKind)>::new();
    for (flow, descriptor) in flows.iter().zip(&flow_descriptors) {
        let source = LpKey::Host(flow.source);
        let emission_count = packet_count(&flow.traffic);
        let mut dcqcn_control_payload = None;
        let collective_ready = flow.collective.as_ref().is_none_or(|stage| {
            stage.local_predecessor_complete && stage.inbound_predecessor_complete
        });
        let next_emission = if emission_count == 0 {
            ScheduledEmission {
                status: GeneratorStatus::Finished,
                departure_time_ns: 0,
                payload: PayloadId(0),
            }
        } else if !collective_ready {
            ScheduledEmission {
                status: GeneratorStatus::Blocked,
                departure_time_ns: 0,
                payload: PayloadId(0),
            }
        } else if flow.collective.is_some() && flow.traffic.initial_delay_ns > model.stop_time_ns {
            ScheduledEmission {
                status: GeneratorStatus::Stopped,
                departure_time_ns: flow.traffic.initial_delay_ns,
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
                (TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_), Termination::Bytes(total_bytes)) => {
                    flow.traffic.packet_size_bytes.min(*total_bytes)
                }
                (TrafficKind::Constant, Termination::Bytes(total_bytes))
                    if flow.collective.is_some() =>
                {
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
                TrafficKind::Dcqcn(_) => PacketKind::Data,
            };
            initial_packets.push(PacketDescriptor {
                id: payload,
                flow: descriptor.id,
                size_bytes: initial_size_bytes,
                ecn_marked: false,
                kind: packet_kind,
            });
            initial_event_inputs.push((
                source,
                flow.traffic.initial_delay_ns,
                descriptor.id,
                payload,
                if matches!(flow.traffic.kind, TrafficKind::Dcqcn(_)) {
                    EventKind::PacingTimer
                } else {
                    EventKind::PacketArrival
                },
            ));
            if let TrafficKind::Dcqcn(config) = flow.traffic.kind {
                let control_time_ns = flow
                    .traffic
                    .initial_delay_ns
                    .checked_add(config.control_interval_ns)
                    .ok_or_else(|| {
                        CompileError::Invalid(format!(
                            "flow {:?} first DCQCN control deadline exceeds u64",
                            descriptor.id
                        ))
                    })?;
                let control_payload = allocate_payload_id(
                    ids.node(source),
                    node_count,
                    *sequence,
                    "DCQCN control payload sequence",
                )?;
                *sequence = sequence.checked_add(1).ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "DCQCN control payload sequence overflow at {source:?}"
                    ))
                })?;
                initial_packets.push(PacketDescriptor {
                    id: control_payload,
                    flow: descriptor.id,
                    size_bytes: 0,
                    ecn_marked: false,
                    kind: PacketKind::DcqcnControlTimer,
                });
                if control_time_ns <= model.stop_time_ns {
                    initial_event_inputs.push((
                        source,
                        control_time_ns,
                        descriptor.id,
                        control_payload,
                        EventKind::PacingTimer,
                    ));
                }
                dcqcn_control_payload = Some(control_payload);
            }
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
                kind: if let Some(stage) = &flow.collective {
                    let FlowKey::CollectiveStage {
                        semantic,
                        duplicate_ordinal,
                        ..
                    } = &flow.key
                    else {
                        unreachable!("collective metadata requires a collective flow key")
                    };
                    let stage_flow = |position: CollectiveStagePosition| {
                        flow_ids[&FlowKey::CollectiveStage {
                            semantic: semantic.clone(),
                            duplicate_ordinal: *duplicate_ordinal,
                            stage: position,
                        }]
                    };
                    FlowGeneratorKind::Collective(CollectiveGenerator {
                        collective_id: stage.collective_id,
                        algorithm: semantic.algorithm,
                        topology_level: 0,
                        topology_group: 0,
                        group_size: u32::try_from(semantic.flow_count)
                            .expect("collective lowering checked group size"),
                        declared_total_bytes: stage.declared_total_bytes,
                        rank: stage.position.rank,
                        phase: stage.position.phase,
                        step: stage.position.step,
                        chunk_policy: CollectiveChunkPolicy::EqualRemainderLast,
                        channel_policy: CollectiveChannelPolicy::RingNext,
                        chunk_offset_bytes: stage.chunk_offset_bytes,
                        chunk_bytes: stage.chunk_bytes,
                        packet_size_bytes: flow.traffic.packet_size_bytes,
                        interval_ns: flow.traffic.interval_ns,
                        local_predecessor: stage.local_predecessor.map(stage_flow).map(FlowId),
                        inbound_predecessor: stage.inbound_predecessor.map(stage_flow).map(FlowId),
                        inbound_predecessor_bytes: stage.inbound_predecessor_bytes,
                        local_predecessor_complete: stage.local_predecessor_complete,
                        inbound_predecessor_complete: stage.inbound_predecessor_complete,
                        inbound_bytes_received: 0,
                    })
                } else {
                    match flow.traffic.kind {
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
                        TrafficKind::Dcqcn(config) => {
                            let Termination::Bytes(total_bytes) = flow.traffic.termination else {
                                unreachable!("DCQCN validation requires byte termination")
                            };
                            let first_control_time_ns = flow
                                .traffic
                                .initial_delay_ns
                                .checked_add(config.control_interval_ns)
                                .expect("DCQCN lowering checked first control deadline");
                            let controller = DcqcnController::new(
                                DcqcnControllerConfig {
                                    initial_rate_bps: config.initial_rate_bps,
                                    minimum_rate_bps: config.minimum_rate_bps,
                                    maximum_rate_bps: config.maximum_rate_bps,
                                    additive_rate_bps: config.additive_rate_bps,
                                    hyper_rate_bps: config.hyper_rate_bps,
                                    g_ppb: config.g_ppb,
                                    decrease_ppb: config.decrease_ppb,
                                    cnp_interval_ns: config.cnp_interval_ns,
                                    control_interval_ns: config.control_interval_ns,
                                    increase_byte_threshold: config.increase_byte_threshold,
                                },
                                first_control_time_ns,
                            )
                            .expect("validated DCQCN controller configuration");
                            FlowGeneratorKind::Dcqcn(DcqcnGenerator {
                                rate: RateGenerator {
                                    first_pacing_time_ns: flow.traffic.initial_delay_ns,
                                    pacing_interval_ns: config.pacing_interval_ns,
                                    packet_size_bytes: flow.traffic.packet_size_bytes,
                                    total_bytes,
                                    rate_numerator_bits_per_second: config.initial_rate_bps,
                                    rate_denominator: 1,
                                    credit_quanta: 0,
                                },
                                controller,
                                control_timer_payload: dcqcn_control_payload
                                    .expect("nonempty DCQCN lowering allocates a control token"),
                                cnp_size_bytes: 64,
                            })
                        }
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
    for (source, time_ns, _, payload, kind) in initial_event_inputs {
        let sequence = origin_sequences.entry(source).or_default();
        let origin_seq = *sequence;
        *sequence = sequence.checked_add(1).ok_or_else(|| {
            CompileError::Invalid(format!("initial origin sequence overflow at {source:?}"))
        })?;
        let origin_node = ids.node(source);
        initial_events.push(Event {
            key: EventKey {
                time_ns,
                phase: event_phase(kind),
                origin_node,
                origin_seq,
            },
            target: origin_node,
            kind,
            payload,
        });
    }
    initial_events.sort_by_key(|event| event.key);

    let mut tcp_receivers_by_target = BTreeMap::<LpKey, Vec<TcpReceiverState>>::new();
    let mut dcqcn_receivers_by_target = BTreeMap::<LpKey, Vec<DcqcnReceiverState>>::new();
    for (flow, descriptor) in flows.iter().zip(&flow_descriptors) {
        if matches!(flow.traffic.kind, TrafficKind::Tcp(_)) {
            tcp_receivers_by_target
                .entry(LpKey::Host(flow.target))
                .or_default()
                .push(TcpReceiverState::new(descriptor.id, 40));
        }
        if let TrafficKind::Dcqcn(config) = flow.traffic.kind {
            dcqcn_receivers_by_target
                .entry(LpKey::Host(flow.target))
                .or_default()
                .push(DcqcnReceiverState {
                    flow: descriptor.id,
                    cnp_interval_ns: config.cnp_interval_ns,
                    cnp_size_bytes: 64,
                    last_cnp_time_ns: None,
                });
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
                dcqcn_receivers: dcqcn_receivers_by_target
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
    let mut switch_states: Vec<SwitchState> = switch_port_keys
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
                    drop_mark: model.drop_mark,
                    pfc: None,
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
            propagation_ns: link_delay.of(key),
        })
        .collect::<Vec<_>>();
    let mut channel_keys = BTreeSet::<(LinkId, NodeId)>::new();
    let mut min_packet_size_by_link = BTreeMap::<LinkId, u64>::new();
    for (flow, input) in flow_descriptors.iter().zip(&flows) {
        if packet_count(&input.traffic) == 0 {
            continue;
        }
        let data_min_size = if matches!(
            input.traffic.kind,
            TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_)
        ) {
            // Both controllers may fill the exact remaining congestion-window bytes, so even a
            // byte-aligned total/MSS pair can legally produce a one-byte intermediate segment.
            1
        } else if input.collective.is_some() {
            let Termination::Bytes(bytes) = input.traffic.termination else {
                unreachable!("collective lowering requires byte termination")
            };
            let remainder = bytes % input.traffic.packet_size_bytes;
            if remainder == 0 {
                input.traffic.packet_size_bytes
            } else {
                remainder
            }
        } else {
            input.traffic.packet_size_bytes
        };
        let mut routed_packets = vec![(flow.route.as_slice(), flow.target, data_min_size, "data")];
        if matches!(input.traffic.kind, TrafficKind::Tcp(_)) {
            routed_packets.push((flow.reverse_route.as_slice(), flow.source, 40, "ACK"));
        }
        if matches!(input.traffic.kind, TrafficKind::Dcqcn(_)) {
            routed_packets.push((flow.reverse_route.as_slice(), flow.source, 64, "CNP"));
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
                        "link {link_id:?} delay overflows for flow {:?} {direction} packet: {error}",
                        flow.id
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
    let mut channels = channel_keys
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

    if let Some(pfc) = &model.pfc {
        let mut monitored_paths = BTreeSet::<(LinkId, NodeId)>::new();
        let mut max_frame_by_link_priority = BTreeMap::<(LinkId, usize), u64>::new();
        for (descriptor, input) in flow_descriptors.iter().zip(&flows) {
            let priority = usize::from(descriptor.priority);
            if pfc.xoff[priority] != 0 {
                for link_id in &descriptor.route {
                    max_frame_by_link_priority
                        .entry((*link_id, priority))
                        .and_modify(|maximum| {
                            *maximum = (*maximum).max(input.traffic.packet_size_bytes)
                        })
                        .or_insert(input.traffic.packet_size_bytes);
                }
                if matches!(
                    input.traffic.kind,
                    TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_)
                ) {
                    for link_id in &descriptor.reverse_route {
                        let feedback_size = if matches!(input.traffic.kind, TrafficKind::Tcp(_)) {
                            40
                        } else {
                            64
                        };
                        max_frame_by_link_priority
                            .entry((*link_id, priority))
                            .and_modify(|maximum| *maximum = (*maximum).max(feedback_size))
                            .or_insert(feedback_size);
                    }
                }
            }
            for pair in descriptor.route.windows(2) {
                let controlled = links[pair[0].0 as usize];
                let downstream = links[pair[1].0 as usize].source;
                if nodes[controlled.source.0 as usize].kind != NodeKind::Switch
                    || nodes[downstream.0 as usize].kind != NodeKind::Switch
                {
                    continue;
                }
                monitored_paths.insert((controlled.id, downstream));
            }
            if matches!(
                input.traffic.kind,
                TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_)
            ) {
                for pair in descriptor.reverse_route.windows(2) {
                    let controlled = links[pair[0].0 as usize];
                    let downstream = links[pair[1].0 as usize].source;
                    if nodes[controlled.source.0 as usize].kind == NodeKind::Switch
                        && nodes[downstream.0 as usize].kind == NodeKind::Switch
                    {
                        monitored_paths.insert((controlled.id, downstream));
                    }
                }
            }
        }
        for (controlled_id, downstream) in monitored_paths {
            let max_frame_bytes = std::array::from_fn(|priority| {
                max_frame_by_link_priority
                    .get(&(controlled_id, priority))
                    .copied()
                    .unwrap_or(0)
            });
            let controlled = links[controlled_id.0 as usize];
            let upstream_physical = switch_states
                [nodes[controlled.source.0 as usize].state_slot as usize]
                .physical_switch;
            let downstream_physical =
                switch_states[nodes[downstream.0 as usize].state_slot as usize].physical_switch;
            let reverse = links
                .iter()
                .copied()
                .find(|candidate| {
                    let source = nodes[candidate.source.0 as usize];
                    let target = nodes[candidate.target.0 as usize];
                    source.kind == NodeKind::Switch
                        && target.kind == NodeKind::Switch
                        && switch_states[source.state_slot as usize].physical_switch
                            == downstream_physical
                        && switch_states[target.state_slot as usize].physical_switch
                            == upstream_physical
                })
                .ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "PFC controlled link {controlled_id:?} has no reverse physical link"
                    ))
                })?;
            let control_channel_index = u32::try_from(channels.len()).map_err(|_| {
                CompileError::Invalid("PFC control channel table exceeds u32".to_owned())
            })?;
            channels.push(RemoteChannel {
                source: downstream,
                target: controlled.source,
                link: reverse.id,
                event_kind: EventKind::RemoteArrival,
                min_delay_ns: reverse.delay_ns(64).map_err(|error| {
                    CompileError::Invalid(format!(
                        "PFC reverse channel for controlled link {controlled_id:?} overflows: {error}"
                    ))
                })?,
            });

            let upstream_slot = nodes[controlled.source.0 as usize].state_slot as usize;
            let upstream_queue = switch_states[upstream_slot]
                .queues
                .iter_mut()
                .find(|queue| queue.egress_link == Some(controlled_id))
                .expect("lowered switch egress owns the controlled link");
            upstream_queue
                .pfc
                .get_or_insert_with(PfcQueueState::default);

            let downstream_slot = nodes[downstream.0 as usize].state_slot as usize;
            let downstream_queue = switch_states[downstream_slot]
                .queues
                .first_mut()
                .expect("lowered switch LP owns one queue");
            downstream_queue
                .pfc
                .get_or_insert_with(PfcQueueState::default)
                .ingresses
                .push(PfcIngressState {
                    controlled_link: controlled_id,
                    control_channel_index,
                    buffer_capacity_bytes: pfc.buffer_capacity,
                    max_frame_bytes,
                    xoff_threshold_bytes: pfc.xoff,
                    xon_threshold_bytes: pfc.xon,
                    occupancy_bytes: [0; 8],
                    pause_asserted: [false; 8],
                });
        }
    }

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
    mut collectives: Vec<CollectiveKey>,
    hosts: &BTreeSet<u64>,
    host_attachments: &HostAttachments,
    seed: u64,
) -> Result<Vec<FlowInput>, CompileError> {
    explicit.sort();
    flow_sets.sort();
    collectives.sort();

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
            priority: semantic.priority,
            traffic: semantic.traffic,
            collective: None,
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
        // A structural pairing is a pure function of the attachment grid and never touches `rng`,
        // so a later `Random` flow set draws exactly the endpoints it would have drawn alone.
        let pairs = match semantic.pairing {
            PairingPolicy::Random => host_attachments
                .sample_canonical_flow_pairs(&mut rng, member_count)
                .map_err(CompileError::Invalid)?,
            policy => host_attachments
                .structural_flow_pairs(policy, member_count)
                .map_err(CompileError::Invalid)?,
        };
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
                priority: semantic.priority,
                traffic: semantic.traffic.clone(),
                collective: None,
            });
        }
    }

    let mut collective_duplicates = BTreeMap::<CollectiveKey, u64>::new();
    let mut next_collective_id = 0_u64;
    for mut semantic in collectives {
        if semantic.sources.is_empty() {
            let participants = hosts.iter().copied().collect::<Vec<_>>();
            if participants.len() != semantic.flow_count as usize {
                return Err(CompileError::Invalid(format!(
                    "collective flow_count {} must match the {} configured hosts when sources/sinks are omitted",
                    semantic.flow_count,
                    participants.len()
                )));
            }
            semantic.sources.clone_from(&participants);
            semantic.sinks = participants
                .iter()
                .cycle()
                .skip(1)
                .take(participants.len())
                .copied()
                .collect();
        }
        for participant in semantic.sources.iter().chain(&semantic.sinks) {
            if !hosts.contains(participant) {
                return Err(CompileError::Invalid(format!(
                    "collective participant {participant} must be a configured host attachment"
                )));
            }
        }
        let unique = semantic.sources.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != semantic.sources.len() {
            return Err(CompileError::Invalid(
                "collective ring participants must be unique".to_owned(),
            ));
        }

        let duplicate = collective_duplicates.entry(semantic.clone()).or_default();
        let duplicate_ordinal = *duplicate;
        *duplicate = duplicate.checked_add(1).ok_or_else(|| {
            CompileError::Invalid("duplicate collective ordinal overflow".to_owned())
        })?;
        let collective_id = next_collective_id;
        next_collective_id = next_collective_id
            .checked_add(1)
            .ok_or_else(|| CompileError::Invalid("collective identity exceeds u64".to_owned()))?;
        expand_collective(&mut flows, semantic, duplicate_ordinal, collective_id)?;
    }

    flows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(flows)
}

fn collective_chunk_bounds(total: u64, group_size: u64, owner: u64) -> (u64, u64) {
    let base = total / group_size;
    let offset = owner * base;
    let bytes = if owner + 1 == group_size {
        total - offset
    } else {
        base
    };
    (offset, bytes)
}

fn collective_stage_owner(
    algorithm: CollectiveAlgorithm,
    phase: CollectivePhase,
    group_size: u64,
    rank: u64,
    step: u64,
) -> u64 {
    match (algorithm, phase) {
        (CollectiveAlgorithm::RingAllReduce, CollectivePhase::AllGather) => {
            (rank + group_size - step + 2) % group_size
        }
        (CollectiveAlgorithm::RingAllReduce, CollectivePhase::ReduceScatter)
        | (CollectiveAlgorithm::AllGather, CollectivePhase::AllGather) => {
            (rank + group_size - step + 1) % group_size
        }
        (CollectiveAlgorithm::AllGather, CollectivePhase::ReduceScatter) => {
            unreachable!("AllGather has no ReduceScatter phase")
        }
    }
}

fn expand_collective(
    flows: &mut Vec<FlowInput>,
    semantic: CollectiveKey,
    duplicate_ordinal: u64,
    collective_id: u64,
) -> Result<(), CompileError> {
    let n = semantic.flow_count;
    let Termination::Bytes(total_bytes) = semantic.traffic.termination else {
        unreachable!("collective validation requires byte termination")
    };
    if semantic.algorithm == CollectiveAlgorithm::AllGather && (n == 1 || total_bytes == 0) {
        return Ok(());
    }
    let stage_count = match semantic.algorithm {
        CollectiveAlgorithm::RingAllReduce => {
            n.checked_mul(n - 1).and_then(|count| count.checked_mul(2))
        }
        CollectiveAlgorithm::AllGather => n.checked_mul(n - 1),
    }
    .ok_or_else(|| CompileError::Invalid("collective stage count exceeds u64".to_owned()))?;
    flows
        .try_reserve_exact(usize::try_from(stage_count).map_err(|_| {
            CompileError::Invalid(
                "collective stage count exceeds the platform index domain".to_owned(),
            )
        })?)
        .map_err(|error| {
            CompileError::Invalid(format!("collective stage table is too large: {error}"))
        })?;
    u32::try_from(n)
        .map_err(|_| CompileError::Invalid("collective group size exceeds u32".to_owned()))?;
    let phases: &[CollectivePhase] = match semantic.algorithm {
        CollectiveAlgorithm::RingAllReduce => {
            &[CollectivePhase::ReduceScatter, CollectivePhase::AllGather]
        }
        CollectiveAlgorithm::AllGather => &[CollectivePhase::AllGather],
    };
    let final_step = u32::try_from(n - 1)
        .map_err(|_| CompileError::Invalid("collective step exceeds u32".to_owned()))?;

    for &phase in phases {
        for rank_u64 in 0..n {
            let rank = u32::try_from(rank_u64)
                .map_err(|_| CompileError::Invalid("collective rank exceeds u32".to_owned()))?;
            let previous_rank = u32::try_from((rank_u64 + n - 1) % n)
                .expect("rank is below validated u32 group size");
            for step_u64 in 1..n {
                let step = u32::try_from(step_u64)
                    .map_err(|_| CompileError::Invalid("collective step exceeds u32".to_owned()))?;
                let owner =
                    collective_stage_owner(semantic.algorithm, phase, n, rank_u64, step_u64);
                let (chunk_offset_bytes, chunk_bytes) =
                    collective_chunk_bounds(total_bytes, n, owner);
                let position = CollectiveStagePosition { phase, rank, step };
                let local_predecessor = if step > 1 {
                    Some(CollectiveStagePosition {
                        phase,
                        rank,
                        step: step - 1,
                    })
                } else if semantic.algorithm == CollectiveAlgorithm::RingAllReduce
                    && phase == CollectivePhase::AllGather
                {
                    Some(CollectiveStagePosition {
                        phase: CollectivePhase::ReduceScatter,
                        rank,
                        step: final_step,
                    })
                } else {
                    None
                };
                let inbound_predecessor = if step > 1 {
                    Some(CollectiveStagePosition {
                        phase,
                        rank: previous_rank,
                        step: step - 1,
                    })
                } else if semantic.algorithm == CollectiveAlgorithm::RingAllReduce
                    && phase == CollectivePhase::AllGather
                {
                    Some(CollectiveStagePosition {
                        phase: CollectivePhase::ReduceScatter,
                        rank: previous_rank,
                        step: final_step,
                    })
                } else {
                    None
                };
                let local_predecessor_complete = local_predecessor.is_none_or(|predecessor| {
                    let predecessor_owner = collective_stage_owner(
                        semantic.algorithm,
                        predecessor.phase,
                        n,
                        u64::from(predecessor.rank),
                        u64::from(predecessor.step),
                    );
                    collective_chunk_bounds(total_bytes, n, predecessor_owner).1 == 0
                });
                let inbound_predecessor_complete =
                    inbound_predecessor.is_none() || chunk_bytes == 0;
                let mut traffic = semantic.traffic.clone();
                traffic.termination = Termination::Bytes(chunk_bytes);
                flows.push(FlowInput {
                    key: FlowKey::CollectiveStage {
                        semantic: semantic.clone(),
                        duplicate_ordinal,
                        stage: position,
                    },
                    source: semantic.sources[rank_u64 as usize],
                    target: semantic.sinks[rank_u64 as usize],
                    priority: semantic.priority,
                    traffic,
                    collective: Some(CollectiveStageInput {
                        collective_id,
                        declared_total_bytes: total_bytes,
                        position,
                        chunk_offset_bytes,
                        chunk_bytes,
                        local_predecessor,
                        inbound_predecessor,
                        inbound_predecessor_bytes: chunk_bytes,
                        local_predecessor_complete,
                        inbound_predecessor_complete,
                    }),
                });
            }
        }
    }
    Ok(())
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
                unreachable!("closed-loop validation requires byte termination")
            };
            flow.traffic
                .packet_size_bytes
                .checked_mul(count)
                .ok_or_else(|| {
                    CompileError::Invalid("closed-loop segment input count overflow".to_owned())
                })?;
            if total_bytes == 0 {
                return Err(CompileError::Invalid(
                    "closed-loop traffic `size` must be positive".to_owned(),
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
    if matches!(traffic.kind, TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_)) {
        let Termination::Bytes(bytes) = traffic.termination else {
            unreachable!("closed-loop validation requires byte termination")
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
            if semantic.priority != 0 {
                state = mix_seed(state ^ u64::from(semantic.priority));
            }
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
            if semantic.priority != 0 {
                state = mix_seed(state ^ u64::from(semantic.priority));
            }
            // Mixed only when a structural policy is named, so every pre-T21 scenario keeps the
            // generator seeds it was measured with.
            match semantic.pairing {
                PairingPolicy::Random => {}
                PairingPolicy::SwitchOffsetHalf => {
                    state = mix_seed(state ^ 0x5041_4952_5f4f_4646);
                }
                PairingPolicy::SameSwitchNext => {
                    state = mix_seed(state ^ 0x5041_4952_5f52_4143);
                }
            }
            state = mix_traffic_seed(state, &semantic.traffic);
            state = mix_seed(state ^ duplicate_ordinal);
            state = mix_seed(state ^ member_ordinal);
            state = mix_seed(state ^ source);
            state = mix_seed(state ^ target);
        }
        FlowKey::CollectiveStage {
            semantic,
            duplicate_ordinal,
            stage,
        } => {
            state = mix_seed(state ^ 0x434f_4c4c_4543_5449);
            state = mix_seed(state ^ semantic.flow_count);
            state = mix_seed(state ^ duplicate_ordinal);
            state = mix_seed(state ^ u64::from(stage.phase as u8));
            state = mix_seed(state ^ u64::from(stage.rank));
            state = mix_seed(state ^ u64::from(stage.step));
            state = mix_traffic_seed(state, &semantic.traffic);
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
        TrafficKind::Dcqcn(dcqcn) => {
            state = mix_seed(state ^ 0x4443_5143_4e00_0000);
            for value in [
                dcqcn.initial_rate_bps,
                dcqcn.minimum_rate_bps,
                dcqcn.maximum_rate_bps,
                dcqcn.additive_rate_bps,
                dcqcn.hyper_rate_bps,
                dcqcn.g_ppb,
                dcqcn.decrease_ppb,
                dcqcn.cnp_interval_ns,
                dcqcn.control_interval_ns,
                dcqcn.pacing_interval_ns,
                u64::from(dcqcn.cnp_priority),
                dcqcn.increase_byte_threshold,
            ] {
                state = mix_seed(state ^ value);
            }
            state
        }
    }
}

fn mix_seed(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::parsed_decimal;

    #[test]
    fn nonzero_decimal_exponents_outside_i64_keep_directional_errors() {
        let positive = parsed_decimal("1e9223372036854775808", "probe")
            .expect_err("positive exponent outside i64 must reject")
            .to_string();
        assert_eq!(
            positive,
            "invalid scenario: probe `1e9223372036854775808` exceeds the u64 representation"
        );

        let negative = parsed_decimal("1e-9223372036854775809", "probe")
            .expect_err("negative exponent outside i64 must reject")
            .to_string();
        assert_eq!(
            negative,
            "unsupported probe `1e-9223372036854775809`; exact representation requires an integer scaled value"
        );
    }
}

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::path::Path;

use days_executor::{
    Backend, CollectiveAlgorithm, CollectiveChannelPolicy, CollectiveChunkPolicy, CollectivePhase,
    CollectiveStage, CollectiveStageIdentity, ComputeStage, ConstantGenerator, DcqcnController,
    DcqcnControllerConfig, DcqcnGenerator, DcqcnReceiverState, DropMarkPolicy, EcnThresholdPolicy,
    Event, EventKey, EventKind, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId,
    GeneratorFeedbackState, GeneratorStatus, GeneratorTermination, HostState, LinkDescriptor,
    LinkId, NodeDescriptor, NodeId, NodeKind, PacketDescriptor, PacketKind, PayloadId,
    PfcIngressState, PfcQueueState, QueueDepthUnit, RateGenerator, RedPolicyState, RemoteChannel,
    RoceGenerator, RocePacer, RoceReceiverState, ScheduledEmission, SchedulerKind, SimulationImage,
    StageDependencies, StageRole, SwitchQueueState, SwitchState, TcpCongestionControl,
    TcpDataHeader, TcpGenerator, TcpReceiverState, event_phase, validate,
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
use crate::topos::rail::{RailProfile, ServerLocality};
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
    compute: Option<Vec<SourceCompute>>,
}

#[derive(Debug, Deserialize)]
struct SourceSwitch {
    port_rate: Option<ExactDecimal>,
    capacity: u64,
    discipline: Option<String>,
    drop: Option<String>,
    ecn_threshold: Option<ExactDecimal>,
    /// P16 H2: an ECN step threshold per egress link rate, in packets (SimAI's ECN rows are keyed
    /// by the port's rate); replaces `ecn_threshold` and needs `drop = "ECN_THRESHOLD"`.
    ecn_by_rate: Option<Vec<SourceEcnRow>>,
    weights: Option<Vec<u64>>,
    priorities: Option<Vec<u64>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceEcnRow {
    rate_bps: u64,
    threshold_packets: u64,
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
    /// SimAI's per-flow Murmur3 choice of spine at the source leaf of the rail fabric (P16 H2).
    SimAiEcmp,
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
    /// P15: also monitor host-to-switch links, so switches pause host NICs (default off).
    host_links: Option<bool>,
    xoff: Option<Vec<u64>>,
    xon: Option<Vec<u64>>,
    buffer_capacity: Option<Vec<u64>>,
    pause_quanta: Option<Vec<u16>>,
    refresh_interval: Option<ExactDecimal>,
    drain_interval: Option<ExactDecimal>,
    /// P16 H2, the rail fabric only: XOFF and XON per switch tier (SimAI's threshold is one per
    /// switch, set by its port count); replaces `xoff` and `xon`.
    by_tier: Option<Vec<SourcePfcTier>>,
    /// P16 H2: each monitor's headroom by its controlled link's rate, so its buffer capacity is
    /// XOFF plus that headroom on every enabled priority; replaces `buffer_capacity`.
    headroom_by_rate: Option<Vec<SourceHeadroomRow>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePfcTier {
    tier: String,
    xoff: Vec<u64>,
    xon: Vec<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceHeadroomRow {
    rate_bps: u64,
    bytes: u64,
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
    /// Stage-group name that `after` fields may reference.
    name: Option<String>,
    /// Compute stage group whose rank-r stage gates this collective's rank-r root stages.
    after: Option<String>,
    collective_type: String,
    first_flow_id: Option<u64>,
    flow_type: Option<String>,
    flow_count: u64,
    graph: Option<Vec<(u64, u64)>>,
    paths: Option<Vec<Vec<u64>>>,
    sources: Option<Vec<u64>>,
    sinks: Option<Vec<u64>>,
    priority: Option<u8>,
    routing: Option<toml::Value>,
    traffic: SourceTraffic,
}

/// A delay-only compute stage group: one timer-only stage per listed host.
#[derive(Debug, Deserialize)]
struct SourceCompute {
    name: String,
    hosts: Vec<u64>,
    duration_ns: u64,
    /// Stage group (compute or TCP collective) that each host's stage waits for.
    after: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SourceCollectiveSet {
    collective_type: String,
    collective_count: u64,
    first_flow_id: Option<u64>,
    flow_type: Option<String>,
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
    roce: Option<SourceRoce>,
}

/// `[flow.traffic.roce]`: the Go-back-N reliability of a RoCE queue pair. The pair's controller
/// and pacer come from `[flow.traffic.dcqcn]`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRoce {
    /// Required: a fixed retransmission timeout, or `0` for none (NACK-only recovery: a lost last
    /// packet then stalls the queue pair for the rest of the run).
    retransmit_timeout_ns: Option<u64>,
    ack_every_packets: Option<u64>,
    nack_interval_ns: Option<u64>,
    feedback_priority: Option<u8>,
    duplicate_ack: Option<bool>,
    ack_size_bytes: Option<u64>,
    /// P16 ruling D7: the window in bytes (SimAI `m_win`); 0 (the default) is no window.
    window_bytes: Option<u64>,
    /// P16 ruling D7: scale the window with the controller's rate (SimAI `m_var_win`).
    variable_window: Option<bool>,
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

/// `[flow.traffic.dcqcn]`: the Mellanox-form DCQCN reaction point (P16) and its pacer. Every key
/// but `max_rate_gbps` defaults to SimAI's and HPCC's shipped block (ruling D8). The paper-form
/// keys `mi_factor`, `rtt_ns` and `increase_byte_threshold` are rejected by name (ruling D9), and
/// any other unknown key by `deny_unknown_fields`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceDcqcn {
    max_rate_gbps: ExactDecimal,
    rate_gbps: Option<ExactDecimal>,
    min_rate_gbps: Option<ExactDecimal>,
    g: Option<ExactDecimal>,
    ai_rate_gbps: Option<ExactDecimal>,
    hai_rate_gbps: Option<ExactDecimal>,
    alpha_resume_interval_ns: Option<ExactDecimal>,
    rate_decrease_interval_ns: Option<ExactDecimal>,
    rp_timer_ns: Option<ExactDecimal>,
    fast_recovery_times: Option<u32>,
    clamp_target_rate: Option<bool>,
    cnp_interval_ns: Option<ExactDecimal>,
    pacing_interval_ns: Option<ExactDecimal>,
    cnp_priority: Option<u8>,
    mi_factor: Option<serde::de::IgnoredAny>,
    rtt_ns: Option<serde::de::IgnoredAny>,
    increase_byte_threshold: Option<serde::de::IgnoredAny>,
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
    /// The ordinal of the flow's [`RoceTrafficKey`] among the scenario's sorted distinct RoCE
    /// keys ([`SupportedModel::roce_keys`]). The mapping is order-isomorphic, so flows order
    /// exactly as they would with the key inline, while every `TrafficKind` keeps its size:
    /// inline, the key would grow `TrafficKey`, and with it every flow's `FlowInput`.
    Roce(u64),
}

/// The full semantic key of one RoCE queue pair: its DCQCN controller and pacer, and its
/// Go-back-N reliability. `dcqcn.cnp_priority` holds the pair's feedback priority (CNP, ACK and
/// NACK).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RoceTrafficKey {
    dcqcn: DcqcnTrafficKey,
    retransmit_timeout_ns: u64,
    ack_every_packets: u64,
    nack_interval_ns: u64,
    duplicate_ack: bool,
    ack_size_bytes: u64,
    window_bytes: u64,
    variable_window: bool,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DcqcnTrafficKey {
    initial_rate_bps: u64,
    minimum_rate_bps: u64,
    maximum_rate_bps: u64,
    additive_rate_bps: u64,
    hyper_rate_bps: u64,
    g_q63: u64,
    alpha_interval_ns: u64,
    decrease_interval_ns: u64,
    increase_interval_ns: u64,
    fast_recovery_steps: u32,
    clamp_target_rate: bool,
    /// The notification point's CNP spacing of an unreliable DCQCN flow; zero for a queue pair.
    cnp_interval_ns: u64,
    pacing_interval_ns: u64,
    cnp_priority: u8,
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
    Roce,
}

impl TrafficKey {
    /// The PFC class of the flow's receiver feedback: a DCQCN flow's CNP class, a RoCE queue
    /// pair's CNP, ACK and NACK class, else `priority`.
    fn feedback_priority(&self, priority: u8, roce_keys: &[RoceTrafficKey]) -> u8 {
        match self.kind {
            TrafficKind::Dcqcn(dcqcn) => dcqcn.cnp_priority,
            TrafficKind::Roce(ordinal) => roce_key(roce_keys, ordinal).dcqcn.cnp_priority,
            TrafficKind::Constant | TrafficKind::Tcp(_) => priority,
        }
    }
}

/// The RoCE key a lowered `TrafficKind::Roce` ordinal names.
fn roce_key(roce_keys: &[RoceTrafficKey], ordinal: u64) -> RoceTrafficKey {
    roce_keys[usize::try_from(ordinal).expect("RoCE key ordinals index the key table")]
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TrafficKey {
    initial_delay_ns: u64,
    interval_ns: u64,
    packet_size_bytes: u64,
    termination: Termination,
    kind: TrafficKind,
}

/// Termination identity used when another backend must select the same semantic ECMP route.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FatTreeEcmpTermination {
    /// Stop after sending the given byte count.
    Bytes(u64),
    /// Stop after the given exact duration in nanoseconds.
    DurationNs(u64),
}

/// Transport identity used when another backend must select the same semantic ECMP route.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FatTreeEcmpTransport {
    /// Constant packet generation.
    Constant,
    /// TCP Reno.
    TcpReno,
    /// TCP CUBIC.
    TcpCubic,
}

/// Traffic identity shared by the exact compiler and legacy fat-tree ECMP selection.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FatTreeEcmpTrafficKey {
    /// Exact source start offset.
    pub initial_delay_ns: u64,
    /// Exact constant packet interval, or zero for window-driven TCP.
    pub interval_ns: u64,
    /// Packet payload size in bytes.
    pub packet_size_bytes: u64,
    /// Flow termination condition.
    pub termination: FatTreeEcmpTermination,
    /// Traffic transport/controller identity.
    pub transport: FatTreeEcmpTransport,
}

impl From<FatTreeEcmpTrafficKey> for TrafficKey {
    fn from(value: FatTreeEcmpTrafficKey) -> Self {
        Self {
            initial_delay_ns: value.initial_delay_ns,
            interval_ns: value.interval_ns,
            packet_size_bytes: value.packet_size_bytes,
            termination: match value.termination {
                FatTreeEcmpTermination::Bytes(bytes) => Termination::Bytes(bytes),
                FatTreeEcmpTermination::DurationNs(duration_ns) => {
                    Termination::DurationNs(duration_ns)
                }
            },
            kind: match value.transport {
                FatTreeEcmpTransport::Constant => TrafficKind::Constant,
                FatTreeEcmpTransport::TcpReno => TrafficKind::Tcp(TcpAlgorithm::Reno),
                FatTreeEcmpTransport::TcpCubic => TrafficKind::Tcp(TcpAlgorithm::Cubic),
            },
        }
    }
}

/// Selects the compiler-identical fat-tree ECMP hash for an explicit flow.
pub fn fat_tree_ecmp_explicit_flow_hash(
    seed: u64,
    source: u64,
    target: u64,
    priority: u8,
    traffic: FatTreeEcmpTrafficKey,
    duplicate_ordinal: u64,
) -> u64 {
    let key = FlowKey::Explicit {
        semantic: ExplicitFlowKey {
            source,
            target,
            priority,
            traffic: traffic.into(),
        },
        duplicate_ordinal,
    };
    generator_seed(seed ^ 0x4543_4d50_5f48_4153, &key, &[], &[])
}

/// Selects the compiler-identical fat-tree ECMP hash for one flow-set member.
#[allow(clippy::too_many_arguments)]
pub fn fat_tree_ecmp_flow_set_member_hash(
    seed: u64,
    flow_count: u64,
    priority: u8,
    traffic: FatTreeEcmpTrafficKey,
    pairing: PairingPolicy,
    duplicate_ordinal: u64,
    member_ordinal: u64,
    source: u64,
    target: u64,
) -> u64 {
    let key = FlowKey::SetMember {
        semantic: FlowSetKey {
            flow_count,
            priority,
            traffic: traffic.into(),
            pairing,
        },
        duplicate_ordinal,
        member_ordinal,
        source,
        target,
    };
    generator_seed(seed ^ 0x4543_4d50_5f48_4153, &key, &[], &[])
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
    /// Stage-group identity; `None` for every unnamed collective, which keeps their order.
    name: Option<String>,
    after: Option<String>,
}

/// One delay-only compute stage group.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ComputeKey {
    name: String,
    hosts: Vec<u64>,
    duration_ns: u64,
    after: Option<String>,
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
    /// `collective` is the ordinal of the stage's normalized [`CollectiveKey`] among the scenario's
    /// sorted distinct collective keys, which [`CanonicalFlows::collectives`] holds. The mapping is
    /// order-isomorphic, so these keys order exactly as keys embedding the full collective key
    /// would, while comparing, cloning and storing in constant space instead of O(ranks).
    CollectiveStage {
        collective: u64,
        duplicate_ordinal: u64,
        stage: CollectiveStagePosition,
    },
    ComputeStage {
        semantic: ComputeKey,
        rank: u32,
    },
}

#[derive(Clone, Debug)]
struct CollectiveStageInput {
    collective_id: u64,
    algorithm: CollectiveAlgorithm,
    group_size: u32,
    declared_total_bytes: u64,
    position: CollectiveStagePosition,
    chunk_offset_bytes: u64,
    chunk_bytes: u64,
    local_predecessor: Option<FlowKey>,
    inbound_predecessor: Option<FlowKey>,
    inbound_predecessor_bytes: u64,
    local_predecessor_complete: bool,
    inbound_predecessor_complete: bool,
    /// P16 H2: the NVLink message delay when the two ranks share a server on the rail fabric, so
    /// the stage lowers to a stage notify; set by `NotifyLowering::plan`, `None` otherwise. Held
    /// in the boxed sidecar, so a flow without a stage pays nothing to ask.
    notify_delay_ns: Option<u64>,
}

impl FlowInput {
    /// The stage notify's message delay of this flow, if it lowers to one (P16 H2).
    fn notify_delay_ns(&self) -> Option<u64> {
        self.collective.as_deref()?.notify_delay_ns
    }
}

#[derive(Clone, Debug)]
struct ComputeStageInput {
    compute_id: u64,
    group_size: u32,
    rank: u32,
    duration_ns: u64,
    local_predecessor: Option<FlowKey>,
    inbound_predecessor: Option<FlowKey>,
    inbound_predecessor_bytes: u64,
}

/// One flow to lower, before the canonical sort assigns its dense identifier.
///
/// The stage sidecars are boxed: most flows have neither, and a sidecar held by value (448 B for a
/// collective stage, 416 B for a compute stage, each with its predecessors' full `FlowKey`s) would
/// be paid by every flow and moved by every step of the flow sort. Boxed, a flow without stages
/// pays one null pointer per sidecar; `tests::flow_input_carries_its_stage_sidecars_out_of_line`
/// bounds the size.
#[derive(Clone, Debug)]
struct FlowInput {
    key: FlowKey,
    source: u64,
    target: u64,
    priority: u8,
    traffic: TrafficKey,
    collective: Option<Box<CollectiveStageInput>>,
    compute: Option<Box<ComputeStageInput>>,
}

/// One host's generator and stage tables as lowering builds them.
///
/// The stage table stays unallocated until the host's first stage generator arrives, is then
/// back-filled with `None` for the generators before it, and grows with every later generator. A
/// host without a stage therefore leaves lowering with an empty table and no stage allocation, and
/// both tables move into the host state without a copy: the canonical shapes of
/// `HostState::stages`.
#[derive(Default)]
struct HostTables {
    generators: Vec<FlowGeneratorState>,
    stages: Vec<Option<CollectiveStage>>,
}

impl HostTables {
    fn push(&mut self, (generator, stage): (FlowGeneratorState, Option<CollectiveStage>)) {
        if stage.is_some() || !self.stages.is_empty() {
            self.stages.resize(self.generators.len(), None);
            self.stages.push(stage);
        }
        self.generators.push(generator);
    }
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
    /// `switch.port_rate`, the one link rate of every topology but the rail fabric, whose
    /// per-class rates come from its topology table (`None` there; required elsewhere).
    rate_bps: Option<u64>,
    queue_capacity_packets: u64,
    scheduler: SchedulerKind,
    drop_mark: DropMarkPolicy,
    /// P16 H2: the ECN step threshold of each egress LP by its link rate, overriding `drop_mark`'s.
    ecn_by_rate: Option<BTreeMap<u64, u64>>,
    pfc: Option<PfcLowering>,
    routing: RoutingPolicy,
    propagation: PropagationModel,
    explicit_flows: Vec<ExplicitFlowKey>,
    flow_sets: Vec<FlowSetKey>,
    collectives: Vec<CollectiveKey>,
    computes: Vec<ComputeKey>,
    /// The scenario's distinct RoCE keys, sorted; `TrafficKind::Roce` holds an index into it.
    roce_keys: Vec<RoceTrafficKey>,
}

/// Sorts the RoCE keys recorded in parse order, removes duplicates, and rewrites the parse-order
/// ordinal of every flow, flow set and collective to its key's rank, so ordinals order exactly as
/// the keys do.
fn canonical_roce_keys(
    recorded: Vec<RoceTrafficKey>,
    explicit_flows: &mut [ExplicitFlowKey],
    flow_sets: &mut [FlowSetKey],
    collectives: &mut [CollectiveKey],
) -> Vec<RoceTrafficKey> {
    if recorded.is_empty() {
        return recorded;
    }
    let mut sorted = recorded.clone();
    sorted.sort_unstable();
    sorted.dedup();
    let rank = recorded
        .iter()
        .map(|key| {
            u64::try_from(
                sorted
                    .binary_search(key)
                    .expect("every recorded key is in the sorted table"),
            )
            .expect("key count fits u64")
        })
        .collect::<Vec<_>>();
    let traffic = explicit_flows
        .iter_mut()
        .map(|flow| &mut flow.traffic)
        .chain(flow_sets.iter_mut().map(|set| &mut set.traffic))
        .chain(
            collectives
                .iter_mut()
                .map(|collective| &mut collective.traffic),
        );
    for traffic in traffic {
        if let TrafficKind::Roce(ordinal) = &mut traffic.kind {
            *ordinal = rank[usize::try_from(*ordinal).expect("parse ordinals index the record")];
        }
    }
    sorted
}

/// How lowering stamps `LinkDescriptor::propagation_ns`.
///
/// `Uniform` is the standing model. `FatTreeTiers` (T21/P12 F-HET) keys the delay on the fabric
/// layer a link belongs to: host attachment, edge-to-aggregation, aggregation-to-core.
#[derive(Clone, Copy, Debug)]
enum PropagationModel {
    /// No delay key: zero on every topology but the rail fabric, whose topology table names its
    /// per-class delays.
    Undeclared,
    Uniform(u64),
    FatTreeTiers(SourcePropagationTiers),
}

/// Per-priority byte thresholds of one PFC table: XOFF and XON.
type PfcThresholds = ([u64; 8], [u64; 8]);

/// XOFF, XON and buffer capacity of one ingress monitor.
type PfcMonitorThresholds = ([u64; 8], [u64; 8], [u64; 8]);

#[derive(Clone)]
struct PfcLowering {
    xoff: [u64; 8],
    xon: [u64; 8],
    buffer_capacity: [u64; 8],
    /// Monitor host-to-switch links as well as switch-to-switch links.
    host_links: bool,
    /// P16 H2: XOFF and XON of the ASW tier and the PSW tier, replacing `xoff` and `xon`.
    tiers: Option<[PfcThresholds; 2]>,
    /// P16 H2: headroom by controlled link rate, replacing `buffer_capacity`.
    headroom_by_rate: Option<BTreeMap<u64, u64>>,
}

impl PfcLowering {
    /// The thresholds of one monitor: its downstream switch's tier (on the rail fabric) and its
    /// controlled link's rate pick XOFF, XON and the buffer capacity.
    fn thresholds(
        &self,
        profile: TopologyProfile,
        downstream_switch: u64,
        controlled_rate_bps: u64,
    ) -> Result<PfcMonitorThresholds, CompileError> {
        let (xoff, xon) = match (&self.tiers, profile) {
            (None, _) => (self.xoff, self.xon),
            (Some(tiers), TopologyProfile::Rail(rail)) => {
                let tier = u32::try_from(downstream_switch)
                    .map(|switch| usize::from(!rail.is_asw(switch)))
                    .map_err(|_| CompileError::Invalid("switch identity exceeds u32".to_owned()))?;
                tiers[tier]
            }
            (Some(_), _) => {
                return Err(CompileError::Unsupported(
                    "unsupported `link.pfc.by_tier` off the SpectrumX rail fabric; switch tiers \
                     are rail tiers"
                        .to_owned(),
                ));
            }
        };
        let buffer_capacity = match &self.headroom_by_rate {
            None => self.buffer_capacity,
            Some(rows) => {
                let headroom = *rows.get(&controlled_rate_bps).ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "`link.pfc.headroom_by_rate` has no row for a {controlled_rate_bps} b/s \
                         controlled link"
                    ))
                })?;
                let mut capacity = [0; 8];
                for priority in 0..8 {
                    if xoff[priority] != 0 {
                        capacity[priority] =
                            xoff[priority].checked_add(headroom).ok_or_else(|| {
                                CompileError::Invalid("PFC buffer capacity exceeds u64".to_owned())
                            })?;
                    }
                }
                capacity
            }
        };
        Ok((xoff, xon, buffer_capacity))
    }
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
        let rate_bps = source
            .switch
            .port_rate
            .as_ref()
            .map(|rate| parse_rate(scenario_text, Some(rate)))
            .transpose()?;

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
            "ECN_THRESHOLD" if source.switch.ecn_by_rate.is_some() => {
                if source.switch.ecn_threshold.is_some() {
                    return Err(CompileError::Invalid(
                        "`switch.ecn_by_rate` replaces `switch.ecn_threshold`".to_owned(),
                    ));
                }
                // Each egress LP takes its link rate's row at lowering; this policy only carries
                // the shared capacity and unit.
                DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
                    unit: QueueDepthUnit::Packets,
                    capacity: source.switch.capacity,
                    threshold: source.switch.capacity,
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

        let ecn_by_rate = match (&source.switch.ecn_by_rate, drop) {
            (None, _) => None,
            (Some(rows), "ECN_THRESHOLD") => Some(ecn_rows(rows, source.switch.capacity)?),
            (Some(_), _) => {
                return Err(CompileError::Invalid(
                    "`switch.ecn_by_rate` needs `switch.drop = \"ECN_THRESHOLD\"`".to_owned(),
                ));
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
                let mut tiers = config.by_tier.as_deref().map(pfc_tiers).transpose()?;
                let headroom_by_rate = config
                    .headroom_by_rate
                    .as_deref()
                    .map(pfc_headroom_rows)
                    .transpose()?;
                if tiers.is_some() && (config.xoff.is_some() || config.xon.is_some()) {
                    return Err(CompileError::Invalid(
                        "`link.pfc.by_tier` replaces `link.pfc.xoff` and `link.pfc.xon`".to_owned(),
                    ));
                }
                if headroom_by_rate.is_some() && config.buffer_capacity.is_some() {
                    return Err(CompileError::Invalid(
                        "`link.pfc.headroom_by_rate` replaces `link.pfc.buffer_capacity`"
                            .to_owned(),
                    ));
                }
                // With tiers, the ASW tier stands for the enabled priorities (both tiers enable
                // the same ones); with headroom rows, XOFF stands for the per-monitor capacity.
                let (mut xoff, mut xon) = match &tiers {
                    Some(tiers) => tiers[0],
                    None => (
                        exact_pfc_array(config.xoff.as_deref(), "xoff")?,
                        exact_pfc_array(config.xon.as_deref(), "xon")?,
                    ),
                };
                let mut buffer_capacity = if headroom_by_rate.is_some() {
                    xoff
                } else {
                    exact_pfc_array(config.buffer_capacity.as_deref(), "buffer_capacity")?
                };
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
                            for (tier_xoff, tier_xon) in tiers.iter_mut().flatten() {
                                tier_xoff[priority] = 0;
                                tier_xon[priority] = 0;
                            }
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
                    host_links: config.host_links.unwrap_or(false),
                    tiers,
                    headroom_by_rate,
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
        // RoCE keys are recorded in parse order and then replaced by their rank among the sorted
        // distinct keys, so lowered flow identity depends only on the scenario's content.
        let mut roce_keys = Vec::new();
        let mut explicit_flows = source
            .flow
            .unwrap_or_default()
            .into_iter()
            .map(|flow| validate_explicit_flow(flow, scenario_text, &mut roce_keys))
            .collect::<Result<Vec<_>, _>>()?;
        let mut flow_sets = source
            .flow_set
            .unwrap_or_default()
            .into_iter()
            .map(|flow_set| validate_flow_set(flow_set, scenario_text, &mut roce_keys))
            .collect::<Result<Vec<_>, _>>()?;
        let mut collectives = source
            .collective
            .unwrap_or_default()
            .into_iter()
            .map(|collective| validate_collective(collective, scenario_text, &mut roce_keys))
            .collect::<Result<Vec<_>, _>>()?;
        for collective_set in source.collective_set.unwrap_or_default() {
            collectives.extend(validate_collective_set(
                collective_set,
                scenario_text,
                &mut roce_keys,
            )?);
        }
        // After the collectives, whose keys sort them in `canonical_flows`.
        let roce_keys = canonical_roce_keys(
            roce_keys,
            &mut explicit_flows,
            &mut flow_sets,
            &mut collectives,
        );
        let computes = source
            .compute
            .unwrap_or_default()
            .into_iter()
            .map(validate_compute)
            .collect::<Result<Vec<_>, _>>()?;

        let routing = match source
            .routing
            .as_ref()
            .map(|routing| routing.policy.as_str())
        {
            None | Some("ShortestPath") => RoutingPolicy::ShortestPath,
            Some("FatTreeEcmp") => RoutingPolicy::FatTreeEcmp,
            Some("SimAiEcmp") => RoutingPolicy::SimAiEcmp,
            Some(unsupported) => {
                return Err(CompileError::Unsupported(format!(
                    "unsupported `routing.policy` `{unsupported}`; Days lowering supports \
                     ShortestPath, FatTreeEcmp and SimAiEcmp"
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
            (Some(propagation_ns), None) => PropagationModel::Uniform(propagation_ns),
            (None, None) => PropagationModel::Undeclared,
        };

        Ok(Self {
            seed,
            stop_time_ns,
            rate_bps,
            queue_capacity_packets: source.switch.capacity,
            scheduler,
            drop_mark,
            ecn_by_rate,
            pfc,
            routing,
            propagation,
            explicit_flows,
            flow_sets,
            collectives,
            computes,
            roce_keys,
        })
    }
}

/// `link.pfc.by_tier`: exactly the `asw` and `psw` tiers, each with eight XOFF and XON entries,
/// enabling the same priorities, as `[(asw xoff, asw xon), (psw xoff, psw xon)]`.
fn pfc_tiers(rows: &[SourcePfcTier]) -> Result<[PfcThresholds; 2], CompileError> {
    let invalid = || {
        CompileError::Invalid(
            "`link.pfc.by_tier` must list the `asw` and `psw` tiers once each, with eight XOFF and \
             XON entries enabling the same priorities"
                .to_owned(),
        )
    };
    let tier = |name: &str| -> Result<PfcThresholds, CompileError> {
        let mut found = rows.iter().filter(|row| row.tier == name);
        let row = found.next().ok_or_else(invalid)?;
        if found.next().is_some() {
            return Err(invalid());
        }
        let xoff: [u64; 8] = row.xoff.as_slice().try_into().map_err(|_| invalid())?;
        let mut xon: [u64; 8] = row.xon.as_slice().try_into().map_err(|_| invalid())?;
        for priority in 0..8 {
            if xoff[priority] == 0 {
                xon[priority] = 0;
            }
        }
        Ok((xoff, xon))
    };
    if rows.len() != 2 {
        return Err(invalid());
    }
    let tiers = [tier("asw")?, tier("psw")?];
    if (0..8).any(|priority| (tiers[0].0[priority] == 0) != (tiers[1].0[priority] == 0)) {
        return Err(invalid());
    }
    Ok(tiers)
}

/// `link.pfc.headroom_by_rate`: one positive headroom per distinct link rate.
fn pfc_headroom_rows(rows: &[SourceHeadroomRow]) -> Result<BTreeMap<u64, u64>, CompileError> {
    let mut headroom = BTreeMap::new();
    for row in rows {
        if row.rate_bps == 0 || row.bytes == 0 || headroom.insert(row.rate_bps, row.bytes).is_some()
        {
            return Err(CompileError::Invalid(
                "`link.pfc.headroom_by_rate` needs one positive headroom per distinct positive rate"
                    .to_owned(),
            ));
        }
    }
    Ok(headroom)
}

/// `switch.ecn_by_rate`: one step threshold per distinct link rate, within the queue capacity.
fn ecn_rows(rows: &[SourceEcnRow], capacity: u64) -> Result<BTreeMap<u64, u64>, CompileError> {
    let mut thresholds = BTreeMap::new();
    for row in rows {
        if row.rate_bps == 0
            || row.threshold_packets == 0
            || row.threshold_packets > capacity
            || thresholds
                .insert(row.rate_bps, row.threshold_packets)
                .is_some()
        {
            return Err(CompileError::Invalid(format!(
                "`switch.ecn_by_rate` needs one threshold in 1..={capacity} packets per distinct \
                 positive rate"
            )));
        }
    }
    Ok(thresholds)
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
        "RoCE" => Ok(SourceFlowKind::Roce),
        _ => Err(CompileError::Unsupported(format!(
            "unsupported flow type `{flow_type}`; Days executor supports PacketDistribution, exact TCP Reno/CUBIC, exact DCQCN, and RoCE queue-pair traffic"
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
    roce_keys: &mut Vec<RoceTrafficKey>,
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
    let traffic = validate_traffic(flow.traffic, flow_kind, priority, scenario_text, roce_keys)?;
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
    roce_keys: &mut Vec<RoceTrafficKey>,
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
    let traffic = validate_traffic(
        flow_set.traffic,
        flow_kind,
        priority,
        scenario_text,
        roce_keys,
    )?;
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

/// Collectives run over a reliable transport, TCP or RoCE queue pairs: a stage completes when its
/// last byte is acknowledged, which fixed-rate and unreliable DCQCN streams cannot report.
fn validate_collective_transport(flow_type: Option<&str>) -> Result<SourceFlowKind, CompileError> {
    match flow_type {
        Some("TCP") => Ok(SourceFlowKind::Tcp),
        Some("RoCE") => Ok(SourceFlowKind::Roce),
        Some(unsupported) => Err(CompileError::Unsupported(format!(
            "unsupported collective flow type `{unsupported}`; collectives require a reliable transport, flow_type = \"TCP\" or \"RoCE\" (a RoCE queue pair is DCQCN with Go-back-N)"
        ))),
        None => Err(CompileError::Unsupported(
            "collective flow_type is missing; collectives require a reliable transport, flow_type = \"TCP\" or \"RoCE\""
                .to_owned(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn collective_key(
    collective_type: &str,
    flow_type: Option<&str>,
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
    roce_keys: &mut Vec<RoceTrafficKey>,
) -> Result<CollectiveKey, CompileError> {
    let algorithm = collective_algorithm(collective_type)?;
    let flow_kind = validate_collective_transport(flow_type)?;
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
    // A RoCE collective records its key with the flows', so its stage queue pairs lower with
    // their `[collective.traffic.roce]` configuration and hash its content into their seeds.
    let traffic = validate_traffic(
        traffic,
        flow_kind,
        priority.unwrap_or(0),
        scenario_text,
        roce_keys,
    )?;
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
    if total_bytes < flow_count {
        // EqualRemainderLast would leave an empty chunk, and a TCP flow or a RoCE queue pair must
        // carry bytes.
        let transport = if flow_kind == SourceFlowKind::Roce {
            "RoCE"
        } else {
            "TCP"
        };
        return Err(CompileError::Invalid(format!(
            "{transport} collective byte size {total_bytes} must be at least flow_count {flow_count}"
        )));
    }
    Ok(CollectiveKey {
        algorithm,
        flow_count,
        sources,
        sinks,
        priority: priority.unwrap_or(0),
        traffic,
        name: None,
        after: None,
    })
}

fn validate_compute(source: SourceCompute) -> Result<ComputeKey, CompileError> {
    if source.duration_ns == 0 {
        return Err(CompileError::Invalid(format!(
            "compute `{}` duration_ns must be positive",
            source.name
        )));
    }
    if source.hosts.is_empty() {
        return Err(CompileError::Invalid(format!(
            "compute `{}` must list at least one host",
            source.name
        )));
    }
    if source.hosts.iter().collect::<BTreeSet<_>>().len() != source.hosts.len() {
        return Err(CompileError::Invalid(format!(
            "compute `{}` hosts must be unique",
            source.name
        )));
    }
    u32::try_from(source.hosts.len()).map_err(|_| {
        CompileError::Invalid(format!("compute `{}` group size exceeds u32", source.name))
    })?;
    Ok(ComputeKey {
        name: source.name,
        hosts: source.hosts,
        duration_ns: source.duration_ns,
        after: source.after,
    })
}

fn validate_collective(
    source: SourceCollective,
    scenario_text: &str,
    roce_keys: &mut Vec<RoceTrafficKey>,
) -> Result<CollectiveKey, CompileError> {
    let (name, after) = (source.name, source.after);
    let mut key = collective_key(
        &source.collective_type,
        source.flow_type.as_deref(),
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
        roce_keys,
    )?;
    key.name = name;
    key.after = after;
    Ok(key)
}

fn validate_collective_set(
    source: SourceCollectiveSet,
    scenario_text: &str,
    roce_keys: &mut Vec<RoceTrafficKey>,
) -> Result<Vec<CollectiveKey>, CompileError> {
    collective_algorithm(&source.collective_type)?;
    validate_collective_transport(source.flow_type.as_deref())?;
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
            source.flow_type.as_deref(),
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
            roce_keys,
        )?);
    }
    Ok(result)
}

/// Parses and validates a `[flow.traffic.dcqcn]` table: the controller and pacer of a DCQCN flow
/// or a RoCE queue pair. `priority` is the flow's class, the default of its feedback (CNP) class.
fn dcqcn_traffic_key(
    dcqcn: &SourceDcqcn,
    priority: u8,
    scenario_text: &str,
) -> Result<DcqcnTrafficKey, CompileError> {
    for (present, key, reason) in [
        (
            dcqcn.mi_factor.is_some(),
            "mi_factor",
            "the Mellanox-form cut is alpha / 2",
        ),
        (
            dcqcn.rtt_ns.is_some(),
            "rtt_ns",
            "the rate-increase timer is `rp_timer_ns`",
        ),
        (
            dcqcn.increase_byte_threshold.is_some(),
            "increase_byte_threshold",
            "the Mellanox form has no byte counter",
        ),
    ] {
        if present {
            return Err(CompileError::Unsupported(format!(
                "unsupported paper-form DCQCN key `{key}` (removed in P16: {reason}); see docs/content/docs/configuration/flows.mdx"
            )));
        }
    }
    let gbps = |value: Option<&ExactDecimal>, default: &str, label: &str| {
        optional_scaled_decimal(scenario_text, value, default, 1_000_000_000, label)
    };
    let nanoseconds = |value: Option<&ExactDecimal>, default: &str, label: &str| {
        optional_scaled_decimal(scenario_text, value, default, 1, label)
    };
    let maximum_rate_bps = scaled_decimal(
        scenario_text,
        &dcqcn.max_rate_gbps,
        1_000_000_000,
        "DCQCN maximum rate",
    )?;
    let g_literal = match dcqcn.g.as_ref() {
        Some(value) => exact_decimal_literal(scenario_text, value, "DCQCN g")?,
        None => "0.00390625",
    };
    let key = DcqcnTrafficKey {
        initial_rate_bps: match dcqcn.rate_gbps.as_ref() {
            Some(value) => scaled_decimal(scenario_text, value, 1_000_000_000, "DCQCN rate")?,
            None => maximum_rate_bps,
        },
        minimum_rate_bps: gbps(dcqcn.min_rate_gbps.as_ref(), "0.1", "DCQCN minimum rate")?,
        maximum_rate_bps,
        additive_rate_bps: gbps(dcqcn.ai_rate_gbps.as_ref(), "0.05", "DCQCN additive rate")?,
        hyper_rate_bps: gbps(dcqcn.hai_rate_gbps.as_ref(), "0.1", "DCQCN hyper rate")?,
        g_q63: q63_decimal_literal(g_literal, "DCQCN g")?,
        alpha_interval_ns: nanoseconds(
            dcqcn.alpha_resume_interval_ns.as_ref(),
            "1000",
            "DCQCN alpha resume interval ns",
        )?,
        decrease_interval_ns: nanoseconds(
            dcqcn.rate_decrease_interval_ns.as_ref(),
            "4000",
            "DCQCN rate decrease interval ns",
        )?,
        increase_interval_ns: nanoseconds(
            dcqcn.rp_timer_ns.as_ref(),
            "900000",
            "DCQCN rp timer ns",
        )?,
        fast_recovery_steps: dcqcn.fast_recovery_times.unwrap_or(1),
        clamp_target_rate: dcqcn.clamp_target_rate.unwrap_or(false),
        cnp_interval_ns: nanoseconds(dcqcn.cnp_interval_ns.as_ref(), "0", "DCQCN CNP interval ns")?,
        pacing_interval_ns: nanoseconds(
            dcqcn.pacing_interval_ns.as_ref(),
            "1000",
            "DCQCN pacing interval ns",
        )?,
        // The flow's feedback priority (P15 ruling D6): CNPs ride the data class unless
        // the flow names another. Before P15 the default was 0 and any other value had to
        // equal the flow priority, so every config accepted then keeps its key.
        cnp_priority: dcqcn.cnp_priority.unwrap_or(priority),
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
    dcqcn_controller_config(key)
        .validate()
        .map_err(|error| CompileError::Invalid(error.to_string()))?;
    Ok(key)
}

impl DcqcnTrafficKey {
    /// The key's content, in declaration order, for the flow seed.
    const fn seed_words(self) -> [u64; 14] {
        [
            self.initial_rate_bps,
            self.minimum_rate_bps,
            self.maximum_rate_bps,
            self.additive_rate_bps,
            self.hyper_rate_bps,
            self.g_q63,
            self.alpha_interval_ns,
            self.decrease_interval_ns,
            self.increase_interval_ns,
            self.fast_recovery_steps as u64,
            self.clamp_target_rate as u64,
            self.cnp_interval_ns,
            self.pacing_interval_ns,
            self.cnp_priority as u64,
        ]
    }
}

/// The controller configuration a lowered DCQCN key names.
const fn dcqcn_controller_config(key: DcqcnTrafficKey) -> DcqcnControllerConfig {
    DcqcnControllerConfig {
        initial_rate_bps: key.initial_rate_bps,
        minimum_rate_bps: key.minimum_rate_bps,
        maximum_rate_bps: key.maximum_rate_bps,
        additive_rate_bps: key.additive_rate_bps,
        hyper_rate_bps: key.hyper_rate_bps,
        g_q63: key.g_q63,
        alpha_interval_ns: key.alpha_interval_ns,
        decrease_interval_ns: key.decrease_interval_ns,
        increase_interval_ns: key.increase_interval_ns,
        fast_recovery_steps: key.fast_recovery_steps,
        clamp_target_rate: key.clamp_target_rate,
    }
}

/// An exact decimal `0 <= g <= 1` in Q63 (`1` is `2^63`), rounded half to even: exact for every
/// dyadic gain down to 2^-63 (1/16, 1/256, 1/1024, ...), and within 2^-64 otherwise (P16 design
/// note §1.6). At most 19 significant digits and 38 fractional places are accepted; integer
/// arithmetic only.
fn q63_decimal_literal(literal: &str, label: &str) -> Result<u64, CompileError> {
    let literal = literal.trim();
    let parsed = parsed_decimal(literal, label)?;
    let out_of_range = || CompileError::Invalid(format!("{label} must be in 0..=1, got {literal}"));
    if parsed.negative && parsed.digits != "0" {
        return Err(out_of_range());
    }
    if parsed.digits == "0" {
        return Ok(0);
    }
    if parsed.digits.len() > 19 || parsed.power < -38 {
        return Err(CompileError::Unsupported(format!(
            "unsupported {label} `{literal}`; at most 19 significant digits and 38 decimal places"
        )));
    }
    let digits = parsed.digits.parse::<u128>().map_err(|_| out_of_range())?;
    let (numerator, denominator) = if parsed.power >= 0 {
        let scale = 10_u128
            .checked_pow(u32::try_from(parsed.power).map_err(|_| out_of_range())?)
            .ok_or_else(out_of_range)?;
        (digits.checked_mul(scale).ok_or_else(out_of_range)?, 1_u128)
    } else {
        (digits, 10_u128.pow(parsed.power.unsigned_abs() as u32))
    };
    if numerator > denominator {
        return Err(out_of_range());
    }
    // numerator <= denominator <= 10^38 < 2^127: binary long division stays within u128.
    long_q63(numerator, denominator).ok_or_else(out_of_range)
}

/// `round_half_even(numerator * 2^63 / denominator)` for `numerator <= denominator`, by binary long
/// division (exact for any `denominator < 2^127`).
fn long_q63(numerator: u128, denominator: u128) -> Option<u64> {
    let mut quotient: u128 = numerator / denominator;
    let mut rest = numerator % denominator;
    for _ in 0..63 {
        rest <<= 1;
        quotient <<= 1;
        if rest >= denominator {
            rest -= denominator;
            quotient |= 1;
        }
    }
    // Round half to even on the remainder.
    let twice = rest << 1;
    if twice > denominator || twice == denominator && quotient & 1 == 1 {
        quotient += 1;
    }
    u64::try_from(quotient).ok().filter(|&q| q <= 1 << 63)
}

/// Validates one flow's traffic options. `priority` is the flow's IEEE 802.1Q class, the default
/// of its receiver feedback class.
fn validate_traffic(
    traffic: SourceTraffic,
    flow_kind: SourceFlowKind,
    priority: u8,
    scenario_text: &str,
    roce_keys: &mut Vec<RoceTrafficKey>,
) -> Result<TrafficKey, CompileError> {
    let packet_size_bytes = constant_packet_size_bytes(&traffic.pkt_size_dist, scenario_text)?;
    if traffic.roce.is_some() && !matches!(flow_kind, SourceFlowKind::Roce | SourceFlowKind::Dcqcn)
    {
        return Err(CompileError::Unsupported(
            "unsupported RoCE options on non-RoCE traffic; use `flow_type = \"RoCE\"`".to_owned(),
        ));
    }
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
            if traffic.roce.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported RoCE options on DCQCN traffic; use `flow_type = \"RoCE\"`"
                        .to_owned(),
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
            let key = dcqcn_traffic_key(&dcqcn, priority, scenario_text)?;
            (
                TrafficKind::Dcqcn(key),
                key.pacing_interval_ns,
                Termination::Bytes(size),
            )
        }
        SourceFlowKind::Roce => {
            if traffic.tcp.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported TCP options on RoCE traffic".to_owned(),
                ));
            }
            let dcqcn = traffic.dcqcn.ok_or_else(|| {
                CompileError::Invalid(
                    "RoCE traffic must provide `[flow.traffic.dcqcn]` or `[flow_set.traffic.dcqcn]` for its controller"
                        .to_owned(),
                )
            })?;
            if dcqcn.cnp_priority.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported `cnp_priority` on RoCE traffic; set the ACK and NACK class with `[flow.traffic.roce] feedback_priority`"
                        .to_owned(),
                ));
            }
            if dcqcn.cnp_interval_ns.is_some() {
                return Err(CompileError::Unsupported(
                    "unsupported `cnp_interval_ns` on RoCE traffic: a queue pair's receiver echoes ECN on its ACKs and NACKs and sends no CNP (P16)"
                        .to_owned(),
                ));
            }
            let roce = traffic.roce.ok_or_else(|| {
                CompileError::Invalid(
                    "RoCE traffic must provide `[flow.traffic.roce]` or `[flow_set.traffic.roce]` with `retransmit_timeout_ns`"
                        .to_owned(),
                )
            })?;
            let size = traffic.size.ok_or_else(|| {
                CompileError::Unsupported(
                    "unsupported duration-terminated RoCE traffic; the executor requires an exact byte `size`"
                        .to_owned(),
                )
            })?;
            if size == 0 {
                return Err(CompileError::Invalid(
                    "RoCE traffic `size` must be positive".to_owned(),
                ));
            }
            // An ACK echoes the size of the packet it answers in 32 bits (P16 ruling D6).
            if packet_size_bytes > u64::from(u32::MAX) {
                return Err(CompileError::Unsupported(
                    "unsupported RoCE packet size above 4294967295 bytes".to_owned(),
                ));
            }
            let feedback_priority = roce.feedback_priority.unwrap_or(priority);
            if feedback_priority > 7 {
                return Err(CompileError::Invalid(
                    "RoCE `feedback_priority` must be in IEEE 802.1Q range 0..=7".to_owned(),
                ));
            }
            let mut controller = dcqcn_traffic_key(&dcqcn, priority, scenario_text)?;
            controller.cnp_priority = feedback_priority;
            let retransmit_timeout_ns = roce.retransmit_timeout_ns.ok_or_else(|| {
                CompileError::Invalid(
                    "RoCE traffic must set `retransmit_timeout_ns`: a fixed timeout, or 0 for none (with no timeout a lost last packet stalls the queue pair for the rest of the run)"
                        .to_owned(),
                )
            })?;
            let ack_every_packets = roce.ack_every_packets.unwrap_or(1);
            if ack_every_packets == 0 {
                return Err(CompileError::Invalid(
                    "RoCE `ack_every_packets` must be positive".to_owned(),
                ));
            }
            let ack_size_bytes = roce.ack_size_bytes.unwrap_or(64);
            if ack_size_bytes == 0 {
                return Err(CompileError::Invalid(
                    "RoCE `ack_size_bytes` must be positive".to_owned(),
                ));
            }
            let duplicate_ack = roce.duplicate_ack.unwrap_or(true);
            if !duplicate_ack && retransmit_timeout_ns != 0 {
                return Err(CompileError::Unsupported(
                    "unsupported `duplicate_ack = false` with a retransmission timeout: a lost final ACK would never be repaired; it is allowed only with `retransmit_timeout_ns = 0`"
                        .to_owned(),
                ));
            }
            let window_bytes = roce.window_bytes.unwrap_or(0);
            let variable_window = roce.variable_window.unwrap_or(false);
            if variable_window && window_bytes == 0 {
                return Err(CompileError::Invalid(
                    "RoCE `variable_window = true` needs a window: set `window_bytes` above 0"
                        .to_owned(),
                ));
            }
            let key = RoceTrafficKey {
                dcqcn: controller,
                retransmit_timeout_ns,
                ack_every_packets,
                nack_interval_ns: roce.nack_interval_ns.unwrap_or(500_000),
                duplicate_ack,
                ack_size_bytes,
                window_bytes,
                variable_window,
            };
            let ordinal = roce_keys
                .iter()
                .position(|existing| *existing == key)
                .unwrap_or_else(|| {
                    roce_keys.push(key);
                    roce_keys.len() - 1
                });
            (
                TrafficKind::Roce(u64::try_from(ordinal).expect("key count fits u64")),
                controller.pacing_interval_ns,
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
    if let TopologyProfile::Rail(rail) = profile {
        if !matches!(propagation, PropagationModel::Undeclared) {
            return Err(CompileError::Unsupported(
                "unsupported `link.propagation_ns` or `link.propagation_tiers` on a SpectrumX \
                 topology; its link delays are `topology.spectrum_x.link_delay_ns`"
                    .to_owned(),
            ));
        }
        return Ok(LinkDelay::Rail {
            link_delay_ns: rail.link_delay_ns,
            nvlink_delay_ns: rail.nvlink_delay_ns,
        });
    }
    let tiers = match propagation {
        PropagationModel::Undeclared => return Ok(LinkDelay::Uniform(0)),
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

/// The link-rate model resolved against one built topology: `switch.port_rate` everywhere, or the
/// rail fabric's per-class rates (NIC links and ASW–PSW uplinks, P16 H2).
#[derive(Clone, Copy, Debug)]
enum LinkRate {
    Uniform(u64),
    Rail {
        nic_bps: u64,
        uplink_bps: u64,
        nvlink_bps: u64,
    },
}

impl LinkRate {
    fn resolve(rate_bps: Option<u64>, profile: TopologyProfile) -> Result<Self, CompileError> {
        match (profile, rate_bps) {
            (TopologyProfile::Rail(rail), None) => Ok(Self::Rail {
                nic_bps: rail.nic_rate_bps,
                uplink_bps: rail.uplink_rate_bps,
                nvlink_bps: rail.nvlink_rate_bps,
            }),
            (TopologyProfile::Rail(_), Some(_)) => Err(CompileError::Unsupported(
                "unsupported `switch.port_rate` on a SpectrumX topology; its link rates are \
                 `topology.spectrum_x.nic_rate_bps` and `uplink_rate_bps`"
                    .to_owned(),
            )),
            (_, Some(rate_bps)) => Ok(Self::Uniform(rate_bps)),
            (_, None) => parse_rate("", None).map(Self::Uniform),
        }
    }

    fn of(self, key: LinkKey) -> u64 {
        match self {
            Self::Uniform(rate_bps) => rate_bps,
            Self::Rail {
                nic_bps,
                uplink_bps,
                nvlink_bps,
            } => match (key.source, key.target) {
                (PhysicalNodeKey::Host(_), PhysicalNodeKey::Host(_)) => nvlink_bps,
                (PhysicalNodeKey::Host(_), _) | (_, PhysicalNodeKey::Host(_)) => nic_bps,
                (PhysicalNodeKey::Switch(_), PhysicalNodeKey::Switch(_)) => uplink_bps,
            },
        }
    }
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
    /// The rail fabric: NIC links and ASW–PSW links share SimAI's one `latency`; a same-server
    /// notify lane is the two NVLink hops through the server's NVSwitch (P16 H2).
    Rail {
        link_delay_ns: u64,
        nvlink_delay_ns: u64,
    },
}

impl LinkDelay {
    fn of(self, key: LinkKey) -> u64 {
        match self {
            Self::Uniform(propagation_ns) => propagation_ns,
            Self::Rail {
                link_delay_ns,
                nvlink_delay_ns,
            } => match (key.source, key.target) {
                (PhysicalNodeKey::Host(_), PhysicalNodeKey::Host(_)) => 2 * nvlink_delay_ns,
                _ => link_delay_ns,
            },
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
    let link_rate = LinkRate::resolve(model.rate_bps, profile)?;
    let roce_keys = model.roce_keys.clone();
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
    let CanonicalFlows {
        mut flows,
        collectives: collective_table,
    } = canonical_flows(
        model.explicit_flows,
        model.flow_sets,
        model.collectives,
        model.computes,
        &host_topology_ids,
        &hosts,
        model.seed,
    )?;
    // Same-server collective messages on the rail fabric cross NVLink, which Days models
    // delay-only: each lowers to a stage notify on a host-to-host lane (P16 H2, ruling H2-1).
    let notify = NotifyLowering::plan(&mut flows, profile)?;
    for &(source, target) in notify.lanes.keys() {
        link_keys.insert(LinkKey {
            source: PhysicalNodeKey::Host(source),
            target: PhysicalNodeKey::Host(target),
        });
    }
    let ids = StableIds::new(node_keys, link_keys.iter().copied())?;
    let flow_ids = dense_ids(flows.iter().map(|flow| flow.key.clone()))?;
    // Compute stages and stage notifies send nothing over the fabric and have no route; every
    // other flow is routed.
    let routed_flows = flows
        .iter()
        .enumerate()
        .filter(|(_, flow)| flow.compute.is_none() && flow.notify_delay_ns().is_none());
    // SimAI's ECMP picks the feedback path by its own hash, so its reverse routes are not the
    // reversed forward routes; every other policy reverses the forward switch path.
    let mut reverse_route_table = None;
    let route_table = match model.routing {
        RoutingPolicy::ShortestPath => compute_shortest_path_route_table_with(
            graph,
            routed_flows.clone().map(|(index, flow)| {
                (
                    index,
                    NodeIndex::new(host_attachment_switches[&flow.source] as usize),
                    NodeIndex::new(host_attachment_switches[&flow.target] as usize),
                )
            }),
            route_workers,
        ),
        RoutingPolicy::SimAiEcmp => {
            let TopologyProfile::Rail(rail) = profile else {
                return Err(CompileError::Unsupported(
                    "unsupported `routing.policy = \"SimAiEcmp\"` on a topology that is not the \
                     SpectrumX rail fabric"
                        .to_owned(),
                ));
            };
            let (forward, reverse) = simai_ecmp_route_tables(
                rail,
                routed_flows.map(|(index, flow)| (index, flow.source, flow.target)),
            );
            reverse_route_table = Some(reverse);
            Ok(forward)
        }
        RoutingPolicy::FatTreeEcmp => compute_fat_tree_ecmp_route_table(
            graph,
            routed_flows.map(|(index, flow)| EcmpFlow {
                key: index,
                source_switch: NodeIndex::new(host_attachment_switches[&flow.source] as usize),
                target_switch: NodeIndex::new(host_attachment_switches[&flow.target] as usize),
                // The hash stands in for a switch's header hash. It is taken from the flow's own
                // semantic key, so it is a pure function of the scenario text.
                flow_hash: generator_seed(
                    model.seed ^ 0x4543_4d50_5f48_4153,
                    &flow.key,
                    &collective_table,
                    &roce_keys,
                ),
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
            if flow.compute.is_some() || flow.notify_delay_ns().is_some() {
                return FlowDescriptor {
                    id: FlowId(flow_ids[&flow.key]),
                    source: ids.node(LpKey::Host(flow.source)),
                    target: ids.node(LpKey::Host(flow.target)),
                    priority: flow.priority,
                    feedback_priority: flow.priority,
                    route: Vec::new(),
                    reverse_route: Vec::new(),
                };
            }
            let switch_path = &route_table[&index];
            let reverse_switch_path = match &reverse_route_table {
                Some(reverse) => reverse[&index].clone(),
                None => switch_path.iter().rev().copied().collect::<Vec<_>>(),
            };
            FlowDescriptor {
                id: FlowId(flow_ids[&flow.key]),
                source: ids.node(LpKey::Host(flow.source)),
                target: ids.node(LpKey::Host(flow.target)),
                priority: flow.priority,
                feedback_priority: flow.traffic.feedback_priority(flow.priority, &roce_keys),
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

    // Each host's generator table and stage table.
    let mut generators_by_source = BTreeMap::<LpKey, HostTables>::new();
    let mut payload_sequences = BTreeMap::<LpKey, u64>::new();
    let mut initial_packets = Vec::with_capacity(flows.len());
    let mut initial_event_inputs = Vec::<(LpKey, u64, FlowId, PayloadId, EventKind)>::new();
    for (flow, descriptor) in flows.iter().zip(&flow_descriptors) {
        let source = LpKey::Host(flow.source);
        if let Some(delay_ns) = flow.notify_delay_ns() {
            let stage = flow
                .collective
                .as_deref()
                .expect("a stage notify carries a collective stage");
            let lane_ns = notify.lanes[&(flow.source, flow.target)];
            // The sender's timer runs `delay - lane`; its notify then crosses the lane in `lane`.
            let lead_ns = delay_ns - lane_ns;
            let root = stage.local_predecessor_complete && stage.inbound_predecessor_complete;
            let next_emission = if !root {
                ScheduledEmission {
                    status: GeneratorStatus::Blocked,
                    departure_time_ns: 0,
                    payload: PayloadId(0),
                }
            } else if lead_ns > model.stop_time_ns {
                ScheduledEmission {
                    status: GeneratorStatus::Stopped,
                    departure_time_ns: lead_ns,
                    payload: PayloadId(0),
                }
            } else {
                // A root message starts at time zero: its notify names the sender's timer and
                // then crosses to the target.
                let sequence = payload_sequences.entry(source).or_default();
                let payload = allocate_payload_id(
                    ids.node(source),
                    node_count,
                    *sequence,
                    "stage notify payload sequence",
                )?;
                *sequence = sequence.checked_add(1).ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "stage notify payload sequence overflow at {source:?}"
                    ))
                })?;
                initial_packets.push(PacketDescriptor {
                    id: payload,
                    flow: descriptor.id,
                    size_bytes: stage.chunk_bytes,
                    ecn_marked: false,
                    kind: PacketKind::StageNotify,
                });
                initial_event_inputs.push((
                    source,
                    lead_ns,
                    descriptor.id,
                    payload,
                    EventKind::PacingTimer,
                ));
                ScheduledEmission {
                    status: GeneratorStatus::Scheduled,
                    departure_time_ns: lead_ns,
                    payload,
                }
            };
            let (identity, dependencies) = collective_stage_record(stage, &flow_ids);
            generators_by_source.entry(source).or_default().push((
                FlowGeneratorState {
                    flow: descriptor.id,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission,
                    rng_state: generator_seed(model.seed, &flow.key, &collective_table, &roce_keys),
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        // A stage notify's lane latency (the image's one record of it per message).
                        first_departure_ns: lane_ns,
                        interval_ns: lead_ns,
                        packet_size_bytes: stage.chunk_bytes,
                        termination: GeneratorTermination::Bytes(stage.chunk_bytes),
                    }),
                },
                Some(CollectiveStage {
                    role: StageRole::Collective(identity),
                    dependencies,
                    activated: root,
                }),
            ));
            continue;
        }
        if let Some(compute) = &flow.compute {
            let root = compute.local_predecessor.is_none() && compute.inbound_predecessor.is_none();
            let next_emission = if !root {
                ScheduledEmission {
                    status: GeneratorStatus::Blocked,
                    departure_time_ns: 0,
                    payload: PayloadId(0),
                }
            } else if compute.duration_ns > model.stop_time_ns {
                ScheduledEmission {
                    status: GeneratorStatus::Stopped,
                    departure_time_ns: compute.duration_ns,
                    payload: PayloadId(0),
                }
            } else {
                // A root compute interval starts at time zero; its zero-byte token names the
                // timer event that completes it and is never enqueued or transmitted.
                let sequence = payload_sequences.entry(source).or_default();
                let payload = allocate_payload_id(
                    ids.node(source),
                    node_count,
                    *sequence,
                    "compute timer payload sequence",
                )?;
                *sequence = sequence.checked_add(1).ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "compute timer payload sequence overflow at {source:?}"
                    ))
                })?;
                initial_packets.push(PacketDescriptor {
                    id: payload,
                    flow: descriptor.id,
                    size_bytes: 0,
                    ecn_marked: false,
                    kind: PacketKind::Data,
                });
                initial_event_inputs.push((
                    source,
                    compute.duration_ns,
                    descriptor.id,
                    payload,
                    EventKind::PacingTimer,
                ));
                ScheduledEmission {
                    status: GeneratorStatus::Scheduled,
                    departure_time_ns: compute.duration_ns,
                    payload,
                }
            };
            let stage_flow = |key: &FlowKey| FlowId(flow_ids[key]);
            generators_by_source.entry(source).or_default().push((
                FlowGeneratorState {
                    flow: descriptor.id,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission,
                    rng_state: generator_seed(model.seed, &flow.key, &collective_table, &roce_keys),
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Constant(ConstantGenerator {
                        first_departure_ns: 0,
                        interval_ns: compute.duration_ns,
                        packet_size_bytes: 0,
                        termination: GeneratorTermination::Bytes(0),
                    }),
                },
                Some(CollectiveStage {
                    role: StageRole::Compute(ComputeStage {
                        compute_id: compute.compute_id,
                        group_size: compute.group_size,
                        rank: compute.rank,
                        duration_ns: compute.duration_ns,
                    }),
                    dependencies: StageDependencies {
                        local_predecessor: compute.local_predecessor.as_ref().map(stage_flow),
                        inbound_predecessor: compute.inbound_predecessor.as_ref().map(stage_flow),
                        inbound_predecessor_bytes: compute.inbound_predecessor_bytes,
                        local_predecessor_complete: compute.local_predecessor.is_none(),
                        inbound_predecessor_complete: compute.inbound_predecessor.is_none(),
                        inbound_bytes_received: 0,
                    },
                    activated: root,
                }),
            ));
            continue;
        }
        let emission_count = packet_count(&flow.traffic);
        // A RoCE queue pair's full key.
        let roce = match flow.traffic.kind {
            TrafficKind::Roce(ordinal) => Some(roce_key(&roce_keys, ordinal)),
            _ => None,
        };
        let collective_ready = flow.collective.as_ref().is_none_or(|stage| {
            stage.local_predecessor_complete && stage.inbound_predecessor_complete
        });
        // A RoCE stage that its prerequisites have not released holds its anchors at zero (ruling
        // C5); its release re-anchors the pacer and the controller at the release instant.
        let gated_roce = roce.is_some() && !collective_ready;
        let anchor_ns = if gated_roce {
            0
        } else {
            flow.traffic.initial_delay_ns
        };
        let next_emission = if emission_count == 0 {
            ScheduledEmission {
                status: GeneratorStatus::Finished,
                departure_time_ns: 0,
                payload: PayloadId(0),
            }
        } else if gated_roce {
            // Ruling C1: a gated RoCE stage's pacing token is allocated here, as a released
            // pair's is, and stays resident with no event, so the CPU executor imports and pins
            // it like any queue pair's. (The Mellanox-form controller owns no token, P16.)
            let sequence = payload_sequences.entry(source).or_default();
            let token = allocate_payload_id(
                ids.node(source),
                node_count,
                *sequence,
                "RoCE stage token payload sequence",
            )?;
            *sequence = sequence.checked_add(1).ok_or_else(|| {
                CompileError::Invalid(format!(
                    "RoCE stage token payload sequence overflow at {source:?}"
                ))
            })?;
            initial_packets.push(PacketDescriptor {
                id: token,
                flow: descriptor.id,
                size_bytes: 0,
                ecn_marked: false,
                kind: PacketKind::RocePacingTimer,
            });
            ScheduledEmission {
                status: GeneratorStatus::Blocked,
                departure_time_ns: 0,
                payload: token,
            }
        } else if !collective_ready {
            ScheduledEmission {
                status: GeneratorStatus::Blocked,
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
                // A queue pair's first payload is its zero-byte pacing token: its data packets
                // are created when the pacer sends them.
                (TrafficKind::Roce(_), _) => 0,
                (TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_), Termination::Bytes(total_bytes)) => {
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
                TrafficKind::Roce(_) => PacketKind::RocePacingTimer,
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
                if matches!(
                    flow.traffic.kind,
                    TrafficKind::Dcqcn(_) | TrafficKind::Roce(_)
                ) {
                    EventKind::PacingTimer
                } else {
                    EventKind::PacketArrival
                },
            ));
            // A DCQCN flow's or queue pair's status predicts its first tick: it sends iff one
            // tick of credit covers the first packet (the validator's next-tick rule).
            let paced_key = match flow.traffic.kind {
                TrafficKind::Dcqcn(config) => Some(config),
                _ => roce.map(|roce| roce.dcqcn),
            };
            ScheduledEmission {
                status: match (paced_key, &flow.traffic.termination) {
                    (Some(key), Termination::Bytes(total_bytes)) => {
                        let first_packet = flow.traffic.packet_size_bytes.min(*total_bytes);
                        let tick_credit =
                            u128::from(key.initial_rate_bps) * u128::from(key.pacing_interval_ns);
                        let cost = u128::from(first_packet) * 8 * 1_000_000_000;
                        if tick_credit >= cost {
                            GeneratorStatus::Scheduled
                        } else {
                            GeneratorStatus::Blocked
                        }
                    }
                    _ => GeneratorStatus::Scheduled,
                },
                departure_time_ns: flow.traffic.initial_delay_ns,
                payload,
            }
        };
        let collective_stage = flow
            .collective
            .as_deref()
            .map(|stage| collective_stage_record(stage, &flow_ids));
        generators_by_source.entry(source).or_default().push((
            FlowGeneratorState {
                // Every collective stage is a TCP or RoCE generator whose dependencies live in this record;
                // compute stages build theirs in the compute branch above.
                flow: descriptor.id,
                packets_emitted: 0,
                bytes_emitted: 0,
                next_emission,
                rng_state: generator_seed(model.seed, &flow.key, &collective_table, &roce_keys),
                feedback: GeneratorFeedbackState {
                    arrivals: 0,
                    outstanding_bytes: 0,
                    unacknowledged_bytes: 0,
                },
                kind: {
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
                            let controller = lowered_dcqcn_controller(config);
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
                                cnp_size_bytes: 64,
                            })
                        }
                        TrafficKind::Roce(_) => {
                            let roce = roce.expect("a RoCE flow carries its key");
                            let Termination::Bytes(total_bytes) = flow.traffic.termination else {
                                unreachable!("RoCE validation requires byte termination")
                            };
                            FlowGeneratorKind::Roce(RoceGenerator {
                                pacer: RocePacer {
                                    first_pacing_time_ns: anchor_ns,
                                    pacing_interval_ns: roce.dcqcn.pacing_interval_ns,
                                    mtu_bytes: flow.traffic.packet_size_bytes,
                                    total_bytes,
                                    credit_quanta: 0,
                                },
                                controller: lowered_dcqcn_controller(roce.dcqcn),
                                pacing_timer_payload: next_emission.payload,
                                next_psn: 0,
                                snd_una: 0,
                                rto_deadline_ns: 0,
                                rto_ns: roce.retransmit_timeout_ns,
                                pacer_armed: !gated_roce,
                                window_bytes: roce.window_bytes,
                                variable_window: roce.variable_window,
                                window_parked: false,
                            })
                        }
                    }
                },
            },
            collective_stage.map(|(identity, dependencies)| CollectiveStage {
                role: StageRole::Collective(identity),
                dependencies,
                activated: collective_ready && emission_count != 0,
            }),
        ));
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
    // Every receiver with its target, then one exact-length slice per target host: one
    // allocation per host that receives queue pairs.
    let mut roce_receivers = Vec::<(LpKey, RoceReceiverState)>::new();
    for (flow, descriptor) in flows.iter().zip(&flow_descriptors) {
        if flow.notify_delay_ns().is_some() {
            // A stage notify needs no receiver state: its one arrival carries the whole chunk.
            continue;
        }
        if let TrafficKind::Roce(ordinal) = flow.traffic.kind {
            let roce = roce_key(&roce_keys, ordinal);
            let Termination::Bytes(total_bytes) = flow.traffic.termination else {
                unreachable!("RoCE validation requires byte termination")
            };
            roce_receivers.push((
                LpKey::Host(flow.target),
                RoceReceiverState {
                    flow: descriptor.id,
                    total_bytes,
                    expected_psn: 0,
                    ack_every_packets: roce.ack_every_packets,
                    packets_since_ack: 0,
                    ack_size_bytes: roce.ack_size_bytes,
                    nack_interval_ns: roce.nack_interval_ns,
                    last_nack: None,
                    duplicate_ack: roce.duplicate_ack,
                },
            ));
        }
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

    // Stable: each target keeps its receivers in canonical `FlowId` order.
    roce_receivers.sort_by_key(|(target, _)| *target);
    let mut roce_receivers_by_target = BTreeMap::<LpKey, Box<[RoceReceiverState]>>::new();
    for group in roce_receivers.chunk_by(|left, right| left.0 == right.0) {
        roce_receivers_by_target.insert(
            group[0].0,
            group.iter().map(|(_, receiver)| *receiver).collect(),
        );
    }
    let mut host_states = host_topology_ids
        .iter()
        .map(|host| {
            let node_key = LpKey::Host(*host);
            let switch = host_attachment_switches[host];
            let egress_key = LinkKey {
                source: PhysicalNodeKey::Host(*host),
                target: PhysicalNodeKey::Switch(switch),
            };
            let HostTables { generators, stages } =
                generators_by_source.remove(&node_key).unwrap_or_default();
            Ok(HostState {
                egress_link: ids.link(egress_key),
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators,
                stages,
                tcp_receivers: tcp_receivers_by_target
                    .remove(&node_key)
                    .unwrap_or_default(),
                dcqcn_receivers: dcqcn_receivers_by_target
                    .remove(&node_key)
                    .unwrap_or_default(),
                // One allocation on a host that receives queue pairs, none elsewhere.
                roce_receivers: roce_receivers_by_target.remove(&node_key),
                pfc: None,
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
            // P16 H2: an ECN row per egress link rate, as SimAI keys its ECN rows by port rate.
            let drop_mark = match (&model.ecn_by_rate, model.drop_mark) {
                (Some(rows), DropMarkPolicy::EcnThreshold(policy)) => {
                    let rate_bps = link_rate.of(egress);
                    let threshold = *rows.get(&rate_bps).ok_or_else(|| {
                        CompileError::Invalid(format!(
                            "`switch.ecn_by_rate` has no row for a {rate_bps} b/s egress link"
                        ))
                    })?;
                    DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
                        threshold,
                        ..policy
                    })
                }
                (_, drop_mark) => drop_mark,
            };
            Ok(SwitchState {
                physical_switch: switch,
                queues: vec![SwitchQueueState {
                    egress_link: Some(ids.link(egress)),
                    scheduler: model.scheduler.clone(),
                    queue_capacity_packets: model.queue_capacity_packets,
                    drop_mark,
                    pfc: None,
                    queue: VecDeque::new(),
                    in_service: None,
                    tx_ready_pending: false,
                }],
                next_origin_seq: 0,
                arrived_packets: 0,
                dropped_packets: 0,
                departed_packets: 0,
            })
        })
        .collect::<Result<_, CompileError>>()?;
    let links = ids
        .links()
        .map(|(key, id)| LinkDescriptor {
            id,
            source: ids.node(LpKey::for_link_source(key)),
            target: ids.node(LpKey::for_link_target(key)),
            rate_bps: link_rate.of(key),
            propagation_ns: link_delay.of(key),
        })
        .collect::<Vec<_>>();
    let mut channel_keys = BTreeSet::<(LinkId, NodeId)>::new();
    let mut min_packet_size_by_link = BTreeMap::<LinkId, u64>::new();
    for (flow, input) in flow_descriptors.iter().zip(&flows) {
        if packet_count(&input.traffic) == 0 || input.notify_delay_ns().is_some() {
            continue;
        }
        let data_min_size = match input.traffic.kind {
            // Both controllers may fill the exact remaining congestion-window bytes, so even a
            // byte-aligned total/MSS pair can legally produce a one-byte intermediate segment.
            TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_) => 1,
            // A queue pair's packets are a full MTU or the short last one, retransmissions
            // included, so its channels keep the exact bound.
            TrafficKind::Roce(_) => {
                let Termination::Bytes(total_bytes) = input.traffic.termination else {
                    unreachable!("RoCE validation requires byte termination")
                };
                match total_bytes % input.traffic.packet_size_bytes {
                    0 => input.traffic.packet_size_bytes,
                    tail => tail,
                }
            }
            TrafficKind::Constant => input.traffic.packet_size_bytes,
        };
        let mut routed_packets = vec![(flow.route.as_slice(), flow.target, data_min_size, "data")];
        if matches!(input.traffic.kind, TrafficKind::Tcp(_)) {
            routed_packets.push((flow.reverse_route.as_slice(), flow.source, 40, "ACK"));
        }
        if matches!(input.traffic.kind, TrafficKind::Dcqcn(_)) {
            routed_packets.push((flow.reverse_route.as_slice(), flow.source, 64, "CNP"));
        }
        if let TrafficKind::Roce(ordinal) = input.traffic.kind {
            let ack_size_bytes = roce_key(&roce_keys, ordinal).ack_size_bytes;
            routed_packets.push((
                flow.reverse_route.as_slice(),
                flow.source,
                ack_size_bytes.min(64),
                "ACK, NACK or CNP",
            ));
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
    // One lane per ordered host pair with same-server messages (P16 H2): the notify crosses it in
    // exactly its latency, out of band, as a PFC frame crosses its control lane.
    for (&(source, target), &lane_ns) in &notify.lanes {
        channels.push(RemoteChannel {
            source: ids.node(LpKey::Host(source)),
            target: ids.node(LpKey::Host(target)),
            link: ids.link(LinkKey {
                source: PhysicalNodeKey::Host(source),
                target: PhysicalNodeKey::Host(target),
            }),
            event_kind: EventKind::RemoteArrival,
            min_delay_ns: lane_ns,
        });
    }

    if let Some(pfc) = &model.pfc {
        let mut monitored_paths = BTreeSet::<(LinkId, NodeId)>::new();
        let mut max_frame_by_link_priority = BTreeMap::<(LinkId, usize), u64>::new();
        for (descriptor, input) in flow_descriptors.iter().zip(&flows) {
            // Data rides the flow's class along its route; receiver feedback rides the feedback
            // class (P15) along the reverse route.
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
            }
            let feedback_priority = usize::from(descriptor.feedback_priority);
            // A TCP ACK, a DCQCN CNP, or a RoCE ACK, NACK or CNP, at its largest.
            let feedback_size = match input.traffic.kind {
                TrafficKind::Tcp(_) => Some(40),
                TrafficKind::Dcqcn(_) => Some(64),
                TrafficKind::Roce(ordinal) => {
                    Some(roce_key(&roce_keys, ordinal).ack_size_bytes.max(64))
                }
                TrafficKind::Constant => None,
            };
            if let Some(feedback_size) = feedback_size.filter(|_| pfc.xoff[feedback_priority] != 0)
            {
                for link_id in &descriptor.reverse_route {
                    max_frame_by_link_priority
                        .entry((*link_id, feedback_priority))
                        .and_modify(|maximum| *maximum = (*maximum).max(feedback_size))
                        .or_insert(feedback_size);
                }
            }
            // A switch egress LP monitors each controlled link that feeds it: a switch-to-switch
            // link, and with `host_links` the host-to-switch link that starts a route.
            let monitored = |controlled: LinkDescriptor, downstream: NodeId| {
                nodes[downstream.0 as usize].kind == NodeKind::Switch
                    && match nodes[controlled.source.0 as usize].kind {
                        NodeKind::Switch => true,
                        NodeKind::Host => pfc.host_links,
                    }
            };
            for pair in descriptor.route.windows(2) {
                let controlled = links[pair[0].0 as usize];
                let downstream = links[pair[1].0 as usize].source;
                if monitored(controlled, downstream) {
                    monitored_paths.insert((controlled.id, downstream));
                }
            }
            if matches!(
                input.traffic.kind,
                TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_) | TrafficKind::Roce(_)
            ) {
                for pair in descriptor.reverse_route.windows(2) {
                    let controlled = links[pair[0].0 as usize];
                    let downstream = links[pair[1].0 as usize].source;
                    if monitored(controlled, downstream) {
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
            let upstream = nodes[controlled.source.0 as usize];
            let downstream_physical =
                switch_states[nodes[downstream.0 as usize].state_slot as usize].physical_switch;
            // The reverse physical link: downstream switch to the upstream switch, or to the host.
            let reverse = links
                .iter()
                .copied()
                .find(|candidate| {
                    let source = nodes[candidate.source.0 as usize];
                    let target = nodes[candidate.target.0 as usize];
                    source.kind == NodeKind::Switch
                        && switch_states[source.state_slot as usize].physical_switch
                            == downstream_physical
                        && match upstream.kind {
                            NodeKind::Switch => {
                                target.kind == NodeKind::Switch
                                    && switch_states[target.state_slot as usize].physical_switch
                                        == switch_states[upstream.state_slot as usize]
                                            .physical_switch
                            }
                            NodeKind::Host => target.id == upstream.id,
                        }
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

            let upstream_slot = upstream.state_slot as usize;
            match upstream.kind {
                NodeKind::Switch => {
                    let upstream_queue = switch_states[upstream_slot]
                        .queues
                        .iter_mut()
                        .find(|queue| queue.egress_link == Some(controlled_id))
                        .expect("lowered switch egress owns the controlled link");
                    upstream_queue
                        .pfc
                        .get_or_insert_with(PfcQueueState::default);
                }
                NodeKind::Host => {
                    host_states[upstream_slot]
                        .pfc
                        .get_or_insert_with(Box::default);
                }
            }

            let (xoff_threshold_bytes, xon_threshold_bytes, buffer_capacity_bytes) =
                pfc.thresholds(profile, downstream_physical, controlled.rate_bps)?;
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
                    buffer_capacity_bytes,
                    max_frame_bytes,
                    xoff_threshold_bytes,
                    xon_threshold_bytes,
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

/// SimAI's ECMP on the rail fabric (P16 H2, ruling H2-6): each routed flow is one SimAI message.
///
/// Message `k` between an ordered host pair, in canonical flow order, takes source port
/// `10000 + k` (SimAI's per-pair counter, wrapping at 2^16); the source ASW hashes the data tuple
/// to pick its PSW, and the target ASW hashes the swapped tuple for the feedback path
/// ([`RailProfile::data_psw`], [`RailProfile::feedback_psw`]). GPUs sharing an ASW cross no PSW.
/// The ordinal stands in for SimAI's run-time issue order (statistically equivalent, ruled; the
/// canonical order is the stage keys' order, which the collective lowering fixes).
fn simai_ecmp_route_tables(
    rail: RailProfile,
    flows: impl Iterator<Item = (usize, u64, u64)>,
) -> (
    BTreeMap<usize, Vec<NodeIndex>>,
    BTreeMap<usize, Vec<NodeIndex>>,
) {
    let mut ordinals = BTreeMap::<(u64, u64), u64>::new();
    let mut forward = BTreeMap::new();
    let mut reverse = BTreeMap::new();
    for (index, source, target) in flows {
        let ordinal = ordinals.entry((source, target)).or_default();
        let sport = RailProfile::simai_sport(*ordinal);
        *ordinal += 1;
        // Rail host identities are GPU ids below `rail.gpus`, a `u32`.
        let (source, target) = (source as u32, target as u32);
        let (source_asw, target_asw) = (rail.asw_of(source), rail.asw_of(target));
        let node = |index: u32| NodeIndex::new(index as usize);
        let path = |from: u32, psw: Option<u32>, to: u32| match psw {
            None => vec![node(from)],
            Some(psw) => vec![node(from), node(rail.psw_index(psw)), node(to)],
        };
        forward.insert(
            index,
            path(source_asw, rail.data_psw(source, target, sport), target_asw),
        );
        reverse.insert(
            index,
            path(
                target_asw,
                rail.feedback_psw(source, target, sport),
                source_asw,
            ),
        );
    }
    (forward, reverse)
}

/// The stage identity and pristine prerequisite state of one collective stage.
fn collective_stage_record(
    stage: &CollectiveStageInput,
    flow_ids: &BTreeMap<FlowKey, u64>,
) -> (CollectiveStageIdentity, StageDependencies) {
    let stage_flow = |key: &FlowKey| FlowId(flow_ids[key]);
    (
        CollectiveStageIdentity {
            collective_id: stage.collective_id,
            algorithm: stage.algorithm,
            topology_level: 0,
            topology_group: 0,
            group_size: stage.group_size,
            declared_total_bytes: stage.declared_total_bytes,
            rank: stage.position.rank,
            phase: stage.position.phase,
            step: stage.position.step,
            chunk_policy: CollectiveChunkPolicy::EqualRemainderLast,
            channel_policy: CollectiveChannelPolicy::RingNext,
            chunk_offset_bytes: stage.chunk_offset_bytes,
            chunk_bytes: stage.chunk_bytes,
        },
        StageDependencies {
            local_predecessor: stage.local_predecessor.as_ref().map(stage_flow),
            inbound_predecessor: stage.inbound_predecessor.as_ref().map(stage_flow),
            inbound_predecessor_bytes: stage.inbound_predecessor_bytes,
            local_predecessor_complete: stage.local_predecessor_complete,
            inbound_predecessor_complete: stage.inbound_predecessor_complete,
            inbound_bytes_received: 0,
        },
    )
}

/// The same-server messages of a rail image and their host-to-host notify lanes (P16 H2).
///
/// A collective stage whose two ranks share a server crosses NVLink, which Days models delay-only
/// (ruling H2-1): the stage lowers to a stage notify. Its delay `d` is
/// [`ServerLocality::nvlink_message_delay_ns`] of its chunk. Every message on one ordered host pair
/// shares that pair's lane, whose latency is `min d - 1` over the pair's messages: the sender's
/// timer runs `d - lane >= 1` and the notify crosses in `lane`, so arrivals on a lane follow the
/// sender's timer order and the lane bounds the safe horizon by its slowest-to-start message.
struct NotifyLowering {
    /// Lane latency per ordered (source, target) host pair.
    lanes: BTreeMap<(u64, u64), u64>,
}

impl NotifyLowering {
    /// Marks each same-server collective stage with its message delay (its sidecar's
    /// `notify_delay_ns`) and collects the lanes. Off the rail fabric it touches nothing.
    fn plan(flows: &mut [FlowInput], profile: TopologyProfile) -> Result<Self, CompileError> {
        let TopologyProfile::Rail(rail) = profile else {
            return Ok(Self {
                lanes: BTreeMap::new(),
            });
        };
        let locality = ServerLocality::new(rail);
        let mut lanes = BTreeMap::<(u64, u64), u64>::new();
        for flow in flows.iter_mut() {
            let (source, target, packet_size) =
                (flow.source, flow.target, flow.traffic.packet_size_bytes);
            let Some(stage) = flow.collective.as_deref_mut() else {
                continue;
            };
            if !locality.same_server(source, target) {
                continue;
            }
            // Today's expansions send one message per rank and step, so the sender's NVLink port
            // carries this chunk alone; a multi-channel expansion passes the port's step bytes.
            let delay = locality
                .nvlink_message_delay_ns(stage.chunk_bytes, stage.chunk_bytes, packet_size)
                .ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "NVLink delay of a {}-byte message from host {source} to host {target} \
                         overflows",
                        stage.chunk_bytes
                    ))
                })?;
            stage.notify_delay_ns = Some(delay);
            let lane = delay - 1;
            lanes
                .entry((source, target))
                .and_modify(|current| *current = (*current).min(lane))
                .or_insert(lane);
        }
        Ok(Self { lanes })
    }
}

/// The canonically ordered flow inputs and the collective keys their stage keys index.
struct CanonicalFlows {
    flows: Vec<FlowInput>,
    /// The normalized collective keys, sorted and distinct: `FlowKey::CollectiveStage::collective`
    /// indexes this table.
    collectives: Vec<CollectiveKey>,
}

/// Ordinal of `semantic` in the sorted distinct `table`.
fn collective_ordinal(table: &[CollectiveKey], semantic: &CollectiveKey) -> u64 {
    let index = table
        .binary_search(semantic)
        .expect("every normalized collective key is in the distinct-key table");
    u64::try_from(index).expect("the collective table length fits u64")
}

fn canonical_flows(
    mut explicit: Vec<ExplicitFlowKey>,
    mut flow_sets: Vec<FlowSetKey>,
    mut collectives: Vec<CollectiveKey>,
    mut computes: Vec<ComputeKey>,
    hosts: &BTreeSet<u64>,
    host_attachments: &HostAttachments,
    seed: u64,
) -> Result<CanonicalFlows, CompileError> {
    explicit.sort();
    flow_sets.sort();
    collectives.sort();
    computes.sort();

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
            compute: None,
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
                compute: None,
            });
        }
    }

    let mut normalized = Vec::with_capacity(collectives.len());
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

        normalized.push(semantic);
    }
    let collectives = normalized;
    for compute in &computes {
        if let Some(host) = compute.hosts.iter().find(|host| !hosts.contains(host)) {
            return Err(CompileError::Invalid(format!(
                "compute `{}` host {host} must be a configured host attachment",
                compute.name
            )));
        }
    }
    let groups = resolve_stage_groups(&collectives, &computes)?;

    // Normalization filled omitted `sources`/`sinks`, which can reorder keys, so the table is built
    // from the normalized keys: the flow sort below orders stages by exactly these keys.
    let collective_table = collectives
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut collective_duplicates = vec![0_u64; collective_table.len()];
    let mut next_collective_id = 0_u64;
    for semantic in &collectives {
        let collective = collective_ordinal(&collective_table, semantic);
        let duplicate = &mut collective_duplicates[collective as usize];
        let duplicate_ordinal = *duplicate;
        *duplicate = duplicate.checked_add(1).ok_or_else(|| {
            CompileError::Invalid("duplicate collective ordinal overflow".to_owned())
        })?;
        let collective_id = next_collective_id;
        next_collective_id = next_collective_id
            .checked_add(1)
            .ok_or_else(|| CompileError::Invalid("collective identity exceeds u64".to_owned()))?;
        let entry = semantic.after.as_ref().map(|name| match groups[name] {
            StageGroup::Compute(compute) => compute.clone(),
            StageGroup::Collective(_) => {
                unreachable!("group resolution admits compute entries only")
            }
        });
        expand_collective(
            &mut flows,
            semantic,
            collective,
            duplicate_ordinal,
            collective_id,
            entry.as_ref(),
        )?;
    }
    for (compute_id, compute) in computes.iter().enumerate() {
        expand_compute(
            &mut flows,
            compute,
            compute_id as u64,
            &groups,
            &collective_table,
        )?;
    }

    flows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(CanonicalFlows {
        flows,
        collectives: collective_table,
    })
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
    semantic: &CollectiveKey,
    collective: u64,
    duplicate_ordinal: u64,
    collective_id: u64,
    entry: Option<&ComputeKey>,
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
    let group_size = u32::try_from(n)
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
                let stage_key = |stage: CollectiveStagePosition| FlowKey::CollectiveStage {
                    collective,
                    duplicate_ordinal,
                    stage,
                };
                // A collective that follows a compute group gates each rank's root stage on that
                // rank's compute stage, the only predecessor a root can have.
                let entry_predecessor = entry
                    .filter(|_| local_predecessor.is_none() && inbound_predecessor.is_none())
                    .map(|compute| FlowKey::ComputeStage {
                        semantic: compute.clone(),
                        rank,
                    });
                let local_predecessor_complete =
                    local_predecessor_complete && entry_predecessor.is_none();
                let local_predecessor = local_predecessor.map(stage_key).or(entry_predecessor);
                let inbound_predecessor = inbound_predecessor.map(stage_key);
                let mut traffic = semantic.traffic.clone();
                traffic.termination = Termination::Bytes(chunk_bytes);
                flows.push(FlowInput {
                    key: stage_key(position),
                    source: semantic.sources[rank_u64 as usize],
                    target: semantic.sinks[rank_u64 as usize],
                    priority: semantic.priority,
                    traffic,
                    collective: Some(Box::new(CollectiveStageInput {
                        collective_id,
                        algorithm: semantic.algorithm,
                        group_size,
                        declared_total_bytes: total_bytes,
                        position,
                        chunk_offset_bytes,
                        chunk_bytes,
                        local_predecessor,
                        inbound_predecessor,
                        inbound_predecessor_bytes: chunk_bytes,
                        local_predecessor_complete,
                        inbound_predecessor_complete,
                        notify_delay_ns: None,
                    })),
                    compute: None,
                });
            }
        }
    }
    Ok(())
}

/// A named stage group that `after` fields may reference.
#[derive(Clone, Copy)]
enum StageGroup<'a> {
    Collective(&'a CollectiveKey),
    Compute(&'a ComputeKey),
}

/// Resolves the provisional stage-group dependencies: every `after` names exactly one group, the
/// named group runs on the same hosts in the same rank order, and the dependencies are acyclic.
fn resolve_stage_groups<'a>(
    collectives: &'a [CollectiveKey],
    computes: &'a [ComputeKey],
) -> Result<BTreeMap<String, StageGroup<'a>>, CompileError> {
    let mut groups = BTreeMap::new();
    let named = collectives
        .iter()
        .filter_map(|collective| {
            collective
                .name
                .as_ref()
                .map(|name| (name, StageGroup::Collective(collective)))
        })
        .chain(
            computes
                .iter()
                .map(|compute| (&compute.name, StageGroup::Compute(compute))),
        );
    for (name, group) in named {
        if groups.insert(name.clone(), group).is_some() {
            return Err(CompileError::Invalid(format!(
                "stage group name `{name}` is used more than once"
            )));
        }
    }
    for collective in collectives {
        let Some(after) = &collective.after else {
            continue;
        };
        let label = collective.name.as_deref().unwrap_or("<unnamed>");
        match groups.get(after) {
            None => {
                return Err(CompileError::Invalid(format!(
                    "collective `{label}` depends on unknown stage group `{after}`"
                )));
            }
            Some(StageGroup::Collective(_)) => {
                return Err(CompileError::Unsupported(format!(
                    "unsupported collective `{label}` dependency on collective `{after}`; a collective may follow only a compute group"
                )));
            }
            Some(StageGroup::Compute(compute)) if compute.hosts != collective.sources => {
                return Err(CompileError::Invalid(format!(
                    "collective `{label}` ranks must equal the hosts of `{after}` in order"
                )));
            }
            Some(StageGroup::Compute(_)) => {}
        }
    }
    for compute in computes {
        let Some(after) = &compute.after else {
            continue;
        };
        let name = &compute.name;
        let hosts = match groups.get(after) {
            None => {
                return Err(CompileError::Invalid(format!(
                    "compute `{name}` depends on unknown stage group `{after}`"
                )));
            }
            Some(StageGroup::Compute(predecessor)) => &predecessor.hosts,
            Some(StageGroup::Collective(collective)) => {
                if collective.flow_count < 2 {
                    return Err(CompileError::Invalid(format!(
                        "compute `{name}` depends on collective `{after}`, which has no stages"
                    )));
                }
                &collective.sources
            }
        };
        if *hosts != compute.hosts {
            return Err(CompileError::Invalid(format!(
                "compute `{name}` hosts must equal the ranks of `{after}` in order"
            )));
        }
    }
    // Each group has at most one `after`, so a cycle is a revisited name on one chain.
    for start in groups.keys() {
        let mut seen = BTreeSet::new();
        let mut current = start.as_str();
        loop {
            if !seen.insert(current) {
                return Err(CompileError::Invalid(format!(
                    "stage group dependencies form a cycle through `{current}`"
                )));
            }
            let after = match groups[current] {
                StageGroup::Collective(collective) => collective.after.as_deref(),
                StageGroup::Compute(compute) => compute.after.as_deref(),
            };
            match after {
                Some(next) => current = next,
                None => break,
            }
        }
    }
    Ok(groups)
}

/// Expands one compute group into one timer-only stage per host.
///
/// After a compute group, rank r waits for that group's rank-r stage. After a collective, rank r
/// waits for its own final stage (local, acknowledged) and the previous rank's final stage
/// (inbound, delivered in order), which together complete the collective at rank r.
fn expand_compute(
    flows: &mut Vec<FlowInput>,
    compute: &ComputeKey,
    compute_id: u64,
    groups: &BTreeMap<String, StageGroup<'_>>,
    collective_table: &[CollectiveKey],
) -> Result<(), CompileError> {
    let group_size = u32::try_from(compute.hosts.len())
        .expect("compute validation bounded the group size by u32");
    let after = compute.after.as_ref().map(|name| groups[name]);
    let after_collective = match after {
        Some(StageGroup::Collective(collective)) => {
            Some(collective_ordinal(collective_table, collective))
        }
        Some(StageGroup::Compute(_)) | None => None,
    };
    for (rank, &host) in (0_u32..).zip(&compute.hosts) {
        let (local_predecessor, inbound_predecessor, inbound_predecessor_bytes) = match after {
            None => (None, None, 0),
            Some(StageGroup::Compute(predecessor)) => (
                Some(FlowKey::ComputeStage {
                    semantic: predecessor.clone(),
                    rank,
                }),
                None,
                0,
            ),
            Some(StageGroup::Collective(collective)) => {
                let n = collective.flow_count;
                let Termination::Bytes(total_bytes) = collective.traffic.termination else {
                    unreachable!("collective validation requires byte termination")
                };
                let final_step = u32::try_from(n - 1).expect("collective group fits u32");
                let previous_rank = u32::try_from((u64::from(rank) + n - 1) % n)
                    .expect("rank is below the u32 group size");
                // A named collective is unique by name, so its only instance has ordinal 0.
                let final_stage = |rank: u32| FlowKey::CollectiveStage {
                    collective: after_collective
                        .expect("a collective predecessor has a table ordinal"),
                    duplicate_ordinal: 0,
                    stage: CollectiveStagePosition {
                        phase: CollectivePhase::AllGather,
                        rank,
                        step: final_step,
                    },
                };
                let owner = collective_stage_owner(
                    collective.algorithm,
                    CollectivePhase::AllGather,
                    n,
                    u64::from(previous_rank),
                    u64::from(final_step),
                );
                (
                    Some(final_stage(rank)),
                    Some(final_stage(previous_rank)),
                    collective_chunk_bounds(total_bytes, n, owner).1,
                )
            }
        };
        flows.push(FlowInput {
            key: FlowKey::ComputeStage {
                semantic: compute.clone(),
                rank,
            },
            source: host,
            target: host,
            priority: 0,
            traffic: TrafficKey {
                initial_delay_ns: 0,
                interval_ns: compute.duration_ns,
                packet_size_bytes: 0,
                termination: Termination::Bytes(0),
                kind: TrafficKind::Constant,
            },
            collective: None,
            compute: Some(Box::new(ComputeStageInput {
                compute_id,
                group_size,
                rank,
                duration_ns: compute.duration_ns,
                local_predecessor,
                inbound_predecessor,
                inbound_predecessor_bytes,
            })),
        });
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

/// The exact Mellanox-form DCQCN reaction point of a DCQCN flow or RoCE queue pair: pristine, with
/// no timer started (its timers start at its first feedback, P16).
fn lowered_dcqcn_controller(config: DcqcnTrafficKey) -> DcqcnController {
    DcqcnController::new(dcqcn_controller_config(config))
        .expect("validated DCQCN controller configuration")
}

fn packet_count(traffic: &TrafficKey) -> u64 {
    if matches!(
        traffic.kind,
        TrafficKind::Tcp(_) | TrafficKind::Dcqcn(_) | TrafficKind::Roce(_)
    ) {
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

/// `collectives` is the table a `FlowKey::CollectiveStage` indexes; other keys never read it.
fn generator_seed(
    image_seed: u64,
    key: &FlowKey,
    collectives: &[CollectiveKey],
    roce_keys: &[RoceTrafficKey],
) -> u64 {
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
            state = mix_traffic_seed(state, &semantic.traffic, roce_keys);
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
            state = mix_traffic_seed(state, &semantic.traffic, roce_keys);
            state = mix_seed(state ^ duplicate_ordinal);
            state = mix_seed(state ^ member_ordinal);
            state = mix_seed(state ^ source);
            state = mix_seed(state ^ target);
        }
        FlowKey::CollectiveStage {
            collective,
            duplicate_ordinal,
            stage,
        } => {
            let semantic = &collectives[usize::try_from(*collective)
                .expect("a collective ordinal indexes the in-memory collective table")];
            state = mix_seed(state ^ 0x434f_4c4c_4543_5449);
            state = mix_seed(state ^ semantic.flow_count);
            state = mix_seed(state ^ duplicate_ordinal);
            state = mix_seed(state ^ u64::from(stage.phase as u8));
            state = mix_seed(state ^ u64::from(stage.rank));
            state = mix_seed(state ^ u64::from(stage.step));
            state = mix_traffic_seed(state, &semantic.traffic, roce_keys);
        }
        FlowKey::ComputeStage { semantic, rank } => {
            state = mix_seed(state ^ 0x434f_4d50_5554_4500);
            state = semantic
                .name
                .bytes()
                .fold(state, |state, byte| mix_seed(state ^ u64::from(byte)));
            state = mix_seed(state ^ semantic.duration_ns);
            state = mix_seed(state ^ u64::from(*rank));
        }
    }
    state
}

fn mix_traffic_seed(mut state: u64, traffic: &TrafficKey, roce_keys: &[RoceTrafficKey]) -> u64 {
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
            for value in dcqcn.seed_words() {
                state = mix_seed(state ^ value);
            }
            state
        }
        // The key's content, not its ordinal, so a queue pair's seed and ECMP route do not
        // depend on the other RoCE keys of the scenario.
        TrafficKind::Roce(ordinal) => {
            let roce = roce_key(roce_keys, ordinal);
            let dcqcn = roce.dcqcn;
            state = mix_seed(state ^ 0x524f_4345_5f51_5000);
            for value in dcqcn.seed_words().into_iter().chain([
                roce.retransmit_timeout_ns,
                roce.ack_every_packets,
                roce.nack_interval_ns,
                u64::from(roce.duplicate_ack),
                roce.ack_size_bytes,
            ]) {
                state = mix_seed(state ^ value);
            }
            // A window joins the key's content only when it is on, so queue pairs without one
            // keep the seeds and routes they had before windows existed (P16 ruling D7).
            if roce.window_bytes != 0 {
                for value in [roce.window_bytes, u64::from(roce.variable_window)] {
                    state = mix_seed(state ^ value);
                }
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
    use super::{FlowInput, parsed_decimal};

    /// `FlowInput` is held once per flow, sorted and moved through lowering, so its size is a
    /// per-flow memory and memory-movement cost: at the 262,144-flow frontier every 100 B is
    /// 26 MB. Before P14 (`main` at 948a0e9) it was 480 B, carrying the stage sidecar inline as an
    /// 80 B `Option<CollectiveStageInput>`. P14's collective and compute sidecars, inline and
    /// holding their predecessors' full `FlowKey`s by value, grew it to 1,224 B at 6cc395c
    /// (1,656 B before P14 coll keyed stages by ordinal) although most flows have neither sidecar.
    /// Each sidecar is now one boxed pointer, so a flow without stages pays 16 B for both:
    /// `FlowKey` 192 + source and target 16 + `TrafficKey` 144 + two sidecar pointers 16 +
    /// priority 1 = 369, padded to 376 B.
    ///
    /// Layout is the compiler's choice, so the bound is an upper bound on 64-bit targets.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn flow_input_carries_its_stage_sidecars_out_of_line() {
        const FLOW_INPUT_MAX_BYTES: usize = 376;
        let size = std::mem::size_of::<FlowInput>();
        assert!(
            size <= FLOW_INPUT_MAX_BYTES,
            "FlowInput grew to {size} B, above {FLOW_INPUT_MAX_BYTES} B: keep per-flow sidecars \
             boxed so flows without stages do not pay for them"
        );
    }

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

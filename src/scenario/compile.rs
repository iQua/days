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
    StageDependencies, StagePredecessors, StageRole, SwitchQueueState, SwitchState,
    TcpCongestionControl, TcpDataHeader, TcpGenerator, TcpReceiverState, event_phase, validate,
};
use num_bigint::BigUint;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::Deserialize;
use thiserror::Error;

use super::collective_shapes::{RoutingSkew, SeededAllToAll};
use super::ids::{IdError, LinkKey, LpKey, PhysicalNodeKey, StableIds, dense_ids};
use crate::topos::build::{
    HostAttachments, PairingPolicy, TopologyError, TopologyProfile,
    build_graph_with_profile_from_str,
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

/// Declares the scenario root ([`SourceConfig`]) together with [`LEGACY_ENGINE_ROOT_KEYS`], so
/// the legacy-engine keys the root accepts are written once.
macro_rules! scenario_root {
    ($($legacy:ident),* $(,)?) => {
        /// The legacy engine's root keys (`legacy/src/config.rs`, `LegacyConfig`) that Days AGO
        /// does not read: the root accepts each by its exact name and ignores it, because the
        /// two engines read one scenario file (user ruling, Oct 8, option 1). Any other unknown
        /// key is refused. `legacy`'s `the_days_ago_root_names_exactly_the_legacy_only_keys`
        /// test keeps the list equal to `LegacyConfig`'s keys that the Days AGO root does not
        /// read itself.
        pub const LEGACY_ENGINE_ROOT_KEYS: &[&str] = &[$(stringify!($legacy)),*];

        /// The scenario root. Unknown keys are refused (`deny_unknown_fields`); `topology`,
        /// `edges` and `hosts` are read by the topology builder, and the legacy-engine keys are
        /// accepted and ignored.
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SourceConfig {
            /// A `[workload]` table: present only in an AICB scenario, which the adapter
            /// rewrites before lowering, so the ordinary path refuses it.
            workload: Option<serde::de::IgnoredAny>,
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
            /// Read by the topology builder (`topos::build`).
            #[allow(dead_code)]
            topology: Option<serde::de::IgnoredAny>,
            #[allow(dead_code)]
            edges: Option<serde::de::IgnoredAny>,
            #[allow(dead_code)]
            hosts: Option<serde::de::IgnoredAny>,
            $(
                #[allow(dead_code)]
                $legacy: Option<serde::de::IgnoredAny>,
            )*
        }
    };
}

scenario_root!(
    ui_interval,
    threading,
    num_threads,
    hot_workers,
    concurrency_level,
    log_path,
    csv_logging,
    report_interval,
    mailbox_capacity,
    legacy_e5_metrics,
    model_host_attachment,
    app_source,
);

/// The keys the scenario root accepts: those Days AGO reads, `topology`, `edges` and `hosts`
/// (read by the topology builder), and [`LEGACY_ENGINE_ROOT_KEYS`].
pub fn scenario_root_keys() -> &'static [&'static str] {
    crate::utils::serde_fields::struct_fields::<SourceConfig>()
}

/// A parse error, naming the table of an unknown key: the header line the key sits under, or
/// the root table.
fn parse_error(content: &str, error: toml::de::Error) -> CompileError {
    match crate::utils::serde_fields::unknown_key_table(content, &error) {
        Some(table) => CompileError::Invalid(format!("{error}(in {table})")),
        None => CompileError::Parse(error),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
struct SourceLink {
    mode: Option<String>,
    pfc: Option<SourcePfc>,
    propagation_ns: Option<u64>,
    propagation_tiers: Option<SourcePropagationTiers>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePropagationTiers {
    host_to_edge_ns: u64,
    edge_to_aggregation_ns: u64,
    aggregation_to_core_ns: u64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
struct SourceCollective {
    /// Stage-group name that `after` fields may reference.
    name: Option<String>,
    /// Stage groups whose rank-r stages gate this collective's rank-r root stages: one name, or a
    /// list (at least one compute group, and possibly the previous collective of the stream).
    #[serde(default)]
    after: Option<AfterGroups>,
    /// The issue stream (SimAI's queue) the collective runs on at each rank, 0 by default: the
    /// stage-aware sizing charges each stream's widest operation once (ruling R11 (a)). A
    /// collective on another stream needs a `name`.
    stream: Option<u32>,
    /// Ring channels: each a ring order of the `sources` hosts (instead of `sinks`).
    channels: Option<Vec<Vec<u64>>>,
    /// The collective's position in its workload's issue order (ruling C2): it orders the
    /// collective's flows, and so SimAI's ECMP port ordinals, before key content.
    issue_ordinal: Option<u64>,
    /// `EqualRemainderLast` (the default for rings) or `UniformFloor`.
    chunk: Option<String>,
    /// An all-to-all's seeded per-pair sizes (`[collective.alltoall]`); uniform without it.
    alltoall: Option<Box<SourceAllToAll>>,
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

/// The seeded routing matrix of an imbalanced all-to-all (`collective_shapes::SeededAllToAll`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceAllToAll {
    seed: u64,
    #[serde(default)]
    matrix: u64,
    #[serde(default)]
    group: u64,
    #[serde(default)]
    transpose: bool,
    experts: u64,
    topk: u64,
    tokens: u64,
    bytes_per_copy: u64,
    /// `Zipf1` (the default) or `Uniform`.
    skew: Option<String>,
}

/// A delay-only compute stage group: one timer-only stage per listed host.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceCompute {
    name: String,
    hosts: Vec<u64>,
    duration_ns: u64,
    /// Stage groups (compute groups or collectives) that each host's stage waits for: one name,
    /// or a list of names whose stages it joins.
    #[serde(default)]
    after: Option<AfterGroups>,
    /// The issue stream the group runs on, 0 by default (see `SourceCollective::stream`).
    stream: Option<u32>,
}

/// One stage-group name, or several. One name is held without a list, so a group with a single
/// predecessor allocates nothing for it.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(untagged)]
enum AfterGroups {
    One(String),
    Many(Box<[String]>),
}

/// The names `after` lists, in order.
fn after_names(after: Option<&AfterGroups>) -> &[String] {
    match after {
        None => &[],
        Some(AfterGroups::One(name)) => std::slice::from_ref(name),
        Some(AfterGroups::Many(names)) => names,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
struct SourceTcp {
    cc_algorithm: String,
    #[serde(default)]
    ecn: bool,
    cubic: Option<SourceCubic>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    generator_seed(seed ^ 0x4543_4d50_5f48_4153, &key, &[], &[], &[])
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
    generator_seed(seed ^ 0x4543_4d50_5f48_4153, &key, &[], &[], &[])
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
    /// The collective's position in its workload's realized issue order (P16 ruling C2): the
    /// leading field, so canonical flow order, and with it SimAI's per-pair ECMP port ordinals
    /// (`simai_ecmp_route_tables`), follow issue order. `None` for every collective that does not
    /// set it, which sort first and keep their order. Held as `ordinal + 1` in a `NonZeroU32`, which
    /// fits the key's padding (`stage_group_keys_keep_their_size`); `Some` orders as the ordinal.
    issue_ordinal: Option<std::num::NonZeroU32>,
    algorithm: CollectiveAlgorithm,
    flow_count: u64,
    sources: Vec<u64>,
    sinks: Vec<u64>,
    priority: u8,
    traffic: TrafficKey,
    /// Stage-group identity; `None` for every unnamed collective, which keeps their order.
    name: Option<String>,
    after: Option<AfterGroups>,
    /// Channel rings or seeded sizes (P16 H1); `None`, holding nothing, for one ring in `sinks`
    /// order and a uniform all-to-all or send/recv, so an ordinary key stays the size it was.
    shape: Option<Box<CollectiveShapeKey>>,
    chunk: CollectiveChunkPolicy,
}

/// The parts of a collective key only channel rings and seeded all-to-alls have. Field order
/// keeps the keys' order as when they were inline (channels, then the chunk, then the sizes):
/// a seeded key is the only one with sizes and has the largest chunk policy.
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct CollectiveShapeKey {
    /// Ring channels as rank-index orders (`rank` is a position in `sources`).
    channels: Vec<Vec<u32>>,
    /// An all-to-all's seeded per-pair sizes.
    seeded: Option<SeededAllToAll>,
}

impl CollectiveKey {
    /// The ring channels; empty for one ring in `sinks` order, an all-to-all and a send/recv.
    fn channels(&self) -> &[Vec<u32>] {
        self.shape.as_ref().map_or(&[], |shape| &shape.channels)
    }

    /// An all-to-all's seeded per-pair sizes.
    fn seeded(&self) -> Option<&SeededAllToAll> {
        self.shape.as_ref().and_then(|shape| shape.seeded.as_ref())
    }
}

/// One delay-only compute stage group.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ComputeKey {
    name: String,
    hosts: Vec<u64>,
    duration_ns: u64,
    /// The stage groups each host's stage waits for, in the order the scenario names them.
    after: Option<AfterGroups>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CollectiveStagePosition {
    phase: CollectivePhase,
    /// The ring channel; 0 for one ring, an all-to-all and a send/recv.
    channel: u32,
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
    /// `compute` is the ordinal of the stage's [`ComputeKey`] among the scenario's sorted compute
    /// keys, which [`CanonicalFlows::computes`] holds; names are unique, so the mapping is
    /// order-isomorphic, as for collectives.
    ComputeStage { compute: u64, rank: u32 },
}

#[derive(Clone, Debug)]
struct CollectiveStageInput {
    collective_id: u64,
    algorithm: CollectiveAlgorithm,
    group_size: u32,
    declared_total_bytes: u64,
    position: CollectiveStagePosition,
    chunk_policy: CollectiveChunkPolicy,
    channel_policy: CollectiveChannelPolicy,
    chunk_offset_bytes: u64,
    chunk_bytes: u64,
    local: PredecessorKeys,
    inbound: PredecessorKeys,
    inbound_predecessor_bytes: u64,
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

/// The predecessors of one kind of a stage, by flow key: inline when there is at most one, so a
/// stage that is not a join allocates nothing for them.
#[derive(Clone, Debug, Default)]
enum PredecessorKeys {
    #[default]
    None,
    One(FlowKey),
    Many(Vec<FlowKey>),
}

impl PredecessorKeys {
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Appends `key`; under `dedup` only when it is not already listed. Returns whether it was
    /// appended. Only a zero-send all-to-all rank's release (`collective_completion`) dedups: the
    /// ordinary paths name each group once, so their keys are distinct and they pay nothing.
    fn insert(&mut self, key: FlowKey, dedup: bool) -> bool {
        let listed = dedup
            && match self {
                Self::None => false,
                Self::One(first) => *first == key,
                Self::Many(keys) => keys.contains(&key),
            };
        if !listed {
            self.push(key);
        }
        !listed
    }

    fn push(&mut self, key: FlowKey) {
        *self = match std::mem::take(self) {
            Self::None => Self::One(key),
            Self::One(first) => Self::Many(vec![first, key]),
            Self::Many(mut keys) => {
                keys.push(key);
                Self::Many(keys)
            }
        };
    }

    /// The image form: a join's flows are sorted and appended to `joins` as one run.
    fn resolve(
        &self,
        flow_ids: &BTreeMap<FlowKey, u64>,
        joins: &mut Vec<FlowId>,
    ) -> Result<StagePredecessors, CompileError> {
        Ok(match self {
            Self::None => StagePredecessors::None,
            Self::One(key) => StagePredecessors::One(FlowId(flow_ids[key])),
            Self::Many(keys) => {
                let mut flows = keys
                    .iter()
                    .map(|key| FlowId(flow_ids[key]))
                    .collect::<Vec<_>>();
                flows.sort_unstable();
                flows.dedup();
                let first = u32::try_from(joins.len()).map_err(|_| {
                    CompileError::Invalid("stage join table exceeds u32".to_owned())
                })?;
                let count = u32::try_from(flows.len())
                    .map_err(|_| CompileError::Invalid("stage join exceeds u32".to_owned()))?;
                if count == 1 {
                    StagePredecessors::One(flows[0])
                } else {
                    joins.extend(flows);
                    StagePredecessors::Join { first, count }
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
struct ComputeStageInput {
    compute_id: u64,
    group_size: u32,
    rank: u32,
    duration_ns: u64,
    local: PredecessorKeys,
    inbound: PredecessorKeys,
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
    compile_scenario(path.as_ref(), route_workers, None)
}

/// Lowers a TOML scenario's topology, switch and link configuration (and any stage groups it
/// declares) together with a typed workload's operations (`super::workload`, ruling R12).
pub fn compile_config_with_workload(
    path: impl AsRef<Path>,
    workload: &super::workload::Workload,
    route_workers: RouteWorkers,
) -> Result<SimulationImage, CompileError> {
    compile_scenario(path.as_ref(), route_workers, Some(workload))
}

/// [`compile_config_with_route_workers`], with the run manifest of an AICB scenario
/// (`[workload.aicb]`, P16 H3): `None` for every other scenario. The manifest is host metadata
/// and never enters the image.
pub fn compile_config_with_manifest(
    path: impl AsRef<Path>,
    route_workers: RouteWorkers,
) -> Result<(SimulationImage, Option<crate::workload::aicb::AicbManifest>), CompileError> {
    let path = path.as_ref();
    let content = read_scenario(path)?;
    match compile_text(&content, route_workers, None) {
        Err(CompileError::Parse(_)) if crate::workload::aicb::is_aicb_scenario(&content) => {
            compile_aicb(path, &content, route_workers)
        }
        other => other.map(|image| (image, None)),
    }
}

fn read_scenario(path: &Path) -> Result<String, CompileError> {
    let content = fs::read_to_string(path).map_err(|source| CompileError::Read {
        path: path.display().to_string(),
        source,
    })?;
    path.to_str()
        .ok_or_else(|| CompileError::Invalid("configuration path is not valid UTF-8".to_owned()))?;
    Ok(content)
}

fn compile_scenario(
    path: &Path,
    route_workers: RouteWorkers,
    workload: Option<&super::workload::Workload>,
) -> Result<SimulationImage, CompileError> {
    let content = read_scenario(path)?;
    match compile_text(&content, route_workers, workload) {
        // An AICB scenario has no `[switch]` of its own, so it does not parse as an ordinary one.
        Err(CompileError::Parse(_))
            if workload.is_none() && crate::workload::aicb::is_aicb_scenario(&content) =>
        {
            compile_aicb(path, &content, route_workers).map(|(image, _)| image)
        }
        other => other,
    }
}

/// Lowers an AICB scenario: the adapter reads the trace and `SimAI.conf` it names and returns the
/// scenario text with the derived fabric tables and the workload IR (`crate::workload::aicb`).
fn compile_aicb(
    path: &Path,
    content: &str,
    route_workers: RouteWorkers,
) -> Result<(SimulationImage, Option<crate::workload::aicb::AicbManifest>), CompileError> {
    let prepared = crate::workload::aicb::prepare(path, content)
        .map_err(|error| CompileError::Invalid(format!("AICB scenario: {error}")))?;
    let image = compile_text(&prepared.text, route_workers, Some(&prepared.workload))?;
    Ok((image, Some(prepared.manifest)))
}

fn compile_text(
    content: &str,
    route_workers: RouteWorkers,
    workload: Option<&super::workload::Workload>,
) -> Result<SimulationImage, CompileError> {
    crate::validate_config_text(content).map_err(CompileError::Unsupported)?;

    let source: SourceConfig =
        toml::from_str(content).map_err(|error| parse_error(content, error))?;
    if source.workload.is_some() {
        return Err(CompileError::Unsupported(
            "a `[workload.aicb]` scenario takes its `[switch]`, `[link]` and `[routing]` tables \
             from SimAI.conf; remove them"
                .to_owned(),
        ));
    }
    let model = SupportedModel::from_source(source, content, workload)?;
    let (graph, hosts, profile) = build_graph_with_profile_from_str(content)?;

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
    /// The issue stream of each named stage group on a stream other than 0.
    streams: BTreeMap<String, u32>,
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
    fn from_source(
        source: SourceConfig,
        scenario_text: &str,
        workload: Option<&super::workload::Workload>,
    ) -> Result<Self, CompileError> {
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
        // The streams are read first, so the groups still collect in place (no new table).
        let mut streams = BTreeMap::new();
        for collective in source.collective.iter().flatten() {
            if let Some(stream) = collective.stream.filter(|&stream| stream != 0) {
                let name = collective.name.clone().ok_or_else(|| {
                    CompileError::Invalid(
                        "a collective on a stream other than 0 needs a `name`".to_owned(),
                    )
                })?;
                streams.insert(name, stream);
            }
        }
        for compute in source.compute.iter().flatten() {
            if let Some(stream) = compute.stream.filter(|&stream| stream != 0) {
                streams.insert(compute.name.clone(), stream);
            }
        }
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
        let mut computes = source
            .compute
            .unwrap_or_default()
            .into_iter()
            .map(validate_compute)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(workload) = workload {
            let (more_collectives, more_computes) =
                workload_keys(workload, &mut roce_keys, &mut streams)?;
            collectives.extend(more_collectives);
            computes.extend(more_computes);
        }
        // After the collectives, whose keys sort them in `canonical_flows`.
        let roce_keys = canonical_roce_keys(
            roce_keys,
            &mut explicit_flows,
            &mut flow_sets,
            &mut collectives,
        );

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
            streams,
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
        "ReduceScatter" => Ok(CollectiveAlgorithm::ReduceScatter),
        "AllToAll" => Ok(CollectiveAlgorithm::AllToAll),
        "SendRecv" => Ok(CollectiveAlgorithm::SendRecv),
        unsupported => Err(CompileError::Unsupported(format!(
            "unsupported collective algorithm `{unsupported}`; Days lowers RingAllReduce, AllGather, ReduceScatter, AllToAll and SendRecv"
        ))),
    }
}

/// Whether `algorithm` runs on rings (and so has channels and ring sinks).
const fn is_ring_algorithm(algorithm: CollectiveAlgorithm) -> bool {
    matches!(
        algorithm,
        CollectiveAlgorithm::RingAllReduce
            | CollectiveAlgorithm::AllGather
            | CollectiveAlgorithm::ReduceScatter
    )
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
    has_channels: bool,
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
    if flow_count == 0
        || algorithm == CollectiveAlgorithm::RingAllReduce && flow_count < 2
        || algorithm == CollectiveAlgorithm::SendRecv && flow_count != 2
    {
        return Err(CompileError::Invalid(match algorithm {
            CollectiveAlgorithm::RingAllReduce => {
                "RingAllReduce flow_count must be at least 2".to_owned()
            }
            CollectiveAlgorithm::SendRecv => {
                "SendRecv flow_count must be 2: sources = [sender, receiver]".to_owned()
            }
            other => format!("{other:?} flow_count must be at least 1"),
        }));
    }
    if !is_ring_algorithm(algorithm) {
        if !sinks.is_empty() {
            return Err(CompileError::Invalid(format!(
                "{algorithm:?} takes `sources` only: no `sinks`"
            )));
        }
        if algorithm == CollectiveAlgorithm::SendRecv && sources.len() != 2 {
            return Err(CompileError::Invalid(
                "SendRecv requires sources = [sender, receiver]".to_owned(),
            ));
        }
        if !sources.is_empty() && sources.len() != flow_count as usize {
            return Err(CompileError::Invalid(format!(
                "collective sources must contain flow_count={flow_count} entries"
            )));
        }
    } else if has_channels {
        // Ring channels replace `sinks`; `shape_collective` checks them against the sources.
        if sources.len() != flow_count as usize || !sinks.is_empty() {
            return Err(CompileError::Invalid(format!(
                "ring channels take flow_count={flow_count} `sources` and no `sinks`"
            )));
        }
    } else if sources.is_empty() != sinks.is_empty() {
        return Err(CompileError::Invalid(
            "collective sources and sinks must either both be provided or both be omitted"
                .to_owned(),
        ));
    }
    if is_ring_algorithm(algorithm)
        && !has_channels
        && !sources.is_empty()
        && (sources.len() != flow_count as usize || sinks.len() != flow_count as usize)
    {
        return Err(CompileError::Invalid(format!(
            "collective sources and sinks must each contain flow_count={flow_count} entries"
        )));
    }
    if is_ring_algorithm(algorithm) && !has_channels && !sources.is_empty() {
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
    if total_bytes < flow_count || algorithm == CollectiveAlgorithm::SendRecv && total_bytes == 0 {
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
        issue_ordinal: None,
        algorithm,
        flow_count,
        sources,
        sinks,
        priority: priority.unwrap_or(0),
        traffic,
        name: None,
        after: None,
        shape: None,
        chunk: CollectiveChunkPolicy::EqualRemainderLast,
    })
}

/// Attaches a collective's ring channels, chunk policy and seeded all-to-all sizes, and checks
/// that every message carries a byte. The compiler checks nothing SimAI-specific: SimAI's size
/// clamps and its skipping of empty rings belong to the AICB adapter (ruling R8).
fn shape_collective(
    key: &mut CollectiveKey,
    channels: Option<Vec<Vec<u64>>>,
    chunk: Option<&str>,
    seeded: Option<SeededAllToAll>,
) -> Result<(), CompileError> {
    let algorithm = key.algorithm;
    let n = key.flow_count;
    let Termination::Bytes(total_bytes) = key.traffic.termination else {
        unreachable!("collective validation requires byte termination")
    };
    key.chunk = match chunk {
        None => match algorithm {
            CollectiveAlgorithm::AllToAll => CollectiveChunkPolicy::UniformFloor,
            _ => CollectiveChunkPolicy::EqualRemainderLast,
        },
        Some("EqualRemainderLast") if is_ring_algorithm(algorithm) => {
            CollectiveChunkPolicy::EqualRemainderLast
        }
        Some("UniformFloor") if algorithm != CollectiveAlgorithm::SendRecv => {
            CollectiveChunkPolicy::UniformFloor
        }
        Some(other) => {
            return Err(CompileError::Unsupported(format!(
                "unsupported {algorithm:?} chunk policy `{other}`; rings take EqualRemainderLast or UniformFloor, an all-to-all UniformFloor"
            )));
        }
    };
    if let Some(channels) = channels {
        if !is_ring_algorithm(algorithm) {
            return Err(CompileError::Invalid(format!(
                "{algorithm:?} has no ring channels"
            )));
        }
        if !key.sinks.is_empty() || key.sources.is_empty() {
            return Err(CompileError::Invalid(
                "ring channels take `sources` (the ranks in order) and no `sinks`".to_owned(),
            ));
        }
        if channels.is_empty() {
            return Err(CompileError::Invalid("`channels` lists no ring".to_owned()));
        }
        let mut sorted = key.sources.clone();
        sorted.sort_unstable();
        let mut rings = Vec::with_capacity(channels.len());
        for ring in &channels {
            let mut members = ring.clone();
            members.sort_unstable();
            if members != sorted {
                return Err(CompileError::Invalid(
                    "every ring channel must order exactly the collective's sources".to_owned(),
                ));
            }
            rings.push(
                ring.iter()
                    .map(|host| {
                        let rank = key
                            .sources
                            .iter()
                            .position(|source| source == host)
                            .expect("a channel member is a source");
                        u32::try_from(rank).expect("the group size fits u32")
                    })
                    .collect(),
            );
        }
        if rings.len() > 1 && key.chunk != CollectiveChunkPolicy::UniformFloor {
            return Err(CompileError::Invalid(
                "a collective of several ring channels requires chunk = \"UniformFloor\""
                    .to_owned(),
            ));
        }
        key.shape.get_or_insert_with(Box::default).channels = rings;
    }
    if let Some(seeded) = seeded {
        if algorithm != CollectiveAlgorithm::AllToAll {
            return Err(CompileError::Invalid(
                "`[collective.alltoall]` sizes apply to an AllToAll only".to_owned(),
            ));
        }
        if seeded.experts == 0 || seeded.experts % n != 0 {
            return Err(CompileError::Invalid(format!(
                "all-to-all experts {} must be a positive multiple of the {n} ranks",
                seeded.experts
            )));
        }
        if seeded.routed_bytes_per_source() != Some(total_bytes) {
            return Err(CompileError::Invalid(format!(
                "a seeded all-to-all's size {total_bytes} must equal tokens x topk x bytes_per_copy"
            )));
        }
        key.chunk = CollectiveChunkPolicy::Seeded;
        key.shape.get_or_insert_with(Box::default).seeded = Some(seeded);
    }
    let channel_count = key.channels().len().max(1) as u64;
    let message = match algorithm {
        CollectiveAlgorithm::SendRecv => total_bytes,
        _ if key.chunk == CollectiveChunkPolicy::Seeded => 1,
        CollectiveAlgorithm::AllToAll => total_bytes / n,
        _ if key.chunk == CollectiveChunkPolicy::UniformFloor => total_bytes / n / channel_count,
        _ => total_bytes / n,
    };
    if message == 0 && n > 1 {
        return Err(CompileError::Invalid(format!(
            "{algorithm:?} of {total_bytes} bytes over {n} ranks and {channel_count} channels sends empty messages; every message must carry a byte"
        )));
    }
    Ok(())
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
    let (channels, chunk, alltoall) = (source.channels, source.chunk, source.alltoall);
    let issue_ordinal = source.issue_ordinal;
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
        channels.is_some(),
        scenario_text,
        roce_keys,
    )?;
    key.name = name;
    key.after = after;
    key.issue_ordinal = issue_ordinal_key(issue_ordinal)?;
    let seeded = alltoall
        .map(|table| seeded_all_to_all(*table))
        .transpose()?;
    shape_collective(&mut key, channels, chunk.as_deref(), seeded)?;
    Ok(key)
}

/// A collective's issue ordinal as its key holds it (`ordinal + 1`), refusing one that does not
/// fit.
fn issue_ordinal_key(ordinal: Option<u64>) -> Result<Option<std::num::NonZeroU32>, CompileError> {
    ordinal
        .map(|ordinal| {
            ordinal
                .checked_add(1)
                .and_then(|value| u32::try_from(value).ok())
                .and_then(std::num::NonZeroU32::new)
                .ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "collective issue_ordinal {ordinal} is above {}",
                        u32::MAX - 1
                    ))
                })
        })
        .transpose()
}

/// A `[collective.alltoall]` table's seeded matrix.
fn seeded_all_to_all(source: SourceAllToAll) -> Result<SeededAllToAll, CompileError> {
    let skew = match source.skew.as_deref() {
        None | Some("Zipf1") => RoutingSkew::Zipf1,
        Some("Uniform") => RoutingSkew::Uniform,
        Some(other) => {
            return Err(CompileError::Unsupported(format!(
                "unsupported all-to-all routing skew `{other}`; Days draws Zipf1 or Uniform"
            )));
        }
    };
    Ok(SeededAllToAll {
        seed: source.seed,
        matrix: source.matrix,
        group: source.group,
        transpose: source.transpose,
        experts: source.experts,
        topk: source.topk,
        tokens: source.tokens,
        bytes_per_copy: source.bytes_per_copy,
        skew,
    })
}

/// The collectives and compute groups of a typed workload, validated as their TOML rendering
/// would be: operation `i` is the stage group `@i`.
fn workload_keys(
    workload: &super::workload::Workload,
    roce_keys: &mut Vec<RoceTrafficKey>,
    streams: &mut BTreeMap<String, u32>,
) -> Result<(Vec<CollectiveKey>, Vec<ComputeKey>), CompileError> {
    use super::workload::OperationKind;
    let transports = workload
        .transports
        .iter()
        .map(|transport| {
            toml::from_str::<SourceTraffic>(&transport.traffic)
                .map(|traffic| (transport, traffic))
                .map_err(CompileError::from)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let name = |index: usize| format!("@{index}");
    let mut collectives = Vec::new();
    let mut computes = Vec::new();
    for (index, operation) in workload.operations.iter().enumerate() {
        let hosts = workload.groups.get(operation.group).ok_or_else(|| {
            CompileError::Invalid(format!("workload operation {index} names an unknown group"))
        })?;
        if let Some(&after) = operation
            .after
            .iter()
            .find(|&&after| after >= workload.operations.len())
        {
            return Err(CompileError::Invalid(format!(
                "workload operation {index} follows unknown operation {after}"
            )));
        }
        if operation.stream != 0 {
            streams.insert(name(index), operation.stream);
        }
        let after = match operation.after.as_slice() {
            [] => None,
            [one] => Some(AfterGroups::One(name(*one))),
            many => Some(AfterGroups::Many(
                many.iter().map(|&after| name(after)).collect(),
            )),
        };
        match &operation.kind {
            OperationKind::Compute { duration_ns } => {
                computes.push(validate_compute(SourceCompute {
                    name: name(index),
                    hosts: hosts.clone(),
                    duration_ns: *duration_ns,
                    after,
                    stream: None,
                })?)
            }
            OperationKind::Collective(collective) => {
                let (transport, template) =
                    transports.get(collective.transport).ok_or_else(|| {
                        CompileError::Invalid(format!(
                            "workload operation {index} names an unknown transport"
                        ))
                    })?;
                let mut traffic = template.clone();
                traffic.size = Some(collective.bytes);
                let ring = collective.channels.is_none()
                    && !matches!(
                        collective.algorithm,
                        super::workload::Algorithm::AllToAll | super::workload::Algorithm::SendRecv
                    );
                let sinks = if ring {
                    hosts
                        .iter()
                        .cycle()
                        .skip(1)
                        .take(hosts.len())
                        .copied()
                        .collect()
                } else {
                    Vec::new()
                };
                let mut key = collective_key(
                    collective.algorithm.collective_type(),
                    Some(transport.flow_type.as_str()),
                    hosts.len() as u64,
                    hosts.clone(),
                    sinks,
                    Some(transport.priority),
                    None,
                    None,
                    None,
                    None,
                    traffic,
                    collective.channels.is_some(),
                    &transport.traffic,
                    roce_keys,
                )?;
                key.name = Some(name(index));
                key.after = after;
                key.issue_ordinal = issue_ordinal_key(collective.issue_ordinal)?;
                let chunk = if collective.uniform_floor {
                    "UniformFloor"
                } else {
                    "EqualRemainderLast"
                };
                let chunk = (collective.algorithm != super::workload::Algorithm::SendRecv
                    && collective.seeded.is_none())
                .then_some(chunk);
                shape_collective(
                    &mut key,
                    collective.channels.clone(),
                    chunk,
                    collective.seeded,
                )?;
                collectives.push(key);
            }
        }
    }
    Ok((collectives, computes))
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
        let mut key = collective_key(
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
            false,
            scenario_text,
            roce_keys,
        )?;
        // Each member is shaped as the same `[[collective]]` block without `channels`, `chunk`
        // or `[collective.alltoall]` is (an all-to-all's UniformFloor chunk, a ring's
        // EqualRemainderLast), which a set cannot declare.
        shape_collective(&mut key, None, None, None)?;
        result.push(key);
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
                "arr_dist",
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
    .and_then(|key| {
        check_distribution_keys(&traffic.arr_dist, "arr_dist", scenario_text)?;
        check_distribution_keys(&traffic.pkt_size_dist, "pkt_size_dist", scenario_text)?;
        Ok(key)
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

/// The text between the braces of the distribution `name` (`arr_dist` or `pkt_size_dist`).
/// Executor distributions are inline tables only: one written as a sub-table or as dotted keys
/// is refused, naming the key and the table (a2aset fix round 2, ruling option (a)). Its span is
/// then its header or its key, never a `{ ... }` value. The table must also fit on one line,
/// without comments or a trailing comma (fix round 3): TOML 1.1 allows all three, but the exact
/// readers split the text on `,` and `=`, so a commented-out `low = ...` would override the live
/// value.
fn inline_distribution<'a>(
    distribution: &SourceDistributionInfo,
    name: &str,
    scenario_text: &'a str,
) -> Result<&'a str, CompileError> {
    let literal = scenario_text
        .get(distribution.span.clone())
        .ok_or_else(|| {
            CompileError::Invalid(
                "distribution source span is outside the scenario text".to_owned(),
            )
        })?;
    let refuse = |rule: &str| {
        let table = crate::utils::serde_fields::table_at(scenario_text, distribution.span.start);
        CompileError::Invalid(format!(
            "executor distributions must use {rule} in `{name}` (in {table})"
        ))
    };
    let body = literal
        .trim()
        .strip_prefix('{')
        .and_then(|body| body.strip_suffix('}'))
        .ok_or_else(|| refuse("an inline TOML table"))?;
    if body.contains(['#', '\n', '\r']) || body.trim_end().ends_with(',') {
        return Err(refuse(
            "a one-line inline TOML table without comments or a trailing comma",
        ));
    }
    Ok(body)
}

fn source_distribution<'a>(
    distribution: &SourceDistributionInfo,
    name: &str,
    scenario_text: &'a str,
) -> Result<ParsedSourceDistribution<'a>, CompileError> {
    let body = inline_distribution(distribution, name, scenario_text)?;
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

/// The fields of each distribution type and whether they are integers: those of
/// [`super::DistributionInfo`], the shared strict schema (`distribution_fields_match_the_shared_schema`
/// keeps the two equal). Only [`check_distribution_keys`]'s allocation-free fast path reads them.
const DISTRIBUTION_FIELDS: [(&str, &[&str], bool); 3] = [
    ("DiscreteUniform", &["low", "high"], true),
    ("Exp", &["lambda"], false),
    ("Uniform", &["low", "high"], false),
];

/// Whether `value`, a value of a document TOML already parsed, is a number the shared schema
/// takes: a decimal integer, or (unless `integer`) any TOML float or integer. Hex, octal, and
/// binary integers, and every non-number (string, table, array, boolean, date), say no and go
/// to the schema.
fn plain_number(value: &str, integer: bool) -> bool {
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    if integer {
        return !unsigned.is_empty() && unsigned.bytes().all(|b| b.is_ascii_digit() || b == b'_');
    }
    if unsigned == "inf" || unsigned == "nan" {
        return true;
    }
    let bytes = unsigned.as_bytes();
    !bytes.is_empty()
        && bytes[0].is_ascii_digit()
        && bytes.iter().enumerate().all(|(index, &b)| match b {
            b'0'..=b'9' | b'_' | b'.' | b'e' | b'E' => true,
            b'+' | b'-' => matches!(bytes[index - 1], b'e' | b'E'),
            _ => false,
        })
}

/// Whether an inline distribution table's keys (`body`, the text between its braces) are exactly `type` and its type's fields, each
/// once and each a plain number (a `key = value` list, as every scenario writes it): then the
/// shared schema accepts it. Anything else goes to the schema, which also accepts what this scan
/// cannot read.
fn distribution_keys_are_exact(body: &str) -> bool {
    let entries = || {
        body.split(',').map(|field| {
            field
                .split_once('=')
                .map(|(key, value)| (key.trim(), value.trim()))
        })
    };
    let Some(kind) = entries().find_map(|entry| {
        entry
            .filter(|(key, _)| *key == "type")
            .and_then(|(_, value)| value.strip_prefix('"')?.strip_suffix('"'))
    }) else {
        return false;
    };
    let Some((_, fields, integer)) = DISTRIBUTION_FIELDS.iter().find(|(name, ..)| *name == kind)
    else {
        return false;
    };
    let mut count = 0;
    for entry in entries() {
        let Some((key, value)) = entry else {
            return false;
        };
        if key != "type" && !(fields.contains(&key) && plain_number(value, *integer)) {
            return false;
        }
        count += 1;
    }
    count == fields.len() + 1
}

/// Refuses a distribution table whose keys are not exactly its type's: the shared, strict schema
/// (`DistributionInfo`, also legacy's) refuses an unknown key or one of another type, which
/// [`source_distribution`] (reading only the keys a type needs) and the traffic kinds that ignore
/// a distribution would drop (a2aset fix round 1, review M1). `name` is the table's key.
fn check_distribution_keys(
    distribution: &SourceDistributionInfo,
    name: &str,
    scenario_text: &str,
) -> Result<(), CompileError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Distribution {
        #[allow(dead_code)]
        distribution: super::DistributionInfo,
    }
    let body = inline_distribution(distribution, name, scenario_text)?;
    // The common case, checked without allocating: exactly `type` and its type's fields.
    if distribution_keys_are_exact(body) {
        return Ok(());
    }
    toml::from_str::<Distribution>(&format!("distribution = {{{body}}}")).map_err(|error| {
        let table = crate::utils::serde_fields::table_at(scenario_text, distribution.span.start);
        CompileError::Invalid(format!("{} in `{name}` (in {table})", error.message()))
    })?;
    Ok(())
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
    match source_distribution(distribution, "pkt_size_dist", scenario_text)? {
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
    name: &str,
    scenario_text: &str,
    scale: u64,
    label: &str,
) -> Result<u64, CompileError> {
    match source_distribution(distribution, name, scenario_text)? {
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
        computes: compute_table,
        seeded: seeded_all_to_alls,
        streams: stage_streams,
    } = canonical_flows(
        model.explicit_flows,
        model.flow_sets,
        {
            let (collectives, computes) =
                single_server_delays(model.collectives, model.computes, profile)?;
            (collectives, computes, &model.streams)
        },
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
                    &compute_table,
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
    // The predecessor runs of join stages, in flow order.
    let mut stage_joins = Vec::new();
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
            // A stage without predecessors starts with its collective (an ungated root).
            let root = stage.local.is_none() && stage.inbound.is_none();
            // A root message starts with its collective, at the traffic's initial delay, as the
            // collective's fabric roots do (review F1); its timer fires the lead after that.
            let root_timer_ns = flow
                .traffic
                .initial_delay_ns
                .checked_add(lead_ns)
                .ok_or_else(|| {
                    CompileError::Invalid(format!(
                        "stage notify timer of flow {} -> {} exceeds u64",
                        flow.source, flow.target
                    ))
                })?;
            let next_emission = if !root {
                ScheduledEmission {
                    status: GeneratorStatus::Blocked,
                    departure_time_ns: 0,
                    payload: PayloadId(0),
                }
            } else if root_timer_ns > model.stop_time_ns {
                ScheduledEmission {
                    status: GeneratorStatus::Stopped,
                    departure_time_ns: root_timer_ns,
                    payload: PayloadId(0),
                }
            } else {
                // Its notify names the sender's timer and then crosses to the target.
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
                    root_timer_ns,
                    descriptor.id,
                    payload,
                    EventKind::PacingTimer,
                ));
                ScheduledEmission {
                    status: GeneratorStatus::Scheduled,
                    departure_time_ns: root_timer_ns,
                    payload,
                }
            };
            let (identity, dependencies) =
                collective_stage_record(stage, &flow_ids, &mut stage_joins)?;
            generators_by_source.entry(source).or_default().push((
                FlowGeneratorState {
                    flow: descriptor.id,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission,
                    rng_state: generator_seed(
                        model.seed,
                        &flow.key,
                        &collective_table,
                        &compute_table,
                        &roce_keys,
                    ),
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
            let root = compute.local.is_none() && compute.inbound.is_none();
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
            let local = compute.local.resolve(&flow_ids, &mut stage_joins)?;
            let inbound = compute.inbound.resolve(&flow_ids, &mut stage_joins)?;
            generators_by_source.entry(source).or_default().push((
                FlowGeneratorState {
                    flow: descriptor.id,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission,
                    rng_state: generator_seed(
                        model.seed,
                        &flow.key,
                        &collective_table,
                        &compute_table,
                        &roce_keys,
                    ),
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
                        local,
                        inbound,
                        inbound_predecessor_bytes: compute.inbound_predecessor_bytes,
                        inbound_bytes_received: 0,
                        local_completed: 0,
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
        // Nothing has run at lowering, so a stage is ready exactly when it waits for nothing.
        let collective_ready = flow
            .collective
            .as_ref()
            .is_none_or(|stage| stage.local.is_none() && stage.inbound.is_none());
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
            .map(|stage| collective_stage_record(stage, &flow_ids, &mut stage_joins))
            .transpose()?;
        generators_by_source.entry(source).or_default().push((
            FlowGeneratorState {
                // Every collective stage is a TCP or RoCE generator whose dependencies live in this record;
                // compute stages build theirs in the compute branch above.
                flow: descriptor.id,
                packets_emitted: 0,
                bytes_emitted: 0,
                next_emission,
                rng_state: generator_seed(
                    model.seed,
                    &flow.key,
                    &collective_table,
                    &compute_table,
                    &roce_keys,
                ),
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
    // P16 H2: an ECN row per egress link rate, as SimAI keys its ECN rows by port rate, set in one
    // pass over the ports only when the scenario has rows (review F2: deciding it inside the map
    // made the map fallible, and a `Result` collect has no size hint, so the vector grew by doubling
    // on every image).
    if let (Some(rows), DropMarkPolicy::EcnThreshold(policy)) =
        (&model.ecn_by_rate, model.drop_mark)
    {
        for (port, state) in switch_port_keys.iter().zip(&mut switch_states) {
            let LpKey::SwitchPort { egress, .. } = *port else {
                unreachable!("switch-port key set contains only switch ports")
            };
            let queue = &mut state.queues[0];
            let rate_bps = link_rate.of(egress);
            let threshold = *rows.get(&rate_bps).ok_or_else(|| {
                CompileError::Invalid(format!(
                    "`switch.ecn_by_rate` has no row for a {rate_bps} b/s egress link"
                ))
            })?;
            queue.drop_mark = DropMarkPolicy::EcnThreshold(EcnThresholdPolicy {
                threshold,
                ..policy
            });
        }
    }
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
        stage_joins,
        seeded_all_to_alls,
        stage_streams,
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
    stage_joins: &mut Vec<FlowId>,
) -> Result<(CollectiveStageIdentity, StageDependencies), CompileError> {
    Ok((
        CollectiveStageIdentity {
            collective_id: stage.collective_id,
            algorithm: stage.algorithm,
            channel: stage.position.channel,
            group_size: stage.group_size,
            declared_total_bytes: stage.declared_total_bytes,
            rank: stage.position.rank,
            phase: stage.position.phase,
            step: stage.position.step,
            chunk_policy: stage.chunk_policy,
            channel_policy: stage.channel_policy,
            chunk_offset_bytes: stage.chunk_offset_bytes,
            chunk_bytes: stage.chunk_bytes,
        },
        StageDependencies {
            local: stage.local.resolve(flow_ids, stage_joins)?,
            inbound: stage.inbound.resolve(flow_ids, stage_joins)?,
            inbound_predecessor_bytes: stage.inbound_predecessor_bytes,
            inbound_bytes_received: 0,
            local_completed: 0,
        },
    ))
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
        // An all-to-all releases all of a rank's sends at once, so its same-server sends share the
        // sender's NVLink port: their concurrency is how many there are (orchestrator ruling,
        // from the N1 SimAI comparison).
        let mut all_to_all_peers = BTreeMap::<(u64, u32), u64>::new();
        for flow in flows.iter() {
            if let Some(stage) = flow.collective.as_deref() {
                if stage.algorithm == CollectiveAlgorithm::AllToAll
                    && locality.same_server(flow.source, flow.target)
                {
                    *all_to_all_peers
                        .entry((stage.collective_id, stage.position.rank))
                        .or_default() += 1;
                }
            }
        }
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
            // A ring hop inside a server of a ring that spans servers runs alone on the sender's
            // port (concurrency 1: SimAI's channels desynchronise behind their own inter-server
            // hops), as does a send/recv; an all-to-all's same-server sends share it, also when
            // the whole all-to-all is inside one server. A ring collective inside one server never
            // reaches here: it is one delay stage per rank (`single_server_delays`).
            let concurrency = match stage.algorithm {
                CollectiveAlgorithm::AllToAll => all_to_all_peers
                    .get(&(stage.collective_id, stage.position.rank))
                    .copied()
                    .unwrap_or(1),
                _ => 1,
            };
            let delay = locality
                .nvlink_message_delay_ns(stage.chunk_bytes, concurrency, packet_size)
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

/// The ring collectives of a rail image whose ranks all share one server, as compute groups
/// (ruling H2-2: one delay stage per rank). Such a collective never leaves the server, so each
/// rank's part is a delay: `steps` ring steps, each sending one message on every one of `c`
/// channels at once through the rank's NVLink port
/// ([`ServerLocality::single_server_collective_delay_ns`] with concurrency `c`, the orchestrator's
/// ruling): SimAI's ring inside a server has `c = n` channels (or the collective's own
/// `channels`), `n - 1` steps for an AllGather or ReduceScatter and `2 (n - 1)` for an AllReduce,
/// and messages of `floor(floor(S / n) / c)` bytes. The group keeps the collective's name, ranks
/// and `after`, so what follows it waits for each rank's delay. Off the rail fabric it returns its
/// inputs unchanged.
fn single_server_delays(
    collectives: Vec<CollectiveKey>,
    mut computes: Vec<ComputeKey>,
    profile: TopologyProfile,
) -> Result<(Vec<CollectiveKey>, Vec<ComputeKey>), CompileError> {
    let TopologyProfile::Rail(rail) = profile else {
        return Ok((collectives, computes));
    };
    let locality = ServerLocality::new(rail);
    let single_server = |key: &CollectiveKey| {
        is_ring_algorithm(key.algorithm)
            && key.flow_count > 1
            && key.sources.iter().all(|&host| {
                locality.server_of(host).is_some()
                    && locality.server_of(host) == locality.server_of(key.sources[0])
            })
    };
    if !collectives.iter().any(single_server) {
        return Ok((collectives, computes));
    }
    let mut remaining = Vec::with_capacity(collectives.len());
    for (index, key) in collectives.into_iter().enumerate() {
        if !single_server(&key) {
            remaining.push(key);
            continue;
        }
        let n = key.flow_count;
        let Termination::Bytes(total_bytes) = key.traffic.termination else {
            unreachable!("collective validation requires byte termination")
        };
        let channels = match key.channels().len() {
            0 => n,
            declared => declared as u64,
        };
        let steps = match key.algorithm {
            CollectiveAlgorithm::RingAllReduce => 2 * (n - 1),
            _ => n - 1,
        };
        let message = total_bytes / n / channels;
        let duration_ns = locality
            .single_server_collective_delay_ns(
                steps,
                message,
                channels,
                key.traffic.packet_size_bytes,
            )
            .filter(|&delay| delay > 0)
            .ok_or_else(|| {
                CompileError::Invalid(format!(
                    "single-server collective of {total_bytes} bytes over {n} ranks and {channels} \
                     channels has no message delay (a message of {message} bytes)"
                ))
            })?;
        computes.push(ComputeKey {
            // An unnamed collective cannot be followed; it still runs its delay.
            name: key
                .name
                .unwrap_or_else(|| format!("\u{0}single-server collective {index}")),
            hosts: key.sources,
            duration_ns,
            after: key.after,
        });
    }
    Ok((remaining, computes))
}

/// The canonically ordered flow inputs and the collective keys their stage keys index.
struct CanonicalFlows {
    flows: Vec<FlowInput>,
    /// The normalized collective keys, sorted and distinct: `FlowKey::CollectiveStage::collective`
    /// indexes this table.
    collectives: Vec<CollectiveKey>,
    /// The compute keys, sorted: `FlowKey::ComputeStage::compute` indexes this table.
    computes: Vec<ComputeKey>,
    /// Each seeded all-to-all's matrix parameters, ascending by collective id.
    seeded: Vec<days_executor::SeededCollective>,
    /// The issue stream of each stage group on a stream other than 0, ascending.
    streams: Vec<days_executor::StageStream>,
}

/// Ordinal of `compute` in the sorted `table`.
fn compute_ordinal(table: &[ComputeKey], compute: &ComputeKey) -> u64 {
    let index = table
        .binary_search(compute)
        .expect("every compute key is in the compute table");
    u64::try_from(index).expect("the compute table length fits u64")
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
    (mut collectives, mut computes, streams): (
        Vec<CollectiveKey>,
        Vec<ComputeKey>,
        &BTreeMap<String, u32>,
    ),
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
            // An all-to-all has no ring; every ring algorithm's ring is the host order.
            if is_ring_algorithm(semantic.algorithm) {
                semantic.sinks = participants
                    .iter()
                    .cycle()
                    .skip(1)
                    .take(participants.len())
                    .copied()
                    .collect();
            }
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
    // Rings in rank order (every TOML ring without `channels`) need no layout table.
    let layouts = if collective_table.iter().any(CollectiveLayout::needed) {
        collective_table
            .iter()
            .map(CollectiveLayout::of)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };
    let plan = StagePlan {
        groups: &groups,
        collectives: &collective_table,
        layouts: &layouts,
        computes: &computes,
    };
    let mut collective_duplicates = vec![0_u64; collective_table.len()];
    let mut next_collective_id = 0_u64;
    let mut seeded = Vec::new();
    let mut stage_streams = Vec::new();
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
        expand_collective(
            &mut flows,
            &plan,
            semantic,
            collective,
            duplicate_ordinal,
            collective_id,
        )?;
        // The image names the matrix of every seeded collective that has stages (ids ascend).
        if let Some(&matrix) = semantic.seeded() {
            if collective_has_stages(semantic) {
                seeded.push(days_executor::SeededCollective {
                    collective_id,
                    matrix,
                });
            }
        }
        let stream = semantic.name.as_ref().and_then(|name| streams.get(name));
        if let Some(&stream) = stream.filter(|_| collective_has_stages(semantic)) {
            stage_streams.push(days_executor::StageStream {
                operation: days_executor::StageOperation::Collective(collective_id),
                stream,
            });
        }
    }
    for (compute_id, compute) in computes.iter().enumerate() {
        expand_compute(&mut flows, &plan, compute, compute_id as u64)?;
        if let Some(&stream) = streams.get(&compute.name) {
            stage_streams.push(days_executor::StageStream {
                operation: days_executor::StageOperation::Compute(compute_id as u64),
                stream,
            });
        }
    }

    flows.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(CanonicalFlows {
        flows,
        collectives: collective_table,
        computes,
        seeded,
        streams: stage_streams,
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

/// The owner offset of the ring recurrence: in step `s` a rank forwards the chunk its ring
/// position `s - offset` back owns.
const fn owner_offset(algorithm: CollectiveAlgorithm, phase: CollectivePhase) -> u64 {
    match (algorithm, phase) {
        (CollectiveAlgorithm::RingAllReduce, CollectivePhase::AllGather) => 2,
        _ => 1,
    }
}

/// The rank whose chunk the stage at ring `position` sends in `step` of a ring of `n`, given the
/// ring's `order` (`order[position]` is the rank there).
fn collective_stage_owner(
    algorithm: CollectiveAlgorithm,
    phase: CollectivePhase,
    layout: &CollectiveLayout,
    channel: usize,
    (n, rank): (u64, u32),
    step: u64,
) -> u64 {
    let position = u64::from(layout.position(channel, rank));
    let owner = (position + n - step + owner_offset(algorithm, phase)) % n;
    u64::from(layout.rank_at(channel, owner as u32))
}

/// The phases of a ring algorithm, in order.
fn ring_phases(algorithm: CollectiveAlgorithm) -> &'static [CollectivePhase] {
    match algorithm {
        CollectiveAlgorithm::RingAllReduce => {
            &[CollectivePhase::ReduceScatter, CollectivePhase::AllGather]
        }
        CollectiveAlgorithm::ReduceScatter => &[CollectivePhase::ReduceScatter],
        _ => &[CollectivePhase::AllGather],
    }
}

/// Whether a collective expands to any stage.
fn collective_has_stages(semantic: &CollectiveKey) -> bool {
    semantic.flow_count > 1
}

/// The message structure of one collective key, computed once for its expansion and for the
/// completion sets its successors wait for. One ring in rank order (every TOML ring without
/// `channels`) is the empty layout, which allocates nothing.
struct CollectiveLayout {
    /// A ring algorithm's channels: each a rank order; empty for one ring in `sources` order.
    rings: Vec<Vec<u32>>,
    /// `positions[k][rank]`: the rank's position in channel `k`.
    positions: Vec<Vec<u32>>,
    /// An all-to-all's bytes per ordered pair, row-major; zero for a pair without a message.
    pair_bytes: Vec<u64>,
}

impl CollectiveLayout {
    fn of(semantic: &CollectiveKey) -> Result<Self, CompileError> {
        let n = usize::try_from(semantic.flow_count)
            .map_err(|_| CompileError::Invalid("collective group exceeds usize".to_owned()))?;
        let mut layout = Self {
            rings: Vec::new(),
            positions: Vec::new(),
            pair_bytes: Vec::new(),
        };
        let Termination::Bytes(total_bytes) = semantic.traffic.termination else {
            unreachable!("collective validation requires byte termination")
        };
        match semantic.algorithm {
            CollectiveAlgorithm::AllToAll => {
                layout.pair_bytes = match semantic.seeded() {
                    Some(seeded) => seeded.bytes(semantic.flow_count).ok_or_else(|| {
                        CompileError::Invalid("seeded all-to-all sizes overflow u64".to_owned())
                    })?,
                    None => {
                        let mut bytes = vec![total_bytes / semantic.flow_count; n * n];
                        for rank in 0..n {
                            bytes[rank * n + rank] = 0;
                        }
                        bytes
                    }
                };
            }
            CollectiveAlgorithm::SendRecv => {}
            _ if semantic.channels().is_empty() => {}
            _ => {
                layout.rings = semantic.channels().to_vec();
                layout.positions = layout
                    .rings
                    .iter()
                    .map(|order| {
                        let mut positions = vec![0_u32; n];
                        for (position, &rank) in order.iter().enumerate() {
                            positions[rank as usize] = position as u32;
                        }
                        positions
                    })
                    .collect();
            }
        }
        Ok(layout)
    }

    /// Whether `semantic` needs a layout of its own: channel rings, or an all-to-all's pairs.
    fn needed(semantic: &CollectiveKey) -> bool {
        !semantic.channels().is_empty() || semantic.algorithm == CollectiveAlgorithm::AllToAll
    }

    /// A ring algorithm's channel count.
    fn channels(&self) -> usize {
        self.rings.len().max(1)
    }

    /// `rank`'s position on channel `channel`.
    fn position(&self, channel: usize, rank: u32) -> u32 {
        self.positions
            .get(channel)
            .map_or(rank, |positions| positions[rank as usize])
    }

    /// The rank at `position` on channel `channel`.
    fn rank_at(&self, channel: usize, position: u32) -> u32 {
        self.rings
            .get(channel)
            .map_or(position, |order| order[position as usize])
    }

    /// The rank before `rank` on channel `channel` of an `n`-rank ring.
    fn previous(&self, channel: usize, rank: u32, n: u32) -> u32 {
        self.rank_at(channel, (self.position(channel, rank) + n - 1) % n)
    }

    /// The rank after `rank` on channel `channel` of an `n`-rank ring.
    fn next(&self, channel: usize, rank: u32, n: u32) -> u32 {
        self.rank_at(channel, (self.position(channel, rank) + 1) % n)
    }
}

/// The tables the expansions read: the stage groups and the collectives' layouts.
struct StagePlan<'a> {
    groups: &'a BTreeMap<String, StageGroup<'a>>,
    collectives: &'a [CollectiveKey],
    /// One per collective, or empty when every collective has the identity layout.
    layouts: &'a [CollectiveLayout],
    computes: &'a [ComputeKey],
}

/// The layout of a ring in rank order and of a send/recv.
static IDENTITY_LAYOUT: CollectiveLayout = CollectiveLayout {
    rings: Vec::new(),
    positions: Vec::new(),
    pair_bytes: Vec::new(),
};

impl StagePlan<'_> {
    fn layout(&self, ordinal: u64) -> &CollectiveLayout {
        self.layouts
            .get(ordinal as usize)
            .unwrap_or(&IDENTITY_LAYOUT)
    }
}

/// A ring stage's chunk: `(offset, bytes)`.
fn ring_chunk(
    semantic: &CollectiveKey,
    layout: &CollectiveLayout,
    phase: CollectivePhase,
    channel: usize,
    rank: u32,
    step: u64,
) -> (u64, u64) {
    let n = semantic.flow_count;
    let Termination::Bytes(total_bytes) = semantic.traffic.termination else {
        unreachable!("collective validation requires byte termination")
    };
    let owner = collective_stage_owner(semantic.algorithm, phase, layout, channel, (n, rank), step);
    match semantic.chunk {
        CollectiveChunkPolicy::UniformFloor => {
            let channels = layout.channels() as u64;
            let bytes = total_bytes / n / channels;
            ((owner * channels + channel as u64) * bytes, bytes)
        }
        _ => collective_chunk_bounds(total_bytes, n, owner),
    }
}

/// The stages that complete collective `ordinal` at `rank`: the rank's own last sends (local,
/// acknowledged) and the last messages delivered to it (inbound, with their bytes). A ring's are
/// each channel's final step; an all-to-all's are every pair from and to the rank; a send/recv's
/// is its one message, local at the sender and inbound at the receiver. A named collective is
/// unique, so its only instance has duplicate ordinal 0.
///
/// An all-to-all rank that sends nothing (an all-zero row of a seeded matrix) has no own send to
/// tie the collective's completion there to its release, so its completion also takes the
/// all-to-all's release at the rank: its own gate stages there (the user's ruling, option 2). A
/// gate group the dependent itself names (`direct`) is not taken twice, and every key the release
/// adds is deduplicated (`dedup`), so a collective reached along several paths (a diamond of
/// zero-send all-to-alls, or directly and through a chain) adds its stages and bytes once: the
/// predecessor set is a union (review M1). An ungated all-to-all has no gate, so a rank of it that
/// neither sends nor receives completes at no stage.
fn collective_completion(
    plan: &StagePlan<'_>,
    ordinal: u64,
    rank: u32,
    (direct, dedup): (&[String], bool),
    local: &mut PredecessorKeys,
    inbound: &mut PredecessorKeys,
) -> Result<u64, CompileError> {
    let semantic = &plan.collectives[ordinal as usize];
    let layout = plan.layout(ordinal);
    let key = |phase, channel, rank, step| FlowKey::CollectiveStage {
        collective: ordinal,
        duplicate_ordinal: 0,
        stage: CollectiveStagePosition {
            phase,
            channel,
            rank,
            step,
        },
    };
    let n = u32::try_from(semantic.flow_count).expect("the group size fits u32");
    let mut bytes = 0_u64;
    let mut add = |more: u64| -> Result<(), CompileError> {
        bytes = bytes
            .checked_add(more)
            .ok_or_else(|| CompileError::Invalid("inbound join bytes exceed u64".to_owned()))?;
        Ok(())
    };
    match semantic.algorithm {
        CollectiveAlgorithm::AllToAll => {
            let size = n as usize;
            let mut sends = false;
            for step in 1..n {
                let target = (rank + step) % n;
                if layout.pair_bytes[rank as usize * size + target as usize] != 0 {
                    sends = true;
                    local.insert(key(CollectivePhase::AllToAll, 0, rank, step), dedup);
                }
            }
            for step in 1..n {
                let source = (rank + n - step) % n;
                let pair = layout.pair_bytes[source as usize * size + rank as usize];
                if pair != 0
                    && inbound.insert(key(CollectivePhase::AllToAll, 0, source, step), dedup)
                {
                    add(pair)?;
                }
            }
            if !sends {
                let after = semantic.after.as_ref();
                let ranks = AfterRanks::new(plan, after, &semantic.sources);
                let host = semantic.sources[rank as usize];
                add(follow_groups(
                    plan,
                    (after, &ranks, direct),
                    (rank, host),
                    (direct, true),
                    local,
                    inbound,
                )?)?;
            }
        }
        CollectiveAlgorithm::SendRecv => {
            let message = key(CollectivePhase::SendRecv, 0, 0, 1);
            if rank == 0 {
                local.insert(message, dedup);
            } else {
                let Termination::Bytes(total_bytes) = semantic.traffic.termination else {
                    unreachable!("collective validation requires byte termination")
                };
                if inbound.insert(message, dedup) {
                    add(total_bytes)?;
                }
            }
        }
        _ => {
            let phase = *ring_phases(semantic.algorithm)
                .last()
                .expect("a ring algorithm has a phase");
            let step = n - 1;
            for channel in 0..layout.channels() {
                let previous = layout.previous(channel, rank, n);
                let channel_u32 = channel as u32;
                local.insert(key(phase, channel_u32, rank, step), dedup);
                if inbound.insert(key(phase, channel_u32, previous, step), dedup) {
                    add(ring_chunk(semantic, layout, phase, channel, previous, u64::from(step)).1)?;
                }
            }
        }
    }
    Ok(bytes)
}

/// How a dependent group's ranks map onto the ranks of the groups its `after` lists (host-matched
/// `after`, the ruling on H3's C1): rank `r` of the dependent, at host `h`, waits for each listed
/// group that runs on `h`, through that group's rank at `h`. A listed group on the same hosts in
/// the same order (every group before the ruling) maps rank `r` to rank `r`; only a group on
/// other hosts gets a `(host, rank)` table, sorted by host, so equal host lists allocate nothing.
struct AfterRanks {
    /// Per listed group, in `after` order: `None` when it runs on the dependent's hosts in order.
    /// `None` overall when every listed group does.
    tables: Option<Vec<Option<HostRanks>>>,
}

/// A group's `(host, rank)` pairs, sorted by host.
type HostRanks = Vec<(u64, u32)>;

impl AfterRanks {
    fn new(plan: &StagePlan<'_>, after: Option<&AfterGroups>, hosts: &[u64]) -> Self {
        let names = after_names(after);
        let hosts_of = |name: &String| match plan.groups[name] {
            StageGroup::Compute(compute) => compute.hosts.as_slice(),
            StageGroup::Collective(collective) => collective.sources.as_slice(),
        };
        if names.iter().all(|name| hosts_of(name) == hosts) {
            return Self { tables: None };
        }
        let tables = names
            .iter()
            .map(|name| {
                let theirs = hosts_of(name);
                (theirs != hosts).then(|| {
                    let mut table = (0_u32..)
                        .zip(theirs)
                        .map(|(rank, &host)| (host, rank))
                        .collect::<Vec<_>>();
                    table.sort_unstable();
                    table
                })
            })
            .collect();
        Self {
            tables: Some(tables),
        }
    }

    /// The rank at `host` of the `index`-th listed group, given the dependent's rank there.
    fn rank(&self, index: usize, rank: u32, host: u64) -> Option<u32> {
        match self
            .tables
            .as_ref()
            .and_then(|tables| tables[index].as_ref())
        {
            None => Some(rank),
            Some(table) => table
                .binary_search_by_key(&host, |&(host, _)| host)
                .ok()
                .map(|position| table[position].1),
        }
    }
}

/// The predecessors a group's rank-`rank` stage, at `host`, takes from the groups it names in
/// `after` that run on `host`: a compute group's stage there, and each collective's completion
/// at its rank there.
fn entry_predecessors(
    plan: &StagePlan<'_>,
    after: Option<&AfterGroups>,
    ranks: &AfterRanks,
    (rank, host): (u32, u64),
) -> Result<(PredecessorKeys, PredecessorKeys, u64), CompileError> {
    let mut local = PredecessorKeys::None;
    let mut inbound = PredecessorKeys::None;
    let bytes = follow_groups(
        plan,
        (after, ranks, &[]),
        (rank, host),
        (after_names(after), false),
        &mut local,
        &mut inbound,
    )?;
    Ok((local, inbound, bytes))
}

/// Appends to `local` and `inbound` what a stage at `host` (rank `rank` of its group) waits for
/// from the groups `after` names there, skipping the groups in `skip`, and returns the inbound
/// bytes added. `direct` is the dependent's own `after` list, which a zero-send all-to-all rank's
/// release does not take twice, and `dedup` is set inside such a release, whose keys may repeat
/// (`collective_completion`).
fn follow_groups(
    plan: &StagePlan<'_>,
    (after, ranks, skip): (Option<&AfterGroups>, &AfterRanks, &[String]),
    (rank, host): (u32, u64),
    (direct, dedup): (&[String], bool),
    local: &mut PredecessorKeys,
    inbound: &mut PredecessorKeys,
) -> Result<u64, CompileError> {
    let mut bytes = 0_u64;
    for (index, name) in after_names(after).iter().enumerate() {
        if skip.contains(name) {
            continue;
        }
        let Some(rank) = ranks.rank(index, rank, host) else {
            continue;
        };
        match plan.groups[name] {
            StageGroup::Compute(predecessor) => {
                local.insert(
                    FlowKey::ComputeStage {
                        compute: compute_ordinal(plan.computes, predecessor),
                        rank,
                    },
                    dedup,
                );
            }
            StageGroup::Collective(collective) => {
                let ordinal = collective_ordinal(plan.collectives, collective);
                let more =
                    collective_completion(plan, ordinal, rank, (direct, dedup), local, inbound)?;
                bytes = bytes.checked_add(more).ok_or_else(|| {
                    CompileError::Invalid("inbound join bytes exceed u64".to_owned())
                })?;
            }
        }
    }
    Ok(bytes)
}

/// Expands one collective into its stages: rings per channel and phase, an all-to-all's ordered
/// pairs, or a send/recv's one message. Each stage waits for its rank's previous step (local) and
/// for the previous rank's (inbound); a root (step one of the first phase, every all-to-all pair,
/// the send) waits for the groups the collective follows.
fn expand_collective(
    flows: &mut Vec<FlowInput>,
    plan: &StagePlan<'_>,
    semantic: &CollectiveKey,
    collective: u64,
    duplicate_ordinal: u64,
    collective_id: u64,
) -> Result<(), CompileError> {
    let n = semantic.flow_count;
    let Termination::Bytes(total_bytes) = semantic.traffic.termination else {
        unreachable!("collective validation requires byte termination")
    };
    if !collective_has_stages(semantic) || total_bytes == 0 {
        return Ok(());
    }
    let layout = plan.layout(collective);
    let after_ranks = AfterRanks::new(plan, semantic.after.as_ref(), &semantic.sources);
    let group_size = u32::try_from(n)
        .map_err(|_| CompileError::Invalid("collective group size exceeds u32".to_owned()))?;
    let stage_key = |stage: CollectiveStagePosition| FlowKey::CollectiveStage {
        collective,
        duplicate_ordinal,
        stage,
    };
    let channel_policy = match semantic.algorithm {
        CollectiveAlgorithm::AllToAll => CollectiveChannelPolicy::AllPairs,
        CollectiveAlgorithm::SendRecv => CollectiveChannelPolicy::Pair,
        _ if semantic.channels().is_empty() => CollectiveChannelPolicy::RingNext,
        _ => CollectiveChannelPolicy::Channels,
    };
    let push = |flows: &mut Vec<FlowInput>,
                position: CollectiveStagePosition,
                target: u64,
                (chunk_offset_bytes, chunk_bytes): (u64, u64),
                (local, inbound, inbound_predecessor_bytes): (
        PredecessorKeys,
        PredecessorKeys,
        u64,
    )| {
        let mut traffic = semantic.traffic.clone();
        traffic.termination = Termination::Bytes(chunk_bytes);
        flows.push(FlowInput {
            key: stage_key(position),
            source: semantic.sources[position.rank as usize],
            target,
            priority: semantic.priority,
            traffic,
            collective: Some(Box::new(CollectiveStageInput {
                collective_id,
                algorithm: semantic.algorithm,
                group_size,
                declared_total_bytes: total_bytes,
                position,
                chunk_policy: semantic.chunk,
                channel_policy,
                chunk_offset_bytes,
                chunk_bytes,
                local,
                inbound,
                inbound_predecessor_bytes,
                notify_delay_ns: None,
            })),
            compute: None,
        });
    };
    match semantic.algorithm {
        CollectiveAlgorithm::AllToAll => {
            let size = usize::try_from(n).expect("the group size fits usize");
            flows
                .try_reserve(size * (size - 1))
                .map_err(|error| CompileError::Invalid(format!("stage table: {error}")))?;
            for rank in 0..group_size {
                let entry = entry_predecessors(
                    plan,
                    semantic.after.as_ref(),
                    &after_ranks,
                    (rank, semantic.sources[rank as usize]),
                )?;
                for step in 1..group_size {
                    let target = (rank + step) % group_size;
                    let bytes = layout.pair_bytes[rank as usize * size + target as usize];
                    if bytes == 0 {
                        continue;
                    }
                    let position = CollectiveStagePosition {
                        phase: CollectivePhase::AllToAll,
                        channel: 0,
                        rank,
                        step,
                    };
                    push(
                        flows,
                        position,
                        semantic.sources[target as usize],
                        (0, bytes),
                        entry.clone(),
                    );
                }
            }
        }
        CollectiveAlgorithm::SendRecv => {
            let position = CollectiveStagePosition {
                phase: CollectivePhase::SendRecv,
                channel: 0,
                rank: 0,
                step: 1,
            };
            let entry = entry_predecessors(
                plan,
                semantic.after.as_ref(),
                &after_ranks,
                (0, semantic.sources[0]),
            )?;
            push(
                flows,
                position,
                semantic.sources[1],
                (0, total_bytes),
                entry,
            );
        }
        _ => {
            let phases = ring_phases(semantic.algorithm);
            let channels = layout.channels();
            let stage_count = (phases.len() as u64)
                .checked_mul(channels as u64)
                .and_then(|count| count.checked_mul(n))
                .and_then(|count| count.checked_mul(n - 1))
                .ok_or_else(|| {
                    CompileError::Invalid("collective stage count exceeds u64".to_owned())
                })?;
            flows
                .try_reserve_exact(usize::try_from(stage_count).map_err(|_| {
                    CompileError::Invalid(
                        "collective stage count exceeds the platform index domain".to_owned(),
                    )
                })?)
                .map_err(|error| {
                    CompileError::Invalid(format!("collective stage table is too large: {error}"))
                })?;
            let final_step = group_size - 1;
            for (phase_index, &phase) in phases.iter().enumerate() {
                for channel in 0..channels {
                    let channel_u32 = channel as u32;
                    for rank in 0..group_size {
                        let previous = layout.previous(channel, rank, group_size);
                        let target =
                            semantic.sources[layout.next(channel, rank, group_size) as usize];
                        for step in 1..group_size {
                            let chunk =
                                ring_chunk(semantic, layout, phase, channel, rank, u64::from(step));
                            let at = |phase, rank, step| CollectiveStagePosition {
                                phase,
                                channel: channel_u32,
                                rank,
                                step,
                            };
                            let predecessors = if step > 1 {
                                (
                                    PredecessorKeys::One(stage_key(at(phase, rank, step - 1))),
                                    PredecessorKeys::One(stage_key(at(phase, previous, step - 1))),
                                    chunk.1,
                                )
                            } else if phase_index > 0 {
                                let first = phases[phase_index - 1];
                                (
                                    PredecessorKeys::One(stage_key(at(first, rank, final_step))),
                                    PredecessorKeys::One(stage_key(at(
                                        first, previous, final_step,
                                    ))),
                                    chunk.1,
                                )
                            } else {
                                entry_predecessors(
                                    plan,
                                    semantic.after.as_ref(),
                                    &after_ranks,
                                    (rank, semantic.sources[rank as usize]),
                                )?
                            };
                            push(flows, at(phase, rank, step), target, chunk, predecessors);
                        }
                    }
                }
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

/// Resolves the provisional stage-group dependencies: every `after` names known groups, each once;
/// the dependencies are acyclic; and they match hosts (the ruling on H3's C1, design §4.2): rank
/// `r` of a group waits, at its host, for each listed group that runs there. So every listed
/// group runs on at least one host where the dependent starts (all its hosts, a Send/Recv's
/// sender alone), and every host where it starts runs at least one listed group.
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
    // Every `after` names known groups, each once, that match the dependent's hosts; a
    // collective follows at least one compute group (ruling R4).
    let dependents = collectives
        .iter()
        .map(|collective| {
            // A Send/Recv starts at its sender alone; every other collective at all its ranks.
            let starts = if collective.algorithm == CollectiveAlgorithm::SendRecv {
                &collective.sources[..1]
            } else {
                &collective.sources[..]
            };
            (
                "collective",
                collective.name.as_deref().unwrap_or("<unnamed>"),
                &collective.sources[..],
                starts,
                collective.after.as_ref(),
                true,
            )
        })
        .chain(computes.iter().map(|compute| {
            (
                "compute",
                compute.name.as_str(),
                &compute.hosts[..],
                &compute.hosts[..],
                compute.after.as_ref(),
                false,
            )
        }));
    for (kind, label, hosts, starts, after, is_collective) in dependents {
        let names = after_names(after);
        let mut follows_compute = false;
        // The sorted hosts of each listed group that does not run on exactly the dependent's
        // hosts in order (none for equal lists, so they allocate nothing).
        let mut other_hosts = Vec::<Vec<u64>>::new();
        for (index, after) in names.iter().enumerate() {
            if names[..index].contains(after) {
                return Err(CompileError::Invalid(format!(
                    "{kind} `{label}` names stage group `{after}` more than once"
                )));
            }
            let predecessor_hosts = match groups.get(after) {
                None => {
                    return Err(CompileError::Invalid(format!(
                        "{kind} `{label}` depends on unknown stage group `{after}`"
                    )));
                }
                Some(StageGroup::Compute(predecessor)) => {
                    follows_compute = true;
                    &predecessor.hosts
                }
                Some(StageGroup::Collective(collective)) => {
                    if !collective_has_stages(collective) {
                        return Err(CompileError::Invalid(format!(
                            "{kind} `{label}` depends on collective `{after}`, which has no stages"
                        )));
                    }
                    &collective.sources
                }
            };
            if predecessor_hosts.as_slice() == hosts {
                continue;
            }
            let mut sorted = predecessor_hosts.clone();
            sorted.sort_unstable();
            if !starts.iter().any(|host| sorted.binary_search(host).is_ok()) {
                return Err(CompileError::Invalid(if is_collective {
                    format!(
                        "collective `{label}` names stage group `{after}`, which runs on none of the hosts it starts at"
                    )
                } else {
                    format!(
                        "compute `{label}` names stage group `{after}`, which runs on none of its hosts"
                    )
                }));
            }
            other_hosts.push(sorted);
        }
        // Every host where the dependent starts runs a listed group (an equal list covers all).
        if !other_hosts.is_empty() && other_hosts.len() == names.len() {
            if let Some((rank, host)) = starts.iter().enumerate().find(|(_, host)| {
                !other_hosts
                    .iter()
                    .any(|sorted| sorted.binary_search(host).is_ok())
            }) {
                return Err(CompileError::Invalid(format!(
                    "{kind} `{label}` rank {rank} (host {host}) starts after no operation its `after` lists"
                )));
            }
        }
        if is_collective && !names.is_empty() && !follows_compute {
            return Err(CompileError::Unsupported(format!(
                "unsupported collective `{label}` dependency on collective `{}` alone; a collective follows at least one compute group",
                names[0]
            )));
        }
    }
    fn afters_of<'a>(group: &StageGroup<'a>) -> &'a [String] {
        match *group {
            StageGroup::Collective(collective) => after_names(collective.after.as_ref()),
            StageGroup::Compute(compute) => after_names(compute.after.as_ref()),
        }
    }
    if groups.values().all(|group| afters_of(group).len() <= 1) {
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
                match afters_of(&groups[current]).first() {
                    Some(next) => current = next,
                    None => break,
                }
            }
        }
        return Ok(groups);
    }
    // A join: a depth-first walk over the `after` edges, in name order, finds any cycle; groups
    // are indexed by their position in name order.
    let names = groups.keys().map(String::as_str).collect::<Vec<_>>();
    let afters = |index: usize| -> &[String] { afters_of(&groups[names[index]]) };
    // 0: unvisited; 1: on the walk's path; 2: finished.
    let mut state = vec![0_u8; names.len()];
    let mut stack = Vec::<(usize, usize)>::new();
    for start in 0..names.len() {
        if state[start] != 0 {
            continue;
        }
        state[start] = 1;
        stack.push((start, 0));
        while let Some((current, cursor)) = stack.last_mut() {
            let current = *current;
            if let Some(next) = afters(current).get(*cursor) {
                *cursor += 1;
                let next = names
                    .binary_search(&next.as_str())
                    .expect("group resolution checked every name");
                match state[next] {
                    1 => {
                        return Err(CompileError::Invalid(format!(
                            "stage group dependencies form a cycle through `{}`",
                            names[next]
                        )));
                    }
                    0 => {
                        state[next] = 1;
                        stack.push((next, 0));
                    }
                    _ => {}
                }
            } else {
                state[current] = 2;
                stack.pop();
            }
        }
    }
    Ok(groups)
}

/// Expands one compute group into one timer-only stage per host.
///
/// Rank r waits for the rank-r stage of each compute group it follows and for each collective's
/// completion at rank r (`collective_completion`): after one ring, its own final stage (local,
/// acknowledged) and the previous rank's (inbound, delivered in order). After several groups, or
/// after an all-to-all or a multi-channel ring, the stage is a join.
fn expand_compute(
    flows: &mut Vec<FlowInput>,
    plan: &StagePlan<'_>,
    compute: &ComputeKey,
    compute_id: u64,
) -> Result<(), CompileError> {
    let group_size = u32::try_from(compute.hosts.len())
        .expect("compute validation bounded the group size by u32");
    let after_ranks = AfterRanks::new(plan, compute.after.as_ref(), &compute.hosts);
    for (rank, &host) in (0_u32..).zip(&compute.hosts) {
        let (local, inbound, inbound_predecessor_bytes) =
            entry_predecessors(plan, compute.after.as_ref(), &after_ranks, (rank, host))?;
        flows.push(FlowInput {
            key: FlowKey::ComputeStage {
                compute: compute_id,
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
                local,
                inbound,
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
    computes: &[ComputeKey],
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
            // Channel 0 mixes nothing, so every single-ring stage keeps its seed.
            if stage.channel != 0 {
                state = mix_seed(state ^ (u64::from(stage.channel) << 32));
            }
            state = mix_seed(state ^ u64::from(stage.rank));
            state = mix_seed(state ^ u64::from(stage.step));
            state = mix_traffic_seed(state, &semantic.traffic, roce_keys);
        }
        FlowKey::ComputeStage { compute, rank } => {
            let semantic = &computes[*compute as usize];
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
    use super::{AfterGroups, CollectiveKey, ComputeKey, FlowInput, parsed_decimal};

    /// P16 H1 (ruling R1's gate): the operations' key fields must not grow the keys of ordinary
    /// collectives and compute groups, which lowering holds and copies per group. `feat/p16`
    /// (669e16b): `CollectiveKey` 248 B, `ComputeKey` 80 B. A channel ring's or a seeded
    /// all-to-all's parts are one boxed pointer (8 B; the chunk policy fits in padding), and one
    /// `after` name or several share the 24 B of the name they replaced.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn stage_group_keys_keep_their_size() {
        for (name, size, bound) in [
            ("CollectiveKey", std::mem::size_of::<CollectiveKey>(), 256),
            ("ComputeKey", std::mem::size_of::<ComputeKey>(), 80),
            (
                "Option<AfterGroups>",
                std::mem::size_of::<Option<AfterGroups>>(),
                24,
            ),
        ] {
            assert!(size <= bound, "{name} grew to {size} B, above {bound} B");
        }
    }

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

    /// `DISTRIBUTION_FIELDS` (the allocation-free fast path's table) equals the shared strict
    /// schema `DistributionInfo`: each type takes exactly its listed fields, refuses every other
    /// type's, and the schema has no type the table lacks.
    #[test]
    fn distribution_fields_match_the_shared_schema() {
        // The fast path reads the text between an inline table's braces.
        let exact = |table: &str| {
            super::distribution_keys_are_exact(
                table
                    .strip_prefix('{')
                    .and_then(|t| t.strip_suffix('}'))
                    .expect("braces"),
            )
        };
        let parse = |table: &str| {
            #[derive(serde::Deserialize)]
            struct Probe {
                #[allow(dead_code)]
                distribution: crate::scenario::DistributionInfo,
            }
            toml::from_str::<Probe>(&format!("distribution = {table}"))
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        let every_field = super::DISTRIBUTION_FIELDS
            .iter()
            .flat_map(|(_, fields, _)| fields.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        for (kind, fields, integer) in super::DISTRIBUTION_FIELDS {
            // Values: the fast path says yes only where the schema does.
            for value in [
                "1",
                "-2",
                "+3",
                "1_000",
                "1.5",
                "-0.25",
                "1e3",
                "2.5E-3",
                "inf",
                "-nan",
                "0x10",
                "0o7",
                "true",
                "\"1\"",
                "[1]",
                "{ a = 1 }",
                "1979-05-27",
                "07:32:00",
            ] {
                let table = fields
                    .iter()
                    .map(|field| format!(", {field} = {value}"))
                    .collect::<String>();
                let table = format!("{{ type = \"{kind}\"{table} }}");
                if exact(&table) {
                    parse(&table).unwrap_or_else(|error| panic!("{table}: {error}"));
                }
                assert_eq!(
                    super::plain_number(value, integer),
                    exact(&table),
                    "{table}"
                );
            }
            let own = fields
                .iter()
                .map(|field| format!(", {field} = 1"))
                .collect::<String>();
            parse(&format!("{{ type = \"{kind}\"{own} }}"))
                .unwrap_or_else(|error| panic!("{kind} takes {fields:?}: {error}"));
            assert!(exact(&format!("{{ type = \"{kind}\"{own} }}")));
            for other in every_field.iter().filter(|field| !fields.contains(field)) {
                let table = format!("{{ type = \"{kind}\"{own}, {other} = 1 }}");
                assert!(parse(&table).is_err(), "{kind} refuses {other}");
                assert!(!exact(&table));
            }
        }
        let unknown = parse("{ type = \"Unknown\" }").expect_err("an unknown type");
        for (kind, ..) in super::DISTRIBUTION_FIELDS {
            assert!(unknown.contains(&format!("`{kind}`")), "{unknown}");
        }
        assert_eq!(
            unknown.matches('`').count(),
            2 * (super::DISTRIBUTION_FIELDS.len() + 1)
        );
    }
}

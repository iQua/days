use std::fs;

use days::topos::config::{DropStrategy, LinkMode, SchedulingDiscipline, ThreadingModel};
use days::topos::route::RoutingConfig;
use serde::Deserialize;

use crate::flows::cc::CCAlgorithm;

/// Complete legacy configuration vocabulary.
///
/// Runtime subsystems deserialize partial views of the same TOML document. This strict view owns
/// both the union of their keys and the cross-field combinations, so input cannot disappear or be
/// replaced while moving between those views.
#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyConfig {
    seed: Option<usize>,
    duration: Option<f64>,
    ui_interval: Option<f64>,
    tracing_active: Option<bool>,
    tracing_interval: Option<f64>,
    threading: Option<ThreadingModel>,
    num_threads: Option<usize>,
    hot_workers: Option<toml::Value>,
    concurrency_level: Option<toml::Value>,
    log_path: Option<toml::Value>,
    report_interval: Option<toml::Value>,
    mailbox_capacity: Option<usize>,
    legacy_e5_metrics: Option<toml::Value>,
    time_quantum_ns: Option<toml::Value>,
    model_host_attachment: Option<bool>,
    edges: Option<Vec<(u32, u32)>>,
    hosts: Option<Vec<usize>>,
    topology: Option<StrictTopology>,
    switch: Option<StrictSwitch>,
    link: Option<StrictLink>,
    app_source: Option<StrictAppSource>,
    routing: Option<StrictRouting>,
    flow: Option<Vec<StrictFlow>>,
    flow_set: Option<Vec<StrictFlowSet>>,
    collective: Option<Vec<StrictCollective>>,
    collective_set: Option<Vec<StrictCollectiveSet>>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(tag = "category", deny_unknown_fields)]
enum StrictTopology {
    FatTree { fat_tree: StrictFatTree },
    Torus { torus: StrictTorus },
    Dragonfly { dragonfly: StrictDragonfly },
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictFatTree {
    k: Option<toml::Value>,
    hosts_per_edge: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTorus {
    dim: Option<toml::Value>,
    n: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictDragonfly {
    routers_per_group: Option<toml::Value>,
    global_ports_per_router: Option<toml::Value>,
    hosts_per_router: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictSwitch {
    port_rate: Option<f64>,
    capacity: Option<usize>,
    discipline: Option<SchedulingDiscipline>,
    drop: Option<DropStrategy>,
    ecn_threshold: Option<f64>,
    weights: Option<Vec<usize>>,
    priorities: Option<Vec<usize>>,
    vticks: Option<Vec<f64>>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictLink {
    mode: Option<LinkMode>,
    pfc: Option<StrictPfc>,
    propagation_ns: Option<u64>,
    propagation_tiers: Option<StrictPropagationTiers>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictPfc {
    xoff: Option<toml::Value>,
    xon: Option<toml::Value>,
    pause_quanta: Option<toml::Value>,
    buffer_capacity: Option<toml::Value>,
    refresh_interval: Option<toml::Value>,
    drain_interval: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictPropagationTiers {
    host_to_edge_ns: Option<toml::Value>,
    edge_to_aggregation_ns: Option<toml::Value>,
    aggregation_to_core_ns: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictAppSource {
    req_channel_capacity: Option<toml::Value>,
    chunk_size: Option<usize>,
    initial_delay: Option<toml::Value>,
    run_interval: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictRouting {
    policy: Option<StrictRoutingPolicy>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum StrictRoutingPolicy {
    ShortestPath,
    FatTreeEcmp,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum StrictFlowType {
    PacketDistribution,
    #[serde(rename = "TCP")]
    Tcp,
    #[cfg(feature = "dcqcn")]
    #[serde(rename = "DCQCN")]
    Dcqcn,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum StrictCollectiveType {
    Broadcast,
    Gather,
    AllReduce,
    RingAllReduce,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictFlow {
    flow_id: Option<toml::Value>,
    starts_before: Option<toml::Value>,
    starts_after: Option<toml::Value>,
    flow_type: Option<StrictFlowType>,
    priority: Option<toml::Value>,
    graph: Option<toml::Value>,
    routing: Option<RoutingConfig>,
    path: Option<toml::Value>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictFlowSet {
    first_flow_id: Option<toml::Value>,
    starts_before: Option<toml::Value>,
    starts_after: Option<toml::Value>,
    flow_type: Option<StrictFlowType>,
    flow_count: Option<usize>,
    priority: Option<toml::Value>,
    routing: Option<RoutingConfig>,
    pairing: Option<toml::Value>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCollective {
    collective_type: Option<StrictCollectiveType>,
    first_flow_id: Option<toml::Value>,
    flow_type: Option<StrictFlowType>,
    flow_count: Option<usize>,
    graph: Option<Vec<(u32, u32)>>,
    paths: Option<Vec<Vec<usize>>>,
    sources: Option<Vec<usize>>,
    sinks: Option<Vec<usize>>,
    routing: Option<RoutingConfig>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCollectiveSet {
    collective_type: Option<StrictCollectiveType>,
    collective_count: Option<usize>,
    first_flow_id: Option<toml::Value>,
    flow_type: Option<StrictFlowType>,
    flow_count: Option<usize>,
    sources: Option<Vec<Vec<usize>>>,
    sinks: Option<Vec<Vec<usize>>>,
    routing: Option<RoutingConfig>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTraffic {
    initial_delay: Option<f64>,
    duration: Option<f64>,
    size: Option<usize>,
    arr_dist: Option<StrictDistribution>,
    pkt_size_dist: Option<StrictDistribution>,
    tcp: Option<StrictTcp>,
    #[cfg(feature = "dcqcn")]
    dcqcn: Option<StrictDcqcn>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
enum StrictDistribution {
    DiscreteUniform { low: i64, high: i64 },
    Exp { lambda: f64 },
    Uniform { low: f64, high: f64 },
}

impl StrictDistribution {
    fn is_uniform(&self, low: f64, high: f64) -> bool {
        matches!(
            self,
            Self::Uniform {
                low: configured_low,
                high: configured_high,
            } if *configured_low == low && *configured_high == high
        )
    }

    fn is_positive_fixed_integral(&self) -> bool {
        match self {
            Self::DiscreteUniform { low, high } if low == high => {
                usize::try_from(*low).ok().is_some_and(|value| value > 0)
            }
            Self::Uniform { low, high } if low == high && low.is_finite() && low.fract() == 0.0 => {
                let value = *low as usize;
                value > 0 && value as f64 == *low
            }
            _ => false,
        }
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTcp {
    cc_algorithm: Option<CCAlgorithm>,
    ecn: Option<toml::Value>,
    cubic: Option<StrictCubic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCubic {
    beta: Option<toml::Value>,
    c: Option<toml::Value>,
    fast_convergence: Option<toml::Value>,
}

#[cfg(feature = "dcqcn")]
#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictDcqcn {
    rate_gbps: Option<f64>,
    min_rate_gbps: Option<f64>,
    max_rate_gbps: Option<f64>,
    g: Option<f64>,
    ai_rate_gbps: Option<f64>,
    hai_rate_gbps: Option<f64>,
    mi_factor: Option<f64>,
    rtt_ns: Option<f64>,
    cnp_interval_ns: Option<f64>,
    pacing_interval_ns: Option<f64>,
    cnp_priority: Option<u8>,
}

fn unsupported(key: &str, reason: &str) -> String {
    format!("unsupported legacy configuration key `{key}`: {reason}")
}

fn validate_distribution(
    key: &str,
    distribution: &StrictDistribution,
    packet_size: bool,
) -> Result<(), String> {
    match distribution {
        StrictDistribution::DiscreteUniform { low, high } => {
            if low > high {
                return Err(unsupported(
                    key,
                    "distribution lower bound exceeds upper bound",
                ));
            }
            if packet_size && *low < 1 {
                return Err(unsupported(
                    key,
                    "packet-size distribution must produce positive byte counts",
                ));
            }
            if !packet_size && *low < 0 {
                return Err(unsupported(
                    key,
                    "arrival distribution must not produce negative intervals",
                ));
            }
        }
        StrictDistribution::Exp { lambda } => {
            if !lambda.is_finite() || *lambda <= 0.0 {
                return Err(unsupported(
                    key,
                    "exponential distribution lambda must be finite and positive",
                ));
            }
        }
        StrictDistribution::Uniform { low, high } => {
            if !low.is_finite() || !high.is_finite() || low > high {
                return Err(unsupported(
                    key,
                    "uniform distribution bounds must be finite and ordered",
                ));
            }
            if packet_size && *low <= 0.0 {
                return Err(unsupported(
                    key,
                    "packet-size distribution must produce positive byte counts",
                ));
            }
            if !packet_size && *low < 0.0 {
                return Err(unsupported(
                    key,
                    "arrival distribution must not produce negative intervals",
                ));
            }
        }
    }
    Ok(())
}

fn validate_routing(
    owner: &str,
    routing: Option<&RoutingConfig>,
    has_path: bool,
) -> Result<(), String> {
    let path_key = match owner {
        "flow" => Some("flow.path"),
        "collective" => Some("collective.paths"),
        _ => None,
    };
    match (routing, has_path) {
        (Some(RoutingConfig::PathFromConfig), false) => {
            let reason = path_key.map_or_else(
                || "`PathFromConfig` is unsupported because this configuration form has no path field".to_owned(),
                |path_key| format!("`PathFromConfig` requires `{path_key}`"),
            );
            Err(unsupported(&format!("{owner}.routing"), &reason))
        }
        (Some(RoutingConfig::ShortestPath | RoutingConfig::ECMP), true) => Err(unsupported(
            &format!("{owner}.routing` + `{}", path_key.expect("path owner")),
            "a configured path overrides the requested routing algorithm",
        )),
        _ => Ok(()),
    }
}

fn validate_traffic(
    owner: &str,
    flow_type: Option<StrictFlowType>,
    traffic: Option<&StrictTraffic>,
) -> Result<(), String> {
    let Some(traffic) = traffic else {
        return Ok(());
    };

    if traffic
        .initial_delay
        .is_some_and(|delay| !delay.is_finite() || delay < 0.0)
    {
        return Err(unsupported(
            &format!("{owner}.traffic.initial_delay"),
            "initial delay must be finite and non-negative",
        ));
    }
    if traffic
        .duration
        .is_some_and(|duration| !duration.is_finite() || duration < 0.0)
    {
        return Err(unsupported(
            &format!("{owner}.traffic.duration"),
            "duration termination must be finite and non-negative",
        ));
    }
    if let Some(distribution) = &traffic.arr_dist {
        validate_distribution(&format!("{owner}.traffic.arr_dist"), distribution, false)?;
    }
    if let Some(distribution) = &traffic.pkt_size_dist {
        validate_distribution(
            &format!("{owner}.traffic.pkt_size_dist"),
            distribution,
            true,
        )?;
    }

    match (traffic.size.is_some(), traffic.duration.is_some()) {
        (true, true) => {
            return Err(unsupported(
                &format!("{owner}.traffic.size` + `{owner}.traffic.duration"),
                "exactly one flow termination condition may be specified",
            ));
        }
        (false, false) => {
            return Err(unsupported(
                &format!("{owner}.traffic.size` or `{owner}.traffic.duration"),
                "one flow termination condition is required",
            ));
        }
        _ => {}
    }

    match flow_type {
        Some(StrictFlowType::PacketDistribution) => {
            if traffic.tcp.is_some() {
                return Err(unsupported(
                    &format!("{owner}.traffic.tcp"),
                    "the table is inactive for `flow_type = \"PacketDistribution\"`",
                ));
            }
            #[cfg(feature = "dcqcn")]
            if traffic.dcqcn.is_some() {
                return Err(unsupported(
                    &format!("{owner}.traffic.dcqcn"),
                    "the table is inactive for `flow_type = \"PacketDistribution\"`",
                ));
            }
        }
        Some(StrictFlowType::Tcp) => {
            let tcp = traffic.tcp.as_ref().ok_or_else(|| {
                unsupported(
                    &format!("{owner}.traffic.tcp"),
                    "the table is required for `flow_type = \"TCP\"`",
                )
            })?;
            #[cfg(feature = "dcqcn")]
            if traffic.dcqcn.is_some() {
                return Err(unsupported(
                    &format!("{owner}.traffic.dcqcn"),
                    "the table is inactive for `flow_type = \"TCP\"`",
                ));
            }
            if tcp.cubic.is_some() && tcp.cc_algorithm != Some(CCAlgorithm::TCPCubic) {
                return Err(unsupported(
                    &format!("{owner}.traffic.tcp.cubic"),
                    "the table requires `cc_algorithm = \"TCPCubic\"`",
                ));
            }
            if traffic.size.is_some()
                && traffic.arr_dist.as_ref().is_some_and(|distribution| {
                    !(distribution.is_uniform(1.0, 1.0)
                        || owner == "collective" && distribution.is_uniform(3.0, 4.0))
                })
            {
                return Err(unsupported(
                    &format!("{owner}.traffic.arr_dist"),
                    "byte-bounded TCP does not sample arrival intervals; only the retained legacy compatibility token is accepted",
                ));
            }
            if traffic
                .pkt_size_dist
                .as_ref()
                .is_some_and(|distribution| !distribution.is_positive_fixed_integral())
            {
                return Err(unsupported(
                    &format!("{owner}.traffic.pkt_size_dist"),
                    "TCP `pkt_size_dist` must be a positive fixed integral MSS",
                ));
            }
        }
        #[cfg(feature = "dcqcn")]
        Some(StrictFlowType::Dcqcn) => {
            if traffic.tcp.is_some() {
                return Err(unsupported(
                    &format!("{owner}.traffic.tcp"),
                    "the table is inactive for `flow_type = \"DCQCN\"`",
                ));
            }
            let dcqcn = traffic.dcqcn.as_ref().ok_or_else(|| {
                unsupported(
                    &format!("{owner}.traffic.dcqcn"),
                    "the table is required for `flow_type = \"DCQCN\"`",
                )
            })?;
            if traffic
                .arr_dist
                .as_ref()
                .is_some_and(|distribution| !distribution.is_uniform(1.0, 1.0))
            {
                return Err(unsupported(
                    &format!("{owner}.traffic.arr_dist"),
                    "DCQCN does not sample arrival intervals; only the retained legacy compatibility token `Uniform [1, 1]` is accepted",
                ));
            }
            validate_dcqcn(&format!("{owner}.traffic.dcqcn"), dcqcn)?;
        }
        None => {}
    }

    Ok(())
}

#[cfg(feature = "dcqcn")]
fn validate_dcqcn(owner: &str, dcqcn: &StrictDcqcn) -> Result<(), String> {
    for (field, value) in [
        ("rate_gbps", dcqcn.rate_gbps),
        ("min_rate_gbps", dcqcn.min_rate_gbps),
        ("max_rate_gbps", dcqcn.max_rate_gbps),
    ] {
        if value.is_some_and(|value| !value.is_finite() || value <= 0.0) {
            return Err(unsupported(
                &format!("{owner}.{field}"),
                "DCQCN rates must be finite and positive",
            ));
        }
    }
    if let (Some(minimum), Some(initial), Some(maximum)) =
        (dcqcn.min_rate_gbps, dcqcn.rate_gbps, dcqcn.max_rate_gbps)
    {
        if minimum > initial || initial > maximum {
            return Err(unsupported(
                &format!("{owner}.min_rate_gbps/rate_gbps/max_rate_gbps"),
                "DCQCN rates must satisfy min <= initial <= max",
            ));
        }
    }
    for (field, value) in [
        ("ai_rate_gbps", dcqcn.ai_rate_gbps),
        ("hai_rate_gbps", dcqcn.hai_rate_gbps),
    ] {
        if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
            return Err(unsupported(
                &format!("{owner}.{field}"),
                "DCQCN additive rates must be finite and non-negative",
            ));
        }
    }
    for (field, value) in [("g", dcqcn.g), ("mi_factor", dcqcn.mi_factor)] {
        if value.is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value)) {
            return Err(unsupported(
                &format!("{owner}.{field}"),
                "DCQCN factors must be finite and within 0..=1",
            ));
        }
    }
    if dcqcn
        .rtt_ns
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        return Err(unsupported(
            &format!("{owner}.rtt_ns"),
            "RTT must be finite and positive instead of relying on runtime clamping",
        ));
    }
    for (field, value) in [
        ("cnp_interval_ns", dcqcn.cnp_interval_ns),
        ("pacing_interval_ns", dcqcn.pacing_interval_ns),
    ] {
        if value.is_some_and(|value| !value.is_finite() || value < 0.0) {
            return Err(unsupported(
                &format!("{owner}.{field}"),
                "interval must be finite and non-negative instead of relying on runtime clamping",
            ));
        }
    }
    if dcqcn.cnp_priority.is_some_and(|priority| priority > 7) {
        return Err(unsupported(
            &format!("{owner}.cnp_priority"),
            "priority must be within 0..=7",
        ));
    }
    Ok(())
}

fn validate_switch(switch: &StrictSwitch) -> Result<(), String> {
    if switch
        .ecn_threshold
        .is_some_and(|threshold| !threshold.is_finite() || threshold <= 0.0 || threshold > 1.0)
    {
        return Err(unsupported(
            "switch.ecn_threshold",
            "ECN threshold must be finite and within 0 < threshold <= 1 instead of relying on runtime normalization",
        ));
    }
    let neutral_weights = switch.weights.as_deref() == Some(&[1]);
    let inactive_weights = |discipline: &str| {
        unsupported(
            "switch.weights",
            &format!(
                "the field is inactive for `{discipline}`; only the historical neutral `[1]` token is accepted"
            ),
        )
    };

    match switch.discipline {
        Some(SchedulingDiscipline::FIFO) => {
            if switch.weights.is_some() && !neutral_weights {
                return Err(inactive_weights("FIFO"));
            }
            if switch.priorities.is_some() {
                return Err(unsupported(
                    "switch.priorities",
                    "the field is inactive for `discipline = \"FIFO\"`",
                ));
            }
            if switch.vticks.is_some() {
                return Err(unsupported(
                    "switch.vticks",
                    "the field is inactive for `discipline = \"FIFO\"`",
                ));
            }
        }
        Some(SchedulingDiscipline::DRR | SchedulingDiscipline::WFQ | SchedulingDiscipline::WRR) => {
            if switch
                .weights
                .as_ref()
                .is_none_or(|weights| weights.is_empty())
            {
                return Err(unsupported(
                    "switch.weights",
                    "a non-empty list is required by the selected weighted discipline",
                ));
            }
            if switch.priorities.is_some() {
                return Err(unsupported(
                    "switch.priorities",
                    "the field is inactive for a weighted discipline",
                ));
            }
            if switch.vticks.is_some() {
                return Err(unsupported(
                    "switch.vticks",
                    "the field is inactive for a weighted discipline",
                ));
            }
        }
        Some(SchedulingDiscipline::SP) => {
            if switch.weights.is_some() && !neutral_weights {
                return Err(inactive_weights("SP"));
            }
            if switch.vticks.is_some() {
                return Err(unsupported(
                    "switch.vticks",
                    "the field is inactive for `discipline = \"SP\"`",
                ));
            }
            if switch
                .priorities
                .as_ref()
                .is_some_and(|priorities| priorities.is_empty())
            {
                return Err(unsupported(
                    "switch.priorities",
                    "an explicitly configured priority list must not be empty",
                ));
            }
        }
        Some(SchedulingDiscipline::VirtualClock) => {
            if switch.weights.is_some() && !neutral_weights {
                return Err(inactive_weights("VirtualClock"));
            }
            if switch.priorities.is_some() {
                return Err(unsupported(
                    "switch.priorities",
                    "the field is inactive for `discipline = \"VirtualClock\"`",
                ));
            }
            if switch
                .vticks
                .as_ref()
                .is_some_and(|vticks| vticks.is_empty())
            {
                return Err(unsupported(
                    "switch.vticks",
                    "an explicitly configured virtual-tick list must not be empty",
                ));
            }
        }
        None => {}
    }

    if switch.ecn_threshold.is_some()
        && !matches!(switch.drop.as_ref(), Some(DropStrategy::EcnThreshold))
    {
        return Err(unsupported(
            "switch.ecn_threshold",
            "the field requires `drop = \"ECN_THRESHOLD\"`",
        ));
    }

    Ok(())
}

fn validate_collective(
    index: usize,
    collective: &StrictCollective,
    configured_hosts: Option<&[usize]>,
) -> Result<(), String> {
    validate_routing(
        "collective",
        collective.routing.as_ref(),
        collective.paths.is_some(),
    )?;
    validate_traffic(
        "collective",
        collective.flow_type,
        collective.traffic.as_ref(),
    )?;

    if collective.sources.is_some() != collective.sinks.is_some() {
        return Err(unsupported(
            "collective.sources` + `collective.sinks",
            &format!("collective entry {index} must specify both endpoint lists or neither"),
        ));
    }
    let flow_count = collective.flow_count;
    if let (Some(sources), Some(sinks)) = (&collective.sources, &collective.sinks) {
        if sources.len() != sinks.len() || flow_count.is_some_and(|count| count != sources.len()) {
            return Err(unsupported(
                "collective.sources` + `collective.sinks` + `collective.flow_count",
                &format!("collective entry {index} endpoint counts must agree"),
            ));
        }
    }
    if let Some(paths) = &collective.paths {
        if paths.iter().any(|path| path.len() < 2) {
            return Err(unsupported(
                "collective.paths",
                &format!("collective entry {index} paths must contain at least two nodes"),
            ));
        }
        if flow_count.is_some_and(|count| count != paths.len()) {
            return Err(unsupported(
                "collective.paths` + `collective.flow_count",
                &format!("collective entry {index} path count must equal flow count"),
            ));
        }
    }
    if let Some(graph) = &collective.graph {
        if configured_hosts.is_some_and(|hosts| {
            graph.iter().any(|&(source, sink)| {
                !hosts.contains(&(source as usize)) || !hosts.contains(&(sink as usize))
            })
        }) {
            return Err(unsupported(
                "collective.graph",
                &format!("collective entry {index} graph endpoints must name configured hosts"),
            ));
        }
        if flow_count.is_some_and(|count| count != graph.len()) {
            return Err(unsupported(
                "collective.graph` + `collective.flow_count",
                &format!("collective entry {index} graph edge count must equal flow count"),
            ));
        }
        if let (Some(sources), Some(sinks)) = (&collective.sources, &collective.sinks) {
            let endpoints_match = graph.iter().zip(sources.iter().zip(sinks)).all(
                |(&(source, sink), (&configured_source, &configured_sink))| {
                    source as usize == configured_source && sink as usize == configured_sink
                },
            );
            if !endpoints_match {
                return Err(unsupported(
                    "collective.graph` + `collective.sources` + `collective.sinks",
                    &format!("collective entry {index} graph edges must match explicit endpoints"),
                ));
            }
        }
        if let Some(paths) = &collective.paths {
            let endpoints_match = graph.iter().zip(paths).all(|(&(source, sink), path)| {
                path.first() == Some(&(source as usize)) && path.last() == Some(&(sink as usize))
            });
            if !endpoints_match {
                return Err(unsupported(
                    "collective.graph` + `collective.paths",
                    &format!("collective entry {index} graph edges must match path endpoints"),
                ));
            }
        }
    }
    if let (Some(sources), Some(sinks), Some(paths)) =
        (&collective.sources, &collective.sinks, &collective.paths)
    {
        let endpoints_match =
            paths
                .iter()
                .zip(sources.iter().zip(sinks))
                .all(|(path, (&source, &sink))| {
                    path.first() == Some(&source) && path.last() == Some(&sink)
                });
        if !endpoints_match {
            return Err(unsupported(
                "collective.paths` + `collective.sources` + `collective.sinks",
                &format!("collective entry {index} paths must match explicit endpoints"),
            ));
        }
    }

    Ok(())
}

fn validate_collective_set(index: usize, set: &StrictCollectiveSet) -> Result<(), String> {
    validate_routing("collective_set", set.routing.as_ref(), false)?;
    validate_traffic("collective_set", set.flow_type, set.traffic.as_ref())?;
    if set.sources.is_some() != set.sinks.is_some() {
        return Err(unsupported(
            "collective_set.sources` + `collective_set.sinks",
            &format!("collective-set entry {index} must specify both endpoint lists or neither"),
        ));
    }
    if let (Some(sources), Some(sinks)) = (&set.sources, &set.sinks) {
        if sources.len() != sinks.len() {
            return Err(unsupported(
                "collective_set.sources` + `collective_set.sinks",
                &format!("collective-set entry {index} endpoint set counts must agree"),
            ));
        }
        if set
            .collective_count
            .is_some_and(|count| count != sources.len())
        {
            return Err(unsupported(
                "collective_set.sources` + `collective_set.sinks` + `collective_set.collective_count",
                &format!("collective-set entry {index} endpoint set count must agree"),
            ));
        }
        for (member, (member_sources, member_sinks)) in sources.iter().zip(sinks).enumerate() {
            if member_sources.is_empty()
                || member_sinks.is_empty()
                || member_sources.len() != member_sinks.len()
                || set
                    .flow_count
                    .is_some_and(|count| count != member_sources.len())
            {
                return Err(unsupported(
                    "collective_set.sources` + `collective_set.sinks` + `collective_set.flow_count",
                    &format!(
                        "collective-set entry {index} member {member} endpoint counts must agree"
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate(file_path: &str) -> Result<(), String> {
    let content = fs::read_to_string(file_path)
        .map_err(|error| format!("failed to read legacy configuration: {error}"))?;
    let config: LegacyConfig = toml::from_str(&content)
        .map_err(|error| format!("unsupported legacy configuration input: {error}"))?;

    if config
        .duration
        .is_some_and(|duration| !duration.is_finite() || duration < 0.0)
    {
        return Err(unsupported(
            "duration",
            "simulation duration must be finite and non-negative",
        ));
    }
    if config
        .ui_interval
        .is_some_and(|interval| !interval.is_finite() || interval <= 0.0)
    {
        return Err(unsupported(
            "ui_interval",
            "UI interval must be finite and positive",
        ));
    }
    if config.tracing_interval.is_some() && config.tracing_active != Some(true) {
        return Err(unsupported(
            "tracing_interval` + `tracing_active",
            "tracing_interval is inactive unless tracing_active = true",
        ));
    }
    if config
        .tracing_interval
        .is_some_and(|interval| !interval.is_finite() || interval <= 0.0)
    {
        return Err(unsupported(
            "tracing_interval",
            "tracing interval must be finite and positive",
        ));
    }

    if config.seed == Some(0) {
        return Err(unsupported(
            "seed",
            "zero selects nondeterministic entropy rather than the requested seed",
        ));
    }

    if config.topology.is_some() {
        if config.edges.is_some() {
            return Err(unsupported(
                "edges",
                "custom graph keys cannot accompany `[topology]`",
            ));
        }
        if config.hosts.is_some() {
            return Err(unsupported(
                "hosts",
                "custom graph keys cannot accompany `[topology]`",
            ));
        }
    }

    if config
        .mailbox_capacity
        .is_some_and(|capacity| capacity > usize::MAX / 2 + 1)
    {
        return Err(unsupported(
            "mailbox_capacity",
            "value exceeds the runtime's maximum representable mailbox capacity",
        ));
    }
    match (config.threading, config.num_threads) {
        (None, Some(_)) => {
            return Err(unsupported(
                "num_threads",
                "the key requires an explicit `threading` model",
            ));
        }
        (Some(ThreadingModel::Single), Some(threads)) if threads != 1 => {
            return Err(unsupported(
                "num_threads",
                "`threading = \"single\"` requires exactly one thread",
            ));
        }
        (Some(ThreadingModel::Multiple), Some(0)) => {
            return Err(unsupported("num_threads", "thread count must be positive"));
        }
        (Some(ThreadingModel::Multiple), Some(threads)) if threads > usize::BITS as usize => {
            return Err(unsupported(
                "num_threads",
                "value exceeds the runtime's maximum worker count",
            ));
        }
        _ => {}
    }

    if let Some(app_source) = &config.app_source {
        if app_source.chunk_size.is_some() {
            return Err(unsupported(
                "app_source.chunk_size",
                "legacy does not implement configurable application chunking",
            ));
        }
        let app_source_is_active = config.collective.as_ref().is_some_and(|collectives| {
            collectives.iter().any(|collective| {
                collective.flow_type == Some(StrictFlowType::Tcp)
                    && matches!(
                        collective.collective_type,
                        Some(StrictCollectiveType::Broadcast | StrictCollectiveType::RingAllReduce)
                    )
            })
        }) || config.collective_set.as_ref().is_some_and(|sets| {
            sets.iter().any(|set| {
                set.flow_type == Some(StrictFlowType::Tcp)
                    && matches!(
                        set.collective_type,
                        Some(StrictCollectiveType::Broadcast | StrictCollectiveType::RingAllReduce)
                    )
            })
        });
        if !app_source_is_active {
            return Err(unsupported(
                "app_source",
                "the table is active only for TCP Broadcast or RingAllReduce collectives",
            ));
        }
    }

    if let Some(switch) = &config.switch {
        validate_switch(switch)?;
    }

    if config.routing.is_some() {
        if config.flow.as_ref().is_some_and(|flows| {
            flows
                .iter()
                .any(|flow| flow.routing.is_some() || flow.path.is_some())
        }) {
            return Err(unsupported(
                "routing` + `flow.routing/path",
                "conflict: root routing cannot be combined with per-flow routing or paths",
            ));
        }
        if config
            .flow_set
            .as_ref()
            .is_some_and(|sets| sets.iter().any(|flow_set| flow_set.routing.is_some()))
        {
            return Err(unsupported(
                "routing` + `flow_set.routing",
                "conflict: root routing cannot be combined with per-flow-set routing",
            ));
        }
        if config
            .collective
            .as_ref()
            .is_some_and(|collectives| !collectives.is_empty())
            || config
                .collective_set
                .as_ref()
                .is_some_and(|sets| !sets.is_empty())
        {
            return Err(unsupported(
                "routing` + `collective/collective_set",
                "root routing is not implemented for legacy collectives",
            ));
        }
    }

    if let Some(flows) = &config.flow {
        for flow in flows {
            validate_routing("flow", flow.routing.as_ref(), flow.path.is_some())?;
            validate_traffic("flow", flow.flow_type, flow.traffic.as_ref())?;
        }
    }
    if let Some(flow_sets) = &config.flow_set {
        for flow_set in flow_sets {
            validate_routing("flow_set", flow_set.routing.as_ref(), false)?;
            validate_traffic("flow_set", flow_set.flow_type, flow_set.traffic.as_ref())?;
        }
    }
    if let Some(collectives) = &config.collective {
        for (index, collective) in collectives.iter().enumerate() {
            validate_collective(index, collective, config.hosts.as_deref())?;
        }
    }
    if let Some(collective_sets) = &config.collective_set {
        for (index, collective_set) in collective_sets.iter().enumerate() {
            validate_collective_set(index, collective_set)?;
        }
    }

    if let Some(link) = &config.link {
        if config.model_host_attachment == Some(false) && link.propagation_ns.is_some() {
            return Err(unsupported(
                "model_host_attachment` + `link.propagation_ns",
                "explicitly disabling host attachment stages conflicts with scalar propagation",
            ));
        }
        if link.propagation_tiers.is_some() {
            return Err(unsupported(
                "link.propagation_tiers",
                "legacy implements only uniform `link.propagation_ns`",
            ));
        }
        if link.pfc.is_some() && link.mode != Some(LinkMode::Pfc) {
            return Err(unsupported(
                "link.pfc",
                "the table requires `link.mode = \"Pfc\"`",
            ));
        }
        if link.mode == Some(LinkMode::Pfc) {
            if link.propagation_ns.is_some() {
                return Err(unsupported(
                    "link.propagation_ns",
                    "legacy PFC links do not implement propagation delay",
                ));
            }
            #[cfg(not(feature = "l2_pfc"))]
            return Err(unsupported(
                "link.mode",
                "`Pfc` requires rebuilding with feature `l2_pfc`",
            ));
        }
    }

    Ok(())
}

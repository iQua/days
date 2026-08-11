use std::fs;

use days::topos::config::LinkMode;
use serde::Deserialize;

/// Complete legacy configuration vocabulary.
///
/// Runtime subsystems deserialize partial views of the same TOML document. This strict view owns
/// the union of their keys so an input cannot disappear between those views. Field values remain
/// under the runtime parsers' authority; this view audits ownership and nesting only.
#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyConfig {
    seed: Option<toml::Value>,
    duration: Option<toml::Value>,
    ui_interval: Option<toml::Value>,
    tracing_active: Option<toml::Value>,
    tracing_interval: Option<toml::Value>,
    threading: Option<toml::Value>,
    num_threads: Option<toml::Value>,
    hot_workers: Option<toml::Value>,
    concurrency_level: Option<toml::Value>,
    log_path: Option<toml::Value>,
    report_interval: Option<toml::Value>,
    mailbox_capacity: Option<toml::Value>,
    legacy_e5_metrics: Option<toml::Value>,
    time_quantum_ns: Option<toml::Value>,
    model_host_attachment: Option<toml::Value>,
    edges: Option<toml::Value>,
    hosts: Option<toml::Value>,
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
    port_rate: Option<toml::Value>,
    capacity: Option<toml::Value>,
    discipline: Option<toml::Value>,
    drop: Option<toml::Value>,
    ecn_threshold: Option<toml::Value>,
    weights: Option<toml::Value>,
    priorities: Option<toml::Value>,
    vticks: Option<toml::Value>,
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
    chunk_size: Option<toml::Value>,
    initial_delay: Option<toml::Value>,
    run_interval: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictRouting {
    policy: Option<toml::Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictFlow {
    flow_id: Option<toml::Value>,
    starts_before: Option<toml::Value>,
    starts_after: Option<toml::Value>,
    flow_type: Option<toml::Value>,
    priority: Option<toml::Value>,
    graph: Option<toml::Value>,
    routing: Option<toml::Value>,
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
    flow_type: Option<toml::Value>,
    flow_count: Option<toml::Value>,
    priority: Option<toml::Value>,
    routing: Option<toml::Value>,
    pairing: Option<toml::Value>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCollective {
    collective_type: Option<toml::Value>,
    first_flow_id: Option<toml::Value>,
    flow_type: Option<toml::Value>,
    flow_count: Option<toml::Value>,
    graph: Option<toml::Value>,
    paths: Option<toml::Value>,
    sources: Option<toml::Value>,
    sinks: Option<toml::Value>,
    routing: Option<toml::Value>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCollectiveSet {
    collective_type: Option<toml::Value>,
    collective_count: Option<toml::Value>,
    first_flow_id: Option<toml::Value>,
    flow_type: Option<toml::Value>,
    flow_count: Option<toml::Value>,
    sources: Option<toml::Value>,
    sinks: Option<toml::Value>,
    routing: Option<toml::Value>,
    traffic: Option<StrictTraffic>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTraffic {
    initial_delay: Option<toml::Value>,
    duration: Option<toml::Value>,
    size: Option<toml::Value>,
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
    DiscreteUniform { low: toml::Value, high: toml::Value },
    Exp { lambda: toml::Value },
    Uniform { low: toml::Value, high: toml::Value },
}

#[allow(dead_code)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictTcp {
    cc_algorithm: Option<toml::Value>,
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
    rate_gbps: Option<toml::Value>,
    min_rate_gbps: Option<toml::Value>,
    max_rate_gbps: Option<toml::Value>,
    g: Option<toml::Value>,
    ai_rate_gbps: Option<toml::Value>,
    hai_rate_gbps: Option<toml::Value>,
    mi_factor: Option<toml::Value>,
    rtt_ns: Option<toml::Value>,
    cnp_interval_ns: Option<toml::Value>,
    pacing_interval_ns: Option<toml::Value>,
    cnp_priority: Option<toml::Value>,
}

fn unsupported(key: &str, reason: &str) -> String {
    format!("unsupported legacy configuration key `{key}`: {reason}")
}

pub(crate) fn validate(file_path: &str) -> Result<(), String> {
    let content = fs::read_to_string(file_path)
        .map_err(|error| format!("failed to read legacy configuration: {error}"))?;
    let config: LegacyConfig = toml::from_str(&content)
        .map_err(|error| format!("unsupported legacy configuration input: {error}"))?;

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

    if let Some(link) = config.link {
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

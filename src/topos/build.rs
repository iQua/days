//! Topology builders that convert TOML configs into graph structures.

use std::fs;

use log::{debug, info};

use petgraph::graph::{NodeIndex, UnGraph};
use rand::prelude::{IndexedRandom, SliceRandom};
use rand::rngs::SmallRng;
use serde::Deserialize;
use thiserror::Error;

use crate::topos::config::{Config, DragonflyConfig, FatTreeConfig, TopoCategory, TorusConfig};

/// Structural identity of a built topology.
///
/// Lowering needs more than the bare graph to answer structural questions — which layer a link
/// belongs to, how many groups a dragonfly has. The builder is the only place that knows, so it
/// says so rather than letting later stages re-derive it from node numbering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopologyProfile {
    FatTree {
        edge_switches: u64,
        aggregation_switches: u64,
        core_switches: u64,
    },
    Torus,
    Dragonfly {
        groups: u64,
        routers_per_group: u64,
    },
    Custom,
}

/// Deterministic endpoint-pairing policy for a flow set (T21/P12).
///
/// `Random` is the standing behaviour: endpoints are drawn from the scenario's endpoint RNG. The
/// structural policies draw nothing — they are pure functions of the host attachment grid — and so
/// leave the RNG stream of any later `Random` flow set exactly where they found it.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
pub enum PairingPolicy {
    #[default]
    Random,
    /// Host at rack ordinal *o* of attachment switch *s* sends to ordinal *o* of switch
    /// *(s + S/2) mod S*. On a fat tree this is the cross-pod permutation matrix GeDES's
    /// `BuildTCPConnections` builds (host *i* to host *i + N/2* under its switch-major numbering).
    SwitchOffsetHalf,
    /// Host at rack ordinal *o* of attachment switch *s* sends to ordinal *(o + 1) mod H* of the
    /// same switch: a rack-local permutation that never leaves the top-of-rack switch.
    SameSwitchNext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostAttachment {
    pub host_id: usize,
    pub switch_id: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostAttachments {
    entries: Vec<HostAttachment>,
    host_ids: Vec<usize>,
    distinct_source_sampling: bool,
}

impl HostAttachments {
    fn new(entries: Vec<HostAttachment>, distinct_source_sampling: bool) -> Result<Self> {
        let host_ids = entries.iter().map(|entry| entry.host_id).collect();
        Ok(Self {
            entries,
            host_ids,
            distinct_source_sampling,
        })
    }

    pub fn identity(host_ids: Vec<usize>) -> Result<Self> {
        Self::new(
            host_ids
                .into_iter()
                .map(|host_id| HostAttachment {
                    host_id,
                    switch_id: host_id,
                })
                .collect(),
            false,
        )
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &HostAttachment> {
        self.entries.iter()
    }

    pub fn host_ids(&self) -> &[usize] {
        &self.host_ids
    }

    pub fn contains(&self, host_id: &usize) -> bool {
        self.switch_for(*host_id).is_some()
    }

    pub fn switch_for(&self, host_id: usize) -> Option<usize> {
        self.entries
            .iter()
            .find(|entry| entry.host_id == host_id)
            .map(|entry| entry.switch_id)
    }

    pub fn sample_flow_pairs(
        &self,
        rng: &mut SmallRng,
        count: usize,
    ) -> std::result::Result<Vec<(usize, usize)>, String> {
        Self::sample_flow_pairs_from(&self.host_ids, self.distinct_source_sampling, rng, count)
    }

    pub(crate) fn sample_canonical_flow_pairs(
        &self,
        rng: &mut SmallRng,
        count: usize,
    ) -> std::result::Result<Vec<(usize, usize)>, String> {
        if self.distinct_source_sampling {
            return self.sample_flow_pairs(rng, count);
        }
        let mut host_ids = self.host_ids.clone();
        host_ids.sort_unstable();
        Self::sample_flow_pairs_from(&host_ids, false, rng, count)
    }

    /// Deterministic structural endpoint pairs, drawn from the attachment grid rather than the RNG.
    ///
    /// Members are enumerated in ascending host topology identity, so member *m* sources at the
    /// *m*-th host. Both policies are permutations, so a flow set may not request more members
    /// than there are hosts.
    pub fn structural_flow_pairs(
        &self,
        policy: PairingPolicy,
        count: usize,
    ) -> std::result::Result<Vec<(usize, usize)>, String> {
        let grid = self.attachment_grid()?;
        let switches = grid.len();
        let ordinals = grid[0].len();
        if count > switches * ordinals {
            return Err(format!(
                "structural flow-set count {count} exceeds the {} configured host attachments",
                switches * ordinals
            ));
        }
        match policy {
            PairingPolicy::Random => {
                return Err("random pairing is not a structural policy".to_owned());
            }
            PairingPolicy::SwitchOffsetHalf if !switches.is_multiple_of(2) => {
                return Err(format!(
                    "SwitchOffsetHalf pairing requires an even attachment-switch count, got {switches}"
                ));
            }
            PairingPolicy::SameSwitchNext if ordinals < 2 => {
                return Err(
                    "SameSwitchNext pairing requires at least two hosts per attachment switch"
                        .to_owned(),
                );
            }
            _ => {}
        }

        let mut sources = self.host_ids.clone();
        sources.sort_unstable();
        sources.truncate(count);
        let position = self.grid_positions(&grid);
        sources
            .into_iter()
            .map(|source| {
                let (switch, ordinal) = position[&source];
                let target = match policy {
                    PairingPolicy::Random => unreachable!("refused above"),
                    PairingPolicy::SwitchOffsetHalf => {
                        grid[(switch + switches / 2) % switches][ordinal]
                    }
                    PairingPolicy::SameSwitchNext => grid[switch][(ordinal + 1) % ordinals],
                };
                Ok((source, target))
            })
            .collect()
    }

    /// Hosts arranged as `[attachment switch rank][rack ordinal]`, both in ascending identity.
    ///
    /// Structural pairings are only well defined on a uniform grid, so a ragged attachment map is
    /// refused here rather than silently pairing across differently populated racks.
    fn attachment_grid(&self) -> std::result::Result<Vec<Vec<usize>>, String> {
        let mut by_switch = std::collections::BTreeMap::<usize, Vec<usize>>::new();
        for entry in &self.entries {
            by_switch
                .entry(entry.switch_id)
                .or_default()
                .push(entry.host_id);
        }
        if by_switch.is_empty() {
            return Err("structural pairing requires at least one host attachment".to_owned());
        }
        let grid = by_switch
            .into_values()
            .map(|mut hosts| {
                hosts.sort_unstable();
                hosts
            })
            .collect::<Vec<_>>();
        let ordinals = grid[0].len();
        if grid.iter().any(|hosts| hosts.len() != ordinals) {
            return Err(
                "structural pairing requires the same host count on every attachment switch"
                    .to_owned(),
            );
        }
        Ok(grid)
    }

    fn grid_positions(
        &self,
        grid: &[Vec<usize>],
    ) -> std::collections::BTreeMap<usize, (usize, usize)> {
        grid.iter()
            .enumerate()
            .flat_map(|(switch, hosts)| {
                hosts
                    .iter()
                    .enumerate()
                    .map(move |(ordinal, host)| (*host, (switch, ordinal)))
            })
            .collect()
    }

    fn sample_flow_pairs_from(
        host_ids: &[usize],
        distinct_source_sampling: bool,
        rng: &mut SmallRng,
        count: usize,
    ) -> std::result::Result<Vec<(usize, usize)>, String> {
        if host_ids.len() < 2 {
            return Err("flow sets require at least two configured host attachments".to_owned());
        }
        if !distinct_source_sampling {
            return Ok((0..count)
                .map(|_| {
                    let pair = host_ids.sample(rng, 2).copied().collect::<Vec<_>>();
                    (pair[0], pair[1])
                })
                .collect());
        }
        if count > host_ids.len() {
            return Err(format!(
                "flow count {count} exceeds the {} distinct source hosts available",
                host_ids.len()
            ));
        }

        let mut sources = host_ids.to_vec();
        sources.shuffle(rng);
        let target_offset = sources.len() / 2;
        Ok((0..count)
            .map(|index| {
                (
                    sources[index],
                    sources[(index + target_offset) % sources.len()],
                )
            })
            .collect())
    }
}

#[derive(Error, Debug)]
pub enum TopologyError {
    #[error("Failed to read configuration file: {0}")]
    ConfigReadError(#[from] std::io::Error),

    #[error("Failed to parse TOML: {0}")]
    TomlParseError(#[from] toml::de::Error),

    #[error("Invalid topology configuration: {0}")]
    InvalidConfig(String),

    #[error("Unsupported torus dimension: {0}")]
    UnsupportedDimension(u32),

    #[error("Numeric overflow in calculation: {0}")]
    NumericOverflow(String),
}

pub type Result<T> = std::result::Result<T, TopologyError>;

/// Represents a network graph configuration from TOML
#[derive(Deserialize)]
struct NetworkGraph {
    edges: Vec<(u32, u32)>,
    hosts: Vec<usize>,
}

impl NetworkGraph {
    fn validate(&self) -> Result<()> {
        if self.edges.is_empty() {
            return Err(TopologyError::InvalidConfig("Empty edge list".into()));
        }
        if self.hosts.is_empty() {
            return Err(TopologyError::InvalidConfig("Empty host list".into()));
        }
        // Additional validation could be added here at a later time
        Ok(())
    }
}

trait TopologyBuilder {
    fn build(&self) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)>;
}

impl TopologyBuilder for FatTreeConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
        let k = u32::try_from(self.k)
            .map_err(|_| TopologyError::NumericOverflow("FatTree k parameter overflow".into()))?;

        validate_fattree_params(k)?;
        let hosts_per_edge = self.hosts_per_edge.unwrap_or(1);
        validate_hosts_per_edge(k, hosts_per_edge)?;
        info!(
            "Building a FatTree topology with k = {} and {} host(s) per edge switch.",
            k, hosts_per_edge
        );

        let (num_layer_switches, layer_switches_per_pod, core_switches_per_agg) =
            calculate_fattree_params(k)?;

        let edges = build_fattree_edges(
            num_layer_switches,
            layer_switches_per_pod,
            core_switches_per_agg,
        );

        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts = create_fattree_host_list(num_layer_switches, hosts_per_edge)?;

        Ok((
            graph,
            hosts,
            TopologyProfile::FatTree {
                edge_switches: u64::from(num_layer_switches),
                aggregation_switches: u64::from(num_layer_switches),
                core_switches: u64::from(k.pow(2) / 4),
            },
        ))
    }
}

impl TopologyBuilder for DragonflyConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
        let routers_per_group = self.routers_per_group;
        let global_ports = self.global_ports_per_router;
        let hosts_per_router = self.hosts_per_router.unwrap_or(1);
        if routers_per_group < 2 {
            return Err(TopologyError::InvalidConfig(
                "dragonfly routers_per_group must be at least 2".into(),
            ));
        }
        if global_ports == 0 {
            return Err(TopologyError::InvalidConfig(
                "dragonfly global_ports_per_router must be positive".into(),
            ));
        }
        if hosts_per_router == 0 {
            return Err(TopologyError::InvalidConfig(
                "dragonfly hosts_per_router must be positive".into(),
            ));
        }
        // Balanced group count: with g = a*h + 1 every pair of groups is joined by exactly one
        // global link, so the global-port budget is exactly consumed and no group pair is
        // multiply connected (parallel physical links are refused by lowering).
        let groups = routers_per_group
            .checked_mul(global_ports)
            .and_then(|ports| ports.checked_add(1))
            .ok_or_else(|| {
                TopologyError::NumericOverflow("dragonfly group count overflow".into())
            })?;
        let router_count = groups.checked_mul(routers_per_group).ok_or_else(|| {
            TopologyError::NumericOverflow("dragonfly router count overflow".into())
        })?;
        u32::try_from(router_count).map_err(|_| {
            TopologyError::NumericOverflow("dragonfly router identity overflow".into())
        })?;
        info!(
            "Building a Dragonfly topology with {groups} group(s), {routers_per_group} router(s) \
             per group, {global_ports} global port(s) per router, and {hosts_per_router} \
             host(s) per router."
        );

        let router = |group: usize, index: usize| (group * routers_per_group + index) as u32;
        let mut edges = Vec::new();
        for group in 0..groups {
            for left in 0..routers_per_group {
                for right in (left + 1)..routers_per_group {
                    edges.push((router(group, left), router(group, right)));
                }
            }
        }
        // Absolute (circulant) global arrangement: global port `port` of group `group` reaches
        // group `(group + port + 1) mod groups`, and the partner's matching port is
        // `groups - port - 2`. Emitting only the `group < partner` half writes each group pair once.
        for group in 0..groups {
            for port in 0..(groups - 1) {
                let partner = (group + port + 1) % groups;
                if group >= partner {
                    continue;
                }
                let partner_port = groups - port - 2;
                edges.push((
                    router(group, port / global_ports),
                    router(partner, partner_port / global_ports),
                ));
            }
        }

        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts = create_uniform_host_list(router_count, hosts_per_router)?;
        Ok((
            graph,
            hosts,
            TopologyProfile::Dragonfly {
                groups: groups as u64,
                routers_per_group: routers_per_group as u64,
            },
        ))
    }
}

impl TopologyBuilder for TorusConfig {
    fn build(&self) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
        let dimension = self.dim as u32;
        let nodes_per_dim = self.n as u32;

        validate_torus_params(dimension, nodes_per_dim)?;

        let total_nodes = calculate_total_nodes(dimension, nodes_per_dim)?;
        info!(
            "Building {}D Torus topology with {} nodes.",
            dimension, total_nodes
        );

        let edges = build_torus_edges(dimension, nodes_per_dim)?;
        let graph = UnGraph::<usize, ()>::from_edges(&edges);
        let hosts = HostAttachments::identity((0..total_nodes).collect())?;

        Ok((graph, hosts, TopologyProfile::Torus))
    }
}

pub fn build_graph(file_path: &str) -> Result<(UnGraph<usize, ()>, HostAttachments)> {
    let (graph, hosts, _) = build_graph_with_profile(file_path)?;
    Ok((graph, hosts))
}

/// [`build_graph`] with the structural profile lowering needs for layer-keyed questions.
pub fn build_graph_with_profile(
    file_path: &str,
) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
    let content = fs::read_to_string(file_path)?;

    let config: Config = match toml::from_str::<Config>(&content) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Failed to deserialize: {}", err);
            return Err(TopologyError::TomlParseError(err));
        }
    };

    match config.topology {
        Some(topo_config) => match topo_config.category {
            TopoCategory::FatTree => {
                debug!("Initializing FatTree graph");
                topo_config
                    .fat_tree
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing FatTree config".into()))?
                    .build()
            }
            TopoCategory::Torus => {
                debug!("Initializing Torus graph");
                topo_config
                    .torus
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing Torus config".into()))?
                    .build()
            }
            TopoCategory::Dragonfly => {
                debug!("Initializing Dragonfly graph");
                topo_config
                    .dragonfly
                    .ok_or_else(|| TopologyError::InvalidConfig("Missing Dragonfly config".into()))?
                    .build()
            }
        },
        None => build_custom_graph(&content),
    }
}

fn build_custom_graph(
    content: &str,
) -> Result<(UnGraph<usize, ()>, HostAttachments, TopologyProfile)> {
    let graph_config: NetworkGraph = toml::from_str(content)?;
    graph_config.validate()?;
    Ok((
        UnGraph::<usize, ()>::from_edges(&graph_config.edges),
        HostAttachments::identity(graph_config.hosts)?,
        TopologyProfile::Custom,
    ))
}

fn validate_fattree_params(k: u32) -> Result<()> {
    if !k.is_multiple_of(2) {
        return Err(TopologyError::InvalidConfig("k must be even".into()));
    }
    if k == 0 {
        return Err(TopologyError::InvalidConfig("k must be positive".into()));
    }
    Ok(())
}

fn validate_hosts_per_edge(k: u32, hosts_per_edge: usize) -> Result<()> {
    let maximum = usize::try_from(k / 2)
        .map_err(|_| TopologyError::NumericOverflow("FatTree k/2 overflow".into()))?;
    if !(1..=maximum).contains(&hosts_per_edge) {
        return Err(TopologyError::InvalidConfig(format!(
            "hosts_per_edge must be in 1..={maximum} for k = {k}, got {hosts_per_edge}"
        )));
    }
    Ok(())
}

fn calculate_fattree_params(k: u32) -> Result<(u32, u32, u32)> {
    let num_layer_switches = k.pow(2) / 2;
    let layer_switches_per_pod = k / 2;
    let num_core_switches = k.pow(2) / 4;
    let core_switches_per_agg = num_core_switches / layer_switches_per_pod;

    Ok((
        num_layer_switches,
        layer_switches_per_pod,
        core_switches_per_agg,
    ))
}

fn build_fattree_edges(
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) -> Vec<(u32, u32)> {
    let mut edges = Vec::with_capacity((num_layer_switches * layer_switches_per_pod * 2) as usize);

    // Edge to aggregation layer connections
    build_edge_to_aggregation_connections(&mut edges, num_layer_switches, layer_switches_per_pod);

    // Aggregation to core layer connections
    build_aggregation_to_core_connections(
        &mut edges,
        num_layer_switches,
        layer_switches_per_pod,
        core_switches_per_agg,
    );

    edges
}

fn build_edge_to_aggregation_connections(
    edges: &mut Vec<(u32, u32)>,
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
) {
    for edge_id in 0..num_layer_switches {
        let pod_id = edge_id / layer_switches_per_pod;
        let agg_start = num_layer_switches + pod_id * layer_switches_per_pod;
        edges.extend(
            (agg_start..agg_start + layer_switches_per_pod).map(|agg_id| (edge_id, agg_id)),
        );
    }
}

fn build_aggregation_to_core_connections(
    edges: &mut Vec<(u32, u32)>,
    num_layer_switches: u32,
    layer_switches_per_pod: u32,
    core_switches_per_agg: u32,
) {
    for agg_id in num_layer_switches..2 * num_layer_switches {
        let core_group = agg_id % layer_switches_per_pod;
        let core_start = 2 * num_layer_switches + core_group * core_switches_per_agg;
        edges.extend(
            (core_start..core_start + core_switches_per_agg).map(|core_id| (agg_id, core_id)),
        );
    }
}

fn validate_torus_params(dimension: u32, nodes_per_dim: u32) -> Result<()> {
    if !(1..=3).contains(&dimension) {
        return Err(TopologyError::UnsupportedDimension(dimension));
    }
    if nodes_per_dim == 0 {
        return Err(TopologyError::InvalidConfig(
            "nodes_per_dim must be positive".into(),
        ));
    }
    Ok(())
}

fn calculate_total_nodes(dimension: u32, nodes_per_dim: u32) -> Result<usize> {
    usize::try_from(nodes_per_dim.pow(dimension))
        .map_err(|_| TopologyError::NumericOverflow("Total node count overflow".into()))
}

fn create_fattree_host_list(
    edge_switch_count: u32,
    hosts_per_edge: usize,
) -> Result<HostAttachments> {
    let edge_switch_count = usize::try_from(edge_switch_count)
        .map_err(|_| TopologyError::NumericOverflow("Edge switch count overflow".into()))?;
    create_uniform_host_list(edge_switch_count, hosts_per_edge)
}

/// Ordinal-major host attachment: `host = ordinal * switch_count + switch`.
///
/// Shared by every builder that attaches a uniform number of hosts to a set of leaf switches, so
/// the fat tree and the dragonfly number their hosts by the same rule and a structural pairing
/// means the same thing on both.
fn create_uniform_host_list(
    switch_count: usize,
    hosts_per_switch: usize,
) -> Result<HostAttachments> {
    let host_count = switch_count
        .checked_mul(hosts_per_switch)
        .ok_or_else(|| TopologyError::NumericOverflow("Host count overflow".into()))?;
    let mut hosts = Vec::new();
    hosts.try_reserve_exact(host_count).map_err(|error| {
        TopologyError::NumericOverflow(format!("Host list allocation: {error}"))
    })?;
    for host_ordinal in 0..hosts_per_switch {
        let host_base = host_ordinal
            .checked_mul(switch_count)
            .ok_or_else(|| TopologyError::NumericOverflow("Host identity overflow".into()))?;
        for switch_id in 0..switch_count {
            let host_id = host_base
                .checked_add(switch_id)
                .ok_or_else(|| TopologyError::NumericOverflow("Host identity overflow".into()))?;
            hosts.push(HostAttachment { host_id, switch_id });
        }
    }
    HostAttachments::new(hosts, hosts_per_switch > 1)
}

fn build_torus_edges(dimension: u32, nodes_per_dim: u32) -> Result<Vec<(u32, u32)>> {
    // Create empty undirected graph
    let mut graph = UnGraph::<(), ()>::default();

    // Add all nodes first
    let total_nodes = nodes_per_dim.pow(dimension);
    for _ in 0..total_nodes {
        graph.add_node(());
    }

    // Add edges based on dimension
    match dimension {
        1 => build_1d_torus_edges(&mut graph, nodes_per_dim),
        2 => build_2d_torus_edges(&mut graph, nodes_per_dim),
        3 => build_3d_torus_edges(&mut graph, nodes_per_dim),
        _ => return Err(TopologyError::UnsupportedDimension(dimension)),
    }

    // Extract edges from graph
    let edges: Vec<(u32, u32)> = graph
        .edge_indices()
        .map(|e| {
            let (a, b) = graph.edge_endpoints(e).unwrap();
            (a.index() as u32, b.index() as u32)
        })
        .collect();

    Ok(edges)
}

fn build_1d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        let next = (i + 1) % nodes_per_dim;
        let i_idx = NodeIndex::new(i as usize);
        let next_idx = NodeIndex::new(next as usize);

        // Add single edge - UnGraph handles bidirectional nature
        graph.add_edge(i_idx, next_idx, ());
    }
}

fn build_2d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        for j in 0..nodes_per_dim {
            let current = i + j * nodes_per_dim;
            let current_idx = NodeIndex::new(current as usize);

            // X dimension connection
            let next_i = (i + 1) % nodes_per_dim + j * nodes_per_dim;
            let next_i_idx = NodeIndex::new(next_i as usize);
            graph.add_edge(current_idx, next_i_idx, ());

            // Y dimension connection
            let next_j = i + ((j + 1) % nodes_per_dim) * nodes_per_dim;
            let next_j_idx = NodeIndex::new(next_j as usize);
            graph.add_edge(current_idx, next_j_idx, ());
        }
    }
}

fn build_3d_torus_edges(graph: &mut UnGraph<(), ()>, nodes_per_dim: u32) {
    for i in 0..nodes_per_dim {
        for j in 0..nodes_per_dim {
            for k in 0..nodes_per_dim {
                let current = i + j * nodes_per_dim + k * nodes_per_dim.pow(2);
                let current_idx = NodeIndex::new(current as usize);

                // X dimension
                let next_i = (i + 1) % nodes_per_dim + j * nodes_per_dim + k * nodes_per_dim.pow(2);
                let next_i_idx = NodeIndex::new(next_i as usize);
                graph.add_edge(current_idx, next_i_idx, ());

                // Y dimension
                let next_j =
                    i + ((j + 1) % nodes_per_dim) * nodes_per_dim + k * nodes_per_dim.pow(2);
                let next_j_idx = NodeIndex::new(next_j as usize);
                graph.add_edge(current_idx, next_j_idx, ());

                // Z dimension
                let next_k =
                    i + j * nodes_per_dim + ((k + 1) % nodes_per_dim) * nodes_per_dim.pow(2);
                let next_k_idx = NodeIndex::new(next_k as usize);
                graph.add_edge(current_idx, next_k_idx, ());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::NodeIndex;
    use petgraph::visit::EdgeRef;
    use rand::SeedableRng;
    use std::collections::HashSet;

    // Helper functions for tests
    fn create_fattree(
        k: usize,
        hosts_per_edge: Option<usize>,
    ) -> (UnGraph<usize, ()>, HostAttachments) {
        let config = FatTreeConfig { k, hosts_per_edge };
        let (graph, hosts, _) = config.build().unwrap();
        (graph, hosts)
    }

    fn create_torus(dim: usize, n: usize) -> (UnGraph<usize, ()>, HostAttachments) {
        let config = TorusConfig { dim, n };
        let (graph, hosts, _) = config.build().unwrap();
        (graph, hosts)
    }

    // FatTree Tests
    mod fattree_tests {
        use super::*;

        #[test]
        fn test_fattree_node_counts() {
            let k = 4;
            let (graph, hosts) = create_fattree(k, None);

            let expected_edge_switches = k * k / 2;
            let expected_agg_switches = k * k / 2;
            let expected_core_switches = k * k / 4;
            let total_expected_switches =
                expected_edge_switches + expected_agg_switches + expected_core_switches;

            assert_eq!(graph.node_count(), total_expected_switches);
            assert_eq!(hosts.len(), expected_edge_switches);
        }

        #[test]
        fn test_fattree_edge_counts() {
            let k = 4;
            let (graph, _) = create_fattree(k, None);

            // Each edge switch connects to k/2 aggregation switches
            // Each aggregation switch connects to k/2 core switches
            // The total number of connections is:
            // (k * k / 2) * (k / 2)      // edge to aggregation connections
            // + (k * k / 2) * (k / 2)    // aggregation to core connections
            let expected_edges = (k * k / 2) * (k / 2) + (k * k / 2) * (k / 2);

            // Each connection is counted only once in our expected count
            assert_eq!(graph.edge_count(), expected_edges);
        }

        #[test]
        fn test_fattree_pod_connectivity() {
            let k = 4;
            let (graph, _) = create_fattree(k, None);

            for pod in 0..k / 2 {
                let pod_edge_switches: Vec<u32> =
                    (0..k / 2).map(|i| (pod * k / 2 + i) as u32).collect();

                let pod_agg_switches: Vec<u32> = (0..k / 2)
                    .map(|i| (k * k / 2 + pod * k / 2 + i) as u32)
                    .collect();

                for &edge_switch in &pod_edge_switches {
                    let neighbors: HashSet<u32> = graph
                        .edges(NodeIndex::new(edge_switch as usize))
                        .map(|e| e.target().index() as u32)
                        .collect();

                    for &agg_switch in &pod_agg_switches {
                        assert!(neighbors.contains(&agg_switch));
                    }
                }
            }
        }

        #[test]
        fn test_fattree_core_connectivity() {
            let k = 4;
            let (graph, _) = create_fattree(k, None);

            let agg_start = k * k / 2;
            let agg_end = k * k;

            for agg_id in agg_start..agg_end {
                let node_idx = NodeIndex::new(agg_id);
                let core_neighbors: HashSet<_> = graph
                    .edges(node_idx)
                    .map(|e| e.target().index())
                    .filter(|&n| n >= (k * k))
                    .collect();

                assert_eq!(core_neighbors.len(), k / 2);
            }
        }

        #[test]
        fn test_fattree_multiple_hosts_per_edge_at_small_k() {
            let k = 4;
            let hosts_per_edge = k / 2;
            let (graph, hosts) = create_fattree(k, Some(hosts_per_edge));
            let edge_switches = k * k / 2;

            assert_eq!(hosts.len(), edge_switches * hosts_per_edge);
            assert_eq!(graph.edge_count(), k * k * k / 2);
            for edge_switch in 0..edge_switches {
                assert_eq!(
                    hosts
                        .iter()
                        .filter(|host| host.switch_id == edge_switch)
                        .count(),
                    hosts_per_edge
                );
                assert_eq!(
                    graph.edges(NodeIndex::new(edge_switch)).count(),
                    k / 2,
                    "host attachments must not alter the physical switch graph degree"
                );
            }
        }

        #[test]
        fn test_fattree_canonical_k32_host_population() {
            let k = 32;
            let hosts_per_edge = k / 2;
            let (graph, hosts) = create_fattree(k, Some(hosts_per_edge));
            let edge_switches = k * k / 2;

            assert_eq!(hosts.len(), k * k * k / 4);
            assert_eq!(graph.edge_count(), k * k * k / 2);
            assert_eq!(
                hosts
                    .iter()
                    .map(|host| host.host_id)
                    .collect::<HashSet<_>>()
                    .len(),
                hosts.len()
            );
            assert!(hosts.iter().all(|host| host.switch_id < edge_switches));
            assert!((0..edge_switches).all(|edge_switch| {
                hosts
                    .iter()
                    .filter(|host| host.switch_id == edge_switch)
                    .count()
                    == hosts_per_edge
            }));
            assert!(
                (0..edge_switches)
                    .all(|edge_switch| graph.edges(NodeIndex::new(edge_switch)).count() == k / 2)
            );
        }

        #[test]
        fn test_fattree_multiple_host_flow_pairs_are_nested_and_non_oversubscribed() {
            let (_, hosts) = create_fattree(4, Some(2));
            let mut small_rng = SmallRng::seed_from_u64(13_032);
            let small = hosts.sample_flow_pairs(&mut small_rng, 3).unwrap();
            let mut large_rng = SmallRng::seed_from_u64(13_032);
            let large = hosts.sample_flow_pairs(&mut large_rng, 7).unwrap();

            assert_eq!(small, large[..small.len()]);
            assert_eq!(
                large
                    .iter()
                    .map(|pair| pair.0)
                    .collect::<HashSet<_>>()
                    .len(),
                large.len()
            );
            assert_eq!(
                large
                    .iter()
                    .map(|pair| pair.1)
                    .collect::<HashSet<_>>()
                    .len(),
                large.len()
            );
            assert!(large.iter().all(|(source, target)| source != target));
        }

        #[test]
        fn test_fattree_hosts_per_edge_range_is_validated() {
            for invalid in [0, 3] {
                let error = FatTreeConfig {
                    k: 4,
                    hosts_per_edge: Some(invalid),
                }
                .build()
                .expect_err("hosts_per_edge outside 1..=k/2 must fail");

                assert!(
                    error.to_string().contains("hosts_per_edge"),
                    "range error must name hosts_per_edge: {error}"
                );
                assert!(
                    error.to_string().contains("1..=2"),
                    "range error must state the accepted bounds: {error}"
                );
            }
        }
    }

    // Torus Tests
    mod torus_tests {
        use super::*;

        #[test]
        fn test_torus_node_counts() {
            let test_cases = [(1, 4), (2, 3), (3, 2)];

            for &(dim, n) in &test_cases {
                let (graph, hosts) = create_torus(dim, n);
                let expected_nodes = n.pow(dim as u32);

                assert_eq!(graph.node_count(), expected_nodes);
                assert_eq!(hosts.len(), expected_nodes);
            }
        }

        #[test]
        fn test_torus_edge_counts() {
            let test_cases = [(1, 4), (2, 3), (3, 2)];

            for &(dim, n) in &test_cases {
                let (graph, _) = create_torus(dim, n);

                // In a d-dimensional torus:
                // - Each node has d connections (one in each dimension)
                // - Total number of nodes is n^d
                // - Each connection is unique in the topology
                let num_nodes = n.pow(dim as u32);
                let expected_edges = num_nodes * dim;

                assert_eq!(
                    graph.edge_count(),
                    expected_edges,
                    "Wrong edge count for {}-D torus with {} nodes per dimension",
                    dim,
                    n
                );
            }
        }

        #[test]
        fn test_torus_node_degrees() {
            // Each node in a k-dimensional torus has 2k connections (2 per dimension)
            let test_cases = [
                (1, 4, 2), // 1D: 2 connections per node
                (2, 3, 4), // 2D: 4 connections per node
                (3, 2, 6), // 3D: 6 connections per node
            ];

            for &(dim, n, expected_degree) in &test_cases {
                let (graph, _) = create_torus(dim, n);

                for node_idx in 0..n.pow(dim as u32) {
                    let node = NodeIndex::new(node_idx);
                    assert_eq!(
                        graph.edges(node).count(),
                        expected_degree,
                        "Node {} in {}-D torus has wrong degree",
                        node_idx,
                        dim
                    );
                }
            }
        }

        #[test]
        fn test_torus_wraparound_connections() {
            // Test 1D torus wraparound
            let (graph, _) = create_torus(1, 4);
            assert!(has_edge(&graph, 0, 3));

            // Test 2D torus wraparound
            let (graph, _) = create_torus(2, 3);
            // Check horizontal wraparound
            assert!(has_edge(&graph, 0, 2));
            assert!(has_edge(&graph, 3, 5));
            assert!(has_edge(&graph, 6, 8));
            // Check vertical wraparound
            assert!(has_edge(&graph, 0, 6));
            assert!(has_edge(&graph, 1, 7));
            assert!(has_edge(&graph, 2, 8));
        }

        fn has_edge(graph: &UnGraph<usize, ()>, from: usize, to: usize) -> bool {
            graph
                .edges(NodeIndex::new(from))
                .any(|e| e.target().index() == to)
        }

        #[test]
        fn test_torus_neighbor_distances() {
            let (graph, _) = create_torus(2, 4);

            let node_idx = NodeIndex::new(5);
            let neighbors: HashSet<_> = graph.edges(node_idx).map(|e| e.target().index()).collect();

            let expected: HashSet<_> = vec![4, 6, 1, 9].into_iter().collect();
            assert_eq!(neighbors, expected);
        }
    }

    #[test]
    fn test_build_graph_missing_torus_config() {
        let toml_content = r#"
            [topology]
            category = "Torus"

            [switch]
            port_rate = 8000
            capacity = 100
            weights = [1]
            discipline = "FIFO"
            drop = "RED"
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let result = build_graph(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "missing torus config should error");
    }

    #[test]
    fn test_build_graph_custom_empty_edges_errors() {
        let toml_content = r#"
            edges = []
            hosts = [0, 1]

            [switch]
            port_rate = 8000
            capacity = 100
            weights = [1]
            discipline = "FIFO"
            drop = "RED"
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let result = build_graph(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "empty edge list should error");
    }

    #[test]
    fn test_build_graph_custom_empty_hosts_errors() {
        let toml_content = r#"
            edges = [[0, 1]]
            hosts = []

            [switch]
            port_rate = 8000
            capacity = 100
            weights = [1]
            discipline = "FIFO"
            drop = "RED"
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let result = build_graph(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "empty host list should error");
    }

    #[test]
    fn test_build_graph_fattree_invalid_k() {
        let toml_content = r#"
            [topology]
            category = "FatTree"

            [topology.fat_tree]
            k = 3

            [switch]
            port_rate = 8000
            capacity = 100
            weights = [1]
            discipline = "FIFO"
            drop = "RED"
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let result = build_graph(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "odd k should error");
    }

    #[test]
    fn test_build_graph_torus_invalid_dimension() {
        let toml_content = r#"
            [topology]
            category = "Torus"

            [topology.torus]
            dim = 4
            n = 2

            [switch]
            port_rate = 8000
            capacity = 100
            weights = [1]
            discipline = "FIFO"
            drop = "RED"
        "#;

        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file.");
        write!(temp_file, "{}", toml_content).expect("Failed to write to temp file.");

        let result = build_graph(temp_file.path().to_str().unwrap());
        assert!(result.is_err(), "unsupported torus dimension should error");
    }
}

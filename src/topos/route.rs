//! The routing protocols that are used to compute the path that each flow
//! takes. Currently, three routing protocols have been implemented:
//!
//! - Shortest path routing: Selects a random candidate from a set of shortest
//!   paths, which are computed by the `petgraph` crate using the A* algorithm.
//! - Path from configuration: Uses the path that is specified in the configuration.
//! - ECMP: Implements the Equal-Cost Multi-Path algorithm (RFC 2992) optimized with A*.
//!
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::thread;

use petgraph::algo;
use petgraph::algo::astar;
use petgraph::graph::{NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use serde::Deserialize;

#[cfg(test)]
std::thread_local! {
    static FAT_TREE_LAYOUT_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Copy, Clone, Debug)]
struct MinScoredNode {
    score: (usize, usize, usize),
    node: NodeIndex,
}

impl PartialEq for MinScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for MinScoredNode {}

impl PartialOrd for MinScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinScoredNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = &self.score;
        let b = &other.score;
        if a == b {
            Ordering::Equal
        } else if a < b {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum RoutingConfig {
    ShortestPath,
    PathFromConfig,
    ECMP,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Routing {
    ShortestPath(ShortestPath),
    PathFromConfig(PathFromConfig),
    ECMP(ECMP),
}

/// Defines the interface for all routing protocols.
pub trait RoutingProtocol {
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex>;
}

#[derive(Debug, Clone)]
pub struct ShortestPath {
    graph: UnGraph<usize, ()>,
}

impl PartialEq for ShortestPath {
    fn eq(&self, other: &Self) -> bool {
        algo::is_isomorphic(&self.graph, &other.graph)
    }
}

impl ShortestPath {
    pub fn new(graph: UnGraph<usize, ()>) -> ShortestPath {
        ShortestPath { graph }
    }

    fn has_fat_tree_layout(
        graph: &UnGraph<usize, ()>,
        num_layer_switches: usize,
        switches_per_pod: usize,
        num_pods: usize,
    ) -> bool {
        let expected_edge_count = num_layer_switches
            .checked_mul(switches_per_pod)
            .and_then(|edge_to_agg| edge_to_agg.checked_mul(2));
        if graph.edge_count() != expected_edge_count.unwrap_or(usize::MAX) {
            return false;
        }

        let core_start = 2 * num_layer_switches;

        for edge_id in 0..num_layer_switches {
            let pod = edge_id / switches_per_pod;
            let agg_base = num_layer_switches + pod * switches_per_pod;

            for agg_offset in 0..switches_per_pod {
                if !graph.contains_edge(
                    NodeIndex::new(edge_id),
                    NodeIndex::new(agg_base + agg_offset),
                ) {
                    return false;
                }
            }
        }

        for agg_id in num_layer_switches..core_start {
            let agg_rel = agg_id - num_layer_switches;
            let pod = agg_rel / switches_per_pod;
            let group = agg_rel % switches_per_pod;
            let edge_base = pod * switches_per_pod;

            for edge_offset in 0..switches_per_pod {
                if !graph.contains_edge(
                    NodeIndex::new(agg_id),
                    NodeIndex::new(edge_base + edge_offset),
                ) {
                    return false;
                }
            }

            for core_offset in 0..switches_per_pod {
                if !graph.contains_edge(
                    NodeIndex::new(agg_id),
                    NodeIndex::new(core_start + group * switches_per_pod + core_offset),
                ) {
                    return false;
                }
            }
        }

        for core_id in core_start..graph.node_count() {
            let core_rel = core_id - core_start;
            let group = core_rel / switches_per_pod;

            for pod in 0..num_pods {
                if !graph.contains_edge(
                    NodeIndex::new(core_id),
                    NodeIndex::new(num_layer_switches + pod * switches_per_pod + group),
                ) {
                    return false;
                }
            }
        }

        true
    }

    fn fat_tree_params(graph: &UnGraph<usize, ()>) -> Option<(usize, usize, usize)> {
        #[cfg(test)]
        FAT_TREE_LAYOUT_CHECKS.with(|checks| checks.set(checks.get() + 1));

        let total_nodes = graph.node_count();
        if total_nodes == 0 || !total_nodes.is_multiple_of(5) {
            return None;
        }

        let num_layer_switches = total_nodes.checked_mul(2)? / 5;
        let doubled = num_layer_switches.checked_mul(2)?;
        let k = (doubled as f64).sqrt() as usize;
        if k == 0 || k * k != doubled || !k.is_multiple_of(2) {
            return None;
        }

        let switches_per_pod = k / 2;
        if !Self::has_fat_tree_layout(graph, num_layer_switches, switches_per_pod, k) {
            return None;
        }

        Some((num_layer_switches, switches_per_pod, k))
    }

    fn compute_fat_tree_route_with_params(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
        (num_layer_switches, switches_per_pod, num_pods): (usize, usize, usize),
    ) -> Option<Vec<NodeIndex>> {
        let core_start = 2 * num_layer_switches;

        let start_idx = start.index();
        let end_idx = end.index();
        if start_idx >= num_layer_switches || end_idx >= num_layer_switches {
            return None;
        }

        let node_count = graph.node_count();
        let mut visit_next = BinaryHeap::new();
        let mut scores = vec![None; node_count];
        let mut came_from = vec![usize::MAX; node_count];

        let start_idx = start.index();
        scores[start_idx] = Some((0, 0, 0));
        visit_next.push(MinScoredNode {
            score: (0, 0, 0),
            node: start,
        });

        while let Some(MinScoredNode {
            score: (f, h, g),
            node,
        }) = visit_next.pop()
        {
            if node == end {
                let mut path = vec![node];
                let mut current = node.index();
                while current != start_idx {
                    let previous = came_from[current];
                    if previous == usize::MAX {
                        break;
                    }
                    path.push(NodeIndex::new(previous));
                    current = previous;
                }
                path.reverse();
                return Some(path);
            }

            let node_idx = node.index();
            if let Some((_, _, old_g)) = scores[node_idx] {
                if old_g < g {
                    continue;
                }
            }
            scores[node_idx] = Some((f, h, g));

            let mut push_neighbor = |neigh: NodeIndex| {
                let neigh_g = g + 1;
                let neigh_score = (neigh_g, 0, neigh_g);
                let neigh_idx = neigh.index();

                if let Some((_, _, old_neigh_g)) = scores[neigh_idx] {
                    if neigh_g >= old_neigh_g {
                        return;
                    }
                }

                scores[neigh_idx] = Some(neigh_score);
                came_from[neigh_idx] = node_idx;
                visit_next.push(MinScoredNode {
                    score: neigh_score,
                    node: neigh,
                });
            };

            if node_idx < num_layer_switches {
                let pod = node_idx / switches_per_pod;
                let agg_base = num_layer_switches + pod * switches_per_pod;
                for agg_offset in (0..switches_per_pod).rev() {
                    push_neighbor(NodeIndex::new(agg_base + agg_offset));
                }
            } else if node_idx < core_start {
                let agg_rel = node_idx - num_layer_switches;
                let pod = agg_rel / switches_per_pod;
                let group = agg_rel % switches_per_pod;

                for core_offset in (0..switches_per_pod).rev() {
                    push_neighbor(NodeIndex::new(
                        core_start + group * switches_per_pod + core_offset,
                    ));
                }

                let edge_base = pod * switches_per_pod;
                for edge_offset in (0..switches_per_pod).rev() {
                    push_neighbor(NodeIndex::new(edge_base + edge_offset));
                }
            } else {
                let core_rel = node_idx - core_start;
                let group = core_rel / switches_per_pod;

                for pod in (0..num_pods).rev() {
                    push_neighbor(NodeIndex::new(
                        num_layer_switches + pod * switches_per_pod + group,
                    ));
                }
            }
        }

        None
    }

    fn try_compute_route_in_canonical_graph(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Option<Vec<NodeIndex>> {
        let fat_tree_params = Self::fat_tree_params(graph);
        Self::try_compute_route_in_classified_canonical_graph(graph, start, end, fat_tree_params)
    }

    fn try_compute_route_in_classified_canonical_graph(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
        fat_tree_params: Option<(usize, usize, usize)>,
    ) -> Option<Vec<NodeIndex>> {
        if let Some(params) = fat_tree_params {
            if let Some(path) = Self::compute_fat_tree_route_with_params(graph, start, end, params)
            {
                return Some(path);
            }
        }
        astar(
            graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        )
        .map(|(_, path)| path)
    }

    pub fn try_compute_route_in(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Option<Vec<NodeIndex>> {
        let graph = canonical_routing_graph(graph);
        Self::try_compute_route_in_canonical_graph(&graph, start, end)
    }

    pub fn compute_route_in(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Vec<NodeIndex> {
        Self::try_compute_route_in(graph, start, end).expect("No path can be found.")
    }
}

fn canonical_routing_graph(graph: &UnGraph<usize, ()>) -> UnGraph<usize, ()> {
    let mut canonical = UnGraph::with_capacity(graph.node_count(), graph.edge_count());
    for node in graph.node_indices() {
        let canonical_node = canonical.add_node(graph[node]);
        debug_assert_eq!(canonical_node, node);
    }

    let mut edges = graph
        .edge_references()
        .map(|edge| {
            let left = edge.source().index();
            let right = edge.target().index();
            (left.min(right), left.max(right))
        })
        .collect::<Vec<_>>();
    edges.sort_unstable();
    for (left, right) in edges {
        canonical.add_edge(NodeIndex::new(left), NodeIndex::new(right), ());
    }
    canonical
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteTableError<K> {
    DuplicateKey(K),
    Unreachable(K),
}

/// Largest host-thread budget the per-flow route scatter will use.
///
/// Every worker owns one disjoint index range, so a budget wider than this only adds thread
/// creation to a phase that is already bounded by memory bandwidth.
pub const MAX_ROUTE_WORKERS: usize = 64;

/// Host-thread budget for per-flow route computation.
///
/// A route is a pure function of the canonical topology graph and the flow endpoints: the
/// pathfinder reads an immutable graph, allocates its own search state, consults no cache, draws
/// no randomness, and never iterates a hash map. The budget therefore changes only how much wall
/// clock the route table costs, never which path a flow receives. `RouteWorkers::serial()` keeps
/// the single-threaded reference available so equality gates can pin the parallel scatter against
/// it.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RouteWorkers(NonZeroUsize);

impl RouteWorkers {
    /// The single-threaded reference budget.
    pub const fn serial() -> Self {
        Self(NonZeroUsize::MIN)
    }

    /// Clamps an arbitrary request into `1..=MAX_ROUTE_WORKERS`.
    pub const fn new(workers: usize) -> Self {
        let clamped = if workers < 1 {
            1
        } else if workers > MAX_ROUTE_WORKERS {
            MAX_ROUTE_WORKERS
        } else {
            workers
        };
        match NonZeroUsize::new(clamped) {
            Some(workers) => Self(workers),
            None => Self::serial(),
        }
    }

    /// The budget lowering uses by default: the host's reported parallelism, clamped.
    pub fn available() -> Self {
        Self::new(thread::available_parallelism().map_or(1, NonZeroUsize::get))
    }

    /// The clamped worker count.
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Number of flows in each contiguous index chunk handed to one route worker.
///
/// The partition is a pure function of the flow count and the budget, so the same flow index
/// always lands in the same chunk at the same offset regardless of thread scheduling.
pub const fn route_chunk_len(flow_count: usize, workers: RouteWorkers) -> usize {
    if flow_count == 0 {
        return 1;
    }
    let chunks = if workers.get() < flow_count {
        workers.get()
    } else {
        flow_count
    };
    flow_count.div_ceil(chunks)
}

/// Number of contiguous index chunks the flow list is partitioned into.
pub const fn route_chunk_count(flow_count: usize, workers: RouteWorkers) -> usize {
    flow_count.div_ceil(route_chunk_len(flow_count, workers))
}

/// Fills one worker's disjoint result slice from its disjoint endpoint slice.
///
/// The two slices are the same contiguous index range of the flow list, so this is an
/// index-addressed scatter: no worker can observe or reach another worker's slot.
fn fill_route_chunk(
    graph: &UnGraph<usize, ()>,
    fat_tree_params: Option<(usize, usize, usize)>,
    endpoints: &[(NodeIndex, NodeIndex)],
    routes: &mut [Option<Vec<NodeIndex>>],
) {
    for (&(source, target), slot) in endpoints.iter().zip(routes.iter_mut()) {
        *slot = ShortestPath::try_compute_route_in_classified_canonical_graph(
            graph,
            source,
            target,
            fat_tree_params,
        );
    }
}

/// Computes one route per endpoint pair into an index-addressed buffer.
///
/// `None` marks an unreachable pair; the caller decides which position becomes the reported error.
fn scatter_routes(
    graph: &UnGraph<usize, ()>,
    fat_tree_params: Option<(usize, usize, usize)>,
    endpoints: &[(NodeIndex, NodeIndex)],
    workers: RouteWorkers,
) -> Vec<Option<Vec<NodeIndex>>> {
    let mut routes = vec![None; endpoints.len()];
    let chunk_len = route_chunk_len(endpoints.len(), workers);
    if route_chunk_count(endpoints.len(), workers) < 2 {
        fill_route_chunk(graph, fat_tree_params, endpoints, &mut routes);
        return routes;
    }

    thread::scope(|scope| {
        for (endpoints, routes) in endpoints
            .chunks(chunk_len)
            .zip(routes.chunks_mut(chunk_len))
        {
            scope.spawn(move || fill_route_chunk(graph, fat_tree_params, endpoints, routes));
        }
    });
    routes
}

/// Position of the first key that repeats an earlier key, in submission order.
fn first_repeated_key<K>(keys: &[K]) -> Option<usize>
where
    K: Ord,
{
    let mut seen = BTreeSet::new();
    keys.iter().position(|key| !seen.insert(key))
}

/// Selects one deterministic physical switch path per flow.
///
/// Legacy Days consumes this table when installing forwarding entries, and exact lowering consumes
/// the same table when materializing `FlowDescriptor` routes. Keeping selection here prevents the
/// two engines from acquiring independent equal-cost-path policies.
///
/// Route computation runs on `RouteWorkers::available()` host threads. Use
/// [`compute_shortest_path_route_table_with`] to pin a budget.
pub fn compute_shortest_path_route_table<K>(
    graph: &UnGraph<usize, ()>,
    flows: impl IntoIterator<Item = (K, NodeIndex, NodeIndex)>,
) -> Result<BTreeMap<K, Vec<NodeIndex>>, RouteTableError<K>>
where
    K: Ord,
{
    compute_shortest_path_route_table_with(graph, flows, RouteWorkers::available())
}

/// [`compute_shortest_path_route_table`] with an explicit host-thread budget.
///
/// The budget is invisible in the result. Flows are partitioned by submission index into at most
/// `workers` contiguous chunks, each worker writes only its own disjoint slice, and the reported
/// error is still the first failing submission position rather than the first one a thread happens
/// to reach.
pub fn compute_shortest_path_route_table_with<K>(
    graph: &UnGraph<usize, ()>,
    flows: impl IntoIterator<Item = (K, NodeIndex, NodeIndex)>,
    workers: RouteWorkers,
) -> Result<BTreeMap<K, Vec<NodeIndex>>, RouteTableError<K>>
where
    K: Ord,
{
    let graph = canonical_routing_graph(graph);
    let fat_tree_params = ShortestPath::fat_tree_params(&graph);

    let flows = flows.into_iter();
    let (lower_bound, _) = flows.size_hint();
    let mut keys = Vec::with_capacity(lower_bound);
    let mut endpoints = Vec::with_capacity(lower_bound);
    for (key, source, target) in flows {
        keys.push(key);
        endpoints.push((source, target));
    }

    // The serial table returned at the first repeated key without asking for its route, so no
    // position at or beyond that key was ever routed. Keeping the same horizon keeps the reported
    // error identical and stops the scatter from routing flows the caller never sees.
    let first_duplicate = first_repeated_key(&keys);
    let routed = first_duplicate.unwrap_or(keys.len());
    let routes = scatter_routes(&graph, fat_tree_params, &endpoints[..routed], workers);

    if let Some(index) = routes.iter().position(Option::is_none) {
        return Err(RouteTableError::Unreachable(keys.swap_remove(index)));
    }
    if let Some(index) = first_duplicate {
        return Err(RouteTableError::DuplicateKey(keys.swap_remove(index)));
    }

    Ok(keys
        .into_iter()
        .zip(routes)
        .map(|(key, route)| {
            (
                key,
                route.expect("unreachable routes were rejected before the table was built"),
            )
        })
        .collect())
}

impl RoutingProtocol for ShortestPath {
    /// Returns a shortest path between two nodes in the graph using A*.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        Self::compute_route_in(&self.graph, start, end)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PathFromConfig {
    pub path: Vec<NodeIndex>,
}

impl PathFromConfig {
    pub fn new(path_from_config: Vec<usize>) -> PathFromConfig {
        let path = path_from_config.into_iter().map(NodeIndex::new).collect();
        PathFromConfig { path }
    }
}

#[derive(Debug, Clone)]
pub struct ECMP {
    graph: UnGraph<usize, ()>,
    flow_id: usize,
    source_host: usize,
    sink_host: usize,
}

impl PartialEq for ECMP {
    fn eq(&self, other: &Self) -> bool {
        self.flow_id == other.flow_id
            && self.source_host == other.source_host
            && self.sink_host == other.sink_host
            && algo::is_isomorphic(&self.graph, &other.graph)
    }
}

impl ECMP {
    pub fn new(
        graph: UnGraph<usize, ()>,
        flow_id: usize,
        source_host: usize,
        sink_host: usize,
    ) -> ECMP {
        ECMP {
            graph,
            flow_id,
            source_host,
            sink_host,
        }
    }

    fn compute_hash_for(flow_id: usize, source_host: usize, sink_host: usize) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        // Compute a hash value based on flow attributes
        flow_id.hash(&mut hasher);
        source_host.hash(&mut hasher);
        sink_host.hash(&mut hasher);
        hasher.finish()
    }

    /// Selects one of the equal-cost paths using a hash of flow attributes.
    fn select_ecmp_path(&self, paths: &[Vec<NodeIndex>]) -> Vec<NodeIndex> {
        Self::select_ecmp_path_for(self.flow_id, self.source_host, self.sink_host, paths)
    }

    fn select_ecmp_path_for(
        flow_id: usize,
        source_host: usize,
        sink_host: usize,
        paths: &[Vec<NodeIndex>],
    ) -> Vec<NodeIndex> {
        // Use the hash to select a path
        let index =
            (Self::compute_hash_for(flow_id, source_host, sink_host) as usize) % paths.len();
        paths[index].clone()
    }

    /// Finds all equal-cost paths using an optimized A* approach.
    fn find_equal_cost_paths(
        &self,
        start: NodeIndex,
        end: NodeIndex,
        shortest_distance: usize,
    ) -> Vec<Vec<NodeIndex>> {
        Self::find_equal_cost_paths_in(&self.graph, start, end, shortest_distance)
    }

    fn find_equal_cost_paths_in(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
        shortest_distance: usize,
    ) -> Vec<Vec<NodeIndex>> {
        let mut paths = Vec::new();
        let mut stack = vec![(start, vec![start], 0)];

        while let Some((current, path, cost)) = stack.pop() {
            if current == end {
                if cost == shortest_distance {
                    paths.push(path.clone());
                }
                continue;
            }

            for neighbor in graph.neighbors(current) {
                if !path.contains(&neighbor) {
                    let new_cost = cost + 1; // Uniform cost
                    if new_cost <= shortest_distance {
                        let mut new_path = path.clone();
                        new_path.push(neighbor);
                        stack.push((neighbor, new_path, new_cost));
                    }
                }
            }
        }

        paths
    }

    pub fn compute_route_in(
        graph: &UnGraph<usize, ()>,
        flow_id: usize,
        source_host: usize,
        sink_host: usize,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Vec<NodeIndex> {
        let shortest_path = astar(
            graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        );

        let shortest_distance = match shortest_path {
            Some((cost, _)) => cost,
            None => panic!("No path can be found."),
        };

        let equal_cost_paths =
            Self::find_equal_cost_paths_in(graph, start, end, shortest_distance as usize);

        if !equal_cost_paths.is_empty() {
            Self::select_ecmp_path_for(flow_id, source_host, sink_host, &equal_cost_paths)
        } else {
            panic!("No equal-cost path can be found.");
        }
    }
}

impl RoutingProtocol for ECMP {
    /// Returns a path based on the Equal-Cost Multi-Path (ECMP) routing protocol optimized with A*.
    fn compute_route(&mut self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        // Use A* to find the shortest path distance
        let shortest_path = astar(
            &self.graph,
            start,
            |n| n == end,
            |_| 1, // Uniform cost
            |_| 0, // Heuristic ignored for uniform cost
        );

        let shortest_distance = match shortest_path {
            Some((cost, _)) => cost,
            None => panic!("No path can be found."),
        };

        // Find all equal-cost paths using the optimized A* approach
        let equal_cost_paths = self.find_equal_cost_paths(start, end, shortest_distance as usize);

        if !equal_cost_paths.is_empty() {
            self.select_ecmp_path(&equal_cost_paths)
        } else {
            panic!("No equal-cost path can be found.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use petgraph::graph::UnGraph;

    fn canonical_k4_fat_tree() -> UnGraph<usize, ()> {
        let mut graph = UnGraph::with_capacity(20, 32);
        for node in 0..20 {
            graph.add_node(node);
        }
        for pod in 0..4 {
            let edge_base = pod * 2;
            let aggregation_base = 8 + pod * 2;
            for edge_offset in 0..2 {
                for aggregation_offset in 0..2 {
                    graph.add_edge(
                        NodeIndex::new(edge_base + edge_offset),
                        NodeIndex::new(aggregation_base + aggregation_offset),
                        (),
                    );
                }
            }
        }
        for aggregation in 8..16 {
            let group = (aggregation - 8) % 2;
            for core_offset in 0..2 {
                graph.add_edge(
                    NodeIndex::new(aggregation),
                    NodeIndex::new(16 + group * 2 + core_offset),
                    (),
                );
            }
        }
        graph
    }

    #[test]
    fn route_table_checks_fat_tree_layout_once_for_all_flows() {
        let graph = canonical_k4_fat_tree();
        FAT_TREE_LAYOUT_CHECKS.with(|checks| checks.set(0));

        let routes = compute_shortest_path_route_table(
            &graph,
            [
                (0, NodeIndex::new(0), NodeIndex::new(1)),
                (1, NodeIndex::new(0), NodeIndex::new(2)),
                (2, NodeIndex::new(3), NodeIndex::new(7)),
            ],
        )
        .expect("canonical fat-tree routes should exist");

        assert_eq!(routes.len(), 3);
        FAT_TREE_LAYOUT_CHECKS.with(|checks| {
            assert_eq!(
                checks.get(),
                1,
                "route-table lowering must classify the immutable topology once"
            );
        });
    }

    /// Five nodes in two components, and not a canonical k=2 fat tree, so route selection takes
    /// the A* fallback and `0 -> 3` has no path at all.
    fn split_fallback_graph() -> UnGraph<usize, ()> {
        let mut graph = UnGraph::<usize, ()>::new_undirected();
        for node in 0..5 {
            graph.add_node(node);
        }
        graph.add_edge(NodeIndex::new(0), NodeIndex::new(1), ());
        graph.add_edge(NodeIndex::new(1), NodeIndex::new(2), ());
        graph.add_edge(NodeIndex::new(3), NodeIndex::new(4), ());
        graph
    }

    const BUDGETS: [RouteWorkers; 5] = [
        RouteWorkers::serial(),
        RouteWorkers::new(2),
        RouteWorkers::new(3),
        RouteWorkers::new(7),
        RouteWorkers::new(MAX_ROUTE_WORKERS),
    ];

    #[test]
    fn every_route_worker_budget_selects_the_same_fat_tree_paths() {
        let graph = canonical_k4_fat_tree();
        let flows = (0..8_usize)
            .flat_map(|source| {
                (0..8_usize)
                    .filter(move |target| *target != source)
                    .map(move |target| {
                        (
                            source * 8 + target,
                            NodeIndex::new(source),
                            NodeIndex::new(target),
                        )
                    })
            })
            .collect::<Vec<_>>();

        let serial =
            compute_shortest_path_route_table_with(&graph, flows.iter().copied(), BUDGETS[0])
                .expect("canonical fat-tree routes should exist");
        assert_eq!(serial.len(), flows.len());
        for workers in BUDGETS {
            let parallel =
                compute_shortest_path_route_table_with(&graph, flows.iter().copied(), workers)
                    .expect("canonical fat-tree routes should exist");
            assert_eq!(
                parallel,
                serial,
                "a {}-worker scatter must select the same path for every flow",
                workers.get()
            );
        }
    }

    #[test]
    fn every_route_worker_budget_selects_the_same_astar_fallback_paths() {
        let graph = split_fallback_graph();
        let flows = [
            (0_usize, NodeIndex::new(0), NodeIndex::new(2)),
            (1, NodeIndex::new(2), NodeIndex::new(0)),
            (2, NodeIndex::new(1), NodeIndex::new(2)),
            (3, NodeIndex::new(3), NodeIndex::new(4)),
            (4, NodeIndex::new(0), NodeIndex::new(1)),
        ];

        let serial = compute_shortest_path_route_table_with(&graph, flows, BUDGETS[0])
            .expect("both components are internally connected");
        for workers in BUDGETS {
            let parallel = compute_shortest_path_route_table_with(&graph, flows, workers)
                .expect("both components are internally connected");
            assert_eq!(
                parallel,
                serial,
                "a {}-worker scatter must not change the A* fallback selection",
                workers.get()
            );
        }
    }

    #[test]
    fn route_table_reports_the_first_failing_submission_position() {
        let graph = split_fallback_graph();
        let reachable = (NodeIndex::new(0), NodeIndex::new(2));
        let unreachable = (NodeIndex::new(0), NodeIndex::new(3));

        // An unreachable flow submitted before a repeated key outranks that key.
        let unreachable_first = [
            (10_usize, reachable.0, reachable.1),
            (11, unreachable.0, unreachable.1),
            (12, reachable.0, reachable.1),
            (10, reachable.0, reachable.1),
        ];
        // A repeated key submitted before an unreachable flow outranks that flow, and the route
        // for the repeated position is never requested.
        let duplicate_first = [
            (20_usize, reachable.0, reachable.1),
            (20, reachable.0, reachable.1),
            (21, unreachable.0, unreachable.1),
        ];
        // A position that is both a repeat and unreachable is still reported as a repeat.
        let duplicate_and_unreachable = [
            (30_usize, reachable.0, reachable.1),
            (30, unreachable.0, unreachable.1),
        ];

        for workers in BUDGETS {
            assert_eq!(
                compute_shortest_path_route_table_with(&graph, unreachable_first, workers),
                Err(RouteTableError::Unreachable(11)),
                "budget {} must report the earlier unreachable flow",
                workers.get()
            );
            assert_eq!(
                compute_shortest_path_route_table_with(&graph, duplicate_first, workers),
                Err(RouteTableError::DuplicateKey(20)),
                "budget {} must report the earlier repeated key",
                workers.get()
            );
            assert_eq!(
                compute_shortest_path_route_table_with(&graph, duplicate_and_unreachable, workers),
                Err(RouteTableError::DuplicateKey(30)),
                "budget {} must prefer the repeat diagnosis at a shared position",
                workers.get()
            );
        }
    }

    #[test]
    fn an_empty_flow_list_needs_no_route_worker() {
        for workers in BUDGETS {
            let routes = compute_shortest_path_route_table_with(
                &canonical_k4_fat_tree(),
                std::iter::empty::<(usize, NodeIndex, NodeIndex)>(),
                workers,
            )
            .expect("an empty submission cannot fail");
            assert!(routes.is_empty());
            assert_eq!(route_chunk_count(0, workers), 0);
        }
    }

    #[test]
    fn a_wide_budget_still_classifies_the_topology_once() {
        let graph = canonical_k4_fat_tree();
        let flows = (0..8_usize)
            .map(|target| (target, NodeIndex::new(0), NodeIndex::new(target)))
            .collect::<Vec<_>>();
        FAT_TREE_LAYOUT_CHECKS.with(|checks| checks.set(0));

        let routes = compute_shortest_path_route_table_with(
            &graph,
            flows,
            RouteWorkers::new(MAX_ROUTE_WORKERS),
        )
        .expect("canonical fat-tree routes should exist");

        assert_eq!(routes.len(), 8);
        FAT_TREE_LAYOUT_CHECKS.with(|checks| {
            assert_eq!(
                checks.get(),
                1,
                "the scatter must reuse the one classification taken on the submitting thread"
            );
        });
    }

    #[test]
    fn classified_fat_tree_route_table_preserves_one_off_route_selection() {
        let graph = canonical_k4_fat_tree();
        let flows = (0..8)
            .flat_map(|source| {
                (0..8)
                    .filter(move |target| *target != source)
                    .map(move |target| {
                        (
                            (source, target),
                            NodeIndex::new(source),
                            NodeIndex::new(target),
                        )
                    })
            })
            .collect::<Vec<_>>();

        let routes = compute_shortest_path_route_table(&graph, flows.iter().copied())
            .expect("canonical fat-tree routes should exist");
        for ((source, target), source_node, target_node) in flows {
            let one_off = ShortestPath::try_compute_route_in(&graph, source_node, target_node)
                .expect("one-off canonical fat-tree route should exist");
            assert_eq!(
                routes[&(source, target)],
                one_off,
                "cached classification must not change equal-cost path selection"
            );
        }
    }

    #[test]
    fn test_ecmp_routing() {
        // Build a graph with multiple equal-cost paths between nodes 0 and 3
        // Graph structure:
        //     1
        //    / \
        //   0   3
        //    \ /
        //     2

        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ()); // Edge 0-1
        graph.add_edge(node1, node3, ()); // Edge 1-3
        graph.add_edge(node0, node2, ()); // Edge 0-2
        graph.add_edge(node2, node3, ()); // Edge 2-3

        // Create an ECMP routing instance
        let flow_id = 1;
        let source_host = 0;
        let sink_host = 3;
        let mut ecmp = ECMP::new(graph, flow_id, source_host, sink_host);

        // Compute the route from node 0 to node 3
        let start = NodeIndex::new(source_host);
        let end = NodeIndex::new(sink_host);
        let path = ecmp.compute_route(start, end);

        // There are two equal-cost paths: [0, 1, 3] and [0, 2, 3]
        let possible_paths = [
            vec![start, NodeIndex::new(1), end],
            vec![start, NodeIndex::new(2), end],
        ];

        // Check that the computed path is one of the possible equal-cost paths
        assert!(
            possible_paths.contains(&path),
            "ECMP routing did not find an equal-cost path"
        );
    }

    #[test]
    fn test_no_path() {
        // Build a disconnected graph where no path exists between nodes 0 and 3
        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let _node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ());
        graph.add_edge(node1, node2, ());
        // Note: No edge connecting to node3

        // Test ShortestPath routing for no path scenario
        let mut shortest_path = ShortestPath::new(graph.clone());
        let start = NodeIndex::new(0);
        let end = NodeIndex::new(3);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shortest_path.compute_route(start, end);
        }));

        assert!(
            result.is_err(),
            "ShortestPath should panic when no path exists"
        );

        // Test ECMP routing for no path scenario
        let flow_id = 1;
        let source_host = 0;
        let sink_host = 3;
        let mut ecmp = ECMP::new(graph, flow_id, source_host, sink_host);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ecmp.compute_route(start, end);
        }));

        assert!(result.is_err(), "ECMP should panic when no path exists");
    }

    #[test]
    fn test_ecmp_hashing_is_deterministic() {
        // Build a graph with multiple equal-cost paths between nodes 0 and 3
        let mut graph = UnGraph::<usize, ()>::new_undirected();
        let node0 = graph.add_node(0);
        let node1 = graph.add_node(1);
        let node2 = graph.add_node(2);
        let node3 = graph.add_node(3);

        graph.add_edge(node0, node1, ()); // Edge 0-1
        graph.add_edge(node1, node3, ()); // Edge 1-3
        graph.add_edge(node0, node2, ()); // Edge 0-2
        graph.add_edge(node2, node3, ()); // Edge 2-3

        let source_host = 0;
        let sink_host = 3;

        let mut ecmp = ECMP::new(graph.clone(), 1, source_host, sink_host);
        let start = NodeIndex::new(source_host);
        let end = NodeIndex::new(sink_host);

        let path1 = ecmp.compute_route(start, end);
        let path2 = ecmp.compute_route(start, end);

        assert_eq!(path1, path2, "ECMP path selection should be stable");
    }

    #[test]
    fn test_shortest_path_custom_five_node_graph_falls_back_from_fat_tree_fast_path() {
        let edges = [(0_u32, 2_u32), (0, 3), (1, 2), (1, 3), (0, 4), (3, 4)];
        let graph = UnGraph::<usize, ()>::from_edges(edges);
        let canonical_graph = canonical_routing_graph(&graph);
        assert_eq!(
            ShortestPath::fat_tree_params(&canonical_graph),
            None,
            "the six-edge custom graph must reject the four-edge canonical k=2 layout"
        );

        let path = ShortestPath::compute_route_in(&graph, NodeIndex::new(0), NodeIndex::new(1));

        assert_eq!(path.first(), Some(&NodeIndex::new(0)));
        assert_eq!(path.last(), Some(&NodeIndex::new(1)));
        assert_eq!(path.len(), 3, "expected a valid 2-hop shortest path");

        for window in path.windows(2) {
            assert!(graph.contains_edge(window[0], window[1]));
        }

        let reordered_graph = UnGraph::<usize, ()>::from_edges(edges.into_iter().rev());
        let reordered_path =
            ShortestPath::compute_route_in(&reordered_graph, NodeIndex::new(0), NodeIndex::new(1));
        assert_eq!(
            reordered_path, path,
            "equivalent edge collections must select the same shortest path"
        );

        let duplicate = compute_shortest_path_route_table(
            &graph,
            [
                (7, NodeIndex::new(0), NodeIndex::new(1)),
                (7, NodeIndex::new(1), NodeIndex::new(0)),
            ],
        );
        assert_eq!(duplicate, Err(RouteTableError::DuplicateKey(7)));
    }
}

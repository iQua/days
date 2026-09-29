//! The routing protocols that are used to compute the path that each flow
//! takes. Currently, three routing protocols have been implemented:
//!
//! - Shortest path routing: Selects one shortest path per endpoint pair, deterministically.
//!   The graph is first canonicalized (`canonical_routing_graph`) so that selection cannot
//!   depend on the order edges were inserted. A canonical fat tree then takes a uniform-cost
//!   best-first search whose neighbours are enumerated by index arithmetic in a fixed order, and
//!   every other topology falls back to the `petgraph` A* implementation at uniform edge cost. The
//!   route table runs the fat-tree search once per source switch, to exhaustion, and reads every
//!   route from that source out of the one search tree; the route it reads is the one a search
//!   stopping at the target selects (the argument is on `FatTreeSearch`).
//!   Equal-cost alternatives are broken by that fixed enumeration order, not by choice: no
//!   randomness, no hash-map iteration, and no shared state is involved, so the same graph and
//!   endpoints always yield the same path on any thread. (A `RandomSimplePath` protocol, which did
//!   draw uniformly from `all_simple_paths`, existed until 2023; it was deleted, and nothing in
//!   the tree selects a random route today.)
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
    /// Fat-tree classifications taken on this thread *outside* per-flow route filling.
    static FAT_TREE_LAYOUT_CHECKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Set while this thread is inside [`fill_route_chunk`], on a worker or on the submitter.
    static FILLING_ROUTE_CHUNK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Fat-tree classifications taken *while per-flow routes are being filled*, on any thread.
///
/// The scatter moves per-flow work onto worker threads, and every worker thread gets its own
/// copy of a `thread_local!`. A per-thread counter therefore cannot see a classification made by
/// a worker: the submitting thread would read its own untouched cell and report success. This
/// counter is process-wide, so it sees every thread.
///
/// It is only ever incremented, never reset. Unrelated tests routing concurrently in the same
/// test binary must not be able to zero it between a regression and the assertion that catches
/// it; and since the correct value is zero for every caller, concurrent traffic cannot inflate it
/// either. `Relaxed` suffices because each reader is ordered after its own workers by the
/// `thread::scope` join.
#[cfg(test)]
static ROUTE_FILL_LAYOUT_CHECKS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Marks its enclosing scope as per-flow route filling for the classification counter.
#[cfg(test)]
struct RouteFillScope(bool);

#[cfg(test)]
impl RouteFillScope {
    fn enter() -> Self {
        RouteFillScope(FILLING_ROUTE_CHUNK.with(|filling| filling.replace(true)))
    }
}

#[cfg(test)]
impl Drop for RouteFillScope {
    fn drop(&mut self) {
        FILLING_ROUTE_CHUNK.with(|filling| filling.set(self.0));
    }
}

/// Work counter for the fat-tree search, carried by value from each route worker to the submitter.
///
/// In test builds it counts neighbour examinations: one per candidate successor the search
/// considers, whether or not that successor is pushed. It is the search's unit of work, and a
/// pure function of the graph and the sources searched. Without the test hooks it is zero-sized
/// and its methods are empty, so production search carries no counter (checked at compile time
/// below).
#[derive(Clone, Copy, Debug, Default)]
struct SearchProbe {
    #[cfg(any(test, feature = "test"))]
    neighbour_examinations: u64,
}

impl SearchProbe {
    #[inline(always)]
    fn examine_neighbour(&mut self) {
        #[cfg(any(test, feature = "test"))]
        {
            self.neighbour_examinations += 1;
        }
    }

    /// Adds another worker's count, received with that worker's routes.
    #[inline(always)]
    fn absorb(&mut self, _other: SearchProbe) {
        #[cfg(any(test, feature = "test"))]
        {
            self.neighbour_examinations += _other.neighbour_examinations;
        }
    }

    /// Adds this route table's count to the submitting thread's running total.
    #[inline(always)]
    fn record_on_submitting_thread(self) {
        #[cfg(any(test, feature = "test"))]
        ROUTE_TABLE_NEIGHBOUR_EXAMINATIONS
            .with(|total| total.set(total.get() + self.neighbour_examinations));
    }
}

// Without the test hooks the probe costs nothing: it is zero-sized. Giving it a field that
// production builds keep fails the build.
#[cfg(not(any(test, feature = "test")))]
const _: () = assert!(std::mem::size_of::<SearchProbe>() == 0);

#[cfg(any(test, feature = "test"))]
std::thread_local! {
    /// Neighbour examinations of every shortest-path route table submitted from this thread.
    ///
    /// Route workers never touch it: each returns its own count with its routes, and the
    /// submitting thread adds the sum here after the join.
    static ROUTE_TABLE_NEIGHBOUR_EXAMINATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Neighbour examinations the fat-tree search has made for every shortest-path route table
/// submitted from the calling thread, cumulative since the thread started (test hooks only).
///
/// The count covers Days' own fat-tree search. The `petgraph` A* fallback is third-party code and
/// is not counted.
#[cfg(any(test, feature = "test"))]
pub fn route_table_neighbour_examinations_for_testing() -> u64 {
    ROUTE_TABLE_NEIGHBOUR_EXAMINATIONS.with(std::cell::Cell::get)
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
    /// Shared semantic-hash ECMP selection on a canonical fat tree.
    FatTreeEcmp {
        flow_hash: u64,
    },
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
        if FILLING_ROUTE_CHUNK.with(std::cell::Cell::get) {
            ROUTE_FILL_LAYOUT_CHECKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            FAT_TREE_LAYOUT_CHECKS.with(|checks| checks.set(checks.get() + 1));
        }

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

    fn try_compute_route_in_canonical_graph(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
    ) -> Option<Vec<NodeIndex>> {
        Self::fat_tree_params(graph)
            .and_then(|params| FatTreeSearch::new(graph.node_count(), params).route(start, end))
            .or_else(|| astar_route(graph, start, end))
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

/// The A* search every non-fat-tree route takes, at uniform edge cost.
fn astar_route(
    graph: &UnGraph<usize, ()>,
    start: NodeIndex,
    end: NodeIndex,
) -> Option<Vec<NodeIndex>> {
    astar(
        graph,
        start,
        |n| n == end,
        |_| 1, // Uniform cost
        |_| 0, // Heuristic ignored for uniform cost
    )
    .map(|(_, path)| path)
}

/// Uniform-cost best-first search over a canonical fat tree, whose neighbours are enumerated by
/// index arithmetic in a fixed order.
///
/// One value is owned by one route worker and reused for every search that worker runs: the score
/// and predecessor arrays are allocated once per worker and reset before each search, never shared.
///
/// ONE TREE PER SOURCE. A search can stop when it pops the target (`route`, the one-off API), or
/// run until the heap is empty (`tree_route`, the route table), leaving in `came_from` a search
/// tree from which the route to every target is read. The two select the same route to every
/// target, byte for byte:
/// 1. The target is read in one place only, the pop-time test `Some(node) == stop`. Until the
///    target is first popped, the sequence of heap pushes and pops, including `BinaryHeap` sift
///    order and every [`MinScoredNode`] tie, is therefore a function of the graph and the source
///    alone. An exhaustive search executes exactly the early-stopping search's operations, then
///    continues.
/// 2. Every edge costs 1 and the heuristic is 0, so a node popped at cost `g` pushes neighbours at
///    `g + 1`, never below the heap's minimum: popped costs never decrease. A node is re-pushed,
///    and its `came_from` rewritten, only at a cost strictly below its current score. Its first
///    push was at `g' + 1`, where `g'` is its first pusher's cost, and every later pusher was
///    popped at a cost of at least `g'`, so that never happens: `came_from[x]` is written exactly
///    once, at `x`'s first push, and never changes afterwards.
/// 3. When the early-stopping search pops the target, every entry on the chain from the target back
///    to the source was written at a first push before that pop. The exhaustive search reaches the
///    same state and never rewrites those entries, so its chain from the target is the same path.
///    The target is popped by the early-stopping search exactly when the exhaustive search pushes
///    it (every pushed node is eventually popped), so `scores[target]` decides reachability.
///
/// The argument relies on the search's own operations, not on the internals of `BinaryHeap`.
/// `tests::every_edge_switch_pair_routes_as_the_reference_search` checks it against the
/// early-stopping search as it stood at `7348a73` (kept verbatim in `reference_search`) for every
/// ordered pair of edge switches at k = 4, 8 and 16.
struct FatTreeSearch {
    /// `(edge switches, switches per pod, pods)` of the canonical fat tree.
    params: (usize, usize, usize),
    scores: Vec<Option<(usize, usize, usize)>>,
    came_from: Vec<usize>,
    visit_next: BinaryHeap<MinScoredNode>,
    /// The source whose complete search tree `came_from` holds, if it holds one.
    tree_source: Option<NodeIndex>,
    probe: SearchProbe,
}

impl FatTreeSearch {
    fn new(node_count: usize, params: (usize, usize, usize)) -> Self {
        FatTreeSearch {
            params,
            scores: vec![None; node_count],
            came_from: vec![usize::MAX; node_count],
            visit_next: BinaryHeap::new(),
            tree_source: None,
            probe: SearchProbe::default(),
        }
    }

    /// Whether both endpoints are edge switches, the only endpoints the fat-tree search serves.
    fn serves(&self, start: NodeIndex, end: NodeIndex) -> bool {
        let (num_layer_switches, _, _) = self.params;
        start.index() < num_layer_switches && end.index() < num_layer_switches
    }

    /// The route to `end` with early stopping, or `None` when the fat-tree search does not serve
    /// the endpoints or never pops `end`.
    fn route(&mut self, start: NodeIndex, end: NodeIndex) -> Option<Vec<NodeIndex>> {
        if !self.serves(start, end) {
            return None;
        }
        self.search(start, Some(end))
            .then(|| self.path_from_came_from(start, end))
    }

    /// The route to `end` read from the complete search tree from `start`, which is built only when
    /// `came_from` does not already hold it. Selects exactly what [`Self::route`] selects.
    fn tree_route(&mut self, start: NodeIndex, end: NodeIndex) -> Option<Vec<NodeIndex>> {
        if !self.serves(start, end) {
            return None;
        }
        if self.tree_source != Some(start) {
            self.search(start, None);
            self.tree_source = Some(start);
        }
        self.scores[end.index()]
            .is_some()
            .then(|| self.path_from_came_from(start, end))
    }

    /// Runs the search from `start` until it pops `stop` (returning `true`) or empties the heap.
    fn search(&mut self, start: NodeIndex, stop: Option<NodeIndex>) -> bool {
        let (num_layer_switches, switches_per_pod, num_pods) = self.params;
        let FatTreeSearch {
            scores,
            came_from,
            visit_next,
            tree_source,
            probe,
            ..
        } = self;
        scores.fill(None);
        came_from.fill(usize::MAX);
        visit_next.clear();
        *tree_source = None;

        let core_start = 2 * num_layer_switches;
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
            if Some(node) == stop {
                return true;
            }

            let node_idx = node.index();
            if let Some((_, _, old_g)) = scores[node_idx] {
                if old_g < g {
                    continue;
                }
            }
            scores[node_idx] = Some((f, h, g));

            let mut push_neighbor = |neigh: NodeIndex| {
                probe.examine_neighbour();
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

        false
    }

    /// The chain `end -> ... -> start` in `came_from`, reversed.
    fn path_from_came_from(&self, start: NodeIndex, end: NodeIndex) -> Vec<NodeIndex> {
        let start_idx = start.index();
        let mut path = vec![end];
        let mut current = end.index();
        while current != start_idx {
            let previous = self.came_from[current];
            if previous == usize::MAX {
                break;
            }
            path.push(NodeIndex::new(previous));
            current = previous;
        }
        path.reverse();
        path
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
    /// A policy that is defined only for one topology family was asked for on another.
    UnsupportedTopology,
}

/// Largest host-thread budget the per-flow route scatter will use.
///
/// Every worker owns one disjoint range of flows, so a budget wider than this only adds thread
/// creation to a phase that is already bounded by memory bandwidth.
pub const MAX_ROUTE_WORKERS: usize = 64;

/// Host-thread budget for per-flow route computation.
///
/// A route is a pure function of the canonical topology graph and the flow endpoints: the
/// pathfinder reads an immutable graph, draws no randomness, and never iterates a hash map. The
/// only state carried across flows is owned by one worker: on a canonical fat tree, the worker's
/// search state holds one source's complete search tree, from which it reads every route from that
/// source; everywhere, a flow whose endpoints equal the previous flow's in the worker's part copies
/// that route. By purity a read or a copy equals a fresh search. The budget therefore changes only
/// how much wall clock the route table costs, never which path a flow receives.
/// `RouteWorkers::serial()` keeps the single-threaded reference available so equality gates can
/// pin the parallel scatter against it.
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

/// Fills one worker's disjoint result slice from its disjoint endpoint slice, by A*.
///
/// The two slices are the same contiguous index range of the flow list, so this is an
/// index-addressed scatter: no worker can observe or reach another worker's slot.
///
/// A route is a pure function of the graph and the endpoints, so a flow whose endpoints equal the
/// previous flow's in the same slice copies that route instead of searching again. Canonical flow
/// order makes such runs common: every stage of one collective rank sends from the same source to
/// the same sink, and a search can cost O(hosts) (the A* fallback on a star expands every leaf).
fn fill_route_chunk(
    graph: &UnGraph<usize, ()>,
    endpoints: &[(NodeIndex, NodeIndex)],
    routes: &mut [Option<Vec<NodeIndex>>],
) {
    #[cfg(test)]
    let _route_fill_scope = RouteFillScope::enter();

    for index in 0..endpoints.len() {
        let (source, target) = endpoints[index];
        routes[index] = if index > 0 && endpoints[index - 1] == (source, target) {
            routes[index - 1].clone()
        } else {
            astar_route(graph, source, target)
        };
    }
}

/// Computes one A* route per endpoint pair into an index-addressed buffer, for a graph that is not
/// a canonical fat tree.
///
/// `None` marks an unreachable pair; the caller decides which position becomes the reported error.
fn scatter_routes(
    graph: &UnGraph<usize, ()>,
    endpoints: &[(NodeIndex, NodeIndex)],
    workers: RouteWorkers,
) -> Vec<Option<Vec<NodeIndex>>> {
    let mut routes = vec![None; endpoints.len()];
    let chunk_len = route_chunk_len(endpoints.len(), workers);
    if route_chunk_count(endpoints.len(), workers) < 2 {
        fill_route_chunk(graph, endpoints, &mut routes);
        return routes;
    }

    thread::scope(|scope| {
        for (endpoints, routes) in endpoints
            .chunks(chunk_len)
            .zip(routes.chunks_mut(chunk_len))
        {
            scope.spawn(move || {
                #[cfg(test)]
                let _route_fill_scope = RouteFillScope::enter();

                fill_route_chunk(graph, endpoints, routes)
            });
        }
    });
    routes
}

/// Most switches a canonical fat-tree route visits (edge, aggregation, core, aggregation, edge):
/// the partition's estimate of the work of reading one route from a search tree.
const FAT_TREE_ROUTE_READ_WEIGHT: u128 = 5;

/// Splits the source-grouped flow positions into at most `workers` contiguous parts of about equal
/// estimated work.
///
/// `grouped` lists flow positions by ascending source switch, and in submission order within one
/// source. Each position weighs [`FAT_TREE_ROUTE_READ_WEIGHT`]; the first position of each source
/// also carries `tree_weight`, the pops and neighbour examinations of one exhaustive search. A
/// position goes to part `floor(w * parts / total)`, where `w` is the weight of the positions
/// before it, so every part's weight is within one tree and one route of `total / parts`: a source
/// with many flows is split between parts instead of loading one worker, and each part holding a
/// piece of it builds that source's tree itself (at most `parts - 1` extra trees in all).
///
/// The partition is a pure function of the endpoints, the graph's size and the budget. It decides
/// only which worker computes a route, never which route a flow receives.
fn source_group_parts(
    endpoints: &[(NodeIndex, NodeIndex)],
    grouped: &[usize],
    tree_weight: u128,
    workers: RouteWorkers,
) -> Vec<std::ops::Range<usize>> {
    if grouped.is_empty() {
        return Vec::new();
    }
    let parts = workers.get().min(grouped.len()) as u128;
    let weight = |index: usize| {
        let starts_source =
            index == 0 || endpoints[grouped[index - 1]].0 != endpoints[grouped[index]].0;
        FAT_TREE_ROUTE_READ_WEIGHT + if starts_source { tree_weight } else { 0 }
    };
    let total = (0..grouped.len()).map(weight).sum::<u128>();

    let mut ranges = Vec::new();
    let mut part_start = 0;
    let mut current_part = 0;
    let mut before = 0;
    for index in 0..grouped.len() {
        // The part is `floor(before * parts / total)`. It never decreases, so it advances by
        // comparison instead of a division per position.
        let mut part = current_part;
        while (part + 1) * total <= before * parts {
            part += 1;
        }
        if part != current_part {
            if part_start < index {
                ranges.push(part_start..index);
            }
            part_start = index;
            current_part = part;
        }
        before += weight(index);
    }
    ranges.push(part_start..grouped.len());
    ranges
}

/// Routes one part of the source-grouped flow positions with this worker's own search state.
///
/// Returns the routes in the part's order, and the search work they took; the submitter writes
/// each route to its flow's position.
fn route_source_groups(
    graph: &UnGraph<usize, ()>,
    params: (usize, usize, usize),
    endpoints: &[(NodeIndex, NodeIndex)],
    positions: &[usize],
) -> (Vec<Option<Vec<NodeIndex>>>, SearchProbe) {
    #[cfg(test)]
    let _route_fill_scope = RouteFillScope::enter();

    let mut search = FatTreeSearch::new(graph.node_count(), params);
    let mut routes: Vec<Option<Vec<NodeIndex>>> = Vec::with_capacity(positions.len());
    for (index, &position) in positions.iter().enumerate() {
        let (source, target) = endpoints[position];
        let route = if index > 0 && endpoints[positions[index - 1]] == (source, target) {
            routes[index - 1].clone()
        } else {
            search
                .tree_route(source, target)
                .or_else(|| astar_route(graph, source, target))
        };
        routes.push(route);
    }
    (routes, search.probe)
}

/// Computes one route per endpoint pair on a canonical fat tree, one search tree per source.
///
/// Flow positions are grouped by source switch (a stable sort, so submission order is kept within
/// a source) and split by [`source_group_parts`]. Each worker receives its part, builds each of its
/// sources' search tree once in search state it owns, reads every route of that source from the
/// tree, and returns the routes; the submitter writes them to their positions after the join. No
/// state is shared or mutated across workers, and every route equals the one the early-stopping
/// search selects (see [`FatTreeSearch`]), so the routes are the same for every budget.
///
/// A flow whose endpoints equal the previous flow's in the same part copies that route. Grouping
/// by source makes the coll lane's consecutive-endpoint reuse apply to every flow of a pair that
/// lands in one part, wherever its flows sit in submission order; by purity the copy equals a read.
fn scatter_fat_tree_routes(
    graph: &UnGraph<usize, ()>,
    params: (usize, usize, usize),
    endpoints: &[(NodeIndex, NodeIndex)],
    workers: RouteWorkers,
) -> (Vec<Option<Vec<NodeIndex>>>, SearchProbe) {
    let mut grouped = (0..endpoints.len()).collect::<Vec<_>>();
    grouped.sort_by_key(|&position| endpoints[position].0);
    let tree_weight = graph.node_count() as u128 + 2 * graph.edge_count() as u128;
    let parts = source_group_parts(endpoints, &grouped, tree_weight, workers);

    let mut routes = vec![None; endpoints.len()];
    let mut probe = SearchProbe::default();
    let mut write = |part: &std::ops::Range<usize>, (part_routes, part_probe): (Vec<_>, _)| {
        for (&position, route) in grouped[part.clone()].iter().zip(part_routes) {
            routes[position] = route;
        }
        probe.absorb(part_probe);
    };
    if parts.len() < 2 {
        for part in &parts {
            write(
                part,
                route_source_groups(graph, params, endpoints, &grouped[part.clone()]),
            );
        }
        return (routes, probe);
    }

    thread::scope(|scope| {
        let workers = parts
            .iter()
            .map(|part| {
                let positions = &grouped[part.clone()];
                scope.spawn(move || route_source_groups(graph, params, endpoints, positions))
            })
            .collect::<Vec<_>>();
        for (part, worker) in parts.iter().zip(workers) {
            write(part, worker.join().expect("a route worker panicked"));
        }
    });
    (routes, probe)
}

/// One flow's routing request under [`compute_fat_tree_ecmp_route_table`].
///
/// `flow_hash` stands in for the header fields a real switch hashes. Lowering derives it from the
/// flow's own semantic identity, so the selection is a pure function of the scenario and is
/// reproduced bit-for-bit by every backend and every host-thread budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EcmpFlow<K> {
    pub key: K,
    pub source_switch: NodeIndex,
    pub target_switch: NodeIndex,
    pub flow_hash: u64,
}

/// Equal-cost multipath selection over a canonical fat tree (T21/P12).
///
/// WHY THIS EXISTS. [`compute_shortest_path_route_table`] returns ONE shortest path per switch
/// pair. Equal-cost successors tie on the score tuple and [`MinScoredNode`]'s `cmp` returns
/// `Ordering::Equal` on a tie — node identity is never consulted — so which equal-cost successor
/// wins is `BinaryHeap` sift order over a reversed push sequence: an implementation artifact, not
/// a stated policy.
///
/// The consequence is severe concentration, and how severe depends on the traffic matrix — a
/// distinction worth keeping, because collapsing the two is how this was first written down wrong.
/// Over the full cross-pod pair set at k = 8 each pod still reaches three of four aggregation
/// groups. Under an OFFSET PERMUTATION — which is exactly what E1 and E2 use — it collapses: every
/// cross-pod route leaving a given pod takes one aggregation group and one core switch.
///
/// Fabric-wide it is never one switch. Measured over E1's matrix (k = 32, 512 edge switches,
/// `s -> (s + 256) mod 512`): 6 distinct core switches of 256 carry the traffic, per-pair histogram
/// `{1025: 144, 1121: 32, 1152: 272, 1217: 16, 1233: 16, 1264: 32}` — the busiest core takes 272
/// of 512 edge-switch pairs, 53%, i.e. roughly 4,352 of the 8,192 flows; six of sixteen
/// aggregation groups are used. Still a bottleneck no real fabric has: at a nominal 10% offered
/// load E1 dropped 36,384 of 139,264 sourced packets (26.1%), and drops zero under this policy.
/// Every external arm in the P12 roster spreads that traffic (Unison exposes `--ecmp`; GeDES
/// reports no queue overflow at 90% on the same matrix), so a cross-arm fixture routed single-path
/// would not be expressing the same fabric as the arms it is compared against.
///
/// THE SELECTION. An inter-pod path is
/// `edge_s -> agg(pod_s, a) -> core(a, c) -> agg(pod_t, a) -> edge_t`, with aggregation group `a`
/// and core offset `c` taken from disjoint halves of the flow hash. The aggregation group has to
/// be the SAME on both sides, because core switch `core(a, c)` is wired only to the group-`a`
/// aggregation switch of each pod. An intra-pod path is `edge_s -> agg(pod, a) -> edge_t`, and
/// endpoints sharing an edge switch give the same one-hop path the shortest-path table gives.
///
/// Selection is O(1) per flow and searches nothing, so it runs serially: the host-thread budget is
/// irrelevant to it and cannot perturb it.
pub fn compute_fat_tree_ecmp_route_table<K>(
    graph: &UnGraph<usize, ()>,
    flows: impl IntoIterator<Item = EcmpFlow<K>>,
) -> Result<BTreeMap<K, Vec<NodeIndex>>, RouteTableError<K>>
where
    K: Ord,
{
    let graph = canonical_routing_graph(graph);
    let Some((num_layer_switches, switches_per_pod, _)) = ShortestPath::fat_tree_params(&graph)
    else {
        return Err(RouteTableError::UnsupportedTopology);
    };
    let core_start = 2 * num_layer_switches;
    let groups = switches_per_pod as u64;

    let mut ordered = Vec::new();
    for flow in flows {
        let source = flow.source_switch.index();
        let target = flow.target_switch.index();
        if source >= num_layer_switches || target >= num_layer_switches {
            ordered.push((flow.key, None));
            continue;
        }
        let route = if source == target {
            vec![NodeIndex::new(source)]
        } else {
            let group = (flow.flow_hash % groups) as usize;
            let source_pod = source / switches_per_pod;
            let target_pod = target / switches_per_pod;
            let source_aggregation = num_layer_switches + source_pod * switches_per_pod + group;
            if source_pod == target_pod {
                vec![
                    NodeIndex::new(source),
                    NodeIndex::new(source_aggregation),
                    NodeIndex::new(target),
                ]
            } else {
                let core = ((flow.flow_hash >> 32) % groups) as usize;
                vec![
                    NodeIndex::new(source),
                    NodeIndex::new(source_aggregation),
                    NodeIndex::new(core_start + group * switches_per_pod + core),
                    NodeIndex::new(num_layer_switches + target_pod * switches_per_pod + group),
                    NodeIndex::new(target),
                ]
            }
        };
        ordered.push((flow.key, Some(route)));
    }

    let first_duplicate = {
        let keys = ordered.iter().map(|(key, _)| key).collect::<Vec<_>>();
        first_repeated_key(&keys)
    };
    if let Some(index) = ordered.iter().position(|(_, route)| route.is_none()) {
        return Err(RouteTableError::Unreachable(ordered.swap_remove(index).0));
    }
    if let Some(index) = first_duplicate {
        return Err(RouteTableError::DuplicateKey(ordered.swap_remove(index).0));
    }
    Ok(ordered
        .into_iter()
        .map(|(key, route)| {
            (
                key,
                route.expect("unreachable routes were rejected before the table was built"),
            )
        })
        .collect())
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
    let routes = match fat_tree_params {
        Some(params) => {
            let (routes, probe) =
                scatter_fat_tree_routes(&graph, params, &endpoints[..routed], workers);
            probe.record_on_submitting_thread();
            routes
        }
        None => scatter_routes(&graph, &endpoints[..routed], workers),
    };

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

/// The early-stopping fat-tree search as it stood at `7348a73`, retained verbatim as the oracle of
/// the per-source search tree's identity gate (the T20j pattern).
///
/// Production builds one exhaustive search tree per source and reads every route from it. These
/// two functions are the per-flow search it replaced: `compute_fat_tree_route_with_params` stops
/// when it pops `end`, and `try_compute_route_in_classified_canonical_graph` falls back to the
/// `petgraph` A* search exactly as the table did. They exist only in unit-test builds, so the
/// equality tests below can compare every route production selects against the route this search
/// selects, on the same graph and endpoints.
#[cfg(test)]
mod reference_search {
    use std::collections::BinaryHeap;

    use petgraph::algo::astar;
    use petgraph::graph::{NodeIndex, UnGraph};

    use super::MinScoredNode;

    pub(super) fn compute_fat_tree_route_with_params(
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

    pub(super) fn try_compute_route_in_classified_canonical_graph(
        graph: &UnGraph<usize, ()>,
        start: NodeIndex,
        end: NodeIndex,
        fat_tree_params: Option<(usize, usize, usize)>,
    ) -> Option<Vec<NodeIndex>> {
        if let Some(params) = fat_tree_params {
            if let Some(path) = compute_fat_tree_route_with_params(graph, start, end, params) {
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

    /// The canonical k-ary fat tree's switch graph: edge switches, then aggregation, then core.
    fn canonical_fat_tree(k: usize) -> UnGraph<usize, ()> {
        let edge_switches = k * k / 2;
        let per_pod = k / 2;
        let mut graph = UnGraph::with_capacity(5 * k * k / 4, k * k * k / 2);
        for node in 0..5 * k * k / 4 {
            graph.add_node(node);
        }
        for edge in 0..edge_switches {
            let aggregation_base = edge_switches + (edge / per_pod) * per_pod;
            for offset in 0..per_pod {
                graph.add_edge(
                    NodeIndex::new(edge),
                    NodeIndex::new(aggregation_base + offset),
                    (),
                );
            }
        }
        for aggregation in edge_switches..2 * edge_switches {
            let group = (aggregation - edge_switches) % per_pod;
            for offset in 0..per_pod {
                graph.add_edge(
                    NodeIndex::new(aggregation),
                    NodeIndex::new(2 * edge_switches + group * per_pod + offset),
                    (),
                );
            }
        }
        graph
    }

    /// The route the pre-tree table selected for one flow: the early-stopping search, or A*.
    fn reference_route(
        graph: &UnGraph<usize, ()>,
        source: NodeIndex,
        target: NodeIndex,
    ) -> Option<Vec<NodeIndex>> {
        let graph = canonical_routing_graph(graph);
        let params = ShortestPath::fat_tree_params(&graph);
        reference_search::try_compute_route_in_classified_canonical_graph(
            &graph, source, target, params,
        )
    }

    /// Every ordered pair of `endpoints` routed through the table under each budget, and one at a
    /// time, compared against the reference search.
    fn assert_pairs_route_as_the_reference(
        k: usize,
        endpoints: usize,
        budgets: &[RouteWorkers],
        label: &str,
    ) {
        let graph = canonical_fat_tree(k);
        let flows = (0..endpoints)
            .flat_map(|source| {
                (0..endpoints).map(move |target| {
                    (
                        (source, target),
                        NodeIndex::new(source),
                        NodeIndex::new(target),
                    )
                })
            })
            .collect::<Vec<_>>();
        let reference = flows
            .iter()
            .map(|&(key, source, target)| {
                let route = reference_route(&graph, source, target)
                    .expect("a canonical fat tree is connected");
                (key, route)
            })
            .collect::<BTreeMap<_, _>>();
        for &workers in budgets {
            let table =
                compute_shortest_path_route_table_with(&graph, flows.iter().copied(), workers)
                    .expect("a canonical fat tree is connected");
            assert!(
                table == reference,
                "k={k}, {} workers: the route table must select the reference route for every \
                 {label} pair",
                workers.get()
            );
        }
        for &(key, source, target) in &flows {
            assert_eq!(
                ShortestPath::try_compute_route_in(&graph, source, target).as_ref(),
                reference.get(&key),
                "k={k}: the one-off route {key:?} must be the reference route"
            );
        }
    }

    /// Every ordered pair of edge switches at k = 4, 8 and 16 (64 + 1,024 + 16,384 pairs): the
    /// route table and the one-off route select exactly the route the early-stopping reference
    /// search selects, for every worker budget.
    #[test]
    fn every_edge_switch_pair_routes_as_the_reference_search() {
        assert_pairs_route_as_the_reference(4, 8, &BUDGETS, "edge-switch");
        assert_pairs_route_as_the_reference(8, 32, &BUDGETS, "edge-switch");
        assert_pairs_route_as_the_reference(
            16,
            128,
            &[RouteWorkers::serial(), RouteWorkers::new(7)],
            "edge-switch",
        );
    }

    /// Every ordered pair of switches at k = 4 and 8, aggregation and core switches included: an
    /// endpoint outside the edge layer leaves the fat-tree search for A*, and the table must still
    /// select the reference route, for every worker budget.
    #[test]
    fn every_switch_pair_routes_as_the_reference_search() {
        assert_pairs_route_as_the_reference(4, 20, &BUDGETS, "switch");
        assert_pairs_route_as_the_reference(8, 80, &BUDGETS, "switch");
    }

    /// One heavy source among light ones, repeated pairs that are not adjacent in submission order,
    /// a flow within one edge switch, and aggregation and core endpoints: every budget selects the
    /// reference route for every flow, although the partition splits the heavy source's flows
    /// between workers.
    #[test]
    fn a_skewed_flow_list_routes_as_the_reference_search_under_every_budget() {
        let k = 8;
        let graph = canonical_fat_tree(k);
        let edge_switches = k * k / 2;
        let switches = 5 * k * k / 4;
        let mut endpoints = Vec::new();
        for round in 0..6 {
            for target in 0..edge_switches {
                endpoints.push((5, (target * 7 + round) % edge_switches));
            }
            endpoints.push((round, edge_switches - 1 - round));
            endpoints.push((5, 5));
            endpoints.push((edge_switches + round, 3));
            endpoints.push((9, switches - 1 - round));
        }
        let flows = endpoints
            .iter()
            .enumerate()
            .map(|(key, &(source, target))| (key, NodeIndex::new(source), NodeIndex::new(target)))
            .collect::<Vec<_>>();
        let reference = flows
            .iter()
            .map(|&(key, source, target)| {
                let route = reference_route(&graph, source, target)
                    .expect("a canonical fat tree is connected");
                (key, route)
            })
            .collect::<BTreeMap<_, _>>();
        for workers in BUDGETS {
            let table =
                compute_shortest_path_route_table_with(&graph, flows.iter().copied(), workers)
                    .expect("a canonical fat tree is connected");
            assert!(
                table == reference,
                "{} workers: every skewed flow must take the reference route",
                workers.get()
            );
        }
    }

    /// With one worker the search runs exactly one exhaustive tree per distinct source edge switch:
    /// every switch popped once, each of its neighbours examined once, `2E` examinations a tree.
    #[test]
    fn one_route_worker_searches_once_per_source_edge_switch() {
        let k = 8;
        let graph = canonical_fat_tree(k);
        let sources = [0_usize, 3, 17, 31];
        let flows = sources
            .iter()
            .flat_map(|&source| (0..32).map(move |target| (source, target)))
            .rev()
            .enumerate()
            .map(|(key, (source, target))| (key, NodeIndex::new(source), NodeIndex::new(target)))
            .collect::<Vec<_>>();
        let before = route_table_neighbour_examinations_for_testing();
        compute_shortest_path_route_table_with(&graph, flows, RouteWorkers::serial())
            .expect("a canonical fat tree is connected");
        let examinations = route_table_neighbour_examinations_for_testing() - before;
        assert_eq!(
            examinations,
            (sources.len() * 2 * graph.edge_count()) as u64,
            "one exhaustive tree per distinct source"
        );
    }

    /// The partition covers the grouped positions with contiguous, non-empty, ascending parts, at
    /// most one per worker, each within one tree and one route of an equal share of the work.
    #[test]
    fn source_group_parts_are_contiguous_bounded_and_balanced() {
        let tree_weight = 100_u128;
        // Source 0 has 400 flows; sources 1..=40 have one each.
        let endpoints = (0..400)
            .map(|target| (NodeIndex::new(0), NodeIndex::new(target % 7)))
            .chain((1..=40).map(|source| (NodeIndex::new(source), NodeIndex::new(0))))
            .collect::<Vec<_>>();
        let mut grouped = (0..endpoints.len()).collect::<Vec<_>>();
        grouped.sort_by_key(|&position| endpoints[position].0);
        let weight = |index: usize| {
            let starts =
                index == 0 || endpoints[grouped[index - 1]].0 != endpoints[grouped[index]].0;
            FAT_TREE_ROUTE_READ_WEIGHT + if starts { tree_weight } else { 0 }
        };
        let total = (0..grouped.len()).map(weight).sum::<u128>();
        for workers in [1, 2, 3, 7, 64].map(RouteWorkers::new) {
            let parts = source_group_parts(&endpoints, &grouped, tree_weight, workers);
            assert!(parts.len() <= workers.get());
            assert_eq!(parts.first().map(|part| part.start), Some(0));
            assert_eq!(parts.last().map(|part| part.end), Some(grouped.len()));
            for pair in parts.windows(2) {
                assert_eq!(pair[0].end, pair[1].start, "parts must be contiguous");
            }
            // Each position lies in part `floor(w * parts / total)`, `w` the weight before it.
            let budget = workers.get().min(grouped.len()) as u128;
            let mut before = 0;
            let mut expected = Vec::<u128>::new();
            for index in 0..grouped.len() {
                expected.push(before * budget / total);
                before += weight(index);
            }
            let mut boundaries = expected
                .windows(2)
                .enumerate()
                .filter(|(_, pair)| pair[0] != pair[1])
                .map(|(index, _)| index + 1)
                .collect::<Vec<_>>();
            boundaries.insert(0, 0);
            boundaries.push(grouped.len());
            let expected_parts = boundaries
                .windows(2)
                .map(|pair| pair[0]..pair[1])
                .collect::<Vec<_>>();
            assert_eq!(parts, expected_parts, "{} workers", workers.get());
            let share = total / parts.len() as u128;
            for part in &parts {
                assert!(!part.is_empty());
                let part_weight = part.clone().map(weight).sum::<u128>();
                assert!(
                    part_weight <= share + tree_weight + 2 * FAT_TREE_ROUTE_READ_WEIGHT,
                    "{} workers: part {part:?} weighs {part_weight}, share {share}",
                    workers.get()
                );
            }
        }
        assert!(source_group_parts(&endpoints, &[], tree_weight, RouteWorkers::new(7)).is_empty());
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
        assert_eq!(
            ROUTE_FILL_LAYOUT_CHECKS.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "no route fill, on any thread, may re-classify the topology"
        );
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
        let workers = RouteWorkers::new(MAX_ROUTE_WORKERS);
        // Without this the assertions below could hold vacuously on a serial partition.
        assert_eq!(
            route_chunk_count(flows.len(), workers),
            8,
            "this budget must actually spread the eight flows over eight worker threads"
        );
        FAT_TREE_LAYOUT_CHECKS.with(|checks| checks.set(0));

        let routes = compute_shortest_path_route_table_with(&graph, flows, workers)
            .expect("canonical fat-tree routes should exist");

        assert_eq!(routes.len(), 8);
        FAT_TREE_LAYOUT_CHECKS.with(|checks| {
            assert_eq!(
                checks.get(),
                1,
                "the scatter must reuse the one classification taken on the submitting thread"
            );
        });
        // The submitting thread cannot see a worker's `thread_local!`, so the per-thread count
        // above is blind to a re-classification inside the scatter. This process-wide counter is
        // the half of the invariant that the worker threads are actually in.
        assert_eq!(
            ROUTE_FILL_LAYOUT_CHECKS.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "no route worker may re-classify the topology"
        );
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

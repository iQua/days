use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{NodeKind, SimulationImage};
use days_legacy::flows::flow::Flow;
use days_legacy::topos::build::build_graph;
use days_legacy::topos::topo::{installed_forwarding_state, installed_host_attachment_state};
use petgraph::graph::NodeIndex;
use tempfile::NamedTempFile;

type HostPair = (usize, usize);
type PhysicalRoute = Vec<(usize, usize)>;
type RouteMap = BTreeMap<HostPair, PhysicalRoute>;

const HOSTS: usize = 8192;
const EDGE_SWITCHES: usize = 512;
const HOSTS_PER_EDGE: usize = 16;
const EDGE_SWITCHES_PER_POD: usize = 16;
const PODS: usize = 32;

fn fixture(load: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!(
        "../configs/benchmarks/p12/e1_open_k32_load_{load}.toml"
    ))
}

fn walk_route(
    graph: &petgraph::graph::UnGraph<usize, ()>,
    source: usize,
    target: usize,
    flow_id: usize,
    fibs: &BTreeMap<usize, BTreeMap<usize, usize>>,
) -> PhysicalRoute {
    let mut current = source;
    let mut seen = BTreeSet::new();
    let mut route = Vec::new();
    while current != target {
        assert!(
            seen.insert(current),
            "legacy FIB contains a cycle for flow {flow_id} at switch {current}"
        );
        let next = fibs
            .get(&current)
            .and_then(|fib| fib.get(&flow_id))
            .copied()
            .unwrap_or_else(|| panic!("legacy FIB is missing flow {flow_id} at switch {current}"));
        assert!(
            graph.contains_edge(NodeIndex::new(current), NodeIndex::new(next)),
            "legacy FIB sends flow {flow_id} across non-link {current} -> {next}"
        );
        route.push((current, next));
        current = next;
    }
    route
}

fn legacy_realization(path: &Path) -> (Vec<HostPair>, RouteMap) {
    let path = path.to_str().unwrap();
    let (graph, hosts) = build_graph(path).expect("legacy E1 topology");
    let flows = Flow::try_flows_from_config_with_attachments(path, &hosts)
        .expect("legacy E1 flow lowering");
    let forwarding = installed_forwarding_state(&graph, &flows);
    let pairs = flows
        .iter()
        .map(|flow| (flow.source_host, flow.sink_host))
        .collect::<Vec<_>>();
    let routes = flows
        .iter()
        .map(|flow| {
            let key = (flow.source_host, flow.sink_host);
            let route = walk_route(
                &graph,
                flow.source_switch,
                flow.sink_switch,
                flow.id,
                &forwarding.fibs,
            );
            (key, route)
        })
        .collect::<RouteMap>();
    assert_eq!(routes.len(), flows.len(), "E1 host pairs must be unique");
    (pairs, routes)
}

fn legacy_installed_propagation(path: &Path) -> BTreeSet<u64> {
    let path = path.to_str().unwrap();
    let (_, hosts) = build_graph(path).expect("legacy E1 topology");
    let flows = Flow::try_flows_from_config_with_attachments(path, &hosts)
        .expect("legacy E1 flow lowering");
    let installed = installed_host_attachment_state(path, &hosts, &flows)
        .expect("E1 scalar link propagation must activate the installed physical stages");
    std::iter::once(installed.physical.propagation_ns)
        .chain(
            installed
                .hosts
                .values()
                .flat_map(|host| [host.injection.propagation_ns, host.delivery.propagation_ns]),
        )
        .collect()
}

fn days_host_topology(image: &SimulationImage) -> BTreeMap<u64, usize> {
    let mut host_nodes = image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Host)
        .map(|node| node.id.0)
        .collect::<Vec<_>>();
    host_nodes.sort_unstable();
    host_nodes
        .into_iter()
        .enumerate()
        .map(|(host, node)| (node, host))
        .collect()
}

fn days_realization(path: &Path) -> (Vec<HostPair>, RouteMap) {
    let image = compile_config(path).expect("Days E1 lowering");
    let host_topology = days_host_topology(&image);
    let nodes = image
        .nodes
        .iter()
        .map(|node| (node.id, node))
        .collect::<BTreeMap<_, _>>();
    let links = image
        .links
        .iter()
        .map(|link| (link.id, link))
        .collect::<BTreeMap<_, _>>();
    let mut flows = image.flows.iter().collect::<Vec<_>>();
    flows.sort_by_key(|flow| flow.id);

    let pairs = flows
        .iter()
        .map(|flow| (host_topology[&flow.source.0], host_topology[&flow.target.0]))
        .collect::<Vec<_>>();
    let routes = flows
        .into_iter()
        .map(|flow| {
            let pair = (host_topology[&flow.source.0], host_topology[&flow.target.0]);
            let route = flow
                .route
                .iter()
                .filter_map(|link_id| {
                    let link = links[link_id];
                    let source = nodes[&link.source];
                    let target = nodes[&link.target];
                    (source.kind == NodeKind::Switch && target.kind == NodeKind::Switch).then(
                        || {
                            (
                                image.switch_states[source.state_slot as usize].physical_switch
                                    as usize,
                                image.switch_states[target.state_slot as usize].physical_switch
                                    as usize,
                            )
                        },
                    )
                })
                .collect::<PhysicalRoute>();
            (pair, route)
        })
        .collect::<RouteMap>();
    assert_eq!(
        routes.len(),
        image.flows.len(),
        "E1 host pairs must be unique"
    );
    (pairs, routes)
}

fn expected_pair(source: usize) -> HostPair {
    let ordinal = source / EDGE_SWITCHES;
    let source_switch = source % EDGE_SWITCHES;
    let target_switch = (source_switch + EDGE_SWITCHES / 2) % EDGE_SWITCHES;
    (source, ordinal * EDGE_SWITCHES + target_switch)
}

fn assert_canonical_cross_pod_routes(routes: &RouteMap) -> (usize, usize) {
    let mut aggregation_offsets = BTreeSet::new();
    let mut cores = BTreeSet::new();
    for (&(source, target), route) in routes {
        assert_eq!(
            route.len(),
            4,
            "cross-pod E1 route must have four switch links"
        );
        let nodes = [route[0].0, route[0].1, route[1].1, route[2].1, route[3].1];
        let source_switch = source % EDGE_SWITCHES;
        let target_switch = target % EDGE_SWITCHES;
        let source_pod = source_switch / EDGE_SWITCHES_PER_POD;
        let target_pod = target_switch / EDGE_SWITCHES_PER_POD;
        assert_eq!(nodes[0], source_switch);
        assert_eq!(nodes[4], target_switch);
        assert_eq!((target_pod + PODS - source_pod) % PODS, PODS / 2);

        let source_aggregation_base = EDGE_SWITCHES + source_pod * EDGE_SWITCHES_PER_POD;
        let aggregation_offset = nodes[1]
            .checked_sub(source_aggregation_base)
            .expect("source aggregation must belong to the source pod");
        assert!(aggregation_offset < EDGE_SWITCHES_PER_POD);
        assert_eq!(
            nodes[3],
            EDGE_SWITCHES + target_pod * EDGE_SWITCHES_PER_POD + aggregation_offset,
            "fat-tree ECMP must use the same aggregation offset in both pods"
        );
        assert!(
            (2 * EDGE_SWITCHES..2 * EDGE_SWITCHES + 256).contains(&nodes[2]),
            "E1 route must traverse a core switch"
        );
        assert_eq!(
            (nodes[2] - 2 * EDGE_SWITCHES) / EDGE_SWITCHES_PER_POD,
            aggregation_offset,
            "core group must match the selected aggregation offset"
        );
        aggregation_offsets.insert(aggregation_offset);
        cores.insert(nodes[2]);
    }
    (aggregation_offsets.len(), cores.len())
}

fn fnv_mix(mut hash: u64, value: usize) -> u64 {
    for byte in (value as u64).to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn matrix_digest(pairs: &[HostPair]) -> u64 {
    pairs
        .iter()
        .fold(0xcbf29ce484222325, |hash, &(source, target)| {
            fnv_mix(fnv_mix(hash, source), target)
        })
}

fn route_digest(routes: &RouteMap) -> u64 {
    routes.iter().fold(
        0xcbf29ce484222325,
        |mut hash, (&(source, target), route)| {
            hash = fnv_mix(fnv_mix(hash, source), target);
            for &(from, to) in route {
                hash = fnv_mix(fnv_mix(hash, from), to);
            }
            hash
        },
    )
}

#[test]
fn full_e1_family_realizes_the_same_matrix_and_ecmp_paths_as_days() {
    let expected_pairs = (0..HOSTS).map(expected_pair).collect::<Vec<_>>();
    let mut path_digests = BTreeSet::new();

    for load in ["10", "30", "60", "90"] {
        let path = fixture(load);
        days_legacy::validate_config(path.to_str().unwrap()).expect("strict E1 validation");
        let (legacy_pairs, legacy_routes) = legacy_realization(&path);
        let (days_pairs, days_routes) = days_realization(&path);

        assert_eq!(legacy_pairs, expected_pairs, "legacy E1 load {load} matrix");
        assert_eq!(days_pairs, expected_pairs, "Days E1 load {load} matrix");
        assert_eq!(legacy_routes, days_routes, "E1 load {load} physical routes");
        let legacy_propagation = legacy_installed_propagation(&path);
        assert_eq!(
            legacy_propagation,
            BTreeSet::from([1000]),
            "legacy E1 load {load} installed propagation"
        );
        assert!(
            compile_config(&path)
                .expect("Days E1 propagation image")
                .links
                .iter()
                .all(|link| link.propagation_ns == 1000),
            "Days E1 load {load} link propagation"
        );
        assert_eq!(
            legacy_pairs
                .iter()
                .map(|&(source, target)| (source % EDGE_SWITCHES, target % EDGE_SWITCHES))
                .collect::<BTreeSet<_>>()
                .len(),
            EDGE_SWITCHES,
            "E1 must have one structural switch pair per edge switch"
        );
        assert!(
            legacy_pairs
                .iter()
                .all(|&(source, target)| expected_pair(source) == (source, target))
        );
        let (aggregation_choices, cores) = assert_canonical_cross_pod_routes(&legacy_routes);
        assert_eq!(aggregation_choices, EDGE_SWITCHES_PER_POD);
        assert_eq!(cores, 256, "E1 ECMP must exercise the full core layer");

        let matrix = matrix_digest(&legacy_pairs);
        let paths = route_digest(&legacy_routes);
        path_digests.insert(paths);
        println!(
            "E1_EXPRESSION load={load} flows={} switch_pairs={} hosts_per_pair={} propagation_ns=1000 matrix_fnv64={matrix:016x} path_fnv64={paths:016x} aggregation_choices={aggregation_choices} cores={cores}",
            legacy_pairs.len(),
            EDGE_SWITCHES,
            HOSTS_PER_EDGE,
        );
    }

    assert_eq!(
        path_digests.len(),
        4,
        "the interval participates in the ECMP identity, so all four path maps must be checked"
    );
}

fn modified_fixture(original: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temporary scenario");
    file.write_all(original.as_bytes()).expect("write scenario");
    file
}

#[test]
fn expression_comparators_detect_the_historical_substitutions() {
    let original_path = fixture("10");
    let original = fs::read_to_string(&original_path).expect("E1 fixture");
    let (expected_pairs, expected_routes) = days_realization(&original_path);

    let random_body = original.replacen("pairing = \"SwitchOffsetHalf\"\n", "", 1);
    assert_ne!(
        random_body, original,
        "pairing red control must change the fixture"
    );
    let random = modified_fixture(&random_body);
    let (random_pairs, _) = legacy_realization(random.path());
    assert_ne!(
        random_pairs, expected_pairs,
        "matrix comparator must detect the historical RNG substitution"
    );

    let shortest_body = original.replacen("[routing]\npolicy = \"FatTreeEcmp\"\n\n", "", 1);
    assert_ne!(
        shortest_body, original,
        "routing red control must change the fixture"
    );
    let shortest = modified_fixture(&shortest_body);
    let (shortest_pairs, shortest_routes) = legacy_realization(shortest.path());
    assert_eq!(shortest_pairs, expected_pairs);
    assert_ne!(
        shortest_routes, expected_routes,
        "route comparator must detect the historical single-path substitution"
    );
}

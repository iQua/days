//! T21 (P12 wave-4) scenario-vocabulary gates.
//!
//! The wave-4 showcase fixtures need three things the image compiler did not express:
//!
//! 1. **Per-tier link propagation.** `LinkDescriptor` already carries a per-link
//!    `propagation_ns`, but lowering stamped one global `link.propagation_ns` onto every link, so
//!    a heterogeneous-delay fabric (F-HET) was unreachable from configuration.
//! 2. **Deterministic structural traffic matrices.** `flow_set` only ever sampled random endpoint
//!    pairs, so the GeDES-matched permutation matrix E1 needs (host *i* to the host at the same
//!    rack ordinal half the fabric away) could not be stated.
//! 3. **A non-fat-tree, non-torus topology.** F-TOPO's capability row needs one.
//!
//! All three are host-side lowering vocabulary. No executor semantics change, and every default
//! path is byte-identical to `89ec7c4` — the frozen anchors in `t21_p12_fixtures.rs` are the gate.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days::topos::build::{PairingPolicy, TopologyProfile, build_graph_with_profile};

fn scenario_path(body: &str, tag: &str) -> PathBuf {
    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-t21-{tag}-{}-{}.toml",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&path, body).expect("scenario fixture should be writable");
    path
}

const TIERED_K4: &str = r#"
seed = 21
duration = 0.00001

[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"

[link.propagation_tiers]
host_to_edge_ns = 100
edge_to_aggregation_ns = 1000
aggregation_to_core_ns = 10000

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 4
traffic = { initial_delay = 0.0, size = 15400, arr_dist = { type = "Uniform", low = 0.000001232, high = 0.000001232 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

#[test]
fn fat_tree_propagation_tiers_lower_to_three_distinct_link_delays() {
    let path = scenario_path(TIERED_K4, "tiers");
    let image = compile_config(&path).expect("tiered fat tree must lower");

    // k = 4: 8 edge switches, 8 aggregation switches, 4 core switches, 2 hosts per edge switch.
    // Directed links: 16 host attachments x 2 = 32 rack, 8 x 2 edge-aggregation x 2 = 32 spine,
    // 8 x 2 aggregation-core x 2 = 32 inter-pod.
    let mut rack = 0;
    let mut spine = 0;
    let mut inter_pod = 0;
    for link in &image.links {
        match link.propagation_ns {
            100 => rack += 1,
            1_000 => spine += 1,
            10_000 => inter_pod += 1,
            other => panic!("unexpected tier delay {other} ns"),
        }
    }
    assert_eq!((rack, spine, inter_pod), (32, 32, 32));
    assert_eq!(image.links.len(), 96);
}

#[test]
fn propagation_tiers_are_refused_outside_the_fat_tree_profile() {
    let body = r#"
seed = 21
duration = 0.00001
edges = [[0, 1], [1, 2]]
hosts = [0, 1, 2]

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"

[link.propagation_tiers]
host_to_edge_ns = 100
edge_to_aggregation_ns = 1000
aggregation_to_core_ns = 10000

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 2]]
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 0.000001, high = 0.000001 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;
    let path = scenario_path(body, "tiers-custom");
    let error = compile_config(&path).expect_err("tiers outside a fat tree must be refused");
    let message = error.to_string();
    assert!(
        message.contains("link.propagation_tiers"),
        "refusal must name the key: {message}"
    );
    assert!(
        message.contains("FatTree"),
        "refusal must name the supported profile: {message}"
    );
}

#[test]
fn propagation_tiers_and_uniform_propagation_are_mutually_exclusive() {
    let body = TIERED_K4.replace(
        "[link.propagation_tiers]",
        "[link]\npropagation_ns = 1000\n\n[link.propagation_tiers]",
    );
    let path = scenario_path(&body, "tiers-conflict");
    let error = compile_config(&path).expect_err("a doubly specified delay must be refused");
    assert!(
        error.to_string().contains("propagation_ns"),
        "refusal must name the conflicting key: {error}"
    );
}

#[test]
fn dragonfly_builds_the_canonical_group_structure() {
    let body = r#"
seed = 21
duration = 0.00001

[topology]
category = "Dragonfly"

[topology.dragonfly]
routers_per_group = 4
global_ports_per_router = 2
hosts_per_router = 2

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"
"#;
    let path = scenario_path(body, "dragonfly-shape");
    let (graph, hosts, profile) =
        build_graph_with_profile(path.to_str().unwrap()).expect("dragonfly must build");

    // a = 4, h = 2 => g = a*h + 1 = 9 groups, 36 routers, p = 2 => 72 hosts.
    assert_eq!(
        profile,
        TopologyProfile::Dragonfly {
            groups: 9,
            routers_per_group: 4,
        }
    );
    assert_eq!(graph.node_count(), 36);
    assert_eq!(hosts.len(), 72);
    // Intra-group all-to-all: 9 groups x C(4,2) = 54. Global: every group pair once = C(9,2) = 36.
    assert_eq!(graph.edge_count(), 54 + 36);
    for router in 0..36 {
        assert_eq!(
            graph.edges(petgraph::graph::NodeIndex::new(router)).count(),
            (4 - 1) + 2,
            "router {router} must carry a-1 local ports and h global ports"
        );
    }
}

#[test]
fn dragonfly_scenario_lowers_and_validates() {
    let body = r#"
seed = 21
duration = 0.00001

[topology]
category = "Dragonfly"

[topology.dragonfly]
routers_per_group = 4
global_ports_per_router = 2
hosts_per_router = 2

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 100

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 8
traffic = { initial_delay = 0.0, size = 15400, arr_dist = { type = "Uniform", low = 0.000001232, high = 0.000001232 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;
    let path = scenario_path(body, "dragonfly-lower");
    let image = compile_config(&path).expect("dragonfly scenario must lower");
    assert_eq!(image.flows.len(), 8);
    // 72 host LPs + one switch-port LP per directed link out of a router.
    assert_eq!(image.links.len(), 2 * (54 + 36) + 2 * 72);
    days_executor::validate(&image, days_executor::Backend::Scalar)
        .expect("dragonfly image must validate");
}

/// Days numbers fat-tree hosts ordinal-major: `host = ordinal * edge_switch_count + switch`.
/// `SwitchOffsetHalf` therefore has to be defined on the (switch, ordinal) grid, not on raw host
/// identity, if it is to be the same *fabric-level* permutation GeDES's `BuildTCPConnections`
/// builds (which pairs host *i* with *i + N/2* under a switch-major numbering).
#[test]
fn switch_offset_half_pairing_is_the_cross_pod_partner_permutation() {
    let body = pairing_scenario("SwitchOffsetHalf", 16);
    let path = scenario_path(&body, "pairing-offset");
    let image = compile_config(&path).expect("structural pairing must lower");

    let mut pairs = endpoint_pairs(&image);
    pairs.sort_unstable();
    // k = 4, hosts_per_edge = 2: 8 edge switches, 2 ordinals, host = ordinal * 8 + switch.
    let expected = (0..16_u64)
        .map(|host| {
            let (ordinal, switch) = (host / 8, host % 8);
            (host, ordinal * 8 + (switch + 4) % 8)
        })
        .collect::<Vec<_>>();
    assert_eq!(pairs, expected);
}

#[test]
fn same_switch_next_pairing_stays_rack_local() {
    let body = pairing_scenario("SameSwitchNext", 16);
    let path = scenario_path(&body, "pairing-rack");
    let image = compile_config(&path).expect("rack-local pairing must lower");

    let mut pairs = endpoint_pairs(&image);
    pairs.sort_unstable();
    let expected = (0..16_u64)
        .map(|host| {
            let (ordinal, switch) = (host / 8, host % 8);
            (host, (ordinal + 1) % 2 * 8 + switch)
        })
        .collect::<Vec<_>>();
    assert_eq!(pairs, expected);
}

#[test]
fn structural_pairing_refuses_more_members_than_hosts() {
    let body = pairing_scenario("SwitchOffsetHalf", 17);
    let path = scenario_path(&body, "pairing-overflow");
    let error = compile_config(&path).expect_err("more members than hosts must be refused");
    assert!(
        error.to_string().contains("17"),
        "refusal must quote the requested member count: {error}"
    );
}

#[test]
fn unknown_pairing_policies_are_refused_by_name() {
    let body = pairing_scenario("Antipodal", 4);
    let path = scenario_path(&body, "pairing-unknown");
    let error = compile_config(&path).expect_err("an unknown pairing must be refused");
    assert!(
        error.to_string().contains("Antipodal"),
        "refusal must quote the unsupported policy: {error}"
    );
}

/// `PairingPolicy::Random` is the default and must keep drawing from the endpoint RNG exactly as
/// it did before T21 — the structural policies draw nothing.
#[test]
fn default_pairing_is_random_and_structural_pairings_do_not_draw_from_the_endpoint_rng() {
    assert_eq!(PairingPolicy::default(), PairingPolicy::Random);

    let mixed = r#"
seed = 21
duration = 0.00001

[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 100

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 8
pairing = "SwitchOffsetHalf"
traffic = { initial_delay = 0.0, size = 15400, arr_dist = { type = "Uniform", low = 0.000001232, high = 0.000001232 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 4
traffic = { initial_delay = 0.0, size = 3080, arr_dist = { type = "Uniform", low = 0.000001232, high = 0.000001232 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;
    let random_only = r#"
seed = 21
duration = 0.00001

[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 100

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 4
traffic = { initial_delay = 0.0, size = 3080, arr_dist = { type = "Uniform", low = 0.000001232, high = 0.000001232 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

    let mixed_image = compile_config(scenario_path(mixed, "pairing-mixed")).expect("must lower");
    let random_image =
        compile_config(scenario_path(random_only, "pairing-random")).expect("must lower");

    // Flow identity is dense over sorted flow keys, and `flow_count` is the leading field, so the
    // four-member random set occupies the first four flow ids in both images.
    let mixed_random_pairs = endpoint_pairs(&mixed_image)
        .into_iter()
        .take(4)
        .collect::<Vec<_>>();
    assert_eq!(mixed_random_pairs, endpoint_pairs(&random_image));
}

fn pairing_scenario(policy: &str, flow_count: u64) -> String {
    format!(
        r#"
seed = 21
duration = 0.00001

[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 100

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = {flow_count}
pairing = "{policy}"
traffic = {{ initial_delay = 0.0, size = 15400, arr_dist = {{ type = "Uniform", low = 0.000001232, high = 0.000001232 }}, pkt_size_dist = {{ type = "DiscreteUniform", low = 1540, high = 1540 }} }}
"#
    )
}

/// Recovers the (source host, target host) topology identities of every lowered flow, in flow-id
/// order, from the image alone.
fn endpoint_pairs(image: &days_executor::SimulationImage) -> Vec<(u64, u64)> {
    let host_topology = host_topology_identities(image);
    let mut flows = image.flows.clone();
    flows.sort_by_key(|flow| flow.id.0);
    flows
        .iter()
        .map(|flow| (host_topology[&flow.source.0], host_topology[&flow.target.0]))
        .collect()
}

/// The lowering assigns host node identities in ascending host topology identity order, so the
/// rank of a host node id inside the sorted host-node set *is* its topology identity.
fn host_topology_identities(
    image: &days_executor::SimulationImage,
) -> std::collections::BTreeMap<u64, u64> {
    let mut hosts = image
        .nodes
        .iter()
        .filter(|node| node.kind == days_executor::NodeKind::Host)
        .map(|node| node.id.0)
        .collect::<Vec<_>>();
    hosts.sort_unstable();
    hosts
        .into_iter()
        .enumerate()
        .map(|(rank, id)| (id, rank as u64))
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Equal-cost multipath (T21/P12). The single-path table breaks ties on the lowest neighbour
// identity, so every inter-pod route in a canonical fat tree crosses core switch 0. That is a
// single-path fabric and it is not what any external P12 arm runs.
// ---------------------------------------------------------------------------------------------

use days::topos::route::{
    EcmpFlow, RouteTableError, compute_fat_tree_ecmp_route_table, compute_shortest_path_route_table,
};
use petgraph::graph::NodeIndex;

fn k8_fat_tree() -> petgraph::graph::UnGraph<usize, ()> {
    let body = r#"
[topology]
category = "FatTree"

[topology.fat_tree]
k = 8
hosts_per_edge = 4

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"
"#;
    let path = scenario_path(body, "k8-graph");
    build_graph_with_profile(path.to_str().unwrap())
        .expect("k8 fat tree must build")
        .0
}

/// The defect the policy exists to remove, stated as a test so it cannot silently come back.
///
/// Over EVERY cross-pod edge-switch pair of a k = 8 fat tree the single-path table reaches only a
/// small minority of the sixteen core switches, because its tie-break is the lowest neighbour
/// identity. Equal-cost multipath reaches all sixteen on the same pair set.
#[test]
fn single_path_routing_concentrates_cross_pod_flows_on_a_few_core_switches() {
    let graph = k8_fat_tree();
    // k = 8: edge switches 0..=31 in eight pods of four; cores are identities 64..=79.
    let pairs = (0..32_usize)
        .flat_map(|source| (0..32_usize).map(move |target| (source, target)))
        .filter(|(source, target)| source / 4 != target / 4)
        .collect::<Vec<_>>();
    let single_path = compute_shortest_path_route_table(
        &graph,
        pairs.iter().enumerate().map(|(index, (source, target))| {
            (index, NodeIndex::new(*source), NodeIndex::new(*target))
        }),
    )
    .expect("cross-pod flows must route");
    let ecmp = compute_fat_tree_ecmp_route_table(
        &graph,
        pairs
            .iter()
            .enumerate()
            .map(|(index, (source, target))| EcmpFlow {
                key: index,
                source_switch: NodeIndex::new(*source),
                target_switch: NodeIndex::new(*target),
                flow_hash: (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
            }),
    )
    .expect("cross-pod flows must route under ECMP");

    let cores = |table: &std::collections::BTreeMap<usize, Vec<NodeIndex>>| {
        table
            .values()
            .flat_map(|route| route.iter().map(|node| node.index()))
            .filter(|node| *node >= 64)
            .collect::<std::collections::BTreeSet<_>>()
    };
    let single_path_cores = cores(&single_path);
    let ecmp_cores = cores(&ecmp);
    assert_eq!(ecmp_cores.len(), 16, "ECMP must reach every core switch");
    assert!(
        single_path_cores.len() * 2 <= ecmp_cores.len(),
        "single-path routing reached {} of 16 core switches; the concentration this policy exists \
         to remove is gone and the fixtures should be revisited",
        single_path_cores.len()
    );
}

#[test]
fn fat_tree_ecmp_spreads_cross_pod_flows_over_every_core_switch() {
    let graph = k8_fat_tree();
    let table = compute_fat_tree_ecmp_route_table(
        &graph,
        (0..4_096_u64).map(|flow| EcmpFlow {
            key: flow,
            source_switch: NodeIndex::new((flow % 32) as usize),
            target_switch: NodeIndex::new(((flow % 32) as usize + 16) % 32),
            // A spread-out stand-in for the header hash lowering supplies.
            flow_hash: flow.wrapping_mul(0x9e37_79b9_7f4a_7c15),
        }),
    )
    .expect("cross-pod flows must route under ECMP");
    let cores = table
        .values()
        .flat_map(|route| route.iter().map(|node| node.index()))
        .filter(|node| *node >= 64)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(cores.len(), 16, "every core switch must carry traffic");
}

#[test]
fn fat_tree_ecmp_routes_are_paths_in_the_graph_and_keep_shortest_length() {
    let graph = k8_fat_tree();
    let table = compute_fat_tree_ecmp_route_table(
        &graph,
        (0..1_024_u64).map(|flow| EcmpFlow {
            key: flow,
            source_switch: NodeIndex::new((flow % 32) as usize),
            target_switch: NodeIndex::new(((flow * 7 + 3) % 32) as usize),
            flow_hash: flow.wrapping_mul(0x9e37_79b9_7f4a_7c15),
        }),
    )
    .expect("ECMP must route every switch pair");
    for (flow, route) in &table {
        let source = route[0].index();
        let target = route[route.len() - 1].index();
        let expected = if source == target {
            1
        } else if source / 4 == target / 4 {
            3
        } else {
            5
        };
        assert_eq!(route.len(), expected, "flow {flow} has a non-shortest path");
        for pair in route.windows(2) {
            assert!(
                graph.find_edge(pair[0], pair[1]).is_some(),
                "flow {flow} uses a link that does not exist: {:?} -> {:?}",
                pair[0],
                pair[1]
            );
        }
    }
}

#[test]
fn fat_tree_ecmp_is_refused_on_a_topology_that_is_not_a_fat_tree() {
    let body = r#"
seed = 21
duration = 0.00001

[topology]
category = "Torus"

[topology.torus]
dim = 2
n = 4

[switch]
port_rate = 100_000_000_000
capacity = 64
discipline = "FIFO"
drop = "TailDrop"
"#;
    let path = scenario_path(body, "ecmp-torus");
    let graph = build_graph_with_profile(path.to_str().unwrap())
        .expect("torus must build")
        .0;
    let error = compute_fat_tree_ecmp_route_table(
        &graph,
        [EcmpFlow {
            key: 0_usize,
            source_switch: NodeIndex::new(0),
            target_switch: NodeIndex::new(5),
            flow_hash: 1,
        }],
    )
    .expect_err("ECMP outside a fat tree must be refused");
    assert_eq!(error, RouteTableError::UnsupportedTopology);
}

#[test]
fn unknown_routing_policies_are_refused_by_name() {
    let body = TIERED_K4.replace(
        "[link.propagation_tiers]",
        "[routing]\npolicy = \"ValiantRandom\"\n\n[link.propagation_tiers]",
    );
    let path = scenario_path(&body, "routing-unknown");
    let error = compile_config(&path).expect_err("an unknown routing policy must be refused");
    assert!(
        error.to_string().contains("ValiantRandom"),
        "refusal must quote the unsupported policy: {error}"
    );
}

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

use days::scenario::compile_config;
use days::topos::build::PairingPolicy;
use days::topos::route::{EcmpFlow, compute_fat_tree_ecmp_route_table};
use days_executor::NodeKind;
use days_legacy::flows::flow::Flow;
use days_legacy::flows::route::Routing;
use days_legacy::topos::build::build_graph;
use days_legacy::topos::topo::installed_forwarding_state;
use petgraph::graph::NodeIndex;
use tempfile::NamedTempFile;

const K4: &str = r#"
seed = 51001
duration = 0.001
threading = "single"

[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 100_000_000_000
capacity = 200
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 1000

[routing]
policy = "FatTreeEcmp"

[[flow_set]]
flow_type = "TCP"
flow_count = 16
pairing = "SwitchOffsetHalf"
traffic = { initial_delay = 0.0, size = 3500, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1460, high = 1460 }, tcp = { cc_algorithm = "Reno" } }
"#;

fn config(body: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temporary scenario");
    file.write_all(body.as_bytes()).expect("write scenario");
    file
}

fn walk_route(
    source: usize,
    target: usize,
    flow_id: usize,
    fibs: &std::collections::BTreeMap<usize, std::collections::BTreeMap<usize, usize>>,
) -> Vec<NodeIndex> {
    let mut route = vec![NodeIndex::new(source)];
    let mut current = source;
    let mut seen = BTreeSet::from([source]);
    while current != target {
        current = fibs[&current][&flow_id];
        assert!(seen.insert(current), "forwarding route must not loop");
        route.push(NodeIndex::new(current));
    }
    route
}

#[test]
fn switch_offset_half_and_fat_tree_ecmp_match_shared_structural_semantics() {
    let file = config(K4);
    let path = file.path().to_str().unwrap();
    let (graph, hosts) = build_graph(path).expect("k=4 topology");
    let flows = Flow::try_flows_from_config_with_attachments(path, &hosts)
        .expect("E5 routing vocabulary must lower");

    let expected_pairs = hosts
        .structural_flow_pairs(PairingPolicy::SwitchOffsetHalf, 16)
        .expect("structural permutation");
    assert_eq!(
        flows
            .iter()
            .map(|flow| (flow.source_host, flow.sink_host))
            .collect::<Vec<_>>(),
        expected_pairs
    );

    let shared = compute_fat_tree_ecmp_route_table(
        &graph,
        flows.iter().map(|flow| {
            let Routing::FatTreeEcmp { flow_hash } = flow.routing else {
                panic!("root FatTreeEcmp must select the shared route policy")
            };
            EcmpFlow {
                key: flow.id,
                source_switch: NodeIndex::new(flow.source_switch),
                target_switch: NodeIndex::new(flow.sink_switch),
                flow_hash,
            }
        }),
    )
    .expect("canonical fat-tree routes");
    let installed = installed_forwarding_state(&graph, &flows);
    let image = compile_config(path).expect("the exact compiler must accept the same scenario");
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
    let exact_routes = image
        .flows
        .iter()
        .map(|flow| {
            flow.route
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
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for (member, flow) in flows.iter().enumerate() {
        let shared_route = &shared[&flow.id];
        assert_eq!(
            walk_route(
                flow.source_switch,
                flow.sink_switch,
                flow.id,
                &installed.fibs,
            ),
            *shared_route,
            "flow {} must install the shared O(1) route",
            flow.id
        );
        assert_eq!(
            exact_routes[member],
            shared_route
                .windows(2)
                .map(|edge| (edge[0].index(), edge[1].index()))
                .collect::<Vec<_>>(),
            "legacy member {member} must select the exact compiler route"
        );
    }
}

#[test]
fn unsupported_or_conflicting_e5_routing_vocabulary_is_refused() {
    for (body, needle) in [
        (K4.replace("FatTreeEcmp", "ValiantRandom"), "ValiantRandom"),
        (K4.replace("SwitchOffsetHalf", "Antipodal"), "Antipodal"),
        (
            K4.replace(
                "pairing = \"SwitchOffsetHalf\"",
                "pairing = \"SwitchOffsetHalf\"\nrouting = \"ECMP\"",
            ),
            "conflict",
        ),
        (
            K4.replace(
                "policy = \"FatTreeEcmp\"",
                "policy = \"FatTreeEcmp\"\nsilently_ignored = true",
            ),
            "silently_ignored",
        ),
    ] {
        let file = config(&body);
        let path = file.path().to_str().unwrap();
        let (_, hosts) = build_graph(path).expect("topology remains valid");
        let error = Flow::try_flows_from_config_with_attachments(path, &hosts)
            .expect_err("unsupported vocabulary must return an error");
        assert!(
            error.contains(needle),
            "refusal must name {needle:?}: {error}"
        );
    }
}

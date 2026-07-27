use std::fs;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{EventKind, FlowId, LinkId, NodeId, NodeKind, run_scalar};
use tempfile::TempDir;

fn write_config(directory: &TempDir, name: &str, contents: &str) -> String {
    let path = directory.path().join(name);
    fs::write(&path, contents).expect("test configuration should be writable");
    path.to_str()
        .expect("temporary path should be valid UTF-8")
        .to_owned()
}

#[test]
fn reordered_source_collections_lower_to_byte_identical_mixed_images() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let first_path = write_config(
        &directory,
        "first.toml",
        r#"
seed = 7
edges = [[2, 0], [0, 1]]
hosts = [2, 1]

[switch]
port_rate = 8_000_000_000
capacity = 2
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 17

[[flow]]
flow_type = "PacketDistribution"
graph = [[2, 1]]
[flow.traffic]
initial_delay = 0.0
size = 6
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 3, high = 3 }

[[flow]]
flow_type = "PacketDistribution"
graph = [[1, 2]]
[flow.traffic]
initial_delay = 0.000000002
size = 4
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 2, high = 2 }
"#,
    );
    let second_path = write_config(
        &directory,
        "second.toml",
        r#"
seed = 7
edges = [[1, 0], [0, 2]]
hosts = [1, 2]

[switch]
port_rate = 8_000_000_000
capacity = 2
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 17

[[flow]]
flow_type = "PacketDistribution"
graph = [[1, 2]]
[flow.traffic]
initial_delay = 0.000000002
size = 4
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 2, high = 2 }

[[flow]]
flow_type = "PacketDistribution"
graph = [[2, 1]]
[flow.traffic]
initial_delay = 0.0
size = 6
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 3, high = 3 }
"#,
    );

    let first = compile_config(&first_path).expect("first scenario should lower");
    let second = compile_config(&second_path).expect("reordered scenario should lower");

    assert_eq!(first, second, "every image field and ID must match");
    assert_eq!(
        format!("{first:#?}").into_bytes(),
        format!("{second:#?}").into_bytes(),
        "the complete ordered image representation must be byte-identical"
    );
    assert_eq!(
        first.nodes.iter().map(|node| node.id).collect::<Vec<_>>(),
        vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
    );
    assert_eq!(
        first.links.iter().map(|link| link.id).collect::<Vec<_>>(),
        (0..8).map(LinkId).collect::<Vec<_>>()
    );
    assert_eq!(
        first.flows.iter().map(|flow| flow.id).collect::<Vec<_>>(),
        vec![FlowId(0), FlowId(1)]
    );
    assert_eq!(
        first
            .flows
            .iter()
            .map(|flow| (flow.source, flow.target))
            .collect::<Vec<_>>(),
        vec![(NodeId(0), NodeId(1)), (NodeId(1), NodeId(0))]
    );
    assert!(
        first.flows.iter().all(|flow| flow.route.len() == 4),
        "each flow retains both host access links and the canonical two-hop switch path"
    );
    assert_eq!(
        first
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Host)
            .map(|node| node.state_slot)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        first
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Switch)
            .map(|node| node.state_slot)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );

    assert!(!first.host_states.is_empty());
    assert!(!first.switch_states.is_empty());
    assert!(
        first.nodes.iter().any(|node| node.kind == NodeKind::Host)
            && first.nodes.iter().any(|node| node.kind == NodeKind::Switch),
        "lowering must return one heterogeneous semantic image"
    );
    assert_eq!(first.channels.len(), first.links.len());
    assert!(
        first
            .switch_states
            .iter()
            .all(|state| state.queues.len() == 2),
        "each switch owns one FIFO/TailDrop queue per directed egress"
    );
    assert!(
        first
            .channels
            .iter()
            .all(|channel| channel.min_delay_ns == 0),
        "T7 must leave channel-bound derivation to T8"
    );
    assert!(
        first
            .initial_events
            .iter()
            .all(|event| event.kind == EventKind::PacketArrival),
        "T7 must not derive switch TxReady events"
    );

    run_scalar(&first, u64::MAX)
        .expect("T7 output should execute through currently implemented ingress handling");
}

#[test]
fn unsupported_source_behaviour_is_rejected_with_specific_diagnostics() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let cases = [
        (
            "scheduler",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "SP"
drop = "TailDrop"
"#,
            "unsupported scheduler `SP`; Days executor v1 supports only FIFO",
        ),
        (
            "drop",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "RED"
"#,
            "unsupported drop policy `RED`; Days executor v1 supports only TailDrop",
        ),
        (
            "tcp",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[[flow]]
flow_type = "TCP"
graph = [[0, 1]]
[flow.traffic]
size = 1
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#,
            "unsupported flow type `TCP`; Days executor v1 supports only open-loop PacketDistribution traffic",
        ),
        (
            "dcqcn",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[[flow]]
flow_type = "DCQCN"
graph = [[0, 1]]
[flow.traffic]
size = 1
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#,
            "unsupported flow type `DCQCN`; Days executor v1 supports only open-loop PacketDistribution traffic",
        ),
        (
            "pfc",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[link]
mode = "Pfc"
"#,
            "unsupported link mode `Pfc`; Days executor v1 does not support PFC",
        ),
        (
            "zero-rate",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 0
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
"#,
            "unsupported link rate: `switch.port_rate` is zero; Days executor v1 requires a positive constant rate",
        ),
        (
            "missing-rate",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
"#,
            "unsupported link rate: `switch.port_rate` is missing; Days executor v1 requires a positive constant rate",
        ),
        (
            "collective-set",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[[collective_set]]
collective_type = "Broadcast"
"#,
            "unsupported collective traffic; Days executor v1 lowering supports only independent open-loop flows",
        ),
        (
            "legacy-run-batch",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
run_batch_size = 2
"#,
            "Configuration key `switch.run_batch_size` was removed; schedulers now select one packet per service start.",
        ),
    ];

    for (name, config, expected) in cases {
        let path = write_config(&directory, &format!("{name}.toml"), config);
        let error = compile_config(&path).expect_err("unsupported scenario should reject");
        assert_eq!(error.to_string(), expected, "case {name}");
    }
}

#[test]
fn p01_fifo_taildrop_flow_set_lowers_without_legacy_id_state() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/baseline/fattree_k4_f8_st.toml");

    let first = compile_config(&path).expect("the smallest P01 fixture should lower");
    let second = compile_config(&path).expect("a second in-process lowering should also succeed");

    assert_eq!(first, second);
    assert_eq!(first.host_states.len(), 8);
    assert_eq!(first.switch_states.len(), 20);
    assert_eq!(first.nodes.len(), 28);
    assert_eq!(first.links.len(), 80);
    assert_eq!(first.channels.len(), 80);
    assert_eq!(first.flows.len(), 8);
    assert_eq!(first.packets.len(), 12_000);
    assert_eq!(first.initial_events.len(), 12_000);
    assert_eq!(
        first
            .switch_states
            .iter()
            .map(|state| state.queues.len())
            .sum::<usize>(),
        72
    );
    assert!(
        first.links.iter().all(|link| link.propagation_ns == 0),
        "propagation defaults to zero"
    );
}

#[test]
fn malformed_or_unrepresentable_source_semantics_reject_instead_of_collapsing() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let cases = [
        (
            "unreachable",
            r#"
seed = 1
edges = [[0, 1], [2, 3]]
hosts = [0, 3]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 3]]
[flow.traffic]
size = 1
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#,
            "unsupported unreachable flow 0 -> 3; no static topology route exists",
        ),
        (
            "parallel-links",
            r#"
seed = 1
edges = [[0, 1], [1, 0]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
"#,
            "unsupported parallel physical links between switch topology identities 0 and 1",
        ),
        (
            "duplicate-host",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
"#,
            "unsupported duplicate host topology identity",
        ),
        (
            "fractional-nanosecond",
            r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
size = 1
arr_dist = { type = "Uniform", low = 0.0000000015, high = 0.0000000015 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#,
            "unsupported packet arrival interval `0.0000000015`; Days executor v1 requires an integer number of nanoseconds",
        ),
    ];

    for (name, config, expected) in cases {
        let path = write_config(&directory, &format!("{name}.toml"), config);
        let error = compile_config(&path).expect_err("source semantics should reject");
        assert_eq!(error.to_string(), expected, "case {name}");
    }
}

#[test]
fn exact_discrete_packet_sizes_do_not_pass_through_floating_point() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "large-packet.toml",
        r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
size = 1
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 9223372036854775807, high = 9223372036854775807 }
"#,
    );

    let image = compile_config(path).expect("exact i64 packet size should lower");

    assert_eq!(image.packets.len(), 1);
    assert_eq!(image.packets[0].size_bytes, 9_223_372_036_854_775_807);
}

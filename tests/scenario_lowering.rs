use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    ArrivalDisposition, Backend, EventKind, FlowId, LinkId, NodeId, NodeKind,
    PacketArrivalObservation, PacketDeparture, PayloadId, SimulationImage, run_scalar, validate,
};
use tempfile::TempDir;

fn write_config(directory: &TempDir, name: &str, contents: &str) -> String {
    let path = directory.path().join(name);
    fs::write(&path, contents).expect("test configuration should be writable");
    path.to_str()
        .expect("temporary path should be valid UTF-8")
        .to_owned()
}

fn certified_delays(image: &SimulationImage) -> BTreeMap<LinkId, u64> {
    let mut delays = BTreeMap::<LinkId, u64>::new();
    for packet in &image.packets {
        let flow = image
            .flows
            .iter()
            .find(|flow| flow.id == packet.flow)
            .expect("lowered packet flow must exist");
        for link_id in &flow.route {
            let link = image
                .links
                .iter()
                .find(|link| link.id == *link_id)
                .expect("lowered route link must exist");
            let delay = link
                .delay_ns(packet.size_bytes)
                .expect("lowered channel arithmetic must fit");
            delays
                .entry(*link_id)
                .and_modify(|minimum| *minimum = (*minimum).min(delay))
                .or_insert(delay);
        }
    }
    delays
}

#[test]
fn configured_duration_stops_packets_that_start_after_the_boundary() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "stop-time.toml",
        r#"
seed = 1
duration = 1.0
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 8_000_000_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 2.0
size = 1
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#,
    );

    let image = compile_config(path).expect("supported scenario should lower");
    assert_eq!(image.stop_time_ns, 1_000_000_000);
    validate(&image, Backend::Scalar).expect("lowered image should validate");
    let result = run_scalar(&image, None).expect("lowered image should run");

    assert_eq!(
        result
            .arrivals
            .iter()
            .filter(|arrival| arrival.disposition == ArrivalDisposition::Delivered)
            .count(),
        0,
        "a packet starting after the configured duration must not be delivered"
    );
}

#[test]
fn fractional_nanosecond_simulation_duration_is_rejected() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "fractional-duration.toml",
        r#"
seed = 1
duration = 0.0000000015
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 8_000
capacity = 1
discipline = "FIFO"
drop = "TailDrop"
"#,
    );

    let error = compile_config(path).expect_err("fractional-nanosecond duration should reject");
    assert_eq!(
        error.to_string(),
        "unsupported simulation duration `0.0000000015`; Days executor v1 requires an integer number of nanoseconds"
    );
}

#[test]
fn reordered_source_collections_lower_to_byte_identical_mixed_images() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let first_path = write_config(
        &directory,
        "first.toml",
        r#"
seed = 7
edges = [[3, 1], [1, 0], [3, 2], [2, 0]]
hosts = [3, 2, 1, 0]

[switch]
port_rate = 8_000_000_000
capacity = 2
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 17

[[flow]]
flow_type = "PacketDistribution"
graph = [[3, 0]]
[flow.traffic]
initial_delay = 0.0
size = 6
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 3, high = 3 }

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 3]]
[flow.traffic]
initial_delay = 0.000000002
size = 4
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 2, high = 2 }

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 2
[flow_set.traffic]
initial_delay = 0.000000005
size = 8
arr_dist = { type = "Uniform", low = 0.000000002, high = 0.000000002 }
pkt_size_dist = { type = "Uniform", low = 4, high = 4 }

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 3
[flow_set.traffic]
initial_delay = 0.000000007
size = 5
arr_dist = { type = "Uniform", low = 0.000000003, high = 0.000000003 }
pkt_size_dist = { type = "Uniform", low = 5, high = 5 }
"#,
    );
    let second_path = write_config(
        &directory,
        "second.toml",
        r#"
seed = 7
edges = [[0, 2], [2, 3], [0, 1], [1, 3]]
hosts = [0, 1, 2, 3]

[switch]
port_rate = 8_000_000_000
capacity = 2
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 17

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 3]]
[flow.traffic]
initial_delay = 0.000000002
size = 4
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 2, high = 2 }

[[flow]]
flow_type = "PacketDistribution"
graph = [[3, 0]]
[flow.traffic]
initial_delay = 0.0
size = 6
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 3, high = 3 }

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 3
[flow_set.traffic]
initial_delay = 0.000000007
size = 5
arr_dist = { type = "Uniform", low = 0.000000003, high = 0.000000003 }
pkt_size_dist = { type = "Uniform", low = 5, high = 5 }

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 2
[flow_set.traffic]
initial_delay = 0.000000005
size = 8
arr_dist = { type = "Uniform", low = 0.000000002, high = 0.000000002 }
pkt_size_dist = { type = "Uniform", low = 4, high = 4 }
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
        (0..8).map(NodeId).collect::<Vec<_>>()
    );
    assert_eq!(
        first.links.iter().map(|link| link.id).collect::<Vec<_>>(),
        (0..16).map(LinkId).collect::<Vec<_>>()
    );
    assert_eq!(
        first.flows.iter().map(|flow| flow.id).collect::<Vec<_>>(),
        (0..7).map(FlowId).collect::<Vec<_>>()
    );
    assert_eq!(
        first
            .flows
            .iter()
            .map(|flow| (flow.source, flow.target))
            .take(2)
            .collect::<Vec<_>>(),
        vec![(NodeId(0), NodeId(3)), (NodeId(3), NodeId(0))]
    );
    assert!(
        first
            .flows
            .iter()
            .all(|flow| (3..=4).contains(&flow.route.len())),
        "each flow retains both host access links and its canonical shortest switch path"
    );
    assert_eq!(
        first
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Host)
            .map(|node| node.state_slot)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(
        first
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Switch)
            .map(|node| node.state_slot)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );

    assert!(!first.host_states.is_empty());
    assert!(!first.switch_states.is_empty());
    assert!(
        first.nodes.iter().any(|node| node.kind == NodeKind::Host)
            && first.nodes.iter().any(|node| node.kind == NodeKind::Switch),
        "lowering must return one heterogeneous semantic image"
    );
    let certified = certified_delays(&first);
    assert_eq!(
        first
            .channels
            .iter()
            .map(|channel| (channel.link, channel.min_delay_ns))
            .collect::<BTreeMap<_, _>>(),
        certified
    );
    assert!(
        first
            .switch_states
            .iter()
            .all(|state| state.queues.len() == 3),
        "each switch owns one FIFO/TailDrop queue per directed egress"
    );
    assert!(
        first
            .channels
            .iter()
            .all(|channel| channel.min_delay_ns > 17),
        "positive serialization must be added to constant propagation"
    );
    assert!(
        first
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .collect::<BTreeSet<_>>()
            .len()
            > 1,
        "route-specific packet sizes should produce distinct certified bounds"
    );
    assert!(
        first
            .initial_events
            .iter()
            .all(|event| event.kind == EventKind::PacketArrival),
        "TxReady must be generated only at the actual service decision point"
    );
    assert_eq!(first.packets.len(), 11);
    assert_eq!(first.initial_events.len(), 11);

    validate(&first, Backend::Scalar).expect("lowered image should validate for scalar");
    validate(&first, Backend::Cpu { workers: 2 })
        .expect("positive bounds should validate for a parallel backend");
    let result = run_scalar(&first, None).expect("lowered image should run end to end");
    assert!(result.pending_events.is_empty());
    assert!(
        result
            .switch_states
            .iter()
            .flat_map(|state| &state.queues)
            .all(|queue| queue.in_service.is_none() && !queue.tx_ready_pending)
    );
    assert!(
        result
            .host_states
            .iter()
            .any(|state| state.received_packets > 0),
        "at least one packet must reach a sink host"
    );
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
    assert_eq!(
        first
            .channels
            .iter()
            .map(|channel| (channel.link, channel.min_delay_ns))
            .collect::<BTreeMap<_, _>>(),
        certified_delays(&first)
    );
    assert_eq!(first.flows.len(), 8);
    assert_eq!(first.packets.len(), 12_000);
    assert_eq!(first.initial_events.len(), 12_000);
    assert_eq!(first.stop_time_ns, 1_500_000_000_000);
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
    assert!(
        first
            .channels
            .iter()
            .all(|channel| channel.min_delay_ns > 0),
        "positive serialization supplies lookahead when propagation is zero"
    );
    validate(&first, Backend::Cpu { workers: 4 })
        .expect("zero propagation with positive serialization is parallel-safe");
    validate(&first, Backend::Scalar).expect("baseline image should validate for scalar execution");
    let result = run_scalar(&first, None).expect("baseline image should run to completion");
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.key.time_ns > first.stop_time_ns),
        "baseline execution should drain every event through its configured duration"
    );
    assert!(
        result
            .host_states
            .iter()
            .any(|state| state.received_packets > 0),
        "baseline execution should deliver traffic before its configured duration"
    );
}

#[test]
fn a_lowered_packet_runs_through_both_switches_to_its_sink() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "end-to-end.toml",
        r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 8_000_000_000
capacity = 4
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 3

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 2
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 2, high = 2 }
"#,
    );

    let image = compile_config(path).expect("supported scenario should lower");
    assert_eq!(image.flows[0].route.len(), 3);
    assert!(
        image
            .channels
            .iter()
            .all(|channel| channel.min_delay_ns == 5)
    );
    validate(&image, Backend::Cpu { workers: 2 })
        .expect("serialization plus propagation gives positive lookahead");

    let result = run_scalar(&image, Some(16)).expect("lowered image should reach the sink");
    assert_eq!(
        result.departures,
        vec![
            PacketDeparture {
                payload: PayloadId(0),
                time_ns: 2,
            },
            PacketDeparture {
                payload: PayloadId(0),
                time_ns: 7,
            },
            PacketDeparture {
                payload: PayloadId(0),
                time_ns: 12,
            },
        ]
    );
    assert_eq!(
        result.arrivals,
        vec![
            PacketArrivalObservation {
                payload: PayloadId(0),
                time_ns: 5,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: PayloadId(0),
                time_ns: 10,
                disposition: ArrivalDisposition::Admitted,
            },
            PacketArrivalObservation {
                payload: PayloadId(0),
                time_ns: 15,
                disposition: ArrivalDisposition::Delivered,
            },
        ]
    );
    assert!(result.pending_events.is_empty());
    assert_eq!(
        result
            .host_states
            .iter()
            .map(|state| state.received_packets)
            .sum::<u64>(),
        1
    );
    assert!(
        result
            .switch_states
            .iter()
            .flat_map(|state| &state.queues)
            .all(|queue| {
                queue.queue.is_empty() && queue.in_service.is_none() && !queue.tx_ready_pending
            })
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
port_rate = 9223372036854775807
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

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use days::flows::flow::Flow;
use days::scenario::compile_config;
use days::topos::build::build_graph;
use days::topos::topo::{
    InstalledHostStage, installed_forwarding_state, installed_host_attachment_state,
};
use days_executor::{
    ArrivalDisposition, Backend, ChunkGranularity, CpuConfig, EventKind, FlowId, LinkId, NodeId,
    NodeKind, ObservationMode, PacketArrivalObservation, PacketDeparture, PayloadId, SchedulerKind,
    SimulationImage, run_cpu, run_scalar, run_scalar_rounds, run_scalar_with_observations,
    validate,
};
use petgraph::graph::NodeIndex;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum QueueSemantics {
    FifoTailDrop,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FlowStage {
    HostInjection {
        host: usize,
        rate_bps: u64,
        propagation_ns: u64,
        capacity_packets: u64,
        queue: QueueSemantics,
    },
    Physical {
        source: usize,
        target: usize,
        rate_bps: u64,
        propagation_ns: u64,
        capacity_packets: u64,
        queue: QueueSemantics,
    },
    HostDelivery {
        host: usize,
        rate_bps: u64,
        propagation_ns: u64,
        capacity_packets: u64,
        queue: QueueSemantics,
    },
}

fn write_config(directory: &TempDir, name: &str, contents: &str) -> String {
    let path = directory.path().join(name);
    fs::write(&path, contents).expect("test configuration should be writable");
    path.to_str()
        .expect("temporary path should be valid UTF-8")
        .to_owned()
}

fn certified_delays(image: &SimulationImage) -> BTreeMap<LinkId, u64> {
    let mut delays = BTreeMap::<LinkId, u64>::new();
    for packet in &image.initial_packets {
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

fn assert_legacy_physical_routes(config_path: &str, image: &SimulationImage) {
    let (graph, hosts) = build_graph(config_path).expect("legacy topology should build");
    let legacy_flows = Flow::flows_from_config(config_path, &hosts);
    let forwarding = installed_forwarding_state(&graph, &legacy_flows);
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

    let physical_links = |route: &[LinkId]| {
        route
            .iter()
            .filter_map(|link_id| {
                let link = links[link_id];
                let source = nodes[&link.source];
                let target = nodes[&link.target];
                (source.kind == NodeKind::Switch && target.kind == NodeKind::Switch)
                    .then_some((source.state_slot as usize, target.state_slot as usize))
            })
            .collect::<Vec<_>>()
    };

    let walk_installed_route = |flow_id: usize,
                                source: usize,
                                target: usize,
                                tables: &BTreeMap<usize, BTreeMap<usize, usize>>,
                                direction: &str| {
        let mut current = source;
        let mut visited = BTreeSet::new();
        let mut route = Vec::new();
        while current != target {
            assert!(
                visited.insert(current),
                "legacy {direction} FIB contains a cycle for flow {flow_id} at switch {current}"
            );
            let next = tables
                .get(&current)
                .and_then(|fib| fib.get(&flow_id))
                .copied()
                .unwrap_or_else(|| {
                    panic!("legacy {direction} FIB is missing flow {flow_id} at switch {current}")
                });
            assert!(
                graph.contains_edge(NodeIndex::new(current), NodeIndex::new(next)),
                "legacy {direction} FIB sends flow {flow_id} across non-link {current} -> {next}"
            );
            route.push((current, next));
            assert!(
                route.len() < graph.node_count(),
                "legacy {direction} FIB does not reach switch {target} for flow {flow_id}"
            );
            current = next;
        }
        route
    };

    let mut legacy_routes = BTreeMap::new();
    for flow in &legacy_flows {
        let forward = walk_installed_route(
            flow.id,
            flow.source_host,
            flow.sink_host,
            &forwarding.fibs,
            "forward",
        );
        let reverse = walk_installed_route(
            flow.id,
            flow.sink_host,
            flow.source_host,
            &forwarding.reverse_fibs,
            "reverse",
        );
        *legacy_routes
            .entry((flow.source_host, flow.sink_host, forward, reverse))
            .or_insert(0_usize) += 1;
    }

    let mut image_routes = BTreeMap::new();
    for flow in &image.flows {
        let first = links[flow
            .route
            .first()
            .expect("lowered flow should have a source attachment")];
        let last = links[flow
            .route
            .last()
            .expect("lowered flow should have a sink attachment")];
        let source = nodes[&first.target].state_slot as usize;
        let target = nodes[&last.source].state_slot as usize;
        *image_routes
            .entry((
                source,
                target,
                physical_links(&flow.route),
                physical_links(&flow.reverse_route),
            ))
            .or_insert(0_usize) += 1;
    }

    assert_eq!(
        image_routes, legacy_routes,
        "lowered flows must use the same endpoints and ordered physical links as legacy Days for {config_path}"
    );

    let Some(attachments) = installed_host_attachment_state(config_path, &hosts, &legacy_flows)
    else {
        return;
    };
    assert_eq!(
        attachments.hosts.len(),
        hosts.iter().copied().collect::<BTreeSet<_>>().len(),
        "legacy must install exactly one shared full-duplex attachment per host"
    );
    if legacy_flows.len() > attachments.hosts.len() {
        assert!(
            attachments
                .hosts
                .values()
                .any(|attachment| attachment.forward_injection_flows.len() > 1),
            "multi-flow fixtures must exercise a shared host injection FIFO"
        );
    }
    let checked_stage = |stage: InstalledHostStage| {
        assert!(
            stage.rate_bps.is_finite()
                && stage.rate_bps > 0.0
                && stage.rate_bps.fract() == 0.0
                && stage.rate_bps <= u64::MAX as f64,
            "exact comparison requires an integral positive legacy stage rate"
        );
        (
            stage.rate_bps as u64,
            stage.propagation_ns,
            stage.capacity_packets as u64,
        )
    };
    let (physical_rate, physical_propagation, physical_capacity) =
        checked_stage(attachments.physical);
    let legacy_stages = |physical: Vec<(usize, usize)>,
                         source: usize,
                         target: usize,
                         injection: InstalledHostStage,
                         delivery: InstalledHostStage| {
        let (injection_rate, injection_propagation, injection_capacity) = checked_stage(injection);
        let (delivery_rate, delivery_propagation, delivery_capacity) = checked_stage(delivery);
        std::iter::once(FlowStage::HostInjection {
            host: source,
            rate_bps: injection_rate,
            propagation_ns: injection_propagation,
            capacity_packets: injection_capacity,
            queue: QueueSemantics::FifoTailDrop,
        })
        .chain(
            physical
                .into_iter()
                .map(|(source, target)| FlowStage::Physical {
                    source,
                    target,
                    rate_bps: physical_rate,
                    propagation_ns: physical_propagation,
                    capacity_packets: physical_capacity,
                    queue: QueueSemantics::FifoTailDrop,
                }),
        )
        .chain(std::iter::once(FlowStage::HostDelivery {
            host: target,
            rate_bps: delivery_rate,
            propagation_ns: delivery_propagation,
            capacity_packets: delivery_capacity,
            queue: QueueSemantics::FifoTailDrop,
        }))
        .collect::<Vec<_>>()
    };
    let mut legacy_complete_routes = BTreeMap::new();
    for flow in &legacy_flows {
        let forward_physical = walk_installed_route(
            flow.id,
            flow.source_host,
            flow.sink_host,
            &forwarding.fibs,
            "forward",
        );
        let reverse_physical = walk_installed_route(
            flow.id,
            flow.sink_host,
            flow.source_host,
            &forwarding.reverse_fibs,
            "reverse",
        );
        let source_attachment = &attachments.hosts[&flow.source_host];
        let sink_attachment = &attachments.hosts[&flow.sink_host];
        assert!(source_attachment.forward_injection_flows.contains(&flow.id));
        assert!(source_attachment.reverse_delivery_flows.contains(&flow.id));
        assert!(sink_attachment.reverse_injection_flows.contains(&flow.id));
        assert!(sink_attachment.forward_delivery_flows.contains(&flow.id));
        let forward = legacy_stages(
            forward_physical,
            flow.source_host,
            flow.sink_host,
            source_attachment.injection,
            sink_attachment.delivery,
        );
        let reverse = legacy_stages(
            reverse_physical,
            flow.sink_host,
            flow.source_host,
            sink_attachment.injection,
            source_attachment.delivery,
        );
        *legacy_complete_routes
            .entry((flow.source_host, flow.sink_host, forward, reverse))
            .or_insert(0_usize) += 1;
    }

    let image_stages = |route: &[LinkId]| {
        route
            .iter()
            .map(|link_id| {
                let link = links[link_id];
                let source = nodes[&link.source];
                let target = nodes[&link.target];
                match (source.kind, target.kind) {
                    (NodeKind::Host, NodeKind::Switch) => FlowStage::HostInjection {
                        host: target.state_slot as usize,
                        rate_bps: link.rate_bps,
                        propagation_ns: link.propagation_ns,
                        capacity_packets: 0,
                        queue: QueueSemantics::FifoTailDrop,
                    },
                    (NodeKind::Switch, NodeKind::Switch) => {
                        let queue = image.switch_states[source.state_slot as usize]
                            .queues
                            .iter()
                            .find(|queue| queue.egress_link == Some(*link_id))
                            .expect("physical link should own one switch egress queue");
                        assert_eq!(
                            queue.scheduler,
                            SchedulerKind::Fifo,
                            "exact physical stage should use FIFO"
                        );
                        FlowStage::Physical {
                            source: source.state_slot as usize,
                            target: target.state_slot as usize,
                            rate_bps: link.rate_bps,
                            propagation_ns: link.propagation_ns,
                            capacity_packets: queue.queue_capacity_packets,
                            queue: QueueSemantics::FifoTailDrop,
                        }
                    }
                    (NodeKind::Switch, NodeKind::Host) => {
                        let queue = image.switch_states[source.state_slot as usize]
                            .queues
                            .iter()
                            .find(|queue| queue.egress_link == Some(*link_id))
                            .expect("host delivery link should own one switch egress queue");
                        assert_eq!(
                            queue.scheduler,
                            SchedulerKind::Fifo,
                            "exact host delivery should use FIFO"
                        );
                        FlowStage::HostDelivery {
                            host: source.state_slot as usize,
                            rate_bps: link.rate_bps,
                            propagation_ns: link.propagation_ns,
                            capacity_packets: queue.queue_capacity_packets,
                            queue: QueueSemantics::FifoTailDrop,
                        }
                    }
                    kinds => panic!("unsupported lowered route stage {kinds:?}"),
                }
            })
            .collect::<Vec<_>>()
    };
    let mut image_complete_routes = BTreeMap::new();
    for flow in &image.flows {
        let forward = image_stages(&flow.route);
        let reverse = image_stages(&flow.reverse_route);
        let source = match forward.first() {
            Some(FlowStage::HostInjection { host, .. }) => *host,
            first => panic!("lowered forward route should start at a host injection: {first:?}"),
        };
        let target = match forward.last() {
            Some(FlowStage::HostDelivery { host, .. }) => *host,
            last => panic!("lowered forward route should end at a host delivery: {last:?}"),
        };
        *image_complete_routes
            .entry((source, target, forward, reverse))
            .or_insert(0_usize) += 1;
    }
    assert_eq!(
        image_complete_routes, legacy_complete_routes,
        "key-on legacy flows must use the same ordered endpoint and physical stages as the exact image for {config_path}"
    );
}

fn compile_for_legacy_comparison(path: &Path) -> SimulationImage {
    let image = compile_config(path).expect("legacy comparison fixture should lower");
    assert_legacy_physical_routes(
        path.to_str()
            .expect("legacy comparison fixture path should be valid UTF-8"),
        &image,
    );
    image
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
        result.summary.received_packets, 0,
        "a packet starting after the configured duration must not be delivered"
    );
    assert_eq!(result.pending_events.len(), 1);
}

#[test]
fn zero_byte_and_zero_duration_flows_have_finished_generators_without_inputs() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "zero-termination.toml",
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
size = 0
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
duration = 0.0
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#,
    );

    let image = compile_config(path).expect("zero termination should lower");
    assert_eq!(image.flows.len(), 2);
    assert!(image.initial_packets.is_empty());
    assert!(image.initial_events.is_empty());
    assert!(image.channels.is_empty());
    assert!(
        image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .all(|generator| generator.next_emission.status
                == days_executor::GeneratorStatus::Finished)
    );
    validate(&image, Backend::Scalar).expect("zero-termination image should validate");
    let result = run_scalar(&image, None).expect("zero-termination image should run");
    assert_eq!(result.summary.sourced_packets, 0);
    assert!(result.pending_events.is_empty());
}

#[test]
fn generator_rng_seed_depends_on_the_semantic_flow_key_not_dense_flow_id() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let common = r#"
seed = 91
duration = 1.0
edges = [[0, 1], [1, 2]]
hosts = [0, 1, 2]

[switch]
port_rate = 8_000_000_000
capacity = 8
discipline = "FIFO"
drop = "TailDrop"
"#;
    let traffic = r#"
[flow.traffic]
size = 2
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#;
    let base = write_config(
        &directory,
        "rng-base.toml",
        &format!(
            "{common}\n[[flow]]\nflow_type = \"PacketDistribution\"\ngraph = [[1, 2]]\n{traffic}"
        ),
    );
    let extended = write_config(
        &directory,
        "rng-extended.toml",
        &format!(
            "{common}\n[[flow]]\nflow_type = \"PacketDistribution\"\ngraph = [[0, 2]]\n{traffic}\n[[flow]]\nflow_type = \"PacketDistribution\"\ngraph = [[1, 2]]\n{traffic}"
        ),
    );

    let base = compile_config(base).expect("base config should lower");
    let extended = compile_config(extended).expect("extended config should lower");
    let rng_for = |image: &SimulationImage, source: NodeId, target: NodeId| {
        let flow = image
            .flows
            .iter()
            .find(|flow| flow.source == source && flow.target == target)
            .expect("semantic flow must exist");
        image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .find(|generator| generator.flow == flow.id)
            .expect("flow generator must exist")
            .rng_state
    };
    assert_eq!(
        rng_for(&base, NodeId(1), NodeId(2)),
        rng_for(&extended, NodeId(1), NodeId(2))
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
    assert_eq!(first.initial_packets.len(), first.flows.len());
    assert_eq!(
        first
            .host_states
            .iter()
            .map(|state| state.generators.len())
            .sum::<usize>(),
        first.flows.len()
    );
    assert_eq!(first.initial_events.len(), first.flows.len());

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

    let first = compile_for_legacy_comparison(&path);
    let second = compile_config(&path).expect("a second in-process lowering should also succeed");

    for fixture in [
        "fattree_k8_f64_st.toml",
        "fattree_k16_f512_st.toml",
        "fattree_k32_f4096_st.toml",
    ] {
        let comparison_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("configs/benchmarks/baseline")
            .join(fixture);
        let comparison_image =
            compile_config(&comparison_path).expect("comparison fixture should lower");
        assert_legacy_physical_routes(
            comparison_path
                .to_str()
                .expect("fixture path should be valid UTF-8"),
            &comparison_image,
        );
    }
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
    assert_eq!(first.initial_packets.len(), first.flows.len());
    assert_eq!(
        first
            .host_states
            .iter()
            .map(|state| state.generators.len())
            .sum::<usize>(),
        first.flows.len()
    );
    assert_eq!(first.initial_events.len(), first.flows.len());
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
    let round_run =
        run_scalar_rounds(&first, None).expect("baseline image should run by safe-horizon rounds");
    let cpu_run = run_cpu(
        &first,
        None,
        CpuConfig {
            workers: 4,
            granularity: ChunkGranularity::Fixed(1),
            straggler_threshold_events: Some(16),
            dedicated_straggler_workers: 1,
            ..CpuConfig::default()
        },
    )
    .expect("baseline image should run on persistent CPU workers");
    assert_eq!(
        round_run.result, result,
        "round execution must match the complete global-priority-queue state"
    );
    assert_eq!(
        cpu_run.result, result,
        "CPU execution must match the complete global-priority-queue state"
    );
    assert_eq!(result.summary.sourced_packets, 12_000);
    assert_eq!(result.summary.sourced_bytes, 12_000_000);
    assert_eq!(result.summary.received_packets, 11_992);
    assert_eq!(result.summary.received_bytes, 11_992_000);
    assert_eq!(result.summary.dropped_packets, 0);
    assert_eq!(result.summary.dropped_bytes, 0);
    assert!(result.arrivals.is_empty());
    assert!(result.departures.is_empty());
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
fn key_on_legacy_endpoint_stage_sequences_match_the_exact_image() {
    let directory = TempDir::new().expect("temporary directory should be available");

    for fixture in [
        "fattree_k4_f8_st.toml",
        "fattree_k8_f64_st.toml",
        "fattree_k16_f512_st.toml",
        "fattree_k32_f4096_st.toml",
    ] {
        let baseline = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("configs/benchmarks/baseline")
            .join(fixture);
        let contents = fs::read_to_string(&baseline).expect("baseline fixture should be readable");
        let enabled = write_config(
            &directory,
            fixture,
            &format!("model_host_attachment = true\n{contents}"),
        );
        let image = compile_config(&enabled).expect("enabled fixture should lower");
        assert_legacy_physical_routes(&enabled, &image);
    }
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

    let result = run_scalar_with_observations(&image, Some(16), ObservationMode::Full)
        .expect("lowered image should reach the sink");
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

    assert_eq!(image.initial_packets.len(), 1);
    assert_eq!(
        image.initial_packets[0].size_bytes,
        9_223_372_036_854_775_807
    );
}

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use assert_cmd::cargo::cargo_bin_cmd;
use days::flows::flow::Flow;
use days::scenario::compile_config;
use days::topos::build::build_graph;
use days::topos::topo::{
    InstalledHostStage, InstalledHostStageDirection, installed_forwarding_state,
    installed_host_attachment_state,
};
use days_executor::{
    ArrivalDisposition, Backend, ChunkGranularity, CpuConfig, EventKind, FlowGeneratorKind, FlowId,
    LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode, PacketArrivalObservation,
    PacketDeparture, PacketKind, PayloadId, SchedulerKind, SimulationImage, TcpCongestionControl,
    run_cpu, run_scalar, run_scalar_rounds, run_scalar_with_observations, validate,
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

fn debug_fnv1a64(value: &impl fmt::Debug) -> u64 {
    struct Fnv1a64(u64);

    impl fmt::Write for Fnv1a64 {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            self.0 = fnv1a64_with_seed(self.0, value.as_bytes());
            Ok(())
        }
    }

    fn fnv1a64_with_seed(seed: u64, bytes: &[u8]) -> u64 {
        bytes.iter().fold(seed, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        })
    }

    let mut hash = Fnv1a64(0xcbf29ce484222325);
    fmt::write(&mut hash, format_args!("{value:#?}"))
        .expect("formatting into the image hasher should succeed");
    hash.0
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

fn assert_port_lp_decomposition(image: &SimulationImage) {
    let nodes = image
        .nodes
        .iter()
        .map(|node| (node.id, node))
        .collect::<BTreeMap<_, _>>();
    let switch_egress_links = image
        .links
        .iter()
        .filter(|link| nodes[&link.source].kind == NodeKind::Switch)
        .collect::<Vec<_>>();

    assert_eq!(
        image.switch_states.len(),
        switch_egress_links.len(),
        "every physical switch egress must own one switch LP state"
    );
    assert_eq!(
        image
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Switch)
            .count(),
        switch_egress_links.len(),
        "every physical switch egress must have one distinct LP identity"
    );
    assert_eq!(
        switch_egress_links
            .iter()
            .map(|link| link.source)
            .collect::<BTreeSet<_>>()
            .len(),
        switch_egress_links.len(),
        "switch egress LP origin identities must be unique"
    );
    assert!(
        image
            .switch_states
            .iter()
            .all(|state| { state.queues.len() == 1 && state.queues[0].egress_link.is_some() }),
        "each switch LP must own exactly one egress queue"
    );

    for flow in &image.flows {
        for (index, link_id) in flow.route.iter().enumerate() {
            let link = &image.links[link_id.0 as usize];
            let direct_target = flow
                .route
                .get(index + 1)
                .map_or(flow.target, |next| image.links[next.0 as usize].source);
            assert!(
                image.channels.iter().any(|channel| {
                    channel.link == *link_id
                        && channel.source == link.source
                        && channel.target == direct_target
                }),
                "physical link {link_id:?} must deliver directly to the route-selected next LP"
            );
        }
    }
}

fn assert_legacy_physical_routes(config_path: &str, image: &SimulationImage) {
    let (graph, hosts) = build_graph(config_path).expect("legacy topology should build");
    let legacy_flows = Flow::flows_from_config_with_attachments(config_path, &hosts);
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
    let physical_switch = |node: &NodeDescriptor| {
        image.switch_states[node.state_slot as usize].physical_switch as usize
    };

    let physical_links = |route: &[LinkId]| {
        route
            .iter()
            .filter_map(|link_id| {
                let link = links[link_id];
                let source = nodes[&link.source];
                let target = nodes[&link.target];
                (source.kind == NodeKind::Switch && target.kind == NodeKind::Switch)
                    .then_some((physical_switch(source), physical_switch(target)))
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
        let source_switch = hosts
            .switch_for(flow.source_host)
            .expect("legacy flow source must have an attachment switch");
        let sink_switch = hosts
            .switch_for(flow.sink_host)
            .expect("legacy flow sink must have an attachment switch");
        let forward = walk_installed_route(
            flow.id,
            source_switch,
            sink_switch,
            &forwarding.fibs,
            "forward",
        );
        let reverse = walk_installed_route(
            flow.id,
            sink_switch,
            source_switch,
            &forwarding.reverse_fibs,
            "reverse",
        );
        *legacy_routes
            .entry((source_switch, sink_switch, forward, reverse))
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
        let source = physical_switch(nodes[&first.target]);
        let target = physical_switch(nodes[&last.source]);
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
        hosts
            .host_ids()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len(),
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
    let checked_stage = |stage: InstalledHostStage,
                         expected_direction: InstalledHostStageDirection| {
        assert_eq!(
            stage.direction, expected_direction,
            "installed legacy stage must retain its instantiated direction"
        );
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
        checked_stage(attachments.physical, InstalledHostStageDirection::Physical);
    let legacy_stages = |physical: Vec<(usize, usize)>,
                         source: usize,
                         target: usize,
                         injection: InstalledHostStage,
                         delivery: InstalledHostStage| {
        let (injection_rate, injection_propagation, injection_capacity) =
            checked_stage(injection, InstalledHostStageDirection::Injection);
        let (delivery_rate, delivery_propagation, delivery_capacity) =
            checked_stage(delivery, InstalledHostStageDirection::Delivery);
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
        let source_switch = hosts
            .switch_for(flow.source_host)
            .expect("legacy flow source must have an attachment switch");
        let sink_switch = hosts
            .switch_for(flow.sink_host)
            .expect("legacy flow sink must have an attachment switch");
        let forward_physical = walk_installed_route(
            flow.id,
            source_switch,
            sink_switch,
            &forwarding.fibs,
            "forward",
        );
        let reverse_physical = walk_installed_route(
            flow.id,
            sink_switch,
            source_switch,
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
            source_switch,
            sink_switch,
            source_attachment.injection,
            sink_attachment.delivery,
        );
        let reverse = legacy_stages(
            reverse_physical,
            sink_switch,
            source_switch,
            sink_attachment.injection,
            source_attachment.delivery,
        );
        *legacy_complete_routes
            .entry((source_switch, sink_switch, forward, reverse))
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
                        host: physical_switch(target),
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
                            source: physical_switch(source),
                            target: physical_switch(target),
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
                            host: physical_switch(source),
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

fn legacy_flow_observations(
    csv_path: &Path,
    packet_column: &str,
    byte_column: &str,
) -> BTreeMap<usize, (u128, u128)> {
    let mut reader = csv::Reader::from_path(csv_path).unwrap_or_else(|error| {
        panic!(
            "legacy observation {} should open: {error}",
            csv_path.display()
        )
    });
    let headers = reader
        .headers()
        .expect("legacy observation CSV should have a header")
        .clone();
    let column = |name: &str| {
        headers
            .iter()
            .position(|header| header == name)
            .unwrap_or_else(|| panic!("legacy observation CSV should have a {name} column"))
    };
    let flow_column = column("flow_id");
    let packet_column = column(packet_column);
    let byte_column = column(byte_column);
    let mut observations = BTreeMap::<usize, (u128, u128)>::new();
    for record in reader.records() {
        let record = record.expect("legacy observation row should parse");
        let flow = record[flow_column]
            .parse::<usize>()
            .expect("legacy flow identity should parse");
        let packets = record[packet_column]
            .parse::<u128>()
            .expect("legacy packet count should parse");
        let bytes = record[byte_column]
            .parse::<u128>()
            .expect("legacy byte count should parse");
        let observation = observations.entry(flow).or_default();
        observation.0 += packets;
        observation.1 += bytes;
    }
    observations
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
        (0..16).map(NodeId).collect::<Vec<_>>()
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
        (0..12).collect::<Vec<_>>()
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
    assert_port_lp_decomposition(&first);
    let channel_targets_by_link = first.channels.iter().fold(
        BTreeMap::<LinkId, BTreeSet<NodeId>>::new(),
        |mut targets, channel| {
            targets
                .entry(channel.link)
                .or_default()
                .insert(channel.target);
            targets
        },
    );
    assert!(
        channel_targets_by_link
            .values()
            .any(|targets| targets.len() > 1),
        "direct downstream routing must allow one physical inbound link to feed several egress LPs"
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
fn representative_lowered_image_bytes_match_frozen_preoptimization_hashes() {
    let fixtures = [
        "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
        "configs/benchmarks/baseline/fattree_k8_f64_st.toml",
        "configs/benchmarks/width_via_load_full/fattree_k32_load_10.toml",
        "configs/benchmarks/real_image_gate/fattree_k64_h16_f32768_st.toml",
    ];
    let actual = fixtures.map(|fixture| {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(fixture);
        let image = compile_config(path).unwrap_or_else(|error| {
            panic!("representative fixture {fixture} should lower: {error}")
        });
        debug_fnv1a64(&image)
    });

    assert_eq!(
        actual,
        [
            8_218_538_847_115_646_020,
            8_308_880_521_678_431_806,
            18_053_182_671_785_655_793,
            787_825_773_756_164_116,
        ],
        "route-construction changes must preserve every ordered post-T23 image byte"
    );
}

#[test]
fn custom_graph_astar_fallback_matches_frozen_preoptimization_image_hash() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "custom-graph-astar-fallback.toml",
        r#"
seed = 17
duration = 0.000001
edges = [[0, 2], [0, 3], [1, 2], [1, 3], [0, 4], [3, 4]]
hosts = [0, 1]

[switch]
port_rate = 8_000_000_000
capacity = 8
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 17

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 16
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 4, high = 4 }
"#,
    );

    let (graph, _) = build_graph(&path).expect("custom topology should build");
    assert_eq!(graph.node_count(), 5);
    assert_eq!(
        graph.edge_count(),
        6,
        "a canonical k=2 fat tree has four edges, so this graph must take the A* fallback"
    );

    let image = compile_config(&path).expect("custom A* fallback scenario should lower");
    assert_eq!(
        debug_fnv1a64(&image),
        11_025_326_788_893_610_349,
        "the post-optimization fallback image must match its frozen post-T23 schema bytes"
    );
}

#[test]
fn configured_tcp_reno_and_cubic_lower_to_the_t23_closed_loop_image() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let path = write_config(
        &directory,
        "tcp-reno-cubic.toml",
        r#"
seed = 24
duration = 0.0001
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 100_000_000_000
capacity = 16
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 1000

[[flow]]
flow_type = "TCP"
graph = [[0, 1]]
[flow.traffic]
initial_delay = 0.0
size = 4096
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
[flow.traffic.tcp]
cc_algorithm = "TCPReno"

[[flow]]
flow_type = "TCP"
graph = [[1, 0]]
[flow.traffic]
initial_delay = 0.0
size = 4096
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
[flow.traffic.tcp]
cc_algorithm = "CUBIC"
[flow.traffic.tcp.cubic]
beta = 0.7
c = 0.4
fast_convergence = true
"#,
    );

    let image = compile_config(&path).expect("supported TCP should lower");
    assert_eq!(image.flows.len(), 2);
    assert_eq!(image.initial_packets.len(), 2);
    assert!(image.initial_packets.iter().all(|packet| {
        matches!(
            packet.kind,
            PacketKind::TcpData(header)
                if header.sequence == 0
                    && header.sent_time_ns == 0
                    && !header.retransmission
        )
    }));

    let generators = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .collect::<Vec<_>>();
    assert_eq!(generators.len(), 2);
    assert!(generators.iter().any(|generator| {
        matches!(
            generator.kind,
            FlowGeneratorKind::Tcp(tcp)
                if matches!(tcp.control, TcpCongestionControl::Reno(_))
                    && tcp.total_bytes == 4096
                    && tcp.mss_bytes == 512
                    && tcp.ack_size_bytes == 40
        )
    }));
    assert!(generators.iter().any(|generator| {
        matches!(
            generator.kind,
            FlowGeneratorKind::Tcp(tcp)
                if matches!(tcp.control, TcpCongestionControl::Cubic(_))
                    && tcp.total_bytes == 4096
                    && tcp.mss_bytes == 512
                    && tcp.ack_size_bytes == 40
        )
    }));
    assert_eq!(
        image
            .host_states
            .iter()
            .map(|state| state.tcp_receivers.len())
            .sum::<usize>(),
        2
    );
    validate(&image, Backend::Scalar).expect("lowered TCP image should validate");

    let scalar = run_scalar(&image, None).expect("scalar TCP execution should succeed");
    let cpu = run_cpu(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
    )
    .expect("CPU TCP execution should succeed")
    .result;
    assert_eq!(cpu, scalar, "configured TCP must remain byte-identical");
}

#[test]
fn configured_tcp_rejects_transport_options_outside_the_t23_lattice() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let cases = [
        (
            "bbr",
            "size = 4096\narr_dist = { type = \"Uniform\", low = 1.0, high = 1.0 }\npkt_size_dist = { type = \"DiscreteUniform\", low = 512, high = 512 }\n[flow.traffic.tcp]\ncc_algorithm = \"TCPBBR\"",
            "unsupported TCP congestion control `TCPBBR`; Days executor supports exact Reno and CUBIC",
        ),
        (
            "ecn",
            "size = 4096\narr_dist = { type = \"Uniform\", low = 1.0, high = 1.0 }\npkt_size_dist = { type = \"DiscreteUniform\", low = 512, high = 512 }\n[flow.traffic.tcp]\ncc_algorithm = \"TCPReno\"\necn = true",
            "unsupported TCP ECN; the T23 executor TCP lattice models loss feedback only",
        ),
        (
            "cubic-beta",
            "size = 4096\narr_dist = { type = \"Uniform\", low = 1.0, high = 1.0 }\npkt_size_dist = { type = \"DiscreteUniform\", low = 512, high = 512 }\n[flow.traffic.tcp]\ncc_algorithm = \"CUBIC\"\n[flow.traffic.tcp.cubic]\nbeta = 0.8",
            "unsupported TCP CUBIC parameters; the exact T23 lattice requires beta=0.7, c=0.4, fast_convergence=true",
        ),
        (
            "duration",
            "duration = 0.001\narr_dist = { type = \"Uniform\", low = 1.0, high = 1.0 }\npkt_size_dist = { type = \"DiscreteUniform\", low = 512, high = 512 }\n[flow.traffic.tcp]\ncc_algorithm = \"TCPReno\"",
            "unsupported duration-terminated TCP traffic; the executor requires an exact byte `size`",
        ),
    ];

    for (name, traffic, expected) in cases {
        let config = format!(
            r#"
seed = 24
duration = 0.001
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 100_000_000_000
capacity = 16
discipline = "FIFO"
drop = "TailDrop"
[[flow]]
flow_type = "TCP"
graph = [[0, 1]]
[flow.traffic]
{traffic}
"#
        );
        let path = write_config(&directory, &format!("tcp-{name}.toml"), &config);
        assert_eq!(
            compile_config(path)
                .expect_err("unsupported TCP option should fail")
                .to_string(),
            expected
        );
    }
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
discipline = "DRR"
drop = "TailDrop"
"#,
            "unsupported scheduler `DRR`; Days executor supports FIFO, SP, and WFQ",
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
            "unsupported flow type `DCQCN`; Days executor supports PacketDistribution and exact TCP Reno/CUBIC traffic",
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
fn sp_and_wfq_config_surfaces_lower_with_validated_class_parameters() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let header = r#"
seed = 1
duration = 0.000000010
edges = [[0, 1]]
hosts = [0, 1]
"#;
    let flow = r#"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
size = 2
arr_dist = { type = "Uniform", low = 0.000000001, high = 0.000000001 }
pkt_size_dist = { type = "Uniform", low = 1, high = 1 }
"#;
    let sp_path = write_config(
        &directory,
        "sp.toml",
        &format!(
            r#"{header}
[switch]
port_rate = 8_000_000_000
capacity = 8
discipline = "SP"
drop = "TailDrop"
priorities = [1, 7, 3]
{flow}
"#
        ),
    );
    let wfq_path = write_config(
        &directory,
        "wfq.toml",
        &format!(
            r#"{header}
[switch]
port_rate = 8_000_000_000
capacity = 8
discipline = "WFQ"
drop = "TailDrop"
weights = [1, 4, 2]
{flow}
"#
        ),
    );

    let sp = compile_config(sp_path).expect("SP scenario should lower");
    let wfq = compile_config(wfq_path).expect("WFQ scenario should lower");
    assert!(
        sp.switch_states
            .iter()
            .all(|state| state.queues.iter().all(|queue| matches!(
                &queue.scheduler,
                SchedulerKind::StaticPriority { priorities } if priorities == &[1, 7, 3]
            )))
    );
    assert!(
        wfq.switch_states
            .iter()
            .all(|state| state.queues.iter().all(|queue| matches!(
                &queue.scheduler,
                SchedulerKind::WeightedFairQueue(state) if state.weights == [1, 4, 2]
            )))
    );
    validate(&sp, Backend::Cpu { workers: 2 }).expect("CPU should accept lowered SP");
    validate(&wfq, Backend::Cpu { workers: 2 }).expect("CPU should accept lowered WFQ");
}

#[test]
fn sp_missing_and_empty_priorities_default_to_one_class() {
    let directory = TempDir::new().expect("temporary directory should be available");
    for (name, priorities) in [("missing", ""), ("empty", "priorities = []")] {
        let path = write_config(
            &directory,
            &format!("sp-{name}.toml"),
            &format!(
                r#"
seed = 1
duration = 0.000000010
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000_000_000
capacity = 8
discipline = "SP"
drop = "TailDrop"
{priorities}

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
size = 2
arr_dist = {{ type = "Uniform", low = 0.000000001, high = 0.000000001 }}
pkt_size_dist = {{ type = "Uniform", low = 1, high = 1 }}
"#
            ),
        );

        let image = compile_config(path).expect("SP defaults should lower");
        assert!(image.switch_states.iter().all(|state| {
            state.queues.iter().all(|queue| {
                matches!(
                    &queue.scheduler,
                    SchedulerKind::StaticPriority { priorities } if priorities == &[1]
                )
            })
        }));
    }
}

#[test]
fn wfq_config_rejects_missing_empty_and_zero_weights() {
    let directory = TempDir::new().expect("temporary directory should be available");
    for (name, weights, expected) in [
        (
            "missing",
            "",
            "invalid scenario: `switch.weights` must be provided for WFQ scheduling",
        ),
        (
            "empty",
            "weights = []",
            "invalid scenario: `switch.weights` must contain at least one class for WFQ scheduling",
        ),
        (
            "zero",
            "weights = [1, 0]",
            "invalid scenario: `switch.weights[1]` must be positive for WFQ scheduling",
        ),
    ] {
        let path = write_config(
            &directory,
            &format!("wfq-{name}.toml"),
            &format!(
                r#"
seed = 1
edges = [[0, 1]]
hosts = [0, 1]
[switch]
port_rate = 8_000_000_000
capacity = 8
discipline = "WFQ"
drop = "TailDrop"
{weights}
"#
            ),
        );
        assert_eq!(
            compile_config(path)
                .expect_err("malformed WFQ weights must be rejected")
                .to_string(),
            expected
        );
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
    assert_eq!(
        first.switch_states.len(),
        72,
        "old per-switch state count was 20; the 72 physical egress queues are now LPs"
    );
    assert_eq!(
        first.nodes.len(),
        80,
        "old node count was 28; 8 hosts plus 72 switch egress LPs are now represented"
    );
    assert_eq!(first.links.len(), 80);
    assert_eq!(
        first
            .channels
            .iter()
            .map(|channel| (channel.link, channel.min_delay_ns))
            .collect::<BTreeMap<_, _>>(),
        certified_delays(&first)
    );
    let pre_split_physical_lookahead = certified_delays(&first).values().copied().min();
    let port_lp_lookahead = first
        .channels
        .iter()
        .map(|channel| channel.min_delay_ns)
        .min();
    assert_eq!(
        port_lp_lookahead, pre_split_physical_lookahead,
        "duplicating physical-link channels by route-selected port must not change global lookahead"
    );
    assert_eq!(
        port_lp_lookahead,
        Some(25_000_000),
        "the pre-split certified global lookahead was 25 ms"
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
    assert_port_lp_decomposition(&first);
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
fn legacy_fattree_with_distinct_host_and_switch_ids_matches_exact_observations() {
    let directory = TempDir::new().expect("temporary directory should be available");
    let log_path = directory.path().join("legacy");
    let path = write_config(
        &directory,
        "fattree-k4-h2.toml",
        &format!(
            r#"
seed = 91
duration = 0.0001
threading = "single"
log_path = "{}"
model_host_attachment = true

[topology]
category = "FatTree"

[topology.fat_tree]
k = 4
hosts_per_edge = 2

[switch]
port_rate = 8_000_000_000
capacity = 32
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 7

[[flow]]
flow_type = "PacketDistribution"
graph = [[8, 9]]
[flow.traffic]
initial_delay = 0.000001
size = 3000
arr_dist = {{ type = "Uniform", low = 0.0000001, high = 0.0000001 }}
pkt_size_dist = {{ type = "Uniform", low = 1000, high = 1000 }}

[[flow]]
flow_type = "PacketDistribution"
graph = [[10, 12]]
[flow.traffic]
initial_delay = 0.000002
size = 4000
arr_dist = {{ type = "Uniform", low = 0.0000001, high = 0.0000001 }}
pkt_size_dist = {{ type = "Uniform", low = 1000, high = 1000 }}

[[flow]]
flow_type = "PacketDistribution"
graph = [[15, 11]]
[flow.traffic]
initial_delay = 0.000003
size = 5000
arr_dist = {{ type = "Uniform", low = 0.0000001, high = 0.0000001 }}
pkt_size_dist = {{ type = "Uniform", low = 1000, high = 1000 }}
"#,
            log_path.display()
        ),
    );

    let image = compile_config(&path).expect("multi-host fixture should lower");
    assert_legacy_physical_routes(&path, &image);
    let exact = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("multi-host fixture should run exactly");

    let mut command = cargo_bin_cmd!("days");
    command
        .env("RUST_LOG", "error")
        .arg(&path)
        .assert()
        .success()
        .stdout("")
        .stderr("");

    let legacy_sources = legacy_flow_observations(
        &log_path.join("sources.csv"),
        "sent_packets",
        "packet_sizes",
    );
    let legacy_sinks = legacy_flow_observations(
        &log_path.join("sinks.csv"),
        "received_packets",
        "received_sizes",
    );
    let packets = exact
        .observed_packets
        .iter()
        .map(|packet| (packet.id, packet))
        .collect::<BTreeMap<_, _>>();
    let mut exact_sources = BTreeMap::<usize, (u128, u128)>::new();
    for packet in exact
        .observed_packets
        .iter()
        .filter(|packet| packet.kind == PacketKind::Data)
    {
        let observation = exact_sources.entry(packet.flow.0 as usize).or_default();
        observation.0 += 1;
        observation.1 += u128::from(packet.size_bytes);
    }
    let mut exact_sinks = BTreeMap::<usize, (u128, u128)>::new();
    for arrival in exact
        .arrivals
        .iter()
        .filter(|arrival| arrival.disposition == ArrivalDisposition::Delivered)
    {
        let packet = packets[&arrival.payload];
        let observation = exact_sinks.entry(packet.flow.0 as usize).or_default();
        observation.0 += 1;
        observation.1 += u128::from(packet.size_bytes);
    }
    assert_eq!(
        legacy_sources,
        BTreeMap::from([(0, (3, 3000)), (1, (4, 4000)), (2, (5, 5000))]),
        "legacy sources must remain attached to all three distinct host identities"
    );
    assert_eq!(
        legacy_sources, exact_sources,
        "legacy source observations must match the exact scalar executor by flow"
    );
    assert_eq!(
        legacy_sinks, exact_sinks,
        "legacy endpoint wiring and attachment demultiplexing must match exact deliveries by flow"
    );

    let exact_summary = exact.summary;
    let legacy_total = legacy_sources
        .values()
        .copied()
        .fold((0_u128, 0_u128), |(packets, bytes), observation| {
            (packets + observation.0, bytes + observation.1)
        });
    assert_eq!(
        (
            exact_summary.sourced_packets,
            exact_summary.sourced_bytes,
            exact_summary.received_packets,
            exact_summary.received_bytes,
            exact_summary.dropped_packets,
            exact_summary.dropped_bytes,
        ),
        (
            legacy_total.0,
            legacy_total.1,
            legacy_total.0,
            legacy_total.1,
            0,
            0,
        ),
        "legacy terminal source/sink observations must match the exact scalar executor"
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

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days::topos::build::build_graph;
use days_executor::{FlowGeneratorKind, GeneratorTermination, SchedulerKind, run_scalar_rounds};
use days_legacy::flows::flow::Flow;

const FIXTURE_DIRECTORY: &str = "configs/benchmarks/width_via_load_full";
const HOST_COUNT: u64 = 8_192;
const HOST_RATE_BPS: u64 = 100_000_000_000;
const INTERVAL_NS: u64 = 21;
const PACKET_SIZE_BYTES: u64 = 256;
const MIN_EVENTS: u128 = 10_000_000;
const MAX_EVENTS: u128 = 50_000_000;

// Keep the full-load lane in Cargo's debug test profile: overflow panics and
// debug assertions are deliberate detection instruments. The checked-in files
// under FIXTURE_DIRECTORY are read-only inputs; any future generated artifacts
// must live in a per-test tempfile::TempDir.

#[derive(Clone, Copy)]
struct Fixture {
    name: &'static str,
    nominal_load_percent: u64,
    flow_count: u64,
    flow_bytes: u64,
    stop_time_ns: u64,
    expected_load_percent: f64,
}

const FIXTURES: [Fixture; 5] = [
    Fixture {
        name: "fattree_k32_load_10.toml",
        nominal_load_percent: 10,
        flow_count: 841,
        flow_bytes: 524_288,
        stop_time_ns: 36_000,
        expected_load_percent: 10.016_076,
    },
    Fixture {
        name: "fattree_k32_load_30.toml",
        nominal_load_percent: 30,
        flow_count: 2_458,
        flow_bytes: 524_288,
        stop_time_ns: 30_000,
        expected_load_percent: 29.270_683,
    },
    Fixture {
        name: "fattree_k32_load_50.toml",
        nominal_load_percent: 50,
        flow_count: 4_096,
        flow_bytes: 524_288,
        stop_time_ns: 24_000,
        expected_load_percent: 48.768_000,
    },
    Fixture {
        name: "fattree_k32_load_70.toml",
        nominal_load_percent: 70,
        flow_count: 5_734,
        flow_bytes: 262_144,
        stop_time_ns: 20_000,
        expected_load_percent: 68.306_275,
    },
    Fixture {
        name: "fattree_k32_load_90.toml",
        nominal_load_percent: 90,
        flow_count: 7_373,
        flow_bytes: 262_144,
        stop_time_ns: 18_000,
        expected_load_percent: 87.861_583,
    },
];

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(FIXTURE_DIRECTORY)
        .join(name)
}

fn read_fixture(path: &Path) -> toml::Table {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        .parse()
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn sourced_bytes_through_stop(fixture: Fixture) -> u128 {
    let configured_packets = fixture.flow_bytes / PACKET_SIZE_BYTES;
    let packets_through_stop = fixture.stop_time_ns / INTERVAL_NS + 1;
    u128::from(
        fixture.flow_count * configured_packets.min(packets_through_stop) * PACKET_SIZE_BYTES,
    )
}

fn offered_host_load_percent(fixture: Fixture) -> f64 {
    let stop_seconds = fixture.stop_time_ns as f64 / 1_000_000_000.0;
    let capacity_bytes = HOST_COUNT as f64 * HOST_RATE_BPS as f64 / 8.0 * stop_seconds;
    sourced_bytes_through_stop(fixture) as f64 / capacity_bytes * 100.0
}

#[test]
fn width_via_load_full_fixtures_hold_the_t13f_design_invariants() {
    let mut previous_flow_count = 0;

    for fixture in FIXTURES {
        let config = read_fixture(&fixture_path(fixture.name));
        let explicit_flow_count = config
            .get("flow")
            .map(|flows| flows.as_array().expect("flow must be a TOML array").len())
            .unwrap_or_default();
        let flow_sets = config["flow_set"]
            .as_array()
            .expect("fixture must contain one flow set");
        let traffic = &flow_sets[0]["traffic"];
        let load = offered_host_load_percent(fixture);

        assert_eq!(config["seed"].as_integer(), Some(13_032));
        assert_eq!(config["topology"]["category"].as_str(), Some("FatTree"));
        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(32));
        assert_eq!(
            config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(16)
        );
        assert_eq!(
            config["switch"]["port_rate"].as_integer(),
            Some(100_000_000_000)
        );
        assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
        assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
        assert_eq!(config["switch"]["capacity"].as_integer(), Some(1_024));
        assert_eq!(config["link"]["propagation_ns"].as_integer(), Some(1_000));
        assert_eq!(
            config["duration"].as_float(),
            Some(fixture.stop_time_ns as f64 / 1_000_000_000.0)
        );
        assert_eq!(explicit_flow_count, 0);
        assert_eq!(flow_sets.len(), 1);
        assert_eq!(
            flow_sets[0]["flow_count"].as_integer(),
            i64::try_from(fixture.flow_count).ok()
        );
        assert_eq!(
            flow_sets[0]["flow_type"].as_str(),
            Some("PacketDistribution")
        );
        for field in ["routing", "starts_before", "starts_after"] {
            assert!(!flow_sets[0].as_table().unwrap().contains_key(field));
        }
        assert_eq!(traffic["initial_delay"].as_float(), Some(0.0));
        assert_eq!(
            traffic["size"].as_integer(),
            i64::try_from(fixture.flow_bytes).ok()
        );
        assert!(traffic.get("duration").is_none());
        assert_eq!(traffic["arr_dist"]["type"].as_str(), Some("Uniform"));
        assert_eq!(traffic["arr_dist"]["low"].as_float(), Some(0.000_000_021));
        assert_eq!(traffic["arr_dist"]["high"].as_float(), Some(0.000_000_021));
        assert_eq!(traffic["pkt_size_dist"]["type"].as_str(), Some("Uniform"));
        assert_eq!(
            traffic["pkt_size_dist"]["low"].as_integer(),
            Some(PACKET_SIZE_BYTES as i64)
        );
        assert_eq!(
            traffic["pkt_size_dist"]["high"].as_integer(),
            Some(PACKET_SIZE_BYTES as i64)
        );
        assert!(fixture.flow_count > previous_flow_count);
        previous_flow_count = fixture.flow_count;
        assert!(
            (10.0..=90.0).contains(&load),
            "{} offered load {load:.6}% is outside [10%, 90%]",
            fixture.name
        );
        assert!(
            (load - fixture.expected_load_percent).abs() < 0.000_001,
            "{} offered load changed: {load:.9}%",
            fixture.name
        );
        assert!(
            (load - fixture.nominal_load_percent as f64).abs() < 3.0,
            "{} no longer represents its nominal load point",
            fixture.name
        );
    }
}

#[test]
fn width_via_load_full_flow_sets_are_nested_with_distinct_endpoints() {
    let mut previous_pairs = Vec::new();

    for fixture in FIXTURES {
        let path = fixture_path(fixture.name);
        let path = path.to_str().expect("fixture path must be UTF-8");
        let (_, hosts) = build_graph(path).expect("fixture topology must build");
        let flows = Flow::flows_from_config_with_attachments(path, &hosts);
        let pairs = flows
            .iter()
            .map(|flow| (flow.source_host, flow.sink_host))
            .collect::<Vec<_>>();

        assert_eq!(hosts.len(), HOST_COUNT as usize);
        assert_eq!(pairs.len(), fixture.flow_count as usize);
        assert_eq!(previous_pairs, pairs[..previous_pairs.len()]);
        assert_eq!(
            pairs
                .iter()
                .map(|pair| pair.0)
                .collect::<HashSet<_>>()
                .len(),
            pairs.len(),
            "{} must not reuse a source host",
            fixture.name
        );
        assert_eq!(
            pairs
                .iter()
                .map(|pair| pair.1)
                .collect::<HashSet<_>>()
                .len(),
            pairs.len(),
            "{} must not hot-spot sink hosts",
            fixture.name
        );
        assert!(
            pairs.iter().all(|(source, sink)| source != sink),
            "{} must use different source and sink hosts",
            fixture.name
        );
        previous_pairs = pairs;
    }
}

#[test]
fn smallest_width_via_load_full_fixture_lowers_and_truncates_pending_tail() {
    let fixture = FIXTURES[0];
    let path = fixture_path(fixture.name);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));

    assert_eq!(image.stop_time_ns, fixture.stop_time_ns);
    assert_eq!(image.seed, 13_032);
    assert_eq!(image.host_states.len(), HOST_COUNT as usize);
    assert_eq!(image.switch_states.len(), 40_960);
    assert_eq!(image.nodes.len(), 49_152);
    assert_eq!(image.links.len(), 49_152);
    assert_eq!(image.flows.len(), fixture.flow_count as usize);
    assert!(
        image
            .links
            .iter()
            .all(|link| link.rate_bps == HOST_RATE_BPS && link.propagation_ns == 1_000)
    );
    assert!(image.switch_states.iter().all(|switch| {
        switch.queues.len() == 1
            && switch.queues[0].scheduler == SchedulerKind::Fifo
            && switch.queues[0].queue_capacity_packets == 1_024
    }));

    let generators = image
        .host_states
        .iter()
        .flat_map(|host| &host.generators)
        .collect::<Vec<_>>();
    let generator_hosts = image
        .host_states
        .iter()
        .enumerate()
        .filter_map(|(host, state)| (!state.generators.is_empty()).then_some(host))
        .collect::<BTreeSet<_>>();
    assert_eq!(generators.len(), fixture.flow_count as usize);
    assert_eq!(generator_hosts.len(), generators.len());
    assert!(
        image
            .host_states
            .iter()
            .all(|host| host.generators.len() <= 1)
    );
    assert!(generators.iter().all(|generator| {
        matches!(
            generator.kind,
            FlowGeneratorKind::Constant(constant)
                if constant.first_departure_ns == 0
                    && constant.interval_ns == INTERVAL_NS
                    && constant.packet_size_bytes == PACKET_SIZE_BYTES
                    && constant.termination == GeneratorTermination::Bytes(fixture.flow_bytes)
        )
    }));

    let run = run_scalar_rounds(&image, None)
        .unwrap_or_else(|error| panic!("failed to execute {}: {error}", path.display()));
    let total_events = run
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();

    assert_eq!(
        run.result.summary.sourced_bytes,
        sourced_bytes_through_stop(fixture)
    );
    assert!(
        (MIN_EVENTS..=MAX_EVENTS).contains(&total_events),
        "{} processed {total_events} events, outside [{MIN_EVENTS}, {MAX_EVENTS}]",
        fixture.name
    );
    assert!(!run.result.pending_events.is_empty());
    assert!(
        run.result
            .pending_events
            .iter()
            .all(|event| event.key.time_ns > image.stop_time_ns)
    );
    assert!(!run.result.resident_packets.is_empty());
}

#[cfg(feature = "metal-spike")]
fn assert_runtime_contract(fixture: Fixture) {
    let path = fixture_path(fixture.name);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let run = run_scalar_rounds(&image, None)
        .unwrap_or_else(|error| panic!("failed to execute {}: {error}", path.display()));
    let total_events = run
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();

    assert_eq!(
        run.result.summary.sourced_bytes,
        sourced_bytes_through_stop(fixture),
        "{} sourced-byte total changed",
        fixture.name
    );
    assert!(
        (MIN_EVENTS..=MAX_EVENTS).contains(&total_events),
        "{} processed {total_events} events, outside [{MIN_EVENTS}, {MAX_EVENTS}]",
        fixture.name
    );
    assert!(
        !run.result.pending_events.is_empty(),
        "{} must retain a pending tail",
        fixture.name
    );
    assert!(
        run.result
            .pending_events
            .iter()
            .all(|event| event.key.time_ns > image.stop_time_ns),
        "{} retained an event at or before the stop time",
        fixture.name
    );
}

#[cfg(feature = "metal-spike")]
#[test]
fn width_via_load_full_load_10_holds_runtime_contract() {
    assert_runtime_contract(FIXTURES[0]);
}

#[cfg(feature = "metal-spike")]
#[test]
fn width_via_load_full_load_30_holds_runtime_contract() {
    assert_runtime_contract(FIXTURES[1]);
}

#[cfg(feature = "metal-spike")]
#[test]
fn width_via_load_full_load_50_holds_runtime_contract() {
    assert_runtime_contract(FIXTURES[2]);
}

#[cfg(feature = "metal-spike")]
#[test]
fn width_via_load_full_load_70_holds_runtime_contract() {
    assert_runtime_contract(FIXTURES[3]);
}

#[cfg(feature = "metal-spike")]
#[test]
fn width_via_load_full_load_90_holds_runtime_contract() {
    assert_runtime_contract(FIXTURES[4]);
}

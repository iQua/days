use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days::topos::build::build_graph;
use days_executor::{
    DeviceEventArenaSizing, FlowGeneratorKind, GeneratorTermination, SchedulerKind,
    size_default_device_plan,
};
use days_legacy::flows::flow::Flow;

const FIXTURE_DIRECTORY: &str = "configs/benchmarks/width_via_load_k48_h16";
const HOST_COUNT: u64 = 18_432;
const SWITCH_PORT_LPS: usize = 129_024;
const TOPOLOGY_LPS: usize = 147_456;
const HOST_RATE_BPS: u64 = 100_000_000_000;
const STOP_TIME_NS: u64 = 1_024_000;
const INTERVAL_NS: u64 = 21;
const PACKET_SIZE_BYTES: u64 = 256;
const FLOW_BYTES: u64 = 16_777_216;
const PACKETS_THROUGH_STOP: u64 = 48_762;
const CONFIGURED_PACKETS: u64 = 65_536;

#[derive(Clone, Copy)]
struct Fixture {
    name: &'static str,
    nominal_load_percent: u64,
    flow_count: u64,
    expected_source_packets: u64,
    expected_source_bytes: u128,
    expected_load_percent: f64,
}

const FIXTURES: [Fixture; 3] = [
    Fixture {
        name: "fattree_k48_h16_load_30_sustained.toml",
        nominal_load_percent: 30,
        flow_count: 5_530,
        expected_source_packets: 269_653_860,
        expected_source_bytes: 69_031_388_160,
        expected_load_percent: 29.259_316_406_25,
    },
    Fixture {
        name: "fattree_k48_h16_load_60_sustained.toml",
        nominal_load_percent: 60,
        flow_count: 11_059,
        expected_source_packets: 539_258_958,
        expected_source_bytes: 138_050_293_248,
        expected_load_percent: 58.513_341_796_875,
    },
    Fixture {
        name: "fattree_k48_h16_load_90_sustained.toml",
        nominal_load_percent: 90,
        flow_count: 16_589,
        expected_source_packets: 808_912_818,
        expected_source_bytes: 207_081_681_408,
        expected_load_percent: 87.772_658_203_125,
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

fn offered_host_load_percent(source_bytes: u128) -> f64 {
    let stop_seconds = STOP_TIME_NS as f64 / 1_000_000_000.0;
    let capacity_bytes = HOST_COUNT as f64 * HOST_RATE_BPS as f64 / 8.0 * stop_seconds;
    source_bytes as f64 / capacity_bytes * 100.0
}

fn endpoint_pairs(fixture: Fixture) -> Vec<(usize, usize)> {
    let path = fixture_path(fixture.name);
    let path = path.to_str().expect("fixture path must be UTF-8");
    let (_, hosts) = build_graph(path).expect("wide fixture topology must build");
    Flow::flows_from_config_with_attachments(path, &hosts)
        .into_iter()
        .map(|flow| (flow.source_host, flow.sink_host))
        .collect()
}

#[test]
fn k48_wide_fixtures_hold_the_budget_and_lowering_invariants() {
    assert_eq!(PACKETS_THROUGH_STOP, STOP_TIME_NS / INTERVAL_NS + 1);
    assert_eq!(CONFIGURED_PACKETS, FLOW_BYTES / PACKET_SIZE_BYTES);
    assert_eq!(CONFIGURED_PACKETS - PACKETS_THROUGH_STOP, 16_774);
    assert_eq!(PACKETS_THROUGH_STOP * INTERVAL_NS, 1_024_002);

    for fixture in FIXTURES {
        let path = fixture_path(fixture.name);
        let config = read_fixture(&path);
        let flow_sets = config["flow_set"]
            .as_array()
            .expect("fixture must contain one flow set");
        let traffic = &flow_sets[0]["traffic"];
        let source_packets = fixture.flow_count * PACKETS_THROUGH_STOP;
        let source_bytes = u128::from(source_packets * PACKET_SIZE_BYTES);
        let load = offered_host_load_percent(source_bytes);

        assert_eq!(config["seed"].as_integer(), Some(17_048));
        assert_eq!(config["duration"].as_float(), Some(0.001_024));
        assert_eq!(config["threading"].as_str(), Some("single"));
        assert_eq!(config["topology"]["category"].as_str(), Some("FatTree"));
        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(48));
        assert_eq!(
            config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(16)
        );
        assert_eq!(
            config["switch"]["port_rate"].as_integer(),
            Some(HOST_RATE_BPS as i64)
        );
        assert_eq!(config["switch"]["capacity"].as_integer(), Some(1_024));
        assert_eq!(config["switch"]["weights"][0].as_integer(), Some(1));
        assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
        assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
        assert_eq!(config["link"]["propagation_ns"].as_integer(), Some(1_000));
        assert!(!config.contains_key("flow"));
        assert_eq!(flow_sets.len(), 1);
        assert_eq!(
            flow_sets[0]["flow_count"].as_integer(),
            Some(fixture.flow_count as i64)
        );
        assert_eq!(
            flow_sets[0]["flow_type"].as_str(),
            Some("PacketDistribution")
        );
        for field in ["routing", "starts_before", "starts_after"] {
            assert!(!flow_sets[0].as_table().unwrap().contains_key(field));
        }
        assert_eq!(traffic["initial_delay"].as_float(), Some(0.0));
        assert_eq!(traffic["size"].as_integer(), Some(FLOW_BYTES as i64));
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
        assert_eq!(source_packets, fixture.expected_source_packets);
        assert_eq!(source_bytes, fixture.expected_source_bytes);
        assert!((load - fixture.expected_load_percent).abs() < 1e-12);
        assert!(
            (load - fixture.nominal_load_percent as f64).abs() < 2.5,
            "{} no longer represents its nominal load point",
            fixture.name
        );

        let image = compile_config(&path)
            .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
        assert_eq!(image.stop_time_ns, STOP_TIME_NS);
        assert_eq!(image.seed, 17_048);
        assert_eq!(image.host_states.len(), HOST_COUNT as usize);
        assert_eq!(image.switch_states.len(), SWITCH_PORT_LPS);
        assert_eq!(image.nodes.len(), TOPOLOGY_LPS);
        assert_eq!(image.links.len(), TOPOLOGY_LPS);
        assert_eq!(image.flows.len(), fixture.flow_count as usize);
        let route_lengths = image
            .flows
            .iter()
            .map(|flow| (flow.route.len(), flow.reverse_route.len()))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            route_lengths,
            BTreeSet::from([(2, 2), (4, 4), (6, 6)]),
            "{} route lengths changed",
            fixture.name
        );
        assert!(image.flows.iter().all(|flow| flow.source != flow.target));
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
        assert_eq!(generators.len(), fixture.flow_count as usize);
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
                        && constant.termination == GeneratorTermination::Bytes(FLOW_BYTES)
            )
        }));
        assert_eq!(
            image
                .flows
                .iter()
                .map(|flow| flow.source)
                .collect::<BTreeSet<_>>()
                .len(),
            fixture.flow_count as usize
        );
        assert_eq!(
            image
                .flows
                .iter()
                .map(|flow| flow.target)
                .collect::<BTreeSet<_>>()
                .len(),
            fixture.flow_count as usize
        );
    }
}

#[test]
fn k48_wide_endpoint_pairs_are_nested_and_distinct() {
    let load_30 = endpoint_pairs(FIXTURES[0]);
    let load_60 = endpoint_pairs(FIXTURES[1]);
    let load_90 = endpoint_pairs(FIXTURES[2]);

    assert_eq!(load_30, load_60[..load_30.len()]);
    assert_eq!(load_60, load_90[..load_60.len()]);
    for (fixture, pairs) in [
        (FIXTURES[0], load_30),
        (FIXTURES[1], load_60),
        (FIXTURES[2], load_90),
    ] {
        assert_eq!(pairs.len(), fixture.flow_count as usize);
        assert_eq!(
            pairs
                .iter()
                .map(|(source, _)| *source)
                .collect::<BTreeSet<_>>()
                .len(),
            pairs.len()
        );
        assert_eq!(
            pairs
                .iter()
                .map(|(_, sink)| *sink)
                .collect::<BTreeSet<_>>()
                .len(),
            pairs.len()
        );
        assert!(pairs.iter().all(|(source, sink)| source != sink));
    }
}

#[test]
fn k48_wide_annotations_cover_actuals_and_exclude_load90_execution() {
    let load_30 = fs::read_to_string(fixture_path(FIXTURES[0].name)).unwrap();
    let load_60 = fs::read_to_string(fixture_path(FIXTURES[1].name)).unwrap();
    let load_90 = fs::read_to_string(fixture_path(FIXTURES[2].name)).unwrap();

    assert!(load_30.contains("Estimated semantic work: 1.37B–1.70B transitions."));
    assert!(load_60.contains("Estimated semantic work: 2.73B–3.32B transitions."));
    assert!(load_90.contains(
        "Status: excluded-from-execution (annotation-only sizing evidence; exceeds Boston VRAM)."
    ));
    assert!(load_90.contains("Estimated semantic work: 4.10B–4.98B transitions."));
}

#[test]
fn k48_wide_load60_sizing_reproduces_retained_arenas_and_plane_total() {
    let image = compile_config(fixture_path(FIXTURES[1].name)).unwrap();
    let report = size_default_device_plan(&image).unwrap();

    // T21 fix 2 appended the per-round scratch to `stream_state`. O2.12 sizes the three stream
    // classes at 11/5/10 words, removing 122,669,440 bytes from this fixture's record plane.
    // Scratch remains image-derived so the physical-record anchor stays legible.
    let t21_round_scratch_bytes =
        days_executor::device_sizing::round_scratch_words(image.nodes.len())
            .expect("round scratch must size")
            * std::mem::size_of::<u64>();
    assert_eq!(
        report.event_arenas,
        DeviceEventArenaSizing {
            legacy_heap_event_slots: 64_396_545,
            fallback_heap_event_slots: 158_515,
            channel_stream_event_slots: 4_197_000,
            service_stream_event_slots: 294_912,
            generator_stream_event_slots: 22_118,
            heap_arena_bytes: 22_472_272,
            stream_arena_bytes: 896_615_984 + t21_round_scratch_bytes,
            legacy_heap_arena_bytes: 7_217_131_632,
        }
    );
    assert_eq!(
        report.total_device_bytes,
        report.planes.iter().map(|plane| plane.bytes).sum::<usize>()
    );
    assert_eq!(
        report.event_arenas.total_event_arena_bytes(),
        [7, 8, 25, 26]
            .into_iter()
            .map(|index| report.planes[index].bytes)
            .sum::<usize>()
    );
}

/// `nvidia-smi --query-gpu=memory.total` on boston's RTX 4090, in bytes.
const RTX_4090_TOTAL_BYTES: usize = 24_564 * 1_048_576;

/// What the plan must leave free on that card.
///
/// P12 wave 2 §2.3 measured the k48/h16 load-90 CUDA arm dying in the allocator on boston twice,
/// with a 23,983 MiB of 24,564 MiB device high-water 19 s before the panic. The dry run is a
/// *plan*, not an allocation trace (P12 wave 2 limitation 3): the CUDA context, the module image,
/// the driver reserve and the transient compaction destination buffer that actually failed are all
/// outside it. A plan that merely "fits" is therefore not a plan that runs, and this gate demands a
/// quarter of the card be left over rather than the 4.06% the plan left at `89ec7c4`.
const REQUIRED_FREE_BYTES: usize = RTX_4090_TOTAL_BYTES / 4;

#[test]
fn k48_wide_load90_default_plan_leaves_a_quarter_of_a_4090_free() {
    let image = compile_config(fixture_path(FIXTURES[2].name)).unwrap();
    let report = size_default_device_plan(&image).unwrap();

    assert!(
        report.total_device_bytes + REQUIRED_FREE_BYTES <= RTX_4090_TOTAL_BYTES,
        "k48/h16 load-90 default plan is {} B of a {RTX_4090_TOTAL_BYTES} B card, leaving \
         {} B free; the gate requires {REQUIRED_FREE_BYTES} B",
        report.total_device_bytes,
        RTX_4090_TOTAL_BYTES.saturating_sub(report.total_device_bytes),
    );

    // The mechanism the margin rests on: `size_default_device_plan` documents itself as sizing the
    // *streams-enabled* plan, and that plan never addresses an outbox slot, so the plane is one
    // record. Every outbox access on both device backends sits behind `P_STREAMS_ENABLED == 0`.
    let outbox = report
        .planes
        .iter()
        .find(|plane| plane.name == "outbox")
        .expect("outbox plane must exist");
    assert_eq!(
        outbox.bytes, 112,
        "the streams-enabled plan must carry one outbox record, not the legacy exchange arena"
    );
}

use std::fs;
use std::path::{Path, PathBuf};

use days::flows::flow::Flow;
use days::topos::build::build_graph;

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days::scenario::compile_config;
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{MetalConfig, run_metal, run_scalar};

const FIXTURE_DIRECTORY: &str = "configs/benchmarks/width_via_load_full";
const HOST_COUNT: u64 = 8_192;
const HOST_RATE_BPS: u64 = 100_000_000_000;
const INTERVAL_NS: u64 = 21;
const PACKET_SIZE_BYTES: u64 = 256;
const PROPAGATION_NS: u64 = 1_000;

#[derive(Clone, Copy)]
struct SustainedFixture {
    sustained_name: &'static str,
    short_name: &'static str,
    flow_count: u64,
    flow_bytes: u64,
    stop_time_ns: u64,
    expected_load_percent: f64,
}

const SUSTAINED_FIXTURES: [SustainedFixture; 2] = [
    SustainedFixture {
        sustained_name: "fattree_k32_load_30_sustained.toml",
        short_name: "fattree_k32_load_30.toml",
        flow_count: 2_458,
        flow_bytes: 33_554_432,
        stop_time_ns: 1_920_000,
        expected_load_percent: 29.262_041_927_083_33,
    },
    SustainedFixture {
        sustained_name: "fattree_k32_load_90_sustained.toml",
        short_name: "fattree_k32_load_90.toml",
        flow_count: 7_373,
        flow_bytes: 16_777_216,
        stop_time_ns: 1_152_000,
        expected_load_percent: 87.775_180_989_583_33,
    },
];

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(FIXTURE_DIRECTORY)
        .join(name)
}

fn read_fixture(path: &Path) -> toml::Table {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
        .parse()
        .unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn sourced_bytes_through_stop(fixture: SustainedFixture) -> u128 {
    let configured_packets = fixture.flow_bytes / PACKET_SIZE_BYTES;
    let packets_through_stop = fixture.stop_time_ns / INTERVAL_NS + 1;
    u128::from(
        fixture.flow_count * configured_packets.min(packets_through_stop) * PACKET_SIZE_BYTES,
    )
}

fn offered_host_load_percent(fixture: SustainedFixture) -> f64 {
    let stop_seconds = fixture.stop_time_ns as f64 / 1_000_000_000.0;
    let capacity_bytes = HOST_COUNT as f64 * HOST_RATE_BPS as f64 / 8.0 * stop_seconds;
    sourced_bytes_through_stop(fixture) as f64 / capacity_bytes * 100.0
}

fn endpoint_pairs(name: &str) -> Vec<(usize, usize)> {
    let path = fixture_path(name);
    let path = path.to_str().expect("fixture path must be UTF-8");
    let (_, hosts) = build_graph(path).expect("fixture topology must build");
    Flow::flows_from_config_with_attachments(path, &hosts)
        .into_iter()
        .map(|flow| (flow.source_host, flow.sink_host))
        .collect()
}

#[test]
fn sustained_fixtures_hold_the_t15e_design_invariants() {
    for fixture in SUSTAINED_FIXTURES {
        let config = read_fixture(&fixture_path(fixture.sustained_name));
        let flow_sets = config["flow_set"]
            .as_array()
            .expect("fixture must contain one flow set");
        let traffic = &flow_sets[0]["traffic"];
        let load = offered_host_load_percent(fixture);

        assert_eq!(config["seed"].as_integer(), Some(13_032));
        assert_eq!(
            config["duration"].as_float(),
            Some(fixture.stop_time_ns as f64 / 1e9)
        );
        assert_eq!(config["threading"].as_str(), Some("single"));
        assert_eq!(config["topology"]["category"].as_str(), Some("FatTree"));
        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(32));
        assert_eq!(
            config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(16)
        );
        assert_eq!(
            config["switch"]["port_rate"].as_integer(),
            Some(HOST_RATE_BPS as i64)
        );
        assert_eq!(config["switch"]["capacity"].as_integer(), Some(1_024));
        assert_eq!(config["switch"]["weights"].as_array().unwrap().len(), 1);
        assert_eq!(config["switch"]["weights"][0].as_integer(), Some(1));
        assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
        assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
        assert_eq!(
            config["link"]["propagation_ns"].as_integer(),
            Some(PROPAGATION_NS as i64)
        );
        assert!(
            !config.contains_key("flow"),
            "{} must not contain explicit [[flow]] entries",
            fixture.sustained_name
        );
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
        assert_eq!(
            traffic["size"].as_integer(),
            Some(fixture.flow_bytes as i64)
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
        assert!(
            fixture.stop_time_ns / PROPAGATION_NS >= 1_000,
            "{} must cover at least 1,000 theoretical source-active rounds",
            fixture.sustained_name
        );
        assert!(
            (load - fixture.expected_load_percent).abs() < 1e-12,
            "{} offered load changed: {load:.12}%",
            fixture.sustained_name
        );
        assert_eq!(
            endpoint_pairs(fixture.sustained_name),
            endpoint_pairs(fixture.short_name),
            "{} endpoint pairs differ from its short fixture",
            fixture.sustained_name
        );
    }
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
#[ignore = "estimated ~54s W4 plus scalar runtime for ~1.35B transitions"]
fn sustained_load_30_matches_scalar_complete_result() {
    let path = fixture_path(SUSTAINED_FIXTURES[0].sustained_name);
    let image = compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()));
    let scalar = run_scalar(&image, None)
        .unwrap_or_else(|error| panic!("scalar failed for {}: {error}", path.display()));
    let metal = run_metal(&image, None, MetalConfig::default())
        .unwrap_or_else(|error| panic!("Metal failed for {}: {error}", path.display()));

    assert_eq!(
        metal.result,
        scalar,
        "Metal result differs from scalar for {}",
        path.display()
    );
}

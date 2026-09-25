use std::fs;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{Backend, validate};

const FIXTURE: &str = "configs/benchmarks/real_image_gate/fattree_k64_f32768_st.toml";
const K48_CANONICAL_FIXTURE: &str =
    "configs/benchmarks/real_image_gate/fattree_k48_h16_f1024_st.toml";
const K64_CANONICAL_FIXTURE: &str =
    "configs/benchmarks/real_image_gate/fattree_k64_h16_f32768_st.toml";

#[test]
fn k64_real_image_gate_fixture_preserves_the_corpus_progression() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let contents = fs::read_to_string(&path).expect("T13d k64 fixture should exist");
    let config = contents
        .parse::<toml::Table>()
        .expect("T13d k64 fixture should parse");

    assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(64));
    assert_eq!(
        config["flow_set"][0]["flow_count"].as_integer(),
        Some(32_768)
    );
    assert_eq!(config["switch"]["discipline"].as_str(), Some("FIFO"));
    assert_eq!(config["switch"]["drop"].as_str(), Some("TailDrop"));
}

#[test]
fn canonical_host_wide_fixtures_preserve_the_t17b_corpus_contract() {
    for (fixture, k, flow_count) in [
        (K48_CANONICAL_FIXTURE, 48, 1_024),
        (K64_CANONICAL_FIXTURE, 64, 32_768),
    ] {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(fixture);
        let contents = fs::read_to_string(&path).expect("T17b wide fixture should exist");
        let config = contents
            .parse::<toml::Table>()
            .expect("T17b wide fixture should parse");

        assert_eq!(config["topology"]["fat_tree"]["k"].as_integer(), Some(k));
        assert_eq!(
            config["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(16)
        );
        assert_eq!(
            config["flow_set"][0]["flow_count"].as_integer(),
            Some(flow_count)
        );
    }
}

#[test]
#[ignore = "explicit canonical-host wide lowering gate"]
fn canonical_host_wide_fixtures_lower_and_validate() {
    for (fixture, nodes, flows) in [
        (K48_CANONICAL_FIXTURE, 147_456, 1_024),
        (K64_CANONICAL_FIXTURE, 327_680, 32_768),
    ] {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(fixture);
        let image = compile_config(path).expect("T17b wide fixture should lower");

        assert_eq!(image.nodes.len(), nodes);
        assert_eq!(image.links.len(), nodes);
        assert_eq!(image.flows.len(), flows);
        assert_eq!(image.initial_packets.len(), flows);
        assert_eq!(image.initial_events.len(), flows);
        validate(&image, Backend::Scalar).expect("wide image should validate for scalar");
        validate(&image, Backend::Cpu { workers: 4 }).expect("wide image should validate for CPU");
    }
}

#[test]
#[ignore = "explicit large-corpus lowering gate"]
fn k64_real_image_gate_fixture_lowers_and_validates() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
    let image = compile_config(path).expect("T13d k64 fixture should lower");

    assert_eq!(image.nodes.len(), 266_240);
    assert_eq!(image.links.len(), 266_240);
    assert_eq!(image.flows.len(), 32_768);
    assert_eq!(image.initial_packets.len(), 32_768);
    assert_eq!(image.initial_events.len(), 32_768);
    validate(&image, Backend::Scalar).expect("k64 image should validate for scalar");
    validate(&image, Backend::Cpu { workers: 4 }).expect("k64 image should validate for CPU");
}

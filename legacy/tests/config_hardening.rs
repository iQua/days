use std::io::Write;

use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use tempfile::NamedTempFile;

const BASE: &str = r#"
seed = 51001
duration = 0.0
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
flow_type = "PacketDistribution"
flow_count = 16
pairing = "SwitchOffsetHalf"
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

fn config(body: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temporary scenario");
    file.write_all(body.as_bytes()).expect("write scenario");
    file
}

#[test]
fn legacy_rejects_every_unclaimed_configuration_key_by_name() {
    let cases = [
        (
            BASE.replacen(
                "seed = 51001",
                "seed = 51001\nmandatory_experiment = true",
                1,
            ),
            "mandatory_experiment",
        ),
        (
            BASE.replacen(
                "hosts_per_edge = 2",
                "hosts_per_edge = 2\nmandatory_shape = true",
                1,
            ),
            "mandatory_shape",
        ),
        (
            BASE.replacen(
                "hosts_per_edge = 2",
                "hosts_per_edge = 2\n\n[topology.torus]\ndim = 2\nn = 2",
                1,
            ),
            "torus",
        ),
        (
            BASE.replacen("seed = 51001", "seed = 51001\nedges = [[0, 1]]", 1),
            "edges",
        ),
        (
            BASE.replacen(
                "capacity = 200",
                "capacity = 200\nmandatory_queue = true",
                1,
            ),
            "mandatory_queue",
        ),
        (
            BASE.replacen(
                "propagation_ns = 1000",
                "propagation_ns = 1000\nmandatory_link = true",
                1,
            ),
            "mandatory_link",
        ),
        (
            BASE.replacen(
                "propagation_ns = 1000",
                "propagation_tiers = { host_to_edge_ns = 1000, edge_to_aggregation_ns = 1000, aggregation_to_core_ns = 1000 }",
                1,
            ),
            "link.propagation_tiers",
        ),
        (
            BASE.replacen(
                "propagation_ns = 1000",
                "propagation_ns = 1000\npfc = { xoff = [1] }",
                1,
            ),
            "link.pfc",
        ),
        (
            BASE.replacen(
                "propagation_ns = 1000",
                "mode = \"Pfc\"\npropagation_ns = 1000",
                1,
            ),
            "link.propagation_ns",
        ),
        (
            BASE.replacen(
                "pairing = \"SwitchOffsetHalf\"",
                "pairng = \"SwitchOffsetHalf\"",
                1,
            ),
            "pairng",
        ),
        (
            BASE.replacen(
                "initial_delay = 0.0, size",
                "initial_delay = 0.0, mandatory_transport = true, size",
                1,
            ),
            "mandatory_transport",
        ),
        (
            BASE.replacen(
                "type = \"Uniform\", low",
                "type = \"Uniform\", mandatory_distribution = true, low",
                1,
            ),
            "mandatory_distribution",
        ),
        (
            BASE.replacen(
                "type = \"Uniform\", low",
                "type = \"Uniform\", lambda = 1.0, low",
                1,
            ),
            "lambda",
        ),
    ];

    for (body, key) in cases {
        let file = config(&body);
        let error = days_legacy::validate_config(file.path().to_str().unwrap())
            .expect_err("legacy must reject configuration input it does not implement");
        assert!(
            error.contains(key),
            "hard error must name unsupported key {key:?}: {error}"
        );
    }
}

#[test]
fn strict_validation_accepts_the_e1_family() {
    for load in ["10", "30", "60", "90"] {
        let path = format!(
            "{}/../configs/benchmarks/p12/e1_open_k32_load_{load}.toml",
            env!("CARGO_MANIFEST_DIR")
        );
        days_legacy::validate_config(&path)
            .unwrap_or_else(|error| panic!("E1 load {load} must validate: {error}"));
    }
}

#[cfg(feature = "l2_pfc")]
#[test]
fn strict_validation_accepts_the_legacy_pfc_fixture_when_enabled() {
    let path = format!(
        "{}/../configs/ci/leanguard_pfc.toml",
        env!("CARGO_MANIFEST_DIR")
    );
    days_legacy::validate_config(&path).expect("enabled legacy PFC fixture must validate");
}

#[cfg(all(feature = "l2_pfc", feature = "dcqcn"))]
#[test]
fn strict_validation_accepts_the_legacy_dcqcn_fixture_when_enabled() {
    let path = format!(
        "{}/../configs/ci/leanguard_dcqcn.toml",
        env!("CARGO_MANIFEST_DIR")
    );
    days_legacy::validate_config(&path).expect("enabled legacy DCQCN fixture must validate");
}

#[test]
fn cli_hard_error_names_the_unclaimed_key() {
    let body = BASE.replacen(
        "seed = 51001",
        "seed = 51001\nmandatory_experiment = true",
        1,
    );
    let file = config(&body);
    cargo_bin_cmd!("days")
        .env("RUST_LOG", "error")
        .arg(file.path())
        .assert()
        .code(1)
        .stderr(predicate::str::contains("mandatory_experiment"));
    println!("NEGATIVE_CONTROL exit=1 named_key=mandatory_experiment");
}

#[cfg(feature = "test")]
#[test]
fn e3_inertness_tripwire_has_red_capability() {
    let body = BASE.replacen(
        "seed = 51001",
        "seed = 51001\nmandatory_experiment = true",
        1,
    );
    let file = config(&body);
    cargo_bin_cmd!("days")
        .env("RUST_LOG", "error")
        .env("DAYS_E3_ASSERT_NO_UNSUPPORTED_CONFIG_INPUT", "1")
        .arg(file.path())
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "E3 entered the unsupported-configuration rejection path",
        ));
    println!(
        "E3_UNSUPPORTED_CONFIG_TRIPWIRE_RED exit=nonzero marker=unsupported-configuration-rejection"
    );
}

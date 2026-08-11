use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};

use assert_cmd::cargo::cargo_bin_cmd;
use days_legacy::flows::TomlTrafficCharacteristics;
use days_legacy::flows::collective::Collective;
use days_legacy::flows::flow::Flow;
use days_legacy::topos::build::{HostAttachments, build_graph, build_graph_with_profile};
use days_legacy::topos::topo::installed_host_attachment_state;
use days_legacy::utils::logger::CsvLogger;
use days_legacy::utils::tracing::{
    is_tracing_active, start_wall_clock_concurrency_sampler, tracing_interval,
};
use days_legacy::utils::ui::UserInterface;
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

const EXPLICIT_FLOW_BASE: &str = r#"
seed = 51002
duration = 0.0
threading = "single"
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 100_000_000_000
capacity = 200
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
routing = "ShortestPath"
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

const COLLECTIVE_BASE: &str = r#"
seed = 51003
duration = 0.0
threading = "single"
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 100_000_000_000
capacity = 200
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[[collective]]
collective_type = "Broadcast"
flow_type = "PacketDistribution"
flow_count = 1
sources = [0]
sinks = [1]
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

const FLOW_SET_BASE: &str = r#"
seed = 51004
duration = 0.0
threading = "single"
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 100_000_000_000
capacity = 200
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[[flow_set]]
flow_type = "PacketDistribution"
flow_count = 1
routing = "ShortestPath"
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

const COLLECTIVE_SET_BASE: &str = r#"
seed = 51005
duration = 0.0
threading = "single"
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 100_000_000_000
capacity = 200
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[[collective_set]]
collective_type = "Broadcast"
collective_count = 1
flow_type = "PacketDistribution"
flow_count = 1
traffic = { initial_delay = 0.0, size = 1540, arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }, pkt_size_dist = { type = "DiscreteUniform", low = 1540, high = 1540 } }
"#;

fn config(body: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temporary scenario");
    file.write_all(body.as_bytes()).expect("write scenario");
    file
}

fn silent_substitution_cases() -> Vec<(String, &'static str)> {
    vec![
        (
            EXPLICIT_FLOW_BASE.replacen(
                "routing = \"ShortestPath\"",
                "routing = \"PathFromConfig\"",
                1,
            ),
            "flow.routing",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "routing = \"ShortestPath\"",
                "routing = \"ECMP\"\npath = [0, 1]",
                1,
            ),
            "flow.routing",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen("size = 1540", "size = 1540, duration = 1.0", 1),
            "flow.traffic.size",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
                "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, tcp = { cc_algorithm = \"TCPCubic\", cubic = { beta = 0.7 } }",
                1,
            ),
            "flow.traffic.tcp",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen("weights = [1]", "weights = [999, 888]", 1),
            "switch.weights",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "weights = [1]",
                "weights = [1]\npriorities = [7, 3]",
                1,
            ),
            "switch.priorities",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "weights = [1]",
                "weights = [1]\nvticks = [7.0, 3.0]",
                1,
            ),
            "switch.vticks",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "drop = \"TailDrop\"",
                "drop = \"TailDrop\"\necn_threshold = 0.2",
                1,
            ),
            "switch.ecn_threshold",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "drop = \"TailDrop\"",
                "drop = \"ECN_THRESHOLD\"\necn_threshold = 0.0",
                1,
            ),
            "switch.ecn_threshold",
        ),
    ]
}

fn remaining_root_control_cases() -> Vec<(String, &'static str)> {
    vec![
        (
            EXPLICIT_FLOW_BASE.replacen(
                "threading = \"single\"",
                "threading = \"single\"\nhot_workers = 999",
                1,
            ),
            "hot_workers",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "threading = \"single\"",
                "threading = \"multiple\"\nnum_threads = 2\nhot_workers = 3",
                1,
            ),
            "hot_workers",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "duration = 0.0",
                "duration = 0.0\nreport_interval = nan",
                1,
            ),
            "report_interval",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "threading = \"single\"",
                "threading = \"single\"\nmailbox_capacity = 0",
                1,
            ),
            "mailbox_capacity",
        ),
    ]
}

fn hunted_substitution_cases() -> Vec<(String, &'static str)> {
    let tcp_with_cubic_on_reno = EXPLICIT_FLOW_BASE
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"TCP\"",
            1,
        )
        .replacen(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, tcp = { cc_algorithm = \"TCPReno\", cubic = { beta = 0.7 } }",
            1,
        );
    let app_source_chunk = COLLECTIVE_BASE
        .replacen(
            "hosts = [0, 1]\n\n[switch]",
            "hosts = [0, 1]\n\n[app_source]\nchunk_size = 999\n\n[switch]",
            1,
        )
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"TCP\"",
            1,
        )
        .replacen(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, tcp = { cc_algorithm = \"TCPReno\" }",
            1,
        );
    let byte_tcp_with_inactive_arrival = EXPLICIT_FLOW_BASE
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"TCP\"",
            1,
        )
        .replacen(
            "low = 1.0, high = 1.0",
            "low = 99.0, high = 99.0",
            1,
        )
        .replacen(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, tcp = { cc_algorithm = \"TCPReno\" }",
            1,
        );
    let byte_tcp_with_variable_mss = EXPLICIT_FLOW_BASE
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"TCP\"",
            1,
        )
        .replacen(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1400, high = 1540 }, tcp = { cc_algorithm = \"TCPReno\" }",
            1,
        );
    let collective_tcp_with_inactive_arrival = COLLECTIVE_BASE
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"TCP\"",
            1,
        )
        .replacen(
            "low = 1.0, high = 1.0",
            "low = 99.0, high = 99.0",
            1,
        )
        .replacen(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, tcp = { cc_algorithm = \"TCPReno\" }",
            1,
        );

    vec![
        (
            FLOW_SET_BASE.replacen(
                "routing = \"ShortestPath\"",
                "routing = \"PathFromConfig\"",
                1,
            ),
            "flow_set.routing",
        ),
        (
            COLLECTIVE_SET_BASE.replacen(
                "flow_count = 1",
                "flow_count = 1\nrouting = \"PathFromConfig\"",
                1,
            ),
            "collective_set.routing",
        ),
        (
            COLLECTIVE_SET_BASE.replacen(
                "flow_count = 1",
                "flow_count = 1\nsources = [[0]]",
                1,
            ),
            "collective_set.sources",
        ),
        (
            COLLECTIVE_BASE.replacen(
                "sinks = [1]",
                "sinks = [1]\nrouting = \"PathFromConfig\"",
                1,
            ),
            "collective.routing",
        ),
        (
            COLLECTIVE_BASE.replacen(
                "sinks = [1]",
                "sinks = [1]\nrouting = \"ECMP\"\npaths = [[0, 1]]",
                1,
            ),
            "collective.routing",
        ),
        (COLLECTIVE_BASE.replacen("sinks = [1]\n", "", 1), "collective.sources"),
        (
            COLLECTIVE_BASE.replacen(
                "sinks = [1]",
                "sinks = [1]\ngraph = [[1, 0]]",
                1,
            ),
            "collective.graph",
        ),
        (
            COLLECTIVE_BASE.replacen(
                "sinks = [1]",
                "sinks = [1]\npaths = [[1, 0]]",
                1,
            ),
            "collective.paths",
        ),
        (tcp_with_cubic_on_reno, "flow.traffic.tcp.cubic"),
        (
            byte_tcp_with_inactive_arrival,
            "flow.traffic.arr_dist",
        ),
        (
            byte_tcp_with_variable_mss,
            "flow.traffic.pkt_size_dist",
        ),
        (
            collective_tcp_with_inactive_arrival,
            "collective.traffic.arr_dist",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "low = 1540, high = 1540",
                "low = 0, high = 0",
                1,
            ),
            "flow.traffic.pkt_size_dist",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "hosts = [0, 1]\n\n[switch]",
                "hosts = [0, 1]\nmodel_host_attachment = false\n\n[link]\npropagation_ns = 1000\n\n[switch]",
                1,
            ),
            "model_host_attachment",
        ),
        (app_source_chunk, "app_source.chunk_size"),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "hosts = [0, 1]\n\n[switch]",
                "hosts = [0, 1]\n\n[app_source]\nreq_channel_capacity = 77\ninitial_delay = 99\nrun_interval = 123\n\n[switch]",
                1,
            ),
            "app_source",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "threading = \"single\"",
                "threading = \"single\"\nmailbox_capacity = 18446744073709551615",
                1,
            ),
            "mailbox_capacity",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "threading = \"single\"",
                "threading = \"multiple\"\nnum_threads = 999",
                1,
            ),
            "num_threads",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen("seed = 51002", "seed = 0", 1),
            "seed",
        ),
        (
            EXPLICIT_FLOW_BASE.replacen(
                "duration = 0.0",
                "duration = 0.0\ntracing_active = false\ntracing_interval = 0.01",
                1,
            ),
            "tracing_interval",
        ),
        (
            EXPLICIT_FLOW_BASE
                .replacen("discipline = \"FIFO\"", "discipline = \"SP\"", 1)
                .replacen("drop = \"TailDrop\"", "priorities = []\ndrop = \"TailDrop\"", 1),
            "switch.priorities",
        ),
        (
            EXPLICIT_FLOW_BASE
                .replacen(
                    "discipline = \"FIFO\"",
                    "discipline = \"VirtualClock\"",
                    1,
                )
                .replacen("drop = \"TailDrop\"", "vticks = []\ndrop = \"TailDrop\"", 1),
            "switch.vticks",
        ),
        (
            EXPLICIT_FLOW_BASE
                .replacen("discipline = \"FIFO\"", "discipline = \"SP\"", 1)
                .replacen("weights = [1]", "weights = [999, 888]\npriorities = [1]", 1),
            "switch.weights",
        ),
        (
            EXPLICIT_FLOW_BASE
                .replacen(
                    "discipline = \"FIFO\"",
                    "discipline = \"VirtualClock\"",
                    1,
                )
                .replacen("weights = [1]", "weights = [999, 888]\nvticks = [1.0]", 1),
            "switch.weights",
        ),
        (
            EXPLICIT_FLOW_BASE
                .replacen("discipline = \"FIFO\"", "discipline = \"WFQ\"", 1)
                .replacen("drop = \"TailDrop\"", "priorities = [7, 3]\ndrop = \"TailDrop\"", 1),
            "switch.priorities",
        ),
        (
            EXPLICIT_FLOW_BASE
                .replacen("discipline = \"FIFO\"", "discipline = \"WFQ\"", 1)
                .replacen("drop = \"TailDrop\"", "vticks = [7.0, 3.0]\ndrop = \"TailDrop\"", 1),
            "switch.vticks",
        ),
        (
            COLLECTIVE_BASE.replacen(
                "[switch]",
                "[routing]\npolicy = \"ShortestPath\"\n\n[switch]",
                1,
            ),
            "routing",
        ),
    ]
}

#[cfg(feature = "dcqcn")]
#[test]
fn legacy_rejects_inactive_dcqcn_arrival_distribution_by_name() {
    let body = dcqcn_body();
    let body = body.replacen("low = 1.0, high = 1.0", "low = 99.0, high = 99.0", 1);
    let file = config(&body);
    let error = days_legacy::validate_config(file.path().to_str().unwrap())
        .expect_err("legacy must reject inactive DCQCN arrival distribution");
    assert!(error.contains("flow.traffic.arr_dist"), "{error}");
}

#[cfg(feature = "dcqcn")]
#[test]
fn legacy_rejects_inactive_transport_tables() {
    let packet_distribution_with_dcqcn = EXPLICIT_FLOW_BASE.replacen(
        "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
        "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, dcqcn = { rate_gbps = 10.0, min_rate_gbps = 1.0, max_rate_gbps = 10.0, g = 0.5, ai_rate_gbps = 0.5, hai_rate_gbps = 1.0, mi_factor = 0.5 }",
        1,
    );
    let tcp_with_dcqcn = packet_distribution_with_dcqcn
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"TCP\"",
            1,
        )
        .replacen(
            "dcqcn =",
            "tcp = { cc_algorithm = \"TCPReno\" }, dcqcn =",
            1,
        );
    let dcqcn_with_tcp = dcqcn_body().replacen(
        "dcqcn =",
        "tcp = { cc_algorithm = \"TCPReno\" }, dcqcn =",
        1,
    );

    for (body, key) in [
        (packet_distribution_with_dcqcn, "flow.traffic.dcqcn"),
        (tcp_with_dcqcn, "flow.traffic.dcqcn"),
        (dcqcn_with_tcp, "flow.traffic.tcp"),
    ] {
        let file = config(&body);
        let error = days_legacy::validate_config(file.path().to_str().unwrap())
            .expect_err("legacy must reject inactive transport configuration");
        assert!(error.contains(key), "{error}");
    }
}

#[cfg(feature = "dcqcn")]
fn dcqcn_body() -> String {
    EXPLICIT_FLOW_BASE
        .replacen(
            "flow_type = \"PacketDistribution\"",
            "flow_type = \"DCQCN\"",
            1,
        )
        .replacen(
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }",
            "pkt_size_dist = { type = \"DiscreteUniform\", low = 1540, high = 1540 }, dcqcn = { rate_gbps = 10.0, min_rate_gbps = 1.0, max_rate_gbps = 10.0, g = 0.5, ai_rate_gbps = 0.5, hai_rate_gbps = 1.0, mi_factor = 0.5, rtt_ns = 100000.0, cnp_interval_ns = 10000.0, pacing_interval_ns = 1000.0, cnp_priority = 0 }",
            1,
        )
}

#[cfg(feature = "dcqcn")]
#[test]
fn legacy_rejects_invalid_dcqcn_domains_by_name() {
    let cases = [
        ("rate_gbps = 10.0", "rate_gbps = 0.0", "rate_gbps"),
        (
            "min_rate_gbps = 1.0",
            "min_rate_gbps = 11.0",
            "min_rate_gbps/rate_gbps/max_rate_gbps",
        ),
        ("g = 0.5", "g = 2.0", ".g"),
        ("ai_rate_gbps = 0.5", "ai_rate_gbps = -1.0", "ai_rate_gbps"),
        (
            "hai_rate_gbps = 1.0",
            "hai_rate_gbps = -1.0",
            "hai_rate_gbps",
        ),
        ("mi_factor = 0.5", "mi_factor = 2.0", "mi_factor"),
        ("rtt_ns = 100000.0", "rtt_ns = 0.0", "rtt_ns"),
        (
            "cnp_interval_ns = 10000.0",
            "cnp_interval_ns = -1.0",
            "cnp_interval_ns",
        ),
        (
            "pacing_interval_ns = 1000.0",
            "pacing_interval_ns = -1.0",
            "pacing_interval_ns",
        ),
        ("cnp_priority = 0", "cnp_priority = 8", "cnp_priority"),
    ];
    for (old, new, key) in cases {
        let body = dcqcn_body().replacen(old, new, 1);
        let file = config(&body);
        let error = days_legacy::validate_config(file.path().to_str().unwrap())
            .expect_err("legacy must reject invalid DCQCN domains");
        assert!(error.contains(key), "error must name {key}: {error}");
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "non-string panic".to_owned()
    }
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
fn public_traffic_view_is_strict_when_deserialized_directly() {
    let unknown_traffic = r#"
initial_delay = 0.0
size = 1000
mandatory_traffic = true
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }
"#;
    let error = toml::from_str::<TomlTrafficCharacteristics>(unknown_traffic)
        .expect_err("public traffic view must reject unknown keys");
    assert!(error.to_string().contains("mandatory_traffic"));

    let unknown_distribution = unknown_traffic.replace(
        "low = 1.0, high = 1.0",
        "low = 1.0, high = 1.0, mandatory_distribution = true",
    );
    let error = toml::from_str::<TomlTrafficCharacteristics>(&unknown_distribution)
        .expect_err("public distribution view must reject unknown keys");
    assert!(error.to_string().contains("mandatory_distribution"));
}

#[test]
fn legacy_rejects_silent_semantic_substitutions_by_name() {
    for (body, key) in silent_substitution_cases() {
        let file = config(&body);
        let error = days_legacy::validate_config(file.path().to_str().unwrap())
            .expect_err("legacy must reject unsupported or inactive combinations");
        assert!(
            error.contains(key),
            "hard error must name offending key or combination {key:?}: {error}"
        );
    }
}

#[test]
fn legacy_rejects_further_hunted_substitutions_by_name() {
    for (body, key) in hunted_substitution_cases() {
        let file = config(&body);
        let error = days_legacy::validate_config(file.path().to_str().unwrap())
            .expect_err("legacy must reject every hunted substitution");
        assert!(
            error.contains(key),
            "hard error must name hunted key or combination {key:?}: {error}"
        );
    }
}

#[test]
fn legacy_rejects_remaining_root_control_substitutions_by_name() {
    for (body, key) in remaining_root_control_cases() {
        let file = config(&body);
        let error = days_legacy::validate_config(file.path().to_str().unwrap())
            .expect_err("legacy must reject inactive, clamped, or invalid root controls");
        assert!(error.contains(key), "hard error must name {key:?}: {error}");
    }
}

#[test]
fn collective_graph_is_executable_endpoint_input() {
    let body = COLLECTIVE_BASE.replacen("sources = [0]\n", "", 1).replacen(
        "sinks = [1]",
        "graph = [[0, 1]]",
        1,
    );
    let file = config(&body);
    let collectives = Collective::collectives_from_config(file.path().to_str().unwrap(), &[0, 1]);
    assert_eq!(collectives.len(), 1);
    assert_eq!(collectives[0].sources, vec![0]);
    assert_eq!(collectives[0].sinks, vec![1]);
}

#[test]
fn cli_hard_errors_name_silent_semantic_substitutions() {
    for (body, key) in silent_substitution_cases() {
        let file = config(&body);
        cargo_bin_cmd!("days")
            .env("RUST_LOG", "error")
            .arg(file.path())
            .assert()
            .code(1)
            .stderr(predicate::str::contains(key));
        println!("SEMANTIC_NEGATIVE_CONTROL exit=1 named_key={key}");
    }
}

#[test]
fn cli_hard_errors_name_remaining_root_control_substitutions() {
    for (body, key) in remaining_root_control_cases() {
        let file = config(&body);
        cargo_bin_cmd!("days")
            .env("RUST_LOG", "error")
            .arg(file.path())
            .assert()
            .code(1)
            .stderr(predicate::str::contains(key));
        println!("ROOT_NEGATIVE_CONTROL exit=1 named_key={key}");
    }
}

#[test]
fn flows_from_config_validates_the_complete_document() {
    let body = EXPLICIT_FLOW_BASE.replacen(
        "graph = [[0, 1]]",
        "graph = [[0, 1]]\nmandatory_flow = true",
        1,
    );
    let file = config(&body);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = Flow::flows_from_config(file.path().to_str().unwrap(), &[0, 1]);
    }))
    .expect_err("public flow lowering must run strict validation");
    assert!(panic_message(panic).contains("mandatory_flow"));
}

#[test]
fn flows_from_config_with_attachments_validates_the_complete_document() {
    let body = EXPLICIT_FLOW_BASE.replacen(
        "initial_delay = 0.0, size",
        "initial_delay = 0.0, mandatory_traffic = true, size",
        1,
    );
    let file = config(&body);
    let hosts = HostAttachments::identity(vec![0, 1]).expect("identity attachments");
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = Flow::flows_from_config_with_attachments(file.path().to_str().unwrap(), &hosts);
    }))
    .expect_err("public attachment-aware flow lowering must run strict validation");
    assert!(panic_message(panic).contains("mandatory_traffic"));
}

#[test]
fn try_flows_from_config_with_attachments_validates_the_complete_document() {
    let body = EXPLICIT_FLOW_BASE.replacen(
        "seed = 51002",
        "seed = 51002\nmandatory_experiment = true",
        1,
    );
    let file = config(&body);
    let hosts = HostAttachments::identity(vec![0, 1]).expect("identity attachments");
    let error = Flow::try_flows_from_config_with_attachments(file.path().to_str().unwrap(), &hosts)
        .expect_err("public fallible flow lowering must run strict validation");
    assert!(error.contains("mandatory_experiment"));
}

#[test]
fn collectives_from_config_validates_the_complete_document() {
    let body = COLLECTIVE_BASE.replacen(
        "flow_count = 1",
        "flow_count = 1\nmandatory_collective = true",
        1,
    );
    let file = config(&body);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = Collective::collectives_from_config(file.path().to_str().unwrap(), &[0, 1]);
    }))
    .expect_err("public collective lowering must run strict validation");
    assert!(panic_message(panic).contains("mandatory_collective"));
}

#[test]
fn other_public_legacy_config_readers_validate_the_complete_document() {
    let body = EXPLICIT_FLOW_BASE.replacen(
        "seed = 51002",
        "seed = 51002\nmandatory_experiment = true",
        1,
    );
    let file = config(&body);
    let path = file.path().to_str().unwrap();
    let hosts = HostAttachments::identity(vec![0, 1]).expect("identity attachments");

    let build_error = build_graph(path).expect_err("legacy graph lowering must validate");
    assert!(build_error.to_string().contains("mandatory_experiment"));
    let profile_error =
        build_graph_with_profile(path).expect_err("profile-aware graph lowering must validate");
    assert!(profile_error.to_string().contains("mandatory_experiment"));

    let logger_error = CsvLogger::new()
        .init_from_config(path)
        .expect_err("legacy logger configuration must validate");
    assert!(logger_error.contains("mandatory_experiment"));

    for call in [
        catch_unwind(AssertUnwindSafe(|| {
            let _ = days_legacy::seed_from_config(path);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = installed_host_attachment_state(path, &hosts, &[]);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = UserInterface::new(0, path);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = is_tracing_active(path);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = tracing_interval(path);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = start_wall_clock_concurrency_sampler(path);
        })),
    ] {
        let panic = call.expect_err("public legacy config reader must validate");
        assert!(panic_message(panic).contains("mandatory_experiment"));
    }
}

#[cfg(all(feature = "l2_pfc", feature = "dcqcn"))]
#[test]
fn strict_validation_accepts_every_tracked_legacy_fixture_except_tiered_delays() {
    fn tomls_below(path: &std::path::Path, output: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).expect("fixture directory") {
            let path = entry.expect("fixture entry").path();
            if path.is_dir() {
                tomls_below(&path, output);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "toml")
            {
                output.push(path);
            }
        }
    }

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root");
    let mut fixtures = Vec::new();
    for directory in ["configs", "legacy/tests", "tests"] {
        tomls_below(&root.join(directory), &mut fixtures);
    }
    fixtures.sort();

    let mut accepted = 0;
    let mut rejected = Vec::new();
    for fixture in fixtures {
        match days_legacy::validate_config(fixture.to_str().unwrap()) {
            Ok(()) => accepted += 1,
            Err(error) => rejected.push((fixture, error)),
        }
    }
    assert_eq!(accepted, 132, "tracked accepted-fixture count changed");
    assert_eq!(
        rejected.len(),
        1,
        "unexpected fixture rejections: {rejected:#?}"
    );
    assert!(
        rejected[0]
            .0
            .ends_with("configs/benchmarks/p12/f_het_k8_tiered_delays.toml"),
        "only the known unsupported tiered-delay fixture may be rejected: {rejected:#?}"
    );
    assert!(rejected[0].1.contains("link.propagation_tiers"));
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

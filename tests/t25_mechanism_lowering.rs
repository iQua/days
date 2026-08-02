use std::collections::BTreeSet;
use std::fs;

use days::scenario::compile_config;
use days_executor::{
    Backend, CpuConfig, DropMarkPolicy, ObservationMode, SchedulerKind, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};

#[test]
fn scheduler_and_aqm_lowering_cartesian_matrix_is_explicit() {
    for discipline in ["DRR", "WRR"] {
        for drop_policy in ["TailDrop", "RED", "RED_ECN", "ECN_THRESHOLD"] {
            let path = std::env::temp_dir().join(format!(
                "days-t25-{}-{discipline}-{drop_policy}.toml",
                std::process::id()
            ));
            let config = format!(
                r#"
seed = 25
edges = [[0, 1]]
hosts = [0, 1]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
weights = [1, 2]
discipline = "{discipline}"
drop = "{drop_policy}"
ecn_threshold = 0.2

[[flow]]
flow_type = "PacketDistribution"
priority = 3
graph = [[0, 1]]

[flow.traffic]
initial_delay = 0.0
size = 1000
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "Uniform", low = 1000, high = 1000 }}
"#
            );
            fs::write(&path, config).expect("temporary T25 fixture must be writable");
            let image = compile_config(&path).unwrap_or_else(|error| {
                panic!("{discipline}/{drop_policy} lowering failed: {error}")
            });
            fs::remove_file(&path).expect("temporary T25 fixture must be removable");

            assert_eq!(image.flows[0].priority, 3);
            for queue in image.switch_states.iter().flat_map(|state| &state.queues) {
                match (discipline, &queue.scheduler) {
                    ("DRR", SchedulerKind::DeficitRoundRobin(state)) => {
                        assert_eq!(state.quanta_bytes, [1_500, 3_000]);
                    }
                    ("WRR", SchedulerKind::WeightedRoundRobin(state)) => {
                        assert_eq!(state.weights, [1, 2]);
                    }
                    _ => panic!("unexpected scheduler for {discipline}"),
                }
                match (drop_policy, queue.drop_mark) {
                    ("TailDrop", DropMarkPolicy::TailDrop) => {}
                    ("RED", DropMarkPolicy::Red(state)) => assert!(!state.mark_ecn),
                    ("RED_ECN", DropMarkPolicy::Red(state)) => assert!(state.mark_ecn),
                    ("ECN_THRESHOLD", DropMarkPolicy::EcnThreshold(state)) => {
                        assert_eq!(state.threshold, 20);
                    }
                    _ => panic!("unexpected AQM policy for {drop_policy}"),
                }
            }
        }
    }

    let path = std::env::temp_dir().join(format!("days-t25-vc-{}.toml", std::process::id()));
    fs::write(
        &path,
        r#"
seed = 25
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 8000000000
capacity = 100
discipline = "VirtualClock"
drop = "TailDrop"
"#,
    )
    .expect("temporary VC fixture must be writable");
    let error = compile_config(&path).expect_err("VC must remain outside the exact scheduler API");
    fs::remove_file(&path).expect("temporary VC fixture must be removable");
    assert_eq!(
        error.to_string(),
        "unsupported scheduler `VirtualClock`; Days executor supports FIFO, SP, WFQ, DRR, and WRR"
    );
}

#[test]
fn red_lowering_reports_the_largest_toml_integer_capacity_without_panicking() {
    let path = std::env::temp_dir().join(format!("days-t25-red-max-{}.toml", std::process::id()));
    fs::write(
        &path,
        r#"
seed = 25
edges = [[0, 1]]
hosts = [0, 1]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 9223372036854775807
discipline = "FIFO"
drop = "RED"
"#,
    )
    .expect("temporary RED boundary fixture must be writable");

    let result = std::panic::catch_unwind(|| compile_config(&path));
    fs::remove_file(&path).expect("temporary RED boundary fixture must be removable");
    let error = result
        .expect("maximum-range RED lowering must return a diagnostic instead of panicking")
        .expect_err("the derived RED counter range exceeds the executor state domain")
        .to_string();
    assert!(
        error.contains("RED worst-case signal spacing exceeds u64 counter state"),
        "expected the post-lowering representability diagnostic, got: {error}"
    );
}

#[test]
fn pfc_lowering_builds_typed_reverse_lanes_and_matches_cpu() {
    let path = std::env::temp_dir().join(format!("days-t25-pfc-{}.toml", std::process::id()));
    let config = r#"
seed = 25
edges = [[0, 1], [1, 2]]
hosts = [0, 2]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[link]
mode = "Pfc"

[link.pfc]
xoff = [0, 0, 0, 1000, 0, 0, 0, 0]
xon = [0, 0, 0, 500, 0, 0, 0, 0]
pause_quanta = [0, 0, 0, 1, 0, 0, 0, 0]
buffer_capacity = [0, 0, 0, 4000, 0, 0, 0, 0]

[[flow]]
flow_type = "PacketDistribution"
priority = 3
graph = [[0, 2]]

[flow.traffic]
initial_delay = 0.0
size = 1000
arr_dist = { type = "Uniform", low = 0.000001, high = 0.000001 }
pkt_size_dist = { type = "Uniform", low = 1000, high = 1000 }
"#;
    fs::write(&path, config).expect("temporary PFC fixture must be writable");
    let image = compile_config(&path).expect("PFC fixture must lower");
    fs::remove_file(&path).expect("temporary PFC fixture must be removable");

    let ingress_count = image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .flat_map(|queue| queue.pfc.as_ref().into_iter())
        .map(|pfc| pfc.ingresses.len())
        .sum::<usize>();
    assert_eq!(ingress_count, 2);
    let control_channels = image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .flat_map(|queue| queue.pfc.as_ref().into_iter())
        .flat_map(|pfc| &pfc.ingresses)
        .map(|ingress| ingress.control_channel_index as usize)
        .collect::<BTreeSet<_>>();
    let control_horizon_ns = control_channels
        .iter()
        .map(|index| image.channels[*index].min_delay_ns)
        .min()
        .unwrap();
    let data_horizon_ns = image
        .channels
        .iter()
        .enumerate()
        .filter(|(index, _)| !control_channels.contains(index))
        .map(|(_, channel)| channel.min_delay_ns)
        .min()
        .unwrap();
    assert_eq!(control_horizon_ns, 64);
    assert_eq!(data_horizon_ns, 1_000);
    assert_eq!(data_horizon_ns - control_horizon_ns, 936);
    validate(&image, Backend::Scalar).unwrap();
    let expected = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    for workers in [1, 2, 4] {
        validate(&image, Backend::Cpu { workers }).unwrap();
        let actual = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(actual.result, expected);
    }
}

#[test]
fn pfc_frame_bound_is_shared_by_link_and_priority_across_branches() {
    let path = std::env::temp_dir().join(format!(
        "days-t25-pfc-branch-frame-scope-{}.toml",
        std::process::id()
    ));
    let config = r#"
seed = 25
edges = [[0, 1], [1, 2], [2, 3], [2, 4]]
hosts = [0, 3, 4]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[link]
mode = "Pfc"

[link.pfc]
xoff = [0, 0, 0, 1000, 0, 0, 0, 0]
xon = [0, 0, 0, 500, 0, 0, 0, 0]
pause_quanta = [0, 0, 0, 1, 0, 0, 0, 0]
buffer_capacity = [0, 0, 0, 10000, 0, 0, 0, 0]

[[flow]]
flow_type = "PacketDistribution"
priority = 3
graph = [[0, 3]]

[flow.traffic]
initial_delay = 0.0
size = 1000
arr_dist = { type = "Uniform", low = 0.000001, high = 0.000001 }
pkt_size_dist = { type = "Uniform", low = 1000, high = 1000 }

[[flow]]
flow_type = "PacketDistribution"
priority = 3
graph = [[0, 4]]

[flow.traffic]
initial_delay = 0.0
size = 2000
arr_dist = { type = "Uniform", low = 0.000001, high = 0.000001 }
pkt_size_dist = { type = "Uniform", low = 2000, high = 2000 }
"#;
    fs::write(&path, config).expect("temporary branched PFC fixture must be writable");
    let compiled = compile_config(&path);
    fs::remove_file(&path).expect("temporary branched PFC fixture must be removable");

    let image = compiled.expect("mixed-frame branches sharing a link/priority must lower");
    validate(&image, Backend::Scalar).expect("branched PFC image must validate");
}

#[test]
fn pfc_lowering_uses_the_executor_tcp_ack_size_on_reverse_routes() {
    let path = std::env::temp_dir().join(format!(
        "days-t25-pfc-tcp-ack-frame-{}.toml",
        std::process::id()
    ));
    let config = r#"
seed = 25
edges = [[0, 1], [1, 2]]
hosts = [0, 2]
duration = 0.00001

[switch]
port_rate = 8000000000
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[link]
mode = "Pfc"

[link.pfc]
xoff = [0, 0, 0, 1000, 0, 0, 0, 0]
xon = [0, 0, 0, 500, 0, 0, 0, 0]
pause_quanta = [0, 0, 0, 1, 0, 0, 0, 0]
buffer_capacity = [0, 0, 0, 4000, 0, 0, 0, 0]

[[flow]]
flow_type = "TCP"
priority = 3
graph = [[0, 2]]

[flow.traffic]
initial_delay = 0.0
size = 1000
arr_dist = { type = "Uniform", low = 0.000001, high = 0.000001 }
pkt_size_dist = { type = "Uniform", low = 1000, high = 1000 }

[flow.traffic.tcp]
cc_algorithm = "TCPReno"
"#;
    fs::write(&path, config).expect("temporary TCP/PFC fixture must be writable");
    let image = compile_config(&path).expect("TCP/PFC fixture must lower");
    fs::remove_file(&path).expect("temporary TCP/PFC fixture must be removable");

    let reverse_links = image.flows[0]
        .reverse_route
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let reverse_bounds = image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .flat_map(|queue| queue.pfc.as_ref().into_iter())
        .flat_map(|pfc| &pfc.ingresses)
        .filter(|ingress| reverse_links.contains(&ingress.controlled_link))
        .map(|ingress| ingress.max_frame_bytes[3])
        .collect::<Vec<_>>();
    assert!(
        !reverse_bounds.is_empty(),
        "fixture must monitor a reverse TCP link"
    );
    assert!(
        reverse_bounds.iter().all(|bound| *bound == 40),
        "compiler TCP ACKs are 40 bytes, not the 64-byte PFC control size: {reverse_bounds:?}"
    );
    validate(&image, Backend::Scalar).expect("TCP/PFC image must validate");
}

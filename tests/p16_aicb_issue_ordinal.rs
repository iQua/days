//! P16 H3 (aicb), ruling C2 (design note A7): SimAI's ECMP port ordinals follow the realized
//! issue order of a workload's collectives, not the content order of their keys.
//!
//! SimAI's source port is `10000 + k` for the `k`-th message between an ordered host pair, a
//! run-time counter in send order. Days numbers a pair's messages in canonical flow order
//! (`simai_ecmp_route_tables`), which sorts collectives by key. A collective's leading
//! `issue_ordinal` (set by the AICB adapter from its realized start order; `None` for every
//! scenario that does not set it) makes that order the issue order. Here an all-to-all issued
//! first and a ring all-reduce issued second share a host pair: by key content the ring
//! (`RingAllReduce`) sorts before the all-to-all, so without the ordinal the ring takes ports
//! 10000 and 10001 and the all-to-all 10002; with it, the all-to-all takes 10000.

use std::fmt::Write as _;

use days::scenario::compile_config;
use days::topos::config::SpectrumXConfig;
use days::topos::rail::{RailProfile, RailTopology};
use days_executor::{CollectiveAlgorithm, SimulationImage, StageRole};

/// H2's miniature rail (`configs/p16/rail_mini_roce.toml`): 8 GPUs, 2 per server, 2 NICs per
/// ASW, 2 PSWs.
const BASE: &str = r#"seed = 7
duration = 0.01

[topology]
category = "SpectrumX"

[topology.spectrum_x]
gpus = 8
gpus_per_server = 2
nics_per_asw = 2
psws = 2
gpu_type = "H100"
nic_rate_bps = 100000000000
uplink_rate_bps = 400000000000
nvlink_rate_bps = 2400000000000
link_delay_ns = 500
nvlink_delay_ns = 25

[routing]
policy = "SimAiEcmp"

[switch]
capacity = 3729
discipline = "FIFO"
drop = "TailDrop"
"#;

const TRAFFIC: &str = r#"initial_delay = 0.0
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 9000, high = 9000 }

[collective.traffic.dcqcn]
max_rate_gbps = 100.0
pacing_interval_ns = 720

[collective.traffic.roce]
retransmit_timeout_ns = 0
ack_size_bytes = 60
"#;

fn rail() -> RailProfile {
    RailTopology::new(&SpectrumXConfig {
        gpus: 8,
        gpus_per_server: 2,
        nics_per_asw: 2,
        psws: 2,
        gpu_type: "H100".to_owned(),
        nic_rate_bps: 100_000_000_000,
        uplink_rate_bps: 400_000_000_000,
        nvlink_rate_bps: 2_400_000_000_000,
        link_delay_ns: 500,
        nvlink_delay_ns: 25,
    })
    .expect("mini rail")
    .profile()
}

/// A compute, an all-to-all, a compute, then a ring all-reduce, all on `[a, b]`; with
/// `ordinals`, the all-to-all and the ring carry issue ordinals 1 and 3.
fn scenario(a: u64, b: u64, ordinals: bool) -> String {
    let mut text = BASE.to_owned();
    let ordinal = |value: u64| {
        if ordinals {
            format!("issue_ordinal = {value}\n")
        } else {
            String::new()
        }
    };
    write!(
        text,
        "\n[[compute]]\nname = \"c0\"\nhosts = [{a}, {b}]\nduration_ns = 1000\n\n\
         [[collective]]\nname = \"a2a\"\nafter = \"c0\"\ncollective_type = \"AllToAll\"\n\
         flow_type = \"RoCE\"\npriority = 3\nflow_count = 2\nsources = [{a}, {b}]\n{}\n\
         [collective.traffic]\nsize = 18000\n{TRAFFIC}\n\
         [[compute]]\nname = \"c1\"\nhosts = [{a}, {b}]\nduration_ns = 100\nafter = \"a2a\"\n\n\
         [[collective]]\nname = \"ring\"\nafter = \"c1\"\ncollective_type = \"RingAllReduce\"\n\
         flow_type = \"RoCE\"\npriority = 3\nflow_count = 2\nsources = [{a}, {b}]\n\
         sinks = [{b}, {a}]\n{}\n[collective.traffic]\nsize = 36000\n{TRAFFIC}",
        ordinal(1),
        ordinal(3),
    )
    .expect("write");
    text
}

fn lower(text: &str) -> SimulationImage {
    let directory = tempfile::TempDir::new().expect("temp dir");
    let path = directory.path().join("issue-ordinal.toml");
    std::fs::write(&path, text).expect("write");
    compile_config(&path).unwrap_or_else(|error| panic!("lowers: {error}"))
}

/// The PSW index (0-based among the PSWs) each `source -> target` flow crosses, with its
/// algorithm, in canonical flow order.
fn pair_flows(
    image: &SimulationImage,
    source: u64,
    target: u64,
) -> Vec<(CollectiveAlgorithm, u32)> {
    let rail = rail();
    let mut flows = Vec::new();
    for host in &image.host_states {
        for (generator, stage) in host.generators.iter().zip(&host.stages) {
            let flow = &image.flows[generator.flow.0 as usize];
            if flow.source.0 != source || flow.target.0 != target {
                continue;
            }
            let Some(StageRole::Collective(identity)) = stage.as_ref().map(|stage| stage.role)
            else {
                continue;
            };
            let owner = image.links[flow.route[2].0 as usize].source;
            let node = image.nodes[owner.0 as usize];
            let psw = image.switch_states[node.state_slot as usize].physical_switch;
            flows.push((generator.flow.0, identity.algorithm, psw as u32 - rail.asws));
        }
    }
    flows.sort();
    flows
        .into_iter()
        .map(|(_, algorithm, psw)| (algorithm, psw))
        .collect()
}

/// The PSWs SimAI's hash gives the pair's messages when they are sent in `order`.
fn expected(
    source: u64,
    target: u64,
    order: &[CollectiveAlgorithm],
) -> Vec<(CollectiveAlgorithm, u32)> {
    let rail = rail();
    order
        .iter()
        .enumerate()
        .map(|(k, &algorithm)| {
            let sport = RailProfile::simai_sport(k as u64);
            let psw = rail
                .data_psw(source as u32, target as u32, sport)
                .expect("a cross-ASW pair crosses a PSW");
            (algorithm, psw)
        })
        .collect()
}

#[test]
fn ports_follow_the_issue_order_not_the_key_order() {
    use CollectiveAlgorithm::{AllToAll, RingAllReduce};
    let rail = rail();
    let issue = [AllToAll, RingAllReduce, RingAllReduce];
    let content = [RingAllReduce, RingAllReduce, AllToAll];
    // A cross-server, cross-ASW pair (a same-server pair is an NVLink notify) whose PSWs differ between the two orders, so the test discriminates.
    let (a, b) = (0..8_u64)
        .flat_map(|a| (0..8_u64).map(move |b| (a, b)))
        .find(|&(a, b)| {
            a != b
                && rail.server_of(a as u32) != rail.server_of(b as u32)
                && rail.asw_of(a as u32) != rail.asw_of(b as u32)
                && {
                    let by_algorithm = |flows: Vec<(CollectiveAlgorithm, u32)>| {
                        let mut flows = flows;
                        flows.sort_by_key(|(algorithm, _)| *algorithm as u8);
                        flows
                    };
                    by_algorithm(expected(a, b, &issue)) != by_algorithm(expected(a, b, &content))
                }
        })
        .expect("some pair discriminates the two orders");

    let with = lower(&scenario(a, b, true));
    let mut flows = pair_flows(&with, a, b);
    flows.sort_by_key(|(algorithm, _)| *algorithm as u8);
    let mut issued = expected(a, b, &issue);
    issued.sort_by_key(|(algorithm, _)| *algorithm as u8);
    assert_eq!(flows, issued, "{a} -> {b}: ports in issue order");

    // Without ordinals the key content decides, as before.
    let without = lower(&scenario(a, b, false));
    let mut flows = pair_flows(&without, a, b);
    flows.sort_by_key(|(algorithm, _)| *algorithm as u8);
    let mut by_content = expected(a, b, &content);
    by_content.sort_by_key(|(algorithm, _)| *algorithm as u8);
    assert_eq!(
        flows, by_content,
        "{a} -> {b}: ports in key order without ordinals"
    );
}

/// A typed workload's `issue_ordinal` lowers exactly as its TOML rendering's (the IR's
/// equivalence with its rendering, `src/scenario/workload.rs`).
#[test]
fn a_workload_issue_ordinal_lowers_as_its_toml_rendering() {
    use days::scenario::workload::{
        Algorithm, Collective, Operation, OperationKind, Transport, Workload,
    };
    let (a, b) = (0, 3);
    let traffic = TRAFFIC
        .replace("[collective.traffic.dcqcn]", "[dcqcn]")
        .replace("[collective.traffic.roce]", "[roce]");
    let collective = |algorithm, bytes, ordinal| {
        OperationKind::Collective(Collective {
            algorithm,
            bytes,
            transport: 0,
            channels: None,
            uniform_floor: algorithm == Algorithm::AllToAll,
            seeded: None,
            issue_ordinal: Some(ordinal),
        })
    };
    let operation = |after: Vec<usize>, kind| Operation {
        group: 0,
        after,
        stream: 0,
        kind,
    };
    let workload = Workload {
        groups: vec![vec![a, b]],
        transports: vec![Transport {
            flow_type: "RoCE".to_owned(),
            priority: 3,
            traffic,
        }],
        operations: vec![
            operation(vec![], OperationKind::Compute { duration_ns: 1000 }),
            operation(vec![0], collective(Algorithm::AllToAll, 18_000, 1)),
            operation(vec![1], OperationKind::Compute { duration_ns: 100 }),
            operation(vec![2], collective(Algorithm::AllReduce, 36_000, 3)),
        ],
    };
    let directory = tempfile::TempDir::new().expect("temp dir");
    let path = directory.path().join("base.toml");
    std::fs::write(&path, BASE).expect("write");
    let lowered = days::scenario::compile_config_with_workload(
        &path,
        &workload,
        days::topos::route::RouteWorkers::serial(),
    )
    .unwrap_or_else(|error| panic!("the workload lowers: {error}"));
    let rendering = scenario(a, b, true)
        .replace("name = \"c0\"", "name = \"@0\"")
        .replace("name = \"a2a\"", "name = \"@1\"")
        .replace("name = \"c1\"", "name = \"@2\"")
        .replace("name = \"ring\"", "name = \"@3\"")
        .replace("after = \"c0\"", "after = \"@0\"")
        .replace("after = \"a2a\"", "after = \"@1\"")
        .replace("after = \"c1\"", "after = \"@2\"");
    assert_eq!(lowered, lower(&rendering));
}

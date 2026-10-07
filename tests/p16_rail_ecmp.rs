//! P16 H2: SimAI's ECMP on the rail fabric (ruling H2-6).
//!
//! Golden vectors come from SimAI's own `SwitchNode::EcmpHash` and `node_id_to_ip`, copied
//! verbatim into `days-gpu/evidence/P16/railtopo-design/tooling/ecmp_golden.cc`
//! (`baseline/ecmp-golden.txt`): for each `(gpus, source, target, k)` the data hash at the source
//! ASW of the tuple with source port `10000 + k`, and the feedback hash at the target ASW of the
//! swapped tuple. SimAI lists an ASW's PSW next hops in descending node id, MEASURED from its
//! printed routing tables on the 128g and 1024g files
//! (`days-gpu/evidence/P16/railtopo-impl/psw-order/`): entry `h % 64` is PSW `63 - h % 64`.

use std::fmt::Write as _;

use days::scenario::compile_config;
use days::topos::config::SpectrumXConfig;
use days::topos::rail::{RailProfile, RailTopology, simai_ecmp_hash, simai_node_ip};
use days_executor::SimulationImage;

/// `(gpus, source, target, k, data hash, feedback hash)`.
const GOLDEN: &[(u32, u32, u32, u16, u32, u32)] = &[
    (1024, 0, 9, 0, 3950278924, 467874184),
    (1024, 0, 9, 1, 1755842688, 2259733376),
    (1024, 0, 9, 2, 1819865414, 2740804611),
    (1024, 0, 9, 3, 3457782745, 693381149),
    (1024, 0, 513, 0, 578459856, 3960334553),
    (1024, 0, 513, 1, 3715120998, 1736930209),
    (1024, 0, 513, 2, 2056675728, 4225192244),
    (1024, 0, 513, 3, 1400197738, 993105852),
    (1024, 9, 0, 0, 1494975977, 2496838089),
    (1024, 9, 0, 1, 3196724668, 876198558),
    (1024, 9, 0, 2, 2685289288, 704658636),
    (1024, 9, 0, 3, 2208572102, 1945374573),
    (1024, 7, 1016, 0, 1557688147, 2633243147),
    (1024, 7, 1016, 1, 929461182, 2790962372),
    (1024, 7, 1016, 2, 2987047995, 4030791069),
    (1024, 7, 1016, 3, 1639879367, 1798024635),
    (1024, 100, 900, 0, 3520388277, 1834755761),
    (1024, 100, 900, 1, 1623619155, 557130490),
    (1024, 100, 900, 2, 1009754595, 2289587078),
    (1024, 100, 900, 3, 2178764272, 1658569797),
    (1024, 3, 12, 0, 4292452749, 2979588119),
    (1024, 3, 12, 1, 1093025729, 3557870472),
    (1024, 3, 12, 2, 2656013939, 882937387),
    (1024, 3, 12, 3, 708719115, 2812258012),
    (1024, 127, 64, 0, 3722750749, 1431956386),
    (1024, 127, 64, 1, 3077867928, 227322909),
    (1024, 127, 64, 2, 1054969980, 3941436710),
    (1024, 127, 64, 3, 1278951646, 2418079287),
    (1024, 64, 127, 0, 2597171024, 86783306),
    (1024, 64, 127, 1, 115343177, 1875352809),
    (1024, 64, 127, 2, 2135000418, 2622897102),
    (1024, 64, 127, 3, 3975803683, 1525692481),
    (128, 0, 9, 0, 2655425682, 3762035686),
    (128, 0, 9, 1, 2448967731, 1614977834),
    (128, 0, 9, 2, 3735944903, 1967760989),
    (128, 0, 9, 3, 1711290106, 3983239944),
    (128, 9, 0, 0, 839137415, 1101479989),
    (128, 9, 0, 1, 2219942841, 4027928213),
    (128, 9, 0, 2, 3558756901, 1471625480),
    (128, 9, 0, 3, 3092788573, 2368171208),
    (128, 7, 120, 0, 3681321574, 1677785554),
    (128, 7, 120, 1, 2226373093, 647287174),
    (128, 7, 120, 2, 2944530569, 86338152),
    (128, 7, 120, 3, 2766986181, 1859202528),
    (128, 3, 12, 0, 2349761081, 3186413136),
    (128, 3, 12, 1, 477484615, 2622418208),
    (128, 3, 12, 2, 1752022174, 3545480940),
    (128, 3, 12, 3, 3925339339, 3842884969),
    (128, 127, 64, 0, 608654243, 814515796),
    (128, 127, 64, 1, 4281992872, 2613975830),
    (128, 127, 64, 2, 925456537, 3205305254),
    (128, 127, 64, 3, 4293967874, 3479918067),
    (128, 64, 127, 0, 2836138626, 3039284460),
    (128, 64, 127, 1, 3294692812, 1459481502),
    (128, 64, 127, 2, 895202445, 2364402426),
    (128, 64, 127, 3, 997097814, 2858800231),
];

fn profile(gpus: u32) -> RailProfile {
    let (nic, nvlink, gpu_type) = if gpus == 128 {
        (100, 2400, "A100")
    } else {
        (400, 2880, "H100")
    };
    RailTopology::new(&SpectrumXConfig {
        gpus: u64::from(gpus),
        gpus_per_server: 8,
        nics_per_asw: 64,
        psws: 64,
        gpu_type: gpu_type.to_owned(),
        nic_rate_bps: nic * 1_000_000_000,
        uplink_rate_bps: 400_000_000_000,
        nvlink_rate_bps: nvlink * 1_000_000_000,
        link_delay_ns: 500,
        nvlink_delay_ns: 25,
    })
    .expect("shape")
    .profile()
}

#[test]
fn murmur3_matches_simais_hash() {
    for &(gpus, source, target, k, data, feedback) in GOLDEN {
        let rail = profile(gpus);
        let sport = u32::from(RailProfile::simai_sport(u64::from(k)));
        let source_seed = rail.simai_switch_id(rail.asw_of(source));
        let target_seed = rail.simai_switch_id(rail.asw_of(target));
        assert_eq!(
            simai_ecmp_hash(
                [
                    simai_node_ip(source),
                    simai_node_ip(target),
                    sport | (100 << 16)
                ],
                source_seed
            ),
            data,
            "{gpus}g {source}->{target} k={k}"
        );
        assert_eq!(
            simai_ecmp_hash(
                [
                    simai_node_ip(target),
                    simai_node_ip(source),
                    100 | (sport << 16)
                ],
                target_seed
            ),
            feedback,
            "{gpus}g {source}->{target} k={k} feedback"
        );
        assert_eq!(
            rail.data_psw(source, target, sport as u16),
            Some(63 - data % 64)
        );
        assert_eq!(
            rail.feedback_psw(source, target, sport as u16),
            Some(63 - feedback % 64)
        );
    }
}

#[test]
fn source_ports_wrap_as_simais_counter_does() {
    assert_eq!(RailProfile::simai_sport(0), 10_000);
    assert_eq!(RailProfile::simai_sport(55_535), 65_535);
    assert_eq!(RailProfile::simai_sport(55_536), 0);
    // GPUs on one ASW (rail 0 of segment 0) cross no PSW.
    let rail = profile(1024);
    assert_eq!(rail.data_psw(0, 8, 10_000), None);
    assert_eq!(rail.feedback_psw(0, 8, 10_000), None);
}

/// The 1024g fabric with four identical TCP flows per golden pair: their duplicate ordinals are the
/// pair's message ordinals 0..3.
fn golden_scenario(routing: &str) -> String {
    let mut text = String::from(
        r#"seed = 3
duration = 0.001

[topology]
category = "SpectrumX"

[topology.spectrum_x]
gpus = 1024
gpus_per_server = 8
nics_per_asw = 64
psws = 64
gpu_type = "H100"
nic_rate_bps = 400000000000
uplink_rate_bps = 400000000000
nvlink_rate_bps = 2880000000000
link_delay_ns = 500
nvlink_delay_ns = 25

[switch]
capacity = 100
discipline = "FIFO"
drop = "TailDrop"
"#,
    );
    writeln!(text, "\n[routing]\npolicy = \"{routing}\"").expect("write");
    for &(gpus, source, target, k, _, _) in GOLDEN {
        if gpus != 1024 || k != 0 {
            continue;
        }
        for _ in 0..4 {
            writeln!(
                text,
                r#"
[[flow]]
flow_type = "TCP"
graph = [[{source}, {target}]]

[flow.traffic]
initial_delay = 0.0
size = 9000
arr_dist = {{ type = "Uniform", low = 1, high = 1 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 9000, high = 9000 }}

[flow.traffic.tcp]
cc_algorithm = "TCPReno"
"#
            )
            .expect("write");
        }
    }
    text
}

fn lower(text: &str) -> Result<SimulationImage, String> {
    let directory = tempfile::TempDir::new().expect("temp dir");
    let path = directory.path().join("ecmp.toml");
    std::fs::write(&path, text).expect("write");
    compile_config(&path).map_err(|error| error.to_string())
}

/// The Days switch index of the switch whose egress LP owns `link`.
fn switch_of(image: &SimulationImage, link: days_executor::LinkId) -> u64 {
    let owner = image.links[link.0 as usize].source;
    let node = image.nodes[owner.0 as usize];
    image.switch_states[node.state_slot as usize].physical_switch
}

#[test]
fn lowered_routes_cross_the_psw_simai_picks() {
    let image = lower(&golden_scenario("SimAiEcmp")).expect("lowers");
    let rail = profile(1024);
    let mut checked = 0;
    for &(gpus, source, target, k, data, feedback) in GOLDEN {
        if gpus != 1024 {
            continue;
        }
        // Flows are in canonical key order: one pair's four duplicates are adjacent, ordinal k.
        let flows = image
            .flows
            .iter()
            .filter(|flow| flow.source.0 == u64::from(source) && flow.target.0 == u64::from(target))
            .collect::<Vec<_>>();
        assert_eq!(flows.len(), 4, "{source}->{target}");
        let flow = flows[usize::from(k)];
        assert_eq!(flow.route.len(), 4, "a cross-ASW route has four links");
        // route = host -> ASW, ASW -> PSW, PSW -> ASW, ASW -> host: the PSW owns link 2.
        let psw = |route: &[days_executor::LinkId]| switch_of(&image, route[2]);
        assert_eq!(psw(&flow.route), u64::from(rail.asws + 63 - data % 64));
        assert_eq!(
            psw(&flow.reverse_route),
            u64::from(rail.asws + 63 - feedback % 64)
        );
        assert_eq!(
            switch_of(&image, flow.route[1]),
            u64::from(rail.asw_of(source))
        );
        assert_eq!(
            switch_of(&image, flow.reverse_route[1]),
            u64::from(rail.asw_of(target))
        );
        checked += 1;
    }
    assert_eq!(checked, 32);
}

#[test]
fn the_simai_policy_is_refused_off_the_rail_fabric() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/configs/p15/roce_ring_allreduce_lossless.toml"
    ))
    .expect("fixture")
        + "\n[routing]\npolicy = \"SimAiEcmp\"\n";
    let error = lower(&text).expect_err("refused");
    assert!(error.contains("SimAiEcmp"), "{error}");
}

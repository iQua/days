//! P16 H2: the native Spectrum-X builder reproduces SimAI's pinned topology files byte for byte,
//! and its derived quantities (server map, NVLink delay, window) follow SimAI's formulas.
//!
//! The pinned files are `days` `origin/add-workload-generator` (`9f13562`)
//! `vendor/evaluation/topo/Spectrum-X_{1024g,512g}_8gps_400Gbps_H100` and
//! `Spectrum-X_128g_8gps_100Gbps_A100` (the b4-simai specimen), written by SimAI's
//! `gen_Topo_Template.py`; only their SHA-256 digests are kept here.

use std::io::Write;

use days::topos::build::{TopologyProfile, build_graph_with_profile};
use days::topos::config::SpectrumXConfig;
use days::topos::rail::{RailTopology, ServerLocality};
use days::utils::sha256::sha256_hex;

fn spectrum_x(gpus: u64, gpu_type: &str, nic_gbps: u64, nvlink_gbps: u64) -> SpectrumXConfig {
    SpectrumXConfig {
        gpus,
        gpus_per_server: 8,
        nics_per_asw: 64,
        psws: 64,
        gpu_type: gpu_type.to_owned(),
        nic_rate_bps: nic_gbps * 1_000_000_000,
        uplink_rate_bps: 400_000_000_000,
        nvlink_rate_bps: nvlink_gbps * 1_000_000_000,
        link_delay_ns: 500,
        nvlink_delay_ns: 25,
    }
}

fn shape_1024g() -> SpectrumXConfig {
    spectrum_x(1024, "H100", 400, 2880)
}

fn shape_512g() -> SpectrumXConfig {
    spectrum_x(512, "H100", 400, 2880)
}

fn shape_128g() -> SpectrumXConfig {
    spectrum_x(128, "A100", 100, 2400)
}

#[test]
fn rendered_fabrics_equal_the_pinned_simai_files() {
    for (config, digest, bytes) in [
        (
            shape_1024g(),
            "2ea55d04c4bf510bf0d71f74745cdc7c472dd235bdea35e3df6adbebb9e36224",
            91_005,
        ),
        (
            shape_512g(),
            "7afe37bf1f19d5b87545f5ccd261b01683e7e148d69a9c25fa77f1050386c6d2",
            43_355,
        ),
        (
            shape_128g(),
            "db2114fe21ffb5092432407eac13bf91ba7cefcc7cfcd7471fd483b0bf7e2705",
            21_274,
        ),
    ] {
        let text = RailTopology::new(&config)
            .expect("valid shape")
            .render_simai();
        assert_eq!(text.len(), bytes, "{} GPUs: rendered length", config.gpus);
        assert_eq!(
            sha256_hex(text.as_bytes()),
            digest,
            "{} GPUs: rendered file differs from SimAI's",
            config.gpus
        );
    }
}

#[test]
fn rail_structure_matches_simai_numbering() {
    let rail = RailTopology::new(&shape_1024g()).expect("valid shape");
    let profile = rail.profile();
    assert_eq!(
        (profile.asws, profile.psws, profile.servers()),
        (16, 64, 128)
    );
    // GPU 0 and GPU 8 share rail 0 of segment 0; GPU 512 starts segment 1.
    assert_eq!(profile.asw_of(0), 0);
    assert_eq!(profile.asw_of(8), 0);
    assert_eq!(profile.asw_of(7), 7);
    assert_eq!(profile.asw_of(513), 9);
    // SimAI ids: ASW 0 is node 1152, PSW 0 node 1168 (the hash seeds).
    assert_eq!(profile.simai_switch_id(profile.asw_of(0)), 1152);
    assert_eq!(profile.simai_switch_id(profile.psw_index(0)), 1168);
    assert_eq!(profile.simai_switch_id(profile.psw_index(63)), 1231);

    let graph = rail.graph();
    assert_eq!(graph.node_count(), 80);
    assert_eq!(graph.edge_count(), 16 * 64);
    let hosts = rail.host_attachments().expect("attachments");
    assert_eq!(hosts.len(), 1024);
    assert_eq!(hosts.switch_for(9), Some(1));
    assert_eq!(hosts.switch_for(1023), Some(15));

    let small = RailTopology::new(&shape_128g())
        .expect("valid shape")
        .profile();
    assert_eq!((small.asws, small.psws, small.servers()), (8, 64, 16));
    assert_eq!(small.simai_switch_id(small.asw_of(0)), 144);
    assert_eq!(small.simai_switch_id(small.psw_index(0)), 152);
}

#[test]
fn rail_configuration_errors_are_refused() {
    let mut ragged = shape_1024g();
    ragged.gpus = 1032; // two segments of 512 plus a partial one
    assert!(RailTopology::new(&ragged).is_err());
    let mut uneven = shape_128g();
    uneven.gpus = 130;
    assert!(RailTopology::new(&uneven).is_err());
    let mut zero = shape_128g();
    zero.nvlink_delay_ns = 0;
    assert!(RailTopology::new(&zero).is_err());
    let mut spaced = shape_128g();
    spaced.gpu_type = "A 100".to_owned();
    assert!(RailTopology::new(&spaced).is_err());
}

#[test]
fn a_scenario_file_builds_the_rail_profile() {
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    write!(
        file,
        r#"
[topology]
category = "SpectrumX"

[topology.spectrum_x]
gpus = 128
gpus_per_server = 8
nics_per_asw = 64
psws = 64
gpu_type = "A100"
nic_rate_bps = 100000000000
uplink_rate_bps = 400000000000
nvlink_rate_bps = 2400000000000
link_delay_ns = 500
nvlink_delay_ns = 25
"#
    )
    .expect("write");
    let (graph, hosts, profile) =
        build_graph_with_profile(file.path().to_str().expect("utf-8")).expect("builds");
    assert_eq!(graph.node_count(), 72);
    assert_eq!(hosts.len(), 128);
    let TopologyProfile::Rail(rail) = profile else {
        panic!("expected the rail profile, got {profile:?}");
    };
    assert_eq!(rail, RailTopology::new(&shape_128g()).unwrap().profile());
}

#[test]
fn window_is_simai_max_bdp() {
    // MEASURED: SimAI prints `maxRtt=5800 maxBdp=72500` on the 128g file (P16 N1 runs).
    let small = ServerLocality::new(RailTopology::new(&shape_128g()).unwrap().profile());
    assert_eq!(small.max_bdp_bytes(9_000), 72_500);
    // DERIVED (simai-semantics-facts §7 item 2): 4,720 ns at 400 Gb/s.
    let large = ServerLocality::new(RailTopology::new(&shape_1024g()).unwrap().profile());
    assert_eq!(large.max_bdp_bytes(9_000), 236_000);
}

#[test]
fn nvlink_message_delay_follows_the_ruled_formula() {
    let locality = ServerLocality::new(RailTopology::new(&shape_1024g()).unwrap().profile());
    assert_eq!(locality.server_of(0), Some(0));
    assert_eq!(locality.server_of(1023), Some(127));
    assert_eq!(locality.server_of(1024), None);
    assert!(locality.same_server(0, 7));
    assert!(!locality.same_server(7, 8));
    assert!(!locality.same_server(3, 3));
    // A 2,097,152 B all-to-all chunk, alone on the port: 50 + ceil(5825.42) + 25 (one MTU).
    assert_eq!(
        locality.nvlink_message_delay_ns(2_097_152, 1, 9_000),
        Some(5_901)
    );
    // Three such chunks on the port at once (the flagship's intra-server all-to-all peers).
    assert_eq!(
        locality.nvlink_message_delay_ns(2_097_152, 3, 9_000),
        Some(17_552)
    );
    // A one-byte message: 50 + 1 + 1.
    assert_eq!(locality.nvlink_message_delay_ns(1, 1, 9_000), Some(52));
    assert_eq!(locality.nvlink_message_delay_ns(2, 0, 9_000), None);
    assert_eq!(locality.nvlink_message_delay_ns(0, 1, 9_000), None);
    // The rate is the fabric's own: on the 128g A100 file NVLink runs at 2,400 Gb/s.
    let a100 = ServerLocality::new(RailTopology::new(&shape_128g()).unwrap().profile());
    assert_eq!(
        a100.nvlink_message_delay_ns(2_097_152, 1, 9_000),
        Some(50 + 6_991 + 30)
    );
    // A TP2 AllGather of 16,777,216 B on one server: SimAI's ring has 2 channels and 1 step, each
    // message 4,194,304 B; the port carries both channels' messages: 50 + ceil(8,388,608 x 8 /
    // 2,880) + 25.
    assert_eq!(
        locality.single_server_collective_delay_ns(1, 4_194_304, 2, 9_000),
        Some(23_377)
    );
    // Its AllReduce form takes 2(n - 1) = 2 steps.
    assert_eq!(
        locality.single_server_collective_delay_ns(2, 4_194_304, 2, 9_000),
        Some(46_754)
    );
    assert_eq!(
        locality.single_server_collective_delay_ns(1, 1, 0, 9_000),
        None
    );
}

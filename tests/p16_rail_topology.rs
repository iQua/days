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
        locality.nvlink_message_delay_ns(2_097_152, 2_097_152, 9_000),
        Some(5_901)
    );
    // Three such chunks on the port at once (the flagship's intra-server all-to-all peers).
    assert_eq!(
        locality.nvlink_message_delay_ns(2_097_152, 3 * 2_097_152, 9_000),
        Some(17_552)
    );
    // A one-byte message: 50 + 1 + 1.
    assert_eq!(locality.nvlink_message_delay_ns(1, 1, 9_000), Some(52));
    assert_eq!(locality.nvlink_message_delay_ns(2, 1, 9_000), None);
    assert_eq!(locality.nvlink_message_delay_ns(0, 1, 9_000), None);
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

/// FIPS 180-4 SHA-256, enough to pin the rendered files without vendoring them.
fn sha256_hex(message: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut padded = message.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&(message.len() as u64 * 8).to_be_bytes());
    for block in padded.as_chunks::<64>().0 {
        let mut w = [0_u32; 64];
        for (index, word) in block.as_chunks::<4>().0.iter().enumerate() {
            w[index] = u32::from_be_bytes(*word);
        }
        for index in 16..64 {
            let s0 = w[index - 15].rotate_right(7)
                ^ w[index - 15].rotate_right(18)
                ^ (w[index - 15] >> 3);
            let s1 = w[index - 2].rotate_right(17)
                ^ w[index - 2].rotate_right(19)
                ^ (w[index - 2] >> 10);
            w[index] = w[index - 16]
                .wrapping_add(s0)
                .wrapping_add(w[index - 7])
                .wrapping_add(s1);
        }
        let mut v = state;
        for index in 0..64 {
            let s1 = v[4].rotate_right(6) ^ v[4].rotate_right(11) ^ v[4].rotate_right(25);
            let choice = (v[4] & v[5]) ^ (!v[4] & v[6]);
            let t1 = v[7]
                .wrapping_add(s1)
                .wrapping_add(choice)
                .wrapping_add(K[index])
                .wrapping_add(w[index]);
            let s0 = v[0].rotate_right(2) ^ v[0].rotate_right(13) ^ v[0].rotate_right(22);
            let majority = (v[0] & v[1]) ^ (v[0] & v[2]) ^ (v[1] & v[2]);
            let t2 = s0.wrapping_add(majority);
            v = [
                t1.wrapping_add(t2),
                v[0],
                v[1],
                v[2],
                v[3].wrapping_add(t1),
                v[4],
                v[5],
                v[6],
            ];
        }
        for (word, add) in state.iter_mut().zip(v) {
            *word = word.wrapping_add(add);
        }
    }
    state.iter().map(|word| format!("{word:08x}")).collect()
}

#[test]
fn sha256_matches_the_standard_vectors() {
    assert_eq!(
        sha256_hex(b""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        sha256_hex(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

//! P16 H3 (aicb): `SimAI.conf` as the single source of an AICB scenario's fabric settings
//! (ruling A3; days-gpu `evidence/P16/aicb-design.md` §4.2). The expected values are G1's profile
//! table (`evidence/P16/colldev-design.md` §6.2, F and S columns) and H2's switch rows
//! (`evidence/P16/railtopo-design.md` §4; `configs/p16/rail_mini_roce.toml` on `p16/railtopo`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use days::workload::aicb::{
    INERT_KEYS, PfcTier, RECORDED_KEYS, RailShape, SimaiConf, derive_fabric, parse_simai_conf,
};

const G: u64 = 1_000_000_000;

fn shipped() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/aicb/SimAI.conf");
    std::fs::read_to_string(path).unwrap()
}

fn conf(text: &str) -> SimaiConf {
    parse_simai_conf(text).unwrap()
}

/// `Spectrum-X_1024g_8gps_400Gbps_H100`.
const RAIL_1024G: RailShape = RailShape {
    gpus: 1024,
    gpus_per_server: 8,
    nics_per_asw: 64,
    psws: 64,
    nic_rate_bps: 400 * G,
    uplink_rate_bps: 400 * G,
    nvlink_rate_bps: 2880 * G,
    link_delay_ns: 500,
    nvlink_delay_ns: 25,
};

/// `Spectrum-X_128g_8gps_100Gbps_A100`.
const RAIL_128G: RailShape = RailShape {
    gpus: 128,
    nic_rate_bps: 100 * G,
    nvlink_rate_bps: 2400 * G,
    ..RAIL_1024G
};

/// H2's miniature rail (`configs/p16/rail_mini_roce.toml`).
const RAIL_MINI: RailShape = RailShape {
    gpus: 8,
    gpus_per_server: 2,
    nics_per_asw: 2,
    psws: 2,
    nic_rate_bps: 100 * G,
    nvlink_rate_bps: 2400 * G,
    ..RAIL_1024G
};

#[test]
fn the_shipped_conf_reads_every_key_once() {
    let conf = conf(&shipped());
    let keys: Vec<_> = conf.keys().collect();
    assert_eq!(keys.len(), 52);
    for key in [
        "CC_MODE",
        "KMAX_MAP",
        "LINK_DOWN",
        "PINT_PROB",
        "BUFFER_SIZE",
    ] {
        assert!(keys.contains(&key), "{key}");
    }
    assert!(INERT_KEYS.contains(&"FCT_OUTPUT_FILE"));
    assert!(RECORDED_KEYS.contains(&"GLOBAL_T"));
    // L2_CHUNK_SIZE is not inert in SimAI (part-1 review F2): it is read as a byte chunk for
    // extra ACKs and back-to-zero recovery, both unreachable under the required
    // L2_ACK_INTERVAL 1 and L2_BACK_TO_ZERO 0, so it is recorded, not ignored.
    assert!(!INERT_KEYS.contains(&"L2_CHUNK_SIZE"));
    assert!(RECORDED_KEYS.contains(&"L2_CHUNK_SIZE"));
}

#[test]
fn the_1024g_fabric_equals_g1_and_h2() {
    let fabric = derive_fabric(&conf(&shipped()), &RAIL_1024G, 236_000).unwrap();
    assert_eq!(fabric.mtu_bytes, 9000);
    assert_eq!(fabric.queue_capacity_packets, 3729);
    assert_eq!(fabric.ecn_by_rate, BTreeMap::from([(400 * G, 223)]));
    assert_eq!(
        fabric.pfc_asw,
        PfcTier {
            xoff_bytes: 2_928_768,
            xon_bytes: 2_925_696
        }
    );
    assert_eq!(
        fabric.pfc_psw,
        PfcTier {
            xoff_bytes: 4_036_112,
            xon_bytes: 4_033_040
        }
    );
    assert_eq!(fabric.headroom_by_rate, BTreeMap::from([(400 * G, 75_000)]));
    assert_eq!(fabric.data_priority, 3);
    let dcqcn = &fabric.dcqcn;
    assert_eq!(dcqcn.max_rate_bps, 400 * G);
    assert_eq!(dcqcn.min_rate_bps, 100_000_000);
    assert_eq!(dcqcn.ai_rate_bps, 50_000_000);
    assert_eq!(dcqcn.hai_rate_bps, 100_000_000);
    assert_eq!(dcqcn.g_literal, "0.00390625");
    assert_eq!(
        (
            dcqcn.alpha_resume_interval_ns,
            dcqcn.rate_decrease_interval_ns,
            dcqcn.rp_timer_ns
        ),
        (1_000, 4_000, 900_000)
    );
    assert_eq!(dcqcn.fast_recovery_times, 1);
    assert!(!dcqcn.clamp_target_rate);
    assert_eq!(dcqcn.pacing_interval_ns, 180);
    let roce = fabric.roce;
    assert_eq!(roce.retransmit_timeout_ns, 0);
    assert_eq!(roce.ack_every_packets, 1);
    assert_eq!(roce.nack_interval_ns, 500_000);
    assert_eq!(roce.ack_size_bytes, 60);
    assert_eq!((roce.window_bytes, roce.variable_window), (236_000, true));
    assert_eq!(roce.feedback_priority, 3);
}

#[test]
fn the_128g_fabric_equals_g1_and_h2() {
    let fabric = derive_fabric(&conf(&shipped()), &RAIL_128G, 72_500).unwrap();
    assert_eq!(
        fabric.ecn_by_rate,
        BTreeMap::from([(100 * G, 112), (400 * G, 223)])
    );
    assert_eq!(
        fabric.pfc_asw,
        PfcTier {
            xoff_bytes: 3_515_844,
            xon_bytes: 3_512_772
        }
    );
    assert_eq!(
        fabric.pfc_psw,
        PfcTier {
            xoff_bytes: 4_115_208,
            xon_bytes: 4_112_136
        }
    );
    // Days validation needs 30,574 B at 100 Gb/s, above SimAI's 18,750 (G1 §6.2, MEASURED).
    assert_eq!(
        fabric.headroom_by_rate,
        BTreeMap::from([(100 * G, 30_574), (400 * G, 75_000)])
    );
    assert_eq!(fabric.dcqcn.max_rate_bps, 100 * G);
    assert_eq!(fabric.dcqcn.pacing_interval_ns, 720);
    assert_eq!(fabric.roce.window_bytes, 72_500);
}

#[test]
fn the_mini_rail_equals_h2s_fixture() {
    let fabric = derive_fabric(&conf(&shipped()), &RAIL_MINI, 72_500).unwrap();
    assert_eq!(fabric.pfc_asw.xoff_bytes, 4_168_818);
    assert_eq!(fabric.pfc_asw.xon_bytes, 4_165_746);
    assert_eq!(fabric.pfc_psw.xoff_bytes, 4_154_756);
    assert_eq!(fabric.pfc_psw.xon_bytes, 4_151_684);
}

#[test]
fn has_win_zero_turns_the_window_off() {
    let text = shipped().replace("HAS_WIN 1", "HAS_WIN 0");
    let fabric = derive_fabric(&conf(&text), &RAIL_1024G, 236_000).unwrap();
    assert_eq!(fabric.roce.window_bytes, 0);
}

#[test]
fn reader_refusals() {
    let shipped = shipped();
    for (text, line, expected) in [
        (
            format!("{shipped}\nBOGUS 1\n"),
            Some(66),
            "unknown key `BOGUS`",
        ),
        (
            format!("{shipped}\nCC_MODE 1\n"),
            Some(66),
            "`CC_MODE` appears twice",
        ),
        (
            format!("{shipped}\nLINK_DOWN 0 0"),
            Some(66),
            "lacks 3 values",
        ),
        (
            shipped.replace("KMAX_MAP 6 25000000000 400", "KMAX_MAP x 25000000000 400"),
            None,
            "count `x` is not an integer",
        ),
        (format!("{shipped}\u{e9}"), None, "not ASCII"),
    ] {
        let error = parse_simai_conf(&text).expect_err(expected);
        assert!(
            error.message.contains(expected),
            "`{}` lacks `{expected}`",
            error.message
        );
        if line.is_some() {
            assert_eq!(error.line, line, "{expected}");
        }
    }
}

#[test]
fn fabric_refusals() {
    let shipped = shipped();
    for (from, to, expected) in [
        ("CC_MODE 1", "CC_MODE 3", "`CC_MODE 3` is not supported"),
        (
            "ENABLE_QCN 1",
            "ENABLE_QCN 0",
            "`ENABLE_QCN 0` is not supported",
        ),
        (
            "USE_DYNAMIC_PFC_THRESHOLD 1",
            "USE_DYNAMIC_PFC_THRESHOLD 0",
            "USE_DYNAMIC_PFC_THRESHOLD 0",
        ),
        (
            "L2_BACK_TO_ZERO 0",
            "L2_BACK_TO_ZERO 1",
            "L2_BACK_TO_ZERO 1",
        ),
        ("RATE_BOUND 1", "RATE_BOUND 0", "RATE_BOUND 0"),
        ("ACK_HIGH_PRIO 0", "ACK_HIGH_PRIO 1", "ACK_HIGH_PRIO 1"),
        (
            "ERROR_RATE_PER_LINK 0.0000",
            "ERROR_RATE_PER_LINK 0.001",
            "lossless links",
        ),
        ("LINK_DOWN 0 0 0", "LINK_DOWN 1000 3 4", "is not modelled"),
        (
            "RATE_AI 50Mb/s",
            "RATE_AI 50MiB/s",
            "`RATE_AI 50MiB/s` is not an exact rate",
        ),
        (
            "RATE_AI 50Mb/s",
            "RATE_AI 0.5b/s",
            "`RATE_AI 0.5b/s` is not an exact rate",
        ),
        (
            "ALPHA_RESUME_INTERVAL 1",
            "ALPHA_RESUME_INTERVAL 0.0005",
            "not a whole number of nanoseconds",
        ),
        (
            "EWMA_GAIN 0.00390625",
            "EWMA_GAIN 1.5",
            "not a decimal in [0, 1]",
        ),
        ("HAS_WIN 1", "HAS_WIN 2", "must be 0 or 1"),
        // L2_ACK_INTERVAL is bytes in SimAI (rdma-hw.cc:586-595), not packets.
        (
            "L2_ACK_INTERVAL 1",
            "L2_ACK_INTERVAL 4000",
            "`L2_ACK_INTERVAL 4000` is not supported (SimAI reads it in bytes",
        ),
        (
            "L2_ACK_INTERVAL 1",
            "L2_ACK_INTERVAL 0",
            "`L2_ACK_INTERVAL 0` is not supported",
        ),
        ("L2_CHUNK_SIZE 4000", "L2_CHUNK_SIZE 0", "`L2_CHUNK_SIZE 0`"),
        (
            "L2_CHUNK_SIZE 4000",
            "L2_CHUNK_SIZE 4k",
            "`L2_CHUNK_SIZE 4k`",
        ),
        (
            "PACKET_PAYLOAD_SIZE 9000",
            "PACKET_PAYLOAD_SIZE 0",
            "must be positive",
        ),
        ("BUFFER_SIZE 32", "BUFFER_SIZE 1", "exceed BUFFER_SIZE"),
        ("RP_TIMER 900", "", "`RP_TIMER` is missing"),
    ] {
        assert!(shipped.contains(from), "{from}");
        let text = shipped.replace(from, to);
        let error = derive_fabric(&conf(&text), &RAIL_1024G, 236_000).expect_err(expected);
        assert!(
            error.message.contains(expected),
            "`{}` lacks `{expected}`",
            error.message
        );
    }
    // The 128g fabric's 100 Gb/s NIC links need ECN rows at 100 Gb/s, as SimAI asserts.
    let text = shipped
        .replace(" 100000000000 1600", "")
        .replace("KMAX_MAP 6", "KMAX_MAP 5");
    let error = derive_fabric(&conf(&text), &RAIL_128G, 72_500).unwrap_err();
    assert!(
        error
            .message
            .contains("`KMAX_MAP` has no entry for 100000000000 b/s"),
        "{error}"
    );
    assert!(derive_fabric(&conf(&text), &RAIL_1024G, 236_000).is_ok());
}

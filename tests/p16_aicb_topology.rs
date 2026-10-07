//! P16 H3 (aicb): SimAI topology file names onto H2's `[topology.spectrum_x]` (design note §4.1).
//!
//! The adapter knows the three SimAI files H2 pinned (`days-gpu/evidence/P16/railtopo-design/
//! baseline/pinned/`) by name, with their parameters and sha256. A scenario that names one must
//! declare exactly those parameters, and H2's renderer must reproduce the file byte for byte.

use days::topos::config::SpectrumXConfig;
use days::workload::aicb::{RailShape, check_simai_topology, simai_topology};

fn config(gpus: u64, gpu_type: &str, nic_gbps: u64, nvlink_gbps: u64) -> SpectrumXConfig {
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

#[test]
fn the_three_pinned_files_map_and_render_exactly() {
    for (name, expected, digest) in [
        (
            "Spectrum-X_1024g_8gps_400Gbps_H100",
            config(1024, "H100", 400, 2880),
            "2ea55d04c4bf510bf0d71f74745cdc7c472dd235bdea35e3df6adbebb9e36224",
        ),
        (
            "Spectrum-X_512g_8gps_400Gbps_H100",
            config(512, "H100", 400, 2880),
            "7afe37bf1f19d5b87545f5ccd261b01683e7e148d69a9c25fa77f1050386c6d2",
        ),
        (
            "Spectrum-X_128g_8gps_100Gbps_A100",
            config(128, "A100", 100, 2400),
            "db2114fe21ffb5092432407eac13bf91ba7cefcc7cfcd7471fd483b0bf7e2705",
        ),
    ] {
        let (config, sha256) = simai_topology(name).unwrap_or_else(|| panic!("{name} is known"));
        assert_eq!(config, expected, "{name}");
        assert_eq!(sha256, digest, "{name}");
        let checked =
            check_simai_topology(name, &expected).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(checked, digest, "{name}: the rendered file's sha256");
    }
}

#[test]
fn a_mismatch_or_an_unknown_name_is_refused() {
    let error = check_simai_topology(
        "Spectrum-X_64g_8gps_400Gbps_H100",
        &config(64, "H100", 400, 2880),
    )
    .unwrap_err();
    assert!(error.message.contains("unknown SimAI topology"), "{error}");
    let mut wrong = config(128, "A100", 100, 2400);
    wrong.nvlink_rate_bps = 2_880_000_000_000;
    let error = check_simai_topology("Spectrum-X_128g_8gps_100Gbps_A100", &wrong).unwrap_err();
    assert!(error.message.contains("nvlink_rate_bps"), "{error}");
}

#[test]
fn a_rail_shape_comes_from_the_topology_table() {
    let shape = RailShape::from(&config(128, "A100", 100, 2400));
    assert_eq!(shape.gpus, 128);
    assert_eq!(shape.nic_rate_bps, 100_000_000_000);
    assert_eq!(shape.nvlink_rate_bps, 2_400_000_000_000);
    assert_eq!(shape.link_delay_ns, 500);
}

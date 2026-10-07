//! SimAI topology file names onto H2's `[topology.spectrum_x]` (design note §4.1).
//!
//! SimAI's `gen_Topo_Template.py` writes each Spectrum-X file from a handful of parameters; H2's
//! renderer reproduces the three files the P16 runs use byte for byte. A scenario that names a
//! SimAI file must declare exactly its parameters, and the rendering of what it declares must
//! hash to the pinned file's sha256, so the SimAI run and the Days run share one fabric.

use crate::topos::config::SpectrumXConfig;
use crate::topos::rail::RailTopology;
use crate::utils::sha256::sha256_hex;

use super::{AicbError, RailShape};

/// `(name, gpus, gpu_type, NIC Gb/s, NVLink Gb/s, sha256)` of the pinned SimAI files
/// (`days-gpu/evidence/P16/railtopo-design/baseline/pinned/`); every one has 8 GPUs per server,
/// 64 NICs per ASW, 64 PSWs, 400 Gb/s uplinks, 500 ns links and 25 ns NVLink.
const PINNED: [(&str, u64, &str, u64, u64, &str); 3] = [
    (
        "Spectrum-X_1024g_8gps_400Gbps_H100",
        1024,
        "H100",
        400,
        2880,
        "2ea55d04c4bf510bf0d71f74745cdc7c472dd235bdea35e3df6adbebb9e36224",
    ),
    (
        "Spectrum-X_512g_8gps_400Gbps_H100",
        512,
        "H100",
        400,
        2880,
        "7afe37bf1f19d5b87545f5ccd261b01683e7e148d69a9c25fa77f1050386c6d2",
    ),
    (
        "Spectrum-X_128g_8gps_100Gbps_A100",
        128,
        "A100",
        100,
        2400,
        "db2114fe21ffb5092432407eac13bf91ba7cefcc7cfcd7471fd483b0bf7e2705",
    ),
];

const GBPS: u64 = 1_000_000_000;

/// The parameters and sha256 of a pinned SimAI topology file, by name.
pub fn simai_topology(name: &str) -> Option<(SpectrumXConfig, &'static str)> {
    PINNED
        .iter()
        .find(|entry| entry.0 == name)
        .map(|&(_, gpus, gpu_type, nic, nvlink, sha256)| {
            (
                SpectrumXConfig {
                    gpus,
                    gpus_per_server: 8,
                    nics_per_asw: 64,
                    psws: 64,
                    gpu_type: gpu_type.to_owned(),
                    nic_rate_bps: nic * GBPS,
                    uplink_rate_bps: 400 * GBPS,
                    nvlink_rate_bps: nvlink * GBPS,
                    link_delay_ns: 500,
                    nvlink_delay_ns: 25,
                },
                sha256,
            )
        })
}

/// Checks that `config` is the named SimAI file's fabric and that H2's renderer reproduces the
/// file; returns the file's sha256.
pub fn check_simai_topology(
    name: &str,
    config: &SpectrumXConfig,
) -> Result<&'static str, AicbError> {
    let (expected, sha256) = simai_topology(name).ok_or_else(|| {
        AicbError::new(format!(
            "unknown SimAI topology `{name}`; Days knows {}",
            PINNED.map(|entry| entry.0).join(", ")
        ))
    })?;
    for (field, declared, pinned) in [
        ("gpus", config.gpus, expected.gpus),
        (
            "gpus_per_server",
            config.gpus_per_server,
            expected.gpus_per_server,
        ),
        ("nics_per_asw", config.nics_per_asw, expected.nics_per_asw),
        ("psws", config.psws, expected.psws),
        ("nic_rate_bps", config.nic_rate_bps, expected.nic_rate_bps),
        (
            "uplink_rate_bps",
            config.uplink_rate_bps,
            expected.uplink_rate_bps,
        ),
        (
            "nvlink_rate_bps",
            config.nvlink_rate_bps,
            expected.nvlink_rate_bps,
        ),
        (
            "link_delay_ns",
            config.link_delay_ns,
            expected.link_delay_ns,
        ),
        (
            "nvlink_delay_ns",
            config.nvlink_delay_ns,
            expected.nvlink_delay_ns,
        ),
    ] {
        if declared != pinned {
            return Err(AicbError::new(format!(
                "[topology.spectrum_x] {field} = {declared}, but SimAI's `{name}` has {pinned}"
            )));
        }
    }
    if config.gpu_type != expected.gpu_type {
        return Err(AicbError::new(format!(
            "[topology.spectrum_x] gpu_type = {:?}, but SimAI's `{name}` has {:?}",
            config.gpu_type, expected.gpu_type
        )));
    }
    let rendered = RailTopology::new(config)
        .map_err(|error| AicbError::new(format!("the rail fabric: {error}")))?
        .render_simai();
    let digest = sha256_hex(rendered.as_bytes());
    if digest != sha256 {
        return Err(AicbError::new(format!(
            "the rendered `{name}` has sha256 {digest}, not SimAI's {sha256}"
        )));
    }
    Ok(sha256)
}

impl From<&SpectrumXConfig> for RailShape {
    fn from(config: &SpectrumXConfig) -> Self {
        RailShape {
            gpus: config.gpus,
            gpus_per_server: config.gpus_per_server,
            nics_per_asw: config.nics_per_asw,
            psws: config.psws,
            nic_rate_bps: config.nic_rate_bps,
            uplink_rate_bps: config.uplink_rate_bps,
            nvlink_rate_bps: config.nvlink_rate_bps,
            link_delay_ns: config.link_delay_ns,
            nvlink_delay_ns: config.nvlink_delay_ns,
        }
    }
}

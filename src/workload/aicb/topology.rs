//! SimAI topology file names onto H2's `[topology.spectrum_x]` (design note §4.1).

use crate::topos::config::SpectrumXConfig;

use super::{AicbError, RailShape};

/// The parameters and sha256 of a pinned SimAI topology file, by name.
pub fn simai_topology(_name: &str) -> Option<(SpectrumXConfig, &'static str)> {
    None
}

/// Checks that `config` is the named SimAI file's fabric; returns the rendered file's sha256.
pub fn check_simai_topology(
    _name: &str,
    _config: &SpectrumXConfig,
) -> Result<&'static str, AicbError> {
    Err(AicbError::new("not implemented"))
}

impl From<&SpectrumXConfig> for RailShape {
    fn from(_config: &SpectrumXConfig) -> Self {
        RailShape {
            gpus: 0,
            gpus_per_server: 0,
            nics_per_asw: 0,
            psws: 0,
            nic_rate_bps: 0,
            uplink_rate_bps: 0,
            nvlink_rate_bps: 0,
            link_delay_ns: 0,
            nvlink_delay_ns: 0,
        }
    }
}

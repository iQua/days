#![allow(dead_code)] // RED skeleton: the reader is not implemented yet.
//! `SimAI.conf`, the single source of an AICB scenario's fabric settings (ruling A3; design note
//! §4.2).
//!
//! The reader is strict where SimAI's (`network_frontend/ns3/common.h:459-662`, a `conf >> key`
//! loop) is lenient: every key must be one SimAI reads, appear once, and carry exactly its
//! values, so a typo cannot shift the rest of the file. [`derive_fabric`] turns the keys Days
//! models into the rail fabric's switch rows (H2 §4), DCQCN and RoCE settings, with SimAI's own
//! integer formulas (`common.h:840-890`), and refuses settings Days does not model.

use std::collections::BTreeMap;

use super::AicbError;

/// How many values a key takes.
#[derive(Clone, Copy)]
enum Arity {
    One,
    Three,
    /// A count, then that many `(rate, value)` pairs.
    RateMap,
}

/// Every key SimAI's reader knows (`common.h:469-661`).
const KEYS: &[(&str, Arity)] = &[
    ("ENABLE_QCN", Arity::One),
    ("USE_DYNAMIC_PFC_THRESHOLD", Arity::One),
    ("CLAMP_TARGET_RATE", Arity::One),
    ("PAUSE_TIME", Arity::One),
    ("DATA_RATE", Arity::One),
    ("LINK_DELAY", Arity::One),
    ("PACKET_PAYLOAD_SIZE", Arity::One),
    ("L2_CHUNK_SIZE", Arity::One),
    ("L2_ACK_INTERVAL", Arity::One),
    ("L2_BACK_TO_ZERO", Arity::One),
    ("FLOW_FILE", Arity::One),
    ("TRACE_FILE", Arity::One),
    ("TRACE_OUTPUT_FILE", Arity::One),
    ("SIMULATOR_STOP_TIME", Arity::One),
    ("ALPHA_RESUME_INTERVAL", Arity::One),
    ("RP_TIMER", Arity::One),
    ("EWMA_GAIN", Arity::One),
    ("FAST_RECOVERY_TIMES", Arity::One),
    ("RATE_AI", Arity::One),
    ("RATE_HAI", Arity::One),
    ("ERROR_RATE_PER_LINK", Arity::One),
    ("CC_MODE", Arity::One),
    ("RATE_DECREASE_INTERVAL", Arity::One),
    ("MIN_RATE", Arity::One),
    ("FCT_OUTPUT_FILE", Arity::One),
    ("HAS_WIN", Arity::One),
    ("GLOBAL_T", Arity::One),
    ("MI_THRESH", Arity::One),
    ("VAR_WIN", Arity::One),
    ("FAST_REACT", Arity::One),
    ("U_TARGET", Arity::One),
    ("INT_MULTI", Arity::One),
    ("RATE_BOUND", Arity::One),
    ("ACK_HIGH_PRIO", Arity::One),
    ("DCTCP_RATE_AI", Arity::One),
    ("NIC_TOTAL_PAUSE_TIME", Arity::One),
    ("PFC_OUTPUT_FILE", Arity::One),
    ("LINK_DOWN", Arity::Three),
    ("ENABLE_TRACE", Arity::One),
    ("KMAX_MAP", Arity::RateMap),
    ("KMIN_MAP", Arity::RateMap),
    ("PMAX_MAP", Arity::RateMap),
    ("BUFFER_SIZE", Arity::One),
    ("QLEN_MON_FILE", Arity::One),
    ("BW_MON_FILE", Arity::One),
    ("RATE_MON_FILE", Arity::One),
    ("CNP_MON_FILE", Arity::One),
    ("MON_START", Arity::One),
    ("MON_END", Arity::One),
    ("QP_MON_INTERVAL", Arity::One),
    ("BW_MON_INTERVAL", Arity::One),
    ("QLEN_MON_INTERVAL", Arity::One),
    ("MULTI_RATE", Arity::One),
    ("SAMPLE_FEEDBACK", Arity::One),
    ("PINT_LOG_BASE", Arity::One),
    ("PINT_PROB", Arity::One),
];

/// Keys whose values do not reach anything Days models under `CC_MODE 1`, or that only name
/// SimAI's outputs. Recorded in the manifest; their values are not read.
pub const INERT_KEYS: &[&str] = &[
    "FLOW_FILE",
    "TRACE_FILE",
    "TRACE_OUTPUT_FILE",
    "FCT_OUTPUT_FILE",
    "PFC_OUTPUT_FILE",
    "QLEN_MON_FILE",
    "BW_MON_FILE",
    "RATE_MON_FILE",
    "CNP_MON_FILE",
    "MON_START",
    "MON_END",
    "QP_MON_INTERVAL",
    "BW_MON_INTERVAL",
    "QLEN_MON_INTERVAL",
    "ENABLE_TRACE",
    "SIMULATOR_STOP_TIME",
    "L2_CHUNK_SIZE",
    "FAST_REACT",
    "U_TARGET",
    "MI_THRESH",
    "INT_MULTI",
    "MULTI_RATE",
    "SAMPLE_FEEDBACK",
    "PINT_LOG_BASE",
    "PINT_PROB",
    "DCTCP_RATE_AI",
];

/// Keys that SimAI reads but whose effect Days does not reproduce; present only as recorded
/// divergences (`GLOBAL_T` is forced to 1 by SimAI itself, `common.h:565-567`; `PAUSE_TIME` is
/// Days' edge-triggered XOFF/XON; `DATA_RATE` and `LINK_DELAY` are unused with a topology file;
/// `NIC_TOTAL_PAUSE_TIME` is a monitor).
pub const RECORDED_KEYS: &[&str] = &[
    "GLOBAL_T",
    "PAUSE_TIME",
    "DATA_RATE",
    "LINK_DELAY",
    "NIC_TOTAL_PAUSE_TIME",
];

/// A parsed `SimAI.conf`: each key once, with its raw tokens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimaiConf {
    entries: BTreeMap<&'static str, (usize, Vec<String>)>,
}

impl SimaiConf {
    /// The keys present, in name order.
    pub fn keys(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.entries.keys().copied()
    }

    fn raw(&self, key: &'static str) -> Option<(usize, &[String])> {
        self.entries
            .get(key)
            .map(|(line, values)| (*line, values.as_slice()))
    }

    fn required(&self, key: &'static str) -> Result<(usize, &str), AicbError> {
        let (line, values) = self
            .raw(key)
            .ok_or_else(|| AicbError::new(format!("SimAI.conf: `{key}` is missing")))?;
        Ok((line, values[0].as_str()))
    }

    fn integer(&self, key: &'static str) -> Result<u64, AicbError> {
        let (line, value) = self.required(key)?;
        integer(value).ok_or_else(|| {
            AicbError::at(
                line,
                format!("SimAI.conf: `{key} {value}` is not an exact non-negative integer"),
            )
        })
    }

    fn flag(&self, key: &'static str) -> Result<bool, AicbError> {
        match self.integer(key)? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(AicbError::new(format!(
                "SimAI.conf: `{key} {other}` must be 0 or 1"
            ))),
        }
    }

    fn require_value(&self, key: &'static str, expected: u64, why: &str) -> Result<(), AicbError> {
        let value = self.integer(key)?;
        if value != expected {
            return Err(AicbError::new(format!(
                "SimAI.conf: `{key} {value}` is not supported ({why}); Days models {key} {expected}"
            )));
        }
        Ok(())
    }

    /// A decimal number of microseconds, exactly in nanoseconds.
    fn microseconds(&self, key: &'static str) -> Result<u64, AicbError> {
        let (line, value) = self.required(key)?;
        scaled_decimal(value, 3).ok_or_else(|| {
            AicbError::at(
                line,
                format!("SimAI.conf: `{key} {value}` is not a whole number of nanoseconds"),
            )
        })
    }

    /// An ns-3 `DataRate` string, exactly in bits per second.
    fn rate(&self, key: &'static str) -> Result<u64, AicbError> {
        let (line, value) = self.required(key)?;
        data_rate_bps(value).ok_or_else(|| {
            AicbError::at(
                line,
                format!(
                    "SimAI.conf: `{key} {value}` is not an exact rate in bps, Kbps, Mbps or Gbps \
                     (or b/s, Kb/s, Mb/s, Gb/s)"
                ),
            )
        })
    }

    /// A rate-keyed map (`KMIN_MAP`, `KMAX_MAP`, `PMAX_MAP`) as raw values.
    fn rate_map(&self, key: &'static str) -> Result<(usize, BTreeMap<u64, &str>), AicbError> {
        let (line, values) = self
            .raw(key)
            .ok_or_else(|| AicbError::new(format!("SimAI.conf: `{key}` is missing")))?;
        let mut map = BTreeMap::new();
        for pair in values[1..].chunks_exact(2) {
            let rate = integer(&pair[0]).ok_or_else(|| {
                AicbError::at(
                    line,
                    format!("SimAI.conf: `{key}` rate `{}` is not an integer", pair[0]),
                )
            })?;
            if map.insert(rate, pair[1].as_str()).is_some() {
                return Err(AicbError::at(
                    line,
                    format!("SimAI.conf: `{key}` lists rate {rate} twice"),
                ));
            }
        }
        Ok((line, map))
    }
}

/// Parses `SimAI.conf` strictly: known keys only, each once, each with exactly its values.
pub fn parse_simai_conf(_text: &str) -> Result<SimaiConf, AicbError> {
    Err(AicbError::new("the SimAI.conf reader is not implemented"))
}

/// The rail fabric's shape, as H2's `[topology.spectrum_x]` declares it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RailShape {
    pub gpus: u64,
    pub gpus_per_server: u64,
    pub nics_per_asw: u64,
    pub psws: u64,
    pub nic_rate_bps: u64,
    pub uplink_rate_bps: u64,
    pub nvlink_rate_bps: u64,
    pub link_delay_ns: u64,
    pub nvlink_delay_ns: u64,
}

/// XOFF and XON of one switch tier's lossless class (static thresholds at an empty pool, H2-7).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PfcTier {
    pub xoff_bytes: u64,
    pub xon_bytes: u64,
}

/// The DCQCN settings of the scenario's queue pairs (the P16 Mellanox form).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimaiDcqcn {
    pub max_rate_bps: u64,
    pub min_rate_bps: u64,
    pub ai_rate_bps: u64,
    pub hai_rate_bps: u64,
    /// `EWMA_GAIN` as written; lowering converts it to Q63 exactly.
    pub g_literal: String,
    pub alpha_resume_interval_ns: u64,
    pub rate_decrease_interval_ns: u64,
    pub rp_timer_ns: u64,
    pub fast_recovery_times: u32,
    pub clamp_target_rate: bool,
    /// One MTU at the NIC rate, rounded up (D14's credit pacer).
    pub pacing_interval_ns: u64,
}

/// The queue pairs' Go-back-N settings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimaiRoce {
    /// SimAI has no retransmission timeout (P15 C3).
    pub retransmit_timeout_ns: u64,
    pub ack_every_packets: u64,
    /// ns-3's `RdmaHw` default; `SimAI.conf` does not set it.
    pub nack_interval_ns: u64,
    pub ack_size_bytes: u64,
    /// `maxBdp` with `HAS_WIN 1`, else 0.
    pub window_bytes: u64,
    pub variable_window: bool,
    /// ACKs and NACKs ride the data class (D16; SimAI's switch never applies `ACK_HIGH_PRIO`).
    pub feedback_priority: u8,
}

/// Every fabric setting an AICB scenario takes from `SimAI.conf`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimaiFabric {
    /// `PACKET_PAYLOAD_SIZE`: the constant packet size.
    pub mtu_bytes: u64,
    /// `ceil(BUFFER_SIZE MiB / MTU)` packets per switch egress queue.
    pub queue_capacity_packets: u64,
    /// ECN step threshold in packets per egress link rate, at the K-ramp midpoint.
    pub ecn_by_rate: BTreeMap<u64, u64>,
    pub pfc_asw: PfcTier,
    pub pfc_psw: PfcTier,
    /// PFC headroom per controlled-link rate: SimAI's, or Days validation's minimum if larger.
    pub headroom_by_rate: BTreeMap<u64, u64>,
    /// The lossless class SimAI's data uses (`pg = 3`).
    pub data_priority: u8,
    pub dcqcn: SimaiDcqcn,
    pub roce: SimaiRoce,
}

const DATA_PRIORITY: u8 = 3;
const ACK_SIZE_BYTES: u64 = 60;
const NACK_INTERVAL_NS: u64 = 500_000;
/// SimAI's per-port reserve and resume offset (`switch-mmu.cc:24-27`).
const PFC_RESERVE_BYTES: u64 = 4096;
const PFC_RESUME_OFFSET_BYTES: u64 = 3072;
const PFC_SHIFT: u32 = 3;

/// Derives the fabric settings for a rail shape. `max_bdp_bytes` is H2's
/// `ServerLocality::max_bdp_bytes(mtu)`, SimAI's window with `HAS_WIN 1`.
pub fn derive_fabric(
    _conf: &SimaiConf,
    _shape: &RailShape,
    _max_bdp_bytes: u64,
) -> Result<SimaiFabric, AicbError> {
    Err(AicbError::new("the fabric derivation is not implemented"))
}

/// A decimal integer: digits only, no sign, no leading zero.
fn integer(token: &str) -> Option<u64> {
    if token.is_empty()
        || !token.bytes().all(|b| b.is_ascii_digit())
        || (token.len() > 1 && token.starts_with('0'))
    {
        return None;
    }
    token.parse().ok()
}

/// `value × 10^power` for a non-negative decimal (`12`, `0.5`, `900.000`), if it is a whole number.
fn scaled_decimal(token: &str, power: u32) -> Option<u64> {
    let (whole, fraction) = token.split_once('.').unwrap_or((token, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || (token.contains('.') && fraction.is_empty())
    {
        return None;
    }
    let fraction = fraction.trim_end_matches('0');
    if fraction.len() > power as usize {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    let value: u128 = digits.parse().ok()?;
    let scale = 10_u128.checked_pow(power - fraction.len() as u32)?;
    u64::try_from(value.checked_mul(scale)?).ok()
}

fn is_zero_decimal(token: &str) -> bool {
    let (whole, fraction) = token.split_once('.').unwrap_or((token, ""));
    !whole.is_empty() && whole.bytes().all(|b| b == b'0') && fraction.bytes().all(|b| b == b'0')
}

/// A decimal in [0, 1]: `0`, `1`, `0.<digits>` or `1.0…0`.
fn is_unit_decimal(token: &str) -> bool {
    match token.split_once('.') {
        None => token == "0" || token == "1",
        Some((whole, fraction)) => {
            !fraction.is_empty()
                && fraction.bytes().all(|b| b.is_ascii_digit())
                && (whole == "0" || (whole == "1" && fraction.bytes().all(|b| b == b'0')))
        }
    }
}

/// An ns-3 `DataRate` string with a decimal SI prefix (`50Mb/s`, `400Gbps`), in bits per second.
fn data_rate_bps(token: &str) -> Option<u64> {
    for (suffix, power) in [
        ("Gbps", 9),
        ("Gb/s", 9),
        ("Mbps", 6),
        ("Mb/s", 6),
        ("Kbps", 3),
        ("Kb/s", 3),
        ("kbps", 3),
        ("kb/s", 3),
        ("bps", 0),
        ("b/s", 0),
    ] {
        if let Some(number) = token.strip_suffix(suffix) {
            return scaled_decimal(number, power).filter(|&rate| rate > 0);
        }
    }
    None
}

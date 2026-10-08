//! `SimAI.conf`, the single source of an AICB scenario's fabric settings (ruling A3; design note
//! §4.2).
//!
//! The reader is strict where SimAI's (`network_frontend/ns3/common.h:459-662`, a `conf >> key`
//! loop) is lenient: every key must be one SimAI reads, appear once, and carry exactly its
//! values, so a typo cannot shift the rest of the file. [`derive_fabric`] turns the keys Days
//! models into the rail fabric's switch rows (H2 §4), DCQCN and RoCE settings, with SimAI's own
//! integer formulas (`common.h:840-890`), and refuses settings Days does not model.

use std::collections::BTreeMap;

use days_executor::{link_arrival_time_ns, pfc_line_rate_bytes, pfc_required_headroom_bytes};

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
/// `NIC_TOTAL_PAUSE_TIME` is a monitor; `L2_CHUNK_SIZE`, a byte chunk that adds ACKs at chunk
/// boundaries and sets back-to-zero recovery points (`rdma-hw.cc:532,591,602`), has no effect
/// under the required `L2_ACK_INTERVAL 1`, which ACKs every packet, and `L2_BACK_TO_ZERO 0`;
/// it must be a positive integer).
pub const RECORDED_KEYS: &[&str] = &[
    "GLOBAL_T",
    "L2_CHUNK_SIZE",
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
    /// `PACKET_PAYLOAD_SIZE`, the packet payload (and Days packet size) in bytes.
    pub fn mtu_bytes(&self) -> Result<u64, AicbError> {
        match self.integer("PACKET_PAYLOAD_SIZE")? {
            0 => Err(AicbError::new(
                "SimAI.conf: PACKET_PAYLOAD_SIZE must be positive",
            )),
            mtu => Ok(mtu),
        }
    }

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
        for pair in values[1..].as_chunks::<2>().0 {
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
pub fn parse_simai_conf(text: &str) -> Result<SimaiConf, AicbError> {
    if !text.is_ascii() {
        return Err(AicbError::new("SimAI.conf is not ASCII"));
    }
    let mut tokens = text.split('\n').enumerate().flat_map(|(index, line)| {
        line.split_ascii_whitespace()
            .map(move |token| (index + 1, token))
    });
    let mut entries = BTreeMap::new();
    while let Some((line, key)) = tokens.next() {
        let (name, arity) = KEYS
            .iter()
            .find(|(name, _)| *name == key)
            .copied()
            .ok_or_else(|| AicbError::at(line, format!("SimAI.conf: unknown key `{key}`")))?;
        let mut take = |what: &str| {
            tokens
                .next()
                .map(|(_, value)| value.to_owned())
                .ok_or_else(|| AicbError::at(line, format!("SimAI.conf: `{name}` lacks {what}")))
        };
        let values = match arity {
            Arity::One => vec![take("its value")?],
            Arity::Three => vec![take("3 values")?, take("3 values")?, take("3 values")?],
            Arity::RateMap => {
                let count_text = take("its count")?;
                let count = integer(&count_text).ok_or_else(|| {
                    AicbError::at(
                        line,
                        format!("SimAI.conf: `{name}` count `{count_text}` is not an integer"),
                    )
                })?;
                let mut values = vec![count_text];
                for _ in 0..count {
                    values.push(take("a (rate, value) pair")?);
                    values.push(take("a (rate, value) pair")?);
                }
                values
            }
        };
        if entries.insert(name, (line, values)).is_some() {
            return Err(AicbError::at(
                line,
                format!("SimAI.conf: `{name}` appears twice"),
            ));
        }
    }
    Ok(SimaiConf { entries })
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
    /// Always 1: `L2_ACK_INTERVAL` is bytes in SimAI, and only its value 1 (an ACK per packet)
    /// is modelled.
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
    /// `ceil(BUFFER_SIZE MiB / MTU)` packets per switch egress queue: the queue's packet capacity,
    /// which only the device planners read (as a sizing hint) under the ECN policy.
    pub queue_capacity_packets: u64,
    /// `BUFFER_SIZE` in bytes: the byte capacity of each egress queue's ECN policy.
    pub queue_capacity_bytes: u64,
    /// ECN step threshold in bytes of queue per egress link rate, at the K-ramp midpoint (SimAI
    /// and real switches count queue depth in bytes; P16 ecnbytes, user ruling Oct 8).
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
    conf: &SimaiConf,
    shape: &RailShape,
    max_bdp_bytes: u64,
) -> Result<SimaiFabric, AicbError> {
    conf.require_value("CC_MODE", 1, "Days runs the Mellanox DCQCN of CC_MODE 1")?;
    conf.require_value("ENABLE_QCN", 1, "ECN marking is part of the DCQCN arms")?;
    conf.require_value(
        "USE_DYNAMIC_PFC_THRESHOLD",
        1,
        "Days derives static per-tier thresholds from SimAI's dynamic rule at an empty pool",
    )?;
    conf.require_value("L2_BACK_TO_ZERO", 0, "queue pairs are Go-back-N")?;
    conf.require_value("RATE_BOUND", 1, "queue pairs are paced at their rate")?;
    conf.require_value("ACK_HIGH_PRIO", 0, "feedback rides the data class (D16)")?;
    let (line, error_rate) = conf.required("ERROR_RATE_PER_LINK")?;
    if !is_zero_decimal(error_rate) {
        return Err(AicbError::at(
            line,
            format!("SimAI.conf: `ERROR_RATE_PER_LINK {error_rate}`: Days models lossless links"),
        ));
    }
    if let Some((line, values)) = conf.raw("LINK_DOWN") {
        if values.iter().any(|value| integer(value) != Some(0)) {
            return Err(AicbError::at(
                line,
                format!(
                    "SimAI.conf: `LINK_DOWN {}` is not modelled",
                    values.join(" ")
                ),
            ));
        }
    }
    let mtu = conf.integer("PACKET_PAYLOAD_SIZE")?;
    if mtu == 0 {
        return Err(AicbError::new(
            "SimAI.conf: PACKET_PAYLOAD_SIZE must be positive",
        ));
    }
    let buffer_bytes = conf
        .integer("BUFFER_SIZE")?
        .checked_mul(1024 * 1024)
        .ok_or_else(|| AicbError::new("SimAI.conf: BUFFER_SIZE overflows"))?;
    // SimAI's ACK interval is in bytes: the receiver ACKs when its next expected byte reaches a
    // milestone that then advances by L2_ACK_INTERVAL, or lands on a multiple of L2_CHUNK_SIZE
    // (rdma-hw.cc:586-595). At 1 every in-order packet is ACKed and the chunk test is never
    // reached; any other value is a byte rule Days does not model (part-1 review F2).
    conf.require_value(
        "L2_ACK_INTERVAL",
        1,
        "SimAI reads it in bytes (rdma-hw.cc:586-595): 1 ACKs every packet, a larger value ACKs \
         once per that many bytes and again at every L2_CHUNK_SIZE-byte boundary",
    )?;
    let ack_every_packets = 1;
    if conf.integer("L2_CHUNK_SIZE")? == 0 {
        return Err(AicbError::new(
            "SimAI.conf: `L2_CHUNK_SIZE 0` must be a positive number of bytes",
        ));
    }

    // The fabric's links and switch ports (gen_Topo_Template.py; H2 §1.1).
    if shape.gpus == 0
        || shape.gpus_per_server == 0
        || shape.nics_per_asw == 0
        || shape.psws == 0
        || shape.nic_rate_bps == 0
        || shape.uplink_rate_bps == 0
    {
        return Err(AicbError::new("the rail shape has a zero count or rate"));
    }
    let segment = shape.gpus_per_server * shape.nics_per_asw;
    if shape.gpus > segment && !shape.gpus.is_multiple_of(segment) {
        return Err(AicbError::new("the rail shape's last segment is ragged"));
    }
    let gpus_per_asw = shape.gpus.min(segment) / shape.gpus_per_server;
    let asws = shape.gpus.div_ceil(segment) * shape.gpus_per_server;
    // SimAI's `nic_rate` is the first host's device 1, its NVLink (the generator lists it first);
    // a port faster than it gets a smaller PFC shift (`common.h:841-866`).
    let shift_of = |rate: u64| {
        let mut shift = PFC_SHIFT;
        let mut rate = rate;
        while rate > shape.nvlink_rate_bps && shift > 0 {
            shift -= 1;
            rate /= 2;
        }
        shift
    };
    let simai_headroom = |rate: u64| -> u64 {
        u64::try_from(u128::from(rate) * u128::from(shape.link_delay_ns) / 8 / 1_000_000_000 * 3)
            .expect("bounded by u64 rates and delays")
    };
    let tier = |ports: &[(u64, u64)], name: &str| -> Result<PfcTier, AicbError> {
        let mut shifts = ports.iter().map(|&(rate, _)| shift_of(rate));
        let shift = shifts.next().expect("a switch has ports");
        if shifts.any(|other| other != shift) {
            return Err(AicbError::new(format!(
                "the {name} tier's ports have different PFC shifts; Days models one XOFF per tier"
            )));
        }
        let reserved = ports.iter().try_fold(0_u64, |sum, &(rate, count)| {
            sum.checked_add(count.checked_mul(simai_headroom(rate) + PFC_RESERVE_BYTES)?)
        });
        let free = reserved
            .and_then(|reserved| buffer_bytes.checked_sub(reserved))
            .ok_or_else(|| {
                AicbError::new(format!(
                    "the {name} tier's headroom and reserve exceed BUFFER_SIZE"
                ))
            })?;
        let xoff_bytes = free >> shift;
        let xon_bytes = xoff_bytes
            .checked_sub(PFC_RESUME_OFFSET_BYTES)
            .ok_or_else(|| {
                AicbError::new(format!(
                    "the {name} tier's XOFF is below SimAI's resume offset"
                ))
            })?;
        Ok(PfcTier {
            xoff_bytes,
            xon_bytes,
        })
    };
    let pfc_asw = tier(
        &[
            (shape.nic_rate_bps, gpus_per_asw),
            (shape.uplink_rate_bps, shape.psws),
        ],
        "ASW",
    )?;
    let pfc_psw = tier(&[(shape.uplink_rate_bps, asws)], "PSW")?;

    // ECN steps at the K-ramp midpoint (KMIN/KMAX in KB) in bytes of queue, one per egress link
    // rate; SimAI asserts an entry for every port rate (`common.h:850-855`). Marking stays a step
    // at enqueue (recorded divergence `ecn-step`: no ramp, no dequeue marking).
    let (kmin_line, kmin) = conf.rate_map("KMIN_MAP")?;
    let (kmax_line, kmax) = conf.rate_map("KMAX_MAP")?;
    let (pmax_line, pmax) = conf.rate_map("PMAX_MAP")?;
    let mut ecn_by_rate = BTreeMap::new();
    let mut headroom_by_rate = BTreeMap::new();
    for rate in [shape.nic_rate_bps, shape.uplink_rate_bps] {
        let kmin = integer(rate_entry(&kmin, rate, kmin_line, "KMIN_MAP")?).ok_or_else(|| {
            AicbError::at(kmin_line, "SimAI.conf: a KMIN_MAP value is not an integer")
        })?;
        let kmax = integer(rate_entry(&kmax, rate, kmax_line, "KMAX_MAP")?).ok_or_else(|| {
            AicbError::at(kmax_line, "SimAI.conf: a KMAX_MAP value is not an integer")
        })?;
        rate_entry(&pmax, rate, pmax_line, "PMAX_MAP")?;
        if kmin > kmax {
            return Err(AicbError::at(
                kmin_line,
                format!("SimAI.conf: KMIN {kmin} above KMAX {kmax} at {rate} b/s"),
            ));
        }
        let threshold = u64::try_from((u128::from(kmin) + u128::from(kmax)) * 500)
            .map_err(|_| AicbError::new("SimAI.conf: an ECN threshold overflows"))?;
        if threshold == 0 || threshold > buffer_bytes {
            return Err(AicbError::at(
                kmin_line,
                format!(
                    "SimAI.conf: the ECN threshold {threshold} B at {rate} b/s is outside \
                     1..=BUFFER_SIZE ({buffer_bytes} B)"
                ),
            ));
        }
        ecn_by_rate.insert(rate, threshold);
        let reverse = link_arrival_time_ns(0, 64, rate, shape.link_delay_ns)
            .map_err(|error| AicbError::new(format!("a pause frame's delay: {error}")))?;
        let line =
            pfc_line_rate_bytes(rate, shape.link_delay_ns, reverse).map_err(AicbError::new)?;
        let minimum = pfc_required_headroom_bytes(mtu, line).map_err(AicbError::new)?;
        let minimum = u64::try_from(minimum).map_err(|_| AicbError::new("a headroom overflows"))?;
        headroom_by_rate.insert(rate, simai_headroom(rate).max(minimum));
    }

    let (g_line, g_literal) = conf.required("EWMA_GAIN")?;
    if !is_unit_decimal(g_literal) {
        return Err(AicbError::at(
            g_line,
            format!("SimAI.conf: `EWMA_GAIN {g_literal}` is not a decimal in [0, 1]"),
        ));
    }
    let fast_recovery_times = u32::try_from(conf.integer("FAST_RECOVERY_TIMES")?)
        .map_err(|_| AicbError::new("SimAI.conf: FAST_RECOVERY_TIMES does not fit u32"))?;
    let pacing_interval_ns = u64::try_from(
        (u128::from(mtu) * 8 * 1_000_000_000).div_ceil(u128::from(shape.nic_rate_bps)),
    )
    .map_err(|_| AicbError::new("the pacing interval overflows"))?;
    let dcqcn = SimaiDcqcn {
        max_rate_bps: shape.nic_rate_bps,
        min_rate_bps: conf.rate("MIN_RATE")?,
        ai_rate_bps: conf.rate("RATE_AI")?,
        hai_rate_bps: conf.rate("RATE_HAI")?,
        g_literal: g_literal.to_owned(),
        alpha_resume_interval_ns: conf.microseconds("ALPHA_RESUME_INTERVAL")?,
        rate_decrease_interval_ns: conf.microseconds("RATE_DECREASE_INTERVAL")?,
        rp_timer_ns: conf.microseconds("RP_TIMER")?,
        fast_recovery_times,
        clamp_target_rate: conf.flag("CLAMP_TARGET_RATE")?,
        pacing_interval_ns,
    };
    let roce = SimaiRoce {
        retransmit_timeout_ns: 0,
        ack_every_packets,
        nack_interval_ns: NACK_INTERVAL_NS,
        ack_size_bytes: ACK_SIZE_BYTES,
        window_bytes: if conf.flag("HAS_WIN")? {
            max_bdp_bytes
        } else {
            0
        },
        variable_window: conf.flag("VAR_WIN")?,
        feedback_priority: DATA_PRIORITY,
    };
    Ok(SimaiFabric {
        mtu_bytes: mtu,
        queue_capacity_packets: buffer_bytes.div_ceil(mtu),
        queue_capacity_bytes: buffer_bytes,
        ecn_by_rate,
        pfc_asw,
        pfc_psw,
        headroom_by_rate,
        data_priority: DATA_PRIORITY,
        dcqcn,
        roce,
    })
}

fn rate_entry<'a>(
    map: &BTreeMap<u64, &'a str>,
    rate: u64,
    line: usize,
    key: &str,
) -> Result<&'a str, AicbError> {
    map.get(&rate).copied().ok_or_else(|| {
        AicbError::at(
            line,
            format!("SimAI.conf: `{key}` has no entry for {rate} b/s"),
        )
    })
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

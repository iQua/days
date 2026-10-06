//! SimAI's rail-optimized single-ToR fabric ("Spectrum-X"), built natively (P16 H2).
//!
//! The structure is the one SimAI's `gen_Topo_Template.py` writes (`Rail_Opti_SingleToR`), in
//! closed form:
//!
//! - SimAI node ids: GPU `0..G`, NVSwitch `G + i / gpus_per_server` (one per server), ASW
//!   `G + S + (i / segment) * gpus_per_server + i % gpus_per_server` (one per segment and rail), PSW
//!   `G + S + A + j`. The ids matter: a switch's id is its ECMP hash seed and a GPU's id is its IP.
//! - Links: per GPU its NVLink, then its NIC link to the ASW; then every ASW to every PSW.
//!
//! [`RailTopology::render_simai`] writes that structure in SimAI's topology-file grammar, and the
//! pinned SimAI files are reproduced byte for byte (`tests/p16_rail_topology.rs`), so one
//! definition serves both engines.
//!
//! Days models NVLink delay-only: the graph holds the ASWs and PSWs (dense indices: ASW `a` is `a`,
//! PSW `j` is `A + j`), each GPU host attaches to its ASW through its one NIC link, and no NVSwitch
//! is instantiated. Same-server messages lower to the out-of-band stage notify of the executor,
//! timed by [`ServerLocality::nvlink_message_delay_ns`].

use petgraph::graph::UnGraph;

use crate::topos::build::{HostAttachment, HostAttachments, TopologyError, TopologyProfile};
use crate::topos::config::SpectrumXConfig;

const NANOS_PER_SECOND: u128 = 1_000_000_000;

/// The structure of one rail fabric, everything lowering and routing need, in exact integers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RailProfile {
    pub gpus: u32,
    pub gpus_per_server: u32,
    pub nics_per_asw: u32,
    pub asws: u32,
    pub psws: u32,
    pub nic_rate_bps: u64,
    pub uplink_rate_bps: u64,
    pub nvlink_rate_bps: u64,
    pub link_delay_ns: u64,
    pub nvlink_delay_ns: u64,
}

impl RailProfile {
    /// GPUs that share one ASW per rail: a segment spans `gpus_per_server * nics_per_asw` GPUs.
    pub const fn segment_gpus(&self) -> u32 {
        self.gpus_per_server * self.nics_per_asw
    }

    pub const fn servers(&self) -> u32 {
        self.gpus / self.gpus_per_server
    }

    /// The Days switch index (graph node) of GPU `gpu`'s ASW.
    pub const fn asw_of(&self, gpu: u32) -> u32 {
        (gpu / self.segment_gpus()) * self.gpus_per_server + gpu % self.gpus_per_server
    }

    /// The Days switch index of PSW `psw`.
    pub const fn psw_index(&self, psw: u32) -> u32 {
        self.asws + psw
    }

    /// SimAI's node id of Days switch `index`: switches follow the GPUs and the NVSwitches.
    pub const fn simai_switch_id(&self, index: u32) -> u32 {
        self.gpus + self.servers() + index
    }

    pub const fn server_of(&self, gpu: u32) -> u32 {
        gpu / self.gpus_per_server
    }

    /// Whether a Days switch index names an ASW.
    pub const fn is_asw(&self, index: u32) -> bool {
        index < self.asws
    }
}

/// SimAI's ECMP hash, `SwitchNode::EcmpHash` (Murmur3 x86-32) over three little-endian 32-bit
/// words, the only length SimAI hashes: `(sip, dip, sport | dport << 16)` (ns-3-alibabacloud
/// `switch-node.cc:63-90,142-178`). Golden vectors from SimAI's own code pin it
/// (`tests/p16_rail_ecmp.rs`).
pub const fn simai_ecmp_hash(words: [u32; 3], seed: u32) -> u32 {
    let mut hash = seed;
    let mut index = 0;
    while index < 3 {
        let mut k = words[index].wrapping_mul(0xcc9e_2d51);
        k = k.rotate_left(15);
        k = k.wrapping_mul(0x1b87_3593);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_add(hash << 2).wrapping_add(0xe654_6b64);
        index += 1;
    }
    hash ^= 12;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^ (hash >> 16)
}

/// SimAI's address of node `id` (`node_id_to_ip`, astra-sim frontend `common.h:146-149`).
pub const fn simai_node_ip(id: u32) -> u32 {
    0x0b00_0001 + (id / 256) * 0x0001_0000 + (id % 256) * 0x0000_0100
}

/// SimAI's destination port of every RDMA flow (`entry.h:115`).
pub const SIMAI_DPORT: u32 = 100;
/// SimAI's first source port per host pair; message `k` of a pair uses `10000 + k`, wrapping as
/// SimAI's `uint16_t` counter does (`common.h:120`, `entry.h:115-127`).
pub const SIMAI_FIRST_SPORT: u16 = 10000;

impl RailProfile {
    /// The SimAI source port of the `ordinal`-th message between one ordered host pair.
    pub const fn simai_sport(ordinal: u64) -> u16 {
        // SimAI's `portNumber[src][dst]++` is a `uint16_t`: the port wraps after 65,535.
        (SIMAI_FIRST_SPORT as u64).wrapping_add(ordinal) as u16
    }

    /// The PSW (0-based, Days order) a source ASW picks for a message `source -> target` with
    /// source port `sport`; `None` when the two GPUs share an ASW (no PSW is crossed).
    ///
    /// SimAI's next-hop vector at an ASW lists the PSWs in descending node id: MEASURED from
    /// SimAI's printed routing tables on the 128g and 1024g files (P16 H2,
    /// `evidence/P16/railtopo-impl/psw-order/`). Entry `k` is therefore PSW `psws - 1 - k`.
    pub const fn data_psw(&self, source: u32, target: u32, sport: u16) -> Option<u32> {
        let asw = self.asw_of(source);
        if asw == self.asw_of(target) {
            return None;
        }
        let words = [
            simai_node_ip(source),
            simai_node_ip(target),
            sport as u32 | (SIMAI_DPORT << 16),
        ];
        let entry = simai_ecmp_hash(words, self.simai_switch_id(asw)) % self.psws;
        Some(self.psws - 1 - entry)
    }

    /// The PSW the target's ASW picks for the ACK or NACK of that message: SimAI swaps the
    /// addresses and the ports (`rdma-hw.cc:430-444`) and hashes with the target ASW's seed, so the
    /// feedback path is an independent choice, not the reverse of the data path.
    pub const fn feedback_psw(&self, source: u32, target: u32, sport: u16) -> Option<u32> {
        let asw = self.asw_of(target);
        if asw == self.asw_of(source) {
            return None;
        }
        let words = [
            simai_node_ip(target),
            simai_node_ip(source),
            SIMAI_DPORT | ((sport as u32) << 16),
        ];
        let entry = simai_ecmp_hash(words, self.simai_switch_id(asw)) % self.psws;
        Some(self.psws - 1 - entry)
    }
}

/// One built rail fabric and its configuration (kept for rendering SimAI's header and strings).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RailTopology {
    profile: RailProfile,
    gpu_type: String,
}

impl RailTopology {
    /// Checks the configuration and fixes the structure. Refuses a ragged last segment (SimAI pads
    /// the ASW count to whole segments but would leave ports unused), a server count that does not
    /// divide, and identities beyond `u32`.
    pub fn new(config: &SpectrumXConfig) -> Result<Self, TopologyError> {
        let invalid = |message: String| TopologyError::InvalidConfig(message);
        let positive = [
            ("gpus", config.gpus),
            ("gpus_per_server", config.gpus_per_server),
            ("nics_per_asw", config.nics_per_asw),
            ("psws", config.psws),
            ("nic_rate_bps", config.nic_rate_bps),
            ("uplink_rate_bps", config.uplink_rate_bps),
            ("nvlink_rate_bps", config.nvlink_rate_bps),
            ("link_delay_ns", config.link_delay_ns),
            ("nvlink_delay_ns", config.nvlink_delay_ns),
        ];
        if let Some((name, _)) = positive.iter().find(|(_, value)| *value == 0) {
            return Err(invalid(format!("spectrum_x.{name} must be positive")));
        }
        if config.gpu_type.is_empty() || config.gpu_type.contains(char::is_whitespace) {
            return Err(invalid(
                "spectrum_x.gpu_type must be one SimAI header token".to_owned(),
            ));
        }
        if !config.gpus.is_multiple_of(config.gpus_per_server) {
            return Err(invalid(format!(
                "spectrum_x.gpus {} is not a multiple of gpus_per_server {}",
                config.gpus, config.gpus_per_server
            )));
        }
        let segment = config
            .gpus_per_server
            .checked_mul(config.nics_per_asw)
            .ok_or_else(|| TopologyError::NumericOverflow("spectrum_x segment size".into()))?;
        if !config.gpus.is_multiple_of(segment) && config.gpus > segment {
            return Err(invalid(format!(
                "spectrum_x.gpus {} leaves a partial segment of {segment} GPUs; Days refuses a \
                 ragged last segment",
                config.gpus
            )));
        }
        let segments = config.gpus.div_ceil(segment);
        let asws = segments
            .checked_mul(config.gpus_per_server)
            .ok_or_else(|| TopologyError::NumericOverflow("spectrum_x ASW count".into()))?;
        let servers = config.gpus / config.gpus_per_server;
        let nodes = [config.gpus, servers, asws, config.psws]
            .into_iter()
            .try_fold(0_u64, u64::checked_add)
            .filter(|nodes| *nodes <= u64::from(u32::MAX))
            .ok_or_else(|| TopologyError::NumericOverflow("spectrum_x node identities".into()))?;
        let narrow = |value: u64| u32::try_from(value).expect("bounded by the node count");
        debug_assert!(nodes > 0);
        Ok(Self {
            profile: RailProfile {
                gpus: narrow(config.gpus),
                gpus_per_server: narrow(config.gpus_per_server),
                nics_per_asw: narrow(config.nics_per_asw),
                asws: narrow(asws),
                psws: narrow(config.psws),
                nic_rate_bps: config.nic_rate_bps,
                uplink_rate_bps: config.uplink_rate_bps,
                nvlink_rate_bps: config.nvlink_rate_bps,
                link_delay_ns: config.link_delay_ns,
                nvlink_delay_ns: config.nvlink_delay_ns,
            },
            gpu_type: config.gpu_type.clone(),
        })
    }

    pub const fn profile(&self) -> RailProfile {
        self.profile
    }

    /// The switch graph (ASWs then PSWs), every ASW–PSW edge in SimAI's file order.
    pub fn graph(&self) -> UnGraph<usize, ()> {
        let profile = self.profile;
        let mut edges = Vec::with_capacity(profile.asws as usize * profile.psws as usize);
        for asw in 0..profile.asws {
            for psw in 0..profile.psws {
                edges.push((asw, profile.psw_index(psw)));
            }
        }
        UnGraph::<usize, ()>::from_edges(&edges)
    }

    /// Each GPU host attached to its ASW, in GPU order.
    pub fn host_attachments(&self) -> Result<HostAttachments, TopologyError> {
        let profile = self.profile;
        let entries = (0..profile.gpus)
            .map(|gpu| HostAttachment {
                host_id: gpu as usize,
                switch_id: profile.asw_of(gpu) as usize,
            })
            .collect();
        let gpus_per_asw = profile.gpus.min(profile.segment_gpus()) / profile.gpus_per_server;
        HostAttachments::new(entries, gpus_per_asw > 1)
    }

    pub fn topology_profile(&self) -> TopologyProfile {
        TopologyProfile::Rail(self.profile)
    }

    /// The fabric in SimAI's topology-file grammar, exactly as `gen_Topo_Template.py` writes it.
    pub fn render_simai(&self) -> String {
        let profile = self.profile;
        let servers = profile.servers();
        let nodes = profile.gpus + servers + profile.asws + profile.psws;
        let links = 2 * u64::from(profile.gpus) + u64::from(profile.asws) * u64::from(profile.psws);
        let nvlink = rate_string(profile.nvlink_rate_bps);
        let nvlink_delay = delay_string(profile.nvlink_delay_ns);
        let nic = rate_string(profile.nic_rate_bps);
        let uplink = rate_string(profile.uplink_rate_bps);
        let delay = delay_string(profile.link_delay_ns);
        let mut out = format!(
            "{nodes} {} {servers} {} {links} {}\n",
            profile.gpus_per_server,
            profile.asws + profile.psws,
            self.gpu_type
        );
        for id in profile.gpus..nodes {
            out.push_str(&format!("{id} "));
        }
        out.push('\n');
        let switch_base = profile.gpus + servers;
        for gpu in 0..profile.gpus {
            let nvswitch = profile.gpus + profile.server_of(gpu);
            out.push_str(&format!("{gpu} {nvswitch} {nvlink} {nvlink_delay} 0\n"));
            let asw = switch_base + profile.asw_of(gpu);
            out.push_str(&format!("{gpu} {asw} {nic} {delay} 0\n"));
        }
        for asw in 0..profile.asws {
            for psw in 0..profile.psws {
                out.push_str(&format!(
                    "{} {} {uplink} {delay} 0\n",
                    switch_base + asw,
                    switch_base + profile.psw_index(psw)
                ));
            }
        }
        out
    }
}

/// An ns-3 `DataRate` string: whole gigabits as `<n>Gbps`, else whole bits as `<n>bps`.
fn rate_string(rate_bps: u64) -> String {
    if rate_bps.is_multiple_of(1_000_000_000) {
        format!("{}Gbps", rate_bps / 1_000_000_000)
    } else {
        format!("{rate_bps}bps")
    }
}

/// An ns-3 time string in milliseconds with the shortest exact decimal (500 ns is `0.0005ms`).
fn delay_string(delay_ns: u64) -> String {
    let whole = delay_ns / 1_000_000;
    let fraction = delay_ns % 1_000_000;
    if fraction == 0 {
        return format!("{whole}ms");
    }
    let digits = format!("{fraction:06}");
    format!("{whole}.{}ms", digits.trim_end_matches('0'))
}

/// Server membership and the delay-only NVLink model, for collective lowering (H1↔H2 interface).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerLocality {
    profile: RailProfile,
}

impl ServerLocality {
    pub const fn new(profile: RailProfile) -> Self {
        Self { profile }
    }

    pub const fn profile(&self) -> RailProfile {
        self.profile
    }

    /// The server of GPU host `host`, or `None` for an identity outside the fabric.
    pub fn server_of(&self, host: u64) -> Option<u64> {
        (host < u64::from(self.profile.gpus))
            .then(|| host / u64::from(self.profile.gpus_per_server))
    }

    /// Whether two GPU hosts share a server, so a message between them crosses NVLink.
    pub fn same_server(&self, left: u64, right: u64) -> bool {
        left != right
            && self.server_of(left).is_some()
            && self.server_of(left) == self.server_of(right)
    }

    /// Delay of one same-server message of `bytes` (ruling H2-3, with concurrency).
    ///
    /// SimAI sends it through the server's NVSwitch: two NVLink hops of `nvlink_delay_ns`, the
    /// sender's port serializing everything it carries in that step (`port_bytes`, which the
    /// collective expansion supplies and which includes this message), and the NVSwitch
    /// storing and forwarding the last packet (at most one MTU) once more:
    /// `2·p + ceil(8·port_bytes·1e9 / r) + ceil(8·min(bytes, mtu)·1e9 / r)`.
    /// `None` when `port_bytes < bytes`, `bytes == 0`, `mtu == 0`, or on overflow.
    pub fn nvlink_message_delay_ns(&self, bytes: u64, port_bytes: u64, mtu: u64) -> Option<u64> {
        if bytes == 0 || mtu == 0 || port_bytes < bytes {
            return None;
        }
        let rate = u128::from(self.profile.nvlink_rate_bps);
        let serialize = |bytes: u64| (u128::from(bytes) * 8 * NANOS_PER_SECOND).div_ceil(rate);
        let total = 2 * u128::from(self.profile.nvlink_delay_ns)
            + serialize(port_bytes)
            + serialize(bytes.min(mtu));
        u64::try_from(total).ok()
    }

    /// SimAI's window `maxBdp` (`HAS_WIN 1`, `GLOBAL_T 1`): the largest bandwidth-delay product
    /// over host pairs, `rtt · min_bw / 1e9 / 8` with `rtt = 2·Σ delay + Σ mtu·8·1e9 / bw` in
    /// SimAI's own integer steps. NVLink pairs are included, as SimAI includes them.
    pub fn max_bdp_bytes(&self, mtu: u64) -> u64 {
        let profile = self.profile;
        // SimAI's per-hop transmission delay: `packet_payload_size * 1e9 * 8 / bw`, truncated.
        let tx = |bw: u64| u128::from(mtu) * NANOS_PER_SECOND * 8 / u128::from(bw);
        let bdp = |hops: &[(u64, u64)]| {
            let delay: u128 = hops.iter().map(|(_, delay)| u128::from(*delay)).sum();
            let tx_delay: u128 = hops.iter().map(|(bw, _)| tx(*bw)).sum();
            let bw = hops
                .iter()
                .map(|(bw, _)| *bw)
                .min()
                .expect("a path has a hop");
            (2 * delay + tx_delay) * u128::from(bw) / NANOS_PER_SECOND / 8
        };
        let nic = (profile.nic_rate_bps, profile.link_delay_ns);
        let up = (profile.uplink_rate_bps, profile.link_delay_ns);
        let nvlink = (profile.nvlink_rate_bps, profile.nvlink_delay_ns);
        let mut best = bdp(&[nvlink, nvlink]);
        let segment = profile.segment_gpus();
        // Same ASW: two GPUs of one rail in one segment (servers differ).
        if profile.gpus.min(segment) > profile.gpus_per_server {
            best = best.max(bdp(&[nic, nic]));
        }
        // Different ASWs: through a PSW.
        if profile.asws > 1 {
            best = best.max(bdp(&[nic, up, up, nic]));
        }
        u64::try_from(best).expect("a fabric's BDP fits u64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ns3_strings_are_exact() {
        assert_eq!(rate_string(400_000_000_000), "400Gbps");
        assert_eq!(rate_string(2_880_000_000_000), "2880Gbps");
        assert_eq!(rate_string(1_500), "1500bps");
        assert_eq!(delay_string(500), "0.0005ms");
        assert_eq!(delay_string(25), "0.000025ms");
        assert_eq!(delay_string(2_000_000), "2ms");
        assert_eq!(delay_string(1_500_000), "1.5ms");
    }
}

//! Shared topology and legacy-runtime configuration schema.

use serde::Deserialize;

#[derive(Deserialize)]
pub struct UIConfig {
    pub ui_interval: Option<f64>,
    pub duration: Option<f64>,
}

#[derive(Deserialize)]
pub struct ConcurrencyConfig {
    pub threading: Option<ThreadingModel>,
    pub num_threads: Option<usize>,
    pub hot_workers: Option<usize>,
    pub concurrency_level: Option<ConcurrencyLevel>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ThreadingModel {
    Single,
    Multiple,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ConcurrencyLevel {
    Default,
    Accelerated,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
    SP,
    VirtualClock,
    WFQ,
    WRR,
}

#[derive(Clone, Debug, Deserialize)]
pub enum DropStrategy {
    TailDrop,
    RED,
    #[serde(rename = "RED_ECN")]
    RedEcn,
    #[serde(rename = "ECN_THRESHOLD")]
    EcnThreshold,
}

#[derive(Deserialize)]
pub struct SwitchConfig {
    pub port_rate: f64,
    pub capacity: usize,
    pub discipline: SchedulingDiscipline,
    pub drop: DropStrategy,
    pub ecn_threshold: Option<f64>,
    pub weights: Option<Vec<usize>>,
    pub priorities: Option<Vec<usize>>,
    pub vticks: Option<Vec<f64>>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum LinkMode {
    None,
    Pfc,
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct PfcLinkConfig {
    pub xoff: Option<Vec<usize>>,
    pub xon: Option<Vec<usize>>,
    pub pause_quanta: Option<Vec<u16>>,
    pub buffer_capacity: Option<Vec<usize>>,
    pub refresh_interval: Option<f64>,
    pub drain_interval: Option<f64>,
}

/// Per-tier propagation delays for a layered fabric (T21/P12 F-HET).
///
/// The lowered image has always carried a per-link `propagation_ns`; only configuration was
/// restricted to one global value. A tier table names the three fat-tree layers explicitly rather
/// than inferring them, so a fixture states its delay heterogeneity in the same place it states
/// its topology.
#[derive(Clone, Copy, Debug, Deserialize, Default, PartialEq, Eq)]
pub struct PropagationTierConfig {
    pub host_to_edge_ns: u64,
    pub edge_to_aggregation_ns: u64,
    pub aggregation_to_core_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct LinkConfig {
    pub mode: Option<LinkMode>,
    pub pfc: Option<PfcLinkConfig>,
    pub propagation_ns: Option<u64>,
    pub propagation_tiers: Option<PropagationTierConfig>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub enum TopoCategory {
    FatTree,
    Torus,
    Dragonfly,
    /// A rail-optimized single-ToR fabric with SimAI's `gen_Topo_Template.py` structure (P16 H2).
    SpectrumX,
}

#[derive(Deserialize)]
pub struct FatTreeConfig {
    pub k: usize,
    #[serde(default)]
    pub hosts_per_edge: Option<usize>,
}

#[derive(Deserialize)]
pub struct TorusConfig {
    pub dim: usize,
    pub n: usize,
}

/// Canonical Kim-style dragonfly (T21/P12 F-TOPO).
///
/// `routers_per_group` (a) routers form an all-to-all group; each router carries
/// `global_ports_per_router` (h) global ports; the group count is the balanced `g = a*h + 1`, so
/// every pair of groups is joined by exactly one global link. `hosts_per_router` (p) defaults to 1.
#[derive(Deserialize)]
pub struct DragonflyConfig {
    pub routers_per_group: usize,
    pub global_ports_per_router: usize,
    #[serde(default)]
    pub hosts_per_router: Option<usize>,
}

/// SimAI's rail-optimized single-ToR ("Spectrum-X") fabric, in exact integers (P16 H2).
///
/// GPU `i` sits in server `i / gpus_per_server` on rail `i % gpus_per_server`. A segment is
/// `gpus_per_server * nics_per_asw` GPUs; each (segment, rail) pair has one leaf (ASW), and every
/// ASW links to every one of the `psws` spines (PSW). NVLink joins the GPUs of a server through one
/// NVSwitch; Days models it delay-only, so the NVLink class enters only the per-message delay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpectrumXConfig {
    pub gpus: u64,
    pub gpus_per_server: u64,
    pub nics_per_asw: u64,
    pub psws: u64,
    /// SimAI's `gpu_type` header token (for example `H100`); it does not change the structure.
    pub gpu_type: String,
    pub nic_rate_bps: u64,
    pub uplink_rate_bps: u64,
    pub nvlink_rate_bps: u64,
    pub link_delay_ns: u64,
    pub nvlink_delay_ns: u64,
}

#[derive(Deserialize)]
pub struct TopoConfig {
    pub category: TopoCategory,
    pub fat_tree: Option<FatTreeConfig>,
    pub torus: Option<TorusConfig>,
    pub dragonfly: Option<DragonflyConfig>,
    pub spectrum_x: Option<SpectrumXConfig>,
}

#[derive(Deserialize)]
pub struct AppSourceConfig {
    pub req_channel_capacity: Option<usize>,
    pub chunk_size: Option<usize>,
    pub initial_delay: Option<u64>,
    pub run_interval: Option<u64>,
}

#[derive(Deserialize)]
pub struct Config {
    pub switch: SwitchConfig,
    pub topology: Option<TopoConfig>,
    pub app_source: Option<AppSourceConfig>,
    pub link: Option<LinkConfig>,
    pub time_quantum_ns: Option<u64>,
    #[serde(default)]
    pub model_host_attachment: bool,
}

/// Legacy endpoint-stage parameters derived from the shared configuration.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HostAttachmentSpec {
    pub rate_bps: f64,
    pub propagation_ns: u64,
    pub injection_capacity_packets: usize,
    pub delivery_capacity_packets: usize,
}

impl Config {
    pub fn host_attachment_spec(&self) -> Option<HostAttachmentSpec> {
        // A declared scalar propagation delay is executable input, not an inert annotation. The
        // legacy topology uses this same physical-stage specification for fabric wires and host
        // attachments, so scalar propagation activates the complete ordered stage model. The
        // explicit key remains available to model finite-rate attachments with zero propagation.
        let scalar_propagation = self
            .link
            .as_ref()
            .and_then(|link| link.propagation_ns)
            .is_some();
        (self.model_host_attachment || scalar_propagation).then_some(HostAttachmentSpec {
            rate_bps: self.switch.port_rate,
            propagation_ns: self
                .link
                .as_ref()
                .and_then(|link| link.propagation_ns)
                .unwrap_or(0),
            injection_capacity_packets: 0,
            delivery_capacity_packets: self.switch.capacity,
        })
    }
}

//! The `[workload.aicb]` scenario table and its run manifest (design note §5; ruling A3).
//!
//! An AICB scenario is a SimAI run's three inputs plus the arm switches:
//!
//! ```toml
//! seed = 7
//! duration = 0.5
//!
//! [topology]
//! category = "SpectrumX"
//! [topology.spectrum_x]            # H2's schema
//! # ...
//!
//! [workload.aicb]
//! trace = "smoke.txt"              # relative to the scenario file
//! trace_sha256 = "..."
//! fidelity = "simai"               # or "megatron"
//! expert_routing = "uniform"       # or "imbalanced", with [workload.aicb.imbalanced]
//!
//! [workload.aicb.simai]
//! conf = "SimAI.conf"
//! conf_sha256 = "..."
//! topology = "Spectrum-X_128g_8gps_100Gbps_A100"   # optional: checked by render sha256
//! send_lat_us = 3                  # the three pins: required under fidelity = "simai"
//! nvls_enable = true
//! pxn_enable = false
//! ```
//!
//! `SimAI.conf` is the fabric's single source: the scenario may not declare `[switch]`, `[link]`,
//! `[routing]` or any flow, collective or compute table. [`prepare`] checks the pins, plans the
//! trace, derives the fabric and returns the scenario text the compiler lowers (the scenario
//! without its workload table, plus the derived switch, link and routing tables), the workload
//! IR, and the manifest the `days` binary prints as `record=days_workload`.

use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::scenario::workload::{Transport, Workload};
use crate::topos::config::SpectrumXConfig;
use crate::topos::rail::{RailTopology, ServerLocality};
use crate::utils::sha256::sha256_hex;

use super::{
    AicbError, DataQueueOrder, ExpertRouting, Fidelity, HopBounds, ImbalanceParams, PlanOptions,
    PropagationBounds, RailShape, SimaiEnv, SimaiFabric, check_simai_topology, derive_fabric,
    form_groups, lower_plan, parse_simai_conf, parse_trace, plan_schedule,
};

/// Top-level tables an AICB scenario may not declare: `SimAI.conf` and the trace replace them.
const DERIVED_TABLES: [&str; 8] = [
    "switch",
    "link",
    "routing",
    "flow",
    "flow_set",
    "collective",
    "collective_set",
    "compute",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadTable {
    aicb: AicbTable,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AicbTable {
    trace: String,
    trace_sha256: String,
    fidelity: String,
    expert_routing: String,
    imbalanced: Option<ImbalancedTable>,
    simai: SimaiTable,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImbalancedTable {
    seed: u64,
    experts: u32,
    topk: u32,
    tokens_per_rank: u64,
    zipf: u8,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SimaiTable {
    conf: String,
    conf_sha256: String,
    topology: Option<String>,
    send_lat_us: Option<u64>,
    nvls_enable: Option<bool>,
    pxn_enable: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ScenarioTopology {
    topology: Option<TopologyTable>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TopologyTable {
    category: String,
    spectrum_x: Option<SpectrumXConfig>,
}

/// What a run of an AICB scenario was made from, and what the adapter derived (design note §5.2).
/// Host metadata only: it never enters the image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AicbManifest {
    pub trace: String,
    pub trace_sha256: String,
    pub records: usize,
    pub tp: u32,
    pub ep: u32,
    pub pp: u32,
    pub ga: u32,
    pub all_gpus: u32,
    pub fidelity: Fidelity,
    pub expert_routing: ExpertRouting,
    pub simai_conf_sha256: String,
    /// The SimAI topology file and its rendered sha256, if the scenario names one.
    pub simai_topology: Option<(String, String)>,
    pub simai_env: Option<SimaiEnv>,
    pub mtu_bytes: u64,
    pub window_bytes: u64,
    pub queue_capacity_packets: u64,
    pub queue_capacity_bytes: u64,
    /// The ECN ramp by egress link rate: `(rate, kmin_bytes, kmax_bytes, pmax)`.
    pub ecn_by_rate: Vec<(u64, u64, u64, String)>,
    pub pfc_asw: (u64, u64),
    pub pfc_psw: (u64, u64),
    pub headroom_by_rate: Vec<(u64, u64)>,
    /// Record-column collectives, and the IR operations they and the fused segments became.
    pub collectives: usize,
    pub operations: usize,
    pub fused_segments: usize,
    pub fused_single_server_ops: usize,
    pub fp_clamps: usize,
    pub elided: usize,
    pub hang_window_recorded: usize,
    pub data_queue: DataQueueOrder,
    pub data_queue_order: Vec<String>,
    pub ecmp_ordinals_exact: bool,
}

impl fmt::Display for AicbManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fidelity = match self.fidelity {
            Fidelity::Simai => "simai",
            Fidelity::Megatron => "megatron",
        };
        write!(
            f,
            "record=days_workload adapter=aicb trace={} trace_sha256={} records={} tp={} ep={} \
             pp={} ga={} all_gpus={} fidelity={fidelity}",
            self.trace,
            self.trace_sha256,
            self.records,
            self.tp,
            self.ep,
            self.pp,
            self.ga,
            self.all_gpus,
        )?;
        match self.expert_routing {
            ExpertRouting::Uniform => f.write_str(" expert_routing=uniform")?,
            ExpertRouting::Imbalanced(params) => write!(
                f,
                " expert_routing=imbalanced seed={} experts={} topk={} tokens_per_rank={} zipf={}",
                params.seed, params.experts, params.topk, params.tokens_per_rank, params.zipf
            )?,
        }
        write!(f, " simai_conf_sha256={}", self.simai_conf_sha256)?;
        match &self.simai_topology {
            Some((name, sha256)) => {
                write!(f, " simai_topology={name} simai_topology_sha256={sha256}")?
            }
            None => f.write_str(" simai_topology=none")?,
        }
        match self.simai_env {
            Some(env) => write!(
                f,
                " send_lat_us={} nvls_enable={} pxn_enable={}",
                env.send_lat_us, env.nvls_enable, env.pxn_enable
            )?,
            None => f.write_str(" send_lat_us=none")?,
        }
        let rows = |rows: &[(u64, u64)]| {
            rows.iter()
                .map(|(rate, value)| format!("{rate}:{value}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        write!(
            f,
            " mtu_bytes={} window_bytes={} queue_capacity_packets={} queue_capacity_bytes={} \
             ecn_ramp_by_rate={} \
             pfc_asw_xoff={} pfc_asw_xon={} pfc_psw_xoff={} pfc_psw_xon={} headroom_by_rate={} \
             collectives={} operations={} fused_segments={} fused_single_server_ops={} \
             fp_clamps={} elided={} hang_window_recorded={} data_queue={} data_queue_order={} \
             ecmp_ordinals={} divergences={}",
            self.mtu_bytes,
            self.window_bytes,
            self.queue_capacity_packets,
            self.queue_capacity_bytes,
            self.ecn_by_rate
                .iter()
                .map(|(rate, kmin, kmax, pmax)| format!("{rate}:{kmin}/{kmax}/{pmax}"))
                .collect::<Vec<_>>()
                .join(","),
            self.pfc_asw.0,
            self.pfc_asw.1,
            self.pfc_psw.0,
            self.pfc_psw.1,
            rows(&self.headroom_by_rate),
            self.collectives,
            self.operations,
            self.fused_segments,
            self.fused_single_server_ops,
            self.fp_clamps,
            self.elided,
            self.hang_window_recorded,
            match self.data_queue {
                DataQueueOrder::Lifo => "lifo",
                DataQueueOrder::Fifo => "fifo",
                DataQueueOrder::FifoFallback => "fifo-fallback",
            },
            self.data_queue_order.join(","),
            if self.ecmp_ordinals_exact {
                "exact"
            } else {
                "proxy"
            },
            DIVERGENCES.join(","),
        )
    }
}

/// The recorded divergences from SimAI that every AICB run carries (design note §4.2).
pub const DIVERGENCES: [&str; 9] = [
    "static-pfc-thresholds",
    "per-queue-capacity",
    "ecn-step",
    "no-52b-header",
    "credit-pacer",
    "edge-triggered-pause",
    "delay-only-nvlink",
    "delivery-semantics",
    "as-send-lat",
];

/// An AICB scenario, ready to lower.
#[derive(Clone, Debug)]
pub struct PreparedScenario {
    /// The scenario text the compiler lowers: the original without `[workload]`, plus the
    /// derived `[routing]`, `[switch]` and `[link]` tables.
    pub text: String,
    pub workload: Workload,
    pub manifest: AicbManifest,
}

/// Whether a scenario declares `[workload.aicb]`.
pub fn is_aicb_scenario(text: &str) -> bool {
    #[derive(Deserialize)]
    struct Probe {
        workload: Option<toml::Table>,
    }
    toml::from_str::<Probe>(text)
        .ok()
        .and_then(|probe| probe.workload)
        .is_some_and(|workload| workload.contains_key("aicb"))
}

fn invalid(message: impl Into<String>) -> AicbError {
    AicbError::new(message)
}

/// Reads a scenario's `[workload.aicb]` and everything it names (design note §5).
pub fn prepare(path: &Path, text: &str) -> Result<PreparedScenario, AicbError> {
    let mut table: toml::Table =
        toml::from_str(text).map_err(|error| invalid(format!("the scenario: {error}")))?;
    for name in DERIVED_TABLES {
        if table.contains_key(name) {
            return Err(invalid(format!(
                "an AICB scenario takes `[{name}]` from SimAI.conf and its trace; remove it"
            )));
        }
    }
    let workload_value = table
        .remove("workload")
        .ok_or_else(|| invalid("the scenario has no [workload.aicb] table"))?;
    let aicb = workload_value
        .try_into::<WorkloadTable>()
        .map_err(|error| invalid(format!("[workload]: {error}")))?
        .aicb;
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    let read = |name: &str, declared: &str, what: &str| -> Result<String, AicbError> {
        let file = directory.join(name);
        let bytes = std::fs::read(&file)
            .map_err(|error| invalid(format!("{what} `{}`: {error}", file.display())))?;
        let digest = sha256_hex(&bytes);
        if digest != declared {
            return Err(invalid(format!(
                "{what} `{}` has sha256 {digest}, not the declared {declared}",
                file.display()
            )));
        }
        String::from_utf8(bytes).map_err(|_| invalid(format!("{what} `{name}` is not text")))
    };
    let trace_text = read(&aicb.trace, &aicb.trace_sha256, "the AICB trace")?;
    let conf_text = read(&aicb.simai.conf, &aicb.simai.conf_sha256, "SimAI.conf")?;

    let fidelity = match aicb.fidelity.as_str() {
        "simai" => Fidelity::Simai,
        "megatron" => Fidelity::Megatron,
        other => {
            return Err(invalid(format!(
                "unknown fidelity `{other}`; use \"simai\" or \"megatron\""
            )));
        }
    };
    let expert_routing = match (aicb.expert_routing.as_str(), aicb.imbalanced) {
        ("uniform", None) => ExpertRouting::Uniform,
        ("imbalanced", Some(params)) => ExpertRouting::Imbalanced(ImbalanceParams {
            seed: params.seed,
            experts: params.experts,
            topk: params.topk,
            tokens_per_rank: params.tokens_per_rank,
            zipf: params.zipf,
        }),
        ("uniform", Some(_)) => {
            return Err(invalid(
                "[workload.aicb.imbalanced] needs expert_routing = \"imbalanced\"",
            ));
        }
        ("imbalanced", None) => {
            return Err(invalid(
                "expert_routing = \"imbalanced\" needs [workload.aicb.imbalanced]",
            ));
        }
        (other, _) => {
            return Err(invalid(format!(
                "unknown expert_routing `{other}`; use \"uniform\" or \"imbalanced\""
            )));
        }
    };
    let simai_env = match (
        aicb.simai.send_lat_us,
        aicb.simai.nvls_enable,
        aicb.simai.pxn_enable,
    ) {
        (Some(send_lat_us), Some(nvls_enable), Some(pxn_enable)) => Some(SimaiEnv {
            send_lat_us,
            nvls_enable,
            pxn_enable,
        }),
        (None, None, None) if fidelity == Fidelity::Megatron => None,
        _ => {
            return Err(invalid(
                "[workload.aicb.simai] send_lat_us, nvls_enable and pxn_enable are required \
                 together under fidelity = \"simai\" (and optional, together, under megatron)",
            ));
        }
    };

    let topology = toml::Value::Table(table.clone())
        .try_into::<ScenarioTopology>()
        .map_err(|error| invalid(format!("[topology]: {error}")))?
        .topology
        .ok_or_else(|| invalid("an AICB scenario needs [topology] category = \"SpectrumX\""))?;
    let spectrum_x = match (topology.category.as_str(), topology.spectrum_x) {
        ("SpectrumX", Some(config)) => config,
        _ => {
            return Err(invalid(
                "an AICB scenario runs on the rail fabric: [topology] category = \"SpectrumX\" \
                 with [topology.spectrum_x]",
            ));
        }
    };
    let simai_topology = aicb
        .simai
        .topology
        .map(|name| check_simai_topology(&name, &spectrum_x).map(|sha| (name, sha.to_owned())))
        .transpose()?;
    let rail = RailTopology::new(&spectrum_x)
        .map_err(|error| invalid(format!("the rail fabric: {error}")))?
        .profile();
    let locality = ServerLocality::new(rail);

    let conf = parse_simai_conf(&conf_text)?;
    let shape = RailShape::from(&spectrum_x);
    let mtu = conf.mtu_bytes()?;
    let fabric = derive_fabric(&conf, &shape, locality.max_bdp_bytes(mtu))?;

    let trace = parse_trace(&trace_text).map_err(|error| prefixed(error, "the AICB trace"))?;
    if u64::from(trace.header.all_gpus) != spectrum_x.gpus {
        return Err(invalid(format!(
            "the trace's all_gpus = {} is not the fabric's {} GPUs",
            trace.header.all_gpus, spectrum_x.gpus
        )));
    }
    let gpus_per_server = u32::try_from(spectrum_x.gpus_per_server)
        .map_err(|_| invalid("gpus_per_server does not fit u32"))?;
    let groups = form_groups(&trace.header, fidelity, gpus_per_server)?;
    let options = PlanOptions {
        fidelity,
        expert_routing,
        mtu_bytes: fabric.mtu_bytes,
        gpu_type: spectrum_x.gpu_type.clone(),
        simai_env,
    };
    let bounds = RailBounds {
        propagation: PropagationBounds {
            link_delay_ns: spectrum_x.link_delay_ns,
            nic_rate_bps: spectrum_x.nic_rate_bps,
            nvlink_delay_ns: spectrum_x.nvlink_delay_ns,
        },
        locality,
        mtu: fabric.mtu_bytes,
    };
    let plan = plan_schedule(&trace, &groups, &options, &bounds)
        .map_err(|error| prefixed(error, "the AICB trace"))?;
    let transport = Transport {
        flow_type: "RoCE".to_owned(),
        priority: fabric.data_priority,
        traffic: transport_traffic(&fabric),
    };
    let workload = lower_plan(
        &plan,
        &groups,
        expert_routing,
        &locality,
        fabric.mtu_bytes,
        transport,
    )?;

    let mut text = toml::to_string(&table).map_err(|error| invalid(error.to_string()))?;
    text.push_str(&fabric_tables(&fabric));
    let stage = &plan.stages[0];
    let manifest = AicbManifest {
        trace: aicb.trace,
        trace_sha256: aicb.trace_sha256,
        records: trace.records.len(),
        tp: trace.header.tp,
        ep: trace.header.ep,
        pp: trace.header.pp,
        ga: trace.header.ga,
        all_gpus: trace.header.all_gpus,
        fidelity,
        expert_routing,
        simai_conf_sha256: aicb.simai.conf_sha256,
        simai_topology,
        simai_env,
        mtu_bytes: fabric.mtu_bytes,
        window_bytes: fabric.roce.window_bytes,
        queue_capacity_packets: fabric.queue_capacity_packets,
        queue_capacity_bytes: fabric.queue_capacity_bytes,
        ecn_by_rate: fabric
            .ecn_by_rate
            .iter()
            .map(|(&rate, ecn)| (rate, ecn.kmin_bytes, ecn.kmax_bytes, ecn.pmax.clone()))
            .collect(),
        pfc_asw: (fabric.pfc_asw.xoff_bytes, fabric.pfc_asw.xon_bytes),
        pfc_psw: (fabric.pfc_psw.xoff_bytes, fabric.pfc_psw.xon_bytes),
        headroom_by_rate: fabric
            .headroom_by_rate
            .iter()
            .map(|(&r, &v)| (r, v))
            .collect(),
        collectives: plan.ops.len(),
        operations: workload.operations.len(),
        fused_segments: stage.segments.len(),
        fused_single_server_ops: stage
            .segments
            .iter()
            .map(|segment| segment.single_server_ops.len())
            .sum(),
        fp_clamps: plan.counters.fp_clamps,
        elided: plan.counters.elided_zero_wg
            + plan.counters.elided_ring_floor
            + plan.counters.elided_zero_all_to_all,
        hang_window_recorded: plan.counters.hang_window_recorded,
        data_queue: plan.data_queue.kind,
        data_queue_order: plan
            .data_queue
            .order
            .iter()
            .map(|&op| trace.records[plan.ops[op].record].name.clone())
            .collect(),
        ecmp_ordinals_exact: plan.ecmp_ordinals_exact,
    };
    Ok(PreparedScenario {
        text,
        workload,
        manifest,
    })
}

fn prefixed(error: AicbError, what: &str) -> AicbError {
    AicbError {
        line: error.line,
        message: format!("{what}: {}", error.message),
    }
}

/// R9's hop bounds on the rail fabric: network hops from link delays and the NIC rate; an NVLink
/// hop at least H2's single-message delay (one message on the port).
struct RailBounds {
    propagation: PropagationBounds,
    locality: ServerLocality,
    mtu: u64,
}

impl HopBounds for RailBounds {
    fn network_hop_ns(&self, bytes: u64) -> u64 {
        self.propagation.network_hop_ns(bytes)
    }

    fn nvlink_hop_ns(&self, bytes: u64, _port_bytes: u64) -> u64 {
        self.locality
            .nvlink_message_delay_ns(bytes, 1, self.mtu)
            .unwrap_or_else(|| self.propagation.nvlink_hop_ns(bytes, bytes))
    }
}

/// An exact decimal of `value / 10^digits` (`50000000, 9` → `0.05`).
fn decimal(value: u64, digits: u32) -> String {
    let scale = 10_u64.pow(digits);
    let (whole, fraction) = (value / scale, value % scale);
    if fraction == 0 {
        return format!("{whole}.0");
    }
    let fraction = format!("{fraction:0width$}", width = digits as usize);
    format!("{whole}.{}", fraction.trim_end_matches('0'))
}

/// The RoCE transport's `[collective.traffic]` body (H1's transport template).
fn transport_traffic(fabric: &SimaiFabric) -> String {
    let dcqcn = &fabric.dcqcn;
    let roce = &fabric.roce;
    format!(
        "initial_delay = 0.0\n\
         arr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\n\
         pkt_size_dist = {{ type = \"DiscreteUniform\", low = {mtu}, high = {mtu} }}\n\n\
         [dcqcn]\n\
         max_rate_gbps = {max}\n\
         min_rate_gbps = {min}\n\
         ai_rate_gbps = {ai}\n\
         hai_rate_gbps = {hai}\n\
         g = {g}\n\
         alpha_resume_interval_ns = {alpha}\n\
         rate_decrease_interval_ns = {decrease}\n\
         rp_timer_ns = {rp}\n\
         fast_recovery_times = {fast}\n\
         clamp_target_rate = {clamp}\n\
         pacing_interval_ns = {pacing}\n\n\
         [roce]\n\
         retransmit_timeout_ns = {rto}\n\
         ack_every_packets = {ack_every}\n\
         nack_interval_ns = {nack}\n\
         feedback_priority = {feedback}\n\
         ack_size_bytes = {ack_size}\n\
         window_bytes = {window}\n\
         variable_window = {variable}\n",
        mtu = fabric.mtu_bytes,
        max = decimal(dcqcn.max_rate_bps, 9),
        min = decimal(dcqcn.min_rate_bps, 9),
        ai = decimal(dcqcn.ai_rate_bps, 9),
        hai = decimal(dcqcn.hai_rate_bps, 9),
        g = dcqcn.g_literal,
        alpha = dcqcn.alpha_resume_interval_ns,
        decrease = dcqcn.rate_decrease_interval_ns,
        rp = dcqcn.rp_timer_ns,
        fast = dcqcn.fast_recovery_times,
        clamp = dcqcn.clamp_target_rate,
        pacing = dcqcn.pacing_interval_ns,
        rto = roce.retransmit_timeout_ns,
        ack_every = roce.ack_every_packets,
        nack = roce.nack_interval_ns,
        feedback = roce.feedback_priority,
        ack_size = roce.ack_size_bytes,
        window = roce.window_bytes,
        variable = roce.variable_window,
    )
}

/// The derived `[routing]`, `[switch]` and `[link]` tables (H2's §4 rows).
fn fabric_tables(fabric: &SimaiFabric) -> String {
    let class = |value: u64| {
        let mut row = [0_u64; 8];
        row[usize::from(fabric.data_priority)] = value;
        format!("{row:?}")
    };
    let rows = |rows: &std::collections::BTreeMap<u64, u64>, name: &str| {
        rows.iter()
            .map(|(rate, value)| format!("{{ rate_bps = {rate}, {name} = {value} }}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "\n[routing]\npolicy = \"SimAiEcmp\"\n\n\
         [switch]\ncapacity = {capacity}\ndiscipline = \"FIFO\"\ndrop = \"TailDrop\"\n\
         ecn_capacity_bytes = {capacity_bytes}\necn_by_rate = [{ecn}]\n\n\
         [link]\nmode = \"Pfc\"\n\n\
         [link.pfc]\nhost_links = true\n\
         by_tier = [{{ tier = \"asw\", xoff = {asw_xoff}, xon = {asw_xon} }}, \
         {{ tier = \"psw\", xoff = {psw_xoff}, xon = {psw_xon} }}]\n\
         headroom_by_rate = [{headroom}]\n",
        capacity = fabric.queue_capacity_packets,
        capacity_bytes = fabric.queue_capacity_bytes,
        ecn = fabric
            .ecn_by_rate
            .iter()
            .map(|(rate, ecn)| format!(
                "{{ rate_bps = {rate}, kmin_bytes = {}, kmax_bytes = {}, pmax = {} }}",
                ecn.kmin_bytes, ecn.kmax_bytes, ecn.pmax
            ))
            .collect::<Vec<_>>()
            .join(", "),
        asw_xoff = class(fabric.pfc_asw.xoff_bytes),
        asw_xon = class(fabric.pfc_asw.xon_bytes),
        psw_xoff = class(fabric.pfc_psw.xoff_bytes),
        psw_xon = class(fabric.pfc_psw.xon_bytes),
        headroom = rows(&fabric.headroom_by_rate, "bytes"),
    )
}

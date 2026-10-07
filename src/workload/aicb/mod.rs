//! The AICB/SimAI workload adapter (P16 lane H3).
//!
//! SimAI's training workloads are AICB trace files: a two-line header and one 12-field record per
//! layer, each with a forward (fp), input-gradient (ig) and weight-gradient (wg) column. This
//! module reads them exactly as SimAI's `Workload::initialize_workload` does, refuses what SimAI
//! would not run (or would run differently from what the file says), forms SimAI's communication
//! groups, and plans the per-rank schedule that Days lowers. Design: days-gpu
//! `evidence/P16/aicb-design.md`. All arithmetic is exact integer arithmetic.

mod error;
mod groups;
mod lower;
mod parse;
mod scenario;
mod schedule;
mod simai_conf;
mod topology;

pub use error::AicbError;
pub use groups::{Fidelity, GroupFamily, Groups, form_groups, render_mockncclgroup};
pub use lower::lower_plan;
pub use parse::{
    Algorithm, Column, ColumnEntry, Comm, GroupKind, Header, Record, Trace, parse_trace,
};
pub use scenario::{AicbManifest, DIVERGENCES, PreparedScenario, is_aicb_scenario, prepare};
pub use schedule::{
    ChainItem, CollectiveOp, DataQueue, DataQueueOrder, ExpertRouting, HopBounds, ImbalanceParams,
    MatrixRef, OpId, PipelineTransfer, Plan, PlanCounters, PlanOptions, PropagationBounds, Segment,
    SegmentEnd, SimaiEnv, StageChain, StartItem, in_hang_window, plan_schedule,
};
pub use simai_conf::{
    INERT_KEYS, PfcTier, RECORDED_KEYS, RailShape, SimaiConf, SimaiDcqcn, SimaiFabric, SimaiRoce,
    derive_fabric, parse_simai_conf,
};
pub use topology::{check_simai_topology, simai_topology};

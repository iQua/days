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
mod parse;
mod schedule;

pub use error::AicbError;
pub use groups::{Fidelity, GroupFamily, Groups, form_groups, render_mockncclgroup};
pub use parse::{
    Algorithm, Column, ColumnEntry, Comm, GroupKind, Header, Record, Trace, parse_trace,
};
pub use schedule::{
    ChainItem, CollectiveOp, DataQueue, DataQueueOrder, ExpertRouting, HopBounds, ImbalanceParams,
    MatrixRef, OpId, PipelineTransfer, Plan, PlanCounters, PlanOptions, PropagationBounds, Segment,
    SegmentEnd, SimaiEnv, StageChain, StartItem, in_hang_window, plan_schedule,
};

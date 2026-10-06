//! The AICB trace grammar (SimAI `Workload.cc:1134-1549`; AICB's writer
//! `SimAI_training_workload_generator.py:841-868`).
//!
//! ```text
//! line 1: HYBRID_TRANSFORMER_FWD_IN_BCKWD model_parallel_NPU_group: <TP> ep: <EP> pp: <PP>
//!         vpp: <L> ga: <GA> all_gpus: <W> checkpoints: 0 checkpoint_initiates: 0 pp_comm: <V>
//! line 2: <N>
//! N lines: name -1 fp_compute fp_type fp_size ig_compute ig_type ig_size
//!          wg_compute wg_type wg_size process_time
//! ```
//!
//! Sizes are bytes of the whole collective; compute and `process_time` are SimAI ticks, which are
//! nanoseconds. Every number is an exact non-negative integer; the parser refuses rather than
//! rounds, and refuses what SimAI tolerates silently (unknown types, extra records).

use super::AicbError;

/// A collective algorithm named by a record column.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Algorithm {
    AllReduce,
    AllGather,
    ReduceScatter,
    AllToAll,
}

/// SimAI's communication group of a column (`MockNccl::GroupType`).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GroupKind {
    Tp,
    Dp,
    Ep,
    DpEp,
}

/// One of a record's three columns.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Column {
    Forward,
    InputGradient,
    WeightGradient,
}

/// A column's collective: its algorithm and group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Comm {
    pub algorithm: Algorithm,
    pub group: GroupKind,
}

/// One column of a record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnEntry {
    pub compute_ns: u64,
    /// `None` for `NONE`.
    pub comm: Option<Comm>,
    /// Bytes of the whole collective, as written (before any SimAI clamp).
    pub size_bytes: u64,
}

/// One trace record (a layer, or a prologue or tail step).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub name: String,
    /// 1-based line in the trace file.
    pub line: usize,
    pub forward: ColumnEntry,
    pub input_gradient: ColumnEntry,
    pub weight_gradient: ColumnEntry,
    /// The delay after each of the record's collectives (`Layer.cc:92-114`).
    pub process_time_ns: u64,
}

impl Record {
    pub fn column(&self, column: Column) -> &ColumnEntry {
        match column {
            Column::Forward => &self.forward,
            Column::InputGradient => &self.input_gradient,
            Column::WeightGradient => &self.weight_gradient,
        }
    }
}

/// The header's parallelism and size fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    /// `model_parallel_NPU_group`.
    pub tp: u32,
    pub ep: u32,
    pub pp: u32,
    pub vpp: u32,
    pub ga: u32,
    pub all_gpus: u32,
    /// Bytes per pipeline-parallel message; 0 exactly when `pp == 1`.
    pub pp_comm_bytes: u64,
}

/// A parsed trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trace {
    pub header: Header,
    pub records: Vec<Record>,
}

/// Parses an AICB trace, refusing anything outside the grammar above.
pub fn parse_trace(_text: &str) -> Result<Trace, AicbError> {
    Err(AicbError::at(1, "the AICB parser is not implemented"))
}

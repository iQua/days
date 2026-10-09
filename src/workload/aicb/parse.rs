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

/// The only parallelism policy AICB writes (`Workload.cc:1046-1071` maps it to
/// `TransformerFwdInBckwd`; `HYBRID_TRANSFORMER` runs a different iterator).
const POLICY: &str = "HYBRID_TRANSFORMER_FWD_IN_BCKWD";

/// The header keys, in AICB's order; every one is required exactly once.
const HEADER_KEYS: [&str; 9] = [
    "model_parallel_NPU_group:",
    "ep:",
    "pp:",
    "vpp:",
    "ga:",
    "all_gpus:",
    "checkpoints:",
    "checkpoint_initiates:",
    "pp_comm:",
];

/// SimAI reads header values with `std::stoi`, which throws above `i32::MAX`.
const STOI_MAX: u64 = i32::MAX as u64;

/// Parses an AICB trace, refusing anything outside the grammar above.
///
/// One pass over borrowed tokens: the only allocations are the record vector and one name per
/// record.
pub fn parse_trace(text: &str) -> Result<Trace, AicbError> {
    if !text.is_ascii() {
        return Err(AicbError::new("the trace is not ASCII"));
    }
    let mut lines = text
        .split('\n')
        .enumerate()
        .map(|(index, line)| (index + 1, line));
    let (_, first) = lines
        .next()
        .ok_or_else(|| AicbError::at(1, "the trace is empty"))?;
    let header = parse_header(first)?;
    let (count_line, count_text) = lines
        .next()
        .ok_or_else(|| AicbError::at(2, "the record count is missing"))?;
    let count = exact_integer(count_text.trim_ascii(), false).ok_or_else(|| {
        AicbError::at(
            count_line,
            format!(
                "record count `{}` is not an exact non-negative integer",
                count_text.trim_ascii()
            ),
        )
    })?;
    let count = usize::try_from(count)
        .map_err(|_| AicbError::at(count_line, "the record count does not fit usize"))?;
    let mut records = Vec::with_capacity(count.min(1 << 20));
    for (line, text) in lines {
        if text.trim_ascii().is_empty() {
            continue;
        }
        records.push(parse_record(line, text)?);
    }
    if records.len() != count {
        return Err(AicbError::at(
            count_line,
            format!(
                "line 2 declares {count} records, the file has {}",
                records.len()
            ),
        ));
    }
    Ok(Trace { header, records })
}

fn parse_header(text: &str) -> Result<Header, AicbError> {
    let refuse = |message: String| AicbError::at(1, message);
    let mut tokens = text.split_ascii_whitespace();
    let policy = tokens.next().unwrap_or("");
    if policy != POLICY {
        return Err(refuse(format!(
            "unsupported parallelism policy `{policy}` (AICB writes `{POLICY}`)"
        )));
    }
    let mut values: [Option<u64>; 9] = [None; 9];
    while let Some(key) = tokens.next() {
        let slot = HEADER_KEYS
            .iter()
            .position(|known| *known == key)
            .ok_or_else(|| refuse(format!("unknown header key `{key}`")))?;
        if values[slot].is_some() {
            return Err(refuse(format!("duplicate header key `{key}`")));
        }
        let value = tokens
            .next()
            .ok_or_else(|| refuse(format!("header key `{key}` has no value")))?;
        // AICB prints `pp_comm` as a Python float (`5242880.0`); only a zero fraction is exact.
        let parsed = exact_integer(value, key == "pp_comm:").ok_or_else(|| {
            refuse(format!(
                "header `{key}` value `{value}` is not an exact non-negative integer"
            ))
        })?;
        if parsed > STOI_MAX {
            return Err(refuse(format!(
                "header `{key}` value {parsed} is above {STOI_MAX} (SimAI reads it with std::stoi)"
            )));
        }
        values[slot] = Some(parsed);
    }
    let get = |slot: usize| {
        values[slot].ok_or_else(|| refuse(format!("missing header key `{}`", HEADER_KEYS[slot])))
    };
    let [
        tp,
        ep,
        pp,
        vpp,
        ga,
        all_gpus,
        checkpoints,
        checkpoint_initiates,
        pp_comm,
    ] = [
        get(0)?,
        get(1)?,
        get(2)?,
        get(3)?,
        get(4)?,
        get(5)?,
        get(6)?,
        get(7)?,
        get(8)?,
    ];
    for (name, value) in [
        ("model_parallel_NPU_group", tp),
        ("ep", ep),
        ("pp", pp),
        ("vpp", vpp),
        ("ga", ga),
        ("all_gpus", all_gpus),
    ] {
        if value == 0 {
            return Err(refuse(format!("{name} must be positive")));
        }
    }
    for (name, value) in [
        ("checkpoints", checkpoints),
        ("checkpoint_initiates", checkpoint_initiates),
    ] {
        if value != 0 {
            return Err(refuse(format!(
                "{name} = {value} (forward-in-backward recomputation is not modelled)"
            )));
        }
    }
    // SimAI only warns on these (`Workload.cc:1255-1263`); Days refuses.
    if (pp == 1) != (pp_comm == 0) {
        return Err(refuse(format!("pp = {pp} with pp_comm = {pp_comm}")));
    }
    let narrow = |value: u64| u32::try_from(value).expect("bounded by STOI_MAX");
    Ok(Header {
        tp: narrow(tp),
        ep: narrow(ep),
        pp: narrow(pp),
        vpp: narrow(vpp),
        ga: narrow(ga),
        all_gpus: narrow(all_gpus),
        pp_comm_bytes: pp_comm,
    })
}

fn parse_record(line: usize, text: &str) -> Result<Record, AicbError> {
    let mut fields = [""; 12];
    let mut count = 0;
    for token in text.split_ascii_whitespace() {
        if count < fields.len() {
            fields[count] = token;
        }
        count += 1;
    }
    if count != fields.len() {
        return Err(AicbError::at(line, format!("{count} fields, expected 12")));
    }
    if fields[1] != "-1" {
        return Err(AicbError::at(
            line,
            format!("depen = `{}`, expected -1", fields[1]),
        ));
    }
    // The label is formatted only on a refusal: a record allocates nothing but its name.
    let integer = |index: usize, label: &str, suffix: &str| {
        exact_integer(fields[index], false).ok_or_else(|| {
            AicbError::at(
                line,
                format!(
                    "{label}{suffix} `{}` is not an exact non-negative integer that fits u64",
                    fields[index]
                ),
            )
        })
    };
    let entry = |base: usize, column: Column, label: &str| -> Result<ColumnEntry, AicbError> {
        Ok(ColumnEntry {
            compute_ns: integer(base, label, "_compute")?,
            comm: comm_type(line, fields[base + 1], column, label)?,
            size_bytes: integer(base + 2, label, "_comm_size")?,
        })
    };
    Ok(Record {
        name: fields[0].to_owned(),
        line,
        forward: entry(2, Column::Forward, "fp")?,
        input_gradient: entry(5, Column::InputGradient, "ig")?,
        weight_gradient: entry(8, Column::WeightGradient, "wg")?,
        process_time_ns: integer(11, "process_time", "")?,
    })
}

/// SimAI's type strings (`Workload.cc:1318-1489`). The bare name is TP in the fp and ig columns
/// and DP in the wg column; `_EP` and `_DP_EP` name those groups in every column.
fn comm_type(
    line: usize,
    token: &str,
    column: Column,
    label: &str,
) -> Result<Option<Comm>, AicbError> {
    if token == "NONE" {
        return Ok(None);
    }
    if token.starts_with("ALLREDUCEALLTOALL") {
        return Err(AicbError::at(
            line,
            format!(
                "{label} comm type `{token}`: SimAI's ALLREDUCE prefix test catches it and gives it no group"
            ),
        ));
    }
    for (base, algorithm) in [
        ("ALLREDUCE", Algorithm::AllReduce),
        ("ALLGATHER", Algorithm::AllGather),
        ("REDUCESCATTER", Algorithm::ReduceScatter),
        ("ALLTOALL", Algorithm::AllToAll),
    ] {
        let Some(suffix) = token.strip_prefix(base) else {
            continue;
        };
        let group = match suffix {
            "" if column == Column::WeightGradient => GroupKind::Dp,
            "" => GroupKind::Tp,
            "_EP" => GroupKind::Ep,
            "_DP_EP" => GroupKind::DpEp,
            _ => break,
        };
        return Ok(Some(Comm { algorithm, group }));
    }
    Err(AicbError::at(
        line,
        format!("unknown {label} comm type `{token}`"),
    ))
}

/// A decimal integer with no sign, no leading zero (except `0` itself) and no exponent; with
/// `zero_fraction`, a `.0…0` suffix is also exact. `None` if malformed or above `u64::MAX`.
fn exact_integer(token: &str, zero_fraction: bool) -> Option<u64> {
    let digits = match token.split_once('.') {
        Some((whole, fraction))
            if zero_fraction && !fraction.is_empty() && fraction.bytes().all(|b| b == b'0') =>
        {
            whole
        }
        Some(_) => return None,
        None => token,
    };
    if digits.is_empty()
        || !digits.bytes().all(|b| b.is_ascii_digit())
        || (digits.len() > 1 && digits.starts_with('0'))
    {
        return None;
    }
    digits.parse().ok()
}
